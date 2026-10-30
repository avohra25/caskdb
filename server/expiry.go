package server

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/avohra25/caskdb"
)

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// maxTTLMs is the largest relative TTL that fits in a time.Duration.
const maxTTLMs = math.MaxInt64 / int64(time.Millisecond)

// expiry is a parsed expiry argument: either relative (ttl) or absolute (at).
type expiry struct {
	set bool
	ttl time.Duration
	at  time.Time
	abs bool
}

// parseExpiry interprets n in the unit implied by the option name: EX and
// EXPIRE take seconds, PX and PEXPIRE milliseconds, and the *AT forms are
// unix timestamps. allowNonPositive permits values <= 0 (EXPIRE deletes the
// key in that case; SET rejects them).
func parseExpiry(opt string, raw []byte, allowNonPositive bool) (expiry, bool) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || (!allowNonPositive && n <= 0) {
		return expiry{}, false
	}
	seconds := !strings.HasPrefix(opt, "P")
	if seconds {
		if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
			return expiry{}, false
		}
		n *= 1000
	}
	if strings.HasSuffix(opt, "AT") {
		return expiry{set: true, abs: true, at: time.UnixMilli(n)}, true
	}
	if n > maxTTLMs || n < -maxTTLMs {
		return expiry{}, false
	}
	return expiry{set: true, ttl: time.Duration(n) * time.Millisecond}, true
}

func writeDBError(w *bufio.Writer, err error) {
	writeError(w, "ERR "+err.Error())
}

// set implements SET key value [NX|XX] [GET] [EX|PX|EXAT|PXAT n|KEEPTTL].
func (s *Server) set(w *bufio.Writer, args [][]byte) {
	key, val := args[1], args[2]
	var nx, xx, get, keep bool
	var exp expiry
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			if exp.set {
				writeError(w, "ERR syntax error")
				return
			}
			keep = true
		case "EX", "PX", "EXAT", "PXAT":
			if exp.set || keep || i+1 == len(args) {
				writeError(w, "ERR syntax error")
				return
			}
			i++
			e, ok := parseExpiry(opt, args[i], false)
			if !ok {
				writeError(w, "ERR invalid expire time in 'set' command")
				return
			}
			exp = e
		default:
			writeError(w, "ERR syntax error")
			return
		}
	}
	if nx && xx {
		writeError(w, "ERR syntax error")
		return
	}

	s.wmu.Lock()
	defer s.wmu.Unlock()
	old, err := s.DB.Get(key)
	exists := err == nil
	if err != nil && !errors.Is(err, caskdb.ErrNotFound) {
		writeDBError(w, err)
		return
	}
	reply := func(applied bool) {
		switch {
		case get && exists:
			writeBulk(w, nonNil(old))
		case get:
			writeBulk(w, nil)
		case applied:
			writeSimple(w, "OK")
		default:
			writeBulk(w, nil)
		}
	}
	if (nx && exists) || (xx && !exists) {
		reply(false)
		return
	}
	switch {
	case exp.set && exp.abs:
		err = s.DB.PutWithExpiry(key, val, exp.at)
	case exp.set:
		err = s.DB.PutWithTTL(key, val, exp.ttl)
	case keep && exists:
		var at time.Time
		if at, err = s.DB.ExpiresAt(key); err == nil {
			if at.IsZero() {
				err = s.DB.Put(key, val)
			} else {
				err = s.DB.PutWithExpiry(key, val, at)
			}
		}
	default:
		err = s.DB.Put(key, val)
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	reply(true)
}

// setex implements SETEX key seconds value and PSETEX key ms value.
func (s *Server) setex(w *bufio.Writer, name string, args [][]byte) {
	opt := "EX"
	if name == "PSETEX" {
		opt = "PX"
	}
	exp, ok := parseExpiry(opt, args[2], false)
	if !ok {
		writeError(w, fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(name)))
		return
	}
	s.wmu.Lock()
	err := s.DB.PutWithTTL(args[1], args[3], exp.ttl)
	s.wmu.Unlock()
	if err != nil {
		writeDBError(w, err)
		return
	}
	writeSimple(w, "OK")
}

// expire implements EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT with the
// optional NX, XX, GT and LT conditions. A key without an expiry counts as
// an infinite TTL for GT and LT, as in Redis.
func (s *Server) expire(w *bufio.Writer, name string, args [][]byte) {
	exp, ok := parseExpiry(name, args[2], true)
	if !ok {
		writeError(w, fmt.Sprintf("ERR invalid expire time in '%s' command", strings.ToLower(name)))
		return
	}
	cond := ""
	if len(args) == 4 {
		cond = strings.ToUpper(string(args[3]))
		switch cond {
		case "NX", "XX", "GT", "LT":
		default:
			writeError(w, "ERR Unsupported option "+string(args[3]))
			return
		}
	}

	s.wmu.Lock()
	defer s.wmu.Unlock()
	cur, err := s.DB.ExpiresAt(args[1])
	if errors.Is(err, caskdb.ErrNotFound) {
		writeInt(w, 0)
		return
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	if cond != "" {
		target := exp.at
		if !exp.abs {
			ttl, err := s.DB.TTL(args[1])
			if err != nil {
				writeDBError(w, err)
				return
			}
			// Compare on the same clock: target = current expiry - ttl + new ttl.
			if !cur.IsZero() {
				target = cur.Add(exp.ttl - ttl)
			}
		}
		permanent := cur.IsZero()
		var apply bool
		switch cond {
		case "NX":
			apply = permanent
		case "XX":
			apply = !permanent
		case "GT":
			apply = !permanent && target.After(cur)
		case "LT":
			apply = permanent || target.Before(cur)
		}
		if !apply {
			writeInt(w, 0)
			return
		}
	}
	var set bool
	if exp.abs {
		set, err = s.DB.ExpireAt(args[1], exp.at)
	} else {
		set, err = s.DB.Expire(args[1], exp.ttl)
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	writeInt(w, boolInt(set))
}

// ttl implements TTL, PTTL, EXPIRETIME and PEXPIRETIME: -2 for a missing
// key, -1 for a key without an expiry.
func (s *Server) ttl(w *bufio.Writer, name string, key []byte) {
	if name == "EXPIRETIME" || name == "PEXPIRETIME" {
		at, err := s.DB.ExpiresAt(key)
		switch {
		case errors.Is(err, caskdb.ErrNotFound):
			writeInt(w, -2)
		case err != nil:
			writeDBError(w, err)
		case at.IsZero():
			writeInt(w, -1)
		case name == "EXPIRETIME":
			writeInt(w, at.Unix())
		default:
			writeInt(w, at.UnixMilli())
		}
		return
	}
	d, err := s.DB.TTL(key)
	switch {
	case errors.Is(err, caskdb.ErrNotFound):
		writeInt(w, -2)
	case err != nil:
		writeDBError(w, err)
	case d == caskdb.NoTTL:
		writeInt(w, -1)
	case name == "TTL":
		writeInt(w, (d.Milliseconds()+500)/1000)
	default:
		writeInt(w, d.Milliseconds())
	}
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
