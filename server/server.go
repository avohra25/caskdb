// Package server exposes a caskdb database over TCP using a subset of the
// Redis protocol (RESP2), so redis-cli and ordinary Redis client libraries
// can talk to it.
//
// Supported commands: PING [msg], ECHO msg, GET key,
// SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL],
// SETEX key s value, PSETEX key ms value, DEL key [key ...],
// EXISTS key [key ...], EXPIRE/PEXPIRE key ttl [NX|XX|GT|LT],
// EXPIREAT/PEXPIREAT key ts [NX|XX|GT|LT], TTL key, PTTL key,
// EXPIRETIME key, PEXPIRETIME key, PERSIST key, KEYS [pattern], DBSIZE,
// MERGE, INFO, QUIT.
package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"path"
	"strings"
	"sync"

	"github.com/avohra25/caskdb"
)

// Server serves one database to any number of TCP clients.
type Server struct {
	DB     *caskdb.DB
	Logger *log.Logger // optional

	// wmu serialises commands that read and then write (SET NX, EXPIRE GT,
	// DEL, ...) so each one is atomic with respect to other clients.
	wmu sync.Mutex

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
	done  bool
}

// New returns a server for db.
func New(db *caskdb.DB) *Server { return &Server{DB: db} }

// ErrServerClosed is returned by Serve after Close.
var ErrServerClosed = errors.New("server: closed")

// ListenAndServe listens on addr and serves until Close is called.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.ln = ln
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.mu.Unlock()
	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			done := s.done
			s.mu.Unlock()
			if done {
				return ErrServerClosed
			}
			return err
		}
		s.mu.Lock()
		if s.done {
			s.mu.Unlock()
			c.Close()
			return ErrServerClosed
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(c)
	}
}

// Close stops accepting, disconnects clients and waits for handlers to exit.
// It does not close the database.
func (s *Server) Close() error {
	s.mu.Lock()
	s.done = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) logf(format string, args ...any) {
	if s.Logger != nil {
		s.Logger.Printf(format, args...)
	}
}

func (s *Server) handle(c net.Conn) {
	defer func() {
		c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		s.wg.Done()
	}()
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)
	for {
		args, err := readCommand(r)
		if err != nil {
			if errors.Is(err, errProtocol) {
				writeError(w, "ERR "+err.Error())
				w.Flush()
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logf("caskdb server: %v: %v", c.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		quit := s.dispatch(w, args)
		// Flush only once the client has nothing more buffered, so pipelined
		// commands are answered in one write.
		if r.Buffered() == 0 || quit {
			if err := w.Flush(); err != nil {
				return
			}
		}
		if quit {
			return
		}
	}
}

func arity(w *bufio.Writer, name string, args [][]byte, min, max int) bool {
	n := len(args) - 1
	if n < min || (max >= 0 && n > max) {
		writeError(w, fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
		return false
	}
	return true
}

// dispatch executes one command and reports whether the client asked to quit.
func (s *Server) dispatch(w *bufio.Writer, args [][]byte) bool {
	name := strings.ToUpper(string(args[0]))
	switch name {
	case "PING":
		if !arity(w, name, args, 0, 1) {
			break
		}
		if len(args) == 2 {
			writeBulk(w, args[1])
		} else {
			writeSimple(w, "PONG")
		}
	case "ECHO":
		if arity(w, name, args, 1, 1) {
			writeBulk(w, args[1])
		}
	case "GET":
		if !arity(w, name, args, 1, 1) {
			break
		}
		v, err := s.DB.Get(args[1])
		switch {
		case errors.Is(err, caskdb.ErrNotFound):
			writeBulk(w, nil)
		case err != nil:
			writeError(w, "ERR "+err.Error())
		default:
			if v == nil {
				v = []byte{}
			}
			writeBulk(w, v)
		}
	case "SET":
		if arity(w, name, args, 2, -1) {
			s.set(w, args)
		}
	case "SETEX", "PSETEX":
		if arity(w, name, args, 3, 3) {
			s.setex(w, name, args)
		}
	case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT":
		if arity(w, name, args, 2, 3) {
			s.expire(w, name, args)
		}
	case "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME":
		if arity(w, name, args, 1, 1) {
			s.ttl(w, name, args[1])
		}
	case "PERSIST":
		if !arity(w, name, args, 1, 1) {
			break
		}
		s.wmu.Lock()
		ok, err := s.DB.Persist(args[1])
		s.wmu.Unlock()
		if err != nil {
			writeError(w, "ERR "+err.Error())
		} else {
			writeInt(w, boolInt(ok))
		}
	case "DEL":
		if !arity(w, name, args, 1, -1) {
			break
		}
		s.wmu.Lock()
		defer s.wmu.Unlock()
		var n int64
		for _, k := range args[1:] {
			if !s.DB.Has(k) {
				continue
			}
			if err := s.DB.Delete(k); err != nil {
				writeError(w, "ERR "+err.Error())
				return false
			}
			n++
		}
		writeInt(w, n)
	case "EXISTS":
		if !arity(w, name, args, 1, -1) {
			break
		}
		var n int64
		for _, k := range args[1:] {
			if s.DB.Has(k) {
				n++
			}
		}
		writeInt(w, n)
	case "KEYS":
		if !arity(w, name, args, 0, 1) {
			break
		}
		pattern := "*"
		if len(args) == 2 {
			pattern = string(args[1])
		}
		if _, err := path.Match(pattern, ""); err != nil {
			writeError(w, "ERR invalid pattern")
			break
		}
		var out []string
		for _, k := range s.DB.Keys() {
			if ok, _ := path.Match(pattern, k); ok {
				out = append(out, k)
			}
		}
		writeArray(w, out)
	case "DBSIZE":
		if arity(w, name, args, 0, 0) {
			writeInt(w, int64(s.DB.Len()))
		}
	case "MERGE":
		if !arity(w, name, args, 0, 0) {
			break
		}
		if err := s.DB.Merge(); err != nil {
			writeError(w, "ERR "+err.Error())
		} else {
			writeSimple(w, "OK")
		}
	case "INFO":
		st, err := s.DB.Stats()
		if err != nil {
			writeError(w, "ERR "+err.Error())
			break
		}
		writeBulk(w, []byte(fmt.Sprintf("# caskdb\r\nkeys:%d\r\nexpiring_keys:%d\r\nexpired_pending:%d\r\ndata_files:%d\r\ndisk_bytes:%d\r\nlive_bytes:%d\r\n",
			st.Keys, st.Expiring, st.Expired, st.DataFiles, st.DiskBytes, st.LiveBytes)))
	case "QUIT":
		writeSimple(w, "OK")
		return true
	default:
		writeError(w, fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	return false
}
