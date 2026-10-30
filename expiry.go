package caskdb

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidTTL is returned for a non-positive TTL or a zero expiry time.
var ErrInvalidTTL = errors.New("caskdb: invalid expiry")

// NoTTL is returned by TTL for a key that exists but never expires.
const NoTTL time.Duration = -1

// expiryMs converts an absolute expiry time to the stored representation.
// Times at or before the Unix epoch map to 1 ms, which is always in the past.
func expiryMs(at time.Time) (int64, error) {
	if at.IsZero() {
		return 0, ErrInvalidTTL
	}
	ms := at.UnixMilli()
	if ms < 1 {
		ms = 1
	}
	return ms, nil
}

// PutWithTTL stores value under key and makes it expire after ttl.
// Expiry has millisecond resolution; once it passes, Get reports
// ErrNotFound and the key disappears from Keys, Len and Fold.
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return db.PutWithExpiry(key, value, db.opts.Now().Add(ttl))
}

// PutWithExpiry stores value under key and makes it expire at the given
// time. An expiry that has already passed deletes the key instead.
func (db *DB) PutWithExpiry(key, value []byte, at time.Time) error {
	ms, err := expiryMs(at)
	if err != nil {
		return err
	}
	return db.put(key, value, ms)
}

// Expire sets a key's time to live, replacing any previous expiry. It
// reports whether the key existed. A non-positive ttl deletes the key.
func (db *DB) Expire(key []byte, ttl time.Duration) (bool, error) {
	return db.ExpireAt(key, db.opts.Now().Add(ttl))
}

// ExpireAt makes key expire at the given time and reports whether the key
// existed. A time that has already passed deletes the key.
func (db *DB) ExpireAt(key []byte, at time.Time) (bool, error) {
	ms, err := expiryMs(at)
	if err != nil {
		return false, err
	}
	return db.rewriteExpiry(key, ms, false)
}

// Persist removes a key's expiry. It reports whether an expiry was removed
// (false when the key is missing or already permanent).
func (db *DB) Persist(key []byte) (bool, error) {
	return db.rewriteExpiry(key, 0, true)
}

// rewriteExpiry appends a copy of key's current value with a new expiry.
// The value has to be rewritten because expiry lives in the data record,
// which keeps the log the single source of truth across restarts.
func (db *DB) rewriteExpiry(key []byte, ms int64, persist bool) (bool, error) {
	if err := checkKV(key, nil); err != nil {
		return false, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, ErrClosed
	}
	cur, ok := db.keydir[string(key)]
	if !ok || cur.expired(db.nowMs()) {
		return false, nil
	}
	if persist && cur.expires == 0 {
		return false, nil
	}
	rec, err := db.readLocked(cur)
	if err != nil {
		return false, err
	}
	if string(rec.key) != string(key) {
		return false, fmt.Errorf("%w: index points at wrong key", ErrCorrupt)
	}
	return true, db.putLocked(key, rec.value, ms)
}

// TTL returns how long key has left to live, NoTTL if it never expires, or
// ErrNotFound if it is missing or already expired.
func (db *DB) TTL(key []byte) (time.Duration, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, ErrClosed
	}
	e, ok := db.keydir[string(key)]
	now := db.nowMs()
	switch {
	case !ok || e.expired(now):
		return 0, ErrNotFound
	case e.expires == 0:
		return NoTTL, nil
	}
	return time.Duration(e.expires-now) * time.Millisecond, nil
}

// SweepExpired removes every expired key from the in-memory index and
// returns how many it removed. Their records stay on disk until the next
// Merge. It holds the write lock for one pass over the index.
func (db *DB) SweepExpired() (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrClosed
	}
	now, n := db.nowMs(), 0
	for k, e := range db.keydir {
		if e.expired(now) {
			delete(db.keydir, k)
			n++
		}
	}
	return n, nil
}

func (db *DB) sweepLoop(every time.Duration) {
	defer close(db.sweepDone)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-db.sweepStop:
			return
		case <-t.C:
			if _, err := db.SweepExpired(); err != nil {
				return
			}
		}
	}
}

func (db *DB) stopSweeper() {
	if db.sweepStop == nil {
		return
	}
	db.stopOnce.Do(func() {
		close(db.sweepStop)
		<-db.sweepDone
	})
}

// ExpiresAt returns the time at which key expires, the zero Time if it
// never expires, or ErrNotFound if it is missing or already expired.
func (db *DB) ExpiresAt(key []byte) (time.Time, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return time.Time{}, ErrClosed
	}
	e, ok := db.keydir[string(key)]
	switch {
	case !ok || e.expired(db.nowMs()):
		return time.Time{}, ErrNotFound
	case e.expires == 0:
		return time.Time{}, nil
	}
	return time.UnixMilli(e.expires), nil
}
