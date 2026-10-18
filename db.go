// Package caskdb is an embeddable, log-structured key/value store modelled on
// Bitcask: every write is appended to a data file, an in-memory hash index
// ("keydir") maps each live key to the exact position of its latest value, and
// a background-safe merge rewrites live data to reclaim space.
package caskdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// MaxKeySize is the largest accepted key, in bytes.
	MaxKeySize = 64 << 10
	// MaxValueSize is the largest accepted value, in bytes.
	MaxValueSize = 64 << 20
	// DefaultMaxFileSize is the size at which the active data file is rotated.
	DefaultMaxFileSize = 64 << 20
)

var (
	ErrNotFound      = errors.New("caskdb: key not found")
	ErrClosed        = errors.New("caskdb: database is closed")
	ErrEmptyKey      = errors.New("caskdb: empty key")
	ErrKeyTooLarge   = errors.New("caskdb: key too large")
	ErrValueTooLarge = errors.New("caskdb: value too large")
)

// Options tune a database. The zero value is valid.
type Options struct {
	// MaxFileSize is the rotation threshold for data files
	// (default DefaultMaxFileSize).
	MaxFileSize int64
	// SyncOnPut fsyncs the active file after every Put and Delete. Without it
	// writes reach the OS page cache immediately but are only durable after
	// Sync, Close or file rotation.
	SyncOnPut bool
}

type entry struct {
	fileID uint32
	offset int64
	size   uint32
	ts     uint64
}

// DB is a handle to an open database directory. It is safe for concurrent
// use by multiple goroutines.
type DB struct {
	mu      sync.RWMutex
	mergeMu sync.Mutex

	dir  string
	opts Options
	lock *os.File

	keydir map[string]entry
	tombs  map[string]uint64 // only populated while loading

	files      map[uint32]*os.File
	active     *os.File
	activeID   uint32
	activeSize int64
	nextID     uint32

	clock  uint64
	closed bool
}

const (
	dataExt = ".data"
	hintExt = ".hint"
	tmpExt  = ".tmp"
)

func fileName(dir string, id uint32, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%09d%s", id, ext))
}

// Open opens (or creates) the database stored in dir.
func Open(dir string, opts *Options) (*DB, error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	if o.MaxFileSize <= 0 {
		o.MaxFileSize = DefaultMaxFileSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	db := &DB{
		dir:    dir,
		opts:   o,
		lock:   lock,
		keydir: make(map[string]entry),
		tombs:  make(map[string]uint64),
		files:  make(map[uint32]*os.File),
	}
	if err := db.load(); err != nil {
		db.closeFiles()
		unlockDir(lock)
		return nil, err
	}
	db.tombs = nil
	return db, nil
}

func listIDs(dir, ext string) ([]uint32, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ext) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ext), 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(n))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (db *DB) load() error {
	if err := db.prepareDir(); err != nil {
		return err
	}
	ids, err := listIDs(db.dir, dataExt)
	if err != nil {
		return err
	}
	for i, id := range ids {
		last := i == len(ids)-1
		flag := os.O_RDONLY
		if last {
			flag = os.O_RDWR
		}
		f, err := os.OpenFile(fileName(db.dir, id, dataExt), flag, 0)
		if err != nil {
			return err
		}
		db.files[id] = f
		if err := db.loadFile(id, f, last); err != nil {
			return err
		}
		db.nextID = id + 1
	}
	if db.nextID == 0 {
		db.nextID = 1
	}
	return db.openActive(ids)
}

// openActive reuses the newest data file for appends when possible, or
// starts a fresh one.
func (db *DB) openActive(ids []uint32) error {
	if n := len(ids); n > 0 && db.canAppend(ids[n-1]) {
		id := ids[n-1]
		db.files[id].Close()
		f, err := os.OpenFile(fileName(db.dir, id, dataExt), os.O_RDWR|os.O_APPEND, 0)
		if err != nil {
			return err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		db.files[id], db.active, db.activeID, db.activeSize = f, f, id, st.Size()
		return nil
	}
	return db.newActive()
}

func (db *DB) newActive() error {
	id := db.nextID
	db.nextID++
	f, err := os.OpenFile(fileName(db.dir, id, dataExt), os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	db.files[id], db.active, db.activeID, db.activeSize = f, f, id, 0
	return nil
}

// applyLoaded folds one record found during startup into the keydir,
// resolving conflicts by logical timestamp so file order never matters.
func (db *DB) applyLoaded(key string, tomb bool, e entry) {
	if e.ts > db.clock {
		db.clock = e.ts
	}
	var newest uint64
	seen := false
	if cur, ok := db.keydir[key]; ok {
		newest, seen = cur.ts, true
	}
	if t, ok := db.tombs[key]; ok && (!seen || t > newest) {
		newest, seen = t, true
	}
	if seen && e.ts < newest {
		return
	}
	if tomb {
		delete(db.keydir, key)
		db.tombs[key] = e.ts
	} else {
		db.keydir[key] = e
		delete(db.tombs, key)
	}
}

func checkKV(key, value []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	case len(value) > MaxValueSize:
		return ErrValueTooLarge
	}
	return nil
}

// Put stores value under key, replacing any previous value.
func (db *DB) Put(key, value []byte) error {
	if err := checkKV(key, value); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.clock++
	e, err := db.appendLocked(&record{ts: db.clock, key: key, value: value})
	if err != nil {
		return err
	}
	db.keydir[string(key)] = e
	return nil
}

// Delete removes key. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	if err := checkKV(key, nil); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if _, ok := db.keydir[string(key)]; !ok {
		return nil
	}
	db.clock++
	if _, err := db.appendLocked(&record{ts: db.clock, flags: flagTombstone, key: key}); err != nil {
		return err
	}
	delete(db.keydir, string(key))
	return nil
}

func (db *DB) appendLocked(r *record) (entry, error) {
	buf := encodeRecord(r)
	if db.activeSize > 0 && db.activeSize+int64(len(buf)) > db.opts.MaxFileSize {
		if err := db.rotateLocked(); err != nil {
			return entry{}, err
		}
	}
	if _, err := db.active.Write(buf); err != nil {
		// Drop any partial write so the log stays well formed.
		db.active.Truncate(db.activeSize)
		return entry{}, err
	}
	if db.opts.SyncOnPut {
		if err := db.active.Sync(); err != nil {
			return entry{}, err
		}
	}
	e := entry{fileID: db.activeID, offset: db.activeSize, size: uint32(len(buf)), ts: r.ts}
	db.activeSize += int64(len(buf))
	return e, nil
}

// rotateLocked seals the active file and starts a new one. The sealed file
// stays open for reads.
func (db *DB) rotateLocked() error {
	if err := db.active.Sync(); err != nil {
		return err
	}
	return db.newActive()
}

// Get returns a copy of the value stored under key, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	e, ok := db.keydir[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	rec, err := db.readLocked(e)
	if err != nil {
		return nil, err
	}
	if string(rec.key) != string(key) {
		return nil, fmt.Errorf("%w: index points at wrong key", ErrCorrupt)
	}
	return rec.value, nil
}

func (db *DB) readLocked(e entry) (*record, error) {
	f, ok := db.files[e.fileID]
	if !ok {
		return nil, fmt.Errorf("caskdb: data file %d missing", e.fileID)
	}
	buf := make([]byte, e.size)
	if _, err := f.ReadAt(buf, e.offset); err != nil {
		return nil, fmt.Errorf("caskdb: read file %d at %d: %w", e.fileID, e.offset, err)
	}
	return decodeRecord(buf)
}

// Has reports whether key is present.
func (db *DB) Has(key []byte) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.keydir[string(key)]
	return ok
}

// Len returns the number of live keys.
func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.keydir)
}

// Keys returns all live keys in sorted order.
func (db *DB) Keys() []string {
	db.mu.RLock()
	keys := make([]string, 0, len(db.keydir))
	for k := range db.keydir {
		keys = append(keys, k)
	}
	db.mu.RUnlock()
	sort.Strings(keys)
	return keys
}

// Fold calls fn for every live key/value pair in key order, stopping at the
// first error. Keys deleted while folding are skipped.
func (db *DB) Fold(fn func(key, value []byte) error) error {
	for _, k := range db.Keys() {
		v, err := db.Get([]byte(k))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

// Sync flushes the active data file to stable storage.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.active.Sync()
}

// Stats describes the on-disk state of a database.
type Stats struct {
	Keys      int
	DataFiles int
	DiskBytes int64 // total size of all data files
	LiveBytes int64 // bytes occupied by the latest version of live keys
}

// Stats returns a snapshot of database statistics.
func (db *DB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Stats{}, ErrClosed
	}
	s := Stats{Keys: len(db.keydir), DataFiles: len(db.files)}
	for _, e := range db.keydir {
		s.LiveBytes += int64(e.size)
	}
	for _, f := range db.files {
		st, err := f.Stat()
		if err != nil {
			return Stats{}, err
		}
		s.DiskBytes += st.Size()
	}
	return s, nil
}

// Close syncs and closes the database. Further calls return ErrClosed.
func (db *DB) Close() error {
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	err := db.active.Sync()
	if cerr := db.closeFiles(); err == nil {
		err = cerr
	}
	unlockDir(db.lock)
	return err
}

func (db *DB) closeFiles() error {
	var first error
	for id, f := range db.files {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
		delete(db.files, id)
	}
	return first
}
