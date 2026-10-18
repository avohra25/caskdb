package caskdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"sync"
	"testing"
)

func openT(t *testing.T, dir string, opts *Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) err = %v, want ErrNotFound", key, err)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	r := &record{ts: 42, key: []byte("k"), value: []byte("hello")}
	got, err := decodeRecord(encodeRecord(r))
	if err != nil {
		t.Fatal(err)
	}
	if got.ts != 42 || string(got.key) != "k" || string(got.value) != "hello" || got.tombstone() {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	buf := encodeRecord(r)
	buf[len(buf)-1] ^= 0xff
	if _, err := decodeRecord(buf); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("flipped byte: err = %v, want ErrCorrupt", err)
	}
}

func TestPutGetDelete(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()

	mustMissing(t, db, "a")
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1")
	if err := db.Put([]byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "2")
	if err := db.Put([]byte("empty"), nil); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "empty", "")
	if !db.Has([]byte("a")) || db.Len() != 2 {
		t.Fatalf("Has/Len wrong: len=%d", db.Len())
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	mustMissing(t, db, "a")
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d, want 1", db.Len())
	}
}

func TestValidation(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	if err := db.Put(nil, []byte("x")); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("empty key: %v", err)
	}
	if err := db.Put(make([]byte, MaxKeySize+1), nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Errorf("big key: %v", err)
	}
	if err := db.Put([]byte("k"), make([]byte, MaxValueSize+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("big value: %v", err)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	val := []byte("original")
	db.Put([]byte("k"), val)
	val[0] = 'X'
	got, _ := db.Get([]byte("k"))
	got[1] = 'Y'
	mustGet(t, db, "k", "original")
}

func TestReopenPersists(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	db.Put([]byte("key007"), []byte("updated"))
	db.Delete([]byte("key050"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openT(t, dir, nil)
	defer db.Close()
	if db.Len() != 99 {
		t.Fatalf("Len after reopen = %d, want 99", db.Len())
	}
	mustGet(t, db, "key007", "updated")
	mustGet(t, db, "key099", "v99")
	mustMissing(t, db, "key050")

	// The logical clock must continue past what is on disk so new writes win.
	db.Put([]byte("key007"), []byte("newest"))
	db.Close()
	db = openT(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "key007", "newest")
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, &Options{MaxFileSize: 256})
	for i := 0; i < 50; i++ {
		db.Put([]byte(fmt.Sprintf("k%02d", i)), bytes.Repeat([]byte{'v'}, 40))
	}
	st, _ := db.Stats()
	if st.DataFiles < 5 {
		t.Fatalf("expected rotation into several files, got %d", st.DataFiles)
	}
	db.Close()
	db = openT(t, dir, &Options{MaxFileSize: 256})
	defer db.Close()
	for i := 0; i < 50; i++ {
		mustGet(t, db, fmt.Sprintf("k%02d", i), string(bytes.Repeat([]byte{'v'}, 40)))
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	db.Put([]byte("good"), []byte("value"))
	db.Put([]byte("torn"), []byte("this record will be cut short"))
	db.Close()

	path := fileName(dir, 1, dataExt)
	st, _ := os.Stat(path)
	if err := os.Truncate(path, st.Size()-5); err != nil {
		t.Fatal(err)
	}

	db = openT(t, dir, nil)
	mustGet(t, db, "good", "value")
	mustMissing(t, db, "torn")
	// Appends after recovery must land on a clean record boundary.
	db.Put([]byte("after"), []byte("crash"))
	db.Close()

	db = openT(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "good", "value")
	mustGet(t, db, "after", "crash")
}

func TestCorruptionInSealedFileIsReported(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, &Options{MaxFileSize: 64})
	for i := 0; i < 10; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("0123456789abcdef"))
	}
	db.Close()

	path := fileName(dir, 1, dataExt)
	data, _ := os.ReadFile(path)
	data[headerSize] ^= 0xff
	os.WriteFile(path, data, 0o644)

	if _, err := Open(dir, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
}

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	if _, err := Open(dir, nil); !errors.Is(err, ErrLocked) {
		t.Skipf("advisory locking not enforced here (err=%v)", err)
	}
	db.Close()
	db2 := openT(t, dir, nil)
	db2.Close()
}

func TestClosed(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	db.Close()
	if err := db.Put([]byte("k"), nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Put after Close: %v", err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close: %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("double Close: %v", err)
	}
}

func TestKeysAndFold(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	for _, k := range []string{"c", "a", "b"} {
		db.Put([]byte(k), []byte(k+k))
	}
	if got := fmt.Sprint(db.Keys()); got != "[a b c]" {
		t.Fatalf("Keys = %s", got)
	}
	var out []string
	db.Fold(func(k, v []byte) error {
		out = append(out, string(k)+"="+string(v))
		return nil
	})
	if got := fmt.Sprint(out); got != "[a=aa b=bb c=cc]" {
		t.Fatalf("Fold = %s", got)
	}
	stop := errors.New("stop")
	if err := db.Fold(func(k, v []byte) error { return stop }); err != stop {
		t.Fatalf("Fold did not propagate error: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	db := openT(t, t.TempDir(), &Options{MaxFileSize: 4096})
	defer db.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i))
				if err := db.Put(k, k); err != nil {
					t.Error(err)
					return
				}
				if v, err := db.Get(k); err != nil || !bytes.Equal(v, k) {
					t.Errorf("Get(%s) = %q, %v", k, v, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if db.Len() != 1600 {
		t.Fatalf("Len = %d, want 1600", db.Len())
	}
}

func TestStats(t *testing.T) {
	db := openT(t, t.TempDir(), nil)
	defer db.Close()
	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))
	st, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Keys != 1 || st.DiskBytes != 2*st.LiveBytes {
		t.Fatalf("unexpected stats %+v", st)
	}

}
