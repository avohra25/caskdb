package caskdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestMergeReclaimsSpace(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, &Options{MaxFileSize: 1024})
	for round := 0; round < 10; round++ {
		for i := 0; i < 20; i++ {
			db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("round-%d", round)))
		}
	}
	for i := 10; i < 20; i++ {
		db.Delete([]byte(fmt.Sprintf("k%02d", i)))
	}
	before, _ := db.Stats()
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	after, _ := db.Stats()
	if after.DiskBytes >= before.DiskBytes/4 {
		t.Fatalf("merge did not reclaim space: %d -> %d bytes", before.DiskBytes, after.DiskBytes)
	}
	if after.DiskBytes != after.LiveBytes {
		t.Fatalf("after merge disk=%d live=%d, want equal", after.DiskBytes, after.LiveBytes)
	}
	check := func(db *DB) {
		t.Helper()
		for i := 0; i < 10; i++ {
			mustGet(t, db, fmt.Sprintf("k%02d", i), "round-9")
		}
		for i := 10; i < 20; i++ {
			mustMissing(t, db, fmt.Sprintf("k%02d", i))
		}
	}
	check(db)
	db.Close()

	db = openT(t, dir, &Options{MaxFileSize: 1024})
	defer db.Close()
	check(db)
	if hints, _ := listIDs(dir, hintExt); len(hints) == 0 {
		t.Fatal("merge wrote no hint files")
	}
}

func TestWritesAfterMergeWinOnReopen(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	db.Put([]byte("a"), []byte("old"))
	db.Put([]byte("b"), []byte("keep"))
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	// The active file now has a lower id than the merge output, so only the
	// logical timestamps keep these newer writes on top.
	db.Put([]byte("a"), []byte("new"))
	db.Delete([]byte("b"))
	db.Close()

	db = openT(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "a", "new")
	mustMissing(t, db, "b")
}

func TestMergeEverythingDeleted(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	db.Put([]byte("a"), []byte("1"))
	db.Delete([]byte("a"))
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); err != nil { // nothing to do the second time
		t.Fatal(err)
	}
	db.Close()
	db = openT(t, dir, nil)
	defer db.Close()
	if db.Len() != 0 {
		t.Fatalf("Len = %d, want 0", db.Len())
	}
}

func TestMergeConcurrentWithWrites(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, &Options{MaxFileSize: 2048})
	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v0"))
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			k := []byte(fmt.Sprintf("k%03d", i))
			if i%3 == 0 {
				db.Delete(k)
			} else {
				db.Put(k, []byte("v1"))
			}
		}
	}()
	for i := 0; i < 3; i++ {
		if err := db.Merge(); err != nil {
			t.Error(err)
		}
	}
	wg.Wait()
	db.Merge()

	verify := func(db *DB) {
		t.Helper()
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("k%03d", i)
			if i%3 == 0 {
				mustMissing(t, db, k)
			} else {
				mustGet(t, db, k, "v1")
			}
		}
	}
	verify(db)
	db.Close()
	db = openT(t, dir, &Options{MaxFileSize: 2048})
	defer db.Close()
	verify(db)
}

func TestInterruptedMergeIsCompletedOnOpen(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("a"), []byte("2"))
	db.Close()

	// Simulate a crash after the manifest was committed but before the old
	// file was removed: the merged copy exists alongside the original.
	db = openT(t, dir, nil)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := writeManifest(dir, []uint32{999}); err != nil {
		t.Fatal(err)
	}
	stale := fileName(dir, 999, dataExt)
	os.WriteFile(stale, encodeRecord(&record{ts: 1, key: []byte("ghost"), value: []byte("x")}), 0o644)
	os.WriteFile(filepath.Join(dir, "000000777.data.tmp"), []byte("partial"), 0o644)

	db = openT(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "a", "2")
	mustMissing(t, db, "ghost")
	for _, p := range []string{stale, filepath.Join(dir, manifestName), filepath.Join(dir, "000000777.data.tmp")} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should have been removed", filepath.Base(p))
		}
	}
}

func TestCorruptHintFallsBackToData(t *testing.T) {
	dir := t.TempDir()
	db := openT(t, dir, nil)
	db.Put([]byte("x"), []byte("from data file"))
	db.Merge()
	db.Close()

	hints, _ := listIDs(dir, hintExt)
	if len(hints) != 1 {
		t.Fatalf("want 1 hint file, got %v", hints)
	}
	os.WriteFile(fileName(dir, hints[0], hintExt), []byte("garbage"), 0o644)

	db = openT(t, dir, nil)
	defer db.Close()
	mustGet(t, db, "x", "from data file")
}

func TestHintRoundTrip(t *testing.T) {
	in := []hint{{"alpha", 1, 0, 30, 0}, {"beta", 7, 30, 99, 1793300000000}, {"gamma", 9, 129, 40, 0}}
	var buf []byte
	for _, h := range in {
		buf = appendHint(buf, h)
	}
	out, err := decodeHints(buf)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(out) != fmt.Sprint(in) {
		t.Fatalf("got %v, want %v", out, in)
	}
	buf[len(buf)-1] ^= 1
	if _, err := decodeHints(buf); err == nil {
		t.Fatal("corrupt hint accepted")
	}
}
