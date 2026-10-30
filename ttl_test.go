package caskdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"testing"
	"time"
)

// fakeClock is a controllable wall clock for expiry tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func mustTTL(t *testing.T, db *DB, key string, want time.Duration) {
	t.Helper()
	got, err := db.TTL([]byte(key))
	if err != nil {
		t.Fatalf("TTL(%q): %v", key, err)
	}
	if got != want {
		t.Fatalf("TTL(%q) = %v, want %v", key, got, want)
	}
}

func TestExpiryRecordRoundTrip(t *testing.T) {
	r := &record{ts: 3, expires: 1793300000123, key: []byte("k"), value: []byte("v")}
	buf := encodeRecord(r)
	if len(buf) != headerSize+1+8+1 {
		t.Fatalf("encoded size %d", len(buf))
	}
	got, err := decodeRecord(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.expires != r.expires || string(got.value) != "v" || string(got.key) != "k" || got.tombstone() {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	// A record from before expiry support (bit 1 clear) still decodes.
	plain, err := decodeRecord(encodeRecord(&record{ts: 1, key: []byte("k"), value: []byte("old")}))
	if err != nil || plain.expires != 0 || string(plain.value) != "old" {
		t.Fatalf("plain record: %+v, %v", plain, err)
	}

	// Expiry flag on a tombstone, or a value region too short to hold the
	// expiry, is rejected as corruption.
	bad := encodeRecord(&record{ts: 1, flags: flagTombstone, key: []byte("k")})
	bad[12] |= flagExpires
	binary.LittleEndian.PutUint32(bad[0:], crc32.ChecksumIEEE(bad[4:]))
	if _, err := decodeRecord(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("tombstone with expiry: err = %v", err)
	}
	bad = encodeRecord(&record{ts: 1, key: []byte("k"), value: []byte("abc")})
	bad[12] |= flagExpires
	binary.LittleEndian.PutUint32(bad[0:], crc32.ChecksumIEEE(bad[4:]))
	if _, err := decodeRecord(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short expiry: err = %v", err)
	}
}

func TestPutWithTTLExpires(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now})
	defer db.Close()

	if err := db.PutWithTTL([]byte("session"), []byte("abc"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("forever"), []byte("x"))
	mustGet(t, db, "session", "abc")
	mustTTL(t, db, "session", 10*time.Second)
	mustTTL(t, db, "forever", NoTTL)

	clk.Advance(9*time.Second + 999*time.Millisecond)
	mustGet(t, db, "session", "abc")
	mustTTL(t, db, "session", time.Millisecond)
	if st, _ := db.Stats(); st.Keys != 2 || st.Expiring != 1 {
		t.Fatalf("stats before expiry: %+v", st)
	}

	clk.Advance(time.Millisecond)
	mustMissing(t, db, "session")
	if db.Has([]byte("session")) {
		t.Fatal("Has reports expired key")
	}
	if _, err := db.TTL([]byte("session")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TTL of expired key: %v", err)
	}
	if n := db.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1", n)
	}
	if keys := db.Keys(); len(keys) != 1 || keys[0] != "forever" {
		t.Fatalf("Keys = %v", keys)
	}
	var folded []string
	db.Fold(func(k, v []byte) error { folded = append(folded, string(k)); return nil })
	if len(folded) != 1 {
		t.Fatalf("Fold visited %v", folded)
	}
	if st, _ := db.Stats(); st.Keys != 1 || st.Expiring != 0 {
		t.Fatalf("stats after expiry: %+v", st)
	}
}

func TestInvalidTTL(t *testing.T) {
	db := openT(t, t.TempDir(), &Options{Now: newClock().Now})
	defer db.Close()
	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := db.PutWithTTL([]byte("k"), []byte("v"), ttl); !errors.Is(err, ErrInvalidTTL) {
			t.Fatalf("ttl %v: err = %v", ttl, err)
		}
	}
	if err := db.PutWithExpiry([]byte("k"), []byte("v"), time.Time{}); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("zero time: err = %v", err)
	}
	if err := db.PutWithTTL(nil, []byte("v"), time.Second); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: err = %v", err)
	}
}

func TestPutClearsTTLAndPastExpiryDeletes(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now})
	defer db.Close()

	db.PutWithTTL([]byte("k"), []byte("1"), time.Second)
	db.Put([]byte("k"), []byte("2"))
	mustTTL(t, db, "k", NoTTL)
	clk.Advance(time.Hour)
	mustGet(t, db, "k", "2")

	if err := db.PutWithExpiry([]byte("k"), []byte("3"), clk.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	mustMissing(t, db, "k")
	if err := db.PutWithExpiry([]byte("old"), []byte("v"), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	mustMissing(t, db, "old")
}

func TestExpireAndPersist(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now})
	defer db.Close()

	if ok, err := db.Expire([]byte("missing"), time.Second); ok || err != nil {
		t.Fatalf("Expire missing = %v, %v", ok, err)
	}
	db.Put([]byte("k"), []byte("v"))
	if ok, err := db.Persist([]byte("k")); ok || err != nil {
		t.Fatalf("Persist permanent key = %v, %v", ok, err)
	}
	if ok, err := db.Expire([]byte("k"), 5*time.Second); !ok || err != nil {
		t.Fatalf("Expire = %v, %v", ok, err)
	}
	mustTTL(t, db, "k", 5*time.Second)
	mustGet(t, db, "k", "v")

	if ok, _ := db.ExpireAt([]byte("k"), clk.Now().Add(time.Minute)); !ok {
		t.Fatal("ExpireAt failed")
	}
	mustTTL(t, db, "k", time.Minute)

	if ok, err := db.Persist([]byte("k")); !ok || err != nil {
		t.Fatalf("Persist = %v, %v", ok, err)
	}
	mustTTL(t, db, "k", NoTTL)
	clk.Advance(24 * time.Hour)
	mustGet(t, db, "k", "v")

	// A non-positive TTL deletes the key, and a deletion is durable.
	if ok, _ := db.Expire([]byte("k"), 0); !ok {
		t.Fatal("Expire(0) reported missing key")
	}
	mustMissing(t, db, "k")
	if ok, _ := db.Expire([]byte("k"), time.Second); ok {
		t.Fatal("Expire on deleted key reported success")
	}
}

func TestDeleteExpiredKeyNeedsNoTombstone(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	db := openT(t, dir, &Options{Now: clk.Now})
	db.Put([]byte("k"), []byte("old"))
	db.PutWithTTL([]byte("k"), []byte("new"), time.Second)
	clk.Advance(2 * time.Second)
	before, _ := db.Stats()
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	after, _ := db.Stats()
	if after.DiskBytes != before.DiskBytes {
		t.Fatalf("delete of expired key wrote %d bytes", after.DiskBytes-before.DiskBytes)
	}
	db.Close()

	// The expired record still shadows the older permanent value.
	db = openT(t, dir, &Options{Now: clk.Now})
	defer db.Close()
	mustMissing(t, db, "k")
}

func TestExpiryPersistsAcrossReopen(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	db := openT(t, dir, &Options{Now: clk.Now})
	db.Put([]byte("a"), []byte("permanent"))
	db.PutWithTTL([]byte("a"), []byte("temporary"), time.Minute)
	db.PutWithTTL([]byte("b"), []byte("short"), time.Second)
	db.PutWithTTL([]byte("c"), []byte("long"), time.Hour)
	db.Close()

	clk.Advance(30 * time.Second)
	db = openT(t, dir, &Options{Now: clk.Now})
	mustGet(t, db, "a", "temporary")
	mustTTL(t, db, "a", 30*time.Second)
	mustMissing(t, db, "b")
	mustTTL(t, db, "c", time.Hour-30*time.Second)
	if n := len(db.keydir); n != 2 {
		t.Fatalf("expired key kept in index on open: %d entries", n)
	}
	db.Close()

	clk.Advance(time.Minute)
	db = openT(t, dir, &Options{Now: clk.Now})
	defer db.Close()
	mustMissing(t, db, "a") // must not fall back to the older permanent value
	mustGet(t, db, "c", "long")
}

func TestExpirySurvivesMergeAndHints(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	opts := &Options{Now: clk.Now, MaxFileSize: 256}
	db := openT(t, dir, opts)
	for i := 0; i < 40; i++ {
		k := []byte{'k', byte('0' + i%10)}
		if i%2 == 0 {
			db.PutWithTTL(k, []byte("ttl-value"), time.Duration(i+1)*time.Minute)
		} else {
			db.Put(k, []byte("plain-value"))
		}
	}
	want := map[string]time.Duration{}
	for _, k := range db.Keys() {
		ttl, _ := db.TTL([]byte(k))
		want[k] = ttl
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	for k, ttl := range want {
		mustTTL(t, db, k, ttl)
	}
	db.Close()

	hints, _ := listIDs(dir, hintExt)
	if len(hints) == 0 {
		t.Fatal("merge wrote no hint files")
	}
	db = openT(t, dir, opts)
	defer db.Close()
	for k, ttl := range want {
		mustTTL(t, db, k, ttl)
	}
	if st, _ := db.Stats(); st.Expiring == 0 || st.Expiring == st.Keys {
		t.Fatalf("expected a mix of expiring and permanent keys: %+v", st)
	}
}

func TestOldFormatHintStillLoads(t *testing.T) {
	// A hint written by an older version (no expiry bit) must still be used.
	buf := appendHint(nil, hint{key: "k", ts: 5, offset: 0, size: 26})
	if binary.LittleEndian.Uint32(buf[24:])&hintHasExpiry != 0 {
		t.Fatal("expiry bit set for permanent key")
	}
	hs, err := decodeHints(buf)
	if err != nil || len(hs) != 1 || hs[0].expires != 0 {
		t.Fatalf("decode: %v, %v", hs, err)
	}
}

func TestMergeDropsExpiredKeys(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	opts := &Options{Now: clk.Now}
	db := openT(t, dir, opts)
	db.Put([]byte("keep"), []byte("forever"))
	db.Put([]byte("gone"), []byte("old permanent value"))
	db.PutWithTTL([]byte("gone"), []byte("temporary"), time.Second)
	db.PutWithTTL([]byte("later"), []byte("still here"), time.Hour)
	clk.Advance(2 * time.Second)

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	st, _ := db.Stats()
	if st.Keys != 2 || st.Expired != 0 || st.Expiring != 1 {
		t.Fatalf("after merge: %+v", st)
	}
	if _, ok := db.keydir["gone"]; ok {
		t.Fatal("expired key still indexed after merge")
	}
	if st.DiskBytes != st.LiveBytes {
		t.Fatalf("merge kept dead bytes: disk %d, live %d", st.DiskBytes, st.LiveBytes)
	}
	db.Close()

	// Neither the expired value nor the permanent value it replaced may
	// come back after a restart.
	clk.Advance(-time.Hour) // even with the clock moved back
	db = openT(t, dir, opts)
	defer db.Close()
	mustMissing(t, db, "gone")
	mustGet(t, db, "keep", "forever")
	mustGet(t, db, "later", "still here")
}

func TestMergeKeepsKeyRewrittenDuringMerge(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now})
	defer db.Close()
	db.PutWithTTL([]byte("k"), []byte("v1"), time.Second)
	clk.Advance(2 * time.Second)
	db.Merge()
	db.Put([]byte("k"), []byte("v2"))
	db.Merge()
	mustGet(t, db, "k", "v2")
}

func TestSweepExpired(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now})
	defer db.Close()
	for i := 0; i < 100; i++ {
		k := []byte{byte('a' + i%26), byte('0' + i/26)}
		if i%4 == 0 {
			db.Put(k, []byte("p"))
		} else {
			db.PutWithTTL(k, []byte("t"), time.Duration(i)*time.Second)
		}
	}
	clk.Advance(50 * time.Second)
	st, _ := db.Stats()
	n, err := db.SweepExpired()
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || n != st.Expired {
		t.Fatalf("swept %d, stats reported %d expired", n, st.Expired)
	}
	if len(db.keydir) != st.Keys {
		t.Fatalf("index has %d entries, want %d", len(db.keydir), st.Keys)
	}
	if n, _ := db.SweepExpired(); n != 0 {
		t.Fatalf("second sweep removed %d", n)
	}
}

func TestBackgroundSweeper(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now, SweepInterval: time.Millisecond})
	db.PutWithTTL([]byte("k"), []byte("v"), time.Second)
	clk.Advance(time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for {
		db.mu.RLock()
		n := len(db.keydir)
		db.mu.RUnlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background sweeper never removed the expired key")
		}
		time.Sleep(time.Millisecond)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close: %v", err)
	}
}

func TestExpiryUnderConcurrency(t *testing.T) {
	clk := newClock()
	db := openT(t, t.TempDir(), &Options{Now: clk.Now, SweepInterval: time.Millisecond, MaxFileSize: 4 << 10})
	defer db.Close()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				k := []byte{byte('a' + g), byte(i % 50)}
				switch i % 3 {
				case 0:
					db.PutWithTTL(k, []byte("ttl"), time.Duration(i%7+1)*time.Millisecond)
				case 1:
					db.Put(k, []byte("plain"))
				default:
					db.Get(k)
					db.TTL(k)
				}
				if i%50 == 0 {
					clk.Advance(3 * time.Millisecond)
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err := db.Merge(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	clk.Advance(time.Second)
	for _, k := range db.Keys() {
		v, err := db.Get([]byte(k))
		if err != nil || string(v) != "plain" {
			t.Fatalf("key %q after all TTLs passed: %q, %v", k, v, err)
		}
	}
}
