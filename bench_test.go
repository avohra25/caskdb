package caskdb

import (
	"fmt"
	"math/rand"
	"testing"
)

func benchDB(b *testing.B, opts *Options) *DB {
	db, err := Open(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func BenchmarkPut(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(fmt.Sprintf("value=%dB", size), func(b *testing.B) {
			db := benchDB(b, nil)
			val := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Put([]byte(fmt.Sprintf("key-%d", i)), val)
			}
		})
	}
}

func BenchmarkPutSync(b *testing.B) {
	db := benchDB(b, &Options{SyncOnPut: true})
	val := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("key-%d", i)), val)
	}
}

func BenchmarkGet(b *testing.B) {
	const n = 100_000
	db := benchDB(b, nil)
	val := make([]byte, 128)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key-%d", i)), val)
	}
	rng := rand.New(rand.NewSource(1))
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key-%d", rng.Intn(n)))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	const n = 100_000
	db := benchDB(b, nil)
	val := make([]byte, 128)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key-%d", i)), val)
	}
	b.SetBytes(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			db.Get([]byte(fmt.Sprintf("key-%d", rng.Intn(n))))
		}
	})
}

func BenchmarkOpen(b *testing.B) {
	for _, merged := range []bool{false, true} {
		b.Run(fmt.Sprintf("merged=%v", merged), func(b *testing.B) {
			dir := b.TempDir()
			db, _ := Open(dir, nil)
			val := make([]byte, 1024)
			for i := 0; i < 50_000; i++ {
				db.Put([]byte(fmt.Sprintf("key-%d", i)), val)
			}
			if merged {
				db.Merge()
			}
			db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db, err := Open(dir, nil)
				if err != nil {
					b.Fatal(err)
				}
				db.Close()
			}
		})
	}
}
