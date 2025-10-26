package redisstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func prep(b *testing.B, n int) (*Client, []string, func()) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("miniredis: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	rc, err := New(ctx, mr.Addr())
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("k:%06d", i)
		if err := rc.Set(ctx, keys[i], []byte("value"), time.Hour); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}

	cleanup := func() {
		cancel()
		_ = rc.Close()
		mr.Close()
	}
	return rc, keys, cleanup
}

func benchMGet(b *testing.B, n int) {
	rc, keys, cleanup := prep(b, n)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		if _, err := rc.MGet(ctx, keys); err != nil {
			b.Fatal(err)
		}
	}
}

func benchGetLoop(b *testing.B, n int) {
	rc, keys, cleanup := prep(b, n)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		for _, k := range keys {
			if _, err := rc.rdb.Get(ctx, k).Bytes(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkReads_64(b *testing.B) {
	b.Run("MGET", func(b *testing.B) { benchMGet(b, 64) })
	b.Run("GETx64", func(b *testing.B) { benchGetLoop(b, 64) })
}

func BenchmarkReads_256(b *testing.B) {
	b.Run("MGET", func(b *testing.B) { benchMGet(b, 256) })
	b.Run("GETx256", func(b *testing.B) { benchGetLoop(b, 256) })
}

func BenchmarkReads_1024(b *testing.B) {
	b.Run("MGET", func(b *testing.B) { benchMGet(b, 1024) })
	b.Run("GETx1024", func(b *testing.B) { benchGetLoop(b, 1024) })
}
