package redisstore

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func TestTTLExpiry_MGetFiltersExpired(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	rc, err := New(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = rc.Close() })

	if err := rc.Set(ctx, "ttl-key", []byte("v"), 2*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := rc.MGet(ctx, []string{"ttl-key"})
	if err != nil || string(got["ttl-key"]) != "v" {
		t.Fatalf("pre expiry got=%v err=%v", got, err)
	}

	mr.FastForward(3 * time.Second)

	got2, err := rc.MGet(ctx, []string{"ttl-key"})
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if _, ok := got2["ttl-key"]; ok {
		t.Fatalf("expected ttl-key to be absent after expiry; got=%v", got2)
	}
}
