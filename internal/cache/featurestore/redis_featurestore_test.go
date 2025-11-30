package featurestore

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
)

func newMini(t *testing.T) (*redisstore.Client, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	cli, err := redisstore.New(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("redisstore.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	return cli, mr
}

func TestRedisFeatureStore_RoundTrip_HitsAndMisses(t *testing.T) {
	cli, mr := newMini(t)
	fs := NewRedisStore(cli, 10*time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	layer := "demo:NR_polygon"

	feats := map[string][]byte{
		"A":       []byte(`{"id":"A"}`),
		"foo-123": []byte(`{"id":"foo-123"}`),
		"42":      []byte(`{"id":"42"}`),
	}
	ttl := 2 * time.Minute

	if err := fs.PutFeatures(ctx, layer, feats, ttl); err != nil {
		t.Fatalf("PutFeatures: %v", err)
	}

	got, err := fs.MGetFeatures(ctx, layer, []string{"A", "foo-123", "42"})
	if err != nil {
		t.Fatalf("MGetFeatures: %v", err)
	}
	if len(got) != len(feats) {
		t.Fatalf("MGetFeatures size=%d want %d", len(got), len(feats))
	}
	for id, want := range feats {
		gotBody, ok := got[id]
		if !ok {
			t.Fatalf("missing id %q in result", id)
		}
		if string(gotBody) != string(want) {
			t.Fatalf("body mismatch for id=%q got=%q want=%q", id, gotBody, want)
		}
	}

	got2, err := fs.MGetFeatures(ctx, layer, []string{"A", "missing"})
	if err != nil {
		t.Fatalf("MGetFeatures (with miss): %v", err)
	}
	if len(got2) != 1 {
		t.Fatalf("MGetFeatures size=%d want 1 (only hit)", len(got2))
	}
	if _, ok := got2["A"]; !ok {
		t.Fatalf("expected hit for id A")
	}
	if _, ok := got2["missing"]; ok {
		t.Fatalf("unexpected entry for missing id")
	}

	for id := range feats {
		k := featureKey(layer, id)
		tt := mr.TTL(k)
		if tt <= 0 || tt > ttl {
			t.Fatalf("unexpected TTL for key %q: %v", k, tt)
		}
	}
}

func TestRedisFeatureStore_EmptyIDs_ReturnsEmptyMap(t *testing.T) {
	cli, _ := newMini(t)
	fs := NewRedisStore(cli, 5*time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	got, err := fs.MGetFeatures(ctx, "demo:layer", nil)
	if err != nil {
		t.Fatalf("MGetFeatures(nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result map, got len=%d", len(got))
	}
}

func TestRedisFeatureStore_DefaultTTLUsedWhenZeroTTL(t *testing.T) {
	cli, mr := newMini(t)
	defaultTTL := 3 * time.Minute
	fs := NewRedisStore(cli, defaultTTL)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	layer := "demo:NR_polygon"
	id := "B"

	if err := fs.PutFeatures(ctx, layer, map[string][]byte{
		id: []byte(`{"id":"B"}`),
	}, 0); err != nil {
		t.Fatalf("PutFeatures(defaultTTL): %v", err)
	}

	k := featureKey(layer, id)
	tt := mr.TTL(k)
	if tt <= 0 || tt > defaultTTL {
		t.Fatalf("unexpected TTL for defaultTTL key %q: %v", k, tt)
	}
}
