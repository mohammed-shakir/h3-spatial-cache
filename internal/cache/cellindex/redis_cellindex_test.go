package cellindex

import (
	"context"
	"reflect"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/redisstore"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
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

func TestRedisCellIndex_RoundTrip_AndDedup(t *testing.T) {
	cli, mr := newMini(t)
	idx := NewRedisIndex(cli)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	layer := "demo:NR_polygon"
	res := 8
	cell := "892a100d2b3ffff"
	filters := model.Filters("status = 'active'")

	ids := []string{"A", "B", "A", "C", "B"}
	ttl := 2 * time.Minute

	if err := idx.SetIDs(ctx, layer, res, cell, filters, ids, ttl); err != nil {
		t.Fatalf("SetIDs: %v", err)
	}

	got, err := idx.GetIDs(ctx, layer, res, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs: %v", err)
	}

	want := []string{"A", "B", "C"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetIDs got=%v want=%v", got, want)
	}

	k := keys.CellIndexKey(layer, res, cell, filters)
	tt := mr.TTL(k)
	if tt <= 0 || tt > ttl {
		t.Fatalf("unexpected TTL for key %q: %v", k, tt)
	}
}

func TestRedisCellIndex_GetIDs_MissingKeyReturnsNil(t *testing.T) {
	cli, _ := newMini(t)
	idx := NewRedisIndex(cli)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	ids, err := idx.GetIDs(ctx, "demo:layer", 7, "892a100d2b3ffff", model.Filters(""))
	if err != nil {
		t.Fatalf("GetIDs: %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil ids for missing key, got=%v", ids)
	}
}

func TestRedisCellIndex_EmptyIDs_DeletesKeyAndReturnsNil(t *testing.T) {
	cli, mr := newMini(t)
	idx := NewRedisIndex(cli)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	layer := "demo:layer"
	res := 7
	cell := "892a100d2b3ffff"
	filters := model.Filters("a=1")

	if err := idx.SetIDs(ctx, layer, res, cell, filters, []string{"X"}, time.Minute); err != nil {
		t.Fatalf("SetIDs initial: %v", err)
	}
	k := keys.CellIndexKey(layer, res, cell, filters)
	if !mr.Exists(k) {
		t.Fatalf("expected key %q to exist after initial SetIDs", k)
	}

	if err := idx.SetIDs(ctx, layer, res, cell, filters, nil, time.Minute); err != nil {
		t.Fatalf("SetIDs empty: %v", err)
	}
	if mr.Exists(k) {
		t.Fatalf("expected key %q to be deleted after empty SetIDs", k)
	}

	ids, err := idx.GetIDs(ctx, layer, res, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs after empty SetIDs: %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil ids after empty SetIDs, got=%v", ids)
	}
}
