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

func TestRedisCellIndex_DistinctKeysPerResolution(t *testing.T) {
	cli, mr := newMini(t)
	idx := NewRedisIndex(cli)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	layer := "demo:NR_polygon"
	cell := "892a100d2b3ffff"
	filters := model.Filters("status='active'")

	ids6 := []string{"A"}
	ids8 := []string{"B"}

	if err := idx.SetIDs(ctx, layer, 6, cell, filters, ids6, time.Minute); err != nil {
		t.Fatalf("SetIDs res=6: %v", err)
	}
	if err := idx.SetIDs(ctx, layer, 8, cell, filters, ids8, time.Minute); err != nil {
		t.Fatalf("SetIDs res=8: %v", err)
	}

	got6, err := idx.GetIDs(ctx, layer, 6, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs res=6: %v", err)
	}
	got8, err := idx.GetIDs(ctx, layer, 8, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs res=8: %v", err)
	}

	if !reflect.DeepEqual(got6, ids6) {
		t.Fatalf("res=6 ids=%v want=%v", got6, ids6)
	}
	if !reflect.DeepEqual(got8, ids8) {
		t.Fatalf("res=8 ids=%v want=%v", got8, ids8)
	}

	k6 := keys.CellIndexKey(layer, 6, cell, filters)
	k8 := keys.CellIndexKey(layer, 8, cell, filters)
	if k6 == k8 {
		t.Fatalf("cell index keys must differ across resolutions; got %q", k6)
	}
	if !mr.Exists(k6) || !mr.Exists(k8) {
		t.Fatalf("expected both keys to exist; k6Exists=%v k8Exists=%v", mr.Exists(k6), mr.Exists(k8))
	}
}

func TestRedisCellIndex_ClearOneResolutionKeepsOther(t *testing.T) {
	cli, mr := newMini(t)
	idx := NewRedisIndex(cli)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	layer := "demo:layer"
	cell := "892a100d2b3ffff"
	filters := model.Filters("a=1")

	if err := idx.SetIDs(ctx, layer, 6, cell, filters, []string{"A"}, time.Minute); err != nil {
		t.Fatalf("SetIDs res=6: %v", err)
	}
	if err := idx.SetIDs(ctx, layer, 8, cell, filters, []string{"B"}, time.Minute); err != nil {
		t.Fatalf("SetIDs res=8: %v", err)
	}

	k6 := keys.CellIndexKey(layer, 6, cell, filters)
	k8 := keys.CellIndexKey(layer, 8, cell, filters)
	if !mr.Exists(k6) || !mr.Exists(k8) {
		t.Fatalf("expected both res=6 and res=8 keys to exist")
	}

	// simulate invalidation only for res=6
	if err := cli.Del(ctx, k6); err != nil {
		t.Fatalf("Del res=6: %v", err)
	}

	ids6, err := idx.GetIDs(ctx, layer, 6, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs res=6: %v", err)
	}
	if ids6 != nil {
		t.Fatalf("expected nil ids for res=6 after deletion, got %v", ids6)
	}

	ids8, err := idx.GetIDs(ctx, layer, 8, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs res=8: %v", err)
	}
	if !reflect.DeepEqual(ids8, []string{"B"}) {
		t.Fatalf("expected res=8 ids unaffected, got %v", ids8)
	}
}

func TestRedisCellIndex_DelCells_RemovesEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	rc, err := redisstore.New(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("redisstore.New: %v", err)
	}

	idx := NewRedisIndex(rc)

	layer := "demo:layer"
	res := 8
	cell := "892a100d2b3ffff"
	filters := model.Filters("status='active'")

	seedIDs := []string{"s:foo", "n:42"}
	if err := idx.SetIDs(ctx, layer, res, cell, filters, seedIDs, time.Minute); err != nil {
		t.Fatalf("SetIDs: %v", err)
	}

	got, err := idx.GetIDs(ctx, layer, res, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs before DelCells: %v", err)
	}
	if len(got) != len(seedIDs) {
		t.Fatalf("GetIDs len before DelCells=%d, want=%d", len(got), len(seedIDs))
	}

	if err := idx.DelCells(ctx, layer, res, []string{cell}, filters); err != nil {
		t.Fatalf("DelCells: %v", err)
	}

	got2, err := idx.GetIDs(ctx, layer, res, cell, filters)
	if err != nil {
		t.Fatalf("GetIDs after DelCells: %v", err)
	}
	if len(got2) != 0 {
		t.Fatalf("GetIDs after DelCells = %v (len=%d), want nil/empty", got2, len(got2))
	}
}
