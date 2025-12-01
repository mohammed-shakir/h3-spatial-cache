package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/composer"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
)

type fakeStore struct{ m map[string][]byte }

func (f fakeStore) MGet(keyList []string) (map[string][]byte, error) {
	out := map[string][]byte{}
	for _, k := range keyList {
		if v, ok := f.m[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}
func (f fakeStore) Set(string, []byte, time.Duration) error { return nil }
func (f fakeStore) Del(...string) error                     { return nil }

type fakeFeatureStore struct {
	mu sync.Mutex
	m  map[string]map[string][]byte
}

func (f *fakeFeatureStore) MGetFeatures(ctx context.Context, layer string, ids []string) (map[string][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[string][]byte, len(ids))
	lm := f.m[layer]
	for _, id := range ids {
		if b, ok := lm[id]; ok {
			out[id] = append([]byte(nil), b...)
		}
	}
	return out, nil
}

func (f *fakeFeatureStore) PutFeatures(
	ctx context.Context,
	layer string,
	feats map[string][]byte,
	ttl time.Duration, // ttl ignored in fake
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.m == nil {
		f.m = make(map[string]map[string][]byte)
	}
	lm := f.m[layer]
	if lm == nil {
		lm = make(map[string][]byte)
		f.m[layer] = lm
	}
	for id, b := range feats {
		lm[id] = append([]byte(nil), b...)
	}
	return nil
}

type cellKey struct {
	layer string
	res   int
	cell  string
	filt  model.Filters
}

type fakeCellIndex struct {
	mu sync.Mutex
	m  map[cellKey][]string
}

func (f *fakeCellIndex) GetIDs(
	ctx context.Context,
	layer string,
	res int,
	cell string,
	filters model.Filters,
) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ids := f.m[cellKey{layer: layer, res: res, cell: cell, filt: filters}]
	return append([]string(nil), ids...), nil
}

func (f *fakeCellIndex) SetIDs(
	ctx context.Context,
	layer string,
	res int,
	cell string,
	filters model.Filters,
	ids []string,
	ttl time.Duration, // ttl ignored in fake
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.m == nil {
		f.m = make(map[cellKey][]string)
	}
	k := cellKey{layer: layer, res: res, cell: cell, filt: filters}
	f.m[k] = append([]string(nil), ids...)
	return nil
}

func newEngineForTest() *Engine {
	disc := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{})
	return &Engine{
		logger: slog.New(disc),
		res:    8,
		minRes: 8,
		maxRes: 8,
		mapr:   h3mapper.New(),
		eng:    composer.Engine{V2: composer.NewGeoJSONV2Adapter(geojsonagg.NewAdvanced())},

		store: fakeStore{m: map[string][]byte{}},

		fs:  &fakeFeatureStore{},
		idx: &fakeCellIndex{},

		owsURL:     &url.URL{Scheme: "http", Host: "example.invalid"},
		ttlDefault: time.Second,
		ttlMap:     map[string]time.Duration{},
		maxWorkers: 1,
		queueSize:  1,
		opTimeout:  150 * time.Millisecond,

		adaptiveEnabled: false,
		adaptiveDryRun:  false,
		serveFreshOnly:  true,
		runID:           "test",
	}
}

func gatherCounter(reg *prometheus.Registry, reason string) float64 {
	const name = "spatial_fresh_rejects_total"
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["reason"] == reason {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestServeOnlyIfFresh_GatingAndMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)
	observability.SetScenario("cache")

	makeReq := func() model.QueryRequest {
		return model.QueryRequest{
			Layer: "ns:roads",
			BBox:  &model.BBox{X1: 0, Y1: 0, X2: 0.01, Y2: 0.01, SRID: "EPSG:4326"},
		}
	}
	const featureTemplate = `{"type":"Feature","id":"%s","geometry":null,"properties":{"name":"%s"}}`

	t.Run("a) full hit fresh → 200", func(t *testing.T) {
		e := newEngineForTest()
		q := makeReq()

		observability.SetLayerInvalidatedAt(q.Layer, time.Time{})

		cells, err := e.cellsForRes(q, e.res)
		if err != nil || len(cells) == 0 {
			t.Fatalf("cells: %v len=%d", err, len(cells))
		}

		fs := e.fs.(*fakeFeatureStore)
		idx := e.idx.(*fakeCellIndex)

		ctx := context.Background()
		for i, c := range cells {
			id := "id-" + fmt.Sprint(i)
			feat := fmt.Appendf(nil, featureTemplate, id, id)

			if err := fs.PutFeatures(ctx, q.Layer, map[string][]byte{id: feat}, time.Minute); err != nil {
				t.Fatalf("seed feature store: %v", err)
			}
			if err := idx.SetIDs(ctx, q.Layer, e.res, c, model.Filters(q.Filters), []string{id}, time.Minute); err != nil {
				t.Fatalf("seed cell index: %v", err)
			}
		}

		rr := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/query", nil)
		e.HandleQuery(context.Background(), rr, r, q)

		if rr.Code != 200 {
			t.Fatalf("want 200, got %d body=%q", rr.Code, rr.Body.String())
		}
		before := gatherCounter(reg, "stale")
		after := gatherCounter(reg, "stale")
		if after-before != 0 {
			t.Fatalf("unexpected stale rejects delta: %v (before=%v after=%v)", after-before, before, after)
		}
	})

	t.Run("b) full hit stale + flag → 412 (reason=stale)", func(t *testing.T) {
		e := newEngineForTest()
		q := makeReq()

		cells, err := e.cellsForRes(q, e.res)
		if err != nil || len(cells) == 0 {
			t.Fatalf("cells: %v len=%d", err, len(cells))
		}

		fs := e.fs.(*fakeFeatureStore)
		idx := e.idx.(*fakeCellIndex)
		ctx := context.Background()

		for i, c := range cells {
			id := "id-" + fmt.Sprint(i)
			feat := fmt.Appendf(nil, featureTemplate, id, id)

			if err := fs.PutFeatures(ctx, q.Layer, map[string][]byte{id: feat}, time.Minute); err != nil {
				t.Fatalf("seed feature store: %v", err)
			}
			if err := idx.SetIDs(ctx, q.Layer, e.res, c, model.Filters(q.Filters), []string{id}, time.Minute); err != nil {
				t.Fatalf("seed cell index: %v", err)
			}
		}

		observability.SetLayerInvalidatedAt(q.Layer, time.Now())

		rr := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/query", nil)
		e.HandleQuery(context.Background(), rr, r, q)

		if rr.Code != 412 {
			t.Fatalf("want 412, got %d body=%q", rr.Code, rr.Body.String())
		}
		if got := rr.Body.String(); got != "fresh content required" {
			t.Fatalf("stable body mismatch, got %q", got)
		}
		if v := gatherCounter(reg, "stale"); v < 1 {
			t.Fatalf("expected stale reject counter to increment, got %v", v)
		}
	})

	t.Run("c) partial miss + flag → 412 (reason=miss)", func(t *testing.T) {
		e := newEngineForTest()
		q := makeReq()

		observability.SetLayerInvalidatedAt(q.Layer, time.Time{})

		cells, err := e.cellsForRes(q, e.res)
		if err != nil {
			t.Fatalf("cells: %v", err)
		}
		if len(cells) < 2 {
			t.Skip("need at least two cells for partial-miss assertion")
		}

		fs := e.fs.(*fakeFeatureStore)
		idx := e.idx.(*fakeCellIndex)
		ctx := context.Background()

		{
			id := "id-hit"
			feat := fmt.Appendf(nil, featureTemplate, id, id)
			if err := fs.PutFeatures(ctx, q.Layer, map[string][]byte{id: feat}, time.Minute); err != nil {
				t.Fatalf("seed feature store: %v", err)
			}
			if err := idx.SetIDs(ctx, q.Layer, e.res, cells[0], model.Filters(q.Filters), []string{id}, time.Minute); err != nil {
				t.Fatalf("seed cell index: %v", err)
			}
		}

		before := gatherCounter(reg, "miss")
		rr := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/query", nil)
		e.HandleQuery(context.Background(), rr, r, q)

		if rr.Code != 412 {
			t.Fatalf("want 412, got %d", rr.Code)
		}
		if got := rr.Body.String(); got != "fresh content required" {
			t.Fatalf("stable body mismatch, got %q", got)
		}
		after := gatherCounter(reg, "miss")
		if after-before < 1 {
			t.Fatalf("expected miss reject counter to increment, got delta=%v (before=%v after=%v)", after-before, before, after)
		}
	})

	t.Run("d) flag off → never 412", func(t *testing.T) {
		e := newEngineForTest()
		e.serveFreshOnly = false

		q := makeReq()
		cells, err := e.cellsForRes(q, e.res)
		if err != nil || len(cells) == 0 {
			t.Fatalf("cells: %v len=%d", err, len(cells))
		}

		fs := e.fs.(*fakeFeatureStore)
		idx := e.idx.(*fakeCellIndex)
		ctx := context.Background()

		for i, c := range cells {
			id := "id-" + fmt.Sprint(i)
			feat := fmt.Appendf(nil, featureTemplate, id, id)

			if err := fs.PutFeatures(ctx, q.Layer, map[string][]byte{id: feat}, time.Minute); err != nil {
				t.Fatalf("seed feature store: %v", err)
			}
			if err := idx.SetIDs(ctx, q.Layer, e.res, c, model.Filters(q.Filters), []string{id}, time.Minute); err != nil {
				t.Fatalf("seed cell index: %v", err)
			}
		}

		observability.SetLayerInvalidatedAt(q.Layer, time.Now())

		rr := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/query", nil)
		e.HandleQuery(context.Background(), rr, r, q)

		if rr.Code != 200 {
			t.Fatalf("want 200 with flag off, got %d", rr.Code)
		}
		before := gatherCounter(reg, "stale")
		after := gatherCounter(reg, "stale")
		if after-before != 0 {
			t.Fatalf("unexpected stale rejects delta: %v (before=%v after=%v)", after-before, before, after)
		}
	})
}

func (f *fakeCellIndex) DelCells(
	ctx context.Context,
	layer string,
	res int,
	cells []string,
	filters model.Filters,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.m == nil {
		return nil
	}
	for _, cell := range cells {
		k := cellKey{
			layer: layer,
			res:   res,
			cell:  cell,
			filt:  filters,
		}
		delete(f.m, k)
	}
	return nil
}
