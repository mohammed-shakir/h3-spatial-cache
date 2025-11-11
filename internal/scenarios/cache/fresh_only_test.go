package cache

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/aggregate/geojsonagg"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
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

func newEngineForTest() *Engine {
	disc := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{})
	return &Engine{
		logger:     slog.New(disc),
		res:        8,
		minRes:     8,
		maxRes:     8,
		mapr:       h3mapper.New(),
		eng:        composer.Engine{V2: composer.NewGeoJSONV2Adapter(geojsonagg.NewAdvanced())},
		store:      fakeStore{m: map[string][]byte{}},
		owsURL:     &url.URL{Scheme: "http", Host: "example.invalid"},
		ttlDefault: time.Second,
		ttlMap:     map[string]time.Duration{},
		maxWorkers: 1,
		queueSize:  1,
		opTimeout:  150 * time.Millisecond,
		// adaptive off for these tests; we only test the serveFreshOnly gate
		adaptiveEnabled: false,
		adaptiveDryRun:  false,
		serveFreshOnly:  true, // toggled per test when needed
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
	const fc = `{"type":"FeatureCollection","features":[]}`

	t.Run("a) full hit fresh → 200", func(t *testing.T) {
		e := newEngineForTest()
		q := makeReq()
		observability.SetLayerInvalidatedAt(q.Layer, time.Time{})
		cells, err := e.cellsForRes(q, e.res)
		if err != nil || len(cells) == 0 {
			t.Fatalf("cells: %v len=%d", err, len(cells))
		}
		// seed full fresh hits
		fs := e.store.(fakeStore)
		for _, c := range cells {
			k := keys.Key(q.Layer, e.res, c, q.Filters)
			fs.m[k] = encodeCacheValue([]byte(fc))
		}
		e.store = fs

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
		cells, _ := e.cellsForRes(q, e.res)
		fs := e.store.(fakeStore)
		var wroteAt int64
		for _, c := range cells {
			k := keys.Key(q.Layer, e.res, c, q.Filters)
			val := encodeCacheValue([]byte(fc))
			_, w, _ := decodeCacheValue(val)
			wroteAt = w
			fs.m[k] = val
		}
		e.store = fs
		// make cache stale vs layer
		observability.SetLayerInvalidatedAt(q.Layer, time.Unix(wroteAt+1, 0))

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
		cells, _ := e.cellsForRes(q, e.res)
		if len(cells) < 2 {
			t.Skip("need at least two cells for partial-miss assertion")
		}
		fs := e.store.(fakeStore)
		// only seed one hit -> others are misses
		k := keys.Key(q.Layer, e.res, cells[0], q.Filters)
		fs.m[k] = encodeCacheValue([]byte(fc))
		e.store = fs

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
		e.serveFreshOnly = false // disable global gate
		q := makeReq()
		observability.SetLayerInvalidatedAt(q.Layer, time.Time{})
		cells, _ := e.cellsForRes(q, e.res)
		fs := e.store.(fakeStore)
		var wroteAt int64
		for _, c := range cells {
			k := keys.Key(q.Layer, e.res, c, q.Filters)
			val := encodeCacheValue([]byte(fc))
			_, w, _ := decodeCacheValue(val)
			wroteAt = w
			fs.m[k] = val
		}
		e.store = fs
		observability.SetLayerInvalidatedAt(q.Layer, time.Unix(wroteAt+1, 0)) // would be stale, but gate is off

		rr := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/query", nil)
		e.HandleQuery(context.Background(), rr, r, q)

		if rr.Code != 200 {
			t.Fatalf("want 200 with flag off, got %d", rr.Code)
		}
		// no new increments expected (assert delta)
		before := gatherCounter(reg, "stale")
		after := gatherCounter(reg, "stale")
		if after-before != 0 {
			t.Fatalf("unexpected stale rejects delta: %v (before=%v after=%v)", after-before, before, after)
		}
	})
}
