package cache_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/baseline"
	_ "github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios/cache"
)

type gsDouble struct {
	calls       int64
	inflight    int64
	maxInflight int64
	started     chan struct{}
	release     chan struct{}
}

type gsFail struct {
	status int
}

func (g *gsFail) handler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "upstream failure", g.status)
}

// simulates geoserver, tracks calls and concurrency
func (g *gsDouble) handler(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&g.calls, 1)
	cur := atomic.AddInt64(&g.inflight, 1)
	for {
		oldMax := atomic.LoadInt64(&g.maxInflight)
		if cur <= oldMax || atomic.CompareAndSwapInt64(&g.maxInflight, oldMax, cur) {
			break
		}
	}

	if g.started != nil {
		select {
		case g.started <- struct{}{}:
		default:
		}
	}

	if g.release != nil {
		<-g.release
	}

	q := r.URL.Query()
	if !strings.Contains(q.Get("cql_filter"), "INTERSECTS(") {
		http.Error(w, "missing INTERSECTS", http.StatusBadRequest)
		atomic.AddInt64(&g.inflight, -1)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{"ok":true}}]}`)
	atomic.AddInt64(&g.inflight, -1)
}

func fullFeature(name string) []byte {
	return []byte(`{"type":"FeatureCollection","features":[{"type":"Feature","id":"` + name + `","geometry":null,"properties":{"name":"` + name + `"}}]}`)
}

func TestCache_FullHit_NoUpstreamCalls(t *testing.T) {
	gs := &gsDouble{}
	srv := httptest.NewServer(http.HandlerFunc(gs.handler))
	defer srv.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	cfg := config.FromEnv()
	cfg.Scenario = "cache"
	cfg.RedisAddr = mr.Addr()
	cfg.GeoServerURL = strings.TrimRight(srv.URL, "/")
	cfg.CacheTTLDefault = 30 * time.Second
	cfg.AdaptiveEnabled = false
	cfg.AdaptiveDryRun = false
	bb := model.BBox{X1: 18.00, Y1: 59.32, X2: 18.02, Y2: 59.34, SRID: "EPSG:4326"}

	mapr := h3mapper.New()
	cells, err := mapr.CellsForBBox(bb, cfg.H3Res)
	if err != nil || len(cells) == 0 {
		t.Fatalf("h3 mapping: %v", err)
	}
	for i, c := range cells {
		k := keys.Key("demo:NR_polygon", cfg.H3Res, c, "")
		_ = mr.Set(k, string(fullFeature(c+":"+fmtInt(i))))
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := scenarios.New("cache", cfg, logger, nil)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	q.Set("bbox", bb.String())
	req.URL.RawQuery = q.Encode()

	rr := httptest.NewRecorder()
	h.HandleQuery(req.Context(), rr, req, model.QueryRequest{Layer: "demo:NR_polygon", BBox: &bb})

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	if gs.calls != 0 {
		t.Fatalf("expected zero upstream calls on full hit; got %d", gs.calls)
	}
	var out struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil || out.Type != "FeatureCollection" {
		t.Fatalf("bad merge output: %v body=%s", err, rr.Body.String())
	}
	if len(rr.Body.Bytes()) == 0 {
		t.Fatalf("expected non-empty body on full cache hit")
	}
}

func TestCache_PartialMiss_FetchesOnlyMissing_BoundedConcurrency(t *testing.T) {
	gs := &gsDouble{
		started: make(chan struct{}, 128),
		release: make(chan struct{}),
	}
	srv := httptest.NewServer(http.HandlerFunc(gs.handler))
	defer srv.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	cfg := config.FromEnv()
	cfg.Scenario = "cache"
	cfg.RedisAddr = mr.Addr()
	cfg.GeoServerURL = strings.TrimRight(srv.URL, "/")
	cfg.CacheTTLDefault = 45 * time.Second
	cfg.CacheTTLOvr = map[string]time.Duration{"demo:NR_polygon": 2 * time.Minute}
	cfg.CacheFillMaxWorkers = 2
	cfg.CacheFillQueue = 16
	cfg.CacheOpTimeout = 750 * time.Millisecond
	cfg.H3Res = 7
	cfg.AdaptiveEnabled = false
	cfg.AdaptiveDryRun = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := scenarios.New("cache", cfg, logger, nil)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	mapr := h3mapper.New()
	bb := model.BBox{X1: 18.00, Y1: 59.32, X2: 18.10, Y2: 59.42, SRID: "EPSG:4326"}
	cells, err := mapr.CellsForBBox(bb, cfg.H3Res)
	if err != nil {
		t.Fatalf("h3: %v", err)
	}
	if len(cells) < 4 {
		t.Fatalf("need >=4 cells for this test; got %d", len(cells))
	}

	for i := range len(cells) / 2 {
		k := keys.Key("demo:NR_polygon", cfg.H3Res, cells[i], "")
		_ = mr.Set(k, string(fullFeature("hit-"+fmtInt(i))))
	}

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	q.Set("bbox", bb.String())
	req.URL.RawQuery = q.Encode()
	rr := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.HandleQuery(req.Context(), rr, req, model.QueryRequest{Layer: "demo:NR_polygon", BBox: &bb})
		close(done)
	}()

	for range cfg.CacheFillMaxWorkers {
		<-gs.started
	}
	close(gs.release)

	<-done

	wantMisses := len(cells) - len(cells)/2
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	if int(gs.calls) != wantMisses {
		t.Fatalf("upstream calls=%d want %d", gs.calls, wantMisses)
	}
	if gs.maxInflight > int64(cfg.CacheFillMaxWorkers) {
		t.Fatalf("max inflight=%d exceeded workers=%d", gs.maxInflight, cfg.CacheFillMaxWorkers)
	}
	for i := len(cells) / 2; i < len(cells); i++ {
		k := keys.Key("demo:NR_polygon", cfg.H3Res, cells[i], "")
		if !mr.Exists(k) {
			t.Fatalf("missing cached key: %s", k)
		}
		ttl := mr.TTL(k)
		if ttl <= 0 || ttl > 2*time.Minute {
			t.Fatalf("unexpected TTL for %s: %v", k, ttl)
		}
	}
	if len(rr.Body.Bytes()) == 0 {
		t.Fatalf("expected non-empty body on partial miss")
	}
	var out struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil || out.Type != "FeatureCollection" {
		t.Fatalf("bad merge output: %v body=%s", err, rr.Body.String())
	}
	if len(out.Features) == 0 {
		t.Fatalf("expected merged features > 0 on partial miss")
	}
}

func TestCache_FullMiss_ReadThrough_Caches(t *testing.T) {
	gs := &gsDouble{}
	srv := httptest.NewServer(http.HandlerFunc(gs.handler))
	defer srv.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	cfg := config.FromEnv()
	cfg.Scenario = "cache"
	cfg.RedisAddr = mr.Addr()
	cfg.GeoServerURL = strings.TrimRight(srv.URL, "/")
	cfg.CacheTTLDefault = 30 * time.Second
	cfg.AdaptiveEnabled = false
	cfg.AdaptiveDryRun = false

	bb := model.BBox{X1: 18.00, Y1: 59.32, X2: 18.02, Y2: 59.34, SRID: "EPSG:4326"}
	mapr := h3mapper.New()
	cells, err := mapr.CellsForBBox(bb, cfg.H3Res)
	if err != nil || len(cells) == 0 {
		t.Fatalf("h3 mapping: %v", err)
	}

	// ensure cache is empty first
	for _, c := range cells {
		k := keys.Key("demo:NR_polygon", cfg.H3Res, c, "")
		if mr.Exists(k) {
			t.Fatalf("expected empty cache before miss, found key: %s", k)
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := scenarios.New("cache", cfg, logger, nil)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	q.Set("bbox", bb.String())
	req.URL.RawQuery = q.Encode()
	rr := httptest.NewRecorder()
	h.HandleQuery(req.Context(), rr, req, model.QueryRequest{Layer: "demo:NR_polygon", BBox: &bb})

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%q", rr.Code, rr.Body.String())
	}
	if gs.calls == 0 {
		t.Fatalf("expected upstream to be called on full miss")
	}
}

func fmtInt(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	s := make([]byte, 0, 20)
	for n > 0 {
		s = append([]byte{digits[n%10]}, s...)
		n /= 10
	}
	return string(s)
}

func TestCache_BackendErrorOnMiss_ReturnsErrorBody(t *testing.T) {
	gs := &gsFail{status: http.StatusInternalServerError}
	srv := httptest.NewServer(http.HandlerFunc(gs.handler))
	defer srv.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	cfg := config.FromEnv()
	cfg.Scenario = "cache"
	cfg.RedisAddr = mr.Addr()
	cfg.GeoServerURL = strings.TrimRight(srv.URL, "/")
	cfg.CacheTTLDefault = 30 * time.Second
	cfg.AdaptiveEnabled = false
	cfg.AdaptiveDryRun = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := scenarios.New("cache", cfg, logger, nil)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	bb := model.BBox{X1: 18.00, Y1: 59.32, X2: 18.02, Y2: 59.34, SRID: "EPSG:4326"}

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	q.Set("bbox", bb.String())
	req.URL.RawQuery = q.Encode()
	rr := httptest.NewRecorder()

	h.HandleQuery(req.Context(), rr, req, model.QueryRequest{Layer: "demo:NR_polygon", BBox: &bb})

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 Bad Gateway", rr.Code)
	}
	if len(rr.Body.Bytes()) == 0 {
		t.Fatalf("expected non-empty error body on upstream failure")
	}
}

// NEW: input validation (no bbox/polygon) should be 400 with an error message
func TestCache_InputValidationError_Returns400(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	cfg := config.FromEnv()
	cfg.Scenario = "cache"
	cfg.RedisAddr = mr.Addr()
	cfg.GeoServerURL = "http://example.invalid/geoserver"
	cfg.CacheTTLDefault = 30 * time.Second
	cfg.AdaptiveEnabled = false
	cfg.AdaptiveDryRun = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := scenarios.New("cache", cfg, logger, nil)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	req.URL.RawQuery = q.Encode()
	rr := httptest.NewRecorder()

	h.HandleQuery(req.Context(), rr, req, model.QueryRequest{Layer: "demo:NR_polygon"})

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", rr.Code)
	}
	if len(rr.Body.Bytes()) == 0 {
		t.Fatalf("expected non-empty body on invalid query")
	}
}
