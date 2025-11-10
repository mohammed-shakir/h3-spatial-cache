package cache

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/config"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/scenarios"
)

type gsOK struct{}

func (gs *gsOK) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{"ok":true}}]}`)
}

func TestDetects_StaleHit_AfterInvalidationMarker(t *testing.T) {
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)
	observability.SetScenario("cache")

	metricsSrv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer metricsSrv.Close()

	gs := &gsOK{}
	srv := httptest.NewServer(http.HandlerFunc(gs.handler))
	defer srv.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	cfg := config.FromEnv()
	cfg.Scenario = "cache"
	cfg.RedisAddr = mr.Addr()
	cfg.GeoServerURL = strings.TrimRight(srv.URL, "/")
	cfg.CacheTTLDefault = 5 * time.Minute
	cfg.H3Res = 7

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := scenarios.New("cache", cfg, logger, nil)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	bb := model.BBox{X1: 18.00, Y1: 59.32, X2: 18.02, Y2: 59.34, SRID: "EPSG:4326"}

	req1 := httptest.NewRequest(http.MethodGet, "/query", nil)
	q := url.Values{}
	q.Set("layer", "demo:NR_polygon")
	q.Set("bbox", bb.String())
	req1.URL.RawQuery = q.Encode()
	rr1 := httptest.NewRecorder()
	h.HandleQuery(req1.Context(), rr1, req1, model.QueryRequest{Layer: "demo:NR_polygon", BBox: &bb})
	if rr1.Code != http.StatusOK {
		t.Fatalf("first status=%d want 200", rr1.Code)
	}

	time.Sleep(2 * time.Second)
	observability.SetLayerInvalidatedAt("demo:NR_polygon", time.Now())

	req2 := httptest.NewRequest(http.MethodGet, "/query", nil)
	req2.URL.RawQuery = q.Encode()
	rr2 := httptest.NewRecorder()
	h.HandleQuery(req2.Context(), rr2, req2, model.QueryRequest{Layer: "demo:NR_polygon", BBox: &bb})
	if rr2.Code != http.StatusOK {
		t.Fatalf("second status=%d want 200", rr2.Code)
	}

	resp, err := metricsSrv.Client().Get(metricsSrv.URL)
	if err != nil {
		t.Fatalf("metrics scrape: %v", err)
	}
	t.Cleanup(func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Fatalf("close body: %v", cerr)
		}
	})
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	out := string(b)

	exp := `spatial_reads_total{cache="hit",scenario="cache",stale="true"} 1`
	if !strings.Contains(out, exp) {
		t.Fatalf("expected %q in metrics; got:\n%s", exp, out)
	}
}
