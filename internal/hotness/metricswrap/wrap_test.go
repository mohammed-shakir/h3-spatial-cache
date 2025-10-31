package metricswrap

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness/expdecay"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/metrics"
)

func Test_HotnessGauge_Updates(t *testing.T) {
	p := metrics.Init(metrics.Config{})
	observability.Init(p.Registerer(), true)

	tr := expdecay.New(30 * time.Second)
	w := New(tr, "topN")

	w.Inc("cellA")
	w.Inc("cellB")
	w.Reset("cellA")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	body := rr.Body.String()

	if !strings.Contains(body, `spatial_cache_hot_keys{scenario="baseline",tier="topN"} 1`) {
		t.Fatalf("expected hot_keys gauge == 1, got:\n%s", body)
	}
}
