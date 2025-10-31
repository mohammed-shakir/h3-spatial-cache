package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
)

func assertHasMetricLine(t *testing.T, body, metric string, wantLabels ...string) {
	t.Helper()
	for ln := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(ln, metric+"{") {
			continue
		}
		ok := true
		for _, s := range wantLabels {
			if !strings.Contains(ln, s) {
				ok = false
				break
			}
		}
		if ok && (len(ln) > 0 && ln[len(ln)-1] >= '0' && ln[len(ln)-1] <= '9') {
			return
		}
	}
	t.Fatalf("expected a %s line with labels %v; got:\n%s", metric, wantLabels, body)
}

func Test_AppMetrics_CustomRegistry_Smoke(t *testing.T) {
	p := Init(Config{Build: BuildInfo{Version: "test"}})
	observability.Init(p.Registerer(), true)
	observability.SetScenario("baseline")
	observability.ExposeBuildInfo("test")

	start := time.Now()
	observability.ObserveSpatialResponse("miss", "geojson", time.Since(start).Seconds())
	observability.ObserveSpatialResponse("full_hit", "geojson", 0.010)

	observability.AddCacheHits(3)
	observability.AddCacheMisses(1)
	observability.ObserveCacheOp("mget", nil, 0.002)

	observability.SetHotKeysGauge("topN", 42)
	observability.IncKafkaConsumerError("decode")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	body := rr.Body.String()
	mustContain := []string{
		`spatial_response_duration_seconds_bucket`,
		`redis_operation_duration_seconds_count`,
		`spatial_cache_hits_total{scenario="baseline"} `,
		`spatial_cache_misses_total{scenario="baseline"} `,
		`spatial_cache_hot_keys{scenario="baseline",tier="topN"} 42`,
		`kafka_consumer_errors_total{kind="decode",scenario="baseline"} `,
	}
	for _, s := range mustContain {
		if !strings.Contains(body, s) {
			t.Fatalf("expected metrics to contain %q;\n---\n%s", s, body)
		}
	}

	assertHasMetricLine(t, body, "spatial_response_total",
		`hit_class="miss"`, `format="geojson"`, `scenario="baseline"`)
	assertHasMetricLine(t, body, "spatial_response_total",
		`hit_class="full_hit"`, `format="geojson"`, `scenario="baseline"`)
	assertHasMetricLine(t, body, "app_build_info",
		`version="test"`)
}
