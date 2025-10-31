package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestSpatialMetrics_RegistrationAndLabels(t *testing.T) {
	ObserveSpatialResponse("full_hit", "geojson", 0.012)
	ObserveSpatialResponse("miss", "geojson", 0.250)
	IncSpatialAggError("merge")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	body := rr.Body.String()

	if !strings.Contains(body, `spatial_response_total{format="geojson",hit_class="full_hit",scenario="baseline"} `) &&
		!strings.Contains(body, `spatial_response_total{hit_class="full_hit",format="geojson",scenario="baseline"} `) {
		t.Fatalf("missing spatial_response_total sample with expected labels:\n%s", body)
	}

	if !strings.Contains(body, `spatial_response_duration_seconds_bucket`) {
		t.Fatalf("missing histogram buckets for spatial_response_duration_seconds:\n%s", body)
	}

	if !strings.Contains(body, `spatial_aggregation_errors_total{stage="merge"} `) {
		t.Fatalf("missing spatial_aggregation_errors_total{stage=\"merge\"}:\n%s", body)
	}
}
