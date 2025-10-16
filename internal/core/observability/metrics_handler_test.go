package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsHandler_Smoke(t *testing.T) {
	ExposeBuildInfo("test")
	ObserveHTTP("GET", "/query", 200, 0.001)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "app_build_info") && !strings.Contains(body, "http_requests_total") {
		t.Fatalf("metrics payload did not contain expected metric names; got:\n%s", body)
	}
}
