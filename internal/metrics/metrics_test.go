package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestProvider_RegistersStandardCollectors_AndBuildInfo(t *testing.T) {
	p := Init(Config{Build: BuildInfo{Version: "test", Revision: "r", Branch: "b", BuildDate: "now"}})

	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_gauge", Help: "smoke"})
	p.Register(g)
	g.Set(42)

	if n := testutil.CollectAndCount(g); n == 0 {
		t.Fatalf("expected at least 1 sample from test_gauge, got %d", n)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rr.Code)
	}
	body := rr.Body.String()

	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("expected go_goroutines in payload; got:\n%s", body)
	}
	if !strings.Contains(body, "process_cpu_seconds_total") && !strings.Contains(body, "process_start_time_seconds") {
		t.Fatalf("expected process_* metrics in payload; got:\n%s", body)
	}
	if !strings.Contains(body, `app_build_info{`) {
		t.Fatalf("expected app_build_info in payload; got:\n%s", body)
	}
}
