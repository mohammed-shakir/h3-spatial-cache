package observability

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestSpatialReadsCounter_LabelsAndIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	Init(reg, true)
	SetScenario("cache")

	ObserveSpatialRead("hit", true)
	ObserveSpatialRead("miss", false)
	ObserveSpatialRead("miss", false)

	// scrape from a dedicated handler bound to our registry
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
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

	exp1 := `spatial_reads_total{cache="hit",scenario="cache",stale="true"} 1`
	exp2 := `spatial_reads_total{cache="miss",scenario="cache",stale="false"} 2`
	if !strings.Contains(out, exp1) {
		t.Fatalf("expected %q in metrics; got:\n%s", exp1, out)
	}
	if !strings.Contains(out, exp2) {
		t.Fatalf("expected %q in metrics; got:\n%s", exp2, out)
	}
}
