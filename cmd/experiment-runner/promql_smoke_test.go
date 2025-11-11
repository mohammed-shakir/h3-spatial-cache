package main

import (
	"net/http"
	"os"
	"testing"
)

func TestPromQLHistogramQuantileSmoke(t *testing.T) {
	base := os.Getenv("PROM_URL")
	if base == "" {
		t.Skip("PROM_URL not set; skipping smoke")
	}
	u := base + "/api/v1/query?query=histogram_quantile(0.95,sum%20by%20(le)%20(rate(spatial_response_duration_seconds_bucket[5m])))"
	// #nosec G107 -- test-only; URL originates from a controlled env var in dev/test.
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("prom query: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
