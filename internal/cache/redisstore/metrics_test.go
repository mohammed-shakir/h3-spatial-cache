package redisstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/metrics"
)

func Test_RedisMetrics_MGet_HitMiss(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	p := metrics.Init(metrics.Config{})
	observability.Init(p.Registerer(), true)
	observability.SetScenario("baseline")

	ctx := context.Background()
	c, err := New(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("new redis: %v", err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			t.Fatalf("close redis client: %v", cerr)
		}
	}()

	_ = c.Set(ctx, "k:hit", []byte("v"), time.Minute)

	_, _ = c.MGet(ctx, []string{"k:hit", "k:miss"})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	body := rr.Body.String()

	if !strings.Contains(body, `redis_operation_duration_seconds_count`) {
		t.Fatalf("missing redis_operation_duration_seconds_count\n%s", body)
	}
	if !strings.Contains(body, `spatial_cache_hits_total{scenario="baseline"} 1`) {
		t.Fatalf("expected 1 hit\n%s", body)
	}
	if !strings.Contains(body, `spatial_cache_misses_total{scenario="baseline"} 1`) {
		t.Fatalf("expected 1 miss\n%s", body)
	}
}
