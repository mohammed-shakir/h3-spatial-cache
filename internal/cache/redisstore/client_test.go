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

// creates new client connected to miniredis for testing
func newMini(t *testing.T) *Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	rc, err := New(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = rc.Close() })
	return rc
}

func TestSetMGetDel_HappyPath_AndMGetFiltersMissing(t *testing.T) {
	rc := newMini(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := rc.Set(ctx, "k1", []byte("v1"), 5*time.Minute)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	err = rc.Set(ctx, "k2", []byte("v2"), time.Minute)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := rc.MGet(ctx, []string{"k1", "k2", "missing"})
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("MGet size=%d want 2", len(got))
	}
	if string(got["k1"]) != "v1" || string(got["k2"]) != "v2" {
		t.Fatalf("unexpected values: %+v", got)
	}

	if err := rc.Del(ctx, "k1", "k2"); err != nil {
		t.Fatalf("Del: %v", err)
	}
}

func TestContextDeadline_IsRespected(t *testing.T) {
	rc := newMini(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := rc.Set(ctx, "k", []byte("v"), time.Second); err == nil {
		t.Fatalf("expected error on Set with canceled context")
	}
	if _, err := rc.MGet(ctx, []string{"k"}); err == nil {
		t.Fatalf("expected error on MGet with canceled context")
	}
	if err := rc.Del(ctx, "k"); err == nil {
		t.Fatalf("expected error on Del with canceled context")
	}
}

func TestMetrics_Incremented(t *testing.T) {
	p := metrics.Init(metrics.Config{})
	observability.Init(p.Registerer(), true)
	observability.SetScenario("baseline")

	rc := newMini(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = rc.Set(ctx, "m1", []byte("x"), time.Minute)
	_, _ = rc.MGet(ctx, []string{"m1"})
	_ = rc.Del(ctx, "m1")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics status=%d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `cache_op_total{op="set"`) ||
		!strings.Contains(body, `cache_op_total{op="mget"`) ||
		!strings.Contains(body, `cache_op_total{op="del"`) {
		t.Fatalf("missing cache_op_total metrics; got:\n%s", body)
	}
	if !strings.Contains(body, `redis_operation_duration_seconds_bucket{op="set"`) {
		t.Fatalf("missing redis_operation_duration_seconds histogram; got:\n%s", body)
	}
}
