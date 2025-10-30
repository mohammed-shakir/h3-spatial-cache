package invalidation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache/keys"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation/kafkaconsumer"
)

type redisAdapter struct{ rdb *redis.Client }

func (a *redisAdapter) MGet(_ []string) (map[string][]byte, error)    { return nil, nil }
func (a *redisAdapter) Set(_ string, _ []byte, _ time.Duration) error { return nil }
func (a *redisAdapter) Del(k ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.rdb.Del(ctx, k...).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

type mapOK struct{}

func (mapOK) CellsForBBox(_ model.BBox, _ int) (model.Cells, error) {
	return model.Cells{"892a100d2b3ffff", "892a100d2b7ffff"}, nil
}

func (mapOK) CellsForPolygon(_ model.Polygon, _ int) (model.Cells, error) {
	return model.Cells{"892a100d2b3ffff"}, nil
}

type hotSink struct{}

func (hotSink) Reset(_ ...string) {}

func TestIntegration_Miniredis_DeleteAndMetrics(t *testing.T) {
	mr, _ := miniredis.Run()
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	layer, res := "demo:NR_polygon", 8
	k1 := keys.Key(layer, res, "892a100d2b3ffff", "")
	k2 := keys.Key(layer, res, "892a100d2b7ffff", "")
	_ = mr.Set(k1, `{"type":"FeatureCollection","features":[]}`)
	_ = mr.Set(k2, `{"type":"FeatureCollection","features":[]}`)

	cons := kafkaconsumer.New(
		kafkaconsumer.FromEnv(),
		nil,
		&redisAdapter{rdb: rdb},
		mapOK{}, hotSink{}, []int{8},
	)

	ev := invalidation.Event{
		Version: 1, Op: "update", Layer: layer, TS: time.Now().UTC(),
		BBox: &invalidation.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	body, _ := json.Marshal(ev)
	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 1, Value: body}

	if err := cons.ProcessOne(context.Background(), msg); err != nil {
		t.Fatalf("processOne: %v", err)
	}

	if mr.Exists(k1) || mr.Exists(k2) {
		t.Fatalf("expected k1/k2 to be deleted")
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rr, req)

	bodyStr := rr.Body.String()
	has := func(s string) {
		if !strings.Contains(bodyStr, s) {
			t.Fatalf("metrics missing %q; got:\n%s", s, bodyStr)
		}
	}
	has("invalidation_events_total")
	has("invalidation_deleted_keys_total")
	has("invalidation_process_seconds_bucket")
}
