package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
)

type fakeCache struct {
	mu  sync.Mutex
	del []string
	err error
}

func (f *fakeCache) MGet(_ []string) (map[string][]byte, error)    { return nil, nil }
func (f *fakeCache) Set(_ string, _ []byte, _ time.Duration) error { return nil }
func (f *fakeCache) Del(keys ...string) error {
	f.mu.Lock()
	f.del = append(f.del, keys...)
	f.mu.Unlock()
	return f.err
}

type mapper struct{}

func (mapper) CellsForBBox(_ model.BBox, _ int) (model.Cells, error) {
	return model.Cells{"892a100d2b3ffff", "892a100d2b7ffff"}, nil
}

func (mapper) CellsForPolygon(_ model.Polygon, _ int) (model.Cells, error) {
	return model.Cells{"892a100d2b3ffff"}, nil
}

func TestWireEvent_Deser_AndIdempotency(t *testing.T) {
	cfg := InvalidationConfig{Enabled: true, Driver: DriverKafka}
	fc := &fakeCache{}
	reg := prometheus.NewRegistry()
	r := New(cfg, fc, mapper{}, Options{Register: reg, ResRange: []int{8}})
	ctx := context.Background()

	w := WireEvent{
		Layer:   "demo:NR_polygon",
		H3Cells: []string{"892a100d2b3ffff"},
		Version: 1,
		TS:      time.Now().UTC(),
		Op:      "update",
	}
	b, _ := json.Marshal(w)
	msg := &sarama.ConsumerMessage{
		Topic:     "t",
		Partition: 0,
		Offset:    1,
		Timestamp: time.Now().UTC(),
		Value:     b,
	}
	if err := r.handleMessage(ctx, msg); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := r.handleMessage(ctx, msg); err != nil {
		t.Fatalf("second handleMessage: %v", err)
	}
}
