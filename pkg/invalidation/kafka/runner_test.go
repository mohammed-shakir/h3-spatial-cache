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
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation"
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

type mockResetter struct {
	mu    sync.Mutex
	calls []string
}

func (m *mockResetter) Reset(cells ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, cells...)
}

func (m *mockResetter) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestWireEvent_Deser_AndIdempotency(t *testing.T) {
	cfg := InvalidationConfig{Enabled: true, Driver: DriverKafka}
	fc := &fakeCache{}
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)
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

func TestWireEvent_HotnessReset_And_Timestamp(t *testing.T) {
	cfg := InvalidationConfig{Enabled: true, Driver: DriverKafka}
	fc := &fakeCache{}
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)

	mr := &mockResetter{}
	r := New(cfg, fc, mapper{}, Options{
		Register: reg, ResRange: []int{8, 9}, Hotness: mr,
	})

	ts := time.Now().Add(-2 * time.Second).UTC()
	w := WireEvent{
		Layer:       "demo:NR_polygon",
		H3Cells:     []string{"892a100d2b3ffff", "892a100d2b7ffff"},
		Resolutions: []int{8, 9},
		Version:     1,
		TS:          ts,
		Op:          "invalidate",
	}
	b, _ := json.Marshal(w)
	msg := &sarama.ConsumerMessage{
		Topic: "t", Partition: 0, Offset: 1, Timestamp: ts, Value: b,
	}
	if err := r.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	// two distinct cells -> two resets
	if got := mr.Count(); got != 2 {
		t.Fatalf("hotness reset count = %d, want 2", got)
	}
	// per-layer invalidation timestamp propagated
	if got := observability.GetLayerInvalidatedAtUnix("demo:NR_polygon"); got != ts.Unix() {
		t.Fatalf("invalidatedAt=%d want %d", got, ts.Unix())
	}
	// duplicate (same version) -> skip_version, no extra resets
	if err := r.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("second handleMessage: %v", err)
	}
	if got := mr.Count(); got != 2 {
		t.Fatalf("resets after duplicate = %d, want still 2", got)
	}
}

// New: spatial-path should reset hotness for all mapped cells.
func TestSpatialEvent_HotnessReset(t *testing.T) {
	cfg := InvalidationConfig{Enabled: true, Driver: DriverKafka}
	fc := &fakeCache{}
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)
	mr := &mockResetter{}
	r := New(cfg, fc, mapper{}, Options{Register: reg, ResRange: []int{8}, Hotness: mr})

	ev := invalidation.Event{
		Op:    "invalidate",
		Layer: "demo:NR_polygon",
		BBox:  &invalidation.BBox{X1: 0, Y1: 0, X2: 1, Y2: 1, SRID: "EPSG:4326"},
	}
	if err := r.applySpatial(context.Background(), ev); err != nil {
		t.Fatalf("applySpatial: %v", err)
	}
	// mapper returns 2 cells for bbox -> two resets
	if got := mr.Count(); got != 2 {
		t.Fatalf("hotness reset count = %d, want 2", got)
	}
}
