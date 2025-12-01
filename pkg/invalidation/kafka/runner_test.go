package kafka

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation"
)

type fakeCellIndex struct {
	mu   sync.Mutex
	dels []cellIndexDelCall
}

type cellIndexDelCall struct {
	layer string
	res   int
	cells []string
}

type fakeCache struct {
	mu  sync.Mutex
	del []string
	err error
}

func (f *fakeCellIndex) GetIDs(
	_ context.Context,
	_ string,
	_ int,
	_ string,
	_ model.Filters,
) ([]string, error) {
	return nil, nil
}

func (f *fakeCellIndex) SetIDs(
	_ context.Context,
	_ string,
	_ int,
	_ string,
	_ model.Filters,
	_ []string,
	_ time.Duration,
) error {
	return nil
}

func (f *fakeCellIndex) DelCells(
	_ context.Context,
	layer string,
	res int,
	cells []string,
	_ model.Filters,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cp := make([]string, len(cells))
	copy(cp, cells)
	f.dels = append(f.dels, cellIndexDelCall{
		layer: layer,
		res:   res,
		cells: cp,
	})
	return nil
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
	idx := &fakeCellIndex{}

	r := New(cfg, fc, mapper{}, Options{
		Register:  reg,
		ResRange:  []int{8, 9},
		Hotness:   mr,
		CellIndex: idx,
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

	if got := mr.Count(); got != 2 {
		t.Fatalf("hotness reset count = %d, want 2", got)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if len(idx.dels) != 2 {
		t.Fatalf("expected 2 DelCells calls (one per res), got %d", len(idx.dels))
	}
	for _, call := range idx.dels {
		if len(call.cells) != 2 {
			t.Fatalf("DelCells for res=%d saw %d cells, want 2", call.res, len(call.cells))
		}
	}

	if got := observability.GetLayerInvalidatedAtUnix("demo:NR_polygon"); got != ts.Unix() {
		t.Fatalf("invalidatedAt=%d want %d", got, ts.Unix())
	}

	idx.dels = nil
	if err := r.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("second handleMessage: %v", err)
	}
	if got := mr.Count(); got != 2 {
		t.Fatalf("resets after duplicate = %d, want still 2", got)
	}
	if len(idx.dels) != 0 {
		t.Fatalf("expected no additional DelCells calls after duplicate, got %d", len(idx.dels))
	}
}

func TestSpatialEvent_HotnessReset(t *testing.T) {
	cfg := InvalidationConfig{Enabled: true, Driver: DriverKafka}
	fc := &fakeCache{}
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)
	mr := &mockResetter{}
	idx := &fakeCellIndex{}

	r := New(cfg, fc, mapper{}, Options{
		Register:  reg,
		ResRange:  []int{8},
		Hotness:   mr,
		CellIndex: idx,
	})

	ev := invalidation.Event{
		Op:    "invalidate",
		Layer: "demo:NR_polygon",
		BBox:  &invalidation.BBox{X1: 0, Y1: 0, X2: 1, Y2: 1, SRID: "EPSG:4326"},
	}
	if err := r.applySpatial(context.Background(), ev); err != nil {
		t.Fatalf("applySpatial: %v", err)
	}
	if got := mr.Count(); got != 2 {
		t.Fatalf("hotness reset count = %d, want 2", got)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if len(idx.dels) != 1 {
		t.Fatalf("expected 1 DelCells call for spatial invalidation, got %d", len(idx.dels))
	}
	if len(idx.dels[0].cells) != 2 {
		t.Fatalf("DelCells saw %d cells, want 2", len(idx.dels[0].cells))
	}
}

func TestRunner_WireEvent_DeletesCellIndex(t *testing.T) {
	cfg := InvalidationConfig{Enabled: true, Driver: DriverKafka}
	fc := &fakeCache{}
	reg := prometheus.NewRegistry()
	observability.Init(reg, true)

	idx := &fakeCellIndex{}

	r := New(cfg, fc, mapper{}, Options{
		Logger:    slogDiscard(),
		Register:  reg,
		ResRange:  []int{8, 9},
		Hotness:   nil,
		CellIndex: idx,
	})

	ts := time.Now().UTC()
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
		Topic:     "t",
		Partition: 0,
		Offset:    1,
		Timestamp: ts,
		Value:     b,
	}
	if err := r.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.dels) != 2 {
		t.Fatalf("expected 2 DelCells calls (one per res), got %d", len(idx.dels))
	}
	for _, call := range idx.dels {
		if call.layer != "demo:NR_polygon" {
			t.Fatalf("DelCells layer=%q want %q", call.layer, "demo:NR_polygon")
		}
		if len(call.cells) != 2 {
			t.Fatalf("DelCells for res=%d saw %d cells, want 2", call.res, len(call.cells))
		}
	}
}

func slogDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}
