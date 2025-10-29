package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/cache"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/model"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/invalidation"
)

type fakeCache struct {
	failFirst atomic.Bool
	seenDel   []string
	mu        sync.Mutex
}

func (f *fakeCache) MGet(_ []string) (map[string][]byte, error)    { return nil, nil }
func (f *fakeCache) Set(_ string, _ []byte, _ time.Duration) error { return nil }
func (f *fakeCache) Del(keys ...string) error {
	f.mu.Lock()
	f.seenDel = append(f.seenDel, keys...)
	f.mu.Unlock()
	if f.failFirst.Load() {
		f.failFirst.Store(false)
		return errors.New("boom")
	}
	return nil
}

type fakeMapper struct{}

func (fakeMapper) CellsForBBox(_ model.BBox, _ int) (model.Cells, error) {
	return model.Cells{"892a100d2b3ffff", "892a100d2b7ffff"}, nil
}

func (fakeMapper) CellsForPolygon(_ model.Polygon, _ int) (model.Cells, error) {
	return model.Cells{"892a100d2b3ffff"}, nil
}

type fakeHot struct {
	reset [][]string
	mu    sync.Mutex
}

func (f *fakeHot) Reset(cells ...string) {
	f.mu.Lock()
	f.reset = append(f.reset, cells)
	f.mu.Unlock()
}

type sess struct {
	ctx    context.Context
	mu     sync.Mutex
	marked []int64
}

func (s *sess) Claims() map[string][]int32 { return nil }
func (s *sess) MemberID() string           { return "" }
func (s *sess) GenerationID() int32        { return 0 }
func (s *sess) MarkMessage(m *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	s.marked = append(s.marked, m.Offset)
	s.mu.Unlock()
}
func (s *sess) ResetOffset(_ string, _ int32, _ int64, _ string) {}
func (s *sess) MarkOffset(_ string, _ int32, _ int64, _ string)  {}
func (s *sess) Context() context.Context                         { return s.ctx }
func (s *sess) Errors() <-chan error                             { return nil }
func (s *sess) Commit()                                          {}

type claim struct {
	part int32
	msgs chan *sarama.ConsumerMessage
}

func (c *claim) Topic() string                            { return "spatial-updates" }
func (c *claim) Partition() int32                         { return c.part }
func (c *claim) InitialOffset() int64                     { return 0 }
func (c *claim) HighWaterMarkOffset() int64               { return 0 }
func (c *claim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }

func eventBytesBBox() []byte {
	ev := invalidation.Event{
		Version: 1, Op: "update", Layer: "demo:places", TS: time.Now().UTC(),
		BBox: &invalidation.BBox{X1: 11, Y1: 55, X2: 12, Y2: 56, SRID: "EPSG:4326"},
	}
	b, _ := json.Marshal(ev)
	return b
}

func newConsumerForTest(fc cache.Interface, hm *fakeHot) *Consumer {
	cfg := Config{Brokers: []string{"x"}, Topic: "spatial-updates", GroupID: "g"}
	logger := slog.Default()
	return New(cfg, logger, fc, fakeMapper{}, hm, []int{8})
}

func TestSinglePartition_OrderAndCommitAfterWork(t *testing.T) {
	fc := &fakeCache{}
	hm := &fakeHot{}
	c := newConsumerForTest(fc, hm)

	g := &groupHandler{process: c.ProcessOne}
	ctx := t.Context()
	s := &sess{ctx: ctx}
	ch := make(chan *sarama.ConsumerMessage, 2)
	cl := &claim{part: 0, msgs: ch}

	ch <- &sarama.ConsumerMessage{Topic: "spatial-updates", Partition: 0, Offset: 10, Value: eventBytesBBox()}
	ch <- &sarama.ConsumerMessage{Topic: "spatial-updates", Partition: 0, Offset: 11, Value: eventBytesBBox()}
	close(ch)

	if err := g.ConsumeClaim(s, cl); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	if len(s.marked) != 2 || s.marked[0] != 10 || s.marked[1] != 11 {
		t.Fatalf("marked offsets=%v want [10 11]", s.marked)
	}
	if len(hm.reset) == 0 {
		t.Fatalf("expected hotness Reset to be called")
	}
}

func TestRetry_CommitOnceAfterSuccess(t *testing.T) {
	fc := &fakeCache{}
	fc.failFirst.Store(true)
	hm := &fakeHot{}
	c := newConsumerForTest(fc, hm)
	ctx := context.Background()

	msg := &sarama.ConsumerMessage{Topic: "spatial-updates", Partition: 0, Offset: 5, Value: eventBytesBBox()}
	if err := c.ProcessOne(ctx, msg); err == nil {
		t.Fatalf("expected error on first attempt")
	}

	s := &sess{ctx: ctx}
	g := &groupHandler{process: c.ProcessOne}
	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- msg
	close(ch)
	if err := g.ConsumeClaim(s, &claim{part: 0, msgs: ch}); err != nil {
		t.Fatalf("ConsumeClaim second attempt: %v", err)
	}
	if len(s.marked) != 1 || s.marked[0] != 5 {
		t.Fatalf("offset was not marked after success; marked=%v", s.marked)
	}
}

func TestMultiPartition_Parallel_NoCrossOrdering(t *testing.T) {
	fc := &fakeCache{}
	hm := &fakeHot{}
	c := newConsumerForTest(fc, hm)
	g := &groupHandler{process: c.ProcessOne}

	ctx := t.Context()
	s := &sess{ctx: ctx}

	p0 := make(chan *sarama.ConsumerMessage, 2)
	p1 := make(chan *sarama.ConsumerMessage, 2)
	p0 <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 1, Value: eventBytesBBox()}
	p0 <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 2, Value: eventBytesBBox()}
	p1 <- &sarama.ConsumerMessage{Topic: "t", Partition: 1, Offset: 1, Value: eventBytesBBox()}
	p1 <- &sarama.ConsumerMessage{Topic: "t", Partition: 1, Offset: 2, Value: eventBytesBBox()}
	close(p0)
	close(p1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = g.ConsumeClaim(s, &claim{part: 0, msgs: p0}) }()
	go func() { defer wg.Done(); _ = g.ConsumeClaim(s, &claim{part: 1, msgs: p1}) }()
	wg.Wait()

	if len(s.marked) != 4 {
		t.Fatalf("expected 4 marks total; got %v", s.marked)
	}
}
