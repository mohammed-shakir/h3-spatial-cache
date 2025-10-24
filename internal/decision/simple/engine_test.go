package simple

import (
	"sync"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/decision"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
)

type fakeHot struct {
	mu sync.Mutex
	m  map[string]float64
}

func newFakeHot() *fakeHot { return &fakeHot{m: make(map[string]float64)} }

func (f *fakeHot) Inc(cell string) {
	f.mu.Lock()
	f.m[cell]++
	f.mu.Unlock()
}

func (f *fakeHot) Score(cell string) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m[cell]
}

func (f *fakeHot) Reset(cells ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(cells) == 0 {
		f.m = make(map[string]float64)
		return
	}
	for _, c := range cells {
		delete(f.m, c)
	}
}

var (
	_ hotness.Interface  = (*fakeHot)(nil)
	_ decision.Interface = (*Engine)(nil)
)

func TestShouldCache_AnyCellCrossesThreshold(t *testing.T) {
	h := newFakeHot()
	e := &Engine{
		Hot:       h,
		Threshold: 2.0,
		BaseRes:   8,
		MinRes:    5,
		MaxRes:    12,
	}

	cells := []string{"8a2a1072a6bffff", "8a2a1072a6cffff", "8a2a1072a6dffff"}

	h.m[cells[0]] = 1.5
	h.m[cells[1]] = 0.0
	h.m[cells[2]] = 1.9
	if e.ShouldCache(cells) {
		t.Fatalf("expected ShouldCache=false when all scores < threshold")
	}

	h.m[cells[2]] = 2.0
	// crossing threshold on one cell should trigger caching
	if !e.ShouldCache(cells) {
		t.Fatalf("expected ShouldCache=true when any score >= threshold")
	}

	h.m[cells[1]] = 5.0
	if !e.ShouldCache(cells) {
		t.Fatalf("expected ShouldCache=true when a later cell crosses threshold")
	}
}

func TestSelectResolution_ReturnsBaseRes(t *testing.T) {
	e := &Engine{
		Hot:       newFakeHot(),
		Threshold: 1.0,
		BaseRes:   9,
		MinRes:    5,
		MaxRes:    12,
	}
	got := e.SelectResolution(decision.LoadStats{})
	if got != 9 {
		t.Fatalf("SelectResolution got %d, want %d", got, 9)
	}
}

func TestResetBehavior_IsolatedToSelectedCells(t *testing.T) {
	h := newFakeHot()
	cold := "892a100d2b3ffff"
	hot := "892a100d2b7ffff"

	h.m[cold] = 0.5
	h.m[hot] = 3.0

	if h.Score(hot) <= 2.0 {
		t.Fatalf("precondition: hot score should be > 2.0")
	}

	h.Reset(hot)
	// only hot cell should be reset
	if h.Score(hot) != 0 {
		t.Fatalf("expected hot to be zero after reset")
	}
	if h.Score(cold) != 0.5 {
		t.Fatalf("expected cold to remain unchanged")
	}
}
