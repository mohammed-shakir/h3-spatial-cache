package expdecay

import (
	"math"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
}

func (f *fakeClock) Add(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func newTrackerForTest(hl time.Duration, fc *fakeClock) *Tracker {
	if fc == nil {
		fc = &fakeClock{}
		fc.Set(time.Unix(0, 0).UTC())
	}
	tr := New(hl)
	tr.now = fc.Now
	return tr
}

func almostEq(t *testing.T, got, want, eps float64) {
	t.Helper()
	if math.Abs(got-want) > eps {
		t.Fatalf("got=%g want=%g (eps=%g)", got, want, eps)
	}
}

func TestIncAndScore_AccumulatesImmediately(t *testing.T) {
	fc := &fakeClock{}
	fc.Set(time.Unix(0, 0).UTC())
	tr := newTrackerForTest(time.Minute, fc)

	cell := "892a100d2b3ffff"

	tr.Inc(cell)
	almostEq(t, tr.Score(cell), 1.0, 1e-9)

	tr.Inc(cell)
	almostEq(t, tr.Score(cell), 2.0, 1e-9)

	tr.Inc(cell)
	almostEq(t, tr.Score(cell), 3.0, 1e-9)
}

func TestHalfLife_DecaysByHalf(t *testing.T) {
	hl := 2 * time.Second
	fc := &fakeClock{}
	fc.Set(time.Unix(0, 0).UTC())
	tr := newTrackerForTest(hl, fc)

	cell := "892a100d2b3ffff"

	tr.Inc(cell)
	almostEq(t, tr.Score(cell), 1.0, 1e-9)

	fc.Add(hl)
	got := tr.Score(cell)
	// after one half-life, score should be halved
	almostEq(t, got, 0.5, 1e-6)

	fc.Add(hl)
	got = tr.Score(cell)
	almostEq(t, got, 0.25, 1e-6)
}

func TestConcurrency_ManyIncSameCell(t *testing.T) {
	fc := &fakeClock{}
	fc.Set(time.Unix(0, 0).UTC())
	tr := newTrackerForTest(1*time.Minute, fc)

	cell := "hot-city-center"
	const N = 256

	var wg sync.WaitGroup
	wg.Add(N)
	for range N {
		go func() {
			tr.Inc(cell)
			wg.Done()
		}()
	}
	wg.Wait()

	// ensure thread safety, total score should be N
	got := tr.Score(cell)
	almostEq(t, got, N, 1e-9)
}

func TestReset_OnlySelectedCells(t *testing.T) {
	fc := &fakeClock{}
	fc.Set(time.Unix(0, 0).UTC())
	tr := newTrackerForTest(30*time.Second, fc)

	a := "cell-A"
	b := "cell-B"

	tr.Inc(a)
	tr.Inc(b)
	if tr.Score(a) <= 0 || tr.Score(b) <= 0 {
		t.Fatalf("precondition failed: scores must be > 0")
	}

	tr.Reset(a)

	if got := tr.Score(a); got != 0 {
		t.Fatalf("reset failed for %s: got %g want 0", a, got)
	}
	if got := tr.Score(b); got <= 0 {
		t.Fatalf("unexpected reset of %s: got %g want >0", b, got)
	}
}

func TestDecayHelper_Edges(t *testing.T) {
	if got := decay(0, 10, 60); got != 0 {
		t.Fatalf("expected 0, got %g", got)
	}
	if got := decay(5, 0, 60); got != 5 {
		t.Fatalf("expected 5, got %g", got)
	}
	if got := decay(5, 10, 0); got != 5 {
		t.Fatalf("expected 5, got %g", got)
	}
}
