package simple

import (
	"testing"
	"time"

	"github.com/mohammed-shakir/h3-spatial-cache/pkg/adaptive"
)

type fakeView map[string]float64

func (f fakeView) Score(c string) float64 { return f[c] }

func TestSimpleDecider_BasicBandsAndRes(t *testing.T) {
	cfg := Config{
		Threshold: 1.0, BaseRes: 8, MinRes: 7, MaxRes: 9,
		TTLCold: 5 * time.Second, TTLWarm: 30 * time.Second, TTLHot: time.Minute,
	}
	d := New(cfg, fakeView{}, nil)

	dec, reason := d.Decide(adaptive.Query{Layer: "L", Cells: []string{"c0"}, BaseRes: 8, MinRes: 7, MaxRes: 9},
		fakeView{"c0": 0.5})
	if dec.Type != adaptive.DecisionBypass || reason != adaptive.ReasonColdAllCells {
		t.Fatalf("expected bypass cold_all_cells, got %+v, %s", dec, reason)
	}

	dec, _ = d.Decide(adaptive.Query{Layer: "L", Cells: []string{"c1"}, BaseRes: 8, MinRes: 7, MaxRes: 9},
		fakeView{"c1": 1.0})
	if dec.Type != adaptive.DecisionFill || dec.TTL != 30*time.Second {
		t.Fatalf("expected fill warm TTL, got %+v", dec)
	}

	dec, _ = d.Decide(adaptive.Query{Layer: "L", Cells: []string{"c2"}, BaseRes: 8, MinRes: 7, MaxRes: 9},
		fakeView{"c2": 4.0})
	if dec.TTL != time.Minute {
		t.Fatalf("expected hot TTL, got %+v", dec)
	}
}

func TestSimpleDecider_DeterministicGivenInputs(t *testing.T) {
	cfg := Config{Threshold: 1.0, BaseRes: 8, MinRes: 7, MaxRes: 9, TTLWarm: 30 * time.Second}
	v := fakeView{"a": 2.0, "b": 0.9}
	d1 := New(cfg, v, nil)
	d2 := New(cfg, v, nil)

	q := adaptive.Query{Layer: "x", Cells: []string{"a", "b"}, BaseRes: 8, MinRes: 7, MaxRes: 9}
	dec1, r1 := d1.Decide(q, v)
	dec2, r2 := d2.Decide(q, v)

	if dec1 != dec2 || r1 != r2 {
		t.Fatalf("decisions should be identical; got %+v/%s vs %+v/%s", dec1, r1, dec2, r2)
	}
}
