package simple

import (
	"time"

	decsimple "github.com/mohammed-shakir/h3-spatial-cache/internal/decision/simple"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
	"github.com/mohammed-shakir/h3-spatial-cache/pkg/adaptive"
)

type Config struct {
	Threshold      float64
	BaseRes        int
	MinRes, MaxRes int
	TTLCold        time.Duration
	TTLWarm        time.Duration
	TTLHot         time.Duration
	Seed           uint64
}

type SimpleDecider struct {
	cfg    Config
	engine *decsimple.Engine
}

func New(cfg Config, hv adaptive.HotnessView, mapper *h3mapper.Mapper) *SimpleDecider {
	if mapper == nil {
		mapper = h3mapper.New()
	}
	ro := &roHot{v: hv}
	eng := decsimple.New(ro, cfg.Threshold, cfg.BaseRes, cfg.MinRes, cfg.MaxRes, mapper)
	return &SimpleDecider{
		cfg:    cfg,
		engine: eng,
	}
}

func (d *SimpleDecider) Decide(q adaptive.Query, view adaptive.HotnessView) (adaptive.Decision, adaptive.Reason) {
	maxScore := 0.0
	any := false
	for _, c := range q.Cells {
		s := view.Score(c)
		if !any || s > maxScore {
			maxScore = s
		}
		any = true
	}
	if !any {
		return adaptive.Decision{Type: adaptive.DecisionBypass, Resolution: q.BaseRes}, adaptive.ReasonColdAllCells
	}

	if maxScore < d.cfg.Threshold {
		return adaptive.Decision{Type: adaptive.DecisionBypass, Resolution: q.BaseRes}, adaptive.ReasonColdAllCells
	}

	effRes := d.engine.EffectiveResolution(q.Cells)

	var ttl time.Duration
	switch {
	case maxScore >= 4*d.cfg.Threshold && d.cfg.TTLHot > 0:
		ttl = d.cfg.TTLHot
	case maxScore >= d.cfg.Threshold && d.cfg.TTLWarm > 0:
		ttl = d.cfg.TTLWarm
	default:
		ttl = d.cfg.TTLCold
	}

	reason := adaptive.ReasonDefaultFill
	switch {
	case effRes < q.BaseRes:
		reason = adaptive.ReasonCoarserParentHot
	case effRes > q.BaseRes:
		reason = adaptive.ReasonFinerKidsHot
	}

	return adaptive.Decision{
		Type:       adaptive.DecisionFill,
		Resolution: effRes,
		TTL:        ttl,
	}, reason
}

type roHot struct{ v adaptive.HotnessView }

var _ hotness.Interface = (*roHot)(nil)

func (r *roHot) Inc(string)                {}
func (r *roHot) Reset(...string)           {}
func (r *roHot) Score(cell string) float64 { return r.v.Score(cell) }

var _ interface {
	Decide(adaptive.Query, adaptive.HotnessView) (adaptive.Decision, adaptive.Reason)
} = (*SimpleDecider)(nil)
