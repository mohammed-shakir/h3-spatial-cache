package simple

import (
	"github.com/mohammed-shakir/h3-spatial-cache/internal/decision"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
	h3mapper "github.com/mohammed-shakir/h3-spatial-cache/internal/mapper/h3"
)

type Engine struct {
	Hot            hotness.Interface
	Threshold      float64
	BaseRes        int
	MinRes, MaxRes int
	Mapper         *h3mapper.Mapper
}

var _ decision.Interface = (*Engine)(nil)

// returns true if any cell's current score reaches the threshold
func (e *Engine) ShouldCache(cells []string) bool {
	if len(cells) == 0 || e.Hot == nil {
		return false
	}
	for _, c := range cells {
		if e.Hot.Score(c) >= e.Threshold {
			return true
		}
	}
	return false
}

func (e *Engine) SelectResolution(_ decision.LoadStats) int {
	return e.BaseRes
}

func (e *Engine) EffectiveResolution(cells []string) int {
	if e.Mapper == nil || len(cells) == 0 {
		return e.BaseRes
	}
	base := e.BaseRes
	if e.MinRes > e.MaxRes {
		return base
	}

	// try coarser by aggregating parents at BaseRes-1.
	if base-1 >= e.MinRes {
		parentSum := make(map[string]float64, len(cells))
		for _, c := range cells {
			p, err := e.Mapper.ToParent(c, base-1)
			if err != nil {
				continue
			}
			parentSum[p] += e.Hot.Score(p)
		}
		for _, s := range parentSum {
			if s >= 2*e.Threshold {
				return base - 1
			}
		}
	}

	// try finer by sampling children at BaseRes+1.
	if base+1 <= e.MaxRes {
		seen := make(map[string]struct{})
		total, hot := 0, 0
		for _, c := range cells {
			kids, err := e.Mapper.ToChildren(c, base+1)
			if err != nil {
				continue
			}
			for _, k := range kids {
				if _, ok := seen[k]; ok {
					continue
				}
				seen[k] = struct{}{}
				total++
				if e.Hot.Score(k) >= e.Threshold {
					hot++
				}
			}
		}
		// go finer if majority of sampled children are hot
		if total > 0 && hot*2 >= total {
			return base + 1
		}
	}

	return base
}
