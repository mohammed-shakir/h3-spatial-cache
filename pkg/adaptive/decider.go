// Package adaptive provides adaptive decider implementations for cache strategies.
package adaptive

import "time"

type HotnessView interface {
	Score(cell string) float64
}

type Query struct {
	Layer   string
	Cells   []string
	BaseRes int
	MinRes  int
	MaxRes  int
}

type DecisionType int

const (
	DecisionBypass DecisionType = iota
	DecisionFill
	DecisionServeOnlyIfFresh
)

type Reason string

const (
	ReasonColdAllCells     Reason = "cold_all_cells"
	ReasonDefaultFill      Reason = "default_fill"
	ReasonCoarserParentHot Reason = "coarser_parent_hot"
	ReasonFinerKidsHot     Reason = "finer_children_hot"
)

type Decision struct {
	Type       DecisionType
	Resolution int
	TTL        time.Duration
}

type Decider interface {
	Decide(q Query, metrics HotnessView) (Decision, Reason)
}
