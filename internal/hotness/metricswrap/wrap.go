package metricswrap

import (
	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
)

type Sizer interface{ Size() int }

type WithMetrics struct {
	inner hotness.Interface
	tier  string
}

func New(inner hotness.Interface, tier string) *WithMetrics {
	if tier == "" {
		tier = "topN"
	}
	return &WithMetrics{inner: inner, tier: tier}
}

func (w *WithMetrics) Inc(cell string) {
	w.inner.Inc(cell)
	if s, ok := w.inner.(Sizer); ok {
		observability.SetHotKeysGauge(w.tier, s.Size())
	}
}

func (w *WithMetrics) Score(cell string) float64 {
	return w.inner.Score(cell)
}

func (w *WithMetrics) Reset(cells ...string) {
	w.inner.Reset(cells...)
	if s, ok := w.inner.(Sizer); ok {
		observability.SetHotKeysGauge(w.tier, s.Size())
	}
}
