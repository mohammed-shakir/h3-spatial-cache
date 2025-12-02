// Package metricswrap wraps hotness calculations with Prometheus metrics.
package metricswrap

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	xx "github.com/cespare/xxhash/v2"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/core/observability"
	"github.com/mohammed-shakir/h3-spatial-cache/internal/hotness"
	mylog "github.com/mohammed-shakir/h3-spatial-cache/internal/logger"
)

type Sizer interface{ Size() int }

type WithMetrics struct {
	inner hotness.Interface
	tier  string
}

var (
	hotThreshold = getenvFloat("HOT_THRESHOLD", 0)
	logHotSample = getenvFloat("LOG_HOTNESS_SAMPLE", 0.01)
)

func getenvFloat(k string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func New(inner hotness.Interface, tier string) *WithMetrics {
	if tier == "" {
		tier = "topN"
	}
	return &WithMetrics{inner: inner, tier: tier}
}

func (w *WithMetrics) Inc(cell string) {
	w.inner.Inc(cell)
	if hotThreshold > 0 {
		score := w.inner.Score(cell)
		if score >= hotThreshold && shouldLog(logHotSample, cell) {
			h := xx.Sum64String(cell)
			l := mylog.Build(mylog.Config{Level: "info", Component: "hotness"}, nil)
			l.Info().
				Str("event", "hotness_threshold").
				Float64("score", score).
				Str("tier", w.tier).
				Str("cell_hash", fmt.Sprintf("%08x", h)).
				Msg("hot cell above threshold")
		}
	}

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

func shouldLog(sample float64, key string) bool {
	if sample <= 0 {
		return false
	}
	if sample >= 1 {
		return true
	}
	const denom = 10000 // 0.01 => 100/10000
	threshold := uint64(sample*denom + 0.5)
	if threshold == 0 {
		return false
	}
	h := xx.Sum64String(key)
	return (h % denom) < threshold
}
