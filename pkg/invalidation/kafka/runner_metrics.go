package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

type metricSet struct {
	msgs     *prometheus.CounterVec
	apply    *prometheus.CounterVec
	proc     *prometheus.HistogramVec
	lagGauge prometheus.Gauge
}

func newMetricSet(r prometheus.Registerer) *metricSet {
	m := &metricSet{
		msgs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "inval_msgs_total",
				Help: "Count of invalidation messages by result.",
			},
			[]string{"result"},
		),
		apply: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "inval_apply_total",
				Help: "Actions taken during invalidation.",
			},
			[]string{"action"},
		),
		proc: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "inval_processing_seconds",
				Help:    "End-to-end processing time for one message.",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
			},
			[]string{"op"},
		),
		lagGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "inval_lag_seconds",
				Help: "Approximate lag: now - message.timestamp.",
			},
		),
	}
	if r != nil {
		r.MustRegister(m.msgs, m.apply, m.proc, m.lagGauge)
	}
	return m
}
