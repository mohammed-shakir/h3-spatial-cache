package observability

import (
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var scenarioLabel atomic.Value

func init() {
	scenarioLabel.Store("baseline")
}

func SetScenario(s string) {
	if s == "" {
		s = "baseline"
	}
	scenarioLabel.Store(s)
}

func getScenario() string {
	if v := scenarioLabel.Load(); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return "baseline"
}

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
		},
		[]string{"method", "route", "status", "scenario"},
	)

	httpRequestDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms to ~20s
		},
		[]string{"method", "route", "status", "scenario"},
	)

	upstreamLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "upstream_latency_seconds",
			Help:    "Latency of upstream calls in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		},
		[]string{"upstream", "scenario"},
	)

	buildInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "app_build_info",
			Help:        "Build information for the binary.",
			ConstLabels: nil,
		},
		[]string{"version"},
	)

	cacheResults = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_results_total",
			Help: "Cache results by outcome.",
		},
		[]string{"outcome", "scenario"},
	)
)

func ObserveHTTP(method, route string, status int, durationSeconds float64) {
	s := getScenario()
	st := strconv.Itoa(status)
	httpRequestsTotal.WithLabelValues(method, route, st, s).Inc()
	httpRequestDurationSeconds.WithLabelValues(method, route, st, s).Observe(durationSeconds)
}

func ObserveUpstreamLatency(upstream string, durationSeconds float64) {
	s := getScenario()
	upstreamLatencySeconds.WithLabelValues(upstream, s).Observe(durationSeconds)
}

func IncCacheHit(scenario string) {
	s := scenario
	if s == "" {
		s = getScenario()
	}
	cacheResults.WithLabelValues("hit", s).Inc()
}

func IncCacheMiss(scenario string) {
	s := scenario
	if s == "" {
		s = getScenario()
	}
	cacheResults.WithLabelValues("miss", s).Inc()
}

func ExposeBuildInfo(version string) {
	if version == "" {
		version = "dev"
	}
	buildInfo.WithLabelValues(version).Set(1)
}
