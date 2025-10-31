package observability

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	enabled   atomic.Bool
	scenarioV atomic.Value
)

func Init(r prometheus.Registerer, isEnabled bool) {
	enabled.Store(isEnabled)
	if scenarioV.Load() == nil {
		scenarioV.Store("baseline")
	}
	if !isEnabled || r == nil {
		return
	}
	initCollectors(r)
}

func Enabled() bool { return enabled.Load() }

func SetScenario(s string) {
	if s == "" {
		s = "baseline"
	}
	scenarioV.Store(s)
}

func getScenario() string {
	v := scenarioV.Load()
	if v == nil {
		return "baseline"
	}
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return "baseline"
}

var (
	httpRequestsTotal              *prometheus.CounterVec
	httpRequestDurationSeconds     *prometheus.HistogramVec
	upstreamLatencySeconds         *prometheus.HistogramVec
	decisionRequestsTotal          *prometheus.CounterVec
	spatialResponseTotal           *prometheus.CounterVec
	spatialResponseDurationSeconds *prometheus.HistogramVec
	spatialAggregationErrorsTotal  *prometheus.CounterVec
	spatialCacheHitsTotal          *prometheus.CounterVec
	spatialCacheMissesTotal        *prometheus.CounterVec
	redisOperationDurationSeconds  *prometheus.HistogramVec
	cacheOpTotal                   *prometheus.CounterVec
	spatialCacheHotKeys            *prometheus.GaugeVec
	invEvents                      *prometheus.CounterVec
	invDeletedKeys                 *prometheus.CounterVec
	invLatency                     *prometheus.HistogramVec
	kafkaConsumerErrorsTotal       *prometheus.CounterVec
)

func initCollectors(r prometheus.Registerer) {
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
		}, []string{"method", "route", "status", "scenario"},
	)
	httpRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"method", "route", "status", "scenario"},
	)
	upstreamLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "upstream_latency_seconds",
			Help:    "Latency of upstream calls in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"upstream", "scenario"},
	)
	decisionRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "decision_requests_total",
			Help: "Number of cache decisions by outcome.",
		}, []string{"outcome", "scenario"},
	)

	spatialResponseTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spatial_response_total",
			Help: "Total number of composed spatial responses by hit class and format.",
		}, []string{"hit_class", "format", "scenario"},
	)
	spatialResponseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "spatial_response_duration_seconds",
			Help:    "End-to-end latency to compose a spatial response (seconds).",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"scenario", "hit_class"},
	)
	spatialAggregationErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spatial_aggregation_errors_total",
			Help: "Count of errors in the spatial aggregation/composition pipeline by stage.",
		}, []string{"stage"},
	)

	spatialCacheHitsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spatial_cache_hits_total",
			Help: "Count of cache hits (keys found).",
		}, []string{"scenario"},
	)
	spatialCacheMissesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spatial_cache_misses_total",
			Help: "Count of cache misses (keys not found).",
		}, []string{"scenario"},
	)
	redisOperationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_operation_duration_seconds",
			Help:    "Latency of Redis operations in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms..~16s
		}, []string{"op", "scenario"},
	)
	cacheOpTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_op_total",
			Help: "Count of cache operations by op and outcome.",
		}, []string{"op", "outcome", "scenario"},
	)

	spatialCacheHotKeys = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spatial_cache_hot_keys",
			Help: "Current hot set size(s) or counts per tier.",
		}, []string{"scenario", "tier"},
	)

	invEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "invalidation_events_total",
			Help: "Number of invalidation events handled.",
		}, []string{"result", "op", "layer"},
	)
	invDeletedKeys = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "invalidation_deleted_keys_total",
			Help: "Total number of cache keys deleted by invalidation.",
		}, []string{"layer"},
	)
	invLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "invalidation_process_seconds",
			Help:    "Time to process a single invalidation event.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}, []string{"op", "layer"},
	)

	kafkaConsumerErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_errors_total",
			Help: "Errors encountered by the Kafka consumer.",
		}, []string{"scenario", "kind"},
	)

	// register
	r.MustRegister(
		httpRequestsTotal, httpRequestDurationSeconds, upstreamLatencySeconds,
		decisionRequestsTotal,
		spatialResponseTotal, spatialResponseDurationSeconds, spatialAggregationErrorsTotal,
		spatialCacheHitsTotal, spatialCacheMissesTotal, redisOperationDurationSeconds, cacheOpTotal,
		spatialCacheHotKeys,
		invEvents, invDeletedKeys, invLatency,
		kafkaConsumerErrorsTotal,
	)
}

func ExposeBuildInfo(_ string) {}

// record an http request metric
func ObserveHTTP(method, route string, status int, durationSeconds float64) {
	if !enabled.Load() || httpRequestsTotal == nil {
		return
	}
	s := getScenario()
	st := strconv.Itoa(status)
	httpRequestsTotal.WithLabelValues(method, route, st, s).Inc()
	httpRequestDurationSeconds.WithLabelValues(method, route, st, s).Observe(durationSeconds)
}

func ObserveUpstreamLatency(upstream string, durationSeconds float64) {
	if !enabled.Load() || upstreamLatencySeconds == nil {
		return
	}
	upstreamLatencySeconds.WithLabelValues(upstream, getScenario()).Observe(durationSeconds)
}

func IncDecision(outcome string) {
	if !enabled.Load() || decisionRequestsTotal == nil {
		return
	}
	if outcome != "cache" && outcome != "nocache" {
		outcome = "nocache"
	}
	decisionRequestsTotal.WithLabelValues(outcome, getScenario()).Inc()
}

func ObserveCacheOp(op string, err error, durationSeconds float64) {
	if !enabled.Load() {
		return
	}
	if op == "" {
		op = "unknown"
	}
	s := getScenario()
	outcome := "ok"
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			outcome = "timeout"
		case errors.Is(err, context.Canceled):
			outcome = "canceled"
		default:
			outcome = "error"
		}
	}
	if cacheOpTotal != nil {
		cacheOpTotal.WithLabelValues(op, outcome, s).Inc()
	}
	if redisOperationDurationSeconds != nil {
		redisOperationDurationSeconds.WithLabelValues(op, s).Observe(durationSeconds)
	}
}

func ObserveInvalidation(op, layer string, keys int, dur time.Duration, err error) {
	if !enabled.Load() || invEvents == nil {
		return
	}
	if err != nil {
		invEvents.WithLabelValues("error", op, layer).Inc()
		return
	}
	invEvents.WithLabelValues("ok", op, layer).Inc()
	invDeletedKeys.WithLabelValues(layer).Add(float64(keys))
	invLatency.WithLabelValues(op, layer).Observe(dur.Seconds())
}

func ObserveSpatialResponse(hitClass, format string, durSeconds float64) {
	if !enabled.Load() || spatialResponseTotal == nil {
		return
	}
	s := getScenario()
	spatialResponseTotal.WithLabelValues(hitClass, format, s).Inc()
	spatialResponseDurationSeconds.WithLabelValues(s, hitClass).Observe(durSeconds)
}

func IncSpatialAggError(stage string) {
	if !enabled.Load() || spatialAggregationErrorsTotal == nil {
		return
	}
	if stage == "" {
		stage = "unknown"
	}
	spatialAggregationErrorsTotal.WithLabelValues(stage).Inc()
}

func AddCacheHits(n int) {
	if !enabled.Load() || spatialCacheHitsTotal == nil || n <= 0 {
		return
	}
	spatialCacheHitsTotal.WithLabelValues(getScenario()).Add(float64(n))
}

func AddCacheMisses(n int) {
	if !enabled.Load() || spatialCacheMissesTotal == nil || n <= 0 {
		return
	}
	spatialCacheMissesTotal.WithLabelValues(getScenario()).Add(float64(n))
}

func SetHotKeysGauge(tier string, n int) {
	if !enabled.Load() || spatialCacheHotKeys == nil {
		return
	}
	if tier == "" {
		tier = "topN"
	}
	spatialCacheHotKeys.WithLabelValues(getScenario(), tier).Set(float64(n))
}

func IncKafkaConsumerError(kind string) {
	if !enabled.Load() || kafkaConsumerErrorsTotal == nil {
		return
	}
	if kind == "" {
		kind = "unknown"
	}
	kafkaConsumerErrorsTotal.WithLabelValues(getScenario(), kind).Inc()
}
