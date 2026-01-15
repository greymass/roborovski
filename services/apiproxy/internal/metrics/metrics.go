package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apiproxy_requests_total",
			Help: "Total number of requests",
		},
		[]string{"method", "path", "status"},
	)

	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "apiproxy_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	RateLimitExceeded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apiproxy_rate_limit_exceeded_total",
			Help: "Total number of rate limit exceeded errors",
		},
		[]string{"ip"},
	)

	CircuitBreakerOpen = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "apiproxy_circuit_breaker_open",
			Help: "Circuit breaker state (1 = open, 0 = closed)",
		},
		[]string{"backend"},
	)

	CacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "apiproxy_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	CacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "apiproxy_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	ValidationRejected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apiproxy_validation_rejected_total",
			Help: "Total number of requests rejected by validation",
		},
		[]string{"path", "reason"},
	)
)
