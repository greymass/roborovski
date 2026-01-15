package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ConnectionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamproxy_connections_active",
			Help: "Current active connections",
		},
		[]string{"chain", "protocol"},
	)

	ConnectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamproxy_connections_total",
			Help: "Total connections established",
		},
		[]string{"chain", "protocol"},
	)

	ConnectionsRejected = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "streamproxy_connections_rejected_total",
			Help: "Total connections rejected (max connections)",
		},
	)

	BytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamproxy_bytes_total",
			Help: "Total bytes transferred",
		},
		[]string{"chain", "direction"},
	)

	BackendHealth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "streamproxy_backend_healthy",
			Help: "Backend health status (1=healthy, 0=unhealthy)",
		},
		[]string{"backend"},
	)

	BackendErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamproxy_backend_errors_total",
			Help: "Total backend connection errors",
		},
		[]string{"backend"},
	)

	ConnectionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "streamproxy_connection_duration_seconds",
			Help:    "Connection duration in seconds",
			Buckets: []float64{60, 300, 900, 3600, 14400, 86400},
		},
		[]string{"chain", "protocol"},
	)
)

func UpdateBackendHealth(name string, healthy bool) {
	val := 0.0
	if healthy {
		val = 1.0
	}
	BackendHealth.WithLabelValues(name).Set(val)
}
