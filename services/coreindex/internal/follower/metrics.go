package follower

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricCommittedHead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coreindex_committed_head_block",
		Help: "Head block of the manifest coreindex last read.",
	})
	metricServedHead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coreindex_served_head_block",
		Help: "Head block coreindex serves over HTTP and corestream.",
	})
	metricABIHead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coreindex_abi_head_block",
		Help: "Block through which the served ABI cache is committed.",
	})
	metricHeadUncertain = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coreindex_head_uncertain",
		Help: "1 while storage extends past the manifest and no writer holds the lock.",
	})
	metricCommitToServe = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "coreindex_commit_to_serve_seconds",
		Help:    "Time from a manifest commit to coreindex serving its head.",
		Buckets: []float64{.01, .025, .05, .1, .15, .2, .3, .5, 1, 2, 5},
	})
)

// RecordWriterCommit sets the head gauges for coreindex in trace ingestion, where it commits and serves the same head.
func RecordWriterCommit(head uint32) {
	metricCommittedHead.Set(float64(head))
	metricServedHead.Set(float64(head))
	metricABIHead.Set(float64(head))
	metricHeadUncertain.Set(0)
}
