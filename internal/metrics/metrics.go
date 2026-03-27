// Package metrics defines Prometheus metrics for a Strata node.
//
// All metrics are registered against the default Prometheus registry on init.
// Embedders who do not want Prometheus instrumentation can blank-import this
// package conditionally or simply ignore the HTTP server (MetricsAddr = "").
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// WritesTotal counts completed write operations by op type.
	WritesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "strata_writes_total",
		Help: "Total write operations (put/create/update/delete/compact).",
	}, []string{"op"})

	// WriteErrors counts write operations that returned an error.
	WriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "strata_write_errors_total",
		Help: "Total write errors by op type.",
	}, []string{"op"})

	// WriteDuration measures the latency of local write operations (WAL + apply).
	WriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "strata_write_duration_seconds",
		Help:    "Write operation duration (local execution, excluding forwarding).",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5},
	}, []string{"op"})

	// ForwardedWritesTotal counts writes forwarded from a follower to the leader.
	ForwardedWritesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "strata_forwarded_writes_total",
		Help: "Total writes forwarded to the leader by this follower.",
	}, []string{"op"})

	// ForwardDuration measures the round-trip of a forwarded write.
	ForwardDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "strata_forward_duration_seconds",
		Help:    "Forwarded write round-trip duration.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"op"})

	// CurrentRevision tracks the latest applied revision.
	CurrentRevision = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "strata_current_revision",
		Help: "Latest applied revision.",
	})

	// CompactRevision tracks the compaction watermark.
	CompactRevision = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "strata_compact_revision",
		Help: "Compaction watermark revision.",
	})

	// Role has one labelled gauge per possible role; the active one is set to 1.
	Role = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "strata_role",
		Help: "Current node role; 1 = active, 0 = inactive.",
	}, []string{"role"})

	// WALUploadsTotal counts WAL segments successfully uploaded to S3.
	WALUploadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "strata_wal_uploads_total",
		Help: "Total WAL segments uploaded to object storage.",
	})

	// WALUploadErrors counts failed WAL segment uploads.
	WALUploadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "strata_wal_upload_errors_total",
		Help: "Total WAL segment upload errors.",
	})

	// WALUploadDuration measures WAL segment upload latency.
	WALUploadDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "strata_wal_upload_duration_seconds",
		Help:    "WAL segment upload duration.",
		Buckets: []float64{.01, .05, .1, .5, 1, 5, 10},
	})

	// WALGCTotal counts WAL segments deleted during GC.
	WALGCTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "strata_wal_gc_segments_total",
		Help: "Total WAL segments deleted from object storage during GC.",
	})

	// CheckpointsTotal counts successful checkpoint writes.
	CheckpointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "strata_checkpoints_total",
		Help: "Total checkpoints written to object storage.",
	})

	// ElectionsTotal counts leader election attempts by outcome.
	ElectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "strata_elections_total",
		Help: "Total leader election attempts by outcome (won/lost).",
	}, []string{"outcome"})
)

// SetRole updates the role gauges so exactly one has value 1.
func SetRole(role string) {
	for _, r := range []string{"leader", "follower", "single"} {
		if r == role {
			Role.WithLabelValues(r).Set(1)
		} else {
			Role.WithLabelValues(r).Set(0)
		}
	}
}
