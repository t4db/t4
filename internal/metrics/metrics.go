// Package metrics defines Prometheus metrics for a t4 node.
//
// Call Register once during node startup to register all metrics on the
// provided registerer. When reg is nil, prometheus.DefaultRegisterer is used.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// WritesTotal counts completed write operations by op type.
	WritesTotal *prometheus.CounterVec

	// WriteErrors counts write operations that returned an error.
	WriteErrors *prometheus.CounterVec

	// WriteDuration measures the latency of local write operations (WAL + apply).
	WriteDuration *prometheus.HistogramVec

	// ForwardedWritesTotal counts writes forwarded from a follower to the leader.
	ForwardedWritesTotal *prometheus.CounterVec

	// ForwardDuration measures the round-trip of a forwarded write.
	ForwardDuration *prometheus.HistogramVec

	// CurrentRevision tracks the latest applied revision.
	CurrentRevision prometheus.Gauge

	// CompactRevision tracks the compaction watermark.
	CompactRevision prometheus.Gauge

	// Role has one labelled gauge per possible role; the active one is set to 1.
	Role *prometheus.GaugeVec

	// WALUploadsTotal counts WAL segments successfully uploaded to S3.
	WALUploadsTotal prometheus.Counter

	// WALUploadErrors counts failed WAL segment uploads.
	WALUploadErrors prometheus.Counter

	// WALUploadDuration measures WAL segment upload latency.
	WALUploadDuration prometheus.Histogram

	// WALGCTotal counts WAL segments deleted during GC.
	WALGCTotal prometheus.Counter

	// CheckpointsTotal counts successful checkpoint writes.
	CheckpointsTotal prometheus.Counter

	// ElectionsTotal counts leader election attempts by outcome.
	ElectionsTotal *prometheus.CounterVec

	// FollowerResyncsTotal counts follower restores triggered by falling out of
	// the leader's streaming window or detecting a revision gap.
	FollowerResyncsTotal *prometheus.CounterVec

	// AuthAttemptsTotal counts authentication attempts by result.
	AuthAttemptsTotal *prometheus.CounterVec

	// FollowerLag tracks how many revisions behind the leader each follower is.
	// Labelled by follower node ID. The gauge is deleted when a follower
	// disconnects so only currently-connected followers appear.
	FollowerLag *prometheus.GaugeVec

	// ObjectStoreOpsTotal counts object storage operations by op type and result.
	ObjectStoreOpsTotal *prometheus.CounterVec

	// ObjectStoreDuration measures object storage operation latency by op type.
	ObjectStoreDuration *prometheus.HistogramVec
)

var once sync.Once
var gatherer prometheus.Gatherer = prometheus.DefaultGatherer

// Register registers all t4 metrics on reg. Only the first call takes
// effect — subsequent calls are no-ops (safe for multiple Open() calls).
// If reg is nil, prometheus.DefaultRegisterer is used.
func Register(reg prometheus.Registerer) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	once.Do(func() {
		if g, ok := reg.(prometheus.Gatherer); ok {
			gatherer = g
		}

		WritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_writes_total",
			Help: "Total write operations (put/create/update/delete/compact).",
		}, []string{"op"})

		WriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_write_errors_total",
			Help: "Total write errors by op type.",
		}, []string{"op"})

		WriteDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t4_write_duration_seconds",
			Help:    "Write operation duration (local execution, excluding forwarding).",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5},
		}, []string{"op"})

		ForwardedWritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_forwarded_writes_total",
			Help: "Total writes forwarded to the leader by this follower.",
		}, []string{"op"})

		ForwardDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t4_forward_duration_seconds",
			Help:    "Forwarded write round-trip duration.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"op"})

		CurrentRevision = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_current_revision",
			Help: "Latest applied revision.",
		})

		CompactRevision = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_compact_revision",
			Help: "Compaction watermark revision.",
		})

		Role = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "t4_role",
			Help: "Current node role; 1 = active, 0 = inactive.",
		}, []string{"role"})

		WALUploadsTotal = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "t4_wal_uploads_total",
			Help: "Total WAL segments uploaded to object storage.",
		})

		WALUploadErrors = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "t4_wal_upload_errors_total",
			Help: "Total WAL segment upload errors.",
		})

		WALUploadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "t4_wal_upload_duration_seconds",
			Help:    "WAL segment upload duration.",
			Buckets: []float64{.01, .05, .1, .5, 1, 5, 10},
		})

		WALGCTotal = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "t4_wal_gc_segments_total",
			Help: "Total WAL segments deleted from object storage during GC.",
		})

		CheckpointsTotal = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "t4_checkpoints_total",
			Help: "Total checkpoints written to object storage.",
		})

		ElectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_elections_total",
			Help: "Total leader election attempts by outcome (won/lost).",
		}, []string{"outcome"})

		FollowerResyncsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_follower_resyncs_total",
			Help: "Total follower resyncs by trigger reason.",
		}, []string{"reason"})

		AuthAttemptsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_auth_attempts_total",
			Help: "Total authentication attempts by result (success/fail/locked).",
		}, []string{"result"})

		FollowerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "t4_follower_lag_revisions",
			Help: "Number of revisions the follower is behind the leader (0 = fully caught up).",
		}, []string{"follower_id"})

		ObjectStoreOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_object_store_ops_total",
			Help: "Total object storage operations by op (get/put/delete/list/get_etag/put_if_absent/put_if_match) and result (success/error).",
		}, []string{"op", "result"})

		ObjectStoreDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t4_object_store_duration_seconds",
			Help:    "Object storage operation latency.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"op"})

		reg.MustRegister(
			WritesTotal,
			WriteErrors,
			WriteDuration,
			ForwardedWritesTotal,
			ForwardDuration,
			CurrentRevision,
			CompactRevision,
			Role,
			WALUploadsTotal,
			WALUploadErrors,
			WALUploadDuration,
			WALGCTotal,
			CheckpointsTotal,
			ElectionsTotal,
			FollowerResyncsTotal,
			AuthAttemptsTotal,
			FollowerLag,
			ObjectStoreOpsTotal,
			ObjectStoreDuration,
		)
	})
}

// Gatherer returns the Prometheus gatherer associated with the singleton
// metrics registration. When the configured registerer does not implement
// prometheus.Gatherer, this falls back to prometheus.DefaultGatherer.
func Gatherer() prometheus.Gatherer {
	return gatherer
}

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
