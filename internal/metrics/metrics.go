// Package metrics defines Prometheus metrics for a t4 node.
//
// For returns the metrics registered on a registerer, registering them on
// first use. Nodes that share a registerer share their metrics; a node that
// should report separately, such as one database of several served by one
// process, uses its own registerer, e.g. one wrapped with a "db" label.
package metrics

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the t4 metrics registered on one registerer.
type Metrics struct {
	// WritesTotal counts completed write operations by op type.
	WritesTotal *prometheus.CounterVec

	// WriteErrors counts write operations that returned an error.
	WriteErrors *prometheus.CounterVec

	// WriteDuration measures the latency of local write operations (WAL + apply).
	WriteDuration *prometheus.HistogramVec

	// ReadsTotal counts completed local read operations by op type
	// (get/exists/list/count). A follower's linearizable-read sync happens
	// before the local read and is not covered here.
	ReadsTotal *prometheus.CounterVec

	// ReadErrors counts read operations that returned an error.
	ReadErrors *prometheus.CounterVec

	// ReadDuration measures the latency of local read operations.
	ReadDuration *prometheus.HistogramVec

	// TxnRequestsTotal counts transactions by resolved write shape, compare
	// outcome, and final result. This separates Kubernetes create/update/delete
	// traffic that otherwise all appears as op="txn" in WritesTotal.
	TxnRequestsTotal *prometheus.CounterVec

	// TxnSubOpsTotal counts committed transaction sub-operations by resolved
	// operation type (create/update/delete).
	TxnSubOpsTotal *prometheus.CounterVec

	// TxnLockWaitDuration measures time waiting to enter the serialized write
	// preparation section.
	TxnLockWaitDuration prometheus.Histogram

	// TxnPrepareDuration measures condition evaluation and WAL preparation while
	// the transaction holds the write mutex, labelled by resolved write shape.
	TxnPrepareDuration *prometheus.HistogramVec

	// ForwardedWritesTotal counts writes forwarded from a follower to the leader.
	ForwardedWritesTotal *prometheus.CounterVec

	// ForwardDuration measures the round-trip of a forwarded write.
	ForwardDuration *prometheus.HistogramVec

	// CurrentRevision tracks the latest applied revision.
	CurrentRevision prometheus.Gauge

	// CompactRevision tracks the compaction watermark.
	CompactRevision prometheus.Gauge

	// DBSizeBytes reports the on-disk size of the local Pebble database
	// (sstables, WAL, manifest and in-progress compaction output), sampled at
	// scrape time from the source installed with SetDBSizeFunc.
	DBSizeBytes prometheus.GaugeFunc

	// Role has one labelled gauge per possible role; the active one is set to 1.
	Role *prometheus.GaugeVec

	// ReplicationDegraded is 1 while the leader cannot meet its configured
	// follower ACK target and is therefore uploading each batch to object
	// storage before acknowledging it. Sustained 1 means writes are paying an
	// S3 round trip and the cluster has lost its replication redundancy.
	ReplicationDegraded prometheus.Gauge

	// WALUploadsTotal counts WAL segments successfully uploaded to S3.
	WALUploadsTotal prometheus.Counter

	// WALUploadErrors counts failed WAL segment uploads.
	WALUploadErrors prometheus.Counter

	// WALUploadConflicts counts WAL segment uploads that were rejected because
	// the object already existed. Expected during leader handover and on
	// upload retries; a sustained non-zero rate means two writers are racing
	// on the same segment key.
	WALUploadConflicts prometheus.Counter

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

	// WatchActive tracks currently active watch subscriptions.
	WatchActive prometheus.Gauge

	// WatchActivePrefixes tracks the number of distinct prefixes with active
	// watch subscriptions. It intentionally avoids raw-prefix labels to keep
	// metric cardinality bounded under Kubernetes workloads.
	WatchActivePrefixes prometheus.Gauge

	// WatchScanDuration measures time spent processing live or replay watch batches.
	WatchScanDuration prometheus.Histogram

	// WatchScanRevisionSpan measures the revision span processed by watch delivery.
	WatchScanRevisionSpan prometheus.Histogram

	// WatchScanEntriesTotal counts records decoded once by live dispatch or
	// replay scans, plus matching deliveries to watch subscribers.
	WatchScanEntriesTotal *prometheus.CounterVec

	dbSizeFunc atomic.Pointer[func() (uint64, bool)]
}

var (
	mu    sync.Mutex
	byReg = map[prometheus.Registerer]*Metrics{}
)

// For returns the metrics registered on reg, creating and registering them
// on the first call for reg. If reg is nil, prometheus.DefaultRegisterer is
// used. reg must be comparable, as every registerer the Prometheus client
// returns is.
func For(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	mu.Lock()
	defer mu.Unlock()
	if m, ok := byReg[reg]; ok {
		return m
	}
	m := newMetrics()
	reg.MustRegister(
		m.WritesTotal,
		m.WriteErrors,
		m.WriteDuration,
		m.ReadsTotal,
		m.ReadErrors,
		m.ReadDuration,
		m.TxnRequestsTotal,
		m.TxnSubOpsTotal,
		m.TxnLockWaitDuration,
		m.TxnPrepareDuration,
		m.ForwardedWritesTotal,
		m.ForwardDuration,
		m.CurrentRevision,
		m.CompactRevision,
		m.DBSizeBytes,
		m.Role,
		m.ReplicationDegraded,
		m.WALUploadsTotal,
		m.WALUploadErrors,
		m.WALUploadConflicts,
		m.WALUploadDuration,
		m.WALGCTotal,
		m.CheckpointsTotal,
		m.ElectionsTotal,
		m.FollowerResyncsTotal,
		m.AuthAttemptsTotal,
		m.FollowerLag,
		m.ObjectStoreOpsTotal,
		m.ObjectStoreDuration,
		m.WatchActive,
		m.WatchActivePrefixes,
		m.WatchScanDuration,
		m.WatchScanRevisionSpan,
		m.WatchScanEntriesTotal,
	)
	byReg[reg] = m
	return m
}

func newMetrics() *Metrics {
	m := &Metrics{}
	m.WritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_writes_total",
		Help: "Total write operations (put/create/update/delete/compact).",
	}, []string{"op"})

	m.WriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_write_errors_total",
		Help: "Total write errors by op type.",
	}, []string{"op"})

	m.WriteDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "t4_write_duration_seconds",
		Help:    "Write operation duration (local execution, excluding forwarding).",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5},
	}, []string{"op"})

	m.ReadsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_reads_total",
		Help: "Total read operations (get/exists/list/count).",
	}, []string{"op"})

	m.ReadErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_read_errors_total",
		Help: "Total read errors by op type.",
	}, []string{"op"})

	m.ReadDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "t4_read_duration_seconds",
		Help: "Read operation duration (local execution, excluding follower sync).",
		// Up to 10s: with a top bucket of 0.5s, quantiles of slow
		// reads (large lists, reads at old revisions) read as 0.5s
		// however long they took.
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"op"})

	m.TxnRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_txn_requests_total",
		Help: "Transactions by resolved write shape, compare outcome, and result.",
	}, []string{"kind", "compare", "result"})

	m.TxnSubOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_txn_suboperations_total",
		Help: "Committed transaction sub-operations by resolved operation type.",
	}, []string{"op"})

	m.TxnLockWaitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "t4_txn_lock_wait_duration_seconds",
		Help:    "Time spent waiting for the transaction write-preparation mutex.",
		Buckets: []float64{.00001, .00005, .0001, .0005, .001, .005, .01, .05, .1, .5},
	})

	m.TxnPrepareDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "t4_txn_prepare_duration_seconds",
		Help:    "Time spent evaluating conditions and preparing a transaction under the write mutex.",
		Buckets: []float64{.00001, .00005, .0001, .0005, .001, .005, .01, .05, .1, .5},
	}, []string{"kind"})

	m.ForwardedWritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_forwarded_writes_total",
		Help: "Total writes forwarded to the leader by this follower.",
	}, []string{"op"})

	m.ForwardDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "t4_forward_duration_seconds",
		Help:    "Forwarded write round-trip duration.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"op"})

	m.CurrentRevision = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "t4_current_revision",
		Help: "Latest applied revision.",
	})

	m.CompactRevision = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "t4_compact_revision",
		Help: "Compaction watermark revision.",
	})

	m.DBSizeBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "t4_db_size_bytes",
		Help: "On-disk size of the local Pebble database in bytes.",
	}, func() float64 {
		if f := m.dbSizeFunc.Load(); f != nil {
			if size, ok := (*f)(); ok {
				return float64(size)
			}
		}
		return 0
	})

	m.Role = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "t4_role",
		Help: "Current node role; 1 = active, 0 = inactive.",
	}, []string{"role"})

	m.ReplicationDegraded = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "t4_replication_degraded",
		Help: "1 when the leader cannot meet its follower ACK target and is flushing each batch to object storage before acknowledging.",
	})

	m.WALUploadsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "t4_wal_uploads_total",
		Help: "Total WAL segments uploaded to object storage.",
	})

	m.WALUploadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "t4_wal_upload_errors_total",
		Help: "Total WAL segment upload errors.",
	})

	m.WALUploadConflicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "t4_wal_upload_conflicts_total",
		Help: "Total WAL segment uploads skipped because the object already existed.",
	})

	m.WALUploadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "t4_wal_upload_duration_seconds",
		Help:    "WAL segment upload duration.",
		Buckets: []float64{.01, .05, .1, .5, 1, 5, 10},
	})

	m.WALGCTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "t4_wal_gc_segments_total",
		Help: "Total WAL segments deleted from object storage during GC.",
	})

	m.CheckpointsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "t4_checkpoints_total",
		Help: "Total checkpoints written to object storage.",
	})

	m.ElectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_elections_total",
		Help: "Total leader election attempts by outcome (won/lost).",
	}, []string{"outcome"})

	m.FollowerResyncsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_follower_resyncs_total",
		Help: "Total follower resyncs by trigger reason.",
	}, []string{"reason"})

	m.AuthAttemptsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_auth_attempts_total",
		Help: "Total authentication attempts by result (success/fail/locked).",
	}, []string{"result"})

	m.FollowerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "t4_follower_lag_revisions",
		Help: "Number of revisions the follower is behind the leader (0 = fully caught up).",
	}, []string{"follower_id"})

	m.ObjectStoreOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_object_store_ops_total",
		Help: "Total object storage operations by op (get/put/delete/list/get_etag/put_if_absent/put_if_match) and result (success/error).",
	}, []string{"op", "result"})

	m.ObjectStoreDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "t4_object_store_duration_seconds",
		Help:    "Object storage operation latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"op"})

	m.WatchActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "t4_watch_active",
		Help: "Currently active watch subscriptions.",
	})

	m.WatchActivePrefixes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "t4_watch_active_prefixes",
		Help: "Number of distinct prefixes with active watch subscriptions.",
	})

	m.WatchScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "t4_watch_scan_duration_seconds",
		Help:    "Duration of live dispatch or replay processing for watch batches.",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
	})

	m.WatchScanRevisionSpan = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "t4_watch_scan_revision_span",
		Help:    "Number of revisions covered by each watch delivery batch.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	})

	m.WatchScanEntriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "t4_watch_scan_entries_total",
		Help: "Watch records decoded or delivered, labelled by scanned or matched.",
	}, []string{"result"})
	return m
}

// SetRole updates the role gauges so exactly one has value 1.
func (m *Metrics) SetRole(role string) {
	for _, r := range []string{"leader", "follower", "single"} {
		if r == role {
			m.Role.WithLabelValues(r).Set(1)
		} else {
			m.Role.WithLabelValues(r).Set(0)
		}
	}
}

// SetDBSizeFunc installs the source sampled by DBSizeBytes on each scrape.
// f reports ok=false when the database is unavailable (e.g. closed).
func (m *Metrics) SetDBSizeFunc(f func() (size uint64, ok bool)) {
	m.dbSizeFunc.Store(&f)
}
