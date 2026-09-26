package replicate

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the replicator's Prometheus metrics.
type Metrics struct {
	SourceRevision  prometheus.Gauge
	AppliedRevision prometheus.Gauge
	LagRevisions    prometheus.Gauge
	AppliedTotal    prometheus.Counter
	ApplyDuration   prometheus.Histogram
	TargetLeases    prometheus.Gauge
	Errors          *prometheus.CounterVec
	Halted          prometheus.Gauge
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		SourceRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_replicate_source_revision",
			Help: "Latest source revision the replicator has observed.",
		}),
		AppliedRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_replicate_applied_revision",
			Help: "Latest source revision applied to the target; the target is at exactly this revision.",
		}),
		LagRevisions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_replicate_lag_revisions",
			Help: "Source revisions observed but not yet applied to the target.",
		}),
		AppliedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "t4_replicate_applied_revisions_total",
			Help: "Source revisions applied to the target.",
		}),
		ApplyDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "t4_replicate_apply_duration_seconds",
			Help:    "Time to apply one source revision to the target.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
		}),
		TargetLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_replicate_target_leases",
			Help: "Leases the replicator mirrors on the target.",
		}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "t4_replicate_errors_total",
			Help: "Retried errors by operation.",
		}, []string{"op"}),
		Halted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "t4_replicate_halted",
			Help: "1 when replication stopped on an error that needs an operator, such as a diverged target.",
		}),
	}
	reg.MustRegister(m.SourceRevision, m.AppliedRevision, m.LagRevisions, m.AppliedTotal,
		m.ApplyDuration, m.TargetLeases, m.Errors, m.Halted)
	return m
}
