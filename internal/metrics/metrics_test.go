package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func mustRegister(t *testing.T, reg prometheus.Registerer) *Metrics {
	t.Helper()
	m, err := Register(reg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRegisterSharesWhileOpen(t *testing.T) {
	reg := prometheus.NewRegistry()

	m := mustRegister(t, reg)
	defer m.Release()
	again := mustRegister(t, reg)
	if again != m {
		t.Fatal("Register returned different metrics for the same registerer")
	}
	again.Release()
	if Lookup(reg) != m {
		t.Fatal("metrics left the registerer while a node still uses them")
	}

	m.TxnRequestsTotal.WithLabelValues("delete", "success", "committed").Inc()
	m.TxnSubOpsTotal.WithLabelValues("delete").Inc()
	m.TxnPrepareDuration.WithLabelValues("delete").Observe(0.001)
	m.ReadsTotal.WithLabelValues("get").Inc()
	m.ReadErrors.WithLabelValues("get").Inc()
	m.ReadDuration.WithLabelValues("get").Observe(0.0005)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]bool{
		"t4_txn_requests_total":             false,
		"t4_txn_suboperations_total":        false,
		"t4_txn_lock_wait_duration_seconds": false,
		"t4_txn_prepare_duration_seconds":   false,
		"t4_reads_total":                    false,
		"t4_read_errors_total":              false,
		"t4_read_duration_seconds":          false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %q was not registered", name)
		}
	}
}

func TestRegisterSeparatesLabelledRegisterers(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := mustRegister(t, prometheus.WrapRegistererWith(prometheus.Labels{"db": "a"}, reg))
	defer a.Release()
	b := mustRegister(t, prometheus.WrapRegistererWith(prometheus.Labels{"db": "b"}, reg))
	defer b.Release()

	a.CurrentRevision.Set(7)
	b.CurrentRevision.Set(9)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "t4_current_revision" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "db" {
					got[label.GetValue()] = metric.GetGauge().GetValue()
				}
			}
		}
	}
	if got["a"] != 7 || got["b"] != 9 {
		t.Fatalf("t4_current_revision by db = %v, want a=7 b=9", got)
	}
}

// TestReleaseUnregisters: once the last node releases its metrics they leave
// the registry, so a node opened again through a new registerer wrapping the
// same registry registers anew, and nothing keeps the closed node alive.
func TestReleaseUnregisters(t *testing.T) {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"db": "a"}
	first := prometheus.WrapRegistererWith(labels, reg)
	m := mustRegister(t, first)
	m.Release()
	if Lookup(first) != nil {
		t.Fatal("released metrics are still listed")
	}
	if families, err := reg.Gather(); err != nil || len(families) != 0 {
		t.Fatalf("registry after release: %d families, %v", len(families), err)
	}
	mustRegister(t, prometheus.WrapRegistererWith(labels, reg)).Release()
}

// TestRegisterConflictIsAnError: metrics that collide with ones already on
// the registry are an error, not a panic.
func TestRegisterConflictIsAnError(t *testing.T) {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"db": "a"}
	m := mustRegister(t, prometheus.WrapRegistererWith(labels, reg))
	defer m.Release()
	if _, err := Register(prometheus.WrapRegistererWith(labels, reg)); err == nil {
		t.Fatal("registering the same metrics twice succeeded")
	}
	// The failed attempt left the first registration intact.
	m.CurrentRevision.Set(3)
	if families, err := reg.Gather(); err != nil || len(families) == 0 {
		t.Fatalf("registry after the failed attempt: %d families, %v", len(families), err)
	}
}

// valueRegisterer is a registerer that cannot be a map key.
type valueRegisterer struct {
	reg  *prometheus.Registry
	tags []string
}

func (v valueRegisterer) Register(c prometheus.Collector) error   { return v.reg.Register(c) }
func (v valueRegisterer) MustRegister(cs ...prometheus.Collector) { v.reg.MustRegister(cs...) }
func (v valueRegisterer) Unregister(c prometheus.Collector) bool  { return v.reg.Unregister(c) }

func TestRegisterAcceptsUnhashableRegisterer(t *testing.T) {
	v := valueRegisterer{reg: prometheus.NewRegistry(), tags: []string{"x"}}
	m := mustRegister(t, v)
	if Lookup(v) != nil {
		t.Fatal("a registerer that is not a pointer is shared")
	}
	m.Release()
	if families, err := v.reg.Gather(); err != nil || len(families) != 0 {
		t.Fatalf("registry after release: %d families, %v", len(families), err)
	}
}
