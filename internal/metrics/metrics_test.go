package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestForRegistersOnRegisterer(t *testing.T) {
	reg := prometheus.NewRegistry()

	m := For(reg)
	if again := For(reg); again != m {
		t.Fatal("For returned different metrics for the same registerer")
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

func TestForSeparatesLabelledRegisterers(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := For(prometheus.WrapRegistererWith(prometheus.Labels{"db": "a"}, reg))
	b := For(prometheus.WrapRegistererWith(prometheus.Labels{"db": "b"}, reg))

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
