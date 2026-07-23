package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterSetsGatherer(t *testing.T) {
	reg := prometheus.NewRegistry()

	Register(reg)

	if got := Gatherer(); got != reg {
		t.Fatalf("Gatherer() = %T, want %T", got, reg)
	}

	TxnRequestsTotal.WithLabelValues("delete", "success", "committed").Inc()
	TxnSubOpsTotal.WithLabelValues("delete").Inc()
	TxnPrepareDuration.WithLabelValues("delete").Observe(0.001)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]bool{
		"t4_txn_requests_total":             false,
		"t4_txn_suboperations_total":        false,
		"t4_txn_lock_wait_duration_seconds": false,
		"t4_txn_prepare_duration_seconds":   false,
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
