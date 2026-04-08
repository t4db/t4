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
}
