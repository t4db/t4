package t4_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/metrics"
)

// TestReopenWithNewLabelledRegisterer reopens a node with a fresh
// registerer wrapping the same registry, as an embedder serving one
// database per label does after closing it.
func TestReopenWithNewLabelledRegisterer(t *testing.T) {
	reg := prometheus.NewRegistry()
	dir := t.TempDir()
	for range 2 {
		wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"db": "a"}, reg)
		n, err := t4.Open(t4.Config{DataDir: dir, MetricsRegisterer: wrapped})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := n.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// Nothing of the closed node stays registered, or reachable.
		if metrics.Lookup(wrapped) != nil {
			t.Fatal("a closed node's metrics are still listed")
		}
		if families, err := reg.Gather(); err != nil || len(families) != 0 {
			t.Fatalf("registry after Close: %d families, %v", len(families), err)
		}
	}
}
