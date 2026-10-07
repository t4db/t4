package t4_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/t4db/t4/internal/metrics"
)

// TestNodeDBSizeMetric proves t4_db_size_bytes reports the open node's Pebble
// size and stops touching the store once the node is closed.
func TestNodeDBSizeMetric(t *testing.T) {
	n := openNode(t)

	if _, err := n.Put(ctx(t), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := testutil.ToFloat64(metrics.DBSizeBytes); got <= 0 {
		t.Fatalf("t4_db_size_bytes on open node: want > 0, got %v", got)
	}

	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := testutil.ToFloat64(metrics.DBSizeBytes); got != 0 {
		t.Fatalf("t4_db_size_bytes on closed node: want 0, got %v", got)
	}
}
