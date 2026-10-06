package etcd_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/t4db/t4/internal/metrics"
)

// TestLeaseLoopNotCountedAsClientReads pins that the leader's lease-expiry
// loop, which lists lease records every second, is not counted as client
// list traffic: an idle server must report no list reads.
func TestLeaseLoopNotCountedAsClientReads(t *testing.T) {
	newServer(t) // starts the lease loop on a single-node leader
	before := testutil.ToFloat64(metrics.ReadsTotal.WithLabelValues("list"))
	time.Sleep(2500 * time.Millisecond) // at least two ticks
	if got := testutil.ToFloat64(metrics.ReadsTotal.WithLabelValues("list")) - before; got != 0 {
		t.Errorf("idle server counted %v list reads", got)
	}
}
