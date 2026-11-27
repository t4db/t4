package t4

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/t4db/t4/internal/metrics"
)

// TestReplicationDegradedGaugeDoesNotWaitForAWrite pins that
// t4_replication_degraded reports the condition and not the writes. It used to
// be set only inside the commit loop, which runs when a batch arrives, so an
// idle leader that had lost every follower kept publishing 0 — silent for as
// long as nothing was written, which is the state an alert is wanted for.
//
// Durability never depended on the gauge: the next write re-evaluates before
// acknowledging. This is about what an operator can see.
func TestReplicationDegradedGaugeDoesNotWaitForAWrite(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	leaderIdx := cluster.indexOf(leader)

	// A leader starts with no followers connected, so wait for the state the
	// test is actually about: a leader whose followers have arrived, reported
	// as healthy.
	waitGauge(t, 0, 15*time.Second, "a leader whose followers have connected")

	// Cut the followers off and write nothing at all from here on.
	cluster.proxies[leaderIdx].block()
	deadline := time.Now().Add(10 * time.Second)
	for !leader.replicationDegraded() {
		if time.Now().After(deadline) {
			t.Fatal("leader did not notice its followers were gone")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The leader knows. The gauge has to say so without a write prompting it;
	// the loop that publishes it wakes every leaderCheckInterval.
	waitGauge(t, 1, 5*time.Second, "an idle leader that lost every follower")

	// And back to 0 once they return, still without a write.
	cluster.proxies[leaderIdx].unblock()
	waitGauge(t, 0, 20*time.Second, "an idle leader whose followers came back")
}

// waitGauge waits for the degraded gauge to reach want, and says which state
// it was waiting for if it does not.
func waitGauge(t *testing.T, want float64, timeout time.Duration, state string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := testutil.ToFloat64(metrics.ReplicationDegraded)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: degraded gauge = %v, want %v", state, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
