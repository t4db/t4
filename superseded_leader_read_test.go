package t4

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSupersededLeaderServesNoStaleRead isolates a leader from object storage
// and from its followers at once, which is the only way two nodes come to
// believe they lead: the old one cannot renew its lock, and the followers stop
// hearing it and elect one of their own. The old one is still reachable by
// clients throughout.
//
// Once the new leader has acknowledged a write, a linearizable read from the
// old leader must not answer out of its own state, which no longer holds that
// write. It has to refuse, so the client asks someone who knows.
//
// The robustness suite reaches this shape only by chance and its checker often
// cannot settle the resulting history; this asserts the property directly.
func TestSupersededLeaderServesNoStaleRead(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	old := cluster.leader(t, 15*time.Second)
	oldIdx := cluster.indexOf(old)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := old.Put(ctx, "/k", []byte("before"), 0); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	for _, n := range cluster.survivors(old) {
		if err := n.WaitForRevision(ctx, old.CurrentRevision()); err != nil {
			t.Fatalf("follower did not catch up: %v", err)
		}
	}

	// Cut it off from the lock and from its followers together.
	cluster.stores[oldIdx].block()
	cluster.proxies[oldIdx].block()

	// One of the followers takes over.
	var fresh *Node
	deadline := time.Now().Add(60 * time.Second)
	for fresh == nil {
		if time.Now().After(deadline) {
			t.Fatal("no follower took over while the leader was isolated")
		}
		for _, n := range cluster.survivors(old) {
			if n.IsLeader() {
				fresh = n
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("takeover: %s now leads", fresh.cfg.NodeID)

	// The new leader accepts a write the old one cannot know about.
	if _, err := fresh.Put(ctx, "/k", []byte("after"), 0); err != nil {
		t.Fatalf("write to the new leader: %v", err)
	}

	// The old leader must not answer "before" as the current value.
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()
	kv, err := old.LinearizableGet(readCtx, "/k")
	switch {
	case err == nil && kv != nil && string(kv.Value) == "before":
		t.Fatalf("superseded leader served the stale value %q as current; "+
			"the new leader had already acknowledged %q", kv.Value, "after")
	case err == nil:
		t.Fatalf("superseded leader served a read at all: value=%q", kv.Value)
	case errors.Is(err, ErrNoLeader), errors.Is(err, ErrClosed),
		errors.Is(err, context.DeadlineExceeded):
		// Refused, or could not confirm leadership. Either is correct.
		t.Logf("superseded leader refused the read: %v", err)
	default:
		// Also a refusal, and worth seeing: an unexpected error here is
		// still correct behavior but says the path changed.
		t.Logf("superseded leader refused the read with an unexpected error: %v", err)
	}
}
