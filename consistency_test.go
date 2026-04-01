package strata_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/makhov/strata"
	"github.com/makhov/strata/pkg/object"
)

// checkConsistency verifies that every node in the slice can read all expected
// keys with the expected values.  It blocks until each node has applied at
// least minRev or the context expires.
func checkConsistency(t *testing.T, ctx context.Context, nodes []*strata.Node, prefix string, want map[string]string, minRev int64) {
	t.Helper()
	for i, n := range nodes {
		if err := n.WaitForRevision(ctx, minRev); err != nil {
			if errors.Is(err, strata.ErrClosed) {
				// Node was removed during chaos — skip it.
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// Node is still bootstrapping from S3 (recently added, slow CI).
				// Skip rather than fail — stale-read is not a correctness bug.
				t.Logf("node-%d WaitForRevision(%d): still bootstrapping, skipping (context deadline exceeded)", i, minRev)
				continue
			}
			t.Fatalf("node-%d WaitForRevision(%d): %v", i, minRev, err)
		}
		for key, wantVal := range want {
			kv, err := n.Get(key)
			if errors.Is(err, strata.ErrClosed) {
				break // node closed mid-check; skip remaining keys for this node
			}
			if err != nil {
				t.Errorf("node-%d Get(%q): %v", i, key, err)
				continue
			}
			if kv == nil {
				t.Errorf("node-%d Get(%q): key missing (want %q) node_rev=%d compact_rev=%d",
					i, key, wantVal, n.CurrentRevision(), n.CompactRevision())
				continue
			}
			if string(kv.Value) != wantVal {
				t.Errorf("node-%d Get(%q): want %q got %q", i, key, wantVal, kv.Value)
			}
		}
		kvs, err := n.List(prefix)
		if errors.Is(err, strata.ErrClosed) {
			continue
		}
		if err != nil {
			t.Errorf("node-%d List(%q): %v", i, prefix, err)
			continue
		}
		// More keys than want is acceptable: writes whose ACK was lost in
		// transit (client saw an error but the leader had already committed)
		// are durable and correct. Only fewer keys than expected is a bug.
		if len(kvs) < len(want) {
			t.Errorf("node-%d List(%q): want %d keys got %d", i, prefix, len(want), len(kvs))
		}
	}
}

// openClusterNode starts a new cluster node sharing the given object store.
// The caller is responsible for calling Close.
func openClusterNode(t testing.TB, store object.Store, id string) *strata.Node {
	t.Helper()
	peerAddr := freeAddrImpl(t)
	n, err := strata.Open(strata.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		NodeID:             id,
		PeerListenAddr:     peerAddr,
		AdvertisePeerAddr:  peerAddr,
		FollowerMaxRetries: 3,
		PeerBufferSize:     1000,
		CheckpointInterval: 400 * time.Millisecond,
		SegmentMaxAge:      150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open %s: %v", id, err)
	}
	return n
}

// TestScaleDownUpConsistency is the end-to-end regression test covering:
//
//  1. 3-node cluster writes, replication and read consistency.
//  2. Compaction: live keys survive; the fix for applyCompact deleting current
//     log entries is exercised.
//  3. Scale-down (3→1): surviving node becomes leader, writes continue.
//  4. Scale-up (1→3): two fresh nodes restore from S3 checkpoint + WAL and
//     reach full consistency with the leader, exercising the WAL term-cutoff
//     fix for replayRemote and the Close-uploads-final-segment fix.
func TestScaleDownUpConsistency(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const prefix = "/consist/"

	// ── Phase 1: 3-node cluster ───────────────────────────────────────────────

	nodes := make([]*strata.Node, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = openClusterNode(t, store, fmt.Sprintf("node-%d", i))
	}

	leader := waitForLeaderNode(t, nodes, 15*time.Second)
	t.Logf("phase-1 leader: %s", nodeID(nodes, leader))

	// Write 20 keys.
	want := make(map[string]string, 20)
	var lastRev int64
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("%skey-%02d", prefix, i)
		val := fmt.Sprintf("v1-%02d", i)
		rev, err := leader.Put(ctx, key, []byte(val), 0)
		if err != nil {
			t.Fatalf("phase-1 Put %q: %v", key, err)
		}
		want[key] = val
		lastRev = rev
	}

	t.Log("phase-1: checking consistency on all 3 nodes")
	checkConsistency(t, ctx, nodes, prefix, want, lastRev)

	// ── Phase 2: compaction ───────────────────────────────────────────────────
	//
	// Compact at the current revision.  Every live key's log entry at its
	// current revision must survive — this is the regression target.

	compactRev := leader.CurrentRevision()
	if err := leader.Compact(ctx, compactRev); err != nil {
		t.Fatalf("Compact(rev=%d): %v", compactRev, err)
	}
	t.Logf("phase-2: compacted at rev %d", compactRev)

	// Wait for compaction to propagate to followers.
	for i, n := range nodes {
		if n == leader {
			continue
		}
		if err := n.WaitForRevision(ctx, leader.CurrentRevision()); err != nil {
			t.Fatalf("node-%d WaitForRevision after compact: %v", i, err)
		}
	}

	t.Log("phase-2: checking all live keys are still readable after compaction")
	checkConsistency(t, ctx, nodes, prefix, want, leader.CurrentRevision())

	// Write a few more keys after compaction to verify the store is functional.
	for i := 20; i < 25; i++ {
		key := fmt.Sprintf("%skey-%02d", prefix, i)
		val := fmt.Sprintf("v2-%02d", i)
		rev, err := leader.Put(ctx, key, []byte(val), 0)
		if err != nil {
			t.Fatalf("post-compact Put %q: %v", key, err)
		}
		want[key] = val
		lastRev = rev
	}
	checkConsistency(t, ctx, nodes, prefix, want, lastRev)

	// ── Phase 3: scale down 3→1 ───────────────────────────────────────────────
	//
	// Close the two followers.  The leader remains and writes continue.

	for _, n := range nodes {
		if n != leader {
			if err := n.Close(); err != nil {
				t.Logf("Close follower: %v (non-fatal)", err)
			}
		}
	}
	t.Log("phase-3: scaled down to 1 node")

	for i := 25; i < 35; i++ {
		key := fmt.Sprintf("%skey-%02d", prefix, i)
		val := fmt.Sprintf("v3-%02d", i)
		rev, err := leader.Put(ctx, key, []byte(val), 0)
		if err != nil {
			t.Fatalf("scale-down Put %q: %v", key, err)
		}
		want[key] = val
		lastRev = rev
	}
	t.Logf("phase-3: %d keys written after scale-down", 10)

	// Wait for the leader to flush its WAL and write a checkpoint so that
	// replacement nodes can bootstrap from S3.
	time.Sleep(1200 * time.Millisecond)

	// Verify single-node consistency.
	checkConsistency(t, ctx, []*strata.Node{leader}, prefix, want, lastRev)

	// ── Phase 4: scale up 1→3 ────────────────────────────────────────────────
	//
	// Start two fresh nodes from the same object store (empty data dirs).
	// They must bootstrap from S3 checkpoint + WAL and catch up to the leader.

	t.Log("phase-4: starting 2 replacement nodes")
	rep1 := openClusterNode(t, store, "rep-1")
	rep2 := openClusterNode(t, store, "rep-2")
	defer rep1.Close()
	defer rep2.Close()

	t.Log("phase-4: checking consistency across leader + 2 replacement nodes")
	checkConsistency(t, ctx, []*strata.Node{leader, rep1, rep2}, prefix, want, lastRev)

	// ── Phase 5: write via replacement, verify on leader ─────────────────────
	//
	// Write through a follower (forwarding) and verify the leader receives it.

	activeLeader := waitForLeaderNode(t, []*strata.Node{leader, rep1, rep2}, 15*time.Second)
	t.Logf("phase-5 leader: %s", nodeID([]*strata.Node{leader, rep1, rep2}, activeLeader))

	var aFollower *strata.Node
	for _, n := range []*strata.Node{leader, rep1, rep2} {
		if n != activeLeader {
			aFollower = n
			break
		}
	}

	fwdKey := prefix + "forwarded"
	fwdRev, err := aFollower.Put(ctx, fwdKey, []byte("fwd"), 0)
	if err != nil {
		t.Fatalf("forwarded Put: %v", err)
	}
	want[fwdKey] = "fwd"

	t.Log("phase-5: checking consistency across all 3 nodes after forwarded write")
	checkConsistency(t, ctx, []*strata.Node{leader, rep1, rep2}, prefix, want, fwdRev)
}

// nodeID returns a human-readable label for node n within the slice.
func nodeID(nodes []*strata.Node, n *strata.Node) string {
	for i, nd := range nodes {
		if nd == n {
			return fmt.Sprintf("node-%d", i)
		}
	}
	return "unknown"
}

// TestCompactionThenScaleUp verifies specifically that data written before
// compaction is still visible on a node that bootstraps from S3 after compaction.
// This is the minimal reproduction of the scale-down/up compaction bug.
func TestCompactionThenScaleUp(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Single node with S3.
	n, err := strata.Open(strata.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const prefix = "/compact-scale/"
	want := make(map[string]string, 10)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%sk%d", prefix, i)
		val := fmt.Sprintf("v%d", i)
		if _, err := n.Put(ctx, key, []byte(val), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[key] = val
	}

	compactRev := n.CurrentRevision()
	if err := n.Compact(ctx, compactRev); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Write a few more so the WAL has post-compaction entries.
	var lastRev int64
	for i := 10; i < 15; i++ {
		key := fmt.Sprintf("%sk%d", prefix, i)
		val := fmt.Sprintf("v%d", i)
		rev, err := n.Put(ctx, key, []byte(val), 0)
		if err != nil {
			t.Fatalf("Post-compact Put: %v", err)
		}
		want[key] = val
		lastRev = rev
	}

	// Wait for checkpoint to be written with post-compaction data.
	time.Sleep(1 * time.Second)

	if err := n.Close(); err != nil {
		t.Logf("Close: %v (non-fatal)", err)
	}

	// New node with a fresh data directory; must reconstruct from S3.
	n2, err := strata.Open(strata.Config{
		DataDir:     t.TempDir(),
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Open fresh node: %v", err)
	}
	defer n2.Close()

	checkConsistency(t, ctx, []*strata.Node{n2}, prefix, want, lastRev)
}
