package strata_test

// TestLongRunningConsistency is a chaos-style stress test that exercises the
// full lifecycle of a strata cluster:
//
//   - Continuous background writes (~10/sec) through any alive node (followers
//     forward to the leader automatically).
//   - Periodic compaction at the current leader revision.
//   - Chaos: every ~12 s a random node is added or removed, keeping the cluster
//     between 1 and 5 nodes.  Scale-up forces new nodes to bootstrap from S3
//     (checkpoint + WAL replay).  Removing the leader forces re-election.
//   - Periodic consistency checks: every ~15 s a snapshot of the committed write
//     set is verified against every currently-alive node via Get + List.
//   - Final full consistency check once the test duration elapses.
//
// Skipped under -short.  Override duration: TEST_DURATION=5m go test -run ...
//
// Recommended invocation:
//
//	go test -run TestLongRunningConsistency -v -timeout 15m

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makhov/strata"
	"github.com/makhov/strata/pkg/object"
)

func TestLongRunningConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running stress test in short mode")
	}

	dur := 3 * time.Minute
	if s := os.Getenv("TEST_DURATION"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			dur = d
		}
	}

	store := object.NewMem()
	// Outer context has generous headroom beyond the test duration for the final
	// consistency check and node shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), dur+90*time.Second)
	defer cancel()
	testEnd := time.Now().Add(dur)

	h := &stressHarness{t: t, store: store}
	for i := 0; i < 3; i++ {
		h.addNode()
	}
	_ = waitForLeaderNode(t, h.aliveNodes(), 15*time.Second)
	t.Log("initial leader elected, starting stress loops")

	// ── Committed-write state ─────────────────────────────────────────────────
	// All mutations protected by mu; readers take RLock for snapshots.
	var mu sync.RWMutex
	committed := make(map[string]string)
	var commitRev int64

	var writeSeq atomic.Int64

	// ── Writer ────────────────────────────────────────────────────────────────
	// Writes to a random alive node; strata forwards to the leader so any node
	// is a valid write target.  Errors during leader transitions are non-fatal.
	go func() {
		for time.Now().Before(testEnd) {
			seq := writeSeq.Add(1)
			key := fmt.Sprintf("/stress/key-%08d", seq)
			val := fmt.Sprintf("v%d-%d", seq, time.Now().UnixNano())

			n := h.pickNode()
			if n == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
			rev, err := n.Put(wctx, key, []byte(val), 0)
			wcancel()
			if err != nil {
				// Expected during leader re-election; just skip this key.
				t.Logf("writer: Put key-%d: %v (skipped)", seq, err)
				time.Sleep(300 * time.Millisecond)
				continue
			}

			mu.Lock()
			committed[key] = val
			if rev > commitRev {
				commitRev = rev
			}
			mu.Unlock()

			time.Sleep(100 * time.Millisecond) // ~10 writes/sec
		}
	}()

	// ── Compaction ────────────────────────────────────────────────────────────
	// Compact at the leader's current revision every 25 s.  This is the primary
	// regression target: new nodes joining after compaction must still see all
	// pre-compaction keys.
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Now().After(testEnd) {
					return
				}
				n := h.findLeader()
				if n == nil {
					continue
				}
				rev := n.CurrentRevision()
				cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
				err := n.Compact(cctx, rev)
				ccancel()
				if err != nil {
					t.Logf("compact at rev=%d: %v (non-fatal)", rev, err)
				} else {
					t.Logf("compacted at rev=%d (nodes=%d)", rev, h.nodeCount())
				}
			}
		}
	}()

	// ── Chaos ─────────────────────────────────────────────────────────────────
	// Randomly adds or removes a node every ~12 s.  Cluster size is kept between
	// 1 and 5.  Removing the current leader is intentional — it forces a new
	// election and exercises follower-promotion.
	go func() {
		ticker := time.NewTicker(12 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Now().After(testEnd) {
					return
				}
				n := h.nodeCount()
				switch {
				case n <= 1:
					h.addNode()
					t.Logf("chaos: scale-up (total=%d)", h.nodeCount())
				case n >= 5:
					id := h.removeRandom()
					t.Logf("chaos: scale-down removed %s (total=%d)", id, h.nodeCount())
				default:
					if rand.Intn(2) == 0 {
						h.addNode()
						t.Logf("chaos: scale-up (total=%d)", h.nodeCount())
					} else {
						id := h.removeRandom()
						t.Logf("chaos: scale-down removed %s (total=%d)", id, h.nodeCount())
					}
				}
			}
		}
	}()

	// ── Periodic consistency checks ───────────────────────────────────────────
	// Every 15 s snapshot the committed write set and verify every currently-alive
	// node agrees: Get for each key, List count for the prefix.
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.RLock()
				nKeys := len(committed)
				if nKeys == 0 {
					mu.RUnlock()
					continue
				}
				snap := make(map[string]string, nKeys)
				for k, v := range committed {
					snap[k] = v
				}
				snapRev := commitRev
				mu.RUnlock()

				nodes := h.aliveNodes()
				t.Logf("periodic check: %d committed keys, rev=%d, nodes=%d",
					nKeys, snapRev, len(nodes))
				cctx, ccancel := context.WithTimeout(ctx, 45*time.Second)
				checkConsistency(t, cctx, nodes, "/stress/", snap, snapRev)
				ccancel()
			}
		}
	}()

	// ── Wait ──────────────────────────────────────────────────────────────────
	<-time.After(dur)

	// ── Final consistency check ───────────────────────────────────────────────
	mu.RLock()
	final := make(map[string]string, len(committed))
	for k, v := range committed {
		final[k] = v
	}
	finalRev := commitRev
	mu.RUnlock()

	nodes := h.aliveNodes()
	t.Logf("final check: %d committed keys, rev=%d, nodes=%d",
		len(final), finalRev, len(nodes))

	fctx, fcancel := context.WithTimeout(ctx, 60*time.Second)
	defer fcancel()
	checkConsistency(t, fctx, nodes, "/stress/", final, finalRev)

	h.closeAll()
}

// stressHarness manages a dynamic pool of cluster nodes sharing one object store.
type stressHarness struct {
	t     testing.TB
	store object.Store

	mu        sync.Mutex
	nodes     []*strata.Node
	counter   int
	closingWg sync.WaitGroup // tracks async Close goroutines from removeRandom
}

// addNode opens a fresh node and adds it to the pool.
func (h *stressHarness) addNode() *strata.Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counter++
	id := fmt.Sprintf("stress-%d", h.counter)
	n := openClusterNode(h.t, h.store, id)
	h.nodes = append(h.nodes, n)
	return n
}

// removeRandom removes a random node from the pool and closes it.
// It never removes the last node.  Returns the node's ID for logging.
func (h *stressHarness) removeRandom() string {
	h.mu.Lock()
	if len(h.nodes) <= 1 {
		h.mu.Unlock()
		return ""
	}
	i := rand.Intn(len(h.nodes))
	n := h.nodes[i]
	h.nodes = append(h.nodes[:i], h.nodes[i+1:]...)
	h.mu.Unlock()

	id := n.Config().NodeID
	// Close asynchronously so the chaos loop is not blocked by a slow shutdown.
	// Track with closingWg so closeAll() can wait before the test exits.
	h.closingWg.Add(1)
	go func() {
		defer h.closingWg.Done()
		if err := n.Close(); err != nil {
			h.t.Logf("stressHarness: close %s: %v (non-fatal)", id, err)
		}
	}()
	return id
}

func (h *stressHarness) nodeCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.nodes)
}

// aliveNodes returns a point-in-time snapshot of the current node pool.
func (h *stressHarness) aliveNodes() []*strata.Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*strata.Node, len(h.nodes))
	copy(out, h.nodes)
	return out
}

// pickNode returns a random alive node.  Strata forwards writes from followers
// to the leader, so any node is a valid write target.
func (h *stressHarness) pickNode() *strata.Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.nodes) == 0 {
		return nil
	}
	return h.nodes[rand.Intn(len(h.nodes))]
}

// findLeader returns the node that currently believes itself to be leader,
// or nil if no leader is elected at the moment.
func (h *stressHarness) findLeader() *strata.Node {
	for _, n := range h.aliveNodes() {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

// closeAll shuts down every node left in the pool.
func (h *stressHarness) closeAll() {
	h.mu.Lock()
	nodes := make([]*strata.Node, len(h.nodes))
	copy(nodes, h.nodes)
	h.nodes = nil
	h.mu.Unlock()

	for _, n := range nodes {
		if err := n.Close(); err != nil {
			h.t.Logf("stressHarness: closeAll %s: %v (non-fatal)", n.Config().NodeID, err)
		}
	}
	// Wait for any nodes that were removed by removeRandom() and are still
	// closing asynchronously. This must complete before the test function
	// returns so that t.TempDir() cleanup does not delete WAL files from
	// under an in-progress upload.
	h.closingWg.Wait()
}
