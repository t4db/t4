package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// Watch failover coverage. A watch opened by a client before a leader change
// must remain serviceable across the transition: when the client reconnects on
// the new leader with the last revision it observed, every event the old
// leader committed past that revision must be delivered exactly once, and
// post-failover events must follow seamlessly.
//
// Subtests:
//
//	Graceful_NoGap   leader.Close() after the client drained everything.
//	Graceful_WithGap leader.Close() while events K+1..K+M are committed but
//	                 undelivered to the client (slow consumer).
//	Abrupt_NoGap     leader becomes isolated from S3+peers (simulating crash)
//	                 with no event gap.
//	Abrupt_WithGap   same isolation + event gap.

// ── gatedStore ────────────────────────────────────────────────────────────────

// gatedStore wraps an object.Store and can deny all operations atomically.
// Each cluster node opens with its own gatedStore over a shared *object.Mem,
// so a single node can be "crashed" (cut off from S3) without affecting the
// others.
type gatedStore struct {
	inner   object.ConditionalStore
	blocked atomic.Bool
}

func newGatedStore(inner object.ConditionalStore) *gatedStore { return &gatedStore{inner: inner} }

func (g *gatedStore) block() { g.blocked.Store(true) }
func (g *gatedStore) unblock() { g.blocked.Store(false) }

var errStoreBlocked = errors.New("gated store: blocked")

func (g *gatedStore) Put(ctx context.Context, key string, r io.Reader) error {
	if g.blocked.Load() {
		return errStoreBlocked
	}
	return g.inner.Put(ctx, key, r)
}
func (g *gatedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if g.blocked.Load() {
		return nil, errStoreBlocked
	}
	return g.inner.Get(ctx, key)
}
func (g *gatedStore) Delete(ctx context.Context, key string) error {
	if g.blocked.Load() {
		return errStoreBlocked
	}
	return g.inner.Delete(ctx, key)
}
func (g *gatedStore) DeleteMany(ctx context.Context, keys []string) error {
	if g.blocked.Load() {
		return errStoreBlocked
	}
	return g.inner.DeleteMany(ctx, keys)
}
func (g *gatedStore) List(ctx context.Context, prefix string) ([]string, error) {
	if g.blocked.Load() {
		return nil, errStoreBlocked
	}
	return g.inner.List(ctx, prefix)
}
func (g *gatedStore) GetETag(ctx context.Context, key string) (*object.GetWithETag, error) {
	if g.blocked.Load() {
		return nil, errStoreBlocked
	}
	return g.inner.GetETag(ctx, key)
}
func (g *gatedStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	if g.blocked.Load() {
		return errStoreBlocked
	}
	return g.inner.PutIfAbsent(ctx, key, r)
}
func (g *gatedStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	if g.blocked.Load() {
		return errStoreBlocked
	}
	return g.inner.PutIfMatch(ctx, key, r, matchETag)
}

// ── cluster builder ───────────────────────────────────────────────────────────

type failoverCluster struct {
	nodes   []*Node
	stores  []*gatedStore
	proxies []*blockableProxyLocal
	shared  object.ConditionalStore
}

func newFailoverCluster(t *testing.T, size int) *failoverCluster {
	t.Helper()
	shared := object.NewMem()
	c := &failoverCluster{
		shared:  shared,
		nodes:   make([]*Node, size),
		stores:  make([]*gatedStore, size),
		proxies: make([]*blockableProxyLocal, size),
	}
	for i := 0; i < size; i++ {
		listenAddr := freeAddrLocal(t)
		proxy := newBlockableProxyLocal(t, listenAddr)
		gs := newGatedStore(shared)
		n, err := Open(Config{
			DataDir:            t.TempDir(),
			ObjectStore:        gs,
			NodeID:             fmt.Sprintf("failover-node-%d", i),
			PeerListenAddr:     listenAddr,
			AdvertisePeerAddr:  proxy.Addr(),
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 300 * time.Millisecond,
			SegmentMaxAge:      200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open node-%d: %v", i, err)
		}
		c.nodes[i] = n
		c.stores[i] = gs
		c.proxies[i] = proxy
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n != nil {
				_ = n.Close()
			}
		}
	})
	return c
}

func (c *failoverCluster) leader(t *testing.T, timeout time.Duration) *Node {
	t.Helper()
	return waitForLeaderNodeLocal(t, c.nodes, timeout)
}

func (c *failoverCluster) survivors(dead *Node) []*Node {
	out := make([]*Node, 0, len(c.nodes)-1)
	for _, n := range c.nodes {
		if n != dead {
			out = append(out, n)
		}
	}
	return out
}

func (c *failoverCluster) indexOf(n *Node) int {
	for i, x := range c.nodes {
		if x == n {
			return i
		}
	}
	return -1
}

// ── drainN ────────────────────────────────────────────────────────────────────

// drainN reads up to n events from ch, returning them. Stops early on timeout
// or if ch closes.
func drainN(ch <-chan Event, n int, timeout time.Duration) []Event {
	out := make([]Event, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
	return out
}

// ── test ──────────────────────────────────────────────────────────────────────

func TestWatchSurvivesLeaderFailover(t *testing.T) {
	cases := []struct {
		name      string
		gap       bool // if true, leave a gap of undelivered events before failover
		abrupt    bool // if true, kill leader by isolation rather than Close
		drainPre  int  // number of pre-failover events to drain on the client
		writePost int  // number of post-failover events
	}{
		{name: "Graceful_NoGap", gap: false, abrupt: false, drainPre: 10, writePost: 3},
		{name: "Graceful_WithGap", gap: true, abrupt: false, drainPre: 5, writePost: 3},
		{name: "Abrupt_NoGap", gap: false, abrupt: true, drainPre: 10, writePost: 3},
		{name: "Abrupt_WithGap", gap: true, abrupt: true, drainPre: 5, writePost: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWatchFailover(t, tc.gap, tc.abrupt, tc.drainPre, tc.writePost)
		})
	}
}

func runWatchFailover(t *testing.T, gap, abrupt bool, drainPre, writePost int) {
	t.Helper()
	cluster := newFailoverCluster(t, 3)

	leader := cluster.leader(t, 15*time.Second)
	leaderIdx := cluster.indexOf(leader)
	t.Logf("initial leader: node-%d", leaderIdx)

	// totalPre = events to write on the old leader before failover.
	// With a gap we write more than the client drains; without a gap drainPre == totalPre.
	totalPre := drainPre
	if gap {
		totalPre = drainPre + 5
	}

	prefix := "/watch-failover/"

	// Open the watch on the leader first so replay-from-zero is clean.
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	ch, err := leader.Watch(wctx, prefix, 0)
	if err != nil {
		t.Fatalf("open watch: %v", err)
	}

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer writeCancel()

	revToKey := make(map[int64]string, totalPre)
	for i := 1; i <= totalPre; i++ {
		key := fmt.Sprintf("%sk%03d", prefix, i)
		rev, err := leader.Put(writeCtx, key, []byte("v"), 0)
		if err != nil {
			t.Fatalf("pre-failover Put %d: %v", i, err)
		}
		revToKey[rev] = key
	}

	// Make sure followers have replicated all pre-failover events. This is
	// what lets the new leader serve revisions past lastSeenRev when we open
	// the post-failover watch.
	lastPreRev := int64(0)
	for r := range revToKey {
		if r > lastPreRev {
			lastPreRev = r
		}
	}
	for _, n := range cluster.survivors(leader) {
		if err := n.WaitForRevision(writeCtx, lastPreRev); err != nil {
			t.Fatalf("follower WaitForRevision(%d): %v", lastPreRev, err)
		}
	}

	// Drain drainPre events on the old leader watch. If there is a gap,
	// the remaining (totalPre - drainPre) events stay undelivered to the
	// client even though they are committed and replicated.
	pre := drainN(ch, drainPre, 10*time.Second)
	if len(pre) != drainPre {
		t.Fatalf("pre-drain: got %d events, want %d", len(pre), drainPre)
	}
	var lastSeenRev int64
	for _, e := range pre {
		if e.KV == nil {
			t.Fatalf("nil KV in pre-drain event: %+v", e)
		}
		if e.KV.Revision <= lastSeenRev {
			t.Fatalf("pre-drain revisions not monotonic: prev=%d cur=%d", lastSeenRev, e.KV.Revision)
		}
		lastSeenRev = e.KV.Revision
	}
	t.Logf("client drained pre-failover events up to rev=%d (totalPre=%d)", lastSeenRev, totalPre)

	// Trigger failover.
	if abrupt {
		// Simulate a hard crash: cut peer traffic and S3 access without
		// letting the leader broadcast a graceful shutdown. Followers see
		// connection failure, the lock liveness goes stale, and a
		// follower promotes itself after LeaderLivenessTTL.
		cluster.proxies[leaderIdx].block()
		cluster.stores[leaderIdx].block()
		// Cancel the watch context so the old leader's watch goroutine
		// unblocks; we are intentionally leaving the Node itself running
		// (isolated) so this is what cleans up the client side.
		wcancel()
	} else {
		_ = leader.Close()
		wcancel()
	}

	// Wait for a new leader.
	survivors := cluster.survivors(leader)
	newLeader := waitForLeaderNodeLocal(t, survivors, 30*time.Second)
	if newLeader == leader {
		t.Fatalf("old leader still owns the lock after failover")
	}
	t.Logf("new leader: node-%d", cluster.indexOf(newLeader))

	// Open a new watch on the new leader resuming from lastSeenRev+1.
	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resumeCancel()
	resumeCh, err := newLeader.Watch(resumeCtx, prefix, lastSeenRev+1)
	if err != nil {
		t.Fatalf("resume watch: %v", err)
	}

	// Write post-failover events on the new leader.
	postRevs := make([]int64, 0, writePost)
	for i := 1; i <= writePost; i++ {
		key := fmt.Sprintf("%spost-%03d", prefix, i)
		rev, err := newLeader.Put(resumeCtx, key, []byte("v"), 0)
		if err != nil {
			t.Fatalf("post-failover Put %d: %v", i, err)
		}
		revToKey[rev] = key
		postRevs = append(postRevs, rev)
	}

	// Expected revisions on the resumed watch: every rev strictly greater
	// than lastSeenRev, including pre-failover events that the client never
	// saw (the "gap") and all post-failover events.
	expected := make(map[int64]string, totalPre+writePost)
	for r, k := range revToKey {
		if r > lastSeenRev {
			expected[r] = k
		}
	}
	if len(expected) == 0 {
		t.Fatalf("test bug: nothing to deliver post-resume")
	}

	// Drain the resume channel until we have seen every expected revision
	// (or time out).
	seen := make(map[int64]string, len(expected))
	deadline := time.After(20 * time.Second)
	var lastResumeRev int64
loop:
	for len(seen) < len(expected) {
		select {
		case e, ok := <-resumeCh:
			if !ok {
				t.Fatalf("resume channel closed early; seen=%d want=%d", len(seen), len(expected))
			}
			if e.KV == nil {
				continue
			}
			if e.KV.Revision <= lastSeenRev {
				t.Fatalf("resume delivered already-seen rev=%d (lastSeenRev=%d)", e.KV.Revision, lastSeenRev)
			}
			if _, dup := seen[e.KV.Revision]; dup {
				t.Fatalf("resume duplicate rev=%d key=%s", e.KV.Revision, e.KV.Key)
			}
			if e.KV.Revision <= lastResumeRev {
				t.Fatalf("resume revisions not monotonic: prev=%d cur=%d", lastResumeRev, e.KV.Revision)
			}
			lastResumeRev = e.KV.Revision
			seen[e.KV.Revision] = e.KV.Key
		case <-deadline:
			break loop
		}
	}

	for r, k := range expected {
		got, ok := seen[r]
		if !ok {
			t.Errorf("missing rev=%d (key=%s) on resumed watch", r, k)
			continue
		}
		if got != k {
			t.Errorf("rev=%d: got key=%s, want=%s", r, got, k)
		}
	}
	if len(seen) > len(expected) {
		for r := range seen {
			if _, want := expected[r]; !want {
				t.Errorf("unexpected rev=%d delivered on resumed watch", r)
			}
		}
	}
	// Ensure post-failover writes are in the delivered set.
	for _, r := range postRevs {
		if _, ok := seen[r]; !ok {
			t.Errorf("post-failover rev=%d never delivered", r)
		}
	}
}
