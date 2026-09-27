package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/pkg/object"
)

// Watch failover coverage on the etcd v3 gRPC surface. Mirrors the embedded
// Node.Watch test in the root package but exercises the wire path real etcd
// clients use, including clientv3's built-in stream auto-resume on
// disconnection.
//
// Each cluster node hosts its own gRPC server. The client is given all three
// endpoints so it transparently fails over when the dying leader's endpoint
// stops responding.

// ── gated store (test-local) ──────────────────────────────────────────────────

type gatedStore struct {
	inner   object.ConditionalStore
	blocked atomic.Bool
}

func newGatedStore(inner object.ConditionalStore) *gatedStore { return &gatedStore{inner: inner} }

func (g *gatedStore) block() { g.blocked.Store(true) }

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

// blockableProxy and newBlockableProxy are defined in failure_test.go.

// ── cluster ───────────────────────────────────────────────────────────────────

type failoverCluster struct {
	nodes       []*t4.Node
	stores      []*gatedStore
	peerProxies []*blockableProxy
	endpoints   []string
	servers     []*grpc.Server
}

func newFailoverCluster(t *testing.T, size int) *failoverCluster {
	t.Helper()
	shared := object.NewMem()
	c := &failoverCluster{
		nodes:       make([]*t4.Node, size),
		stores:      make([]*gatedStore, size),
		peerProxies: make([]*blockableProxy, size),
		endpoints:   make([]string, size),
		servers:     make([]*grpc.Server, size),
	}
	for i := 0; i < size; i++ {
		peerListen := freeAddrImpl(t)
		peerProxy := newBlockableProxy(t, peerListen)
		gs := newGatedStore(shared)
		n, err := t4.Open(t4.Config{
			DataDir:             t.TempDir(),
			ObjectStore:         gs,
			NodeID:              fmt.Sprintf("wf-node-%d", i),
			PeerListenAddr:      peerListen,
			AdvertisePeerAddr:   peerProxy.Addr(),
			FollowerMaxRetries:  2,
			PeerBufferSize:      1000,
			CheckpointInterval:  300 * time.Millisecond,
			SegmentMaxAge:       200 * time.Millisecond,
			LeaderWatchInterval: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("open node-%d: %v", i, err)
		}
		c.nodes[i] = n
		c.stores[i] = gs
		c.peerProxies[i] = peerProxy

		// gRPC etcd server for this node.
		grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("grpc listen: %v", err)
		}
		srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
		t4etcd.New(n, nil, nil).Register(srv)
		go func() { _ = srv.Serve(grpcLis) }()
		c.endpoints[i] = grpcLis.Addr().String()
		c.servers[i] = srv
	}
	t.Cleanup(func() {
		for _, srv := range c.servers {
			if srv != nil {
				srv.Stop()
			}
		}
		for _, n := range c.nodes {
			if n != nil {
				_ = n.Close()
			}
		}
	})
	return c
}

func (c *failoverCluster) leaderIdx(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if n.IsLeader() {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return -1
}

func (c *failoverCluster) waitNewLeader(t *testing.T, exclude int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if i == exclude {
				continue
			}
			if n.IsLeader() {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no new leader elected within %v", timeout)
	return -1
}

func newEtcdClient(t *testing.T, endpoints []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// ── test ──────────────────────────────────────────────────────────────────────

func TestEtcdWatchSurvivesLeaderFailover(t *testing.T) {
	cases := []struct {
		name      string
		gap       bool
		abrupt    bool
		drainPre  int
		writePost int
	}{
		{name: "Graceful_NoGap", gap: false, abrupt: false, drainPre: 10, writePost: 3},
		{name: "Graceful_WithGap", gap: true, abrupt: false, drainPre: 5, writePost: 3},
		{name: "Abrupt_NoGap", gap: false, abrupt: true, drainPre: 10, writePost: 3},
		{name: "Abrupt_WithGap", gap: true, abrupt: true, drainPre: 5, writePost: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runEtcdWatchFailover(t, tc.gap, tc.abrupt, tc.drainPre, tc.writePost)
		})
	}
}

func runEtcdWatchFailover(t *testing.T, gap, abrupt bool, drainPre, writePost int) {
	t.Helper()
	cluster := newFailoverCluster(t, 3)
	leaderIdx := cluster.leaderIdx(t, 15*time.Second)
	leader := cluster.nodes[leaderIdx]
	t.Logf("initial leader: node-%d (endpoint=%s)", leaderIdx, cluster.endpoints[leaderIdx])

	totalPre := drainPre
	if gap {
		totalPre += 5
	}

	prefix := "/etcd-watch-failover/"

	// clientv3 with all 3 endpoints; auto-resume on stream disconnect.
	cli := newEtcdClient(t, cluster.endpoints)

	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	wch := cli.Watch(wctx, prefix, clientv3.WithPrefix())

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer writeCancel()

	// revToKey is keyed by etcd-wire revision (internal rev + 1) to match
	// what clientv3 events carry.
	revToKey := make(map[int64]string, totalPre)
	var lastInternalPre int64
	for i := 1; i <= totalPre; i++ {
		key := fmt.Sprintf("%sk%03d", prefix, i)
		rev, err := leader.Put(writeCtx, key, []byte("v"), 0)
		if err != nil {
			t.Fatalf("pre-failover Put %d: %v", i, err)
		}
		revToKey[rev+1] = key
		if rev > lastInternalPre {
			lastInternalPre = rev
		}
	}

	// Ensure every follower has the full pre-failover history so it can
	// serve a watch resume after promotion.
	for i, n := range cluster.nodes {
		if i == leaderIdx {
			continue
		}
		if err := n.WaitForRevision(writeCtx, lastInternalPre); err != nil {
			t.Fatalf("node-%d WaitForRevision(%d): %v", i, lastInternalPre, err)
		}
	}

	// Drain drainPre events from the watch.
	pre, rest, lastSeenRev := drainEtcdEvents(t, wch, drainPre, 15*time.Second)
	if len(pre) != drainPre {
		t.Fatalf("pre-drain: got %d events, want %d", len(pre), drainPre)
	}
	t.Logf("client drained pre-failover events up to rev=%d (totalPre=%d)", lastSeenRev, totalPre)

	// Trigger failover.
	if abrupt {
		cluster.peerProxies[leaderIdx].block()
		cluster.stores[leaderIdx].block()
		// Stop this node's etcd gRPC server so clientv3 sees the endpoint
		// drop and fails over to another endpoint promptly. The Node
		// itself stays alive (isolated) to exercise the liveness-TTL
		// takeover path.
		cluster.servers[leaderIdx].Stop()
		cluster.servers[leaderIdx] = nil
	} else {
		// Stop the gRPC server first so the client's stream breaks
		// cleanly, then close the Node so followers see graceful
		// shutdown.
		cluster.servers[leaderIdx].Stop()
		cluster.servers[leaderIdx] = nil
		_ = leader.Close()
		cluster.nodes[leaderIdx] = nil
	}

	newIdx := cluster.waitNewLeader(t, leaderIdx, 60*time.Second)
	t.Logf("new leader: node-%d", newIdx)
	newLeader := cluster.nodes[newIdx]

	postRevs := make([]int64, 0, writePost)
	for i := 1; i <= writePost; i++ {
		key := fmt.Sprintf("%spost-%03d", prefix, i)
		rev, err := newLeader.Put(writeCtx, key, []byte("v"), 0)
		if err != nil {
			t.Fatalf("post-failover Put %d: %v", i, err)
		}
		wireRev := rev + 1
		revToKey[wireRev] = key
		postRevs = append(postRevs, wireRev)
	}

	expected := make(map[int64]string, totalPre+writePost-drainPre)
	for r, k := range revToKey {
		if r > lastSeenRev {
			expected[r] = k
		}
	}

	// Continue draining the same watch channel — clientv3 should
	// transparently resume from the last delivered revision when the
	// stream reconnects to a surviving endpoint.
	// Events that arrived in the same response as the last drained one
	// were delivered before the failover; the client resumes after them.
	seen := make(map[int64]string, len(expected))
	var lastResumeRev int64
	for _, ev := range rest {
		seen[ev.Kv.ModRevision] = string(ev.Kv.Key)
		lastResumeRev = ev.Kv.ModRevision
	}
	deadline := time.After(30 * time.Second)
loop:
	for len(seen) < len(expected) {
		select {
		case wr, ok := <-wch:
			if !ok {
				t.Fatalf("watch channel closed; seen=%d want=%d", len(seen), len(expected))
			}
			if err := wr.Err(); err != nil {
				t.Fatalf("watch error: %v", err)
			}
			for _, ev := range wr.Events {
				rev := ev.Kv.ModRevision
				if rev <= lastSeenRev {
					t.Fatalf("resume delivered already-seen rev=%d (lastSeenRev=%d)", rev, lastSeenRev)
				}
				if _, dup := seen[rev]; dup {
					t.Fatalf("resume duplicate rev=%d key=%s", rev, ev.Kv.Key)
				}
				if rev <= lastResumeRev {
					t.Fatalf("resume revisions not monotonic: prev=%d cur=%d", lastResumeRev, rev)
				}
				lastResumeRev = rev
				seen[rev] = string(ev.Kv.Key)
			}
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
	for r := range seen {
		if _, want := expected[r]; !want {
			t.Errorf("unexpected rev=%d delivered on resumed watch", r)
		}
	}
	for _, r := range postRevs {
		if _, ok := seen[r]; !ok {
			t.Errorf("post-failover rev=%d never delivered", r)
		}
	}
}

// drainEtcdEvents reads events from a clientv3 watch channel until n events
// have been received (or timeout). Returns the events drained, the events
// that arrived in the same response after the n-th (the client has received
// them, so a resumed watch will not deliver them again), and the highest
// ModRevision among the drained events.
func drainEtcdEvents(t *testing.T, wch clientv3.WatchChan, n int, timeout time.Duration) (drained, rest []*clientv3.Event, lastRev int64) {
	t.Helper()
	drained = make([]*clientv3.Event, 0, n)
	deadline := time.After(timeout)
	for len(drained) < n {
		select {
		case wr, ok := <-wch:
			if !ok {
				return drained, nil, lastRev
			}
			if err := wr.Err(); err != nil {
				t.Fatalf("watch error during drain: %v", err)
			}
			for i, ev := range wr.Events {
				drained = append(drained, ev)
				if ev.Kv.ModRevision > lastRev {
					lastRev = ev.Kv.ModRevision
				}
				if len(drained) >= n {
					return drained, wr.Events[i+1:], lastRev
				}
			}
		case <-deadline:
			return drained, nil, lastRev
		}
	}
	return drained, nil, lastRev
}
