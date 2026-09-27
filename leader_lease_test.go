package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/pkg/object"
)

// stallingProxy forwards TCP traffic to target until stall is called; from
// then on it silently stops forwarding while keeping every connection open,
// like a network partition that drops packets: neither side sees an error.
type stallingProxy struct {
	lis     net.Listener
	target  string
	stalled atomic.Bool
}

func newStallingProxy(t *testing.T, target string) *stallingProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallingProxy{lis: lis, target: target}
	go p.serve()
	t.Cleanup(func() { _ = lis.Close() })
	return p
}

func (p *stallingProxy) Addr() string { return p.lis.Addr().String() }
func (p *stallingProxy) stall()       { p.stalled.Store(true) }

func (p *stallingProxy) serve() {
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return
		}
		dst, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		go p.pipe(dst, c)
		go p.pipe(c, dst)
	}
}

func (p *stallingProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		for p.stalled.Load() {
			time.Sleep(50 * time.Millisecond) // hold the bytes, keep the connection
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				_ = dst.Close()
			}
			return
		}
	}
}

// TestLeaderHeartbeatNotBlockedByPendingWrites: when a partition silently
// stalls the follower streams, writes wait for acknowledgements that never
// come. The leader is alive and reaches object storage, so its liveness
// heartbeat must keep going and no follower may take over.
func TestLeaderHeartbeatNotBlockedByPendingWrites(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 3)
	proxies := make([]*stallingProxy, 3)
	for i := range nodes {
		listen := freeAddrLocal(t)
		proxies[i] = newStallingProxy(t, listen)
		n, err := Open(Config{
			DataDir:           t.TempDir(),
			ObjectStore:       shared,
			NodeID:            fmt.Sprintf("node-%d", i),
			PeerListenAddr:    listen,
			AdvertisePeerAddr: proxies[i].Addr(),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	var leaderIdx int
	var follower *Node
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
		} else if follower == nil {
			follower = n
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.WaitForRevision(ctx, rev); err != nil {
		t.Fatal(err)
	}

	proxies[leaderIdx].stall()
	writeCtx, writeCancel := context.WithCancel(ctx)
	defer writeCancel()
	go func() { _, _ = leader.Put(writeCtx, "/stuck", []byte("v"), 0) }() // waits for acknowledgements

	time.Sleep(peer.LeaderLivenessTTL + 2*time.Second)
	lock := election.NewLock(shared, follower.cfg.NodeID, follower.cfg.AdvertisePeerAddr)
	if _, promoted := follower.attemptPromotion(follower.bgCtx, lock, false); promoted {
		t.Fatalf("follower took over from a live leader whose heartbeat was blocked behind a pending write (leader IsLeader=%v)", leader.IsLeader())
	}
}

// TestDeposedLeaderStopsServing: a leader cut off from object storage and
// from its followers is replaced once its followers stop hearing it. By then
// it must have stopped acknowledging writes and serving linearizable reads.
func TestDeposedLeaderStopsServing(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	leaderIdx := cluster.indexOf(leader)
	survivors := cluster.survivors(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("before"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range survivors {
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatal(err)
		}
	}

	cluster.stores[leaderIdx].block()
	cluster.proxies[leaderIdx].block()
	start := time.Now()
	waitForLeaderNodeLocal(t, survivors, 30*time.Second)
	t.Logf("a survivor took over after %v", time.Since(start).Round(time.Millisecond))

	wctx, wcancel := context.WithTimeout(ctx, 3*time.Second)
	defer wcancel()
	if wrev, err := leader.Put(wctx, "/k", []byte("after-takeover"), 0); err == nil {
		t.Fatalf("deposed leader acknowledged a write (rev=%d) that the new leader does not have", wrev)
	}
	if kv, err := leader.LinearizableGet(wctx, "/k"); err == nil {
		t.Fatalf("deposed leader served a linearizable read (%q) after being replaced", kv.Value)
	}
	// Conditional writes that fail are answered from local state too.
	if _, err := leader.Create(wctx, "/k", []byte("x"), 0); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("deposed leader answered Create on an existing key with %v, want ErrNoLeader", err)
	}
	if _, _, _, err := leader.Update(wctx, "/k", []byte("x"), rev+100, 0); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("deposed leader answered a mismatched Update with %v, want ErrNoLeader", err)
	}
	if _, err := leader.Delete(wctx, "/missing"); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("deposed leader answered Delete of a missing key with %v, want ErrNoLeader", err)
	}
	failing := TxnRequest{Conditions: []TxnCondition{{Key: "/k", Target: TxnCondVersion, Result: TxnCondEqual, Version: 99}}}
	if _, err := leader.Txn(wctx, failing); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("deposed leader answered a failing Txn with %v, want ErrNoLeader", err)
	}
	// The meta keyspace has its own write and read paths.
	if err := leader.MetaPut(wctx, "/meta-k", []byte("x")); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("deposed leader answered MetaPut with %v, want ErrNoLeader", err)
	}
	if _, _, err := leader.LinearizableMetaGet(wctx, "/meta-k"); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("deposed leader answered LinearizableMetaGet with %v, want ErrNoLeader", err)
	}
}

// TestLeaderWithoutObjectStoreKeepsServing: a leader that loses object
// storage but still hears all its followers stays the leader on its slow
// lease; none of its followers may take over while they hear it.
func TestLeaderWithoutObjectStoreKeepsServing(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	survivors := cluster.survivors(leader)
	for _, n := range survivors {
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatal(err)
		}
	}
	// Let the leader settle into slow mode with a fence covering the write.
	time.Sleep(2 * time.Second)
	if _, err := leader.Put(ctx, "/k", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}

	cluster.stores[cluster.indexOf(leader)].block()
	time.Sleep(peer.LeaderLivenessTTL + 2*time.Second)
	candidate := survivors[0]
	lock := election.NewLock(cluster.shared, candidate.cfg.NodeID, candidate.cfg.AdvertisePeerAddr)
	if _, promoted := candidate.attemptPromotion(candidate.bgCtx, lock, false); promoted {
		t.Fatal("a follower that hears the leader took over")
	}
	if _, err := leader.Put(ctx, "/k", []byte("v3"), 0); err != nil {
		t.Fatalf("leader with its followers but without object storage stopped serving: %v", err)
	}
}

// TestNoTakeoverFromLiveLeader: a follower that loses its stream (a network
// partition between it and the leader, while both still reach object
// storage) must not be able to take over from a leader that is alive and
// still serving. Otherwise two nodes act as leader at once: each serves its
// own "linearizable" reads, and writes the old leader acknowledges are lost.
func TestNoTakeoverFromLiveLeader(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 3)
	for i := range nodes {
		addr := freeAddrLocal(t)
		n, err := Open(Config{
			DataDir:             t.TempDir(),
			ObjectStore:         shared,
			NodeID:              fmt.Sprintf("node-%d", i),
			PeerListenAddr:      addr,
			AdvertisePeerAddr:   addr,
			LeaderWatchInterval: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("before"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.WaitForRevision(ctx, rev); err != nil {
		t.Fatal(err)
	}

	// A healthy cluster, well past the liveness TTL.
	time.Sleep(peer.LeaderLivenessTTL + 2*time.Second)

	// The follower decides the leader is unreachable, as it would when a
	// partition cuts its stream.
	lock := election.NewLock(shared, follower.cfg.NodeID, follower.cfg.AdvertisePeerAddr)
	if _, promoted := follower.attemptPromotion(follower.bgCtx, lock, false); promoted {
		// Demonstrate the consequence before failing: the old leader still
		// acknowledges writes that the new leader never sees.
		wrev, werr := leader.Put(ctx, "/k", []byte("acked-by-old-leader"), 0)
		kv, _ := follower.Get("/k")
		t.Fatalf("follower took over while the leader was alive: old leader IsLeader=%v, "+
			"old leader acknowledged a write (rev=%d err=%v) that the new leader does not have (it reads %q)",
			leader.IsLeader(), wrev, werr, kv.Value)
	}
	if !leader.IsLeader() {
		t.Fatal("leader lost leadership")
	}
}

// TestWriteNotStuckOnSilentFollowers: a partition that silently drops the
// leader's traffic to its followers breaks no connection. Heartbeats let the
// leader notice and stop waiting for acknowledgements that cannot come, so
// the write completes instead of hanging until the client gives up.
func TestWriteNotStuckOnSilentFollowers(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 3)
	proxies := make([]*stallingProxy, 3)
	for i := range nodes {
		listen := freeAddrLocal(t)
		proxies[i] = newStallingProxy(t, listen)
		n, err := Open(Config{
			DataDir:           t.TempDir(),
			ObjectStore:       shared,
			NodeID:            fmt.Sprintf("node-%d", i),
			PeerListenAddr:    listen,
			AdvertisePeerAddr: proxies[i].Addr(),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	var leaderIdx int
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatal(err)
		}
	}
	if c := leader.peerSrv.ConnectedFollowers(); c != 2 {
		t.Fatalf("precondition: want 2 connected followers, have %d", c)
	}

	proxies[leaderIdx].stall()
	wctx, wcancel := context.WithTimeout(ctx, peer.HeartbeatTimeout+5*time.Second)
	defer wcancel()
	start := time.Now()
	if _, err := leader.Put(wctx, "/k", []byte("during-partition"), 0); err != nil {
		t.Fatalf("write waiting on silent followers did not complete: %v", err)
	}
	t.Logf("write completed after %v", time.Since(start).Round(time.Millisecond))
}
