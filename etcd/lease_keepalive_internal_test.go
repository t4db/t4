package etcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/pkg/object"
)

// openPair opens a two-node cluster with the meta keyspace and returns its
// leader and follower, each serving the etcd API.
func openPair(t *testing.T) (leader, follower *t4.Node, leaderCli, followerCli *clientv3.Client) {
	t.Helper()
	shared := object.NewMem()
	nodes := make([]*t4.Node, 2)
	for i := range nodes {
		n, err := t4.Open(t4.Config{
			DataDir:        t.TempDir(),
			ObjectStore:    shared,
			NodeID:         fmt.Sprintf("node-%d", i),
			PeerListenAddr: freeAddr(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader, follower = waitLeader(t, nodes...), nil
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	if on, err := leader.MetaEnabled(); err != nil || !on {
		t.Fatalf("precondition: expected the meta keyspace (%v)", err)
	}
	return leader, follower, serveEtcd(t, leader), serveEtcd(t, follower)
}

func waitLeader(t *testing.T, nodes ...*t4.Node) *t4.Node {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// storedExpiry returns the expiry stored in the lease's record.
func storedExpiry(t *testing.T, node *t4.Node, id clientv3.LeaseID) int64 {
	t.Helper()
	value, ok, err := node.MetaGet(leaseKey(int64(id)))
	if err != nil || !ok {
		t.Fatalf("lease record %x: found=%v, %v", id, ok, err)
	}
	rec, err := decodeLease(value)
	if err != nil {
		t.Fatal(err)
	}
	return rec.ExpiryUnixNs
}

// keepAliveFor renews lease id through cli every 500ms for d.
func keepAliveFor(t *testing.T, ctx context.Context, cli *clientv3.Client, id clientv3.LeaseID, d time.Duration) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
		if _, err := cli.KeepAliveOnce(ctx, id); err != nil {
			t.Fatalf("keepalive: %v", err)
		}
	}
}

func waitGone(t *testing.T, node *t4.Node, key string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		kv, err := node.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if kv == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still exists %v after its lease stopped being renewed", key, within)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestKeepAliveIsNotStored: in a database with the meta keyspace a keepalive
// is kept in the leader's memory, not written, yet keeps the lease alive past
// the expiry its record stores, and the lease expires once keepalives stop.
func TestKeepAliveIsNotStored(t *testing.T) {
	node := openStateNode(t, object.NewMem(), t.TempDir(), false)
	cli := serveEtcd(t, node)
	ctx := stateCtx(t)

	lease, err := cli.Grant(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	stored, rev := storedExpiry(t, node, lease.ID), node.CurrentRevision()

	keepAliveFor(t, ctx, cli, lease.ID, 5*time.Second)
	if got := storedExpiry(t, node, lease.ID); got != stored {
		t.Fatalf("keepalives were stored: expiry %d, granted with %d", got, stored)
	}
	if got := node.CurrentRevision(); got != rev {
		t.Fatalf("keepalives moved the revision from %d to %d", rev, got)
	}
	if time.Now().UnixNano() < stored {
		t.Fatal("precondition: the stored expiry should have passed")
	}
	ttl, err := cli.TimeToLive(ctx, lease.ID)
	if err != nil || ttl.TTL <= 0 {
		t.Fatalf("TimeToLive of a lease kept alive = %+v, %v", ttl, err)
	}
	if leases, err := cli.Leases(ctx); err != nil || len(leases.Leases) != 1 {
		t.Fatalf("Leases = %+v, %v; want the lease kept alive", leases, err)
	}
	if kv, err := node.Get("/leased"); err != nil || kv == nil {
		t.Fatalf("leased key expired although the lease was kept alive (%v)", err)
	}

	waitGone(t, node, "/leased", 5*time.Second)
}

// TestKeepAliveThroughFollower: a follower forwards keepalives and reads of
// a lease's time to live to the leader, which keeps the lease alive in memory.
func TestKeepAliveThroughFollower(t *testing.T) {
	leader, _, _, cli := openPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	lease, err := cli.Grant(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	stored := storedExpiry(t, leader, lease.ID)

	keepAliveFor(t, ctx, cli, lease.ID, 5*time.Second)
	if got := storedExpiry(t, leader, lease.ID); got != stored {
		t.Fatalf("keepalives were stored: expiry %d, granted with %d", got, stored)
	}
	ttl, err := cli.TimeToLive(ctx, lease.ID)
	if err != nil || ttl.TTL <= 0 {
		t.Fatalf("TimeToLive through the follower = %+v, %v", ttl, err)
	}
	if leases, err := cli.Leases(ctx); err != nil || len(leases.Leases) != 1 {
		t.Fatalf("Leases through the follower = %+v, %v", leases, err)
	}
	// A write attaching a key checks on the follower that the lease is
	// alive, past its stored expiry.
	if _, err := cli.Put(ctx, "/leased2", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("put with a lease kept alive: %v", err)
	}

	waitGone(t, leader, "/leased", 5*time.Second)
	waitGone(t, leader, "/leased2", time.Second)
}

// TestNewLeaderRenewsLeases: a node that takes over treats every lease as
// renewed when its term starts, so a lease kept alive only in the previous
// leader's memory survives the failover, then expires once not renewed.
func TestNewLeaderRenewsLeases(t *testing.T) {
	leader, follower, leaderCli, _ := openPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	lease, err := leaderCli.Grant(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaderCli.Put(ctx, "/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	keepAliveFor(t, ctx, leaderCli, lease.ID, 4*time.Second)
	if time.Now().UnixNano() < storedExpiry(t, follower, lease.ID) {
		t.Fatal("precondition: the stored expiry should have passed")
	}

	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, follower)
	if kv, err := follower.Get("/leased"); err != nil || kv == nil {
		t.Fatalf("leased key lost in the failover (%v)", err)
	}
	// The expiry loop runs every second: give it two passes within the TTL
	// the takeover renewed.
	time.Sleep(2 * time.Second)
	if kv, err := follower.Get("/leased"); err != nil || kv == nil {
		t.Fatalf("new leader expired the lease before a TTL had passed since it took over (%v)", err)
	}
	waitGone(t, follower, "/leased", 5*time.Second)
}

// TestKeepAliveStoredUnderOlderLeader: a follower whose leader predates
// in-memory renewal stores each keepalive, as that leader expects.
func TestKeepAliveStoredUnderOlderLeader(t *testing.T) {
	leader, _, _, cli := openPair(t)
	testhook.LeaderWithoutLeaseRenewal.Store(true)
	t.Cleanup(func() { testhook.LeaderWithoutLeaseRenewal.Store(false) })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	stored := storedExpiry(t, leader, lease.ID)
	time.Sleep(10 * time.Millisecond)
	if _, err := cli.KeepAliveOnce(ctx, lease.ID); err != nil {
		t.Fatalf("keepalive: %v", err)
	}
	if got := storedExpiry(t, leader, lease.ID); got <= stored {
		t.Fatalf("keepalive under an older leader was not stored: expiry %d, granted with %d", got, stored)
	}
	if ttl, err := cli.TimeToLive(ctx, lease.ID); err != nil || ttl.TTL <= 0 {
		t.Fatalf("TimeToLive under an older leader = %+v, %v", ttl, err)
	}
}
