package etcd_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/makhov/strata"
	strataetcd "github.com/makhov/strata/etcd"
	"github.com/makhov/strata/internal/object"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// startEtcdServer starts a gRPC server with the etcd adapter and returns its endpoint.
func startEtcdServer(tb testing.TB, node *strata.Node) string {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	strataetcd.New(node).Register(srv)
	go srv.Serve(lis)
	tb.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// newEtcdClient creates an etcd v3 client connected to endpoint.
func newEtcdClient(tb testing.TB, endpoint string) *clientv3.Client {
	tb.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		tb.Fatalf("etcd client: %v", err)
	}
	tb.Cleanup(func() { cli.Close() })
	return cli
}

// freeAddr returns a free localhost TCP address.
func freeAddr(tb testing.TB) string {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("freeAddr: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// waitForLeader polls until one of the nodes reports IsLeader, then returns it.
func waitForLeader(t *testing.T, nodes []*strata.Node, timeout time.Duration) *strata.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// basicCRUD runs a standard put/get/delete/watch smoke test against cli.
func basicCRUD(t *testing.T, cli *clientv3.Client) {
	t.Helper()
	ctx := context.Background()

	// Put + Get.
	_, err := cli.Put(ctx, "/smoke/k", "hello")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := cli.Get(ctx, "/smoke/k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "hello" {
		t.Fatalf("Get: unexpected kvs %v", resp.Kvs)
	}

	// Prefix list.
	for i := 0; i < 3; i++ {
		cli.Put(ctx, fmt.Sprintf("/smoke/list/%d", i), "v")
	}
	lr, err := cli.Get(ctx, "/smoke/list/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Get prefix: %v", err)
	}
	if len(lr.Kvs) != 3 {
		t.Fatalf("prefix list: want 3 got %d", len(lr.Kvs))
	}

	// Txn: create-if-not-exists.
	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/smoke/txnkey"), "=", 0)).
		Then(clientv3.OpPut("/smoke/txnkey", "created")).
		Else(clientv3.OpGet("/smoke/txnkey")).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !txnResp.Succeeded {
		t.Error("txn create: expected Succeeded=true")
	}

	// Txn: CAS update.
	getR, _ := cli.Get(ctx, "/smoke/txnkey")
	rev := getR.Kvs[0].ModRevision
	txnResp, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/smoke/txnkey"), "=", rev)).
		Then(clientv3.OpPut("/smoke/txnkey", "updated")).
		Commit()
	if err != nil {
		t.Fatalf("Txn CAS: %v", err)
	}
	if !txnResp.Succeeded {
		t.Error("txn CAS update: expected Succeeded=true")
	}

	// Delete.
	dr, err := cli.Delete(ctx, "/smoke/k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dr.Deleted != 1 {
		t.Errorf("deleted: want 1 got %d", dr.Deleted)
	}

	// Watch.
	watchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	wch := cli.Watch(watchCtx, "/smoke/watch/")
	go func() { cli.Put(watchCtx, "/smoke/watch/key", "event") }()
	select {
	case wr := <-wch:
		if len(wr.Events) == 0 {
			t.Error("expected at least one watch event")
		}
	case <-watchCtx.Done():
		t.Error("timeout waiting for watch event")
	}
}

// ── offline mode ─────────────────────────────────────────────────────────────

// TestE2EOffline verifies a single-node deployment with no object store.
func TestE2EOffline(t *testing.T) {
	node, err := strata.Open(strata.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("strata.Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	endpoint := startEtcdServer(t, node)
	cli := newEtcdClient(t, endpoint)

	basicCRUD(t, cli)
}

// ── single node + fake S3 ────────────────────────────────────────────────────

// TestE2ESingleNodeS3 verifies S3-backed durability: data written before a
// restart is recovered from the object store.
func TestE2ESingleNodeS3(t *testing.T) {
	store := object.NewMem()
	dir := t.TempDir()

	// First run: open, write data, close.
	func() {
		node, err := strata.Open(strata.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour, // disable auto-checkpoint
		})
		if err != nil {
			t.Fatalf("first open: %v", err)
		}
		endpoint := startEtcdServer(t, node)
		cli := newEtcdClient(t, endpoint)

		ctx := context.Background()
		for i := 0; i < 10; i++ {
			if _, err := cli.Put(ctx, fmt.Sprintf("/persist/%d", i), fmt.Sprintf("v%d", i)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		node.Close()
	}()

	// Second run: open same dir + same store, verify data survived.
	node, err := strata.Open(strata.Config{
		DataDir:     dir,
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	endpoint := startEtcdServer(t, node)
	cli := newEtcdClient(t, endpoint)

	ctx := context.Background()
	resp, err := cli.Get(ctx, "/persist/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if len(resp.Kvs) != 10 {
		t.Errorf("after restart: want 10 keys got %d", len(resp.Kvs))
	}
}

// ── 3-node cluster + fake S3 ─────────────────────────────────────────────────

// TestE2EThreeNode verifies:
//   - Leader election across 3 nodes sharing a MemStore.
//   - Write replication to followers.
//   - Write forwarding from follower to leader.
//   - Leader failover: close the leader, a follower takes over.
func TestE2EThreeNode(t *testing.T) {
	store := object.NewMem()
	const n = 3

	nodes := make([]*strata.Node, n)
	endpoints := make([]string, n)

	for i := 0; i < n; i++ {
		peerAddr := freeAddr(t)
		node, err := strata.Open(strata.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			NodeID:             fmt.Sprintf("node-%d", i),
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2, // fail fast for test speed
			PeerBufferSize:     1000,
		})
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		t.Cleanup(func() { node.Close() })
		endpoints[i] = startEtcdServer(t, node)
		nodes[i] = node
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// ── elect a leader ────────────────────────────────────────────────────────
	leader := waitForLeader(t, nodes, 10*time.Second)
	leaderIdx := -1
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
			break
		}
	}
	t.Logf("leader: node-%d (%s)", leaderIdx, endpoints[leaderIdx])

	leaderCli := newEtcdClient(t, endpoints[leaderIdx])

	// ── replication ───────────────────────────────────────────────────────────
	putResp, err := leaderCli.Put(ctx, "/cluster/replicated", "yes")
	if err != nil {
		t.Fatalf("leader put: %v", err)
	}
	writtenRev := putResp.Header.Revision

	// Wait for all followers to apply the write, then read from each.
	for i, node := range nodes {
		if node == leader {
			continue
		}
		if err := node.WaitForRevision(ctx, writtenRev); err != nil {
			t.Fatalf("node-%d WaitForRevision(%d): %v", i, writtenRev, err)
		}
		resp, err := newEtcdClient(t, endpoints[i]).Get(ctx, "/cluster/replicated")
		if err != nil {
			t.Fatalf("node-%d Get: %v", i, err)
		}
		if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "yes" {
			t.Errorf("node-%d: expected replicated value, got %v", i, resp.Kvs)
		}
	}

	// ── write forwarding ──────────────────────────────────────────────────────
	// Find a follower and write through it.
	followerIdx := -1
	for i, node := range nodes {
		if node != leader {
			followerIdx = i
			break
		}
	}
	followerCli := newEtcdClient(t, endpoints[followerIdx])
	fwdResp, err := followerCli.Put(ctx, "/cluster/forwarded", "from-follower")
	if err != nil {
		t.Fatalf("forwarded put via follower: %v", err)
	}
	fwdRev := fwdResp.Header.Revision

	// Wait for the write to replicate and verify on the leader.
	if err := leader.WaitForRevision(ctx, fwdRev); err != nil {
		t.Fatalf("leader WaitForRevision(%d): %v", fwdRev, err)
	}
	gr, err := leaderCli.Get(ctx, "/cluster/forwarded")
	if err != nil || len(gr.Kvs) != 1 || string(gr.Kvs[0].Value) != "from-follower" {
		t.Errorf("forwarded key not on leader: err=%v kvs=%v", err, gr.Kvs)
	}

	// ── leader failover ───────────────────────────────────────────────────────
	t.Logf("closing leader node-%d to trigger failover", leaderIdx)
	leader.Close()

	// Only poll the surviving nodes; the closed node's role doesn't reset.
	var survivors []*strata.Node
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}

	// A follower should detect stream failure and call TakeOver.
	// FollowerMaxRetries=2 → at most 2×2s=4s before TakeOver.
	newLeader := waitForLeader(t, survivors, 30*time.Second)
	if newLeader == leader {
		t.Fatal("old leader should not win re-election")
	}
	newLeaderIdx := -1
	for i, n := range nodes {
		if n == newLeader {
			newLeaderIdx = i
		}
	}
	t.Logf("new leader: node-%d", newLeaderIdx)

	// Write to new leader and verify it succeeds.
	newLeaderCli := newEtcdClient(t, endpoints[newLeaderIdx])
	if _, err := newLeaderCli.Put(ctx, "/cluster/after-failover", "ok"); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	gr, err = newLeaderCli.Get(ctx, "/cluster/after-failover")
	if err != nil || len(gr.Kvs) != 1 {
		t.Errorf("read after failover: err=%v kvs=%v", err, gr.Kvs)
	}
}
