package etcd

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	"github.com/t4db/t4/etcd/auth"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/pkg/object"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// TestFollowerCompatibleWithV1Leader covers a rolling upgrade of a database
// created before the meta keyspace: followers run this release while the
// leader still runs v1.1. Every lease and auth write made through a follower
// is forwarded to that leader, which ignores txn conditions and op types it
// does not know. The leader here rejects them instead, and the lease below
// survives only if every keepalive actually reaches it.
func TestFollowerCompatibleWithV1Leader(t *testing.T) {
	shared := object.NewMem()
	testhook.LegacyNewDatabases.Store(true)
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
	testhook.LegacyNewDatabases.Store(false)
	testhook.V1Leader.Store(true)
	t.Cleanup(func() { testhook.V1Leader.Store(false) })

	var leader, follower *t4.Node
	deadline := time.Now().Add(10 * time.Second)
	for leader == nil {
		for i, n := range nodes {
			if n.IsLeader() {
				leader, follower = n, nodes[1-i]
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if on, _ := follower.MetaEnabled(); on {
		t.Fatal("precondition: expected a legacy database")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	authStore, err := auth.NewStore(follower)
	if err != nil {
		t.Fatal(err)
	}
	tokens := auth.NewTokenStore(ctx, time.Hour, follower)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(NewServerOptions(nil, nil)...)
	New(follower, authStore, tokens).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{lis.Addr().String()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	// Leases: grant, attach, keep alive past the TTL, revoke.
	short, err := cli.Grant(ctx, 3)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := cli.Put(ctx, "/leased", "v", clientv3.WithLease(short.ID)); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(6 * time.Second); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
		if _, err := cli.KeepAliveOnce(ctx, short.ID); err != nil {
			t.Fatalf("keepalive: %v", err)
		}
	}
	if kv, err := leader.Get("/leased"); err != nil || kv == nil {
		t.Fatalf("leased key expired although it was kept alive (%v): keepalives did not reach the leader", err)
	}
	if _, err := cli.Grant(ctx, 60); err != nil {
		t.Fatalf("second grant: %v", err)
	}
	if _, err := cli.Revoke(ctx, short.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if kv, _ := leader.Get("/leased"); kv != nil {
		t.Fatal("revoke did not delete the attached key")
	}

	// Auth state written through the follower is persisted by the leader.
	if err := authStore.PutUser(ctx, auth.User{Name: "alice"}, "pw"); err != nil {
		t.Fatalf("put user: %v", err)
	}
	if err := authStore.PutRole(ctx, auth.Role{Name: "reader"}); err != nil {
		t.Fatalf("put role: %v", err)
	}
	if err := authStore.DeleteRole(ctx, "reader"); err != nil {
		t.Fatalf("delete role: %v", err)
	}
	if _, err := tokens.Generate("alice"); err != nil {
		t.Fatalf("token: %v", err)
	}
	onLeader, err := auth.NewStore(leader)
	if err != nil {
		t.Fatal(err)
	}
	if err := onLeader.CheckPassword("alice", "pw"); err != nil {
		t.Fatalf("user written through the follower is missing on the leader: %v", err)
	}
	if _, err := onLeader.GetRole("reader"); err == nil {
		t.Fatal("role deleted through the follower still exists on the leader")
	}
}
