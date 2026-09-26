package etcd

import (
	"context"
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

func openStateNode(t *testing.T, store object.Store, dataDir string, legacy bool) *t4.Node {
	t.Helper()
	testhook.LegacyNewDatabases.Store(legacy)
	node, err := t4.Open(t4.Config{DataDir: dataDir, ObjectStore: store})
	testhook.LegacyNewDatabases.Store(false)
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	return node
}

func serveEtcd(t *testing.T, node *t4.Node) *clientv3.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(NewServerOptions(nil, nil)...)
	New(node, nil, nil).Register(gs)
	go gs.Serve(lis)
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
	return cli
}

func stateCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func dataKeys(t *testing.T, node *t4.Node, prefixes ...string) []string {
	t.Helper()
	var keys []string
	for _, prefix := range prefixes {
		kvs, err := node.List(prefix)
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range kvs {
			keys = append(keys, kv.Key)
		}
	}
	return keys
}

// TestLeaseAndAuthStateDoNotConsumeRevisions checks the etcd revision model on
// a new database: lease grants, keepalives, revokes of leases without keys,
// and auth changes leave the revision alone; revoking a lease with keys
// deletes them in one revision.
func TestLeaseAndAuthStateDoNotConsumeRevisions(t *testing.T) {
	node := openStateNode(t, object.NewMem(), t.TempDir(), false)
	cli := serveEtcd(t, node)
	ctx := stateCtx(t)
	authStore, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}

	withKeys, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/leased", "v", clientv3.WithLease(withKeys.ID)); err != nil {
		t.Fatal(err)
	}
	rev := node.CurrentRevision()

	noKeys, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.KeepAliveOnce(ctx, withKeys.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Revoke(ctx, noKeys.ID); err != nil {
		t.Fatal(err)
	}
	if err := authStore.PutUser(ctx, auth.User{Name: auth.RootUser}, "secret"); err != nil {
		t.Fatal(err)
	}
	if err := authStore.PutRole(ctx, auth.Role{Name: "reader"}); err != nil {
		t.Fatal(err)
	}
	token, err := auth.NewTokenStore(ctx, time.Hour, node).Generate(auth.RootUser)
	if err != nil {
		t.Fatal(err)
	}
	if got := node.CurrentRevision(); got != rev {
		t.Fatalf("revision moved from %d to %d on lease/auth bookkeeping", rev, got)
	}
	if keys := dataKeys(t, node, leasePrefix, "\x00auth/"); len(keys) != 0 {
		t.Fatalf("lease/auth state written to the data keyspace: %q", keys)
	}

	// The state is readable back.
	ttl, err := cli.TimeToLive(ctx, withKeys.ID, clientv3.WithAttachedKeys())
	if err != nil || ttl.TTL <= 0 || len(ttl.Keys) != 1 {
		t.Fatalf("TimeToLive = %+v, %v", ttl, err)
	}
	reloaded, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.CheckPassword(auth.RootUser, "secret"); err != nil {
		t.Fatalf("root password: %v", err)
	}
	if _, err := reloaded.GetRole("reader"); err != nil {
		t.Fatalf("role: %v", err)
	}
	if user, ok := auth.NewTokenStore(ctx, time.Hour, node).Lookup(token); !ok || user != auth.RootUser {
		t.Fatalf("token: user=%q ok=%v", user, ok)
	}

	if _, err := cli.Revoke(ctx, withKeys.ID); err != nil {
		t.Fatal(err)
	}
	if got := node.CurrentRevision(); got != rev+1 {
		t.Fatalf("revision after revoking a lease with keys = %d, want %d", got, rev+1)
	}
	if resp, err := cli.Get(ctx, "/leased"); err != nil || len(resp.Kvs) != 0 {
		t.Fatalf("leased key after revoke: %+v, %v", resp, err)
	}
	if leases, err := cli.Leases(ctx); err != nil || len(leases.Leases) != 0 {
		t.Fatalf("Leases after revokes = %+v, %v", leases, err)
	}
}

// TestLegacyDatabaseKeepsLeaseAndAuthInDataKeys checks that a database
// created before the meta keyspace keeps working as before when opened by
// this release: lease and auth state stays in revisioned data keys, where an
// older release can still read it.
func TestLegacyDatabaseKeepsLeaseAndAuthInDataKeys(t *testing.T) {
	store, dataDir := object.NewMem(), t.TempDir()
	node := openStateNode(t, store, dataDir, true)
	cli := serveEtcd(t, node)
	ctx := stateCtx(t)

	rev := node.CurrentRevision()
	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.KeepAliveOnce(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	authStore, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}
	if err := authStore.PutUser(ctx, auth.User{Name: auth.RootUser}, "secret"); err != nil {
		t.Fatal(err)
	}
	if got := node.CurrentRevision(); got != rev+3 {
		t.Fatalf("revision = %d, want %d (grant, keepalive, user each consume one)", got, rev+3)
	}
	if keys := dataKeys(t, node, leasePrefix, "\x00auth/"); len(keys) != 2 {
		t.Fatalf("legacy data keys = %q, want the lease record and the user", keys)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node = openStateNode(t, store, dataDir, false)
	cli = serveEtcd(t, node)
	if ok, _ := node.MetaEnabled(); ok {
		t.Fatal("reopened legacy database switched to the meta keyspace")
	}
	if ttl, err := cli.TimeToLive(ctx, lease.ID); err != nil || ttl.TTL <= 0 {
		t.Fatalf("lease after reopen: %+v, %v", ttl, err)
	}
	if _, err := cli.KeepAliveOnce(ctx, lease.ID); err != nil {
		t.Fatalf("keepalive after reopen: %v", err)
	}
	reloaded, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.CheckPassword(auth.RootUser, "secret"); err != nil {
		t.Fatalf("root password after reopen: %v", err)
	}
}
