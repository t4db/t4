package etcd_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
)

// rbacCluster is an auth-enabled server where root may do anything and alice
// may read and write /pub/ and only write /w/.
type rbacCluster struct {
	root, alice *clientv3.Client
}

func newRBACCluster(t *testing.T) rbacCluster {
	t.Helper()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	authStore, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}
	ctx := parityCtx(t)
	tokens := auth.NewTokenStore(ctx, time.Hour, node)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(t4etcd.NewServerOptions(authStore, tokens)...)
	t4etcd.New(node, authStore, tokens).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	addr := lis.Addr().String()

	admin := newEtcdClient(t, addr)
	steps := []func() error{
		func() error { _, err := admin.UserAdd(ctx, "root", "rootpw"); return err },
		func() error { _, err := admin.RoleAdd(ctx, "root"); return err },
		func() error { _, err := admin.UserGrantRole(ctx, "root", "root"); return err },
		func() error { _, err := admin.UserAdd(ctx, "alice", "alicepw"); return err },
		func() error { _, err := admin.RoleAdd(ctx, "pub"); return err },
		func() error {
			_, err := admin.RoleGrantPermission(ctx, "pub", "/pub/", clientv3.GetPrefixRangeEnd("/pub/"), clientv3.PermissionType(clientv3.PermReadWrite))
			return err
		},
		func() error { _, err := admin.RoleAdd(ctx, "w"); return err },
		func() error {
			_, err := admin.RoleGrantPermission(ctx, "w", "/w/", clientv3.GetPrefixRangeEnd("/w/"), clientv3.PermissionType(clientv3.PermWrite))
			return err
		},
		func() error { _, err := admin.UserGrantRole(ctx, "alice", "pub"); return err },
		func() error { _, err := admin.UserGrantRole(ctx, "alice", "w"); return err },
		func() error { _, err := admin.Put(ctx, "/secret", "s3cr3t"); return err },
		func() error { _, err := admin.Put(ctx, "/pub/a", "a"); return err },
		func() error { _, err := admin.AuthEnable(ctx); return err },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	login := func(user, pw string) *clientv3.Client {
		c, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, Username: user, Password: pw, DialTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	return rbacCluster{root: login("root", "rootpw"), alice: login("alice", "alicepw")}
}

func requireDenied(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, rpctypes.ErrPermissionDenied) {
		t.Errorf("%s: %v, want permission denied", what, err)
	}
}

func requireAllowed(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v, want it allowed", what, err)
	}
}

// TestRBACChecksWholeRange: a request's permission covers every key in the
// range it touches, not only the first, as etcd checks it. A range starting
// in a permitted prefix and running past it must not read or delete keys the
// user has no permission for.
func TestRBACChecksWholeRange(t *testing.T) {
	c := newRBACCluster(t)
	ctx := parityCtx(t)

	_, err := c.alice.Get(ctx, "/pub/", clientv3.WithFromKey())
	requireDenied(t, "Get from /pub/ to the end", err)
	_, err = c.alice.Get(ctx, "/pub/", clientv3.WithRange("/zzz"))
	requireDenied(t, "Get from /pub/ to /zzz", err)
	_, err = c.alice.Delete(ctx, "/pub/", clientv3.WithFromKey())
	requireDenied(t, "Delete from /pub/ to the end", err)
	if r, err := c.root.Get(ctx, "/secret"); err != nil || len(r.Kvs) != 1 {
		t.Fatalf("/secret after alice's denied delete: %v, %v", r, err)
	}

	_, err = c.alice.Get(ctx, "/pub/", clientv3.WithPrefix())
	requireAllowed(t, "Get prefix /pub/", err)
	_, err = c.alice.Get(ctx, "/pub/a")
	requireAllowed(t, "Get /pub/a", err)
	_, err = c.alice.Delete(ctx, "/pub/", clientv3.WithPrefix())
	requireAllowed(t, "Delete prefix /pub/", err)
}

// TestRBACChecksPrevKV: a write that returns the previous value reads it, and
// needs read permission too.
func TestRBACChecksPrevKV(t *testing.T) {
	c := newRBACCluster(t)
	ctx := parityCtx(t)
	_, err := c.alice.Put(ctx, "/w/x", "v")
	requireAllowed(t, "Put /w/x", err)
	_, err = c.alice.Put(ctx, "/w/x", "v2", clientv3.WithPrevKV())
	requireDenied(t, "Put /w/x returning the previous value", err)
	_, err = c.alice.Delete(ctx, "/w/x", clientv3.WithPrevKV())
	requireDenied(t, "Delete /w/x returning the previous value", err)
	_, err = c.alice.Delete(ctx, "/w/x")
	requireAllowed(t, "Delete /w/x", err)
}

// TestRBACChecksTxn: a transaction needs read permission on every key it
// compares or reads, and the permission each of its writes needs.
func TestRBACChecksTxn(t *testing.T) {
	c := newRBACCluster(t)
	ctx := parityCtx(t)
	_, err := c.alice.Txn(ctx).If(clientv3.Compare(clientv3.Value("/secret"), "=", "s3cr3t")).Commit()
	requireDenied(t, "Txn comparing /secret", err)
	_, err = c.alice.Txn(ctx).Then(clientv3.OpGet("/secret")).Commit()
	requireDenied(t, "Txn reading /secret", err)
	_, err = c.alice.Txn(ctx).Then(clientv3.OpGet("/pub/", clientv3.WithFromKey())).Commit()
	requireDenied(t, "Txn reading from /pub/ to the end", err)
	_, err = c.alice.Txn(ctx).Else(clientv3.OpPut("/secret", "x")).Commit()
	requireDenied(t, "Txn writing /secret in its else branch", err)
	_, err = c.alice.Txn(ctx).If(clientv3.Compare(clientv3.Version("/pub/a"), ">", 0)).Then(clientv3.OpGet("/pub/", clientv3.WithPrefix()), clientv3.OpPut("/pub/b", "b")).Commit()
	requireAllowed(t, "Txn within /pub/", err)
}

// TestRBACChecksWatch: a watch needs read permission on its whole range. A
// denied watch is canceled, as etcd does, without ending the stream: the
// client's other watches go on.
func TestRBACChecksWatch(t *testing.T) {
	c := newRBACCluster(t)
	ctx, cancel := context.WithTimeout(parityCtx(t), 20*time.Second)
	defer cancel()

	allowed := c.alice.Watch(ctx, "/pub/", clientv3.WithPrefix())
	for _, w := range []struct {
		name string
		ch   clientv3.WatchChan
	}{
		{"Watch /secret", c.alice.Watch(ctx, "/secret")},
		{"Watch from /pub/ to the end", c.alice.Watch(ctx, "/pub/", clientv3.WithFromKey())},
	} {
		select {
		case resp, ok := <-w.ch:
			if err := resp.Err(); !ok || !resp.Canceled || err == nil || !strings.Contains(err.Error(), "etcdserver: permission denied") {
				t.Errorf("%s: canceled %v, err %v, reason %q (open %v), want it canceled as permission denied", w.name, resp.Canceled, resp.Err(), resp.CancelReason, ok)
			}
			for _, ev := range resp.Events {
				t.Errorf("%s delivered %s", w.name, ev.Kv.Key)
			}
		case <-ctx.Done():
			t.Fatalf("%s: no response", w.name)
		}
	}

	if _, err := c.root.Put(ctx, "/secret", "changed"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.root.Put(ctx, "/pub/c", "c"); err != nil {
		t.Fatal(err)
	}
	select {
	case resp := <-allowed:
		if err := resp.Err(); err != nil {
			t.Fatalf("allowed watch: %v", err)
		}
		for _, ev := range resp.Events {
			if string(ev.Kv.Key) != "/pub/c" {
				t.Errorf("allowed watch delivered %s", ev.Kv.Key)
			}
		}
	case <-ctx.Done():
		t.Fatal("the allowed watch stopped after another watch was denied")
	}
}

// TestRBACChecksLeaseKeys: revoking a lease deletes every key attached to it,
// attaching a key to a lease puts it with those, and its time to live reports
// them, so each needs permission on all of them, as etcd checks.
func TestRBACChecksLeaseKeys(t *testing.T) {
	c := newRBACCluster(t)
	ctx := parityCtx(t)
	lease, err := c.alice.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.alice.Put(ctx, "/pub/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("alice attaching /pub/leased to her lease: %v", err)
	}
	_, err = c.alice.TimeToLive(ctx, lease.ID)
	requireAllowed(t, "TimeToLive of a lease with only /pub/ keys", err)

	if _, err := c.root.Put(ctx, "/secret", "s", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	_, err = c.alice.Revoke(ctx, lease.ID)
	requireDenied(t, "Revoke of a lease /secret is attached to", err)
	if r, err := c.root.Get(ctx, "/secret"); err != nil || len(r.Kvs) != 1 {
		t.Fatalf("/secret after alice's denied revoke: %v, %v", r, err)
	}
	_, err = c.alice.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	requireDenied(t, "TimeToLive of a lease /secret is attached to", err)
	_, err = c.alice.Put(ctx, "/pub/more", "v", clientv3.WithLease(lease.ID))
	requireDenied(t, "Put attaching to a lease /secret is attached to", err)
	_, err = c.root.Revoke(ctx, lease.ID)
	requireAllowed(t, "root's Revoke", err)
}
