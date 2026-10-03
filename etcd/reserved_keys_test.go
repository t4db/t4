package etcd_test

import (
	"net"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
)

// The auth store keeps users, roles, tokens and the enabled flag in reserved
// \x00auth/ keys of the same keyspace as client data. Like lease state, they
// must never surface through the KV and watch APIs, whatever range a client
// asks for: they hold password hashes and the tokens of logged-in users.

// startAuthServer serves an etcd endpoint with the auth API, as t4 run
// --auth-enabled does, with user alice and role reader created. Auth itself
// stays disabled.
func startAuthServer(t *testing.T) (*clientv3.Client, string) {
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
	tokens := auth.NewTokenStore(t.Context(), time.Hour, node)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(t4etcd.NewServerOptions(authStore, tokens)...)
	t4etcd.New(node, authStore, tokens).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	addr := lis.Addr().String()
	cli := newEtcdClient(t, addr)
	ctx := parityCtx(t)
	if _, err := cli.RoleAdd(ctx, "reader"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.UserAdd(ctx, "alice", "alice-password"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.UserGrantRole(ctx, "alice", "reader"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/data", "v"); err != nil {
		t.Fatal(err)
	}
	return cli, addr
}

func assertNoReservedKeys(t *testing.T, what string, resp *clientv3.GetResponse) {
	t.Helper()
	for _, kv := range resp.Kvs {
		if strings.HasPrefix(string(kv.Key), "\x00") {
			t.Errorf("%s returned reserved key %q", what, kv.Key)
		}
	}
}

func TestRangeOverAllKeysHidesAuthKeys(t *testing.T) {
	cli, _ := startAuthServer(t)
	ctx := parityCtx(t)

	all, err := cli.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	assertNoReservedKeys(t, "range over all keys", all)
	if len(all.Kvs) != 1 || all.Count != 1 {
		t.Errorf("range over all keys: %d kvs, count %d; want only /data", len(all.Kvs), all.Count)
	}

	fromNul, err := cli.Get(ctx, "\x00", clientv3.WithFromKey())
	if err != nil {
		t.Fatal(err)
	}
	assertNoReservedKeys(t, "range from \\x00", fromNul)

	keysOnly, err := cli.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatal(err)
	}
	assertNoReservedKeys(t, "keys-only range over all keys", keysOnly)

	count, err := cli.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatal(err)
	}
	if count.Count != 1 {
		t.Errorf("count over all keys = %d, want 1 (/data)", count.Count)
	}
}

// TestUserWithFullReadCannotSeeTokens: with auth enabled, a user allowed to
// read every key must still not read other users' tokens, which would let it
// act as them (root included).
func TestUserWithFullReadCannotSeeTokens(t *testing.T) {
	cli, addr := startAuthServer(t)
	ctx := parityCtx(t)
	if _, err := cli.RoleGrantPermission(ctx, "reader", "", "\x00", clientv3.PermissionType(clientv3.PermRead)); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.RoleAdd(ctx, "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.UserAdd(ctx, "root", "root-password"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.AuthEnable(ctx); err != nil {
		t.Fatal(err)
	}

	login := func(user, pass string) *clientv3.Client {
		c, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{addr},
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			Username:    user,
			Password:    pass,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	root := login("root", "root-password")
	if _, err := root.Get(ctx, "/data"); err != nil { // issues root's token
		t.Fatal(err)
	}
	alice := login("alice", "alice-password")
	all, err := alice.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	assertNoReservedKeys(t, "alice's range over all keys", all)
}

func TestWatchOverAllKeysHidesAuthKeys(t *testing.T) {
	cli, _ := startAuthServer(t)
	ctx := parityCtx(t)

	wch := cli.Watch(ctx, "", clientv3.WithPrefix())
	// The watch is registered once a no-op progress request is answered.
	if err := cli.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.UserAdd(ctx, "bob", "bob-password"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/after", "v"); err != nil {
		t.Fatal(err)
	}
	for resp := range wch {
		if err := resp.Err(); err != nil {
			t.Fatal(err)
		}
		for _, ev := range resp.Events {
			if strings.HasPrefix(string(ev.Kv.Key), "\x00") {
				t.Fatalf("watch over all keys delivered reserved key %q", ev.Kv.Key)
			}
			if string(ev.Kv.Key) == "/after" {
				return
			}
		}
	}
	t.Fatal("watch closed before /after arrived")
}

func TestDeleteOverAllKeysKeepsAuthState(t *testing.T) {
	cli, _ := startAuthServer(t)
	ctx := parityCtx(t)

	del, err := cli.Delete(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if del.Deleted != 1 {
		t.Errorf("delete over all keys deleted %d keys, want 1 (/data)", del.Deleted)
	}
	if _, err := cli.UserGet(ctx, "alice"); err != nil {
		t.Errorf("user alice gone after deleting all keys: %v", err)
	}
	if _, err := cli.RoleGet(ctx, "reader"); err != nil {
		t.Errorf("role reader gone after deleting all keys: %v", err)
	}
}
