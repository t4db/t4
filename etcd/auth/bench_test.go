package auth_test

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
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
)

// ── benchmark helpers ─────────────────────────────────────────────────────────

func newBenchNode(b *testing.B) *t4.Node {
	b.Helper()
	node, err := t4.Open(t4.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("t4.Open: %v", err)
	}
	b.Cleanup(func() { node.Close() })
	return node
}

func newBenchStore(b *testing.B, node *t4.Node) *auth.Store {
	b.Helper()
	s, err := auth.NewStore(node)
	if err != nil {
		b.Fatalf("auth.NewStore: %v", err)
	}
	return s
}

// startBenchServer spins up a gRPC server with auth enabled and returns the
// listen address. Uses *testing.B cleanup for teardown.
func startBenchServer(b *testing.B, node *t4.Node, store *auth.Store, tokens *auth.TokenStore) string {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	opts := t4etcd.NewServerOptions(store, tokens)
	srv := grpc.NewServer(opts...)
	t4etcd.New(node, store, tokens).Register(srv)
	go srv.Serve(lis) //nolint:errcheck
	b.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// startBenchServerNoAuth spins up a gRPC server with no auth interceptors.
func startBenchServerNoAuth(b *testing.B, node *t4.Node) string {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	t4etcd.New(node, nil, nil).Register(srv)
	go srv.Serve(lis) //nolint:errcheck
	b.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func benchClientWithToken(b *testing.B, addr, token string) *clientv3.Client {
	b.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(tokenCreds{token}),
		},
	})
	if err != nil {
		b.Fatalf("etcd client: %v", err)
	}
	b.Cleanup(func() { cli.Close() })
	return cli
}

func benchUnauthClient(b *testing.B, addr string) *clientv3.Client {
	b.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		b.Fatalf("etcd client: %v", err)
	}
	b.Cleanup(func() { cli.Close() })
	return cli
}

// setupAuthStore creates a store with root + a regular user ("bench") who has
// read+write access to "/bench/" and returns the store, token store, and a
// valid token for "bench".
func setupAuthStore(b *testing.B) (*auth.Store, *auth.TokenStore, string) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	node := newBenchNode(b)
	store := newBenchStore(b, node)
	tokens := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	// Root user (required before Enable).
	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		b.Fatalf("PutUser root: %v", err)
	}

	// Bench user with a dedicated role.
	if err := store.PutUser(ctx, auth.User{Name: "bench"}, "benchpass"); err != nil {
		b.Fatalf("PutUser bench: %v", err)
	}

	if err := store.Enable(ctx); err != nil {
		b.Fatalf("Enable: %v", err)
	}

	// Grant root role to root user.
	if err := store.GrantRole(ctx, auth.RootUser, auth.RootRole); err != nil {
		b.Fatalf("GrantRole root: %v", err)
	}

	// Create a role with read+write on /bench/ prefix.
	role := auth.Role{
		Name: "bench-role",
		Permissions: []auth.Permission{
			{Key: "/bench/", RangeEnd: "/bench0", PermType: auth.READWRITE},
		},
	}
	if err := store.PutRole(ctx, role); err != nil {
		b.Fatalf("PutRole: %v", err)
	}
	if err := store.GrantRole(ctx, "bench", "bench-role"); err != nil {
		b.Fatalf("GrantRole bench: %v", err)
	}

	tok, err := tokens.Generate("bench")
	if err != nil {
		b.Fatalf("Generate token: %v", err)
	}
	return store, tokens, tok
}

// ── benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkCheckPermission measures the hot-path RBAC check — the work done
// on every authenticated request after the token has already been validated.
// With the in-memory cache this is a RWMutex RLock + map lookup (~100-200 ns).
func BenchmarkCheckPermission(b *testing.B) {
	store, _, _ := setupAuthStore(b)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := store.CheckPermission("bench", "/bench/key", auth.READWRITE); err != nil {
			b.Fatalf("CheckPermission: %v", err)
		}
	}
}

// BenchmarkCheckPermissionParallel verifies the RWMutex does not become a
// bottleneck under concurrent read-only RBAC checks.
func BenchmarkCheckPermissionParallel(b *testing.B) {
	store, _, _ := setupAuthStore(b)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.CheckPermission("bench", "/bench/key", auth.READWRITE); err != nil {
				b.Fatalf("CheckPermission: %v", err)
			}
		}
	})
}

// BenchmarkCheckPassword measures bcrypt verification — the cost paid once per
// Authenticate RPC (token issuance), NOT on every request.
// Expect ~60-80 ms at DefaultCost; included for reference so it appears in
// benchmark output alongside the per-request numbers.
func BenchmarkCheckPassword(b *testing.B) {
	ctx := context.Background()
	node := newBenchNode(b)
	store := newBenchStore(b, node)

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		b.Fatalf("PutUser: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		b.Fatalf("Enable: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := store.CheckPassword(auth.RootUser, "rootpass"); err != nil {
			b.Fatalf("CheckPassword: %v", err)
		}
	}
}

// BenchmarkKVGetWithAuth measures the full gRPC round-trip for KV.Get with
// auth enabled: interceptor → token lookup → RBAC check → Pebble read.
// Diff vs BenchmarkKVGetNoAuth isolates the total auth overhead.
func BenchmarkKVGetWithAuth(b *testing.B) {
	ctx := context.Background()
	node := newBenchNode(b)
	store, tokens, tok := setupAuthStore(b)
	addr := startBenchServer(b, node, store, tokens)
	cli := benchClientWithToken(b, addr, tok)

	if _, err := cli.Put(ctx, "/bench/key", "value"); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Get(ctx, "/bench/key"); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkKVGetNoAuth is the no-auth baseline for KV.Get.
func BenchmarkKVGetNoAuth(b *testing.B) {
	ctx := context.Background()
	node := newBenchNode(b)
	addr := startBenchServerNoAuth(b, node)
	cli := benchUnauthClient(b, addr)

	if _, err := cli.Put(ctx, "/bench/key", "value"); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Get(ctx, "/bench/key"); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkKVPutWithAuth measures the full gRPC round-trip for KV.Put with
// auth enabled.
func BenchmarkKVPutWithAuth(b *testing.B) {
	ctx := context.Background()
	node := newBenchNode(b)
	store, tokens, tok := setupAuthStore(b)
	addr := startBenchServer(b, node, store, tokens)
	cli := benchClientWithToken(b, addr, tok)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("/bench/key/%d", i), "value"); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

// BenchmarkKVPutNoAuth is the no-auth baseline for KV.Put.
func BenchmarkKVPutNoAuth(b *testing.B) {
	ctx := context.Background()
	node := newBenchNode(b)
	addr := startBenchServerNoAuth(b, node)
	cli := benchUnauthClient(b, addr)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("/bench/key/%d", i), "value"); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}
