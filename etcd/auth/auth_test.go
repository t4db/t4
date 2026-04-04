package auth_test

import (
	"context"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/strata-db/strata"
	strataetcd "github.com/strata-db/strata/etcd"
	"github.com/strata-db/strata/etcd/auth"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// tokenCreds is a gRPC PerRPCCredentials that injects a static bearer token
// into the "token" metadata header, matching the etcd auth convention.
type tokenCreds struct{ token string }

func (t tokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"token": t.token}, nil
}
func (tokenCreds) RequireTransportSecurity() bool { return false }

func newNode(t *testing.T) *strata.Node {
	t.Helper()
	node, err := strata.Open(strata.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("strata.Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	return node
}

func newStore(t *testing.T, node *strata.Node) *auth.Store {
	t.Helper()
	s, err := auth.NewStore(node)
	if err != nil {
		t.Fatalf("auth.NewStore: %v", err)
	}
	return s
}

// startServer spins up a gRPC server with auth enabled and returns the listen
// address.
func startServer(t *testing.T, node *strata.Node, store *auth.Store, tokens *auth.TokenStore) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	opts := strataetcd.NewServerOptions(store, tokens)
	srv := grpc.NewServer(opts...)
	strataetcd.New(node, store, tokens).Register(srv)
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// clientWithToken creates an etcd client that injects the given bearer token
// on every request.
func clientWithToken(t *testing.T, addr, token string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(tokenCreds{token}),
		},
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// unauthClient creates an etcd client with no credentials.
func unauthClient(t *testing.T, addr string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// ── AuthStore unit tests ──────────────────────────────────────────────────────

func TestAuthStore_EnableRequiresRootUser(t *testing.T) {
	store := newStore(t, newNode(t))
	if err := store.Enable(context.Background()); err == nil {
		t.Fatal("expected error enabling auth without root user")
	}
}

func TestAuthStore_EnableDisable(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, newNode(t))

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if !store.IsEnabled() {
		t.Fatal("expected auth to be enabled")
	}
	if err := store.Disable(ctx); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if store.IsEnabled() {
		t.Fatal("expected auth to be disabled")
	}
}

func TestAuthStore_UserCRUD(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, newNode(t))

	if err := store.PutUser(ctx, auth.User{Name: "alice"}, "secret"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	u, err := store.GetUser("alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Name != "alice" {
		t.Fatalf("got name %q, want alice", u.Name)
	}

	users, err := store.ListUsers()
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("got %d users, want 1", len(users))
	}

	if err := store.DeleteUser(ctx, "alice"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	if _, err := store.GetUser("alice"); err == nil {
		t.Fatal("expected error getting deleted user")
	}
}

func TestAuthStore_RootUserProtected(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, newNode(t))

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		t.Fatalf("PutUser root: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := store.DeleteUser(ctx, auth.RootUser); err == nil {
		t.Fatal("expected error deleting root user while auth enabled")
	}
}

func TestAuthStore_RoleCRUD(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, newNode(t))

	if err := store.PutRole(ctx, auth.Role{Name: "viewer"}); err != nil {
		t.Fatalf("PutRole: %v", err)
	}
	r, err := store.GetRole("viewer")
	if err != nil {
		t.Fatalf("GetRole: %v", err)
	}
	if r.Name != "viewer" {
		t.Fatalf("got name %q, want viewer", r.Name)
	}

	if err := store.GrantPermission(ctx, "viewer", auth.Permission{
		Key: "/foo/", RangeEnd: "\x00", PermType: auth.READ,
	}); err != nil {
		t.Fatalf("GrantPermission: %v", err)
	}
	r, _ = store.GetRole("viewer")
	if len(r.Permissions) != 1 {
		t.Fatalf("got %d perms, want 1", len(r.Permissions))
	}

	if err := store.RevokePermission(ctx, "viewer", "/foo/", "\x00"); err != nil {
		t.Fatalf("RevokePermission: %v", err)
	}
	r, _ = store.GetRole("viewer")
	if len(r.Permissions) != 0 {
		t.Fatalf("got %d perms after revoke, want 0", len(r.Permissions))
	}

	if err := store.DeleteRole(ctx, "viewer"); err != nil {
		t.Fatalf("DeleteRole: %v", err)
	}
}

func TestAuthStore_CheckPermission(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, newNode(t))

	// "/data/" prefix range: rangeEnd is the next key after all "/data/..." keys.
	// '/' is 0x2F, so the next char is '0' (0x30).
	if err := store.PutRole(ctx, auth.Role{
		Name: "reader",
		Permissions: []auth.Permission{
			{Key: "/data/", RangeEnd: "/data0", PermType: auth.READ},
		},
	}); err != nil {
		t.Fatalf("PutRole: %v", err)
	}
	if err := store.PutUser(ctx, auth.User{Name: "bob"}, "pass"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	if err := store.GrantRole(ctx, "bob", "reader"); err != nil {
		t.Fatalf("GrantRole: %v", err)
	}
	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "r"); err != nil {
		t.Fatalf("PutUser root: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	if err := store.CheckPermission("bob", "/data/foo", auth.READ); err != nil {
		t.Errorf("expected read allowed, got: %v", err)
	}
	if err := store.CheckPermission("bob", "/data/foo", auth.WRITE); err == nil {
		t.Error("expected write denied for reader")
	}
	if err := store.CheckPermission("bob", "/other/key", auth.READ); err == nil {
		t.Error("expected read denied outside permitted prefix")
	}
}

// ── TokenStore unit tests ─────────────────────────────────────────────────────

func TestTokenStore_GenerateLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	tok, err := ts.Generate("alice")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	user, ok := ts.Lookup(tok)
	if !ok {
		t.Fatal("Lookup returned false for valid token")
	}
	if user != "alice" {
		t.Fatalf("got user %q, want alice", user)
	}
}

func TestTokenStore_Revoke(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	tok, _ := ts.Generate("alice")
	ts.Revoke(tok)
	if _, ok := ts.Lookup(tok); ok {
		t.Fatal("expected Lookup to fail after Revoke")
	}
}

func TestTokenStore_Expiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := auth.NewTokenStore(ctx, 50*time.Millisecond, nil)

	tok, _ := ts.Generate("alice")
	time.Sleep(100 * time.Millisecond)
	if _, ok := ts.Lookup(tok); ok {
		t.Fatal("expected token to be expired")
	}
}

func TestTokenStore_PersistenceAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newNode(t)
	ts := auth.NewTokenStore(ctx, 5*time.Minute, node)

	tok, err := ts.Generate("alice")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	// Simulate a restart: create a new TokenStore backed by the same node.
	ts2 := auth.NewTokenStore(ctx, 5*time.Minute, node)
	user, ok := ts2.Lookup(tok)
	if !ok {
		t.Fatal("token not found after simulated restart")
	}
	if user != "alice" {
		t.Fatalf("got user %q after restart, want alice", user)
	}
}

func TestTokenStore_RevokeDeletedFromPebble(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newNode(t)
	ts := auth.NewTokenStore(ctx, 5*time.Minute, node)

	tok, _ := ts.Generate("alice")
	ts.Revoke(tok)

	// New TokenStore backed by same node should not find the revoked token.
	ts2 := auth.NewTokenStore(ctx, 5*time.Minute, node)
	if _, ok := ts2.Lookup(tok); ok {
		t.Fatal("revoked token should not be visible after restart")
	}
}

// ── RateLimiter tests ─────────────────────────────────────────────────────────

func TestAuth_RateLimitLockout(t *testing.T) {
	ctx := context.Background()
	node := newNode(t)
	store := newStore(t, node)

	if err := store.PutUser(ctx, auth.User{Name: "alice"}, "correct"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}

	// Exhaust the failure budget with wrong passwords.
	for i := 0; i < 5; i++ {
		if err := store.CheckPassword("alice", "wrong"); err == nil {
			t.Fatalf("attempt %d: expected error", i+1)
		}
	}

	// The next attempt — even with the correct password — must be rejected.
	if err := store.CheckPassword("alice", "correct"); err == nil {
		t.Fatal("expected lockout error after too many failures")
	}
}

func TestAuth_RateLimitResetOnSuccess(t *testing.T) {
	ctx := context.Background()
	node := newNode(t)
	store := newStore(t, node)

	if err := store.PutUser(ctx, auth.User{Name: "alice"}, "correct"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}

	// A few failures followed by a success should reset the counter.
	for i := 0; i < 3; i++ {
		store.CheckPassword("alice", "wrong") //nolint:errcheck
	}
	if err := store.CheckPassword("alice", "correct"); err != nil {
		t.Fatalf("correct password denied after partial failures: %v", err)
	}
	// Subsequent failures should start from zero again.
	for i := 0; i < 4; i++ {
		store.CheckPassword("alice", "wrong") //nolint:errcheck
	}
	// 4 failures after a reset should not lock (threshold is 5).
	if err := store.CheckPassword("alice", "correct"); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}

func TestAuth_RateLimitUnknownUser(t *testing.T) {
	store := newStore(t, newNode(t))
	// Failures for an unknown user should not panic and should eventually lock.
	for i := 0; i < 5; i++ {
		store.CheckPassword("ghost", "x") //nolint:errcheck
	}
	if err := store.CheckPassword("ghost", "x"); err == nil {
		t.Fatal("expected lockout for unknown user after many failures")
	}
}

// ── Integration tests ─────────────────────────────────────────────────────────

func TestAuth_UnauthenticatedDenied(t *testing.T) {
	ctx := context.Background()
	node := newNode(t)
	store := newStore(t, node)
	tokens := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	addr := startServer(t, node, store, tokens)
	cli := unauthClient(t, addr)

	_, err := cli.Put(ctx, "/foo", "bar")
	if err == nil {
		t.Fatal("expected unauthenticated error, got nil")
	}
}

func TestAuth_RootUserFullAccess(t *testing.T) {
	ctx := context.Background()
	node := newNode(t)
	store := newStore(t, node)
	tokens := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	// Root role is created by Enable; grant it to the root user now.
	if err := store.GrantRole(ctx, auth.RootUser, auth.RootRole); err != nil {
		t.Fatalf("GrantRole: %v", err)
	}

	addr := startServer(t, node, store, tokens)

	rootTok, err := tokens.Generate(auth.RootUser)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cli := clientWithToken(t, addr, rootTok)

	if _, err := cli.Put(ctx, "/any/key", "value"); err != nil {
		t.Fatalf("Put as root: %v", err)
	}
	resp, err := cli.Get(ctx, "/any/key")
	if err != nil {
		t.Fatalf("Get as root: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "value" {
		t.Fatalf("unexpected value: %+v", resp.Kvs)
	}
}

func TestAuth_RBACKeyPrefixEnforced(t *testing.T) {
	ctx := context.Background()
	node := newNode(t)
	store := newStore(t, node)
	tokens := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "rootpass"); err != nil {
		t.Fatalf("PutUser root: %v", err)
	}
	if err := store.PutRole(ctx, auth.Role{
		Name: "writer",
		Permissions: []auth.Permission{
			{Key: "/allowed/", RangeEnd: "/allowed0", PermType: auth.READWRITE},
		},
	}); err != nil {
		t.Fatalf("PutRole: %v", err)
	}
	if err := store.PutUser(ctx, auth.User{Name: "alice"}, "alicepass"); err != nil {
		t.Fatalf("PutUser alice: %v", err)
	}
	if err := store.GrantRole(ctx, "alice", "writer"); err != nil {
		t.Fatalf("GrantRole: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	addr := startServer(t, node, store, tokens)

	aliceTok, err := tokens.Generate("alice")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cli := clientWithToken(t, addr, aliceTok)

	if _, err := cli.Put(ctx, "/allowed/key", "val"); err != nil {
		t.Fatalf("Put within allowed prefix: %v", err)
	}
	if _, err := cli.Put(ctx, "/secret/key", "val"); err == nil {
		t.Fatal("expected permission denied for key outside allowed prefix")
	}
}

func TestAuth_AuthNamespaceBlocked(t *testing.T) {
	ctx := context.Background()
	node := newNode(t)
	store := newStore(t, node)
	tokens := auth.NewTokenStore(ctx, 5*time.Minute, nil)

	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "r"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := store.GrantRole(ctx, auth.RootUser, auth.RootRole); err != nil {
		t.Fatalf("GrantRole: %v", err)
	}

	addr := startServer(t, node, store, tokens)
	rootTok, _ := tokens.Generate(auth.RootUser)
	cli := clientWithToken(t, addr, rootTok)

	// Even the root user must not access \x00auth/ keys via the KV service.
	if _, err := cli.Put(ctx, "\x00auth/users/injected", "evil"); err == nil {
		t.Fatal("expected auth namespace to be blocked even for root")
	}
}
