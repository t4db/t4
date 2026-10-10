package compat_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
)

// stateBaseline is the last release before the meta keyspace. Its fixture
// holds etcd lease and auth state, which that release keeps in reserved keys
// of the revisioned data keyspace; see generate_state_fixture.sh.
const stateBaseline = "v1.1.11"

type stateMetadata struct {
	Baseline           string `json:"baseline"`
	CheckpointRevision int64  `json:"checkpoint_revision"`
	Revision           int64  `json:"revision"`
	LeaseWithKeys      int64  `json:"lease_with_keys"`
	LeaseNoKeys        int64  `json:"lease_no_keys"`
	LeaseInWAL         int64  `json:"lease_in_wal"`
	Token              string `json:"token"`
}

func readStateMetadata(t *testing.T) stateMetadata {
	t.Helper()
	path := filepath.Join("testdata", stateBaseline, "state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var meta stateMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if meta.Baseline != stateBaseline {
		t.Fatalf("fixture baseline: got %q, want %q", meta.Baseline, stateBaseline)
	}
	return meta
}

// TestPreviousReleaseStateKeepsLegacyMode opens a database holding lease and
// auth state written by the last release before the meta keyspace, both from
// its local data directory and restored from its object store, part from a
// checkpoint and part replayed from the WAL. The database must stay in the
// legacy mode, serve that state, and keep writing new state the old way.
func TestPreviousReleaseStateKeepsLegacyMode(t *testing.T) {
	meta := readStateMetadata(t)
	for _, tc := range []struct {
		name string
		cfg  func(t *testing.T) t4.Config
	}{
		{"local data dir", func(t *testing.T) t4.Config {
			return t4.Config{DataDir: extractFixture(t, stateBaseline, "state-local-data.tar.gz")}
		}},
		{"object store", func(t *testing.T) t4.Config {
			return t4.Config{
				DataDir:       t.TempDir(),
				ObjectStore:   &fileStore{root: extractFixture(t, stateBaseline, "state-object-store.tar.gz")},
				SegmentMaxAge: time.Hour,
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg(t)
			cfg.CheckpointInterval = time.Hour
			node, err := t4.Open(cfg)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = node.Close() })
			checkLegacyState(t, node, meta)
		})
	}
}

func checkLegacyState(t *testing.T, node *t4.Node, meta stateMetadata) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if got := node.CurrentRevision(); got != meta.Revision {
		t.Fatalf("revision: got %d, want %d", got, meta.Revision)
	}
	requireLegacy(t, node)

	// Auth state, including what only the WAL holds.
	authStore, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}
	tokens := auth.NewTokenStore(ctx, time.Hour, node)
	if !authStore.IsEnabled() {
		t.Fatal("auth is not enabled")
	}
	if err := authStore.CheckPassword("alice", "alice-pw"); err != nil {
		t.Fatalf("alice: %v", err)
	}
	if _, err := authStore.GetRole("reader"); err != nil {
		t.Fatalf("reader role: %v", err)
	}
	if user, ok := tokens.Lookup(meta.Token); !ok || user != "alice" {
		t.Fatalf("token: got (%q, %v), want alice", user, ok)
	}

	cli := serveFixture(t, node, authStore, tokens)

	// Lease state, and the keys attached to each lease.
	resp, err := cli.Leases(ctx)
	if err != nil {
		t.Fatalf("leases: %v", err)
	}
	var ids []int64
	for _, l := range resp.Leases {
		ids = append(ids, int64(l.ID))
	}
	want := []int64{meta.LeaseWithKeys, meta.LeaseNoKeys, meta.LeaseInWAL}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("leases: got %v, want %v", ids, want)
	}
	requireLeaseKeys(t, ctx, cli, meta.LeaseWithKeys, "/compat/lease/a", "/compat/lease/b")
	requireLeaseKeys(t, ctx, cli, meta.LeaseNoKeys)
	requireLeaseKeys(t, ctx, cli, meta.LeaseInWAL, "/compat/lease/c")

	// New state is still written to the data keyspace, so each write
	// consumes a revision, as in the release that created the database.
	rev := node.CurrentRevision()
	granted, err := cli.Grant(ctx, 600)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if got := node.CurrentRevision(); got != rev+1 {
		t.Fatalf("grant moved the revision from %d to %d, want %d", rev, got, rev+1)
	}
	if _, err := cli.Put(ctx, "/compat/lease/d", "lease-d", clientv3.WithLease(granted.ID)); err != nil {
		t.Fatal(err)
	}
	requireLeaseKeys(t, ctx, cli, int64(granted.ID), "/compat/lease/d")
	if err := authStore.PutUser(ctx, auth.User{Name: "bob"}, "bob-pw"); err != nil {
		t.Fatalf("put user: %v", err)
	}
	if got := node.CurrentRevision(); got != rev+3 {
		t.Fatalf("revision after put user: got %d, want %d", got, rev+3)
	}

	// Revoking an old lease deletes the keys attached before the upgrade.
	if _, err := cli.Revoke(ctx, clientv3.LeaseID(meta.LeaseWithKeys)); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	for _, key := range []string{"/compat/lease/a", "/compat/lease/b"} {
		if kv, err := node.Get(key); err != nil || kv != nil {
			t.Fatalf("%s after revoke: got (%v, %v), want deleted", key, kv, err)
		}
	}
	if kv, err := node.Get("/compat/plain"); err != nil || kv == nil {
		t.Fatalf("/compat/plain after revoke: got (%v, %v), want kept", kv, err)
	}
	requireLegacy(t, node)
}

func requireLegacy(t *testing.T, node *t4.Node) {
	t.Helper()
	on, err := node.MetaEnabled()
	if err != nil {
		t.Fatal(err)
	}
	if on {
		t.Fatal("a database created before the meta keyspace switched to it")
	}
}

func requireLeaseKeys(t *testing.T, ctx context.Context, cli *clientv3.Client, id int64, want ...string) {
	t.Helper()
	resp, err := cli.TimeToLive(ctx, clientv3.LeaseID(id), clientv3.WithAttachedKeys())
	if err != nil {
		t.Fatalf("time to live %d: %v", id, err)
	}
	if resp.TTL <= 0 {
		t.Fatalf("lease %d: TTL %d, want live", id, resp.TTL)
	}
	var got []string
	for _, k := range resp.Keys {
		got = append(got, string(k))
	}
	sort.Strings(got)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lease %d keys: got %q, want %q", id, got, want)
	}
}

// serveFixture serves node's etcd API with auth on and returns a client
// authenticated as the fixture's root user.
func serveFixture(t *testing.T, node *t4.Node, authStore *auth.Store, tokens *auth.TokenStore) *clientv3.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(t4etcd.NewServerOptions(authStore, tokens)...)
	t4etcd.New(node, authStore, tokens).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{lis.Addr().String()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Username:    auth.RootUser,
		Password:    "root-pw",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}
