package etcd_test

// compat_test.go mirrors the applicable subset of etcd's integration tests
// (go.etcd.io/etcd/tests/v3/integration/clientv3) against t4's etcd adapter.
//
// Unsupported / out-of-scope features that are intentionally skipped:
//   - Historical Gets with WithRev (point-in-time reads)
//   - Range deletes (WithPrefix / WithFromKey on Delete)
//   - Auth, member API, progress notifications
//
// Watch compaction tests live in watch_test.go (TestWatchKubeLikeCompactionRecovery).

import (
	"context"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/t4db/t4"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newCompatNode(t *testing.T) (*t4.Node, *clientv3.Client) {
	t.Helper()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	endpoint := startEtcdServer(t, node)
	cli := newEtcdClient(t, endpoint)
	return node, cli
}

// ── KV: Put ───────────────────────────────────────────────────────────────────

// TestCompatKVPutCreateRevision verifies that CreateRevision is set on first
// Put and unchanged on subsequent updates (mirrors TestKVPut from etcd).
func TestCompatKVPutCreateRevision(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	r1, err := cli.Put(ctx, "/compat/k", "v1")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	createRev := r1.Header.Revision

	r2, err := cli.Put(ctx, "/compat/k", "v2")
	if err != nil {
		t.Fatalf("Put update: %v", err)
	}
	if r2.Header.Revision <= createRev {
		t.Errorf("second Put revision %d should be > first %d", r2.Header.Revision, createRev)
	}

	// CreateRevision must equal the first put's revision.
	resp, err := cli.Get(ctx, "/compat/k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("Get: want 1 kv, got %d", len(resp.Kvs))
	}
	kv := resp.Kvs[0]
	if kv.CreateRevision != createRev {
		t.Errorf("CreateRevision: want %d got %d", createRev, kv.CreateRevision)
	}
	if kv.ModRevision != r2.Header.Revision {
		t.Errorf("ModRevision: want %d got %d", r2.Header.Revision, kv.ModRevision)
	}
	if string(kv.Value) != "v2" {
		t.Errorf("Value: want v2 got %q", kv.Value)
	}
}

// TestCompatKVPutPrevKV verifies that WithPrevKV returns the old value on
// overwrite, and nil on first creation.
func TestCompatKVPutPrevKV(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	// First put — no previous value.
	r1, err := cli.Put(ctx, "/compat/prevkv", "first", clientv3.WithPrevKV())
	if err != nil {
		t.Fatalf("Put (first): %v", err)
	}
	if r1.PrevKv != nil {
		t.Errorf("PrevKv on first put: want nil, got %v", r1.PrevKv)
	}

	// Overwrite — should get "first" back.
	r2, err := cli.Put(ctx, "/compat/prevkv", "second", clientv3.WithPrevKV())
	if err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}
	if r2.PrevKv == nil {
		t.Fatal("PrevKv on overwrite: want non-nil, got nil")
	}
	if string(r2.PrevKv.Value) != "first" {
		t.Errorf("PrevKv.Value: want first got %q", r2.PrevKv.Value)
	}
}

// ── KV: Get ───────────────────────────────────────────────────────────────────

// TestCompatKVGetMissingKey verifies an empty result (not an error) for a
// key that does not exist (mirrors TestKVRange missing-key case).
func TestCompatKVGetMissingKey(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	resp, err := cli.Get(ctx, "/compat/no-such-key")
	if err != nil {
		t.Fatalf("Get missing key: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Errorf("want 0 kvs, got %d", len(resp.Kvs))
	}
	if resp.Count != 0 {
		t.Errorf("want Count=0, got %d", resp.Count)
	}
}

// TestCompatKVGetPrefix verifies that WithPrefix returns all matching keys
// and none that don't match.
func TestCompatKVGetPrefix(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	keys := []string{"/compat/pfx/a", "/compat/pfx/b", "/compat/pfx/c"}
	for _, k := range keys {
		if _, err := cli.Put(ctx, k, "v"); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	// This should NOT appear in prefix results.
	if _, err := cli.Put(ctx, "/compat/other/x", "v"); err != nil {
		t.Fatalf("Put other: %v", err)
	}

	resp, err := cli.Get(ctx, "/compat/pfx/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Get prefix: %v", err)
	}
	if int(resp.Count) != len(keys) {
		t.Errorf("Count: want %d got %d", len(keys), resp.Count)
	}
	if len(resp.Kvs) != len(keys) {
		t.Errorf("Kvs length: want %d got %d", len(keys), len(resp.Kvs))
	}
	for _, kv := range resp.Kvs {
		if string(kv.Key) == "/compat/other/x" {
			t.Error("non-matching key /compat/other/x appeared in prefix result")
		}
	}
}

// TestCompatKVGetLimit verifies that WithLimit truncates results correctly.
func TestCompatKVGetLimit(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := cli.Put(ctx, "/compat/lim/"+string(rune('a'+i)), "v"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	resp, err := cli.Get(ctx, "/compat/lim/", clientv3.WithPrefix(), clientv3.WithLimit(3))
	if err != nil {
		t.Fatalf("Get with limit: %v", err)
	}
	if len(resp.Kvs) != 3 {
		t.Errorf("want 3 kvs, got %d", len(resp.Kvs))
	}
	// Note: t4 does not set the More flag in paginated responses.
}

// TestCompatKVGetSortedAscend verifies ascending key order from WithPrefix.
func TestCompatKVGetSortedAscend(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	// Insert in reverse order.
	for _, k := range []string{"c", "a", "b"} {
		if _, err := cli.Put(ctx, "/compat/sort/"+k, k); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	resp, err := cli.Get(ctx, "/compat/sort/", clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		t.Fatalf("Get sorted: %v", err)
	}
	want := []string{"/compat/sort/a", "/compat/sort/b", "/compat/sort/c"}
	for i, kv := range resp.Kvs {
		if string(kv.Key) != want[i] {
			t.Errorf("kv[%d]: want %s got %s", i, want[i], kv.Key)
		}
	}
}

// ── KV: Delete ────────────────────────────────────────────────────────────────

// TestCompatKVDeleteExisting verifies Delete returns Deleted=1 and the
// previous value via WithPrevKV (mirrors TestKVDelete).
func TestCompatKVDeleteExisting(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/compat/del/k", "bye"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	dr, err := cli.Delete(ctx, "/compat/del/k", clientv3.WithPrevKV())
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dr.Deleted != 1 {
		t.Errorf("Deleted: want 1 got %d", dr.Deleted)
	}
	if len(dr.PrevKvs) != 1 {
		t.Fatalf("PrevKvs: want 1 got %d", len(dr.PrevKvs))
	}
	if string(dr.PrevKvs[0].Value) != "bye" {
		t.Errorf("PrevKvs[0].Value: want bye got %q", dr.PrevKvs[0].Value)
	}

	// Key must be gone.
	resp, _ := cli.Get(ctx, "/compat/del/k")
	if len(resp.Kvs) != 0 {
		t.Error("key still exists after Delete")
	}
}

// TestCompatKVDeleteMissing verifies Delete on a non-existent key returns
// Deleted=0 without error.
func TestCompatKVDeleteMissing(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	dr, err := cli.Delete(ctx, "/compat/del/no-such-key")
	if err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if dr.Deleted != 0 {
		t.Errorf("Deleted: want 0 got %d", dr.Deleted)
	}
}

// ── KV: Compact ───────────────────────────────────────────────────────────────

// TestCompatKVCompactError verifies that compacting and then trying to Get
// (or Watch) at the compacted revision returns an appropriate error
// (mirrors TestKVCompactError / TestKVCompact).
func TestCompatKVCompactError(t *testing.T) {
	node, cli := newCompatNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Write some history.
	r1, _ := cli.Put(ctx, "/compat/compact/k", "v1")
	cli.Put(ctx, "/compat/compact/k", "v2")
	compactRev := r1.Header.Revision

	if err := node.Compact(ctx, compactRev-1); err != nil {
		t.Fatalf("Compact(%d): %v", compactRev, err)
	}

	// A watch starting at the compacted revision must fail with ErrCompacted.
	wch := cli.Watch(ctx, "/compat/compact/k", clientv3.WithRev(compactRev))
	select {
	case wr := <-wch:
		if wr.Err() == nil {
			t.Fatal("expected ErrCompacted on watch at compacted revision")
		}
		// etcd clients surface this as rpctypes.ErrCompacted; just check non-nil.
	case <-ctx.Done():
		t.Fatal("timeout waiting for compacted watch error")
	}
}

// TestCompatKVCompactIdempotent verifies that compacting the same revision
// twice succeeds without error.
func TestCompatKVCompactIdempotent(t *testing.T) {
	node, cli := newCompatNode(t)
	ctx := context.Background()

	cli.Put(ctx, "/compat/compact2/k", "v")
	compactRev := node.CurrentRevision()

	if err := node.Compact(ctx, compactRev); err != nil {
		t.Fatalf("first Compact(%d): %v", compactRev, err)
	}
	if err := node.Compact(ctx, compactRev); err != nil {
		t.Fatalf("second Compact(%d) (idempotent): %v", compactRev, err)
	}
}

// ── Txn ───────────────────────────────────────────────────────────────────────

// TestCompatTxnSucceeds verifies that a true condition causes the Then branch
// to execute (mirrors TestTxnSuccess).
func TestCompatTxnSucceeds(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	// Key does not exist yet → ModRevision == 0.
	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/compat/txn/new"), "=", 0)).
		Then(clientv3.OpPut("/compat/txn/new", "created")).
		Else(clientv3.OpGet("/compat/txn/new")).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !txnResp.Succeeded {
		t.Error("Txn: want Succeeded=true, got false")
	}

	// Key must now exist.
	resp, _ := cli.Get(ctx, "/compat/txn/new")
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "created" {
		t.Errorf("after Txn Then: unexpected kvs %v", resp.Kvs)
	}
}

// TestCompatTxnFails verifies that a false condition causes the Else branch to
// execute and Succeeded is false (mirrors TestTxnError).
func TestCompatTxnFails(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	// Pre-create the key so ModRevision != 0.
	if _, err := cli.Put(ctx, "/compat/txn/exists", "already"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/compat/txn/exists"), "=", 0)).
		Then(clientv3.OpPut("/compat/txn/exists", "overwritten")).
		Else(clientv3.OpGet("/compat/txn/exists")).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if txnResp.Succeeded {
		t.Error("Txn: want Succeeded=false, got true")
	}
	if len(txnResp.Responses) == 0 {
		t.Fatal("Txn Else: expected at least one response (OpGet)")
	}
	// Value must still be the original.
	resp, _ := cli.Get(ctx, "/compat/txn/exists")
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "already" {
		t.Errorf("after Txn Else: unexpected kvs %v", resp.Kvs)
	}
}

// TestCompatTxnCAS verifies compare-and-swap semantics: the update succeeds
// when revision matches and fails when it does not.
func TestCompatTxnCAS(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	r, err := cli.Put(ctx, "/compat/txn/cas", "v1")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	modRev := r.Header.Revision

	// CAS with correct revision — must succeed.
	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/compat/txn/cas"), "=", modRev)).
		Then(clientv3.OpPut("/compat/txn/cas", "v2")).
		Commit()
	if err != nil {
		t.Fatalf("Txn CAS (correct rev): %v", err)
	}
	if !txnResp.Succeeded {
		t.Error("Txn CAS (correct rev): want Succeeded=true")
	}

	// CAS with stale revision — must fail.
	txnResp, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/compat/txn/cas"), "=", modRev)).
		Then(clientv3.OpPut("/compat/txn/cas", "v3")).
		Commit()
	if err != nil {
		t.Fatalf("Txn CAS (stale rev): %v", err)
	}
	if txnResp.Succeeded {
		t.Error("Txn CAS (stale rev): want Succeeded=false")
	}
}

// TestCompatTxnMultipleOps verifies that unconditional multi-op transactions
// execute all ops in the Success branch.
func TestCompatTxnMultipleOps(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	resp, err := cli.Txn(ctx).
		Then(
			clientv3.OpPut("/compat/txn/multi/a", "va"),
			clientv3.OpPut("/compat/txn/multi/b", "vb"),
			clientv3.OpPut("/compat/txn/multi/c", "vc"),
		).
		Commit()
	if err != nil {
		t.Fatalf("Txn multi-op: unexpected error: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("Txn multi-op: expected Succeeded=true")
	}
	for _, key := range []string{"/compat/txn/multi/a", "/compat/txn/multi/b", "/compat/txn/multi/c"} {
		r, err := cli.Get(ctx, key)
		if err != nil || r.Count == 0 {
			t.Errorf("key %s not found after txn: err=%v count=%d", key, err, r.Count)
		}
	}
}

// ── Leases ───────────────────────────────────────────────────────────────────

func TestCompatLeaseGrantAttachTTL(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	lease, err := cli.Grant(ctx, 5)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if _, err := cli.Put(ctx, "/compat/lease/k", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("Put with lease: %v", err)
	}

	resp, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	if err != nil {
		t.Fatalf("TimeToLive: %v", err)
	}
	if resp.ID != lease.ID {
		t.Fatalf("TimeToLive ID: want %d got %d", lease.ID, resp.ID)
	}
	if resp.GrantedTTL != 5 {
		t.Fatalf("TimeToLive GrantedTTL: want 5 got %d", resp.GrantedTTL)
	}
	if resp.TTL < 1 || resp.TTL > 5 {
		t.Fatalf("TimeToLive TTL: want 1..5 got %d", resp.TTL)
	}
	if len(resp.Keys) != 1 || string(resp.Keys[0]) != "/compat/lease/k" {
		t.Fatalf("TimeToLive Keys: got %q", resp.Keys)
	}

	getResp, err := cli.Get(ctx, "", clientv3.WithFromKey())
	if err != nil {
		t.Fatalf("Get all keys: %v", err)
	}
	for _, kv := range getResp.Kvs {
		if strings.HasPrefix(string(kv.Key), "\x00t4/") {
			t.Fatalf("internal key leaked through etcd API: %q", kv.Key)
		}
	}
}

func TestCompatLeaseKeepAliveAndLeases(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	lease, err := cli.Grant(ctx, 2)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)

	ka, err := cli.KeepAliveOnce(ctx, lease.ID)
	if err != nil {
		t.Fatalf("KeepAliveOnce: %v", err)
	}
	if ka.ID != lease.ID {
		t.Fatalf("KeepAliveOnce ID: want %d got %d", lease.ID, ka.ID)
	}

	ttlResp, err := cli.TimeToLive(ctx, lease.ID)
	if err != nil {
		t.Fatalf("TimeToLive after keepalive: %v", err)
	}
	if ttlResp.TTL < 1 {
		t.Fatalf("TimeToLive after keepalive: want positive TTL got %d", ttlResp.TTL)
	}

	leases, err := cli.Leases(ctx)
	if err != nil {
		t.Fatalf("Leases: %v", err)
	}
	found := false
	for _, ls := range leases.Leases {
		if ls.ID == lease.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Leases: missing lease %d", lease.ID)
	}
}

func TestCompatLeaseRevokeDeletesKeys(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	lease, err := cli.Grant(ctx, 10)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if _, err := cli.Put(ctx, "/compat/lease/revoke", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("Put with lease: %v", err)
	}
	if _, err := cli.Revoke(ctx, lease.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	resp, err := cli.Get(ctx, "/compat/lease/revoke")
	if err != nil {
		t.Fatalf("Get after revoke: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("expected key deletion on revoke, got %d keys", len(resp.Kvs))
	}
	if _, err := cli.TimeToLive(ctx, lease.ID); err == nil {
		t.Fatal("expected TimeToLive on revoked lease to fail")
	}
}

func TestCompatLeaseExpiryDeletesKeys(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	lease, err := cli.Grant(ctx, 1)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if _, err := cli.Put(ctx, "/compat/lease/expire", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("Put with lease: %v", err)
	}

	for {
		resp, err := cli.Get(ctx, "/compat/lease/expire")
		if err != nil {
			t.Fatalf("Get after expiry: %v", err)
		}
		if len(resp.Kvs) == 0 {
			break
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("timeout waiting for lease expiry deletion")
		}
	}
	if _, err := cli.TimeToLive(ctx, lease.ID); err == nil {
		t.Fatal("expected expired lease to disappear")
	}
}

// ── Header revision monotonicity ─────────────────────────────────────────────

// TestCompatHeaderRevisionMonotonic verifies that successive mutating
// operations return strictly increasing Header.Revision values.
func TestCompatHeaderRevisionMonotonic(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	var last int64
	for i := 0; i < 5; i++ {
		r, err := cli.Put(ctx, "/compat/mono/k", "v")
		if err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
		if r.Header.Revision <= last {
			t.Errorf("Put[%d]: revision %d not greater than previous %d", i, r.Header.Revision, last)
		}
		last = r.Header.Revision
	}
}

// TestCompatDeleteRevisionAdvances verifies that Delete also advances the
// global revision.
func TestCompatDeleteRevisionAdvances(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	pr, err := cli.Put(ctx, "/compat/delrev/k", "v")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	putRev := pr.Header.Revision

	dr, err := cli.Delete(ctx, "/compat/delrev/k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if dr.Header.Revision <= putRev {
		t.Errorf("Delete revision %d should be > put revision %d", dr.Header.Revision, putRev)
	}
}

// TestTxnMultiKeyAtomic verifies that a multi-key transaction writes all keys
// at the same revision and that all keys are visible after the txn commits.
func TestTxnMultiKeyAtomic(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	txnResp, err := cli.Txn(ctx).
		Then(
			clientv3.OpPut("/txn/multi/a", "alpha"),
			clientv3.OpPut("/txn/multi/b", "beta"),
			clientv3.OpPut("/txn/multi/c", "gamma"),
		).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !txnResp.Succeeded {
		t.Fatal("Txn: want Succeeded=true")
	}

	// All keys must be present and share the same ModRevision.
	getResp, err := cli.Get(ctx, "/txn/multi/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Get prefix: %v", err)
	}
	if len(getResp.Kvs) != 3 {
		t.Fatalf("want 3 keys, got %d", len(getResp.Kvs))
	}
	rev := getResp.Kvs[0].ModRevision
	for _, kv := range getResp.Kvs {
		if kv.ModRevision != rev {
			t.Errorf("key %q: ModRevision %d != txn revision %d", kv.Key, kv.ModRevision, rev)
		}
	}
}

// TestTxnMultiKeyConditionalSuccess verifies that a true condition applies the
// Then branch atomically across multiple keys.
func TestTxnMultiKeyConditionalSuccess(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	putResp, err := cli.Put(ctx, "/txn/cond/lock", "free")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	lockRev := putResp.Header.Revision

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/txn/cond/lock"), "=", lockRev)).
		Then(
			clientv3.OpPut("/txn/cond/lock", "held"),
			clientv3.OpPut("/txn/cond/data", "written"),
		).
		Else(clientv3.OpPut("/txn/cond/data", "not-written")).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !txnResp.Succeeded {
		t.Fatal("want Succeeded=true")
	}

	lock, _ := cli.Get(ctx, "/txn/cond/lock")
	data, _ := cli.Get(ctx, "/txn/cond/data")
	if len(lock.Kvs) == 0 || string(lock.Kvs[0].Value) != "held" {
		t.Errorf("lock: want held, got %v", lock.Kvs)
	}
	if len(data.Kvs) == 0 || string(data.Kvs[0].Value) != "written" {
		t.Errorf("data: want written, got %v", data.Kvs)
	}
	if lock.Kvs[0].ModRevision != data.Kvs[0].ModRevision {
		t.Errorf("lock and data must share revision: %d vs %d",
			lock.Kvs[0].ModRevision, data.Kvs[0].ModRevision)
	}
}

// TestTxnMultiKeyConditionalFailure verifies that a false condition applies the
// Else branch and returns Succeeded=false.
func TestTxnMultiKeyConditionalFailure(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/txn/fail/key", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Second write to bump ModRevision past 0.
	if _, err := cli.Put(ctx, "/txn/fail/key", "v2"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/txn/fail/key"), "=", 0)).
		Then(clientv3.OpPut("/txn/fail/result", "success")).
		Else(
			clientv3.OpPut("/txn/fail/result", "failure"),
			clientv3.OpPut("/txn/fail/extra", "extra-value"),
		).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if txnResp.Succeeded {
		t.Fatal("want Succeeded=false")
	}

	result, _ := cli.Get(ctx, "/txn/fail/result")
	extra, _ := cli.Get(ctx, "/txn/fail/extra")
	if len(result.Kvs) == 0 || string(result.Kvs[0].Value) != "failure" {
		t.Errorf("result: want failure, got %v", result.Kvs)
	}
	if len(extra.Kvs) == 0 || string(extra.Kvs[0].Value) != "extra-value" {
		t.Errorf("extra: want extra-value, got %v", extra.Kvs)
	}
	if result.Kvs[0].ModRevision != extra.Kvs[0].ModRevision {
		t.Errorf("result and extra must share revision: %d vs %d",
			result.Kvs[0].ModRevision, extra.Kvs[0].ModRevision)
	}
}

// TestTxnMultiKeyDeleteAccuracy verifies that Deleted counts reflect actual
// deletions rather than blindly reporting 1 per delete op.
func TestTxnMultiKeyDeleteAccuracy(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/txn/del/exists", "yes"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	txnResp, err := cli.Txn(ctx).
		Then(
			clientv3.OpDelete("/txn/del/exists"),  // exists → Deleted=1
			clientv3.OpDelete("/txn/del/missing"), // absent → Deleted=0
		).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}

	if len(txnResp.Responses) != 2 {
		t.Fatalf("want 2 responses, got %d", len(txnResp.Responses))
	}
	dr0 := txnResp.Responses[0].GetResponseDeleteRange()
	dr1 := txnResp.Responses[1].GetResponseDeleteRange()
	if dr0 == nil || dr0.Deleted != 1 {
		t.Errorf("response[0] Deleted: want 1 got %v", dr0)
	}
	if dr1 == nil || dr1.Deleted != 0 {
		t.Errorf("response[1] Deleted: want 0 got %v", dr1)
	}
}

// TestTxnVersionNonZeroRejected verifies that VERSION comparisons against a
// non-zero value return an error rather than silently producing wrong results.
func TestTxnVersionNonZeroRejected(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/txn/version/key", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/txn/version/key"), "=", 2)).
		Then(clientv3.OpPut("/txn/version/key", "bad")).
		Commit()
	if err == nil {
		t.Fatal("expected error for VERSION == 2, got nil")
	}
}

// TestTxnInvalidLeaseRejected verifies that a txn Put referencing a
// non-existent lease is rejected before any write is committed.
func TestTxnInvalidLeaseRejected(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := context.Background()

	const bogusLeaseID = 99999999
	_, err := cli.Txn(ctx).
		Then(clientv3.OpPut("/txn/lease/key", "v",
			clientv3.WithLease(clientv3.LeaseID(bogusLeaseID)))).
		Commit()
	if err == nil {
		t.Fatal("expected error for txn Put with non-existent lease, got nil")
	}

	// Key must not have been written.
	resp, _ := cli.Get(ctx, "/txn/lease/key")
	if len(resp.Kvs) != 0 {
		t.Errorf("key should not exist after rejected txn, got %v", resp.Kvs)
	}
}
