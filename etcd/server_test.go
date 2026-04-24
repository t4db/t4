package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// newServer opens a single-node t4 Node (no S3) and wraps it in an etcd Server.
func newServer(t *testing.T) *t4etcd.Server {
	t.Helper()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	return t4etcd.New(node, nil, nil)
}

func put(t *testing.T, srv *t4etcd.Server, key, val string) int64 {
	t.Helper()
	resp, err := srv.Put(context.Background(), &etcdserverpb.PutRequest{
		Key:   []byte(key),
		Value: []byte(val),
	})
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return resp.Header.Revision
}

// ── Range ────────────────────────────────────────────────────────────────────

func TestRangeSingleKey(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	// Missing key → empty response.
	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k")})
	if err != nil || len(r.Kvs) != 0 {
		t.Fatalf("expected empty: err=%v kvs=%v", err, r.Kvs)
	}
	if r.Header.Revision == 0 {
		t.Fatal("empty range returned revision 0")
	}

	put(t, srv, "/k", "v")

	r, err = srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 1 {
		t.Fatalf("expected 1 kv, got %d", len(r.Kvs))
	}
	if string(r.Kvs[0].Value) != "v" {
		t.Errorf("value: want v got %q", r.Kvs[0].Value)
	}
	if r.Kvs[0].ModRevision == 0 {
		t.Error("ModRevision should be set")
	}
	if r.Kvs[0].CreateRevision == 0 {
		t.Error("CreateRevision should be set")
	}
}

// TestRangeHeaderMatchesKvRevision guards the wire-revision shift: the header
// revision returned on a Range must equal the ModRevision on the kv that was
// just written, and the same for CreateRevision on first write. If a future
// change adds a new revision-emitting path and forgets toEtcdRevision, the two
// numbers drift apart and clients (notably kube-apiserver's cacher) break.
func TestRangeHeaderMatchesKvRevision(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	putRev := put(t, srv, "/rev-match", "v")

	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/rev-match")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 1 {
		t.Fatalf("want 1 kv, got %d", len(r.Kvs))
	}
	if r.Header.Revision != putRev {
		t.Errorf("header revision %d != put revision %d", r.Header.Revision, putRev)
	}
	if r.Kvs[0].ModRevision != r.Header.Revision {
		t.Errorf("ModRevision %d != header %d", r.Kvs[0].ModRevision, r.Header.Revision)
	}
	if r.Kvs[0].CreateRevision != r.Header.Revision {
		t.Errorf("CreateRevision %d != header %d on first write", r.Kvs[0].CreateRevision, r.Header.Revision)
	}
}

func TestRangePrefix(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	for _, k := range []string{"/a/1", "/a/2", "/a/3", "/b/1"} {
		put(t, srv, k, "v")
	}

	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/a/"),
		RangeEnd: []byte("/a0"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 3 {
		t.Fatalf("prefix scan: want 3 got %d", len(r.Kvs))
	}
}

func TestRangeLimit(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		put(t, srv, fmt.Sprintf("/lim/%02d", i), "v")
	}

	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/lim/"),
		RangeEnd: []byte("/lim0"),
		Limit:    3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 3 {
		t.Fatalf("limit: want 3 got %d", len(r.Kvs))
	}
}

func TestRangeCountOnly(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		put(t, srv, fmt.Sprintf("/cnt/%d", i), "v")
	}

	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:       []byte("/cnt/"),
		RangeEnd:  []byte("/cnt0"),
		CountOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Count != 5 {
		t.Errorf("count: want 5 got %d", r.Count)
	}
	if len(r.Kvs) != 0 {
		t.Error("CountOnly should not return Kvs")
	}
}

// ── Put ──────────────────────────────────────────────────────────────────────

func TestPut(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	resp, err := srv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/p"), Value: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Revision == 0 {
		t.Error("revision should be set after put")
	}
}

func TestPutPrevKv(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	put(t, srv, "/prev", "old")

	resp, err := srv.Put(ctx, &etcdserverpb.PutRequest{
		Key:    []byte("/prev"),
		Value:  []byte("new"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.PrevKv == nil {
		t.Fatal("expected PrevKv")
	}
	if string(resp.PrevKv.Value) != "old" {
		t.Errorf("PrevKv.Value: want old got %q", resp.PrevKv.Value)
	}
}

// ── DeleteRange ──────────────────────────────────────────────────────────────

func TestDeleteRange(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	put(t, srv, "/del", "v")

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("/del")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Deleted != 1 {
		t.Errorf("deleted: want 1 got %d", resp.Deleted)
	}

	r, _ := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/del")})
	if len(r.Kvs) != 0 {
		t.Error("key should be gone after delete")
	}
}

func TestDeleteRangePrevKv(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	put(t, srv, "/delprev", "bye")

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:    []byte("/delprev"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.PrevKvs) != 1 || string(resp.PrevKvs[0].Value) != "bye" {
		t.Errorf("unexpected PrevKvs: %v", resp.PrevKvs)
	}
}

// ── Txn ──────────────────────────────────────────────────────────────────────

func createTxn(key, value []byte) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: value},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	}
}

func TestTxnCreate(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	key := []byte("/txn/create")

	// First create: succeeds.
	resp, err := srv.Txn(ctx, createTxn(key, []byte("v1")))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Succeeded {
		t.Fatal("expected Succeeded=true on first create")
	}

	// Second create: key exists → Succeeded=false, Failure response returns current kv.
	resp, err = srv.Txn(ctx, createTxn(key, []byte("v2")))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Succeeded {
		t.Fatal("expected Succeeded=false on duplicate create")
	}
	if len(resp.Responses) == 0 {
		t.Fatal("expected Failure responses with current kv")
	}
	rrResp := resp.Responses[0].GetResponseRange()
	if rrResp == nil || len(rrResp.Kvs) == 0 {
		t.Fatal("expected existing kv in failure response")
	}
	if string(rrResp.Kvs[0].Value) != "v1" {
		t.Errorf("failure kv value: want v1 got %q", rrResp.Kvs[0].Value)
	}
}

func TestTxnCASUpdate(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	key := []byte("/txn/cas")

	// Create the key first.
	put(t, srv, "/txn/cas", "orig")

	getResp, _ := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	modRev := getResp.Kvs[0].ModRevision

	// CAS with correct revision: succeeds.
	resp, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: modRev},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("updated")},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Succeeded {
		t.Fatal("expected Succeeded=true on CAS with correct revision")
	}

	// CAS with stale revision: fails.
	resp, err = srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: modRev}, // old rev
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("stale")},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Succeeded {
		t.Fatal("expected Succeeded=false on CAS with stale revision")
	}
}

func TestTxnCASDelete(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	key := []byte("/txn/del")

	put(t, srv, "/txn/del", "v")
	getResp, _ := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	modRev := getResp.Kvs[0].ModRevision

	resp, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: modRev},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Succeeded {
		t.Fatal("expected Succeeded=true on CAS delete")
	}

	r, _ := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	if len(r.Kvs) != 0 {
		t.Error("key should be deleted")
	}
}

func TestTxnUnconditional(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	resp, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/uncond"), Value: []byte("yes")},
			},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("expected Succeeded=true for unconditional txn")
	}
	kv, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/uncond")})
	if err != nil || kv.Count != 1 || string(kv.Kvs[0].Value) != "yes" {
		t.Fatalf("key not written: resp=%v err=%v", kv, err)
	}
}

// ── Compact ──────────────────────────────────────────────────────────────────

func TestCompact(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	rev := put(t, srv, "/compact", "v")

	resp, err := srv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: rev})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if resp.Header == nil {
		t.Error("expected response header")
	}
}
