package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func delRangeOp(key, rangeEnd string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
				Key: []byte(key), RangeEnd: []byte(rangeEnd),
			},
		},
	}
}

// A range delete in a branch removes the whole range in the transaction's
// single revision, and reports how many keys it removed.
func TestTxnBranchRangeDelete(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	for _, k := range []string{"/a/1", "/a/2", "/a/3", "/b/1"} {
		put(t, srv, k, "v")
	}

	resp, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte("/a/1"),
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{delRangeOp("/a/", "/a0")},
	})
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatalf("compare failed: %+v", resp)
	}
	if got := resp.Responses[0].GetResponseDeleteRange().GetDeleted(); got != 3 {
		t.Errorf("deleted = %d, want 3", got)
	}
	for _, k := range []string{"/a/1", "/a/2", "/a/3"} {
		if v := get(t, srv, k); v != "" {
			t.Errorf("%s = %q, want absent", k, v)
		}
	}
	if v := get(t, srv, "/b/1"); v != "v" {
		t.Errorf("/b/1 = %q, want v — outside the range", v)
	}

	// One revision for the whole range, not one per key.
	if resp.Header.Revision != 5+1 {
		t.Errorf("revision = %d, want 6 (four puts, then one txn)", resp.Header.Revision)
	}
}

// The branch that is not taken must not delete anything, and a read later in
// the taken branch must see the range already gone.
func TestTxnBranchRangeDeleteAtomicity(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	put(t, srv, "/keep/1", "v")
	put(t, srv, "/drop/1", "v")
	put(t, srv, "/drop/2", "v")

	resp, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte("/missing"),
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{
			delRangeOp("/drop/", "/drop0"),
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{
					Key: []byte("/drop/"), RangeEnd: []byte("/drop0"),
				},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{delRangeOp("/keep/", "/keep0")},
	})
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatalf("compare on an absent key failed: %+v", resp)
	}
	if got := resp.Responses[0].GetResponseDeleteRange().GetDeleted(); got != 2 {
		t.Errorf("deleted = %d, want 2", got)
	}
	if kvs := resp.Responses[1].GetResponseRange().GetKvs(); len(kvs) != 0 {
		t.Errorf("read after the range delete returned %d keys, want none", len(kvs))
	}
	if v := get(t, srv, "/keep/1"); v != "v" {
		t.Errorf("/keep/1 = %q — the untaken branch deleted it", v)
	}
}

// A range that could reach T4's own keys is refused rather than resolved,
// because the core expands the range without knowing which keys are reserved.
func TestTxnBranchRangeDeleteRefusesReservedRange(t *testing.T) {
	srv := newServer(t)
	for _, tc := range []struct{ name, key, end string }{
		{"whole keyspace", "", "\x00"},
		{"nul prefixed", "\x00", "\x01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := srv.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{delRangeOp(tc.key, tc.end)},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("err = %v, want InvalidArgument", err)
			}
		})
	}
}

// The scan is bounded: a range over more keys than a branch may apply is
// refused instead of being resolved under the write lock.
func TestTxnBranchRangeDeleteBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 65536 keys")
	}
	srv := newServer(t)
	ctx := context.Background()

	// One past the per-branch cap, seeded in batches rather than one write
	// per key: 65536 separate revisions take minutes.
	const total = 65536
	for start := 0; start < total; start += 20000 {
		ops := make([]*etcdserverpb.RequestOp, 0, 20000)
		for i := start; i < min(start+20000, total); i++ {
			ops = append(ops, putOp(fmt.Sprintf("/big/%06d", i), "v"))
		}
		if _, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{Success: ops}); err != nil {
			t.Fatalf("seed from %d: %v", start, err)
		}
	}

	_, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{delRangeOp("/big/", "/big0")},
	})
	if err == nil {
		t.Fatal("an over-large range delete was accepted")
	}
	if v := get(t, srv, "/big/000000"); v != "v" {
		t.Error("a refused range delete removed keys anyway")
	}
}
