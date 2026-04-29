package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestDeleteRangeDeletedCountMissingKey is the regression test for the P2 fix:
// DeleteRange must report Deleted=0 when the key does not exist, rather than
// always returning 1.
func TestDeleteRangeDeletedCountMissingKey(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/no/such/key"),
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 0 {
		t.Errorf("Deleted: want 0 for missing key, got %d", resp.Deleted)
	}
}

// TestDeleteRangeDeletedCountExistingKey verifies the positive case: Deleted=1
// when the key exists and is successfully removed.
func TestDeleteRangeDeletedCountExistingKey(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	put(t, srv, "/del/key", "val")

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/del/key"),
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("Deleted: want 1 for existing key, got %d", resp.Deleted)
	}
}

// TestDeleteRangeDeletedCountIdempotent verifies that deleting the same key
// twice returns Deleted=1 on the first call and Deleted=0 on the second.
func TestDeleteRangeDeletedCountIdempotent(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	put(t, srv, "/del/idempotent", "val")

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/del/idempotent"),
	})
	if err != nil || resp.Deleted != 1 {
		t.Fatalf("first DeleteRange: err=%v deleted=%d", err, resp.Deleted)
	}

	resp, err = srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/del/idempotent"),
	})
	if err != nil {
		t.Fatalf("second DeleteRange: %v", err)
	}
	if resp.Deleted != 0 {
		t.Errorf("second DeleteRange: want Deleted=0, got %d", resp.Deleted)
	}
}

// TestDeleteRangePrefixAtomic verifies that a prefix DeleteRange removes every
// matching key, reports the correct Deleted count, returns all PrevKvs when
// requested, and commits at a single revision (one atomic Txn, not N writes).
func TestDeleteRangePrefixAtomic(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	// Populate two disjoint key groups to confirm the prefix scope.
	for i := range 5 {
		put(t, srv, fmt.Sprintf("/a/%d", i), fmt.Sprintf("v%d", i))
	}
	keepRev := put(t, srv, "/b/keep", "untouched")

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/a/"),
		RangeEnd: []byte("/a0"),
		PrevKv:   true,
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 5 {
		t.Errorf("Deleted: want 5 got %d", resp.Deleted)
	}
	if len(resp.PrevKvs) != 5 {
		t.Errorf("PrevKvs: want 5 got %d", len(resp.PrevKvs))
	}

	// Single atomic Txn means exactly one revision bump past the last Put.
	if got := resp.Header.Revision - keepRev; got != 1 {
		t.Errorf("range delete should bump revision by 1, got %d", got)
	}

	// Keys under /a/ gone.
	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/a/"),
		RangeEnd: []byte("/a0"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 0 {
		t.Errorf("prefix should be empty after DeleteRange, got %d kvs", len(r.Kvs))
	}

	// Out-of-range key untouched.
	r, err = srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/b/keep")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "untouched" {
		t.Errorf("/b/keep should survive DeleteRange on /a/ prefix, got %v", r.Kvs)
	}
}

func TestDeleteRangeFromKeyIsNotTreatedAsPrefix(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	for _, k := range []string{"/del-from/a", "/del-from/b", "/del-later/a"} {
		put(t, srv, k, "v")
	}

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/del-from/b"),
		RangeEnd: []byte("\x00"),
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("from-key delete: want 2 deleted got %d", resp.Deleted)
	}

	for _, key := range []string{"/del-from/b", "/del-later/a"} {
		r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Kvs) != 0 {
			t.Fatalf("%q should have been deleted, got %v", key, r.Kvs)
		}
	}
	r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/del-from/a")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) != 1 {
		t.Fatalf("/del-from/a should survive from-key delete, got %v", r.Kvs)
	}
}

// TestDeleteRangeEmptyPrefix verifies that DeleteRange over a prefix with no
// matching keys returns Deleted=0 and no PrevKvs, and does not bump revision.
func TestDeleteRangeEmptyPrefix(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	put(t, srv, "/other/k", "v")
	before, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/other/k")})
	if err != nil {
		t.Fatal(err)
	}
	startRev := before.Header.Revision

	resp, err := srv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/none/"),
		RangeEnd: []byte("/none0"),
		PrevKv:   true,
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 0 {
		t.Errorf("Deleted: want 0 got %d", resp.Deleted)
	}
	if len(resp.PrevKvs) != 0 {
		t.Errorf("PrevKvs: want 0 got %d", len(resp.PrevKvs))
	}
	if resp.Header.Revision != startRev {
		t.Errorf("empty-range delete should not bump revision: before=%d after=%d", startRev, resp.Header.Revision)
	}
}
