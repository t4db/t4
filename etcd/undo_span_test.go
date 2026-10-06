package etcd_test

import (
	"context"
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// TestRangePinnedBeyondUndoSpan pins that with Config.MaxUndoSpan set, a Range
// pinned further behind HEAD than the span fails with the etcd compaction
// error, so kine/apiserver-style clients resync from HEAD instead of paying
// for the undo scan — and that without the cap, any retained revision stays
// readable.
func TestRangePinnedBeyondUndoSpan(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir(), MaxUndoSpan: 2})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	srv := t4etcd.New(node, nil, nil)
	ctx := context.Background()

	pin := put(t, srv, "/k/a", "v")
	for _, k := range []string{"/k/b", "/k/c", "/k/d", "/k/e"} {
		put(t, srv, k, "v")
	} // HEAD is now pin+4.

	if _, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/a"), Revision: pin + 2}); err != nil {
		t.Fatalf("range at cap boundary: %v", err)
	}
	if _, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/a"), Revision: pin}); !errors.Is(err, rpctypes.ErrGRPCCompacted) {
		t.Fatalf("range beyond cap: got %v, want ErrGRPCCompacted", err)
	}

	nc := newServer(t) // no cap
	pinC := put(t, nc, "/k/a", "v")
	for _, k := range []string{"/k/b", "/k/c", "/k/d", "/k/e"} {
		put(t, nc, k, "v")
	}
	if _, err := nc.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k/a"), Revision: pinC}); err != nil {
		t.Fatalf("uncapped range at pin: %v", err)
	}
}
