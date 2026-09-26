package etcd_test

// etcd_parity_test.go pins behaviours where the adapter used to answer
// differently from etcd v3.7. Each case was found by running the same
// workload against a real etcd and against t4 and comparing the responses.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
)

func parityCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestWatchDeleteEventIsTombstone: etcd reports a deletion with a tombstone
// KV carrying only the key and the delete revision; the deleted value's
// metadata is in PrevKv.
func TestWatchDeleteEventIsTombstone(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	put, err := cli.Put(ctx, "/tomb", "v", clientv3.WithLease(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	wch := cli.Watch(ctx, "/tomb", clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV())
	del, err := cli.Delete(ctx, "/tomb")
	if err != nil {
		t.Fatal(err)
	}

	resp := <-wch
	if len(resp.Events) != 1 || resp.Events[0].Type != mvccpb.DELETE {
		t.Fatalf("events = %+v, want one DELETE", resp.Events)
	}
	ev := resp.Events[0]
	if string(ev.Kv.Key) != "/tomb" || ev.Kv.ModRevision != del.Header.Revision ||
		ev.Kv.CreateRevision != 0 || ev.Kv.Version != 0 || ev.Kv.Lease != 0 || len(ev.Kv.Value) != 0 {
		t.Fatalf("delete event kv = %v, want a tombstone with only key /tomb and mod revision %d", ev.Kv, del.Header.Revision)
	}
	if ev.PrevKv == nil || ev.PrevKv.CreateRevision != put.Header.Revision || ev.PrevKv.Lease != int64(lease.ID) {
		t.Fatalf("delete event prev kv = %+v", ev.PrevKv)
	}
}

// TestWatchFromRevisionOneReplaysHistory: revision 1 is the empty store, so a
// watch from it replays every write, including those made before the watch.
func TestWatchFromRevisionOneReplaysHistory(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	for i := 0; i < 3; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("/hist/%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	wch := cli.Watch(ctx, "/hist/", clientv3.WithPrefix(), clientv3.WithRev(1))
	var got []string
	for len(got) < 3 {
		select {
		case resp := <-wch:
			for _, ev := range resp.Events {
				got = append(got, string(ev.Kv.Key))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("watch from revision 1 delivered %v, want the 3 earlier writes", got)
		}
	}
}

// TestCompareRevisionEdgeOperands: an absent key compares as revision 0 and
// revision 1 is the empty store, so operands 1 and below need exact handling.
func TestCompareRevisionEdgeOperands(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := parityCtx(t)

	put, err := cli.Put(ctx, "/present", "v")
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]int64{"/absent": 0, "/present": put.Header.Revision}
	ops := map[string]func(a, b int64) bool{
		"=":  func(a, b int64) bool { return a == b },
		"!=": func(a, b int64) bool { return a != b },
		"<":  func(a, b int64) bool { return a < b },
		">":  func(a, b int64) bool { return a > b },
	}
	for key, rev := range keys {
		for op, holds := range ops {
			for _, operand := range []int64{-1, 0, 1, 2, rev, rev + 1} {
				for target, cmp := range map[string]clientv3.Cmp{
					"mod":    clientv3.Compare(clientv3.ModRevision(key), op, operand),
					"create": clientv3.Compare(clientv3.CreateRevision(key), op, operand),
				} {
					resp, err := cli.Txn(ctx).If(cmp).Commit()
					if err != nil {
						t.Fatal(err)
					}
					if want := holds(rev, operand); resp.Succeeded != want {
						t.Errorf("%s(%s)=%d %s %d: succeeded=%v, want %v", target, key, rev, op, operand, resp.Succeeded, want)
					}
				}
			}
		}
	}
}

// TestRangeHeaderIsCurrentRevision: a read at an older revision reports the
// current revision in its header, as etcd does, including a read of the
// empty store at revision 1.
func TestRangeHeaderIsCurrentRevision(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := parityCtx(t)

	old, err := cli.Put(ctx, "/k", "old")
	if err != nil {
		t.Fatal(err)
	}
	cur, err := cli.Put(ctx, "/k", "new")
	if err != nil {
		t.Fatal(err)
	}
	for _, rev := range []int64{1, old.Header.Revision} {
		resp, err := cli.Get(ctx, "/k", clientv3.WithRev(rev))
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Revision != cur.Header.Revision {
			t.Errorf("Get at rev %d: header revision %d, want current %d", rev, resp.Header.Revision, cur.Header.Revision)
		}
	}
}

// TestKeysOnlyRangeOmitsLease: etcd serves keys-only reads from its index,
// which carries no lease.
func TestKeysOnlyRangeOmitsLease(t *testing.T) {
	_, cli := newCompatNode(t)
	ctx := parityCtx(t)

	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	for _, opts := range [][]clientv3.OpOption{{clientv3.WithKeysOnly()}, {clientv3.WithKeysOnly(), clientv3.WithPrefix()}} {
		resp, err := cli.Get(ctx, "/leased", opts...)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) != 1 || resp.Kvs[0].Lease != 0 || len(resp.Kvs[0].Value) != 0 || resp.Kvs[0].ModRevision == 0 {
			t.Fatalf("keys-only kvs = %+v, want key and revisions without value or lease", resp.Kvs)
		}
	}
	full, err := cli.Get(ctx, "/leased")
	if err != nil || full.Kvs[0].Lease != int64(lease.ID) {
		t.Fatalf("full read = %+v, %v; want the lease", full, err)
	}
}

// TestAuthResponsesCarryHeader: every etcd Auth response has a header with
// the current revision.
func TestAuthResponsesCarryHeader(t *testing.T) {
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
	srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, authStore, tokens).Register(srv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	cli := newEtcdClient(t, lis.Addr().String())

	for name, call := range map[string]func() (*etcdserverpb.ResponseHeader, error){
		"UserAdd": func() (*etcdserverpb.ResponseHeader, error) {
			r, err := cli.UserAdd(ctx, "alice", "pw")
			if err != nil {
				return nil, err
			}
			return r.Header, nil
		},
		"UserList": func() (*etcdserverpb.ResponseHeader, error) {
			r, err := cli.UserList(ctx)
			if err != nil {
				return nil, err
			}
			return r.Header, nil
		},
		"RoleAdd": func() (*etcdserverpb.ResponseHeader, error) {
			r, err := cli.RoleAdd(ctx, "reader")
			if err != nil {
				return nil, err
			}
			return r.Header, nil
		},
		"AuthStatus": func() (*etcdserverpb.ResponseHeader, error) {
			r, err := cli.AuthStatus(ctx)
			if err != nil {
				return nil, err
			}
			return r.Header, nil
		},
	} {
		h, err := call()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		cur, err := cli.Get(ctx, "/any")
		if err != nil {
			t.Fatal(err)
		}
		if h == nil || h.Revision != cur.Header.Revision {
			t.Errorf("%s: header %v, want one with the current revision %d", name, h, cur.Header.Revision)
		}
	}
}
