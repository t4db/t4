package etcd_test

// etcd_parity_test.go pins behaviours where the adapter used to answer
// differently from etcd v3.7. Each case was found by running the same
// workload against a real etcd and against t4 and comparing the responses.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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
	go func() { _ = srv.Serve(lis) }()
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

// TestWatchDeliversRevisionInOneResponse: etcd delivers every event of a
// revision in one WatchResponse. Splitting a revision across responses is
// unsafe beyond appearance: clientv3 resumes a broken stream from the last
// received event's ModRevision+1, so a stream that drops between two parts
// of a revision silently loses the rest of it.
func TestWatchDeliversRevisionInOneResponse(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	const n = 130
	bigTxn := func() int64 {
		ops := make([]clientv3.Op, n)
		for i := range ops {
			ops[i] = clientv3.OpPut(fmt.Sprintf("/rev/%03d", i), "v")
		}
		resp, err := cli.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			t.Fatal(err)
		}
		return resp.Header.Revision
	}

	// A live watch sees the txn as it is dispatched; a replay watch reads it
	// back from history.
	live := cli.Watch(ctx, "/rev/", clientv3.WithPrefix())
	if err := cli.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	<-live // wait until the watch is established before writing
	rev := bigTxn()
	replay := cli.Watch(ctx, "/rev/", clientv3.WithPrefix(), clientv3.WithRev(rev))

	for name, wch := range map[string]clientv3.WatchChan{"live": live, "replay": replay} {
		var sizes []int
		for got := 0; got < n; {
			select {
			case resp := <-wch:
				if err := resp.Err(); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if len(resp.Events) == 0 {
					continue
				}
				sizes = append(sizes, len(resp.Events))
				got += len(resp.Events)
			case <-ctx.Done():
				t.Fatalf("%s: timed out after responses of %v events", name, sizes)
			}
		}
		if len(sizes) != 1 {
			t.Errorf("%s: revision %d arrived in responses of %v events, want one of %d", name, rev, sizes, n)
		}
	}
}

// TestTxnRangeSeesPrecedingOpsOnly: etcd executes a transaction's ops in
// order against one snapshot, so a Range sees the writes of the ops before it
// and none of the ops after it — nor any write committed by another client
// after the transaction.
func TestTxnRangeSeesPrecedingOpsOnly(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	for _, k := range []string{"/tx/a", "/tx/c"} {
		if _, err := cli.Put(ctx, k, "before"); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := cli.Txn(ctx).Then(
		clientv3.OpGet("/tx/a"),
		clientv3.OpGet("/tx/", clientv3.WithPrefix()),
		clientv3.OpPut("/tx/a", "after"),
		clientv3.OpPut("/tx/b", "after"),
		clientv3.OpDelete("/tx/c"),
		clientv3.OpGet("/tx/a"),
		clientv3.OpGet("/tx/", clientv3.WithPrefix()),
		clientv3.OpGet("/tx/", clientv3.WithPrefix(), clientv3.WithLimit(1)),
		clientv3.OpGet("/tx/", clientv3.WithPrefix(), clientv3.WithCountOnly()),
	).Commit()
	if err != nil {
		t.Fatal(err)
	}
	rev := resp.Header.Revision

	kvs := func(i int) string {
		var out []string
		for _, kv := range resp.Responses[i].GetResponseRange().Kvs {
			out = append(out, fmt.Sprintf("%s=%s@%d", kv.Key, kv.Value, kv.ModRevision))
		}
		return fmt.Sprint(out)
	}
	for i, want := range map[int]string{
		0: "[/tx/a=before@2]",
		1: "[/tx/a=before@2 /tx/c=before@3]",
		5: fmt.Sprintf("[/tx/a=after@%d]", rev),
		6: fmt.Sprintf("[/tx/a=after@%d /tx/b=after@%d]", rev, rev),
		7: fmt.Sprintf("[/tx/a=after@%d]", rev),
	} {
		if got := kvs(i); got != want {
			t.Errorf("op %d: got %s, want %s", i, got, want)
		}
	}
	if r := resp.Responses[7].GetResponseRange(); r.Count != 2 || !r.More {
		t.Errorf("op 7 (limit 1): count=%d more=%v, want count=2 more=true", r.Count, r.More)
	}
	if r := resp.Responses[8].GetResponseRange(); r.Count != 2 || len(r.Kvs) != 0 {
		t.Errorf("op 8 (count only): count=%d kvs=%d, want count=2 and no kvs", r.Count, len(r.Kvs))
	}
}

// TestRangeSort: etcd sorts a range by the requested target and order, and
// applies Limit after sorting — a descending read with a limit returns the
// last keys. A target other than KEY with no order sorts ascending.
func TestRangeSort(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	// Writes in this order give: key a<b<c, value c<a<b,
	// mod revision b<c<a, version b(1)<c(2)<a(3), create revision a<b<c.
	for _, kv := range [][2]string{
		{"/s/a", "x"}, {"/s/b", "z"}, {"/s/c", "a"}, {"/s/c", "a"}, {"/s/a", "x"}, {"/s/a", "m"},
	} {
		if _, err := cli.Put(ctx, kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}

	keys := func(resp *clientv3.GetResponse) string {
		var out []string
		for _, kv := range resp.Kvs {
			out = append(out, string(kv.Key[len("/s/"):]))
		}
		return fmt.Sprint(out)
	}
	for _, tc := range []struct {
		name  string
		opts  []clientv3.OpOption
		want  string
		count int64
		more  bool
	}{
		{"key desc", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend)}, "[c b a]", 3, false},
		{"key desc limit", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend), clientv3.WithLimit(2)}, "[c b]", 3, true},
		{"value asc", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend)}, "[c a b]", 3, false},
		{"value desc", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend)}, "[b a c]", 3, false},
		{"mod asc", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend)}, "[b c a]", 3, false},
		{"mod desc limit", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend), clientv3.WithLimit(1)}, "[a]", 3, true},
		{"version none", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByVersion, clientv3.SortNone)}, "[b c a]", 3, false},
		{"create desc", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend)}, "[c b a]", 3, false},
		{"keys only value desc", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend), clientv3.WithKeysOnly()}, "[b a c]", 3, false},
	} {
		resp, err := cli.Get(ctx, "/s/", append([]clientv3.OpOption{clientv3.WithPrefix()}, tc.opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		if got := keys(resp); got != tc.want || resp.Count != tc.count || resp.More != tc.more {
			t.Errorf("%s: got %s count=%d more=%v, want %s count=%d more=%v",
				tc.name, got, resp.Count, resp.More, tc.want, tc.count, tc.more)
		}
	}

	// A transaction's Range sorts too, including when it has to merge the
	// branch's own earlier writes into its snapshot.
	txn, err := cli.Txn(ctx).Then(
		clientv3.OpPut("/s/d", "0"),
		clientv3.OpGet("/s/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend), clientv3.WithLimit(2)),
	).Commit()
	if err != nil {
		t.Fatal(err)
	}
	r := txn.Responses[1].GetResponseRange()
	if got := keys((*clientv3.GetResponse)(r)); got != "[d c]" || r.Count != 4 || !r.More {
		t.Errorf("txn key desc limit: got %s count=%d more=%v, want [d c] count=4 more=true", got, r.Count, r.More)
	}
}

// TestRangeRevisionFilters: etcd drops keys outside the requested mod and
// create revision bounds before sorting and applying Limit. Count is taken
// before the filters, so it stays the number of keys in the range.
func TestRangeRevisionFilters(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	rev := map[string]int64{}
	for _, k := range []string{"a", "b", "c", "d", "b"} {
		resp, err := cli.Put(ctx, "/f/"+k, "v")
		if err != nil {
			t.Fatal(err)
		}
		rev[k] = resp.Header.Revision
	}
	// create: a<b<c<d; mod: a<c<d<b.
	keys := func(kvs []*mvccpb.KeyValue) string {
		var out []string
		for _, kv := range kvs {
			out = append(out, string(kv.Key[len("/f/"):]))
		}
		return fmt.Sprint(out)
	}
	for _, tc := range []struct {
		name string
		opts []clientv3.OpOption
		want string
		more bool
	}{
		{"min mod", []clientv3.OpOption{clientv3.WithMinModRev(rev["c"])}, "[b c d]", false},
		{"max mod", []clientv3.OpOption{clientv3.WithMaxModRev(rev["c"])}, "[a c]", false},
		{"min create", []clientv3.OpOption{clientv3.WithMinCreateRev(rev["c"])}, "[c d]", false},
		{"max create", []clientv3.OpOption{clientv3.WithMaxCreateRev(rev["a"] + 1)}, "[a b]", false},
		{"min mod limit", []clientv3.OpOption{clientv3.WithMinModRev(rev["c"]), clientv3.WithLimit(2)}, "[b c]", true},
		{"min mod limit fits", []clientv3.OpOption{clientv3.WithMinModRev(rev["d"]), clientv3.WithLimit(2)}, "[b d]", false},
		{"max create sorted desc", []clientv3.OpOption{clientv3.WithMaxCreateRev(rev["c"]),
			clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend)}, "[b c a]", false},
	} {
		resp, err := cli.Get(ctx, "/f/", append([]clientv3.OpOption{clientv3.WithPrefix()}, tc.opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		if got := keys(resp.Kvs); got != tc.want || resp.Count != 4 || resp.More != tc.more {
			t.Errorf("%s: got %s count=%d more=%v, want %s count=4 more=%v",
				tc.name, got, resp.Count, resp.More, tc.want, tc.more)
		}
	}

	// A single-key Get is filtered the same way.
	if resp, err := cli.Get(ctx, "/f/a", clientv3.WithMinModRev(rev["c"])); err != nil {
		t.Fatal(err)
	} else if len(resp.Kvs) != 0 || resp.Count != 1 {
		t.Errorf("single key min mod: got %s count=%d, want [] count=1", keys(resp.Kvs), resp.Count)
	}

	// A transaction's Range filters its merged result too.
	txn, err := cli.Txn(ctx).Then(
		clientv3.OpPut("/f/e", "v"),
		clientv3.OpGet("/f/", clientv3.WithPrefix(), clientv3.WithMinCreateRev(rev["d"])),
	).Commit()
	if err != nil {
		t.Fatal(err)
	}
	if r := txn.Responses[1].GetResponseRange(); keys(r.Kvs) != "[d e]" || r.Count != 5 {
		t.Errorf("txn min create: got %s count=%d, want [d e] count=5", keys(r.Kvs), r.Count)
	}
}

// TestWatchBelowCompactionReportsErrCompacted: etcd acknowledges a watch
// that starts below the compaction revision and then cancels it with the
// compact revision, which clientv3 reports as ErrCompacted.
func TestWatchBelowCompactionReportsErrCompacted(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx := parityCtx(t)

	var last int64
	for i := 0; i < 3; i++ {
		resp, err := cli.Put(ctx, "/c", fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		last = resp.Header.Revision
	}
	if _, err := cli.Compact(ctx, last); err != nil {
		t.Fatal(err)
	}

	resp := <-cli.Watch(ctx, "/c", clientv3.WithRev(2))
	if !errors.Is(resp.Err(), rpctypes.ErrCompacted) || resp.CompactRevision != last {
		t.Fatalf("watch below compaction: err=%v compactRevision=%d, want ErrCompacted at %d", resp.Err(), resp.CompactRevision, last)
	}
}
