package etcd_test

import (
	"context"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// nestedLeaderTxn is the transaction patroni issues on every leader refresh
// once its lease has changed: keep the leader key if it still holds my value,
// and otherwise take it only if nobody else has.
func nestedLeaderTxn(key, value string, lease int64) *etcdserverpb.TxnRequest {
	put := &etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{
				Key: []byte(key), Value: []byte(value), Lease: lease,
			},
		},
	}
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(key),
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte(value)},
		}},
		Success: []*etcdserverpb.RequestOp{put},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         []byte(key),
						Target:      etcdserverpb.Compare_CREATE,
						Result:      etcdserverpb.Compare_EQUAL,
						TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
					}},
					Success: []*etcdserverpb.RequestOp{put},
				},
			},
		}},
	}
}

// innerSucceeded reads the result the way patroni does.
func innerSucceeded(resp *etcdserverpb.TxnResponse) bool {
	if resp.Succeeded {
		return true
	}
	if len(resp.Responses) == 0 {
		return false
	}
	return resp.Responses[0].GetResponseTxn().GetSucceeded()
}

func TestNestedTxnLeaderHandover(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	const key = "/service/pg/leader"

	// Nobody holds the key: the outer compare fails, the nested one takes it.
	resp, err := srv.Txn(ctx, nestedLeaderTxn(key, "node1", 0))
	if err != nil {
		t.Fatalf("txn on empty key: %v", err)
	}
	if resp.Succeeded {
		t.Error("outer compare matched an absent key")
	}
	if !innerSucceeded(resp) {
		t.Fatalf("nested txn did not take the key: %+v", resp)
	}
	if got := get(t, srv, key); got != "node1" {
		t.Fatalf("leader key = %q, want node1", got)
	}

	// node1 still holds it: the outer compare matches and no nesting runs.
	resp, err = srv.Txn(ctx, nestedLeaderTxn(key, "node1", 0))
	if err != nil {
		t.Fatalf("txn as holder: %v", err)
	}
	if !resp.Succeeded {
		t.Fatalf("outer compare did not match own value: %+v", resp)
	}
	if resp.Responses[0].GetResponsePut() == nil {
		t.Errorf("want a put response, got %+v", resp.Responses[0])
	}

	// node2 must lose: the outer compare fails on the value, the nested one
	// on the key already existing. The key must not change hands.
	resp, err = srv.Txn(ctx, nestedLeaderTxn(key, "node2", 0))
	if err != nil {
		t.Fatalf("txn as challenger: %v", err)
	}
	if innerSucceeded(resp) {
		t.Fatalf("challenger took a held leader key: %+v", resp)
	}
	if got := get(t, srv, key); got != "node1" {
		t.Fatalf("leader key = %q after a lost race, want node1", got)
	}
}

// TestNestedTxnBranchNotTakenDoesNotWrite is the atomicity property: the
// nested branch that is not selected must leave nothing behind.
func TestNestedTxnBranchNotTakenDoesNotWrite(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	put(t, srv, "/guard", "set")

	_, err := srv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte("/guard"),
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("set")},
		}},
		// Taken: writes /taken.
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         []byte("/guard"),
						Target:      etcdserverpb.Compare_VERSION,
						Result:      etcdserverpb.Compare_GREATER,
						TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
					}},
					Success: []*etcdserverpb.RequestOp{putOp("/taken", "yes")},
					Failure: []*etcdserverpb.RequestOp{putOp("/inner-skipped", "no")},
				},
			},
		}},
		// Not taken: must write nothing at all.
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{putOp("/outer-skipped", "no")},
					Failure: []*etcdserverpb.RequestOp{putOp("/outer-skipped2", "no")},
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if got := get(t, srv, "/taken"); got != "yes" {
		t.Errorf("/taken = %q, want yes", got)
	}
	for _, k := range []string{"/inner-skipped", "/outer-skipped", "/outer-skipped2"} {
		if got := get(t, srv, k); got != "" {
			t.Errorf("%s = %q, want absent", k, got)
		}
	}
}

// TestNestedTxnReadsSeeThePreTxnSnapshot covers the response tree when a
// nested branch both writes and reads: as in etcd, ops run against the state
// the conditions were evaluated on, plus the writes of the ops before them.
// A put nested inside the taken branch still counts as a write, so a read
// after it must see it while a read before it must not.
func TestNestedTxnReadsSeeThePreTxnSnapshot(t *testing.T) {
	srv := newServer(t)
	put(t, srv, "/k", "old")

	rangeOp := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)},
			},
		}
	}

	resp, err := srv.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			rangeOp("/k"), // before the nested write: must read "old"
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						putOp("/k", "new"),
						rangeOp("/k"), // after it, inside the nesting: "new"
					},
				},
			}},
			rangeOp("/k"), // after it, outside the nesting: "new"
		},
	})
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if len(resp.Responses) != 3 {
		t.Fatalf("want 3 responses, got %d", len(resp.Responses))
	}
	first := resp.Responses[0].GetResponseRange()
	if len(first.Kvs) != 1 || string(first.Kvs[0].Value) != "old" {
		t.Errorf("read before the nested put = %+v, want old", first.Kvs)
	}
	inner := resp.Responses[1].GetResponseTxn()
	if inner == nil || len(inner.Responses) != 2 {
		t.Fatalf("nested response = %+v", resp.Responses[1])
	}
	innerRead := inner.Responses[1].GetResponseRange()
	if len(innerRead.Kvs) != 1 || string(innerRead.Kvs[0].Value) != "new" {
		t.Errorf("read after the nested put = %+v, want new", innerRead.Kvs)
	}
	last := resp.Responses[2].GetResponseRange()
	if len(last.Kvs) != 1 || string(last.Kvs[0].Value) != "new" {
		t.Errorf("read outside after the nested put = %+v, want new", last.Kvs)
	}
}

// TestNestedTxnTooDeep refuses a chain deeper than t4.MaxTxnDepth with the
// error etcd uses for an oversized transaction.
func TestNestedTxnTooDeep(t *testing.T) {
	srv := newServer(t)

	build := func(depth int) *etcdserverpb.TxnRequest {
		inner := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{putOp("/deep", "v")}}
		for range depth {
			inner = &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: inner},
				}},
			}
		}
		return inner
	}

	if _, err := srv.Txn(context.Background(), build(t4.MaxTxnDepth-1)); err != nil {
		t.Fatalf("depth within the limit was refused: %v", err)
	}
	_, err := srv.Txn(context.Background(), build(t4.MaxTxnDepth+1))
	if err == nil {
		t.Fatal("over-deep txn was accepted")
	}
	if status.Code(err) != status.Code(rpctypes.ErrGRPCTooManyOps) {
		t.Errorf("error = %v, want %v", err, rpctypes.ErrGRPCTooManyOps)
	}
}

// get returns the value at key, or "" when it is absent.
func get(t *testing.T, srv *t4etcd.Server, key string) string {
	t.Helper()
	resp, err := srv.Range(context.Background(), &etcdserverpb.RangeRequest{Key: []byte(key)})
	if err != nil {
		t.Fatalf("Range(%q): %v", key, err)
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}

func putOp(key, value string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(value)},
		},
	}
}
