package etcd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/pkg/object"
)

// TestNestedTxnForwardedToLeader covers the path a nested transaction takes
// from a follower: the whole tree goes on the wire and the leader evaluates
// it, because only the leader can decide the branches atomically.
//
// It also covers the half of the wire gate that matters for a rolling
// upgrade: a leader that predates nested ops must refuse the forward rather
// than apply a transaction with a branch missing, while the follower's
// ordinary writes keep working against it.
func TestNestedTxnForwardedToLeader(t *testing.T) {
	_, srv := twoNodeFollower(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	runNestedForwardChecks(t, ctx, srv)
}

// twoNodeFollower brings up a two-node cluster over a shared in-memory object
// store and returns the follower together with an etcd Server in front of it,
// so every write it takes is forwarded to the leader.
func twoNodeFollower(t *testing.T) (*t4.Node, *Server) {
	t.Helper()
	shared := object.NewMem()
	nodes := make([]*t4.Node, 2)
	for i := range nodes {
		n, err := t4.Open(t4.Config{
			DataDir:        t.TempDir(),
			ObjectStore:    shared,
			NodeID:         fmt.Sprintf("node-%d", i),
			PeerListenAddr: freeAddr(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}

	var follower *t4.Node
	deadline := time.Now().Add(10 * time.Second)
	for follower == nil {
		for i, n := range nodes {
			if n.IsLeader() {
				follower = nodes[1-i]
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	return follower, New(follower, nil, nil)
}

func runNestedForwardChecks(t *testing.T, ctx context.Context, srv *Server) {
	t.Helper()

	const key = "/service/pg/leader"
	txn := func(value string) *etcdserverpb.TxnRequest {
		put := &etcdserverpb.RequestOp{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(value)},
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

	// The nested branch takes the key, through the leader.
	resp, err := srv.Txn(ctx, txn("node1"))
	if err != nil {
		t.Fatalf("forwarded nested txn: %v", err)
	}
	if resp.Succeeded {
		t.Error("outer compare matched an absent key")
	}
	if !resp.Responses[0].GetResponseTxn().GetSucceeded() {
		t.Fatalf("nested result did not survive the round trip: %+v", resp)
	}
	got, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "node1" {
		t.Fatalf("leader key = %+v, want node1", got.Kvs)
	}

	// A challenger must lose through the forward path too.
	resp, err = srv.Txn(ctx, txn("node2"))
	if err != nil {
		t.Fatalf("forwarded challenger txn: %v", err)
	}
	if resp.Succeeded || resp.Responses[0].GetResponseTxn().GetSucceeded() {
		t.Fatalf("challenger took a held key: %+v", resp)
	}

	// Against a leader from before nested ops: the nested txn fails closed,
	// and a plain write over the same connection still goes through.
	testhook.V1Leader.Store(true)
	t.Cleanup(func() { testhook.V1Leader.Store(false) })

	if _, err := srv.Txn(ctx, txn("node3")); err == nil {
		t.Fatal("a v1 leader accepted a nested txn")
	} else if !strings.Contains(err.Error(), "unknown to a v1.1 leader") {
		t.Errorf("want a version rejection, got %v", err)
	}
	if _, err := srv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/plain"), Value: []byte("v"),
	}); err != nil {
		t.Errorf("a plain write to a v1 leader must still work, got %v", err)
	}
}

// TestBranchRangeDeleteForwardedToLeader is the same contract for the other
// feature wire version 3 carries. It matters more than nesting does for a
// leader that predates it: a range delete's op type is an ordinary single-key
// delete, so such a leader would decode the op, ignore the range end, and
// delete one key where the branch asked for a range.
func TestBranchRangeDeleteForwardedToLeader(t *testing.T) {
	follower, srv := twoNodeFollower(t)
	_ = follower
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	for _, k := range []string{"/r/1", "/r/2", "/keep"} {
		if _, err := srv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(k), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}

	del := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte("/r/"), RangeEnd: []byte("/r0"),
				},
			},
		}},
	}
	resp, err := srv.Txn(ctx, del)
	if err != nil {
		t.Fatalf("forwarded range delete: %v", err)
	}
	if got := resp.Responses[0].GetResponseDeleteRange().GetDeleted(); got != 2 {
		t.Errorf("deleted = %d, want 2", got)
	}
	got, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/keep")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 1 {
		t.Errorf("/keep was removed by a range that excludes it")
	}

	testhook.V1Leader.Store(true)
	t.Cleanup(func() { testhook.V1Leader.Store(false) })
	if _, err := srv.Txn(ctx, del); err == nil {
		t.Fatal("a v1 leader accepted a branch range delete")
	} else if !strings.Contains(err.Error(), "unknown to a v1.1 leader") {
		t.Errorf("want a version rejection, got %v", err)
	}
}
