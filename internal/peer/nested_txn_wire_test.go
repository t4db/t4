package peer_test

import (
	"testing"

	"github.com/t4db/t4/internal/peer"
)

// A forwarded txn is stamped with the lowest wire version that can carry it,
// not the newest this binary knows. The leader's gate is an exact match on
// the versions it understands, so stamping every request with the newest
// would make a leader one release behind refuse all of a new follower's
// writes instead of only the ones using a feature it lacks.
func TestForwardWireVersionIsPerRequest(t *testing.T) {
	codec := peer.Codec{}
	put := peer.TxnOpMsg{Type: 0, Key: "/k", Value: []byte("v")}

	for _, tc := range []struct {
		name string
		req  *peer.ForwardRequest
		want uint8
	}{
		{"plain put", &peer.ForwardRequest{Op: peer.ForwardPut, Key: "/k"}, 2},
		{"flat txn", &peer.ForwardRequest{
			Op:     peer.ForwardTxn,
			TxnReq: &peer.TxnReqMsg{Success: []peer.TxnOpMsg{put}},
		}, 2},
		{"nested in success", &peer.ForwardRequest{
			Op: peer.ForwardTxn,
			TxnReq: &peer.TxnReqMsg{Success: []peer.TxnOpMsg{{
				Type: peer.TxnOpNested,
				Txn:  &peer.TxnReqMsg{Success: []peer.TxnOpMsg{put}},
			}}},
		}, 3},
		{"nested in failure", &peer.ForwardRequest{
			Op: peer.ForwardTxn,
			TxnReq: &peer.TxnReqMsg{
				Success: []peer.TxnOpMsg{put},
				Failure: []peer.TxnOpMsg{{
					Type: peer.TxnOpNested,
					Txn:  &peer.TxnReqMsg{Success: []peer.TxnOpMsg{put}},
				}},
			},
		}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := codec.Marshal(tc.req)
			if err != nil {
				t.Fatal(err)
			}
			var got peer.ForwardRequest
			if err := codec.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if got.WireVersion != tc.want {
				t.Fatalf("wire version = %d, want %d", got.WireVersion, tc.want)
			}
		})
	}
}

// The nested transaction itself has to survive the round trip, or the leader
// would evaluate a branch that is missing its conditions.
func TestNestedTxnRoundTrip(t *testing.T) {
	codec := peer.Codec{}
	req := &peer.ForwardRequest{
		Op: peer.ForwardTxn,
		TxnReq: &peer.TxnReqMsg{
			Conditions: []peer.TxnCondMsg{{Key: "/leader", Target: 3, Value: []byte("node1")}},
			Failure: []peer.TxnOpMsg{{
				Type: peer.TxnOpNested,
				Txn: &peer.TxnReqMsg{
					Conditions: []peer.TxnCondMsg{{Key: "/leader", Target: 2}},
					Success: []peer.TxnOpMsg{
						{Type: 0, Key: "/leader", Value: []byte("node1"), Lease: 7},
					},
				},
			}},
		},
	}
	data, err := codec.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var got peer.ForwardRequest
	if err := codec.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	inner := got.TxnReq.Failure[0].Txn
	if inner == nil {
		t.Fatal("nested transaction was dropped")
	}
	if len(inner.Conditions) != 1 || string(inner.Conditions[0].Key) != "/leader" {
		t.Fatalf("nested conditions = %+v", inner.Conditions)
	}
	if len(inner.Success) != 1 || inner.Success[0].Lease != 7 {
		t.Fatalf("nested ops = %+v", inner.Success)
	}
}
