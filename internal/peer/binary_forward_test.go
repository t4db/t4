package peer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/t4db/t4/internal/peer"
)

// Exercise every key-bearing leaf through an actual peer gRPC roundtrip.
func TestBinaryKeyForward(t *testing.T) {
	keys := []string{"/juicefs\xfdsetting", "\x80\xff\xe2\x82", "/k\x00x", "/normal/key", "/clé/東京", "/\ufffd/valid", ""}
	for i, key := range keys {
		for _, op := range []peer.ForwardOp{peer.ForwardPut, peer.ForwardCreate, peer.ForwardUpdate, peer.ForwardDeleteIfRevision, peer.ForwardMetaPut, peer.ForwardMetaDelete, peer.ForwardTxn} {
			t.Run(fmt.Sprintf("%d/%d", i, op), func(t *testing.T) {
				req := &peer.ForwardRequest{Op: op, Key: peer.WireKey(key), Value: []byte{0xff, 0, 0xfd}, Revision: 42, Lease: 9}
				if op == peer.ForwardTxn {
					req.TxnReq = &peer.TxnReqMsg{
						Guards:     []peer.TxnCondMsg{{Key: peer.WireKey(key + "\x00\xfd")}},
						Conditions: []peer.TxnCondMsg{{Key: peer.WireKey(key + "\x01\xfd")}},
						Success:    []peer.TxnOpMsg{{Key: peer.WireKey(key + "\x02\xfd")}},
						Failure:    []peer.TxnOpMsg{{Key: peer.WireKey(key + "\x03\xfd")}},
					}
				}
				expected := &peer.ForwardResponse{Revision: 43, Succeeded: true, OldKV: &peer.KVMsg{Key: peer.WireKey(key), Value: req.Value, Revision: 42, CreateRevision: 2, PrevRevision: 3, Version: 4, Lease: 9}, ErrCode: "example", ErrMsg: "unchanged"}
				expected.DeletedKeys = []peer.WireKey{peer.WireKey(key), peer.WireKey(key + "\xff")}
				handler := &stubForwardHandler{resp: expected}
				srv := peer.NewServer(1000, 0, nil)
				srv.SetForwardHandler(handler)
				cli := peer.NewClient(startServer(t, srv), "binary", 3, nil, nil, nil)
				defer cli.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				got, err := cli.ForwardWrite(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				if handler.lastReq == nil {
					t.Fatal("handler not invoked")
				}
				// The version stamp is a transport detail; compare all semantic fields.
				normalized := *handler.lastReq
				normalized.WireVersion = 0
				if !reflect.DeepEqual(req, &normalized) {
					t.Errorf("request byte mismatch: key want %x got %x; txn want %#v got %#v", key, handler.lastReq.Key, req.TxnReq, handler.lastReq.TxnReq)
				}
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("response byte mismatch: key want %x got %x; deleted want %x got %x", key, got.OldKV.Key, expected.DeletedKeys, got.DeletedKeys)
				}
			})
		}
	}
}

// Verify absent optional fields remain absent across the same wire boundary.
func TestBinaryForwardOptionalFields(t *testing.T) {
	codec := peer.Codec{}
	for _, original := range []any{&peer.ForwardRequest{}, &peer.ForwardResponse{}} {
		data, err := codec.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"txn", "old_kv", "deleted_keys"} {
			if _, ok := fields[field]; ok {
				t.Fatalf("unexpected optional field %s", field)
			}
		}
	}
}
