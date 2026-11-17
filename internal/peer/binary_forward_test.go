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

// Use JSON fixtures so these regressions compile unchanged with legacy string
// fields and the byte-preserving wire type. The RPC itself always uses Codec.
func TestBinaryKeyForward(t *testing.T) {
	keys := []string{"/juicefs\xfdsetting", "\x80\xff\xe2\x82", "/k\x00x", "/normal/key", "/clé/東京", "/\ufffd/valid", ""}
	for i, key := range keys {
		for _, op := range []peer.ForwardOp{peer.ForwardPut, peer.ForwardCreate, peer.ForwardUpdate, peer.ForwardDeleteIfRevision, peer.ForwardMetaPut, peer.ForwardMetaDelete, peer.ForwardTxn} {
			t.Run(fmt.Sprintf("%d/%d", i, op), func(t *testing.T) {
				req := &peer.ForwardRequest{Op: op, Value: []byte{0xff, 0, 0xfd}, Revision: 42, Lease: 9}
				// Reflection sets the underlying string without passing invalid bytes
				// through encoding/json during fixture construction.
				reflect.ValueOf(req).Elem().FieldByName("Key").SetString(key)
				if op == peer.ForwardTxn {
					req.TxnReq = &peer.TxnReqMsg{Guards: make([]peer.TxnCondMsg, 1), Conditions: make([]peer.TxnCondMsg, 1), Success: make([]peer.TxnOpMsg, 1), Failure: make([]peer.TxnOpMsg, 1)}
					for j, field := range []any{&req.TxnReq.Guards[0], &req.TxnReq.Conditions[0], &req.TxnReq.Success[0], &req.TxnReq.Failure[0]} {
						reflect.ValueOf(field).Elem().FieldByName("Key").SetString(key + string([]byte{byte(j), 0xfd}))
					}
				}
				expected := &peer.ForwardResponse{Revision: 43, Succeeded: true, OldKV: &peer.KVMsg{Value: req.Value, Revision: 42, CreateRevision: 2, PrevRevision: 3, Version: 4, Lease: 9}, ErrCode: "example", ErrMsg: "unchanged"}
				reflect.ValueOf(expected.OldKV).Elem().FieldByName("Key").SetString(key)
				deleted := reflect.ValueOf(expected).Elem().FieldByName("DeletedKeys")
				deleted.Set(reflect.MakeSlice(deleted.Type(), 2, 2))
				deleted.Index(0).SetString(key)
				deleted.Index(1).SetString(key + "\xff")
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
				v := reflect.ValueOf(&normalized).Elem().FieldByName("WireVersion")
				if v.IsValid() {
					v.SetUint(0)
				}
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
