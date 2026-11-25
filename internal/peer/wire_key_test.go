package peer_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/internal/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestWireKeyCompatibility(t *testing.T) {
	codec := peer.Codec{}
	for _, key := range []string{"", "/normal", "/clé/東京", "/\ufffd", "\x00", "\xfd", "\xff\x80", "\xe2\x82"} {
		t.Run(strings.ReplaceAll(key, "/", "_"), func(t *testing.T) {
			req := &peer.ForwardRequest{Key: peer.WireKey(key), WireVersion: 99}
			data, err := codec.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			if req.WireVersion != 99 {
				t.Fatal("codec mutated caller")
			}
			var got peer.ForwardRequest
			if err := codec.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if string(got.Key) != key || got.WireVersion != 2 {
				t.Fatalf("want %x/v2 got %x/v%d", key, got.Key, got.WireVersion)
			}
			var legacy struct {
				Key string `json:"key"`
			}
			err = codec.Unmarshal(data, &legacy)
			plain, _ := json.Marshal(key)
			encoded, _ := json.Marshal(peer.WireKey(key))
			if string(plain) == string(encoded) {
				if err != nil || legacy.Key != key {
					t.Fatalf("legacy UTF-8: %v %x", err, legacy.Key)
				}
			} else {
				var typeErr *json.UnmarshalTypeError
				if !errors.As(err, &typeErr) {
					t.Fatalf("legacy binary must reject object: %v", err)
				}
			}
		})
	}
	// These must remain distinct, including in response slices.
	original := &peer.ForwardResponse{DeletedKeys: []peer.WireKey{"\xfd", "\ufffd"}}
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var got peer.ForwardResponse
	if err := codec.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, &got) {
		t.Fatalf("distinct keys collapsed: %x", got.DeletedKeys)
	}
}

func TestWireKeyRejectMalformed(t *testing.T) {
	for _, input := range []string{`null`, `[]`, `1`, `true`, `{}`, `{"other":"/Q=="}`, `{"base64":"%"}`, `{"base64":null}`, `{"base64":[]}`, `{"base64":{}}`, `{"base64":"/Q==","extra":1}`, `{"base64":"/Q==","base64":"/w=="}`, `{"base64":"/R=="}`, `{"base64":"/Q==\n"}`} {
		var req peer.ForwardRequest
		if err := (peer.Codec{}).Unmarshal([]byte(`{"wire_version":2,"key":`+input+`}`), &req); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
}

// The legacy codec deliberately bypasses Codec's version stamp, as an old
// follower would. Malformed keys also travel through the actual gRPC decoder.
type legacyForwardCodec struct{}

func (legacyForwardCodec) Name() string                       { return "t4-bin" }
func (legacyForwardCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (legacyForwardCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type countingForwardHandler struct{ calls atomic.Int64 }

func (h *countingForwardHandler) HandleForward(context.Context, *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	h.calls.Add(1)
	return &peer.ForwardResponse{Succeeded: true}, nil
}

func TestWireKeyVersionGateGRPC(t *testing.T) {
	srv := peer.NewServer(100, 0, nil)
	handler := &countingForwardHandler{}
	srv.SetForwardHandler(handler)
	conn, err := grpc.NewClient(startServer(t, srv), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(legacyForwardCodec{})))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// A legacy sender's key may already have lost bytes, so anything carrying
	// key material is refused — whether the key is at the top level or only
	// inside a transaction. Version 3 is understood (nested txn ops), so the
	// unknown ones either side of the supported range stand in for it.
	for _, version := range []int{0, 1, 4, 255} {
		for _, req := range []map[string]any{
			{"op": peer.ForwardPut, "key": "\xfd"},
			{"op": peer.ForwardTxn, "txn": map[string]any{"success": []map[string]any{{"key": "\xfd"}}}},
		} {
			if version != 0 {
				req["wire_version"] = version
			}
			err := conn.Invoke(ctx, "/peer.WalStream/Forward", req, &peer.ForwardResponse{})
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "forward_wire_version_unsupported") {
				t.Fatalf("v%d op %v: %v", version, req["op"], err)
			}
		}
	}
	// A stamp this binary understands is still refused when the request needs
	// a newer one: a nested txn applied by a leader that ignored the nesting
	// would commit a transaction with a branch missing.
	nested := map[string]any{
		"wire_version": 2,
		"op":           peer.ForwardTxn,
		"txn": map[string]any{"failure": []map[string]any{
			{"type": peer.TxnOpNested, "txn": map[string]any{
				"success": []map[string]any{{"key": "/k"}},
			}},
		}},
	}
	err = conn.Invoke(ctx, "/peer.WalStream/Forward", nested, &peer.ForwardResponse{})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "forward_wire_version_unsupported") {
		t.Fatalf("nested txn stamped v2: %v", err)
	}

	malformed := map[string]any{"wire_version": 2, "key": map[string]any{"base64": "%"}}
	if err := conn.Invoke(ctx, "/peer.WalStream/Forward", malformed, &peer.ForwardResponse{}); err == nil {
		t.Fatal("malformed key accepted")
	}
	if _, err := srv.Forward(ctx, nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("nil request: %v", err)
	}
	if handler.calls.Load() != 0 {
		t.Fatal("rejected request reached handler")
	}
	// A legacy forward with no key material cannot carry a mangled key, and
	// no response to one carries a key either: a read index, a compaction and
	// a lease renewal keep working while the follower waits to be upgraded.
	keyless := []peer.ForwardOp{peer.ForwardGetRevision, peer.ForwardGetSequence, peer.ForwardCompact, peer.ForwardLeaseRenew, peer.ForwardLeaseRenewedAt}
	want := int64(0)
	for _, version := range []int{0, 1, 3, 255} {
		for _, op := range keyless {
			req := map[string]any{"op": op}
			if version != 0 {
				req["wire_version"] = version
			}
			if err := conn.Invoke(ctx, "/peer.WalStream/Forward", req, &peer.ForwardResponse{}); err != nil {
				t.Fatalf("v%d op %v: %v", version, op, err)
			}
			want++
			if got := handler.calls.Load(); got != want {
				t.Fatalf("v%d op %v: handler calls %d, want %d", version, op, got, want)
			}
		}
	}
	valid := map[string]any{"wire_version": 2, "key": "/valid", "op": peer.ForwardPut}
	if err := conn.Invoke(ctx, "/peer.WalStream/Forward", valid, &peer.ForwardResponse{}); err != nil {
		t.Fatal(err)
	}
	if handler.calls.Load() != want+1 {
		t.Fatal("valid request not dispatched")
	}
}

func FuzzWireKeyJSONRoundTrip(f *testing.F) {
	for _, key := range [][]byte{nil, {}, []byte("/normal"), []byte("/clé/東京"), []byte("\ufffd"), {0xfd}, {0xff, 0x80}, {0}, {0xe2, 0x82}} {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key []byte) {
		req := &peer.ForwardRequest{Key: peer.WireKey(key)}
		data, err := (peer.Codec{}).Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		var got peer.ForwardRequest
		if err := (peer.Codec{}).Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if string(got.Key) != string(key) {
			t.Fatalf("want %x got %x", key, got.Key)
		}
	})
}
