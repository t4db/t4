package etcd

import (
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/t4db/t4"
)

// TestProtoBatchMatchesSingleConversion pins that messages built from a batch
// are those kvToProto builds, and stay intact as the batch moves on to new
// chunks and key buffers.
func TestProtoBatchMatchesSingleConversion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		batch func([]*t4.KeyValue) *protoBatch
	}{
		{"range", newRangeBatch},
		{"watch", func([]*t4.KeyValue) *protoBatch { return newProtoBatch(1, watchProtoChunkMax) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var kvs []*t4.KeyValue
			for i := range 300 { // spans several chunks and key buffers
				kvs = append(kvs, &t4.KeyValue{
					Key:            fmt.Sprintf("/registry/pods/ns/pod-%d-%s", i, string(make([]byte, i%97))),
					Value:          []byte(fmt.Sprintf("v%d", i)),
					Revision:       int64(100 + i),
					CreateRevision: int64(50 + i),
					Version:        int64(i % 3),
					Lease:          int64(i % 2),
				})
			}
			b := tc.batch(kvs)
			got := make([]*mvccpb.KeyValue, len(kvs))
			for i, kv := range kvs {
				got[i] = b.kv(kv)
			}
			for i, kv := range kvs {
				want := kvToProto(kv)
				if string(got[i].Key) != string(want.Key) || string(got[i].Value) != string(want.Value) ||
					got[i].ModRevision != want.ModRevision || got[i].CreateRevision != want.CreateRevision ||
					got[i].Version != want.Version || got[i].Lease != want.Lease {
					t.Fatalf("kv %d: got %v, want %v", i, got[i], want)
				}
			}

			// A key's capacity ends at its length: appending to it must
			// not overwrite the next key in the shared buffer.
			next := string(got[1].Key)
			_ = append(got[0].Key, "overwrite"...)
			if string(got[1].Key) != next {
				t.Fatalf("appending to one key changed the next: %q, want %q", got[1].Key, next)
			}
		})
	}
}

// TestProtoBatchEvents pins that batched watch events match etcd's shapes: a
// put carries the full KeyValue, a delete a tombstone with only the key and
// revision, and PrevKv when present.
func TestProtoBatchEvents(t *testing.T) {
	b := newProtoBatch(1, watchProtoChunkMax)
	prev := &t4.KeyValue{Key: "/k", Value: []byte("old"), Revision: 5, CreateRevision: 3, Version: 2}
	put := b.event(t4.Event{Type: t4.EventPut, KV: &t4.KeyValue{Key: "/k", Value: []byte("new"), Revision: 6, CreateRevision: 3, Version: 3}, PrevKV: prev})
	del := b.event(t4.Event{Type: t4.EventDelete, KV: &t4.KeyValue{Key: "/k", Value: []byte("ignored"), Revision: 7, CreateRevision: 3, Version: 4}, PrevKV: prev})

	if put.Type != mvccpb.PUT || string(put.Kv.Value) != "new" || put.Kv.Version != 3 || put.Kv.ModRevision != toEtcdRevision(6) {
		t.Errorf("put event = %v", put)
	}
	if put.PrevKv == nil || string(put.PrevKv.Value) != "old" {
		t.Errorf("put PrevKv = %v", put.PrevKv)
	}
	if del.Type != mvccpb.DELETE || string(del.Kv.Key) != "/k" || del.Kv.ModRevision != toEtcdRevision(7) ||
		del.Kv.Value != nil || del.Kv.Version != 0 || del.Kv.CreateRevision != 0 {
		t.Errorf("delete tombstone = %v", del.Kv)
	}
	if del.PrevKv == nil || string(del.PrevKv.Value) != "old" {
		t.Errorf("delete PrevKv = %v", del.PrevKv)
	}
}
