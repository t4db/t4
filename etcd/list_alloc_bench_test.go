package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// BenchmarkRangeLargeList measures one unpaginated Range over a collection of
// Kubernetes-sized values, including the protobuf encoding gRPC performs to
// send it. Allocations per call relative to the data returned show how many
// times a list copies its result.
func BenchmarkRangeLargeList(b *testing.B) {
	const (
		prefix  = "/registry/configmaps/default/"
		keys    = 5000
		valSize = 4096
		batch   = 100
	)
	ctx := context.Background()
	node, err := t4.Open(t4.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("t4.Open: %v", err)
	}
	b.Cleanup(func() { _ = node.Close() })

	value := make([]byte, valSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	// The collection, and an unrelated one of the same size elsewhere in the
	// keyspace, as real stores hold many resource types.
	for _, p := range []string{prefix, "/registry/secrets/default/"} {
		for i := 0; i < keys; i += batch {
			ops := make([]t4.TxnOp, 0, batch)
			for j := i; j < i+batch && j < keys; j++ {
				ops = append(ops, t4.TxnOp{Type: t4.TxnPut, Key: fmt.Sprintf("%sobj-%06d", p, j), Value: value})
			}
			if _, err := node.Txn(ctx, t4.TxnRequest{Success: ops}); err != nil {
				b.Fatalf("Txn: %v", err)
			}
		}
	}
	srv := t4etcd.New(node, nil, nil)
	req := &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: prefixEnd(prefix), Serializable: true}

	// Updates after this revision make a read at it go through history.
	head, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: req.Key, Serializable: true})
	if err != nil {
		b.Fatalf("Range: %v", err)
	}
	oldRev := head.Header.Revision
	for i := 0; i < keys/10; i++ {
		if _, err := node.Put(ctx, fmt.Sprintf("%sobj-%06d", prefix, i*10), value, 0); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	b.Run("range@old-rev", func(b *testing.B) {
		b.ReportAllocs()
		at := &etcdserverpb.RangeRequest{Key: req.Key, RangeEnd: req.RangeEnd, Serializable: true, Revision: oldRev}
		for i := 0; i < b.N; i++ {
			resp, err := srv.Range(ctx, at)
			if err != nil {
				b.Fatalf("Range: %v", err)
			}
			if len(resp.Kvs) != keys {
				b.Fatalf("got %d kvs, want %d", len(resp.Kvs), keys)
			}
		}
	})
	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		cnt := &etcdserverpb.RangeRequest{Key: req.Key, RangeEnd: req.RangeEnd, Serializable: true, CountOnly: true}
		for i := 0; i < b.N; i++ {
			if _, err := srv.Range(ctx, cnt); err != nil {
				b.Fatalf("Range: %v", err)
			}
		}
	})
	b.Run("count@old-rev", func(b *testing.B) {
		b.ReportAllocs()
		cnt := &etcdserverpb.RangeRequest{Key: req.Key, RangeEnd: req.RangeEnd, Serializable: true, CountOnly: true, Revision: oldRev}
		for i := 0; i < b.N; i++ {
			if _, err := srv.Range(ctx, cnt); err != nil {
				b.Fatalf("Range: %v", err)
			}
		}
	})

	for _, encode := range []bool{false, true} {
		name := "range"
		if encode {
			name = "range+encode"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			var data int
			for i := 0; i < b.N; i++ {
				resp, err := srv.Range(ctx, req)
				if err != nil {
					b.Fatalf("Range: %v", err)
				}
				if len(resp.Kvs) != keys {
					b.Fatalf("got %d kvs, want %d", len(resp.Kvs), keys)
				}
				if encode {
					out, err := proto.Marshal(resp)
					if err != nil {
						b.Fatal(err)
					}
					data = len(out)
				}
			}
			if encode {
				b.ReportMetric(float64(data)/(1<<20), "MiB-resp")
			}
		})
	}
}
