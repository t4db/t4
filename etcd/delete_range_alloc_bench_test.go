package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// BenchmarkDeleteRangeCollection measures one prefix delete over a collection
// of Kubernetes-sized values — a namespace teardown or a CRD deletion.
//
// The delete needs key names only, so allocations per call should stay far
// below the bytes the collection holds. The prev-kv case is the exception: the
// client asked for every previous value back, so loading them is the contract.
func BenchmarkDeleteRangeCollection(b *testing.B) {
	const (
		prefix  = "/registry/widgets/default/"
		keys    = 5000
		valSize = 4096
		batch   = 100
	)
	ctx := context.Background()
	value := make([]byte, valSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	for _, prevKV := range []bool{false, true} {
		name := "delete"
		if prevKV {
			name = "delete+prevkv"
		}
		b.Run(name, func(b *testing.B) {
			node, err := t4.Open(t4.Config{DataDir: b.TempDir()})
			if err != nil {
				b.Fatalf("t4.Open: %v", err)
			}
			b.Cleanup(func() { _ = node.Close() })
			srv := t4etcd.New(node, nil, nil)

			seed := func() {
				for i := 0; i < keys; i += batch {
					ops := make([]t4.TxnOp, 0, batch)
					for j := i; j < i+batch && j < keys; j++ {
						ops = append(ops, t4.TxnOp{Type: t4.TxnPut, Key: fmt.Sprintf("%sobj-%06d", prefix, j), Value: value})
					}
					if _, err := node.Txn(ctx, t4.TxnRequest{Success: ops}); err != nil {
						b.Fatalf("Txn: %v", err)
					}
				}
			}

			req := &etcdserverpb.DeleteRangeRequest{
				Key:      []byte(prefix),
				RangeEnd: prefixEnd(prefix),
				PrevKv:   prevKV,
			}

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				seed()
				b.StartTimer()
				resp, err := srv.DeleteRange(ctx, req)
				if err != nil {
					b.Fatalf("DeleteRange: %v", err)
				}
				if resp.Deleted != keys {
					b.Fatalf("deleted %d, want %d", resp.Deleted, keys)
				}
			}
			b.ReportMetric(float64(keys*valSize)/(1<<20), "MiB-collection")
		})
	}
}
