package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// BenchmarkPaginateCollection walks a whole collection the way kube-apiserver
// does: repeated Range calls with a limit, each continuing from the previous
// page's last key. Reported per full walk, not per page.
func BenchmarkPaginateCollection(b *testing.B) {
	const (
		prefix     = "/registry/pods/default/"
		collection = 20000
		decoys     = 20000 // unrelated keys elsewhere in the keyspace
		limit      = 500
	)

	ctx := context.Background()
	node, err := t4.Open(t4.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("t4.Open: %v", err)
	}
	b.Cleanup(func() { _ = node.Close() })

	for i := 0; i < collection; i++ {
		if _, err := node.Put(ctx, fmt.Sprintf("%spod-%06d", prefix, i), []byte("value"), 0); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	for i := 0; i < decoys; i++ {
		if _, err := node.Put(ctx, fmt.Sprintf("/registry/configmaps/cm-%06d", i), []byte("value"), 0); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}

	srv := t4etcd.New(node, nil, nil)
	end := prefixEnd(prefix)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(prefix)
		seen := 0
		for {
			resp, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
				Key: key, RangeEnd: end, Limit: limit,
			})
			if err != nil {
				b.Fatalf("Range: %v", err)
			}
			seen += len(resp.Kvs)
			if !resp.More {
				break
			}
			key = append(append([]byte{}, resp.Kvs[len(resp.Kvs)-1].Key...), 0)
		}
		if seen != collection {
			b.Fatalf("paged %d keys, want %d", seen, collection)
		}
	}
}
