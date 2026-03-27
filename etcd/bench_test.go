package etcd_test

import (
	"context"
	"fmt"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/makhov/strata"
)

func benchNode(b *testing.B) (*strata.Node, *clientv3.Client) {
	b.Helper()
	node, err := strata.Open(strata.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("strata.Open: %v", err)
	}
	b.Cleanup(func() { node.Close() })
	endpoint := startEtcdServer(b, node)
	cli := newEtcdClient(b, endpoint)
	return node, cli
}

func BenchmarkPut(b *testing.B) {
	_, cli := benchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("/bench/put/%d", i), "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	_, cli := benchNode(b)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/bench/get/key", "value"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Get(ctx, "/bench/get/key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxnCreate(b *testing.B) {
	_, cli := benchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("/bench/create/%d", i)
		cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "v")).
			Commit()
	}
}

func BenchmarkWatch(b *testing.B) {
	_, cli := benchNode(b)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	wch := cli.Watch(ctx, "/bench/watch/", clientv3.WithPrefix())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cli.Put(ctx, fmt.Sprintf("/bench/watch/%d", i), "v")
		<-wch
	}
}
