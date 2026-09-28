package etcd_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// TestStreamWorkersHeldByWatches checks that long-lived streams can't starve
// the server when they hold every RPC worker. A Watch keeps its worker for the
// stream's lifetime; once all workers are held, gRPC must fall back to a
// goroutine per RPC so unary calls and further streams are still served.
func TestStreamWorkersHeldByWatches(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	defer func() { _ = node.Close() }()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil, t4etcd.WithStreamWorkers(1))...)
	t4etcd.New(node, nil, nil).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	ep := lis.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Each client opens its own Watch stream; three streams outnumber the
	// single worker.
	const watchers = 3
	chans := make([]clientv3.WatchChan, watchers)
	for i := range chans {
		chans[i] = newEtcdClient(t, ep).Watch(ctx, "/workers/", clientv3.WithPrefix())
	}

	cli := newEtcdClient(t, ep)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("/workers/%d", i)
		if _, err := cli.Put(ctx, key, "v"); err != nil {
			t.Fatalf("Put %s with all workers held by watches: %v", key, err)
		}
		if _, err := cli.Get(ctx, key); err != nil {
			t.Fatalf("Get %s with all workers held by watches: %v", key, err)
		}
		for w, ch := range chans {
			select {
			case resp := <-ch:
				if err := resp.Err(); err != nil {
					t.Fatalf("watcher %d: %v", w, err)
				}
				if len(resp.Events) != 1 || string(resp.Events[0].Kv.Key) != key {
					t.Fatalf("watcher %d: got %v, want one event for %s", w, resp.Events, key)
				}
			case <-ctx.Done():
				t.Fatalf("watcher %d: no event for %s", w, key)
			}
		}
	}
}
