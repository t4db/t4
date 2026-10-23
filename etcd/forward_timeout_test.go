package etcd

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
)

// putServer answers every Put with err and counts them.
type putServer struct {
	etcdserverpb.UnimplementedKVServer
	err   error
	calls atomic.Int32
}

func (s *putServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.calls.Add(1)
	return nil, s.err
}

// putCalls returns how many Puts an etcd client sends for one Put that the
// server answers with err.
func putCalls(t *testing.T, err error) int32 {
	t.Helper()
	srv := &putServer{err: err}
	gs := grpc.NewServer()
	etcdserverpb.RegisterKVServer(gs, srv)
	lis, lerr := net.Listen("tcp", "127.0.0.1:0")
	if lerr != nil {
		t.Fatal(lerr)
	}
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	cli, cerr := clientv3.New(clientv3.Config{Endpoints: []string{lis.Addr().String()}, DialTimeout: 5 * time.Second})
	if cerr != nil {
		t.Fatal(cerr)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, perr := cli.Put(ctx, "/k", "v"); perr == nil {
		t.Fatal("Put succeeded against a server that fails it")
	}
	return srv.calls.Load()
}

// TestTimeoutIsNotSentAgain checks that the error a follower reports for a
// forward its peer connection cut short does not make etcd clients send the
// write again: the leader may have applied it. A Canceled status, which the
// forward itself fails with, does.
func TestTimeoutIsNotSentAgain(t *testing.T) {
	if n := putCalls(t, status.Error(codes.Canceled, "grpc: the client connection is closing")); n < 2 {
		t.Fatalf("precondition: etcd clients send a Put again after Canceled, sent %d", n)
	}
	timeout := kvError(fmt.Errorf("%w: forward cut short", t4.ErrTimeout))
	if n := putCalls(t, timeout); n != 1 {
		t.Fatalf("a Put answered with %v was sent %d times, want once", timeout, n)
	}
}
