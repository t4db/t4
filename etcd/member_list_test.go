package etcd_test

import (
	"context"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

func TestMemberListDiscovery(t *testing.T) {
	if _, err := newServer(t).MemberList(context.Background(), &etcdserverpb.MemberListRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unconfigured MemberList: %v", err)
	}
	for _, urls := range [][]string{{"https://t4.example.com:443"}, {"http://node-a:3379", "http://node-b:3379", "http://node-c:3379"}} {
		t.Run(urls[0], func(t *testing.T) {
			want := append([]string(nil), urls...)
			srv := newServer(t, t4etcd.WithAdvertiseClientURLs(urls))
			urls[0] = "http://mutated:1"
			rev := put(t, srv, "/discovery", "value")
			kv, err := srv.Range(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/discovery")})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 16 {
				wg.Go(func() {
					for range 10 {
						r, err := srv.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
						if err != nil {
							t.Error(err)
							return
						}
						if !reflect.DeepEqual(r.Header, kv.Header) || r.Header.Revision != rev {
							t.Errorf("MemberList header %v, want %v", r.Header, kv.Header)
						}
						if len(r.Members) != 1 {
							t.Errorf("members: %v", r.Members)
							return
						}
						m := r.Members[0]
						if m.ID != 1 || m.Name != "t4" || len(m.PeerURLs) != 0 || !reflect.DeepEqual(m.ClientURLs, want) {
							t.Errorf("member: %v, want client URLs %v", m, want)
						}
						m.ClientURLs[0] = "http://response-mutation:1"
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestClientEndpointSync(t *testing.T) {
	for _, configured := range []bool{true, false} {
		t.Run(map[bool]string{true: "configured", false: "unconfigured"}[configured], func(t *testing.T) {
			node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close() })
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			advertised := "http://" + lis.Addr().String()
			var opts []t4etcd.ServerOption
			if configured {
				opts = append(opts, t4etcd.WithAdvertiseClientURLs([]string{advertised}))
			}
			var memberCalls atomic.Int64
			gs := grpc.NewServer(append(t4etcd.NewServerOptions(nil, nil), grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				resp, err := handler(ctx, req)
				if info.FullMethod == "/etcdserverpb.Cluster/MemberList" && err == nil {
					memberCalls.Add(1)
				}
				return resp, err
			}))...)
			t4etcd.New(node, nil, nil, opts...).Register(gs)
			go func() { _ = gs.Serve(lis) }()
			t.Cleanup(gs.Stop)
			// Start with a different spelling to prove AutoSync really refreshes.
			initial := lis.Addr().String()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{initial}, DialTimeout: 2 * time.Second, AutoSyncInterval: 25 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cli.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			checkKV := func() {
				t.Helper()
				if _, err := cli.Put(ctx, "/sync", "value"); err != nil {
					t.Fatal(err)
				}
				r, err := cli.Get(ctx, "/sync")
				if err != nil {
					t.Fatal(err)
				}
				if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "value" {
					t.Fatalf("Get: %v", r)
				}
			}
			checkKV()
			if configured {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for memberCalls.Load() < 2 || !reflect.DeepEqual(cli.Endpoints(), []string{advertised}) {
					select {
					case <-ctx.Done():
						t.Fatal("AutoSync did not refresh endpoints")
					case <-ticker.C:
					}
				}
				checkKV()
			}
			for range 3 {
				err := cli.Sync(ctx)
				want := initial
				if configured {
					if err != nil {
						t.Fatal(err)
					}
					want = advertised
				} else if status.Code(err) != codes.Unimplemented {
					t.Fatalf("Sync: %v", err)
				}
				if !reflect.DeepEqual(cli.Endpoints(), []string{want}) {
					t.Fatalf("endpoints: %v, want %s", cli.Endpoints(), want)
				}
				checkKV()
			}
		})
	}
}
