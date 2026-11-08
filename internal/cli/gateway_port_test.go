package cli

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/internal/gateway"
)

// sharedPort starts a gRPC server and the JSON gateway on one listener the way
// serveClientPort does for `t4 run`, and returns the address.
//
// tlsConf nil means a plaintext port, which is split by cmux; non-nil means the
// split moves above HTTP, which is the arrangement worth a regression test
// because gRPC is then served through Go's HTTP/2 stack.
func sharedPort(tb testing.TB, tlsConf *tls.Config) string {
	tb.Helper()

	node, err := t4.Open(t4.Config{DataDir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open node: %v", err)
	}
	tb.Cleanup(func() { _ = node.Close() })

	// No grpc.Creds: with a gateway, TLS is terminated before gRPC, exactly as
	// in the run command.
	grpcSrv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(grpcSrv)

	loopback := gateway.NewLoopback()
	go func() { _ = grpcSrv.Serve(loopback) }()
	conn, err := loopback.ClientConn()
	if err != nil {
		tb.Fatalf("loopback conn: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handler, err := gateway.New(ctx, conn, gateway.Config{})
	if err != nil {
		tb.Fatalf("gateway: %v", err)
	}
	httpSrv := &http.Server{Handler: handler, ReadHeaderTimeout: time.Minute}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	tb.Cleanup(func() {
		cancel()
		_ = httpSrv.Close()
		grpcSrv.Stop()
		_ = conn.Close()
		_ = loopback.Close()
		_ = lis.Close()
	})

	serveClientPort(grpcSrv, httpSrv, lis, nil, tlsConf)
	return lis.Addr().String()
}

func gwEnc(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// postJSON sends a gateway request and returns the status, body and the HTTP
// version actually negotiated. The version matters: routing by the connection's
// ALPN protocol rather than the request's content type would send every HTTP/2
// client to the gRPC server, so a test that silently fell back to HTTP/1.1
// would pass while the bug it guards was present.
func postJSON(tb testing.TB, client *http.Client, url, body string) (int, string, string) {
	tb.Helper()
	resp, err := client.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		tb.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b), resp.Proto
}

// TestSharedPortPlaintext checks that one plaintext port answers both an etcd
// gRPC client and a JSON client, which is what cmux is there for.
func TestSharedPortPlaintext(t *testing.T) {
	addr := sharedPort(t, nil)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, "/shared/grpc", "from-grpc"); err != nil {
		t.Fatalf("grpc put on a shared port: %v", err)
	}

	code, body, _ := postJSON(t, http.DefaultClient,
		"http://"+addr+"/v3/kv/range", `{"key":"`+gwEnc("/shared/grpc")+`"}`)
	if code != http.StatusOK {
		t.Fatalf("json range: status %d body %s", code, body)
	}
	if !strings.Contains(body, gwEnc("from-grpc")) {
		t.Errorf("json did not see the gRPC write: %s", body)
	}
}

// TestSharedPortTLS is the regression test for the arrangement that cost the
// most to get right: under TLS the protocols can only be told apart per
// request, so an ordinary HTTPS client must not be mistaken for a gRPC one.
// Browsers speak HTTP/2, so both HTTP versions are checked.
func TestSharedPortTLS(t *testing.T) {
	certPath, keyPath, roots := writeTLSTestCertificate(t)
	tlsConf, err := buildClientTLSConfig(certPath, keyPath, "")
	if err != nil {
		t.Fatalf("build client TLS: %v", err)
	}
	addr := sharedPort(t, tlsConf)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://" + addr},
		DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{RootCAs: roots, ServerName: "localhost"},
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, "/shared/tls", "from-grpc-tls"); err != nil {
		t.Fatalf("grpc put on a shared TLS port: %v", err)
	}

	// A gRPC watch must survive on the HTTP/2 path too, since that transport
	// is Go's rather than gRPC's own.
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	events := cli.Watch(wctx, "/shared/watched")
	if _, err := cli.Put(ctx, "/shared/watched", "event"); err != nil {
		t.Fatalf("put watched: %v", err)
	}
	select {
	case wr, ok := <-events:
		if !ok || wr.Err() != nil {
			t.Fatalf("watch on a shared TLS port: ok=%v err=%v", ok, wr.Err())
		}
		if len(wr.Events) == 0 {
			t.Error("watch delivered no events")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watch on a shared TLS port delivered nothing")
	}

	for _, tc := range []struct {
		name      string
		wantProto string
	}{
		{"http/2", "HTTP/2.0"},
		{"http/1.1", "HTTP/1.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &http.Transport{
				TLSClientConfig:   &tls.Config{RootCAs: roots, ServerName: "localhost"},
				ForceAttemptHTTP2: tc.wantProto == "HTTP/2.0",
			}
			if tc.wantProto == "HTTP/1.1" {
				// Offering only http/1.1 over ALPN is what pins this subtest
				// to the version it means to exercise.
				tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
			}
			client := &http.Client{Transport: tr}
			code, body, proto := postJSON(t, client,
				"https://"+addr+"/v3/kv/range", `{"key":"`+gwEnc("/shared/tls")+`"}`)
			if proto != tc.wantProto {
				t.Fatalf("negotiated %s, want %s: this subtest is not exercising what it claims", proto, tc.wantProto)
			}
			if code != http.StatusOK {
				t.Fatalf("json range: status %d body %s", code, body)
			}
			if !strings.Contains(body, gwEnc("from-grpc-tls")) {
				t.Errorf("json did not see the gRPC write: %s", body)
			}
		})
	}
}

// TestSeparatePortLeavesGRPCAlone checks the escape hatch from the latency
// shared-port TLS costs: with its own listener the gateway answers JSON while
// the gRPC port keeps the transport it has when no gateway exists.
func TestSeparatePortLeavesGRPCAlone(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	grpcSrv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(grpcSrv)

	loopback := gateway.NewLoopback()
	go func() { _ = grpcSrv.Serve(loopback) }()
	conn, err := loopback.ClientConn()
	if err != nil {
		t.Fatalf("loopback conn: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	handler, err := gateway.New(ctx, conn, gateway.Config{})
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	httpSrv := &http.Server{Handler: handler, ReadHeaderTimeout: time.Minute}

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = httpSrv.Close()
		grpcSrv.Stop()
		_ = conn.Close()
		_ = loopback.Close()
	})

	serveClientPort(grpcSrv, httpSrv, grpcLis, gwLis, nil)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{grpcLis.Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	putCtx, putCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer putCancel()
	if _, err := cli.Put(putCtx, "/split/key", "from-grpc"); err != nil {
		t.Fatalf("grpc put: %v", err)
	}

	code, body, _ := postJSON(t, http.DefaultClient,
		"http://"+gwLis.Addr().String()+"/v3/kv/range", `{"key":"`+gwEnc("/split/key")+`"}`)
	if code != http.StatusOK {
		t.Fatalf("json range on the gateway port: status %d body %s", code, body)
	}
	if !strings.Contains(body, gwEnc("from-grpc")) {
		t.Errorf("gateway port did not see the gRPC write: %s", body)
	}

	// The gRPC port must not answer JSON: that is the whole point of splitting.
	resp, err := http.Post("http://"+grpcLis.Addr().String()+"/v3/kv/range",
		"application/json", strings.NewReader(`{"key":"`+gwEnc("/split/key")+`"}`))
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Error("the gRPC port served a JSON request")
		}
	}
}

// TestDerivedGatewayAddr covers the address the gateway is given under TLS when
// none was asked for. One below the client port, because the peer port is one
// above it by convention everywhere in t4's own documentation.
func TestDerivedGatewayAddr(t *testing.T) {
	for _, tc := range []struct {
		listen string
		want   string
		ok     bool
	}{
		{"0.0.0.0:3379", "0.0.0.0:3378", true},
		{"127.0.0.1:2379", "127.0.0.1:2378", true},
		{"[::1]:3379", "[::1]:3378", true},
		{":3379", ":3378", true},
		// Nothing to derive from: the caller shares the port and says so.
		{"0.0.0.0", "", false},
		{"0.0.0.0:notaport", "", false},
		{"0.0.0.0:1", "", false},
		{"0.0.0.0:0", "", false},
		{"", "", false},
	} {
		got, ok := derivedGatewayAddr(tc.listen)
		if ok != tc.ok || got != tc.want {
			t.Errorf("derivedGatewayAddr(%q) = %q, %v; want %q, %v", tc.listen, got, ok, tc.want, tc.ok)
		}
	}
}

// TestDerivedGatewayAddrAvoidsPeerPort pins the reason for choosing the port
// below rather than above: above is where a peer listens in every example t4
// documents, so deriving upward would collide with multi-node mode.
func TestDerivedGatewayAddrAvoidsPeerPort(t *testing.T) {
	const listen, peer = "0.0.0.0:3379", "0.0.0.0:3380"
	got, ok := derivedGatewayAddr(listen)
	if !ok {
		t.Fatalf("derivedGatewayAddr(%q) failed", listen)
	}
	if got == peer {
		t.Errorf("derived %q, which is the documented peer address", got)
	}
}
