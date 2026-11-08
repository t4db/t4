package gateway_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/internal/gateway"
)

// newGateway brings up a t4 node, a gRPC server for it on an in-process
// loopback, and the JSON gateway in front of that. It returns the gateway's
// base URL and a gRPC client connection to the same server, so a test can
// check that both protocols see one keyspace.
func newGateway(tb testing.TB, cfg gateway.Config) (string, *grpc.ClientConn) {
	tb.Helper()

	node, err := t4.Open(t4.Config{DataDir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open node: %v", err)
	}
	tb.Cleanup(func() { _ = node.Close() })

	loopback := gateway.NewLoopback()
	srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(srv)
	go func() { _ = srv.Serve(loopback) }()
	tb.Cleanup(func() {
		srv.GracefulStop()
		_ = loopback.Close()
	})

	conn, err := loopback.ClientConn()
	if err != nil {
		tb.Fatalf("loopback conn: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)

	handler, err := gateway.New(ctx, conn, cfg)
	if err != nil {
		tb.Fatalf("gateway: %v", err)
	}
	ts := httptest.NewServer(handler)
	tb.Cleanup(ts.Close)

	return ts.URL, conn
}

// post sends a JSON request to the gateway and returns the status and body.
func post(tb testing.TB, base, path, body string, hdr map[string]string) (int, string) {
	tb.Helper()
	req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	if err != nil {
		tb.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		tb.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err = readAll(resp.Body)
	if err != nil {
		tb.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func readAll(r io.Reader) (string, error) {
	b, err := io.ReadAll(r)
	return string(b), err
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// TestKVRoundTrip is the shape every etcd JSON client depends on: keys and
// values base64, field names in snake_case, counts and revisions as strings.
func TestKVRoundTrip(t *testing.T) {
	base, _ := newGateway(t, gateway.Config{})

	code, body := post(t, base, "/v3/kv/put",
		`{"key":"`+b64("/gw/key")+`","value":"`+b64("hello")+`"}`, nil)
	if code != http.StatusOK {
		t.Fatalf("put: status %d body %s", code, body)
	}

	code, body = post(t, base, "/v3/kv/range", `{"key":"`+b64("/gw/key")+`"}`, nil)
	if code != http.StatusOK {
		t.Fatalf("range: status %d body %s", code, body)
	}

	var got struct {
		Kvs []struct {
			Key            string `json:"key"`
			Value          string `json:"value"`
			CreateRevision string `json:"create_revision"`
			ModRevision    string `json:"mod_revision"`
		} `json:"kvs"`
		Count string `json:"count"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if got.Count != "1" || len(got.Kvs) != 1 {
		t.Fatalf("want one kv, got count=%q kvs=%d (%s)", got.Count, len(got.Kvs), body)
	}
	if got.Kvs[0].Key != b64("/gw/key") || got.Kvs[0].Value != b64("hello") {
		t.Errorf("key/value not base64 as etcd sends them: %s", body)
	}
	// UseProtoNames: without it these arrive as createRevision/modRevision and
	// every etcd client reading the response sees zeroes.
	if got.Kvs[0].CreateRevision == "" || got.Kvs[0].ModRevision == "" {
		t.Errorf("snake_case revision fields missing: %s", body)
	}
}

// TestKeyspaceIsShared checks that the gateway writes where gRPC reads.
func TestKeyspaceIsShared(t *testing.T) {
	base, conn := newGateway(t, gateway.Config{})

	if code, body := post(t, base, "/v3/kv/put",
		`{"key":"`+b64("/shared")+`","value":"`+b64("via-json")+`"}`, nil); code != http.StatusOK {
		t.Fatalf("put: status %d body %s", code, body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := etcdserverpb.NewKVClient(conn).Range(ctx,
		&etcdserverpb.RangeRequest{Key: []byte("/shared")})
	if err != nil {
		t.Fatalf("grpc range: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "via-json" {
		t.Fatalf("grpc did not see the JSON write: %+v", resp.Kvs)
	}
}

// TestV3BetaRewrite covers the prefix etcd served before 3.4 and still accepts.
func TestV3BetaRewrite(t *testing.T) {
	base, _ := newGateway(t, gateway.Config{})
	if code, body := post(t, base, "/v3beta/kv/range", `{"key":"`+b64("/absent")+`"}`, nil); code != http.StatusOK {
		t.Fatalf("v3beta range: status %d body %s", code, body)
	}
}

func TestCORS(t *testing.T) {
	const origin = "http://localhost:3000"

	t.Run("absent by default", func(t *testing.T) {
		base, _ := newGateway(t, gateway.Config{})
		req, _ := http.NewRequest(http.MethodOptions, base+"/v3/kv/range", nil)
		req.Header.Set("Origin", origin)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("options: %v", err)
		}
		defer resp.Body.Close()
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("no origin is configured, so none should be allowed; got %q", got)
		}
	})

	t.Run("named origin allowed", func(t *testing.T) {
		base, _ := newGateway(t, gateway.Config{CORSOrigins: []string{origin}})
		req, _ := http.NewRequest(http.MethodOptions, base+"/v3/kv/range", nil)
		req.Header.Set("Origin", origin)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("options: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("preflight status %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("Allow-Origin = %q, want %q", got, origin)
		}
	})

	t.Run("other origin refused", func(t *testing.T) {
		base, _ := newGateway(t, gateway.Config{CORSOrigins: []string{origin}})
		req, _ := http.NewRequest(http.MethodOptions, base+"/v3/kv/range", nil)
		req.Header.Set("Origin", "http://evil.example")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("options: %v", err)
		}
		defer resp.Body.Close()
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Allow-Origin = %q, want none", got)
		}
	})
}

// TestHostWhitelist covers the DNS rebinding guard. A websocket handshake is
// checked separately because CORS cannot reach it.
func TestHostWhitelist(t *testing.T) {
	base, _ := newGateway(t, gateway.Config{HostWhitelist: []string{"t4.internal"}})

	req, _ := http.NewRequest(http.MethodPost, base+"/v3/kv/range",
		strings.NewReader(`{"key":"`+b64("/x")+`"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "attacker.example"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("status %d, want %d for an unlisted Host", resp.StatusCode, http.StatusMisdirectedRequest)
	}

	// A listed host still gets through.
	req, _ = http.NewRequest(http.MethodPost, base+"/v3/kv/range",
		strings.NewReader(`{"key":"`+b64("/x")+`"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "t4.internal"
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status %d for a listed Host", resp2.StatusCode)
	}
}

// TestWebsocketOriginRefused guards the gap CORS leaves: a websocket handshake
// carries no preflight, so without this check any page could open a watch.
func TestWebsocketOriginRefused(t *testing.T) {
	base, _ := newGateway(t, gateway.Config{CORSOrigins: []string{"http://localhost:3000"}})

	req, _ := http.NewRequest(http.MethodGet, base+"/v3/watch", nil)
	req.Header.Set("Origin", "http://evil.example")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-Websocket-Version", "13")
	req.Header.Set("Sec-Websocket-Key", "x3JJHMbDL1EzLkh9GBhXDw==")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d, want %d for a disallowed websocket origin", resp.StatusCode, http.StatusForbidden)
	}
}

// TestLoopbackClosed checks that a torn-down loopback reports itself as a
// closed listener rather than hanging, which is what lets grpc.Server.Serve
// and the serving code in internal/cli tell a shutdown from a failure.
func TestLoopbackClosed(t *testing.T) {
	l := gateway.NewLoopback()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := l.Accept(); !isClosed(err) {
		t.Errorf("Accept after Close = %v, want a net.ErrClosed", err)
	}
	if _, err := l.Dial(context.Background(), ""); !isClosed(err) {
		t.Errorf("Dial after Close = %v, want a net.ErrClosed", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func isClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), net.ErrClosed.Error())
}
