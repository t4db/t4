package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
	"github.com/t4db/t4/internal/gateway"
)

// newAuthGateway brings up an auth-enabled server with one user, root, behind
// the JSON gateway.
func newAuthGateway(tb testing.TB) string {
	tb.Helper()

	node, err := t4.Open(t4.Config{DataDir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open node: %v", err)
	}
	tb.Cleanup(func() { _ = node.Close() })

	authStore, err := auth.NewStore(node)
	if err != nil {
		tb.Fatalf("auth store: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	tokens := auth.NewTokenStore(ctx, time.Hour, node)

	loopback := gateway.NewLoopback()
	srv := grpc.NewServer(t4etcd.NewServerOptions(authStore, tokens)...)
	t4etcd.New(node, authStore, tokens).Register(srv)
	go func() { _ = srv.Serve(loopback) }()
	tb.Cleanup(func() {
		srv.Stop()
		_ = loopback.Close()
	})

	conn, err := loopback.ClientConn()
	if err != nil {
		tb.Fatalf("loopback conn: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close() })

	handler, err := gateway.New(ctx, conn, gateway.Config{})
	if err != nil {
		tb.Fatalf("gateway: %v", err)
	}
	ts := httptest.NewServer(handler)
	tb.Cleanup(ts.Close)

	// Bootstrap over the gateway itself, which also exercises the auth routes.
	base := ts.URL
	for _, step := range []struct{ path, body string }{
		{"/v3/auth/user/add", `{"name":"root","password":"rootpw"}`},
		{"/v3/auth/role/add", `{"name":"root"}`},
		{"/v3/auth/user/grant", `{"user":"root","role":"root"}`},
		{"/v3/auth/enable", `{}`},
	} {
		if code, body := post(tb, base, step.path, step.body, nil); code != http.StatusOK {
			tb.Fatalf("%s: status %d body %s", step.path, code, body)
		}
	}
	return base
}

// TestAuthOverGateway proves the claim that makes the gateway safe to enable:
// a JSON caller crosses the same interceptor chain as a gRPC one, so RBAC
// applies, and the token travels in the header etcd's own clients use.
func TestAuthOverGateway(t *testing.T) {
	base := newAuthGateway(t)

	t.Run("no token is refused", func(t *testing.T) {
		code, body := post(t, base, "/v3/kv/range", `{"key":"`+b64("/x")+`"}`, nil)
		if code == http.StatusOK {
			t.Fatalf("an unauthenticated range succeeded: %s", body)
		}
	})

	// The gateway forwards Authorization as bare "authorization" metadata,
	// which is why the interceptor accepts that key as well as "token".
	var authed struct {
		Token string `json:"token"`
	}
	code, body := post(t, base, "/v3/auth/authenticate", `{"name":"root","password":"rootpw"}`, nil)
	if code != http.StatusOK {
		t.Fatalf("authenticate: status %d body %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &authed); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if authed.Token == "" {
		t.Fatalf("no token in %s", body)
	}

	t.Run("token in the Authorization header is accepted", func(t *testing.T) {
		hdr := map[string]string{"Authorization": authed.Token}
		if code, body := post(t, base, "/v3/kv/put",
			`{"key":"`+b64("/x")+`","value":"`+b64("ok")+`"}`, hdr); code != http.StatusOK {
			t.Fatalf("put: status %d body %s", code, body)
		}
		code, body := post(t, base, "/v3/kv/range", `{"key":"`+b64("/x")+`"}`, hdr)
		if code != http.StatusOK {
			t.Fatalf("range: status %d body %s", code, body)
		}
		if want := b64("ok"); !strings.Contains(body, want) {
			t.Errorf("range did not return the value: %s", body)
		}
	})

	t.Run("a bad token is refused", func(t *testing.T) {
		hdr := map[string]string{"Authorization": "not-a-token"}
		if code, body := post(t, base, "/v3/kv/range", `{"key":"`+b64("/x")+`"}`, hdr); code == http.StatusOK {
			t.Fatalf("an invalid token was accepted: %s", body)
		}
	})
}
