package gateway_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/t4db/t4/internal/gateway"
)

// TestGRPCHandlerRouting covers the decision a shared TLS port has to make per
// request, since ALPN cannot make it per connection.
func TestGRPCHandlerRouting(t *testing.T) {
	// A nil *grpc.Server would panic if it were ever reached, which is exactly
	// the assertion wanted for the requests that must not be routed to gRPC.
	var nilServer *grpc.Server
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusTeapot)
	})
	h := gateway.GRPCHandler(nilServer, next)

	for _, tc := range []struct {
		name        string
		proto       int
		contentType string
		wantStatus  int
		wantNext    bool
	}{
		{"json over http/1.1", 1, "application/json", http.StatusTeapot, true},
		{"json over http/2", 2, "application/json", http.StatusTeapot, true},
		// grpc-web is not gRPC, so it is passed on; the gateway behind this
		// handler is what answers it (see TestWatchRejectsGRPCWeb).
		{"grpc-web", 2, "application/grpc-web+proto", http.StatusTeapot, true},
		{"grpc-web-text", 2, "application/grpc-web-text", http.StatusTeapot, true},
		// A gRPC content type on an HTTP/1.1 request is not gRPC: the protocol
		// requires HTTP/2, so it belongs to the gateway, which will 404 it.
		{"grpc over http/1.1", 1, "application/grpc", http.StatusTeapot, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reached = false
			req := httptest.NewRequest(http.MethodPost, "/anything", strings.NewReader(""))
			req.ProtoMajor = tc.proto
			req.Header.Set("Content-Type", tc.contentType)
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body %q)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if reached != tc.wantNext {
				t.Errorf("passed to the gateway = %v, want %v", reached, tc.wantNext)
			}
		})
	}
}

// TestGRPCHandlerMatchesGRPCExactly pins the content types that count as gRPC.
// etcd tests this with strings.Contains, which also matches grpc-web and hands
// it to a server that can only reject it.
func TestGRPCHandlerMatchesGRPCExactly(t *testing.T) {
	routed := map[string]bool{}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routed[r.Header.Get("Content-Type")] = true
		w.WriteHeader(http.StatusTeapot)
	})
	h := gateway.GRPCHandler(nil, next)

	// Every one of these is gRPC, so none should reach the gateway; reaching a
	// nil server panics, which is caught here and read as "routed to gRPC".
	for _, ct := range []string{"application/grpc", "application/grpc+proto", "application/grpc;charset=utf-8"} {
		if !panics(func() {
			req := httptest.NewRequest(http.MethodPost, "/x", nil)
			req.ProtoMajor = 2
			req.Header.Set("Content-Type", ct)
			h.ServeHTTP(httptest.NewRecorder(), req)
		}) {
			t.Errorf("%q was not routed to gRPC", ct)
		}
		if routed[ct] {
			t.Errorf("%q reached the gateway", ct)
		}
	}
}

func panics(f func()) (did bool) {
	defer func() {
		if recover() != nil {
			did = true
		}
	}()
	f()
	return did
}
