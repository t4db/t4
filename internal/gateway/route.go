package gateway

import (
	"net/http"
	"strings"

	"google.golang.org/grpc"
)

// GRPCHandler sorts requests arriving on a shared TLS port: gRPC goes to
// grpcSrv, everything else to next.
//
// A plaintext port can be split at the connection level, because the first
// bytes a client sends say which protocol it speaks. TLS offers no such tell.
// The handshake could be terminated and the negotiated ALPN protocol read, but
// that answers the wrong question: gRPC and an ordinary HTTPS client both
// negotiate "h2", so routing by ALPN sends every browser — which speaks HTTP/2
// and asks for JSON — to the gRPC server, which answers 415. The content type
// is the only thing that distinguishes them, and it is per request rather than
// per connection, so the split has to happen here, above HTTP.
//
// The cost is that gRPC is served through grpc.Server.ServeHTTP, on Go's HTTP/2
// stack rather than its own: that path builds its transport from the response
// writer alone, so the keepalive policy and stream limit the gRPC server was
// given no longer apply, and it carries whatever overhead the extra layer
// costs. The caller sets the stream limit again on the HTTP server; the
// keepalive settings have no equivalent there, which is why the plaintext port
// is still split at the connection level instead.
func GRPCHandler(grpcSrv *grpc.Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && isGRPCContentType(r.Header.Get("Content-Type")) {
			grpcSrv.ServeHTTP(w, r)
			return
		}
		// Everything else, grpc-web included, belongs to the gateway, which
		// answers for it; see accessController.
		next.ServeHTTP(w, r)
	})
}

// isGRPCContentType reports whether ct names the gRPC protocol.
//
// etcd tests this with strings.Contains(ct, "application/grpc"), which also
// matches application/grpc-web+proto and hands grpc-web to a server that
// cannot answer it. The separators checked here are the ones gRPC itself
// allows after the subtype, so "application/grpc-web" no longer qualifies.
func isGRPCContentType(ct string) bool {
	const prefix = "application/grpc"
	if !strings.HasPrefix(ct, prefix) {
		return false
	}
	rest := ct[len(prefix):]
	return rest == "" || rest[0] == '+' || rest[0] == ';'
}
