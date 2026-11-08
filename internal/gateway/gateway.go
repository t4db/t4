// Package gateway serves the etcd v3 JSON/HTTP API in front of t4's gRPC
// server, so that clients which cannot speak gRPC — browsers above all, and
// anything reaching for curl — get the same keyspace on the same port.
//
// The HTTP surface is not hand-written. etcd publishes the generated
// grpc-gateway handlers for its own protocol in
// go.etcd.io/etcd/api/v3/etcdserverpb/gw, and this package registers them
// against a connection to t4's own gRPC server, which makes the route table,
// the JSON shapes and the field names etcd's by construction rather than by
// imitation. Requests therefore cross the server's interceptors, so auth and
// RBAC apply to a JSON caller exactly as to a gRPC one.
//
// Streaming RPCs arrive over websockets. Watch and LeaseKeepAlive are
// bidirectional, and no plain-HTTP framing carries those, which is why a
// browser cannot watch over grpc-web either: grpc-web has no bidirectional
// streaming, so the client half-closes after its first message and the watch is
// torn down before an event can reach it. The websocket proxy is what makes
// them work, and it is why this package exists rather than a grpc-web filter.
package gateway

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/sirupsen/logrus"
	"github.com/tmc/grpc-websocket-proxy/wsproxy"
	"go.etcd.io/etcd/api/v3/etcdserverpb/gw"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// Config is the gateway's HTTP-facing policy. Everything else about it is
// etcd's protocol and is not configurable.
type Config struct {
	// CORSOrigins lists the web origins allowed to call the gateway, or holds
	// "*" for all. Empty — the default — sends no CORS headers at all, which
	// leaves browsers unable to reach the gateway cross-origin while leaving
	// every non-browser client untouched.
	//
	// etcd defaults this to "*". t4 does not, because the gateway is on by
	// default: an etcd-matching default would mean any page a user happens to
	// visit could call any t4 their browser can route to.
	CORSOrigins []string

	// HostWhitelist lists the Host header values accepted on connections that
	// are not TLS, or holds "*" for all. It is the guard against DNS rebinding
	// (CVE-2018-5702), where a name under an attacker's control is made to
	// resolve to a t4 the browser can reach but the attacker cannot. Empty or
	// "*" allows any host, as on etcd.
	HostWhitelist []string
}

// New returns the gateway handler. conn should reach t4's own gRPC server;
// Loopback.ClientConn gives one that never leaves the process.
//
// ctx governs the registered handlers for the life of the gateway: cancelling
// it releases their streams. Closing conn is the caller's job.
func New(ctx context.Context, conn *grpc.ClientConn, cfg Config) (http.Handler, error) {
	// etcd's marshaler, field for field. UseProtoNames is the load-bearing
	// one: without it a response carries createRevision where every etcd
	// client, and etcd's own documentation, expects create_revision.
	gwmux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.HTTPBodyMarshaler{
			Marshaler: &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{
					UseProtoNames:   true,
					EmitUnpopulated: false,
				},
				UnmarshalOptions: protojson.UnmarshalOptions{
					DiscardUnknown: true,
				},
			},
		}),
	)

	// The same six services etcd/server.go Register wires onto the gRPC server.
	// t4 serves no Lock or Election service, so the gateway offers neither.
	for _, register := range []func(context.Context, *runtime.ServeMux, *grpc.ClientConn) error{
		gw.RegisterKVHandler,
		gw.RegisterWatchHandler,
		gw.RegisterLeaseHandler,
		gw.RegisterClusterHandler,
		gw.RegisterMaintenanceHandler,
		gw.RegisterAuthHandler,
	} {
		if err := register(ctx, gwmux, conn); err != nil {
			return nil, err
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/v3/", wsproxy.WebsocketProxy(gwmux,
		// A websocket carries no method, and every gateway route is a POST.
		wsproxy.WithRequestMutator(func(_ *http.Request, outgoing *http.Request) *http.Request {
			outgoing.Method = http.MethodPost
			return outgoing
		}),
		// A watch response is bounded by the server's own send limit, not by
		// anything the proxy should second-guess.
		wsproxy.WithMaxRespBodyBufferSize(0x7fffffff),
		wsproxy.WithLogger(logrus.StandardLogger()),
	))

	// The two lists read their emptiness in opposite directions, so only one of
	// them can be left to the zero value. An empty host whitelist allows every
	// host, as on etcd, while empty CORS origins allow no browser at all; the
	// host set therefore becomes an explicit "*" here and the CORS set does not.
	hosts := cfg.HostWhitelist
	if len(newStringSet(hosts)) == 0 {
		hosts = []string{"*"}
	}

	return &accessController{
		mux:   mux,
		cors:  newStringSet(cfg.CORSOrigins),
		hosts: newStringSet(hosts),
	}, nil
}

// accessController is the first thing an HTTP request meets. It mirrors the
// wrapper etcd puts in the same position (embed/serve.go, createAccessController)
// and adds an origin check on the websocket handshake, which CORS cannot cover.
type accessController struct {
	mux   *http.ServeMux
	cors  stringSet
	hosts stringSet
}

func (ac *accessController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// etcd served this API under /v3beta/ before 3.4 and still accepts the old
	// prefix; clients written against it keep working.
	if r.URL != nil && strings.HasPrefix(r.URL.Path, "/v3beta/") {
		r.URL.Path = strings.Replace(r.URL.Path, "/v3beta/", "/v3/", 1)
	}

	// grpc-web is not served, and saying so is worth the few lines. The
	// marshaler below accepts any content type, so a grpc-web client POSTing
	// to /v3/watch would otherwise be handed an empty stream and a 200 —
	// which is the silent non-answer that sends people looking for this
	// gateway in the first place.
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc-web") {
		http.Error(w,
			"grpc-web is not supported; use the JSON API under /v3/, "+
				"with websockets for watch and lease keepalive",
			http.StatusNotImplemented)
		return
	}

	// Over TLS the certificate already establishes which server the client
	// reached, so a rebinding attack cannot get this far.
	if r.TLS == nil && !ac.hosts.allows(hostname(r)) {
		logrus.WithField("host", r.Host).Warn(
			"t4 gateway: rejecting request with unlisted Host header; see --gateway-host-whitelist")
		http.Error(w, "host not permitted; see --gateway-host-whitelist", http.StatusMisdirectedRequest)
		return
	}

	origin := r.Header.Get("Origin")
	allowed := ac.cors.allows(origin)

	// A websocket handshake is not a cross-origin request as far as the browser
	// is concerned, so it sends no preflight and CORS never gets a say: without
	// this check, --gateway-cors-origins would guard the JSON routes while any
	// page on the internet could still open a watch. Only browsers set Origin,
	// so non-browser clients are unaffected.
	if origin != "" && !allowed && isWebsocketUpgrade(r) {
		http.Error(w, "origin not permitted; see --gateway-cors-origins", http.StatusForbidden)
		return
	}

	if allowed && origin != "" {
		setCORSHeaders(w, origin)
	} else if ac.cors.allowsAll() {
		setCORSHeaders(w, "*")
	}

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	ac.mux.ServeHTTP(w, r)
}

func setCORSHeaders(w http.ResponseWriter, origin string) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "accept, content-type, authorization")
}

func isWebsocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// hostname is the Host header without its port, which is what a whitelist
// entry names.
func hostname(r *http.Request) string {
	h, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		return r.Host
	}
	return h
}

// stringSet answers membership with "*" meaning everything, the convention
// etcd uses for both of these lists.
type stringSet map[string]struct{}

func newStringSet(vals []string) stringSet {
	s := make(stringSet, len(vals))
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			s[v] = struct{}{}
		}
	}
	return s
}

func (s stringSet) allowsAll() bool {
	_, ok := s["*"]
	return ok
}

// allows reports whether v is permitted. An empty set permits nothing, which is
// why the CORS default sends no headers; a set holding "*" permits everything.
func (s stringSet) allows(v string) bool {
	if len(s) == 0 {
		return false
	}
	if s.allowsAll() {
		return true
	}
	_, ok := s[v]
	return ok
}
