// Package etcd exposes a t4 Node as an etcd v3 gRPC server.
//
// Register wires the KV, Watch, Lease, Cluster, Maintenance, and Auth services
// onto a *grpc.Server. Any etcd v3 client — etcdctl, go.etcd.io/etcd/client/v3,
// Kubernetes — can talk to it without modification.
//
// Not all etcd RPCs are meaningful for a single-node embedded store; unimplemented
// ones return codes.Unimplemented.
package etcd

import (
	"context"
	"math"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
	"github.com/t4db/t4/etcd/auth"
)

const (
	defaultMaxConcurrentStreams = math.MaxUint32
	defaultMaxRequestBytes      = int(1.5 * 1024 * 1024)
	grpcOverheadBytes           = 512 * 1024
	maxSendBytes                = math.MaxInt32
)

// Server implements the etcd v3 gRPC protocol on top of a t4 Node.
type Server struct {
	node          *t4.Node
	authStore     *auth.Store
	tokens        *auth.TokenStore
	leaseLoopOnce sync.Once
}

// New returns a Server backed by node. When authStore and tokens are non-nil,
// the Auth gRPC service is registered and RBAC is enforced on all KV/Watch
// calls.
func New(node *t4.Node, authStore *auth.Store, tokens *auth.TokenStore) *Server {
	s := &Server{node: node, authStore: authStore, tokens: tokens}
	s.maybeStartLeaseLoop()
	return s
}

// KeepaliveOptions configures the gRPC server's HTTP/2 keepalive enforcement
// and ping cadence. Field names mirror gRPC-Go's keepalive types and etcd's
// CLI flags (--grpc-keepalive-{min-time,interval,timeout}).
//
// The defaults match real etcd. The values are intentionally compatible with
// the etcd v3 client default (`DialKeepAliveTime: 30s`, PermitWithoutStream:
// true): MinTime=5s leaves comfortable headroom; PermitWithoutStream=true
// matches the client side so idle Watch streams aren't kicked.
//
// Without these defaults, gRPC-Go applies `MinTime: 5*time.Minute` /
// `PermitWithoutStream: false`, which causes the server to send
// `GOAWAY too_many_pings` against any etcd v3 client and drop in-flight RPCs.
type KeepaliveOptions struct {
	// MinTime is the minimum interval the server demands between client
	// pings during a quiet stream. Pings closer than this earn a strike;
	// two strikes trigger GOAWAY too_many_pings.
	MinTime time.Duration

	// PermitWithoutStream lets clients ping when no streams are active.
	// Required because etcd clients ping the connection itself, not a
	// specific stream.
	PermitWithoutStream bool

	// Time is the server-side keepalive ping interval (after this much
	// inactivity, the server pings the client). Etcd uses 2h.
	Time time.Duration

	// Timeout is how long the server waits for a ping ack before declaring
	// the connection dead.
	Timeout time.Duration
}

// DefaultKeepaliveOptions returns the etcd-compatible defaults.
func DefaultKeepaliveOptions() KeepaliveOptions {
	return KeepaliveOptions{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
		Time:                2 * time.Hour,
		Timeout:             20 * time.Second,
	}
}

// Option configures NewServerOptions. Use WithKeepalive to override the
// keepalive policy.
type Option func(*serverConfig)

type serverConfig struct {
	keepalive            KeepaliveOptions
	maxConcurrentStreams uint32
	maxRecvMsgSize       int
	maxSendMsgSize       int
}

// WithKeepalive sets the keepalive enforcement policy and server ping params.
// Pass DefaultKeepaliveOptions() (with field overrides) for etcd-compatible
// behaviour.
func WithKeepalive(k KeepaliveOptions) Option {
	return func(c *serverConfig) { c.keepalive = k }
}

// WithGRPCLimits sets etcd-style gRPC transport limits. Zero values keep the
// default for that field.
func WithGRPCLimits(maxConcurrentStreams uint32, maxRecvMsgSize, maxSendMsgSize int) Option {
	return func(c *serverConfig) {
		if maxConcurrentStreams != 0 {
			c.maxConcurrentStreams = maxConcurrentStreams
		}
		if maxRecvMsgSize != 0 {
			c.maxRecvMsgSize = maxRecvMsgSize
		}
		if maxSendMsgSize != 0 {
			c.maxSendMsgSize = maxSendMsgSize
		}
	}
}

// NewServerOptions returns the gRPC server options needed to host the
// etcd v3 surface: auth interceptors (when authStore is non-nil) and a
// keepalive enforcement policy compatible with etcd v3 clients.
//
// Defaults come from DefaultKeepaliveOptions; pass WithKeepalive to override.
func NewServerOptions(authStore *auth.Store, tokens *auth.TokenStore, opts ...Option) []grpc.ServerOption {
	cfg := serverConfig{
		keepalive:            DefaultKeepaliveOptions(),
		maxConcurrentStreams: defaultMaxConcurrentStreams,
		maxRecvMsgSize:       defaultMaxRequestBytes + grpcOverheadBytes,
		maxSendMsgSize:       maxSendBytes,
	}
	for _, o := range opts {
		o(&cfg)
	}
	out := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             cfg.keepalive.MinTime,
			PermitWithoutStream: cfg.keepalive.PermitWithoutStream,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    cfg.keepalive.Time,
			Timeout: cfg.keepalive.Timeout,
		}),
		grpc.MaxConcurrentStreams(cfg.maxConcurrentStreams),
		grpc.MaxRecvMsgSize(cfg.maxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.maxSendMsgSize),
	}
	if authStore != nil {
		unary, stream := auth.Interceptors(authStore, tokens)
		out = append(out,
			grpc.UnaryInterceptor(unary),
			grpc.StreamInterceptor(stream),
		)
	}
	return out
}

// Register wires the etcd services onto srv.
func (s *Server) Register(srv *grpc.Server) {
	etcdserverpb.RegisterKVServer(srv, s)
	etcdserverpb.RegisterWatchServer(srv, s)
	etcdserverpb.RegisterLeaseServer(srv, s)
	etcdserverpb.RegisterClusterServer(srv, s)
	etcdserverpb.RegisterMaintenanceServer(srv, s)

	if s.authStore != nil {
		etcdserverpb.RegisterAuthServer(srv, auth.NewService(s.authStore, s.tokens))
	} else {
		etcdserverpb.RegisterAuthServer(srv, &etcdserverpb.UnimplementedAuthServer{})
	}

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)
}

// header builds a ResponseHeader from the current node state.
//
// Revision is the wire revision, not the internal t4 clock: see toEtcdRevision.
// Any new RPC that exposes a revision on the wire must go through toEtcdRevision
// (outgoing) or fromEtcdRevision (incoming) to stay consistent with this header.
func (s *Server) header() *etcdserverpb.ResponseHeader {
	return s.headerAt(s.node.CurrentRevision())
}

// headerAt builds a ResponseHeader pinned to a specific internal t4 revision.
//
// Mutating RPCs MUST use this with the commit revision returned by the node,
// not the global CurrentRevision after the call. Reading CurrentRevision is
// racy under concurrent writes — by the time the response is built another
// transaction may have advanced the clock, and the caller's view (e.g. a
// kube-apiserver computing the new resourceVersion) would diverge from the
// actual mod_revision recorded for the key. That mismatch surfaces to clients
// as "the object has been modified" 409 conflicts on the next Update.
func (s *Server) headerAt(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1,
		MemberId:  1,
		Revision:  toEtcdRevision(rev),
		RaftTerm:  1,
	}
}

// toEtcdRevision maps t4's internal revision clock to the etcd wire revision.
//
// T4's native store starts at revision 0. Kubernetes rejects list responses with
// resourceVersion=0, while etcd presents a non-zero revision even before user
// writes. Keep the native clock unchanged and shift only the etcd API surface.
func toEtcdRevision(rev int64) int64 {
	return rev + 1
}

// fromEtcdRevision maps non-zero etcd wire revisions back to t4's internal
// revision clock. Revision 0 is a sentinel in etcd comparisons for absent keys,
// not a real storage revision, so it stays 0.
func fromEtcdRevision(rev int64) int64 {
	if rev <= 0 {
		return 0
	}
	return rev - 1
}

// kvToProto converts a t4 KeyValue to the etcd wire format.
//
// ModRevision and CreateRevision are wire revisions (see toEtcdRevision); they
// must match the header revision produced by header() for the same underlying
// state so that clients comparing header rev to KV rev see a consistent world.
func kvToProto(kv *t4.KeyValue) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		ModRevision:    toEtcdRevision(kv.Revision),
		CreateRevision: toEtcdRevision(kv.CreateRevision),
		Lease:          kv.Lease,
		Version:        1,
	}
}

// eventToProto converts a t4 watch Event to the etcd mvccpb format.
func eventToProto(e t4.Event) *mvccpb.Event {
	ev := &mvccpb.Event{Kv: kvToProto(e.KV)}
	if e.Type == t4.EventDelete {
		ev.Type = mvccpb.DELETE
	} else {
		ev.Type = mvccpb.PUT
	}
	if e.PrevKV != nil {
		ev.PrevKv = kvToProto(e.PrevKV)
	}
	return ev
}

// unimplemented returns a standard gRPC unimplemented error.
func unimplemented() error {
	return status.Error(codes.Unimplemented, "not supported")
}

// ── Cluster (stubs) ──────────────────────────────────────────────────────────

func (s *Server) MemberAdd(_ context.Context, _ *etcdserverpb.MemberAddRequest) (*etcdserverpb.MemberAddResponse, error) {
	return nil, unimplemented()
}
func (s *Server) MemberRemove(_ context.Context, _ *etcdserverpb.MemberRemoveRequest) (*etcdserverpb.MemberRemoveResponse, error) {
	return nil, unimplemented()
}
func (s *Server) MemberUpdate(_ context.Context, _ *etcdserverpb.MemberUpdateRequest) (*etcdserverpb.MemberUpdateResponse, error) {
	return nil, unimplemented()
}
func (s *Server) MemberList(_ context.Context, _ *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	return &etcdserverpb.MemberListResponse{
		Header: s.header(),
		Members: []*etcdserverpb.Member{{
			ID:   1,
			Name: "t4",
		}},
	}, nil
}
func (s *Server) MemberPromote(_ context.Context, _ *etcdserverpb.MemberPromoteRequest) (*etcdserverpb.MemberPromoteResponse, error) {
	return nil, unimplemented()
}

// ── Maintenance ──────────────────────────────────────────────────────────────

// Alarm returns an empty alarm list. T4 has no quota or corruption alarm
// subsystem — Pebble manages storage internally with no fixed size cap.
func (s *Server) Alarm(_ context.Context, _ *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	return &etcdserverpb.AlarmResponse{Header: s.header()}, nil
}

// Status returns basic node status: current revision, leader, and version.
func (s *Server) Status(_ context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	rev := s.node.CurrentRevision()
	leader := uint64(0)
	if s.node.IsLeader() {
		leader = 1
	}
	etcdRev := toEtcdRevision(rev)
	return &etcdserverpb.StatusResponse{
		Header:           s.header(),
		Version:          "t4",
		Leader:           leader,
		RaftIndex:        uint64(etcdRev),
		RaftAppliedIndex: uint64(etcdRev),
		RaftTerm:         1,
	}, nil
}

// Defragment is a no-op — Pebble manages compaction internally.
func (s *Server) Defragment(_ context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	return &etcdserverpb.DefragmentResponse{Header: s.header()}, nil
}

func (s *Server) Hash(_ context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return nil, unimplemented()
}
func (s *Server) HashKV(_ context.Context, _ *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	return nil, unimplemented()
}
func (s *Server) Snapshot(_ *etcdserverpb.SnapshotRequest, _ etcdserverpb.Maintenance_SnapshotServer) error {
	return unimplemented()
}
func (s *Server) MoveLeader(_ context.Context, _ *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	return nil, unimplemented()
}
func (s *Server) Downgrade(_ context.Context, _ *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	return nil, unimplemented()
}
