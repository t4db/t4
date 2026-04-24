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
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
	"github.com/t4db/t4/etcd/auth"
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

// NewServerOptions returns the gRPC server options required for auth
// enforcement (interceptors). Pass the returned options to grpc.NewServer.
// When authStore is nil the returned options are empty (no-op).
func NewServerOptions(authStore *auth.Store, tokens *auth.TokenStore) []grpc.ServerOption {
	if authStore == nil {
		return nil
	}
	unary, stream := auth.Interceptors(authStore, tokens)
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(unary),
		grpc.StreamInterceptor(stream),
	}
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
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1,
		MemberId:  1,
		Revision:  toEtcdRevision(s.node.CurrentRevision()),
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
