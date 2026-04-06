// Package etcd exposes a strata Node as an etcd v3 gRPC server.
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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/strata-db/strata"
	"github.com/strata-db/strata/etcd/auth"
)

// Server implements the etcd v3 gRPC protocol on top of a strata Node.
type Server struct {
	node      *strata.Node
	authStore *auth.Store
	tokens    *auth.TokenStore
}

// New returns a Server backed by node. When authStore and tokens are non-nil,
// the Auth gRPC service is registered and RBAC is enforced on all KV/Watch
// calls.
func New(node *strata.Node, authStore *auth.Store, tokens *auth.TokenStore) *Server {
	return &Server{node: node, authStore: authStore, tokens: tokens}
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
func (s *Server) header() *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: 1,
		MemberId:  1,
		Revision:  s.node.CurrentRevision(),
		RaftTerm:  1,
	}
}

// kvToProto converts a strata KeyValue to the etcd wire format.
func kvToProto(kv *strata.KeyValue) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		ModRevision:    kv.Revision,
		CreateRevision: kv.CreateRevision,
		Lease:          kv.Lease,
		Version:        1,
	}
}

// eventToProto converts a strata watch Event to the etcd mvccpb format.
func eventToProto(e strata.Event) *mvccpb.Event {
	ev := &mvccpb.Event{Kv: kvToProto(e.KV)}
	if e.Type == strata.EventDelete {
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
			Name: "strata",
		}},
	}, nil
}
func (s *Server) MemberPromote(_ context.Context, _ *etcdserverpb.MemberPromoteRequest) (*etcdserverpb.MemberPromoteResponse, error) {
	return nil, unimplemented()
}

// ── Maintenance (stubs) ──────────────────────────────────────────────────────

func (s *Server) Alarm(_ context.Context, _ *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	return nil, unimplemented()
}
func (s *Server) Status(_ context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	return nil, unimplemented()
}
func (s *Server) Defragment(_ context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	return nil, unimplemented()
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
