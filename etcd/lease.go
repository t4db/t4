package etcd

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func unimplementedLease() error {
	return status.Error(codes.Unimplemented, "lease APIs are not supported yet")
}

// LeaseGrant is not yet supported because Strata does not currently enforce
// etcd lease TTL semantics.
func (s *Server) LeaseGrant(_ context.Context, r *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return nil, unimplementedLease()
}

func (s *Server) LeaseRevoke(_ context.Context, _ *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, unimplementedLease()
}

func (s *Server) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	return unimplementedLease()
}

func (s *Server) LeaseTimeToLive(_ context.Context, r *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, unimplementedLease()
}

func (s *Server) LeaseLeases(_ context.Context, _ *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, unimplementedLease()
}
