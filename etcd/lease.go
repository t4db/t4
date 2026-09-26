package etcd

import (
	"context"
	"encoding/json"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/sysstate"
)

// LeaseGrant creates or reserves a lease ID with a real expiry time.
//
// The lease key is written with Node.Create so concurrent grants of the same
// explicit ID are serialised by the leader's WAL: exactly one Create commits,
// the rest return ErrKeyExists which surfaces as AlreadyExists.
func (s *Server) LeaseGrant(ctx context.Context, r *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	if r.TTL <= 0 {
		return nil, status.Error(codes.InvalidArgument, "lease TTL must be positive")
	}
	id := r.ID

	if id == 0 {
		const maxAttempts = 16
		for attempt := 0; attempt < maxAttempts; attempt++ {
			newID, err := newLeaseID()
			if err != nil {
				return nil, status.Errorf(codes.Internal, "generate lease ID: %v", err)
			}
			rec := &leaseRecord{
				ID:           newID,
				GrantedTTL:   r.TTL,
				ExpiryUnixNs: time.Now().Add(time.Duration(r.TTL) * time.Second).UnixNano(),
			}
			err = s.createLease(ctx, rec)
			if err == nil {
				return &etcdserverpb.LeaseGrantResponse{Header: s.header(), ID: rec.ID, TTL: rec.GrantedTTL}, nil
			}
			if status.Code(err) != codes.AlreadyExists {
				return nil, err
			}
		}
		return nil, status.Errorf(codes.Internal, "failed to allocate unique lease ID after %d attempts", maxAttempts)
	}

	if err := validateLeaseID(id); err != nil {
		return nil, err
	}
	rec := &leaseRecord{
		ID:           id,
		GrantedTTL:   r.TTL,
		ExpiryUnixNs: time.Now().Add(time.Duration(r.TTL) * time.Second).UnixNano(),
	}
	if err := s.createLease(ctx, rec); err != nil {
		return nil, err
	}
	return &etcdserverpb.LeaseGrantResponse{Header: s.header(), ID: rec.ID, TTL: rec.GrantedTTL}, nil
}

// createLease writes a fresh lease record using an atomic create. It returns
// AlreadyExists if another caller has already committed the key.
func (s *Server) createLease(ctx context.Context, rec *leaseRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal lease: %v", err)
	}
	created, err := sysstate.Create(ctx, s.node, leaseKey(rec.ID), data)
	if err != nil {
		return status.Errorf(codes.Internal, "create lease: %v", err)
	}
	if !created {
		return status.Errorf(codes.AlreadyExists, "lease %d already exists", rec.ID)
	}
	return nil
}

func (s *Server) LeaseRevoke(ctx context.Context, r *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	if _, err := s.getLease(ctx, r.ID, true); err != nil {
		return nil, err
	}
	if err := s.revokeLease(ctx, r.ID); err != nil {
		return nil, err
	}
	return &etcdserverpb.LeaseRevokeResponse{Header: s.header()}, nil
}

func (s *Server) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		rec, err := s.getLease(stream.Context(), req.ID, true)
		if err != nil {
			return err
		}
		rec.ExpiryUnixNs = time.Now().Add(time.Duration(rec.GrantedTTL) * time.Second).UnixNano()
		if err := s.putLease(stream.Context(), rec); err != nil {
			return err
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{
			Header: s.header(),
			ID:     rec.ID,
			TTL:    rec.GrantedTTL,
		}); err != nil {
			return nil
		}
	}
}

func (s *Server) LeaseTimeToLive(ctx context.Context, r *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	rec, err := s.getLease(ctx, r.ID, true)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     s.header(),
		ID:         rec.ID,
		TTL:        ttlRemaining(rec, time.Now()),
		GrantedTTL: rec.GrantedTTL,
	}
	if r.Keys {
		keys, err := s.collectLeaseKeys(ctx, r.ID, true)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			resp.Keys = append(resp.Keys, []byte(key))
		}
	}
	return resp, nil
}

func (s *Server) LeaseLeases(ctx context.Context, _ *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	leases, err := s.listLeases(ctx, true)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.LeaseLeasesResponse{Header: s.header()}
	now := time.Now()
	for _, rec := range leases {
		if ttlRemaining(rec, now) > 0 {
			resp.Leases = append(resp.Leases, &etcdserverpb.LeaseStatus{ID: rec.ID})
		}
	}
	return resp, nil
}
