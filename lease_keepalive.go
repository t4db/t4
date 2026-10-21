package t4

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/peer"
)

// Client lease keepalives.
//
// Granting and revoking a client lease are writes, but keeping one alive is
// not: as in etcd, the leader keeps when each lease was last renewed in
// memory, and a node that becomes leader treats every lease as renewed when
// its term starts. That never shortens a lease: a node takes over only once
// the previous leader's lease has lapsed, so every renewal that leader
// acknowledged came before the new term started. Followers forward renewals
// and reads of renewal times to the leader, which alone knows them.

// ErrLeaseRenewUnsupported is returned by RenewLease and LeaseRenewedAt on a
// follower whose leader predates in-memory lease renewal. Such a leader
// expects renewals to be written, as the expiry it then reads.
var ErrLeaseRenewUnsupported = errors.New("t4: the leader does not keep lease renewals in memory")

// leaseRenewals is the leader's record of client lease renewals.
type leaseRenewals struct {
	mu        sync.Mutex
	termStart time.Time              // every lease counts as renewed then
	at        map[int64]leaseRenewal // later renewals
	pruneAt   int                    // prune once len(at) reaches this
}

type leaseRenewal struct {
	at  time.Time
	ttl time.Duration
}

// startTerm forgets earlier renewals: from start on, every lease counts as
// renewed at start.
func (r *leaseRenewals) startTerm(start time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.termStart = start
	r.at = make(map[int64]leaseRenewal)
	r.pruneAt = 1024
}

func (r *leaseRenewals) renew(id int64, ttl time.Duration, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.at == nil {
		r.at = make(map[int64]leaseRenewal)
	}
	r.at[id] = leaseRenewal{at: now, ttl: ttl}
	if len(r.at) < r.pruneAt {
		return
	}
	// A renewal whose lease would have expired since decides nothing: the
	// lease is expired by it, so only a later renewal or the term start can
	// keep it alive, and those are kept. Revoked leases leave entries too.
	for id, rn := range r.at {
		if !now.Before(rn.at.Add(rn.ttl)) {
			delete(r.at, id)
		}
	}
	r.pruneAt = max(1024, 2*len(r.at))
}

// renewedAt returns when lease id was last renewed: its latest renewal in
// this term, or the term start.
func (r *leaseRenewals) renewedAt(id int64) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rn, ok := r.at[id]; ok && rn.at.After(r.termStart) {
		return rn.at
	}
	return r.termStart
}

// RenewLease records that client lease id, granted with ttl, was renewed
// now. The leader keeps it in memory; a follower forwards it to the leader.
// The caller checks that the lease exists.
func (n *Node) RenewLease(ctx context.Context, id int64, ttl time.Duration) error {
	if n.closed.Load() {
		return ErrClosed
	}
	if n.loadRole() == roleFollower {
		_, err := n.forwardLeaseOp(ctx, &peer.ForwardRequest{Op: peer.ForwardLeaseRenew, Lease: id, Revision: int64(ttl)})
		return err
	}
	if err := n.checkLease(); err != nil {
		return err
	}
	n.renewals.renew(id, ttl, time.Now())
	return nil
}

// LeaseRenewedAt returns, in this node's clock, when each of the client
// leases ids was last renewed: the latest renewal the leader holds, or the
// start of its term. A follower asks the leader.
func (n *Node) LeaseRenewedAt(ctx context.Context, ids []int64) ([]time.Time, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	if n.loadRole() == roleFollower {
		resp, err := n.forwardLeaseOp(ctx, &peer.ForwardRequest{Op: peer.ForwardLeaseRenewedAt, Leases: ids})
		if err != nil {
			return nil, err
		}
		if len(resp.Ages) != len(ids) {
			return nil, errors.New("t4: leader answered lease renewal times for a different number of leases")
		}
		// The leader answers how long ago, so that clock skew between the
		// nodes does not shift the times.
		now := time.Now()
		out := make([]time.Time, len(ids))
		for i, age := range resp.Ages {
			out[i] = now.Add(-time.Duration(age))
		}
		return out, nil
	}
	if err := n.checkLease(); err != nil {
		return nil, err
	}
	out := make([]time.Time, len(ids))
	for i, id := range ids {
		out[i] = n.renewals.renewedAt(id)
	}
	return out, nil
}

// handleLeaseForward answers a follower's forwarded lease renewal or read of
// renewal times on the leader. A node that cannot act as leader, having
// stepped down after the follower picked it or lost its lease, answers
// Unavailable, which the follower reports as ErrNoLeader.
func (n *Node) handleLeaseForward(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	if n.loadRole() == roleFollower {
		return nil, status.Error(codes.Unavailable, ErrNotLeader.Error())
	}
	resp := &peer.ForwardResponse{Succeeded: true}
	var err error
	if req.Op == peer.ForwardLeaseRenew {
		err = n.RenewLease(ctx, req.Lease, time.Duration(req.Revision))
	} else { // peer.ForwardLeaseRenewedAt
		var at []time.Time
		at, err = n.LeaseRenewedAt(ctx, req.Leases)
		now := time.Now()
		for _, t := range at {
			resp.Ages = append(resp.Ages, int64(now.Sub(t)))
		}
	}
	if errors.Is(err, ErrNoLeader) {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if err != nil {
		resp.Succeeded, resp.Ages = false, nil
		resp.ErrCode, resp.ErrMsg = encodeErr(err)
	}
	return resp, nil
}

// forwardLeaseOp sends a lease op to the leader.
func (n *Node) forwardLeaseOp(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	cli := n.leaderCli.Load()
	if cli == nil {
		return nil, ErrNoLeader
	}
	resp, err := cli.ForwardWrite(ctx, req)
	if err != nil {
		if isUnknownForwardOp(err) {
			return nil, ErrLeaseRenewUnsupported
		}
		if isLeaderUnavailable(err) {
			return nil, ErrNoLeader
		}
		return nil, err
	}
	if err := decodeErr(resp.ErrCode, resp.ErrMsg); err != nil {
		return nil, err
	}
	return resp, nil
}
