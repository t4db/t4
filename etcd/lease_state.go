package etcd

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
	"github.com/t4db/t4/etcd/auth"
	"github.com/t4db/t4/internal/sysstate"
)

const (
	internalPrefix = "\x00t4/"
	leasePrefix    = internalPrefix + "lease/"
)

type leaseRecord struct {
	ID           int64 `json:"id"`
	GrantedTTL   int64 `json:"granted_ttl"`
	ExpiryUnixNs int64 `json:"expiry_unix_ns"`
}

// isInternalKey reports whether key belongs to T4's own state, which the KV
// and watch APIs never expose or let clients change: lease state, and auth
// users, roles and tokens. A token key holds the bearer token itself.
func isInternalKey(key string) bool {
	return strings.HasPrefix(key, internalPrefix) || auth.IsAuthPrefix(key)
}

func leaseKey(id int64) string {
	return fmt.Sprintf("%s%020d", leasePrefix, id)
}

func validateUserKey(key string) error {
	if isInternalKey(key) {
		return status.Error(codes.InvalidArgument, "key uses reserved internal prefix")
	}
	return nil
}

func validateLeaseID(id int64) error {
	if id <= 0 {
		return status.Error(codes.InvalidArgument, "lease ID must be positive")
	}
	return nil
}

func ttlRemaining(rec *leaseRecord, now time.Time) int64 {
	if rec == nil {
		return -1
	}
	remaining := time.Until(time.Unix(0, rec.ExpiryUnixNs))
	if !now.IsZero() {
		remaining = time.Unix(0, rec.ExpiryUnixNs).Sub(now)
	}
	if remaining <= 0 {
		return 0
	}
	secs := int64(math.Ceil(remaining.Seconds()))
	if secs < 1 {
		return 1
	}
	return secs
}

func decodeLease(value []byte) (*leaseRecord, error) {
	var rec leaseRecord
	if err := json.Unmarshal(value, &rec); err != nil {
		return nil, status.Errorf(codes.Internal, "decode lease: %v", err)
	}
	return &rec, nil
}

// Lease records are system state (see internal/sysstate): meta keys in
// databases created with the meta keyspace, where granting, keeping alive, and
// revoking a lease without keys consume no revision, as in etcd; revisioned
// data keys in databases created before it.

func (s *Server) getLease(ctx context.Context, id int64, linearizable bool) (*leaseRecord, error) {
	if err := validateLeaseID(id); err != nil {
		return nil, err
	}
	var (
		value []byte
		found bool
		err   error
	)
	if linearizable {
		value, found, err = sysstate.LinearizableGet(ctx, s.node, leaseKey(id))
	} else {
		value, found, err = sysstate.Get(s.node, leaseKey(id))
	}
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errLeaseNotFound
	}
	rec, err := decodeLease(value)
	if err != nil {
		return nil, err
	}
	if err := s.leaseExpiries(ctx, []*leaseRecord{rec}, false); err != nil {
		return nil, err
	}
	if time.Now().UnixNano() >= rec.ExpiryUnixNs {
		return nil, errLeaseNotFound
	}
	return rec, nil
}

// renewLease renews the lease of rec. In a database with the meta keyspace
// the leader keeps renewals in memory, as etcd does (see Node.RenewLease).
// Elsewhere, and under a leader that predates that, a renewal is stored as
// the record's expiry: a database created before the meta keyspace may be led
// by a v1.1 node during a rolling upgrade, which expires leases by the stored
// expiry alone and would revoke a lease renewed only in memory.
func (s *Server) renewLease(ctx context.Context, rec *leaseRecord) error {
	on, err := s.node.MetaEnabledSync(ctx)
	if err != nil {
		return err
	}
	if on {
		err := s.node.RenewLease(ctx, rec.ID, time.Duration(rec.GrantedTTL)*time.Second)
		if !errors.Is(err, t4.ErrLeaseRenewUnsupported) {
			return err
		}
	}
	rec.ExpiryUnixNs = time.Now().Add(time.Duration(rec.GrantedTTL) * time.Second).UnixNano()
	return s.refreshLease(ctx, rec)
}

// leaseExpiries sets each record's expiry to when its lease expires: its
// stored expiry, or its last renewal in the leader's memory plus its TTL,
// whichever is later (see renewLease); a follower asks the leader. Unless
// exact, only records whose stored expiry has passed are looked up: the
// others are alive either way.
func (s *Server) leaseExpiries(ctx context.Context, recs []*leaseRecord, exact bool) error {
	if on, err := s.node.MetaEnabledSync(ctx); err != nil || !on {
		return err
	}
	now := time.Now().UnixNano()
	var due []*leaseRecord
	for _, rec := range recs {
		if exact || rec.ExpiryUnixNs <= now {
			due = append(due, rec)
		}
	}
	if len(due) == 0 {
		return nil
	}
	ids := make([]int64, len(due))
	for i, rec := range due {
		ids[i] = rec.ID
	}
	renewed, err := s.node.LeaseRenewedAt(ctx, ids)
	if errors.Is(err, t4.ErrLeaseRenewUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}
	for i, rec := range due {
		rec.ExpiryUnixNs = max(rec.ExpiryUnixNs, renewed[i].Add(time.Duration(rec.GrantedTTL)*time.Second).UnixNano())
	}
	return nil
}

// refreshLease stores rec over the existing record of its lease. It returns
// errLeaseNotFound if the lease was revoked meanwhile, rather than bring it
// back.
func (s *Server) refreshLease(ctx context.Context, rec *leaseRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal lease: %v", err)
	}
	guards, err := s.leaseGuards(ctx, rec.ID)
	if err != nil {
		return err
	}
	if guards == nil {
		return sysstate.Put(ctx, s.node, leaseKey(rec.ID), data)
	}
	_, err = s.node.Txn(ctx, t4.TxnRequest{
		Guards:  guards,
		Success: []t4.TxnOp{{Type: t4.TxnMetaPut, Key: leaseKey(rec.ID), Value: data}},
	})
	if errors.Is(err, t4.ErrTxnGuard) {
		return errLeaseNotFound
	}
	return err
}

func (s *Server) listLeases(ctx context.Context, linearizable bool) ([]*leaseRecord, error) {
	var (
		kvs []t4.MetaKV
		err error
	)
	if linearizable {
		kvs, err = sysstate.LinearizableList(ctx, s.node, leasePrefix)
	} else {
		kvs, err = sysstate.List(s.node, leasePrefix)
	}
	if err != nil {
		return nil, err
	}
	out := make([]*leaseRecord, 0, len(kvs))
	for _, kv := range kvs {
		rec, err := decodeLease(kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func newLeaseID() (int64, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, err
	}
	id := int64(binary.BigEndian.Uint64(buf[:]) & math.MaxInt64)
	if id == 0 {
		return 1, nil
	}
	return id, nil
}

func userEvent(e t4.Event) (t4.Event, bool) {
	if e.KV == nil || isInternalKey(e.KV.Key) {
		return t4.Event{}, false
	}
	return e, true
}

func (s *Server) collectLeaseKeys(ctx context.Context, leaseID int64, linearizable bool) ([]string, error) {
	var (
		keys []string
		err  error
	)
	if linearizable {
		keys, err = s.node.LinearizableLeaseKeys(ctx, leaseID)
	} else {
		keys, err = s.node.LeaseKeys(leaseID)
	}
	if err != nil {
		return nil, err
	}
	out := keys[:0]
	for _, key := range keys {
		if !isInternalKey(key) {
			out = append(out, key)
		}
	}
	return out, nil
}

// revokeLease deletes the lease record and every key attached to it in one
// revision, and returns that revision.
func (s *Server) revokeLease(ctx context.Context, leaseID int64) (int64, error) {
	on, err := s.node.MetaEnabledSync(ctx)
	if err != nil {
		return 0, err
	}
	if on {
		// As etcd revokes: the leader finds the attached keys when the
		// revoke commits, so a key attached or detached by a concurrent
		// write is deleted, or kept, accordingly.
		resp, err := s.node.Txn(ctx, t4.TxnRequest{Success: []t4.TxnOp{
			{Type: t4.TxnDeleteLease, Lease: leaseID},
			{Type: t4.TxnMetaDelete, Key: leaseKey(leaseID)},
		}})
		return resp.Revision, err
	}
	// A legacy database may still be led by a v1.1 node during a rolling
	// upgrade, which ignores TxnDeleteLease. Delete the keys found attached
	// only while each still is, so a key a concurrent write detached
	// survives; a key attached after they were found is not deleted.
	for {
		keys, err := s.collectLeaseKeys(ctx, leaseID, true)
		if err != nil {
			return 0, err
		}
		conds := make([]t4.TxnCondition, 0, len(keys))
		ops := make([]t4.TxnOp, 0, len(keys)+1)
		for _, key := range keys {
			conds = append(conds, t4.TxnCondition{Key: key, Target: t4.TxnCondLease, Result: t4.TxnCondEqual, Lease: leaseID})
			ops = append(ops, t4.TxnOp{Type: t4.TxnDelete, Key: key})
		}
		ops = append(ops, t4.TxnOp{Type: t4.TxnDelete, Key: leaseKey(leaseID)})
		resp, err := s.node.Txn(ctx, t4.TxnRequest{Conditions: conds, Success: ops})
		if err != nil {
			return 0, err
		}
		if resp.Succeeded {
			return resp.Revision, nil
		}
	}
}

// leaseGuards returns guards that hold while the given leases exist, so a
// write attaching keys to them cannot commit after they are revoked. A
// legacy database gets none: a v1.1 leader would ignore them.
func (s *Server) leaseGuards(ctx context.Context, leases ...int64) ([]t4.TxnCondition, error) {
	on, err := s.node.MetaEnabledSync(ctx)
	if err != nil || !on {
		return nil, err
	}
	var guards []t4.TxnCondition
	for _, id := range leases {
		if id == 0 || slices.ContainsFunc(guards, func(g t4.TxnCondition) bool { return g.Key == leaseKey(id) }) {
			continue
		}
		guards = append(guards, t4.TxnCondition{Key: leaseKey(id), Target: t4.TxnCondMetaExists, Result: t4.TxnCondEqual, Version: 1})
	}
	return guards, nil
}

// errLeaseNotFound is returned for a lease that does not exist.
var errLeaseNotFound = status.Error(codes.NotFound, "lease not found")

func (s *Server) maybeStartLeaseLoop() {
	s.leaseLoopOnce.Do(func() {
		go s.leaseLoop()
	})
}

func (s *Server) leaseLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if !s.node.IsLeader() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		leases, err := s.listLeases(ctx, false)
		if err != nil {
			cancel()
			if err == t4.ErrClosed {
				return
			}
			continue
		}
		if err := s.leaseExpiries(ctx, leases, false); err != nil {
			cancel()
			continue
		}
		now := time.Now()
		for _, rec := range leases {
			if ttlRemaining(rec, now) == 0 {
				_, _ = s.revokeLease(ctx, rec.ID)
			}
		}
		cancel()
	}
}
