package etcd

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
	"github.com/t4db/t4/etcd/auth"
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

func decodeLease(kv *t4.KeyValue) (*leaseRecord, error) {
	if kv == nil {
		return nil, nil
	}
	var rec leaseRecord
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return nil, status.Errorf(codes.Internal, "decode lease: %v", err)
	}
	return &rec, nil
}

func (s *Server) getLease(ctx context.Context, id int64, linearizable bool) (*leaseRecord, error) {
	if err := validateLeaseID(id); err != nil {
		return nil, err
	}
	var (
		kv  *t4.KeyValue
		err error
	)
	if linearizable {
		kv, err = s.node.LinearizableGet(ctx, leaseKey(id), t4.WithInternalRead())
	} else {
		kv, err = s.node.Get(leaseKey(id), t4.WithInternalRead())
	}
	if err != nil {
		return nil, err
	}
	rec, err := decodeLease(kv)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, status.Error(codes.NotFound, "lease not found")
	}
	if time.Now().UnixNano() >= rec.ExpiryUnixNs {
		return nil, status.Error(codes.NotFound, "lease not found")
	}
	return rec, nil
}

func (s *Server) putLease(ctx context.Context, rec *leaseRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal lease: %v", err)
	}
	if _, err := s.node.Put(ctx, leaseKey(rec.ID), data, 0); err != nil {
		return err
	}
	return nil
}

func (s *Server) listLeases(ctx context.Context, linearizable bool) ([]*leaseRecord, error) {
	var (
		kvs []*t4.KeyValue
		err error
	)
	if linearizable {
		kvs, err = s.node.LinearizableList(ctx, leasePrefix, t4.WithInternalRead())
	} else {
		kvs, err = s.node.List(leasePrefix, t4.WithInternalRead())
	}
	if err != nil {
		return nil, err
	}
	out := make([]*leaseRecord, 0, len(kvs))
	for _, kv := range kvs {
		rec, err := decodeLease(kv)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			out = append(out, rec)
		}
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

// revokeLease deletes every key attached to leaseID and the lease record
// itself in a single atomic transaction. Crash-safety: either every delete
// commits to the WAL together or none of them do, so a leader crash during
// revoke can never leave a "half-revoked" lease (record present but attached
// keys missing, or vice versa).
//
// Note: there is still a small race between collectLeaseKeys and the Txn
// where a concurrent Put attaching a new key to leaseID could land. Etcd has
// the same race; v1 accepts it.
func (s *Server) revokeLease(ctx context.Context, leaseID int64) error {
	keys, err := s.collectLeaseKeys(ctx, leaseID, true)
	if err != nil {
		return err
	}
	ops := make([]t4.TxnOp, 0, len(keys)+1)
	for _, key := range keys {
		ops = append(ops, t4.TxnOp{Type: t4.TxnDelete, Key: key})
	}
	ops = append(ops, t4.TxnOp{Type: t4.TxnDelete, Key: leaseKey(leaseID)})
	if _, err := s.node.Txn(ctx, t4.TxnRequest{Success: ops}); err != nil {
		return err
	}
	return nil
}

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
		now := time.Now()
		for _, rec := range leases {
			if ttlRemaining(rec, now) == 0 {
				_ = s.revokeLease(ctx, rec.ID)
			}
		}
		cancel()
	}
}
