// Package election implements S3-based leader election.
//
// The protocol uses atomic conditional PUT operations to safely resolve races:
//
//  1. Read the current lock object (with its ETag).
//     2a. If absent: PutIfAbsent — only one concurrent writer can succeed.
//     2b. If owned by another: become a follower.
//     2c. If owned by us (restart) or being taken over (TakeOver): PutIfMatch
//     using the observed ETag — only succeeds if no one wrote between our
//     Read and our Put.
//  3. On ErrPreconditionFailed: re-read and retry once to find the winner.
//
// There is no TTL on the lock. Liveness is detected via the WAL stream
// (followers attempt a TakeOver after the stream becomes unreachable).
// Leaders do an infrequent read-only watch to detect if they have been
// superseded and step down gracefully.
package election

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// LockKey is the fixed object-storage key for the leader lock.
const LockKey = "leader-lock"

// FastTTL is how long a lock written in fast mode stays valid, and the
// liveness TTL of releases before ValidUntilNano existed: they back off while
// LastSeenNano is younger than this. Equal to peer.LeaderLivenessTTL.
const FastTTL = 6 * time.Second

// LockRecord is the content of the leader-lock object.
//
// RenewedNano and ValidUntilNano are written by every lock write of a leader
// and cleared when it releases the lock. LastSeenNano is kept at
// ValidUntilNano - FastTTL, so that nodes of earlier releases, which back off
// while LastSeenNano is younger than FastTTL, back off until ValidUntilNano.
type LockRecord struct {
	NodeID         string `json:"node_id"`
	Term           uint64 `json:"term"`
	LeaderAddr     string `json:"leader_addr"`                // follower peer-stream address
	LastSeenNano   int64  `json:"last_seen_nano"`             // Unix ns; ValidUntilNano - FastTTL, 0 once released
	CommittedRev   int64  `json:"committed_rev"`              // leader's highest committed revision; used as election fence
	RenewedNano    int64  `json:"renewed_nano,omitempty"`     // Unix ns (leader clock) the last lock write started
	ValidUntilNano int64  `json:"valid_until_nano,omitempty"` // Unix ns (leader clock) before which nobody else may take over
}

// Released reports whether the leader released the lock on shutdown.
func (r *LockRecord) Released() bool {
	return r.LastSeenNano == 0 && r.RenewedNano == 0
}

// Renewed returns when the lock was last written by its leader. Records of
// earlier releases carry only LastSeenNano.
func (r *LockRecord) Renewed() time.Time {
	if r.RenewedNano != 0 {
		return time.Unix(0, r.RenewedNano)
	}
	return time.Unix(0, r.LastSeenNano)
}

// ValidUntil returns the time before which no node the leader does not know
// about may take over.
func (r *LockRecord) ValidUntil() time.Time {
	if r.ValidUntilNano != 0 {
		return time.Unix(0, r.ValidUntilNano)
	}
	return time.Unix(0, r.LastSeenNano).Add(FastTTL)
}

// stamp records a lock write that started at start and stays valid for ttl.
func (r *LockRecord) stamp(start time.Time, ttl time.Duration) {
	r.RenewedNano = start.UnixNano()
	r.ValidUntilNano = start.Add(ttl).UnixNano()
	r.LastSeenNano = r.ValidUntilNano - int64(FastTTL)
}

// lockWithETag pairs a decoded lock record with the ETag of the S3 object
// that was read, so that callers can use PutIfMatch for atomic updates.
type lockWithETag struct {
	rec  *LockRecord
	etag string // "" means the object was absent
}

// Lock manages leader election via a single S3 object.
type Lock struct {
	store         object.Store
	nodeID        string
	advertiseAddr string
	// conditional is non-nil when the store supports atomic conditional writes.
	conditional object.ConditionalStore
}

// NewLock creates a Lock.
// advertiseAddr is the address followers use to reach this node's peer stream.
func NewLock(store object.Store, nodeID, advertiseAddr string) *Lock {
	l := &Lock{
		store:         store,
		nodeID:        nodeID,
		advertiseAddr: advertiseAddr,
	}
	if cs, ok := store.(object.ConditionalStore); ok {
		l.conditional = cs
	}
	return l
}

// TryAcquire attempts to acquire the leader lock at startup.
//
// It writes only if the lock is absent or already owned by this node.
// If another node holds the lock, it returns (existing, false, nil) so the
// caller can become a follower of that node.
//
// floorTerm ensures the new term is always strictly greater than any
// previously observed term, preventing term regression after restart.
// committedRev is written into the lock so candidates can use it as a
// revision fence (see TakeOver).
func (l *Lock) TryAcquire(ctx context.Context, floorTerm uint64, committedRev int64) (*LockRecord, bool, error) {
	cur, err := l.readWithETag(ctx)
	if err != nil {
		return nil, false, err
	}

	// Another node holds the lock — become a follower.
	if cur.rec != nil && cur.rec.NodeID != l.nodeID {
		return cur.rec, false, nil
	}

	newTerm := floorTerm + 1
	if cur.rec != nil && cur.rec.Term >= newTerm {
		newTerm = cur.rec.Term + 1
	}

	return l.writeAtomic(ctx, newTerm, committedRev, cur)
}

// TakeOver forcefully attempts to acquire the lock, overwriting any existing
// owner. Called by a follower after it has determined the leader is
// unreachable.  Uses an atomic conditional PUT to resolve races between
// concurrent candidates: only the node that observed a specific ETag can
// overwrite it.
//
// If, by the time TakeOver reads the lock, a different node already holds a
// term higher than floorTerm, that node won a concurrent TakeOver race.
// Back off and return (winner, false) so the caller can follow the new leader.
//
// committedRev is the caller's own highest committed revision.  If the current
// lock's CommittedRev is higher, this node is behind the departing leader and
// must not take over — it would either discard those entries or be unable to
// serve reads that clients already received.
//
// allow, if not nil, decides whether the current holder's lock may be taken
// (see the leader-liveness rules in the caller). It is evaluated on the very
// record whose ETag the conditional write is based on, so the holder cannot
// renew between the check and the takeover.
func (l *Lock) TakeOver(ctx context.Context, floorTerm uint64, committedRev int64, allow func(*LockRecord) bool) (*LockRecord, bool, error) {
	cur, err := l.readWithETag(ctx)
	if err != nil {
		return nil, false, err
	}

	if cur.rec != nil && cur.rec.NodeID != l.nodeID {
		if allow != nil && !allow(cur.rec) {
			return cur.rec, false, nil
		}
		// Another node already took over at a higher term: back off.
		if cur.rec.Term > floorTerm {
			return cur.rec, false, nil
		}
		// Current leader has committed entries we haven't applied yet: back off
		// to prevent promoting a node that is missing data.
		if cur.rec.CommittedRev > committedRev {
			return cur.rec, false, nil
		}
	}

	newTerm := floorTerm + 1
	if cur.rec != nil && cur.rec.Term >= newTerm {
		newTerm = cur.rec.Term + 1
	}

	return l.writeAtomic(ctx, newTerm, committedRev, cur)
}

// ErrNotOwner is returned by Relinquish when the lock no longer belongs to
// the caller at the given term.
var ErrNotOwner = errors.New("election: lock held by another leader")

// Relinquish marks the lock as released by a leader that has stopped serving:
// LastSeenNano is cleared, so no candidate waits out the liveness TTL, while
// CommittedRev keeps fencing out candidates that are behind. It writes only if
// the caller still owns the lock at term, conditionally on the ETag it read,
// and otherwise returns ErrNotOwner without touching a newer leader's lock.
func (l *Lock) Relinquish(ctx context.Context, term uint64, committedRev int64) error {
	cur, err := l.readWithETag(ctx)
	if err != nil {
		return err
	}
	if cur.rec == nil || cur.rec.NodeID != l.nodeID || cur.rec.Term != term {
		return ErrNotOwner
	}
	rec := *cur.rec
	rec.LastSeenNano, rec.RenewedNano, rec.ValidUntilNano = 0, 0, 0
	if committedRev > rec.CommittedRev {
		rec.CommittedRev = committedRev
	}
	if l.conditional == nil || cur.etag == "" {
		return l.write(ctx, &rec)
	}
	b, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	if err := l.conditional.PutIfMatch(ctx, LockKey, bytes.NewReader(b), cur.etag); err != nil {
		if errors.Is(err, object.ErrPreconditionFailed) {
			return ErrNotOwner
		}
		return fmt.Errorf("election: relinquish lock: %w", err)
	}
	return nil
}

// Release deletes the lock. Safe to call if the lock is not held.
func (l *Lock) Release(ctx context.Context) error {
	return l.store.Delete(ctx, LockKey)
}

// Read returns the current lock record, or nil if none exists.
func (l *Lock) Read(ctx context.Context) (*LockRecord, error) {
	cur, err := l.readWithETag(ctx)
	if err != nil {
		return nil, err
	}
	return cur.rec, nil
}

// ReadETag returns the current lock record together with the S3 ETag of the
// object.  The ETag can be passed to TouchIfMatch to make the subsequent
// liveness touch conditional, closing the Read→Touch race.
// Returns ("", nil, nil) if the lock object is absent.
func (l *Lock) ReadETag(ctx context.Context) (*LockRecord, string, error) {
	cur, err := l.readWithETag(ctx)
	if err != nil {
		return nil, "", err
	}
	return cur.rec, cur.etag, nil
}

// readWithETag reads the lock and returns it together with its ETag.
func (l *Lock) readWithETag(ctx context.Context) (*lockWithETag, error) {
	if l.conditional != nil {
		res, err := l.conditional.GetETag(ctx, LockKey)
		if err == object.ErrNotFound {
			return &lockWithETag{}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("election: read lock: %w", err)
		}
		defer res.Body.Close()
		var rec LockRecord
		if err := json.NewDecoder(res.Body).Decode(&rec); err != nil {
			return nil, fmt.Errorf("election: decode lock: %w", err)
		}
		return &lockWithETag{rec: &rec, etag: res.ETag}, nil
	}
	// Fallback: store doesn't support conditional ops.
	rc, err := l.store.Get(ctx, LockKey)
	if err == object.ErrNotFound {
		return &lockWithETag{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("election: read lock: %w", err)
	}
	defer rc.Close()
	var rec LockRecord
	if err := json.NewDecoder(rc).Decode(&rec); err != nil {
		return nil, fmt.Errorf("election: decode lock: %w", err)
	}
	return &lockWithETag{rec: &rec}, nil
}

// writeAtomic writes a new lock record for this node using a conditional PUT
// when possible.  If the store doesn't support conditional writes, it falls
// back to the old optimistic read-back approach.
//
// Stamps the record as renewed now, in fast mode, so that a freshly elected
// leader is immediately visible as alive to any follower checking liveness.
func (l *Lock) writeAtomic(ctx context.Context, newTerm uint64, committedRev int64, observed *lockWithETag) (*LockRecord, bool, error) {
	rec := &LockRecord{
		NodeID:       l.nodeID,
		Term:         newTerm,
		LeaderAddr:   l.advertiseAddr,
		CommittedRev: committedRev,
	}
	rec.stamp(time.Now(), FastTTL)
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, false, err
	}

	if l.conditional != nil {
		var putErr error
		if observed.rec == nil {
			// Lock is absent: use If-None-Match: * — only one writer can win.
			putErr = l.conditional.PutIfAbsent(ctx, LockKey, bytes.NewReader(b))
		} else {
			// Lock exists: use If-Match: <etag> — only wins if nobody else
			// wrote between our Read and our Put.
			putErr = l.conditional.PutIfMatch(ctx, LockKey, bytes.NewReader(b), observed.etag)
		}
		if putErr == nil {
			return rec, true, nil
		}
		if errors.Is(putErr, object.ErrPreconditionFailed) {
			// Someone else won the race — re-read to find out who.
			winner, err := l.Read(ctx)
			if err != nil {
				return nil, false, err
			}
			return winner, false, nil
		}
		return nil, false, fmt.Errorf("election: conditional write: %w", putErr)
	}

	// Fallback: unconditional write + read-back (old behaviour).
	if err := l.store.Put(ctx, LockKey, bytes.NewReader(b)); err != nil {
		return nil, false, fmt.Errorf("election: write lock: %w", err)
	}
	time.Sleep(100 * time.Millisecond)
	verify, err := l.Read(ctx)
	if err != nil {
		return nil, false, err
	}
	if verify == nil || verify.NodeID != l.nodeID || verify.Term != newTerm {
		return verify, false, nil
	}
	return rec, true, nil
}

func (l *Lock) write(ctx context.Context, rec *LockRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := l.store.Put(ctx, LockKey, bytes.NewReader(b)); err != nil {
		return fmt.Errorf("election: write lock: %w", err)
	}
	return nil
}

// Renew rewrites the lock of the leader holding term, recording a renewal
// that started at start and stays valid for ttl, and committedRev as the
// election fence. The write is conditional on etag, the ETag of the caller's
// preceding read, so that it fails with object.ErrPreconditionFailed
// (returned unwrapped) if another node wrote the lock in between. Without
// conditional writes (etag == "" or no ConditionalStore) it is unconditional.
func (l *Lock) Renew(ctx context.Context, term uint64, leaderAddr, etag string, committedRev int64, start time.Time, ttl time.Duration) error {
	rec := &LockRecord{
		NodeID:       l.nodeID,
		Term:         term,
		LeaderAddr:   leaderAddr,
		CommittedRev: committedRev,
	}
	rec.stamp(start, ttl)
	if l.conditional == nil || etag == "" {
		return l.write(ctx, rec)
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return l.conditional.PutIfMatch(ctx, LockKey, bytes.NewReader(b), etag)
}
