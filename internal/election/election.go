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

	"github.com/makhov/strata/pkg/object"
)

// LockKey is the fixed object-storage key for the leader lock.
const LockKey = "leader-lock"

// LockRecord is the content of the leader-lock object.
type LockRecord struct {
	NodeID       string `json:"node_id"`
	Term         uint64 `json:"term"`
	LeaderAddr   string `json:"leader_addr"`    // follower peer-stream address
	LastSeenNano int64  `json:"last_seen_nano"` // Unix ns; set by leader on liveness touch
	CommittedRev int64  `json:"committed_rev"`  // leader's highest committed revision; used as election fence
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
func (l *Lock) TakeOver(ctx context.Context, floorTerm uint64, committedRev int64) (*LockRecord, bool, error) {
	cur, err := l.readWithETag(ctx)
	if err != nil {
		return nil, false, err
	}

	if cur.rec != nil && cur.rec.NodeID != l.nodeID {
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
// Sets LastSeenNano to now so that a freshly elected leader is immediately
// visible as "alive" to any follower checking liveness — avoiding the window
// where the first periodic Touch hasn't fired yet.
func (l *Lock) writeAtomic(ctx context.Context, newTerm uint64, committedRev int64, observed *lockWithETag) (*LockRecord, bool, error) {
	rec := &LockRecord{
		NodeID:       l.nodeID,
		Term:         newTerm,
		LeaderAddr:   l.advertiseAddr,
		LastSeenNano: time.Now().UnixNano(),
		CommittedRev: committedRev,
	}
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

// Touch updates LastSeenNano and CommittedRev on the lock record to signal
// that this node is still the active leader. It must only be called when the
// caller has already verified (via Read) that it still holds the lock with the
// given term — i.e., under the leader's fenceMu write-lock. No additional
// read-back is performed because the term check was just done by the caller.
func (l *Lock) Touch(ctx context.Context, term uint64, leaderAddr string, committedRev int64) error {
	rec := &LockRecord{
		NodeID:       l.nodeID,
		Term:         term,
		LeaderAddr:   leaderAddr,
		LastSeenNano: time.Now().UnixNano(),
		CommittedRev: committedRev,
	}
	return l.write(ctx, rec)
}

// TouchIfMatch is like Touch but uses a conditional PUT (If-Match: <etag>)
// so that the write is atomic with respect to the preceding Read.
//
// If another node wrote the lock between the caller's Read and this call,
// the store returns ErrPreconditionFailed, which is returned unwrapped so
// the caller can detect supersession without a second round-trip.
//
// Falls back to unconditional Touch when the store does not support
// conditional writes (etag == "" or no ConditionalStore).
func (l *Lock) TouchIfMatch(ctx context.Context, term uint64, leaderAddr, etag string, committedRev int64) error {
	if l.conditional == nil || etag == "" {
		return l.Touch(ctx, term, leaderAddr, committedRev)
	}
	rec := &LockRecord{
		NodeID:       l.nodeID,
		Term:         term,
		LeaderAddr:   leaderAddr,
		LastSeenNano: time.Now().UnixNano(),
		CommittedRev: committedRev,
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return l.conditional.PutIfMatch(ctx, LockKey, bytes.NewReader(b), etag)
}
