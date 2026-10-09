package store

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// Lease index.
//
// Every live key attached to a lease has an entry leaseIdxKey(lease, key), so
// the keys of one lease are found by a prefix scan rather than by reading
// every key in the store.
//
// Apply only adds entries, and only for writes that attach a key to a lease:
// learning which lease a write detaches a key from would cost a read on every
// write. So the index may also hold stale entries, for keys that have since
// been deleted or moved to another lease. LeaseKeys skips those by checking
// each entry against the key, and compaction removes them: every stale entry
// has a superseded record that attached the key to that lease, and when
// compaction drops such a record it drops the entry too unless the key is
// still attached. Stale entries therefore last until the next compaction.
// Recover, which overwrites the record of a key a newer term replaced rather
// than superseding it, drops that key's entry itself.
//
// The index is local derived state, like the key index: it is not in the WAL,
// and each node maintains its own as it applies entries. A binary that does
// not know about it leaves it incomplete; leaseIdxMaintained detects that and
// Open rebuilds it.

// leaseApply records the lease index entries one apply batch adds. The batch
// is not readable before it commits, so a compaction later in the same batch
// learns from here that an entry it would otherwise judge stale is new.
type leaseApply struct {
	added map[string]struct{}
}

// liveLease is the lease r leaves its key attached to: none once deleted.
func liveLease(r *record) int64 {
	if r.delete {
		return 0
	}
	return r.lease
}

// attach writes into b the entry for key being attached to lease, if any.
func (la *leaseApply) attach(b *pebble.Batch, key string, lease int64) error {
	if lease <= 0 {
		return nil
	}
	k := leaseIdxKey(lease, key)
	if err := b.Set(k, nil, pebble.NoSync); err != nil {
		return fmt.Errorf("store: set lease idx %d %q: %w", lease, key, err)
	}
	if la.added == nil {
		la.added = make(map[string]struct{})
	}
	la.added[string(k)] = struct{}{}
	return nil
}

// discard writes into b the removal of key's entry under lease, for a record
// Recover discards along with the key's index entry, unless this batch added
// the entry.
func (la *leaseApply) discard(b *pebble.Batch, key string, lease int64) error {
	if lease <= 0 {
		return nil
	}
	k := leaseIdxKey(lease, key)
	if _, ok := la.added[string(k)]; ok {
		return nil
	}
	if err := b.Delete(k, pebble.NoSync); err != nil {
		return fmt.Errorf("store: delete lease idx %d %q: %w", lease, key, err)
	}
	return nil
}

// dropStaleLeaseIdx writes into b the removal of key's entry under lease, for
// a superseded record compaction drops, unless the key is still attached to
// lease. cur caches current leases across one compaction.
func (s *Store) dropStaleLeaseIdx(b *pebble.Batch, la *leaseApply, cur map[string]int64, key string, lease int64) error {
	if lease <= 0 {
		return nil
	}
	k := leaseIdxKey(lease, key)
	if _, ok := la.added[string(k)]; ok {
		return nil
	}
	now, ok := cur[key]
	if !ok {
		var err error
		if now, err = currentLease(s.db, key); err != nil {
			return err
		}
		cur[key] = now
	}
	if now == lease {
		return nil
	}
	if err := b.Delete(k, pebble.NoSync); err != nil {
		return fmt.Errorf("store: delete lease idx %d %q: %w", lease, key, err)
	}
	return nil
}

// currentLease returns the lease key is attached to in r, or 0 if it has none
// or is not live.
func currentLease(r pebble.Reader, key string) (int64, error) {
	ie, err := idxEntryFrom(r, key)
	if err != nil || ie.rev == 0 {
		return 0, err
	}
	kv, err := logEntryForIdx(r, make([]byte, logKeyScratchSize), key, ie)
	if err != nil || kv == nil {
		return 0, err
	}
	return kv.Lease, nil
}

// LeaseKeys returns the live keys attached to lease, in key order.
func (s *Store) LeaseKeys(lease int64) ([]string, error) {
	if lease <= 0 {
		return nil, nil
	}
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	lower, upper := leaseIdxBounds(lease)
	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("store: lease keys iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[len(lower):])
		// Skip stale entries: the key was deleted or moved to another lease.
		cur, err := currentLease(snap, key)
		if err != nil {
			return nil, err
		}
		if cur == lease {
			keys = append(keys, key)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("store: lease keys scan: %w", err)
	}
	return keys, nil
}

// leaseIdxBatchSize bounds how many entries one rebuild batch writes.
const leaseIdxBatchSize = 10000

// ensureLeaseIdx rebuilds the lease index unless the last batch committed
// kept it up to date. A store a binary without the index wrote to, including
// every store created before it existed, is rebuilt once here.
func (s *Store) ensureLeaseIdx(log logger) error {
	v, closer, err := s.db.Get(metaCurrentRevKey)
	switch {
	case err == pebble.ErrNotFound:
		if s.currentRev == 0 {
			return nil // nothing applied yet
		}
	case err != nil:
		return fmt.Errorf("store: read current rev: %w", err)
	default:
		maintained := len(v) > 8 && v[8] == leaseIdxMaintained
		_ = closer.Close()
		if maintained {
			return nil
		}
	}
	start := time.Now()
	n, err := s.rebuildLeaseIdx()
	if err != nil {
		return err
	}
	if log != nil {
		log.Warnf("t4: rebuilt lease index at revision %d: %d keys attached to leases (%s)", s.currentRev, n, time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// rebuildLeaseIdx replaces the lease index with one built from the live keys
// and records it as current. It returns how many entries it wrote. A crash
// part way leaves leaseIdxMaintained unset, so the next Open starts over.
func (s *Store) rebuildLeaseIdx() (int, error) {
	if err := s.db.DeleteRange([]byte{prefixLease}, []byte{prefixLease + 1}, pebble.NoSync); err != nil {
		return 0, fmt.Errorf("store: clear lease idx: %w", err)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixIdx}, UpperBound: idxUpper})
	if err != nil {
		return 0, fmt.Errorf("store: lease idx rebuild iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	lk := make([]byte, logKeyScratchSize)
	n, inBatch := 0, 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[1:])
		kv, err := logEntryForIdx(s.db, lk, key, decodeIdx(iter.Value()))
		if err != nil {
			return n, err
		}
		if kv == nil || kv.Lease <= 0 {
			continue
		}
		lease := kv.Lease
		if err := b.Set(leaseIdxKey(lease, key), nil, pebble.NoSync); err != nil {
			return n, fmt.Errorf("store: set lease idx %d %q: %w", lease, key, err)
		}
		n++
		if inBatch++; inBatch == leaseIdxBatchSize {
			if err := b.Commit(pebble.NoSync); err != nil {
				return n, fmt.Errorf("store: commit lease idx: %w", err)
			}
			_ = b.Close()
			b = s.db.NewBatch()
			inBatch = 0
		}
	}
	if err := iter.Error(); err != nil {
		return n, fmt.Errorf("store: lease idx rebuild scan: %w", err)
	}
	if err := b.Set(metaCurrentRevKey, encodeCurrentRev(s.currentRev), pebble.NoSync); err != nil {
		return n, fmt.Errorf("store: set current rev: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return n, fmt.Errorf("store: commit lease idx: %w", err)
	}
	return n, nil
}
