package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/wal"
)

// logger is the minimal logging interface required by Store.
type logger interface {
	Warnf(format string, args ...interface{})
}

// Sentinel read errors.
var (
	ErrClosed         = errors.New("store: closed")
	ErrCompacted      = errors.New("store: required revision has been compacted")
	ErrFutureRevision = errors.New("store: required revision is a future revision")
)

// Store is the Pebble-backed state machine.
//
// The key space is described in keys.go. WAL entries are applied in order;
// each application advances the current revision and notifies watchers.
type Store struct {
	db *pebble.DB

	// currentRev, compactRev and lastSeq are accessed atomically.
	// currentRev is the highest user-visible revision (advanced by data
	// writes only). lastSeq is the highest WAL/peer-stream sequence applied
	// (advanced by every entry, including Compact). They diverge after
	// Compact entries, which consume a sequence but not a revision.
	currentRev int64
	compactRev int64
	lastSeq    int64

	// mu protects notify and watch prefix accounting.
	mu        sync.RWMutex
	notify    chan struct{} // closed and replaced on each revision advance
	closed    chan struct{} // closed once when Store.Close is called
	closeOnce sync.Once

	// watchMu serializes watch registration against Close. sync.WaitGroup
	// requires Add not to race with Wait when the counter can be zero.
	watchMu   sync.Mutex
	watcherWg sync.WaitGroup // tracks active watchLoop goroutines

	watchPrefixes map[string]int

	// watchHubMu protects the live-watch registry. Live revisions are decoded
	// once after Apply and fanned out in memory instead of making every watch
	// rescan the Pebble revision log.
	watchHubMu   sync.Mutex
	watchers     map[uint64]*watchSubscription
	nextWatchID  uint64
	dispatchOnce sync.Once
	dispatchWg   sync.WaitGroup
}

// lockRetryTimeout is how long Open retries when another process holds the
// pebble LOCK file. This covers the window where a previous pod is still
// terminating when the replacement starts.
const lockRetryTimeout = 30 * time.Second

// PebbleOption is a functional option for configuring pebble.Options.
type PebbleOption = func(*pebble.Options)

// Open opens (or creates) the Pebble database at dir and returns a Store.
// The caller should call Recover to replay WAL entries before serving requests.
//
// If the database is locked by another process, Open retries for up to
// lockRetryTimeout before returning an error. This handles the Kubernetes pod
// replacement race where the old instance has not yet released the lock.
func Open(dir string, log logger, extraOpts ...func(*pebble.Options)) (*Store, error) {
	opts := &pebble.Options{}
	for _, fn := range extraOpts {
		fn(opts)
	}
	deadline := time.Now().Add(lockRetryTimeout)
	for {
		db, err := pebble.Open(dir, opts)
		if err == nil {
			s := &Store{
				db:            db,
				notify:        make(chan struct{}),
				closed:        make(chan struct{}),
				watchPrefixes: make(map[string]int),
			}
			if err := s.loadMeta(); err != nil {
				db.Close()
				return nil, err
			}
			return s, nil
		}
		if !isPebbleLockError(err) || time.Now().After(deadline) {
			return nil, fmt.Errorf("store: open pebble %q: %w", dir, err)
		}
		log.Warnf("t4: pebble locked at %q, retrying in 1s (previous instance still terminating?)", dir)
		time.Sleep(time.Second)
	}
}

// OpenReadOnly opens an existing Pebble database in read-only mode.
// It is intended for offline inspection tools and never creates the DB.
func OpenReadOnly(dir string, extraOpts ...func(*pebble.Options)) (*Store, error) {
	opts := &pebble.Options{ReadOnly: true}
	for _, fn := range extraOpts {
		fn(opts)
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("store: open read-only pebble %q: %w", dir, err)
	}
	s := &Store{db: db, notify: make(chan struct{}), closed: make(chan struct{})}
	if err := s.loadMeta(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// isPebbleLockError reports whether err is a lock-file contention error.
// Pebble names its lock file "LOCK", so the path always appears in the message.
func isPebbleLockError(err error) bool {
	return strings.Contains(err.Error(), "LOCK")
}

// OpenMem opens an in-memory Pebble store (for testing / followers).
func OpenMem() (*Store, error) {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		return nil, fmt.Errorf("store: open in-memory pebble: %w", err)
	}
	return &Store{db: db, notify: make(chan struct{}), closed: make(chan struct{})}, nil
}

// loadMeta reads the compact and current revisions from Pebble.
//
// currentRev is stored explicitly in metaCurrentRevKey (written by Apply and
// Recover). This is necessary because OpCompact entries do not write a log key,
// so scanning log entries would return a stale revision when the last WAL entry
// in a checkpoint was a compaction. Older stores without the meta key fall back
// to scanning log entries for backward compatibility.
func (s *Store) loadMeta() error {
	// Read compact revision.
	v, closer, err := s.db.Get(metaCompactKey)
	if err == nil {
		s.compactRev = decodeRev(v)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return fmt.Errorf("store: read compact rev: %w", err)
	}

	// Read last applied WAL sequence. Older stores written before
	// metaLastSeqKey fall back to currentRev (which equals sequence in the
	// pre-Compact-doesn't-bump-rev world).
	v, closer, err = s.db.Get(metaLastSeqKey)
	if err == nil {
		s.lastSeq = decodeRev(v)
		_ = closer.Close()
	} else if err != pebble.ErrNotFound {
		return fmt.Errorf("store: read last seq: %w", err)
	}

	// Read current revision from explicit meta key (written since the
	// metaCurrentRevKey was introduced).
	v, closer, err = s.db.Get(metaCurrentRevKey)
	if err == nil {
		s.currentRev = decodeRev(v)
		closer.Close()
		if s.lastSeq < s.currentRev {
			s.lastSeq = s.currentRev
		}
		return nil
	} else if err != pebble.ErrNotFound {
		return fmt.Errorf("store: read current rev: %w", err)
	}

	// Fallback for stores written before metaCurrentRevKey: derive current
	// revision by scanning to the last log entry.
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: logLower,
		UpperBound: logUpper,
	})
	if err != nil {
		return fmt.Errorf("store: new iter for loadMeta: %w", err)
	}
	defer func() { _ = iter.Close() }()
	if iter.Last() {
		s.currentRev = decodeLogKey(iter.Key())
	}
	if s.lastSeq < s.currentRev {
		s.lastSeq = s.currentRev
	}
	return nil
}

// SignalClose closes the s.closed channel (idempotent). It unblocks any
// goroutines waiting in WaitForRevision or watchLoop without closing Pebble.
// node.Close calls this before waiting on readWg so that in-flight
// WaitForRevision callers (which hold readWg) can return ErrClosed promptly.
func (s *Store) SignalClose() {
	s.closeOnce.Do(func() { close(s.closed) })
}

// Close closes the underlying Pebble database.
func (s *Store) Close() error {
	s.watchMu.Lock()
	s.SignalClose()
	s.watchMu.Unlock()
	s.watcherWg.Wait()
	s.dispatchWg.Wait()
	return s.db.Close()
}

// Pebble exposes the underlying *pebble.DB for checkpoint creation.
func (s *Store) Pebble() *pebble.DB { return s.db }

// Flush forces Pebble to flush any buffered writes so a subsequent checkpoint
// captures the latest applied state even when live commits use pebble.NoSync.
func (s *Store) Flush() error { return s.db.Flush() }

// CurrentRevision returns the latest applied revision.
func (s *Store) CurrentRevision() int64 { return atomic.LoadInt64(&s.currentRev) }

// CompactRevision returns the oldest revision still available.
func (s *Store) CompactRevision() int64 { return atomic.LoadInt64(&s.compactRev) }

// LastSequence returns the highest WAL/peer-stream sequence applied. Used by
// WAL-replay code to validate stream continuity. Diverges from
// CurrentRevision after Compact entries.
func (s *Store) LastSequence() int64 { return atomic.LoadInt64(&s.lastSeq) }

// RevisionSample records that rev was current at ts. Samples are operational
// metadata used by time-window autocompaction; they are not part of the WAL
// data model and are safe to recreate approximately.
func (s *Store) RevisionSample(rev int64, ts time.Time) error {
	if rev <= 0 || ts.IsZero() {
		return nil
	}
	if err := s.db.Set(revisionSampleKey(ts.UnixNano()), encodeRev(rev), pebble.NoSync); err != nil {
		return fmt.Errorf("store: set revision sample rev=%d: %w", rev, err)
	}
	return nil
}

// LatestRevisionSample returns the newest revision/time sample in the store.
func (s *Store) LatestRevisionSample() (rev int64, ts time.Time, ok bool, err error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: revisionSampleLower,
		UpperBound: revisionSampleUpper,
	})
	if err != nil {
		return 0, time.Time{}, false, fmt.Errorf("store: latest revision sample iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, time.Time{}, false, fmt.Errorf("store: latest revision sample: %w", err)
		}
		return 0, time.Time{}, false, nil
	}
	return decodeRev(iter.Value()), time.Unix(0, decodeRevisionSampleKey(iter.Key())), true, nil
}

// RevisionSampleAtOrBefore returns the newest revision/time sample at or
// before cutoff.
func (s *Store) RevisionSampleAtOrBefore(cutoff time.Time) (rev int64, ts time.Time, ok bool, err error) {
	if cutoff.IsZero() {
		return 0, time.Time{}, false, nil
	}
	upper := revisionSampleKey(cutoff.UnixNano() + 1)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: revisionSampleLower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, time.Time{}, false, fmt.Errorf("store: revision sample iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, time.Time{}, false, fmt.Errorf("store: revision sample: %w", err)
		}
		return 0, time.Time{}, false, nil
	}
	return decodeRev(iter.Value()), time.Unix(0, decodeRevisionSampleKey(iter.Key())), true, nil
}

// DeleteRevisionSamplesBefore removes sample metadata older than cutoff.
func (s *Store) DeleteRevisionSamplesBefore(cutoff time.Time) error {
	if cutoff.IsZero() {
		return nil
	}
	if err := s.db.DeleteRange(revisionSampleLower, revisionSampleKey(cutoff.UnixNano()), pebble.NoSync); err != nil {
		return fmt.Errorf("store: delete revision samples: %w", err)
	}
	return nil
}

// Apply applies a batch of WAL entries to the store and notifies watchers.
// Entries must be ordered by revision. Apply is not safe for concurrent use.
func (s *Store) Apply(entries []wal.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	b := s.db.NewBatch()
	var maxRev, maxSeq int64
	for i := range entries {
		e := &entries[i]
		if e.Op == wal.OpCompact {
			// PrevRevision carries the compact target (see node.go Compact).
			if err := s.applyCompact(b, e.PrevRevision); err != nil {
				_ = b.Close()
				return err
			}
			if seq := e.Sequence(); seq > maxSeq {
				maxSeq = seq
			}
			continue
		}
		if err := s.applyEntry(b, e); err != nil {
			_ = b.Close()
			return err
		}
		if e.Revision > maxRev {
			maxRev = e.Revision
		}
		if seq := e.Sequence(); seq > maxSeq {
			maxSeq = seq
		}
	}
	if err := s.commitBatch(b, maxRev, maxSeq, false); err != nil {
		return err
	}
	s.broadcast()
	return nil
}

// commitBatch writes the current-revision and last-sequence meta keys into b,
// commits it (durably when sync is true), then advances the in-memory
// currentRev/lastSeq counters. The counters only ever move forward. On any
// error b is closed and the error is returned.
func (s *Store) commitBatch(b *pebble.Batch, maxRev, maxSeq int64, sync bool) error {
	if maxRev > 0 {
		if err := b.Set(metaCurrentRevKey, encodeRev(maxRev), pebble.NoSync); err != nil {
			_ = b.Close()
			return fmt.Errorf("store: set current rev: %w", err)
		}
	}
	if maxSeq > 0 {
		if err := b.Set(metaLastSeqKey, encodeRev(maxSeq), pebble.NoSync); err != nil {
			_ = b.Close()
			return fmt.Errorf("store: set last seq: %w", err)
		}
	}
	writeOpts := pebble.NoSync
	if sync {
		writeOpts = pebble.Sync
	}
	if err := b.Commit(writeOpts); err != nil {
		_ = b.Close()
		return fmt.Errorf("store: commit batch: %w", err)
	}
	if maxRev > atomic.LoadInt64(&s.currentRev) {
		atomic.StoreInt64(&s.currentRev, maxRev)
	}
	if maxSeq > atomic.LoadInt64(&s.lastSeq) {
		atomic.StoreInt64(&s.lastSeq, maxSeq)
	}
	return nil
}

// Recover applies entries without broadcasting to watchers. Used during
// startup replay before the node is serving requests.
func (s *Store) Recover(entries []wal.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	b := s.db.NewBatch()
	var maxRev, maxSeq int64
	for i := range entries {
		e := &entries[i]
		if seq := e.Sequence(); seq > maxSeq {
			maxSeq = seq
		}
		if e.Op == wal.OpCompact {
			if err := s.applyCompact(b, e.PrevRevision); err != nil {
				_ = b.Close()
				return err
			}
		} else {
			// Term-conflict cleanup: during WAL replay after a leader change, a
			// newer term may write a different key at the same revision as the
			// old term. Remove any stale index pointer before overwriting the
			// log entry so idx[oldKey]=rev doesn't dangle.
			//
			// For OpTxn entries we must also purge any stale sub-keys at this
			// revision written by a previous term (single-key or txn).
			if e.Op == wal.OpTxn {
				// Delete all existing log entries in [logKey(rev), logKey(rev+1))
				// and remove any dangling idx pointers they reference.
				lo := logKey(e.Revision)
				hi := logKey(e.Revision + 1)
				iter, iterErr := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
				if iterErr == nil {
					for iter.First(); iter.Valid(); iter.Next() {
						if stale, serr := unmarshalRecord(iter.Value()); serr == nil && !stale.delete {
							_ = b.Delete(idxKey(stale.key), pebble.NoSync)
						}
						_ = b.Delete(iter.Key(), pebble.NoSync)
					}
					iter.Close()
				}
			} else {
				if old, closer, err := s.db.Get(logKey(e.Revision)); err == nil {
					r, rerr := unmarshalRecord(old)
					closer.Close()
					if rerr == nil && !r.delete && r.key != e.Key {
						if err := b.Delete(idxKey(r.key), pebble.NoSync); err != nil {
							b.Close()
							return fmt.Errorf("store: cleanup stale idx %q rev=%d: %w", r.key, e.Revision, err)
						}
					}
				}
			}
			if err := s.applyEntry(b, e); err != nil {
				b.Close()
				return err
			}
		}
		if e.Op != wal.OpCompact && e.Revision > maxRev {
			maxRev = e.Revision
		}
	}
	return s.commitBatch(b, maxRev, maxSeq, true)
}

func (s *Store) applyEntry(b *pebble.Batch, e *wal.Entry) error {
	if e.Op == wal.OpTxn {
		return s.applyTxnEntry(b, e)
	}
	lk := logKey(e.Revision)

	r := &record{
		key:            e.Key,
		value:          e.Value,
		createRevision: e.CreateRevision,
		prevRevision:   e.PrevRevision,
		version:        entryVersion(e.Version),
		lease:          e.Lease,
		create:         e.Op == wal.OpCreate,
		delete:         e.Op == wal.OpDelete,
	}
	if err := b.Set(lk, marshalRecord(r), pebble.NoSync); err != nil {
		return fmt.Errorf("store: set log key rev=%d: %w", e.Revision, err)
	}
	ik := idxKey(e.Key)
	if e.Op == wal.OpDelete {
		if err := b.Delete(ik, pebble.NoSync); err != nil {
			return fmt.Errorf("store: delete idx key %q: %w", e.Key, err)
		}
	} else {
		if err := b.Set(ik, encodeRev(e.Revision), pebble.NoSync); err != nil {
			return fmt.Errorf("store: set idx key %q rev=%d: %w", e.Key, e.Revision, err)
		}
	}
	return nil
}

// applyTxnEntry decodes and atomically applies all sub-operations from an
// OpTxn WAL entry. Each sub-op is stored at logKeyWithSub(rev, i) so that
// the log scan in Watch returns one event per key at the transaction revision.
func (s *Store) applyTxnEntry(b *pebble.Batch, e *wal.Entry) error {
	ops, err := wal.DecodeTxnOps(e.Value)
	if err != nil {
		return fmt.Errorf("store: decode txn ops rev=%d: %w", e.Revision, err)
	}
	for i, op := range ops {
		lk := logKeyWithSub(e.Revision, uint16(i))
		r := &record{
			key:            op.Key,
			value:          op.Value,
			createRevision: op.CreateRevision,
			prevRevision:   op.PrevRevision,
			version:        entryVersion(op.Version),
			lease:          op.Lease,
			create:         op.Op == wal.OpCreate,
			delete:         op.Op == wal.OpDelete,
		}
		if err := b.Set(lk, marshalRecord(r), pebble.NoSync); err != nil {
			return fmt.Errorf("store: set txn log key rev=%d sub=%d: %w", e.Revision, i, err)
		}
		ik := idxKey(op.Key)
		if op.Op == wal.OpDelete {
			if err := b.Delete(ik, pebble.NoSync); err != nil {
				return fmt.Errorf("store: delete txn idx key %q: %w", op.Key, err)
			}
		} else {
			if err := b.Set(ik, encodeRev(e.Revision), pebble.NoSync); err != nil {
				return fmt.Errorf("store: set txn idx key %q rev=%d: %w", op.Key, e.Revision, err)
			}
		}
	}
	return nil
}

func (s *Store) applyCompact(b *pebble.Batch, compactRev int64) error {
	if err := b.Set(metaCompactKey, encodeRev(compactRev), pebble.NoSync); err != nil {
		return err
	}
	lo := logKey(atomic.LoadInt64(&s.compactRev))
	hi := logKey(compactRev + 1)

	// Walk the compacted history backwards. The first record seen for a key is
	// its newest version at or before compactRev and must be retained as the
	// anchor for historical reads at and immediately after the watermark. Any
	// earlier record for the same key can be removed.
	//
	// This is deliberately a single O(entries) pass. The old implementation
	// scanned the remaining history for every entry to find a later version,
	// which made compaction O(entries^2) for high-cardinality workloads.
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return fmt.Errorf("store: compact iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	seen := make(map[string]struct{})
	for iter.Last(); iter.Valid(); iter.Prev() {
		entryRev := decodeLogKey(iter.Key())
		r, err := unmarshalRecord(iter.Value())
		if err != nil {
			return fmt.Errorf("store: compact decode rev=%d: %w", entryRev, err)
		}
		if _, ok := seen[r.key]; !ok {
			seen[r.key] = struct{}{}
			continue
		}
		if err := b.Delete(iter.Key(), pebble.NoSync); err != nil {
			return fmt.Errorf("store: compact delete rev=%d: %w", entryRev, err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("store: compact scan: %w", err)
	}

	atomic.StoreInt64(&s.compactRev, compactRev)
	return nil
}

// broadcast replaces the notify channel, waking all current waiters.
func (s *Store) broadcast() {
	s.mu.Lock()
	old := s.notify
	s.notify = make(chan struct{})
	s.mu.Unlock()
	close(old)
}

// NotifyRevision wakes any goroutines blocked in WaitForRevision without
// writing a new entry.  Called after bulk recovery (Recover) so that readers
// sleeping on the notify channel see the updated currentRev without waiting
// for the next live Apply.
func (s *Store) NotifyRevision() { s.broadcast() }

// waitChan returns the current notify channel. Callers wait on it to be
// closed, then re-read the revision.
func (s *Store) waitChan() <-chan struct{} {
	s.mu.RLock()
	ch := s.notify
	s.mu.RUnlock()
	return ch
}

// WaitForRevision blocks until currentRev >= rev, ctx is cancelled, or the
// store is closed.
func (s *Store) WaitForRevision(ctx context.Context, rev int64) error {
	for {
		select {
		case <-s.closed:
			return ErrClosed
		default:
		}
		// Snapshot the notify channel before re-checking currentRev. If a
		// broadcast races between the load and the select, ch is already
		// closed and the select returns immediately — no lost wakeup.
		ch := s.waitChan()
		if atomic.LoadInt64(&s.currentRev) >= rev {
			return nil
		}
		select {
		case <-ch:
		case <-s.closed:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// --- Read path ---

// KeyValue is the result of a point lookup or range scan.
type KeyValue struct {
	Key            string
	Value          []byte
	Revision       int64
	CreateRevision int64
	PrevRevision   int64
	Version        int64
	Lease          int64
}

// ReadOptions refines store reads. Zero values mean HEAD, no lower key bound,
// and no limit.
type ReadOptions struct {
	Revision int64
	FromKey  string
	Limit    int64
}

// Get returns the current value of key, or nil if not found.
func (s *Store) Get(key string) (*KeyValue, error) {
	rev, err := s.getIdxRev(key)
	if err != nil || rev == 0 {
		return nil, err
	}
	return s.getLogEntry(key, rev)
}

// GetAt returns the value of key as of revision. A revision of 0 means current.
func (s *Store) GetAt(key string, revision int64) (*KeyValue, error) {
	targetRev, err := s.resolveReadRevision(revision)
	if err != nil {
		return nil, err
	}
	if revision == 0 {
		return s.Get(key)
	}
	return s.getAtRevision(key, targetRev)
}

// Exists reports whether key is currently live without loading its value.
func (s *Store) Exists(key string) (bool, error) {
	rev, err := s.getIdxRev(key)
	if err != nil {
		return false, err
	}
	return rev != 0, nil
}

// ExistsAt reports whether key was live as of revision. A revision of 0 means current.
func (s *Store) ExistsAt(key string, revision int64) (bool, error) {
	if revision == 0 {
		return s.Exists(key)
	}
	kv, err := s.GetAt(key, revision)
	return kv != nil, err
}

func (s *Store) resolveReadRevision(revision int64) (int64, error) {
	currentRev := atomic.LoadInt64(&s.currentRev)
	if revision == 0 {
		return currentRev, nil
	}
	// etcd convention: Compact(N) preserves data at rev=N; only reads
	// strictly below N are rejected. (Distinct from the Watch path, where
	// startRev == compactRev is also rejected — events at and below
	// compactRev are not retained.)
	if compactRev := atomic.LoadInt64(&s.compactRev); compactRev > 0 && revision < compactRev {
		return 0, ErrCompacted
	}
	if revision > currentRev {
		return 0, ErrFutureRevision
	}
	return revision, nil
}

func (s *Store) getIdxRev(key string) (int64, error) {
	v, closer, err := s.db.Get(idxKey(key))
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: get idx %q: %w", key, err)
	}
	rev := decodeRev(v)
	closer.Close()
	return rev, nil
}

// recordToKV builds a KeyValue from a decoded log record at the given revision.
func recordToKV(key string, rev int64, r *record) *KeyValue {
	return &KeyValue{
		Key:            key,
		Value:          r.value,
		Revision:       rev,
		CreateRevision: r.createRevision,
		PrevRevision:   r.prevRevision,
		Version:        recordVersion(r),
		Lease:          r.lease,
	}
}

func (s *Store) getLogEntry(key string, rev int64) (*KeyValue, error) {
	// Fast path: non-txn entries are stored at logKey(rev).
	v, closer, err := s.db.Get(logKey(rev))
	if err == nil {
		defer closer.Close()
		r, err := unmarshalRecord(v)
		if err != nil {
			return nil, err
		}
		return recordToKV(key, rev, r), nil
	}
	if err != pebble.ErrNotFound {
		return nil, fmt.Errorf("store: get log rev=%d: %w", rev, err)
	}

	// Slow path: txn entries are stored at logKeyWithSub(rev, subIndex).
	// Scan all sub-keys at this revision and find the one matching key.
	lower := logKeyWithSub(rev, 0)
	upper := logKey(rev + 1)
	iter, iterErr := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if iterErr != nil {
		return nil, fmt.Errorf("store: get log txn scan rev=%d: %w", rev, iterErr)
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		r, rerr := unmarshalRecord(iter.Value())
		if rerr != nil {
			continue
		}
		if r.key == key {
			return recordToKV(key, rev, r), nil
		}
	}
	return nil, fmt.Errorf("store: get log rev=%d key=%q: not found in txn sub-ops", rev, key)
}

func (s *Store) getAtRevision(key string, targetRev int64) (*KeyValue, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: logLower,
		UpperBound: logKey(targetRev + 1),
	})
	if err != nil {
		return nil, fmt.Errorf("store: get-at iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.Last(); iter.Valid(); iter.Prev() {
		rev := decodeLogKey(iter.Key())
		r, err := unmarshalRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		if r.key != key {
			continue
		}
		if r.delete {
			return nil, nil
		}
		return recordToKV(r.key, rev, r), nil
	}
	return nil, iter.Error()
}

// List returns all live keys with the given prefix, sorted lexicographically.
// If prefix is empty, all keys are returned.
func (s *Store) List(prefix string) ([]*KeyValue, error) {
	return s.ListLimit(prefix, 0)
}

// ListLimit returns up to limit live keys with the given prefix. A limit <= 0
// returns all matching keys.
func (s *Store) ListLimit(prefix string, limit int64) ([]*KeyValue, error) {
	return s.ListRange(prefix, ReadOptions{Limit: limit})
}

// ListRange returns live keys matching prefix and opts, sorted lexicographically.
func (s *Store) ListRange(prefix string, opts ReadOptions) ([]*KeyValue, error) {
	targetRev, err := s.resolveReadRevision(opts.Revision)
	if err != nil {
		return nil, err
	}
	if opts.Revision == 0 {
		return s.listCurrent(prefix, opts.FromKey, opts.Limit)
	}

	events, _, err := s.scanLog(prefix, 1, targetRev, false)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]*KeyValue)
	for _, ev := range events {
		if ev.Type == EventDelete {
			delete(latest, ev.KV.Key)
			continue
		}
		latest[ev.KV.Key] = ev.KV
	}

	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]*KeyValue, 0, len(keys))
	for _, key := range keys {
		if opts.FromKey != "" && key < opts.FromKey {
			continue
		}
		if opts.Limit > 0 && int64(len(out)) >= opts.Limit {
			break
		}
		out = append(out, latest[key])
	}
	return out, nil
}

// idxBounds returns the index-iteration bounds for keys with prefix, advancing
// the lower bound to fromKey when fromKey is lexicographically beyond prefix.
func idxBounds(prefix, fromKey string) (lower, upper []byte) {
	lower = idxKey(prefix)
	if fromKey != "" {
		fromLower := idxKey(fromKey)
		if string(fromLower) > string(lower) {
			lower = fromLower
		}
	}
	return lower, idxKeyUpper(prefix)
}

func (s *Store) listCurrent(prefix, fromKey string, limit int64) ([]*KeyValue, error) {
	lower, upper := idxBounds(prefix, fromKey)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("store: list iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var out []*KeyValue
	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
		k := string(iter.Key()[1:]) // strip 'i' prefix
		rev := decodeRev(iter.Value())
		kv, err := s.getLogEntry(k, rev)
		if err != nil {
			return nil, err
		}
		out = append(out, kv)
	}
	return out, iter.Error()
}

// Count returns the number of live keys with the given prefix.
func (s *Store) Count(prefix string) (int64, error) {
	return s.CountRange(prefix, ReadOptions{})
}

// CountRange returns the number of live keys matching prefix and opts.
func (s *Store) CountRange(prefix string, opts ReadOptions) (int64, error) {
	if opts.Revision == 0 {
		return s.countCurrent(prefix, opts.FromKey)
	}
	kvs, err := s.ListRange(prefix, ReadOptions{Revision: opts.Revision, FromKey: opts.FromKey})
	if err != nil {
		return 0, err
	}
	return int64(len(kvs)), nil
}

// countCurrent counts live keys with prefix at HEAD whose key is
// lexicographically >= fromKey when fromKey is set.
func (s *Store) countCurrent(prefix, fromKey string) (int64, error) {
	lower, upper := idxBounds(prefix, fromKey)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("store: count-from iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n, iter.Error()
}

// History returns change events for a single key in revision order.
func (s *Store) History(key string) ([]Event, error) {
	if key == "" {
		return nil, fmt.Errorf("store: history key must not be empty")
	}
	events, _, err := s.scanLog("", 1, atomic.LoadInt64(&s.currentRev), true)
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(events))
	for _, ev := range events {
		if ev.KV != nil && ev.KV.Key == key {
			out = append(out, ev)
		}
	}
	return out, nil
}

// Changes returns change events for keys matching prefix in [fromRev, toRev].
func (s *Store) Changes(prefix string, fromRev, toRev int64) ([]Event, error) {
	if fromRev <= 0 {
		return nil, fmt.Errorf("store: from revision must be >= 1")
	}
	if toRev < fromRev {
		return nil, fmt.Errorf("store: to revision must be >= from revision")
	}
	currentRev := atomic.LoadInt64(&s.currentRev)
	if currentRev == 0 || fromRev > currentRev {
		return []Event{}, nil
	}
	if toRev > currentRev {
		toRev = currentRev
	}
	events, _, err := s.scanLog(prefix, fromRev, toRev, true)
	return events, err
}

// --- Watch ---

const watchLiveBatchBuffer = 1024

type watchSubscription struct {
	id         uint64
	prefix     string
	withPrevKV bool
	nextRev    int64
	live       chan []Event
}

// EventType classifies a watch event.
type EventType int

const (
	EventPut    EventType = iota // create or update
	EventDelete                  // deletion
)

// Event is a single watch notification.
type Event struct {
	Type   EventType
	KV     *KeyValue
	PrevKV *KeyValue // nil for creates
}

// Watch streams events for keys matching prefix starting from startRev+1.
// The channel is closed when ctx is cancelled. When withPrevKV is false,
// emitted events have PrevKV == nil; this avoids one Pebble lookup per
// non-create event in hot watch paths.
//
// The channel buffer is intentionally small. Backpressure should flow back to
// scanLog quickly so a slow consumer doesn't accumulate large amounts of
// converted events in memory and doesn't delay compaction by holding live
// references to old revisions. The etcd handler's drain loop coalesces
// whatever is immediately available into one WatchResponse, so a small buffer
// still amortises gRPC Send overhead.
func (s *Store) Watch(ctx context.Context, prefix string, startRev int64, withPrevKV bool) (<-chan Event, error) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()

	select {
	case <-s.closed:
		return nil, ErrClosed
	default:
	}
	ch := make(chan Event, 64)
	unregister := s.registerWatch(prefix)
	sub := &watchSubscription{
		prefix:     prefix,
		withPrevKV: withPrevKV,
		live:       make(chan []Event, watchLiveBatchBuffer),
	}

	// Most Kubernetes watches start at the store's current revision. Register
	// those synchronously so an Apply immediately following Watch cannot turn
	// into a separate historical scan. Watches starting behind currentRev use
	// the replay/handoff path in watchLoop.
	s.watchHubMu.Lock()
	boundary := atomic.LoadInt64(&s.currentRev)
	registered := startRev >= boundary
	if registered {
		s.addWatchSubscriptionLocked(sub, startRev+1)
	}
	s.watchHubMu.Unlock()
	s.startWatchDispatcher(boundary + 1)

	s.watcherWg.Add(1)
	go s.watchLoop(ctx, prefix, startRev, withPrevKV, boundary, registered, ch, sub, unregister)
	return ch, nil
}

func (s *Store) addWatchSubscriptionLocked(sub *watchSubscription, nextRev int64) {
	if s.watchers == nil {
		s.watchers = make(map[uint64]*watchSubscription)
	}
	s.nextWatchID++
	sub.id = s.nextWatchID
	sub.nextRev = nextRev
	s.watchers[sub.id] = sub
}

func (s *Store) removeWatchSubscription(sub *watchSubscription) {
	s.watchHubMu.Lock()
	if current, ok := s.watchers[sub.id]; ok && current == sub {
		delete(s.watchers, sub.id)
		close(sub.live)
	}
	s.watchHubMu.Unlock()
}

func (s *Store) registerWatch(prefix string) func() {
	s.mu.Lock()
	if s.watchPrefixes == nil {
		s.watchPrefixes = make(map[string]int)
	}
	s.watchPrefixes[prefix]++
	prefixes := len(s.watchPrefixes)
	s.mu.Unlock()

	if metrics.WatchActive != nil {
		metrics.WatchActive.Inc()
		metrics.WatchActivePrefixes.Set(float64(prefixes))
	}

	return func() {
		s.mu.Lock()
		if n := s.watchPrefixes[prefix]; n <= 1 {
			delete(s.watchPrefixes, prefix)
		} else {
			s.watchPrefixes[prefix] = n - 1
		}
		prefixes := len(s.watchPrefixes)
		s.mu.Unlock()

		if metrics.WatchActive != nil {
			metrics.WatchActive.Dec()
			metrics.WatchActivePrefixes.Set(float64(prefixes))
		}
	}
}

func (s *Store) watchLoop(
	ctx context.Context,
	prefix string,
	startRev int64,
	withPrevKV bool,
	boundary int64,
	registered bool,
	ch chan<- Event,
	sub *watchSubscription,
	unregister func(),
) {
	defer s.watcherWg.Done()
	defer unregister()
	defer close(ch)
	defer s.removeWatchSubscription(sub)

	if !registered {
		nextRev := startRev + 1
		if err := s.WaitForRevision(ctx, nextRev); err != nil {
			return
		}
		if boundary < nextRev {
			boundary = atomic.LoadInt64(&s.currentRev)
		}

		replay, ok := s.scanWatchRange(prefix, nextRev, boundary, withPrevKV)
		if !ok {
			return
		}

		// Atomically choose a handoff revision and register only for later
		// revisions. The small gap is replayed below; concurrent Apply calls
		// queue revisions after handoff in sub.live, so there is no gap or
		// duplicate at the replay/live boundary.
		s.watchHubMu.Lock()
		handoff := atomic.LoadInt64(&s.currentRev)
		s.addWatchSubscriptionLocked(sub, handoff+1)
		s.watchHubMu.Unlock()

		gap, ok := s.scanWatchRange(prefix, boundary+1, handoff, withPrevKV)
		if !ok || !s.sendWatchEvents(ctx, ch, replay) || !s.sendWatchEvents(ctx, ch, gap) {
			return
		}
	}

	for {
		select {
		case events, ok := <-sub.live:
			if !ok || !s.sendWatchEvents(ctx, ch, events) {
				return
			}
		case <-s.closed:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) scanWatchRange(prefix string, fromRev, toRev int64, withPrevKV bool) ([]Event, bool) {
	if toRev < fromRev {
		return nil, true
	}
	start := time.Now()
	events, scanned, err := s.scanLog(prefix, fromRev, toRev, withPrevKV)
	if err != nil {
		return nil, false
	}
	recordWatchScanMetrics(time.Since(start), fromRev, toRev, scanned, len(events))
	return events, true
}

func (s *Store) sendWatchEvents(ctx context.Context, ch chan<- Event, events []Event) bool {
	for _, ev := range events {
		select {
		case ch <- ev:
		case <-s.closed:
			return false
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (s *Store) startWatchDispatcher(nextRev int64) {
	s.dispatchOnce.Do(func() {
		s.dispatchWg.Add(1)
		go s.watchDispatchLoop(nextRev)
	})
}

// watchDispatchLoop is the single live revision-log scanner for this Store.
// It is deliberately asynchronous from Apply: commits only wake the loop, and
// watch decoding, PrevKV reconstruction, and slow-consumer handling never add
// latency to WAL apply or follower acknowledgement.
func (s *Store) watchDispatchLoop(nextRev int64) {
	defer s.dispatchWg.Done()
	ctx := context.Background()

	for {
		s.watchHubMu.Lock()
		hasWatchers := len(s.watchers) > 0
		s.watchHubMu.Unlock()
		if !hasWatchers {
			// No subscriber needs the intervening history. A later historical
			// watch replays its own requested range before joining live delivery.
			nextRev = atomic.LoadInt64(&s.currentRev) + 1
		}
		if err := s.WaitForRevision(ctx, nextRev); err != nil {
			return
		}
		toRev := atomic.LoadInt64(&s.currentRev)
		start := time.Now()
		events, scanned, err := s.scanLog("", nextRev, toRev, false)
		if err != nil {
			return
		}
		matched := s.dispatchWatchEvents(events)
		recordWatchScanMetrics(time.Since(start), nextRev, toRev, scanned, matched)
		nextRev = toRev + 1
	}
}

// dispatchWatchEvents fans one shared decoded batch out in memory. A slow
// consumer cannot block other watches: when its bounded queue fills, that
// watch is closed and can reconnect from its last delivered revision.
func (s *Store) dispatchWatchEvents(events []Event) int {
	type prevKey struct {
		key string
		rev int64
	}
	prevCache := make(map[prevKey]*KeyValue)
	matched := 0

	s.watchHubMu.Lock()
	for id, sub := range s.watchers {
		var batch []Event
		for i := range events {
			ev := events[i]
			if ev.KV.Revision < sub.nextRev ||
				(sub.prefix != "" && !strings.HasPrefix(ev.KV.Key, sub.prefix)) {
				continue
			}
			if sub.withPrevKV && ev.KV.PrevRevision > 0 {
				pk := prevKey{key: ev.KV.Key, rev: ev.KV.PrevRevision}
				prev, found := prevCache[pk]
				if !found {
					prev, _ = s.getLogEntry(pk.key, pk.rev)
					prevCache[pk] = prev
				}
				ev.PrevKV = prev
			}
			batch = append(batch, ev)
		}
		if len(batch) == 0 {
			continue
		}
		matched += len(batch)
		select {
		case sub.live <- batch:
		default:
			delete(s.watchers, id)
			close(sub.live)
		}
	}
	s.watchHubMu.Unlock()
	return matched
}

func recordWatchScanMetrics(d time.Duration, fromRev, toRev int64, scanned, matched int) {
	if metrics.WatchScanDuration == nil {
		return
	}
	metrics.WatchScanDuration.Observe(d.Seconds())
	metrics.WatchScanRevisionSpan.Observe(float64(toRev - fromRev + 1))
	metrics.WatchScanEntriesTotal.WithLabelValues("scanned").Add(float64(scanned))
	metrics.WatchScanEntriesTotal.WithLabelValues("matched").Add(float64(matched))
}

// scanLog reads log entries in [fromRev, toRev] and returns events for keys
// matching prefix plus the number of log records scanned. When withPrevKV is
// false, the per-event PrevKV lookup is skipped.
func (s *Store) scanLog(prefix string, fromRev, toRev int64, withPrevKV bool) ([]Event, int, error) {
	lower := logKey(fromRev)
	upper := logKey(toRev + 1)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("store: scan log iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var events []Event
	var scanned int
	for iter.First(); iter.Valid(); iter.Next() {
		scanned++
		rev := decodeLogKey(iter.Key())
		r, err := unmarshalRecord(iter.Value())
		if err != nil {
			return nil, scanned, err
		}
		if prefix != "" && (len(r.key) < len(prefix) || r.key[:len(prefix)] != prefix) {
			continue
		}
		kv := recordToKV(r.key, rev, r)
		var prevKV *KeyValue
		if withPrevKV && r.prevRevision > 0 {
			prevKV, err = s.getLogEntry(r.key, r.prevRevision)
			if err != nil {
				// Previous entry may have been compacted; non-fatal.
				prevKV = nil
			}
		}
		et := EventPut
		if r.delete {
			et = EventDelete
		}
		events = append(events, Event{
			Type:   et,
			KV:     kv,
			PrevKV: prevKV,
		})
	}
	return events, scanned, iter.Error()
}

func recordVersion(r *record) int64 {
	if r.version > 0 {
		return r.version
	}
	if r.delete {
		return 0
	}
	return 1
}

func entryVersion(version int64) int64 {
	if version > 0 {
		return version
	}
	return 1
}
