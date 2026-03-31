package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/makhov/strata/internal/wal"
	"github.com/sirupsen/logrus"
)

// ErrClosed is returned by WaitForRevision when the store has been closed.
var ErrClosed = errors.New("store: closed")

// Store is the Pebble-backed state machine.
//
// The key space is described in keys.go. WAL entries are applied in order;
// each application advances the current revision and notifies watchers.
type Store struct {
	db *pebble.DB

	// currentRev and compactRev are accessed atomically.
	currentRev int64
	compactRev int64

	// mu protects notify and the watchpoint map.
	mu        sync.RWMutex
	notify    chan struct{} // closed and replaced on each revision advance
	closed    chan struct{} // closed once when Store.Close is called
	closeOnce sync.Once
	watcherWg sync.WaitGroup // tracks active watchLoop goroutines
}

// lockRetryTimeout is how long Open retries when another process holds the
// pebble LOCK file. This covers the window where a previous pod is still
// terminating when the replacement starts.
const lockRetryTimeout = 30 * time.Second

// Open opens (or creates) the Pebble database at dir and returns a Store.
// The caller should call Recover to replay WAL entries before serving requests.
//
// If the database is locked by another process, Open retries for up to
// lockRetryTimeout before returning an error. This handles the Kubernetes pod
// replacement race where the old instance has not yet released the lock.
func Open(dir string) (*Store, error) {
	deadline := time.Now().Add(lockRetryTimeout)
	for {
		db, err := pebble.Open(dir, &pebble.Options{})
		if err == nil {
			s := &Store{db: db, notify: make(chan struct{}), closed: make(chan struct{})}
			if err := s.loadMeta(); err != nil {
				db.Close()
				return nil, err
			}
			return s, nil
		}
		if !isPebbleLockError(err) || time.Now().After(deadline) {
			return nil, fmt.Errorf("store: open pebble %q: %w", dir, err)
		}
		logrus.Warnf("strata: pebble locked at %q, retrying in 1s (previous instance still terminating?)", dir)
		time.Sleep(time.Second)
	}
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

// loadMeta reads the compact revision from Pebble, and derives the current
// revision from the last log entry.
func (s *Store) loadMeta() error {
	// Read compact revision.
	v, closer, err := s.db.Get(metaCompactKey)
	if err == nil {
		s.compactRev = decodeRev(v)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return fmt.Errorf("store: read compact rev: %w", err)
	}

	// Scan log in reverse to find the highest revision.
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: logLower,
		UpperBound: logUpper,
	})
	if err != nil {
		return fmt.Errorf("store: new iter for loadMeta: %w", err)
	}
	defer iter.Close()
	if iter.Last() {
		s.currentRev = decodeLogKey(iter.Key())
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
	s.SignalClose()
	s.watcherWg.Wait()
	return s.db.Close()
}

// Pebble exposes the underlying *pebble.DB for checkpoint creation.
func (s *Store) Pebble() *pebble.DB { return s.db }

// CurrentRevision returns the latest applied revision.
func (s *Store) CurrentRevision() int64 { return atomic.LoadInt64(&s.currentRev) }

// CompactRevision returns the oldest revision still available.
func (s *Store) CompactRevision() int64 { return atomic.LoadInt64(&s.compactRev) }

// Apply applies a batch of WAL entries to the store and notifies watchers.
// Entries must be ordered by revision. Apply is not safe for concurrent use.
func (s *Store) Apply(entries []wal.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	b := s.db.NewBatch()
	var maxRev int64
	for i := range entries {
		e := &entries[i]
		if e.Op == wal.OpCompact {
			// PrevRevision carries the compact target (see node.go Compact).
			if err := s.applyCompact(b, e.PrevRevision); err != nil {
				b.Close()
				return err
			}
			if e.Revision > maxRev {
				maxRev = e.Revision
			}
			continue
		}
		if err := s.applyEntry(b, e); err != nil {
			b.Close()
			return err
		}
		if e.Revision > maxRev {
			maxRev = e.Revision
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		b.Close()
		return fmt.Errorf("store: commit batch: %w", err)
	}
	atomic.StoreInt64(&s.currentRev, maxRev)
	s.broadcast()
	return nil
}

// Recover applies entries without broadcasting to watchers. Used during
// startup replay before the node is serving requests.
func (s *Store) Recover(entries []wal.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	b := s.db.NewBatch()
	var maxRev int64
	for i := range entries {
		e := &entries[i]
		if e.Op == wal.OpCompact {
			if err := s.applyCompact(b, e.PrevRevision); err != nil {
				b.Close()
				return err
			}
		} else {
			// Term-conflict cleanup: during WAL replay after a leader change, a
			// newer term may write a different key at the same revision as the
			// old term. Remove any stale index pointer before overwriting the
			// log entry so idx[oldKey]=rev doesn't dangle.
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
			if err := s.applyEntry(b, e); err != nil {
				b.Close()
				return err
			}
		}
		if e.Revision > maxRev {
			maxRev = e.Revision
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		b.Close()
		return fmt.Errorf("store: commit recover batch: %w", err)
	}
	if maxRev > atomic.LoadInt64(&s.currentRev) {
		atomic.StoreInt64(&s.currentRev, maxRev)
	}
	return nil
}

func (s *Store) applyEntry(b *pebble.Batch, e *wal.Entry) error {
	lk := logKey(e.Revision)

	r := &record{
		key:            e.Key,
		value:          e.Value,
		createRevision: e.CreateRevision,
		prevRevision:   e.PrevRevision,
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

func (s *Store) applyCompact(b *pebble.Batch, compactRev int64) error {
	if err := b.Set(metaCompactKey, encodeRev(compactRev), pebble.NoSync); err != nil {
		return err
	}
	lo := logKey(atomic.LoadInt64(&s.compactRev))
	hi := logKey(compactRev)

	// Scan log entries in [lo, hi) and delete only those that are safe to
	// remove. A log entry at revision R for key K is safe to delete when:
	//   (a) the entry is a delete operation (key no longer exists), or
	//   (b) the index for K points to a revision > R (key has since been updated).
	// Entries where the index still points to R are the current value of a live
	// key and must be preserved, otherwise a subsequent Get would fail with
	// "pebble: not found" for a key that still exists.
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return fmt.Errorf("store: compact iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		entryRev := decodeLogKey(iter.Key())
		r, err := unmarshalRecord(iter.Value())
		if err == nil && !r.delete {
			idxRev, err := s.getIdxRev(r.key)
			if err != nil {
				return fmt.Errorf("store: compact idx lookup %q: %w", r.key, err)
			}
			if idxRev == entryRev {
				continue // live key at its current revision — keep
			}
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
	for atomic.LoadInt64(&s.currentRev) < rev {
		ch := s.waitChan()
		select {
		case <-ch:
		case <-s.closed:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// --- Read path ---

// KeyValue is the result of a point lookup or range scan.
type KeyValue struct {
	Key            string
	Value          []byte
	Revision       int64
	CreateRevision int64
	PrevRevision   int64
	Lease          int64
}

// Get returns the current value of key, or nil if not found.
func (s *Store) Get(key string) (*KeyValue, error) {
	rev, err := s.getIdxRev(key)
	if err != nil || rev == 0 {
		return nil, err
	}
	return s.getLogEntry(key, rev)
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

func (s *Store) getLogEntry(key string, rev int64) (*KeyValue, error) {
	v, closer, err := s.db.Get(logKey(rev))
	if err != nil {
		return nil, fmt.Errorf("store: get log rev=%d: %w", rev, err)
	}
	defer closer.Close()
	r, err := unmarshalRecord(v)
	if err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:            key,
		Value:          r.value,
		Revision:       rev,
		CreateRevision: r.createRevision,
		PrevRevision:   r.prevRevision,
		Lease:          r.lease,
	}, nil
}

// List returns all live keys with the given prefix, sorted lexicographically.
// If prefix is empty, all keys are returned.
func (s *Store) List(prefix string) ([]*KeyValue, error) {
	lower := idxKey(prefix)
	upper := idxKeyUpper(prefix)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("store: list iter: %w", err)
	}
	defer iter.Close()

	var out []*KeyValue
	for iter.First(); iter.Valid(); iter.Next() {
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
	lower := idxKey(prefix)
	upper := idxKeyUpper(prefix)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("store: count iter: %w", err)
	}
	defer iter.Close()

	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n, iter.Error()
}

// --- Watch ---

// Event is a single watch notification.
type Event struct {
	KV      *KeyValue
	PrevKV  *KeyValue // nil for creates
	Deleted bool
}

// Watch streams events for keys matching prefix starting from startRev+1.
// The channel is closed when ctx is cancelled.
func (s *Store) Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error) {
	ch := make(chan Event, 64)
	s.watcherWg.Add(1)
	go s.watchLoop(ctx, prefix, startRev, ch)
	return ch, nil
}

func (s *Store) watchLoop(ctx context.Context, prefix string, startRev int64, ch chan<- Event) {
	defer s.watcherWg.Done()
	defer close(ch)

	nextRev := startRev + 1
	for {
		// Wait until we have entries at or beyond nextRev.
		if err := s.WaitForRevision(ctx, nextRev); err != nil {
			return
		}
		curRev := atomic.LoadInt64(&s.currentRev)

		// Scan the log for events in [nextRev, curRev].
		events, err := s.scanLog(prefix, nextRev, curRev)
		if err != nil {
			return
		}
		for _, ev := range events {
			select {
			case ch <- ev:
			case <-s.closed:
				return
			case <-ctx.Done():
				return
			}
		}
		nextRev = curRev + 1
	}
}

// scanLog reads log entries in [fromRev, toRev] and returns events for keys
// matching prefix.
func (s *Store) scanLog(prefix string, fromRev, toRev int64) ([]Event, error) {
	lower := logKey(fromRev)
	upper := logKey(toRev + 1)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("store: scan log iter: %w", err)
	}
	defer iter.Close()

	var events []Event
	for iter.First(); iter.Valid(); iter.Next() {
		rev := decodeLogKey(iter.Key())
		r, err := unmarshalRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		if prefix != "" && (len(r.key) < len(prefix) || r.key[:len(prefix)] != prefix) {
			continue
		}
		kv := &KeyValue{
			Key:            r.key,
			Value:          r.value,
			Revision:       rev,
			CreateRevision: r.createRevision,
			PrevRevision:   r.prevRevision,
			Lease:          r.lease,
		}
		var prevKV *KeyValue
		if r.prevRevision > 0 {
			prevKV, err = s.getLogEntry(r.key, r.prevRevision)
			if err != nil {
				// Previous entry may have been compacted; non-fatal.
				prevKV = nil
			}
		}
		events = append(events, Event{
			KV:      kv,
			PrevKV:  prevKV,
			Deleted: r.delete,
		})
	}
	return events, iter.Error()
}
