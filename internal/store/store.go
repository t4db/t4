package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/makhov/strata/internal/wal"
)

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
	mu     sync.RWMutex
	notify chan struct{} // closed and replaced on each revision advance
}

// Open opens (or creates) the Pebble database at dir and returns a Store.
// The caller should call Recover to replay WAL entries before serving requests.
func Open(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("store: open pebble %q: %w", dir, err)
	}
	s := &Store{
		db:     db,
		notify: make(chan struct{}),
	}
	if err := s.loadMeta(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// OpenMem opens an in-memory Pebble store (for testing / followers).
func OpenMem() (*Store, error) {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		return nil, fmt.Errorf("store: open in-memory pebble: %w", err)
	}
	return &Store{db: db, notify: make(chan struct{})}, nil
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

// Close closes the underlying Pebble database.
func (s *Store) Close() error { return s.db.Close() }

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
	// Delete log entries below the compact revision.
	lo := logKey(atomic.LoadInt64(&s.compactRev))
	hi := logKey(compactRev)
	if err := b.DeleteRange(lo, hi, pebble.NoSync); err != nil {
		return fmt.Errorf("store: delete compact range: %w", err)
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

// waitChan returns the current notify channel. Callers wait on it to be
// closed, then re-read the revision.
func (s *Store) waitChan() <-chan struct{} {
	s.mu.RLock()
	ch := s.notify
	s.mu.RUnlock()
	return ch
}

// WaitForRevision blocks until currentRev >= rev or ctx is cancelled.
func (s *Store) WaitForRevision(ctx context.Context, rev int64) error {
	for atomic.LoadInt64(&s.currentRev) < rev {
		ch := s.waitChan()
		select {
		case <-ch:
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
	go s.watchLoop(ctx, prefix, startRev, ch)
	return ch, nil
}

func (s *Store) watchLoop(ctx context.Context, prefix string, startRev int64, ch chan<- Event) {
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
