package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
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
	// lastTerm is the term of the entry at lastSeq (see position.go).
	lastTerm atomic.Uint64

	// hist is the in-memory recent-history ring serving revision-pinned
	// reads; nil keeps reads on the Pebble undo/replay paths. Swapped
	// wholesale by SetHistoryRingSize.
	hist atomic.Pointer[historyRing]

	// posMu guards the leader-known position: the key it is stored under
	// (nil until SetNodeID) and its value.
	posMu     sync.Mutex
	streamKey []byte
	streamPos Position

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
	// watcherWake is closed and replaced when a watch registers. The dispatch
	// loop waits on it while the hub is empty so it goes fully idle instead of
	// scanning every commit for no subscriber.
	watcherWake chan struct{}
}

// lockRetryTimeout is how long Open retries when another process holds the
// pebble LOCK file. This covers the window where a previous pod is still
// terminating when the replacement starts.
const lockRetryTimeout = 30 * time.Second

// PebbleOption is a functional option for configuring pebble.Options.
type PebbleOption = func(*pebble.Options)

// defaultBlockCacheSize is the Pebble block cache T4 opens with. Pebble's own
// default is 8 MiB, which is far too small for a database serving a working
// set of live keys: nearly every read misses and pays a disk read plus block
// decompression. 64 MiB is a compromise for an embeddable store — large enough
// that hot keys stay resident, small enough not to surprise a process that
// embeds T4 alongside other work. Override via Config.PebbleOptions on a
// dedicated node.
const defaultBlockCacheSize = 64 << 20

var (
	blockCacheOnce sync.Once
	blockCache     *pebble.Cache
)

// sharedBlockCache returns the process-wide Pebble block cache.
//
// The cache is shared across every Store rather than allocated per Open, which
// is what Pebble intends. A process that opens several databases — a test
// binary, a multi-node embedding, or the resync path opening a replacement DB
// alongside the old one — would otherwise multiply the cache by the number of
// open stores.
//
// The initial reference is held for the process lifetime. Each pebble.Open
// takes its own reference and releases it on Close, so the cache correctly
// outlives any individual Store.
func sharedBlockCache() *pebble.Cache {
	blockCacheOnce.Do(func() { blockCache = pebble.NewCache(defaultBlockCacheSize) })
	return blockCache
}

// maxCompactions bounds Pebble's compaction concurrency: enough to keep up
// with a sustained write stream, never more than half the machine.
func maxCompactions() int {
	n := runtime.GOMAXPROCS(0) / 2
	if n < 2 {
		return 2
	}
	if n > 4 {
		return 4
	}
	return n
}

// defaultPebbleOptions returns the Pebble configuration T4 opens with.
//
// A zero pebble.Options is not a reasonable configuration for this workload.
// It leaves FilterPolicy nil — so Pebble cannot answer "key absent" from a
// filter and must read and decompress index and data blocks from every
// candidate SST — and caps the block cache at 8 MiB. Every T4 write performs a
// read-before-write to find the previous revision, so both defaults land
// directly on the write path: profiling a 4 KiB write workload attributed
// ~9% of total CPU to snappy decompression inside that single lookup.
//
// Bloom filters matter most for the negative lookups, which are not the rare
// case here: every write of a new key (Kubernetes Events, new Pods) is a miss.
func defaultPebbleOptions() *pebble.Options {
	opts := &pebble.Options{
		Cache: sharedBlockCache(),

		// Pebble's 4 MiB memtable holds only ~1000 Kubernetes-sized values, so
		// a write-heavy workload flushes ~10 times a second and hands the
		// compactor a steady stream of tiny L0 files. Profiling attributed
		// ~25-30% of CPU to compaction at 4 KiB values. A larger memtable cuts
		// the flush rate proportionally and produces fewer, larger SSTs.
		MemTableSize: 32 << 20,

		// With a 32 MiB memtable, the default threshold of 2 stalls writes
		// after 64 MiB in flight. Allowing four absorbs a flush that is slower
		// than the incoming write rate instead of stopping the commit loop.
		MemTableStopWritesThreshold: 4,

		// Pebble defaults to a single compaction goroutine, which cannot keep
		// pace with a sustained write stream and eventually backs up L0 until
		// writes stop entirely. Scale with the machine, but stay bounded so an
		// embedded T4 does not take over its host process.
		MaxConcurrentCompactions: func() int { return maxCompactions() },
	}
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	return opts
}

// Open opens (or creates) the Pebble database at dir and returns a Store.
// The caller should call Recover to replay WAL entries before serving requests.
//
// If the database is locked by another process, Open retries for up to
// lockRetryTimeout before returning an error. This handles the Kubernetes pod
// replacement race where the old instance has not yet released the lock.
func Open(dir string, log logger, extraOpts ...func(*pebble.Options)) (*Store, error) {
	opts := defaultPebbleOptions()
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

	// Read the term of the last applied entry (absent in older stores).
	v, closer, err = s.db.Get(metaLastTermKey)
	if err == nil {
		if len(v) == 8 {
			s.lastTerm.Store(binary.BigEndian.Uint64(v))
		}
		_ = closer.Close()
	} else if err != pebble.ErrNotFound {
		return fmt.Errorf("store: read last term: %w", err)
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

// SetHistoryRingSize enables the in-memory recent-history ring with capacity
// for the last n revisions (n <= 0 disables it). Revision-pinned reads
// covered by the ring build their undo map from memory and touch only the
// changed keys' records, instead of scanning the Pebble log tail whose cost
// grows with every write since the pinned revision.
func (s *Store) SetHistoryRingSize(n int) {
	if n <= 0 {
		s.hist.Store(nil)
		return
	}
	s.hist.Store(newHistoryRing(n))
}

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
	var tip Position
	// Changes for the history ring, grouped by revision. The ring is fed
	// only from here — Recover's replay, whose term-conflict rewrites would
	// leave stale anchors, clears it instead. An empty ring is fine: pinned
	// reads fall back to the Pebble paths until it warms.
	ring := s.hist.Load()
	var histChanges []histRev
	var curRev int64
	var curChanges []histChange
	for i := range entries {
		e := &entries[i]
		if seq := e.Sequence(); seq > tip.Seq {
			tip = Position{Term: e.Term, Seq: seq}
		}
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
		if ring != nil {
			cs, err := histChangesOf(e)
			if err != nil {
				_ = b.Close()
				return err
			}
			if e.Revision != curRev {
				if len(curChanges) > 0 {
					histChanges = append(histChanges, histRev{rev: curRev, changes: curChanges})
					curChanges = nil
				}
				curRev = e.Revision
			}
			curChanges = append(curChanges, cs...)
		}
		if e.Revision > maxRev {
			maxRev = e.Revision
		}
		if seq := e.Sequence(); seq > maxSeq {
			maxSeq = seq
		}
	}
	if len(curChanges) > 0 {
		histChanges = append(histChanges, histRev{rev: curRev, changes: curChanges})
	}
	if err := s.commitBatch(b, maxRev, maxSeq, tip, true, false, histChanges); err != nil {
		return err
	}
	s.broadcast()
	return nil
}

// histChanges returns e's per-key changes for the history ring: a create is
// anchored at revision 0 (the key had no state before), updates and deletes
// anchor at the record they replace. All txn sub-ops are kept in order; the
// ring merge's first-change-wins rule resolves duplicate keys inside a txn.
func histChangesOf(e *wal.Entry) ([]histChange, error) {
	if e.Op == wal.OpTxn {
		ops, err := wal.DecodeTxnOps(e.Value)
		if err != nil {
			return nil, fmt.Errorf("store: history decode txn ops rev=%d: %w", e.Revision, err)
		}
		cs := make([]histChange, 0, len(ops))
		for _, op := range ops {
			cs = append(cs, histChange{key: op.Key, prevRev: op.PrevRevision, create: op.Op == wal.OpCreate})
		}
		return cs, nil
	}
	return []histChange{{key: e.Key, prevRev: e.PrevRevision, create: e.Op == wal.OpCreate}}, nil
}

// commitBatch writes the current-revision and last-sequence meta keys into b,
// commits it (durably when sync is true), then advances the in-memory
// currentRev/lastSeq counters. The counters only ever move forward. On any
// error b is closed and the error is returned.
//
// histChanges are appended to the history ring before the commit, so every
// change a reader's snapshot can hold is already in the ring; ring entries
// newer than the snapshot are harmless (see changesSince). A failed commit
// clears the ring rather than leave changes Pebble never saw.
//
// tip is the position of the batch's last entry. fromLeader marks entries a
// leader streamed to this node or it wrote as leader: only those advance the
// leader-known position (position.go).
func (s *Store) commitBatch(b *pebble.Batch, maxRev, maxSeq int64, tip Position, fromLeader, sync bool, histChanges []histRev) error {
	known, advanceKnown, err := s.recordPositions(b, tip, fromLeader)
	if err != nil {
		_ = b.Close()
		return err
	}
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
	ring := s.hist.Load()
	if ring != nil {
		for _, hr := range histChanges {
			ring.append(hr.rev, hr.changes)
		}
	}
	writeOpts := pebble.NoSync
	if sync {
		writeOpts = pebble.Sync
	}
	if err := b.Commit(writeOpts); err != nil {
		if ring != nil && len(histChanges) > 0 {
			ring.reset()
		}
		_ = b.Close()
		return fmt.Errorf("store: commit batch: %w", err)
	}
	if maxRev > atomic.LoadInt64(&s.currentRev) {
		atomic.StoreInt64(&s.currentRev, maxRev)
	}
	if maxSeq >= atomic.LoadInt64(&s.lastSeq) && tip.Seq == maxSeq {
		s.lastTerm.Store(tip.Term)
	}
	if maxSeq > atomic.LoadInt64(&s.lastSeq) {
		atomic.StoreInt64(&s.lastSeq, maxSeq)
	}
	if advanceKnown {
		s.posMu.Lock()
		if s.streamPos.Less(known) {
			s.streamPos = known
		}
		s.posMu.Unlock()
	}
	return nil
}

// Recover applies entries without broadcasting to watchers. Used during
// startup replay and by follower resync and leader catch-up, which run on the
// live store. Its revisions bypass the history ring, so the ring is cleared
// first: a ring that skipped them would still claim to cover pins below them.
// Clearing before the commit keeps any snapshot holding these revisions from
// meeting a ring that predates them.
func (s *Store) Recover(entries []wal.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if ring := s.hist.Load(); ring != nil {
		ring.reset()
	}
	b := s.db.NewBatch()
	var maxRev, maxSeq int64
	var tip Position
	for i := range entries {
		e := &entries[i]
		if seq := e.Sequence(); seq > maxSeq {
			maxSeq = seq
			tip = Position{Term: e.Term, Seq: seq}
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
	return s.commitBatch(b, maxRev, maxSeq, tip, false, true, nil)
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
		if err := b.Set(ik, encodeIdx(e.Revision, r.createRevision, r.version, idxSubNone), pebble.NoSync); err != nil {
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
			if err := b.Set(ik, encodeIdx(e.Revision, r.createRevision, r.version, uint16(i)), pebble.NoSync); err != nil {
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
// no limit, and full results.
type ReadOptions struct {
	Revision int64
	FromKey  string
	Limit    int64
	// KeysOnly returns each key with its Revision, CreateRevision and Version
	// only: Value, Lease and PrevRevision are left unset. A listing at HEAD is
	// then served from the index without loading log records.
	KeysOnly bool
}

// Get returns the current value of key, or nil if not found.
func (s *Store) Get(key string) (*KeyValue, error) {
	ie, err := idxEntryFrom(s.db, key)
	if err != nil || ie.rev == 0 {
		return nil, err
	}
	return logEntryForIdx(s.db, make([]byte, logKeyScratchSize), key, ie)
}

// GetAt returns the value of key as of revision. A revision of 0 means current.
// Revision-pinned reads prefer the history ring, then the undo-from-HEAD
// scan, then log replay; both ring and scan signal with errUndoChain when
// they cannot serve the revision.
func (s *Store) GetAt(key string, revision int64) (*KeyValue, error) {
	targetRev, err := s.resolveReadRevision(revision)
	if err != nil {
		return nil, err
	}
	if revision == 0 {
		return s.Get(key)
	}
	if s.nearHead(targetRev) {
		// Ring first, then the undo scan; both signal errUndoChain on gaps.
		kv, err := s.getAtFromHist(key, targetRev)
		if !errors.Is(err, errUndoChain) {
			return kv, err
		}
		kv, err = s.getAtFromHead(key, targetRev)
		if !errors.Is(err, errUndoChain) {
			return kv, err
		}
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
	// strictly below N are rejected. The Watch path uses the same boundary:
	// events at rev=N survive compaction and remain replayable.
	if compactRev := atomic.LoadInt64(&s.compactRev); compactRev > 0 && revision < compactRev {
		return 0, ErrCompacted
	}
	if revision > currentRev {
		return 0, ErrFutureRevision
	}
	return revision, nil
}

func (s *Store) getIdxRev(key string) (int64, error) {
	return idxRevFrom(s.db, key)
}

// idxRevFrom returns key's current revision in r, or 0 if it is not live.
func idxRevFrom(r pebble.Reader, key string) (int64, error) {
	ie, err := idxEntryFrom(r, key)
	return ie.rev, err
}

// idxEntryFrom returns key's decoded index value in r; rev is 0 if it is not
// live.
func idxEntryFrom(r pebble.Reader, key string) (idxEntry, error) {
	v, closer, err := r.Get(idxKey(key))
	if err == pebble.ErrNotFound {
		return idxEntry{}, nil
	}
	if err != nil {
		return idxEntry{}, fmt.Errorf("store: get idx %q: %w", key, err)
	}
	ie := decodeIdx(v)
	closer.Close()
	return ie, nil
}

// idxToKeysOnlyKV builds a keys-only KeyValue from a v2 index value.
func idxToKeysOnlyKV(key string, ie idxEntry) *KeyValue {
	kv := &KeyValue{}
	idxToKeysOnlyKVInto(kv, key, ie)
	return kv
}

func idxToKeysOnlyKVInto(kv *KeyValue, key string, ie idxEntry) {
	*kv = KeyValue{Key: key, Revision: ie.rev, CreateRevision: ie.createRevision, Version: ie.version}
}

// kvSlab hands out KeyValues from shared backing arrays, so that a listing of
// n keys allocates a few arrays rather than n structs. An array is never
// grown in place, so the pointers already handed out stay valid.
//
// Arrays start at kvSlabMin and double up to kvSlabMax, never past what the
// listing's limit can still use: a limit is an upper bound, and paginated
// reads often return far less (small collections, last pages).
type kvSlab struct {
	free  []KeyValue
	size  int
	limit int64 // 0: unlimited
	given int64 // KeyValues allocated so far
}

func newKVSlab(limit int64) *kvSlab {
	return &kvSlab{size: kvSlabMin / 2, limit: limit}
}

const (
	kvSlabMin = 16
	kvSlabMax = 1024
)

func (s *kvSlab) next() *KeyValue {
	if len(s.free) == 0 {
		s.size = min(2*s.size, kvSlabMax)
		n := int64(s.size)
		if s.limit > 0 {
			n = max(min(n, s.limit-s.given), 1)
		}
		s.free = make([]KeyValue, n)
		s.given += n
	}
	kv := &s.free[0]
	s.free = s.free[1:]
	return kv
}

// stripToKeysOnly clears what a keys-only read does not return, so results
// served from the log match those served from the index.
func stripToKeysOnly(kv *KeyValue) {
	kv.Value, kv.Lease, kv.PrevRevision = nil, 0, 0
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

// viewToKV builds a KeyValue from a record decoded in place, copying its
// value out of the source buffer.
func viewToKV(key string, rev int64, r *recordView) *KeyValue {
	kv := &KeyValue{}
	viewToKVInto(kv, key, rev, r)
	return kv
}

// viewToKVInto is viewToKV filling kv.
func viewToKVInto(kv *KeyValue, key string, rev int64, r *recordView) {
	*kv = KeyValue{
		Key:            key,
		Value:          cloneValue(r.value),
		Revision:       rev,
		CreateRevision: r.createRevision,
		PrevRevision:   r.prevRevision,
		Version:        logVersion(r.version, r.delete),
		Lease:          r.lease,
	}
}

func (s *Store) getLogEntry(key string, rev int64) (*KeyValue, error) {
	return logEntryFrom(s.db, key, rev)
}

func logEntryFrom(db pebble.Reader, key string, rev int64) (*KeyValue, error) {
	return logEntryAt(db, make([]byte, logKeyScratchSize), key, rev)
}

// logKeyScratchSize fits both a log key and a txn sub-op log key.
const logKeyScratchSize = 11

// logEntryForIdx loads key's log record located by its index value. A v2
// value points at the record directly; a v1 value falls back to logEntryAt's
// search. lk is a logKeyScratchSize scratch buffer.
func logEntryForIdx(db pebble.Reader, lk []byte, key string, ie idxEntry) (*KeyValue, error) {
	return logEntryForIdxInto(db, lk, key, ie, nil)
}

// logEntryForIdxInto is logEntryForIdx filling dst, when non-nil, for a
// record found where a v2 index value points. Other records come back in a
// KeyValue of their own.
func logEntryForIdxInto(db pebble.Reader, lk []byte, key string, ie idxEntry, dst *KeyValue) (*KeyValue, error) {
	if !ie.v2 {
		return logEntryAt(db, lk, key, ie.rev)
	}
	k := lk[:9]
	putLogKey(k, ie.rev)
	if ie.sub != idxSubNone {
		k = lk[:11]
		binary.BigEndian.PutUint16(k[9:], ie.sub)
	}
	v, closer, err := db.Get(k)
	if err == pebble.ErrNotFound {
		// Not where the index points; search as for a v1 value.
		return logEntryAt(db, lk, key, ie.rev)
	}
	if err != nil {
		return nil, fmt.Errorf("store: get log rev=%d: %w", ie.rev, err)
	}
	defer func() { _ = closer.Close() }()
	r, err := decodeRecord(v)
	if err != nil {
		return nil, err
	}
	if string(r.key) != key {
		return logEntryAt(db, lk, key, ie.rev)
	}
	if dst == nil {
		return viewToKV(key, ie.rev, &r), nil
	}
	viewToKVInto(dst, key, ie.rev, &r)
	return dst, nil
}

// logEntryAt is logEntryFrom with a caller-supplied logKeyScratchSize scratch
// buffer for the log key, so scans can look up many entries without
// allocating a key for each.
func logEntryAt(db pebble.Reader, lk []byte, key string, rev int64) (*KeyValue, error) {
	// Fast path: non-txn entries are stored at logKey(rev).
	lk = lk[:9]
	putLogKey(lk, rev)
	v, closer, err := db.Get(lk)
	if err == nil {
		defer closer.Close()
		r, err := decodeRecord(v)
		if err != nil {
			return nil, err
		}
		return viewToKV(key, rev, &r), nil
	}
	if err != pebble.ErrNotFound {
		return nil, fmt.Errorf("store: get log rev=%d: %w", rev, err)
	}

	// Slow path: txn entries are stored at logKeyWithSub(rev, subIndex).
	// Scan all sub-keys at this revision and find the one matching key.
	lower := logKeyWithSub(rev, 0)
	upper := logKey(rev + 1)
	iter, iterErr := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if iterErr != nil {
		return nil, fmt.Errorf("store: get log txn scan rev=%d: %w", rev, iterErr)
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		r, rerr := decodeRecord(iter.Value())
		if rerr != nil {
			continue
		}
		if string(r.key) == key {
			return viewToKV(key, rev, &r), nil
		}
	}
	return nil, fmt.Errorf("store: get log rev=%d key=%q: not found in txn sub-ops", rev, key)
}

// Reads at a past revision R have two strategies. Replaying the log up to R
// costs O(retained history before R); starting from HEAD and undoing the
// changes made after R costs O(revisions after R). Reads at a recent
// revision — a transaction's snapshot, apiserver's paginated LIST pages —
// are served from HEAD. The replay stays as the fallback for revisions deep
// in history and for any log whose undo chain does not check out.

// errUndoChain reports that a record's PrevRevision did not lead back to the
// state at the target revision, so the caller must replay the log instead.
var errUndoChain = errors.New("store: undo chain inconsistent")

// nearHead reports whether rev is closer to HEAD than to the compaction
// watermark, i.e. whether undoing from HEAD scans fewer revisions than
// replaying up to rev.
func (s *Store) nearHead(rev int64) bool {
	return atomic.LoadInt64(&s.currentRev)-rev <= rev-atomic.LoadInt64(&s.compactRev)
}

// undoAfter returns, for every key matching prefix, fromKey and match that
// changed after rev in snap, its state at rev (nil when it was absent).
//
// The first change to a key after rev records in PrevRevision the revision
// of the key's live version at rev, or 0 when the key did not exist then.
// Compaction keeps that version: it only drops a key's records older than
// its newest one at or before the compaction revision, and rev is at or
// above it.
func undoAfter(snap pebble.Reader, prefix, fromKey string, rev int64, match func(string) bool) (map[string]*KeyValue, error) {
	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: logKey(rev + 1), UpperBound: logUpper})
	if err != nil {
		return nil, fmt.Errorf("store: undo scan iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	undo := make(map[string]*KeyValue)
	lk := make([]byte, 9)
	for iter.First(); iter.Valid(); iter.Next() {
		r, err := decodeRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		// Filter on the aliased key bytes; the comparisons don't allocate.
		if _, seen := undo[string(r.key)]; seen || len(r.key) < len(prefix) ||
			string(r.key[:len(prefix)]) != prefix || string(r.key) < fromKey {
			continue
		}
		key := string(r.key)
		if match != nil && !match(key) {
			continue
		}
		if r.prevRevision == 0 {
			if !r.create {
				return nil, errUndoChain
			}
			undo[key] = nil
			continue
		}
		if r.prevRevision > rev {
			return nil, errUndoChain
		}
		prev, err := logEntryAt(snap, lk, key, r.prevRevision)
		if err != nil || prev == nil {
			return nil, errUndoChain
		}
		undo[key] = prev
	}
	return undo, iter.Error()
}

func (s *Store) getAtFromHead(key string, rev int64) (*KeyValue, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	ie, err := idxEntryFrom(snap, key)
	if err != nil {
		return nil, err
	}
	cur := ie.rev
	if cur != 0 && cur <= rev {
		// Unchanged since rev.
		return logEntryForIdx(snap, make([]byte, logKeyScratchSize), key, ie)
	}
	undo, err := undoAfter(snap, key, "", rev, func(k string) bool { return k == key })
	if err != nil {
		return nil, err
	}
	if kv, changed := undo[key]; changed {
		return kv, nil
	}
	if cur != 0 {
		// Live at a revision after rev, yet no change after rev.
		return nil, errUndoChain
	}
	return nil, nil
}

func (s *Store) listAtFromHead(prefix string, opts ReadOptions, rev int64) ([]*KeyValue, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	undo, err := undoAfter(snap, prefix, opts.FromKey, rev, nil)
	if err != nil {
		return nil, err
	}
	return listFromUndo(snap, prefix, opts, undo)
}

func (s *Store) countAtFromHead(prefix, fromKey string, rev int64) (int64, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	undo, err := undoAfter(snap, prefix, fromKey, rev, nil)
	if err != nil {
		return 0, err
	}
	return countFromUndo(snap, prefix, fromKey, undo)
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
		r, err := decodeRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		if string(r.key) != key {
			continue
		}
		if r.delete {
			return nil, nil
		}
		return viewToKV(key, rev, &r), nil
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
		return s.listCurrent(prefix, opts.FromKey, opts.Limit, opts.KeysOnly)
	}
	var kvs []*KeyValue
	fromHead := s.nearHead(targetRev)
	if fromHead {
		// The ring serves the same shape undoFromHead would; it earns its
		// keep here, where the alternative is scanning the log tail.
		kvs, err = s.listAtFromHist(prefix, opts, targetRev)
		if errors.Is(err, errUndoChain) {
			kvs, err = s.listAtFromHead(prefix, opts, targetRev)
		}
	}
	if !fromHead || errors.Is(err, errUndoChain) {
		kvs, err = s.listAtByReplay(prefix, opts, targetRev)
	}
	if err != nil {
		return nil, err
	}
	// Past-revision listings take some or all records from the log.
	if opts.KeysOnly {
		for _, kv := range kvs {
			stripToKeysOnly(kv)
		}
	}
	return kvs, nil
}

// listAtByReplay rebuilds the state at targetRev by replaying the log from
// the start. Its cost grows with the retained history rather than with the
// distance from HEAD, which is what listAtFromHead avoids.
func (s *Store) listAtByReplay(prefix string, opts ReadOptions, targetRev int64) ([]*KeyValue, error) {
	events, _, err := s.scanLog(prefix, 1, targetRev, false, opts.KeysOnly)
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

func (s *Store) listCurrent(prefix, fromKey string, limit int64, keysOnly bool) ([]*KeyValue, error) {
	return listCurrentFrom(s.db, prefix, fromKey, limit, keysOnly)
}

// listCurrentFrom lists live keys at HEAD in db. keysOnly results are built
// from v2 index values alone; only keys with a v1 value load their record.
func listCurrentFrom(db pebble.Reader, prefix, fromKey string, limit int64, keysOnly bool) ([]*KeyValue, error) {
	lower, upper := idxBounds(prefix, fromKey)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("store: list iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var out []*KeyValue
	lk := make([]byte, logKeyScratchSize)
	slab := newKVSlab(limit)
	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
		k := string(iter.Key()[1:]) // strip 'i' prefix
		ie := decodeIdx(iter.Value())
		if keysOnly && ie.v2 {
			kv := slab.next()
			idxToKeysOnlyKVInto(kv, k, ie)
			out = append(out, kv)
			continue
		}
		kv, err := logEntryForIdxInto(db, lk, k, ie, slab.next())
		if err != nil {
			return nil, err
		}
		if keysOnly {
			stripToKeysOnly(kv)
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
	targetRev, err := s.resolveReadRevision(opts.Revision)
	if err != nil {
		return 0, err
	}
	if s.nearHead(targetRev) {
		if n, err := s.countAtFromHist(prefix, opts.FromKey, targetRev); !errors.Is(err, errUndoChain) {
			return n, err
		}
		n, err := s.countAtFromHead(prefix, opts.FromKey, targetRev)
		if !errors.Is(err, errUndoChain) {
			return n, err
		}
	}
	kvs, err := s.ListRange(prefix, ReadOptions{Revision: opts.Revision, FromKey: opts.FromKey, KeysOnly: true})
	if err != nil {
		return 0, err
	}
	return int64(len(kvs)), nil
}

// countCurrent counts live keys with prefix at HEAD whose key is
// lexicographically >= fromKey when fromKey is set.
func (s *Store) countCurrent(prefix, fromKey string) (int64, error) {
	return countCurrentFrom(s.db, prefix, fromKey)
}

func countCurrentFrom(db pebble.Reader, prefix, fromKey string) (int64, error) {
	lower, upper := idxBounds(prefix, fromKey)
	iter, err := db.NewIter(&pebble.IterOptions{
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
	events, _, err := s.scanLog("", 1, atomic.LoadInt64(&s.currentRev), true, false)
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
	events, _, err := s.scanLog(prefix, fromRev, toRev, true, false)
	return events, err
}

// --- Watch ---

const watchLiveBatchBuffer = 1024

// watchProgressInterval bounds how often a watch whose prefix saw no events is
// sent a standalone progress marker. A watch that *did* receive events gets its
// marker appended to that batch for free, so this only paces the otherwise
// silent ones. Consumers use progress to answer "how current am I" on roughly
// a one-second cadence, so pacing markers to match keeps the cost proportional
// to what is consumed rather than to the write rate.
const watchProgressInterval = time.Second

type watchSubscription struct {
	id         uint64
	prefix     string
	withPrevKV bool
	progress   bool
	nextRev    int64
	live       chan []Event
}

// EventType classifies a watch event.
type EventType int

const (
	EventPut      EventType = iota // create or update
	EventDelete                    // deletion
	EventProgress                  // no data change; reports how far this watch is synced
)

// Event is a single watch notification.
type Event struct {
	Type   EventType
	KV     *KeyValue
	PrevKV *KeyValue // nil for creates

	// Revision is set only on EventProgress, where it reports the revision
	// through which every matching event has already been delivered on this
	// channel. Put and Delete leave it zero; their revision is KV.Revision.
	Revision int64
}

// WatchOptions configures an optional watch parameter.
type WatchOptions struct {
	// PrevKV requests the previous KV on updates and deletes. Off by default:
	// populating it adds one Pebble lookup per non-create event, which is
	// significant under high churn.
	PrevKV bool

	// Progress requests EventProgress markers on the channel. Off by default
	// so consumers that only switch on Put/Delete never observe an event with
	// a nil KV.
	Progress bool
}

// Watch streams events for keys matching prefix starting from startRev+1.
// The channel is closed when ctx is cancelled.
//
// With opts.Progress, the channel also carries EventProgress markers. A marker
// is emitted in-band, so it can never overtake an undelivered event: on receipt,
// every matching event at or below its Revision has already been read from this
// channel. That is what lets a consumer answer "I am synced through R" for a
// prefix that has seen no writes at all. Every run of events is followed by a
// marker, so a consumer that needs whole revisions can hold back the newest
// one until a marker (or a later revision's event) shows it is complete.
//
// The channel buffer is intentionally small. Backpressure should flow back to
// scanLog quickly so a slow consumer doesn't accumulate large amounts of
// converted events in memory and doesn't delay compaction by holding live
// references to old revisions. The etcd handler's drain loop coalesces
// whatever is immediately available into one WatchResponse, so a small buffer
// still amortises gRPC Send overhead.
func (s *Store) Watch(ctx context.Context, prefix string, startRev int64, opts WatchOptions) (<-chan Event, error) {
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
		withPrevKV: opts.PrevKV,
		progress:   opts.Progress,
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
	go s.watchLoop(ctx, prefix, startRev, opts.PrevKV, boundary, registered, ch, sub, unregister)
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
	// Wake an idle dispatch loop. Close-and-replace so a waiter that captured
	// the channel before this registration observes the close.
	if s.watcherWake != nil {
		close(s.watcherWake)
		s.watcherWake = nil
	}
}

// watcherWakeChanLocked returns the current wake channel, creating it on first
// use. Callers must hold watchHubMu.
func (s *Store) watcherWakeChanLocked() chan struct{} {
	if s.watcherWake == nil {
		s.watcherWake = make(chan struct{})
	}
	return s.watcherWake
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
		// Seal the replay the same way the dispatcher seals a live batch.
		if sub.progress && !s.sendWatchEvents(ctx, ch, []Event{progressEvent(handoff)}) {
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
	events, scanned, err := s.scanLog(prefix, fromRev, toRev, withPrevKV, false)
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
	// Pacing state for standalone markers: the revision last announced to
	// otherwise-silent watches, and when.
	var (
		lastMarkerRev int64
		lastMarkerAt  time.Time
	)

	for {
		// While no watch is subscribed, reset the scan cursor and block until a
		// watch registers instead of scanning every commit for no consumer. The
		// check, reset, and wake-channel capture happen under one lock hold: a
		// watch registers under watchHubMu, so it either predates the check
		// (map non-empty, no wait) or closes the captured wake channel after it.
		// A later watch replays its own requested range before joining live
		// delivery, so skipping the intervening history here loses nothing.
		s.watchHubMu.Lock()
		for len(s.watchers) == 0 {
			nextRev = atomic.LoadInt64(&s.currentRev) + 1
			wake := s.watcherWakeChanLocked()
			s.watchHubMu.Unlock()
			select {
			case <-wake:
			case <-s.closed:
				return
			}
			s.watchHubMu.Lock()
		}
		s.watchHubMu.Unlock()
		if err := s.WaitForRevision(ctx, nextRev); err != nil {
			return
		}
		toRev := atomic.LoadInt64(&s.currentRev)
		start := time.Now()
		events, scanned, err := s.scanLog("", nextRev, toRev, false, false)
		if err != nil {
			return
		}
		// Standalone markers go to watches that matched nothing; watches that
		// did receive events carry their marker in the same batch for free and
		// are never gated here.
		//
		// Pacing bounds the cost under sustained writes, but it must never be
		// what leaves the newest revision unannounced: once the loop catches
		// up it blocks until the next commit, so a marker suppressed here
		// would have no later iteration to correct it and silent watches would
		// stay stale for as long as the store is quiet. Hence "paced, or
		// caught up".
		caughtUp := atomic.LoadInt64(&s.currentRev) <= toRev
		standalone := toRev > lastMarkerRev &&
			(caughtUp || time.Since(lastMarkerAt) >= watchProgressInterval)
		if standalone {
			lastMarkerRev, lastMarkerAt = toRev, time.Now()
		}
		matched := s.dispatchWatchEvents(events, toRev, standalone)
		recordWatchScanMetrics(time.Since(start), nextRev, toRev, scanned, matched)
		nextRev = toRev + 1
	}
}

// dispatchWatchEvents fans one shared decoded batch out in memory. A slow
// consumer cannot block other watches: when its bounded queue fills, that
// watch is closed and can reconnect from its last delivered revision.
//
// PrevKV reconstruction reads the Pebble revision log, which can miss the block
// cache and touch disk. Those reads run between two short watchHubMu holds
// rather than under one, so a cold lookup never stalls watch registration,
// teardown, or delivery to other watches. A watch registered after the snapshot
// has nextRev beyond every revision in this batch (the dispatcher only reaches
// here after scanning up to the committed revision), so it correctly receives
// nothing from this batch and joins from the next one.
func (s *Store) dispatchWatchEvents(events []Event, toRev int64, standalone bool) int {
	// snap is a copy of a subscription's routing fields plus a pointer back to
	// the subscription, taken so PrevKV disk reads below run without the lock.
	type snap struct {
		w          *watchSubscription
		id         uint64
		prefix     string
		withPrevKV bool
		progress   bool
		nextRev    int64
	}

	s.watchHubMu.Lock()
	snaps := make([]snap, 0, len(s.watchers))
	for id, w := range s.watchers {
		snaps = append(snaps, snap{
			w:          w,
			id:         id,
			prefix:     w.prefix,
			withPrevKV: w.withPrevKV,
			progress:   w.progress,
			nextRev:    w.nextRev,
		})
	}
	s.watchHubMu.Unlock()
	if len(snaps) == 0 {
		return 0
	}

	// Build each subscriber's batch without holding the hub lock, resolving
	// PrevKV lookups through a shared cache (this loop runs single-threaded).
	type prevKey struct {
		key string
		rev int64
	}
	prevCache := make(map[prevKey]*KeyValue)
	batches := make([][]Event, len(snaps))
	matched := 0
	for i, sn := range snaps {
		var batch []Event
		for j := range events {
			ev := events[j]
			if ev.KV.Revision < sn.nextRev ||
				(sn.prefix != "" && !strings.HasPrefix(ev.KV.Key, sn.prefix)) {
				continue
			}
			if sn.withPrevKV && ev.KV.PrevRevision > 0 {
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
		matched += len(batch)
		// A watch that received events carries its marker in the same batch,
		// even when its last event is already at toRev. The marker is what
		// tells the consumer that revision is complete: the channel carries
		// one event at a time, so without it a consumer that finds the channel
		// empty cannot tell a finished revision from one still being sent.
		if sn.progress && len(batch) > 0 {
			batch = append(batch, progressEvent(toRev))
		}
		batches[i] = batch
	}

	// Watches that matched nothing share one marker slice; every consumer only
	// reads it, so this is a single allocation for the whole fan-out.
	var silent []Event
	if standalone {
		silent = []Event{progressEvent(toRev)}
	}

	// Re-acquire only to publish batches and evict slow consumers. Skip any
	// watch that unsubscribed while batches were being built; IDs are never
	// reused, so an identity check reliably detects removal and prevents a
	// send on a closed channel.
	s.watchHubMu.Lock()
	for i, sn := range snaps {
		batch := batches[i]
		markerOnly := false
		if len(batch) == 0 {
			if silent == nil || !sn.progress {
				continue
			}
			batch, markerOnly = silent, true
		}
		if current, ok := s.watchers[sn.id]; !ok || current != sn.w {
			continue
		}
		select {
		case sn.w.live <- batch:
		default:
			// A full queue means this consumer is too slow for the event
			// stream, so it is evicted and can reconnect from its last
			// delivered revision. A marker is advisory and the next one
			// supersedes it, so dropping one must not cost a watch its
			// subscription — otherwise a watch on a quiet prefix could be
			// evicted by progress traffic alone.
			if markerOnly {
				continue
			}
			delete(s.watchers, sn.id)
			close(sn.w.live)
		}
	}
	s.watchHubMu.Unlock()
	return matched
}

func progressEvent(rev int64) Event {
	return Event{Type: EventProgress, Revision: rev}
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
// false, the per-event PrevKV lookup is skipped; when keysOnly is set, event
// values are left nil. Records are decoded in place and only matching ones
// are copied out.
func (s *Store) scanLog(prefix string, fromRev, toRev int64, withPrevKV, keysOnly bool) ([]Event, int, error) {
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

	prefixBytes := []byte(prefix)
	var events []Event
	var scanned int
	for iter.First(); iter.Valid(); iter.Next() {
		scanned++
		r, err := decodeRecord(iter.Value())
		if err != nil {
			return nil, scanned, err
		}
		if !bytes.HasPrefix(r.key, prefixBytes) {
			continue
		}
		rev := decodeLogKey(iter.Key())
		if keysOnly {
			r.value = nil
		}
		kv := viewToKV(string(r.key), rev, &r)
		var prevKV *KeyValue
		if withPrevKV && r.prevRevision > 0 {
			prevKV, err = s.getLogEntry(kv.Key, r.prevRevision)
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
	return logVersion(r.version, r.delete)
}

// logVersion is a record's key version. Records written before versions were
// stored carry 0: 1 for a live key, 0 for a deletion.
func logVersion(version int64, deleted bool) int64 {
	if version > 0 {
		return version
	}
	if deleted {
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
