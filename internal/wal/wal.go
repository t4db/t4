package wal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// walLogger is the minimal logging interface required by WAL.
type walLogger interface {
	Debugf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// stdlibLogger wraps the standard library log package so tests that call
// Open without WithLogger still get output rather than a panic.
type stdlibLogger struct{}

func (stdlibLogger) Debugf(format string, args ...interface{}) {}
func (stdlibLogger) Warnf(format string, args ...interface{})  { log.Printf("[WARN]  "+format, args...) }
func (stdlibLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

const (
	DefaultSegmentMaxSize = 50 << 20         // 50 MB
	DefaultSegmentMaxAge  = 60 * time.Second // 1 minute; controls S3 PUT frequency
)

// Uploader is called when a segment is ready to be persisted to object storage.
// The segment at localPath should be uploaded to objectKey and, on success,
// the local file should be deleted. The call must be idempotent.
type Uploader func(ctx context.Context, localPath, objectKey string) error

// WAL manages the write-ahead log for a single node.
//
// Writes are appended to the active local segment file (fsynced per entry).
// When the active segment exceeds the size or age threshold it is sealed and
// an upload is triggered asynchronously. The local file is removed after a
// confirmed upload.
//
// If object storage is not configured (uploader == nil) segments accumulate
// locally and serve as the sole crash-recovery mechanism.
type WAL struct {
	dir        string
	term       uint64
	segMaxSize int64
	segMaxAge  time.Duration
	uploader   Uploader // may be nil (no object storage)
	syncUpload bool     // seal+upload synchronously on every AppendBatch
	log        walLogger

	mu     sync.Mutex
	active *SegmentWriter
	closed bool

	// uploadCtx is derived from the context passed to Start. It is used for
	// synchronous S3 uploads inside rotateSyncLocked so that per-request
	// timeouts (batchCtx) cannot cancel a durable upload mid-way.
	uploadCtx    context.Context
	uploadCancel context.CancelFunc

	uploadWake  chan struct{} // signals uploadLoop that a segment is pending
	wg          sync.WaitGroup
	cancelLoops context.CancelFunc // cancels rotationLoop and uploadLoop

	// pending holds sealed segments whose upload has not been confirmed, by
	// object key: it is the upload queue. Synchronous-upload mode uploads them
	// before the active segment, so object storage never holds an
	// acknowledged write while missing an earlier one.
	pendingMu sync.Mutex
	pending   map[string]string // object key → local path

	// leftover holds segment files found in the directory when the WAL
	// started, by object key, guarded by pendingMu. They are uploaded in the
	// background like pending segments, but never gate synchronous uploads:
	// a follower's segments, left behind when it becomes leader, can collide
	// with the previous leader's segments under the same key, and such a
	// conflict must not block writes.
	leftover map[string]string
}

// ErrSegmentConflict is returned by an Uploader when the segment's object key
// is already taken by an object that does not contain all of the segment's
// entries. Retrying cannot succeed: the key is immutable, so the entries are
// not in object storage under that key and never will be.
var ErrSegmentConflict = errors.New("wal: segment key already holds different entries")

// Upload retry backoff: after a failed pass, uploadLoop waits uploadRetryMin,
// doubling per consecutive failure up to uploadRetryMax. Variables so that
// tests can shorten them.
var (
	uploadRetryMin = time.Second
	uploadRetryMax = 30 * time.Second
)

// RecoveryStore is the state-machine subset needed for WAL replay.
type RecoveryStore interface {
	Recover(entries []Entry) error
}

// New returns a WAL configured with opts. Call Open before use.
func New(opts ...Option) *WAL {
	w := &WAL{
		segMaxSize: DefaultSegmentMaxSize,
		segMaxAge:  DefaultSegmentMaxAge,
		uploadWake: make(chan struct{}, 1),
		pending:    make(map[string]string),
		leftover:   make(map[string]string),
	}
	for _, o := range opts {
		o(w)
	}
	if w.log == nil {
		w.log = stdlibLogger{}
	}
	return w
}

// Open opens (or creates) the WAL directory and returns a ready WAL.
// Callers must call Start to begin background processing.
func Open(dir string, term uint64, startRev int64, opts ...Option) (*WAL, error) {
	w := New(opts...)
	if err := w.Open(dir, term, startRev); err != nil {
		return nil, err
	}
	return w, nil
}

// MaxSequence returns the highest WAL sequence found in local segment files.
// It is used before opening a new writer so metadata-only entries that do not
// advance the user revision still keep the next segment from reusing their ID.
func MaxSequence(dir string) (int64, error) {
	paths, err := LocalSegments(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	var maxSeq int64
	var firstErr error
	for _, path := range paths {
		sr, closer, err := OpenSegmentFile(path)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		entries, readErr := sr.ReadAll()
		closer()
		for _, e := range entries {
			if seq := e.Sequence(); seq > maxSeq {
				maxSeq = seq
			}
		}
		if readErr != nil {
			if firstErr == nil {
				firstErr = readErr
			}
			continue
		}
	}
	return maxSeq, firstErr
}

// Open opens (or creates) the WAL directory. Callers must call Start to begin
// background processing.
//
// No segment is created here: the first append creates one named after its
// first entry's sequence (see ensureActiveLocked). startRev is not used to
// name it — at Open the caller may not yet know the next sequence, since
// recovery can advance it — and is kept for the WALWriter interface.
func (w *WAL) Open(dir string, term uint64, startRev int64) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dir = dir
	w.term = term
	w.closed = false
	w.uploadWake = make(chan struct{}, 1)
	w.active = nil
	return nil
}

// ensureActiveLocked creates the active segment, named after firstSeq, if
// there is none. Segments are created lazily, by the append that writes their
// first entry, so a segment's name always matches its first entry. A name
// picked in advance is a guess: after recovery it can name a segment that is
// already in object storage, and uploading different entries under that key
// collides with it. Must be called with w.mu held.
func (w *WAL) ensureActiveLocked(firstSeq int64) error {
	if w.active != nil {
		return nil
	}
	sw, err := OpenSegmentWriter(w.dir, w.term, firstSeq)
	if err != nil {
		return err
	}
	w.active = sw
	return nil
}

// discardEmptyActiveLocked removes the active segment if it holds no entries,
// so the next append creates one named after its own first entry. Must be
// called with w.mu held.
func (w *WAL) discardEmptyActiveLocked() {
	if w.active == nil || w.active.EntryCount() > 0 {
		return
	}
	_ = w.active.Close()
	_ = os.Remove(w.active.Path())
	w.active = nil
}

// ReplayLocal replays locally stored WAL segments into db, applying entries
// whose Sequence is greater than afterSeq. afterSeq is the highest
// WAL/peer-stream sequence already applied (typically db.LastSequence());
// filtering by Sequence rather than Revision is required after the seq/rev
// split because Compact entries share their Revision with the preceding
// data write but have a distinct Sequence.
func (w *WAL) ReplayLocal(db RecoveryStore, afterSeq int64) error {
	paths, err := LocalSegments(w.dir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		sr, closer, err := OpenSegmentFile(path)
		if err != nil {
			return err
		}
		entries, readErr := sr.ReadAll()
		closer()
		if readErr != nil {
			w.log.Warnf("wal: partial local segment %q: %v", path, readErr)
		}
		var applicable []Entry
		for _, e := range entries {
			if e.Sequence() > afterSeq {
				applicable = append(applicable, *e)
			}
		}
		if len(applicable) > 0 {
			if err := db.Recover(applicable); err != nil {
				return err
			}
		}
	}
	return nil
}

// Option configures a WAL.
type Option func(*WAL)

// WithUploader sets the function used to archive sealed segments to object storage.
func WithUploader(u Uploader) Option {
	return func(w *WAL) { w.uploader = u }
}

// WithSegmentMaxSize sets the byte threshold that triggers segment rotation.
func WithSegmentMaxSize(n int64) Option {
	return func(w *WAL) { w.segMaxSize = n }
}

// WithSegmentMaxAge sets the time threshold that triggers segment rotation.
func WithSegmentMaxAge(d time.Duration) Option {
	return func(w *WAL) { w.segMaxAge = d }
}

// SetSyncUpload turns synchronous upload on or off for subsequent AppendBatch
// calls. A leader flips this on when replication degrades below its configured
// ACK target: with too few followers to make a write durable by replication,
// object storage becomes the only place an acknowledged write survives, so it
// has to get there before the caller is told the write succeeded.
//
// Each batch appended in this mode seals and uploads its own segment, so a
// sustained degradation trades one PUT per batch for the durability guarantee.
func (w *WAL) SetSyncUpload(on bool) {
	w.mu.Lock()
	w.syncUpload = on
	w.mu.Unlock()
}

// WithSyncUpload makes every AppendBatch upload the active segment to object
// storage synchronously before returning. This guarantees that any acknowledged
// write is durable in S3, even if the process crashes immediately after. Has no
// effect when no uploader is configured.
func WithSyncUpload() Option {
	return func(w *WAL) { w.syncUpload = true }
}

// WithLogger sets the logger used by the WAL. When not provided the WAL
// uses a stdlib-backed logger that discards DEBUG output.
func WithLogger(log walLogger) Option {
	return func(w *WAL) { w.log = log }
}

// Start launches background goroutines. Must be called before Append.
func (w *WAL) Start(ctx context.Context) {
	// uploadCtx lives as long as the WAL itself (cancelled by Close) so that
	// synchronous uploads in rotateSyncLocked are not cancelled by per-request
	// deadline contexts.
	w.uploadCtx, w.uploadCancel = context.WithCancel(ctx)
	loopCtx, cancel := context.WithCancel(ctx)
	w.cancelLoops = cancel
	if w.uploader != nil {
		w.queueLocalSegments()
	}
	w.wg.Add(2)
	go w.rotationLoop(loopCtx)
	go w.uploadLoop(loopCtx)
}

// queueLocalSegments queues the segment files already in the WAL directory
// as leftovers: an uploaded segment's file is removed, so these were never
// uploaded, by an earlier run, or by this node as a follower.
func (w *WAL) queueLocalSegments() {
	paths, err := LocalSegments(w.dir)
	if err != nil {
		w.log.Errorf("wal: list local segments to upload: %v", err)
		return
	}
	for _, path := range paths {
		term, firstRev, ok := ParseSegmentName(filepath.Base(path))
		if !ok {
			continue
		}
		w.pendingMu.Lock()
		w.leftover[ObjectKey(term, firstRev)] = path
		w.pendingMu.Unlock()
	}
	w.wakeUploader()
}

// queueUpload marks a sealed segment for upload and wakes uploadLoop. It
// never blocks, so it is safe to call with w.mu held.
func (w *WAL) queueUpload(objKey, localPath string) {
	w.addPending(objKey, localPath)
	w.wakeUploader()
}

func (w *WAL) wakeUploader() {
	select {
	case w.uploadWake <- struct{}{}:
	default: // uploadLoop is already due to run
	}
}

// Append writes e to the active segment and fsyncs.
// Safe to call concurrently; writes are serialised under the mutex.
func (w *WAL) Append(e *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal: closed")
	}
	if err := w.ensureActiveLocked(e.Sequence()); err != nil {
		return err
	}
	if err := w.active.Append(e); err != nil {
		return err
	}
	// Size-based rotation happens in the background loop; we only trigger it
	// here to avoid holding the lock during the potentially slow seal+open.
	if w.active.Size() >= w.segMaxSize {
		w.rotateLocked()
	}
	return nil
}

// AppendBatch writes all entries to the active segment and fsyncs once.
// This amortises the fsync cost across all entries in the batch.
// Safe to call concurrently; writes are serialised under the mutex.
// ctx is checked before acquiring the lock; a cancelled ctx causes an early
// return. The fsync itself is not interrupted mid-way.
//
// If WithSyncUpload was set, the active segment is uploaded to object storage
// synchronously before this method returns. AppendBatch rolls back the batch and
// fails (so the write is not acknowledged) if the upload fails.
func (w *WAL) AppendBatch(ctx context.Context, entries []*Entry) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal: closed")
	}
	if len(entries) == 0 {
		return nil
	}
	if err := w.ensureActiveLocked(entries[0].Sequence()); err != nil {
		return err
	}
	rollbackSize := w.active.Size()
	rollbackEntryCount := w.active.EntryCount()
	for _, e := range entries {
		if err := w.active.AppendNoSync(e); err != nil {
			return err
		}
	}
	if err := w.active.Sync(); err != nil {
		return err
	}
	if w.syncUpload && w.uploader != nil {
		if err := w.rotateSyncLocked(rollbackSize, rollbackEntryCount); err != nil {
			return err
		}
	} else if w.active.Size() >= w.segMaxSize {
		w.rotateLocked()
	}
	return nil
}

// rotateSyncLocked uploads the active segment to object storage synchronously.
// The segment is only sealed and rotated after the upload succeeds. If the
// upload fails, the just-appended batch is truncated away so local replay cannot
// later expose a write that was never acknowledged.
//
// The upload uses w.uploadCtx (derived from the WAL's Start context) rather
// than the per-batch ctx so that a per-request deadline cannot cancel the
// upload mid-way.
//
// Must be called with w.mu held; returns with w.mu held.
func (w *WAL) rotateSyncLocked(rollbackSize int64, rollbackEntryCount int) error {
	seg := w.active
	if seg == nil || seg.EntryCount() == 0 {
		return nil
	}
	if err := w.uploadPendingLocked(); err != nil {
		if rollbackErr := seg.rollback(rollbackSize, rollbackEntryCount); rollbackErr != nil {
			return fmt.Errorf("wal: upload of earlier segments failed and rollback failed: upload: %w; rollback: %v", err, rollbackErr)
		}
		w.discardEmptyActiveLocked()
		return err
	}
	objKey := ObjectKey(seg.Term(), seg.FirstRev())
	localPath := seg.Path()

	uploadErr := w.uploader(w.uploadCtx, localPath, objKey)
	if uploadErr != nil {
		w.log.Errorf("wal: sync upload %q → %q: %v", localPath, objKey, uploadErr)
		if rollbackErr := seg.rollback(rollbackSize, rollbackEntryCount); rollbackErr != nil {
			return fmt.Errorf("wal: sync upload failed and rollback failed: upload: %w; rollback: %v", uploadErr, rollbackErr)
		}
		w.discardEmptyActiveLocked()
		return uploadErr
	}

	if err := seg.Seal(); err != nil {
		return fmt.Errorf("wal: seal segment after sync upload: %w", err)
	}
	w.active = nil
	return nil
}

func (w *WAL) addPending(objKey, localPath string) {
	w.pendingMu.Lock()
	w.pending[objKey] = localPath
	w.pendingMu.Unlock()
}

func (w *WAL) donePending(objKey string) {
	w.pendingMu.Lock()
	delete(w.pending, objKey)
	w.pendingMu.Unlock()
}

// queuedUpload is a segment waiting for upload.
type queuedUpload struct {
	key, path string
	leftover  bool
}

// uploadQueue returns the pending and leftover segments oldest first (keys
// sort by term, then first sequence).
func (w *WAL) uploadQueue() []queuedUpload {
	w.pendingMu.Lock()
	q := make([]queuedUpload, 0, len(w.pending)+len(w.leftover))
	for k, p := range w.pending {
		q = append(q, queuedUpload{key: k, path: p})
	}
	for k, p := range w.leftover {
		if _, ok := w.pending[k]; !ok {
			q = append(q, queuedUpload{key: k, path: p, leftover: true})
		}
	}
	w.pendingMu.Unlock()
	sort.Slice(q, func(i, j int) bool { return q[i].key < q[j].key })
	return q
}

// doneUpload removes a segment from the upload queue.
func (w *WAL) doneUpload(objKey string) {
	w.pendingMu.Lock()
	delete(w.pending, objKey)
	delete(w.leftover, objKey)
	w.pendingMu.Unlock()
}

// uploadOne uploads a queued segment and reports whether the caller should go
// on with the next one. A conflict is permanent: a pending segment stays
// queued, so that synchronous-upload mode does not publish later writes past
// entries missing from object storage, and is skipped from then on; for a
// leftover the published object is authoritative, so it is dropped.
func (w *WAL) uploadOne(ctx context.Context, u queuedUpload, conflicted map[string]bool) error {
	err := w.uploader(ctx, u.path, u.key)
	switch {
	case err == nil, errors.Is(err, os.ErrNotExist):
		// Without the local file there is nothing left to upload.
		w.doneUpload(u.key)
	case errors.Is(err, ErrSegmentConflict) && u.leftover:
		w.log.Warnf("wal: leftover segment %q differs from published %q — keeping the published object: %v", u.path, u.key, err)
		w.doneUpload(u.key)
	case errors.Is(err, ErrSegmentConflict):
		w.log.Errorf("wal: upload %q → %q: %v", u.path, u.key, err)
		conflicted[u.key] = true
	default:
		return fmt.Errorf("%q → %q: %w", u.path, u.key, err)
	}
	return nil
}

// pendingSorted returns the pending segments' object keys oldest first (keys
// sort by term, then first sequence), with their local paths.
func (w *WAL) pendingSorted() ([]string, map[string]string) {
	w.pendingMu.Lock()
	keys := make([]string, 0, len(w.pending))
	paths := make(map[string]string, len(w.pending))
	for k, p := range w.pending {
		keys = append(keys, k)
		paths[k] = p
	}
	w.pendingMu.Unlock()
	sort.Strings(keys)
	return keys, paths
}

// uploadPendingLocked uploads the sealed segments whose asynchronous upload
// has not been confirmed, oldest first (object keys sort by term, then first
// sequence). The uploader is idempotent, so racing the upload loop on the
// same segment is harmless; a segment whose local file is already gone was
// uploaded by that loop. Must be called with w.mu held.
func (w *WAL) uploadPendingLocked() error {
	keys, paths := w.pendingSorted()
	for _, k := range keys {
		err := w.uploader(w.uploadCtx, paths[k], k)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			w.log.Errorf("wal: sync upload of earlier segment %q → %q: %v", paths[k], k, err)
			return fmt.Errorf("wal: upload earlier segment %s: %w", k, err)
		}
		w.donePending(k)
	}
	return nil
}

// rotateLocked seals the active segment and opens a fresh one.
// Must be called with w.mu held.
func (w *WAL) rotateLocked() {
	if w.active == nil {
		return
	}
	seg := w.active
	if err := seg.Seal(); err != nil {
		// Seal failed; keep the old (unsealed) segment as active so the next
		// Append returns an error rather than panicking on a nil dereference.
		w.log.Errorf("wal: seal segment %q: %v", seg.Path(), err)
		return
	}
	if w.uploader != nil {
		w.queueUpload(ObjectKey(seg.Term(), seg.FirstRev()), seg.Path())
	}
	w.log.Debugf("wal: sealed segment %q (%d entries, %d bytes)", seg.Path(), seg.EntryCount(), seg.Size())
	w.active = nil
}

// rotationLoop periodically rotates the active segment based on age.
func (w *WAL) rotationLoop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.segMaxAge)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.active != nil && w.active.EntryCount() > 0 {
				if err := w.active.Seal(); err != nil {
					w.log.Errorf("wal: age-rotate seal: %v", err)
					w.mu.Unlock()
					continue
				}
				old := w.active
				w.active = nil
				if w.uploader != nil {
					w.queueUpload(ObjectKey(old.Term(), old.FirstRev()), old.Path())
				}
			}
			w.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

// uploadLoop uploads pending segments in the background, oldest first. A
// failed upload ends the pass, and the next pass starts after a backoff that
// doubles from uploadRetryMin up to uploadRetryMax: while object storage is
// down that is one attempt per backoff period, not one per pending segment,
// however long the outage and the backlog grow. Segments sealed during the
// backoff wait for the next pass.
func (w *WAL) uploadLoop(ctx context.Context) {
	defer w.wg.Done()
	if w.uploader == nil {
		return
	}
	conflicted := make(map[string]bool)
	var (
		backoff time.Duration
		retryC  <-chan time.Time
	)
	for {
		select {
		case <-w.uploadWake:
			if retryC != nil {
				continue // backing off; the retry pass covers it
			}
		case <-retryC:
			retryC = nil
		case <-ctx.Done():
			return
		}
		if err := w.uploadPass(ctx, conflicted); err != nil {
			if ctx.Err() != nil {
				return
			}
			backoff = min(max(2*backoff, uploadRetryMin), uploadRetryMax)
			w.log.Errorf("wal: upload %v; retrying in %v", err, backoff)
			retryC = time.After(backoff)
			continue
		}
		backoff = 0
	}
}

// uploadPass uploads queued segments oldest first and stops at the first
// failure. Pending segments in conflicted are skipped: their key holds
// different entries, so they can never be uploaded (see uploadOne).
func (w *WAL) uploadPass(ctx context.Context, conflicted map[string]bool) error {
	for _, u := range w.uploadQueue() {
		if conflicted[u.key] {
			continue
		}
		if err := w.uploadOne(ctx, u, conflicted); err != nil {
			return err
		}
	}
	return nil
}

// Close seals the active segment (if any), uploads it synchronously so that
// all acknowledged writes are durable before this call returns, then stops
// background goroutines.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	var finalSeg *SegmentWriter
	if w.active != nil {
		if w.active.EntryCount() > 0 {
			if err := w.active.Seal(); err != nil {
				w.log.Errorf("wal: close seal: %v", err)
			} else {
				finalSeg = w.active
			}
		} else {
			w.active.Close()
			os.Remove(w.active.Path()) // empty segment, discard
		}
		w.active = nil
	}
	w.mu.Unlock()

	// Stop background loops first; after wg.Wait() the upload loop has fully
	// exited and nothing else uploads pending segments.
	if w.cancelLoops != nil {
		w.cancelLoops()
	}
	w.wg.Wait()
	// Cancel the upload context AFTER the background loops exit so that any
	// in-flight rotateSyncLocked that is mid-upload can still complete.
	if w.uploadCancel != nil {
		w.uploadCancel()
	}

	if w.uploader == nil {
		return nil
	}

	// Upload the segments still pending, oldest first, then the final
	// segment. All uploads use a fresh context so they are not affected by
	// the already-cancelled bgCtx. Stop at the first failure: the rest stay
	// local and are queued again when the WAL next starts.
	uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer uploadCancel()

	conflicted := make(map[string]bool)
	for _, u := range w.uploadQueue() {
		if err := w.uploadOne(uploadCtx, u, conflicted); err != nil {
			w.log.Errorf("wal: close upload %v", err)
			return err
		}
	}
	if finalSeg != nil {
		objKey := ObjectKey(finalSeg.Term(), finalSeg.FirstRev())
		if err := w.uploader(uploadCtx, finalSeg.Path(), objKey); err != nil {
			w.log.Errorf("wal: close upload final segment: %v", err)
			return err
		}
	}
	return nil
}

// SealAndFlush seals the active segment immediately (blocking) and queues it
// for upload. The next append starts a new segment named after its first
// entry; nextSeq is kept for the WALWriter interface. Used before taking a
// checkpoint.
func (w *WAL) SealAndFlush(nextSeq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil || w.active.EntryCount() == 0 {
		return nil // nothing to flush
	}
	old := w.active
	if err := old.Seal(); err != nil {
		return err
	}
	w.active = nil
	if w.uploader != nil {
		w.queueUpload(ObjectKey(old.Term(), old.FirstRev()), old.Path())
	}
	return nil
}

// LocalSegments returns paths of all local WAL segment files sorted by
// (term, firstRev), useful for startup recovery.
func LocalSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(paths) // lexicographic == chronological given our naming
	return paths, nil
}

// ObjectKey returns the S3 object key for a segment.
func ObjectKey(term uint64, firstRev int64) string {
	return fmt.Sprintf("wal/%010d/%020d", term, firstRev)
}
