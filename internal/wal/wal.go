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

	uploadC     chan uploadTask
	wg          sync.WaitGroup
	cancelLoops context.CancelFunc // cancels rotationLoop and uploadLoop
}

type uploadTask struct {
	localPath string
	objectKey string
}

// RecoveryStore is the state-machine subset needed for WAL replay.
type RecoveryStore interface {
	Recover(entries []Entry) error
}

// New returns a WAL configured with opts. Call Open before use.
func New(opts ...Option) *WAL {
	w := &WAL{
		segMaxSize: DefaultSegmentMaxSize,
		segMaxAge:  DefaultSegmentMaxAge,
		uploadC:    make(chan uploadTask, 64),
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

// Open opens (or creates) the WAL directory and prepares the active segment.
// Callers must call Start to begin background processing.
func (w *WAL) Open(dir string, term uint64, startRev int64) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dir = dir
	w.term = term
	w.closed = false
	w.uploadC = make(chan uploadTask, 64)
	sw, err := OpenSegmentWriter(dir, term, startRev)
	if err != nil {
		return err
	}
	w.active = sw
	return nil
}

// ReplayLocal replays locally stored WAL segments into db, applying entries
// whose revision is greater than afterRev.
func (w *WAL) ReplayLocal(db RecoveryStore, afterRev int64) error {
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
			if e.Revision > afterRev {
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
	w.wg.Add(2)
	go w.rotationLoop(loopCtx)
	go w.uploadLoop(loopCtx)
}

// Append writes e to the active segment and fsyncs.
// Safe to call concurrently; writes are serialised under the mutex.
func (w *WAL) Append(e *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal: closed")
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
	nextRev := seg.FirstRev() + int64(seg.EntryCount())
	objKey := ObjectKey(seg.Term(), seg.FirstRev())
	localPath := seg.Path()

	uploadErr := w.uploader(w.uploadCtx, localPath, objKey)
	if uploadErr != nil {
		w.log.Errorf("wal: sync upload %q → %q: %v", localPath, objKey, uploadErr)
		if rollbackErr := seg.rollback(rollbackSize, rollbackEntryCount); rollbackErr != nil {
			return fmt.Errorf("wal: sync upload failed and rollback failed: upload: %w; rollback: %v", uploadErr, rollbackErr)
		}
		return uploadErr
	}

	if err := seg.Seal(); err != nil {
		return fmt.Errorf("wal: seal segment after sync upload: %w", err)
	}
	sw, err := OpenSegmentWriter(w.dir, w.term, nextRev)
	if err != nil {
		return fmt.Errorf("wal: open segment after sync rotate: %w", err)
	}
	w.active = sw
	return nil
}

// rotateLocked seals the active segment and opens a fresh one.
// Must be called with w.mu held.
func (w *WAL) rotateLocked() {
	if w.active == nil {
		return
	}
	seg := w.active
	nextRev := seg.FirstRev() + int64(seg.EntryCount())
	if err := seg.Seal(); err != nil {
		// Seal failed; keep the old (unsealed) segment as active so the next
		// Append returns an error rather than panicking on a nil dereference.
		w.log.Errorf("wal: seal segment %q: %v", seg.Path(), err)
		return
	}
	if w.uploader != nil {
		objKey := ObjectKey(seg.Term(), seg.FirstRev())
		select {
		case w.uploadC <- uploadTask{localPath: seg.Path(), objectKey: objKey}:
		default:
			w.log.Warnf("wal: upload queue full, dropping %q (will retry on restart)", seg.Path())
		}
	}
	w.log.Debugf("wal: sealed segment %q (%d entries, %d bytes)", seg.Path(), seg.EntryCount(), seg.Size())
	sw, err := OpenSegmentWriter(w.dir, w.term, nextRev)
	if err != nil {
		// Cannot open the next segment. Keep the sealed segment as active so
		// the next Append call returns a write error rather than panicking.
		w.log.Errorf("wal: open new segment after rotation: %v", err)
		w.active = seg
		return
	}
	w.active = sw
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
				rev := w.active.FirstRev() + int64(w.active.EntryCount()) // approx next rev
				if err := w.active.Seal(); err != nil {
					w.log.Errorf("wal: age-rotate seal: %v", err)
					w.mu.Unlock()
					continue
				}
				old := w.active
				sw, err := OpenSegmentWriter(w.dir, w.term, rev)
				if err != nil {
					// Keep the sealed segment as active so Append returns an
					// error rather than panicking on a nil dereference.
					w.log.Errorf("wal: age-rotate open new segment: %v", err)
					w.active = old
					w.mu.Unlock()
					continue
				}
				if w.uploader != nil {
					objKey := ObjectKey(old.Term(), old.FirstRev())
					select {
					case w.uploadC <- uploadTask{localPath: old.Path(), objectKey: objKey}:
					default:
						w.log.Warnf("wal: upload queue full, segment %q will be retried on restart", old.Path())
					}
				}
				w.active = sw
			}
			w.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

// uploadLoop drains the upload queue.
func (w *WAL) uploadLoop(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case task := <-w.uploadC:
			if w.uploader == nil {
				continue
			}
			if err := w.uploader(ctx, task.localPath, task.objectKey); err != nil {
				if ctx.Err() != nil {
					return
				}
				w.log.Errorf("wal: upload %q → %q: %v", task.localPath, task.objectKey, err)
				// If the local file is gone the segment was already uploaded and
				// cleaned up (or discarded as empty). Retrying cannot help.
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				// Re-queue with a delay so we don't spin on transient S3 errors.
				go func(t uploadTask) {
					select {
					case <-time.After(5 * time.Second):
						select {
						case w.uploadC <- t:
						default:
						}
					case <-ctx.Done():
					}
				}(task)
			}
		case <-ctx.Done():
			return
		}
	}
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
	// exited and we own uploadC exclusively for synchronous draining below.
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

	// Drain any segments that were sealed and queued before Close() was called
	// but not yet uploaded (the upload loop may have exited mid-queue due to
	// context cancellation).  Then upload the final segment.  All uploads use a
	// fresh context so they are not affected by the already-cancelled bgCtx.
	uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer uploadCancel()

	var uploadErr error
	for {
		select {
		case task := <-w.uploadC:
			if err := w.uploader(uploadCtx, task.localPath, task.objectKey); err != nil {
				w.log.Errorf("wal: close drain upload %q: %v", task.localPath, err)
				if uploadErr == nil {
					uploadErr = err
				}
			}
		default:
			goto drained
		}
	}
drained:
	if finalSeg != nil {
		objKey := ObjectKey(finalSeg.Term(), finalSeg.FirstRev())
		if err := w.uploader(uploadCtx, finalSeg.Path(), objKey); err != nil {
			w.log.Errorf("wal: close upload final segment: %v", err)
			if uploadErr == nil {
				uploadErr = err
			}
		}
	}
	return uploadErr
}

// SealAndFlush seals the active segment immediately (blocking) and queues it
// for upload. Used before taking a checkpoint.
func (w *WAL) SealAndFlush(nextRev int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil || w.active.EntryCount() == 0 {
		return nil // nothing to flush
	}
	old := w.active
	if err := old.Seal(); err != nil {
		return err
	}
	sw, err := OpenSegmentWriter(w.dir, w.term, nextRev)
	if err != nil {
		return err
	}
	w.active = sw
	if w.uploader != nil {
		objKey := ObjectKey(old.Term(), old.FirstRev())
		w.uploadC <- uploadTask{localPath: old.Path(), objectKey: objKey}
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
