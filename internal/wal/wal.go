package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultSegmentMaxSize = 50 << 20 // 50 MB
	DefaultSegmentMaxAge  = 10 * time.Second
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

	mu     sync.Mutex
	active *SegmentWriter
	closed bool

	uploadC     chan uploadTask
	wg          sync.WaitGroup
	cancelLoops context.CancelFunc // cancels rotationLoop and uploadLoop
}

type uploadTask struct {
	localPath string
	objectKey string
}

// Open opens (or creates) the WAL directory and returns a ready WAL.
// Callers must call Start to begin background processing.
func Open(dir string, term uint64, startRev int64, opts ...Option) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}
	w := &WAL{
		dir:        dir,
		term:       term,
		segMaxSize: DefaultSegmentMaxSize,
		segMaxAge:  DefaultSegmentMaxAge,
		uploadC:    make(chan uploadTask, 64),
	}
	for _, o := range opts {
		o(w)
	}
	sw, err := OpenSegmentWriter(dir, term, startRev)
	if err != nil {
		return nil, err
	}
	w.active = sw
	return w, nil
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

// Start launches background goroutines. Must be called before Append.
func (w *WAL) Start(ctx context.Context) {
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

// rotateLocked seals the active segment and opens a fresh one.
// Must be called with w.mu held.
func (w *WAL) rotateLocked() {
	if w.active == nil {
		return
	}
	seg := w.active
	w.active = nil
	nextRev := seg.FirstRev() + int64(seg.EntryCount())
	if err := seg.Seal(); err != nil {
		logrus.Errorf("wal: seal segment %q: %v", seg.Path(), err)
		return
	}
	if w.uploader != nil {
		objKey := ObjectKey(seg.Term(), seg.FirstRev())
		select {
		case w.uploadC <- uploadTask{localPath: seg.Path(), objectKey: objKey}:
		default:
			logrus.Warnf("wal: upload queue full, dropping %q (will retry on restart)", seg.Path())
		}
	}
	logrus.Debugf("wal: sealed segment %q (%d entries, %d bytes)", seg.Path(), seg.EntryCount(), seg.Size())
	sw, err := OpenSegmentWriter(w.dir, w.term, nextRev)
	if err != nil {
		logrus.Errorf("wal: open new segment after rotation: %v", err)
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
					logrus.Errorf("wal: age-rotate seal: %v", err)
					w.mu.Unlock()
					continue
				}
				old := w.active
				sw, err := OpenSegmentWriter(w.dir, w.term, rev)
				if err != nil {
					logrus.Errorf("wal: age-rotate open new segment: %v", err)
					w.active = nil
					w.mu.Unlock()
					continue
				}
				if w.uploader != nil {
					objKey := ObjectKey(old.Term(), old.FirstRev())
					select {
					case w.uploadC <- uploadTask{localPath: old.Path(), objectKey: objKey}:
					default:
						logrus.Warnf("wal: upload queue full, segment %q will be retried on restart", old.Path())
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
				logrus.Errorf("wal: upload %q → %q: %v", task.localPath, task.objectKey, err)
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

// Close seals the active segment (if any) and waits for background goroutines.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	if w.active != nil {
		if w.active.EntryCount() > 0 {
			if err := w.active.Seal(); err != nil {
				logrus.Errorf("wal: close seal: %v", err)
			}
		} else {
			w.active.Close()
			os.Remove(w.active.Path()) // empty segment, discard
		}
		w.active = nil
	}
	w.mu.Unlock()
	if w.cancelLoops != nil {
		w.cancelLoops()
	}
	w.wg.Wait()
	return nil
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
