package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/sirupsen/logrus"

	"github.com/t4db/t4/pkg/object"
)

// SSTUploader streams Pebble SST files to object storage as they are created,
// keeping an in-memory registry of filename → S3 key for all live SSTs.
//
// This decouples SST upload from checkpoint creation: checkpoints only write
// a JSON index (already-uploaded SST keys), so there is no upload burst at
// checkpoint time.
//
// Usage:
//  1. Create before opening Pebble.
//  2. Pass EventListener() to pebble.Options so new SSTs are tracked.
//  3. Call Reconcile after Pebble opens to upload any SSTs already on disk.
//  4. Call Wait before writing a checkpoint so all pending uploads complete.
//  5. Call Registry/InheritedRegistry to get the SST maps for checkpoint.Write.
type SSTUploader struct {
	store     object.Store
	pebbleDir string

	mu        sync.RWMutex
	local     map[string]string // filename → "sst/{hash16}/{name}" in this store
	inherited map[string]string // filename → s3 key in ancestor store

	pending sync.WaitGroup
	uploadC chan string // local file paths queued for upload

	// stopMu protects stopped. EventListener acquires a read-lock to gate
	// pending.Add; Start sets stopped under the write-lock before draining
	// the channel. This closes the race where Start exits while EventListener
	// is mid-enqueue.
	stopMu  sync.RWMutex
	stopped bool
}

// NewSSTUploader creates an uploader that will upload SSTs to store.
// pebbleDir is the local Pebble data directory (used for reconciliation).
func NewSSTUploader(store object.Store, pebbleDir string) *SSTUploader {
	return &SSTUploader{
		store:     store,
		pebbleDir: pebbleDir,
		local:     make(map[string]string),
		inherited: make(map[string]string),
		uploadC:   make(chan string, 512),
	}
}

// EventListener returns a pebble.EventListener that queues new SST files for
// upload after they are fully written. Pass this to pebble.Options.EventListener
// before opening Pebble.
//
// NOTE: TableCreated fires when Pebble creates the file (still empty). We must
// use FlushEnd and CompactionEnd instead, which fire after all data is written.
func (u *SSTUploader) EventListener() pebble.EventListener {
	queueTable := func(fileNum fmt.Stringer) {
		path := filepath.Join(u.pebbleDir, fileNum.String()+".sst")

		// Hold stopMu read-lock while incrementing pending and sending to
		// the channel. Start() acquires the write-lock before draining,
		// so we can never have a pending.Add that nobody will Done().
		u.stopMu.RLock()
		if u.stopped {
			u.stopMu.RUnlock()
			// Uploader is shutting down; WriteWithRegistry's inline fallback
			// will handle any SSTs that end up in the checkpoint.
			return
		}
		u.pending.Add(1)
		u.stopMu.RUnlock()

		select {
		case u.uploadC <- path:
			// Start goroutine will call pending.Done() after upload.
		default:
			// Channel full: upload synchronously so we never drop a file.
			if err := u.uploadOne(context.Background(), path); err != nil {
				logrus.Warnf("sstuploader: sync upload %q: %v", path, err)
			}
			u.pending.Done()
		}
	}
	return pebble.EventListener{
		FlushEnd: func(info pebble.FlushInfo) {
			if info.Err != nil {
				return
			}
			for i := range info.Output {
				queueTable(info.Output[i].FileNum)
			}
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			if info.Err != nil {
				return
			}
			for i := range info.Output.Tables {
				queueTable(info.Output.Tables[i].FileNum)
			}
		},
	}
}

// PebbleOption returns a PebbleOption that installs this uploader's
// EventListener on pebble.Options. Pass the result to store.Open.
func (u *SSTUploader) PebbleOption() PebbleOption {
	listener := u.EventListener()
	return func(o *pebble.Options) {
		o.EventListener = &listener
	}
}

// SetInherited records a set of SST filenames that came from an ancestor
// store restore. These are referenced in checkpoints as AncestorSSTFiles and
// are not uploaded to the local store.
func (u *SSTUploader) SetInherited(filenames map[string]string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for name, key := range filenames {
		u.inherited[name] = key
	}
}

// Reconcile walks pebbleDir and uploads any SST files not already in the
// registry. Must be called after Pebble opens and before serving requests.
func (u *SSTUploader) Reconcile(ctx context.Context) error {
	entries, err := os.ReadDir(u.pebbleDir)
	if err != nil {
		return fmt.Errorf("sstuploader: readdir %q: %w", u.pebbleDir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		// Skip SST files that are still being written by Pebble (TableCreated
		// fires when the file is created but still empty; data is written before
		// FlushEnd/CompactionEnd). Uploading a 0-byte file here would poison the
		// local registry and prevent the correct upload triggered by those events.
		if info, err := e.Info(); err != nil || info.Size() == 0 {
			continue
		}
		u.mu.RLock()
		_, inLocal := u.local[e.Name()]
		_, inInherited := u.inherited[e.Name()]
		u.mu.RUnlock()
		if inLocal || inInherited {
			continue
		}
		path := filepath.Join(u.pebbleDir, e.Name())
		if err := u.uploadOne(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

// Start launches the background upload goroutine. Call once; runs until ctx
// is cancelled.
func (u *SSTUploader) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case path := <-u.uploadC:
				go func(p string) {
					defer u.pending.Done()
					if err := u.uploadOne(ctx, p); err != nil {
						logrus.Warnf("sstuploader: upload %q: %v", p, err)
					}
				}(path)
			case <-ctx.Done():
				// Signal EventListener to stop incrementing pending BEFORE we
				// drain the channel. The write-lock ensures no EventListener
				// call is mid-way through pending.Add when we start draining.
				u.stopMu.Lock()
				u.stopped = true
				u.stopMu.Unlock()

				// Drain remaining items (added before we set stopped).
				for {
					select {
					case path := <-u.uploadC:
						go func(p string) {
							defer u.pending.Done()
							u.uploadOne(context.Background(), p) //nolint:errcheck
						}(path)
					default:
						return
					}
				}
			}
		}
	}()
}

// Wait blocks until all in-flight and queued uploads complete. Safe to call
// even after Start's context is cancelled — the start goroutine drains the
// channel on exit so pending.Wait() will always unblock.
func (u *SSTUploader) Wait() {
	u.pending.Wait()
}

// Registry returns a snapshot of filename → s3Key for all SSTs uploaded to
// the local store. Safe to call concurrently.
func (u *SSTUploader) Registry() map[string]string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	out := make(map[string]string, len(u.local))
	for k, v := range u.local {
		out[k] = v
	}
	return out
}

// InheritedRegistry returns a snapshot of filename → s3Key for inherited
// (ancestor) SSTs. Safe to call concurrently.
func (u *SSTUploader) InheritedRegistry() map[string]string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	out := make(map[string]string, len(u.inherited))
	for k, v := range u.inherited {
		out[k] = v
	}
	return out
}

// uploadOne uploads path to object storage and registers it. Idempotent: if
// the file is already in the local registry, returns immediately.
func (u *SSTUploader) uploadOne(ctx context.Context, path string) error {
	name := filepath.Base(path)

	u.mu.RLock()
	_, exists := u.local[name]
	u.mu.RUnlock()
	if exists {
		return nil
	}

	s3Key, err := contentSSTKey(path, name)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // compacted away before we could upload; safe to skip
		}
		return fmt.Errorf("sstuploader: hash %q: %w", name, err)
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // same: already compacted
		}
		return fmt.Errorf("sstuploader: open %q: %w", name, err)
	}
	defer f.Close()

	if err := u.store.Put(ctx, s3Key, f); err != nil {
		return fmt.Errorf("sstuploader: put %q: %w", s3Key, err)
	}

	u.mu.Lock()
	u.local[name] = s3Key
	u.mu.Unlock()
	return nil
}

// contentSSTKey returns "sst/{first16hexOfSHA256}/{name}" for the file at path.
func contentSSTKey(path, name string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sst/" + hex.EncodeToString(h.Sum(nil)[:8]) + "/" + name, nil
}
