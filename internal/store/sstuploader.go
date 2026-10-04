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

	uploadC chan string        // local file paths queued for upload
	waitC   chan chan struct{} // Wait requests to Start's loop
	exited  chan struct{}      // closed once Start's loop has finished
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
		waitC:     make(chan chan struct{}),
		exited:    make(chan struct{}),
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

		select {
		case <-u.exited:
			// Uploader has shut down; WriteWithRegistry's inline fallback
			// will handle any SSTs that end up in the checkpoint.
			return
		default:
		}
		select {
		case u.uploadC <- path:
		default:
			// Channel full: upload synchronously so we never drop a file.
			// Wait does not cover it, which is safe: checkpoint writing
			// uploads any SST missing from the registry itself.
			if err := u.uploadOne(context.Background(), path); err != nil {
				logrus.Warnf("sstuploader: sync upload %q: %v", path, err)
			}
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

// ReconcileVerified is Reconcile for a registry that may be stale: SSTs it
// records as uploaded may have been deleted since. It lists the store's SSTs
// once, forgets the registered ones that are gone, and then uploads every
// local SST not in the registry, which re-uploads those.
//
// A node fills its registry when it opens, whatever its role. A follower
// keeps the SSTs it restored from a checkpoint while the leader's checkpoint
// GC may delete them from the store once no checkpoint references them. A
// follower about to become leader must call this before its first
// checkpoint: trusting the registry, that checkpoint would reference SSTs
// that no longer exist.
//
// If the store cannot be listed, the whole registry is forgotten, so that
// checkpoints upload every SST they reference themselves.
func (u *SSTUploader) ReconcileVerified(ctx context.Context) error {
	keys, err := u.store.List(ctx, "sst/")
	if err != nil {
		u.mu.Lock()
		clear(u.local)
		u.mu.Unlock()
		return fmt.Errorf("sstuploader: list uploaded ssts: %w", err)
	}
	present := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		present[k] = struct{}{}
	}
	u.mu.Lock()
	for name, key := range u.local {
		if _, ok := present[key]; !ok {
			delete(u.local, name)
		}
	}
	u.mu.Unlock()
	return u.Reconcile(ctx)
}

// Start launches the background upload goroutine. Call once; runs until ctx
// is cancelled.
//
// The loop is the only goroutine that touches its WaitGroup, both to Add an
// upload and to Wait for them, which is what a WaitGroup requires: an Add
// from zero must not run concurrently with Wait. Pebble reports new tables
// from its own goroutines at any time, so they only queue paths.
func (u *SSTUploader) Start(ctx context.Context) {
	go func() {
		var uploads sync.WaitGroup
		upload := func(ctx context.Context, path string) {
			uploads.Add(1)
			go func() {
				defer uploads.Done()
				if err := u.uploadOne(ctx, path); err != nil {
					logrus.Warnf("sstuploader: upload %q: %v", path, err)
				}
			}()
		}
		drain := func(ctx context.Context) {
			for {
				select {
				case path := <-u.uploadC:
					upload(ctx, path)
				default:
					return
				}
			}
		}
		for {
			select {
			case path := <-u.uploadC:
				upload(ctx, path)
			case done := <-u.waitC:
				drain(ctx)
				uploads.Wait()
				close(done)
			case <-ctx.Done():
				// Upload what is still queued before exiting, so a Wait during
				// shutdown still covers it. A path queued after this is left to
				// checkpoint writing, which uploads any SST it finds missing.
				drain(context.Background())
				uploads.Wait()
				close(u.exited)
				return
			}
		}
	}()
}

// Wait blocks until every upload queued so far is complete. Called before
// Start, it waits for Start's loop, which then uploads what was queued
// meanwhile; after Start's context is cancelled, it waits for the loop to
// finish its final uploads.
func (u *SSTUploader) Wait() {
	done := make(chan struct{})
	select {
	case u.waitC <- done:
		<-done
	case <-u.exited:
	}
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
