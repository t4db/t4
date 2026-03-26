package strata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/makhov/strata/internal/checkpoint"
	"github.com/makhov/strata/internal/object"
	istore "github.com/makhov/strata/internal/store"
	"github.com/makhov/strata/internal/wal"
)

// Node is the top-level single-node Strata instance.
//
// Write path:  Put/Delete → WAL.Append (fsync) → store.Apply → notify watchers
// Background:  WAL segments uploaded to S3 when sealed (size or age threshold)
//
//	Periodic checkpoints written to S3
//
// ErrKeyExists is returned by Create when the key already exists.
var ErrKeyExists = errors.New("strata: key already exists")

type Node struct {
	cfg  Config
	term uint64 // current term, read-only after Open

	db  *istore.Store
	wal *wal.WAL

	// mu serialises all writes so CAS operations are safe on single node.
	mu sync.Mutex

	// entriesSinceCheckpoint is incremented on every Apply; reset on checkpoint.
	entriesSinceCheckpoint int64

	cancelBg context.CancelFunc
}

// Open starts a Node using the given Config.
//
// On startup it:
//  1. Downloads the latest checkpoint from object storage (if any)
//  2. Opens (or creates) Pebble in DataDir
//  3. Replays any WAL segments that postdate the checkpoint
//  4. Starts background WAL rotation and upload goroutines
func Open(cfg Config) (*Node, error) {
	cfg.setDefaults()

	pebbleDir := filepath.Join(cfg.DataDir, "db")
	walDir := filepath.Join(cfg.DataDir, "wal")

	var (
		startRev int64
		term     uint64 = 1
	)

	// ── Step 1: restore checkpoint from object storage ───────────────────────
	if cfg.ObjectStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		manifest, err := checkpoint.ReadManifest(ctx, cfg.ObjectStore)
		if err != nil {
			return nil, fmt.Errorf("strata: read manifest: %w", err)
		}
		if manifest != nil {
			logrus.Infof("strata: restoring checkpoint %q (rev=%d)", manifest.CheckpointKey, manifest.Revision)
			if _, err := os.Stat(pebbleDir); errors.Is(err, os.ErrNotExist) {
				t, rev, err := checkpoint.Restore(ctx, cfg.ObjectStore, manifest.CheckpointKey, pebbleDir)
				if err != nil {
					return nil, fmt.Errorf("strata: restore checkpoint: %w", err)
				}
				term = t
				startRev = rev
				logrus.Infof("strata: checkpoint restored (term=%d rev=%d)", term, startRev)
			}
		}
	}

	// ── Step 2: open Pebble ──────────────────────────────────────────────────
	db, err := istore.Open(pebbleDir)
	if err != nil {
		return nil, fmt.Errorf("strata: open store: %w", err)
	}

	// Pebble may have a higher revision from a prior crash-recovery; use it.
	if dbRev := db.CurrentRevision(); dbRev > startRev {
		startRev = dbRev
	}

	// ── Step 3: open WAL and replay local + remote segments ─────────────────
	var uploader wal.Uploader
	if cfg.ObjectStore != nil {
		uploader = makeUploader(cfg.ObjectStore)
	}

	w, err := wal.Open(walDir, term, startRev+1,
		wal.WithUploader(uploader),
		wal.WithSegmentMaxSize(cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(cfg.SegmentMaxAge),
	)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("strata: open wal: %w", err)
	}

	// Replay local segments that postdate startRev.
	if err := replayLocal(db, walDir, startRev); err != nil {
		w.Close()
		db.Close()
		return nil, fmt.Errorf("strata: local WAL replay: %w", err)
	}

	// If object storage is configured, also replay remote segments
	// that postdate the checkpoint.
	if cfg.ObjectStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := replayRemote(ctx, db, cfg.ObjectStore, startRev); err != nil {
			w.Close()
			db.Close()
			return nil, fmt.Errorf("strata: remote WAL replay: %w", err)
		}
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		term:     term,
		db:       db,
		wal:      w,
		cancelBg: bgCancel,
	}

	w.Start(bgCtx)

	if cfg.ObjectStore != nil && cfg.CheckpointInterval > 0 {
		go n.checkpointLoop(bgCtx)
	}

	return n, nil
}

// Close shuts down the node cleanly.
func (n *Node) Close() error {
	n.cancelBg()
	if err := n.wal.Close(); err != nil {
		logrus.Errorf("strata: wal close: %v", err)
	}
	return n.db.Close()
}

// ── Write path ────────────────────────────────────────────────────────────────

// Put creates or updates key with value. Returns the new revision.
func (n *Node) Put(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.putLocked(key, value, lease)
}

func (n *Node) putLocked(key string, value []byte, lease int64) (int64, error) {
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, err
	}
	curRev := n.db.CurrentRevision()
	newRev := curRev + 1

	var op wal.Op
	var createRev, prevRev int64
	if existing == nil {
		op = wal.OpCreate
		createRev = newRev
	} else {
		op = wal.OpUpdate
		createRev = existing.CreateRevision
		prevRev = existing.Revision
	}
	return n.appendAndApply(wal.Entry{
		Revision:       newRev,
		Term:           n.term,
		Op:             op,
		Key:            key,
		Value:          value,
		Lease:          lease,
		CreateRevision: createRev,
		PrevRevision:   prevRev,
	})
}

// Create creates key only if it does not already exist.
// Returns (newRev, nil) on success; (0, ErrKeyExists) if already present.
func (n *Node) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		return 0, ErrKeyExists
	}
	curRev := n.db.CurrentRevision()
	newRev := curRev + 1
	return n.appendAndApply(wal.Entry{
		Revision:       newRev,
		Term:           n.term,
		Op:             wal.OpCreate,
		Key:            key,
		Value:          value,
		Lease:          lease,
		CreateRevision: newRev,
	})
}

// Update updates key only if its current revision matches revision (CAS).
// Returns (newRev, oldKV, true, nil) on match; (currentRev, currentKV, false, nil) on mismatch.
func (n *Node) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, nil, false, err
	}
	curRev := n.db.CurrentRevision()
	if existing == nil || existing.Revision != revision {
		return curRev, toKV(existing), false, nil
	}
	newRev, err := n.appendAndApply(wal.Entry{
		Revision:       curRev + 1,
		Term:           n.term,
		Op:             wal.OpUpdate,
		Key:            key,
		Value:          value,
		Lease:          lease,
		CreateRevision: existing.CreateRevision,
		PrevRevision:   existing.Revision,
	})
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, toKV(existing), true, nil
}

// Delete removes key. Returns the new (tombstone) revision, or 0 if not found.
func (n *Node) Delete(ctx context.Context, key string) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.deleteLocked(key, 0)
}

// DeleteIfRevision deletes key only if its current revision == revision (CAS).
// revision==0 means unconditional delete.
// Returns (newRev, oldKV, true, nil) on success; (currentRev, currentKV, false, nil) on mismatch.
func (n *Node) DeleteIfRevision(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, nil, false, err
	}
	curRev := n.db.CurrentRevision()
	if existing == nil {
		return curRev, nil, false, nil
	}
	if revision != 0 && existing.Revision != revision {
		return curRev, toKV(existing), false, nil
	}
	newRev, err := n.deleteLocked(key, revision)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, toKV(existing), true, nil
}

func (n *Node) deleteLocked(key string, _ int64) (int64, error) {
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, err
	}
	if existing == nil {
		return 0, nil
	}
	curRev := n.db.CurrentRevision()
	return n.appendAndApply(wal.Entry{
		Revision:       curRev + 1,
		Term:           n.term,
		Op:             wal.OpDelete,
		Key:            key,
		CreateRevision: existing.CreateRevision,
		PrevRevision:   existing.Revision,
	})
}

func (n *Node) appendAndApply(e wal.Entry) (int64, error) {
	if err := n.wal.Append(&e); err != nil {
		return 0, fmt.Errorf("strata: wal append: %w", err)
	}
	if err := n.db.Apply([]wal.Entry{e}); err != nil {
		return 0, fmt.Errorf("strata: apply: %w", err)
	}
	atomic.AddInt64(&n.entriesSinceCheckpoint, 1)
	return e.Revision, nil
}

// ── Read path ─────────────────────────────────────────────────────────────────

// Get returns the current value for key, or nil if not found.
func (n *Node) Get(key string) (*KeyValue, error) {
	sv, err := n.db.Get(key)
	if err != nil || sv == nil {
		return nil, err
	}
	return toKV(sv), nil
}

// List returns all live keys with the given prefix.
func (n *Node) List(prefix string) ([]*KeyValue, error) {
	svs, err := n.db.List(prefix)
	if err != nil {
		return nil, err
	}
	out := make([]*KeyValue, len(svs))
	for i, sv := range svs {
		out[i] = toKV(sv)
	}
	return out, nil
}

// Count returns the number of live keys with the given prefix.
func (n *Node) Count(prefix string) (int64, error) {
	return n.db.Count(prefix)
}

// CurrentRevision returns the latest applied revision.
func (n *Node) CurrentRevision() int64 { return n.db.CurrentRevision() }

// CompactRevision returns the oldest available revision.
func (n *Node) CompactRevision() int64 { return n.db.CompactRevision() }

// Watch streams events for keys matching prefix from startRev+1 onwards.
// The returned channel is closed when ctx is cancelled.
func (n *Node) Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error) {
	sch, err := n.db.Watch(ctx, prefix, startRev)
	if err != nil {
		return nil, err
	}
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		for ev := range sch {
			et := EventPut
			if ev.Deleted {
				et = EventDelete
			}
			ne := Event{
				Type: et,
				KV:   toKV(ev.KV),
			}
			if ev.PrevKV != nil {
				ne.PrevKV = toKV(ev.PrevKV)
			}
			select {
			case out <- ne:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Config returns the node's configuration.
func (n *Node) Config() Config { return n.cfg }

// WaitForRevision blocks until CurrentRevision() >= rev or ctx is cancelled.
// In single-node mode this returns immediately since the node is always current.
func (n *Node) WaitForRevision(ctx context.Context, rev int64) error {
	return n.db.WaitForRevision(ctx, rev)
}

// Compact removes log entries at or below revision and advances CompactRevision.
func (n *Node) Compact(ctx context.Context, revision int64) error {
	curRev := n.db.CurrentRevision()
	newRev := curRev + 1
	e := wal.Entry{
		Revision:     newRev,
		Term:         n.term,
		Op:           wal.OpCompact,
		PrevRevision: revision, // repurpose PrevRevision to carry the compact target
	}
	if err := n.wal.Append(&e); err != nil {
		return fmt.Errorf("strata: compact wal append: %w", err)
	}
	return n.db.Apply([]wal.Entry{e})
}

// ── Background checkpoint loop ────────────────────────────────────────────────

func (n *Node) checkpointLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.maybeCheckpoint(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (n *Node) maybeCheckpoint(ctx context.Context) {
	if atomic.LoadInt64(&n.entriesSinceCheckpoint) == 0 {
		return // nothing new since the last checkpoint
	}
	rev := n.db.CurrentRevision()
	if rev == 0 {
		return
	}

	// Seal and flush the current WAL segment before snapshotting.
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		logrus.Errorf("strata: checkpoint seal WAL: %v", err)
		return
	}

	// TODO: pass last uploaded WAL object key once the upload loop exposes it.
	if err := checkpoint.Write(ctx, n.db.Pebble(), n.cfg.ObjectStore, n.term, rev, ""); err != nil {
		logrus.Errorf("strata: write checkpoint rev=%d: %v", rev, err)
		return
	}
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	logrus.Infof("strata: checkpoint written (rev=%d)", rev)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func toKV(sv *istore.KeyValue) *KeyValue {
	if sv == nil {
		return nil
	}
	return &KeyValue{
		Key:            sv.Key,
		Value:          sv.Value,
		Revision:       sv.Revision,
		CreateRevision: sv.CreateRevision,
		PrevRevision:   sv.PrevRevision,
		Lease:          sv.Lease,
	}
}

// makeUploader returns a wal.Uploader that streams a local file to object
// storage and removes it on success.
func makeUploader(obj object.Store) wal.Uploader {
	return func(ctx context.Context, localPath, objectKey string) error {
		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("uploader: open %q: %w", localPath, err)
		}
		defer f.Close()
		if err := obj.Put(ctx, objectKey, f); err != nil {
			return err
		}
		return os.Remove(localPath)
	}
}

func replayLocal(db *istore.Store, walDir string, afterRev int64) error {
	paths, err := wal.LocalSegments(walDir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		sr, closer, err := wal.OpenSegmentFile(path)
		if err != nil {
			return err
		}
		entries, readErr := sr.ReadAll()
		closer()
		if readErr != nil {
			logrus.Warnf("strata: partial local segment %q: %v", path, readErr)
		}
		var applicable []wal.Entry
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

func replayRemote(ctx context.Context, db *istore.Store, obj object.Store, afterRev int64) error {
	keys, err := obj.List(ctx, "wal/")
	if err != nil {
		return err
	}
	for _, key := range keys {
		rc, err := obj.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("replayRemote get %q: %w", key, err)
		}
		sr, err := wal.NewSegmentReader(rc)
		if err != nil {
			rc.Close()
			return fmt.Errorf("replayRemote segment %q: %w", key, err)
		}
		entries, readErr := sr.ReadAll()
		rc.Close()
		if readErr != nil {
			logrus.Warnf("strata: partial remote segment %q: %v", key, readErr)
		}
		var applicable []wal.Entry
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
