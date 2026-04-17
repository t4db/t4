package t4

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// replayPinned replays the specific WAL segments listed in rp, applying
// entries with revision > afterRev. Used during RestorePoint bootstrap.
func replayPinned(ctx context.Context, db *istore.Store, rp *RestorePoint, afterRev int64, log Logger) error {
	for _, seg := range rp.WALSegments {
		rc, err := rp.Store.GetVersioned(ctx, seg.Key, seg.VersionID)
		if err != nil {
			return fmt.Errorf("replayPinned get %q@%s: %w", seg.Key, seg.VersionID, err)
		}
		sr, err := wal.NewSegmentReader(rc)
		if err != nil {
			_ = rc.Close()
			return fmt.Errorf("replayPinned segment %q: %w", seg.Key, err)
		}
		entries, readErr := sr.ReadAll()
		_ = rc.Close()
		if readErr != nil {
			log.Warnf("t4: partial pinned segment %q: %v", seg.Key, readErr)
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

// restoreDBIfBehindCheckpoint checks whether the node's Pebble database is
// behind the latest S3 checkpoint. If so, it performs an in-place restore:
//
//  1. (No lock) Download checkpoint SSTs to a temp directory, retrying if the
//     checkpoint was GC'd between the manifest read and the download.
//  2. (fenceMu.Lock) Close the old Pebble, atomic-rename the temp dir to the
//     canonical db/ path, open the new Pebble. fenceMu blocks all concurrent
//     read/write handlers for the brief swap window.
//
// On success n.db and n.term are updated and the function returns true.
// Returns (false, nil) when no restore was needed (already at or above
// the latest checkpoint revision).
func (n *Node) restoreDBIfBehindCheckpoint(ctx context.Context) (bool, error) {
	manifest, err := n.cp.ReadManifest(ctx, n.cfg.ObjectStore)
	if err != nil {
		return false, fmt.Errorf("read manifest: %w", err)
	}
	if manifest == nil || manifest.Revision <= n.db.Load().CurrentRevision() {
		return false, nil // already at or above checkpoint
	}
	n.log.Infof("t4: node at rev=%d is behind checkpoint rev=%d — restoring in place",
		n.db.Load().CurrentRevision(), manifest.Revision)

	pebbleDir := filepath.Join(n.cfg.DataDir, "db")
	tmpDir := pebbleDir + ".resync"

	// ── Phase 1: restore checkpoint SSTs to a temp dir (no lock held) ───────
	var newTerm uint64
	for attempt := 0; ; attempt++ {
		_ = os.RemoveAll(tmpDir)
		t, _, rerr := n.cp.Restore(ctx, n.cfg.ObjectStore, manifest.CheckpointKey, tmpDir)
		if rerr == nil {
			newTerm = t
			break
		}
		if !errors.Is(rerr, object.ErrNotFound) || attempt >= 4 {
			_ = os.RemoveAll(tmpDir)
			return false, fmt.Errorf("restore checkpoint: %w", rerr)
		}
		// Checkpoint was GC'd between manifest read and restore — re-read
		// the manifest so we target whatever the leader most recently wrote.
		n.log.Warnf("t4: checkpoint %q not found during restore, re-reading manifest (attempt %d/5)",
			manifest.CheckpointKey, attempt+1)
		time.Sleep(500 * time.Millisecond)
		manifest, err = n.cp.ReadManifest(ctx, n.cfg.ObjectStore)
		if err != nil {
			_ = os.RemoveAll(tmpDir)
			return false, fmt.Errorf("re-read manifest: %w", err)
		}
		if manifest == nil {
			_ = os.RemoveAll(tmpDir)
			return false, fmt.Errorf("manifest disappeared during restore")
		}
	}

	// ── Phase 2: swap Pebble directories under fenceMu.Lock + readMu.Lock ───
	// fenceMu.Lock drains in-flight writes (which hold fenceMu.RLock).
	//
	// We first signal-close the old store before acquiring readMu.Lock.
	// SignalClose unblocks any goroutine blocked in WaitForRevision
	// (those goroutines hold readMu.RLock while waiting). If we acquired
	// readMu.Lock first, WaitForRevision callers would never release their
	// RLocks → deadlock.
	//
	// After SignalClose, readMu.Lock drains the remaining in-flight reads
	// (Get/List and any WaitForRevision callers that were unblocked by
	// SignalClose). We then close the old store while still holding readMu,
	// so no new read can load the old *Store after Pebble has been closed.
	// We keep readMu until n.db.Store(newDB) so no read can observe the
	// closed store pointer during the on-disk swap.
	//
	// Lock order: fenceMu → readMu (no other code path acquires them in the
	// reverse order, so no deadlock is possible here).
	n.fenceMu.Lock()
	oldDB := n.db.Load()
	oldDB.SignalClose() // unblocks WaitForRevision waiters before we take readMu.Lock
	n.readMu.Lock()
	if rerr := oldDB.Close(); rerr != nil {
		n.readMu.Unlock()
		n.fenceMu.Unlock()
		_ = os.RemoveAll(tmpDir)
		return false, fmt.Errorf("close old pebble before restore swap: %w", rerr)
	}
	if rerr := os.RemoveAll(pebbleDir); rerr != nil {
		n.readMu.Unlock()
		n.fenceMu.Unlock()
		_ = os.RemoveAll(tmpDir)
		return false, fmt.Errorf("remove old pebble dir: %w", rerr)
	}
	if rerr := os.Rename(tmpDir, pebbleDir); rerr != nil {
		n.readMu.Unlock()
		n.fenceMu.Unlock()
		_ = os.RemoveAll(tmpDir)
		return false, fmt.Errorf("rename resync dir: %w", rerr)
	}
	newDB, rerr := istore.Open(pebbleDir, n.log)
	if rerr != nil {
		n.readMu.Unlock()
		n.fenceMu.Unlock()
		return false, fmt.Errorf("open new pebble after restore: %w", rerr)
	}
	n.db.Store(newDB)
	n.readMu.Unlock()
	n.term = newTerm
	n.fenceMu.Unlock()
	return true, nil
}

func replayRemote(ctx context.Context, db *istore.Store, obj object.Store, afterRev int64, log Logger) error {
	keys, err := obj.List(ctx, "wal/")
	if err != nil {
		return err
	}

	// Build per-term cutoffs to handle leader-change conflicts.
	//
	// When a new leader takes over at revision X, it starts writing a new term
	// from revision X+1. The old leader may have written S3 segments that cover
	// some of those same revisions (X+1, X+2, …). Applying both the old-term
	// and new-term entries at the same revision would corrupt the index.
	//
	// cutoff[term] is the minimum firstRev among all segments with term > term.
	// Old-term entries at revision >= cutoff are superseded by the new term and
	// must be skipped. For the highest term present, cutoff = math.MaxInt64 (no
	// upper bound — apply all entries).
	cutoff := walTermCutoffs(keys)

	var all []wal.Entry
	for _, key := range keys {
		term, _ := parseWALKey(key)
		termCutoff := cutoff[term]

		rc, err := obj.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("replayRemote get %q: %w", key, err)
		}
		sr, err := wal.NewSegmentReader(rc)
		if err != nil {
			_ = rc.Close()
			return fmt.Errorf("replayRemote segment %q: %w", key, err)
		}
		entries, readErr := sr.ReadAll()
		_ = rc.Close()
		if readErr != nil {
			log.Warnf("t4: partial remote segment %q: %v", key, readErr)
		}
		for _, e := range entries {
			if e.Revision <= afterRev {
				continue // already covered by checkpoint / local WAL
			}
			if e.Revision >= termCutoff {
				continue // superseded by a higher-term entry at this revision
			}
			all = append(all, *e)
		}
	}

	if len(all) == 0 {
		return nil
	}

	// Ensure deterministic order and resolve same-revision duplicates by
	// keeping the highest-term entry at each revision.
	sort.Slice(all, func(i, j int) bool {
		if all[i].Revision != all[j].Revision {
			return all[i].Revision < all[j].Revision
		}
		return all[i].Term < all[j].Term
	})
	merged := make([]wal.Entry, 0, len(all))
	for _, e := range all {
		if n := len(merged); n > 0 && merged[n-1].Revision == e.Revision {
			if e.Term >= merged[n-1].Term {
				merged[n-1] = e
			}
			continue
		}
		merged = append(merged, e)
	}

	// Fail closed on holes: a missing revision means we likely raced WAL GC
	// during bootstrap/resync and must restore from a fresher checkpoint.
	expected := afterRev + 1
	for _, e := range merged {
		if e.Revision != expected {
			return fmt.Errorf("replayRemote: missing revision(s): expected %d got %d", expected, e.Revision)
		}
		expected++
	}

	if err := db.Recover(merged); err != nil {
		return err
	}
	return nil
}

// walTermCutoffs returns a map from term → the minimum firstRev of any
// segment whose term is strictly greater. Entries from a given term at
// revisions >= cutoff are superseded by the newer term and should be skipped.
// The highest term maps to math.MaxInt64 (no cutoff).
func walTermCutoffs(keys []string) map[uint64]int64 {
	// Collect the minimum firstRev for each term.
	minFirstRev := map[uint64]int64{}
	for _, key := range keys {
		term, firstRev := parseWALKey(key)
		if existing, ok := minFirstRev[term]; !ok || firstRev < existing {
			minFirstRev[term] = firstRev
		}
	}
	cutoff := make(map[uint64]int64, len(minFirstRev))
	for term := range minFirstRev {
		c := int64(math.MaxInt64)
		for t, fr := range minFirstRev {
			if t > term && fr < c {
				c = fr
			}
		}
		cutoff[term] = c
	}
	return cutoff
}

// parseWALKey extracts term and firstRev from a WAL object key of the form
// "wal/{term:010d}/{firstRev:020d}".
func parseWALKey(key string) (term uint64, firstRev int64) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 {
		return 0, 0
	}
	term64, _ := strconv.ParseUint(strings.TrimLeft(parts[1], "0"), 10, 64)
	rev, _ := strconv.ParseInt(strings.TrimLeft(parts[2], "0"), 10, 64)
	return term64, rev
}
