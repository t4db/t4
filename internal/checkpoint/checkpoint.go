// Package checkpoint handles creating, writing, and restoring Pebble snapshots
// to/from object storage.
package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/makhov/strata/pkg/object"
)

// Manifest is stored at "manifest/latest" in object storage.
// It points to the latest checkpoint so startup only needs one GET.
type Manifest struct {
	CheckpointKey string `json:"checkpoint_key"`
	Revision      int64  `json:"revision"`
	Term          uint64 `json:"term"`
	// LastWALKey is the object key of the last fully uploaded WAL segment
	// whose last entry has revision <= Revision. Used to bound WAL replay.
	LastWALKey string `json:"last_wal_key,omitempty"`
}

// ManifestKey is the fixed object storage key for the manifest.
const ManifestKey = "manifest/latest"

// CheckpointIndex is the per-checkpoint manifest stored at
// "checkpoint/{term}/{rev}/manifest.json".
type CheckpointIndex struct {
	Term     uint64 `json:"term"`
	Revision int64  `json:"revision"`
	// SSTFiles are full object keys ("sst/{hash16}/{name}") of SST files
	// stored in this store.
	SSTFiles []string `json:"sst_files"`
	// AncestorSSTFiles are full object keys ("sst/{hash16}/{name}") of SST
	// files stored in the ancestor (source) store. Only set for branch nodes.
	AncestorSSTFiles []string `json:"ancestor_sst_files,omitempty"`
	// PebbleMeta are Pebble metadata filenames stored alongside the index
	// at "checkpoint/{term}/{rev}/{name}".
	PebbleMeta []string `json:"pebble_meta"`
}

// CheckpointKey returns the directory prefix for a checkpoint:
// "checkpoint/{term}/{rev}". The full index key is CheckpointIndexKey.
func CheckpointKey(term uint64, revision int64) string {
	return fmt.Sprintf("checkpoint/%010d/%020d", term, revision)
}

// CheckpointIndexKey returns the checkpoint index object key.
func CheckpointIndexKey(term uint64, revision int64) string {
	return CheckpointKey(term, revision) + "/manifest.json"
}

// contentSSTKey returns a content-addressed object key for an SST file:
// "sst/{first16hexOfSHA256}/{name}". Same content always maps to the same key,
// so deduplication is safe across DB instances that may share SST filenames.
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
	hashPrefix := hex.EncodeToString(h.Sum(nil)[:8]) // 16 hex chars
	return "sst/" + hashPrefix + "/" + name, nil
}

// ReadManifest reads and parses the manifest from object storage.
// Returns nil, nil if no manifest exists yet.
func ReadManifest(ctx context.Context, store object.Store) (*Manifest, error) {
	rc, err := store.Get(ctx, ManifestKey)
	if err == object.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read manifest: %w", err)
	}
	defer rc.Close()
	var m Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, fmt.Errorf("checkpoint: decode manifest: %w", err)
	}
	return &m, nil
}

// WriteManifest writes m to object storage.
func WriteManifest(ctx context.Context, store object.Store, m *Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := store.Put(ctx, ManifestKey, bytes.NewReader(b)); err != nil {
		return fmt.Errorf("checkpoint: write manifest: %w", err)
	}
	return nil
}

// Write creates a Pebble checkpoint and uploads it: individual SST files at
// "sst/{hash16}/{name}" (skipping already-uploaded ones) and Pebble metadata
// files at "checkpoint/{term}/{rev}/{name}", with a
// "checkpoint/{term}/{rev}/manifest.json" index. It then updates manifest/latest.
//
// ancestorStore, if non-nil, is the source node's object store (for branch
// nodes). SST files already present there are recorded as AncestorSSTFiles
// and not re-uploaded to store.
func Write(ctx context.Context, db *pebble.DB, store object.Store, term uint64, revision int64, lastWALKey string, ancestorStore object.Store) error {
	tmpDir, err := os.MkdirTemp("", "strata-checkpoint-*")
	if err != nil {
		return fmt.Errorf("checkpoint: mktemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	cpDir := filepath.Join(tmpDir, "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		return fmt.Errorf("checkpoint: pebble checkpoint: %w", err)
	}

	localSSTs, err := listSSTSet(ctx, store)
	if err != nil {
		return fmt.Errorf("checkpoint: list local ssts: %w", err)
	}
	var ancestorSSTs map[string]struct{}
	if ancestorStore != nil {
		ancestorSSTs, err = listSSTSet(ctx, ancestorStore)
		if err != nil {
			return fmt.Errorf("checkpoint: list ancestor ssts: %w", err)
		}
	}

	var sstFiles, ancestorSSTFiles, metaFiles []string
	err = filepath.Walk(cpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		name := filepath.Base(path)
		if strings.HasSuffix(name, ".sst") {
			sstKey, err := contentSSTKey(path, name)
			if err != nil {
				return err
			}
			if _, ok := localSSTs[sstKey]; ok {
				sstFiles = append(sstFiles, sstKey)
				return nil // already uploaded (same content)
			}
			if _, ok := ancestorSSTs[sstKey]; ok {
				ancestorSSTFiles = append(ancestorSSTFiles, sstKey)
				return nil // in ancestor store, no upload needed
			}
			sstFiles = append(sstFiles, sstKey)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			if err := store.Put(ctx, sstKey, f); err != nil {
				return fmt.Errorf("upload sst %q: %w", sstKey, err)
			}
		} else {
			metaFiles = append(metaFiles, name)
			metaKey := fmt.Sprintf("checkpoint/%010d/%020d/%s", term, revision, name)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			if err := store.Put(ctx, metaKey, f); err != nil {
				return fmt.Errorf("upload meta %q: %w", name, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("checkpoint: walk: %w", err)
	}

	indexKey := CheckpointIndexKey(term, revision)
	idx := &CheckpointIndex{
		Term:             term,
		Revision:         revision,
		SSTFiles:         sstFiles,
		AncestorSSTFiles: ancestorSSTFiles,
		PebbleMeta:       metaFiles,
	}
	b, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	if err := store.Put(ctx, indexKey, bytes.NewReader(b)); err != nil {
		return fmt.Errorf("checkpoint: upload index: %w", err)
	}

	m := &Manifest{
		CheckpointKey: indexKey,
		Revision:      revision,
		Term:          term,
		LastWALKey:    lastWALKey,
	}
	return WriteManifest(ctx, store, m)
}

// listSSTSet returns the set of full object keys in the store's "sst/" prefix.
func listSSTSet(ctx context.Context, store object.Store) (map[string]struct{}, error) {
	keys, err := store.List(ctx, "sst/")
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return set, nil
}

// Restore downloads a checkpoint and restores it to targetDir (which must not
// exist). Returns the term and revision encoded in the checkpoint.
func Restore(ctx context.Context, store object.Store, objKey, targetDir string) (term uint64, revision int64, err error) {
	return restoreFromIndex(ctx, store, nil, objKey, targetDir)
}

// RestoreBranch restores a checkpoint that may reference SST files in a
// separate ancestorStore. Used by branch nodes on first boot via BranchPoint.
func RestoreBranch(ctx context.Context, store object.Store, ancestorStore object.Store, objKey, targetDir string) (term uint64, revision int64, err error) {
	return restoreFromIndex(ctx, store, ancestorStore, objKey, targetDir)
}

// restoreFromIndex restores a v2 checkpoint from its CheckpointIndex.
// ancestorStore is used for AncestorSSTFiles; if nil, store is used for all.
func restoreFromIndex(ctx context.Context, store object.Store, ancestorStore object.Store, indexKey, targetDir string) (uint64, int64, error) {
	idx, err := readCheckpointIndex(ctx, store, indexKey)
	if err != nil {
		return 0, 0, fmt.Errorf("checkpoint: read index %q: %w", indexKey, err)
	}
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return 0, 0, err
	}

	for _, sstKey := range idx.SSTFiles {
		name := filepath.Base(sstKey)
		if err := downloadFile(ctx, store, sstKey, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download sst %q: %w", sstKey, err)
		}
	}

	anc := store
	if ancestorStore != nil {
		anc = ancestorStore
	}
	for _, sstKey := range idx.AncestorSSTFiles {
		name := filepath.Base(sstKey)
		if err := downloadFile(ctx, anc, sstKey, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download ancestor sst %q: %w", sstKey, err)
		}
	}

	metaPrefix := strings.TrimSuffix(indexKey, "manifest.json")
	for _, name := range idx.PebbleMeta {
		if err := downloadFile(ctx, store, metaPrefix+name, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download meta %q: %w", name, err)
		}
	}
	return idx.Term, idx.Revision, nil
}

// RestoreVersioned downloads a pinned version of a checkpoint index and
// restores it to targetDir. SSTs and pebble meta are fetched from the live
// store (content-addressed SSTs are immutable; meta files remain live as long
// as they're within the keep window). Requires S3 versioning on the store.
func RestoreVersioned(ctx context.Context, store object.VersionedStore, objKey, versionID, targetDir string) (term uint64, revision int64, err error) {
	rc, err := store.GetVersioned(ctx, objKey, versionID)
	if err != nil {
		return 0, 0, fmt.Errorf("checkpoint: download versioned %q@%s: %w", objKey, versionID, err)
	}
	var idx CheckpointIndex
	decErr := json.NewDecoder(rc).Decode(&idx)
	rc.Close()
	if decErr != nil {
		return 0, 0, fmt.Errorf("checkpoint: decode versioned index %q: %w", objKey, decErr)
	}
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return 0, 0, err
	}
	for _, sstKey := range idx.SSTFiles {
		name := filepath.Base(sstKey)
		if err := downloadFile(ctx, store, sstKey, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download versioned sst %q: %w", sstKey, err)
		}
	}
	metaPrefix := strings.TrimSuffix(objKey, "manifest.json")
	for _, name := range idx.PebbleMeta {
		if err := downloadFile(ctx, store, metaPrefix+name, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download versioned meta %q: %w", name, err)
		}
	}
	return idx.Term, idx.Revision, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func readCheckpointIndex(ctx context.Context, store object.Store, key string) (*CheckpointIndex, error) {
	rc, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var idx CheckpointIndex
	decErr := json.NewDecoder(rc).Decode(&idx)
	rc.Close()
	if decErr != nil {
		return nil, fmt.Errorf("decode checkpoint index %q: %w", key, decErr)
	}
	return &idx, nil
}

func downloadFile(ctx context.Context, store object.Store, key, dest string) error {
	rc, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	defer rc.Close()
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, rc)
	return err
}

// ── ListRemote ────────────────────────────────────────────────────────────────

// ListRemote returns the checkpoint index key for each checkpoint in object
// storage, sorted lexicographically (== chronologically).
func ListRemote(ctx context.Context, store object.Store) ([]string, error) {
	keys, err := store.List(ctx, "checkpoint/")
	if err != nil {
		return nil, err
	}
	var result []string
	for _, k := range keys {
		if strings.HasSuffix(k, "/manifest.json") {
			result = append(result, k)
		}
	}
	sort.Strings(result)
	return result, nil
}

// ── GC ────────────────────────────────────────────────────────────────────────

// GCCheckpoints deletes old checkpoint archives from object storage, keeping
// only the most recent `keep` checkpoints. Returns the number deleted.
//
// The checkpoint currently referenced by manifest/latest is always preserved
// even if it would otherwise fall outside the `keep` window.
func GCCheckpoints(ctx context.Context, store object.Store, keep int) (int, error) {
	if keep < 1 {
		keep = 1
	}

	manifest, err := ReadManifest(ctx, store)
	if err != nil {
		return 0, fmt.Errorf("checkpoint gc: read manifest: %w", err)
	}

	keys, err := ListRemote(ctx, store)
	if err != nil {
		return 0, fmt.Errorf("checkpoint gc: list: %w", err)
	}
	if len(keys) <= keep {
		return 0, nil
	}
	toDelete := keys[:len(keys)-keep]
	var deleted int
	for _, k := range toDelete {
		if manifest != nil && k == manifest.CheckpointKey {
			continue
		}
		if err := deleteCheckpoint(ctx, store, k); err != nil {
			return deleted, fmt.Errorf("checkpoint gc: delete %q: %w", k, err)
		}
		deleted++
	}
	return deleted, nil
}

// deleteCheckpoint deletes all objects under "checkpoint/{term}/{rev}/".
func deleteCheckpoint(ctx context.Context, store object.Store, key string) error {
	prefix := key[:strings.LastIndex(key, "/")+1]
	subkeys, err := store.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, sk := range subkeys {
		if err := store.Delete(ctx, sk); err != nil {
			return err
		}
	}
	return nil
}

// GCOrphanSSTs deletes SST files from "sst/" that are not referenced by any
// live checkpoint index or branch registry entry. This is phase 2 of GC and
// should be called after GCCheckpoints. Returns the number of SST files deleted.
func GCOrphanSSTs(ctx context.Context, store object.Store) (int, error) {
	referenced, err := collectReferencedSSTs(ctx, store)
	if err != nil {
		return 0, fmt.Errorf("checkpoint gc ssts: collect refs: %w", err)
	}
	allSSTs, err := store.List(ctx, "sst/")
	if err != nil {
		return 0, fmt.Errorf("checkpoint gc ssts: list: %w", err)
	}
	var deleted int
	for _, k := range allSSTs {
		if _, ok := referenced[k]; !ok {
			if err := store.Delete(ctx, k); err != nil {
				return deleted, fmt.Errorf("checkpoint gc ssts: delete %q: %w", k, err)
			}
			deleted++
		}
	}
	return deleted, nil
}

// collectReferencedSSTs returns the union of all SST keys referenced by live
// checkpoint indexes and by branch registry entries in this store.
func collectReferencedSSTs(ctx context.Context, store object.Store) (map[string]struct{}, error) {
	referenced := make(map[string]struct{})

	cpKeys, err := ListRemote(ctx, store)
	if err != nil {
		return nil, err
	}
	for _, k := range cpKeys {
		idx, err := readCheckpointIndex(ctx, store, k)
		if err != nil {
			return nil, err
		}
		for _, sstKey := range idx.SSTFiles {
			referenced[sstKey] = struct{}{}
		}
	}

	branches, err := ReadBranchEntries(ctx, store)
	if err != nil {
		return nil, err
	}
	for _, entry := range branches {
		if entry.AncestorCheckpointKey == "" {
			continue
		}
		idx, err := readCheckpointIndex(ctx, store, entry.AncestorCheckpointKey)
		if err == object.ErrNotFound {
			continue // ancestor checkpoint already GC'd; branch must have its own
		}
		if err != nil {
			return nil, err
		}
		for _, sstKey := range idx.SSTFiles {
			referenced[sstKey] = struct{}{}
		}
	}
	return referenced, nil
}
