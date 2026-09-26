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
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/t4db/t4/pkg/object"
)

// checkpointLogger is the minimal leveled logging interface used by the
// checkpoint package.
type checkpointLogger interface {
	Warnf(format string, args ...interface{})
}

// stdlibCheckpointLogger is used when New is called with nil.
type stdlibCheckpointLogger struct{}

func (stdlibCheckpointLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

// Manager handles checkpoint creation, reading, and restoration.
// Create one with New and store it on the Node so each node has its own
// logger — no global state.
type Manager struct {
	log checkpointLogger
}

// New creates a Manager.  If log is nil a stdlib-backed logger is used.
func New(log checkpointLogger) *Manager {
	if log == nil {
		log = stdlibCheckpointLogger{}
	}
	return &Manager{log: log}
}

// FormatVersion constants for checkpoint objects.
//
// Version history:
//
//	1 — original format (all existing clusters); introduced as an explicit
//	    field so future incompatible changes can be detected at runtime.
//	2 — the Pebble state includes the meta keyspace (see wal.OpMetaPut). A
//	    binary that predates it would restore without that state, so it must
//	    refuse the checkpoint instead.
//
// Compatibility rules:
//   - Adding new JSON fields with omitempty tags is always backward-compatible:
//     older nodes that do not know the field simply ignore it.
//   - Incrementing FormatVersion signals an incompatible change. A node that
//     reads a FormatVersion it does not understand logs a warning and returns
//     an error rather than silently producing corrupt state.
//   - Nodes running version N can safely read checkpoints written by version N-1.
//     Downgrade (new → old) is only safe if no FormatVersion > 1 checkpoint
//     has been written.
//   - Writers use the lowest version that can represent the checkpoint, so
//     data that does not need a newer format stays readable by older nodes.
const (
	// FormatVersionBase is written for checkpoints without meta keyspace state.
	FormatVersionBase uint32 = 1
	// FormatVersionMeta is written once the meta keyspace is non-empty.
	FormatVersionMeta uint32 = 2

	// CheckpointFormatVersion is the newest format this binary can read.
	// Increment it when a format change is incompatible with older readers.
	CheckpointFormatVersion = FormatVersionMeta
)

// Manifest is stored at "manifest/latest" in object storage.
// It points to the latest checkpoint so startup only needs one GET.
type Manifest struct {
	FormatVersion uint32 `json:"format_version,omitempty"`
	CheckpointKey string `json:"checkpoint_key"`
	Revision      int64  `json:"revision"`
	LastSequence  int64  `json:"last_sequence,omitempty"`
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
	FormatVersion uint32 `json:"format_version,omitempty"`
	Term          uint64 `json:"term"`
	Revision      int64  `json:"revision"`
	LastSequence  int64  `json:"last_sequence,omitempty"`
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
func (m *Manager) ReadManifest(ctx context.Context, store object.Store) (*Manifest, error) {
	rc, err := store.Get(ctx, ManifestKey)
	if err == object.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read manifest: %w", err)
	}
	defer rc.Close()
	var manifest Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("checkpoint: decode manifest: %w", err)
	}
	// FormatVersion == 0 means the manifest was written by an older node that
	// did not emit the field (backward-compatible: treat as version 1).
	if manifest.FormatVersion > CheckpointFormatVersion {
		m.log.Warnf("checkpoint: manifest format_version=%d > known=%d - this node is too old; upgrade required",
			manifest.FormatVersion, CheckpointFormatVersion)
		return nil, fmt.Errorf("checkpoint: manifest format_version=%d is newer than supported format_version=%d",
			manifest.FormatVersion, CheckpointFormatVersion)
	}
	if manifest.LastSequence == 0 {
		manifest.LastSequence = manifest.Revision
	}
	return &manifest, nil
}

// WriteManifest writes m to object storage.
func (mgr *Manager) WriteManifest(ctx context.Context, store object.Store, m *Manifest) error {
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
//
// SST deduplication uses the previous checkpoint index rather than issuing a
// LIST request, keeping the per-checkpoint S3 cost to O(1) GETs regardless of
// how many SST files have accumulated in the bucket.
func (mgr *Manager) Write(ctx context.Context, db *pebble.DB, store object.Store, term uint64, revision int64, lastWALKey string, ancestorStore object.Store) error {
	return mgr.WriteAtSequence(ctx, db, store, term, revision, revision, lastWALKey, ancestorStore, FormatVersionBase)
}

// WriteAtSequence is like Write, but records the WAL sequence covered by the
// checkpoint separately from the user-visible revision.
func (mgr *Manager) WriteAtSequence(ctx context.Context, db *pebble.DB, store object.Store, term uint64, revision, sequence int64, lastWALKey string, ancestorStore object.Store, formatVersion uint32) error {
	tmpDir, err := os.MkdirTemp("", "t4-checkpoint-*")
	if err != nil {
		return fmt.Errorf("checkpoint: mktemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	cpDir := filepath.Join(tmpDir, "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		return fmt.Errorf("checkpoint: pebble checkpoint: %w", err)
	}

	// Build known-SST sets from the previous checkpoint index (2 GETs) instead
	// of issuing a LIST sst/ (which grows with bucket size). On a brand-new
	// store there is no previous index, so both sets start empty and every SST
	// is uploaded — correct behaviour for a first checkpoint.
	localSSTs, ancestorSSTs, err := mgr.knownSSTSets(ctx, store, ancestorStore)
	if err != nil {
		return fmt.Errorf("checkpoint: resolve known ssts: %w", err)
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

	return mgr.writeIndex(ctx, store, term, revision, sequence, lastWALKey, sstFiles, ancestorSSTFiles, metaFiles, formatVersion)
}

// WriteWithRegistry creates a Pebble checkpoint using a pre-built SST registry
// instead of uploading SSTs inline. All SSTs must already be present in store
// (uploaded by SSTUploader). This is the fast path used when streaming SST
// upload is active: the checkpoint write costs only a few small PUTs.
//
// localRegistry maps Pebble SST filename → "sst/{hash}/{name}" key in store.
// inheritedRegistry maps Pebble SST filename → s3 key in the ancestor store
// (for branch nodes); these are recorded as AncestorSSTFiles.
func (mgr *Manager) WriteWithRegistry(ctx context.Context, db *pebble.DB, store object.Store, term uint64, revision int64, lastWALKey string, localRegistry, inheritedRegistry map[string]string) error {
	return mgr.WriteWithRegistryAtSequence(ctx, db, store, term, revision, revision, lastWALKey, localRegistry, inheritedRegistry, FormatVersionBase)
}

// WriteWithRegistryAtSequence is like WriteWithRegistry, but records the WAL
// sequence covered by the checkpoint separately from the user-visible revision.
func (mgr *Manager) WriteWithRegistryAtSequence(ctx context.Context, db *pebble.DB, store object.Store, term uint64, revision, sequence int64, lastWALKey string, localRegistry, inheritedRegistry map[string]string, formatVersion uint32) error {
	tmpDir, err := os.MkdirTemp("", "t4-checkpoint-*")
	if err != nil {
		return fmt.Errorf("checkpoint: mktemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	cpDir := filepath.Join(tmpDir, "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		return fmt.Errorf("checkpoint: pebble checkpoint: %w", err)
	}

	var sstFiles, ancestorSSTFiles, metaFiles []string
	err = filepath.Walk(cpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		name := filepath.Base(path)
		if strings.HasSuffix(name, ".sst") {
			if key, ok := inheritedRegistry[name]; ok {
				ancestorSSTFiles = append(ancestorSSTFiles, key)
				return nil
			}
			key, ok := localRegistry[name]
			if !ok {
				// SST created after registry snapshot — should not happen if
				// Wait() was called before WriteWithRegistry, but handle it
				// gracefully by hashing and uploading inline.
				var uploadErr error
				key, uploadErr = contentSSTKey(path, name)
				if uploadErr != nil {
					return uploadErr
				}
				f, ferr := os.Open(path)
				if ferr != nil {
					return ferr
				}
				defer f.Close()
				if putErr := store.Put(ctx, key, f); putErr != nil {
					return fmt.Errorf("upload missing sst %q: %w", name, putErr)
				}
			}
			sstFiles = append(sstFiles, key)
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

	return mgr.writeIndex(ctx, store, term, revision, sequence, lastWALKey, sstFiles, ancestorSSTFiles, metaFiles, formatVersion)
}

// writeIndex writes the checkpoint index JSON and updates manifest/latest.
func (mgr *Manager) writeIndex(ctx context.Context, store object.Store, term uint64, revision, sequence int64, lastWALKey string, sstFiles, ancestorSSTFiles, metaFiles []string, formatVersion uint32) error {
	indexKey := CheckpointIndexKey(term, revision)
	idx := &CheckpointIndex{
		FormatVersion:    formatVersion,
		Term:             term,
		Revision:         revision,
		LastSequence:     sequence,
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
		FormatVersion: formatVersion,
		CheckpointKey: indexKey,
		Revision:      revision,
		LastSequence:  sequence,
		Term:          term,
		LastWALKey:    lastWALKey,
	}
	return mgr.WriteManifest(ctx, store, m)
}

// knownSSTSets returns the sets of SST keys already uploaded to store and to
// ancestorStore by reading the previous checkpoint index rather than issuing
// LIST requests.
//
// For the local store: read manifest/latest → read its index → use SSTFiles.
//
// For the ancestor store on a branch node's first checkpoint (no previous
// branch index yet, so AncestorSSTFiles is empty): fall back to listing the
// ancestor store's sst/ prefix once. On every subsequent checkpoint the
// ancestor set is carried forward from the previous index's AncestorSSTFiles,
// so no LIST is needed after the first.
func (mgr *Manager) knownSSTSets(ctx context.Context, store object.Store, ancestorStore object.Store) (local, ancestor map[string]struct{}, err error) {
	local = make(map[string]struct{})
	ancestor = make(map[string]struct{})

	manifest, err := mgr.ReadManifest(ctx, store)
	if err != nil || manifest == nil {
		err = nil // fresh store; sets stay empty
	} else {
		prevIdx, idxErr := mgr.ReadCheckpointIndex(ctx, store, manifest.CheckpointKey)
		if idxErr == nil {
			for _, k := range prevIdx.SSTFiles {
				local[k] = struct{}{}
			}
			for _, k := range prevIdx.AncestorSSTFiles {
				ancestor[k] = struct{}{}
			}
		}
	}

	// Branch node, first checkpoint: no AncestorSSTFiles in previous index yet.
	// LIST the ancestor store once so we correctly attribute inherited SSTs
	// instead of re-uploading them to the branch prefix.
	if ancestorStore != nil && len(ancestor) == 0 {
		ancestor, err = listSSTSet(ctx, ancestorStore)
		if err != nil {
			return nil, nil, fmt.Errorf("list ancestor ssts: %w", err)
		}
	}

	return local, ancestor, nil
}

// listSSTSet returns the set of full object keys in the store's "sst/" prefix.
// Used by GCOrphanSSTs and as a fallback for a branch node's first checkpoint.
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
func (mgr *Manager) Restore(ctx context.Context, store object.Store, objKey, targetDir string) (term uint64, revision int64, err error) {
	return restoreFromIndex(ctx, mgr, store, nil, objKey, targetDir)
}

// RestoreBranch restores a checkpoint that may reference SST files in a
// separate ancestorStore. Used by branch nodes on first boot via BranchPoint.
func (mgr *Manager) RestoreBranch(ctx context.Context, store object.Store, ancestorStore object.Store, objKey, targetDir string) (term uint64, revision int64, err error) {
	return restoreFromIndex(ctx, mgr, store, ancestorStore, objKey, targetDir)
}

// restoreFromIndex restores a v2 checkpoint from its CheckpointIndex.
// ancestorStore is used for AncestorSSTFiles; if nil, store is used for all.
func restoreFromIndex(ctx context.Context, mgr *Manager, store object.Store, ancestorStore object.Store, indexKey, targetDir string) (uint64, int64, error) {
	if !strings.HasSuffix(indexKey, "manifest.json") {
		return 0, 0, fmt.Errorf("checkpoint: index key %q must end with \"manifest.json\"", indexKey)
	}
	idx, err := mgr.ReadCheckpointIndex(ctx, store, indexKey)
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
// restores it to targetDir. checkpointFileVersions pins SST and Pebble meta
// object versions referenced by the index.
//
// If checkpointFileVersions is non-empty, RestoreVersioned runs in strict
// mode: every SST and Pebble meta key referenced by the index must be
// present in the map, otherwise restore fails before any download. This
// surfaces caller bugs (typos, stale enumeration) instead of silently
// falling through to a live read of a possibly-GC'd object.
//
// If checkpointFileVersions is empty, RestoreVersioned reads SSTs and meta
// from the live store. This is the legacy path for callers that captured
// only the index version; it is unsafe against source checkpoint GC.
func (mgr *Manager) RestoreVersioned(ctx context.Context, store object.VersionedStore, objKey, versionID string, checkpointFileVersions map[string]string, targetDir string) (term uint64, revision int64, err error) {
	if !strings.HasSuffix(objKey, "manifest.json") {
		return 0, 0, fmt.Errorf("checkpoint: index key %q must end with \"manifest.json\"", objKey)
	}
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
	metaPrefix := strings.TrimSuffix(objKey, "manifest.json")
	if len(checkpointFileVersions) > 0 {
		for _, sstKey := range idx.SSTFiles {
			if _, ok := checkpointFileVersions[sstKey]; !ok {
				return 0, 0, fmt.Errorf("checkpoint: pinned versions missing SST %q referenced by index %q (strict mode: pin every referenced key or pass an empty map for legacy live reads)", sstKey, objKey)
			}
		}
		for _, name := range idx.PebbleMeta {
			key := metaPrefix + name
			if _, ok := checkpointFileVersions[key]; !ok {
				return 0, 0, fmt.Errorf("checkpoint: pinned versions missing Pebble meta %q referenced by index %q (strict mode: pin every referenced key or pass an empty map for legacy live reads)", key, objKey)
			}
		}
	}
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return 0, 0, err
	}
	for _, sstKey := range idx.SSTFiles {
		name := filepath.Base(sstKey)
		if err := downloadVersionedOrLive(ctx, store, sstKey, checkpointFileVersions, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download versioned sst %q: %w", sstKey, err)
		}
	}
	for _, name := range idx.PebbleMeta {
		key := metaPrefix + name
		if err := downloadVersionedOrLive(ctx, store, key, checkpointFileVersions, filepath.Join(targetDir, name)); err != nil {
			return 0, 0, fmt.Errorf("checkpoint: download versioned meta %q: %w", name, err)
		}
	}
	return idx.Term, idx.Revision, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func (mgr *Manager) ReadCheckpointIndex(ctx context.Context, store object.Store, key string) (*CheckpointIndex, error) {
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
	if idx.FormatVersion > CheckpointFormatVersion {
		mgr.log.Warnf("checkpoint: index %q format_version=%d > known=%d - this node is too old; upgrade required",
			key, idx.FormatVersion, CheckpointFormatVersion)
		return nil, fmt.Errorf("checkpoint: index %q format_version=%d is newer than supported format_version=%d",
			key, idx.FormatVersion, CheckpointFormatVersion)
	}
	if idx.LastSequence == 0 {
		idx.LastSequence = idx.Revision
	}
	return &idx, nil
}

// streamToFile copies rc to dest and closes rc.
func streamToFile(rc io.ReadCloser, dest string) error {
	defer func() { _ = rc.Close() }()
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, rc)
	return err
}

func downloadFile(ctx context.Context, store object.Store, key, dest string) error {
	rc, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	return streamToFile(rc, dest)
}

func downloadVersionedOrLive(ctx context.Context, store object.VersionedStore, key string, versions map[string]string, dest string) error {
	if versionID := versions[key]; versionID != "" {
		rc, err := store.GetVersioned(ctx, key, versionID)
		if err != nil {
			return err
		}
		return streamToFile(rc, dest)
	}
	return downloadFile(ctx, store, key, dest)
}

// ── ListRemote ────────────────────────────────────────────────────────────────

// ListRemote returns the checkpoint index key for each checkpoint in object
// storage, sorted lexicographically (== chronologically).
func (mgr *Manager) ListRemote(ctx context.Context, store object.Store) ([]string, error) {
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
// even if it would otherwise fall outside the `keep` window. Checkpoints
// referenced by active branch entries are also always preserved, since branch
// nodes need the checkpoint index to call RestoreBranch.
// GCCheckpoints deletes old checkpoints (keeping the most recent keep) and
// returns the count of deleted checkpoints. Pinned branch checkpoints are
// never deleted. Also returns the set of SST keys that were referenced only
// by deleted checkpoints (orphan candidates for GCOrphanSSTs).
func (mgr *Manager) GCCheckpoints(ctx context.Context, store object.Store, keep int) (int, map[string]struct{}, error) {
	if keep < 1 {
		keep = 1
	}

	manifest, err := mgr.ReadManifest(ctx, store)
	if err != nil {
		return 0, nil, fmt.Errorf("checkpoint gc: read manifest: %w", err)
	}

	branches, err := mgr.ReadBranchEntries(ctx, store)
	if err != nil {
		return 0, nil, fmt.Errorf("checkpoint gc: read branch entries: %w", err)
	}
	pinnedKeys := make(map[string]bool, len(branches))
	for _, entry := range branches {
		if entry.AncestorCheckpointKey != "" {
			pinnedKeys[entry.AncestorCheckpointKey] = true
		}
	}

	keys, err := mgr.ListRemote(ctx, store)
	if err != nil {
		return 0, nil, fmt.Errorf("checkpoint gc: list: %w", err)
	}
	if len(keys) <= keep {
		return 0, nil, nil
	}

	// Collect SSTs referenced by surviving checkpoints so we never delete them.
	liveSSTs := make(map[string]struct{})
	for _, k := range keys[len(keys)-keep:] {
		idx, err := mgr.ReadCheckpointIndex(ctx, store, k)
		if err != nil {
			continue
		}
		for _, s := range idx.SSTFiles {
			liveSSTs[s] = struct{}{}
		}
	}
	// Also protect SSTs from pinned (branch) checkpoints.
	for k := range pinnedKeys {
		idx, err := mgr.ReadCheckpointIndex(ctx, store, k)
		if err != nil {
			continue
		}
		for _, s := range idx.SSTFiles {
			liveSSTs[s] = struct{}{}
		}
	}

	// Orphan candidates = SSTs from deleted checkpoints that are not live.
	orphanCandidates := make(map[string]struct{})

	toDelete := keys[:len(keys)-keep]
	var deleted int
	for _, k := range toDelete {
		if manifest != nil && k == manifest.CheckpointKey {
			continue
		}
		if pinnedKeys[k] {
			continue // branch is pinned to this checkpoint; preserve it
		}
		// Harvest SST candidates from this checkpoint before deleting it.
		idx, err := mgr.ReadCheckpointIndex(ctx, store, k)
		if err == nil {
			for _, s := range idx.SSTFiles {
				if _, live := liveSSTs[s]; !live {
					orphanCandidates[s] = struct{}{}
				}
			}
		}
		if err := deleteCheckpoint(ctx, store, k); err != nil {
			return deleted, orphanCandidates, fmt.Errorf("checkpoint gc: delete %q: %w", k, err)
		}
		deleted++
	}
	return deleted, orphanCandidates, nil
}

// deleteCheckpoint deletes all objects under "checkpoint/{term}/{rev}/".
func deleteCheckpoint(ctx context.Context, store object.Store, key string) error {
	prefix := key[:strings.LastIndex(key, "/")+1]
	subkeys, err := store.List(ctx, prefix)
	if err != nil {
		return err
	}
	return store.DeleteMany(ctx, subkeys)
}

// GCOrphanSSTs deletes SST files that were exclusively referenced by deleted
// checkpoints. The candidates map must be built by GCCheckpoints — it already
// excludes SSTs that are still referenced by any live checkpoint, so this
// function simply deletes everything in the set.
//
// Using a pre-computed candidate set (rather than listing all "sst/" keys and
// diffing against live checkpoints) closes a race where a newly-promoted leader
// uploads SSTs before writing its first checkpoint: those SSTs would appear as
// "orphans" in a full-LIST approach even though they are about to be referenced.
func (mgr *Manager) GCOrphanSSTs(ctx context.Context, store object.Store, candidates map[string]struct{}) (int, error) {
	if len(candidates) == 0 {
		return 0, nil
	}
	keys := make([]string, 0, len(candidates))
	for k := range candidates {
		keys = append(keys, k)
	}
	if err := store.DeleteMany(ctx, keys); err != nil {
		return 0, fmt.Errorf("checkpoint gc ssts: %w", err)
	}
	return len(keys), nil
}
