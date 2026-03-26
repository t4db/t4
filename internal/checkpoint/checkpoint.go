// Package checkpoint handles creating, writing, and restoring Pebble snapshots
// to/from object storage.
package checkpoint

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/makhov/strata/internal/object"
)

// Manifest is stored at "manifest/latest" in object storage.
// It points to the latest checkpoint so startup only needs one GET.
type Manifest struct {
	CheckpointKey string `json:"checkpoint_key"` // e.g. "checkpoint/0000000001/00000000000000000042"
	Revision      int64  `json:"revision"`
	Term          uint64 `json:"term"`
	// LastWALKey is the object key of the last fully uploaded WAL segment
	// whose last entry has revision <= Revision. Used to bound WAL replay.
	LastWALKey string `json:"last_wal_key,omitempty"`
}

// ManifestKey is the fixed object storage key for the manifest.
const ManifestKey = "manifest/latest"

// CheckpointKey returns the object storage key for a checkpoint.
func CheckpointKey(term uint64, revision int64) string {
	return fmt.Sprintf("checkpoint/%010d/%020d", term, revision)
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

// checkpointHeader is the binary file header for a checkpoint archive.
// Format: magic(8) + term(8) + revision(8) = 24 bytes.
const (
	cpMagic  = "STRTCHK\n"
	cpHdrLen = 24
)

// Write creates a Pebble checkpoint in a temp directory, tarballs it, and
// uploads it to object storage. It then updates the manifest.
func Write(ctx context.Context, db *pebble.DB, store object.Store, term uint64, revision int64, lastWALKey string) error {
	tmpDir, err := os.MkdirTemp("", "strata-checkpoint-*")
	if err != nil {
		return fmt.Errorf("checkpoint: mktemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	cpDir := filepath.Join(tmpDir, "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		return fmt.Errorf("checkpoint: pebble checkpoint: %w", err)
	}

	objKey := CheckpointKey(term, revision)
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- writeArchive(pw, term, revision, cpDir)
		pw.Close()
	}()
	if err := store.Put(ctx, objKey, pr); err != nil {
		return fmt.Errorf("checkpoint: upload %q: %w", objKey, err)
	}
	if archErr := <-errCh; archErr != nil {
		return archErr
	}

	m := &Manifest{
		CheckpointKey: objKey,
		Revision:      revision,
		Term:          term,
		LastWALKey:    lastWALKey,
	}
	return WriteManifest(ctx, store, m)
}

// Restore downloads a checkpoint from object storage and restores it to
// targetDir (which must not exist). Returns the revision encoded in the
// checkpoint.
func Restore(ctx context.Context, store object.Store, objKey, targetDir string) (term uint64, revision int64, err error) {
	rc, err := store.Get(ctx, objKey)
	if err != nil {
		return 0, 0, fmt.Errorf("checkpoint: download %q: %w", objKey, err)
	}
	defer rc.Close()

	term, revision, err = readArchive(rc, targetDir)
	if err != nil {
		return 0, 0, fmt.Errorf("checkpoint: restore %q: %w", objKey, err)
	}
	return term, revision, nil
}

// ── archive format ────────────────────────────────────────────────────────────
//
// Header (24 bytes): magic(8) + term(8 BE) + revision(8 BE)
// Followed by: stream of file records
//   [4: nameLen uint32 BE][nameLen: relative path][8: fileSize uint64 BE][fileSize: content]

func writeArchive(w io.Writer, term uint64, revision int64, dir string) error {
	hdr := make([]byte, cpHdrLen)
	copy(hdr[0:8], cpMagic)
	binary.BigEndian.PutUint64(hdr[8:16], term)
	binary.BigEndian.PutUint64(hdr[16:24], uint64(revision))
	if _, err := w.Write(hdr); err != nil {
		return err
	}

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		nameBuf := []byte(rel)
		var meta [12]byte
		binary.BigEndian.PutUint32(meta[0:4], uint32(len(nameBuf)))
		binary.BigEndian.PutUint64(meta[4:12], uint64(info.Size()))
		if _, err := w.Write(meta[:]); err != nil {
			return err
		}
		if _, err := w.Write(nameBuf); err != nil {
			return err
		}
		_, err = io.Copy(w, f)
		return err
	})
}

func readArchive(r io.Reader, targetDir string) (term uint64, revision int64, err error) {
	hdr := make([]byte, cpHdrLen)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, 0, fmt.Errorf("read header: %w", err)
	}
	if string(hdr[0:8]) != cpMagic {
		return 0, 0, fmt.Errorf("bad checkpoint magic")
	}
	term = binary.BigEndian.Uint64(hdr[8:16])
	revision = int64(binary.BigEndian.Uint64(hdr[16:24]))

	for {
		var meta [12]byte
		_, err := io.ReadFull(r, meta[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return term, revision, nil
		}
		if err != nil {
			return 0, 0, err
		}
		nameLen := binary.BigEndian.Uint32(meta[0:4])
		fileSize := binary.BigEndian.Uint64(meta[4:12])

		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return 0, 0, err
		}
		relPath := string(nameBuf)
		// Safety: reject absolute paths or path traversal.
		if filepath.IsAbs(relPath) || strings.Contains(relPath, "..") {
			return 0, 0, fmt.Errorf("unsafe path in checkpoint: %q", relPath)
		}

		dest := filepath.Join(targetDir, filepath.FromSlash(relPath))
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return 0, 0, err
		}
		f, err := os.Create(dest)
		if err != nil {
			return 0, 0, err
		}
		if _, err := io.CopyN(f, r, int64(fileSize)); err != nil {
			f.Close()
			return 0, 0, err
		}
		f.Close()
	}
}

// ListRemote returns checkpoint object keys from object storage, sorted
// lexicographically (which equals chronological order given the naming scheme).
func ListRemote(ctx context.Context, store object.Store) ([]string, error) {
	keys, err := store.List(ctx, "checkpoint/")
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}
