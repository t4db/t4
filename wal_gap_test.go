package t4

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// TestOpenReplaysObjectStoreBeforeLocalGap covers a crash after the local
// WAL dropped segments it had uploaded and before Pebble persisted them:
// Pebble writes without syncing, so a SIGKILL can lose them. The local WAL
// then starts past what Pebble holds, and replaying it first would apply its
// entries across the gap and make the object store replay skip the entries
// only it has.
func TestOpenReplaysObjectStoreBeforeLocalGap(t *testing.T) {
	obj := object.NewMem()
	create := func(seq int64, key, value string) wal.Entry {
		return wal.Entry{ID: seq, Revision: seq, Term: 1, Op: wal.OpCreate, Key: key, Value: []byte(value), CreateRevision: seq}
	}
	// Sequences 1-2 are only in the object store.
	putSegment(t, obj, 1, create(1, "/a", "a1"), create(2, "/b", "b1"))

	// Sequences 3-4 are only in the local WAL.
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	sw, err := wal.OpenSegmentWriter(walDir, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	local := []wal.Entry{
		create(3, "/c", "c1"),
		{ID: 4, Revision: 4, Term: 1, Op: wal.OpUpdate, Key: "/a", Value: []byte("a2"), CreateRevision: 1, PrevRevision: 1, Version: 2},
	}
	for i := range local {
		if err := sw.Append(&local[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := Open(Config{DataDir: dataDir, ObjectStore: obj})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if rev := n.CurrentRevision(); rev != 4 {
		t.Errorf("CurrentRevision = %d, want 4", rev)
	}
	for key, want := range map[string]string{"/a": "a2", "/b": "b1", "/c": "c1"} {
		kv, err := n.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if kv == nil || string(kv.Value) != want {
			t.Errorf("%s = %v, want %q", key, kv, want)
		}
	}
}

// TestOpenRefusesUnfilledLocalGap checks that a local WAL past a gap the
// object store cannot fill fails Open instead of starting without the
// entries in the gap.
func TestOpenRefusesUnfilledLocalGap(t *testing.T) {
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	sw, err := wal.OpenSegmentWriter(walDir, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	e := wal.Entry{ID: 3, Revision: 3, Term: 1, Op: wal.OpCreate, Key: "/c", Value: []byte("c1"), CreateRevision: 3}
	if err := sw.Append(&e); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := Open(Config{DataDir: dataDir, ObjectStore: object.NewMem()})
	if err == nil {
		_ = n.Close()
		t.Fatal("Open succeeded with sequences 1-2 missing from both the local WAL and the object store")
	}
	if !errors.Is(err, errReplayGap) {
		t.Fatalf("Open: %v, want errReplayGap", err)
	}
}
