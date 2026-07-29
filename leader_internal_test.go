package t4

import (
	"context"
	"path/filepath"
	"testing"

	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/wal"
)

// TestRecoverLocalWALBeforeLeadership reproduces the takeover state seen in
// the 1,000 pod/s ramp: the follower has durably acknowledged a committed WAL
// entry but crashes before applying it to Pebble. Promotion must apply the
// entry, not start the new term after it and leave a permanent stream gap.
func TestRecoverLocalWALBeforeLeadership(t *testing.T) {
	root := t.TempDir()
	db, err := istore.Open(filepath.Join(root, "db"), NoopLogger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	walDir := filepath.Join(root, "wal")
	w := wal.New(wal.WithLogger(NoopLogger))
	if err := w.Open(walDir, 7, 42); err != nil {
		t.Fatal(err)
	}
	w.Start(context.Background())
	entry := &wal.Entry{
		ID:             42,
		Revision:       11,
		Term:           7,
		Op:             wal.OpCreate,
		Key:            "/registry/pods/default/recovered",
		Value:          []byte("committed"),
		CreateRevision: 11,
		Version:        1,
	}
	if err := w.Append(entry); err != nil {
		t.Fatal(err)
	}

	n := &Node{wal: w}
	n.db.Store(db)
	if got := db.LastSequence(); got != 0 {
		t.Fatalf("precondition: Pebble sequence = %d, want 0", got)
	}
	if err := n.recoverLocalWALBeforeLeadership(walDir); err != nil {
		t.Fatal(err)
	}
	if got := db.LastSequence(); got != 42 {
		t.Fatalf("Pebble sequence after recovery = %d, want 42", got)
	}
	kv, err := db.Get(entry.Key)
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil || string(kv.Value) != "committed" {
		t.Fatalf("recovered value = %#v, want committed entry", kv)
	}
}

// TestRecoverLocalWALBeforeLeadershipNoopWhenCaughtUp covers the guard's pass
// path: when Pebble has already applied every local WAL entry, promotion
// replays nothing, the WAL-ahead-of-Pebble check does not fire, and the applied
// sequence is unchanged. This is the common same-node re-election case.
func TestRecoverLocalWALBeforeLeadershipNoopWhenCaughtUp(t *testing.T) {
	root := t.TempDir()
	db, err := istore.Open(filepath.Join(root, "db"), NoopLogger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	walDir := filepath.Join(root, "wal")
	w := wal.New(wal.WithLogger(NoopLogger))
	if err := w.Open(walDir, 7, 42); err != nil {
		t.Fatal(err)
	}
	w.Start(context.Background())
	entry := wal.Entry{
		ID:             42,
		Revision:       11,
		Term:           7,
		Op:             wal.OpCreate,
		Key:            "/registry/pods/default/caughtup",
		Value:          []byte("committed"),
		CreateRevision: 11,
		Version:        1,
	}
	if err := w.Append(&entry); err != nil {
		t.Fatal(err)
	}

	// Pebble is already at the WAL head before promotion.
	if err := db.Recover([]wal.Entry{entry}); err != nil {
		t.Fatal(err)
	}
	if got := db.LastSequence(); got != 42 {
		t.Fatalf("precondition: Pebble sequence = %d, want 42", got)
	}

	n := &Node{wal: w}
	n.db.Store(db)
	if err := n.recoverLocalWALBeforeLeadership(walDir); err != nil {
		t.Fatalf("recover for caught-up node returned error: %v", err)
	}
	if got := db.LastSequence(); got != 42 {
		t.Fatalf("Pebble sequence after no-op recovery = %d, want 42", got)
	}
}
