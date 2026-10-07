package checkpoint_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

var errNoConditional = errors.New("NotImplemented")

// noConditionalStore advertises conditional writes but rejects them, as
// older radosgw does for If-Match.
type noConditionalStore struct {
	*object.Mem
}

func (noConditionalStore) PutIfAbsent(context.Context, string, io.Reader) error {
	return errNoConditional
}

func (noConditionalStore) PutIfMatch(context.Context, string, io.Reader, string) error {
	return errNoConditional
}

// TestSingleWriterManifestSkipsConditionalWrites pins that a single-writer
// Manager advances manifest/latest with plain writes, so stores that reject
// conditional writes keep working in single-node mode.
func TestSingleWriterManifestSkipsConditionalWrites(t *testing.T) {
	ctx := context.Background()
	store := noConditionalStore{object.NewMem()}
	cp := checkpoint.New(nil)
	cp.SetSingleWriter()

	for seq := int64(1); seq <= 2; seq++ {
		if err := cp.WriteManifest(ctx, store, &checkpoint.Manifest{Term: 1, Revision: seq, LastSequence: seq}); err != nil {
			t.Fatalf("WriteManifest seq=%d: %v", seq, err)
		}
	}
	m, err := cp.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if m.LastSequence != 2 {
		t.Fatalf("manifest seq=%d, want 2", m.LastSequence)
	}
}

// TestSingleWriterManifestNeverGoesBackwards pins that dropping the
// conditional write keeps the staleness check: a single writer still never
// moves manifest/latest to an older checkpoint.
func TestSingleWriterManifestNeverGoesBackwards(t *testing.T) {
	ctx := context.Background()
	store := object.NewMem()
	cp := checkpoint.New(nil)
	cp.SetSingleWriter()

	if err := cp.WriteManifest(ctx, store, &checkpoint.Manifest{Term: 2, Revision: 5, LastSequence: 5}); err != nil {
		t.Fatal(err)
	}
	for _, old := range []*checkpoint.Manifest{
		{Term: 2, Revision: 4, LastSequence: 4},
		{Term: 1, Revision: 9, LastSequence: 9},
	} {
		if err := cp.WriteManifest(ctx, store, old); !errors.Is(err, checkpoint.ErrStaleManifest) {
			t.Errorf("WriteManifest term=%d seq=%d: err=%v, want ErrStaleManifest", old.Term, old.LastSequence, err)
		}
	}
	m, err := cp.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if m.Term != 2 || m.LastSequence != 5 {
		t.Fatalf("manifest term=%d seq=%d, want term=2 seq=5", m.Term, m.LastSequence)
	}
}

// TestClusterManifestRequiresConditionalWrites pins that without
// SetSingleWriter the manifest is still written conditionally: several nodes
// may write it, and only a conditional write rejects a deposed leader's late
// one.
func TestClusterManifestRequiresConditionalWrites(t *testing.T) {
	ctx := context.Background()
	store := noConditionalStore{object.NewMem()}
	err := checkpoint.New(nil).WriteManifest(ctx, store, &checkpoint.Manifest{Term: 1, Revision: 1, LastSequence: 1})
	if !errors.Is(err, errNoConditional) {
		t.Fatalf("WriteManifest err=%v, want the conditional write's error", err)
	}
}
