package t4

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/t4db/t4/pkg/object"
)

// noIfMatchStore rejects If-Match writes, as some S3-compatible stores (older
// radosgw) do, while accepting If-None-Match ones.
type noIfMatchStore struct {
	*object.Mem
}

func (noIfMatchStore) PutIfMatch(context.Context, string, io.Reader, string) error {
	return errors.New("NotImplemented: A header you provided implies functionality that is not implemented")
}

// TestSingleNodeCheckpointsWithoutIfMatch pins that a single node keeps
// advancing manifest/latest on a store without If-Match. It is the only
// writer, so it needs no conditional write to keep the manifest from moving
// backwards; requiring one stopped every checkpoint after the first.
func TestSingleNodeCheckpointsWithoutIfMatch(t *testing.T) {
	store := noIfMatchStore{object.NewMem()}
	n := openCheckpointTestNode(t, store)
	ctx := context.Background()

	for i := range 3 {
		if _, err := n.Put(ctx, "k", []byte{byte(i)}, 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
		n.maybeCheckpoint(ctx)
	}

	m, err := n.cp.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m == nil {
		t.Fatal("no manifest written")
	}
	if want := n.db.Load().LastSequence(); m.LastSequence != want {
		t.Fatalf("manifest/latest seq=%d, want last sequence %d", m.LastSequence, want)
	}
}
