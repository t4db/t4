package t4

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/t4db/t4/pkg/object"
)

// TestSSTGCRemovesUncheckpointedSSTs pins that SSTs the uploader streamed to
// the object store but that no checkpoint ever referenced — Pebble compacted
// them away between two checkpoints — are eventually deleted. GC used to
// harvest deletion candidates only from expiring checkpoints, so such SSTs
// stayed in the bucket forever.
func TestSSTGCRemovesUncheckpointedSSTs(t *testing.T) {
	store := object.NewMem()
	n := openCheckpointTestNode(t, store)
	ctx := context.Background()
	db := n.db.Load().Pebble()

	// Flush several L0 tables, wait for the uploader to stream them, then
	// compact them into one table so none survives to a checkpoint.
	for i := range 4 {
		if _, err := n.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	n.sstUploader.Wait()
	if flushed, err := store.List(ctx, "sst/"); err != nil || len(flushed) < 4 {
		t.Fatalf("flushed tables not uploaded: %d SSTs, err=%v", len(flushed), err)
	}
	if err := db.Compact([]byte{0}, []byte{0xff}, true); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	n.sstUploader.Wait()

	// Run enough checkpoint+GC rounds for old checkpoints to expire and for
	// unreferenced SSTs to be swept.
	for i := range 4 {
		if _, err := n.Put(ctx, fmt.Sprintf("r%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
		n.maybeCheckpoint(ctx)
	}

	referenced := make(map[string]struct{})
	cps, err := n.cp.ListRemote(ctx, store)
	if err != nil {
		t.Fatalf("ListRemote: %v", err)
	}
	for _, k := range cps {
		idx, err := n.cp.ReadCheckpointIndex(ctx, store, k)
		if err != nil {
			t.Fatalf("ReadCheckpointIndex %q: %v", k, err)
		}
		for _, s := range idx.SSTFiles {
			referenced[s] = struct{}{}
		}
	}
	// The registry may still name tables Pebble has deleted; only tables
	// still on disk count as live.
	for name, key := range n.sstUploader.Registry() {
		if _, err := os.Stat(filepath.Join(n.cfg.DataDir, "db", name)); err == nil {
			referenced[key] = struct{}{}
		}
	}

	ssts, err := store.List(ctx, "sst/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var leaked []string
	for _, k := range ssts {
		if _, ok := referenced[k]; !ok {
			leaked = append(leaked, k)
		}
	}
	sort.Strings(leaked)
	if len(leaked) > 0 {
		t.Fatalf("%d of %d SSTs in the object store are referenced by no checkpoint and no live table:\n%s",
			len(leaked), len(ssts), strings.Join(leaked, "\n"))
	}
}
