package checkpoint_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/t4db/t4/pkg/object"
)

func putSSTs(t *testing.T, store object.Store, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := store.Put(context.Background(), k, bytes.NewReader([]byte(k))); err != nil {
			t.Fatal(err)
		}
	}
}

func listSSTs(t *testing.T, store object.Store) []string {
	t.Helper()
	keys, err := store.List(context.Background(), "sst/")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	return keys
}

func set(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

func equalKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSweepUnreferencedSSTs pins the sweep's rules: SSTs a checkpoint lists
// or the caller reports live are kept, and any other SST is deleted only once
// two consecutive sweeps found it unreferenced.
func TestSweepUnreferencedSSTs(t *testing.T) {
	ctx := context.Background()
	store := object.NewMem()
	putIndex(t, store, 1, 1, "sst/r/1.sst")
	putSSTs(t, store, "sst/r/1.sst", "sst/l/2.sst", "sst/u/3.sst")
	live := set("sst/l/2.sst")

	deleted, marks, err := testCP.SweepUnreferencedSSTs(ctx, store, live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("first sweep deleted %d SSTs, want 0", deleted)
	}
	if !equalKeys(sortedKeys(marks), []string{"sst/u/3.sst"}) {
		t.Fatalf("marks = %v, want [sst/u/3.sst]", sortedKeys(marks))
	}

	// An SST uploaded between sweeps is only marked by the next one.
	putSSTs(t, store, "sst/n/4.sst")
	deleted, marks, err = testCP.SweepUnreferencedSSTs(ctx, store, live, marks)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("second sweep deleted %d SSTs, want 1", deleted)
	}
	if got, want := listSSTs(t, store), []string{"sst/l/2.sst", "sst/n/4.sst", "sst/r/1.sst"}; !equalKeys(got, want) {
		t.Fatalf("SSTs after sweep = %v, want %v", got, want)
	}
	if !equalKeys(sortedKeys(marks), []string{"sst/n/4.sst"}) {
		t.Fatalf("marks = %v, want [sst/n/4.sst]", sortedKeys(marks))
	}
}

// TestSweepKeepsMarkedSSTsNowInUse pins that a mark alone never deletes: an
// SST the previous sweep caught between its upload and its registration, or
// before a checkpoint referenced it, is kept once it is live or referenced.
func TestSweepKeepsMarkedSSTsNowInUse(t *testing.T) {
	ctx := context.Background()
	store := object.NewMem()
	putSSTs(t, store, "sst/a/1.sst", "sst/b/2.sst")
	putIndex(t, store, 1, 1, "sst/b/2.sst")

	deleted, marks, err := testCP.SweepUnreferencedSSTs(ctx, store, set("sst/a/1.sst"), set("sst/a/1.sst", "sst/b/2.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if got := listSSTs(t, store); deleted != 0 || len(got) != 2 {
		t.Fatalf("deleted %d, SSTs left %v; want both kept", deleted, got)
	}
	if len(marks) != 0 {
		t.Errorf("marks = %v, want none", sortedKeys(marks))
	}
}

// TestSweepAbortsWhenIndexUnreadable pins that the sweep deletes nothing when
// it cannot read a checkpoint's index: it could not tell which SSTs that
// checkpoint needs.
func TestSweepAbortsWhenIndexUnreadable(t *testing.T) {
	ctx := context.Background()
	mem := object.NewMem()
	idx := putIndex(t, mem, 1, 1, "sst/a/1.sst")
	putSSTs(t, mem, "sst/a/1.sst")

	marked := set("sst/a/1.sst")
	deleted, marks, err := testCP.SweepUnreferencedSSTs(ctx, flakyGetStore{Store: mem, key: idx}, nil, marked)
	if err == nil {
		t.Fatal("sweep ran although a checkpoint index could not be read")
	}
	if deleted != 0 || len(listSSTs(t, mem)) != 1 {
		t.Fatalf("sweep deleted SSTs despite the error")
	}
	if !equalKeys(sortedKeys(marks), []string{"sst/a/1.sst"}) {
		t.Errorf("marks = %v, want the previous marks kept", sortedKeys(marks))
	}
}

// failingDeleteStore fails DeleteMany, as an unreachable store would.
type failingDeleteStore struct{ object.Store }

func (failingDeleteStore) DeleteMany(context.Context, []string) error {
	return errors.New("delete failed")
}

// TestSweepRetriesFailedDelete pins that SSTs a sweep failed to delete stay
// marked, so the next sweep deletes them without waiting another round.
func TestSweepRetriesFailedDelete(t *testing.T) {
	ctx := context.Background()
	mem := object.NewMem()
	putSSTs(t, mem, "sst/a/1.sst")

	_, marks, err := testCP.SweepUnreferencedSSTs(ctx, failingDeleteStore{mem}, nil, set("sst/a/1.sst"))
	if err == nil {
		t.Fatal("want delete error")
	}
	deleted, _, err := testCP.SweepUnreferencedSSTs(ctx, mem, nil, marks)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("retry deleted %d SSTs, want 1", deleted)
	}
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
