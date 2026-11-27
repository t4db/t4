package store

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/t4db/t4/internal/wal"
)

// Compaction is only observable through the records it leaves behind: a read
// or a watch that still answers says nothing about history that was never
// reclaimed. These tests therefore count physical log records.

// logRecords returns every retained log record as "rev:key", in revision
// order.
func logRecords(t *testing.T, s *Store) []string {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: logLower, UpperBound: logUpper})
	if err != nil {
		t.Fatalf("log iter: %v", err)
	}
	defer func() { _ = iter.Close() }()
	var out []string
	for iter.First(); iter.Valid(); iter.Next() {
		r, err := decodeRecord(iter.Value())
		if err != nil {
			t.Fatalf("decode rev=%d: %v", decodeLogKey(iter.Key()), err)
		}
		out = append(out, fmt.Sprintf("%d:%s", decodeLogKey(iter.Key()), r.key))
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("log scan: %v", err)
	}
	return out
}

// TestCompactSeesPrecedingEntriesInSameBatch: applying the same WAL entries
// must retain the same history however they are grouped into batches.
// Compaction used to read the committed database only, so mutations earlier in
// its own batch were invisible to it and survived (#422).
func TestCompactSeesPrecedingEntriesInSameBatch(t *testing.T) {
	entries := []wal.Entry{
		createEntry(1, "k", []byte("v1")),
		updateEntry(2, "k", []byte("v2"), 1, 1),
		createEntry(3, "other", []byte("v1")),
		compactEntry(3, 3),
	}

	for _, tc := range []struct {
		name  string
		apply func(*Store, []wal.Entry) error
	}{
		{"Apply", (*Store).Apply},
		{"Recover", (*Store).Recover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			separate := openMem(t)
			for i := range entries {
				if err := tc.apply(separate, entries[i:i+1]); err != nil {
					t.Fatalf("%s one at a time: %v", tc.name, err)
				}
			}
			together := openMem(t)
			if err := tc.apply(together, entries); err != nil {
				t.Fatalf("%s in one batch: %v", tc.name, err)
			}

			one, all := logRecords(t, separate), logRecords(t, together)
			if fmt.Sprint(one) != fmt.Sprint(all) {
				t.Fatalf("batch grouping changed retained history:\n one at a time: %v\n one batch:     %v", one, all)
			}
			if want := "[2:k 3:other]"; fmt.Sprint(all) != want {
				t.Fatalf("retained history %v, want %s", all, want)
			}
		})
	}
}
