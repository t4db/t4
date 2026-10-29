package store

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/wal"
)

// A revision-pinned count is the HEAD count adjusted by the keys that changed
// since the pin, and all it needs per changed key is whether the key was live
// then. These tests hold that adjustment to the only definition that matters:
// the count must equal the length of the list at the same revision, whatever
// landed afterwards.

// countAtMustMatchList is the invariant. Every case below asserts it, because
// a count that disagrees with its own list is the bug worth catching however
// the undo set was built.
func countAtMustMatchList(t *testing.T, s *Store, prefix string, rev int64) int64 {
	t.Helper()
	n, err := s.CountRange(prefix, ReadOptions{Revision: rev})
	if err != nil {
		t.Fatalf("CountRange(%q, rev=%d): %v", prefix, rev, err)
	}
	kvs, err := s.ListRange(prefix, ReadOptions{Revision: rev})
	if err != nil {
		t.Fatalf("ListRange(%q, rev=%d): %v", prefix, rev, err)
	}
	if n != int64(len(kvs)) {
		t.Fatalf("CountRange(rev=%d) = %d, but ListRange(rev=%d) returned %d keys", rev, n, rev, len(kvs))
	}
	return n
}

func TestCountAtPinnedRevisionMatchesList(t *testing.T) {
	// ring=0 disables the history ring, so the count takes the Pebble log
	// scan instead; both build the same undo set and must agree.
	for _, ringSize := range []int{0, 100000} {
		name := "undo=pebble"
		if ringSize > 0 {
			name = "undo=ring"
		}
		t.Run(name, func(t *testing.T) {
			s := openMem(t)
			s.SetHistoryRingSize(ringSize)

			const n = 24
			var rev int64
			createRev := make([]int64, n)
			for i := 0; i < n; i++ {
				rev++
				createRev[i] = rev
				apply(t, s, createEntry(rev, fmt.Sprintf("/c/k%03d", i), []byte("v")))
			}
			pin := rev

			// Each kind of later change adjusts the count differently: an
			// update leaves the key counted, a delete must still count it at
			// the pin, and a key created afterwards must not be counted.
			rev++
			apply(t, s, updateEntry(rev, "/c/k000", []byte("v2"), createRev[0], createRev[0]))
			rev++
			apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpDelete, Key: "/c/k001",
				CreateRevision: createRev[1], PrevRevision: createRev[1]})
			rev++
			apply(t, s, createEntry(rev, "/c/k999", []byte("new")))

			if got := countAtMustMatchList(t, s, "/c/", pin); got != n {
				t.Fatalf("count at the pin = %d, want the %d keys that existed then", got, n)
			}
			// And at HEAD: one deleted, one added.
			head, err := s.CountRange("/c/", ReadOptions{})
			if err != nil {
				t.Fatalf("CountRange at HEAD: %v", err)
			}
			if head != n {
				t.Fatalf("count at HEAD = %d, want %d", head, n)
			}
		})
	}
}

// TestCountAtPinnedRevisionAcrossRepeatedChanges pins once and then churns the
// same keys, so the undo set holds keys whose first change after the pin is an
// update, a delete, and a recreate.
func TestCountAtPinnedRevisionAcrossRepeatedChanges(t *testing.T) {
	s := openMem(t)
	s.SetHistoryRingSize(100000)

	const n = 30
	var rev int64
	createRev := make([]int64, n)
	for i := 0; i < n; i++ {
		rev++
		createRev[i] = rev
		apply(t, s, createEntry(rev, fmt.Sprintf("/d/k%03d", i), []byte("v")))
	}
	pin := rev

	// Delete a key, then create it again: its first change after the pin is
	// the delete, which is what fixes its state at the pin.
	rev++
	apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpDelete, Key: "/d/k000",
		CreateRevision: createRev[0], PrevRevision: createRev[0]})
	recreated := rev
	rev++
	apply(t, s, createEntry(rev, "/d/k000", []byte("again")))
	// Update one key twice.
	rev++
	apply(t, s, updateEntry(rev, "/d/k002", []byte("v2"), createRev[2], createRev[2]))
	prev := rev
	rev++
	apply(t, s, updateEntry(rev, "/d/k002", []byte("v3"), createRev[2], prev))

	if got := countAtMustMatchList(t, s, "/d/", pin); got != n {
		t.Fatalf("count at the pin = %d, want %d", got, n)
	}
	// A pin between the delete and the recreate sees one key fewer.
	if got := countAtMustMatchList(t, s, "/d/", recreated); got != n-1 {
		t.Fatalf("count at the delete = %d, want %d", got, n-1)
	}
}

// TestCountAtPinnedRevisionWithFromKey covers the continuation point, which is
// what makes a paginated list's count the number of keys remaining.
func TestCountAtPinnedRevisionWithFromKey(t *testing.T) {
	s := openMem(t)
	s.SetHistoryRingSize(100000)

	const n = 20
	var rev int64
	createRev := make([]int64, n)
	for i := 0; i < n; i++ {
		rev++
		createRev[i] = rev
		apply(t, s, createEntry(rev, fmt.Sprintf("/e/k%03d", i), []byte("v")))
	}
	pin := rev
	rev++
	apply(t, s, updateEntry(rev, "/e/k015", []byte("v2"), createRev[15], createRev[15]))

	for _, from := range []string{"", "/e/k005", "/e/k015", "/e/k019"} {
		n, err := s.CountRange("/e/", ReadOptions{Revision: pin, FromKey: from})
		if err != nil {
			t.Fatalf("CountRange(from=%q): %v", from, err)
		}
		kvs, err := s.ListRange("/e/", ReadOptions{Revision: pin, FromKey: from})
		if err != nil {
			t.Fatalf("ListRange(from=%q): %v", from, err)
		}
		if n != int64(len(kvs)) {
			t.Fatalf("from=%q: count = %d, list returned %d", from, n, len(kvs))
		}
	}
}

// TestCountAtBrokenUndoChainFallsBack synthesizes the one state the pinned
// count cannot reason about: a key whose first recorded change after the pin
// anchors at a revision above the pin, which happens when the records below it
// have gone missing. The count must refuse that undo set and fall back rather
// than guess, because guessing reads the key as live at the pin when it was
// not there at all.
func TestCountAtBrokenUndoChainFallsBack(t *testing.T) {
	s := openMem(t)
	s.SetHistoryRingSize(0) // the Pebble log scan, which reads prevRevision

	const n = 20
	var rev int64
	for i := 0; i < n; i++ {
		rev++
		apply(t, s, createEntry(rev, fmt.Sprintf("/b/k%03d", i), []byte("v")))
	}
	pin := rev

	// A key that does not exist at the pin, created and then updated after it.
	rev++
	created := rev
	apply(t, s, createEntry(created, "/b/k777", []byte("new")))
	rev++
	apply(t, s, updateEntry(rev, "/b/k777", []byte("newer"), created, created))

	// Remove the create, so the earliest change the scan can see for that key
	// is the update, anchored at a revision above the pin.
	if err := s.db.Delete(logKey(created), pebble.NoSync); err != nil {
		t.Fatalf("delete the create record: %v", err)
	}

	// The key was absent at the pin, so the pinned count is the original n —
	// reached through the fallback, not through the undo set.
	if got := countAtMustMatchList(t, s, "/b/", pin); got != n {
		t.Fatalf("count at the pin = %d, want %d: a key created after the pin was counted as live at it", got, n)
	}
}
