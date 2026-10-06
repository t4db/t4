package store

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/t4db/t4/internal/wal"
)

func TestHistoryRingAppend(t *testing.T) {
	h := newHistoryRing(3)
	h.append(10, []histChange{{key: "/a", prevRev: 0}})
	h.append(11, []histChange{{key: "/a", prevRev: 10}, {key: "/b", prevRev: 0}})
	h.append(12, []histChange{{key: "/c", prevRev: 4}})
	h.append(13, []histChange{{key: "/a", prevRev: 11}}) // evicts rev 10

	if f, ok := h.floor(); !ok || f != 11 {
		t.Fatalf("floor = %d, %v; want 11, true", f, ok)
	}

	// A pin at 9 needs rev 10's changes, which eviction dropped; a pin at 10
	// only needs what the ring holds.
	if _, ok := h.changesSince("", "", 9); ok {
		t.Fatal("changesSince(9) covered, want evicted")
	}

	m, ok := h.changesSince("", "", 11)
	if !ok {
		t.Fatal("changesSince(11) not covered")
	}
	want := map[string]int64{"/c": 4, "/a": 11}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("changesSince(11) = %v, want %v", m, want)
	}

	// Same-revision append replaces (term rewrite): rev 13's old /a change is gone.
	h.append(13, []histChange{{key: "/d", prevRev: 2}})
	m, ok = h.changesSince("", "", 12)
	if !ok {
		t.Fatal("changesSince(12) not covered")
	}
	want = map[string]int64{"/d": 2}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("changesSince(12) = %v, want %v", m, want)
	}

	// Out-of-order is ignored.
	h.append(12, []histChange{{key: "/bad", prevRev: 1}})
	m, _ = h.changesSince("", "", 12)
	if _, bad := m["/bad"]; bad {
		t.Fatal("out-of-order append leaked into merge")
	}
}

func TestHistoryRingEmptyUncovered(t *testing.T) {
	h := newHistoryRing(5)
	if _, ok := h.changesSince("", "", 1); ok {
		t.Fatal("empty ring covers")
	}
	h.append(7, []histChange{{key: "/a", prevRev: 0}})
	if _, ok := h.changesSince("", "", 5); ok {
		t.Fatal("ring covers rev before its floor")
	}
}

// TestReadAtHistoryMatchesReplay runs the random-history workload with the
// ring enabled and checks every pinned read against the replay reference.
func TestReadAtHistoryMatchesReplay(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			s := openMem(t)
			s.SetHistoryRingSize(1024)
			buildRandomHistory(t, s, rand.New(rand.NewSource(seed)))

			keys := []string{"/a/1", "/a/2", "/a/3", "/a/4", "/b/1", "/b/2", "/c"}
			for r := max(s.CompactRevision(), 1); r <= s.CurrentRevision(); r++ {
				for _, key := range keys {
					want, err := s.getAtRevision(key, r)
					if err != nil {
						t.Fatal(err)
					}
					got, err := s.GetAt(key, r)
					if err != nil {
						t.Fatalf("GetAt(%q, %d): %v", key, r, err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("GetAt(%q, %d) = %+v, replay %+v", key, r, got, want)
					}
				}
				for _, prefix := range []string{"", "/a/", "/b/"} {
					for _, limit := range []int64{0, 2} {
						opts := ReadOptions{Revision: r, Limit: limit}
						want, err := s.listAtByReplay(prefix, opts, r)
						if err != nil {
							t.Fatal(err)
						}
						got, err := s.ListRange(prefix, opts)
						if err != nil {
							t.Fatalf("ListRange(%q, %+v): %v", prefix, opts, err)
						}
						if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
							t.Fatalf("ListRange %q rev=%d limit=%d: ring %v, replay %v", prefix, r, limit, kvKeys(got), kvKeys(want))
						}
					}
					all, err := s.listAtByReplay(prefix, ReadOptions{Revision: r}, r)
					if err != nil {
						t.Fatal(err)
					}
					n, err := s.CountRange(prefix, ReadOptions{Revision: r})
					if err != nil {
						t.Fatalf("CountRange(%q, %d): %v", prefix, r, err)
					}
					if n != int64(len(all)) {
						t.Fatalf("CountRange %q rev=%d: ring %d, replay %d", prefix, r, n, len(all))
					}
				}
			}
			checkKeysOnlyMatchesFull(t, s)
		})
	}
}

// TestReadAtHistoryEvictionFallsBack sizes the ring far below the history
// length: uncovered pins must still return identical results via the Pebble
// undo/replay paths.
func TestReadAtHistoryEvictionFallsBack(t *testing.T) {
	s := openMem(t)
	s.SetHistoryRingSize(8)
	buildRandomHistory(t, s, rand.New(rand.NewSource(42)))

	head := s.CurrentRevision()
	if floor, ok := s.hist.Load().floor(); !ok || floor <= 10 {
		t.Fatalf("ring floor = %d, %v at head %d; eviction did not happen", floor, ok, head)
	}

	for r := max(s.CompactRevision(), 1); r <= head; r++ {
		want, err := s.listAtByReplay("", ReadOptions{Revision: r}, r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.ListRange("", ReadOptions{Revision: r})
		if err != nil {
			t.Fatalf("ListRange(rev=%d): %v", r, err)
		}
		if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
			t.Fatalf("ListRange rev=%d: %v, replay %v", r, kvKeys(got), kvKeys(want))
		}
	}
}

// TestReadAtHistoryAcrossCompactions checks ring reads at and right above the
// compaction watermark, where anchors sit at the retained newest-at-or-before
// record.
func TestReadAtHistoryAcrossCompactions(t *testing.T) {
	s := openMem(t)
	s.SetHistoryRingSize(100)
	var rev int64
	put := func(key, val string) int64 {
		rev++
		apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpCreate, Key: key, Value: []byte(val), CreateRevision: rev})
		return rev
	}
	upd := func(key, val string, createRev, prevRev int64) {
		rev++
		apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpUpdate, Key: key, Value: []byte(val), CreateRevision: createRev, PrevRevision: prevRev})
	}

	c1 := put("/k", "v1")
	put("/other", "w1")
	upd("/k", "v2", c1, c1)
	compactAt := rev
	apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpCompact, PrevRevision: compactAt})
	upd("/k", "v3", c1, rev)

	got, err := s.GetAt("/k", compactAt)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != "v2" {
		t.Fatalf("GetAt(/k, %d) = %q, want v2", compactAt, got.Value)
	}
	got, err = s.GetAt("/k", compactAt+1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != "v3" {
		t.Fatalf("GetAt(/k, %d) = %q, want v3", compactAt+1, got.Value)
	}
	kvs, err := s.ListRange("", ReadOptions{Revision: compactAt})
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 {
		t.Fatalf("ListRange(rev=%d): %v, want 2 keys", compactAt, kvKeys(kvs))
	}
}

// BenchmarkListAtHist compares one paginated page of a revision-pinned list:
// served by the ring versus the Pebble undo-from-HEAD path it replaces. The
// shape matches a resync storm: a filled keyspace, a storm of writes outside
// the listed prefix, then a 500-key page pinned below the storm — near
// enough to HEAD that listAtFromHead scans the whole storm's log records.
func BenchmarkListAtHist(b *testing.B) {
	for _, ringSize := range []int{0, 100000} {
		b.Run(fmt.Sprintf("ring=%d", ringSize), func(b *testing.B) {
			s, err := OpenMem()
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { s.Close() })
			// Enabled before any writes: the ring warms only from new commits.
			s.SetHistoryRingSize(ringSize)
			var rev int64
			put := func(key string) {
				rev++
				if err := s.Apply([]wal.Entry{{Revision: rev, Term: 1, Op: wal.OpCreate, Key: key, Value: []byte("v"), CreateRevision: rev}}); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < 20000; i++ {
				put(fmt.Sprintf("/k/%05d", i))
			}
			pin := rev
			for i := 0; i < 5000; i++ { // storm outside the listed prefix
				put(fmt.Sprintf("/other/%05d", i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				kvs, err := s.ListRange("/k/", ReadOptions{Revision: pin, Limit: 500})
				if err != nil {
					b.Fatal(err)
				}
				if len(kvs) != 500 {
					b.Fatalf("len = %d", len(kvs))
				}
			}
		})
	}
}
