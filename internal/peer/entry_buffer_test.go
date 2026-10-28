package peer

import (
	"testing"

	"github.com/t4db/t4/internal/wal"
)

func bufEntry(seq int64, valBytes int) *wal.Entry {
	return &wal.Entry{ID: seq, Revision: seq, Term: 1, Op: wal.OpCreate,
		Key: "/reg/k", Value: make([]byte, valBytes)}
}

// The catch-up buffer is bounded by entry count and by bytes. The count alone
// does not bound memory: entries are retained with their values, so the same
// 10 000 entries are tens of megabytes of small objects and gigabytes of large
// ones.

func TestEntryBufferBoundsBytes(t *testing.T) {
	const (
		valBytes = 64 << 10 // 64 KiB per entry
		maxBytes = 1 << 20  // 1 MiB budget: ~16 entries
		pushes   = 200
	)
	// A count cap high enough that only the byte bound can evict.
	b := newEntryBuffer(10_000, maxBytes)
	for i := 1; i <= pushes; i++ {
		b.push(bufEntry(int64(i), valBytes))
	}

	if b.bytes > maxBytes {
		t.Fatalf("buffer holds %d bytes, over its %d budget", b.bytes, maxBytes)
	}
	if len(b.entries) >= pushes {
		t.Fatalf("buffer kept %d of %d entries; the byte bound never evicted", len(b.entries), pushes)
	}
	// The accounting must match what is actually retained, or the bound
	// drifts as the buffer churns.
	var want int
	for _, e := range b.entries {
		want += e.MemSize()
	}
	if b.bytes != want {
		t.Fatalf("buffer accounts for %d bytes, entries hold %d", b.bytes, want)
	}
	// The newest entry is the one a follower is most likely to need.
	if last := b.entries[len(b.entries)-1]; last.Sequence() != pushes {
		t.Fatalf("newest retained entry is %d, want %d", last.Sequence(), pushes)
	}
}

func TestEntryBufferKeepsCountBound(t *testing.T) {
	// No byte bound, so this is the behaviour from before it existed.
	b := newEntryBuffer(3, 0)
	for i := 1; i <= 10; i++ {
		b.push(bufEntry(int64(i), 8))
	}
	if len(b.entries) != 3 {
		t.Fatalf("buffer holds %d entries, want its cap of 3", len(b.entries))
	}
	if first := b.entries[0].Sequence(); first != 8 {
		t.Fatalf("oldest retained entry is %d, want 8", first)
	}
}

// TestEntryBufferKeepsOneOversizeEntry covers the entry that is larger than
// the whole budget. Dropping it would leave the buffer empty and unable to
// serve even the write that just happened.
func TestEntryBufferKeepsOneOversizeEntry(t *testing.T) {
	b := newEntryBuffer(10_000, 1<<20)
	b.push(bufEntry(1, 8))
	b.push(bufEntry(2, 4<<20)) // four times the budget on its own

	if len(b.entries) != 1 {
		t.Fatalf("buffer holds %d entries, want just the oversize one", len(b.entries))
	}
	if got := b.entries[0].Sequence(); got != 2 {
		t.Fatalf("buffer kept entry %d, want the newest (2)", got)
	}
}

// TestEntryBufferReportsEvictedAsMissing checks the contract the eviction
// relies on: a follower asking for a sequence the buffer no longer holds must
// be told so, and resync from the object store instead.
func TestEntryBufferReportsEvictedAsMissing(t *testing.T) {
	b := newEntryBuffer(10_000, 1<<20)
	for i := 1; i <= 200; i++ {
		b.push(bufEntry(int64(i), 64<<10))
	}
	oldest := b.entries[0].Sequence()

	if _, ok := b.since(oldest - 1); ok {
		t.Fatalf("since(%d) claimed to serve a sequence evicted from the buffer", oldest-1)
	}
	got, ok := b.since(oldest)
	if !ok {
		t.Fatalf("since(%d) refused the oldest retained sequence", oldest)
	}
	if len(got) != len(b.entries) {
		t.Fatalf("since(%d) returned %d entries, want all %d retained", oldest, len(got), len(b.entries))
	}
}
