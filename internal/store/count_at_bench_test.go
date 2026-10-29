package store

import (
	"fmt"
	"testing"

	"github.com/t4db/t4/internal/wal"
)

// BenchmarkCountAtPinned measures a revision-pinned count over a collection
// whose own keys have been written since the pin — which is what every page
// after the first of a paginated list is, because the continue token carries
// the revision and the cluster keeps writing.
//
// The count is derived from the HEAD count plus the keys that changed, and all
// it needs per changed key is whether the key was live at the pinned revision.
// Both undo paths are covered: the history ring, and the Pebble log scan the
// ring falls back to.
func BenchmarkCountAtPinned(b *testing.B) {
	const (
		prefix  = "/registry/pods/default/"
		keys    = 20000
		changed = 5000
		valSize = 4096
	)
	value := make([]byte, valSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	for _, ringSize := range []int{0, 100000} {
		name := "undo=pebble"
		if ringSize > 0 {
			name = "undo=ring"
		}
		b.Run(name, func(b *testing.B) {
			s, err := OpenMem()
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })
			// Enabled before any writes: the ring warms only from new commits.
			s.SetHistoryRingSize(ringSize)

			var rev int64
			createRev := make([]int64, keys)
			for i := 0; i < keys; i++ {
				rev++
				createRev[i] = rev
				if err := s.Apply([]wal.Entry{createEntry(rev, fmt.Sprintf("%spod-%06d", prefix, i), value)}); err != nil {
					b.Fatal(err)
				}
			}
			pin := rev
			// The storm is inside the counted prefix, so every one of these
			// keys lands in the undo set the count has to resolve.
			for i := 0; i < changed; i++ {
				rev++
				key := fmt.Sprintf("%spod-%06d", prefix, i)
				if err := s.Apply([]wal.Entry{updateEntry(rev, key, value, createRev[i], createRev[i])}); err != nil {
					b.Fatal(err)
				}
			}

			want, err := s.CountRange(prefix, ReadOptions{Revision: pin})
			if err != nil {
				b.Fatal(err)
			}
			if want != keys {
				b.Fatalf("pinned count = %d, want %d", want, keys)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n, err := s.CountRange(prefix, ReadOptions{Revision: pin})
				if err != nil {
					b.Fatal(err)
				}
				if n != keys {
					b.Fatalf("pinned count = %d, want %d", n, keys)
				}
			}
		})
	}
}
