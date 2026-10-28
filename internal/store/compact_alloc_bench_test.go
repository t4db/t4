package store

import (
	"fmt"
	"testing"

	"github.com/t4db/t4/internal/wal"
)

// BenchmarkCompactSupersededVersions measures one compaction over a window
// holding many superseded versions of Kubernetes-sized values — the shape of
// a first compaction on a store that has been running with autocompaction off,
// which is the default.
//
// The scan reads each record's key and lease and nothing else, so allocations
// per call should track the number of distinct keys it retains rather than the
// bytes of history it is removing.
func BenchmarkCompactSupersededVersions(b *testing.B) {
	const (
		keys     = 500
		versions = 20
		valSize  = 4096
	)
	value := make([]byte, valSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s, err := OpenMem()
		if err != nil {
			b.Fatalf("OpenMem: %v", err)
		}
		rev := int64(0)
		for v := 0; v < versions; v++ {
			for k := 0; k < keys; k++ {
				rev++
				key := fmt.Sprintf("/reg/obj-%06d", k)
				var e wal.Entry
				if v == 0 {
					e = createEntry(rev, key, value)
				} else {
					e = updateEntry(rev, key, value, rev-int64(keys)*int64(v), rev-int64(keys))
				}
				if err := s.Apply([]wal.Entry{e}); err != nil {
					b.Fatalf("Apply: %v", err)
				}
			}
		}
		target := rev
		b.StartTimer()

		if err := s.Apply([]wal.Entry{{Revision: rev, Term: 1, Op: wal.OpCompact, PrevRevision: target}}); err != nil {
			b.Fatalf("Apply compact: %v", err)
		}

		b.StopTimer()
		if got := s.CompactRevision(); got != target {
			b.Fatalf("CompactRevision = %d, want %d", got, target)
		}
		_ = s.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(keys*versions*valSize)/(1<<20), "MiB-history")
}
