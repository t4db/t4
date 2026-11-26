package wal

import (
	"os"
	"testing"
)

// BenchmarkSegmentCommit measures one small entry appended and made durable,
// the WAL cost of a single-key write. Set T4_BENCH_DIR to measure a
// particular disk; it matters most on Linux, where Sync is fdatasync.
func BenchmarkSegmentCommit(b *testing.B) {
	for _, prealloc := range []bool{false, true} {
		name := "grow"
		if prealloc {
			name = "prealloc"
		}
		b.Run(name, func(b *testing.B) {
			dir := os.Getenv("T4_BENCH_DIR")
			if dir == "" {
				dir = b.TempDir()
			} else {
				d, err := os.MkdirTemp(dir, "bench")
				if err != nil {
					b.Fatal(err)
				}
				defer func() { _ = os.RemoveAll(d) }()
				dir = d
			}
			sw, err := OpenSegmentWriter(dir, 1, 1)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = sw.Close() }()
			if prealloc {
				sw.Preallocate()
			}
			e := &Entry{Term: 1, Op: OpCreate, Key: "/registry/pods/default/bench", Value: make([]byte, 256)}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.Revision = int64(i + 1)
				if err := sw.AppendNoSync(e); err != nil {
					b.Fatal(err)
				}
				if err := sw.Sync(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
