package store

import "testing"

// TestDividedAmongSharesTheWriteBudget pins the two numbers a process serving
// several databases depends on: the write buffer shrinks with the count rather
// than every database taking a full one, and only a single database keeps the
// full compaction concurrency.
func TestDividedAmongSharesTheWriteBudget(t *testing.T) {
	for _, tc := range []struct {
		name            string
		count           int
		wantSize        uint64
		wantCompactions int
	}{
		{"one database is unchanged", 1, defaultMemTableSize, maxCompactions()},
		{"a count below one is one", 0, defaultMemTableSize, maxCompactions()},
		{"four share the buffer", 4, defaultMemTableSize / 4, 1},
		{"many stop at the floor", 64, minMemTableSize, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaultPebbleOptions()
			DividedAmong(tc.count)(opts)
			if opts.MemTableSize != tc.wantSize {
				t.Errorf("MemTableSize = %d, want %d", opts.MemTableSize, tc.wantSize)
			}
			if got := opts.MaxConcurrentCompactions(); got != tc.wantCompactions {
				t.Errorf("MaxConcurrentCompactions = %d, want %d", got, tc.wantCompactions)
			}
		})
	}
}

// TestDividedAmongNeverBelowPebblesOwnDefault guards the floor directly: a
// buffer smaller than Pebble's default flushes so often that it defeats the
// reason T4 raised it.
func TestDividedAmongNeverBelowPebblesOwnDefault(t *testing.T) {
	for count := 1; count <= 512; count++ {
		opts := defaultPebbleOptions()
		DividedAmong(count)(opts)
		if opts.MemTableSize < minMemTableSize {
			t.Fatalf("count %d: MemTableSize = %d, below the %d floor", count, opts.MemTableSize, minMemTableSize)
		}
	}
}
