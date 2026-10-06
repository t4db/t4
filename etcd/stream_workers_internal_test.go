package etcd

import (
	"runtime"
	"testing"
)

// TestDefaultStreamWorkersBounded pins the pool size: four per processor,
// at least 16, and at most maxDefaultStreamWorkers, so a host without a CPU
// limit (GOMAXPROCS in the hundreds) doesn't keep thousands of idle workers.
func TestDefaultStreamWorkersBounded(t *testing.T) {
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)
	for _, tc := range []struct{ procs, want int }{
		{1, 16},
		{8, 32},
		{384, maxDefaultStreamWorkers},
	} {
		runtime.GOMAXPROCS(tc.procs)
		if got := DefaultStreamWorkers(); got != tc.want {
			t.Errorf("GOMAXPROCS=%d: %d workers, want %d", tc.procs, got, tc.want)
		}
	}
}
