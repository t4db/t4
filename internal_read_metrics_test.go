package t4

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/t4db/t4/internal/metrics"
)

// TestInternalReadsLeaveClientMetrics pins that reads T4 makes for its own
// bookkeeping (WithInternalRead) are left out of the client read metrics: the
// lease-expiry loop alone otherwise adds a list per second on the leader.
func TestInternalReadsLeaveClientMetrics(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()

	reads := func(op string) float64 { return testutil.ToFloat64(metrics.ReadsTotal.WithLabelValues(op)) }
	ops := map[string]func(opts ...ReadOption) error{
		"get":    func(o ...ReadOption) error { _, err := n.Get("/k", o...); return err },
		"exists": func(o ...ReadOption) error { _, err := n.Exists("/k", o...); return err },
		"list":   func(o ...ReadOption) error { _, err := n.List("/p/", o...); return err },
		"count":  func(o ...ReadOption) error { _, err := n.Count("/p/", o...); return err },
	}
	for op, read := range ops {
		before := reads(op)
		if err := read(WithInternalRead()); err != nil {
			t.Fatal(err)
		}
		if got := reads(op) - before; got != 0 {
			t.Errorf("%s: internal read counted %v times, want 0", op, got)
		}
		if err := read(); err != nil {
			t.Fatal(err)
		}
		if got := reads(op) - before; got != 1 {
			t.Errorf("%s: client read counted %v times, want 1", op, got)
		}
	}
}
