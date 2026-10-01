package t4_test

import (
	"errors"
	"testing"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/metrics"
)

// TestNodeReadMetrics proves the read path records one counter increment and
// one latency observation per local read, and that failed reads land only in
// the error counter — mirroring the write-side instrumentation semantics.
func TestNodeReadMetrics(t *testing.T) {
	n := openNode(t)

	if _, err := n.Put(ctx(t), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	before := gatherReadMetrics(t)

	if _, err := n.Get("k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv, err := n.Get("missing"); err != nil || kv != nil {
		t.Fatalf("Get missing: kv=%v, err=%v", kv, err)
	}
	if _, err := n.List(""); err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, err := n.Exists("k"); err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if _, err := n.Count(""); err != nil {
		t.Fatalf("Count: %v", err)
	}

	after := gatherReadMetrics(t)
	for op, want := range map[string]float64{"get": 2, "exists": 1, "list": 1, "count": 1} {
		if got := after.reads[op] - before.reads[op]; got != want {
			t.Errorf("t4_reads_total{op=%q}: delta want %v got %v", op, want, got)
		}
		if got := after.latency[op] - before.latency[op]; got != uint64(want) {
			t.Errorf("t4_read_duration_seconds{op=%q} count: delta want %v got %v", op, want, got)
		}
		if got := after.errs[op] - before.errs[op]; got != 0 {
			t.Errorf("t4_read_errors_total{op=%q}: delta want 0 got %v", op, got)
		}
	}

	// A read on a closed node is an error.
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	errBefore := gatherReadMetrics(t)
	if _, err := n.Get("k"); !errors.Is(err, t4.ErrClosed) {
		t.Fatalf("Get on closed node: %v", err)
	}
	errAfter := gatherReadMetrics(t)
	if got := errAfter.errs["get"] - errBefore.errs["get"]; got != 1 {
		t.Errorf("t4_read_errors_total{op=\"get\"} after closed read: delta want 1 got %v", got)
	}
	if got := errAfter.reads["get"] - errBefore.reads["get"]; got != 0 {
		t.Errorf("t4_reads_total{op=\"get\"} must not count errors: delta %v", got)
	}
}

type readMetricsSnapshot struct {
	reads   map[string]float64
	errs    map[string]float64
	latency map[string]uint64
}

func gatherReadMetrics(t *testing.T) readMetricsSnapshot {
	t.Helper()
	snap := readMetricsSnapshot{
		reads:   map[string]float64{},
		errs:    map[string]float64{},
		latency: map[string]uint64{},
	}
	families, err := metrics.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			op := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "op" {
					op = l.GetValue()
				}
			}
			switch f.GetName() {
			case "t4_reads_total":
				snap.reads[op] += m.GetCounter().GetValue()
			case "t4_read_errors_total":
				snap.errs[op] += m.GetCounter().GetValue()
			case "t4_read_duration_seconds":
				snap.latency[op] += m.GetHistogram().GetSampleCount()
			}
		}
	}
	return snap
}
