package etcd

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
)

// TestDrainWatchSlowWatcherCancellation verifies that a watch whose
// downstream sendCh blocks for longer than WatchSendTimeout is canceled
// with reason "mvcc: watcher is slow" and that drainWatch releases its
// goroutine + the per-watch context. Driven directly against drainWatch
// (no gRPC) so the cancellation contract is decoupled from clientv3
// retry / backpressure timing.
func TestDrainWatchSlowWatcherCancellation(t *testing.T) {
	node, err := t4.Open(t4.Config{
		DataDir:          t.TempDir(),
		WatchSendTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	srv := New(node, nil, nil)

	events := make(chan t4.Event, 4)
	sendCh := make(chan []*etcdserverpb.WatchResponse) // unbuffered: every flush blocks until consumed

	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.drainWatch(wctx, 42, testSubscription(wcancel, events), sendCh)
		close(done)
	}()

	// One event is enough to make drainWatch attempt a flush; the unbuffered
	// sendCh has no consumer yet, so the flush will block.
	events <- t4.Event{Type: t4.EventPut, KV: &t4.KeyValue{Key: "/slow/k", Value: []byte("v"), Revision: 1}}

	// Sleep past WatchSendTimeout so the slow-watcher branch fires before
	// the test reads sendCh. With a 200 ms timeout, 350 ms is comfortably
	// past the trip point.
	time.Sleep(350 * time.Millisecond)

	// drainWatch is now in the cancel-delivery window. The first run
	// pushed through sendCh MUST contain the cancellation, not the
	// abandoned event flush.
	var run []*etcdserverpb.WatchResponse
	select {
	case run = <-sendCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no response on sendCh after slow trip")
	}
	if len(run) != 1 {
		t.Fatalf("expected single-frame cancel run, got %d frames", len(run))
	}
	resp := run[0]

	if !resp.Canceled {
		t.Fatalf("expected Canceled=true after slow trip, got %+v", resp)
	}
	if resp.CancelReason != "mvcc: watcher is slow" {
		t.Errorf("CancelReason: got %q, want %q", resp.CancelReason, "mvcc: watcher is slow")
	}
	if resp.WatchId != 42 {
		t.Errorf("WatchId: got %d, want 42", resp.WatchId)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainWatch did not exit after slow-watcher cancellation")
	}

	if wctx.Err() == nil {
		t.Errorf("wctx not canceled after drainWatch exit; goroutine leak risk for upstream scanLog")
	}
}

// TestDrainWatchTimeoutDisabled verifies that setting WatchSendTimeout=0 (or
// negative) restores the legacy blocking-send behavior. Used as a documented
// escape hatch for embedders that prefer unbounded buffering over best-effort
// cancellation.
func TestDrainWatchTimeoutDisabled(t *testing.T) {
	node, err := t4.Open(t4.Config{
		DataDir:          t.TempDir(),
		WatchSendTimeout: -1, // explicitly disable; 0 would be replaced by setDefaults
	})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	srv := New(node, nil, nil)

	events := make(chan t4.Event, 4)
	sendCh := make(chan []*etcdserverpb.WatchResponse) // unbuffered

	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.drainWatch(wctx, 1, testSubscription(wcancel, events), sendCh)
		close(done)
	}()

	events <- t4.Event{Type: t4.EventPut, KV: &t4.KeyValue{Key: "/k", Value: []byte("v"), Revision: 1}}

	// drainWatch must stay blocked on the unbuffered sendCh — no cancellation.
	select {
	case <-done:
		t.Fatal("drainWatch exited even though WatchSendTimeout is disabled")
	case <-time.After(500 * time.Millisecond):
	}

	// Draining sendCh lets drainWatch's flush succeed, after which it sits
	// in the outer select waiting for more events. Closing events stops it.
	<-sendCh
	close(events)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainWatch did not exit after events channel closed")
	}
}

// testSubscription builds the minimal watchSubscription drainWatch needs when
// it is driven directly, without a gRPC stream: no filtering, no fragmentation,
// no periodic progress.
func testSubscription(cancel context.CancelFunc, events <-chan t4.Event) *watchSubscription {
	return &watchSubscription{
		events:      events,
		match:       func(string) bool { return true },
		cancel:      cancel,
		progressReq: make(chan struct{}, 1),
	}
}
