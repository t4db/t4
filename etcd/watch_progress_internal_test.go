package etcd

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
)

// TestDrainWatchProgressPinsToDeliveredRevision is the core soundness
// property behind kube-apiserver's consistent-read-from-cache path: an
// on-demand progress notification must report the revision this watch has
// actually delivered, never the live node clock.
//
// apiserver asks for progress and then serves LISTs from its watchCache at
// the revision we report. Reporting a higher revision would let the cache
// advance past events still queued for delivery, silently dropping them.
func TestDrainWatchProgressPinsToDeliveredRevision(t *testing.T) {
	ctx := context.Background()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	// Advance the node clock well past where the watch starts, so a progress
	// notification sourced from the live clock is distinguishable.
	for i := 0; i < 5; i++ {
		if _, err := node.Put(ctx, "/other/k", []byte("v"), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	liveRev := node.CurrentRevision()
	if liveRev < 5 {
		t.Fatalf("expected node revision >= 5, got %d", liveRev)
	}

	srv := New(node, nil, nil)

	events := make(chan t4.Event)
	sendCh := make(chan []*etcdserverpb.WatchResponse, 4)
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()

	// A watch replaying from rev 3: it is caught up through rev 2 and has
	// delivered nothing beyond that.
	sub := testSubscription(wcancel, events)
	sub.startRev = 2
	go srv.drainWatch(wctx, 7, sub, sendCh)

	sub.requestProgress()
	resp := recvOne(t, sendCh)
	if got, want := resp.Header.Revision, toEtcdRevision(2); got != want {
		t.Errorf("progress before any delivery: revision = %d, want %d (live clock is %d)",
			got, want, toEtcdRevision(liveRev))
	}
	if resp.WatchId != 7 {
		t.Errorf("progress WatchId = %d, want 7", resp.WatchId)
	}
	if len(resp.Events) != 0 {
		t.Errorf("progress notification carried %d events, want 0", len(resp.Events))
	}

	// Deliver one event; progress must now advance to exactly that revision
	// and no further.
	events <- t4.Event{Type: t4.EventPut, KV: &t4.KeyValue{Key: "/w/k", Value: []byte("v"), Revision: 3}}
	events <- t4.Event{Type: t4.EventProgress, Revision: 3}
	if ev := recvOne(t, sendCh); len(ev.Events) != 1 {
		t.Fatalf("expected the event frame, got %d events", len(ev.Events))
	}

	sub.requestProgress()
	if got, want := recvOne(t, sendCh).Header.Revision, toEtcdRevision(3); got != want {
		t.Errorf("progress after delivering rev 3: revision = %d, want %d", got, want)
	}
}

// TestSubscribeWatchSeedsStartRevision checks the seed drainWatch's progress
// accounting starts from: the revision the watch is already caught up through.
func TestSubscribeWatchSeedsStartRevision(t *testing.T) {
	ctx := context.Background()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	for i := 0; i < 4; i++ {
		if _, err := node.Put(ctx, "/k", []byte("v"), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	srv := New(node, nil, nil)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A replay watch has delivered nothing below its start revision.
	sub, err := srv.subscribeWatch(wctx, &etcdserverpb.WatchCreateRequest{
		Key:           []byte("/"),
		RangeEnd:      []byte("0"),
		StartRevision: toEtcdRevision(2),
	})
	if err != nil {
		t.Fatalf("subscribeWatch: %v", err)
	}
	if sub.startRev != 1 {
		t.Errorf("replay watch startRev = %d, want 1", sub.startRev)
	}

	// A live watch (StartRevision 0) begins after the current revision, so it
	// is genuinely caught up there.
	live, err := srv.subscribeWatch(wctx, &etcdserverpb.WatchCreateRequest{
		Key:      []byte("/"),
		RangeEnd: []byte("0"),
	})
	if err != nil {
		t.Fatalf("subscribeWatch: %v", err)
	}
	if live.startRev != node.CurrentRevision() {
		t.Errorf("live watch startRev = %d, want %d", live.startRev, node.CurrentRevision())
	}
}

// TestDrainWatchHoldsIncompleteRevision: the events channel carries one
// event at a time, so the newest revision may still be arriving when the
// channel runs dry. drainWatch must hold it until a later revision or a
// progress marker seals it, and must drop rather than send it if the channel
// closes first — a client resumes from the last event it received, so a
// partial revision would lose the rest of it.
func TestDrainWatchHoldsIncompleteRevision(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	srv := New(node, nil, nil)

	events := make(chan t4.Event)
	sendCh := make(chan []*etcdserverpb.WatchResponse, 4)
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	done := make(chan struct{})
	go func() {
		srv.drainWatch(wctx, 1, testSubscription(wcancel, events), sendCh)
		close(done)
	}()

	put := func(key string, rev int64) {
		events <- t4.Event{Type: t4.EventPut, KV: &t4.KeyValue{Key: key, Value: []byte("v"), Revision: rev}}
	}
	quiet := func() {
		t.Helper()
		select {
		case run := <-sendCh:
			t.Fatalf("sent %d frame(s) for an incomplete revision: %+v", len(run), run[0])
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Revision 5 arrives in two parts; nothing may be sent in between.
	put("/a", 5)
	quiet()
	put("/b", 5)
	quiet()

	// An event of revision 6 seals 5, which ships whole; 6 is held back.
	put("/c", 6)
	resp := recvOne(t, sendCh)
	if len(resp.Events) != 2 || resp.Header.Revision != toEtcdRevision(5) {
		t.Fatalf("got %d events at header revision %d, want revision 5's 2 events at %d",
			len(resp.Events), resp.Header.Revision, toEtcdRevision(5))
	}
	quiet()

	// A progress marker seals 6.
	events <- t4.Event{Type: t4.EventProgress, Revision: 6}
	if resp := recvOne(t, sendCh); len(resp.Events) != 1 || resp.Header.Revision != toEtcdRevision(6) {
		t.Fatalf("got %d events at header revision %d, want revision 6's event at %d",
			len(resp.Events), resp.Header.Revision, toEtcdRevision(6))
	}

	// Revision 7 is still open when the channel closes: dropped, not sent.
	put("/d", 7)
	close(events)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainWatch did not exit after events channel closed")
	}
	select {
	case run := <-sendCh:
		t.Fatalf("sent an incomplete revision on close: %+v", run[0])
	default:
	}
}

func recvOne(t *testing.T, sendCh <-chan []*etcdserverpb.WatchResponse) *etcdserverpb.WatchResponse {
	t.Helper()
	select {
	case run := <-sendCh:
		if len(run) != 1 {
			t.Fatalf("expected a single-frame run, got %d frames", len(run))
		}
		return run[0]
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for a WatchResponse")
		return nil
	}
}
