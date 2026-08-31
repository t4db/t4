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
