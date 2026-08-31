package store

import (
	"context"
	"testing"
	"time"
)

// TestWatchProgressAdvancesOnIdlePrefix is the property progress markers exist
// for: a watch whose own prefix sees no writes must still learn that it is
// caught up past revisions committed elsewhere. Without markers the only
// source of revision truth is the watch's own events, so an idle prefix looks
// identical to a watch that has fallen behind.
func TestWatchProgressAdvancesOnIdlePrefix(t *testing.T) {
	s := openMem(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx, "/watched/", 0, WatchOptions{Progress: true})
	if err != nil {
		t.Fatal(err)
	}

	// Every write lands outside the watched prefix, so this watch receives no
	// data events at all.
	apply(t, s,
		createEntry(1, "/elsewhere/a", []byte("1")),
		createEntry(2, "/elsewhere/b", []byte("2")),
	)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("watch channel closed before a progress marker arrived")
			}
			if ev.Type != EventProgress {
				t.Fatalf("unexpected %v event for key %q on an idle prefix", ev.Type, ev.KV.Key)
			}
			if ev.KV != nil {
				t.Errorf("progress event carried a KV: %+v", ev.KV)
			}
			if ev.Revision >= 2 {
				return // caught up past the writes it never saw
			}
		case <-deadline:
			t.Fatal("no progress marker reported revision >= 2")
		}
	}
}

// TestWatchWithoutProgressNeverSeesMarkers guards existing consumers: a watch
// that did not opt in must never observe an event with a nil KV.
func TestWatchWithoutProgressNeverSeesMarkers(t *testing.T) {
	s := openMem(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx, "/w/", 0, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}

	apply(t, s,
		createEntry(1, "/elsewhere/a", []byte("1")),
		createEntry(2, "/w/x", []byte("2")),
	)

	// The matching event must arrive with nothing else ahead of it.
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed")
		}
		if ev.Type == EventProgress || ev.KV == nil {
			t.Fatalf("watch without Progress received a marker: %+v", ev)
		}
		if ev.KV.Key != "/w/x" {
			t.Fatalf("unexpected event for %q", ev.KV.Key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for the data event")
	}
}

// TestMarkerOnlyBatchDoesNotEvictSlowWatch pins the backpressure rule. A full
// queue means the consumer is too slow for the event stream and is evicted;
// a marker is advisory and the next one supersedes it, so dropping one must
// never cost a watch its subscription. Otherwise a watch on a quiet prefix
// could be evicted by progress traffic alone.
func TestMarkerOnlyBatchDoesNotEvictSlowWatch(t *testing.T) {
	s := openMem(t)

	sub := &watchSubscription{
		prefix:   "/quiet/",
		progress: true,
		live:     make(chan []Event, 1),
	}
	s.watchHubMu.Lock()
	s.addWatchSubscriptionLocked(sub, 1)
	id := sub.id
	s.watchHubMu.Unlock()

	// Wedge the queue so any further send finds it full.
	sub.live <- []Event{progressEvent(1)}

	// Marker-only fan-out: nothing matches /quiet/, so this watch is offered a
	// standalone marker it cannot accept.
	s.dispatchWatchEvents([]Event{{Type: EventPut, KV: &KeyValue{Key: "/other/k", Revision: 2}}}, 2, true)

	s.watchHubMu.Lock()
	_, alive := s.watchers[id]
	s.watchHubMu.Unlock()
	if !alive {
		t.Fatal("watch was evicted by a dropped progress marker")
	}

	// An events batch it cannot accept still evicts: that is real backpressure.
	s.dispatchWatchEvents([]Event{{Type: EventPut, KV: &KeyValue{Key: "/quiet/k", Revision: 3}}}, 3, false)

	s.watchHubMu.Lock()
	_, alive = s.watchers[id]
	s.watchHubMu.Unlock()
	if alive {
		t.Fatal("slow watch was not evicted by an undeliverable event batch")
	}
}
