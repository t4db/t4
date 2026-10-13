package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestWatchReplayEndsOnCompactedHistory: a watch whose history is compacted
// before its replay reads it ends with no events, rather than replaying what
// is left as if nothing were missing. Node.Watch refuses a compacted start
// revision only when the watch starts; a compaction can land before the
// replay goroutine scans the log.
func TestWatchReplayEndsOnCompactedHistory(t *testing.T) {
	s := openMem(t)
	for rev := int64(1); rev <= 10; rev++ {
		apply(t, s, createEntry(rev, fmt.Sprintf("k%02d", rev), []byte("v")))
	}
	apply(t, s, compactEntry(11, 6))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// Last seen revision 2: the watch asks for revisions from 3 on.
	ch, err := s.Watch(ctx, "", 2, WatchOptions{Progress: true})
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev.Type != EventProgress {
				t.Fatalf("got %s at revision %d from a watch from revision 3, compacted at 6", ev.KV.Key, ev.KV.Revision)
			}
		case <-ctx.Done():
			t.Fatal("the watch of compacted history did not end")
		}
	}
}

// TestWatchReplayAtCompactRevision: the compact revision itself stays
// watchable, as in etcd.
func TestWatchReplayAtCompactRevision(t *testing.T) {
	s := openMem(t)
	for rev := int64(1); rev <= 10; rev++ {
		apply(t, s, createEntry(rev, fmt.Sprintf("k%02d", rev), []byte("v")))
	}
	apply(t, s, compactEntry(11, 6))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ch, err := s.Watch(ctx, "", 5, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for want := int64(6); want <= 10; want++ {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("the watch ended before revision %d", want)
			}
			if ev.KV.Revision != want {
				t.Fatalf("got revision %d, want %d", ev.KV.Revision, want)
			}
		case <-ctx.Done():
			t.Fatalf("no event at revision %d", want)
		}
	}
}
