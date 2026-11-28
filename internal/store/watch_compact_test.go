package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
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

// TestLiveWatchEndsOnCompactionInSameBatch: a batch that writes revisions
// and compacts past them (follower catch-up, WAL recovery) leaves the live
// dispatcher only the survivors. A watch that wanted the removed revisions
// ends rather than receive the survivors as a gapless stream.
func TestLiveWatchEndsOnCompactionInSameBatch(t *testing.T) {
	s := openMem(t)
	apply(t, s, createEntry(1, "a", []byte("1")))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ch, err := s.Watch(ctx, "", 1, WatchOptions{Progress: true})
	if err != nil {
		t.Fatal(err)
	}
	var batch []wal.Entry
	for rev := int64(2); rev <= 10; rev++ {
		batch = append(batch, updateEntry(rev, "a", []byte(fmt.Sprint(rev)), 1, rev-1))
	}
	apply(t, s, append(batch, compactEntry(10, 8))...)

	want := int64(2)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev.Type == EventProgress {
				continue
			}
			if ev.KV.Revision != want {
				t.Fatalf("got revision %d, want %d: the watch skipped compacted revisions", ev.KV.Revision, want)
			}
			if want++; want > 10 {
				return
			}
		case <-ctx.Done():
			t.Fatal("the watch neither delivered every revision nor ended")
		}
	}
}
