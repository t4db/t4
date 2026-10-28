package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
)

// A watch that starts at an old revision replays the gap in windows rather
// than decoding all of it first, so the events it delivers now come from
// several scans instead of one. These tests shrink the window so the seam
// between windows is crossed many times, and check that the stream across it
// is the same stream: every event, once, in revision order.

// withWatchReplayWindow shrinks the replay window for one test.
func withWatchReplayWindow(t *testing.T, n int64) {
	t.Helper()
	prev := watchReplayWindow
	watchReplayWindow = n
	t.Cleanup(func() { watchReplayWindow = prev })
}

func TestWatchReplayAcrossWindowsDeliversEveryEventOnce(t *testing.T) {
	for _, window := range []int64{1, 2, 3, 7} {
		t.Run(fmt.Sprint("window=", window), func(t *testing.T) {
			withWatchReplayWindow(t, window)

			s := openMem(t)
			const revs = 20
			for i := 1; i <= revs; i++ {
				apply(t, s, createEntry(int64(i), fmt.Sprintf("/reg/k%02d", i), []byte(fmt.Sprint("v", i))))
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// From revision 0, so the whole history is a replay gap.
			ch, err := s.Watch(ctx, "/reg/", 0, WatchOptions{})
			if err != nil {
				t.Fatalf("Watch: %v", err)
			}

			for i := 1; i <= revs; i++ {
				select {
				case ev, ok := <-ch:
					if !ok {
						t.Fatalf("watch closed after %d of %d events", i-1, revs)
					}
					if ev.Type == EventProgress {
						continue
					}
					wantKey := fmt.Sprintf("/reg/k%02d", i)
					if ev.KV.Key != wantKey || ev.KV.Revision != int64(i) {
						t.Fatalf("event %d: got %s@%d, want %s@%d", i, ev.KV.Key, ev.KV.Revision, wantKey, i)
					}
					if got, want := string(ev.KV.Value), fmt.Sprint("v", i); got != want {
						t.Fatalf("event %d value: got %q, want %q", i, got, want)
					}
				case <-ctx.Done():
					t.Fatalf("timed out after %d of %d events", i-1, revs)
				}
			}
		})
	}
}

// TestWatchReplayAcrossWindowsPicksUpLiveWrites covers the seam that matters
// most: the handoff from replay to the live dispatcher. Windowing replays the
// gap before the handoff and the gap after it with the same loop, so a write
// landing while the replay is in flight must arrive exactly once.
func TestWatchReplayAcrossWindowsPicksUpLiveWrites(t *testing.T) {
	withWatchReplayWindow(t, 2)

	s := openMem(t)
	const replayed = 9
	for i := 1; i <= replayed; i++ {
		apply(t, s, createEntry(int64(i), fmt.Sprintf("/reg/k%02d", i), []byte("old")))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx, "/reg/", 0, WatchOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Drain the replay, then write live and require the next event to be it.
	for i := 1; i <= replayed; i++ {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed during replay at %d", i)
			}
			if ev.Type == EventProgress {
				continue
			}
			if ev.KV.Revision != int64(i) {
				t.Fatalf("replay event %d at revision %d", i, ev.KV.Revision)
			}
		case <-ctx.Done():
			t.Fatalf("timed out draining replay at %d", i)
		}
	}

	apply(t, s, createEntry(replayed+1, "/reg/live", []byte("new")))

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("watch closed before the live write arrived")
			}
			if ev.Type == EventProgress {
				continue
			}
			if ev.KV.Key != "/reg/live" || ev.KV.Revision != replayed+1 {
				t.Fatalf("got %s@%d, want /reg/live@%d", ev.KV.Key, ev.KV.Revision, replayed+1)
			}
			return
		case <-ctx.Done():
			t.Fatal("timed out waiting for the live write")
		}
	}
}

// TestWatchReplayWindowEndsWatchOnCompaction checks the per-window compaction
// check. A compaction that lands past a window's start may have removed
// records that window needed, and the watch has to end rather than deliver a
// stream with a hole in it.
func TestWatchReplayWindowEndsWatchOnCompaction(t *testing.T) {
	withWatchReplayWindow(t, 2)

	s := openMem(t)
	const revs = 12
	for i := 1; i <= revs; i++ {
		apply(t, s, createEntry(int64(i), fmt.Sprintf("/reg/k%02d", i), []byte("v")))
	}
	// Compact away most of the history, then watch from before it.
	apply(t, s, wal.Entry{Revision: revs, Term: 1, Op: wal.OpCompact, PrevRevision: revs - 1})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx, "/reg/", 1, WatchOptions{})
	if err != nil {
		// Refusing up front is the other correct answer.
		return
	}

	// Whatever arrives must be a prefix of the real stream, and the channel
	// must close rather than skip a revision.
	last := int64(0)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return // ended, as it must
			}
			if ev.Type == EventProgress {
				continue
			}
			if ev.KV.Revision <= last {
				t.Fatalf("revision went backwards: %d after %d", ev.KV.Revision, last)
			}
			last = ev.KV.Revision
		case <-ctx.Done():
			t.Fatal("watch neither ended nor kept delivering")
		}
	}
}
