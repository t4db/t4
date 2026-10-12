package t4

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/internal/wal"
)

// TestFollowerHistoricalReadSeesCompaction covers a linearizable read at a
// past revision through a follower that has acknowledged a compaction but
// not applied it yet. A compaction takes no revision, so a follower synced to
// the leader's revision may still lack it; the read must wait for it and
// fail as compacted, as it would on the leader.
func TestFollowerHistoricalReadSeesCompaction(t *testing.T) {
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 10*time.Second)
	follower := cluster.survivors(leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first, err := leader.Put(ctx, "k", []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	last, err := leader.Put(ctx, "k", []byte("v2"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.WaitForRevision(ctx, last); err != nil {
		t.Fatal(err)
	}

	// Hold the follower before it applies the compaction.
	release := make(chan struct{})
	var once sync.Once
	hook := func(entries []wal.Entry) {
		for _, e := range entries {
			if e.Op == wal.OpCompact {
				<-release
				return
			}
		}
	}
	testhook.BeforeFollowerApply.Store(&hook)
	t.Cleanup(func() {
		testhook.BeforeFollowerApply.Store(nil)
		once.Do(func() { close(release) })
	})

	if err := leader.Compact(ctx, last); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	type result struct {
		kv  *KeyValue
		err error
	}
	done := make(chan result, 1)
	go func() {
		kv, err := follower.LinearizableGet(ctx, "k", WithRevision(first))
		done <- result{kv, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("read at compacted revision %d answered before the follower applied the compaction: %v, %v", first, r.kv, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	r := <-done
	if !errors.Is(r.err, ErrCompacted) {
		t.Fatalf("read at compacted revision %d = %v, %v, want ErrCompacted", first, r.kv, r.err)
	}
}

// TestFollowerHistoricalReadWithV1Leader: during a rolling upgrade from v1.1
// the leader does not know the applied-sequence read index, and a follower's
// read at a past revision falls back to syncing to its revision.
func TestFollowerHistoricalReadWithV1Leader(t *testing.T) {
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 10*time.Second)
	follower := cluster.survivors(leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := leader.Put(ctx, "k", []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Put(ctx, "k", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}

	testhook.V1Leader.Store(true)
	t.Cleanup(func() { testhook.V1Leader.Store(false) })
	kv, err := follower.LinearizableGet(ctx, "k", WithRevision(first))
	if err != nil || kv == nil || string(kv.Value) != "v1" {
		t.Fatalf("read at revision %d with a v1.1 leader = %v, %v, want v1", first, kv, err)
	}
}
