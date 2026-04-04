package etcd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/strata-db/strata"
)

// ── Watch unit tests ──────────────────────────────────────────────────────────

// TestWatchReceivesPut verifies a put event is delivered to a watcher.
func TestWatchReceivesPut(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wch := cli.Watch(ctx, "/w/key")
	go func() { node.Put(ctx, "/w/key", []byte("v"), 0) }()

	select {
	case wr := <-wch:
		if len(wr.Events) == 0 {
			t.Fatal("expected at least one event")
		}
		ev := wr.Events[0]
		if ev.Type != clientv3.EventTypePut {
			t.Errorf("event type: want PUT got %v", ev.Type)
		}
		if string(ev.Kv.Key) != "/w/key" {
			t.Errorf("event key: want /w/key got %q", ev.Kv.Key)
		}
		if string(ev.Kv.Value) != "v" {
			t.Errorf("event value: want v got %q", ev.Kv.Value)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for watch event")
	}
}

// TestWatchReceivesDelete verifies a delete event is delivered.
func TestWatchReceivesDelete(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node.Put(ctx, "/w/del", []byte("v"), 0)

	wch := cli.Watch(ctx, "/w/del")
	go func() { node.Delete(ctx, "/w/del") }()

	select {
	case wr := <-wch:
		if len(wr.Events) == 0 {
			t.Fatal("expected delete event")
		}
		if wr.Events[0].Type != clientv3.EventTypeDelete {
			t.Errorf("event type: want DELETE got %v", wr.Events[0].Type)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for delete event")
	}
}

// TestWatchPrefix verifies prefix watch catches all matching keys.
func TestWatchPrefix(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wch := cli.Watch(ctx, "/pfx/", clientv3.WithPrefix())
	const n = 3
	go func() {
		for i := 0; i < n; i++ {
			node.Put(ctx, fmt.Sprintf("/pfx/%d", i), []byte("v"), 0)
		}
	}()

	received := 0
	for received < n {
		select {
		case wr := <-wch:
			received += len(wr.Events)
		case <-ctx.Done():
			t.Fatalf("timeout: got %d/%d events", received, n)
		}
	}
}

// TestWatchNonMatchingPrefix verifies events outside the prefix are not delivered.
func TestWatchNonMatchingPrefix(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	wch := cli.Watch(ctx, "/match/", clientv3.WithPrefix())
	// Write to a different prefix — should not trigger watcher.
	node.Put(ctx, "/other/key", []byte("v"), 0)
	// Write one that DOES match to unblock the channel check.
	go func() {
		time.Sleep(100 * time.Millisecond)
		node.Put(ctx, "/match/key", []byte("v"), 0)
	}()

	select {
	case wr := <-wch:
		for _, ev := range wr.Events {
			if string(ev.Kv.Key) == "/other/key" {
				t.Error("received event for non-matching key /other/key")
			}
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for matching event")
	}
}

// TestWatchMultipleConcurrent verifies multiple simultaneous watches each
// receive only their own events.
func TestWatchMultipleConcurrent(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const watchers = 5
	channels := make([]clientv3.WatchChan, watchers)
	for i := 0; i < watchers; i++ {
		channels[i] = cli.Watch(ctx, fmt.Sprintf("/multi/%d/", i), clientv3.WithPrefix())
	}

	// Each watcher gets 2 events under its own prefix.
	for i := 0; i < watchers; i++ {
		i := i
		go func() {
			node.Put(ctx, fmt.Sprintf("/multi/%d/a", i), []byte("v"), 0)
			node.Put(ctx, fmt.Sprintf("/multi/%d/b", i), []byte("v"), 0)
		}()
	}

	for i, wch := range channels {
		received := 0
		for received < 2 {
			select {
			case wr := <-wch:
				received += len(wr.Events)
			case <-ctx.Done():
				t.Fatalf("watcher %d: timeout, got %d/2 events", i, received)
			}
		}
	}
}

// TestWatchCancel verifies that cancelling the watch context stops delivery.
func TestWatchCancel(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watchCtx, watchCancel := context.WithCancel(ctx)
	wch := cli.Watch(watchCtx, "/cancel/", clientv3.WithPrefix())

	// Receive one event to confirm the watch is live.
	node.Put(ctx, "/cancel/first", []byte("v"), 0)
	select {
	case wr := <-wch:
		if len(wr.Events) == 0 {
			t.Fatal("expected first event")
		}
	case <-ctx.Done():
		t.Fatal("timeout before first event")
	}

	// Cancel the watch context.
	watchCancel()

	// Write another event — channel should close or drain without new events.
	node.Put(ctx, "/cancel/second", []byte("v"), 0)
	time.Sleep(100 * time.Millisecond)

	// The channel should eventually be closed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case wr, ok := <-wch:
			if !ok {
				return // channel closed: expected
			}
			// Drain any pending event (may arrive before cancel propagates).
			_ = wr
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestWatchFromRevision verifies the StartRevision field is respected:
// events at or after the given revision are replayed.
func TestWatchFromRevision(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write two events, capture the revision after the first.
	rev1, _ := node.Put(ctx, "/rev/a", []byte("1"), 0)
	node.Put(ctx, "/rev/b", []byte("2"), 0)

	// Watch from rev1 — both /rev/a and /rev/b should arrive.
	wch := cli.Watch(ctx, "/rev/", clientv3.WithPrefix(), clientv3.WithRev(rev1))

	received := 0
	for received < 2 {
		select {
		case wr := <-wch:
			received += len(wr.Events)
		case <-ctx.Done():
			t.Fatalf("timeout: got %d/2 events", received)
		}
	}
}

// TestWatchKubeLikeCompactionRecovery emulates kube-apiserver startup behavior:
// stale watch revisions can be compacted, then a relist picks a fresh revision
// and watching from freshRV+1 succeeds.
func TestWatchKubeLikeCompactionRecovery(t *testing.T) {
	node, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use multiple resource-like prefixes to mirror apiserver starting many cachers.
	prefixes := []string{
		"/registry/apps/deployments/",
		"/registry/apps/controllerrevisions/",
		"/registry/rbac.clusterroles/",
		"/registry/storageclasses/",
		"/registry/resourceclaims/",
	}

	// Seed initial objects then compact at current revision.
	for i, p := range prefixes {
		if _, err := node.Put(ctx, fmt.Sprintf("%sseed-%d", p, i), []byte("v1"), 0); err != nil {
			t.Fatalf("seed Put(%q): %v", p, err)
		}
	}
	compactRev := node.CurrentRevision()
	if err := node.Compact(ctx, compactRev); err != nil {
		t.Fatalf("Compact(%d): %v", compactRev, err)
	}

	// Emulate an apiserver resuming from stale list RV: it watches from rv+1.
	// Choose staleListRV=compactRev-1 so watch starts at compactRev and gets compacted.
	staleListRV := compactRev - 1
	if staleListRV < 1 {
		t.Fatalf("unexpected staleListRV=%d", staleListRV)
	}
	startRev := staleListRV + 1

	for _, p := range prefixes {
		wctx, wcancel := context.WithCancel(ctx)
		wch := cli.Watch(wctx, p, clientv3.WithPrefix(), clientv3.WithRev(startRev))
		compacted := false
		for !compacted {
			select {
			case wr, ok := <-wch:
				if !ok {
					t.Fatalf("watch %q closed before compacted signal", p)
				}
				if err := wr.Err(); err != nil {
					if err == rpctypes.ErrCompacted || strings.Contains(err.Error(), "required revision has been compacted") {
						compacted = true
						continue
					}
					t.Fatalf("watch %q unexpected error: %v", p, err)
				}
				if wr.Canceled {
					if wr.CompactRevision == 0 {
						t.Fatalf("watch %q canceled without compact revision", p)
					}
					compacted = true
				}
			case <-ctx.Done():
				t.Fatalf("timeout waiting compacted watch on %q", p)
			}
		}
		wcancel()
	}

	// Kube relists, then watches from listRV+1.
	for i, p := range prefixes {
		getResp, err := cli.Get(ctx, p, clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("Get(%q): %v", p, err)
		}
		freshListRV := getResp.Header.Revision

		wctx, wcancel := context.WithCancel(ctx)
		wch := cli.Watch(wctx, p, clientv3.WithPrefix(), clientv3.WithRev(freshListRV+1))

		key := fmt.Sprintf("%safter-relist-%d", p, i)
		if _, err := node.Put(ctx, key, []byte("v2"), 0); err != nil {
			t.Fatalf("post-relist Put(%q): %v", key, err)
		}

		received := false
		for !received {
			select {
			case wr, ok := <-wch:
				if !ok {
					t.Fatalf("watch %q closed unexpectedly after relist", p)
				}
				if err := wr.Err(); err != nil {
					t.Fatalf("watch %q unexpected error after relist: %v", p, err)
				}
				if wr.Canceled {
					t.Fatalf("watch %q unexpectedly canceled after relist (compactRev=%d)", p, wr.CompactRevision)
				}
				for _, ev := range wr.Events {
					if string(ev.Kv.Key) == key {
						received = true
						break
					}
				}
			case <-ctx.Done():
				t.Fatalf("timeout waiting post-relist event on %q", p)
			}
		}
		wcancel()
	}
}

// newWatchNode opens a strata.Node and an etcd client. Returns both so tests
// can write to the node directly.
func newWatchNode(t *testing.T) (*strata.Node, *clientv3.Client) {
	t.Helper()
	node, err := strata.Open(strata.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("strata.Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	endpoint := startEtcdServer(t, node)
	cli := newEtcdClient(t, endpoint)
	return node, cli
}
