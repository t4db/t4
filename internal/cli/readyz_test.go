package cli

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/internal/testutil"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// startMetrics serves node's health endpoints and returns their base URL.
func startMetrics(t *testing.T, node *t4.Node) string {
	t.Helper()
	addr := testutil.FreeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go serveMetrics(ctx, addr, node, false)
	return "http://" + addr
}

// status GETs url, retrying only while the server is still coming up.
func status(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return resp.StatusCode
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReadyzSingleNode(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	url := startMetrics(t, node)
	if got := status(t, url+"/readyz"); got != http.StatusOK {
		t.Fatalf("serving node: /readyz = %d, want 200", got)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/readyz", "/readyz?exclude=linearizable_read"} {
		if got := status(t, url+path); got != http.StatusServiceUnavailable {
			t.Errorf("stopped node: %s = %d, want 503", path, got)
		}
	}
}

// A follower that cannot catch up to the leader is the case #416 reported:
// it has a local revision, so the old check called it ready, yet it cannot
// complete a linearizable read. Holding its apply step back reproduces that
// deterministically — the technique is from the reporter's reference fix.
func TestReadyzFollowerBehindLeader(t *testing.T) {
	store := object.NewMem()
	nodes := make([]*t4.Node, 3)
	urls := make([]string, 3)
	for i := range nodes {
		addr := testutil.FreeAddr(t)
		n, err := t4.Open(t4.Config{
			DataDir: t.TempDir(), ObjectStore: store, NodeID: fmt.Sprintf("readyz-%d", i),
			PeerListenAddr: addr, AdvertisePeerAddr: addr,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		nodes[i], urls[i] = n, startMetrics(t, n)
	}

	leader := -1
	for deadline := time.Now().Add(30 * time.Second); leader < 0; {
		for i, n := range nodes {
			if n.IsLeader() {
				leader = i
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Hold back the first follower to receive this key. The other follower
	// applies it, so the write still commits.
	release, held := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(entries []wal.Entry) {
		for _, e := range entries {
			if e.Key == "/readyz/held" {
				once.Do(func() { close(held); <-release })
				return
			}
		}
	}
	testhook.BeforeFollowerApply.Store(&hook)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); testhook.BeforeFollowerApply.Store(nil) })

	rev, err := nodes[leader].Put(ctx, "/readyz/held", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("no follower reached the apply hook")
	}
	behind := -1
	for i, n := range nodes {
		if i != leader && n.CurrentRevision() < rev {
			wait, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			if n.WaitForRevision(wait, rev) != nil {
				behind = i
			}
			stop()
		}
	}
	if behind < 0 {
		t.Fatal("no follower stayed behind the leader")
	}

	if got := status(t, urls[leader]+"/readyz"); got != http.StatusOK {
		t.Errorf("leader: /readyz = %d, want 200", got)
	}
	start := time.Now()
	if got := status(t, urls[behind]+"/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("follower behind the leader: /readyz = %d, want 503", got)
	}
	if took := time.Since(start); took > readinessTimeout+time.Second {
		t.Errorf("/readyz took %v; it must answer within its own %v bound", took, readinessTimeout)
	}
	// Liveness stays local, so a lagging follower is not restarted.
	if got := status(t, urls[behind]+"/healthz"); got != http.StatusOK {
		t.Errorf("follower behind the leader: /healthz = %d, want 200", got)
	}
	// etcd's opt-out drops the leader dependency.
	if got := status(t, urls[behind]+"/readyz?exclude=linearizable_read"); got != http.StatusOK {
		t.Errorf("follower behind the leader: /readyz?exclude=linearizable_read = %d, want 200", got)
	}

	unblock()
	if err := nodes[behind].WaitForRevision(ctx, rev); err != nil {
		t.Fatal(err)
	}
	if got := status(t, urls[behind]+"/readyz"); got != http.StatusOK {
		t.Errorf("follower caught up: /readyz = %d, want 200", got)
	}
}
