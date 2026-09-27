package t4

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"
)

// lockWriteCounter counts writes to the leader lock.
type lockWriteCounter struct {
	object.ConditionalStore
	writes atomic.Int64
}

func (c *lockWriteCounter) count(key string) {
	if key == election.LockKey {
		c.writes.Add(1)
	}
}

func (c *lockWriteCounter) Put(ctx context.Context, key string, r io.Reader) error {
	c.count(key)
	return c.ConditionalStore.Put(ctx, key, r)
}

func (c *lockWriteCounter) PutIfMatch(ctx context.Context, key string, r io.Reader, etag string) error {
	c.count(key)
	return c.ConditionalStore.PutIfMatch(ctx, key, r, etag)
}

// openCountedCluster opens a 3-node cluster with default timing whose nodes
// reach the leader through blockable proxies, and counts lock writes.
func openCountedCluster(t *testing.T) ([]*Node, []*blockableProxyLocal, *lockWriteCounter) {
	t.Helper()
	store := &lockWriteCounter{ConditionalStore: object.NewMem()}
	nodes := make([]*Node, 3)
	proxies := make([]*blockableProxyLocal, 3)
	for i := range nodes {
		listen := freeAddrLocal(t)
		proxies[i] = newBlockableProxyLocal(t, listen)
		n, err := Open(Config{
			DataDir:           t.TempDir(),
			ObjectStore:       store,
			NodeID:            fmt.Sprintf("mode-node-%d", i),
			PeerListenAddr:    listen,
			AdvertisePeerAddr: proxies[i].Addr(),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	return nodes, proxies, store
}

func settleCluster(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/settle", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatal(err)
		}
	}
	return leader
}

// TestSlowRenewalWhileFollowersHeard: while the leader hears every follower
// it renews the lock every slowRenewInterval, not every 2 s.
func TestSlowRenewalWhileFollowersHeard(t *testing.T) {
	nodes, _, store := openCountedCluster(t)
	leader := settleCluster(t, nodes)
	time.Sleep(3 * time.Second) // leave the start-of-term renewals behind

	before := store.writes.Load()
	const window = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deadline := time.Now().Add(window)
	for i := 0; time.Now().Before(deadline); i++ {
		if _, err := leader.Put(ctx, fmt.Sprintf("/k%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Fast renewal would write the lock 5 times in 10 s.
	if n := store.writes.Load() - before; n > 1 {
		t.Fatalf("leader wrote the lock %d times in %v while hearing every follower, want at most 1", n, window)
	}
}

// TestFastRenewalWhileFollowersSilent: once followers stop hearing the
// leader, it renews every fastRenewInterval, so that followers that lost it
// find it alive.
func TestFastRenewalWhileFollowersSilent(t *testing.T) {
	nodes, proxies, store := openCountedCluster(t)
	leader := settleCluster(t, nodes)
	for i, n := range nodes {
		if n == leader {
			proxies[i].block()
		}
	}
	time.Sleep(3 * time.Second) // let the leader notice
	before := store.writes.Load()
	time.Sleep(6 * time.Second)
	if n := store.writes.Load() - before; n < 2 {
		t.Fatalf("leader wrote the lock %d times in 6 s with its followers gone, want fast renewal", n)
	}
	if !leader.IsLeader() {
		t.Fatal("leader lost leadership although it still reaches object storage")
	}
}

// TestFenceMovesAfterFollowersLeave: a write acknowledged after followers
// left must be covered by the lock's election fence, so that none of them,
// all lacking it, can win a takeover.
func TestFenceMovesAfterFollowersLeave(t *testing.T) {
	nodes, proxies, store := openCountedCluster(t)
	leader := settleCluster(t, nodes)
	for i, n := range nodes {
		if n == leader {
			proxies[i].block()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/after", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := election.NewLock(store, "reader", "").Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.CommittedRev < rev {
		t.Fatalf("write rev=%d acknowledged with the lock's fence at %d: a follower lacking it could take over", rev, rec.CommittedRev)
	}
}

// TestMayTakeOver checks the takeover rules on lock records.
func TestMayTakeOver(t *testing.T) {
	now := time.Now()
	record := func(term uint64, renewedAgo, ttl time.Duration) *election.LockRecord {
		start := now.Add(-renewedAgo)
		return &election.LockRecord{
			Term:           term,
			RenewedNano:    start.UnixNano(),
			ValidUntilNano: start.Add(ttl).UnixNano(),
			LastSeenNano:   start.Add(ttl - fastTTL).UnixNano(),
		}
	}
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	cases := []struct {
		name      string
		rec       *election.LockRecord
		heardTerm uint64
		heardAt   time.Time
		want      bool
	}{
		{"released", &election.LockRecord{Term: 3}, 0, time.Time{}, true},
		{"unknown, slow lock still valid", record(3, 30*time.Second, slowTTL), 0, time.Time{}, false},
		{"unknown, slow lock expired", record(3, 61*time.Second, slowTTL), 0, time.Time{}, true},
		{"unknown, fast lock renewed 5 s ago", record(3, 5*time.Second, fastTTL), 0, time.Time{}, false},
		{"known, heard just now", record(3, 30*time.Second, slowTTL), 3, ago(time.Second), false},
		{"known, silent for 8 s, lock renewed 30 s ago", record(3, 30*time.Second, slowTTL), 3, ago(8 * time.Second), true},
		{"known, silent for 8 s, lock renewed 1 s ago", record(3, time.Second, fastTTL), 3, ago(8 * time.Second), false},
		{"heard an older term: unknown", record(4, 30*time.Second, slowTTL), 3, ago(8 * time.Second), false},
		{"heard too long ago: unknown", record(3, 30*time.Second, slowTTL), 3, ago(followerKnownWindow + time.Second), false},
		{"record of an earlier release, fresh", &election.LockRecord{Term: 3, LastSeenNano: ago(time.Second).UnixNano()}, 0, time.Time{}, false},
		{"record of an earlier release, stale", &election.LockRecord{Term: 3, LastSeenNano: ago(7 * time.Second).UnixNano()}, 0, time.Time{}, true},
	}
	for _, tc := range cases {
		if got := mayTakeOver(tc.rec, tc.heardTerm, tc.heardAt, now); got != tc.want {
			t.Errorf("%s: mayTakeOver = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// staleLockOnce serves one read of the lock with its liveness fields pushed
// into the past, as if the holder renewed only after that read.
type staleLockOnce struct {
	object.ConditionalStore
	armed atomic.Bool
}

func (s *staleLockOnce) GetETag(ctx context.Context, key string) (*object.GetWithETag, error) {
	res, err := s.ConditionalStore.GetETag(ctx, key)
	if err != nil || key != election.LockKey || !s.armed.CompareAndSwap(true, false) {
		return res, err
	}
	defer func() { _ = res.Body.Close() }()
	var rec election.LockRecord
	if err := json.NewDecoder(res.Body).Decode(&rec); err != nil {
		return nil, err
	}
	old := time.Now().Add(-10 * time.Minute)
	rec.RenewedNano = old.UnixNano()
	rec.ValidUntilNano = old.Add(fastTTL).UnixNano()
	rec.LastSeenNano = old.UnixNano()
	b, _ := json.Marshal(&rec)
	return &object.GetWithETag{Body: io.NopCloser(bytes.NewReader(b)), ETag: res.ETag}, nil
}

// TestTakeOverChecksTheRecordItReplaces: the liveness check must apply to
// the lock record whose ETag the takeover's conditional write is based on.
// A candidate that saw a stale lock, while the holder renewed it before the
// takeover read it again, must not take over.
func TestTakeOverChecksTheRecordItReplaces(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 3)
	stores := make([]*staleLockOnce, 3)
	for i := range nodes {
		addr := freeAddrLocal(t)
		stores[i] = &staleLockOnce{ConditionalStore: shared}
		n, err := Open(Config{
			DataDir:           t.TempDir(),
			ObjectStore:       stores[i],
			NodeID:            fmt.Sprintf("recheck-node-%d", i),
			PeerListenAddr:    addr,
			AdvertisePeerAddr: addr,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := settleCluster(t, nodes)
	idx := 0
	for i, n := range nodes {
		if n != leader {
			idx = i
			break
		}
	}
	candidate := nodes[idx]
	// A candidate that never heard the leader goes by ValidUntil alone.
	candidate.leaderCli.Store(nil)
	stores[idx].armed.Store(true)
	lock := election.NewLock(stores[idx], candidate.cfg.NodeID, candidate.cfg.AdvertisePeerAddr)
	if _, promoted := candidate.attemptPromotion(candidate.bgCtx, lock, false); promoted {
		t.Fatal("candidate took over a lock whose holder renewed it after the candidate's liveness check")
	}
	if !leader.IsLeader() {
		t.Fatal("leader lost the lock")
	}
}
