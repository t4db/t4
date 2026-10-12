package t4

import (
	"context"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"
)

// TestNodeDoneWhenLeadershipTaken checks that a leader whose lock another
// node took over reports that it stopped. It cannot serve again without a
// restart, so whatever runs it must learn of it and restart it.
func TestNodeDoneWhenLeadershipTaken(t *testing.T) {
	store := object.NewMem()
	addr := freeAddrLocal(t)
	n, err := Open(Config{
		DataDir:             t.TempDir(),
		ObjectStore:         store,
		NodeID:              "a",
		PeerListenAddr:      addr,
		AdvertisePeerAddr:   addr,
		LeaderWatchInterval: minLeaderWatchInterval,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	waitForLeaderNodeLocal(t, []*Node{n}, 10*time.Second)
	select {
	case <-n.Done():
		t.Fatalf("leader reports it stopped: %v", n.Err())
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	intruder := election.NewLock(store, "b", "127.0.0.1:1")
	big := election.Fence{Rev: 1 << 40, Seq: 1 << 40, Term: n.term}
	if _, won, err := intruder.TakeOver(ctx, n.term, big, func(*election.LockRecord) bool { return true }); err != nil || !won {
		t.Fatalf("TakeOver: won=%v err=%v", won, err)
	}

	select {
	case <-n.Done():
	case <-ctx.Done():
		t.Fatal("the superseded leader never reported that it stopped")
	}
	if err := n.Err(); err == nil {
		t.Fatal("Err is nil after the node stopped on its own")
	}
}

// TestNodeDoneAfterClose checks that Close also closes Done, without an error:
// the node did not stop on its own.
func TestNodeDoneAfterClose(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after Close")
	}
	if err := n.Err(); err != nil {
		t.Fatalf("Err after Close = %v, want nil", err)
	}
}
