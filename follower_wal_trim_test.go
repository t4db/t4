package t4

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// A follower deletes the local WAL segments the leader has uploaded (#439);
// before, they accumulated for as long as it stayed a follower.
func TestFollowerTrimsUploadedWAL(t *testing.T) {
	old := followerWALTrimInterval
	followerWALTrimInterval = 100 * time.Millisecond
	t.Cleanup(func() { followerWALTrimInterval = old })

	store := object.NewMem()
	open := func(id string) (*Node, string) {
		dir, addr := t.TempDir(), freeLocalAddr(t)
		n, err := Open(Config{
			DataDir: dir, ObjectStore: store, NodeID: id,
			PeerListenAddr: addr, AdvertisePeerAddr: addr,
			SegmentMaxAge: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n, dir
	}
	leader, _ := open("a")
	waitLeader(t, leader)
	follower, fdir := open("b")
	if follower.IsLeader() {
		t.Fatal("second node became leader")
	}

	ctx := context.Background()
	segments := func() int {
		paths, err := wal.LocalSegments(filepath.Join(fdir, "wal"))
		if err != nil {
			t.Fatal(err)
		}
		return len(paths)
	}
	// Writes spread over many segment ages, so the follower seals many.
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		if _, err := leader.Put(ctx, fmt.Sprintf("/trim/%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Keep writing a little so the follower applies batches after the
	// leader's uploads catch up, which is when it trims.
	wait := time.Now().Add(10 * time.Second)
	for segments() > 3 && time.Now().Before(wait) {
		if _, err := leader.Put(ctx, "/trim/tick", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := segments(); n > 3 {
		t.Errorf("follower keeps %d local WAL segments; uploaded ones should be deleted", n)
	}
}
