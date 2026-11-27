package t4

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestLinearizableReadNeedsTheLeaderLease pins the guard that keeps a leader
// which can no longer renew its lock from answering reads out of its own
// state. Another node takes over once the lock goes unrenewed, so such a read
// can be older than writes that node has already acknowledged. Writes are
// refused by the same check; this covers the read a client sends straight to
// the leader, which reaches neither the write path nor the follower read
// index.
func TestLinearizableReadNeedsTheLeaderLease(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	ctx := context.Background()

	if _, err := n.Put(ctx, "/k", []byte("v"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}

	// A single node answers reads without any lease: it is nobody's follower
	// and cannot be superseded.
	if _, err := n.LinearizableGet(ctx, "/k"); err != nil {
		t.Fatalf("single node refused a read: %v", err)
	}

	// As an elected leader whose last lock write has aged out, it must not.
	n.storeRole(roleLeader)
	n.lease.Store(&leaseGrant{start: time.Now().Add(-time.Hour)})
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"get", func() error { _, err := n.LinearizableGet(ctx, "/k"); return err }},
		{"exists", func() error { _, err := n.LinearizableExists(ctx, "/k"); return err }},
		{"list", func() error { _, err := n.LinearizableList(ctx, "/"); return err }},
	} {
		if err := tc.call(); !errors.Is(err, ErrNoLeader) {
			t.Errorf("%s with an expired lease: err = %v, want ErrNoLeader", tc.name, err)
		}
	}

	// A renewed lease serves again, so the check follows the lease rather than
	// latching.
	n.lease.Store(&leaseGrant{start: time.Now()})
	if _, err := n.LinearizableGet(ctx, "/k"); err != nil {
		t.Errorf("read refused after the lease was renewed: %v", err)
	}
}
