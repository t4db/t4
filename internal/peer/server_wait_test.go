package peer

import (
	"context"
	"testing"
	"time"

	"github.com/strata-db/strata/internal/wal"
)

func TestWaitForFollowersNoneReturnsImmediately(t *testing.T) {
	srv := NewServer(16)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *wal.Entry)
	srv.followers["f2"] = make(chan *wal.Entry)
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := srv.WaitForFollowers(ctx, 10, WaitNone); err != nil {
		t.Fatalf("WaitForFollowers(none): %v", err)
	}
}

func TestWaitForFollowersQuorumNeedsOneOfTwoFollowers(t *testing.T) {
	srv := NewServer(16)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *wal.Entry)
	srv.followers["f2"] = make(chan *wal.Entry)
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- srv.WaitForFollowers(ctx, 7, WaitQuorum)
	}()

	select {
	case err := <-done:
		t.Fatalf("WaitForFollowers(quorum) returned too early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	srv.mu.Lock()
	srv.followerAckRevs["f1"] = 7
	srv.mu.Unlock()
	srv.notifyACK()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForFollowers(quorum): %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for quorum completion")
	}
}

func TestWaitForFollowersAllNeedsEveryConnectedFollower(t *testing.T) {
	srv := NewServer(16)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *wal.Entry)
	srv.followers["f2"] = make(chan *wal.Entry)
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- srv.WaitForFollowers(ctx, 9, WaitAll)
	}()

	srv.mu.Lock()
	srv.followerAckRevs["f1"] = 9
	srv.mu.Unlock()
	srv.notifyACK()

	select {
	case err := <-done:
		t.Fatalf("WaitForFollowers(all) returned before every follower acked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	srv.mu.Lock()
	srv.followerAckRevs["f2"] = 9
	srv.mu.Unlock()
	srv.notifyACK()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForFollowers(all): %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for all followers")
	}
}

func TestWaitForFollowersAllReturnsWhenRemainingFollowersDisconnect(t *testing.T) {
	srv := NewServer(16)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *wal.Entry)
	srv.followers["f2"] = make(chan *wal.Entry)
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- srv.WaitForFollowers(ctx, 11, WaitAll)
	}()

	srv.mu.Lock()
	delete(srv.followers, "f1")
	delete(srv.followers, "f2")
	srv.mu.Unlock()
	srv.notifyACK()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForFollowers(all) after disconnects: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for disconnect completion")
	}
}
