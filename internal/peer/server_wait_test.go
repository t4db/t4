package peer

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/t4db/t4/internal/wal"
)

func TestWaitForFollowersNoneReturnsImmediately(t *testing.T) {
	srv := NewServer(16, nil)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *WalEntryMsg)
	srv.followers["f2"] = make(chan *WalEntryMsg)
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := srv.WaitForFollowers(ctx, 10, WaitNone); err != nil {
		t.Fatalf("WaitForFollowers(none): %v", err)
	}
}

func TestWaitForFollowersQuorumNeedsOneOfTwoFollowers(t *testing.T) {
	srv := NewServer(16, nil)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *WalEntryMsg)
	srv.followers["f2"] = make(chan *WalEntryMsg)
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
	srv := NewServer(16, nil)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *WalEntryMsg)
	srv.followers["f2"] = make(chan *WalEntryMsg)
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
	srv := NewServer(16, nil)
	srv.mu.Lock()
	srv.followers["f1"] = make(chan *WalEntryMsg)
	srv.followers["f2"] = make(chan *WalEntryMsg)
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

type fakeFollowStream struct {
	ctx   context.Context
	sendC chan *WalEntryMsg
}

func newFakeFollowStream(ctx context.Context) *fakeFollowStream {
	return &fakeFollowStream{
		ctx:   ctx,
		sendC: make(chan *WalEntryMsg, 32),
	}
}

func (f *fakeFollowStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeFollowStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeFollowStream) SetTrailer(metadata.MD)       {}
func (f *fakeFollowStream) Context() context.Context     { return f.ctx }

func (f *fakeFollowStream) SendMsg(m interface{}) error {
	msg, _ := m.(*WalEntryMsg)
	return f.Send(msg)
}

func (f *fakeFollowStream) RecvMsg(_ interface{}) error {
	<-f.ctx.Done()
	return f.ctx.Err()
}

func (f *fakeFollowStream) Send(msg *WalEntryMsg) error {
	select {
	case f.sendC <- msg:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}

func waitForStreamMessage(t *testing.T, ch <-chan *WalEntryMsg, pred func(*WalEntryMsg) bool, timeout time.Duration, desc string) *WalEntryMsg {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-ch:
			if pred(msg) {
				return msg
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %s", desc)
		}
	}
}

func TestFollowCleanupDoesNotDropReplacementFollower(t *testing.T) {
	srv := NewServer(16, nil)
	for rev := int64(1); rev <= 3; rev++ {
		srv.Broadcast(&wal.Entry{
			Revision:       rev,
			Term:           1,
			Op:             wal.OpCreate,
			Key:            "/k",
			Value:          []byte("v"),
			CreateRevision: rev,
		})
	}
	srv.BroadcastCommit(1, 3)

	oldCtx, oldCancel := context.WithCancel(t.Context())
	defer oldCancel()
	oldStream := newFakeFollowStream(oldCtx)
	oldErrCh := make(chan error, 1)
	go func() {
		oldErrCh <- srv.Follow(&FollowRequest{FromRevision: 1, NodeID: "f1"}, oldStream)
	}()
	waitForStreamMessage(t, oldStream.sendC, func(msg *WalEntryMsg) bool {
		return !msg.Commit && msg.Revision == 1
	}, time.Second, "old follower snapshot")

	newCtx, newCancel := context.WithCancel(t.Context())
	defer newCancel()
	newStream := newFakeFollowStream(newCtx)
	newErrCh := make(chan error, 1)
	go func() {
		newErrCh <- srv.Follow(&FollowRequest{FromRevision: 2, NodeID: "f1"}, newStream)
	}()
	waitForStreamMessage(t, newStream.sendC, func(msg *WalEntryMsg) bool {
		return !msg.Commit && msg.Revision == 2
	}, time.Second, "replacement follower snapshot")

	// Now that the replacement stream is registered, let the original stream
	// exit. Its deferred cleanup must not delete the replacement follower.
	oldCancel()
	select {
	case <-oldErrCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for original stream to exit")
	}

	srv.Broadcast(&wal.Entry{
		Revision:       4,
		Term:           1,
		Op:             wal.OpCreate,
		Key:            "/k2",
		Value:          []byte("v"),
		CreateRevision: 4,
	})
	srv.BroadcastCommit(4, 4)

	waitForStreamMessage(t, newStream.sendC, func(msg *WalEntryMsg) bool {
		return !msg.Commit && msg.Revision == 4
	}, time.Second, "replacement follower live entry")
	waitForStreamMessage(t, newStream.sendC, func(msg *WalEntryMsg) bool {
		return msg.Commit && msg.CommitStartRevision == 4 && msg.CommitRevision == 4
	}, time.Second, "replacement follower live commit")

	newCancel()
	select {
	case <-newErrCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for replacement stream to exit")
	}
}
