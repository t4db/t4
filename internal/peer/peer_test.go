package peer_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/internal/wal"
)

// startServer starts a peer gRPC server on a random port, registers srv, and
// returns the listening address and a cleanup function.
func startServer(t *testing.T, srv peer.WalStreamServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.ForceServerCodec(peer.Codec{}))
	peer.RegisterWalStreamServer(s, srv)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func makeEntry(rev int64) *wal.Entry {
	return &wal.Entry{
		Revision: rev, Term: 1, Op: wal.OpCreate,
		Key: fmt.Sprintf("key-%d", rev), Value: []byte(fmt.Sprintf("val-%d", rev)),
		CreateRevision: rev,
	}
}

func noopWAL([]wal.Entry) error { return nil }

// TestStreamDelivery verifies that entries broadcast by the leader reach the follower.
func TestStreamDelivery(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	addr := startServer(t, srv)

	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	received := make(chan wal.Entry, 16)
	go func() {
		cli.Follow(ctx, 1, noopWAL, func(entries []wal.Entry) error {
			for _, e := range entries {
				received <- e
			}
			return nil
		})
	}()

	// Give the follower a moment to connect.
	time.Sleep(100 * time.Millisecond)

	const n = 5
	for i := int64(1); i <= n; i++ {
		srv.Broadcast(makeEntry(i))
	}
	srv.BroadcastCommit(n)

	for i := int64(1); i <= n; i++ {
		select {
		case e := <-received:
			if e.Revision != i {
				t.Errorf("event %d: got revision %d", i, e.Revision)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for entry %d", i)
		}
	}
}

// TestCatchUp verifies that a follower connecting after entries were broadcast
// receives the buffered entries first, then live ones.
func TestCatchUp(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	addr := startServer(t, srv)

	// Broadcast some entries before the follower connects.
	for i := int64(1); i <= 3; i++ {
		srv.Broadcast(makeEntry(i))
	}

	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	received := make(chan wal.Entry, 16)
	go func() {
		cli.Follow(ctx, 1, noopWAL, func(entries []wal.Entry) error {
			for _, e := range entries {
				received <- e
			}
			return nil
		})
	}()

	// Collect the 3 buffered entries.
	for i := int64(1); i <= 3; i++ {
		select {
		case e := <-received:
			if e.Revision != i {
				t.Errorf("catch-up entry %d: got revision %d", i, e.Revision)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for catch-up entry %d", i)
		}
	}

	// Broadcast a live entry and verify it arrives.
	srv.Broadcast(makeEntry(4))
	srv.BroadcastCommit(4)
	select {
	case e := <-received:
		if e.Revision != 4 {
			t.Errorf("live entry: got revision %d", e.Revision)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for live entry")
	}
}

// TestResyncRequired verifies that a follower whose fromRevision is too old
// receives ErrResyncRequired rather than hanging or getting partial data.
func TestResyncRequired(t *testing.T) {
	srv := peer.NewServer(3, nil) // tiny buffer: 3 entries max

	// Fill the buffer past capacity so old entries are evicted.
	for i := int64(1); i <= 10; i++ {
		srv.Broadcast(makeEntry(i))
	}

	addr := startServer(t, srv)
	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		// Ask for revision 1, which has been evicted from the 3-entry buffer.
		errCh <- cli.Follow(ctx, 1, noopWAL, func([]wal.Entry) error { return nil })
	}()

	select {
	case err := <-errCh:
		if !peer.IsResyncRequired(err) {
			t.Errorf("expected IsResyncRequired, got: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout — expected resync error")
	}
}

// TestForwardWrite verifies that a follower can forward a write to the leader
// and receive the correct response, including error propagation.
func TestForwardWrite(t *testing.T) {
	// Stub ForwardHandler that records calls and returns canned responses.
	handler := &stubForwardHandler{}
	srv := peer.NewServer(1000, nil)
	srv.SetForwardHandler(handler)
	addr := startServer(t, srv)

	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)
	defer cli.Close()

	ctx := t.Context()

	// Successful Put.
	handler.resp = &peer.ForwardResponse{Revision: 42, Succeeded: true}
	resp, err := cli.ForwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardPut, Key: "/k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("ForwardWrite: %v", err)
	}
	if resp.Revision != 42 {
		t.Errorf("revision: want 42 got %d", resp.Revision)
	}
	if handler.lastReq.Key != "/k" {
		t.Errorf("key not forwarded: got %q", handler.lastReq.Key)
	}

	// Application error (key exists).
	handler.resp = &peer.ForwardResponse{ErrCode: "key_exists"}
	resp, err = cli.ForwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardCreate, Key: "/k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("ForwardWrite (key_exists): %v", err)
	}
	if resp.ErrCode != "key_exists" {
		t.Errorf("expected key_exists err code, got %q", resp.ErrCode)
	}
}

type stubForwardHandler struct {
	lastReq *peer.ForwardRequest
	resp    *peer.ForwardResponse
}

func (h *stubForwardHandler) HandleForward(_ context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	h.lastReq = req
	return h.resp, nil
}

// TestLeaderUnreachable verifies that Follow returns ErrLeaderUnreachable
// after maxRetries consecutive connection failures.
func TestLeaderUnreachable(t *testing.T) {
	// Point at a port where nothing is listening.
	cli := peer.NewClient("127.0.0.1:19999", "follower-1", 3, nil, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err := cli.Follow(ctx, 1, noopWAL, func([]wal.Entry) error { return nil })
	if !peer.IsLeaderUnreachable(err) {
		t.Errorf("expected IsLeaderUnreachable, got: %v", err)
	}
}

// TestMultipleFollowers verifies fan-out to multiple concurrent followers.
func TestMultipleFollowers(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	addr := startServer(t, srv)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const nFollowers = 3
	received := make([]chan wal.Entry, nFollowers)
	for i := range received {
		received[i] = make(chan wal.Entry, 16)
		ch := received[i]
		cli := peer.NewClient(addr, fmt.Sprintf("follower-%d", i), 3, nil, nil)
		go cli.Follow(ctx, 1, noopWAL, func(entries []wal.Entry) error {
			for _, e := range entries {
				ch <- e
			}
			return nil
		})
	}

	time.Sleep(150 * time.Millisecond) // let followers connect

	srv.Broadcast(makeEntry(1))
	srv.Broadcast(makeEntry(2))
	srv.BroadcastCommit(2)

	for i, ch := range received {
		for rev := int64(1); rev <= 2; rev++ {
			select {
			case e := <-ch:
				if e.Revision != rev {
					t.Errorf("follower %d rev %d: got %d", i, rev, e.Revision)
				}
			case <-ctx.Done():
				t.Fatalf("follower %d: timeout for rev %d", i, rev)
			}
		}
	}
}

// TestNoDuplicatesOnCatchUp ensures entries in both the snapshot and the live
// stream are not delivered twice.
func TestNoDuplicatesOnCatchUp(t *testing.T) {
	srv := peer.NewServer(1000, nil)

	// Pre-populate buffer.
	for i := int64(1); i <= 5; i++ {
		srv.Broadcast(makeEntry(i))
	}

	addr := startServer(t, srv)
	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var revisions []int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		cli.Follow(ctx, 1, noopWAL, func(entries []wal.Entry) error {
			for _, e := range entries {
				revisions = append(revisions, e.Revision)
				if e.Revision >= 6 {
					cancel() // we have enough
				}
			}
			return nil
		})
	}()

	// Send more entries concurrently.
	time.Sleep(50 * time.Millisecond)
	for i := int64(6); i <= 8; i++ {
		srv.Broadcast(makeEntry(i))
	}
	srv.BroadcastCommit(8)

	<-done

	// Check monotonically increasing, no duplicates.
	for i := 1; i < len(revisions); i++ {
		if revisions[i] <= revisions[i-1] {
			t.Errorf("non-monotonic revisions at index %d: %v -> %v", i, revisions[i-1], revisions[i])
		}
	}
}

func TestFollowerAppliesOnlyAfterCommit(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	addr := startServer(t, srv)

	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	applied := make(chan wal.Entry, 4)
	go func() {
		_ = cli.Follow(ctx, 1, noopWAL, func(entries []wal.Entry) error {
			for _, e := range entries {
				applied <- e
			}
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	srv.Broadcast(makeEntry(1))

	select {
	case e := <-applied:
		t.Fatalf("entry applied before commit: rev=%d", e.Revision)
	case <-time.After(200 * time.Millisecond):
	}

	srv.BroadcastCommit(1)
	select {
	case e := <-applied:
		if e.Revision != 1 {
			t.Fatalf("applied wrong revision: got %d", e.Revision)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for committed entry")
	}
}

func TestCommitWithoutStagedEntryTriggersResyncRequired(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	addr := startServer(t, srv)

	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- cli.Follow(ctx, 1, noopWAL, func([]wal.Entry) error { return nil })
	}()

	time.Sleep(100 * time.Millisecond)
	srv.BroadcastCommit(1)

	select {
	case err := <-errCh:
		if !peer.IsResyncRequired(err) {
			t.Fatalf("expected resync_required, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for resync error")
	}
}

type flakyReplayServer struct {
	mu       sync.Mutex
	attempts int
}

func (s *flakyReplayServer) Follow(_ *peer.FollowRequest, stream peer.WalStream_FollowServer) error {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()

	switch attempt {
	case 1:
		// First attempt: send staged-but-uncommitted entries, then drop the stream.
		for _, rev := range []int64{1, 2} {
			if err := stream.Send(peer.EntryToMsg(makeEntry(rev))); err != nil {
				return err
			}
		}
		return status.Error(codes.Unavailable, "injected_disconnect")
	case 2:
		// Replay the same entries after reconnect, then send the commit marker.
		for _, rev := range []int64{1, 2} {
			if err := stream.Send(peer.EntryToMsg(makeEntry(rev))); err != nil {
				return err
			}
		}
		if err := stream.Send(&peer.WalEntryMsg{Commit: true, CommitRevision: 2}); err != nil {
			return err
		}
		// Wait for the follower ACK so the client can complete normally.
		ack := new(peer.AckMsg)
		if err := stream.RecvMsg(ack); err != nil {
			return err
		}
		if ack.Revision != 2 {
			return fmt.Errorf("got ack revision %d, want 2", ack.Revision)
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	default:
		return status.Error(codes.Unavailable, "unexpected_extra_attempt")
	}
}

func (s *flakyReplayServer) Forward(context.Context, *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	return nil, status.Error(codes.Unimplemented, "unused")
}

func (s *flakyReplayServer) GoodBye(context.Context, *peer.GoodByeRequest) (*peer.GoodByeResponse, error) {
	return &peer.GoodByeResponse{}, nil
}

func TestReconnectReplaysUncommittedEntriesOnlyOnce(t *testing.T) {
	addr := startServer(t, &flakyReplayServer{})

	cli := peer.NewClient(addr, "follower-1", 3, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		applied  []int64
		walCalls int
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- cli.Follow(
			ctx,
			1,
			func(entries []wal.Entry) error {
				mu.Lock()
				walCalls++
				mu.Unlock()
				return nil
			},
			func(entries []wal.Entry) error {
				mu.Lock()
				defer mu.Unlock()
				for _, e := range entries {
					applied = append(applied, e.Revision)
				}
				cancel()
				return nil
			},
		)
	}()

	select {
	case err := <-errCh:
		if err != context.Canceled && err != context.DeadlineExceeded {
			t.Fatalf("Follow: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for follow to finish")
	}

	mu.Lock()
	defer mu.Unlock()
	if walCalls != 1 {
		t.Fatalf("walFn called %d times, want 1", walCalls)
	}
	if len(applied) != 2 || applied[0] != 1 || applied[1] != 2 {
		t.Fatalf("applied revisions = %v, want [1 2]", applied)
	}
}
