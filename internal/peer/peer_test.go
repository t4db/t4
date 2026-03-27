package peer_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/makhov/strata/internal/peer"
	"github.com/makhov/strata/internal/wal"
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

// TestStreamDelivery verifies that entries broadcast by the leader reach the follower.
func TestStreamDelivery(t *testing.T) {
	srv := peer.NewServer(1000)
	addr := startServer(t, srv)

	cli := peer.NewClient(addr, "follower-1", 3, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	received := make(chan wal.Entry, 16)
	go func() {
		cli.Follow(ctx, 1, func(e wal.Entry) error {
			received <- e
			return nil
		})
	}()

	// Give the follower a moment to connect.
	time.Sleep(100 * time.Millisecond)

	const n = 5
	for i := int64(1); i <= n; i++ {
		srv.Broadcast(makeEntry(i))
	}

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
	srv := peer.NewServer(1000)
	addr := startServer(t, srv)

	// Broadcast some entries before the follower connects.
	for i := int64(1); i <= 3; i++ {
		srv.Broadcast(makeEntry(i))
	}

	cli := peer.NewClient(addr, "follower-1", 3, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	received := make(chan wal.Entry, 16)
	go func() {
		cli.Follow(ctx, 1, func(e wal.Entry) error {
			received <- e
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
	srv := peer.NewServer(3) // tiny buffer: 3 entries max

	// Fill the buffer past capacity so old entries are evicted.
	for i := int64(1); i <= 10; i++ {
		srv.Broadcast(makeEntry(i))
	}

	addr := startServer(t, srv)
	cli := peer.NewClient(addr, "follower-1", 3, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		// Ask for revision 1, which has been evicted from the 3-entry buffer.
		errCh <- cli.Follow(ctx, 1, func(e wal.Entry) error { return nil })
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
	srv := peer.NewServer(1000)
	srv.SetForwardHandler(handler)
	addr := startServer(t, srv)

	cli := peer.NewClient(addr, "follower-1", 3, nil)
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
	cli := peer.NewClient("127.0.0.1:19999", "follower-1", 3, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err := cli.Follow(ctx, 1, func(e wal.Entry) error { return nil })
	if !peer.IsLeaderUnreachable(err) {
		t.Errorf("expected IsLeaderUnreachable, got: %v", err)
	}
}

// TestMultipleFollowers verifies fan-out to multiple concurrent followers.
func TestMultipleFollowers(t *testing.T) {
	srv := peer.NewServer(1000)
	addr := startServer(t, srv)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const nFollowers = 3
	received := make([]chan wal.Entry, nFollowers)
	for i := range received {
		received[i] = make(chan wal.Entry, 16)
		ch := received[i]
		cli := peer.NewClient(addr, fmt.Sprintf("follower-%d", i), 3, nil)
		go cli.Follow(ctx, 1, func(e wal.Entry) error {
			ch <- e
			return nil
		})
	}

	time.Sleep(150 * time.Millisecond) // let followers connect

	srv.Broadcast(makeEntry(1))
	srv.Broadcast(makeEntry(2))

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
	srv := peer.NewServer(1000)

	// Pre-populate buffer.
	for i := int64(1); i <= 5; i++ {
		srv.Broadcast(makeEntry(i))
	}

	addr := startServer(t, srv)
	cli := peer.NewClient(addr, "follower-1", 3, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var revisions []int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		cli.Follow(ctx, 1, func(e wal.Entry) error {
			revisions = append(revisions, e.Revision)
			if e.Revision >= 6 {
				cancel() // we have enough
			}
			return nil
		})
	}()

	// Send more entries concurrently.
	time.Sleep(50 * time.Millisecond)
	for i := int64(6); i <= 8; i++ {
		srv.Broadcast(makeEntry(i))
	}

	<-done

	// Check monotonically increasing, no duplicates.
	for i := 1; i < len(revisions); i++ {
		if revisions[i] <= revisions[i-1] {
			t.Errorf("non-monotonic revisions at index %d: %v -> %v", i, revisions[i-1], revisions[i])
		}
	}
}
