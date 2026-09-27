package peer_test

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/internal/wal"
)

// stallingProxy forwards TCP traffic to target until stall is called; from
// then on it silently stops forwarding while keeping every connection open,
// like a network partition that drops packets: neither side sees an error.
type stallingProxy struct {
	lis     net.Listener
	target  string
	stalled atomic.Bool

	mu    sync.Mutex
	conns []net.Conn
}

func newStallingProxy(t *testing.T, target string) *stallingProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallingProxy{lis: lis, target: target}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *stallingProxy) Addr() string { return p.lis.Addr().String() }
func (p *stallingProxy) stall()       { p.stalled.Store(true) }

// close drops every connection so that neither end waits on a stalled one.
func (p *stallingProxy) close() {
	_ = p.lis.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func (p *stallingProxy) serve() {
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return
		}
		dst, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, dst)
		p.mu.Unlock()
		go p.pipe(dst, c)
		go p.pipe(c, dst)
	}
}

func (p *stallingProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		for p.stalled.Load() {
			time.Sleep(50 * time.Millisecond) // hold the bytes, keep the connection
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				_ = dst.Close()
			}
			return
		}
	}
}

func waitConnected(t *testing.T, srv *peer.Server, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for srv.ConnectedFollowers() != n {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d connected followers, have %d", n, srv.ConnectedFollowers())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestFollowerDetectsSilentLeader: a partition that drops packets breaks no
// connection, so without heartbeats the follower waits on its stream forever
// and never looks for a new leader.
func TestFollowerDetectsSilentLeader(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	proxy := newStallingProxy(t, startServer(t, srv))
	cli := peer.NewClient(proxy.Addr(), "follower-1", 2, nil, nil, nil)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	errC := make(chan error, 1)
	go func() { errC <- cli.Follow(ctx, 1, noopWAL, func([]wal.Entry) error { return nil }) }()
	waitConnected(t, srv, 1)
	time.Sleep(2 * peer.HeartbeatInterval) // let the first heartbeats through

	proxy.stall()
	start := time.Now()
	select {
	case err := <-errC:
		if !peer.IsLeaderUnreachable(err) {
			t.Fatalf("Follow returned %v, want ErrLeaderUnreachable", err)
		}
		t.Logf("follower gave up on the silent leader after %v", time.Since(start).Round(time.Millisecond))
	case <-ctx.Done():
		t.Fatal("follower never noticed that the leader went silent")
	}
}

// TestLeaderDropsSilentFollower: the leader ends the stream of a follower it
// no longer hears from, which releases writes waiting for that follower.
func TestLeaderDropsSilentFollower(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	proxy := newStallingProxy(t, startServer(t, srv))
	cli := peer.NewClient(proxy.Addr(), "follower-1", 0, nil, nil, nil)
	defer cli.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = cli.Follow(ctx, 1, noopWAL, func([]wal.Entry) error { return nil }) }()
	waitConnected(t, srv, 1)

	proxy.stall()
	srv.Broadcast(makeEntry(1))
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 3*peer.HeartbeatTimeout)
	defer waitCancel()
	if err := srv.WaitForFollowers(waitCtx, 1, peer.WaitAll); err != nil {
		t.Fatalf("write still waiting for a silent follower: %v", err)
	}
	if n := srv.ConnectedFollowers(); n != 0 {
		t.Fatalf("silent follower still counted as connected (%d)", n)
	}
	select {
	case <-srv.DisconnectC:
	default:
		t.Fatal("dropping the silent follower did not signal DisconnectC")
	}
}

// oldLeader hides the follower's heartbeat request, as a leader that
// predates heartbeats would.
type oldLeader struct{ *peer.Server }

func (s oldLeader) Follow(req *peer.FollowRequest, stream peer.WalStream_FollowServer) error {
	req.Heartbeats = false
	return s.Server.Follow(req, stream)
}

// TestFollowerKeepsIdleStreamToOldLeader: a leader that sends no heartbeats
// is not taken for a silent one.
func TestFollowerKeepsIdleStreamToOldLeader(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	addr := startServer(t, oldLeader{srv})
	cli := peer.NewClient(addr, "follower-1", 1, nil, nil, nil)
	defer cli.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	received := make(chan wal.Entry, 1)
	errC := make(chan error, 1)
	go func() {
		errC <- cli.Follow(ctx, 1, noopWAL, func(es []wal.Entry) error {
			for _, e := range es {
				received <- e
			}
			return nil
		})
	}()
	waitConnected(t, srv, 1)

	time.Sleep(2 * peer.HeartbeatTimeout)
	select {
	case err := <-errC:
		t.Fatalf("follower left an idle leader without heartbeats: %v", err)
	default:
	}
	srv.Broadcast(makeEntry(1))
	srv.BroadcastCommit(1, 1)
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("entry not delivered on the long-idle stream")
	}
}

// TestLeaderDropsStalledFollower: a follower whose WAL writes stall keeps
// heartbeating, so it looks alive, but it stops acknowledging. The leader
// must drop it rather than keep writes waiting for it indefinitely.
func TestLeaderDropsStalledFollower(t *testing.T) {
	srv := peer.NewServer(1000, nil)
	cli := peer.NewClient(startServer(t, srv), "follower-1", 0, nil, nil, nil)
	defer cli.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stall := make(chan struct{})
	defer close(stall)
	go func() {
		_ = cli.Follow(ctx, 1, func([]wal.Entry) error {
			select { // the disk hangs
			case <-stall:
			case <-ctx.Done():
			}
			return ctx.Err()
		}, func([]wal.Entry) error { return nil })
	}()
	waitConnected(t, srv, 1)
	time.Sleep(2 * peer.HeartbeatInterval) // heartbeats flowing

	srv.Broadcast(makeEntry(1))
	srv.BroadcastCommit(1, 1)
	waitCtx, waitCancel := context.WithTimeout(t.Context(), peer.AckProgressTimeout+3*time.Second)
	defer waitCancel()
	start := time.Now()
	if err := srv.WaitForFollowers(waitCtx, 1, peer.WaitAll); err != nil {
		t.Fatalf("write still waiting for a stalled follower: %v", err)
	}
	t.Logf("stalled follower dropped after %v", time.Since(start).Round(time.Millisecond))
}
