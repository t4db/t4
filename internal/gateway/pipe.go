package gateway

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// errLoopbackClosed ends Accept and Dial once the loopback is torn down. It
// wraps net.ErrClosed so that callers which sort a deliberate shutdown from a
// real failure — as serving code does before it logs — need know nothing about
// this package.
var errLoopbackClosed = fmt.Errorf("gateway: loopback listener closed: %w", net.ErrClosed)

// Loopback carries gRPC calls from the JSON gateway to the gRPC server inside
// one process, over in-memory pipes rather than a socket.
//
// The gateway has to reach the gRPC server as a client, because the generated
// handlers that answer /v3/... speak to a *grpc.ClientConn. etcd solves this by
// dialing its own client port, which forces it to build a client TLS config
// that trusts its own certificate; it gets there by cloning the server config
// and setting InsecureSkipVerify (see embed/etcd.go, grpcGatewayDial). t4
// cannot follow: --client-tls-ca names the CA that verifies *clients*, which
// need not be the issuer of --client-tls-cert, and under mTLS a self-dial would
// also have to present a client certificate that no flag supplies.
//
// Going through memory avoids all of it, and costs nothing in authorization:
// the calls still cross the server's interceptor chain, so auth and RBAC apply
// exactly as they do to a remote client. What the hop skips is transport
// encryption on a connection that never leaves the process; callers still meet
// the gateway's own TLS listener, client certificate requirement included.
type Loopback struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

// NewLoopback returns a Loopback ready to be served and dialed.
func NewLoopback() *Loopback {
	return &Loopback{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

// Accept implements net.Listener. Pass the Loopback to grpc.Server.Serve
// alongside the real listener; a gRPC server may serve any number of them.
func (l *Loopback) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, errLoopbackClosed
	}
}

// Close implements net.Listener. It stops Accept and Dial but does not touch
// connections already handed out; grpc.Server.GracefulStop drains those.
func (l *Loopback) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

// Addr implements net.Listener.
func (l *Loopback) Addr() net.Addr { return loopbackAddr{} }

// Dial hands the server one end of a new pipe and returns the other. It is a
// grpc.WithContextDialer function; the address is ignored because there is only
// ever one peer.
//
// The handoff is unbuffered, so Dial blocks until the server's Accept takes the
// connection. That is what makes a dial fail cleanly when the server is gone
// rather than succeed into a connection nothing reads.
func (l *Loopback) Dial(ctx context.Context, _ string) (net.Conn, error) {
	server, client := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		_ = server.Close()
		_ = client.Close()
		return nil, errLoopbackClosed
	case <-ctx.Done():
		_ = server.Close()
		_ = client.Close()
		return nil, ctx.Err()
	}
}

// ClientConn returns a connection to the server on the other end of l.
//
// The receive limit is unbounded because the gateway is a relay: a response the
// gRPC server was willing to send is a response the gateway has to be able to
// read, and the size a client may ask for is already bounded on the way in by
// the server's own MaxRecvMsgSize. etcd sets the same limit for the same reason.
func (l *Loopback) ClientConn() (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///loopback",
		grpc.WithContextDialer(l.Dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	)
}

// loopbackAddr names the in-memory transport. Nothing in t4 derives identity or
// policy from a peer address, so it only has to be stable and legible in logs.
type loopbackAddr struct{}

func (loopbackAddr) Network() string { return "pipe" }
func (loopbackAddr) String() string  { return "t4-gateway-loopback" }
