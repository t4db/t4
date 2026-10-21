// Package dbmux serves several databases on one client listener. It
// completes the TLS handshake itself, requiring a verified client
// certificate, and hands each connection to the database named by the
// certificate's Common Name. A client can therefore only reach the database
// its certificate was issued for.
//
// Each database is served by its own gRPC server on the listener returned by
// Listener, with Credentials as its transport credentials.
package dbmux

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

// handshakeTimeout bounds how long a connection may take to complete the TLS
// handshake, so that clients that never finish it do not hold resources.
const handshakeTimeout = 10 * time.Second

// Logger is the subset of logging the mux needs.
type Logger interface {
	Warnf(format string, args ...any)
}

// Mux accepts connections on one listener and routes them by client
// certificate Common Name.
type Mux struct {
	lis    net.Listener
	tlsCfg *tls.Config
	log    Logger
	routes map[string]*dbListener
}

// New returns a Mux that accepts on lis and routes to the given database
// names. tlsCfg must verify client certificates (tls.RequireAndVerifyClientCert
// with ClientCAs set); New adds the "h2" ALPN protocol gRPC requires.
func New(lis net.Listener, tlsCfg *tls.Config, names []string, log Logger) (*Mux, error) {
	if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert || tlsCfg.ClientCAs == nil {
		return nil, errors.New("dbmux: client certificates must be required and verified")
	}
	cfg := tlsCfg.Clone()
	cfg.NextProtos = []string{"h2"}
	m := &Mux{lis: lis, tlsCfg: cfg, log: log, routes: make(map[string]*dbListener, len(names))}
	for _, name := range names {
		if _, dup := m.routes[name]; dup {
			return nil, fmt.Errorf("dbmux: database %q listed twice", name)
		}
		m.routes[name] = &dbListener{addr: lis.Addr(), conns: make(chan net.Conn), done: make(chan struct{})}
	}
	return m, nil
}

// Listener returns the listener that yields the connections for database
// name. It is nil for a name New was not given.
func (m *Mux) Listener(name string) net.Listener {
	if l, ok := m.routes[name]; ok {
		return l
	}
	return nil
}

// Serve accepts connections until the underlying listener is closed, then
// closes the database listeners. It retries other accept errors, such as
// running out of file descriptors, with backoff.
func (m *Mux) Serve() error {
	defer func() {
		for _, l := range m.routes {
			_ = l.Close()
		}
	}()
	var delay time.Duration
	for {
		conn, err := m.lis.Accept()
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		if err != nil {
			delay = min(max(2*delay, 5*time.Millisecond), time.Second)
			m.log.Warnf("dbmux: accept: %v; retrying in %v", err, delay)
			time.Sleep(delay)
			continue
		}
		delay = 0
		go m.route(conn)
	}
}

// Close closes the underlying listener, which ends Serve.
func (m *Mux) Close() error {
	return m.lis.Close()
}

func (m *Mux) route(raw net.Conn) {
	conn := tls.Server(raw, m.tlsCfg)
	ctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	err := conn.HandshakeContext(ctx)
	cancel()
	if err != nil {
		m.log.Warnf("dbmux: TLS handshake from %s: %v", raw.RemoteAddr(), err)
		_ = conn.Close()
		return
	}
	// The handshake verified the chain, so PeerCertificates[0] is the
	// client's own certificate.
	name := conn.ConnectionState().PeerCertificates[0].Subject.CommonName
	l, ok := m.routes[name]
	if !ok {
		m.log.Warnf("dbmux: client %s presented a certificate for %q, which is not a database served here", raw.RemoteAddr(), name)
		_ = conn.Close()
		return
	}
	select {
	case l.conns <- conn:
	case <-l.done:
		_ = conn.Close()
	}
}

// dbListener is the net.Listener of one database.
type dbListener struct {
	addr      net.Addr
	conns     chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func (l *dbListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *dbListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}

func (l *dbListener) Addr() net.Addr { return l.addr }

// Credentials returns the transport credentials for a gRPC server serving a
// Listener: the connections it yields have completed the TLS handshake, so
// they are passed through with their TLS state as auth info.
func Credentials() credentials.TransportCredentials { return handshakenTLS{} }

type handshakenTLS struct{}

func (handshakenTLS) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return nil, nil, fmt.Errorf("dbmux: connection %T was not accepted by a Mux", conn)
	}
	return tc, credentials.TLSInfo{
		State:          tc.ConnectionState(),
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
	}, nil
}

func (handshakenTLS) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("dbmux: credentials are server-side only")
}

func (handshakenTLS) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c handshakenTLS) Clone() credentials.TransportCredentials { return c }

func (handshakenTLS) OverrideServerName(string) error { return nil }
