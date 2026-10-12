package robustness

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// proxy forwards TCP connections to a target and can delay or blackhole the
// traffic, as etcd's robustness proxies do. A blackholed proxy reads and
// drops what either side sends, so connections stall and then break the way
// they would on a lossy network, rather than closing cleanly.
type proxy struct {
	lis    net.Listener
	target string

	mu        sync.Mutex
	blackhole bool
	delay     time.Duration
	conns     map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	lis, err := net.Listen("tcp", freeAddr(t))
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{lis: lis, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { p.close() })
	return p
}

func (p *proxy) addr() string { return p.lis.Addr().String() }

// setBlackhole starts or stops dropping traffic.
func (p *proxy) setBlackhole(on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackhole = on
}

// setDelay delays every chunk forwarded in either direction by d.
func (p *proxy) setDelay(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.delay = d
}

func (p *proxy) state() (blackhole bool, delay time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blackhole, p.delay
}

func (p *proxy) close() {
	_ = p.lis.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
}

func (p *proxy) track(c net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.conns[c] = struct{}{}
	} else {
		delete(p.conns, c)
	}
}

func (p *proxy) serve() {
	for {
		in, err := p.lis.Accept()
		if err != nil {
			return
		}
		go p.handle(in)
	}
}

func (p *proxy) handle(in net.Conn) {
	out, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = in.Close()
		return
	}
	p.track(in, true)
	p.track(out, true)
	defer func() {
		p.track(in, false)
		p.track(out, false)
		_ = in.Close()
		_ = out.Close()
	}()
	done := make(chan struct{}, 2)
	go func() { p.pipe(out, in); done <- struct{}{} }()
	go func() { p.pipe(in, out); done <- struct{}{} }()
	<-done
}

// pipe copies src to dst, dropping or delaying chunks as the proxy is set.
func (p *proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			blackhole, delay := p.state()
			if delay > 0 {
				time.Sleep(delay)
			}
			if !blackhole {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = dst.Close()
			}
			return
		}
	}
}
