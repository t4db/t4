// Package testutil holds helpers shared by tests across packages.
package testutil

import (
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
)

// Ports are picked below the kernels' ephemeral ranges (Linux 32768-60999,
// macOS and Windows 49152-65535). Anything that asks the kernel for port 0,
// in this or another test process, gets a port from those ranges, so it
// cannot take a port handed out here in the moment between FreeAddr closing
// its probe and the caller binding the port.
const (
	lowPort  = 20000
	highPort = 32000
)

var (
	mu   sync.Mutex
	used = map[int]bool{}
)

// FreeAddr returns a 127.0.0.1 address whose port is free now and has not
// been returned before by this process.
func FreeAddr(tb testing.TB) string {
	tb.Helper()
	mu.Lock()
	defer mu.Unlock()
	for range 1000 {
		port := lowPort + rand.IntN(highPort-lowPort)
		if used[port] {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue // taken by someone else
		}
		addr := l.Addr().String()
		if err := l.Close(); err != nil {
			continue
		}
		used[port] = true
		return addr
	}
	tb.Fatal("testutil: no free port found")
	return ""
}
