package cli

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/t4db/t4"
)

// TestHealthzFailsOnceNodeStops checks that /healthz reports a node that has
// stopped serving, so that whatever runs t4 restarts it.
func TestHealthzFailsOnceNodeStops(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go serveMetrics(ctx, addr, promhttp.Handler(), []*t4.Node{node}, false)

	status := func() int {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			resp, err := http.Get("http://" + addr + "/healthz")
			if err == nil {
				_ = resp.Body.Close()
				return resp.StatusCode
			}
			if time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if got := status(); got != http.StatusOK {
		t.Fatalf("serving node: /healthz = %d, want 200", got)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != http.StatusServiceUnavailable {
		t.Fatalf("stopped node: /healthz = %d, want 503", got)
	}
}
