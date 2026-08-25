// Package testserver adapts T4's etcd API to Kubernetes apiserver storage tests.
package testserver

import (
	"context"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

var autoPortLock sync.Mutex

func NewTestConfig(t testing.TB) *embed.Config {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.UnsafeNoFsync = true
	cfg.WatchProgressNotifyInterval = time.Second

	clientPort, peerPort := freePorts(t, 2)
	clientURL := url.URL{Scheme: "http", Host: net.JoinHostPort("localhost", strconv.Itoa(clientPort))}
	peerURL := url.URL{Scheme: "http", Host: net.JoinHostPort("localhost", strconv.Itoa(peerPort))}

	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("t4-apiserver"))
	cfg.Dir = t.TempDir()
	_ = os.Chmod(cfg.Dir, 0700)
	return cfg
}

func RunEtcd(t testing.TB, cfg *embed.Config) *kubernetes.Client {
	t.Helper()

	if cfg == nil {
		autoPortLock.Lock()
		defer autoPortLock.Unlock()
		cfg = NewTestConfig(t)
	}
	if len(cfg.ListenClientUrls) == 0 {
		t.Fatal("missing client listen URL")
	}

	cmd := startT4(t, cfg)

	tlsConfig, err := cfg.ClientTLSInfo.ClientConfig()
	if err != nil {
		t.Fatalf("client TLS: %v", err)
	}
	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   clientEndpoints(cfg),
		DialTimeout: 10 * time.Second,
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("t4-etcd-client"),
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	waitReady(t, client)
	client.KV = storagetesting.NewKVRecorder(client.KV)
	client.Kubernetes = storagetesting.NewKubernetesRecorder(client.Kubernetes)
	t.Cleanup(func() {
		_ = client.Close()
		stopT4(t, cmd)
	})
	return client
}

// waitReady blocks until t4 answers a request. grpc.WithBlock() is a no-op for
// clients built with grpc.NewClient (which clientv3 uses), so clientv3.New
// returns before the connection is up: without this, the first RPC of a test
// races t4's startup and can outlive short test deadlines.
func waitReady(t testing.TB, client *kubernetes.Client) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.KV.Get(ctx, "/t4-readiness-probe")
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("t4 not ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startT4(t testing.TB, cfg *embed.Config) *exec.Cmd {
	t.Helper()

	bin := os.Getenv("T4_APISERVER_T4_BIN")
	if bin == "" {
		t.Fatal("T4_APISERVER_T4_BIN is not set")
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	args := []string{
		"run",
		"--data-dir", cfg.Dir,
		"--listen", cfg.ListenClientUrls[0].Host,
		"--metrics-addr", "127.0.0.1:0",
		"--log-level", "error",
	}
	if !cfg.ClientTLSInfo.Empty() {
		args = append(args,
			"--client-tls-cert", cfg.ClientTLSInfo.CertFile,
			"--client-tls-key", cfg.ClientTLSInfo.KeyFile,
		)
		if cfg.ClientTLSInfo.TrustedCAFile != "" {
			args = append(args, "--client-tls-ca", cfg.ClientTLSInfo.TrustedCAFile)
		}
	}

	cmd := exec.Command(bin, args...)
	cmd.Stdout = t.Output()
	cmd.Stderr = t.Output()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start t4: %v", err)
	}
	return cmd
}

func stopT4(t testing.TB, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func freePorts(t testing.TB, count int) (int, int) {
	t.Helper()

	ports := make([]int, 0, count)
	listeners := make([]net.Listener, 0, count)
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		listeners = append(listeners, lis)
		ports = append(ports, lis.Addr().(*net.TCPAddr).Port)
	}
	for _, lis := range listeners {
		_ = lis.Close()
	}
	return ports[0], ports[1]
}

func clientEndpoints(cfg *embed.Config) []string {
	urls := cfg.AdvertiseClientUrls
	if len(urls) == 0 {
		urls = cfg.ListenClientUrls
	}
	out := make([]string, 0, len(urls))
	for _, u := range urls {
		out = append(out, u.String())
	}
	return out
}
