// Package testserver adapts T4's etcd API to Kubernetes apiserver storage tests.
package testserver

import (
	"context"
	"errors"
	"fmt"
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

	autoPorts := cfg == nil
	if autoPorts {
		autoPortLock.Lock()
		defer autoPortLock.Unlock()
		cfg = NewTestConfig(t)
	}
	if len(cfg.ListenClientUrls) == 0 {
		t.Fatal("missing client listen URL")
	}

	// freePorts releases the ports it picks before t4 binds them, so another
	// process can take one in between and t4 exits at startup. With ports we
	// chose, pick new ones and try again.
	const attempts = 3
	for attempt := 1; ; attempt++ {
		client, proc, err := startClient(t, cfg)
		if err == nil {
			t.Cleanup(func() {
				_ = client.Close()
				stopT4(proc)
			})
			return client
		}
		if !autoPorts || !errors.Is(err, errExited) || attempt == attempts {
			t.Fatalf("t4 not ready: %v", err)
		}
		t.Logf("t4 exited at startup (attempt %d), retrying on new ports: %v", attempt, err)
		cfg = NewTestConfig(t)
	}
}

// startClient starts t4 and returns a client once t4 answers.
func startClient(t testing.TB, cfg *embed.Config) (*kubernetes.Client, *t4Proc, error) {
	t.Helper()

	proc := startT4(t, cfg)
	tlsConfig, err := cfg.ClientTLSInfo.ClientConfig()
	if err != nil {
		stopT4(proc)
		t.Fatalf("client TLS: %v", err)
	}
	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   clientEndpoints(cfg),
		DialTimeout: 10 * time.Second,
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("t4-etcd-client"),
	})
	if err != nil {
		stopT4(proc)
		t.Fatalf("etcd client: %v", err)
	}
	if err := waitReady(client, proc); err != nil {
		_ = client.Close()
		stopT4(proc)
		return nil, nil, err
	}
	client.KV = storagetesting.NewKVRecorder(client.KV)
	client.Kubernetes = storagetesting.NewKubernetesRecorder(client.Kubernetes)
	return client, proc, nil
}

var errExited = errors.New("t4 exited")

// waitReady blocks until t4 answers a request. grpc.WithBlock() is a no-op for
// clients built with grpc.NewClient (which clientv3 uses), so clientv3.New
// returns before the connection is up: without this, the first RPC of a test
// races t4's startup and can outlive short test deadlines. It returns
// errExited as soon as t4 exits instead of waiting out the deadline.
func waitReady(client *kubernetes.Client, proc *t4Proc) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.KV.Get(ctx, "/t4-readiness-probe")
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-proc.done:
			return fmt.Errorf("%w: %v", errExited, proc.err)
		default:
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// t4Proc is a running t4 process. done is closed once it has exited, with
// its exit status in err.
type t4Proc struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startT4(t testing.TB, cfg *embed.Config) *t4Proc {
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
	proc := &t4Proc{cmd: cmd, done: make(chan struct{})}
	go func() {
		proc.err = cmd.Wait()
		close(proc.done)
	}()
	return proc
}

func stopT4(proc *t4Proc) {
	if err := proc.cmd.Process.Signal(os.Interrupt); err != nil {
		_ = proc.cmd.Process.Kill()
		<-proc.done
		return
	}
	select {
	case <-proc.done:
	case <-time.After(5 * time.Second):
		_ = proc.cmd.Process.Kill()
		<-proc.done
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
