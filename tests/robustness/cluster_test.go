package robustness

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// s3Config points the cluster at an S3-compatible object store. Multi-node
// clusters need one: nodes elect a leader and find each other through it.
type s3Config struct {
	endpoint, region, bucket, access, secret string
}

// s3FromEnv returns the object store configured by the environment, or false
// when T4_ROBUSTNESS_S3 is unset.
func s3FromEnv() (s3Config, bool) {
	if os.Getenv("T4_ROBUSTNESS_S3") == "" {
		return s3Config{}, false
	}
	return s3Config{
		endpoint: envOr("S3_ENDPOINT", "http://127.0.0.1:9000"),
		region:   envOr("S3_REGION", "us-east-1"),
		bucket:   envOr("S3_BUCKET", "t4-robustness"),
		access:   envOr("S3_ACCESS_KEY", "t4testadmin"),
		secret:   envOr("S3_SECRET_KEY", "t4testadmin"),
	}, true
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// ensureBucket creates the bucket unless it exists, retrying while the
// server comes up.
func ensureBucket(ctx context.Context, cfg s3Config) error {
	endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.endpoint, "http://"), "https://")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.access, cfg.secret, ""),
		Secure: strings.HasPrefix(cfg.endpoint, "https://"),
		Region: cfg.region,
	})
	if err != nil {
		return err
	}
	var lastErr error
	for range 30 {
		exists, err := client.BucketExists(ctx, cfg.bucket)
		if err == nil && exists {
			return nil
		}
		if err == nil {
			err = client.MakeBucket(ctx, cfg.bucket, minio.MakeBucketOptions{Region: cfg.region})
			if err == nil {
				return nil
			}
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return fmt.Errorf("ensure bucket %q: %w", cfg.bucket, lastErr)
}

// cluster is a set of `t4 run` processes sharing one S3 prefix, or a single
// node without S3.
type cluster struct {
	t     *testing.T
	nodes []*node
}

type node struct {
	name       string
	dataDir    string
	clientAddr string
	args       []string
	logPath    string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
}

func newCluster(t *testing.T, bin string, size int, s3 *s3Config) *cluster {
	t.Helper()
	c := &cluster{t: t}
	dir := t.TempDir()
	prefix := fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
	for i := range size {
		n := &node{
			name:       fmt.Sprintf("n%d", i+1),
			dataDir:    filepath.Join(dir, fmt.Sprintf("n%d", i+1), "data"),
			clientAddr: freeAddr(t),
		}
		n.logPath = filepath.Join(dir, n.name, "t4.log")
		n.args = []string{"run",
			"--node-id", n.name,
			"--data-dir", n.dataDir,
			"--listen", n.clientAddr,
			"--metrics-addr", freeAddr(t),
			"--log-level", "info",
		}
		if s3 != nil {
			peer := freeAddr(t)
			n.args = append(n.args,
				"--peer-listen", peer,
				"--advertise-peer", peer,
				"--s3-endpoint", s3.endpoint,
				"--s3-region", s3.region,
				"--s3-bucket", s3.bucket,
				"--s3-prefix", prefix,
				"--s3-access-key-id", s3.access,
				"--s3-secret-access-key", s3.secret,
				// Fail over quickly after the leader is killed.
				"--follower-max-retries", "2",
				"--leader-watch-interval-sec", "2",
			)
		}
		if err := os.MkdirAll(filepath.Dir(n.logPath), 0o755); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.kill()
		}
		if t.Failed() {
			for _, n := range c.nodes {
				t.Logf("%s log: %s", n.name, n.logPath)
			}
		}
	})
	return c
}

func (c *cluster) start(ctx context.Context, bin string) error {
	for _, n := range c.nodes {
		if err := n.start(bin); err != nil {
			return err
		}
	}
	return c.waitReady(ctx)
}

// waitReady waits until every node serves linearizable reads, which in a
// cluster also means a leader has been elected.
func (c *cluster) waitReady(ctx context.Context) error {
	for _, n := range c.nodes {
		if err := n.waitReady(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) endpoints() []string {
	eps := make([]string, 0, len(c.nodes))
	for _, n := range c.nodes {
		eps = append(eps, n.clientAddr)
	}
	return eps
}

// dataDirs maps node names to the directories a failure report keeps: each
// node's data and its log.
func (c *cluster) dataDirs() map[string]string {
	dirs := make(map[string]string, len(c.nodes))
	for _, n := range c.nodes {
		dirs[n.name] = filepath.Dir(n.logPath)
	}
	return dirs
}

// leader returns the node that reports itself leader.
func (c *cluster) leader(ctx context.Context) (*node, error) {
	for _, n := range c.nodes {
		resp, err := n.status(ctx)
		if err == nil && resp.Leader != 0 {
			return n, nil
		}
	}
	return nil, errors.New("no node reports itself leader")
}

func (n *node) start(bin string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, n.args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = dieWithParent()
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start %s: %w", n.name, err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(exited)
	}()
	n.cmd, n.exited = cmd, exited
	return nil
}

// kill stops the node with SIGKILL and waits for it to exit.
func (n *node) kill() {
	n.signal(syscall.SIGKILL)
}

// stop stops the node with SIGTERM, letting it shut down gracefully.
func (n *node) stop() {
	n.signal(syscall.SIGTERM)
}

func (n *node) signal(sig syscall.Signal) {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.cmd = nil
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = cmd.Process.Signal(sig)
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-exited
	}
}

func (n *node) client() (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{n.clientAddr},
		DialTimeout: time.Second,
		Logger:      zap.NewNop(),
	})
}

func (n *node) status(ctx context.Context) (*clientv3.StatusResponse, error) {
	c, err := n.client()
	if err != nil {
		return nil, err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return c.Status(ctx, n.clientAddr)
}

func (n *node) waitReady(ctx context.Context) error {
	c, err := n.client()
	if err != nil {
		return err
	}
	defer c.Close()
	var lastErr error
	for {
		rctx, cancel := context.WithTimeout(ctx, time.Second)
		_, lastErr = c.Get(rctx, "health")
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready: %w (log: %s)", n.name, lastErr, n.logPath)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

var (
	portMu   sync.Mutex
	usedPort = map[int]bool{}
)

// freeAddr returns a 127.0.0.1 address with a free port below the kernels'
// ephemeral ranges, never the same one twice, as tests/etcddiff does.
func freeAddr(t *testing.T) string {
	t.Helper()
	portMu.Lock()
	defer portMu.Unlock()
	for range 1000 {
		port := 20000 + rand.Intn(12000)
		if usedPort[port] {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		addr := l.Addr().String()
		_ = l.Close()
		usedPort[port] = true
		return addr
	}
	t.Fatal("no free port")
	return ""
}
