package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type s3TestConfig struct {
	bin      string
	endpoint string
	bucket   string
	prefix   string
	access   string
	secret   string
	region   string
}

func TestS3CLISmoke(t *testing.T) {
	if os.Getenv("T4_E2E_S3") == "" {
		t.Skip("set T4_E2E_S3=1 to run the S3-backed CLI smoke test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	workDir := t.TempDir()
	cfg := s3TestConfig{
		bin:      envOr("T4_SMOKE_BIN", ""),
		endpoint: envOr("S3_ENDPOINT", "http://127.0.0.1:9000"),
		bucket:   envOr("S3_BUCKET", fmt.Sprintf("t4-smoke-%d", time.Now().UnixNano())),
		prefix:   envOr("T4_SMOKE_PREFIX", fmt.Sprintf("smoke-%d", time.Now().UnixNano())),
		access:   envOr("S3_ACCESS_KEY", "t4testadmin"),
		secret:   envOr("S3_SECRET_KEY", "t4testadmin"),
		region:   envOr("S3_REGION", "us-east-1"),
	}
	if cfg.bin == "" {
		cfg.bin = buildT4(t, ctx, workDir)
	}

	if err := ensureBucket(ctx, cfg); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	listenAddr := freeAddr(t)
	dataDir := filepath.Join(workDir, "node")
	node, nodeLog := startNode(t, ctx, cfg, dataDir, listenAddr)
	defer stopNode(node)

	if err := writeData(ctx, listenAddr); err != nil {
		t.Fatalf("write data: %v\nnode log:\n%s", err, nodeLog.String())
	}
	restoreDir, err := waitForRestoredCount(ctx, cfg, workDir, "/smoke/", 3)
	if err != nil {
		t.Fatalf("wait for restorable data: %v\nnode log:\n%s", err, nodeLog.String())
	}

	statusArgs := append([]string{"status"}, s3Args(cfg)...)
	if out, err := runT4(ctx, cfg, statusArgs...); err != nil {
		t.Fatalf("status: %v\n%s", err, out)
	} else if !strings.Contains(out, "Latest checkpoint") {
		t.Fatalf("status output missing checkpoint section:\n%s", out)
	}

	if out, err := runT4(ctx, cfg, "inspect", "count", "--data-dir", restoreDir, "--prefix", "/smoke/"); err != nil {
		t.Fatalf("inspect restored data: %v\n%s", err, out)
	} else if lastNonEmptyLine(out) != "/smoke/: 3" {
		t.Fatalf("unexpected restored count %q", strings.TrimSpace(out))
	}

	s3cli, err := s3Client(ctx, cfg)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}

	const branchID = "smoke-branch"
	branchArgs := append([]string{"branch", "fork", "--branch-id", branchID}, s3Args(cfg)...)
	branchOut, err := runT4(ctx, cfg, branchArgs...)
	if err != nil {
		t.Fatalf("branch fork: %v\n%s", err, branchOut)
	}
	branchKey := lastNonEmptyLine(branchOut)
	if !strings.HasPrefix(branchKey, "checkpoint/") {
		t.Fatalf("unexpected branch checkpoint key %q from output:\n%s", branchKey, branchOut)
	}

	// Branch registry entry must exist after fork.
	registryKey := "branches/" + branchID
	if ok, err := s3KeyExists(ctx, s3cli, cfg, registryKey); err != nil {
		t.Fatalf("HEAD %s after fork: %v", registryKey, err)
	} else if !ok {
		t.Fatalf("branch registry key %s missing after fork", registryKey)
	}

	// Capture the SSTs the pinned checkpoint depends on so we can verify
	// none are deleted by GC while the branch is active.
	pinnedSSTs, err := checkpointSSTs(ctx, s3cli, cfg, branchKey)
	if err != nil {
		t.Fatalf("read pinned checkpoint manifest %s: %v", branchKey, err)
	}
	if len(pinnedSSTs) == 0 {
		t.Fatalf("pinned checkpoint %s lists no SSTs; nothing to protect", branchKey)
	}
	t.Logf("branch pinned checkpoint %s protects %d SST(s)", branchKey, len(pinnedSSTs))

	// Write more data so a newer checkpoint is uploaded, leaving branchKey
	// as an older (only-reachable-through-the-branch) checkpoint. Without
	// this step, GC --keep 1 would retain branchKey purely because it is
	// the latest checkpoint, masking the branch-pinning behavior we want
	// to assert here.
	if err := writeMoreData(ctx, listenAddr); err != nil {
		t.Fatalf("post-fork write: %v\nnode log:\n%s", err, nodeLog.String())
	}
	if _, err := waitForRestoredCount(ctx, cfg, workDir, "/smoke-post/", 3); err != nil {
		t.Fatalf("wait for newer checkpoint: %v", err)
	}

	gcArgs := append([]string{"gc", "--keep", "1"}, s3Args(cfg)...)
	if out, err := runT4(ctx, cfg, gcArgs...); err != nil {
		t.Fatalf("gc with pinned branch: %v\n%s", err, out)
	} else if !strings.Contains(out, "GC complete") {
		t.Fatalf("gc output missing success text:\n%s", out)
	}

	// While the branch is pinned, GC must not delete the registry entry,
	// the pinned checkpoint, or any of its SSTs.
	if ok, err := s3KeyExists(ctx, s3cli, cfg, registryKey); err != nil {
		t.Fatalf("HEAD %s after pinned gc: %v", registryKey, err)
	} else if !ok {
		t.Fatalf("GC deleted branch registry key %s while branch was pinned", registryKey)
	}
	if ok, err := s3KeyExists(ctx, s3cli, cfg, branchKey); err != nil {
		t.Fatalf("HEAD %s after pinned gc: %v", branchKey, err)
	} else if !ok {
		t.Fatalf("GC deleted pinned checkpoint %s", branchKey)
	}
	for _, sst := range pinnedSSTs {
		if ok, err := s3KeyExists(ctx, s3cli, cfg, sst); err != nil {
			t.Fatalf("HEAD %s after pinned gc: %v", sst, err)
		} else if !ok {
			t.Fatalf("GC deleted branch-pinned SST %s", sst)
		}
	}

	unforkArgs := append([]string{"branch", "unfork", "--branch-id", branchID}, s3Args(cfg)...)
	if out, err := runT4(ctx, cfg, unforkArgs...); err != nil {
		t.Fatalf("branch unfork: %v\n%s", err, out)
	}

	// Unfork must remove the registry entry.
	if ok, err := s3KeyExists(ctx, s3cli, cfg, registryKey); err != nil {
		t.Fatalf("HEAD %s after unfork: %v", registryKey, err)
	} else if ok {
		t.Fatalf("branch registry key %s still present after unfork", registryKey)
	}

	if out, err := runT4(ctx, cfg, gcArgs...); err != nil {
		t.Fatalf("gc after unfork: %v\n%s", err, out)
	}

	// After unfork + gc the pinned checkpoint must be reclaimable. Any SST
	// that is ONLY referenced by that checkpoint (i.e. not by the latest
	// checkpoint that --keep=1 retains) must be reclaimed as well.
	if ok, err := s3KeyExists(ctx, s3cli, cfg, branchKey); err != nil {
		t.Fatalf("HEAD %s after unfork gc: %v", branchKey, err)
	} else if ok {
		t.Fatalf("GC did not reclaim unforked checkpoint %s", branchKey)
	}
	survivingSSTs, err := latestCheckpointSSTs(ctx, s3cli, cfg)
	if err != nil {
		t.Fatalf("read latest checkpoint manifest: %v", err)
	}
	stillNeeded := make(map[string]struct{}, len(survivingSSTs))
	for _, k := range survivingSSTs {
		stillNeeded[k] = struct{}{}
	}
	for _, sst := range pinnedSSTs {
		if _, shared := stillNeeded[sst]; shared {
			continue
		}
		if ok, err := s3KeyExists(ctx, s3cli, cfg, sst); err != nil {
			t.Fatalf("HEAD %s after unfork gc: %v", sst, err)
		} else if ok {
			t.Fatalf("GC did not reclaim orphan SST %s after unfork", sst)
		}
	}
}

// s3KeyExists reports whether the object at the prefix-relative key exists in
// the test bucket. Returns false (no error) when the object is absent.
func s3KeyExists(ctx context.Context, client *minio.Client, cfg s3TestConfig, relKey string) (bool, error) {
	_, err := client.StatObject(ctx, cfg.bucket, prefixedKey(cfg, relKey), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isS3NotFound(err) {
		return false, nil
	}
	return false, err
}

// s3GetObject downloads and returns the body of a prefix-relative object key.
func s3GetObject(ctx context.Context, client *minio.Client, cfg s3TestConfig, relKey string) ([]byte, error) {
	obj, err := client.GetObject(ctx, cfg.bucket, prefixedKey(cfg, relKey), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

// checkpointSSTs reads the checkpoint manifest at relKey and returns the union
// of its own_store and ancestor_store SST keys (prefix-relative).
func checkpointSSTs(ctx context.Context, client *minio.Client, cfg s3TestConfig, relKey string) ([]string, error) {
	body, err := s3GetObject(ctx, client, cfg, relKey)
	if err != nil {
		return nil, err
	}
	var m struct {
		SSTFiles         []string `json:"sst_files"`
		AncestorSSTFiles []string `json:"ancestor_sst_files"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decode checkpoint manifest: %w", err)
	}
	out := make([]string, 0, len(m.SSTFiles)+len(m.AncestorSSTFiles))
	out = append(out, m.SSTFiles...)
	out = append(out, m.AncestorSSTFiles...)
	return out, nil
}

// latestCheckpointSSTs returns the SST keys referenced by the current
// manifest/latest pointer's checkpoint.
func latestCheckpointSSTs(ctx context.Context, client *minio.Client, cfg s3TestConfig) ([]string, error) {
	body, err := s3GetObject(ctx, client, cfg, "manifest/latest")
	if err != nil {
		return nil, err
	}
	var m struct {
		CheckpointKey string `json:"checkpoint_key"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decode manifest/latest: %w", err)
	}
	if m.CheckpointKey == "" {
		return nil, fmt.Errorf("manifest/latest has empty checkpoint_key")
	}
	return checkpointSSTs(ctx, client, cfg, m.CheckpointKey)
}

func prefixedKey(cfg s3TestConfig, relKey string) string {
	if cfg.prefix == "" {
		return relKey
	}
	return path.Join(cfg.prefix, relKey)
}

func isS3NotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == 404 || resp.Code == "NoSuchKey"
}

func buildT4(t *testing.T, ctx context.Context, workDir string) string {
	t.Helper()
	binPath := filepath.Join(workDir, "t4")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, "./cmd/t4")
	cmd.Dir = filepath.Join("..", "..")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("build t4: %v\n%s", err, out.String())
	}
	return binPath
}

func ensureBucket(ctx context.Context, cfg s3TestConfig) error {
	client, err := s3Client(ctx, cfg)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		err = client.MakeBucket(ctx, cfg.bucket, minio.MakeBucketOptions{Region: cfg.region})
		if err == nil {
			return nil
		}
		if exists, headErr := client.BucketExists(ctx, cfg.bucket); headErr == nil && exists {
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("create bucket %q: %w", cfg.bucket, lastErr)
}

func s3Client(ctx context.Context, cfg s3TestConfig) (*minio.Client, error) {
	_ = ctx
	u, err := url.Parse(cfg.endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint %q: %w", cfg.endpoint, err)
	}
	host := u.Host
	if host == "" {
		host = cfg.endpoint
	}
	return minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.access, cfg.secret, ""),
		Secure: u.Scheme == "https",
		Region: cfg.region,
	})
}

func startNode(t *testing.T, ctx context.Context, cfg s3TestConfig, dataDir, listenAddr string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	args := []string{
		"run",
		"--data-dir", dataDir,
		"--listen", listenAddr,
		"--metrics-addr", "127.0.0.1:0",
		"--checkpoint-entries", "1",
		"--checkpoint-interval-min", "1",
		"--segment-max-age-sec", "1",
		"--log-level", "warn",
	}
	args = append(args, s3Args(cfg)...)

	var log bytes.Buffer
	cmd := exec.CommandContext(ctx, cfg.bin, args...)
	cmd.Stdout = &log
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start t4 node: %v", err)
	}
	if err := waitForEtcd(ctx, listenAddr); err != nil {
		stopNode(cmd)
		t.Fatalf("wait for etcd endpoint: %v\nnode log:\n%s", err, log.String())
	}
	return cmd, &log
}

func writeData(ctx context.Context, listenAddr string) error {
	return writeKeysWithPrefix(ctx, listenAddr, "/smoke/", 3)
}

func writeMoreData(ctx context.Context, listenAddr string) error {
	return writeKeysWithPrefix(ctx, listenAddr, "/smoke-post/", 3)
}

func writeKeysWithPrefix(ctx context.Context, listenAddr, prefix string, n int) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{listenAddr},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	for i := 0; i < n; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%s%d", prefix, i), fmt.Sprintf("value-%d", i)); err != nil {
			return err
		}
	}
	return nil
}

func waitForEtcd(ctx context.Context, listenAddr string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := writeProbe(ctx, listenAddr); err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return errors.New("timed out waiting for t4 etcd endpoint")
}

func writeProbe(ctx context.Context, listenAddr string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{listenAddr},
		DialTimeout: time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	_, err = cli.Put(probeCtx, "/__smoke_probe", "ready")
	return err
}

func waitForRestoredCount(ctx context.Context, cfg s3TestConfig, workDir, prefix string, want int) (string, error) {
	deadline := time.Now().Add(45 * time.Second)
	wantOut := fmt.Sprintf("%s: %d", prefix, want)
	var lastOut string
	for time.Now().Before(deadline) {
		restoreDir := filepath.Join(workDir, fmt.Sprintf("restored-%d", time.Now().UnixNano()))
		restoreArgs := append([]string{"restore", "checkpoint", "--data-dir", restoreDir}, s3Args(cfg)...)
		restoreOut, err := runT4(ctx, cfg, restoreArgs...)
		if err == nil && strings.Contains(restoreOut, "Restored checkpoint") {
			countOut, countErr := runT4(ctx, cfg, "inspect", "count", "--data-dir", restoreDir, "--prefix", prefix)
			if countErr == nil && lastNonEmptyLine(countOut) == wantOut {
				return restoreDir, nil
			}
			lastOut = countOut
			if countErr != nil {
				lastOut = fmt.Sprintf("%v\n%s", countErr, countOut)
			}
		} else {
			lastOut = restoreOut
			if err != nil {
				lastOut = fmt.Sprintf("%v\n%s", err, restoreOut)
			}
		}
		_ = os.RemoveAll(restoreDir)
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("timed out waiting for restored %q count %d; last output:\n%s", prefix, want, lastOut)
}

func runT4(ctx context.Context, cfg s3TestConfig, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, cfg.bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

func s3Args(cfg s3TestConfig) []string {
	return []string{
		"--s3-bucket", cfg.bucket,
		"--s3-prefix", cfg.prefix,
		"--s3-endpoint", cfg.endpoint,
		"--s3-region", cfg.region,
		"--s3-access-key-id", cfg.access,
		"--s3-secret-access-key", cfg.secret,
	}
}

func stopNode(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().String()
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func lastNonEmptyLine(out string) string {
	lines := strings.Split(out, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return line
		}
	}
	return ""
}
