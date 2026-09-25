package e2e_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func tlsTransport(cfg *tls.Config) *http.Transport {
	return &http.Transport{
		TLSClientConfig: cfg.Clone(),
	}
}

// TestTLSE2E exercises all three TLS surfaces on a single 2-node t4 cluster:
//   - Client TLS with mTLS (client cert required).
//   - Peer mTLS between the two t4 nodes.
//   - S3 HTTPS to an S3-compatible (RustFS) instance using a self-signed CA.
//
// Setup:
//   - Generate a single self-signed CA + leaf certs in-process.
//   - Start a RustFS container with our CA-signed server cert.
//   - Spawn two t4 binaries with all three TLS flags wired up and
//     SSL_CERT_FILE pointing at our CA so the AWS SDK trusts the S3 server.
//
// Positive: a TLS clientv3 (mTLS client cert + our CA as root) writes via
// node A and reads from node B, verifying replication across peer mTLS.
//
// Negative subtests run against the same cluster:
//   - Plaintext client → handshake fails.
//   - Client with wrong CA → handshake fails.
//   - mTLS server requires client cert, client doesn't present one → fails.
func TestTLSE2E(t *testing.T) {
	if os.Getenv("T4_E2E_S3") == "" {
		t.Skip("set T4_E2E_S3=1 to run the TLS S3 e2e test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available — required to bring up a TLS S3 server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	workDir := t.TempDir()
	certs := newCA(t, workDir, "t4-tls-test-ca")
	caPath := certs.writeCACert(t, "ca.crt")

	// S3 HTTPS: server cert with localhost / 127.0.0.1 SANs.
	s3CertPath, s3KeyPath := certs.mintServer(t, "s3", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	// T4 client TLS: server cert with localhost / 127.0.0.1 SANs.
	clientServerCertPath, clientServerKeyPath := certs.mintServer(t, "t4-client", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	// T4 peer mTLS: per-node cert. Each node is both server and client for
	// its peer connections, so a single cert with both EKUs works.
	peerCertPath, peerKeyPath := certs.mintServer(t, "t4-peer", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	// Client cert for mTLS clientv3.
	mTLSClientCertPath, mTLSClientKeyPath := certs.mintClient(t, "tls-test-client")

	s3Endpoint := startTLSS3(t, ctx, workDir, s3CertPath, s3KeyPath, caPath)

	bin := buildT4(t, ctx, workDir)

	bucket := fmt.Sprintf("t4-tls-%d", time.Now().UnixNano())
	prefix := fmt.Sprintf("tls-%d", time.Now().UnixNano())
	if err := ensureBucketTLS(ctx, bucket, s3Endpoint, caPath); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	listenA := freeAddr(t)
	listenB := freeAddr(t)
	peerA := freeAddr(t)
	peerB := freeAddr(t)

	nodeA, logA := startTLSNode(t, ctx, bin, tlsNodeArgs{
		dataDir:    filepath.Join(workDir, "node-a"),
		listen:     listenA,
		peerListen: peerA,
		nodeID:     "node-a",
		clientCert: clientServerCertPath,
		clientKey:  clientServerKeyPath,
		clientCA:   caPath,
		peerCert:   peerCertPath,
		peerKey:    peerKeyPath,
		peerCA:     caPath,
		s3Bucket:   bucket,
		s3Prefix:   prefix,
		s3Endpoint: s3Endpoint,
		s3CABundle: caPath,
	})
	defer stopNode(nodeA)
	nodeB, logB := startTLSNode(t, ctx, bin, tlsNodeArgs{
		dataDir:    filepath.Join(workDir, "node-b"),
		listen:     listenB,
		peerListen: peerB,
		nodeID:     "node-b",
		clientCert: clientServerCertPath,
		clientKey:  clientServerKeyPath,
		clientCA:   caPath,
		peerCert:   peerCertPath,
		peerKey:    peerKeyPath,
		peerCA:     caPath,
		s3Bucket:   bucket,
		s3Prefix:   prefix,
		s3Endpoint: s3Endpoint,
		s3CABundle: caPath,
	})
	defer stopNode(nodeB)

	if err := waitForTLSEtcd(ctx, listenA, caPath, mTLSClientCertPath, mTLSClientKeyPath); err != nil {
		t.Fatalf("wait for node-a: %v\nlog-a:\n%s\nlog-b:\n%s", err, logA.String(), logB.String())
	}
	if err := waitForTLSEtcd(ctx, listenB, caPath, mTLSClientCertPath, mTLSClientKeyPath); err != nil {
		t.Fatalf("wait for node-b: %v\nlog-a:\n%s\nlog-b:\n%s", err, logA.String(), logB.String())
	}

	// ── Positive: write on A via TLS clientv3, read on B ─────────────────
	cliA := newTLSClient(t, listenA, caPath, mTLSClientCertPath, mTLSClientKeyPath)
	if _, err := cliA.Put(ctx, "/tls/hello", "world"); err != nil {
		t.Fatalf("TLS Put on node-a: %v\nlog-a:\n%s", err, logA.String())
	}
	cliB := newTLSClient(t, listenB, caPath, mTLSClientCertPath, mTLSClientKeyPath)
	deadline := time.Now().Add(15 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		resp, err := cliB.Get(ctx, "/tls/hello")
		if err == nil && len(resp.Kvs) == 1 {
			got = string(resp.Kvs[0].Value)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got != "world" {
		t.Fatalf("replicated value on node-b: got %q, want \"world\"\nlog-a:\n%s\nlog-b:\n%s", got, logA.String(), logB.String())
	}

	// ── Negative: plaintext client to TLS port ─────────────────────────────
	t.Run("PlaintextRejected", func(t *testing.T) {
		plaintextCli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{listenA},
			DialTimeout: 3 * time.Second,
		})
		if err != nil {
			return // refused at dial — that's a pass
		}
		defer func() { _ = plaintextCli.Close() }()
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		defer probeCancel()
		if _, err := plaintextCli.Put(probeCtx, "/tls/plaintext", "denied"); err == nil {
			t.Fatal("expected plaintext clientv3 to fail against TLS port, got success")
		}
	})

	// ── Negative: TLS client with a different CA ───────────────────────────
	t.Run("WrongCA", func(t *testing.T) {
		wrongDir := t.TempDir()
		wrongCA := newCA(t, wrongDir, "wrong-ca")
		wrongCAPath := wrongCA.writeCACert(t, "wrong-ca.crt")
		wrongClientCert, wrongClientKey := wrongCA.mintClient(t, "wrong-client")
		wrongCli, err := newTLSClientWithCA(t, listenA, wrongCAPath, wrongClientCert, wrongClientKey)
		if err != nil {
			return // dial-time refusal counts as pass
		}
		defer func() { _ = wrongCli.Close() }()
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		defer probeCancel()
		if _, err := wrongCli.Put(probeCtx, "/tls/wrong-ca", "denied"); err == nil {
			t.Fatal("expected wrong-CA clientv3 to fail, got success")
		}
	})

	// ── Negative: mTLS server requires a client cert; provide none ─────────
	t.Run("NoClientCert", func(t *testing.T) {
		caBytes, err := os.ReadFile(caPath)
		if err != nil {
			t.Fatalf("read ca: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			t.Fatalf("appending CA to pool")
		}
		noCertCli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{listenA},
			DialTimeout: 3 * time.Second,
			TLS: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
				ServerName: "localhost",
			},
		})
		if err != nil {
			return
		}
		defer func() { _ = noCertCli.Close() }()
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		defer probeCancel()
		if _, err := noCertCli.Put(probeCtx, "/tls/no-cert", "denied"); err == nil {
			t.Fatal("expected mTLS without client cert to fail, got success")
		}
	})
}

// ── helpers ──────────────────────────────────────────────────────────────────

type tlsNodeArgs struct {
	dataDir    string
	listen     string
	peerListen string
	nodeID     string
	clientCert string
	clientKey  string
	clientCA   string
	peerCert   string
	peerKey    string
	peerCA     string
	s3Bucket   string
	s3Prefix   string
	s3Endpoint string
	s3CABundle string
}

func startTLSNode(t *testing.T, ctx context.Context, bin string, args tlsNodeArgs) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	cmdArgs := []string{
		"run",
		"--data-dir", args.dataDir,
		"--listen", args.listen,
		"--peer-listen", args.peerListen,
		"--advertise-peer", args.peerListen,
		"--node-id", args.nodeID,
		"--metrics-addr", "127.0.0.1:0",
		"--checkpoint-entries", "1",
		"--checkpoint-interval-min", "1",
		"--segment-max-age-sec", "1",
		"--log-level", "warn",
		"--client-tls-cert", args.clientCert,
		"--client-tls-key", args.clientKey,
		"--client-tls-ca", args.clientCA,
		"--peer-tls-cert", args.peerCert,
		"--peer-tls-key", args.peerKey,
		"--peer-tls-ca", args.peerCA,
		"--s3-bucket", args.s3Bucket,
		"--s3-prefix", args.s3Prefix,
		"--s3-endpoint", args.s3Endpoint,
		"--s3-region", "us-east-1",
		"--s3-access-key-id", "t4testadmin",
		"--s3-secret-access-key", "t4testadmin",
		"--s3-ca-bundle", args.s3CABundle,
	}
	var log bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, cmdArgs...)
	cmd.Stdout = &log
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start t4 (%s): %v", args.nodeID, err)
	}
	return cmd, &log
}

// s3TestImage is the S3-compatible server used for the TLS test. MinIO's
// community images were withdrawn from Docker Hub and quay.io; keep in sync
// with the s3-cli-smoke job in .github/workflows/ci.yml.
const s3TestImage = "rustfs/rustfs:1.0.0"

// startTLSS3 launches an S3-compatible container (RustFS) with TLS on a
// free host port and returns its https://127.0.0.1:<port> endpoint.
func startTLSS3(t *testing.T, ctx context.Context, workDir, certPath, keyPath, caPath string) string {
	t.Helper()
	certsDir := filepath.Join(workDir, "s3-certs")
	if err := os.Mkdir(certsDir, 0o755); err != nil {
		t.Fatalf("mkdir s3-certs: %v", err)
	}
	// The container runs as a non-root user, so the throwaway cert and key
	// must be world-readable once copied in.
	for src, name := range map[string]string{certPath: "rustfs_cert.pem", keyPath: "rustfs_key.pem"} {
		dst := filepath.Join(certsDir, name)
		if err := copyFile(src, dst); err != nil {
			t.Fatalf("copy %s: %v", name, err)
		}
		if err := os.Chmod(dst, 0o644); err != nil {
			t.Fatalf("chmod %s: %v", name, err)
		}
	}

	hostPort := freeAddr(t)
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	_ = host

	// Certs are copied in with `docker cp` rather than bind-mounted so the
	// test works on Docker hosts that don't share the temp dir (e.g. macOS).
	containerName := fmt.Sprintf("t4-tls-s3-%d", time.Now().UnixNano())
	runCtx, runCancel := context.WithTimeout(ctx, 30*time.Second)
	defer runCancel()
	docker := func(args ...string) {
		t.Helper()
		if out, err := exec.CommandContext(runCtx, "docker", args...).CombinedOutput(); err != nil {
			t.Fatalf("docker %s: %v\n%s", args[0], err, out)
		}
	}
	docker("create",
		"--name", containerName,
		"-p", fmt.Sprintf("%s:9000", port),
		"-e", "RUSTFS_TLS_PATH=/certs",
		"-e", "RUSTFS_ACCESS_KEY=t4testadmin",
		"-e", "RUSTFS_SECRET_KEY=t4testadmin",
		s3TestImage,
	)
	t.Cleanup(func() {
		stopCmd := exec.Command("docker", "rm", "-f", containerName)
		if out, err := stopCmd.CombinedOutput(); err != nil {
			t.Logf("docker rm -f %s: %v\n%s", containerName, err, out)
		}
	})
	docker("cp", certsDir, containerName+":/certs")
	docker("start", containerName)

	endpoint := fmt.Sprintf("https://127.0.0.1:%s", port)
	if err := waitForS3TLS(ctx, endpoint, caPath); err != nil {
		dumpCmd := exec.Command("docker", "logs", containerName)
		out, _ := dumpCmd.CombinedOutput()
		t.Fatalf("wait for S3 HTTPS: %v\n%s", err, out)
	}
	return endpoint
}

func waitForS3TLS(ctx context.Context, endpoint, caPath string) error {
	deadline := time.Now().Add(60 * time.Second)
	cfg, err := tlsClientConfig(caPath, "", "")
	if err != nil {
		return err
	}
	for time.Now().Before(deadline) {
		client, err := tlsS3Client(endpoint, cfg)
		if err != nil {
			return err
		}
		_, listErr := client.ListBuckets(ctx)
		if listErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errors.New("timed out waiting for S3 TLS endpoint")
}

func ensureBucketTLS(ctx context.Context, bucket, endpoint, caPath string) error {
	cfg, err := tlsClientConfig(caPath, "", "")
	if err != nil {
		return err
	}
	client, err := tlsS3Client(endpoint, cfg)
	if err != nil {
		return err
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		// Already-exists is fine.
		if exists, headErr := client.BucketExists(ctx, bucket); headErr == nil && exists {
			return nil
		}
		if !strings.Contains(err.Error(), "BucketAlready") {
			return fmt.Errorf("create bucket: %w", err)
		}
	}
	return nil
}

func tlsS3Client(endpoint string, tlsCfg *tls.Config) (*minio.Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint %q: %w", endpoint, err)
	}
	host := u.Host
	if host == "" {
		host = endpoint
	}
	return minio.New(host, &minio.Options{
		Creds:     credentials.NewStaticV4("t4testadmin", "t4testadmin", ""),
		Secure:    u.Scheme == "https",
		Region:    "us-east-1",
		Transport: tlsTransport(tlsCfg),
	})
}

func tlsClientConfig(caPath, clientCert, clientKey string) (*tls.Config, error) {
	pool := x509.NewCertPool()
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("appending CA to pool")
	}
	cfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
		ServerName: "localhost",
	}
	if clientCert != "" {
		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func newTLSClient(t *testing.T, endpoint, caPath, clientCert, clientKey string) *clientv3.Client {
	t.Helper()
	cli, err := newTLSClientWithCA(t, endpoint, caPath, clientCert, clientKey)
	if err != nil {
		t.Fatalf("new TLS clientv3: %v", err)
	}
	return cli
}

func newTLSClientWithCA(t *testing.T, endpoint, caPath, clientCert, clientKey string) (*clientv3.Client, error) {
	t.Helper()
	cfg, err := tlsClientConfig(caPath, clientCert, clientKey)
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{tlsEndpoint(endpoint)},
		DialTimeout: 5 * time.Second,
		TLS:         cfg,
	})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, nil
}

func tlsEndpoint(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "https://" + addr
}

func waitForTLSEtcd(ctx context.Context, listenAddr, caPath, clientCert, clientKey string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cfg, err := tlsClientConfig(caPath, clientCert, clientKey)
		if err != nil {
			return err
		}
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{tlsEndpoint(listenAddr)},
			DialTimeout: 2 * time.Second,
			TLS:         cfg,
		})
		if err == nil {
			probeCtx, probeCancel := context.WithTimeout(ctx, 2*time.Second)
			_, putErr := cli.Put(probeCtx, "/__tls_probe", "ready")
			probeCancel()
			_ = cli.Close()
			if putErr == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return errors.New("timed out waiting for t4 TLS etcd endpoint")
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600)
}
