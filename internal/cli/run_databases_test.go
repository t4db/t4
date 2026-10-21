package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestRunServesSeveralDatabases runs t4 run with two databases and checks
// that each client certificate reaches only the database it names.
func TestRunServesSeveralDatabases(t *testing.T) {
	pki := newTestPKI(t)
	serverCert, serverKey := pki.issue(t, "localhost", true)
	dataDir := t.TempDir()
	addr := freeAddr(t)
	metricsAddr := freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		cmd := NewRootCmd()
		cmd.SetArgs([]string{
			"run",
			"--db", "tenant-a", "--db", "tenant-b",
			"--data-dir", dataDir,
			"--listen", addr,
			"--metrics-addr", metricsAddr,
			"--client-tls-cert", serverCert,
			"--client-tls-key", serverKey,
			"--client-tls-ca", pki.caPath,
		})
		done <- cmd.ExecuteContext(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("t4 run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("t4 run did not stop")
		}
	})

	a := pki.client(t, addr, "tenant-a")
	b := pki.client(t, addr, "tenant-b")
	put(t, a, "/k", "from-a")
	if got := get(t, b, "/k"); got != "" {
		t.Fatalf("tenant-b sees tenant-a's key: %q", got)
	}
	put(t, b, "/k", "from-b")
	if got := get(t, a, "/k"); got != "from-a" {
		t.Fatalf("tenant-a reads %q, want from-a", got)
	}

	// A certificate for a database that is not served gets no connection.
	other := pki.client(t, addr, "tenant-c")
	opCtx, opCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer opCancel()
	if _, err := other.Get(opCtx, "/k"); err == nil {
		t.Fatal("client for an unserved database was served")
	}

	for _, name := range []string{"tenant-a", "tenant-b"} {
		if _, err := os.Stat(filepath.Join(dataDir, "dbs", name, "db")); err != nil {
			t.Errorf("database %s: %v", name, err)
		}
	}
	resp, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	revisions := map[string]bool{}
	for _, name := range []string{"tenant-a", "tenant-b"} {
		revisions[name] = strings.Contains(string(body), `t4_current_revision{db="`+name+`"}`)
	}
	if !revisions["tenant-a"] || !revisions["tenant-b"] {
		t.Errorf("t4_current_revision per db served: %v, want tenant-a and tenant-b", revisions)
	}
}

func TestRunRejectsInvalidDatabases(t *testing.T) {
	pki := newTestPKI(t)
	serverCert, serverKey := pki.issue(t, "localhost", true)
	tlsFlags := []string{"--client-tls-cert", serverCert, "--client-tls-key", serverKey, "--client-tls-ca", pki.caPath}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no client mTLS", []string{"--db", "a"}, "--db requires --client-tls-cert"},
		{"invalid name", append([]string{"--db", "A/b"}, tlsFlags...), "must be a DNS label"},
		{"duplicate name", append([]string{"--db", "a", "--db", "a"}, tlsFlags...), "listed twice"},
		{"peers", append([]string{"--db", "a", "--peer-listen", "127.0.0.1:0"}, tlsFlags...), "does not support --peer-listen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewRootCmd()
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs(append([]string{"run", "--data-dir", t.TempDir(), "--metrics-addr", "127.0.0.1:0"}, tc.args...))
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

type testPKI struct {
	caPath string
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
	pool   *x509.CertPool
	dir    string
	serial int64
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "t4-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	p := &testPKI{ca: ca, caKey: key, pool: x509.NewCertPool(), dir: t.TempDir(), serial: 1}
	p.pool.AddCert(ca)
	p.caPath = filepath.Join(p.dir, "ca.crt")
	writeTestPEM(t, p.caPath, "CERTIFICATE", der)
	return p
}

// issue writes a certificate for cn signed by the CA and returns its paths.
func (p *testPKI) issue(t *testing.T, cn string, server bool) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p.serial++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(p.serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(p.dir, cn+".crt")
	keyPath = filepath.Join(p.dir, cn+".key")
	writeTestPEM(t, certPath, "CERTIFICATE", der)
	writeTestPEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

// client returns an etcd client authenticating as cn.
func (p *testPKI) client(t *testing.T, addr, cn string) *clientv3.Client {
	t.Helper()
	certPath, keyPath := p.issue(t, cn, false)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://" + addr},
		DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{RootCAs: p.pool, Certificates: []tls.Certificate{cert}, ServerName: "localhost"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// put retries until the server is up.
func put(t *testing.T, c *clientv3.Client, key, value string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.Put(ctx, key, value)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("put %s: %v", key, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func get(t *testing.T, c *clientv3.Client, key string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}
