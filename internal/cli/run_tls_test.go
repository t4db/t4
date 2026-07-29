package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
)

func TestEtcdAPIClientTLSNegotiatesHybridPostQuantumKeyExchange(t *testing.T) {
	certPath, keyPath, roots := writeTLSTestCertificate(t)
	serverCreds, err := buildClientTLS(certPath, keyPath, "")
	if err != nil {
		t.Fatalf("build client-facing TLS: %v", err)
	}
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open t4 node: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverOpts := append(t4etcd.NewServerOptions(nil, nil), grpc.Creds(serverCreds))
	server := grpc.NewServer(serverOpts...)
	t4etcd.New(node, nil, nil).Register(server)
	go func() {
		_ = server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	negotiated := make(chan tls.ConnectionState, 1)
	clientTLS := &tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
		VerifyConnection: func(state tls.ConnectionState) error {
			negotiated <- state
			return nil
		},
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://" + lis.Addr().String()},
		DialTimeout: 5 * time.Second,
		TLS:         clientTLS,
	})
	if err != nil {
		t.Fatalf("create etcd TLS client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, "/pqc/transport", "hybrid"); err != nil {
		t.Fatalf("etcd Put over TLS: %v", err)
	}
	response, err := client.Get(ctx, "/pqc/transport")
	if err != nil {
		t.Fatalf("etcd Get over TLS: %v", err)
	}
	if len(response.Kvs) != 1 || string(response.Kvs[0].Value) != "hybrid" {
		t.Fatalf("etcd Get = %q, want %q", response.Kvs, "hybrid")
	}

	assertHybridPostQuantumTLS(t, ctx, negotiated)
}

func TestPeerTLSNegotiatesHybridPostQuantumKeyExchange(t *testing.T) {
	caPath, certPath, keyPath := writePeerTLSTestCertificates(t)
	serverCreds, clientCreds, err := buildPeerTLS(caPath, certPath, keyPath)
	if err != nil {
		t.Fatalf("build peer TLS: %v", err)
	}

	negotiated := make(chan tls.ConnectionState, 1)
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		remote, ok := peer.FromContext(ctx)
		if !ok {
			t.Error("peer RPC has no remote peer information")
		} else if tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo); !ok {
			t.Errorf("peer RPC auth info is %T, want credentials.TLSInfo", remote.AuthInfo)
		} else {
			negotiated <- tlsInfo.State
		}
		return handler(ctx, req)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(serverCreds), grpc.UnaryInterceptor(interceptor))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() {
		_ = server.Serve(lis)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	if err != nil {
		t.Fatalf("create peer TLS client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("peer RPC over mTLS: %v", err)
	}

	assertHybridPostQuantumTLS(t, ctx, negotiated)
}

func assertHybridPostQuantumTLS(t *testing.T, ctx context.Context, negotiated <-chan tls.ConnectionState) {
	t.Helper()
	select {
	case state := <-negotiated:
		if state.Version != tls.VersionTLS13 {
			t.Fatalf("TLS version = %#x, want TLS 1.3 (%#x)", state.Version, tls.VersionTLS13)
		}
		if state.CurveID != tls.X25519MLKEM768 {
			t.Fatalf("key exchange = %v, want %v", state.CurveID, tls.X25519MLKEM768)
		}
	case <-ctx.Done():
		t.Fatal("TLS handshake completed without reporting its negotiated state")
	}
}

func writeTLSTestCertificate(t *testing.T) (certPath, keyPath string, roots *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate TLS test key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create TLS test certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal TLS test key: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "server.crt")
	keyPath = filepath.Join(dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write TLS test certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write TLS test key: %v", err)
	}

	roots = x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("add TLS test certificate to root pool")
	}
	return certPath, keyPath, roots
}

func writePeerTLSTestCertificates(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate peer test CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "t4-peer-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create peer test CA: %v", err)
	}

	peerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate peer test key: %v", err)
	}
	peerTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "t4-peer"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	peerDER, err := x509.CreateCertificate(rand.Reader, peerTemplate, caTemplate, &peerKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create peer test certificate: %v", err)
	}
	peerKeyDER, err := x509.MarshalECPrivateKey(peerKey)
	if err != nil {
		t.Fatalf("marshal peer test key: %v", err)
	}

	dir := t.TempDir()
	caPath = filepath.Join(dir, "ca.crt")
	certPath = filepath.Join(dir, "peer.crt")
	keyPath = filepath.Join(dir, "peer.key")
	writeTestPEM(t, caPath, "CERTIFICATE", caDER)
	writeTestPEM(t, certPath, "CERTIFICATE", peerDER)
	writeTestPEM(t, keyPath, "EC PRIVATE KEY", peerKeyDER)
	return caPath, certPath, keyPath
}

func writeTestPEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
