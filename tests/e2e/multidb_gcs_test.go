package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// TestMultiDatabaseGCS is TestMultiDatabaseS3 on the native GCS backend: two
// databases from one t4 run, each reached by its own client certificate, each
// below its own prefix in one bucket, each recovering from the store alone.
//
// It is worth running on both backends rather than trusting the S3 run,
// because dividing a store per database is the backend's own doing: the two
// implement Sub over different clients, and only an object listing says
// whether the keys really landed where the prefix claims.
func TestMultiDatabaseGCS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cfg := gcsConfig(t, "multidb")
	store, err := object.NewGCSStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("new GCS store: %v", err)
	}

	workDir := t.TempDir()
	certs := newCA(t, workDir, "t4-multidb-gcs-test-ca")
	caPath := certs.writeCACert(t, "ca.crt")
	serverCert, serverKey := certs.mintServer(t, "t4", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	certA, keyA := certs.mintClient(t, "tenant-a")
	certB, keyB := certs.mintClient(t, "tenant-b")
	bin := buildT4(t, ctx, workDir)
	listen := freeAddr(t)

	start := func(dataDir string) (*exec.Cmd, *bytes.Buffer) {
		t.Helper()
		args := append([]string{
			"run",
			"--db", "tenant-a", "--db", "tenant-b",
			"--data-dir", dataDir,
			"--listen", listen,
			"--metrics-addr", "127.0.0.1:0",
			"--checkpoint-entries", "1",
			"--segment-max-age-sec", "1",
			"--log-level", "warn",
			"--client-tls-cert", serverCert,
			"--client-tls-key", serverKey,
			"--client-tls-ca", caPath,
		}, gcsCLIArgs(cfg)...)
		var log bytes.Buffer
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Stdout, cmd.Stderr = &log, &log
		if err := cmd.Start(); err != nil {
			t.Fatalf("start t4: %v", err)
		}
		return cmd, &log
	}
	stop := func(cmd *exec.Cmd, log *bytes.Buffer) {
		t.Helper()
		stopNode(cmd)
		if !cmd.ProcessState.Success() {
			t.Fatalf("t4 did not stop cleanly on SIGTERM: %v\n%s", cmd.ProcessState, log)
		}
	}

	cmd, log := start(filepath.Join(workDir, "data-1"))
	a := newTLSClient(t, tlsEndpoint(listen), caPath, certA, keyA)
	b := newTLSClient(t, tlsEndpoint(listen), caPath, certB, keyB)
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	waitServed(t, ctx, a, log)
	waitServed(t, ctx, b, log)

	for i := range 3 {
		if _, err := a.Put(ctx, fmt.Sprintf("/k/%d", i), fmt.Sprintf("a-%d", i)); err != nil {
			t.Fatalf("put tenant-a: %v", err)
		}
		if _, err := b.Put(ctx, fmt.Sprintf("/k/%d", i), fmt.Sprintf("b-%d", i)); err != nil {
			t.Fatalf("put tenant-b: %v", err)
		}
	}
	assertTenantData(t, ctx, a, "a")
	assertTenantData(t, ctx, b, "b")

	// Every object the process wrote lies below one database's prefix. The
	// keys come back relative to the store's own prefix, so what is left is
	// exactly dbs/<database>/...
	waitForObjectsUnder(t, ctx, store, "tenant-a", "tenant-b")

	// Without its data directory, each database comes back from GCS.
	stop(cmd, log)
	cmd, log = start(filepath.Join(workDir, "data-2"))
	defer stop(cmd, log)
	waitServed(t, ctx, a, log)
	waitServed(t, ctx, b, log)
	assertTenantData(t, ctx, a, "a")
	assertTenantData(t, ctx, b, "b")
}

// gcsCLIArgs names cfg's bucket on the command line, as restoreS3CLIArgs does
// for S3.
func gcsCLIArgs(cfg object.GCSConfig) []string {
	args := []string{"--gcs-bucket", cfg.Bucket, "--gcs-prefix", cfg.Prefix}
	if cfg.Endpoint != "" {
		args = append(args, "--gcs-endpoint", cfg.Endpoint)
	}
	if cfg.CredentialsFile != "" {
		args = append(args, "--gcs-credentials-file", cfg.CredentialsFile)
	}
	return args
}

// waitForObjectsUnder waits until the store holds objects for every named
// database and asserts that it holds nothing outside them. Uploads are
// asynchronous, so the first listing may be empty or partial; only a listing
// with every database in it proves the prefixes are separate.
func waitForObjectsUnder(t *testing.T, ctx context.Context, store object.Store, databases ...string) {
	t.Helper()
	want := make(map[string]bool, len(databases))
	for _, db := range databases {
		want[db] = true
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		keys, err := store.List(ctx, "")
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}
		seen := map[string]bool{}
		for _, key := range keys {
			db, _, ok := strings.Cut(strings.TrimPrefix(key, "dbs/"), "/")
			if !strings.HasPrefix(key, "dbs/") || !ok || !want[db] {
				t.Fatalf("object %q is not below dbs/<database>/ for one of %v", key, databases)
			}
			seen[db] = true
		}
		if len(seen) == len(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("objects found for databases %v, want %v", seen, databases)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
