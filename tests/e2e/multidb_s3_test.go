package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/t4db/t4/internal/cli"
)

// TestMultiDatabaseS3 runs the t4 binary with two databases on a real
// S3-compatible bucket. Each client certificate reaches only its own
// database; each database's objects stay below <prefix>/dbs/<name>/, where
// the CLI finds its checkpoints; and after the process stops and loses its
// data directory, each database comes back from S3 with its own data.
func TestMultiDatabaseS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("multidb-%d", time.Now().UnixNano())
	o := newObjectStoreTest(t, ctx, prefix, false)

	workDir := t.TempDir()
	certs := newCA(t, workDir, "t4-multidb-test-ca")
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
		}, restoreS3CLIArgs(o)...)
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

	var revA, revB int64
	for i := range 3 {
		ra, err := a.Put(ctx, fmt.Sprintf("/k/%d", i), fmt.Sprintf("a-%d", i))
		if err != nil {
			t.Fatalf("put tenant-a: %v", err)
		}
		rb, err := b.Put(ctx, fmt.Sprintf("/k/%d", i), fmt.Sprintf("b-%d", i))
		if err != nil {
			t.Fatalf("put tenant-b: %v", err)
		}
		revA, revB = ra.Header.Revision, rb.Header.Revision
	}
	assertTenantData(t, ctx, a, "a")
	assertTenantData(t, ctx, b, "b")

	// Each database checkpoints below its own prefix, where the CLI finds it.
	// Checkpoints carry T4's own revisions, one below the etcd API's.
	waitForCheckpointAtLeast(t, ctx, o.store.Sub("dbs/tenant-a"), revA-1)
	waitForCheckpointAtLeast(t, ctx, o.store.Sub("dbs/tenant-b"), revB-1)
	listArgs := restoreS3CLIArgs(o)
	for i, arg := range listArgs {
		if arg == "--s3-prefix" {
			listArgs[i+1] = path.Join(prefix, "dbs", "tenant-a")
		}
	}
	listOut := &bytes.Buffer{}
	listCmd := cli.NewRootCmd()
	listCmd.SetOut(listOut)
	listCmd.SetErr(listOut)
	listCmd.SetArgs(append([]string{"restore", "list"}, listArgs...))
	if err := listCmd.Execute(); err != nil {
		t.Fatalf("restore list for tenant-a: %v\n%s", err, listOut)
	}
	if !strings.Contains(listOut.String(), "(latest)") {
		t.Fatalf("restore list for tenant-a shows no latest checkpoint:\n%s", listOut)
	}

	// Every object of the process lies below one database's prefix.
	seen := map[string]bool{}
	for obj := range o.raw.ListObjects(ctx, o.bucket, minio.ListObjectsOptions{Prefix: prefix + "/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list objects: %v", obj.Err)
		}
		rel := strings.TrimPrefix(obj.Key, prefix+"/")
		db, _, ok := strings.Cut(strings.TrimPrefix(rel, "dbs/"), "/")
		if !strings.HasPrefix(rel, "dbs/") || !ok || (db != "tenant-a" && db != "tenant-b") {
			t.Fatalf("object %q is not below %s/dbs/<database>/", obj.Key, prefix)
		}
		seen[db] = true
	}
	if !seen["tenant-a"] || !seen["tenant-b"] {
		t.Fatalf("objects found for databases %v, want tenant-a and tenant-b", seen)
	}

	// Without its data directory, each database comes back from S3.
	stop(cmd, log)
	cmd, log = start(filepath.Join(workDir, "data-2"))
	defer stop(cmd, log)
	waitServed(t, ctx, a, log)
	waitServed(t, ctx, b, log)
	assertTenantData(t, ctx, a, "a")
	assertTenantData(t, ctx, b, "b")
}

// waitServed waits until cli's database answers a read.
func waitServed(t *testing.T, ctx context.Context, cli *clientv3.Client, log *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := cli.Get(readCtx, "/")
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("database not served: %v\nt4 log:\n%s", err, log)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// assertTenantData checks that the database holds exactly the keys its
// tenant wrote, with values tagged by tag.
func assertTenantData(t *testing.T, ctx context.Context, cli *clientv3.Client, tag string) {
	t.Helper()
	resp, err := cli.Get(ctx, "/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var got []string
	for _, kv := range resp.Kvs {
		got = append(got, fmt.Sprintf("%s=%s", kv.Key, kv.Value))
	}
	want := fmt.Sprintf("/k/0=%[1]s-0 /k/1=%[1]s-1 /k/2=%[1]s-2", tag)
	if strings.Join(got, " ") != want {
		t.Fatalf("database of tenant %s holds %v, want %s", tag, got, want)
	}
}
