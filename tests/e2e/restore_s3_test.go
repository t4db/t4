package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/cli"
	"github.com/t4db/t4/pkg/object"
)

// TestRestoreCheckpointFromS3Smoke exercises the `t4 restore checkpoint`
// command against a real S3-compatible bucket. The test seeds the bucket by running
// an in-process source node, then invokes the CLI restore command.
func TestRestoreCheckpointFromS3Smoke(t *testing.T) {
	if os.Getenv("T4_E2E_S3") == "" {
		t.Skip("set T4_E2E_S3=1 to run the S3-backed restore smoke test")
	}

	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, fmt.Sprintf("smoke-%d", time.Now().UnixNano()), false)

	source, err := t4.Open(t4.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        o.store,
		CheckpointInterval: 25 * time.Millisecond,
		CheckpointEntries:  1,
		SegmentMaxAge:      25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open source node: %v", err)
	}

	var lastRev int64
	for i := range 3 {
		lastRev, err = source.Put(ctx, fmt.Sprintf("/restore/%d", i), []byte(fmt.Sprintf("value-%d", i)), 0)
		if err != nil {
			t.Fatalf("put source key %d: %v", i, err)
		}
	}
	waitForCheckpointAtLeast(t, ctx, o.store, lastRev)
	if err := source.Close(); err != nil {
		t.Fatalf("close source node: %v", err)
	}

	args := restoreS3CLIArgs(o)

	listOut := &bytes.Buffer{}
	listCmd := cli.NewRootCmd()
	listCmd.SetOut(listOut)
	listCmd.SetErr(listOut)
	listCmd.SetArgs(append([]string{"restore", "list"}, args...))
	if err := listCmd.Execute(); err != nil {
		t.Fatalf("restore list: %v\noutput:\n%s", err, listOut.String())
	}
	if out := listOut.String(); !strings.Contains(out, "CHECKPOINT") || !strings.Contains(out, "(latest)") {
		t.Fatalf("expected listed latest checkpoint, got:\n%s", out)
	}

	restoreDir := t.TempDir()
	restoreOut := &bytes.Buffer{}
	restoreCmd := cli.NewRootCmd()
	restoreCmd.SetOut(restoreOut)
	restoreCmd.SetErr(restoreOut)
	restoreCmd.SetArgs(append([]string{"restore", "checkpoint", "--data-dir", restoreDir}, args...))
	if err := restoreCmd.Execute(); err != nil {
		t.Fatalf("restore checkpoint: %v\noutput:\n%s", err, restoreOut.String())
	}
	if out := restoreOut.String(); !strings.Contains(out, "Restored checkpoint") || !strings.Contains(out, fmt.Sprintf("revision:  %d", lastRev)) {
		t.Fatalf("expected restore summary for revision %d, got:\n%s", lastRev, out)
	}

	countOut := &bytes.Buffer{}
	countCmd := cli.NewRootCmd()
	countCmd.SetOut(countOut)
	countCmd.SetErr(countOut)
	countCmd.SetArgs([]string{"inspect", "count", "--data-dir", restoreDir, "--prefix", "/restore/"})
	if err := countCmd.Execute(); err != nil {
		t.Fatalf("inspect restored count: %v\noutput:\n%s", err, countOut.String())
	}
	if got := strings.TrimSpace(countOut.String()); got != "/restore/: 3" {
		t.Fatalf("unexpected restored count output: %q", got)
	}
}

// TestRestoreCheckpointFromEncryptedS3Smoke exercises object-store encryption
// against a real S3-compatible bucket. It verifies that T4 writes encrypted checkpoint
// metadata to S3 and that the CLI can list and restore it only when the same
// object-store encryption key is supplied.
func TestRestoreCheckpointFromEncryptedS3Smoke(t *testing.T) {
	if os.Getenv("T4_E2E_S3") == "" {
		t.Skip("set T4_E2E_S3=1 to run the encrypted S3 restore smoke test")
	}

	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, fmt.Sprintf("encrypted-smoke-%d", time.Now().UnixNano()), false)

	keyBytes := bytes.Repeat([]byte{0x5a}, 32)
	keyProvider, err := object.NewStaticKeyProvider(keyBytes)
	if err != nil {
		t.Fatalf("encryption key provider: %v", err)
	}
	encryptedStore := object.NewEncryptedStore(o.store, keyProvider)

	source, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(),
		ObjectStore: o.store,
		ObjectStoreEncryption: &t4.ObjectStoreEncryptionConfig{
			KeyProvider: keyProvider,
		},
		CheckpointInterval: 25 * time.Millisecond,
		CheckpointEntries:  1,
		SegmentMaxAge:      25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open encrypted source node: %v", err)
	}
	sourceClosed := false
	t.Cleanup(func() {
		if !sourceClosed {
			_ = source.Close()
		}
	})

	var lastRev int64
	for i := range 3 {
		lastRev, err = source.Put(ctx, fmt.Sprintf("/encrypted-restore/%d", i), []byte(fmt.Sprintf("secret-value-%d", i)), 0)
		if err != nil {
			t.Fatalf("put encrypted source key %d: %v", i, err)
		}
	}
	waitForCheckpointAtLeast(t, ctx, encryptedStore, lastRev)

	rawManifest, err := s3GetObject(ctx, o.raw, o.cfg, checkpoint.ManifestKey)
	if err != nil {
		t.Fatalf("read raw encrypted manifest: %v", err)
	}
	if !bytes.HasPrefix(rawManifest, []byte("T4E1")) {
		t.Fatalf("raw manifest is not in encrypted object format; prefix=%q", rawManifest[:min(len(rawManifest), 8)])
	}
	if bytes.Contains(rawManifest, []byte("checkpoint_key")) || bytes.Contains(rawManifest, []byte("secret-value")) {
		t.Fatalf("raw manifest contains plaintext content")
	}

	if err := source.Close(); err != nil {
		t.Fatalf("close encrypted source node: %v", err)
	}
	sourceClosed = true

	plainRestoreOut := &bytes.Buffer{}
	plainRestoreCmd := cli.NewRootCmd()
	plainRestoreCmd.SetOut(plainRestoreOut)
	plainRestoreCmd.SetErr(plainRestoreOut)
	plainRestoreCmd.SetArgs(append([]string{"restore", "checkpoint", "--data-dir", t.TempDir()}, restoreS3CLIArgs(o)...))
	if err := plainRestoreCmd.Execute(); err == nil {
		t.Fatalf("restore without encryption key unexpectedly succeeded:\n%s", plainRestoreOut.String())
	}

	keyPath := writeObjectStoreEncryptionKeyFile(t, keyBytes)
	args := append(restoreS3CLIArgs(o), "--object-store-encryption-key-file", keyPath)

	listOut := &bytes.Buffer{}
	listCmd := cli.NewRootCmd()
	listCmd.SetOut(listOut)
	listCmd.SetErr(listOut)
	listCmd.SetArgs(append([]string{"restore", "list"}, args...))
	if err := listCmd.Execute(); err != nil {
		t.Fatalf("encrypted restore list: %v\noutput:\n%s", err, listOut.String())
	}
	if out := listOut.String(); !strings.Contains(out, "CHECKPOINT") || !strings.Contains(out, "(latest)") {
		t.Fatalf("expected listed latest encrypted checkpoint, got:\n%s", out)
	}

	restoreDir := t.TempDir()
	restoreOut := &bytes.Buffer{}
	restoreCmd := cli.NewRootCmd()
	restoreCmd.SetOut(restoreOut)
	restoreCmd.SetErr(restoreOut)
	restoreCmd.SetArgs(append([]string{"restore", "checkpoint", "--data-dir", restoreDir}, args...))
	if err := restoreCmd.Execute(); err != nil {
		t.Fatalf("encrypted restore checkpoint: %v\noutput:\n%s", err, restoreOut.String())
	}
	if out := restoreOut.String(); !strings.Contains(out, "Restored checkpoint") || !strings.Contains(out, fmt.Sprintf("revision:  %d", lastRev)) {
		t.Fatalf("expected encrypted restore summary for revision %d, got:\n%s", lastRev, out)
	}

	countOut := &bytes.Buffer{}
	countCmd := cli.NewRootCmd()
	countCmd.SetOut(countOut)
	countCmd.SetErr(countOut)
	countCmd.SetArgs([]string{"inspect", "count", "--data-dir", restoreDir, "--prefix", "/encrypted-restore/"})
	if err := countCmd.Execute(); err != nil {
		t.Fatalf("inspect encrypted restored count: %v\noutput:\n%s", err, countOut.String())
	}
	if got := strings.TrimSpace(countOut.String()); got != "/encrypted-restore/: 3" {
		t.Fatalf("unexpected encrypted restored count output: %q", got)
	}
}

func restoreS3CLIArgs(o *objectStoreConfig) []string {
	return []string{
		"--s3-bucket", o.bucket,
		"--s3-prefix", o.prefix,
		"--s3-endpoint", o.cfg.endpoint,
		"--s3-region", o.cfg.region,
		"--s3-access-key-id", o.cfg.access,
		"--s3-secret-access-key", o.cfg.secret,
	}
}

func writeObjectStoreEncryptionKeyFile(t *testing.T, key []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "object-store-key")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatalf("write object-store encryption key file: %v", err)
	}
	return path
}

func waitForCheckpointAtLeast(t *testing.T, ctx context.Context, store object.Store, minRevision int64) {
	t.Helper()
	cp := checkpoint.New(nil)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		manifest, err := cp.ReadManifest(ctx, store)
		if err != nil {
			t.Fatalf("read checkpoint manifest: %v", err)
		}
		if manifest != nil && manifest.Revision >= minRevision {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for checkpoint at revision >= %d", minRevision)
}
