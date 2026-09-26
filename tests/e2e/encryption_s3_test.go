package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"
)

// encryptedObjectMagic is the header every object written through
// object.NewEncryptedStore starts with.
const encryptedObjectMagic = "T4E1"

// TestEncryptedS3Cluster runs a multi-node cluster with object-store
// encryption against a real S3-compatible bucket. It covers replication,
// write forwarding, leader failover, and a late-joining node that bootstraps
// from the encrypted checkpoint, then checks that every object in the prefix
// — WAL segments, checkpoints, the manifest and the leader lock — is encrypted.
func TestEncryptedS3Cluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	o := newObjectStoreTest(t, ctx, fmt.Sprintf("encrypted-cluster-%d", time.Now().UnixNano()), false)
	key := bytes.Repeat([]byte{0x7e}, 32)
	kp := newTestKeyProvider(t, key)

	openNode := func(id string) *t4.Node {
		t.Helper()
		peerAddr := freeAddr(t)
		node, err := t4.Open(t4.Config{
			DataDir:               t.TempDir(),
			ObjectStore:           o.store,
			ObjectStoreEncryption: &t4.ObjectStoreEncryptionConfig{KeyProvider: kp},
			NodeID:                id,
			PeerListenAddr:        peerAddr,
			AdvertisePeerAddr:     peerAddr,
			FollowerMaxRetries:    2,
			PeerBufferSize:        1000,
			CheckpointInterval:    25 * time.Millisecond,
			CheckpointEntries:     1,
			SegmentMaxAge:         25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = node.Close() })
		return node
	}

	nodes := []*t4.Node{openNode("node-0"), openNode("node-1")}
	leader := waitForLeader(t, nodes, 15*time.Second)
	follower := otherNode(nodes, leader)

	rev, err := leader.Put(ctx, "/enc-cluster/replicated", []byte("secret-replicated"), 0)
	if err != nil {
		t.Fatalf("leader put: %v", err)
	}
	assertNodeValue(t, ctx, follower, rev, "/enc-cluster/replicated", "secret-replicated")

	rev, err = follower.Put(ctx, "/enc-cluster/forwarded", []byte("secret-forwarded"), 0)
	if err != nil {
		t.Fatalf("forwarded put: %v", err)
	}
	assertNodeValue(t, ctx, leader, rev, "/enc-cluster/forwarded", "secret-forwarded")
	waitForCheckpointAtLeast(t, ctx, object.NewEncryptedStore(o.store, kp), rev)

	// A node joining after a checkpoint exists must bootstrap from the
	// encrypted checkpoint and WAL.
	joiner := openNode("node-2")
	assertNodeValue(t, ctx, joiner, rev, "/enc-cluster/replicated", "secret-replicated")
	assertNodeValue(t, ctx, joiner, rev, "/enc-cluster/forwarded", "secret-forwarded")

	if err := leader.Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	survivors := []*t4.Node{follower, joiner}
	newLeader := waitForLeader(t, survivors, 30*time.Second)
	rev, err = newLeader.Put(ctx, "/enc-cluster/after-failover", []byte("secret-after-failover"), 0)
	if err != nil {
		t.Fatalf("put after failover: %v", err)
	}
	assertNodeValue(t, ctx, otherNode(survivors, newLeader), rev, "/enc-cluster/after-failover", "secret-after-failover")
	waitForCheckpointAtLeast(t, ctx, object.NewEncryptedStore(o.store, kp), rev)

	cfg := o.cfg
	cfg.encKey = key
	assertPrefixEncrypted(t, ctx, o.raw, cfg,
		[]string{"wal/", "checkpoint/", checkpoint.ManifestKey, election.LockKey},
		"secret-", "/enc-cluster/")
}

// TestEncryptedS3RejectsMismatchedPrefix verifies that a node fails closed
// instead of serving or overwriting data when its encryption setting does not
// match what is already stored under the prefix.
func TestEncryptedS3RejectsMismatchedPrefix(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	wrongKey := bytes.Repeat([]byte{0x22}, 32)

	seed := func(t *testing.T, ctx context.Context, o *objectStoreConfig, enc *t4.ObjectStoreEncryptionConfig) int64 {
		t.Helper()
		node, err := t4.Open(t4.Config{
			DataDir:               t.TempDir(),
			ObjectStore:           o.store,
			ObjectStoreEncryption: enc,
			CheckpointInterval:    25 * time.Millisecond,
			CheckpointEntries:     1,
			SegmentMaxAge:         25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open seed node: %v", err)
		}
		var rev int64
		for i := range 3 {
			rev, err = node.Put(ctx, fmt.Sprintf("/mismatch/%d", i), []byte("v"), 0)
			if err != nil {
				_ = node.Close()
				t.Fatalf("seed put: %v", err)
			}
		}
		var cpStore object.Store = o.store
		if enc != nil {
			cpStore = object.NewEncryptedStore(o.store, enc.KeyProvider)
		}
		waitForCheckpointAtLeast(t, ctx, cpStore, rev)
		if err := node.Close(); err != nil {
			t.Fatalf("close seed node: %v", err)
		}
		return rev
	}

	cases := []struct {
		name      string
		seedEnc   func(t *testing.T) *t4.ObjectStoreEncryptionConfig
		reopenKey []byte
	}{
		{
			name:      "encrypted node on plaintext prefix",
			seedEnc:   func(*testing.T) *t4.ObjectStoreEncryptionConfig { return nil },
			reopenKey: key,
		},
		{
			name: "wrong key on encrypted prefix",
			seedEnc: func(t *testing.T) *t4.ObjectStoreEncryptionConfig {
				return &t4.ObjectStoreEncryptionConfig{KeyProvider: newTestKeyProvider(t, key)}
			},
			reopenKey: wrongKey,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			o := newObjectStoreTest(t, ctx, fmt.Sprintf("encrypted-mismatch-%d", time.Now().UnixNano()), false)
			seedEnc := tc.seedEnc(t)
			seed(t, ctx, o, seedEnc)

			before, err := s3GetObject(ctx, o.raw, o.cfg, checkpoint.ManifestKey)
			if err != nil {
				t.Fatalf("read manifest before reopen: %v", err)
			}

			node, err := t4.Open(t4.Config{
				DataDir:               t.TempDir(),
				ObjectStore:           o.store,
				ObjectStoreEncryption: &t4.ObjectStoreEncryptionConfig{KeyProvider: newTestKeyProvider(t, tc.reopenKey)},
			})
			if err == nil {
				_ = node.Close()
				t.Fatalf("node opened on a prefix it cannot decrypt")
			}
			t.Logf("open failed as expected: %v", err)

			after, err := s3GetObject(ctx, o.raw, o.cfg, checkpoint.ManifestKey)
			if err != nil {
				t.Fatalf("read manifest after failed reopen: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("failed open modified manifest/latest")
			}
		})
	}
}

// TestEncryptedS3ConditionalPutIsAtomic races two conditional writers through
// the encrypted store against real S3. Exactly one must win and the loser must
// see ErrPreconditionFailed; leader election relies on this to prevent two
// nodes from taking the same term.
func TestEncryptedS3ConditionalPutIsAtomic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	o := newObjectStoreTest(t, ctx, fmt.Sprintf("encrypted-cas-%d", time.Now().UnixNano()), false)
	store, ok := object.NewEncryptedStore(o.store, newTestKeyProvider(t, bytes.Repeat([]byte{0x33}, 32))).(object.ConditionalStore)
	if !ok {
		t.Fatal("encrypted S3 store does not implement ConditionalStore")
	}

	race := func(t *testing.T, put func(w int) error) {
		t.Helper()
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for w := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[w] = put(w)
			}()
		}
		wg.Wait()
		wins := 0
		for _, err := range errs {
			switch {
			case err == nil:
				wins++
			case !errors.Is(err, object.ErrPreconditionFailed):
				t.Fatalf("losing writer got %v, want ErrPreconditionFailed", err)
			}
		}
		if wins != 1 {
			t.Fatalf("%d writers won, want exactly 1 (errs=%v)", wins, errs)
		}
	}

	for i := range 10 {
		key := fmt.Sprintf("cas/%d", i)
		t.Run(fmt.Sprintf("PutIfAbsent/%d", i), func(t *testing.T) {
			race(t, func(w int) error {
				return store.PutIfAbsent(ctx, key, strings.NewReader(fmt.Sprintf("absent-%d", w)))
			})
		})
		res, err := store.GetETag(ctx, key)
		if err != nil {
			t.Fatalf("GetETag %s: %v", key, err)
		}
		_ = res.Body.Close()
		t.Run(fmt.Sprintf("PutIfMatch/%d", i), func(t *testing.T) {
			race(t, func(w int) error {
				return store.PutIfMatch(ctx, key, strings.NewReader(fmt.Sprintf("match-%d", w)), res.ETag)
			})
		})
	}
}

func newTestKeyProvider(t *testing.T, key []byte) *object.StaticKeyProvider {
	t.Helper()
	kp, err := object.NewStaticKeyProvider(key)
	if err != nil {
		t.Fatalf("encryption key provider: %v", err)
	}
	return kp
}

// decryptObject decrypts the raw body of the object stored at the logical
// (prefix-relative) key relKey. The logical key is authenticated data, so it
// must match the key the object was written under.
func decryptObject(ctx context.Context, key []byte, relKey string, raw []byte) ([]byte, error) {
	kp, err := object.NewStaticKeyProvider(key)
	if err != nil {
		return nil, err
	}
	mem := object.NewMem()
	if err := mem.Put(ctx, relKey, bytes.NewReader(raw)); err != nil {
		return nil, err
	}
	rc, err := object.NewEncryptedStore(mem, kp).Get(ctx, relKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// assertPrefixEncrypted reads every raw object under cfg's prefix and checks
// that it carries the encrypted-object header, decrypts with cfg.encKey, and
// does not contain any of the plaintext markers. Each entry in required must
// match at least one object (by exact key or key prefix), so the check cannot
// pass vacuously for an object class the test expects to exist.
func assertPrefixEncrypted(t *testing.T, ctx context.Context, client *minio.Client, cfg s3TestConfig, required []string, plaintextMarkers ...string) {
	t.Helper()
	listPrefix := ""
	if cfg.prefix != "" {
		listPrefix = strings.TrimSuffix(cfg.prefix, "/") + "/"
	}
	seen := make(map[string]int, len(required))
	total := 0
	for obj := range client.ListObjects(ctx, cfg.bucket, minio.ListObjectsOptions{Prefix: listPrefix, Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list objects: %v", obj.Err)
		}
		relKey := strings.TrimPrefix(obj.Key, listPrefix)
		raw, err := s3GetObject(ctx, client, cfg, relKey)
		if err != nil {
			t.Fatalf("get raw %s: %v", relKey, err)
		}
		if !bytes.HasPrefix(raw, []byte(encryptedObjectMagic)) {
			t.Errorf("object %s is not encrypted; prefix=%q", relKey, raw[:min(len(raw), 8)])
			continue
		}
		for _, marker := range plaintextMarkers {
			if bytes.Contains(raw, []byte(marker)) {
				t.Errorf("raw object %s contains plaintext %q", relKey, marker)
			}
		}
		if _, err := decryptObject(ctx, cfg.encKey, relKey, raw); err != nil {
			t.Errorf("decrypt %s: %v", relKey, err)
		}
		for _, r := range required {
			if relKey == r || (strings.HasSuffix(r, "/") && strings.HasPrefix(relKey, r)) {
				seen[r]++
			}
		}
		total++
	}
	for _, r := range required {
		if seen[r] == 0 {
			t.Errorf("no object matching %q found under prefix %q", r, cfg.prefix)
		}
	}
	t.Logf("checked %d encrypted objects under %q", total, cfg.prefix)
}

// assertEncryptedCLIRejectsBadKeys runs the key-aware CLI commands against an
// encrypted prefix with no key and with a wrong key, and checks that each one
// fails without changing what is stored.
func assertEncryptedCLIRejectsBadKeys(t *testing.T, ctx context.Context, cfg s3TestConfig, workDir string) {
	t.Helper()
	s3cli, err := s3Client(ctx, cfg)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	manifestBefore, err := s3GetObject(ctx, s3cli, cfg, checkpoint.ManifestKey)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	noKey := cfg
	noKey.encKeyFile = ""
	wrongKey := cfg
	wrongKey.encKeyFile = writeObjectStoreEncryptionKeyFile(t, bytes.Repeat([]byte{0xa5}, 32))

	for _, variant := range []struct {
		name string
		cfg  s3TestConfig
	}{{"no key", noKey}, {"wrong key", wrongKey}} {
		const badBranch = "bad-key-branch"
		commands := [][]string{
			{"status"},
			{"restore", "checkpoint", "--data-dir", filepath.Join(workDir, fmt.Sprintf("bad-restore-%d", time.Now().UnixNano()))},
			{"branch", "fork", "--branch-id", badBranch},
			{"gc", "--keep", "1"},
		}
		for _, cmd := range commands {
			args := append(append([]string{}, cmd...), s3Args(variant.cfg)...)
			if out, err := runT4(ctx, variant.cfg, args...); err == nil {
				t.Errorf("t4 %s with %s unexpectedly succeeded:\n%s", strings.Join(cmd, " "), variant.name, out)
			}
		}
		// restore list is best-effort: checkpoint keys are plaintext, so it
		// still lists them, but must not surface any decrypted metadata.
		listArgs := append([]string{"restore", "list"}, s3Args(variant.cfg)...)
		if out, err := runT4(ctx, variant.cfg, listArgs...); err != nil {
			t.Errorf("t4 restore list with %s: %v\n%s", variant.name, err, out)
		} else if strings.Contains(out, "(latest)") || !strings.Contains(out, "?") {
			t.Errorf("t4 restore list with %s showed decrypted checkpoint metadata:\n%s", variant.name, out)
		}
		if ok, err := s3KeyExists(ctx, s3cli, cfg, "branches/"+badBranch); err != nil {
			t.Fatalf("HEAD bad branch registry: %v", err)
		} else if ok {
			t.Errorf("branch fork with %s created registry entry", variant.name)
		}
	}

	manifestAfter, err := s3GetObject(ctx, s3cli, cfg, checkpoint.ManifestKey)
	if err != nil {
		t.Fatalf("read manifest after bad-key commands: %v", err)
	}
	if !bytes.Equal(manifestBefore, manifestAfter) {
		t.Errorf("bad-key commands modified manifest/latest")
	}
	// The latest checkpoint must still restore with the right key.
	if _, err := waitForRestoredCount(ctx, cfg, workDir, "/smoke/", 3); err != nil {
		t.Fatalf("restore with correct key after bad-key commands: %v", err)
	}
}

func waitForLeader(t *testing.T, nodes []*t4.Node, timeout time.Duration) *t4.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func otherNode(nodes []*t4.Node, not *t4.Node) *t4.Node {
	for _, n := range nodes {
		if n != not {
			return n
		}
	}
	return nil
}

func assertNodeValue(t *testing.T, ctx context.Context, node *t4.Node, rev int64, key, want string) {
	t.Helper()
	if err := node.WaitForRevision(ctx, rev); err != nil {
		t.Fatalf("WaitForRevision(%d): %v", rev, err)
	}
	kv, err := node.Get(key)
	if err != nil || kv == nil || string(kv.Value) != want {
		t.Fatalf("get %s: err=%v kv=%v, want %q", key, err, kv, want)
	}
}
