package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/pkg/object"
)

// Google Cloud Storage, end to end.
//
// This is the claim #373 says is impossible through GCS's S3-compatible
// endpoint: a cluster on GCS electing one leader and surviving its loss. That
// endpoint accepts the conditional writes the leader lock is built on and
// ignores them, so two nodes would both be told they won the election; GCS's
// own generation preconditions are enforced, which is what the native backend
// uses.
//
// Set T4_E2E_GCS=1 and GCS_ENDPOINT to an emulator, which CI starts:
//
//	docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http
//	T4_E2E_GCS=1 GCS_ENDPOINT=http://127.0.0.1:4443 go test ./tests/e2e -run GCS

func gcsConfig(t *testing.T, name string) object.GCSConfig {
	t.Helper()
	if os.Getenv("T4_E2E_GCS") == "" {
		t.Skip("set T4_E2E_GCS=1 to run the GCS-backed tests")
	}
	endpoint := os.Getenv("GCS_ENDPOINT")
	bucket := fmt.Sprintf("t4-e2e-%d", time.Now().UnixNano())

	if endpoint != "" {
		createGCSBucket(t, endpoint, bucket)
	} else if b := os.Getenv("GCS_BUCKET"); b != "" {
		// Against real GCS the bucket already exists and is shared, so each
		// run takes its own prefix instead of its own bucket.
		bucket = b
	} else {
		t.Skip("set GCS_ENDPOINT for an emulator, or GCS_BUCKET for a real bucket")
	}

	return object.GCSConfig{
		Bucket:          bucket,
		Prefix:          fmt.Sprintf("t4/%s-%d", name, time.Now().UnixNano()),
		Endpoint:        endpoint,
		CredentialsFile: os.Getenv("GCS_CREDENTIALS_FILE"),
	}
}

func createGCSBucket(t *testing.T, endpoint, bucket string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(endpoint, "/")+"/storage/v1/b",
		strings.NewReader(fmt.Sprintf(`{"name":%q}`, bucket)))
	if err != nil {
		t.Fatalf("create bucket request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusConflict {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		t.Fatalf("create bucket %q: HTTP %d: %s", bucket, resp.StatusCode, msg)
	}
}

func newGCSStore(t *testing.T, ctx context.Context, name string) *object.GCSStore {
	t.Helper()
	store, err := object.NewGCSStoreFromConfig(ctx, gcsConfig(t, name))
	if err != nil {
		t.Fatalf("new GCS store: %v", err)
	}
	return store
}

// Before trusting a cluster to this store, confirm it enforces what the leader
// lock depends on. If this fails, nothing below is meaningful — and a failure
// here is the specific, legible one rather than a puzzling split brain.
func TestGCSStoreEnforcesConditionalWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := newGCSStore(t, ctx, "probe")

	support, err := object.ProbeConditional(ctx, store, "node-0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !support.Enforced() {
		t.Fatalf("GCS must enforce both preconditions, got %+v", support)
	}
}

func TestGCSSingleNodeRecoversFromTheObjectStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg := gcsConfig(t, "recover")
	store, err := object.NewGCSStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("new GCS store: %v", err)
	}

	open := func() *t4.Node {
		t.Helper()
		node, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			CheckpointInterval: 25 * time.Millisecond,
			CheckpointEntries:  1,
			SegmentMaxAge:      25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return node
	}

	node := open()
	rev, err := node.Put(ctx, "/gcs/durable", []byte("written-to-gcs"), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	waitForCheckpointAtLeast(t, ctx, store, rev)
	if err := node.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A fresh data directory, so everything has to come back from GCS.
	recovered := open()
	t.Cleanup(func() { _ = recovered.Close() })
	assertNodeValue(t, ctx, recovered, rev, "/gcs/durable", "written-to-gcs")
}

// The point of the backend: a cluster on GCS elects one leader, replicates,
// bootstraps a joiner from the object store, and survives losing the leader.
func TestGCSCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cfg := gcsConfig(t, "cluster")
	store, err := object.NewGCSStoreFromConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("new GCS store: %v", err)
	}

	openNode := func(id string) *t4.Node {
		t.Helper()
		peerAddr := freeAddr(t)
		node, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			NodeID:             id,
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 25 * time.Millisecond,
			CheckpointEntries:  1,
			SegmentMaxAge:      25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = node.Close() })
		return node
	}

	nodes := []*t4.Node{openNode("node-0"), openNode("node-1")}
	leader := waitForLeader(t, nodes, 30*time.Second)
	follower := otherNode(nodes, leader)

	// Exactly one leader. Two would mean the lock's conditional writes were
	// not enforced, which is the failure this whole backend exists to avoid.
	leaders := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d nodes believe they are leader; GCS is not keeping the lock honest", leaders)
	}

	rev, err := leader.Put(ctx, "/gcs-cluster/replicated", []byte("replicated"), 0)
	if err != nil {
		t.Fatalf("leader put: %v", err)
	}
	assertNodeValue(t, ctx, follower, rev, "/gcs-cluster/replicated", "replicated")

	rev, err = follower.Put(ctx, "/gcs-cluster/forwarded", []byte("forwarded"), 0)
	if err != nil {
		t.Fatalf("forwarded put: %v", err)
	}
	assertNodeValue(t, ctx, leader, rev, "/gcs-cluster/forwarded", "forwarded")
	waitForCheckpointAtLeast(t, ctx, store, rev)

	// A node joining after a checkpoint exists bootstraps from GCS.
	joiner := openNode("node-2")
	assertNodeValue(t, ctx, joiner, rev, "/gcs-cluster/replicated", "replicated")

	if err := leader.Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	survivors := []*t4.Node{follower, joiner}
	newLeader := waitForLeader(t, survivors, 60*time.Second)
	rev, err = newLeader.Put(ctx, "/gcs-cluster/after-failover", []byte("after-failover"), 0)
	if err != nil {
		t.Fatalf("put after failover: %v", err)
	}
	assertNodeValue(t, ctx, otherNode(survivors, newLeader), rev, "/gcs-cluster/after-failover", "after-failover")
}
