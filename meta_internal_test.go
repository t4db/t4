package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// openMetaNode opens a single node. Checkpoints are left to the test (see
// maybeCheckpoint) because they garbage-collect the WAL segments it inspects.
func openMetaNode(t *testing.T, store object.Store, dataDir string) *Node {
	t.Helper()
	n, err := Open(Config{
		DataDir:            dataDir,
		ObjectStore:        store,
		CheckpointInterval: time.Hour,
		SegmentMaxAge:      100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func metaCtx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func enableMetaForTest(t *testing.T, n *Node) {
	t.Helper()
	if err := n.enableMeta(metaCtx(t)); err != nil {
		t.Fatalf("enableMeta: %v", err)
	}
}

func TestMetaDisabledUntilEnabled(t *testing.T) {
	n := openMetaNode(t, object.NewMem(), t.TempDir())
	ctx := metaCtx(t)

	if ok, err := n.MetaEnabled(); err != nil || ok {
		t.Fatalf("MetaEnabled = %v, %v; want false", ok, err)
	}
	if err := n.MetaPut(ctx, "k", []byte("v")); !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("MetaPut: want ErrMetaDisabled, got %v", err)
	}
	if err := n.MetaDelete(ctx, "k"); !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("MetaDelete: want ErrMetaDisabled, got %v", err)
	}
	_, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{
		{Type: TxnPut, Key: "data", Value: []byte("v")},
		{Type: TxnMetaPut, Key: "k", Value: []byte("v")},
	}})
	if !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("Txn with meta op: want ErrMetaDisabled, got %v", err)
	}
	if kv, _ := n.Get("data"); kv != nil {
		t.Fatal("rejected txn partially applied")
	}

	enableMetaForTest(t, n)
	enableMetaForTest(t, n) // idempotent
	if ok, err := n.MetaEnabled(); err != nil || !ok {
		t.Fatalf("MetaEnabled = %v, %v; want true", ok, err)
	}
	if err := n.MetaPut(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("MetaPut after enable: %v", err)
	}
}

func TestMetaKeyValidation(t *testing.T) {
	n := openMetaNode(t, object.NewMem(), t.TempDir())
	enableMetaForTest(t, n)
	ctx := metaCtx(t)

	if err := n.MetaPut(ctx, metaFormatKey, []byte("x")); err == nil {
		t.Fatal("MetaPut of the reserved format key succeeded")
	}
	if err := n.MetaDelete(ctx, metaFormatKey); err == nil {
		t.Fatal("MetaDelete of the reserved format key succeeded")
	}
	if err := n.MetaPut(ctx, "", []byte("x")); err == nil {
		t.Fatal("MetaPut with empty key succeeded")
	}
	if _, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{{Type: TxnMetaDelete, Key: metaFormatKey}}}); err == nil {
		t.Fatal("Txn deleting the reserved format key succeeded")
	}
	if _, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{{Type: TxnOpType(42), Key: "k"}}}); err == nil {
		t.Fatal("Txn with an unknown op type succeeded")
	}
	if _, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{
		{Type: TxnMetaPut, Key: "k", Value: []byte("1")},
		{Type: TxnMetaDelete, Key: "k"},
	}}); err == nil {
		t.Fatal("Txn with a duplicate meta key succeeded")
	}
	// The same name in the data and meta keyspaces is not a duplicate.
	if _, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{
		{Type: TxnPut, Key: "k", Value: []byte("data")},
		{Type: TxnMetaPut, Key: "k", Value: []byte("meta")},
	}}); err != nil {
		t.Fatalf("Txn with the same key in both keyspaces: %v", err)
	}
}

// TestMetaOpsDoNotConsumeRevisions is the property the etcd replication design
// relies on: meta writes leave the revision sequence and watch stream exactly
// as if they had not happened.
func TestMetaOpsDoNotConsumeRevisions(t *testing.T) {
	n := openMetaNode(t, object.NewMem(), t.TempDir())
	enableMetaForTest(t, n)
	ctx := metaCtx(t)

	events, err := n.Watch(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := n.Put(ctx, "a", []byte("1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.MetaPut(ctx, "a", []byte("meta")); err != nil {
		t.Fatal(err)
	}
	resp, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{
		{Type: TxnMetaPut, Key: "b", Value: []byte("meta")},
		{Type: TxnMetaDelete, Key: "a"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Revision != r1 {
		t.Fatalf("meta-only txn revision = %d, want %d", resp.Revision, r1)
	}
	if err := n.MetaDelete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if got := n.CurrentRevision(); got != r1 {
		t.Fatalf("CurrentRevision after meta writes = %d, want %d", got, r1)
	}

	resp, err = n.Txn(ctx, TxnRequest{Success: []TxnOp{
		{Type: TxnPut, Key: "c", Value: []byte("3")},
		{Type: TxnMetaPut, Key: "c", Value: []byte("meta")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Revision != r1+1 {
		t.Fatalf("mixed txn revision = %d, want %d", resp.Revision, r1+1)
	}
	if v, ok, _ := n.MetaGet("c"); !ok || string(v) != "meta" {
		t.Fatalf("MetaGet(c) = %q, %v", v, ok)
	}
	if kv, _ := n.Get("c"); kv == nil || string(kv.Value) != "3" {
		t.Fatalf("Get(c) = %+v", kv)
	}

	var got []string
	for len(got) < 2 {
		select {
		case e := <-events:
			got = append(got, fmt.Sprintf("%s@%d", e.KV.Key, e.KV.Revision))
		case <-ctx.Done():
			t.Fatalf("timed out; events so far: %v", got)
		}
	}
	want := []string{fmt.Sprintf("a@%d", r1), fmt.Sprintf("c@%d", r1+1)}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("watch events = %v, want %v", got, want)
	}
	select {
	case e := <-events:
		t.Fatalf("unexpected extra watch event: %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestMetaSurvivesRestartAndRestore checks both recovery paths (local WAL and
// object storage) and that the durable artifacts carry the formats that keep
// older binaries out: WAL segments with meta ops at format 3 and checkpoints
// at format 2.
func TestMetaSurvivesRestartAndRestore(t *testing.T) {
	store := object.NewMem()
	dataDir := t.TempDir()
	n := openMetaNode(t, store, dataDir)
	ctx := metaCtx(t)

	if _, err := n.Put(ctx, "a", []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	n.maybeCheckpoint(ctx)
	if got := manifestFormat(t, store); got != checkpoint.FormatVersionBase {
		t.Fatalf("checkpoint format before meta = %d, want %d", got, checkpoint.FormatVersionBase)
	}

	enableMetaForTest(t, n)
	if err := n.MetaPut(ctx, "lease/1", []byte("ttl=10")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Put(ctx, "b", []byte("2"), 0); err != nil {
		t.Fatal(err)
	}
	assertMetaSegmentsAreFormat3(t, store)
	n.maybeCheckpoint(ctx)
	if got := manifestFormat(t, store); got != checkpoint.FormatVersionMeta {
		t.Fatalf("checkpoint format with meta = %d, want %d", got, checkpoint.FormatVersionMeta)
	}
	// Written after the last checkpoint, so recovery must replay it from WAL.
	if err := n.MetaPut(ctx, "lease/2", []byte("ttl=20")); err != nil {
		t.Fatal(err)
	}

	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		dataDir string
	}{
		{"restart from local data", dataDir},
		{"restore from object storage", t.TempDir()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := openMetaNode(t, store, tc.dataDir)
			if v, ok, err := n.MetaGet("lease/1"); err != nil || !ok || string(v) != "ttl=10" {
				t.Fatalf("MetaGet(lease/1) = %q, %v, %v", v, ok, err)
			}
			if v, ok, err := n.MetaGet("lease/2"); err != nil || !ok || string(v) != "ttl=20" {
				t.Fatalf("MetaGet(lease/2) (replayed from WAL) = %q, %v, %v", v, ok, err)
			}
			if ok, _ := n.MetaEnabled(); !ok {
				t.Fatal("MetaEnabled = false after recovery")
			}
			if kv, _ := n.Get("b"); kv == nil || string(kv.Value) != "2" {
				t.Fatalf("Get(b) = %+v", kv)
			}
			if err := n.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func manifestFormat(t *testing.T, store object.Store) uint32 {
	t.Helper()
	m, err := checkpoint.New(nil).ReadManifest(context.Background(), store)
	if err != nil || m == nil {
		t.Fatalf("ReadManifest = %+v, %v", m, err)
	}
	return m.FormatVersion
}

// assertMetaSegmentsAreFormat3 checks that every uploaded WAL segment holding
// a meta op declares format 3, and that at least one such segment exists.
func assertMetaSegmentsAreFormat3(t *testing.T, store object.Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		keys, err := store.List(context.Background(), "wal/")
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, key := range keys {
			rc, err := store.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), "lease/1") {
				continue
			}
			found = true
			if b[2] != 3 {
				t.Fatalf("segment %s holds a meta op but declares format %d", key, b[2])
			}
		}
		if found {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no uploaded WAL segment contains the meta op")
}

// TestMetaOnFollower covers forwarding meta writes to the leader and the
// sequence-based ReadIndex for linearizable meta reads on a follower.
func TestMetaOnFollower(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 2)
	for i := range nodes {
		n, err := Open(Config{
			DataDir:        t.TempDir(),
			ObjectStore:    shared,
			NodeID:         fmt.Sprintf("meta-node-%d", i),
			PeerListenAddr: freeAddrLocal(t),
		})
		if err != nil {
			t.Fatalf("open node-%d: %v", i, err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	follower := nodes[0]
	if follower == leader {
		follower = nodes[1]
	}
	ctx := metaCtx(t)

	if err := follower.MetaPut(ctx, "k", []byte("v")); !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("follower MetaPut before enable: want ErrMetaDisabled, got %v", err)
	}
	if err := follower.enableMeta(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower enableMeta: want ErrNotLeader, got %v", err)
	}
	enableMetaForTest(t, leader)

	rev := leader.CurrentRevision()
	if err := follower.MetaPut(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("follower MetaPut: %v", err)
	}
	if v, ok, err := follower.LinearizableMetaGet(ctx, "k"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("follower LinearizableMetaGet = %q, %v, %v", v, ok, err)
	}
	if v, ok, _ := leader.MetaGet("k"); !ok || string(v) != "v" {
		t.Fatalf("leader MetaGet = %q, %v", v, ok)
	}

	if _, err := follower.Txn(ctx, TxnRequest{Success: []TxnOp{{Type: TxnMetaPut, Key: "t", Value: []byte("x")}}}); err != nil {
		t.Fatalf("follower meta txn: %v", err)
	}
	if err := follower.MetaDelete(ctx, "k"); err != nil {
		t.Fatalf("follower MetaDelete: %v", err)
	}
	list, err := follower.LinearizableMetaList(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, kv := range list {
		keys = append(keys, kv.Key)
	}
	if strings.Join(keys, ",") != metaFormatKey+",t" {
		t.Fatalf("follower LinearizableMetaList keys = %q", keys)
	}
	if got := leader.CurrentRevision(); got != rev {
		t.Fatalf("leader revision moved from %d to %d on meta-only writes", rev, got)
	}
}
