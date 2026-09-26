package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/internal/testhook"
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

// openLegacyNode opens a node that creates databases the way releases before
// the meta keyspace did, so the database keeps that format for good.
func openLegacyNode(t *testing.T, store object.Store, dataDir string) *Node {
	t.Helper()
	testhook.LegacyNewDatabases.Store(true)
	defer testhook.LegacyNewDatabases.Store(false)
	return openMetaNode(t, store, dataDir)
}

// TestMetaFormatFixedAtCreation checks that a new database gets the meta
// keyspace as its first entry, while a database created in the legacy format
// keeps it: no meta writes, and nothing an older release could not read.
func TestMetaFormatFixedAtCreation(t *testing.T) {
	ctx := metaCtx(t)

	n := openMetaNode(t, object.NewMem(), t.TempDir())
	if ok, err := n.MetaEnabled(); err != nil || !ok {
		t.Fatalf("new database: MetaEnabled = %v, %v; want true", ok, err)
	}
	if rev, seq := n.CurrentRevision(), n.db.Load().LastSequence(); rev != 0 || seq != 1 {
		t.Fatalf("new database: rev=%d seq=%d, want the format marker as the only entry (rev=0 seq=1)", rev, seq)
	}
	if err := n.MetaPut(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("new database: MetaPut: %v", err)
	}

	store, dataDir := object.NewMem(), t.TempDir()
	legacy := openLegacyNode(t, store, dataDir)
	if ok, err := legacy.MetaEnabled(); err != nil || ok {
		t.Fatalf("legacy database: MetaEnabled = %v, %v; want false", ok, err)
	}
	if err := legacy.MetaPut(ctx, "k", []byte("v")); !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("legacy MetaPut: want ErrMetaDisabled, got %v", err)
	}
	if err := legacy.MetaDelete(ctx, "k"); !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("legacy MetaDelete: want ErrMetaDisabled, got %v", err)
	}
	_, err := legacy.Txn(ctx, TxnRequest{Success: []TxnOp{
		{Type: TxnPut, Key: "data", Value: []byte("v")},
		{Type: TxnMetaPut, Key: "k", Value: []byte("v")},
	}})
	if !errors.Is(err, ErrMetaDisabled) {
		t.Fatalf("legacy Txn with meta op: want ErrMetaDisabled, got %v", err)
	}
	if kv, _ := legacy.Get("data"); kv != nil {
		t.Fatal("rejected txn partially applied")
	}
	if _, err := legacy.Put(ctx, "data", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	legacy.maybeCheckpoint(ctx)
	if got := manifestFormat(t, store); got != checkpoint.FormatVersionBase {
		t.Fatalf("legacy checkpoint format = %d, want %d", got, checkpoint.FormatVersionBase)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	assertNoSegmentAboveFormat(t, store, 2)

	// Reopened by this release, an existing database keeps its format.
	for _, dir := range []string{dataDir, t.TempDir()} {
		reopened := openMetaNode(t, store, dir)
		if ok, err := reopened.MetaEnabled(); err != nil || ok {
			t.Fatalf("reopened legacy database: MetaEnabled = %v, %v; want false", ok, err)
		}
		if kv, _ := reopened.Get("data"); kv == nil {
			t.Fatal("reopened legacy database lost data")
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// assertNoSegmentAboveFormat checks that every uploaded WAL segment declares
// at most format max, i.e. an older release can still read them.
func assertNoSegmentAboveFormat(t *testing.T, store object.Store, max byte) {
	t.Helper()
	keys, err := store.List(context.Background(), "wal/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("no uploaded WAL segments")
	}
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
		if b[2] > max {
			t.Fatalf("segment %s declares format %d, want <= %d", key, b[2], max)
		}
	}
}

func TestMetaKeyValidation(t *testing.T) {
	n := openMetaNode(t, object.NewMem(), t.TempDir())
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

func TestTxnMetaExistsCondition(t *testing.T) {
	ctx := metaCtx(t)
	legacy := openLegacyNode(t, object.NewMem(), t.TempDir())
	if resp, err := legacy.Txn(ctx, TxnRequest{Conditions: []TxnCondition{MetaDisabled()}}); err != nil || !resp.Succeeded {
		t.Fatalf("MetaDisabled on a legacy database: %+v, %v", resp, err)
	}
	n := openMetaNode(t, object.NewMem(), t.TempDir())
	if resp, err := n.Txn(ctx, TxnRequest{Conditions: []TxnCondition{MetaDisabled()}}); err != nil || resp.Succeeded {
		t.Fatalf("MetaDisabled on a new database: %+v, %v", resp, err)
	}

	absent := TxnCondition{Key: "k", Target: TxnCondMetaExists, Result: TxnCondEqual, Version: 0}
	create := TxnRequest{Conditions: []TxnCondition{absent}, Success: []TxnOp{{Type: TxnMetaPut, Key: "k", Value: []byte("v")}}}
	if resp, err := n.Txn(ctx, create); err != nil || !resp.Succeeded {
		t.Fatalf("create absent: %+v, %v", resp, err)
	}
	if resp, err := n.Txn(ctx, create); err != nil || resp.Succeeded {
		t.Fatalf("create existing: %+v, %v", resp, err)
	}
	// A data key of the same name does not count as a meta key.
	if _, err := n.Put(ctx, "d", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	absent.Key = "d"
	if resp, err := n.Txn(ctx, TxnRequest{Conditions: []TxnCondition{absent}}); err != nil || !resp.Succeeded {
		t.Fatalf("meta condition on a data key: %+v, %v", resp, err)
	}
}

// TestMetaCreateIfAbsentIsAtomic races create-if-absent transactions on one
// key: in-flight meta writes must be visible to the condition, so exactly one
// wins even though none has been applied when the others are evaluated.
func TestMetaCreateIfAbsentIsAtomic(t *testing.T) {
	n := openMetaNode(t, object.NewMem(), t.TempDir())
	ctx := metaCtx(t)

	for round := 0; round < 20; round++ {
		key := fmt.Sprintf("lease/%d", round)
		req := TxnRequest{
			Conditions: []TxnCondition{{Key: key, Target: TxnCondMetaExists, Result: TxnCondEqual, Version: 0}},
			Success:    []TxnOp{{Type: TxnMetaPut, Key: key, Value: []byte("v")}},
		}
		const racers = 8
		wins := make(chan bool, racers)
		start := make(chan struct{})
		for i := 0; i < racers; i++ {
			go func() {
				<-start
				resp, err := n.Txn(ctx, req)
				if err != nil {
					t.Error(err)
				}
				wins <- resp.Succeeded
			}()
		}
		close(start)
		won := 0
		for i := 0; i < racers; i++ {
			if <-wins {
				won++
			}
		}
		if won != 1 {
			t.Fatalf("round %d: %d create-if-absent txns succeeded, want 1", round, won)
		}
	}
}

// followAsOldBinary opens a WAL stream the way a release without the
// wal_format field does (WALFormat 0). The stream is cancelled after a second,
// so a Recv on an accepted stream cannot hang the test.
func followAsOldBinary(t *testing.T, addr, nodeID string, fromSeq int64) peer.WalStream_FollowClient {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(peer.Codec{})))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(func() { cancel(); _ = conn.Close() })
	stream, err := peer.NewWalStreamClient(conn).Follow(ctx, &peer.FollowRequest{FromRevision: fromSeq, NodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

// TestOldFollowersRefusedByNewDatabases checks that a leader of a database
// with the meta keyspace never streams to a follower that cannot read it,
// including after a restart, while a legacy database still serves them.
func TestOldFollowersRefusedByNewDatabases(t *testing.T) {
	open := func(t *testing.T, legacy bool, cfg Config) *Node {
		t.Helper()
		testhook.LegacyNewDatabases.Store(legacy)
		n, err := Open(cfg)
		testhook.LegacyNewDatabases.Store(false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		waitForLeaderNodeLocal(t, []*Node{n}, 10*time.Second)
		return n
	}
	newCfg := func(t *testing.T) Config {
		return Config{
			DataDir:        t.TempDir(),
			ObjectStore:    object.NewMem(),
			NodeID:         "leader",
			PeerListenAddr: freeAddrLocal(t),
		}
	}
	recv := func(n *Node, addr string) error {
		stream := followAsOldBinary(t, addr, "old-follower", n.db.Load().LastSequence()+1)
		_, err := stream.Recv()
		return err
	}
	refused := func(err error) bool {
		return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), "wal_format_unsupported")
	}

	t.Run("new database", func(t *testing.T) {
		cfg := newCfg(t)
		n := open(t, false, cfg)
		if err := recv(n, cfg.PeerListenAddr); !refused(err) {
			t.Fatalf("old follower: want wal_format_unsupported, got %v", err)
		}
		if err := n.Close(); err != nil {
			t.Fatal(err)
		}
		n = open(t, false, cfg)
		if err := recv(n, cfg.PeerListenAddr); !refused(err) {
			t.Fatalf("old follower after restart: want wal_format_unsupported, got %v", err)
		}
	})
	t.Run("legacy database", func(t *testing.T) {
		cfg := newCfg(t)
		n := open(t, true, cfg)
		// Accepted: the stream stays open until its deadline.
		if err := recv(n, cfg.PeerListenAddr); refused(err) || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("old follower of a legacy database: want an open stream, got %v", err)
		}
	})
}
