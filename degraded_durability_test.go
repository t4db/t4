package t4

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func waitLeader(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("node never became leader")
}

func walObjects(t *testing.T, store object.Store) []string {
	t.Helper()
	keys, err := store.List(context.Background(), "wal/")
	if err != nil {
		t.Fatalf("list wal/: %v", err)
	}
	return keys
}

// A multi-node leader with no connected followers cannot make a write durable
// by replication, so the write must reach object storage before it is
// acknowledged. Without this the only copy is a local disk that a multi-node
// deployment never promised would outlive the process.
func TestDegradedLeaderFlushesBeforeAck(t *testing.T) {
	store := object.NewMem()
	addr := freeLocalAddr(t)
	n, err := Open(Config{
		DataDir:           t.TempDir(),
		ObjectStore:       store,
		NodeID:            "solo-leader",
		PeerListenAddr:    addr,
		AdvertisePeerAddr: addr,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = n.Close()
	}()
	waitLeader(t, n)

	before := len(walObjects(t, store))
	if _, err := n.Put(context.Background(), "/degraded/key", []byte("v"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Asserted immediately after Put returns: the upload must be part of the
	// write, not a background task that happens to catch up.
	if got := len(walObjects(t, store)); got <= before {
		t.Fatalf("no WAL segment in object storage after acknowledged write (before=%d after=%d)", before, got)
	}
}

// The healthy path must not pay an S3 round trip per batch: with the ACK target
// satisfiable, uploads stay asynchronous on SegmentMaxAge.
func TestReplicatedLeaderDoesNotFlushPerBatch(t *testing.T) {
	store := object.NewMem()
	laddr := freeLocalAddr(t)
	leader, err := Open(Config{
		DataDir:           t.TempDir(),
		ObjectStore:       store,
		NodeID:            "leader",
		PeerListenAddr:    laddr,
		AdvertisePeerAddr: laddr,
		SegmentMaxAge:     time.Hour, // no age-based rotation during the test
	})
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer func() {
		_ = leader.Close()
	}()
	waitLeader(t, leader)

	faddr := freeLocalAddr(t)
	follower, err := Open(Config{
		DataDir:           t.TempDir(),
		ObjectStore:       store,
		NodeID:            "follower",
		PeerListenAddr:    faddr,
		AdvertisePeerAddr: faddr,
		SegmentMaxAge:     time.Hour,
	})
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer func() {
		_ = follower.Close()
	}()

	// Wait for the follower's stream to register with the leader.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !leader.peerSrv.ReplicationSatisfiable("quorum") {
		time.Sleep(20 * time.Millisecond)
	}
	if !leader.peerSrv.ReplicationSatisfiable("quorum") {
		t.Fatal("follower never connected")
	}

	before := len(walObjects(t, store))
	for i := 0; i < 20; i++ {
		if _, err := leader.Put(context.Background(), "/replicated/key", []byte("v"), 0); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if got := len(walObjects(t, store)); got != before {
		t.Fatalf("replicated writes triggered %d synchronous upload(s); uploads should stay async", got-before)
	}
}

// WALSyncUpload=false is the operator asserting local storage is durable, so
// degradation must not start blocking writes on S3.
func TestDegradedLeaderRespectsSyncUploadOptOut(t *testing.T) {
	store := object.NewMem()
	addr := freeLocalAddr(t)
	off := false
	n, err := Open(Config{
		DataDir:           t.TempDir(),
		ObjectStore:       store,
		NodeID:            "solo-optout",
		PeerListenAddr:    addr,
		AdvertisePeerAddr: addr,
		WALSyncUpload:     &off,
		SegmentMaxAge:     time.Hour,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = n.Close()
	}()
	waitLeader(t, n)

	before := len(walObjects(t, store))
	if _, err := n.Put(context.Background(), "/optout/key", []byte("v"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := len(walObjects(t, store)); got != before {
		t.Fatalf("opt-out still uploaded synchronously (before=%d after=%d)", before, got)
	}
}
