package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"

	"github.com/t4db/t4/internal/testutil"
)

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	return testutil.FreeAddr(t)
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

// A cluster leader with no follower connected uploads each batch before
// acknowledging it even with WALSyncUpload=false: object storage is then the
// only other copy of a write, and a candidate may catch up from it only while
// the lock records that it holds every acknowledged write
// (docs/design/takeover-fence.md).
func TestLeaderWithoutFollowersUploadsDespiteOptOut(t *testing.T) {
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
	if got := len(walObjects(t, store)); got != before+1 {
		t.Fatalf("leader without followers did not upload synchronously (before=%d after=%d)", before, got)
	}
	rec, err := election.NewLock(store, "observer", "").Read(context.Background())
	if err != nil || rec == nil || !rec.ObjectStoreComplete {
		t.Fatalf("lock does not record object storage as complete: %+v, %v", rec, err)
	}
}

// walRejectingStore fails every WAL segment upload.
type walRejectingStore struct{ object.Store }

var errWALUploadRejected = errors.New("wal upload rejected")

func (s walRejectingStore) Put(ctx context.Context, key string, r io.Reader) error {
	if strings.HasPrefix(key, "wal/") {
		return errWALUploadRejected
	}
	return s.Store.Put(ctx, key, r)
}

// WALSyncUpload=false is the operator asserting local storage is durable. A
// single node has no failover, so it keeps uploading asynchronously: writes
// succeed while WAL uploads fail, which they could not if a write waited for
// its upload.
func TestSingleNodeRespectsSyncUploadOptOut(t *testing.T) {
	off := false
	n, err := Open(Config{
		DataDir:       t.TempDir(),
		ObjectStore:   walRejectingStore{object.NewMem()},
		WALSyncUpload: &off,
		SegmentMaxAge: time.Hour,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = n.Close()
	}()

	for i := range 3 {
		if _, err := n.Put(context.Background(), fmt.Sprintf("/optout/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("put with WAL uploads failing: %v", err)
		}
	}
}

// When a follower connects to a leader that was uploading every batch, the
// leader clears the lock's ObjectStoreComplete before it acknowledges a write
// it does not upload: otherwise a candidate could trust object storage while
// that write exists only on the leader and the follower.
func TestObjectStoreFlagClearedWhenFollowerConnects(t *testing.T) {
	store := object.NewMem()
	open := func(id string) *Node {
		addr := freeLocalAddr(t)
		n, err := Open(Config{
			DataDir:           t.TempDir(),
			ObjectStore:       store,
			NodeID:            id,
			PeerListenAddr:    addr,
			AdvertisePeerAddr: addr,
			SegmentMaxAge:     time.Hour,
		})
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}
	ctx := context.Background()
	lock := election.NewLock(store, "observer", "")
	flag := func() bool {
		t.Helper()
		rec, err := lock.Read(ctx)
		if err != nil || rec == nil {
			t.Fatalf("read lock: %+v, %v", rec, err)
		}
		return rec.ObjectStoreComplete
	}

	leader := open("flag-leader")
	waitLeader(t, leader)
	if _, err := leader.Put(ctx, "/alone", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if !flag() {
		t.Fatal("leader without followers did not record object storage as complete")
	}

	follower := open("flag-follower")
	deadline := time.Now().Add(10 * time.Second)
	for leader.peerSrv.ConnectedFollowers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("follower never connected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	before := len(walObjects(t, store))
	rev, err := leader.Put(ctx, "/replicated", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(walObjects(t, store)); got != before {
		t.Fatalf("write with a follower connected was uploaded synchronously (before=%d after=%d)", before, got)
	}
	if flag() {
		t.Fatal("write acknowledged without upload while the lock still claims object storage is complete")
	}
	if err := follower.WaitForRevision(ctx, rev); err != nil {
		t.Fatal(err)
	}
}

// A lock write setting ObjectStoreComplete that reports an error may still
// have landed. Until a write of false succeeds, the leader must treat the lock
// as claiming object storage is complete, so that it does not acknowledge a
// write it did not upload without first clearing the flag.
func TestObjectStoreFlagClaimedFromAttempt(t *testing.T) {
	n := &Node{}
	n.objectStoreComplete.Store(true)
	n.flagWriteStarting(true) // the write reports an error: no flagWritten

	n.objectStoreComplete.Store(false) // a follower is back
	if !n.flagStale() {
		t.Fatal("a write of the flag that may have landed was not counted: an unuploaded write would be acknowledged without clearing it")
	}
	n.flagWriteStarting(false) // this write fails too
	if !n.flagStale() {
		t.Fatal("a failed write of false cleared the flag")
	}
	n.flagWriteStarting(false)
	n.flagWritten(false)
	if n.flagStale() {
		t.Fatal("a successful write of false did not clear the flag")
	}
}
