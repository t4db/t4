package t4_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/testutil"
	"github.com/t4db/t4/pkg/object"
)

// captureLogger records what Open logged, so a warning can be asserted on
// rather than assumed.
type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *captureLogger) add(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *captureLogger) Debugf(f string, a ...interface{}) { l.add(f, a...) }
func (l *captureLogger) Infof(f string, a ...interface{})  { l.add(f, a...) }
func (l *captureLogger) Warnf(f string, a ...interface{})  { l.add(f, a...) }
func (l *captureLogger) Errorf(f string, a ...interface{}) { l.add(f, a...) }

func (l *captureLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

// A cluster cannot elect a leader safely on a store that ignores the
// preconditions the lock is written with: two nodes that read the same lock
// would both be told their write won. Open must refuse rather than serve.
func TestClusterRefusesAStoreThatIgnoresConditionalWrites(t *testing.T) {
	addr := testutil.FreeAddr(t)
	n, err := t4.Open(t4.Config{
		NodeID:            "n1",
		DataDir:           filepath.Join(t.TempDir(), "db"),
		ObjectStore:       testutil.NewLyingStore(object.NewMem()),
		PeerListenAddr:    addr,
		AdvertisePeerAddr: addr,
	})
	if err == nil {
		_ = n.Close()
		t.Fatal("a cluster node started on a store that does not enforce conditional writes")
	}
	if !errors.Is(err, t4.ErrConditionalWritesIgnored) {
		t.Fatalf("err=%v, want ErrConditionalWritesIgnored", err)
	}
	// The message has to tell an operator which precondition was not kept;
	// "the object store is unsuitable" would send them nowhere.
	if !strings.Contains(err.Error(), "If-Match") && !strings.Contains(err.Error(), "If-None-Match") {
		t.Errorf("error should name the precondition that failed, got %q", err.Error())
	}
}

// A single node is the only writer of its own lock and manifest, so it starts —
// but what it loses must be said out loud.
func TestSingleNodeWarnsOnAStoreThatIgnoresConditionalWrites(t *testing.T) {
	log := &captureLogger{}
	n, err := t4.Open(t4.Config{
		NodeID:             "solo",
		DataDir:            filepath.Join(t.TempDir(), "db"),
		ObjectStore:        testutil.NewLyingStore(object.NewMem()),
		Logger:             log,
		CheckpointInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("a single node should start on such a store: %v", err)
	}
	defer func() { _ = n.Close() }()

	if _, err := n.Put(context.Background(), "/k", []byte("v"), 0); err != nil {
		t.Fatalf("the node should be usable: %v", err)
	}
	if !log.contains("does not enforce conditional writes") {
		t.Fatalf("no warning was logged; lines: %v", log.lines)
	}
}

// A store that enforces both preconditions must be unaffected: no refusal, and
// no warning that would train operators to ignore them.
func TestClusterStartsOnAStoreThatEnforcesConditionalWrites(t *testing.T) {
	log := &captureLogger{}
	addr := testutil.FreeAddr(t)
	n, err := t4.Open(t4.Config{
		NodeID:            "n1",
		DataDir:           filepath.Join(t.TempDir(), "db"),
		ObjectStore:       object.NewMem(),
		PeerListenAddr:    addr,
		AdvertisePeerAddr: addr,
		Logger:            log,
	})
	if err != nil {
		t.Fatalf("Open on a conforming store: %v", err)
	}
	defer func() { _ = n.Close() }()
	if !n.IsLeader() {
		t.Fatal("node did not become leader")
	}
	if log.contains("conditional writes") {
		t.Errorf("a conforming store should produce no conditional-write log line; lines: %v", log.lines)
	}
}

// probeBlindStore works for everything except the probe's own scratch object,
// as a bucket policy that denies writes under one prefix would. The probe then
// reaches no conclusion while the store is otherwise healthy.
type probeBlindStore struct {
	*object.Mem
}

var errProbeDenied = errors.New("access denied")

func (s *probeBlindStore) Delete(ctx context.Context, key string) error {
	if strings.Contains(key, "conditional-probe") {
		return errProbeDenied
	}
	return s.Mem.Delete(ctx, key)
}

// A probe that cannot run says nothing about the store. Refusing a cluster on
// that basis would make an unrelated permissions quirk an outage, so the node
// starts and says what it could not determine.
func TestInconclusiveProbeDoesNotRefuseACluster(t *testing.T) {
	log := &captureLogger{}
	addr := testutil.FreeAddr(t)
	n, err := t4.Open(t4.Config{
		NodeID:            "n1",
		DataDir:           filepath.Join(t.TempDir(), "db"),
		ObjectStore:       &probeBlindStore{Mem: object.NewMem()},
		PeerListenAddr:    addr,
		AdvertisePeerAddr: addr,
		Logger:            log,
	})
	if err != nil {
		t.Fatalf("an inconclusive probe must not refuse startup: %v", err)
	}
	defer func() { _ = n.Close() }()
	if !n.IsLeader() {
		t.Fatal("node did not become leader")
	}
	if !log.contains("could not check whether the object store enforces conditional writes") {
		t.Fatalf("the node should say what it could not determine; lines: %v", log.lines)
	}
}

// An unreachable store leaves the question unanswered too, and a single node
// with local data must still start from it.
func TestInconclusiveProbeDoesNotRefuseStartup(t *testing.T) {
	store := &outageStore{Mem: object.NewMem()}
	dir := filepath.Join(t.TempDir(), "data")
	async := false
	cfg := t4.Config{
		DataDir:            dir,
		ObjectStore:        store,
		WALSyncUpload:      &async,
		CheckpointInterval: time.Hour,
	}

	n, err := t4.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Put(context.Background(), "/before", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	store.down.Store(true)
	log := &captureLogger{}
	cfg.Logger = log
	n, err = t4.Open(cfg)
	if err != nil {
		t.Fatalf("an unreachable store must not be reported as unsafe: %v", err)
	}
	defer func() { _ = n.Close() }()
}

// The store #256 was written for rejects If-Match and keeps If-None-Match. A
// single node runs on it, as that commit arranged, and must keep doing so.
func TestSingleNodeStillStartsOnAStoreWithoutIfMatch(t *testing.T) {
	store := &testutil.RejectingStore{
		ConditionalStore:  object.NewMem(),
		Err:               errors.New("501 NotImplemented"),
		RejectIfMatchOnly: true,
	}
	log := &captureLogger{}
	n, err := t4.Open(t4.Config{
		NodeID:             "solo",
		DataDir:            filepath.Join(t.TempDir(), "db"),
		ObjectStore:        store,
		Logger:             log,
		CheckpointInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("a single node must still run on a store without If-Match: %v", err)
	}
	defer func() { _ = n.Close() }()
	if _, err := n.Put(context.Background(), "/k", []byte("v"), 0); err != nil {
		t.Fatalf("write on a store without If-Match: %v", err)
	}
	// The warning should say the store rejects If-Match, not that it ignores
	// it: the remedy differs, and so does how worried an operator should be.
	if !log.contains("If-Match is rejected rather than honored") {
		t.Errorf("warning should say If-Match is rejected; lines: %v", log.lines)
	}
}
