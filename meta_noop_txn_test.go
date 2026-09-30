package t4

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A transaction whose conditions read an in-flight write may be answered
// before that write, from committed state, when the branch committed state
// selects writes nothing. Meta ops and meta conditions must not be mistaken
// for that case: whatever the transaction answers must hold at the revision
// it reports.

func openMetaWithValue(t *testing.T, key, value string) (*Node, *fakeWAL) {
	t.Helper()
	n, fw := openWithValue(t, key, value)
	if on, err := n.MetaEnabled(); err != nil || !on {
		t.Fatalf("meta keyspace not enabled on a new database: %v, %v", on, err)
	}
	return n, fw
}

func metaExists(t *testing.T, n *Node, key string) bool {
	t.Helper()
	_, ok, err := n.db.Load().MetaGet(key)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// A meta put is a write. When committed state selects a branch holding one,
// the transaction cannot be answered as a no-op from committed state.
func TestNoopTxnMetaPutIsAWrite(t *testing.T) {
	n, fw := openMetaWithValue(t, "/k", "v1")
	committed := n.CurrentRevision()
	release, _, putErr := holdPut(t, n, fw, "/k", "v2")
	defer release()

	// In-flight state says v2: the empty success branch. Committed state says
	// v1: the failure branch, which writes meta key m.
	req := TxnRequest{
		Conditions: []TxnCondition{{Key: "/k", Target: TxnCondValue, Result: TxnCondEqual, Value: []byte("v2")}},
		Failure:    []TxnOp{{Type: TxnMetaPut, Key: "m", Value: []byte("x")}},
	}
	txnC := goTxn(n, req)
	var r txnResult
	select {
	case r = <-txnC:
	case <-time.After(200 * time.Millisecond):
		release()
		if err := <-putErr; err != nil {
			t.Fatal(err)
		}
		r = <-txnC
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !r.resp.Succeeded && !metaExists(t, n, "m") {
		t.Fatalf("txn = %+v: reported the failure branch at revision %d without its meta put", r.resp, r.resp.Revision)
	}
	if r.resp.Succeeded && r.resp.Revision == committed {
		t.Fatalf("txn = %+v: succeeded at committed revision %d, where /k is still v1", r.resp, committed)
	}
}

// A meta condition cannot be evaluated against the data key of the same name.
func TestNoopTxnMetaConditionAtCommittedState(t *testing.T) {
	n, fw := openMetaWithValue(t, "/k", "v1")
	if err := n.MetaPut(context.Background(), "m", []byte("x")); err != nil {
		t.Fatal(err)
	}
	committed := n.CurrentRevision()
	release, _, putErr := holdPut(t, n, fw, "/k", "v2")
	defer release()

	// Holds at the committed revision (m exists, /k is v1), fails once the
	// in-flight v2 commits. Neither branch writes.
	req := TxnRequest{Conditions: []TxnCondition{
		{Key: "m", Target: TxnCondMetaExists, Result: TxnCondEqual, Version: 1},
		{Key: "/k", Target: TxnCondValue, Result: TxnCondEqual, Value: []byte("v1")},
	}}
	txnC := goTxn(n, req)
	var r txnResult
	select {
	case r = <-txnC:
	case <-time.After(200 * time.Millisecond):
		release()
		if err := <-putErr; err != nil {
			t.Fatal(err)
		}
		r = <-txnC
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.resp.Revision == committed && !r.resp.Succeeded {
		t.Fatalf("txn = %+v: failed at revision %d, where m exists and /k is v1", r.resp, committed)
	}
	if r.resp.Revision > committed && r.resp.Succeeded {
		t.Fatalf("txn = %+v: succeeded at revision %d, where /k is v2", r.resp, r.resp.Revision)
	}
}

// holdMetaPut starts a MetaPut of key behind a blocked WAL and returns once it
// is in flight, like holdPut.
func holdMetaPut(t *testing.T, n *Node, fw *fakeWAL, key string) (func(), context.CancelFunc, <-chan error) {
	t.Helper()
	block := make(chan struct{})
	fw.setBlockChan(block)
	var once sync.Once
	release := func() { once.Do(func() { fw.setBlockChan(nil); close(block) }) }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errC := make(chan error, 1)
	go func() { errC <- n.MetaPut(ctx, key, []byte("x")) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		n.mu.Lock()
		_, inFlight := n.pendingMeta[key]
		n.mu.Unlock()
		if inFlight {
			return release, cancel, errC
		}
		if time.Now().After(deadline) {
			t.Fatal("MetaPut never went in flight")
		}
		time.Sleep(time.Millisecond)
	}
}

// A meta-exists condition that reads an in-flight meta write depends on that
// write's outcome, like a data condition reading a pending data write: the
// transaction must not be answered before it, and is evaluated again if the
// write is abandoned. Create-if-absent (sysstate.Create, a lease grant) has
// exactly this shape.
func TestNoopTxnWaitsForObservedMetaWrite(t *testing.T) {
	n, fw := openMetaWithValue(t, "/k", "v1")
	release, cancelPut, putErr := holdMetaPut(t, n, fw, "m")
	defer release()

	req := TxnRequest{
		Conditions: []TxnCondition{{Key: "m", Target: TxnCondMetaExists, Result: TxnCondEqual, Version: 0}},
		Success:    []TxnOp{{Type: TxnMetaPut, Key: "m", Value: []byte("mine")}},
	}
	txnC := goTxn(n, req)
	select {
	case r := <-txnC:
		t.Fatalf("txn answered from an uncommitted meta write: %+v, %v", r.resp, r.err)
	case <-time.After(200 * time.Millisecond):
	}

	// The in-flight write is abandoned, so m was never written: the txn
	// finds it absent after all and writes it.
	cancelPut()
	if err := <-putErr; err == nil {
		t.Fatal("abandoned MetaPut reported success")
	}
	release()
	r := <-txnC
	if r.err != nil || !r.resp.Succeeded {
		t.Fatalf("txn = %+v, %v; want succeeded: the observed meta write was never committed", r.resp, r.err)
	}
	if v, ok, err := n.db.Load().MetaGet("m"); err != nil || !ok || string(v) != "mine" {
		t.Fatalf("meta m = %q, %v, %v; want the txn's write", v, ok, err)
	}
}
