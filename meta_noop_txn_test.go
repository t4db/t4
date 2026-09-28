package t4

import (
	"context"
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
