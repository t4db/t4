package t4

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A transaction's conditions see writes still being committed. When the
// selected branch writes nothing, the response must not report a revision
// older than a write the conditions depended on, nor a result that depended
// on a write that never committed.

// holdPut starts a Put of value to key behind a blocked WAL and returns once
// the write is in flight, with a func that unblocks the WAL, the Put
// context's cancel, and a channel carrying the Put's error.
func holdPut(t *testing.T, n *Node, fw *fakeWAL, key, value string) (func(), context.CancelFunc, <-chan error) {
	t.Helper()
	block := make(chan struct{})
	fw.setBlockChan(block)
	var once sync.Once
	release := func() { once.Do(func() { fw.setBlockChan(nil); close(block) }) }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	before := n.CurrentRevision()
	errC := make(chan error, 1)
	go func() {
		_, err := n.Put(ctx, key, []byte(value), 0)
		errC <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		n.mu.Lock()
		_, inFlight := n.pending[key]
		n.mu.Unlock()
		if inFlight {
			return release, cancel, errC
		}
		if time.Now().After(deadline) {
			t.Fatalf("Put never went in flight (revision %d)", before)
		}
		time.Sleep(time.Millisecond)
	}
}

// valueIsThenPut compares key's value and, when it matches, writes other.
func valueIsThenPut(key, value, other string) TxnRequest {
	return TxnRequest{
		Conditions: []TxnCondition{{Key: key, Target: TxnCondValue, Result: TxnCondEqual, Value: []byte(value)}},
		Success:    []TxnOp{{Type: TxnPut, Key: other, Value: []byte("x")}},
	}
}

type txnResult struct {
	resp TxnResponse
	err  error
}

func goTxn(n *Node, req TxnRequest) <-chan txnResult {
	c := make(chan txnResult, 1)
	go func() {
		resp, err := n.Txn(context.Background(), req)
		c <- txnResult{resp, err}
	}()
	return c
}

func openWithValue(t *testing.T, key, value string) (*Node, *fakeWAL) {
	t.Helper()
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if _, err := n.Put(context.Background(), key, []byte(value), 0); err != nil {
		t.Fatal(err)
	}
	return n, newFakeWAL(n)
}

// When the committed state alone also leads to a branch that writes
// nothing, the txn is ordered before the in-flight write and answered at
// once, from committed state.
func TestNoopTxnAnswersBeforeInFlightWrite(t *testing.T) {
	n, fw := openWithValue(t, "/k", "v1")
	release, _, putErr := holdPut(t, n, fw, "/k", "v2")
	defer release()

	// In-flight state says v2 (succeeded), committed state says v1 (failed);
	// neither branch writes.
	req := TxnRequest{Conditions: []TxnCondition{{Key: "/k", Target: TxnCondValue, Result: TxnCondEqual, Value: []byte("v2")}}}
	select {
	case r := <-goTxn(n, req):
		if r.err != nil || r.resp.Succeeded || r.resp.Revision != 1 {
			t.Fatalf("txn = %+v, %v; want failed at revision 1, ordered before the in-flight v2", r.resp, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("txn waited for an in-flight write it did not need")
	}
	release()
	if err := <-putErr; err != nil {
		t.Fatal(err)
	}
}

// When the committed state would lead to a write, the txn cannot be ordered
// before the in-flight write, so it waits and answers at that write's
// revision.
func TestNoopTxnWaitsForObservedWrite(t *testing.T) {
	n, fw := openWithValue(t, "/k", "v1")
	release, _, putErr := holdPut(t, n, fw, "/k", "v2")

	txnC := goTxn(n, valueIsThenPut("/k", "v1", "/other"))
	select {
	case r := <-txnC:
		t.Fatalf("txn answered before the write it compared against committed: %+v, %v", r.resp, r.err)
	case <-time.After(200 * time.Millisecond):
	}

	release()
	if err := <-putErr; err != nil {
		t.Fatalf("Put: %v", err)
	}
	r := <-txnC
	if r.err != nil || r.resp.Succeeded || r.resp.Revision != 2 {
		t.Fatalf("txn = %+v, %v; want failed at revision 2 (the write it compared against)", r.resp, r.err)
	}
	if kv, _ := n.Get("/other"); kv != nil {
		t.Fatalf("failed txn wrote /other: %+v", kv)
	}
}

// When the in-flight write the txn read is abandoned, the txn is evaluated
// again against what was actually written.
func TestNoopTxnReevaluatesAfterAbandonedWrite(t *testing.T) {
	n, fw := openWithValue(t, "/k", "v1")
	release, cancelPut, putErr := holdPut(t, n, fw, "/k", "v2")
	defer release()

	txnC := goTxn(n, valueIsThenPut("/k", "v1", "/other"))
	time.Sleep(50 * time.Millisecond)

	// The writer gives up before its WAL append completes, so the commit loop
	// abandons the batch and v2 is never written.
	cancelPut()
	if err := <-putErr; err == nil {
		t.Fatal("abandoned Put reported success")
	}
	release()
	r := <-txnC
	if r.err != nil || !r.resp.Succeeded {
		t.Fatalf("txn = %+v, %v; want succeeded: v2 was never written, so /k is still v1", r.resp, r.err)
	}
	if kv, err := n.Get("/other"); err != nil || kv == nil {
		t.Fatalf("Get /other = %+v, %v; want the txn's write", kv, err)
	}
}
