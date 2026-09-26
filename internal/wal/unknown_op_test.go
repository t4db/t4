package wal

import (
	"bytes"
	"errors"
	"testing"
)

// opFromFuture is an op code this binary does not understand, standing in for
// an op added by a newer release.
const opFromFuture Op = 99

func TestReadEntryRejectsUnknownOp(t *testing.T) {
	var buf bytes.Buffer
	if err := AppendEntry(&buf, &Entry{Revision: 1, Term: 1, Op: opFromFuture, Key: "k"}); err != nil {
		t.Fatalf("AppendEntry: %v", err)
	}
	if _, err := ReadEntry(&buf); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("ReadEntry: want ErrUnknownOp, got %v", err)
	}
}

func TestDecodeTxnOpsRejectsUnknownSubOp(t *testing.T) {
	for _, op := range []Op{OpCompact, OpTxn, opFromFuture} {
		b := EncodeTxnOps([]TxnSubOp{
			{Op: OpCreate, Key: "a", Value: []byte("1")},
			{Op: op, Key: "b"},
		})
		if _, err := DecodeTxnOps(b); !errors.Is(err, ErrUnknownOp) {
			t.Errorf("op=%d: DecodeTxnOps: want ErrUnknownOp, got %v", op, err)
		}
	}
}

func TestValidateEntry(t *testing.T) {
	goodTxn := EncodeTxnOps([]TxnSubOp{
		{Op: OpCreate, Key: "a", Value: []byte("1")},
		{Op: OpUpdate, Key: "b", Value: []byte("2")},
		{Op: OpDelete, Key: "c"},
	})
	badTxn := EncodeTxnOps([]TxnSubOp{{Op: opFromFuture, Key: "a"}})

	for _, tc := range []struct {
		name    string
		e       Entry
		wantErr error
	}{
		{"create", Entry{Op: OpCreate, Key: "k"}, nil},
		{"update", Entry{Op: OpUpdate, Key: "k"}, nil},
		{"delete", Entry{Op: OpDelete, Key: "k"}, nil},
		{"compact", Entry{Op: OpCompact}, nil},
		{"txn", Entry{Op: OpTxn, Value: goodTxn}, nil},
		{"zero op", Entry{Op: 0}, ErrUnknownOp},
		{"unknown op", Entry{Op: opFromFuture}, ErrUnknownOp},
		{"txn with unknown sub-op", Entry{Op: OpTxn, Value: badTxn}, ErrUnknownOp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEntry(&tc.e)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("want nil, got %v", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
		})
	}

	t.Run("truncated txn", func(t *testing.T) {
		e := Entry{Op: OpTxn, Value: goodTxn[:len(goodTxn)-1]}
		if err := ValidateEntry(&e); err == nil {
			t.Fatal("want error for truncated txn payload")
		}
	})
}

// TestReplayLocalFailsOnUnknownOp checks that replay refuses a segment holding
// an op it cannot apply, instead of treating it as a torn tail and silently
// dropping the rest of the segment.
func TestReplayLocalFailsOnUnknownOp(t *testing.T) {
	dir := t.TempDir()
	sw, err := OpenSegmentWriter(dir, 1, 1)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	for _, e := range []*Entry{
		{ID: 1, Revision: 1, Term: 1, Op: OpCreate, Key: "a", Value: []byte("1")},
		{ID: 2, Revision: 1, Term: 1, Op: opFromFuture, Key: "meta"},
		{ID: 3, Revision: 2, Term: 1, Op: OpCreate, Key: "b", Value: []byte("2")},
	} {
		if err := sw.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w, err := Open(dir, 2, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	recovered := &recordingRecoveryStore{}
	if err := w.ReplayLocal(recovered, 0); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("ReplayLocal: want ErrUnknownOp, got %v", err)
	}
	if len(recovered.entries) != 0 {
		t.Fatalf("ReplayLocal applied %d entries from a segment it cannot fully read", len(recovered.entries))
	}
}
