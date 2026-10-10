package wal

import (
	"os"
	"testing"
)

func TestConsumesRevisionAndRequiredFormat(t *testing.T) {
	dataTxn := EncodeTxnOps([]TxnSubOp{{Op: OpCreate, Key: "a"}})
	metaTxn := EncodeTxnOps([]TxnSubOp{{Op: OpMetaPut, Key: "m"}, {Op: OpMetaDelete, Key: "n"}})
	mixedTxn := EncodeTxnOps([]TxnSubOp{{Op: OpCreate, Key: "a"}, {Op: OpMetaPut, Key: "m"}})

	for _, tc := range []struct {
		name     string
		e        Entry
		consumes bool
		format   int
	}{
		{"create", Entry{Op: OpCreate}, true, formatBase},
		{"update", Entry{Op: OpUpdate}, true, formatBase},
		{"delete", Entry{Op: OpDelete}, true, formatBase},
		{"compact", Entry{Op: OpCompact}, false, formatBase},
		{"meta put", Entry{Op: OpMetaPut}, false, formatMeta},
		{"meta delete", Entry{Op: OpMetaDelete}, false, formatMeta},
		{"data txn", Entry{Op: OpTxn, Value: dataTxn}, true, formatBase},
		{"meta-only txn", Entry{Op: OpTxn, Value: metaTxn}, false, formatMeta},
		{"mixed txn", Entry{Op: OpTxn, Value: mixedTxn}, true, formatMeta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.e.ConsumesRevision(); got != tc.consumes {
				t.Errorf("ConsumesRevision = %v, want %v", got, tc.consumes)
			}
			if got := RequiredFormat(&tc.e); got != tc.format {
				t.Errorf("RequiredFormat = %d, want %d", got, tc.format)
			}
			if err := ValidateEntry(&tc.e); err != nil {
				t.Errorf("ValidateEntry: %v", err)
			}
		})
	}
}

func TestTxnOpsRoundtripWithMetaSubOps(t *testing.T) {
	want := []TxnSubOp{
		{Op: OpCreate, Key: "a", Value: []byte("1"), CreateRevision: 4, Version: 1},
		{Op: OpMetaPut, Key: "a", Value: []byte("meta")},
		{Op: OpMetaDelete, Key: "b"},
	}
	got, err := DecodeTxnOps(EncodeTxnOps(want))
	if err != nil {
		t.Fatalf("DecodeTxnOps: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("op %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func segmentFormat(t *testing.T, path string) byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b[2]
}

// TestSegmentRaisesFormatForMetaOps checks that a segment stays at the base
// format until it receives an entry needing format 3, is raised in place, and
// still reads back every entry.
func TestSegmentRaisesFormatForMetaOps(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta Entry
	}{
		{"meta op", Entry{ID: 2, Revision: 1, Term: 1, Op: OpMetaPut, Key: "m", Value: []byte("v")}},
		{"txn with meta sub-op", Entry{ID: 2, Revision: 1, Term: 1, Op: OpTxn,
			Value: EncodeTxnOps([]TxnSubOp{{Op: OpMetaDelete, Key: "m"}})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sw, err := OpenSegmentWriter(t.TempDir(), 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := sw.Append(&Entry{ID: 1, Revision: 1, Term: 1, Op: OpCreate, Key: "a", Value: []byte("1")}); err != nil {
				t.Fatal(err)
			}
			if got := segmentFormat(t, sw.Path()); got != formatBase {
				t.Fatalf("format after data entry = %d, want %d", got, formatBase)
			}
			if err := sw.AppendNoSync(&tc.meta); err != nil {
				t.Fatal(err)
			}
			if err := sw.Append(&Entry{ID: 3, Revision: 2, Term: 1, Op: OpCreate, Key: "b", Value: []byte("2")}); err != nil {
				t.Fatal(err)
			}
			if err := sw.Seal(); err != nil {
				t.Fatal(err)
			}
			if got := segmentFormat(t, sw.Path()); got != formatMeta {
				t.Fatalf("format after meta entry = %d, want %d", got, formatMeta)
			}

			sr, closer, err := OpenSegmentFile(sw.Path())
			if err != nil {
				t.Fatal(err)
			}
			defer closer()
			entries, err := sr.ReadAll()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if len(entries) != 3 || entries[1].Op != tc.meta.Op || entries[2].Key != "b" {
				t.Fatalf("read back %d entries: %+v", len(entries), entries)
			}
		})
	}
}
