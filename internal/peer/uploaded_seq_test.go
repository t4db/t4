package peer

import "testing"

// UploadedSeq rides after the value, where older peers stop reading, and
// only when set: entries keep their size, and either side can be older.
func TestUploadedSeqWire(t *testing.T) {
	hb := &WalEntryMsg{Heartbeat: true, Term: 7, UploadedSeq: 12345}
	b, err := marshalWalEntryMsg(hb)
	if err != nil {
		t.Fatal(err)
	}
	var got WalEntryMsg
	if err := unmarshalWalEntryMsg(b, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Heartbeat || got.Term != 7 || got.UploadedSeq != 12345 {
		t.Errorf("heartbeat round trip: %+v", got)
	}
	// What an older peer sends: no trailing field.
	var old WalEntryMsg
	if err := unmarshalWalEntryMsg(b[:walEntryMsgFixedSize], &old); err != nil || old.UploadedSeq != 0 || !old.Heartbeat {
		t.Errorf("message without the field: %+v %v", old, err)
	}
	e := &WalEntryMsg{Revision: 3, Key: "k", Value: []byte("v")}
	if b, _ := marshalWalEntryMsg(e); len(b) != walEntryMsgFixedSize+2 {
		t.Errorf("entry without UploadedSeq is %d bytes, want %d", len(b), walEntryMsgFixedSize+2)
	}
}
