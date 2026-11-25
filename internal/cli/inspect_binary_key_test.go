package cli

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	istore "github.com/t4db/t4/internal/store"
)

// A key that is not valid UTF-8 must come out of inspect's JSON exactly as it
// is in the store. encoding/json rewrites such a key inside a Go string, so
// the readable field alone cannot carry it.
func TestInspectJSONCarriesBinaryKeys(t *testing.T) {
	const binary = "/juicefs\xfdsetting"
	if utf8.ValidString(binary) {
		t.Fatal("precondition: the test key should not be valid UTF-8")
	}

	t.Run("key value", func(t *testing.T) {
		data, err := json.Marshal(newJSONKV(&istore.KeyValue{Key: binary, Value: []byte("v"), Revision: 7}))
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Key    string
			KeyHex string
		}
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.KeyHex != hex.EncodeToString([]byte(binary)) {
			t.Errorf("KeyHex = %q, want %x", got.KeyHex, binary)
		}
		raw, err := hex.DecodeString(got.KeyHex)
		if err != nil || string(raw) != binary {
			t.Errorf("KeyHex does not decode back to the key: %v", err)
		}
		// The readable field is where the bytes are lost, which is why the
		// hex one exists.
		if !strings.Contains(got.Key, "\xef\xbf\xbd") {
			t.Logf("Key survived JSON unchanged: %q", got.Key)
		}
	})

	t.Run("printable key has no hex field", func(t *testing.T) {
		data, err := json.Marshal(newJSONKV(&istore.KeyValue{Key: "/plain", Revision: 1}))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "KeyHex") {
			t.Errorf("a valid UTF-8 key should emit no KeyHex: %s", data)
		}
	})

	t.Run("history events", func(t *testing.T) {
		data, err := json.Marshal(newJSONEvents([]istore.Event{
			{Type: istore.EventPut, KV: &istore.KeyValue{Key: binary, Revision: 2}},
		}))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), hex.EncodeToString([]byte(binary))) {
			t.Errorf("event JSON lacks the exact key: %s", data)
		}
	})

	t.Run("change summary", func(t *testing.T) {
		ch := inspectChange{Key: binary, KeyHex: exactKeyHex(binary)}
		data, err := json.Marshal(ch)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), hex.EncodeToString([]byte(binary))) {
			t.Errorf("change JSON lacks the exact key: %s", data)
		}
	})
}
