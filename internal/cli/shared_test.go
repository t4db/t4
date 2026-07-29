package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestDecodeKeyMaterial(t *testing.T) {
	key := bytes.Repeat([]byte{0x7a}, 32)
	cases := map[string][]byte{
		"raw":             key,
		"hex":             []byte(hex.EncodeToString(key)),
		"base64":          []byte(base64.StdEncoding.EncodeToString(key)),
		"hex_newline":     []byte(hex.EncodeToString(key) + "\n"),
		"base64_newline":  []byte(base64.StdEncoding.EncodeToString(key) + "\n"),
		"raw_with_spaces": append(append([]byte(nil), 0x20), append(bytes.Repeat([]byte{0x7a}, 30), 0x09)...),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := decodeKeyMaterial(raw)
			if err != nil {
				t.Fatalf("decodeKeyMaterial: %v", err)
			}
			want := key
			if name == "raw_with_spaces" {
				want = raw
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("decoded key mismatch")
			}
		})
	}
}

func TestDecodeKeyMaterialRejectsBadLength(t *testing.T) {
	if _, err := decodeKeyMaterial([]byte("short")); err == nil {
		t.Fatal("expected error")
	}
}
