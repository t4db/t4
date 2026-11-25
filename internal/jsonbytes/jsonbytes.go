// Package jsonbytes carries a string of arbitrary bytes through JSON without
// losing any of them.
//
// encoding/json replaces invalid UTF-8 in a Go string with U+FFFD, so a key
// that is not valid UTF-8 comes back as a different key. That is wrong on the
// peer wire, where it would commit the wrong key, and wrong on disk, where an
// RBAC range would start enforcing something other than what was granted.
//
// The encoding keeps the plain JSON string for anything already valid UTF-8,
// so existing data and existing readers are unaffected, and falls back to a
// {"base64": "..."} object only for bytes a string cannot hold. A reader that
// expects a plain string fails on that object rather than silently taking a
// mangled value.
package jsonbytes

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// String is a string whose bytes survive a JSON round trip.
// UTF-8 -> plain JSON string; other bytes -> {"base64":"..."}.
// The object shape makes a plain-string decoder reject those bytes rather
// than mutate them. WAL, checkpoint and public key types are unaffected.
type String string

func (k String) MarshalJSON() ([]byte, error) {
	if utf8.ValidString(string(k)) {
		return json.Marshal(string(k))
	}
	return json.Marshal(struct {
		Base64 string `json:"base64"`
	}{
		base64.StdEncoding.EncodeToString([]byte(k)),
	})
}

func (k *String) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return fmt.Errorf("jsonbytes: empty wire key")
	}
	if data[0] == '"' {
		// JSON strings must already be valid UTF-8. Legacy senders may have
		// replaced bytes before transmission; only the version gate can detect
		// that sender, never guessing from a legitimate replacement rune.
		if !utf8.Valid(data) {
			return fmt.Errorf("jsonbytes: invalid UTF-8 wire key string")
		}
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		*k = String(text)
		return nil
	}
	// Read a fixed object grammar instead of a map: duplicate members must
	// fail too, and nested input is never recursively decoded by this parser.
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("jsonbytes: wire key must be string or base64 object")
	}
	token, err = d.Token()
	if err != nil || token != "base64" {
		return fmt.Errorf("jsonbytes: wire key requires base64 member")
	}
	token, err = d.Token()
	text, ok := token.(string)
	if err != nil || !ok {
		return fmt.Errorf("jsonbytes: wire key base64 must be string")
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') {
		return fmt.Errorf("jsonbytes: wire key requires exactly one member")
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("jsonbytes: trailing wire key data")
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(text)
	// Strict rejects nonzero padding bits; re-encoding also rejects CR/LF,
	// which the standard decoder deliberately ignores even in strict mode.
	if err != nil || base64.StdEncoding.EncodeToString(raw) != text {
		return fmt.Errorf("jsonbytes: invalid wire key base64")
	}
	*k = String(raw)
	return nil
}
