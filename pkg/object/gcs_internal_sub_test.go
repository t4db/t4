package object

import (
	"strings"
	"testing"
)

// TestGCSStoreSubNestsBelowPrefix mirrors TestS3StoreSubNestsBelowPrefix: a
// sub-store's keys land below the parent's prefix, whatever slashes either
// one was written with.
func TestGCSStoreSubNestsBelowPrefix(t *testing.T) {
	for _, tc := range []struct{ prefix, sub, want string }{
		{"t4", "dbs/a", "t4/dbs/a/wal/1"},
		{"", "dbs/a", "dbs/a/wal/1"},
		{"/t4/", "/dbs/a/", "t4/dbs/a/wal/1"},
	} {
		parent := &GCSStore{bucket: "bucket", prefix: strings.Trim(tc.prefix, "/")}
		if got := parent.Sub(tc.sub).(*GCSStore).name("wal/1"); got != tc.want {
			t.Errorf("prefix %q sub %q: name = %q, want %q", tc.prefix, tc.sub, got, tc.want)
		}
	}
}

// TestGCSStoreSubSharesTheClient pins that dividing a store does not open a
// second connection pool: many databases on one bucket share one client.
func TestGCSStoreSubSharesTheClient(t *testing.T) {
	parent, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "bucket", Endpoint: "http://127.0.0.1:1", Prefix: "t4"})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	child := parent.Sub("dbs/a").(*GCSStore)
	if child.http != parent.http {
		t.Error("Sub built its own HTTP client; it should share the parent's")
	}
	if child.endpoint != parent.endpoint {
		t.Errorf("Sub endpoint = %q, want the parent's %q", child.endpoint, parent.endpoint)
	}
}
