package object

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestS3TransportBoundsResponseWait checks that every S3 client gives up on a
// request S3 does not start answering within s3ResponseHeaderTimeout, with or
// without a CA bundle. A request on a connection that silently stopped
// carrying traffic would otherwise hold whatever waits on it, such as a
// leader flushing each write to S3, for a minute or, with a CA bundle, for
// good.
func TestS3TransportBoundsResponseWait(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	bundle := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		cfg  S3Config
	}{
		{"http", S3Config{Endpoint: "http://127.0.0.1:9000", Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s"}},
		{"https", S3Config{Endpoint: "https://127.0.0.1:9000", Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s"}},
		{"CA bundle", S3Config{Endpoint: "https://127.0.0.1:9000", Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s", CABundle: bundle}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := s3MinioOptions(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			tr, ok := opts.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport is %T, want *http.Transport", opts.Transport)
			}
			if tr.ResponseHeaderTimeout <= 0 || tr.ResponseHeaderTimeout > 15*time.Second {
				t.Fatalf("ResponseHeaderTimeout = %v, want at most 15s", tr.ResponseHeaderTimeout)
			}
			if tc.cfg.CABundle != "" && (tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs == nil) {
				t.Fatal("CA bundle not used")
			}
		})
	}
}

// TestS3RetriesRequestWithNoAnswer checks that a write S3 never answers is
// retried once s3ResponseHeaderTimeout passes, rather than waiting longer.
func TestS3RetriesRequestWithNoAnswer(t *testing.T) {
	defer func(d time.Duration) { s3ResponseHeaderTimeout = d }(s3ResponseHeaderTimeout)
	s3ResponseHeaderTimeout = 200 * time.Millisecond

	release := make(chan struct{})
	requests := make(chan struct{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- struct{}{}
		if len(requests) == 1 {
			<-release // the first request gets no answer
			return
		}
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	store, err := NewS3StoreFromConfig(t.Context(), S3Config{Endpoint: srv.URL, Bucket: "bucket", AccessKeyID: "k", SecretAccessKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := store.Put(t.Context(), "key", strings.NewReader("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Put took %v", took)
	}
}
