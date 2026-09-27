package object

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// overwritingS3 is a minimal S3 endpoint holding one object that is
// overwritten right after the first request for it is served, the way the
// checkpoint manifest and the leader lock are rewritten continuously. A
// request conditioned on the old ETag then fails with 412, as S3 does.
type overwritingS3 struct {
	mu     sync.Mutex
	served bool
}

func (s *overwritingS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	etag, body := `"v1"`, "old-content"
	if s.served {
		etag, body = `"v2"`, "new-content"
	}
	s.served = true
	s.mu.Unlock()

	if !strings.HasSuffix(r.URL.Path, "/bucket/obj") {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>no such key</Message></Error>`)
		return
	}
	if m := r.Header.Get("If-Match"); m != "" && m != etag {
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>PreconditionFailed</Code><Message>At least one of the preconditions you specified did not hold.</Message></Error>`)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", "11")
	w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.WriteString(w, body)
}

func newTestS3Store(t *testing.T) *S3Store {
	t.Helper()
	srv := httptest.NewServer(&overwritingS3{})
	t.Cleanup(srv.Close)
	client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{
		Creds:        credentials.NewStaticV4("access", "secret", ""),
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewS3Store(client, "bucket", "")
}

// TestS3GetSurvivesConcurrentOverwrite: reading an object that is being
// overwritten must return one complete version, not fail. Get used to issue
// a HEAD and then a GET conditioned on the HEAD's ETag, which fails with 412
// whenever the object changes in between.
func TestS3GetSurvivesConcurrentOverwrite(t *testing.T) {
	for name, get := range map[string]func(*S3Store) (io.ReadCloser, error){
		"Get": func(s *S3Store) (io.ReadCloser, error) { return s.Get(context.Background(), "obj") },
		"GetETag": func(s *S3Store) (io.ReadCloser, error) {
			res, err := s.GetETag(context.Background(), "obj")
			if err != nil {
				return nil, err
			}
			if res.ETag == "" {
				t.Error("GetETag returned no ETag")
			}
			return res.Body, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			rc, err := get(newTestS3Store(t))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rc.Close() }()
			b, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if got := string(b); got != "old-content" && got != "new-content" {
				t.Fatalf("read %q, want one complete version", got)
			}
		})
	}
}

func TestS3GetMissingKey(t *testing.T) {
	s := newTestS3Store(t)
	if _, err := s.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing key: want ErrNotFound, got %v", err)
	}
	if _, err := s.GetETag(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetETag missing key: want ErrNotFound, got %v", err)
	}
}
