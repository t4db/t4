package object_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/t4db/t4/pkg/object"
)

// These tests put a recording server in place of GCS, so the request T4 sends
// can be asserted exactly. They cover what an emulator cannot: that the right
// query parameters go out, and that each status code maps to the error callers
// match on. They need no emulator and run in the normal suite.

type recordedRequest struct {
	Method string
	// Path is the decoded path. RawURI is the request line as it arrived, which
	// is where percent-encoding inside the object name can be checked: the
	// server decodes %2F back to "/" in URL.Path, so asserting on Path would
	// pass even if the escaping were missing.
	Path   string
	RawURI string
	Query  map[string][]string
	Header http.Header
	Body   string
}

type fakeGCS struct {
	mu       sync.Mutex
	requests []recordedRequest
	// handler answers each request; when nil, an empty object JSON is returned.
	handler func(w http.ResponseWriter, r *http.Request)
	server  *httptest.Server
}

func newFakeGCS(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *fakeGCS {
	t.Helper()
	f := &fakeGCS{handler: handler}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, recordedRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			RawURI: r.RequestURI,
			Query:  r.URL.Query(),
			Header: r.Header.Clone(),
			Body:   string(body),
		})
		f.mu.Unlock()
		if f.handler != nil {
			f.handler(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"o","generation":"123"}`))
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeGCS) store(t *testing.T, prefix string) *object.GCSStore {
	t.Helper()
	s, err := object.NewGCSStoreFromConfig(context.Background(), object.GCSConfig{
		Bucket:   "bkt",
		Prefix:   prefix,
		Endpoint: f.server.URL,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s
}

func (f *fakeGCS) last(t *testing.T) recordedRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		t.Fatal("no request was sent")
	}
	return f.requests[len(f.requests)-1]
}

// A versioned read must name the generation it wants, or it silently returns
// the live object and a point-in-time restore quietly restores the wrong data.
func TestGCSGetVersionedNamesTheGeneration(t *testing.T) {
	f := newFakeGCS(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Goog-Generation", "17")
		_, _ = w.Write([]byte("payload"))
	})
	store := f.store(t, "t4")

	rc, err := store.GetVersioned(context.Background(), "checkpoint/x", "17")
	if err != nil {
		t.Fatalf("get versioned: %v", err)
	}
	_ = rc.Close()

	got := f.last(t)
	if g := got.Query["generation"]; len(g) != 1 || g[0] != "17" {
		t.Errorf("generation=%v, want [17]", g)
	}
	if a := got.Query["alt"]; len(a) != 1 || a[0] != "media" {
		t.Errorf("alt=%v, want [media]", a)
	}
	if !strings.Contains(got.RawURI, "t4%2Fcheckpoint%2Fx") {
		t.Errorf("request %q should carry the prefixed, escaped object name", got.RawURI)
	}
}

// The object name is one path segment, so a "/" inside it has to be escaped or
// the request addresses something else entirely.
func TestGCSEscapesTheObjectNameAsOneSegment(t *testing.T) {
	f := newFakeGCS(t, nil)
	store := f.store(t, "")

	if _, err := store.Get(context.Background(), "wal/0001/seg"); err != nil {
		t.Fatalf("get: %v", err)
	}
	got := f.last(t)
	if strings.Contains(got.RawURI, "/o/wal/0001") {
		t.Errorf("request %q leaves separators unescaped inside the object name", got.RawURI)
	}
	if !strings.Contains(got.RawURI, "wal%2F0001%2Fseg") {
		t.Errorf("request %q should escape every separator in the name", got.RawURI)
	}
}

func TestGCSConditionalWritesSendTheRightPrecondition(t *testing.T) {
	t.Run("PutIfAbsent uses generation 0", func(t *testing.T) {
		f := newFakeGCS(t, nil)
		store := f.store(t, "t4")
		if err := store.PutIfAbsent(context.Background(), "lock", strings.NewReader("v")); err != nil {
			t.Fatalf("put-if-absent: %v", err)
		}
		got := f.last(t)
		if g := got.Query["ifGenerationMatch"]; len(g) != 1 || g[0] != "0" {
			t.Errorf("ifGenerationMatch=%v, want [0]", g)
		}
		if u := got.Query["uploadType"]; len(u) != 1 || u[0] != "media" {
			t.Errorf("uploadType=%v, want [media] — a conditional write must not be resumable", u)
		}
		if n := got.Query["name"]; len(n) != 1 || n[0] != "t4/lock" {
			t.Errorf("name=%v, want [t4/lock]", n)
		}
	})

	t.Run("PutIfMatch uses the given generation", func(t *testing.T) {
		f := newFakeGCS(t, nil)
		store := f.store(t, "t4")
		if err := store.PutIfMatch(context.Background(), "lock", strings.NewReader("v"), "4242"); err != nil {
			t.Fatalf("put-if-match: %v", err)
		}
		got := f.last(t)
		if g := got.Query["ifGenerationMatch"]; len(g) != 1 || g[0] != "4242" {
			t.Errorf("ifGenerationMatch=%v, want [4242]", g)
		}
		if u := got.Query["uploadType"]; len(u) != 1 || u[0] != "media" {
			t.Errorf("uploadType=%v, want [media]", u)
		}
	})
}

// A conditional write that cannot express its condition must fail, not send
// the write without one.
func TestGCSPutIfMatchSendsNothingForAnUnusableETag(t *testing.T) {
	f := newFakeGCS(t, nil)
	store := f.store(t, "t4")

	err := store.PutIfMatch(context.Background(), "lock", strings.NewReader("v"), "not-a-generation")
	if err == nil {
		t.Fatal("want an error for an ETag that is not a generation")
	}
	f.mu.Lock()
	n := len(f.requests)
	f.mu.Unlock()
	if n != 0 {
		t.Errorf("%d request(s) sent; an unusable precondition must not reach the store", n)
	}
}

func TestGCSStatusCodesMapToTheExpectedErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   error
	}{
		{"404 is not found", http.StatusNotFound, object.ErrNotFound},
		{"412 is a precondition failure", http.StatusPreconditionFailed, object.ErrPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeGCS(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":{"code":0,"message":"nope"}}`))
			})
			store := f.store(t, "")
			_, err := store.Get(context.Background(), "k")
			if !errors.Is(err, tc.want) {
				t.Errorf("Get: err=%v, want %v", err, tc.want)
			}
			if err := store.PutIfAbsent(context.Background(), "k", strings.NewReader("v")); !errors.Is(err, tc.want) {
				t.Errorf("PutIfAbsent: err=%v, want %v", err, tc.want)
			}
		})
	}
}

// A 500 carries GCS's own message, which is the only clue an operator gets.
func TestGCSServerErrorKeepsTheMessage(t *testing.T) {
	f := newFakeGCS(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":500,"message":"backend unavailable"}}`))
	})
	store := f.store(t, "")

	_, err := store.Get(context.Background(), "k")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "backend unavailable") {
		t.Errorf("err=%v, should carry the store's message", err)
	}
	if errors.Is(err, object.ErrNotFound) || errors.Is(err, object.ErrPreconditionFailed) {
		t.Errorf("err=%v must not be mistaken for a sentinel", err)
	}
}

// Deleting what is not there is not an error, per Store's contract.
func TestGCSDeleteToleratesAMissingObject(t *testing.T) {
	f := newFakeGCS(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	store := f.store(t, "")
	if err := store.Delete(context.Background(), "gone"); err != nil {
		t.Errorf("delete missing: %v", err)
	}
}

// List has to follow pageToken, or it silently reports only the first page and
// GC would miss everything after it.
func TestGCSListFollowsPagination(t *testing.T) {
	var calls int
	f := newFakeGCS(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("pageToken") == "" {
			_, _ = w.Write([]byte(`{"items":[{"name":"t4/wal/1"},{"name":"t4/wal/2"}],"nextPageToken":"tok"}`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[{"name":"t4/wal/3"}]}`))
	})
	store := f.store(t, "t4")

	keys, err := store.List(context.Background(), "wal/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"wal/1", "wal/2", "wal/3"}
	if len(keys) != len(want) {
		t.Fatalf("got %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("key %d: got %q, want %q (prefix should be stripped)", i, keys[i], want[i])
		}
	}
	if calls != 2 {
		t.Errorf("made %d requests, want 2 — the second page was not fetched", calls)
	}
}

// An endpoint override is for emulators, which have no credentials to present
// to. Without this, constructing a store in a test or against an emulator would
// need real GCP credentials.
func TestGCSEndpointOverrideSkipsCredentials(t *testing.T) {
	f := newFakeGCS(t, nil)
	store := f.store(t, "")
	if _, err := store.Get(context.Background(), "k"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if auth := f.last(t).Header.Get("Authorization"); auth != "" {
		t.Errorf("Authorization header was sent to an overridden endpoint: %q", auth)
	}
}

func TestGCSBucketIsRequired(t *testing.T) {
	_, err := object.NewGCSStoreFromConfig(context.Background(), object.GCSConfig{Prefix: "t4"})
	if err == nil {
		t.Fatal("want an error without a bucket")
	}
}

// The emulator accepts a Content-Range it should not, so these assert the
// header directly. A real GCS rejects a range whose end does not match the
// bytes sent, and an object uploaded against a lenient server would then fail
// only in production.
func TestGCSResumableSendsExactContentRanges(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want []string
	}{
		{"empty", "", []string{"bytes */0"}},
		{"shorter than a chunk", "abc", []string{"bytes 0-2/3"}},
		{"exactly one chunk", "abcd", []string{"bytes 0-3/4"}},
		{"one byte over a chunk", "abcde", []string{"bytes 0-3/*", "bytes 4-4/5"}},
		{"exact multiple", "abcdefgh", []string{"bytes 0-3/*", "bytes 4-7/8"}},
		{"ragged tail", "abcdefghij", []string{"bytes 0-3/*", "bytes 4-7/*", "bytes 8-9/10"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object.SetGCSChunkSizeForTest(t, 4)
			f := newFakeGCS(t, nil)
			f.handler = func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.Header().Set("Location", f.server.URL+"/upload-session")
					return
				}
				// A chunk that names a total is the last one.
				if strings.HasSuffix(r.Header.Get("Content-Range"), "/*") {
					w.WriteHeader(http.StatusPermanentRedirect)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"name":"o","generation":"1"}`))
			}
			store := f.store(t, "t4")

			// readerWithoutLen hides the length, so Put takes the resumable path.
			if err := store.Put(context.Background(), "big", readerWithoutLen{strings.NewReader(tc.body)}); err != nil {
				t.Fatalf("put: %v", err)
			}

			var got []string
			f.mu.Lock()
			for _, req := range f.requests {
				if req.Method == http.MethodPut {
					got = append(got, req.Header.Get("Content-Range"))
				}
			}
			f.mu.Unlock()

			if len(got) != len(tc.want) {
				t.Fatalf("Content-Range headers = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("chunk %d: Content-Range = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// Which upload shape is used is a deliberate choice, not an accident: an object
// large enough to be worth resuming goes up in chunks, a small one does not pay
// for a session it cannot benefit from. Both shapes produce a correct object, so
// only the request shape shows the intent.
func TestGCSPicksTheUploadShapeBySize(t *testing.T) {
	t.Run("at or above the chunk size is resumable", func(t *testing.T) {
		object.SetGCSChunkSizeForTest(t, 4)
		f := newFakeGCS(t, nil)
		f.handler = func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				w.Header().Set("Location", f.server.URL+"/upload-session")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"o","generation":"1"}`))
		}
		store := f.store(t, "t4")
		if err := store.Put(context.Background(), "big", strings.NewReader("abcdefgh")); err != nil {
			t.Fatalf("put: %v", err)
		}
		f.mu.Lock()
		first := f.requests[0]
		f.mu.Unlock()
		if u := first.Query["uploadType"]; len(u) != 1 || u[0] != "resumable" {
			t.Errorf("uploadType=%v, want [resumable] for an object at the chunk size", u)
		}
	})

	t.Run("below the chunk size is a single request", func(t *testing.T) {
		object.SetGCSChunkSizeForTest(t, 1<<20)
		f := newFakeGCS(t, nil)
		store := f.store(t, "t4")
		if err := store.Put(context.Background(), "small", strings.NewReader("abc")); err != nil {
			t.Fatalf("put: %v", err)
		}
		f.mu.Lock()
		n := len(f.requests)
		first := f.requests[0]
		f.mu.Unlock()
		if n != 1 {
			t.Errorf("made %d requests, want 1 for a small object", n)
		}
		if u := first.Query["uploadType"]; len(u) != 1 || u[0] != "media" {
			t.Errorf("uploadType=%v, want [media] for a small object", u)
		}
	})
}

// A credentials file is loaded as a service account and nothing else, which is
// what --gcs-credentials-file documents. The unconstrained loader is deprecated
// for exactly this reason: it will accept whatever type the file claims to be,
// including an external-account configuration that names a URL to fetch a token
// from.
func TestGCSCredentialsFileMustBeAServiceAccount(t *testing.T) {
	write := func(t *testing.T, contents string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "creds.json")
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write credentials: %v", err)
		}
		return path
	}

	t.Run("a service account key is accepted", func(t *testing.T) {
		_, err := object.NewGCSStoreFromConfig(context.Background(), object.GCSConfig{
			Bucket:          "b",
			CredentialsFile: write(t, serviceAccountJSON(t)),
		})
		if err != nil {
			t.Fatalf("a service account key should load: %v", err)
		}
	})

	for _, tc := range []struct {
		name string
		json string
	}{
		{"an authorized user", `{"type":"authorized_user","client_id":"x","client_secret":"y","refresh_token":"z"}`},
		{"an external account", `{"type":"external_account","audience":"//iam.googleapis.com/x","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":"https://sts.googleapis.com/v1/token","credential_source":{"url":"http://169.254.169.254/token"}}`},
		{"no type at all", `{"client_email":"a@b.com"}`},
	} {
		t.Run(tc.name+" is refused", func(t *testing.T) {
			_, err := object.NewGCSStoreFromConfig(context.Background(), object.GCSConfig{
				Bucket:          "b",
				CredentialsFile: write(t, tc.json),
			})
			if err == nil {
				t.Fatalf("%s must not be loaded from --gcs-credentials-file", tc.name)
			}
			if !strings.Contains(err.Error(), "service account") {
				t.Errorf("err=%v, should say a service account key was expected", err)
			}
		})
	}

	t.Run("a missing file is reported as such", func(t *testing.T) {
		_, err := object.NewGCSStoreFromConfig(context.Background(), object.GCSConfig{
			Bucket:          "b",
			CredentialsFile: filepath.Join(t.TempDir(), "absent.json"),
		})
		if err == nil {
			t.Fatal("want an error for a missing credentials file")
		}
		if !strings.Contains(err.Error(), "read credentials file") {
			t.Errorf("err=%v, should say the file could not be read", err)
		}
	})
}

// serviceAccountJSON builds a syntactically valid service account key, with a
// real RSA key so the loader's PEM parsing is satisfied. Nothing signs with it.
func serviceAccountJSON(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	doc := map[string]string{
		"type":         "service_account",
		"project_id":   "t4-test",
		"private_key":  string(pemBytes),
		"client_email": "t4@t4-test.iam.gserviceaccount.com",
		"token_uri":    "https://oauth2.googleapis.com/token",
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal credentials: %v", err)
	}
	return string(encoded)
}
