package object

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Google Cloud Storage, spoken over its JSON API.
//
// T4 does not use GCS through the S3-compatible XML API. That endpoint accepts
// If-None-Match and If-Match and honors neither, so a conditional write returns
// success whether or not its precondition held, and two nodes racing for the
// leader lock would both be told they won it (see ProbeConditional, which
// refuses a cluster on such a store).
//
// GCS's own preconditions are enforced. ifGenerationMatch names the generation
// a write expects to replace, and the value 0 means "only if no live version
// exists" — the two conditions ConditionalStore needs. A generation is this
// store's ETag: T4 treats an ETag as an opaque version token and never
// interprets one, so the only code that reads a generation back out of an ETag
// is this file, which put it there.
//
// The client here is hand-rolled over net/http rather than
// cloud.google.com/go/storage, which costs 29 further modules and 23 MiB of
// binary for an API surface of six operations.

const (
	defaultGCSEndpoint = "https://storage.googleapis.com"
	// gcsScope is read/write on objects. T4 never creates or configures
	// buckets, so the full-control scope is not requested.
	gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"
)

// gcsResponseHeaderTimeout bounds the wait for GCS to start answering a request
// that has been sent, as s3ResponseHeaderTimeout does for S3 and for the same
// reason: a connection that silently stopped carrying traffic otherwise holds
// whatever waits on it, and a leader that flushes each write to the object
// store stops accepting writes meanwhile.
var gcsResponseHeaderTimeout = 15 * time.Second

// gcsChunkSize is how much of a large object goes in one request of a resumable
// upload. An object at or above this size is uploaded resumably, so a failure
// costs one chunk rather than the whole transfer; a smaller one goes in a single
// request, where resumability would buy nothing.
var gcsChunkSize int64 = 16 << 20

// GCSConfig holds the parameters for NewGCSStoreFromConfig.
type GCSConfig struct {
	// Bucket is the GCS bucket name. Required.
	Bucket string

	// Prefix is an optional object name prefix. Leading and trailing slashes
	// are ignored, so "t4", "t4/" and "/t4/" all address the same objects.
	Prefix string

	// CredentialsFile is the path to a service account key file. When empty,
	// credentials come from Application Default Credentials: the
	// GOOGLE_APPLICATION_CREDENTIALS environment variable, then the gcloud
	// user credentials, then the metadata server, which is what workload
	// identity on GKE provides.
	//
	// This differs from the S3 path, which refuses to consult an ambient
	// credentials chain unless --s3-profile selects one. On GCP the ambient
	// chain is how a workload is normally given an identity at all, and
	// requiring an explicit opt-in would mean every deployment sets a flag
	// saying "yes, really" to the only sensible answer.
	CredentialsFile string

	// Endpoint overrides the GCS endpoint, for an emulator or test double.
	// Empty means the real service.
	//
	// Requests to an overridden endpoint are unauthenticated unless
	// CredentialsFile is also set, since an emulator has no credentials to
	// present to and ADC would fail or, worse, send a real token somewhere
	// unexpected.
	Endpoint string
}

// GCSStore implements Store, ConditionalStore and VersionedStore against a GCS
// bucket.
type GCSStore struct {
	http     *http.Client
	endpoint string
	bucket   string
	prefix   string
}

var (
	_ Store            = (*GCSStore)(nil)
	_ ConditionalStore = (*GCSStore)(nil)
	_ VersionedStore   = (*GCSStore)(nil)
)

// NewGCSStoreFromConfig creates a GCSStore from explicit t4 GCS settings.
func NewGCSStoreFromConfig(ctx context.Context, cfg GCSConfig) (*GCSStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("object/gcs: bucket is required")
	}
	endpoint := strings.TrimSuffix(cfg.Endpoint, "/")
	if endpoint == "" {
		endpoint = defaultGCSEndpoint
	}

	base := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: gcsResponseHeaderTimeout,
	}

	client := &http.Client{Transport: base}
	if creds, err := gcsCredentials(ctx, cfg); err != nil {
		return nil, err
	} else if creds != nil {
		client = &http.Client{Transport: &oauth2.Transport{Base: base, Source: creds}}
	}

	return &GCSStore{
		http:     client,
		endpoint: endpoint,
		bucket:   cfg.Bucket,
		prefix:   strings.Trim(cfg.Prefix, "/"),
	}, nil
}

// gcsCredentials returns the token source to sign requests with, or nil for an
// unauthenticated client.
func gcsCredentials(ctx context.Context, cfg GCSConfig) (oauth2.TokenSource, error) {
	if cfg.CredentialsFile != "" {
		data, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("object/gcs: read credentials file: %w", err)
		}
		creds, err := google.CredentialsFromJSON(ctx, data, gcsScope)
		if err != nil {
			return nil, fmt.Errorf("object/gcs: parse credentials file: %w", err)
		}
		return creds.TokenSource, nil
	}
	if cfg.Endpoint != "" {
		// An emulator: see GCSConfig.Endpoint.
		return nil, nil
	}
	creds, err := google.FindDefaultCredentials(ctx, gcsScope)
	if err != nil {
		return nil, fmt.Errorf("object/gcs: application default credentials: %w "+
			"(set --gcs-credentials-file, or GOOGLE_APPLICATION_CREDENTIALS)", err)
	}
	return creds.TokenSource, nil
}

// name returns the full object name for a store-relative key.
func (s *GCSStore) name(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// objectURL addresses one object's metadata or, with alt=media, its content.
//
// The object name occupies a single path segment, so a "/" inside it has to
// arrive as %2F or the request addresses a different object. url.PathEscape is
// exactly the right tool: it escapes a string for use *within* a segment, so
// it encodes "/" too.
func (s *GCSStore) objectURL(key string, q url.Values) string {
	escaped := url.PathEscape(s.name(key))
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s", s.endpoint, url.PathEscape(s.bucket), escaped)
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

// uploadURL addresses the upload endpoint. The object name rides as a query
// parameter here, where url.Values escapes it.
func (s *GCSStore) uploadURL(key string, q url.Values) string {
	q.Set("name", s.name(key))
	return fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", s.endpoint, url.PathEscape(s.bucket), q.Encode())
}

// gcsObject is the part of GCS's object metadata that T4 reads.
type gcsObject struct {
	Name       string `json:"name"`
	Generation string `json:"generation"`
}

// gcsErrorBody is GCS's JSON error envelope.
type gcsErrorBody struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// statusError turns a non-2xx response into the error T4 expects, consuming the
// body. ErrNotFound and ErrPreconditionFailed are returned unwrapped, as the
// S3 store returns them, because callers match on them with errors.Is.
func statusError(op string, resp *http.Response) error {
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	switch resp.StatusCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusPreconditionFailed:
		return ErrPreconditionFailed
	}
	var env gcsErrorBody
	if err := json.Unmarshal(body, &env); err == nil && env.Error.Message != "" {
		return fmt.Errorf("object/gcs: %s: %s (HTTP %d)", op, env.Error.Message, resp.StatusCode)
	}
	return fmt.Errorf("object/gcs: %s: HTTP %d: %s", op, resp.StatusCode, strings.TrimSpace(string(body)))
}

func (s *GCSStore) do(req *http.Request) (*http.Response, error) {
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ── reads ─────────────────────────────────────────────────────────────────────

func (s *GCSStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, _, err := s.get(ctx, key, nil, "get")
	return rc, err
}

// GetETag returns the object body and the generation it was read at, so the
// two always describe the same version.
func (s *GCSStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	rc, gen, err := s.get(ctx, key, nil, "get-etag")
	if err != nil {
		return nil, err
	}
	return &GetWithETag{Body: rc, ETag: gen}, nil
}

// GetVersioned reads one stored generation of key.
func (s *GCSStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	rc, _, err := s.get(ctx, key, url.Values{"generation": {versionID}}, "get-versioned")
	return rc, err
}

func (s *GCSStore) get(ctx context.Context, key string, q url.Values, op string) (io.ReadCloser, string, error) {
	if q == nil {
		q = url.Values{}
	}
	q.Set("alt", "media")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.objectURL(key, q), nil)
	if err != nil {
		return nil, "", fmt.Errorf("object/gcs: %s %q: %w", op, key, err)
	}
	resp, err := s.do(req)
	if err != nil {
		return nil, "", fmt.Errorf("object/gcs: %s %q: %w", op, key, err)
	}
	if resp.StatusCode/100 != 2 {
		err := statusError(op+" "+strconv.Quote(key), resp)
		return nil, "", err
	}
	// The generation of the version just read, which is what makes a later
	// PutIfMatch a compare-and-swap against it.
	return resp.Body, resp.Header.Get("X-Goog-Generation"), nil
}

func (s *GCSStore) List(ctx context.Context, prefix string) ([]string, error) {
	full := s.name(prefix)
	var keys []string
	var pageToken string
	for {
		q := url.Values{"prefix": {full}}
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}
		u := fmt.Sprintf("%s/storage/v1/b/%s/o?%s", s.endpoint, url.PathEscape(s.bucket), q.Encode())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, fmt.Errorf("object/gcs: list %q: %w", prefix, err)
		}
		resp, err := s.do(req)
		if err != nil {
			return nil, fmt.Errorf("object/gcs: list %q: %w", prefix, err)
		}
		if resp.StatusCode/100 != 2 {
			return nil, statusError("list "+strconv.Quote(prefix), resp)
		}
		var page struct {
			Items         []gcsObject `json:"items"`
			NextPageToken string      `json:"nextPageToken"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("object/gcs: list %q: decode: %w", prefix, err)
		}
		for _, it := range page.Items {
			k := it.Name
			if s.prefix != "" {
				k = strings.TrimPrefix(k, s.prefix+"/")
			}
			keys = append(keys, k)
		}
		if page.NextPageToken == "" {
			return keys, nil
		}
		pageToken = page.NextPageToken
	}
}

// ── deletes ───────────────────────────────────────────────────────────────────

func (s *GCSStore) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.objectURL(key, nil), nil)
	if err != nil {
		return fmt.Errorf("object/gcs: delete %q: %w", key, err)
	}
	resp, err := s.do(req)
	if err != nil {
		return fmt.Errorf("object/gcs: delete %q: %w", key, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		// Deleting a missing object is not an error, per Store.
		_ = resp.Body.Close()
		return nil
	}
	if resp.StatusCode/100 != 2 {
		return statusError("delete "+strconv.Quote(key), resp)
	}
	_ = resp.Body.Close()
	return nil
}

// DeleteMany deletes one object per request. GCS has a batch endpoint, but it
// is a multipart envelope around the same individual requests and reports each
// outcome separately, so it saves round trips rather than work; this stays
// simple until a profile says otherwise.
func (s *GCSStore) DeleteMany(ctx context.Context, keys []string) error {
	for _, k := range keys {
		if err := s.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// ── writes ────────────────────────────────────────────────────────────────────

// Put writes key, resumably when the object is large enough for that to matter.
func (s *GCSStore) Put(ctx context.Context, key string, r io.Reader) error {
	if size := sniffReaderSize(r); size >= 0 && size < gcsChunkSize {
		_, err := s.uploadSimple(ctx, key, r, size, nil, "put")
		return err
	}
	return s.uploadResumable(ctx, key, r)
}

// PutIfAbsent writes key only if no live version exists
// (ifGenerationMatch=0), and returns ErrPreconditionFailed otherwise.
func (s *GCSStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	_, err := s.uploadConditional(ctx, key, r, "0", "put-if-absent")
	return err
}

// PutIfMatch writes key only if its current generation is the one matchETag
// names, and returns ErrPreconditionFailed otherwise.
func (s *GCSStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	gen := strings.Trim(matchETag, `"`)
	if _, err := strconv.ParseInt(gen, 10, 64); err != nil {
		// Refuse rather than drop the condition: a write that silently stops
		// being conditional is the failure ProbeConditional exists to catch.
		return fmt.Errorf("object/gcs: put-if-match %q: %q is not a GCS generation: %w", key, matchETag, err)
	}
	_, err := s.uploadConditional(ctx, key, r, gen, "put-if-match")
	return err
}

// uploadConditional sends the whole body in one request.
//
// Deliberately not resumable: a resumable upload applies its precondition when
// the session is created, not when it completes, so two writers could both be
// admitted and the second would overwrite the first — the hazard
// encryptToBuffer documents for S3 multipart. T4's conditional objects are the
// leader lock, the checkpoint manifest and the branch registry, all small.
func (s *GCSStore) uploadConditional(ctx context.Context, key string, r io.Reader, generation, op string) (string, error) {
	return s.uploadSimple(ctx, key, r, sniffReaderSize(r), url.Values{"ifGenerationMatch": {generation}}, op)
}

// uploadSimple PUTs the body in a single request and returns the new
// generation. size may be -1, in which case the body is sent with chunked
// transfer encoding.
func (s *GCSStore) uploadSimple(ctx context.Context, key string, r io.Reader, size int64, q url.Values, op string) (string, error) {
	if q == nil {
		q = url.Values{}
	}
	q.Set("uploadType", "media")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.uploadURL(key, q), r)
	if err != nil {
		return "", fmt.Errorf("object/gcs: %s %q: %w", op, key, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if size >= 0 {
		req.ContentLength = size
	}
	resp, err := s.do(req)
	if err != nil {
		return "", fmt.Errorf("object/gcs: %s %q: %w", op, key, err)
	}
	if resp.StatusCode/100 != 2 {
		return "", statusError(op+" "+strconv.Quote(key), resp)
	}
	defer func() { _ = resp.Body.Close() }()
	var obj gcsObject
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return "", fmt.Errorf("object/gcs: %s %q: decode response: %w", op, key, err)
	}
	return obj.Generation, nil
}

// uploadResumable uploads in gcsChunkSize pieces, so a failure costs one chunk
// rather than the whole object.
//
// GCS needs the total length on the final chunk, which an io.Reader does not
// offer, so this holds one chunk back: a chunk is only sent once the next read
// has shown whether it is the last.
func (s *GCSStore) uploadResumable(ctx context.Context, key string, r io.Reader) error {
	session, err := s.startResumable(ctx, key)
	if err != nil {
		return err
	}

	bufA := make([]byte, gcsChunkSize)
	bufB := make([]byte, gcsChunkSize)
	pending, err := readChunk(r, bufA)
	if err != nil {
		return fmt.Errorf("object/gcs: put %q: read: %w", key, err)
	}
	var offset int64
	for {
		next, err := readChunk(r, bufB)
		if err != nil {
			return fmt.Errorf("object/gcs: put %q: read: %w", key, err)
		}
		if len(next) == 0 {
			// pending is the last chunk, so the total length is known.
			return s.putChunk(ctx, session, key, pending, offset, offset+int64(len(pending)))
		}
		if err := s.putChunk(ctx, session, key, pending, offset, -1); err != nil {
			return err
		}
		offset += int64(len(pending))
		pending, bufA, bufB = next, bufB, bufA
	}
}

// readChunk fills buf as far as the reader allows, returning a short or empty
// slice at the end of the stream.
func readChunk(r io.Reader, buf []byte) ([]byte, error) {
	n, err := io.ReadFull(r, buf)
	switch {
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return buf[:n], nil
	default:
		return nil, err
	}
}

// startResumable opens an upload session and returns its URI.
func (s *GCSStore) startResumable(ctx context.Context, key string) (string, error) {
	u := s.uploadURL(key, url.Values{"uploadType": {"resumable"}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", fmt.Errorf("object/gcs: put %q: start upload: %w", key, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = 0
	resp, err := s.do(req)
	if err != nil {
		return "", fmt.Errorf("object/gcs: put %q: start upload: %w", key, err)
	}
	if resp.StatusCode/100 != 2 {
		return "", statusError("put "+strconv.Quote(key)+": start upload", resp)
	}
	_ = resp.Body.Close()
	session := resp.Header.Get("Location")
	if session == "" {
		return "", fmt.Errorf("object/gcs: put %q: start upload: no session URI in the response", key)
	}
	return session, nil
}

// putChunk sends one piece of a resumable upload. total is the object's full
// length on the last chunk and -1 before it is known.
func (s *GCSStore) putChunk(ctx context.Context, session, key string, chunk []byte, offset, total int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session, strings.NewReader(string(chunk)))
	if err != nil {
		return fmt.Errorf("object/gcs: put %q: chunk at %d: %w", key, offset, err)
	}
	req.ContentLength = int64(len(chunk))
	req.Header.Set("Content-Range", contentRange(offset, len(chunk), total))
	resp, err := s.do(req)
	if err != nil {
		return fmt.Errorf("object/gcs: put %q: chunk at %d: %w", key, offset, err)
	}
	// 308 Resume Incomplete is how GCS acknowledges a chunk that is not the
	// last one; it is success here, not a redirect to follow.
	if resp.StatusCode == http.StatusPermanentRedirect {
		_ = resp.Body.Close()
		return nil
	}
	if resp.StatusCode/100 != 2 {
		return statusError(fmt.Sprintf("put %s: chunk at %d", strconv.Quote(key), offset), resp)
	}
	_ = resp.Body.Close()
	return nil
}

// contentRange formats the header a resumable chunk carries. An empty object
// has no bytes to name, so it reports only its length.
func contentRange(offset int64, n int, total int64) string {
	if n == 0 {
		return "bytes */0"
	}
	last := offset + int64(n) - 1
	if total < 0 {
		return fmt.Sprintf("bytes %d-%d/*", offset, last)
	}
	return fmt.Sprintf("bytes %d-%d/%d", offset, last, total)
}
