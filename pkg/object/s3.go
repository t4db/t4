package object

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	defaultS3EndpointRegion = "us-east-1"
	defaultAWSS3Endpoint    = "s3.amazonaws.com"
)

// S3Store implements Store backed by an S3-compatible bucket via minio-go.
type S3Store struct {
	client *minio.Client
	bucket string
	prefix string // optional key prefix, without leading or trailing slashes
}

// NewS3Store creates a Store backed by the given S3 bucket.
// prefix is prepended to every key (may be empty). Leading and trailing
// slashes are ignored, so "t4", "t4/" and "/t4/" all address the same keys.
func NewS3Store(client *minio.Client, bucket, prefix string) *S3Store {
	return &S3Store{client: client, bucket: bucket, prefix: strings.Trim(prefix, "/")}
}

// S3Config holds the parameters for NewS3StoreFromConfig.
type S3Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string

	// Prefix is an optional key prefix. Leading and trailing slashes are
	// ignored.
	Prefix string

	// Endpoint overrides the S3 endpoint URL for MinIO and other
	// S3-compatible stores. Must include a scheme (http:// or https://).
	// Empty means the standard AWS endpoint.
	Endpoint string

	// Region overrides the AWS region. When Endpoint is set and Region is
	// empty, t4 uses us-east-1 as the signing region for S3-compatible stores.
	Region string

	// Profile enables the ambient AWS credentials chain (env vars
	// → ~/.aws/credentials[profile] → EC2/EKS IMDS) for credential
	// resolution. The named profile is consulted as part of that chain.
	// SSO and AssumeRole profiles are not supported. Ignored when
	// AccessKeyID is set.
	Profile string

	// AccessKeyID and SecretAccessKey provide static credentials. Both must be
	// set together. SessionToken is optional (set when using temporary
	// credentials from STS) and is ignored unless AccessKeyID is also set.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// CABundle is an optional PEM-encoded CA certificate bundle to trust
	// when talking HTTPS to the S3 endpoint. Empty means use the system
	// trust store. Useful for MinIO and other S3-compatible stores running
	// behind a self-signed CA. Honored regardless of OS — pin the bundle
	// explicitly rather than relying on SSL_CERT_FILE / Keychain.
	CABundle string
}

// NewS3StoreFromConfig creates an S3Store from explicit t4 S3 settings.
//
// Credential resolution order:
//   - AccessKeyID + SecretAccessKey: use static credentials (with optional
//     SessionToken for temporary STS credentials).
//   - Profile: walk the ambient AWS credentials chain — env vars first
//     (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN), then
//     the named profile in ~/.aws/credentials, then EC2/EKS IMDS.
//   - otherwise: fail closed. t4 does not consult any ambient credentials chain
//     unless a profile is explicitly selected.
//
// Use NewS3Store directly when you already have a preconfigured *minio.Client.
func NewS3StoreFromConfig(ctx context.Context, cfg S3Config) (*S3Store, error) {
	opts, err := s3MinioOptions(cfg)
	if err != nil {
		return nil, err
	}
	endpoint, err := s3EndpointHost(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("object/s3: minio client: %w", err)
	}
	_ = ctx // reserved for future credential-resolution side effects
	return NewS3Store(client, cfg.Bucket, cfg.Prefix), nil
}

// s3MinioOptions builds the *minio.Options the constructor would pass.
// Exported within the package so tests can assert on it without making
// network calls.
func s3MinioOptions(cfg S3Config) (*minio.Options, error) {
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return nil, fmt.Errorf("object/s3: AccessKeyID and SecretAccessKey must both be set or both be empty")
	}
	if cfg.SessionToken != "" && cfg.AccessKeyID == "" {
		return nil, fmt.Errorf("object/s3: SessionToken set without AccessKeyID/SecretAccessKey")
	}
	if cfg.AccessKeyID == "" && cfg.Profile == "" {
		return nil, fmt.Errorf("object/s3: credentials not configured; set T4_S3_ACCESS_KEY_ID and T4_S3_SECRET_ACCESS_KEY, or set T4_S3_PROFILE (for example \"default\")")
	}

	var creds *credentials.Credentials
	if cfg.AccessKeyID != "" {
		creds = credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	} else {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{Profile: cfg.Profile},
			&credentials.IAM{Client: &http.Client{Transport: http.DefaultTransport}},
		})
	}

	secure := s3EndpointSecure(cfg.Endpoint)

	opts := &minio.Options{
		Creds:  creds,
		Secure: secure,
		Region: effectiveS3Region(cfg),
	}

	if cfg.CABundle != "" {
		transport, err := s3TransportWithCABundle(cfg.CABundle)
		if err != nil {
			return nil, err
		}
		opts.Transport = transport
	}

	return opts, nil
}

// s3EndpointHost extracts the host[:port] component minio.New expects from
// the user-supplied URL. Empty means use the AWS default endpoint.
func s3EndpointHost(endpoint string) (string, error) {
	if endpoint == "" {
		return defaultAWSS3Endpoint, nil
	}
	if !strings.Contains(endpoint, "://") {
		return "", fmt.Errorf("object/s3: endpoint %q must include scheme (http:// or https://)", endpoint)
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("object/s3: parse endpoint %q: %w", endpoint, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("object/s3: endpoint %q has empty host", endpoint)
	}
	return u.Host, nil
}

func s3EndpointSecure(endpoint string) bool {
	if endpoint == "" {
		return true
	}
	return strings.HasPrefix(endpoint, "https://")
}

// s3TransportWithCABundle returns an *http.Transport whose TLS config trusts
// only the CA roots in caBundlePath (the system pool is intentionally omitted
// so the operator's stated CA is the single source of truth for the S3
// endpoint).
func s3TransportWithCABundle(caBundlePath string) (*http.Transport, error) {
	pemBytes, err := os.ReadFile(caBundlePath)
	if err != nil {
		return nil, fmt.Errorf("object/s3: read CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("object/s3: CA bundle %q contains no PEM certificates", caBundlePath)
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		},
	}, nil
}

func effectiveS3Region(cfg S3Config) string {
	if cfg.Region != "" {
		return cfg.Region
	}
	if cfg.Endpoint != "" {
		return defaultS3EndpointRegion
	}
	return ""
}

func (s *S3Store) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return s.prefix + "/" + k
}

func (s *S3Store) Put(ctx context.Context, key string, r io.Reader) error {
	size := sniffReaderSize(r)
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), r, size, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("object/s3: put %q: %w", key, err)
	}
	return nil
}

// sniffReaderSize returns the remaining byte count of r when it exposes a
// cheap length (Len()-style readers and *os.File). Otherwise -1, which makes
// minio-go fall back to multipart-streaming PutObject.
func sniffReaderSize(r io.Reader) int64 {
	type lenReader interface{ Len() int }
	if lr, ok := r.(lenReader); ok {
		return int64(lr.Len())
	}
	if f, ok := r.(*os.File); ok {
		if fi, err := f.Stat(); err == nil {
			cur, cerr := f.Seek(0, io.SeekCurrent)
			if cerr == nil {
				return fi.Size() - cur
			}
		}
	}
	return -1
}

func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.getVersioned(ctx, key, "", "get")
}

// GetETag returns the object body and its current ETag.
// The body and the ETag come from the same GET, so they always describe the
// same version of the object.
func (s *S3Store) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	body, info, _, err := minio.Core{Client: s.client}.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/s3: get-etag %q: %w", key, err)
	}
	return &GetWithETag{Body: body, ETag: info.ETag}, nil
}

// PutIfAbsent writes to key only if it does not exist (If-None-Match: *).
// Returns ErrPreconditionFailed if the key already exists.
func (s *S3Store) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), r, sniffReaderSize(r), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return ErrPreconditionFailed
		}
		return fmt.Errorf("object/s3: put-if-absent %q: %w", key, err)
	}
	return nil
}

// PutIfMatch writes to key only if its current ETag equals matchETag.
// Returns ErrPreconditionFailed if the ETag has changed.
func (s *S3Store) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	opts := minio.PutObjectOptions{}
	opts.SetMatchETag(matchETag)
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), r, sniffReaderSize(r), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return ErrPreconditionFailed
		}
		return fmt.Errorf("object/s3: put-if-match %q: %w", key, err)
	}
	return nil
}

// GetVersioned retrieves a specific stored version of key. Requires S3
// versioning to be enabled on the bucket.
func (s *S3Store) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	return s.getVersioned(ctx, key, versionID, "get versioned")
}

func (s *S3Store) getVersioned(ctx context.Context, key, versionID, label string) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if versionID != "" {
		opts.VersionID = versionID
	}
	// One GET returns the body and surfaces a missing key. A separate stat
	// first would make minio-go condition the GET on the stat's ETag, which
	// fails with 412 if the object is overwritten in between, as the
	// manifest and the leader lock routinely are.
	body, _, _, err := minio.Core{Client: s.client}.GetObject(ctx, s.bucket, s.key(key), opts)
	if err != nil {
		if isNotFound(err) || (versionID != "" && isInvalidVersion(err)) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/s3: %s %q: %w", label, key, err)
	}
	return body, nil
}

// isInvalidVersion reports whether err is the S3/MinIO response for a
// version ID that doesn't refer to a stored version (e.g. malformed or
// never existed). Real MinIO surfaces this as Code=InvalidArgument with
// "Invalid version id specified"; AWS S3 surfaces it as Code=NoSuchVersion.
func isInvalidVersion(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		if resp.Code == "NoSuchVersion" {
			return true
		}
		if resp.Code == "InvalidArgument" && strings.Contains(resp.Message, "version") {
			return true
		}
	}
	return false
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, s.key(key), minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("object/s3: delete %q: %w", key, err)
	}
	return nil
}

func (s *S3Store) DeleteMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	objectsCh := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		objectsCh <- minio.ObjectInfo{Key: s.key(k)}
	}
	close(objectsCh)

	var errs []error
	for e := range s.client.RemoveObjects(ctx, s.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if e.Err == nil {
			continue
		}
		errs = append(errs, fmt.Errorf("object/s3: delete-many %q: %w", e.ObjectName, e.Err))
	}
	return errors.Join(errs...)
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.key(prefix)
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    fullPrefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("object/s3: list %q: %w", prefix, obj.Err)
		}
		k := obj.Key
		if s.prefix != "" {
			k = k[len(s.prefix)+1:]
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func isNotFound(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		return resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound
	}
	return false
}

func isPreconditionFailed(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		// S3 returns PreconditionFailed (412) for If-Match failures and
		// ConditionalRequestConflict (409) for If-None-Match conflicts.
		return resp.Code == "PreconditionFailed" || resp.Code == "ConditionalRequestConflict"
	}
	return false
}
