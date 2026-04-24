package object

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const defaultS3EndpointRegion = "us-east-1"

// S3Store implements Store backed by an AWS S3 bucket.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string // optional key prefix (no trailing slash needed)
}

// NewS3Store creates a Store backed by the given S3 bucket.
// prefix is prepended to every key (may be empty).
func NewS3Store(client *s3.Client, bucket, prefix string) *S3Store {
	return &S3Store{client: client, bucket: bucket, prefix: prefix}
}

// S3Config holds the parameters for NewS3StoreFromConfig.
type S3Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string

	// Prefix is an optional key prefix (no trailing slash needed).
	Prefix string

	// Endpoint overrides the S3 endpoint URL for MinIO and other
	// S3-compatible stores. When set, path-style addressing is used
	// automatically. Empty means the standard AWS endpoint.
	Endpoint string

	// Region overrides the AWS region. When Endpoint is set and Region is
	// empty, t4 uses us-east-1 as the signing region for S3-compatible stores.
	Region string

	// Profile selects a named profile from the shared AWS config/credentials
	// files. When empty, shared config/credentials files are ignored entirely.
	// Ignored when AccessKeyID is set.
	Profile string

	// AccessKeyID and SecretAccessKey provide static credentials. Both must be
	// set together.
	AccessKeyID     string
	SecretAccessKey string
}

// NewS3StoreFromConfig creates an S3Store from explicit t4 S3 settings.
//
// Credential resolution order:
//   - AccessKeyID + SecretAccessKey: use static credentials.
//   - Profile: load only that named shared config/credentials profile.
//   - otherwise: fail closed. t4 does not use the AWS ambient/default chain
//     unless a profile is explicitly selected.
//
// Use NewS3Store directly when you already have a preconfigured *s3.Client.
func NewS3StoreFromConfig(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return nil, fmt.Errorf("object/s3: AccessKeyID and SecretAccessKey must both be set or both be empty")
	}
	if cfg.AccessKeyID == "" && cfg.Profile == "" {
		return nil, fmt.Errorf("object/s3: credentials not configured; set T4_S3_ACCESS_KEY_ID and T4_S3_SECRET_ACCESS_KEY, or set T4_S3_PROFILE (for example \"default\")")
	}
	optFns := s3LoadOptions(cfg)
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("object/s3: load aws config: %w", err)
	}
	var clientOpts []func(*s3.Options)
	if cfg.Endpoint != "" {
		// Path-style addressing is required for MinIO and other S3-compatible stores.
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	client := s3.NewFromConfig(awsCfg, clientOpts...)
	return NewS3Store(client, cfg.Bucket, cfg.Prefix), nil
}

func s3LoadOptions(cfg S3Config) []func(*awsconfig.LoadOptions) error {
	optFns := []func(*awsconfig.LoadOptions) error{}
	if cfg.AccessKeyID != "" {
		optFns = append(optFns, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	} else if cfg.Profile != "" {
		optFns = append(optFns, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}
	if cfg.Endpoint != "" {
		optFns = append(optFns, awsconfig.WithBaseEndpoint(cfg.Endpoint))
	}
	if region := effectiveS3Region(cfg); region != "" {
		optFns = append(optFns, awsconfig.WithRegion(region))
	}
	return optFns
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
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("object/s3: put %q: %w", key, err)
	}
	return nil
}

func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/s3: get %q: %w", key, err)
	}
	return out.Body, nil
}

// GetETag returns the object body and its current ETag.
func (s *S3Store) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/s3: get-etag %q: %w", key, err)
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return &GetWithETag{Body: out.Body, ETag: etag}, nil
}

// PutIfAbsent writes to key only if it does not exist (If-None-Match: *).
// Returns ErrPreconditionFailed if the key already exists.
func (s *S3Store) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.key(key)),
		Body:        r,
		IfNoneMatch: aws.String("*"),
	})
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
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(s.bucket),
		Key:     aws.String(s.key(key)),
		Body:    r,
		IfMatch: aws.String(matchETag),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return ErrPreconditionFailed
		}
		return fmt.Errorf("object/s3: put-if-match %q: %w", key, err)
	}
	return nil
}

func isPreconditionFailed(err error) bool {
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		c := apiErr.ErrorCode()
		// S3 returns PreconditionFailed (412) for If-Match failures and
		// ConditionalRequestConflict (409) for If-None-Match conflicts.
		return c == "PreconditionFailed" || c == "ConditionalRequestConflict"
	}
	return false
}

// GetVersioned retrieves a specific stored version of key. Requires S3
// versioning to be enabled on the bucket.
func (s *S3Store) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(s.bucket),
		Key:       aws.String(s.key(key)),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/s3: get versioned %q@%s: %w", key, versionID, err)
	}
	return out.Body, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return fmt.Errorf("object/s3: delete %q: %w", key, err)
	}
	return nil
}

const deleteObjectsMaxKeys = 1000

func (s *S3Store) DeleteMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	var errs []error
	for len(keys) > 0 {
		batch := keys
		if len(batch) > deleteObjectsMaxKeys {
			batch = keys[:deleteObjectsMaxKeys]
		}
		keys = keys[len(batch):]

		objs := make([]types.ObjectIdentifier, len(batch))
		for i, k := range batch {
			objs[i] = types.ObjectIdentifier{Key: aws.String(s.key(k))}
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: objs,
				Quiet:   aws.Bool(true), // only report errors, not successes
			},
		})
		if err != nil {
			return fmt.Errorf("object/s3: delete-many: %w", err)
		}
		for _, e := range out.Errors {
			key := ""
			if e.Key != nil {
				key = *e.Key
			}
			msg := ""
			if e.Message != nil {
				msg = *e.Message
			}
			errs = append(errs, fmt.Errorf("object/s3: delete-many %q: %s", key, msg))
		}
	}
	return errors.Join(errs...)
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.key(prefix)
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("object/s3: list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			k := *obj.Key
			// Strip the store-level prefix so callers see bare keys.
			if s.prefix != "" {
				k = k[len(s.prefix)+1:]
			}
			keys = append(keys, k)
		}
	}
	return keys, nil
}
