package object

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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
