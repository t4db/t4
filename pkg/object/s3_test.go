package object_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/t4db/t4/pkg/object"
)

const testBucket = "test-bucket"

// newFakeS3Client returns a configured S3 client and httptest server for the
// given fake S3 backend. The server is shut down via t.Cleanup.
func newFakeS3Client(t *testing.T, backend gofakes3.Backend) (*s3.Client, string) {
	t.Helper()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	t.Cleanup(ts.Close)

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("access", "secret", ""),
		),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: ts.URL}, nil
			}),
		),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	return client, ts.URL
}

// newFakeS3 spins up an in-process fake S3 server and returns an S3Store
// pointed at it. The server is shut down via t.Cleanup.
func newFakeS3(t *testing.T, prefix string) *object.S3Store {
	t.Helper()
	client, _ := newFakeS3Client(t, s3mem.New())
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return object.NewS3Store(client, testBucket, prefix)
}

// newFakeS3Versioned creates a fake S3 store with versioning enabled on the
// bucket. Returns both the store and the raw client (to capture version IDs).
func newFakeS3Versioned(t *testing.T, prefix string) (*object.S3Store, *s3.Client) {
	t.Helper()
	client, _ := newFakeS3Client(t, s3mem.New())
	ctx := context.Background()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	return object.NewS3Store(client, testBucket, prefix), client
}

func TestS3PutGet(t *testing.T) {
	store := newFakeS3(t, "")
	ctx := context.Background()

	if err := store.Put(ctx, "hello/world", strings.NewReader("content")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := store.Get(ctx, "hello/world")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "content" {
		t.Errorf("Get value: want %q got %q", "content", got)
	}
}

func TestS3GetNotFound(t *testing.T) {
	store := newFakeS3(t, "")
	_, err := store.Get(context.Background(), "does/not/exist")
	if err != object.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestS3Overwrite(t *testing.T) {
	store := newFakeS3(t, "")
	ctx := context.Background()

	store.Put(ctx, "k", strings.NewReader("old"))
	store.Put(ctx, "k", strings.NewReader("new"))

	rc, _ := store.Get(ctx, "k")
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "new" {
		t.Errorf("overwrite: want new got %q", got)
	}
}

func TestS3Delete(t *testing.T) {
	store := newFakeS3(t, "")
	ctx := context.Background()

	store.Put(ctx, "todel", strings.NewReader("v"))
	if err := store.Delete(ctx, "todel"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := store.Get(ctx, "todel")
	if err != object.ErrNotFound {
		t.Errorf("after delete: want ErrNotFound, got %v", err)
	}
}

func TestS3List(t *testing.T) {
	store := newFakeS3(t, "")
	ctx := context.Background()

	keys := []string{"wal/0001/0001", "wal/0001/0002", "wal/0002/0001", "checkpoint/0001/0001"}
	for _, k := range keys {
		store.Put(ctx, k, strings.NewReader("x"))
	}

	got, err := store.List(ctx, "wal/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("List wal/: want 3 got %d: %v", len(got), got)
	}
	for _, k := range got {
		if !strings.HasPrefix(k, "wal/") {
			t.Errorf("unexpected key in list: %q", k)
		}
	}
}

func TestS3ListEmpty(t *testing.T) {
	store := newFakeS3(t, "")
	got, err := store.List(context.Background(), "nothing/")
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want empty list, got %v", got)
	}
}

func TestS3Prefix(t *testing.T) {
	store := newFakeS3(t, "tenant-1")
	ctx := context.Background()

	if err := store.Put(ctx, "wal/seg1", strings.NewReader("data")); err != nil {
		t.Fatalf("Put with prefix: %v", err)
	}

	// Key visible under its bare name (prefix stripped).
	rc, err := store.Get(ctx, "wal/seg1")
	if err != nil {
		t.Fatalf("Get with prefix: %v", err)
	}
	rc.Close()

	// List returns bare keys without the prefix.
	keys, err := store.List(ctx, "wal/")
	if err != nil {
		t.Fatalf("List with prefix: %v", err)
	}
	if len(keys) != 1 || keys[0] != "wal/seg1" {
		t.Errorf("List with prefix: want [wal/seg1] got %v", keys)
	}
}

func TestS3LargePayload(t *testing.T) {
	store := newFakeS3(t, "")
	ctx := context.Background()

	payload := strings.Repeat("x", 10<<20) // 10 MB
	if err := store.Put(ctx, "large", strings.NewReader(payload)); err != nil {
		t.Fatalf("Put large: %v", err)
	}
	rc, err := store.Get(ctx, "large")
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll large: %v", err)
	}
	if len(got) != len(payload) {
		t.Errorf("large payload: want %d bytes got %d", len(payload), len(got))
	}
}

func TestS3GetVersioned(t *testing.T) {
	store, client := newFakeS3Versioned(t, "")
	ctx := context.Background()

	// Put v1 via raw client to capture the version ID.
	out1, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("seg"),
		Body:   strings.NewReader("v1-content"),
	})
	if err != nil {
		t.Fatalf("put v1: %v", err)
	}
	v1 := aws.ToString(out1.VersionId)

	// Overwrite with v2.
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("seg"),
		Body:   strings.NewReader("v2-content"),
	}); err != nil {
		t.Fatalf("put v2: %v", err)
	}

	// Regular Get returns latest (v2).
	rc, err := store.Get(ctx, "seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "v2-content" {
		t.Errorf("Get: want v2-content got %q", got)
	}

	// GetVersioned with v1's ID returns the original content.
	rc, err = store.GetVersioned(ctx, "seg", v1)
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	got, _ = io.ReadAll(rc)
	rc.Close()
	if string(got) != "v1-content" {
		t.Errorf("GetVersioned: want v1-content got %q", got)
	}
}

func TestS3GetVersionedNotFound(t *testing.T) {
	store, _ := newFakeS3Versioned(t, "")
	_, err := store.GetVersioned(context.Background(), "no-such-key", "no-such-version")
	if err != object.ErrNotFound {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

func TestS3GetVersionedWithPrefix(t *testing.T) {
	store, client := newFakeS3Versioned(t, "pfx")
	ctx := context.Background()

	out, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("pfx/wal/seg1"),
		Body:   strings.NewReader("old"),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	ver := aws.ToString(out.VersionId)

	// Overwrite via store (goes through prefix logic).
	if err := store.Put(ctx, "wal/seg1", strings.NewReader("new")); err != nil {
		t.Fatalf("store put: %v", err)
	}

	rc, err := store.GetVersioned(ctx, "wal/seg1", ver)
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "old" {
		t.Errorf("GetVersioned with prefix: want old got %q", got)
	}
}
