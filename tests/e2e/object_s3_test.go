package e2e_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/t4db/t4/pkg/object"
)

// objectStoreConfig wires the S3 endpoint into an object.S3Config and
// keeps the raw minio-go client around for test-side bucket setup.
type objectStoreConfig struct {
	cfg    s3TestConfig
	bucket string
	prefix string
	store  *object.S3Store
	raw    *minio.Client
}

func newObjectStoreTest(t *testing.T, ctx context.Context, prefix string, versioned bool) *objectStoreConfig {
	t.Helper()
	if os.Getenv("T4_E2E_S3") == "" {
		t.Skip("set T4_E2E_S3=1 to run the S3-backed object store test")
	}
	cfg := s3TestConfig{
		endpoint: envOr("S3_ENDPOINT", "http://127.0.0.1:9000"),
		bucket:   fmt.Sprintf("t4-object-%d", time.Now().UnixNano()),
		prefix:   prefix,
		access:   envOr("S3_ACCESS_KEY", "t4testadmin"),
		secret:   envOr("S3_SECRET_KEY", "t4testadmin"),
		region:   envOr("S3_REGION", "us-east-1"),
	}
	if err := ensureBucket(ctx, cfg); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	raw, err := s3Client(ctx, cfg)
	if err != nil {
		t.Fatalf("raw s3 client: %v", err)
	}
	if versioned {
		if err := raw.EnableVersioning(ctx, cfg.bucket); err != nil {
			t.Fatalf("EnableVersioning: %v", err)
		}
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ch := make(chan minio.ObjectInfo)
		go func() {
			defer close(ch)
			for obj := range raw.ListObjects(cleanCtx, cfg.bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: versioned}) {
				if obj.Err != nil {
					continue
				}
				ch <- obj
			}
		}()
		for err := range raw.RemoveObjects(cleanCtx, cfg.bucket, ch, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
			_ = err
		}
		_ = raw.RemoveBucket(cleanCtx, cfg.bucket)
	})

	store, err := object.NewS3StoreFromConfig(ctx, object.S3Config{
		Bucket:          cfg.bucket,
		Prefix:          cfg.prefix,
		Endpoint:        cfg.endpoint,
		Region:          cfg.region,
		AccessKeyID:     cfg.access,
		SecretAccessKey: cfg.secret,
	})
	if err != nil {
		t.Fatalf("S3Store: %v", err)
	}
	return &objectStoreConfig{cfg: cfg, bucket: cfg.bucket, prefix: prefix, store: store, raw: raw}
}

func TestS3PutGet(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	if err := o.store.Put(ctx, "hello/world", strings.NewReader("content")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := o.store.Get(ctx, "hello/world")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "content" {
		t.Errorf("Get value: want %q got %q", "content", got)
	}
}

func TestS3GetNotFound(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	if _, err := o.store.Get(ctx, "does/not/exist"); err != object.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestS3Overwrite(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	err := o.store.Put(ctx, "k", strings.NewReader("old"))
	if err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	if err := o.store.Put(ctx, "k", strings.NewReader("new")); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	rc, _ := o.store.Get(ctx, "k")
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if string(got) != "new" {
		t.Errorf("overwrite: want new got %q", got)
	}
}

func TestS3Delete(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	err := o.store.Put(ctx, "todel", strings.NewReader("v"))
	if err != nil {
		t.Fatalf("Put before delete: %v", err)
	}
	if err := o.store.Delete(ctx, "todel"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := o.store.Get(ctx, "todel"); err != object.ErrNotFound {
		t.Errorf("after delete: want ErrNotFound, got %v", err)
	}
}

func TestS3List(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	keys := []string{"wal/0001/0001", "wal/0001/0002", "wal/0002/0001", "checkpoint/0001/0001"}
	for _, k := range keys {
		if err := o.store.Put(ctx, k, strings.NewReader("x")); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	got, err := o.store.List(ctx, "wal/")
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
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	got, err := o.store.List(ctx, "nothing/")
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want empty list, got %v", got)
	}
}

func TestS3Prefix(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "tenant-1", false)

	if err := o.store.Put(ctx, "wal/seg1", strings.NewReader("data")); err != nil {
		t.Fatalf("Put with prefix: %v", err)
	}
	rc, err := o.store.Get(ctx, "wal/seg1")
	if err != nil {
		t.Fatalf("Get with prefix: %v", err)
	}
	_ = rc.Close()

	keys, err := o.store.List(ctx, "wal/")
	if err != nil {
		t.Fatalf("List with prefix: %v", err)
	}
	if len(keys) != 1 || keys[0] != "wal/seg1" {
		t.Errorf("List with prefix: want [wal/seg1] got %v", keys)
	}
}

func TestS3LargePayload(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", false)

	payload := strings.Repeat("x", 10<<20)
	if err := o.store.Put(ctx, "large", strings.NewReader(payload)); err != nil {
		t.Fatalf("Put large: %v", err)
	}
	rc, err := o.store.Get(ctx, "large")
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll large: %v", err)
	}
	if len(got) != len(payload) {
		t.Errorf("large payload: want %d bytes got %d", len(payload), len(got))
	}
}

func TestS3GetVersioned(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", true)

	info1, err := o.raw.PutObject(ctx, o.bucket, "seg", strings.NewReader("v1-content"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("put v1: %v", err)
	}
	v1 := info1.VersionID

	if _, err := o.raw.PutObject(ctx, o.bucket, "seg", strings.NewReader("v2-content"), -1, minio.PutObjectOptions{}); err != nil {
		t.Fatalf("put v2: %v", err)
	}

	rc, err := o.store.Get(ctx, "seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "v2-content" {
		t.Errorf("Get: want v2-content got %q", got)
	}

	rc, err = o.store.GetVersioned(ctx, "seg", v1)
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "v1-content" {
		t.Errorf("GetVersioned: want v1-content got %q", got)
	}
}

func TestS3GetVersionedNotFound(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "", true)

	if _, err := o.store.GetVersioned(ctx, "no-such-key", "00000000-0000-0000-0000-000000000000"); err != object.ErrNotFound {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

func TestS3GetVersionedWithPrefix(t *testing.T) {
	ctx := context.Background()
	o := newObjectStoreTest(t, ctx, "pfx", true)

	info, err := o.raw.PutObject(ctx, o.bucket, "pfx/wal/seg1", strings.NewReader("old"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	ver := info.VersionID

	if err := o.store.Put(ctx, "wal/seg1", strings.NewReader("new")); err != nil {
		t.Fatalf("store put: %v", err)
	}

	rc, err := o.store.GetVersioned(ctx, "wal/seg1", ver)
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "old" {
		t.Errorf("GetVersioned with prefix: want old got %q", got)
	}
}
