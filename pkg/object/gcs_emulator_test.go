package object_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/t4db/t4/pkg/object"
)

// These run against a GCS emulator, which CI starts and which a developer can
// start with:
//
//	docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http
//	T4_GCS_ENDPOINT=http://127.0.0.1:4443 go test ./pkg/object/
//
// Without T4_GCS_ENDPOINT they skip. The emulator was checked by hand to
// enforce every precondition these tests rely on; TestGCSPassesTheConditionalWriteProbe
// keeps that honest, so a store or emulator that stops enforcing them fails
// here rather than as a split-brain much later.
func gcsEndpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("T4_GCS_ENDPOINT")
	if ep == "" {
		t.Skip("set T4_GCS_ENDPOINT to a GCS emulator to run the GCS tests")
	}
	return ep
}

// newGCSBucket creates a bucket on the emulator and returns a store over it.
func newGCSBucket(t *testing.T, prefix string) *object.GCSStore {
	t.Helper()
	ep := gcsEndpoint(t)
	bucket := fmt.Sprintf("t4-%s-%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), os.Getpid())
	bucket = strings.ReplaceAll(bucket, "_", "-")

	body := strings.NewReader(fmt.Sprintf(`{"name":%q}`, bucket))
	req, err := http.NewRequest(http.MethodPost, ep+"/storage/v1/b", body)
	if err != nil {
		t.Fatalf("create bucket request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusConflict {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("create bucket %q: HTTP %d: %s", bucket, resp.StatusCode, msg)
	}

	store, err := object.NewGCSStoreFromConfig(context.Background(), object.GCSConfig{
		Bucket:   bucket,
		Prefix:   prefix,
		Endpoint: ep,
	})
	if err != nil {
		t.Fatalf("new GCS store: %v", err)
	}
	return store
}

// The acceptance test for the whole backend: whatever else works, a store T4
// will elect a leader on has to enforce both preconditions.
func TestGCSPassesTheConditionalWriteProbe(t *testing.T) {
	store := newGCSBucket(t, "t4")

	support, err := object.ProbeConditional(context.Background(), store, "node-a")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !support.Offered {
		t.Fatal("the GCS store must offer conditional writes")
	}
	if !support.IfNoneMatch {
		t.Errorf("create-if-absent is not enforced: %s", support.Detail)
	}
	if !support.IfMatch {
		t.Errorf("compare-and-swap is not enforced: %s", support.Detail)
	}
	if !support.Enforced() {
		t.Fatalf("not usable for a cluster: %s", support.Detail)
	}
}

func TestGCSRoundTrip(t *testing.T) {
	store := newGCSBucket(t, "t4")
	ctx := context.Background()

	if err := store.Put(ctx, "a/b.txt", strings.NewReader("hello")); err != nil {
		t.Fatalf("put: %v", err)
	}
	rc, err := store.Get(ctx, "a/b.txt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}

	if _, err := store.Get(ctx, "a/missing.txt"); !errors.Is(err, object.ErrNotFound) {
		t.Errorf("missing object: err=%v, want ErrNotFound", err)
	}
}

// The prefix must be applied on write and stripped on list, so a store with a
// prefix behaves exactly like one without.
func TestGCSPrefixIsAppliedAndStripped(t *testing.T) {
	store := newGCSBucket(t, "t4/sub")
	ctx := context.Background()

	for _, k := range []string{"wal/0001", "wal/0002", "checkpoint/x"} {
		if err := store.Put(ctx, k, strings.NewReader("v")); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
	}
	keys, err := store.List(ctx, "wal/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := map[string]bool{"wal/0001": true, "wal/0002": true}
	if len(keys) != len(want) {
		t.Fatalf("got %v, want the two wal keys", keys)
	}
	for _, k := range keys {
		if !want[k] {
			t.Errorf("unexpected key %q — prefix not stripped?", k)
		}
	}
}

func TestGCSConditionalWrites(t *testing.T) {
	store := newGCSBucket(t, "t4")
	ctx := context.Background()

	if err := store.PutIfAbsent(ctx, "lock", strings.NewReader("one")); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := store.PutIfAbsent(ctx, "lock", strings.NewReader("two")); !errors.Is(err, object.ErrPreconditionFailed) {
		t.Fatalf("second create: err=%v, want ErrPreconditionFailed", err)
	}

	got, err := store.GetETag(ctx, "lock")
	if err != nil {
		t.Fatalf("get etag: %v", err)
	}
	_ = got.Body.Close()
	if got.ETag == "" {
		t.Fatal("no ETag; a generation is required for compare-and-swap")
	}
	stale := got.ETag

	if err := store.PutIfMatch(ctx, "lock", strings.NewReader("three"), stale); err != nil {
		t.Fatalf("cas with the current generation: %v", err)
	}
	if err := store.PutIfMatch(ctx, "lock", strings.NewReader("four"), stale); !errors.Is(err, object.ErrPreconditionFailed) {
		t.Fatalf("cas with a superseded generation: err=%v, want ErrPreconditionFailed", err)
	}
}

// A malformed ETag must fail loudly. Dropping the condition and writing anyway
// is the exact failure ProbeConditional exists to catch.
func TestGCSPutIfMatchRefusesANonGenerationETag(t *testing.T) {
	store := newGCSBucket(t, "t4")
	ctx := context.Background()

	if err := store.Put(ctx, "lock", strings.NewReader("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	err := store.PutIfMatch(ctx, "lock", strings.NewReader("two"), `"d41d8cd98f00b204e9800998ecf8427e"`)
	if err == nil {
		t.Fatal("an S3-style MD5 ETag is not a generation and must not write unconditionally")
	}
	if errors.Is(err, object.ErrPreconditionFailed) {
		t.Errorf("err=%v, want a malformed-ETag error rather than a precondition failure", err)
	}
}

// GetVersioned is covered in gcs_test.go against a recording server instead of
// here: it needs object versioning on the bucket, which fake-gcs-server rejects
// (HTTP 500 on a bucket with versioning enabled). The S3 path gets real
// coverage because rustfs supports versioning; until the GCS emulator does, the
// request shape is asserted directly and the behaviour is the store's own
// documented requirement, as it is for S3 (docs/api.md: "S3 versioning must be
// enabled on the source bucket").

func TestGCSDelete(t *testing.T) {
	store := newGCSBucket(t, "t4")
	ctx := context.Background()

	for _, k := range []string{"a", "b", "c"} {
		if err := store.Put(ctx, k, strings.NewReader("v")); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
	}
	if err := store.DeleteMany(ctx, []string{"a", "b"}); err != nil {
		t.Fatalf("delete many: %v", err)
	}
	keys, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 1 || keys[0] != "c" {
		t.Errorf("got %v, want only [c]", keys)
	}
	// Deleting what is not there is not an error, per Store.
	if err := store.Delete(ctx, "a"); err != nil {
		t.Errorf("delete missing: %v", err)
	}
}

// An object at or above the chunk size goes up resumably, in several requests.
// The sizes here straddle the boundary cases the chunking gets wrong most
// easily: an exact multiple, one byte over, and empty.
func TestGCSResumableUploadSizes(t *testing.T) {
	store := newGCSBucket(t, "t4")
	ctx := context.Background()
	object.SetGCSChunkSizeForTest(t, 1<<16) // 64 KiB, so the test stays quick

	for _, size := range []int{0, 1, (1 << 16) - 1, 1 << 16, (1 << 16) + 1, 3 << 16, (3 << 16) + 7} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			want := make([]byte, size)
			for i := range want {
				want[i] = byte(i % 251)
			}
			key := fmt.Sprintf("big/%d", size)
			// A bytes.Reader reports its length; an io.Reader that does not is
			// the path that has to hold a chunk back to find the end.
			if err := store.Put(ctx, key, readerWithoutLen{bytes.NewReader(want)}); err != nil {
				t.Fatalf("put: %v", err)
			}
			rc, err := store.Get(ctx, key)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			got, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("round trip of %d bytes: got %d bytes back%s", size, len(got), firstDiff(got, want))
			}
		})
	}
}

// readerWithoutLen hides Len() so sniffReaderSize cannot shortcut, forcing the
// resumable path regardless of size.
type readerWithoutLen struct{ r io.Reader }

func (r readerWithoutLen) Read(p []byte) (int, error) { return r.r.Read(p) }

func firstDiff(got, want []byte) string {
	for i := range got {
		if i >= len(want) {
			return fmt.Sprintf(", first extra byte at %d", i)
		}
		if got[i] != want[i] {
			return fmt.Sprintf(", first difference at byte %d", i)
		}
	}
	if len(want) > len(got) {
		return fmt.Sprintf(", truncated at byte %d", len(got))
	}
	return ""
}

// Exactly one writer may win a contested conditional write. This is the
// invariant the leader lock rests on: election.Lock reads the lock and writes
// it back conditionally, and treats a write that was not refused as having won.
// If the store admits two writers, two nodes serve as leader at once.
//
// The probe checks the preconditions one call at a time, which a store can
// satisfy while still racing internally. These put real concurrency on it.
func TestGCSContestedWritesHaveExactlyOneWinner(t *testing.T) {
	const (
		racers = 16
		rounds = 25
	)

	t.Run("create-if-absent", func(t *testing.T) {
		store := newGCSBucket(t, "contested-create")
		for round := 0; round < rounds; round++ {
			key := fmt.Sprintf("lock-%d", round)
			won, refused, failed := raceWriters(racers, func(i int) error {
				return store.PutIfAbsent(context.Background(), key, strings.NewReader(strconv.Itoa(i)))
			})
			if won != 1 {
				t.Fatalf("round %d: %d writers won a contested create, want exactly 1 (%d refused, %d errored)",
					round, won, refused, len(failed))
			}
			for _, err := range failed {
				t.Errorf("round %d: unexpected error: %v", round, err)
			}
		}
	})

	t.Run("compare-and-swap on the same generation", func(t *testing.T) {
		store := newGCSBucket(t, "contested-cas")
		ctx := context.Background()
		for round := 0; round < rounds; round++ {
			key := fmt.Sprintf("lock-%d", round)
			if err := store.Put(ctx, key, strings.NewReader("seed")); err != nil {
				t.Fatalf("round %d: seed: %v", round, err)
			}
			seen, err := store.GetETag(ctx, key)
			if err != nil {
				t.Fatalf("round %d: read generation: %v", round, err)
			}
			_ = seen.Body.Close()

			// Every writer presents the generation it read, as two nodes racing
			// to renew the same lock would.
			won, refused, failed := raceWriters(racers, func(i int) error {
				return store.PutIfMatch(ctx, key, strings.NewReader(strconv.Itoa(i)), seen.ETag)
			})
			if won != 1 {
				t.Fatalf("round %d: %d writers won a contested compare-and-swap on one generation, want exactly 1 (%d refused, %d errored)",
					round, won, refused, len(failed))
			}
			for _, err := range failed {
				t.Errorf("round %d: unexpected error: %v", round, err)
			}
		}
	})
}

// raceWriters runs write n times at once and classifies the outcomes: how many
// were allowed, how many were refused by the precondition, and any error that
// was neither.
func raceWriters(n int, write func(i int) error) (won, refused int, failed []error) {
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := write(i)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, object.ErrPreconditionFailed):
				refused++
			default:
				failed = append(failed, err)
			}
		}(i)
	}
	wg.Wait()
	return won, refused, failed
}
