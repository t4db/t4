package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// prefixEnd is the exclusive upper bound clientv3.WithPrefix sends for prefix.
func prefixEnd(prefix string) []byte {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return b[:i+1]
		}
	}
	return []byte{0}
}

// TestRangePaginatesPrefix walks a prefix the way kube-apiserver paginates a
// LIST: the first page sends key == prefix, and every continuation page
// repeats the same rangeEnd with key advanced to the continue token
// (lastKey + "\x00"). Every page must be served by seeking into the prefix,
// return the next Limit keys in order, and report Count as the number of keys
// remaining from the continuation point.
func TestRangePaginatesPrefix(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	const (
		prefix = "/registry/pods/default/"
		total  = 10
		limit  = 3
	)
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("%spod-%02d", prefix, i)
		put(t, srv, k, "v")
		want = append(want, k)
	}
	// Decoys on either side of the prefix range: a shorter sibling that sorts
	// before it and a longer one that sorts after. Neither may appear on any
	// page, and neither may be counted.
	put(t, srv, "/registry/pods/aaa", "v")
	put(t, srv, "/registry/podz/zzz", "v")

	end := prefixEnd(prefix)
	key := []byte(prefix)

	var got []string
	for page := 0; ; page++ {
		if page > total {
			t.Fatalf("pagination did not terminate after %d pages", page)
		}
		resp, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      key,
			RangeEnd: end,
			Limit:    limit,
		})
		if err != nil {
			t.Fatalf("page %d: Range: %v", page, err)
		}

		// Count is "keys remaining in [key, rangeEnd)", which apiserver
		// surfaces as RemainingItemCount.
		wantRemaining := int64(total - len(got))
		if resp.Count != wantRemaining {
			t.Errorf("page %d: Count = %d, want %d", page, resp.Count, wantRemaining)
		}
		if wantMore := wantRemaining > limit; resp.More != wantMore {
			t.Errorf("page %d: More = %v, want %v", page, resp.More, wantMore)
		}
		if len(resp.Kvs) == 0 {
			t.Fatalf("page %d: empty page with %d keys still outstanding", page, wantRemaining)
		}
		if int64(len(resp.Kvs)) > limit {
			t.Fatalf("page %d: returned %d kvs, limit is %d", page, len(resp.Kvs), limit)
		}

		for _, kv := range resp.Kvs {
			got = append(got, string(kv.Key))
		}
		if !resp.More {
			break
		}
		// Continue token: the byte after the last key returned.
		key = append(append([]byte{}, resp.Kvs[len(resp.Kvs)-1].Key...), 0)
	}

	if len(got) != len(want) {
		t.Fatalf("paged %d keys, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("key %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestRangeContinuationCountExcludesEarlierKeys pins the Count semantics a
// seeked continuation page must have: keys before the continuation point are
// already delivered and must not be counted again.
func TestRangeContinuationCountExcludesEarlierKeys(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	const prefix = "/registry/cm/"
	for i := 0; i < 5; i++ {
		put(t, srv, fmt.Sprintf("%sc%d", prefix, i), "v")
	}

	resp, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:       []byte(prefix + "c3"),
		RangeEnd:  prefixEnd(prefix),
		CountOnly: true,
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	// c3, c4 remain.
	if resp.Count != 2 {
		t.Errorf("Count = %d, want 2", resp.Count)
	}
}

// TestRangeNonPrefixBoundsStillFiltered guards the fallback path: a rangeEnd
// that is not any prefix's upper bound must still return exactly the keys in
// [key, rangeEnd).
func TestRangeNonPrefixBoundsStillFiltered(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	for _, k := range []string{"/a/1", "/a/2", "/a/3", "/a/4", "/b/1"} {
		put(t, srv, k, "v")
	}

	resp, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/a/2"),
		RangeEnd: []byte("/a/4"), // explicit bound, not a prefix end
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	var got []string
	for _, kv := range resp.Kvs {
		got = append(got, string(kv.Key))
	}
	if len(got) != 2 || got[0] != "/a/2" || got[1] != "/a/3" {
		t.Errorf("got %v, want [/a/2 /a/3]", got)
	}
}

// TestRangeFromKeyOpenEnded guards rangeEnd == "\x00" ("all keys >= key"),
// which has no predecessor byte and so cannot be served as a prefix seek.
func TestRangeFromKeyOpenEnded(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	for _, k := range []string{"/a", "/m", "/z"} {
		put(t, srv, k, "v")
	}

	resp, err := srv.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/m"),
		RangeEnd: []byte{0},
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	var got []string
	for _, kv := range resp.Kvs {
		got = append(got, string(kv.Key))
	}
	if len(got) != 2 || got[0] != "/m" || got[1] != "/z" {
		t.Errorf("got %v, want [/m /z]", got)
	}
}
