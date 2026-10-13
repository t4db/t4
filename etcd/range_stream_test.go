package etcd_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// collectRangeStream merges a RangeStream's chunks the way etcd documents
// them, checking that only the last carries the header, More and Count.
func collectRangeStream(t *testing.T, kv etcdserverpb.KVClient, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, int, error) {
	t.Helper()
	stream, err := kv.RangeStream(context.Background(), r)
	if err != nil {
		return nil, 0, err
	}
	merged := &etcdserverpb.RangeResponse{}
	chunks := 0
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, chunks, err
		}
		if merged.Header != nil {
			t.Fatalf("chunk %d after the one carrying the header", chunks)
		}
		chunks++
		rr := resp.RangeResponse
		merged.Kvs = append(merged.Kvs, rr.Kvs...)
		if rr.Header != nil {
			merged.Header, merged.More, merged.Count = rr.Header, rr.More, rr.Count
		} else if rr.More || rr.Count != 0 {
			t.Fatalf("chunk %d has More or Count but no header", chunks)
		}
	}
	if merged.Header == nil {
		t.Fatal("no chunk carried the header")
	}
	return merged, chunks, nil
}

// TestRangeStreamMatchesRange: RangeStream answers what Range answers, split
// into chunks of about a megabyte.
func TestRangeStreamMatchesRange(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	value := strings.Repeat("v", 40<<10)
	var revs []int64
	for i := range 60 {
		r, err := cli.Put(ctx, fmt.Sprintf("/rs/%03d", i), value)
		if err != nil {
			t.Fatal(err)
		}
		revs = append(revs, r.Header.Revision)
	}
	if _, err := cli.Put(ctx, "/other", "x"); err != nil {
		t.Fatal(err)
	}
	// Leases keep records under T4's internal prefix, which a scan of the
	// whole keyspace passes over.
	for range 30 {
		lease, err := cli.Grant(ctx, 600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli.Put(ctx, fmt.Sprintf("/leased/%d", lease.ID), "v", clientv3.WithLease(lease.ID)); err != nil {
			t.Fatal(err)
		}
	}
	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	end := []byte(clientv3.GetPrefixRangeEnd("/rs/"))

	for _, tc := range []struct {
		name      string
		req       *etcdserverpb.RangeRequest
		minChunks int
	}{
		{"prefix", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end}, 2},
		{"limit", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end, Limit: 30}, 2},
		{"keys only", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end, KeysOnly: true}, 1},
		{"count only", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end, CountOnly: true}, 1},
		{"at a past revision", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end, Revision: revs[9]}, 1},
		{"serializable", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end, Serializable: true}, 2},
		{"from a key", &etcdserverpb.RangeRequest{Key: []byte("/rs/050"), RangeEnd: []byte{0}}, 1},
		{"single key", &etcdserverpb.RangeRequest{Key: []byte("/rs/007")}, 1},
		{"no match", &etcdserverpb.RangeRequest{Key: []byte("/none/"), RangeEnd: []byte("/none0")}, 1},
		{"a range that is no prefix", &etcdserverpb.RangeRequest{Key: []byte("/rs/010"), RangeEnd: []byte("/rs/050")}, 2},
		{"a range that is no prefix, with a limit", &etcdserverpb.RangeRequest{Key: []byte("/rs/010"), RangeEnd: []byte("/rs/050"), Limit: 25}, 2},
		{"the whole keyspace", &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}, 2},
		{"the whole keyspace, keys only, with a limit", &etcdserverpb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, KeysOnly: true, Limit: 70}, 1},
		{"key ascending is the default order", &etcdserverpb.RangeRequest{Key: []byte("/rs/"), RangeEnd: end, SortTarget: etcdserverpb.RangeRequest_KEY, SortOrder: etcdserverpb.RangeRequest_ASCEND}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := kv.Range(ctx, proto.Clone(tc.req).(*etcdserverpb.RangeRequest))
			if err != nil {
				t.Fatal(err)
			}
			got, chunks, err := collectRangeStream(t, kv, proto.Clone(tc.req).(*etcdserverpb.RangeRequest))
			if err != nil {
				t.Fatal(err)
			}
			if chunks < tc.minChunks {
				t.Errorf("%d chunks, want at least %d", chunks, tc.minChunks)
			}
			if got.Header.Revision != want.Header.Revision || got.More != want.More || got.Count != want.Count {
				t.Errorf("header revision %d, more %v, count %d; Range answers %d, %v, %d",
					got.Header.Revision, got.More, got.Count, want.Header.Revision, want.More, want.Count)
			}
			if len(got.Kvs) != len(want.Kvs) {
				t.Fatalf("%d key-values, Range answers %d", len(got.Kvs), len(want.Kvs))
			}
			for i := range want.Kvs {
				if !proto.Equal(got.Kvs[i], want.Kvs[i]) {
					t.Fatalf("key-value %d is %s, Range answers %s", i, got.Kvs[i].Key, want.Kvs[i].Key)
				}
			}
		})
	}
}

// TestRangeStreamRefusesWhatEtcdRefuses: like etcd, RangeStream refuses sort
// orders other than the default and revision filters.
func TestRangeStreamRefusesWhatEtcdRefuses(t *testing.T) {
	_, cli := newWatchNode(t)
	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	for name, req := range map[string]*etcdserverpb.RangeRequest{
		"descending":          {Key: []byte("/a"), RangeEnd: []byte("/b"), SortOrder: etcdserverpb.RangeRequest_DESCEND},
		"by version":          {Key: []byte("/a"), RangeEnd: []byte("/b"), SortTarget: etcdserverpb.RangeRequest_VERSION, SortOrder: etcdserverpb.RangeRequest_ASCEND},
		"min mod revision":    {Key: []byte("/a"), RangeEnd: []byte("/b"), MinModRevision: 2},
		"max create revision": {Key: []byte("/a"), RangeEnd: []byte("/b"), MaxCreateRevision: 2},
	} {
		_, _, err := collectRangeStream(t, kv, req)
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("%s: %v, want Unimplemented", name, err)
		}
	}
}

// TestRangeStreamChecksRBAC: RangeStream needs read permission on its whole
// range, as Range does.
func TestRangeStreamChecksRBAC(t *testing.T) {
	c := newRBACCluster(t)
	kv := etcdserverpb.NewKVClient(c.alice.ActiveConnection())
	_, _, err := collectRangeStream(t, kv, &etcdserverpb.RangeRequest{Key: []byte("/pub/"), RangeEnd: []byte{0}})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("RangeStream from /pub/ to the end: %v, want PermissionDenied", err)
	}
	got, _, err := collectRangeStream(t, kv, &etcdserverpb.RangeRequest{Key: []byte("/pub/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/pub/"))})
	if err != nil {
		t.Fatalf("RangeStream of /pub/: %v", err)
	}
	if len(got.Kvs) != 1 || string(got.Kvs[0].Key) != "/pub/a" {
		t.Errorf("RangeStream of /pub/ = %v, want /pub/a", got.Kvs)
	}
}

// TestRangeStreamReadsOneRevision: every chunk reads at the revision the
// stream started at, so writes made while it runs are not in it.
func TestRangeStreamReadsOneRevision(t *testing.T) {
	_, cli := newWatchNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	value := strings.Repeat("v", 100<<10)
	for i := range 40 {
		if _, err := cli.Put(ctx, fmt.Sprintf("/snap/%03d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	req := &etcdserverpb.RangeRequest{Key: []byte("/snap/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/snap/"))}
	stream, err := kv.RangeStream(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.RangeResponse.Header != nil {
		t.Fatal("the whole range came in one chunk; the test needs several")
	}
	// Change the range while the stream is under way.
	if _, err := cli.Put(ctx, "/snap/039", "changed"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/snap/zzz", "added"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Delete(ctx, "/snap/020"); err != nil {
		t.Fatal(err)
	}
	got := &etcdserverpb.RangeResponse{Kvs: first.RangeResponse.Kvs}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got.Kvs = append(got.Kvs, resp.RangeResponse.Kvs...)
		if h := resp.RangeResponse.Header; h != nil {
			got.Header, got.Count = h, resp.RangeResponse.Count
		}
	}
	at := proto.Clone(req).(*etcdserverpb.RangeRequest)
	at.Revision = got.Header.Revision
	want, err := kv.Range(ctx, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 40 || len(want.Kvs) != 40 || got.Count != 40 {
		t.Fatalf("%d key-values, count %d; Range at revision %d has %d, want 40", len(got.Kvs), got.Count, got.Header.Revision, len(want.Kvs))
	}
	for i := range want.Kvs {
		if !proto.Equal(got.Kvs[i], want.Kvs[i]) {
			t.Fatalf("key-value %d is %s at mod revision %d, Range at revision %d has %s at %d",
				i, got.Kvs[i].Key, got.Kvs[i].ModRevision, got.Header.Revision, want.Kvs[i].Key, want.Kvs[i].ModRevision)
		}
	}
}
