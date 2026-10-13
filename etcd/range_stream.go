package etcd

import (
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/t4db/t4"
	"github.com/t4db/t4/etcd/auth"
)

// Chunk sizing for RangeStream, as etcd sizes them: the first chunk holds a
// few keys, and each next one twice or half as many as the last until chunks
// come out near rangeStreamChunkBytes.
const (
	rangeStreamFirstChunk  = 10
	rangeStreamChunkBytes  = 1 << 20
	rangeStreamCountBuffer = 1000 // keys read at a time to count what is left
)

// RangeStream answers what Range answers, split into chunks, without holding
// the whole range: each chunk reads the next run of keys at the revision the
// stream started at, and carries those key-values in key order. Only the last
// chunk carries the header, More and Count, as etcd sends them. Writes made
// after the stream starts are not in it, and a compaction past its revision
// before it ends fails it, as in etcd.
//
// Like etcd, it refuses sort orders other than the default and revision
// filters. A sort target without an order is the default there too, and the
// key-values come in key order. A single key, and a count alone, carry no
// key-values worth streaming; Range answers those, in one chunk.
func (s *Server) RangeStream(r *etcdserverpb.RangeRequest, stream etcdserverpb.KV_RangeStreamServer) error {
	if r.SortOrder != etcdserverpb.RangeRequest_NONE &&
		(r.SortTarget != etcdserverpb.RangeRequest_KEY || r.SortOrder != etcdserverpb.RangeRequest_ASCEND) {
		return status.Error(codes.Unimplemented, "RangeStream does not support custom sort orders")
	}
	if r.MinModRevision != 0 || r.MaxModRevision != 0 || r.MinCreateRevision != 0 || r.MaxCreateRevision != 0 {
		return status.Error(codes.Unimplemented, "RangeStream does not support revision filters")
	}
	r.SortOrder, r.SortTarget = etcdserverpb.RangeRequest_NONE, etcdserverpb.RangeRequest_KEY
	ctx := stream.Context()

	if len(r.RangeEnd) == 0 || r.CountOnly {
		resp, err := s.Range(ctx, r)
		if err != nil {
			return err
		}
		return stream.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: resp})
	}

	// Pin the revision once, after the read index if the read is
	// linearizable; every chunk then reads locally at it.
	head := s.node.CurrentRevision()
	if !r.Serializable && s.node.ReadConsistency() != t4.ReadConsistencySerializable {
		rev, err := s.node.LinearizableRevision(ctx)
		if err != nil {
			return kvError(err)
		}
		head = rev
	}
	readRev := head
	if r.Revision > 0 {
		readRev = fromEtcdRevision(r.Revision)
		if compactRev := s.node.CompactRevision(); compactRev > 0 && readRev < compactRev {
			return rpctypes.ErrGRPCCompacted
		}
	}
	final := &etcdserverpb.RangeResponse{Header: s.headerAt(head)}
	if readRev == 0 {
		// Nothing was written by then.
		return stream.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: final})
	}

	scan := rangeStreamScan{s: s, r: r, rev: readRev, rangeEnd: string(r.RangeEnd)}
	scan.prefix, _ = rangeScan(string(r.Key), scan.rangeEnd)
	from := string(r.Key)
	limit := r.Limit // 0: no limit
	chunk := int64(rangeStreamFirstChunk)
	var sent int64
	for {
		n := chunk
		if limit > 0 {
			n = min(n, limit-sent)
		}
		kvs, next, err := scan.read(from, n, r.KeysOnly)
		if err != nil {
			return err
		}
		sent += int64(len(kvs))
		last := next == "" || (limit > 0 && sent == limit)
		if !last {
			if err := stream.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Kvs: kvs}}); err != nil {
				return err
			}
			chunk = nextChunk(chunk, kvs)
			from = next
			continue
		}
		final.Kvs, final.Count = kvs, sent
		if next != "" {
			// The limit cut the range short: count the rest, as etcd does.
			rest, err := scan.count(next)
			if err != nil {
				return err
			}
			final.More, final.Count = rest > 0, sent+rest
		}
		return stream.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: final})
	}
}

// nextChunk sizes the next chunk from the last one, as etcd does: twice as
// many keys if it came out under half the target size, half as many if over
// twice it.
func nextChunk(n int64, kvs []*mvccpb.KeyValue) int64 {
	size := 0
	for _, kv := range kvs {
		size += proto.Size(kv)
	}
	switch {
	case size < rangeStreamChunkBytes/2:
		n *= 2
	case size > rangeStreamChunkBytes*2:
		n /= 2
	}
	return max(n, 1)
}

// rangeStreamScan reads r's range at rev in runs. prefix scopes the reads
// when the range is prefix-shaped; otherwise they scan on from the start key
// and stop at the range end.
type rangeStreamScan struct {
	s        *Server
	r        *etcdserverpb.RangeRequest
	prefix   string
	rangeEnd string
	rev      int64
}

// read returns up to n key-values of the range from key from on, and the key
// to continue from, or "" if the range has no more.
func (sc rangeStreamScan) read(from string, n int64, keysOnly bool) ([]*mvccpb.KeyValue, string, error) {
	var kept []*t4.KeyValue
	for {
		// One more than needed tells where the next read continues.
		want := n + 1 - int64(len(kept))
		opts := []t4.ReadOption{t4.WithRevision(sc.rev), t4.WithFromKey(from), t4.WithLimit(want)}
		if keysOnly {
			opts = append(opts, t4.WithKeysOnly())
		}
		all, err := sc.s.node.List(sc.prefix, opts...)
		if err != nil {
			return nil, "", kvError(err)
		}
		skipTo := ""
		for _, kv := range all {
			if sc.pastEnd(kv.Key) {
				return sc.convert(kept), "", nil
			}
			if isInternalKey(kv.Key) {
				// Jump past T4's own keys rather than read through them.
				skipTo = internalKeysEnd(kv.Key)
				break
			}
			if int64(len(kept)) == n {
				return sc.convert(kept), kv.Key, nil
			}
			kept = append(kept, kv)
		}
		switch {
		case skipTo != "":
			from = skipTo
		case int64(len(all)) < want:
			return sc.convert(kept), "", nil // the scan has no more
		default:
			from = all[len(all)-1].Key + "\x00"
		}
	}
}

// internalKeysEnd returns the first key past the reserved prefix key is in.
func internalKeysEnd(key string) string {
	for _, prefix := range []string{internalPrefix, auth.AuthPrefix} {
		if strings.HasPrefix(key, prefix) {
			return prefixRangeEnd(prefix)
		}
	}
	return key + "\x00"
}

// count counts the range's keys from key from on.
func (sc rangeStreamScan) count(from string) (int64, error) {
	if sc.prefix != "" {
		n, err := sc.s.node.Count(sc.prefix, t4.WithRevision(sc.rev), t4.WithFromKey(from))
		return n, kvError(err)
	}
	var n int64
	for from != "" {
		kvs, next, err := sc.read(from, rangeStreamCountBuffer, true)
		if err != nil {
			return 0, err
		}
		n += int64(len(kvs))
		from = next
	}
	return n, nil
}

func (sc rangeStreamScan) pastEnd(key string) bool {
	return sc.rangeEnd != "\x00" && key >= sc.rangeEnd
}

func (sc rangeStreamScan) convert(kvs []*t4.KeyValue) []*mvccpb.KeyValue {
	batch := newRangeBatch(kvs)
	out := make([]*mvccpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, batch.rangeKV(kv, sc.r))
	}
	return out
}
