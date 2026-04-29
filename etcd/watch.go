package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/t4db/t4"
)

// Maximum events coalesced into a single WatchResponse frame. Real etcd
// batches events on the wire; per-event Send is the dominant cost under high
// churn. The drain loop only takes events that are *immediately* available
// from the upstream channel (buffered in t4.Node.Watch), so this is a soft
// cap aligned with the upstream buffer — slow clients don't accumulate
// hundreds of events here.
const watchMaxBatch = 64

// Watch implements WatchServer.Watch (bidirectional streaming).
//
// One stream multiplexes many watches. gRPC requires that Send on a stream is
// not invoked concurrently, so all responses funnel through sendCh. Each
// watch runs in its own goroutine that drains events from the underlying
// t4.Node.Watch channel into batched WatchResponses after the watch creation
// response has been queued.
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()

	sendCh := make(chan *etcdserverpb.WatchResponse, 128)
	go func() {
		for {
			select {
			case resp := <-sendCh:
				_ = stream.Send(resp)
			case <-ctx.Done():
				return
			}
		}
	}()

	var watches sync.Map // map[int64]context.CancelFunc
	var nextID int64 = 1

	defer func() {
		watches.Range(func(_, v any) bool {
			v.(context.CancelFunc)()
			return true
		})
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}

		switch v := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			cr := v.CreateRequest
			if isInternalKey(string(cr.Key)) {
				select {
				case sendCh <- &etcdserverpb.WatchResponse{
					Header:       s.header(),
					WatchId:      -1,
					Canceled:     true,
					CancelReason: "reserved internal prefix is not watchable",
				}:
				case <-ctx.Done():
					return nil
				}
				continue
			}
			id := nextID
			nextID++

			wctx, cancel := context.WithCancel(ctx)

			// Subscribe synchronously so ErrCompacted is reported immediately,
			// but do not start draining replay events until after the Created
			// response is queued. Etcd clients expect the create ack to be the
			// first response for a new watch ID.
			sub, err := s.subscribeWatch(wctx, cr)
			if err != nil {
				cancel()
				if errors.Is(err, t4.ErrCompacted) {
					select {
					case sendCh <- &etcdserverpb.WatchResponse{
						Header:          s.header(),
						WatchId:         id,
						Created:         true,
						Canceled:        true,
						CancelReason:    "mvcc: required revision has been compacted",
						CompactRevision: toEtcdRevision(s.node.CompactRevision()),
					}:
					case <-ctx.Done():
						return nil
					}
				}
				continue
			}

			watches.Store(id, context.CancelFunc(cancel))

			select {
			case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Created: true}:
				go s.drainWatch(wctx, id, sub.progressNotify, sub.events, sub.match, sendCh)
			case <-ctx.Done():
				cancel()
				return nil
			}

		case *etcdserverpb.WatchRequest_CancelRequest:
			id := v.CancelRequest.WatchId
			if c, ok := watches.LoadAndDelete(id); ok {
				c.(context.CancelFunc)()
				select {
				case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Canceled: true}:
				case <-ctx.Done():
					return nil
				}
			}
		case *etcdserverpb.WatchRequest_ProgressRequest:
			watches.Range(func(k, _ any) bool {
				select {
				case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: k.(int64)}:
				case <-ctx.Done():
					return false
				}
				return true
			})
		}
	}
}

type watchSubscription struct {
	events         <-chan t4.Event
	match          func(string) bool
	progressNotify bool
}

// subscribeWatch subscribes to t4.Node.Watch synchronously. Subscribe errors
// (ErrCompacted, etc.) are returned to the caller before the watch is
// registered on the etcd stream.
func (s *Server) subscribeWatch(wctx context.Context, cr *etcdserverpb.WatchCreateRequest) (*watchSubscription, error) {
	scanPrefix, match := watchScan(cr)
	var watchOpts []t4.WatchOption
	if cr.PrevKv {
		watchOpts = append(watchOpts, t4.WithPrevKV())
	}
	events, err := s.node.Watch(wctx, scanPrefix, fromEtcdRevision(cr.StartRevision), watchOpts...)
	if err != nil {
		return nil, err
	}
	return &watchSubscription{
		events:         events,
		match:          match,
		progressNotify: cr.ProgressNotify,
	}, nil
}

// drainWatch reads events, coalesces them into a single WatchResponse per
// burst, and forwards through sendCh until wctx is done or events closes.
func (s *Server) drainWatch(wctx context.Context, watchID int64, progressNotify bool, events <-chan t4.Event, match func(string) bool, sendCh chan<- *etcdserverpb.WatchResponse) {
	var progressC <-chan time.Time
	if progressNotify {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		progressC = t.C
	}

	batch := make([]*mvccpb.Event, 0, watchMaxBatch)
	// batchMaxRev tracks the highest revision observed since the last flush.
	// progressRev is the rev we have actually delivered to the watcher so far.
	// WatchResponse Header.Revision must reflect events included in this frame,
	// not the live node clock — apiserver uses the header rev to advance its
	// watchCache, and if it leapfrogs past events that arrive in a later frame,
	// those events are silently dropped from the cache.
	var batchMaxRev, progressRev int64
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		resp := &etcdserverpb.WatchResponse{
			Header:  s.headerAt(batchMaxRev),
			WatchId: watchID,
			Events:  batch,
		}
		batch = make([]*mvccpb.Event, 0, watchMaxBatch)
		progressRev = batchMaxRev
		batchMaxRev = 0
		select {
		case sendCh <- resp:
			return true
		case <-wctx.Done():
			return false
		}
	}
	appendEvent := func(e t4.Event) {
		// Track every observed revision, even ones we filter out, so the
		// header rev reflects how far this watch has actually scanned.
		if e.KV != nil && e.KV.Revision > batchMaxRev {
			batchMaxRev = e.KV.Revision
		}
		if !match(e.KV.Key) {
			return
		}
		ev, ok := userEvent(e)
		if !ok {
			return
		}
		batch = append(batch, eventToProto(ev))
	}

	for {
		select {
		case e, ok := <-events:
			if !ok {
				flush()
				return
			}
			appendEvent(e)
			// Drain everything else already buffered so a burst from scanLog
			// ships in one frame.
		drain:
			for len(batch) < watchMaxBatch {
				select {
				case e2, ok2 := <-events:
					if !ok2 {
						flush()
						return
					}
					appendEvent(e2)
				default:
					break drain
				}
			}
			if !flush() {
				return
			}
		case <-progressC:
			if !flush() {
				return
			}
			// Pin the progress notification to the rev we have actually
			// delivered. Claiming a higher rev would let apiserver advance
			// its watchCache past undelivered events.
			select {
			case sendCh <- &etcdserverpb.WatchResponse{Header: s.headerAt(progressRev), WatchId: watchID}:
			case <-wctx.Done():
				return
			}
		case <-wctx.Done():
			return
		}
	}
}

func watchScan(cr *etcdserverpb.WatchCreateRequest) (string, func(string) bool) {
	key := string(cr.Key)
	end := string(cr.RangeEnd)
	if end == "" {
		return key, func(candidate string) bool { return candidate == key }
	}
	match := func(candidate string) bool {
		if end == "\x00" {
			return candidate >= key
		}
		return candidate >= key && candidate < end
	}
	if isPrefixRangeEnd(key, end) {
		return key, match
	}
	return "", match
}

func isPrefixRangeEnd(prefix, end string) bool {
	return prefixRangeEnd(prefix) == end
}

func prefixRangeEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "\x00"
}
