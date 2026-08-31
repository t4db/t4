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

// Maximum approximate proto size of a single WatchResponse fragment. When a
// client opts in to fragmentation (WatchCreateRequest.Fragment=true) and a
// flush would exceed this, the batch is split into multiple WatchResponses
// — all but the last carry Fragment=true. Picked well below clientv3's
// default 4 MiB max-recv so clients with smaller limits also fit. Not part
// of the v1 contract; the only requirement is that fragments be small
// enough for the client.
//
// Declared as var (not const) so unit tests can shrink the budget to exercise
// the fragment branch without generating multi-MB payloads.
var watchFragmentBytes = 1 << 20 // 1 MiB

// Watch implements WatchServer.Watch (bidirectional streaming).
//
// One stream multiplexes many watches. gRPC requires that Send on a stream is
// not invoked concurrently, so all responses funnel through sendCh. Each
// watch runs in its own goroutine that drains events from the underlying
// t4.Node.Watch channel into batched WatchResponses after the watch creation
// response has been queued.
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()

	// sendCh carries *runs* of WatchResponses. A run is a slice of frames
	// that must be sent contiguously over the gRPC stream — typically one
	// frame, but a fragmented event flush is multiple frames sharing a
	// WatchId and Header.Revision. The sender drains a whole run before
	// reading the next; this prevents another watch's response from being
	// interleaved between two fragments of the same logical batch.
	sendCh := make(chan []*etcdserverpb.WatchResponse, 128)
	go func() {
		for {
			select {
			case run := <-sendCh:
				for _, resp := range run {
					_ = stream.Send(resp)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var watches sync.Map // map[int64]*watchSubscription
	var nextID int64 = 1

	defer func() {
		watches.Range(func(_, v any) bool {
			v.(*watchSubscription).cancel()
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
				case sendCh <- []*etcdserverpb.WatchResponse{{
					Header:       s.header(),
					WatchId:      -1,
					Canceled:     true,
					CancelReason: "reserved internal prefix is not watchable",
				}}:
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
					case sendCh <- []*etcdserverpb.WatchResponse{{
						Header:          s.header(),
						WatchId:         id,
						Created:         true,
						Canceled:        true,
						CancelReason:    "mvcc: required revision has been compacted",
						CompactRevision: toEtcdRevision(s.node.CompactRevision()),
					}}:
					case <-ctx.Done():
						return nil
					}
				}
				continue
			}

			sub.cancel = cancel
			watches.Store(id, sub)

			select {
			case sendCh <- []*etcdserverpb.WatchResponse{{Header: s.header(), WatchId: id, Created: true}}:
				go s.drainWatch(wctx, id, sub, sendCh)
			case <-ctx.Done():
				cancel()
				return nil
			}

		case *etcdserverpb.WatchRequest_CancelRequest:
			id := v.CancelRequest.WatchId
			if w, ok := watches.LoadAndDelete(id); ok {
				w.(*watchSubscription).cancel()
				select {
				case sendCh <- []*etcdserverpb.WatchResponse{{Header: s.header(), WatchId: id, Canceled: true}}:
				case <-ctx.Done():
					return nil
				}
			}
		case *etcdserverpb.WatchRequest_ProgressRequest:
			// Ask each drain to emit the notification rather than answering
			// from here. The revision reported must be the one that watch has
			// actually delivered, which only its drain goroutine knows;
			// answering with the live node revision would let apiserver
			// advance its watchCache past events still queued for delivery.
			// This is the same invariant the periodic progress ticker holds,
			// and the guarantee kube-apiserver's consistent-read-from-cache
			// path is built on: it asks for progress, then serves LISTs from
			// cache at the revision we report.
			watches.Range(func(_, v any) bool {
				v.(*watchSubscription).requestProgress()
				return true
			})
		}
	}
}

type watchSubscription struct {
	events         <-chan t4.Event
	match          func(string) bool
	progressNotify bool
	fragment       bool

	// cancel tears down the drain goroutine and the upstream Node.Watch.
	// Set by Watch once the subscription is registered on the stream.
	cancel context.CancelFunc

	// progressReq carries on-demand progress requests from the stream's
	// receive loop to the drain goroutine. Depth 1: a request already
	// pending covers any concurrent one, so requestProgress never blocks
	// the receive loop behind a slow watch.
	progressReq chan struct{}

	// startRev is the revision this watch is caught up through before any
	// event is delivered — the seed for the drain's progress accounting.
	startRev int64
}

func (w *watchSubscription) requestProgress() {
	select {
	case w.progressReq <- struct{}{}:
	default:
	}
}

// subscribeWatch subscribes to t4.Node.Watch synchronously. Subscribe errors
// (ErrCompacted, etc.) are returned to the caller before the watch is
// registered on the etcd stream.
func (s *Server) subscribeWatch(wctx context.Context, cr *etcdserverpb.WatchCreateRequest) (*watchSubscription, error) {
	scanPrefix, match := watchScan(cr)

	// Sample the revision this watch starts caught up through *before*
	// subscribing. A replay watch (StartRevision > 0) has delivered nothing
	// below its start point; a live watch begins after the current revision.
	// Sampling first means a commit racing the subscribe is under-reported
	// rather than claimed as delivered — the safe direction.
	startRev := fromEtcdRevision(cr.StartRevision)
	lastRev := startRev - 1
	if startRev == 0 {
		lastRev = s.node.CurrentRevision()
	}

	var watchOpts []t4.WatchOption
	if cr.PrevKv {
		watchOpts = append(watchOpts, t4.WithPrevKV())
	}
	events, err := s.node.Watch(wctx, scanPrefix, startRev, watchOpts...)
	if err != nil {
		return nil, err
	}
	return &watchSubscription{
		events:         events,
		match:          match,
		progressNotify: cr.ProgressNotify,
		fragment:       cr.Fragment,
		progressReq:    make(chan struct{}, 1),
		startRev:       lastRev,
	}, nil
}

// sendEvents emits a batch of events as one or more WatchResponse frames
// through sendCh.
//
// When fragment is false or the batch fits in a single frame, one frame is
// sent. When fragment is true and the estimated proto size exceeds
// watchFragmentBytes, the batch is greedily partitioned into fragments;
// every fragment shares Header.Revision = rev and WatchId, and all but the
// last carry Fragment=true. clientv3 buffers the run until Fragment=false
// and then surfaces one combined WatchResponse.
//
// Returns false (and the caller must exit drainWatch) when sendOrCancelSlow
// times out on any fragment.
func (s *Server) sendEvents(wctx context.Context, sendCh chan<- []*etcdserverpb.WatchResponse, watchID int64, batch []*mvccpb.Event, rev int64, fragment bool) bool {
	header := s.headerAt(rev)
	if !fragment || estimateEventsSize(batch) <= watchFragmentBytes {
		return s.sendOrCancelSlow(wctx, sendCh, []*etcdserverpb.WatchResponse{{
			Header:  header,
			WatchId: watchID,
			Events:  batch,
		}}, watchID)
	}
	chunks := splitEventsBySize(batch, watchFragmentBytes)
	run := make([]*etcdserverpb.WatchResponse, len(chunks))
	for i, chunk := range chunks {
		run[i] = &etcdserverpb.WatchResponse{
			Header:   header,
			WatchId:  watchID,
			Events:   chunk,
			Fragment: i < len(chunks)-1,
		}
	}
	return s.sendOrCancelSlow(wctx, sendCh, run, watchID)
}

// estimateEventSize returns a cheap upper-bound estimate of the proto-encoded
// size of one mvccpb.Event. The exact size depends on protobuf framing, but
// for a fragmentation budget the dominant terms (key + value bytes) are
// enough — a constant-per-event overhead absorbs framing.
func estimateEventSize(e *mvccpb.Event) int {
	if e == nil {
		return 0
	}
	const perEventOverhead = 32 // type tag + length prefixes + framing
	const perKVOverhead = 48    // KeyValue struct's tagged fields
	size := perEventOverhead
	if e.Kv != nil {
		size += perKVOverhead + len(e.Kv.Key) + len(e.Kv.Value)
	}
	if e.PrevKv != nil {
		size += perKVOverhead + len(e.PrevKv.Key) + len(e.PrevKv.Value)
	}
	return size
}

func estimateEventsSize(events []*mvccpb.Event) int {
	total := 0
	for _, e := range events {
		total += estimateEventSize(e)
	}
	return total
}

// splitEventsBySize greedy-partitions events into chunks of at most maxBytes
// estimated size. A single event larger than maxBytes is its own chunk
// (sending it alone is the best we can do — gRPC max-send is large enough to
// hold any one event under the request-size limit).
func splitEventsBySize(events []*mvccpb.Event, maxBytes int) [][]*mvccpb.Event {
	var chunks [][]*mvccpb.Event
	var current []*mvccpb.Event
	currentSize := 0
	for _, e := range events {
		size := estimateEventSize(e)
		if currentSize > 0 && currentSize+size > maxBytes {
			chunks = append(chunks, current)
			current = nil
			currentSize = 0
		}
		current = append(current, e)
		currentSize += size
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

// sendOrCancelSlow tries to enqueue resp on sendCh. It returns true on
// success; on wctx cancellation it returns false; if the send blocks longer
// than the configured WatchSendTimeout the watcher is treated as slow:
//   - A `Canceled=true, CancelReason="mvcc: watcher is slow"` response is
//     pushed to sendCh, best-effort within a second WatchSendTimeout window so
//     it has a chance to land once the client (eventually) drains a slot. The
//     cancel response is then "lost" only if buffers stay stuck for the full
//     window.
//   - false is returned. The caller MUST exit the per-watch goroutine.
func (s *Server) sendOrCancelSlow(wctx context.Context, sendCh chan<- []*etcdserverpb.WatchResponse, run []*etcdserverpb.WatchResponse, watchID int64) bool {
	timeout := s.node.WatchSendTimeout()
	if timeout <= 0 {
		select {
		case sendCh <- run:
			return true
		case <-wctx.Done():
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case sendCh <- run:
		return true
	case <-wctx.Done():
		return false
	case <-timer.C:
		cancelRun := []*etcdserverpb.WatchResponse{{
			Header:       s.header(),
			WatchId:      watchID,
			Canceled:     true,
			CancelReason: "mvcc: watcher is slow",
		}}
		deliveryTimer := time.NewTimer(timeout)
		defer deliveryTimer.Stop()
		select {
		case sendCh <- cancelRun:
		case <-deliveryTimer.C:
		case <-wctx.Done():
		}
		return false
	}
}

// drainWatch reads events, coalesces them into a single WatchResponse per
// burst, and forwards through sendCh until wctx is done or events closes.
//
// drainWatch calls sub.cancel on exit so the upstream Node.Watch goroutine
// (sitting on a blocked channel send) is released along with this drain.
//
// sub.fragment mirrors WatchCreateRequest.Fragment: when true and a flush
// would exceed watchFragmentBytes, the batch is split into multiple
// WatchResponses sharing the same Header.Revision; all but the last carry
// Fragment=true.
func (s *Server) drainWatch(wctx context.Context, watchID int64, sub *watchSubscription, sendCh chan<- []*etcdserverpb.WatchResponse) {
	defer sub.cancel()
	events, match, fragment := sub.events, sub.match, sub.fragment
	var progressC <-chan time.Time
	if sub.progressNotify {
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
	// those events are silently dropped from the cache. Seeded from the
	// subscription's start point for the same reason: a replay watch has not
	// yet delivered the history between its start revision and the live clock.
	var batchMaxRev int64
	progressRev := sub.startRev
	flush := func() bool {
		if len(batch) == 0 {
			if batchMaxRev > progressRev {
				progressRev = batchMaxRev
			}
			batchMaxRev = 0
			return true
		}
		toSend := batch
		rev := batchMaxRev
		batch = make([]*mvccpb.Event, 0, watchMaxBatch)
		progressRev = rev
		batchMaxRev = 0
		return s.sendEvents(wctx, sendCh, watchID, toSend, rev, fragment)
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
	// sendProgress flushes any pending batch, then reports the revision this
	// watch has actually delivered. Claiming a higher rev — the live node
	// clock, say — would let apiserver advance its watchCache past events
	// still queued here and silently drop them.
	sendProgress := func() bool {
		if !flush() {
			return false
		}
		return s.sendOrCancelSlow(wctx, sendCh, []*etcdserverpb.WatchResponse{{Header: s.headerAt(progressRev), WatchId: watchID}}, watchID)
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
			if !sendProgress() {
				return
			}
		case <-sub.progressReq:
			if !sendProgress() {
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
