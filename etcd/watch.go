package etcd

import (
	"context"
	"errors"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/t4db/t4"
)

// Watch implements WatchServer.Watch (bidirectional streaming).
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()

	// All sends are serialized through sendCh to avoid concurrent SendMsg calls.
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

	type entry struct{ cancel context.CancelFunc }
	var watchesMu sync.Mutex
	watches := map[int64]entry{}
	var nextID int64 = 1
	removeWatch := func(id int64) (context.CancelFunc, bool) {
		watchesMu.Lock()
		w, ok := watches[id]
		if ok {
			delete(watches, id)
		}
		watchesMu.Unlock()
		if !ok {
			return nil, false
		}
		return w.cancel, true
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			break
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
					goto done
				}
				continue
			}
			id := nextID
			nextID++

			wctx, cancel := context.WithCancel(ctx)
			watchesMu.Lock()
			watches[id] = entry{cancel}
			watchesMu.Unlock()

			// Confirm the watch was created.
			select {
			case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Created: true}:
			case <-ctx.Done():
				cancel()
				goto done
			}

			go func(watchID int64, startRev int64) {
				events, err := s.node.Watch(wctx, string(cr.Key), fromEtcdRevision(startRev))
				if errors.Is(err, t4.ErrCompacted) {
					// Remove the watch first, but do not cancel wctx before sending
					// the compacted response: that races the select below and can
					// drop the required canceled notification.
					_, _ = removeWatch(watchID)
					select {
					case sendCh <- &etcdserverpb.WatchResponse{
						Header:          s.header(),
						WatchId:         watchID,
						Canceled:        true,
						CancelReason:    "mvcc: required revision has been compacted",
						CompactRevision: toEtcdRevision(s.node.CompactRevision()),
					}:
					case <-ctx.Done():
					}
					return
				}
				if err != nil {
					return
				}
				for e := range events {
					e, ok := userEvent(e)
					if !ok {
						continue
					}
					resp := &etcdserverpb.WatchResponse{
						Header:  s.header(),
						WatchId: watchID,
						Events:  []*mvccpb.Event{eventToProto(e)},
					}
					select {
					case sendCh <- resp:
					case <-wctx.Done():
						return
					}
				}
			}(id, cr.StartRevision)

		case *etcdserverpb.WatchRequest_CancelRequest:
			id := v.CancelRequest.WatchId
			if cancel, ok := removeWatch(id); ok {
				cancel()
				select {
				case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Canceled: true}:
				case <-ctx.Done():
					goto done
				}
			}
		}
	}

done:
	watchesMu.Lock()
	all := make([]entry, 0, len(watches))
	for _, w := range watches {
		all = append(all, w)
	}
	watches = map[int64]entry{}
	watchesMu.Unlock()
	for _, w := range all {
		w.cancel()
	}
	return nil
}
