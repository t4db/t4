package etcd

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/makhov/strata"
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
	watches := map[int64]entry{}
	var nextID int64 = 1

	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}

		switch v := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			cr := v.CreateRequest
			id := nextID
			nextID++

			wctx, cancel := context.WithCancel(ctx)
			watches[id] = entry{cancel}

			// Confirm the watch was created.
			select {
			case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Created: true}:
			case <-ctx.Done():
				cancel()
				goto done
			}

			// node.Watch uses "last seen revision" semantics (delivers from startRev+1).
			// The etcd protocol uses "first desired revision" semantics (0 = current, N = from N inclusive).
			storeStartRev := s.node.CurrentRevision()
			if cr.StartRevision > 0 {
				storeStartRev = cr.StartRevision - 1
			}

			go func(watchID int64, startRev int64) {
				events, err := s.node.Watch(wctx, string(cr.Key), startRev)
				if errors.Is(err, strata.ErrCompacted) {
					select {
					case sendCh <- &etcdserverpb.WatchResponse{
						Header:          s.header(),
						WatchId:         watchID,
						Canceled:        true,
						CancelReason:    "mvcc: required revision has been compacted",
						CompactRevision: s.node.CompactRevision(),
					}:
					case <-wctx.Done():
					}
					return
				}
				if err != nil {
					return
				}
				for e := range events {
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
			}(id, storeStartRev)

		case *etcdserverpb.WatchRequest_CancelRequest:
			id := v.CancelRequest.WatchId
			if w, ok := watches[id]; ok {
				w.cancel()
				delete(watches, id)
				select {
				case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Canceled: true}:
				case <-ctx.Done():
					goto done
				}
			}
		}
	}

done:
	for _, w := range watches {
		w.cancel()
	}
	return nil
}
