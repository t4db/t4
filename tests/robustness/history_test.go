package robustness

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/robustness/model"
	"go.etcd.io/etcd/tests/v3/robustness/report"
	"go.uber.org/zap"
)

// Upstream validation replays the requests etcd persisted, read from its raft
// WAL, to check watches and stale reads and to decide which failed writes took
// effect. T4 has no raft WAL to read, so the history is rebuilt from a watch
// over the whole keyspace: every revision becomes a transaction of the puts
// and deletes it holds, with the lease grants and revokes the model needs to
// attach and detach keys. Failed transactions and reads change nothing and
// are not needed.

// collectHistory watches every key from the first revision until it has seen
// maxRev, which arrives on maxRevCh once traffic has stopped. seen holds the
// revision it can resume from: compacting past it would lose the history.
func collectHistory(ctx context.Context, endpoints []string, maxRevCh <-chan int64, seen *atomic.Int64) ([]*clientv3.Event, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var events []*clientv3.Event
	// lastRev is the newest revision seen. A revision can arrive split over
	// several responses, so a resumed watch starts over at lastRev.
	lastRev := int64(1)
	maxRev := int64(-1)
	for {
		for len(events) > 0 && events[len(events)-1].Kv.ModRevision == lastRev {
			events = events[:len(events)-1]
		}
		wctx, cancel := context.WithCancel(ctx)
		wch := c.Watch(wctx, "", clientv3.WithPrefix(), clientv3.WithRev(lastRev), clientv3.WithPrevKV(), clientv3.WithProgressNotify())
		done, err := func() (bool, error) {
			defer cancel()
			for {
				if maxRev >= 0 && lastRev >= maxRev {
					return true, nil
				}
				select {
				case <-ctx.Done():
					return false, ctx.Err()
				case rev, ok := <-maxRevCh:
					if !ok {
						return false, fmt.Errorf("traffic ended without reporting its max revision")
					}
					maxRev = rev
					maxRevCh = nil
				case <-time.After(100 * time.Millisecond):
					_ = c.RequestProgress(wctx)
				case resp, ok := <-wch:
					if !ok {
						return false, nil
					}
					if err := resp.Err(); err != nil {
						if resp.CompactRevision != 0 {
							return false, fmt.Errorf("history watch compacted at %d: %w", resp.CompactRevision, err)
						}
						return false, nil
					}
					for _, ev := range resp.Events {
						if ev.Kv.ModRevision < lastRev {
							return false, fmt.Errorf("history watch went back from revision %d to %d", lastRev, ev.Kv.ModRevision)
						}
						events = append(events, ev)
					}
					if n := len(resp.Events); n > 0 {
						lastRev = resp.Events[n-1].Kv.ModRevision
						seen.Store(lastRev)
					}
				}
			}
		}()
		if err != nil {
			return nil, err
		}
		if done {
			return events, nil
		}
	}
}

// persistedRequests rebuilds the requests that produced events. A revision
// that deletes exactly the keys of a lease some client revoked, which no later
// put uses, is that revoke; any other revision is a transaction. The
// compactions that took effect follow, see persistedCompactions.
func persistedRequests(events []*clientv3.Event, reports []report.ClientReport, compacted int64) []model.EtcdRequest {
	revoked := map[int64]bool{}
	for _, r := range reports {
		for _, op := range r.KeyValue {
			req := op.Input.(model.EtcdRequest)
			if req.Type == model.LeaseRevoke {
				revoked[req.LeaseRevoke.LeaseID] = true
			}
		}
	}
	lastUse := map[int64]int64{}
	for _, ev := range events {
		if ev.Type == mvccpb.PUT && ev.Kv.Lease != 0 {
			lastUse[ev.Kv.Lease] = ev.Kv.ModRevision
		}
	}

	var requests []model.EtcdRequest
	granted := map[int64]bool{}
	keyLease := map[string]int64{}
	for i := 0; i < len(events); {
		rev := events[i].Kv.ModRevision
		j := i
		for j < len(events) && events[j].Kv.ModRevision == rev {
			j++
		}
		group := events[i:j]
		i = j

		if lease, ok := revokedLease(group, keyLease, revoked); ok && lastUse[lease] < rev {
			requests = append(requests, model.EtcdRequest{
				Type:        model.LeaseRevoke,
				LeaseRevoke: &model.LeaseRevokeRequest{LeaseID: lease},
			})
			delete(granted, lease)
			for _, ev := range group {
				delete(keyLease, string(ev.Kv.Key))
			}
			continue
		}

		txn := &model.TxnRequest{}
		for _, ev := range group {
			key := string(ev.Kv.Key)
			if ev.Type == mvccpb.DELETE {
				txn.OperationsOnSuccess = append(txn.OperationsOnSuccess, model.EtcdOperation{
					Type:   model.DeleteOperation,
					Delete: model.DeleteOptions{Key: key},
				})
				delete(keyLease, key)
				continue
			}
			if lease := ev.Kv.Lease; lease != 0 && !granted[lease] {
				requests = append(requests, model.EtcdRequest{
					Type:       model.LeaseGrant,
					LeaseGrant: &model.LeaseGrantRequest{LeaseID: lease},
				})
				granted[lease] = true
			}
			txn.OperationsOnSuccess = append(txn.OperationsOnSuccess, model.EtcdOperation{
				Type: model.PutOperation,
				Put: model.PutOptions{
					Key:     key,
					Value:   model.ToValueOrHash(string(ev.Kv.Value)),
					LeaseID: ev.Kv.Lease,
				},
			})
			if ev.Kv.Lease != 0 {
				keyLease[key] = ev.Kv.Lease
			} else {
				delete(keyLease, key)
			}
		}
		requests = append(requests, model.EtcdRequest{Type: model.Txn, Txn: txn})
	}
	return append(requests, persistedCompactions(reports, compacted)...)
}

// persistedCompactions returns the compactions to add to the rebuilt
// history: only one whose outcome is unknown, if it took effect.
//
// A compaction that succeeded is left out. It is a known operation, which
// linearization places by itself, and adding it would do harm: validation
// gives an operation the return time of the persisted requests after it, so
// it would have to sit exactly where it took effect among the revisions, and
// a compaction takes no revision a client can learn. Nothing else needs it:
// replay checks a read at a past revision against the state at that
// revision, which a compaction does not change, and skips reads that failed.
//
// A compaction refused as compacted or as a future revision did not take
// effect. One that failed otherwise may have; runCompactLoop makes no
// compaction after it, so it took effect if and only if it does not exceed
// compacted, the compaction revision at the end. Its return time is unknown,
// so it can go last without giving anything an earlier one.
func persistedCompactions(reports []report.ClientReport, compacted int64) []model.EtcdRequest {
	var requests []model.EtcdRequest
	for _, r := range reports {
		for _, op := range r.KeyValue {
			req := op.Input.(model.EtcdRequest)
			if req.Type != model.Compact {
				continue
			}
			resp := op.Output.(model.MaybeEtcdResponse)
			if resp.Error != "" && req.Compact.Revision <= compacted {
				requests = append(requests, req)
			}
		}
	}
	return requests
}

// revokedLease reports whether group deletes exactly the keys attached to one
// lease that a client revoked, and which lease.
func revokedLease(group []*clientv3.Event, keyLease map[string]int64, revoked map[int64]bool) (int64, bool) {
	var lease int64
	for _, ev := range group {
		if ev.Type != mvccpb.DELETE {
			return 0, false
		}
		l := keyLease[string(ev.Kv.Key)]
		if l == 0 || (lease != 0 && l != lease) {
			return 0, false
		}
		lease = l
	}
	if lease == 0 || !revoked[lease] {
		return 0, false
	}
	attached := 0
	for _, l := range keyLease {
		if l == lease {
			attached++
		}
	}
	return lease, attached == len(group)
}
