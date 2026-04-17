package t4

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/internal/wal"
)

// followLoop receives WAL entries from the leader and applies them locally.
// On ErrLeaderUnreachable it attempts a TakeOver election.
func (n *Node) followLoop(bgCtx context.Context) {
	lock := election.NewLock(n.cfg.ObjectStore, n.cfg.NodeID, n.cfg.AdvertisePeerAddr)
	cli := n.peerCli
	fromRev := n.db.Load().CurrentRevision() + 1

	for {
		err := cli.Follow(
			bgCtx,
			fromRev,
			func(entries []wal.Entry) error {
				// Followers must apply a contiguous revision stream. If the leader
				// stream skips (or rewinds) a revision, force a full resync rather
				// than silently advancing currentRev with holes.
				for i, e := range entries {
					if e.Revision != fromRev+int64(i) {
						return peer.ErrResyncRequired
					}
				}
				ptrs := make([]*wal.Entry, len(entries))
				for i := range entries {
					ptrs[i] = &entries[i]
				}
				if err := n.wal.AppendBatch(bgCtx, ptrs); err != nil {
					return err
				}
				return nil
			},
			func(entries []wal.Entry) error {
				if err := n.db.Load().Apply(entries); err != nil {
					return err
				}
				// Track the leader's term so attemptPromotion uses the correct
				// floorTerm when calling TakeOver.  Without this, n.term stays at
				// its Open() value and TakeOver backs off because it sees the
				// current lock term as "already taken over at a higher term".
				// The last entry in the batch has the highest-or-equal term.
				if last := entries[len(entries)-1]; last.Term > n.term {
					n.mu.Lock()
					if last.Term > n.term {
						n.term = last.Term
					}
					n.mu.Unlock()
				}
				// Advance only after a successful apply so a reconnect retries
				// from the start of the failed batch rather than skipping it.
				fromRev = entries[len(entries)-1].Revision + 1
				return nil
			},
		)

		if bgCtx.Err() != nil {
			return
		}

		if peer.IsResyncRequired(err) {
			metrics.FollowerResyncsTotal.WithLabelValues("stream_gap").Inc()
			if n.cfg.ObjectStore == nil {
				n.log.Errorf("t4: follower resync required but no object store — restart node")
				n.cancelBg()
				return
			}
			// Ring buffer miss: the follower has been offline long enough that
			// the leader's ring buffer no longer covers fromRev. Restore from
			// the latest S3 checkpoint (if the follower's Pebble is behind it),
			// then replay any remaining WAL entries from S3.
			n.log.Warnf("t4: follower resync required — restoring from checkpoint")
			if cpErr := n.resyncFromCheckpoint(bgCtx); cpErr != nil {
				n.log.Errorf("t4: follower in-place resync failed: %v — cancelling", cpErr)
				n.cancelBg()
				return
			}
			reCtx, reCancel := context.WithTimeout(bgCtx, 5*time.Minute)
			rerr := replayRemote(reCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().CurrentRevision(), n.log)
			reCancel()
			if rerr != nil {
				n.log.Errorf("t4: follower S3 resync failed: %v — retrying", rerr)
				select {
				case <-time.After(2 * time.Second):
				case <-bgCtx.Done():
					return
				}
			} else {
				fromRev = n.db.Load().CurrentRevision() + 1
				// Wake any goroutines blocked in WaitForRevision that entered
				// their wait loop while replayRemote was running. Recover does
				// not broadcast, so without this they would sleep until the
				// next live Apply — causing unnecessary read latency.
				n.db.Load().NotifyRevision()
				n.log.Infof("t4: follower resync complete (now at rev=%d)", n.db.Load().CurrentRevision())
			}
			continue
		}

		if peer.IsLeaderUnreachable(err) || peer.IsLeaderShutdown(err) {
			if peer.IsLeaderShutdown(err) {
				n.log.Infof("t4: leader shut down gracefully — attempting immediate election takeover")
			} else {
				n.log.Warnf("t4: leader unreachable — attempting election takeover")
			}
			newCli, promoted := n.attemptPromotion(bgCtx, lock, peer.IsLeaderShutdown(err))
			if promoted {
				return
			}
			if newCli != nil {
				oldCli := cli
				cli = newCli
				n.leaderCli.Store(newCli)
				oldCli.Close()
				n.log.Infof("t4: following new leader")
			}
			continue
		}

		n.log.Warnf("t4: follow loop error (will retry): %v", err)
		select {
		case <-time.After(2 * time.Second):
		case <-bgCtx.Done():
			return
		}
	}
}

// attemptPromotion tries to take over the leader lock after the stream dies.
// Returns (nil, true) if promoted to leader.
// Returns (newClient, false) if another node won; newClient follows that node.
// Returns (nil, false) on S3 errors or when the current leader's liveness
// record is fresh enough that TakeOver would risk a split-brain.
//
// graceful should be true when the leader sent an explicit shutdown signal:
// in that case the liveness check is skipped because the leader intentionally
// vacated and the fresh LastSeenNano would otherwise block all followers.
func (n *Node) attemptPromotion(bgCtx context.Context, lock *election.Lock, graceful bool) (*peer.Client, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Read the current lock before attempting TakeOver. If the leader recently
	// wrote a liveness touch (LastSeenNano is fresh) AND the shutdown was not
	// graceful, the leader may still have other followers and we are an
	// isolated minority node — back off to prevent split-brain.
	// On graceful shutdown we skip this check: the leader intentionally left,
	// so its fresh LastSeenNano must not block election.
	existing, err := lock.Read(ctx)
	if err != nil {
		n.log.Errorf("t4: takeover: read lock: %v", err)
		return nil, false
	}
	if !graceful && existing != nil && existing.LastSeenNano > 0 {
		age := time.Since(time.Unix(0, existing.LastSeenNano))
		if age < peer.LeaderLivenessTTL {
			n.log.Infof("t4: takeover: leader liveness is fresh (%v ago) — backing off to avoid split-brain", age.Round(time.Millisecond))
			// Back off from election, but do not keep retrying a stale endpoint.
			// If the lock advertises a leader address, switch followLoop to it.
			if existing.LeaderAddr != "" {
				return peer.NewClient(existing.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log), false
			}
			return nil, false
		}
	}
	// Revision fence: refuse to become leader if we are behind the last known
	// committed revision. A node missing entries would either drop them (data
	// loss) or fail to serve reads that clients already observed.
	// If the lock already points to a current leader, return their address so
	// followLoop switches to following them — connecting will trigger an
	// in-place resync that catches up this node's revision.
	if existing != nil && existing.CommittedRev > n.db.Load().CurrentRevision() {
		n.log.Infof("t4: takeover: node is behind leader committed rev (ours=%d, leader=%d) — following current leader to catch up",
			n.db.Load().CurrentRevision(), existing.CommittedRev)
		if existing.LeaderAddr != "" {
			return peer.NewClient(existing.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log), false
		}
		return nil, false
	}

	rec, won, err := lock.TakeOver(ctx, n.term, n.db.Load().CurrentRevision())
	if err != nil {
		n.log.Errorf("t4: takeover election error: %v", err)
		return nil, false
	}

	if won {
		if err := n.becomeLeader(bgCtx, lock, rec); err != nil {
			n.log.Errorf("t4: promotion failed: %v", err)
			return nil, false
		}
		// Start write-processing loops immediately so that client writes are
		// not blocked while we run Reconcile and the startup checkpoint below.
		// forceCheckpoint (and periodic maybeCheckpoint) already hold
		// fenceMu.Lock() during I/O, which briefly pauses new writes — that
		// is the same behaviour as during a normal checkpoint interval.
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.commitLoop(bgCtx) }()
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			n.bgWg.Add(1)
			go func() { defer n.bgWg.Done(); n.checkpointLoop(bgCtx) }()
		}
		// Upload any SSTs that exist on disk but aren't in S3 yet. The
		// follower didn't run SSTUploader.Start(), so its SSTs were never
		// streamed. Additionally, becomeLeader may have restored from a
		// checkpoint and replayed WAL, creating new SST files. Reconcile
		// ensures all of them are in S3 before the first checkpoint.
		if n.sstUploader != nil {
			rCtx, rCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			if rErr := n.sstUploader.Reconcile(rCtx); rErr != nil {
				n.log.Warnf("t4: promoted leader SST reconcile: %v", rErr)
			}
			rCancel()
			n.sstUploader.Start(bgCtx)
		}
		// Write a checkpoint immediately after Reconcile so that all
		// uploaded SSTs are referenced by a live checkpoint. Without this,
		// the old leader's GCOrphanSSTs could delete the just-uploaded SSTs
		// before the checkpointLoop gets a chance to write its startup
		// checkpoint. The checkpointLoop's own forceCheckpoint is redundant
		// but harmless (same rev → same checkpoint key → idempotent overwrite).
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			n.forceCheckpoint(bgCtx)
		}
		return nil, true
	}

	if rec != nil && rec.LeaderAddr != "" {
		n.log.Infof("t4: lost election to %s (term=%d) — following", rec.NodeID, rec.Term)
		return peer.NewClient(rec.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log), false
	}
	return nil, false
}

// forwardWrite sends a write request to the leader and decodes the response.
func (n *Node) forwardWrite(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	cli := n.leaderCli.Load()
	if cli == nil {
		return nil, ErrNotLeader
	}
	op := fwdOpLabel(req.Op)
	start := time.Now()
	resp, err := cli.ForwardWrite(ctx, req)
	metrics.ForwardedWritesTotal.WithLabelValues(op).Inc()
	metrics.ForwardDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return resp, err
}

func fwdOpLabel(op peer.ForwardOp) string {
	switch op {
	case peer.ForwardPut:
		return "put"
	case peer.ForwardCreate:
		return "create"
	case peer.ForwardUpdate:
		return "update"
	case peer.ForwardDeleteIfRevision:
		return "delete"
	case peer.ForwardCompact:
		return "compact"
	case peer.ForwardGetRevision:
		return "get_revision"
	case peer.ForwardTxn:
		return "txn"
	default:
		return "unknown"
	}
}

// resyncFromCheckpoint is called from followLoop when IsResyncRequired fires.
// It uses restoreDBIfBehindCheckpoint to close the WAL gap, then (if a restore
// was actually performed) replaces the local WAL so subsequent Appends from the
// live stream start at the correct revision.
func (n *Node) resyncFromCheckpoint(bgCtx context.Context) error {
	ctx, cancel := context.WithTimeout(bgCtx, 5*time.Minute)
	defer cancel()

	restored, err := n.restoreDBIfBehindCheckpoint(ctx)
	if err != nil {
		return err
	}
	if !restored {
		return nil
	}

	// ── Phase 3: replace WAL and update node metadata ────────────────────────
	// followLoop is the sole WAL writer for a follower, so no concurrent
	// Append calls can race with this replacement.
	walDir := filepath.Join(n.cfg.DataDir, "wal")
	newRev := n.db.Load().CurrentRevision()
	_ = n.wal.Close()
	if rerr := os.RemoveAll(walDir); rerr != nil {
		n.log.Warnf("t4: remove old wal dir during resync: %v", rerr)
	}
	newWal := wal.New(
		wal.WithSegmentMaxSize(n.cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(n.cfg.SegmentMaxAge),
		wal.WithLogger(n.log),
	)
	if rerr := newWal.Open(walDir, n.term, newRev+1); rerr != nil {
		return fmt.Errorf("open new wal after resync: %w", rerr)
	}
	newWal.Start(bgCtx)

	n.mu.Lock()
	n.wal = newWal
	n.nextRev = newRev
	n.mu.Unlock()

	n.log.Infof("t4: follower in-place resync complete (rev=%d term=%d)", newRev, n.term)
	return nil
}
