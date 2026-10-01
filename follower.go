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
	// fromRev is the next WAL/peer-stream sequence to request. The peer
	// protocol's FollowRequest.FromRevision is a sequence number (after the
	// seq/rev split, Compact entries consume sequences but not revisions),
	// so seed from LastSequence rather than CurrentRevision.
	fromRev := n.db.Load().LastSequence() + 1

	for {
		cli.SetLeaderGoneCheck(func(ctx context.Context) bool { return n.lockReleased(ctx, lock) })
		err := cli.Follow(
			bgCtx,
			fromRev,
			func(entries []wal.Entry) error {
				// Followers must apply a contiguous revision stream. If the leader
				// stream skips (or rewinds) a revision, force a full resync rather
				// than silently advancing currentRev with holes.
				for i, e := range entries {
					if e.Sequence() != fromRev+int64(i) {
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
				n.maybeRecordRevisionSample(entries)
				// Track the leader's term so attemptPromotion uses the correct
				// floorTerm when calling TakeOver.  Without this, n.term stays at
				// its Open() value and TakeOver backs off because it sees the
				// current lock term as "already taken over at a higher term".
				// The last entry in the batch has the highest-or-equal term.
				n.observeTerm(entries[len(entries)-1].Term)
				// Advance only after a successful apply so a reconnect retries
				// from the start of the failed batch rather than skipping it.
				fromRev = entries[len(entries)-1].Sequence() + 1
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
			rerr := replayRemote(reCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().LastSequence(), n.log)
			reCancel()
			if rerr != nil {
				n.log.Errorf("t4: follower S3 resync failed: %v — retrying", rerr)
				select {
				case <-time.After(2 * time.Second):
				case <-bgCtx.Done():
					return
				}
			} else {
				fromRev = n.db.Load().LastSequence() + 1
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

// lockReleased reports whether the leader lock has been released by a leader
// that shut down gracefully (see election.Lock.Relinquish). A follower that
// missed the shutdown broadcast learns this way that the leader is gone. A
// live leader's lock always carries LastSeenNano, so this never lets a node
// skip the liveness wait against a leader that is still serving.
func (n *Node) lockReleased(ctx context.Context, lock *election.Lock) bool {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rec, err := lock.Read(rctx)
	return err == nil && rec != nil && rec.Released()
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

	// Unless the leader left gracefully, it may only be presumed gone under
	// the rules of mayTakeOver (lease.go), which depend on whether and when
	// this node last heard it. TakeOver re-applies them to the record it
	// replaces; checking here first avoids a pointless catch-up.
	var heardTerm uint64
	var heardAt time.Time
	if cli := n.leaderCli.Load(); cli != nil {
		heardTerm, heardAt = cli.LastHeard()
	}
	var allow func(*election.LockRecord) bool
	if !graceful {
		allow = func(rec *election.LockRecord) bool {
			return mayTakeOver(rec, heardTerm, heardAt, time.Now())
		}
	}
	existing, err := lock.Read(ctx)
	if err != nil {
		n.log.Errorf("t4: takeover: read lock: %v", err)
		return nil, false
	}
	if allow != nil && existing != nil && existing.NodeID != n.cfg.NodeID && !allow(existing) {
		n.log.Infof("t4: takeover: leader may still be serving (renewed %v ago, valid until %v) — backing off to avoid split-brain",
			time.Since(existing.Renewed()).Round(time.Millisecond), existing.ValidUntil().Format(time.RFC3339))
		// Back off from election, but do not keep retrying a stale endpoint.
		// If the lock advertises a leader address, switch followLoop to it.
		if existing.LeaderAddr != "" {
			n.observeTerm(existing.Term)
			cli := peer.NewClient(existing.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log, n.cfg.TracerProvider)
			// Still the leader this node knows: keep counting from when it
			// last heard it, or a dead leader would look unknown and
			// failover would wait out its full ValidUntil.
			if existing.Term == heardTerm {
				cli.SetLastHeard(heardTerm, heardAt)
			}
			return cli, false
		}
		return nil, false
	}
	// Election fence: refuse to become leader while the lock's fence blocks
	// this node. A node missing entries would either drop them (data loss) or
	// fail to serve reads that clients already observed.
	//
	// The node is judged by its leader-known position: what it applied from a
	// leader, not what it restored from object storage. Object storage can hold
	// more than the fence covers and less than the leader acknowledged, so
	// catching up from it could carry this node past the fence while it still
	// lacks writes another node holds (docs/design/takeover-fence.md).
	candidate := n.leaderKnownFence()
	if existing != nil && existing.NodeID == n.cfg.NodeID {
		// Its own lock: every entry it holds, it wrote or received as a
		// member of this cluster.
		candidate = n.committedFence()
	}
	if existing != nil && existing.NodeID != n.cfg.NodeID && existing.Blocks(candidate) {
		// Catching up from object storage (checkpoint, then WAL) and taking
		// over is safe only while it holds every acknowledged write: the
		// leader released the lock after uploading its WAL, or recorded that
		// it uploaded every write before acknowledging it.
		complete := existing.Released() || existing.ObjectStoreComplete
		if complete && n.cfg.ObjectStore != nil {
			n.log.Infof("t4: takeover: catching up from object storage before takeover (ours=%+v, leader=%+v)",
				candidate, existing.Fence())
			// WAL segments covered by the latest checkpoint may already be
			// garbage-collected: restore the checkpoint first if this node is
			// behind it, as the follow loop's resync does.
			if err := n.resyncFromCheckpoint(bgCtx); err != nil {
				n.log.Errorf("t4: takeover catch-up checkpoint restore: %v — will retry", err)
				return nil, false
			}
			catchupCtx, catchupCancel := context.WithTimeout(bgCtx, 2*time.Minute)
			rerr := replayRemote(catchupCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().LastSequence(), n.log)
			catchupCancel()
			if rerr != nil {
				n.log.Errorf("t4: takeover catch-up replay: %v — will retry", rerr)
				return nil, false
			}
			n.db.Load().NotifyRevision()
			candidate = n.committedFence()
		}
		if existing.Blocks(candidate) {
			if graceful {
				n.log.Warnf("t4: takeover: still behind the fence (ours=%+v, leader=%+v) — will retry once the leader has released the lock",
					candidate, existing.Fence())
				return nil, false
			}
			// Acknowledged writes may exist only on nodes this one cannot see:
			// the leader, if it is alive after all, or followers that heard
			// it. Follow the leader; connecting triggers an in-place resync of
			// this node. Otherwise wait for one of them to take over.
			if complete {
				n.log.Infof("t4: takeover: still behind the fence after catch-up (ours=%+v, leader=%+v) — following current leader",
					candidate, existing.Fence())
			} else {
				n.log.Infof("t4: takeover: behind the fence and object storage may lack acknowledged writes (ours=%+v, leader=%+v) — waiting for a node that has them",
					candidate, existing.Fence())
			}
			if existing.LeaderAddr != "" {
				n.observeTerm(existing.Term)
				return peer.NewClient(existing.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log, n.cfg.TracerProvider), false
			}
			return nil, false
		}
		// Caught up: fall through to TakeOver.
	}

	takeoverStart := time.Now()
	rec, won, err := lock.TakeOver(ctx, n.currentTerm(), candidate, allow)
	if err != nil {
		n.log.Errorf("t4: takeover election error: %v", err)
		return nil, false
	}

	if won {
		if err := n.becomeLeader(bgCtx, lock, rec, takeoverStart); err != nil {
			n.log.Errorf("t4: promotion failed: %v", err)
			return nil, false
		}
		// Start write-processing loops immediately so that client writes are
		// not blocked while we run Reconcile and the startup checkpoint below.
		// Checkpoints hold fenceMu.Lock() only for local I/O (WAL seal, Pebble
		// flush and checkpoint copy); the object-store upload runs unfenced.
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.commitLoop(bgCtx) }()
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			n.bgWg.Add(1)
			go func() { defer n.bgWg.Done(); n.checkpointLoop(bgCtx) }()
		}
		if n.autoCompactEnabled() {
			n.bgWg.Add(1)
			go func() { defer n.bgWg.Done(); n.autoCompactLoop(bgCtx) }()
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
		// checkpoint, which may have run before Reconcile finished.
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			n.forceCheckpoint(bgCtx)
		}
		return nil, true
	}

	if rec != nil && rec.LeaderAddr != "" {
		n.observeTerm(rec.Term)
		n.log.Infof("t4: lost election to %s (term=%d) — following", rec.NodeID, rec.Term)
		return peer.NewClient(rec.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log, n.cfg.TracerProvider), false
	}
	return nil, false
}

// forwardWrite sends a write request to the leader and decodes the response.
func (n *Node) forwardWrite(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	cli := n.leaderCli.Load()
	if cli == nil {
		return nil, ErrNoLeader
	}
	op := fwdOpLabel(req.Op)
	start := time.Now()
	resp, err := cli.ForwardWrite(ctx, req)
	metrics.ForwardedWritesTotal.WithLabelValues(op).Inc()
	metrics.ForwardDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if err != nil && isLeaderUnavailable(err) {
		return nil, ErrNoLeader
	}
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
	newSeq := n.db.Load().LastSequence()
	_ = n.wal.Close()
	if rerr := os.RemoveAll(walDir); rerr != nil {
		n.log.Warnf("t4: remove old wal dir during resync: %v", rerr)
	}
	newWal := wal.New(
		wal.WithSegmentMaxSize(n.cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(n.cfg.SegmentMaxAge),
		wal.WithLogger(n.log),
	)
	if rerr := newWal.Open(walDir, n.term, newSeq+1); rerr != nil {
		return fmt.Errorf("open new wal after resync: %w", rerr)
	}
	newWal.Start(bgCtx)

	n.mu.Lock()
	n.wal = newWal
	n.nextRev = newRev
	n.nextSeq = newSeq
	n.mu.Unlock()

	n.log.Infof("t4: follower in-place resync complete (rev=%d seq=%d term=%d)", newRev, newSeq, n.term)
	return nil
}
