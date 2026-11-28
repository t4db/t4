package t4

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/peer"
	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/testhook"
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
	// lastTakeover is when the last non-graceful takeover attempt began.
	var lastTakeover time.Time
	// lastTrim is when local WAL segments were last trimmed (trimLocalWAL).
	var lastTrim time.Time

	for {
		cli.SetLeaderGoneCheck(func(ctx context.Context) bool { return n.lockReleased(ctx, lock) })
		cli.SetLastTermFn(func() uint64 { return n.db.Load().LastPosition().Term })
		// A follower that heard the leader may take over knownTakeoverDelay
		// after it last did (see mayTakeOver); try then, not only once
		// FollowerMaxRetries reconnects have failed.
		cli.SetTakeoverAfter(knownTakeoverDelay)
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
				if hook := testhook.BeforeFollowerApply.Load(); hook != nil {
					(*hook)(entries)
				}
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
				if time.Since(lastTrim) >= followerWALTrimInterval {
					lastTrim = time.Now()
					n.trimLocalWAL(min(cli.UploadedSeq(), fromRev-1))
				}
				return nil
			},
		)

		if bgCtx.Err() != nil {
			return
		}

		if term, ok := peer.IsDiverged(err); ok {
			n.log.Warnf("t4: this node's log diverged from the leader's (term %d) — discarding it for a checkpoint", term)
			if rerr := n.forcedResync(bgCtx, term); rerr != nil {
				n.log.Errorf("t4: resync after divergence failed: %v — stopping", rerr)
				n.stop(fmt.Errorf("t4: resync after divergence: %w", rerr))
				return
			}
			fromRev = n.db.Load().LastSequence() + 1
			continue
		}

		if peer.IsResyncRequired(err) {
			metrics.FollowerResyncsTotal.WithLabelValues("stream_gap").Inc()
			if n.cfg.ObjectStore == nil {
				n.log.Errorf("t4: follower resync required but no object store — restart node")
				n.stop(errors.New("t4: follower fell behind the leader's buffer and has no object store to catch up from"))
				return
			}
			// Ring buffer miss: the follower has been offline long enough that
			// the leader's ring buffer no longer covers fromRev. Restore from
			// the latest S3 checkpoint (if the follower's Pebble is behind it),
			// then replay any remaining WAL entries from S3.
			n.log.Warnf("t4: follower resync required — restoring from checkpoint")
			covered, cpErr := n.resyncFromCheckpoint(bgCtx)
			if cpErr != nil {
				n.log.Errorf("t4: follower in-place resync failed: %v — cancelling", cpErr)
				n.stop(fmt.Errorf("t4: follower resync from checkpoint: %w", cpErr))
				return
			}
			reCtx, reCancel := context.WithTimeout(bgCtx, 5*time.Minute)
			rerr := replayRemote(reCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().LastSequence(), covered, n.log)
			reCancel()
			if term, ok := replayDiverged(rerr, n.currentTerm()); ok {
				n.log.Warnf("t4: %v — discarding this node's log for a checkpoint", rerr)
				rerr = n.forcedResync(bgCtx, term)
			}
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

		if errors.Is(err, wal.ErrUnknownOp) {
			n.log.Errorf("t4: leader sent a WAL entry this binary cannot apply — upgrade this node; stopping: %v", err)
			n.stop(fmt.Errorf("t4: cannot apply the leader's WAL, upgrade this node: %w", err))
			return
		}

		if peer.IsLeaderUnreachable(err) || peer.IsLeaderShutdown(err) || peer.IsLeaderStale(err) {
			if peer.IsLeaderShutdown(err) {
				n.log.Infof("t4: leader shut down gracefully — attempting immediate election takeover")
			} else {
				// A refused attempt hands back a client that may give up on
				// its first failed dial, so space attempts out: each one
				// reads the lock from object storage.
				if wait := peer.FollowerRetryInterval - time.Since(lastTakeover); wait > 0 {
					select {
					case <-time.After(wait):
					case <-bgCtx.Done():
						return
					}
				}
				lastTakeover = time.Now()
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
			covered, err := n.resyncFromCheckpoint(bgCtx)
			if err != nil {
				n.log.Errorf("t4: takeover catch-up checkpoint restore: %v — will retry", err)
				return nil, false
			}
			catchupCtx, catchupCancel := context.WithTimeout(bgCtx, 2*time.Minute)
			rerr := replayRemote(catchupCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().LastSequence(), covered, n.log)
			catchupCancel()
			if term, ok := replayDiverged(rerr, n.currentTerm()); ok {
				n.log.Warnf("t4: takeover catch-up: %v — discarding this node's log for a checkpoint", rerr)
				rerr = n.forcedResync(bgCtx, term)
			}
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
		if err := n.becomeLeader(bgCtx, lock, rec, takeoverStart, false); err != nil {
			n.log.Errorf("t4: promotion failed: %v", err)
			return nil, false
		}
		// Start write-processing loops immediately so that client writes are
		// not blocked while we reconcile SSTs below.
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.commitLoop(bgCtx) }()
		if n.autoCompactEnabled() {
			n.bgWg.Add(1)
			go func() { defer n.bgWg.Done(); n.autoCompactLoop(bgCtx) }()
		}
		// Upload the SSTs on disk that aren't in S3: the follower didn't run
		// SSTUploader.Start(), and becomeLeader may have restored a checkpoint
		// and replayed WAL. Its registry may also list SSTs the previous
		// leader's GC has deleted since, such as those this node restored
		// from a checkpoint, so it is verified against S3 first.
		if n.sstUploader != nil {
			rCtx, rCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			tables, rErr := istore.LiveTables(n.db.Load().Pebble())
			if rErr == nil {
				rErr = n.sstUploader.ReconcileVerified(rCtx, tables)
			}
			if rErr != nil {
				n.log.Warnf("t4: promoted leader SST reconcile: %v", rErr)
			}
			rCancel()
			n.sstUploader.Start(bgCtx)
		}
		// Only now start checkpointing: its startup checkpoint references
		// every SST on disk, and they must all be in S3 by then. Writing it
		// promptly also keeps the previous leader's GC from deleting SSTs this
		// node just uploaded, since a live checkpoint references them.
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			n.bgWg.Add(1)
			go func() { defer n.bgWg.Done(); n.checkpointLoop(bgCtx) }()
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
	if err != nil {
		return nil, forwardError(ctx, err)
	}
	return resp, nil
}

// forwardError is the error to report for a forward to the leader that
// failed with err. A forward the peer connection cut short (it closed, or
// timed out) fails with a gRPC Canceled or DeadlineExceeded status, which
// etcd clients take for the end of their own attempt and send again, even a
// write the leader may have applied already. Unless ctx itself ended, report
// the outcome unknown instead, as etcd does.
func forwardError(ctx context.Context, err error) error {
	switch {
	case isLeaderUnavailable(err):
		return ErrNoLeader
	case ctx.Err() == nil && (status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded):
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	return err
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
	case peer.ForwardMetaPut:
		return "meta_put"
	case peer.ForwardMetaDelete:
		return "meta_delete"
	case peer.ForwardGetSequence:
		return "get_sequence"
	default:
		return "unknown"
	}
}

// resyncFromCheckpoint is called from followLoop when IsResyncRequired fires.
// It uses restoreDBIfBehindCheckpoint to close the WAL gap, then (if a restore
// was actually performed) replaces the local WAL so subsequent Appends from the
// live stream start at the correct revision.
//
// covered is the latest checkpoint's sequence, for replayRemote.
func (n *Node) resyncFromCheckpoint(bgCtx context.Context) (covered int64, err error) {
	ctx, cancel := context.WithTimeout(bgCtx, 5*time.Minute)
	defer cancel()

	covered, restored, err := n.restoreDBIfBehindCheckpoint(ctx)
	if err != nil || !restored {
		return covered, err
	}
	return covered, n.replaceFollowerWAL(bgCtx)
}

// replaceFollowerWAL starts an empty follower WAL after a checkpoint restore,
// so the old segments are neither replayed on restart nor uploaded if this
// node is promoted. followLoop is the sole WAL writer for a follower, so no
// concurrent Append can race with the replacement.
func (n *Node) replaceFollowerWAL(bgCtx context.Context) error {
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
	if rerr := newWal.Open(walDir, n.currentTerm(), newSeq+1); rerr != nil {
		return fmt.Errorf("open new wal after resync: %w", rerr)
	}
	newWal.Start(bgCtx)

	n.mu.Lock()
	n.wal = newWal
	n.nextRev = newRev
	n.nextSeq = newSeq
	n.mu.Unlock()

	n.log.Infof("t4: follower in-place resync complete (rev=%d seq=%d term=%d)", newRev, newSeq, n.currentTerm())
	return nil
}

// followerWALTrimInterval is how often a follower deletes local WAL segments
// the leader has uploaded. A variable so tests can shorten it.
var followerWALTrimInterval = 10 * time.Second

// trimLocalWAL deletes the follower's sealed local WAL segments whose entries
// are all at or below seq: applied here and, per the leader, in object
// storage, so the local copies no longer protect anything (#439). Called
// from followLoop, the follower WAL's only writer.
func (n *Node) trimLocalWAL(seq int64) {
	t, ok := n.wal.(interface{ TrimThrough(int64) (int, error) })
	if !ok || seq <= 0 {
		return
	}
	if removed, err := t.TrimThrough(seq); err != nil {
		n.log.Warnf("t4: trim local WAL through seq %d: %v", seq, err)
	} else if removed > 0 {
		n.log.Debugf("t4: trimmed %d local WAL segments through seq %d", removed, seq)
	}
}

// forcedResyncTimeout bounds how long a diverged node waits for a checkpoint
// of the current term before giving up.
const forcedResyncTimeout = 5 * time.Minute

// forcedResync repairs a node whose log diverged from the leader's or object
// storage's (#441): it discards its state for a checkpoint of term or later
// (waiting for one to be written), starts an empty WAL and replays object
// storage, checked like every replay. Run by followLoop, the follower WAL's
// only writer.
func (n *Node) forcedResync(bgCtx context.Context, term uint64) error {
	if n.cfg.ObjectStore == nil {
		return errors.New("diverged from the leader and no object store to resync from")
	}
	ctx, cancel := context.WithTimeout(bgCtx, forcedResyncTimeout)
	defer cancel()
	wait := 500 * time.Millisecond
	for {
		covered, _, err := n.restoreDBFromCheckpoint(ctx, term, true)
		if err == nil {
			if err = n.replaceFollowerWAL(bgCtx); err != nil {
				return err
			}
			err = replayRemote(ctx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().LastSequence(), covered, n.log)
			if err == nil {
				n.db.Load().NotifyRevision()
				n.log.Infof("t4: diverged node resynced from a checkpoint of term >= %d (now at seq=%d)", term, n.db.Load().LastSequence())
				return nil
			}
		}
		var div *divergedError
		switch {
		case errors.As(err, &div):
			term = max(term, div.newest) // a newer leader wrote meanwhile
		case errors.Is(err, errCheckpointTooOld), errors.Is(err, errBoundaryUnverified):
		default:
			return err
		}
		n.log.Infof("t4: diverged node waiting for a checkpoint of term >= %d: %v", term, err)
		select {
		case <-time.After(wait):
			wait = min(2*wait, 10*time.Second)
		case <-ctx.Done():
			return fmt.Errorf("resync after divergence: %w (last: %v)", ctx.Err(), err)
		}
	}
}

// replayDiverged reports whether a replay error means this node's log cannot
// be shown to continue object storage's, and the term a checkpoint that
// repairs it must have: the newest in object storage when the logs differ,
// or at least this node's current term when the boundary entry is missing.
func replayDiverged(err error, current uint64) (uint64, bool) {
	var div *divergedError
	switch {
	case errors.As(err, &div):
		return max(div.newest, current), true
	case errors.Is(err, errBoundaryUnverified):
		return current, true
	}
	return 0, false
}
