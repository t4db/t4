package t4

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// becomeLeader transitions this node to leader role.
// Re-opens the WAL with an S3 uploader, starts the peer gRPC server,
// and launches the watchLoop. Must NOT be called with n.mu held.
func (n *Node) becomeLeader(bgCtx context.Context, lock *election.Lock, rec *election.LockRecord) error {
	_ = n.wal.Close()
	walDir := filepath.Join(n.cfg.DataDir, "wal")

	// Upload any local WAL segments that were not yet in S3 before taking on
	// writes. This covers the same-node re-election case where the previous WAL
	// had no uploader (follower WAL) or crashed before the upload completed.
	// After this point, new leader writes are uploaded async (SegmentMaxAge).
	upCtx, upCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	uploadLocalWALSegments(upCtx, walDir, n.cfg.ObjectStore, n.log)
	upCancel()

	// Replay any remote WAL entries not yet in our Pebble. A follower that wins
	// election may be behind the former leader if the former leader committed
	// entries during single-node mode (no quorum required) before crashing.
	// Check against the latest S3 checkpoint first: if this node is behind the
	// checkpoint, restore it before replaying WAL so replayRemote only needs
	// segments still present in S3.
	if n.cfg.ObjectStore != nil {
		cpCtx, cpCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if _, cpErr := n.restoreDBIfBehindCheckpoint(cpCtx); cpErr != nil {
			cpCancel()
			return fmt.Errorf("t4: leader checkpoint catch-up: %w", cpErr)
		}
		cpCancel()

		reCtx, reCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := replayRemote(reCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().CurrentRevision(), n.log); err != nil {
			reCancel()
			return fmt.Errorf("t4: becomeLeader replay remote WAL: %w", err)
		}
		reCancel()
	}

	// With quorum commit, every committed entry exists on at least two nodes'
	// WALs before the caller sees success. S3 is disaster-recovery only (both
	// nodes fail simultaneously), so uploads can be async — driven by
	// SegmentMaxAge — without affecting write durability.
	w2 := wal.New(
		wal.WithUploader(makeUploader(n.cfg.ObjectStore, n.log)),
		wal.WithSegmentMaxSize(n.cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(n.cfg.SegmentMaxAge),
		wal.WithLogger(n.log),
	)
	if err := w2.Open(walDir, rec.Term, n.db.Load().CurrentRevision()+1); err != nil {
		return fmt.Errorf("t4: open WAL as leader: %w", err)
	}
	w2.Start(bgCtx)

	peerSrv := peer.NewServer(n.cfg.PeerBufferSize, n.log)
	lis, err := net.Listen("tcp", n.cfg.PeerListenAddr)
	if err != nil {
		_ = w2.Close()
		return fmt.Errorf("t4: peer listen %s: %w", n.cfg.PeerListenAddr, err)
	}
	serverOpts := []grpc.ServerOption{grpc.ForceServerCodec(peer.Codec{})}
	if n.cfg.PeerServerTLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(n.cfg.PeerServerTLS))
	}
	grpcSrv := grpc.NewServer(serverOpts...)
	peer.RegisterWalStreamServer(grpcSrv, peerSrv)

	// Commit state transition atomically before accepting connections.
	n.mu.Lock()
	n.wal = w2
	n.term = rec.Term
	n.peerSrv = peerSrv
	n.peerLis = lis
	n.peerGRPC = grpcSrv
	n.leaderCli.Store(nil) // leader does not forward writes
	n.storeRole(roleLeader)
	n.nextRev = n.db.Load().CurrentRevision() // sync revision counter after any replay
	n.pending = make(map[string]pendingKV)
	n.mu.Unlock()

	// Install the forward handler after role is set to leader so that
	// HandleForward sees the correct role and executes writes directly.
	peerSrv.SetForwardHandler(n)
	// Tell the peer server what the first revision this leader will write is.
	// Followers connecting with a lower fromRev are missing entries that were
	// only replayed into Pebble from S3 (never in the ring buffer) and must
	// re-sync before consuming the live stream.
	peerSrv.SetStartRev(n.db.Load().CurrentRevision() + 1)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			n.log.Warnf("t4: peer server: %v", err)
		}
	}()

	n.updateMetrics()
	metrics.ElectionsTotal.WithLabelValues("won").Inc()
	n.log.Infof("t4: elected leader (term=%d, peer=%s)", rec.Term, n.cfg.PeerListenAddr)
	go n.watchLoop(bgCtx, lock, rec.Term)
	return nil
}

// watchLoop periodically reads the lock from S3 to detect supersession.
// Steps down (cancelBg) if the lock's term or owner changes.
// On clean shutdown, releases the lock.
//
// Split-brain prevention strategy:
//
//  1. Periodic fallback: LeaderWatchInterval reads S3 to detect supersession.
//     Does not touch LastSeenNano — followers are healthy while connected.
//
//  2. On follower disconnect: immediately fence writes (~50ms), verify still
//     leader, touch LastSeenNano so the disconnected follower backs off from
//     TakeOver. Begin polling every peer.FollowerRetryInterval to keep
//     LastSeenNano fresh and detect any TakeOver that follows. Stop polling
//     once followers reconnect.
//
//  3. TakeOver safety: if a follower fails to reconnect after FollowerMaxRetries
//     it calls TakeOver. If LastSeenNano is older than LeaderLivenessTTL the
//     TakeOver proceeds (via atomic conditional PUT). The leader detects the
//     supersession at its next fencedCheck and steps down cleanly.
func (n *Node) watchLoop(ctx context.Context, lock *election.Lock, term uint64) {
	ticker := time.NewTicker(n.cfg.LeaderWatchInterval)
	defer ticker.Stop()

	// Disconnect channel from the peer server, if we're running in multi-node mode.
	var disconnectC <-chan struct{}
	if n.peerSrv != nil {
		disconnectC = n.peerSrv.DisconnectC
	}

	// pollC is non-nil while liveness-touch polling is active (nil blocks in select).
	var (
		activePollTicker *time.Ticker
		pollC            <-chan time.Time
	)

	stopPolling := func() {
		if activePollTicker != nil {
			activePollTicker.Stop()
			activePollTicker = nil
		}
		pollC = nil
	}

	startPolling := func() {
		if activePollTicker != nil {
			return // already polling
		}
		activePollTicker = time.NewTicker(peer.FollowerRetryInterval)
		pollC = activePollTicker.C
	}

	// Always start polling immediately in cluster mode so LastSeenNano is
	// kept fresh from the very first tick, even before any follower connects.
	// Without this, a leader with no followers never touches the lock and the
	// liveness record goes stale after LeaderLivenessTTL, letting any
	// recovering follower win TakeOver and create a split-brain.
	if n.peerSrv != nil {
		startPolling()
	}

	// fencedCheck fences all leader writes for the duration of one S3 GET (and
	// optional PUT). Returns false and steps down if the lock has been superseded.
	// When touch is true and still leader, also writes LastSeenNano to the lock
	// so disconnected followers see a fresh liveness signal and back off TakeOver.
	//
	// The Read and the Touch (when requested) are tied together via a conditional
	// PUT (If-Match: <etag>): if another node wins the lock between our Read and
	// our Touch, TouchIfMatch returns ErrPreconditionFailed and we step down
	// immediately — closing the Read→Touch split-brain race.
	//
	// NOTE: fenceMu is released explicitly (not via defer) so that grpcSrv.Stop()
	// can be called outside the lock.  grpc.Server.Stop waits for in-flight
	// handlers to finish; those handlers (Put/Create/…) acquire fenceMu.RLock()
	// themselves, so calling Stop() while holding the write lock would deadlock.
	fencedCheck := func(reason string, touch bool) bool {
		n.fenceMu.Lock()
		rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rec, etag, err := lock.ReadETag(rCtx)
		cancel()
		if err != nil {
			n.fenceMu.Unlock()
			n.log.Warnf("t4: leader watch (%s): read lock: %v", reason, err)
			return true // transient S3 error; keep running
		}
		if rec == nil || rec.Term != term || rec.NodeID != n.cfg.NodeID {
			n.log.Errorf("t4: leader watch (%s): lock superseded (current: %+v) — stepping down", reason, rec)
			n.cancelBg()
			n.fenceMu.Unlock()
			// Stop the peer gRPC server so followers immediately lose their
			// streams and detect the leadership change.  Without this, followers
			// keep forwarding ForwardGetRevision to this zombie leader and receive
			// a stale revision, causing linearizability violations.
			if grpcSrv := n.peerGRPC; grpcSrv != nil {
				grpcSrv.Stop()
			}
			return false
		}
		if touch && n.peerSrv != nil {
			tCtx, tCancel := context.WithTimeout(ctx, 5*time.Second)
			err := lock.TouchIfMatch(tCtx, term, n.cfg.AdvertisePeerAddr, etag, n.db.Load().CurrentRevision())
			tCancel()
			if errors.Is(err, object.ErrPreconditionFailed) {
				// Another node wrote the lock between our Read and our Touch —
				// we have been superseded.  Step down immediately.
				n.log.Errorf("t4: leader watch (%s): touch precondition failed — lock taken, stepping down", reason)
				n.cancelBg()
				n.fenceMu.Unlock()
				if grpcSrv := n.peerGRPC; grpcSrv != nil {
					grpcSrv.Stop()
				}
				return false
			}
			if err != nil {
				n.log.Warnf("t4: leader watch (%s): touch lock: %v", reason, err)
			}
		}
		n.fenceMu.Unlock()
		return true
	}

	for {
		select {
		case <-ticker.C:
			if !fencedCheck("periodic", false) {
				return
			}

		case <-disconnectC:
			// Immediate check + touch: catches TakeOver that already happened,
			// and signals liveness to the disconnected follower.
			n.log.Infof("t4: leader watch: follower disconnected — fencing writes and checking lock")
			if !fencedCheck("disconnect", true) {
				return
			}
			// Resume polling so LastSeenNano stays fresh while the follower
			// is away.  startPolling is idempotent — if we were already polling
			// (e.g. started at launch with no followers), this is a no-op.
			startPolling()

		case <-pollC:
			// Touch the lock to signal liveness and detect supersession.
			if !fencedCheck("poll", true) {
				stopPolling()
				return
			}
			// If followers have (re)connected, liveness touches are no longer
			// necessary — stop polling and rely on the periodic ticker.
			if n.peerSrv != nil && n.peerSrv.ConnectedFollowers() > 0 {
				n.log.Debugf("t4: leader watch: followers connected — pausing liveness poll")
				stopPolling()
			}

		case <-ctx.Done():
			stopPolling()
			return
		}
	}
}

// ── Write forwarding (leader side) ───────────────────────────────────────────

// HandleForward implements peer.ForwardHandler. Called by the peer gRPC server
// when a follower forwards a write. Dispatches to the appropriate Node method.
// Since HandleForward runs on the leader, all write methods execute directly.
func (n *Node) HandleForward(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	switch req.Op {
	case peer.ForwardPut:
		rev, err := n.Put(ctx, req.Key, req.Value, req.Lease)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Revision: rev, Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardCreate:
		rev, err := n.Create(ctx, req.Key, req.Value, req.Lease)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Revision: rev, Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardUpdate:
		newRev, oldKV, updated, err := n.Update(ctx, req.Key, req.Value, req.Revision, req.Lease)
		code, msg := encodeErr(err)
		resp := &peer.ForwardResponse{Revision: newRev, Succeeded: updated, ErrCode: code, ErrMsg: msg}
		resp.OldKV = kvToMsg(oldKV)
		return resp, nil

	case peer.ForwardDeleteIfRevision:
		newRev, oldKV, deleted, err := n.DeleteIfRevision(ctx, req.Key, req.Revision)
		code, msg := encodeErr(err)
		resp := &peer.ForwardResponse{Revision: newRev, Succeeded: deleted, ErrCode: code, ErrMsg: msg}
		resp.OldKV = kvToMsg(oldKV)
		return resp, nil

	case peer.ForwardCompact:
		err := n.Compact(ctx, req.Revision)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardGetRevision:
		// Return nextRev (the highest *assigned* revision), not db.CurrentRevision()
		// (the last *applied* revision). A write increments nextRev under n.mu and
		// sends to writeC before the commit loop applies it to Pebble. If we returned
		// db.CurrentRevision() here, a follower could sync to a revision that precedes
		// an in-flight write whose acknowledgment is about to be sent to the client —
		// causing a stale read that violates linearizability.
		n.mu.Lock()
		rev := n.nextRev
		n.mu.Unlock()
		return &peer.ForwardResponse{Revision: rev, Succeeded: true}, nil

	case peer.ForwardTxn:
		if req.TxnReq == nil {
			return nil, fmt.Errorf("t4: ForwardTxn missing TxnReq")
		}
		txnReq := forwardMsgToTxnRequest(req.TxnReq)
		resp, err := n.Txn(ctx, txnReq)
		if err != nil {
			code, msg := encodeErr(err)
			return &peer.ForwardResponse{ErrCode: code, ErrMsg: msg}, nil
		}
		deletedKeys := make([]string, 0, len(resp.DeletedKeys))
		for k := range resp.DeletedKeys {
			deletedKeys = append(deletedKeys, k)
		}
		return &peer.ForwardResponse{
			Revision:    resp.Revision,
			Succeeded:   resp.Succeeded,
			DeletedKeys: deletedKeys,
		}, nil
	}
	return nil, fmt.Errorf("t4: unknown forward op %d", req.Op)
}

// commitLoop is the group-commit pipeline for leader/single-node writes.
// It drains writeC, writes all entries to WAL with a single fsync, applies
// them to Pebble as a batch, and signals each caller's done channel.
func (n *Node) commitLoop(ctx context.Context) {
	defer func() {
		// Fence the node first so new writers fail fast.
		n.closed.Store(true)

		// Drain requests immediately to free queue slots. This unblocks writers
		// that might be stuck on n.writeC <- req while holding n.mu.
	drain:
		for {
			select {
			case req := <-n.writeC:
				req.done <- ErrClosed
			default:
				break drain
			}
		}

		// Wait for any writer currently in the critical section (between closed
		// check and queue send) to finish, then perform a final drain pass.
		n.mu.Lock()
		n.mu.Unlock() //nolint:staticcheck // SA2001: intentional memory barrier, not a mistake
		for {
			select {
			case req := <-n.writeC:
				req.done <- ErrClosed
			default:
				return
			}
		}
	}()

	for {
		// Block until at least one request arrives.
		var batch []*writeReq
		select {
		case req := <-n.writeC:
			batch = append(batch, req)
		case <-ctx.Done():
			return
		}
		// Drain any additional requests that arrived while we were processing.
	drain:
		for {
			select {
			case req := <-n.writeC:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		// Build a context that's cancelled when any batch caller gives up.
		// This lets the WAL abort early (in tests / context-aware WALs) if
		// all callers have abandoned the batch. Real fsyncs complete normally.
		batchCtx, batchCancel := context.WithCancel(ctx)
		for _, req := range batch {
			r := req
			go func() {
				select {
				case <-r.ctx.Done():
					batchCancel()
				case <-batchCtx.Done():
				}
			}()
		}

		// Write all entries to WAL with one fsync.
		entries := make([]*wal.Entry, len(batch))
		for i, req := range batch {
			entries[i] = &req.entry
		}

		var err error
		if n.peerSrv != nil {
			// Pipeline: overlap network delivery to followers with the leader's
			// own WAL fsync, but do not let followers make entries durable or
			// visible until the leader has fsynced successfully. Followers stage
			// entry messages in memory and wait for BroadcastCommit before
			// appending/applying the batch locally.
			walErrC := make(chan error, 1)
			go func() { walErrC <- n.wal.AppendBatch(batchCtx, entries) }()

			for _, req := range batch {
				n.peerSrv.Broadcast(&req.entry)
			}

			startRev := batch[0].entry.Revision
			maxRev := batch[len(batch)-1].entry.Revision
			err = <-walErrC
			if err == nil {
				n.peerSrv.BroadcastCommit(startRev, maxRev)

				// Wait for follower ACKs according to the configured policy.
				// Use the commit loop's own context (node lifetime), NOT batchCtx:
				// batchCtx is cancelled after AppendBatch returns and passing it
				// here would cause WaitForFollowers to return instantly.
				//
				// Availability policy: if all followers disconnect mid-wait, we
				// proceed anyway — the entry is already durable in the leader's
				// WAL and will be replayed by followers when they reconnect.
				if waitErr := n.peerSrv.WaitForFollowers(ctx, maxRev, peer.WaitMode(n.cfg.FollowerWaitMode)); waitErr != nil {
					err = waitErr
				}
			}
		} else {
			err = n.wal.AppendBatch(batchCtx, entries)
		}
		batchCancel() // release watcher goroutines

		// Apply all entries to Pebble as one batch (in order).
		if err == nil {
			dbEntries := make([]wal.Entry, len(batch))
			for i, req := range batch {
				dbEntries[i] = req.entry
			}
			err = n.db.Load().Apply(dbEntries)
		}

		// Clear optimistic state before waking callers so a failed batch cannot
		// leak stale pending revisions into a racing follow-up write.
		n.clearPendingBatch(batch)

		// Signal all callers.
		for _, req := range batch {
			req.done <- err
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Callers abandoned the batch; this is not a permanent fault.
				// Let the loop continue for the next batch.
				continue
			}
			// A WAL or Pebble error leaves the segment in an unknown state.
			// Stop accepting writes immediately; the defer will fence the node.
			return
		}
	}
}

// uploadLocalWALSegments uploads any sealed local WAL segment files that are
// not yet present in S3. This is called when becoming leader so that local
// entries (recovered via replayLocal) are durable in object storage before
// followers can bootstrap.
func uploadLocalWALSegments(ctx context.Context, walDir string, store object.Store, log Logger) {
	paths, err := wal.LocalSegments(walDir)
	if err != nil || len(paths) == 0 {
		return
	}

	// Build set of keys already in S3 to skip redundant uploads.
	s3Keys, _ := store.List(ctx, "wal/")
	inS3 := make(map[string]struct{}, len(s3Keys))
	for _, k := range s3Keys {
		inS3[k] = struct{}{}
	}

	up := makeUploader(store, log)
	for _, path := range paths {
		term, firstRev, ok := wal.ParseSegmentName(filepath.Base(path))
		if !ok {
			continue
		}
		objKey := wal.ObjectKey(term, firstRev)
		if _, exists := inS3[objKey]; exists {
			continue // already uploaded
		}
		if err := up(ctx, path, objKey); err != nil {
			log.Warnf("t4: pre-leader upload %q → %q: %v", path, objKey, err)
		}
	}
}

// ── Background checkpoint loop ────────────────────────────────────────────────

func (n *Node) checkpointLoop(ctx context.Context) {
	// Write an immediate checkpoint before entering the ticker so that any
	// entries recovered from local WAL segments (but not yet in S3) are
	// captured in the checkpoint. Without this, a crash after becoming leader
	// but before the first periodic checkpoint could leave new followers unable
	// to see those entries.
	n.forceCheckpoint(ctx)

	ticker := time.NewTicker(n.cfg.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.maybeCheckpoint(ctx)
		case <-n.checkpointTriggerC:
			n.maybeCheckpoint(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// forceCheckpoint writes a checkpoint unconditionally (bypassing the
// entriesSinceCheckpoint guard). Used on startup to capture local state.
func (n *Node) forceCheckpoint(ctx context.Context) {
	n.fenceMu.Lock()
	rev := n.db.Load().CurrentRevision()
	if rev == 0 {
		n.fenceMu.Unlock()
		return
	}
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: startup checkpoint seal WAL: %v", err)
		return
	}
	if err := n.db.Load().Flush(); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: startup checkpoint flush pebble: %v", err)
		return
	}
	if n.sstUploader != nil {
		n.sstUploader.Wait()
		if err := n.cp.WriteWithRegistry(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, "", n.sstUploader.Registry(), n.sstUploader.InheritedRegistry()); err != nil {
			n.fenceMu.Unlock()
			n.log.Errorf("t4: startup checkpoint rev=%d: %v", rev, err)
			return
		}
	} else if err := n.cp.Write(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, "", n.cfg.AncestorStore); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: startup checkpoint rev=%d: %v", rev, err)
		return
	}
	n.fenceMu.Unlock()
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	n.log.Infof("t4: startup checkpoint written (rev=%d)", rev)
}

func (n *Node) maybeCheckpoint(ctx context.Context) {
	if atomic.LoadInt64(&n.entriesSinceCheckpoint) == 0 {
		return
	}
	n.fenceMu.Lock()
	rev := n.db.Load().CurrentRevision()
	if rev == 0 {
		n.fenceMu.Unlock()
		return
	}
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: checkpoint seal WAL: %v", err)
		return
	}
	if err := n.db.Load().Flush(); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: checkpoint flush pebble: %v", err)
		return
	}
	if n.sstUploader != nil {
		n.sstUploader.Wait()
		if err := n.cp.WriteWithRegistry(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, "", n.sstUploader.Registry(), n.sstUploader.InheritedRegistry()); err != nil {
			n.fenceMu.Unlock()
			n.log.Errorf("t4: write checkpoint rev=%d: %v", rev, err)
			return
		}
	} else if err := n.cp.Write(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, "", n.cfg.AncestorStore); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: write checkpoint rev=%d: %v", rev, err)
		return
	}
	n.fenceMu.Unlock()
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	n.log.Infof("t4: checkpoint written (rev=%d)", rev)

	// GC WAL segments from S3 that are fully covered by this checkpoint AND
	// that all connected followers have applied. Using min(leaderRev,
	// minFollowerAppliedRev) as the GC boundary ensures we never delete a
	// segment that a follower still needs to replay.
	gcCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	gcRev := rev
	if n.peerSrv != nil {
		if minFollower := n.peerSrv.MinFollowerAppliedRev(); minFollower < gcRev {
			gcRev = minFollower
		}
	}
	deleted, gcErr := wal.GCSegments(gcCtx, n.cfg.ObjectStore, gcRev, n.log)
	if gcErr != nil {
		n.log.Warnf("t4: wal gc: %v", gcErr)
	} else if deleted > 0 {
		metrics.WALGCTotal.Add(float64(deleted))
		n.log.Infof("t4: wal gc: deleted %d segments (covered by checkpoint rev=%d)", deleted, gcRev)
	}

	// GC old checkpoint archives from S3, keeping the 2 most recent so that
	// any in-flight bootstrap that read manifest/latest just before we
	// overwrote it can still fetch the previous checkpoint.
	// GCCheckpoints deletes old checkpoint archives and returns the set of SST
	// keys that were exclusively referenced by the deleted checkpoints. Passing
	// that candidate set to GCOrphanSSTs (instead of listing all "sst/" keys)
	// eliminates the race where a newly-promoted leader uploads SSTs before
	// writing its first checkpoint — those SSTs never appear in any deleted
	// checkpoint's index, so they are never mistakenly treated as orphans.
	cpDeleted, orphanSSTs, cpGCErr := n.cp.GCCheckpoints(gcCtx, n.cfg.ObjectStore, 2)
	if cpGCErr != nil {
		n.log.Warnf("t4: checkpoint gc: %v", cpGCErr)
	} else if cpDeleted > 0 {
		n.log.Infof("t4: checkpoint gc: deleted %d old checkpoint(s)", cpDeleted)
	}

	// Only run SST GC when old checkpoints were actually deleted and there are
	// candidate SSTs to clean up. This skips the Delete loop entirely when
	// nothing changed, which is the common case.
	if cpDeleted > 0 && len(orphanSSTs) > 0 {
		sstDeleted, sstGCErr := n.cp.GCOrphanSSTs(gcCtx, n.cfg.ObjectStore, orphanSSTs)
		if sstGCErr != nil {
			n.log.Warnf("t4: sst gc: %v", sstGCErr)
		} else if sstDeleted > 0 {
			n.log.Infof("t4: sst gc: deleted %d orphan sst(s)", sstDeleted)
		}
	}
}
