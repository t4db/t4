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
	walDir := filepath.Join(n.cfg.DataDir, "wal")
	if err := n.recoverLocalWALBeforeLeadership(walDir); err != nil {
		return err
	}

	// Upload any local WAL segments that were not yet in S3 before taking on
	// writes. This covers the same-node re-election case where the previous WAL
	// had no uploader (follower WAL) or crashed before the upload completed.
	// After this point, new leader writes are uploaded async (SegmentMaxAge).
	upCtx, upCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	uploadErr := uploadLocalWALSegments(upCtx, walDir, n.cfg.ObjectStore, n.log)
	upCancel()
	if uploadErr != nil {
		return fmt.Errorf("t4: upload committed local WAL before leadership: %w", uploadErr)
	}

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
		if err := replayRemote(reCtx, n.db.Load(), n.cfg.ObjectStore, n.db.Load().LastSequence(), n.log); err != nil {
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
	nextSeq := n.db.Load().LastSequence()
	if err := w2.Open(walDir, rec.Term, nextSeq+1); err != nil {
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
	n.nextSeq = nextSeq
	n.pending = make(map[string]pendingKV)
	n.mu.Unlock()

	// Install the forward handler after role is set to leader so that
	// HandleForward sees the correct role and executes writes directly.
	peerSrv.SetForwardHandler(n)
	// Tell the peer server what the first sequence this leader will write is.
	// FollowRequest.FromRevision is interpreted as a WAL/peer-stream sequence
	// (see peer.Server). Followers connecting with a lower fromRev are missing
	// entries that were only replayed into Pebble from S3 (never in the ring
	// buffer) and must re-sync before consuming the live stream. After a
	// post-compact checkpoint LastSequence > CurrentRevision, so seeding
	// startRev from CurrentRevision would let stale followers connect from a
	// sequence that exists only in Pebble/S3 — not the ring buffer.
	peerSrv.SetStartRev(n.db.Load().LastSequence() + 1)

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

// recoverLocalWALBeforeLeadership closes the follower WAL, applies every
// committed local entry to Pebble, and proves that promotion cannot skip a WAL
// sequence. Followers persist a committed peer batch before acknowledging it,
// then apply it to Pebble, so WAL can legitimately be ahead after a crash.
func (n *Node) recoverLocalWALBeforeLeadership(walDir string) error {
	if err := n.wal.Close(); err != nil {
		return fmt.Errorf("t4: close follower WAL before leadership: %w", err)
	}
	if err := n.wal.ReplayLocal(n.db.Load(), n.db.Load().LastSequence()); err != nil {
		return fmt.Errorf("t4: replay committed local WAL before leadership: %w", err)
	}
	maxSeq, err := wal.MaxSequence(walDir)
	if err != nil {
		return fmt.Errorf("t4: scan local WAL sequence before leadership: %w", err)
	}
	if appliedSeq := n.db.Load().LastSequence(); maxSeq > appliedSeq {
		return fmt.Errorf("t4: refusing leadership: local WAL sequence %d is ahead of Pebble sequence %d after replay", maxSeq, appliedSeq)
	}
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
		// A fenced node (Close in flight, commitLoop dead from a fatal
		// error, or any future code path that flips n.closed) must NOT
		// refresh LastSeenNano on the cluster lock. Touching the lock
		// here would assert "I'm a healthy leader" while in fact this
		// node can no longer make progress, causing followers' TakeOver
		// to back off on a liveness check and the cluster to stall.
		// Exit the watch loop instead; whichever path set n.closed is
		// responsible for tearing down peerGRPC.
		if n.closed.Load() {
			n.log.Debugf("t4: leader watch (%s): node fenced — exiting watch loop", reason)
			return false
		}
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
	// fatalExit is set when commitLoop returns because of a WAL or Pebble
	// error, as opposed to a clean ctx.Done shutdown. In the fatal case the
	// leader must step down so it does not hold the cluster lock as a
	// zombie: dead writer, live lock holder. Without stepdown a follower
	// cannot win TakeOver (the watchLoop keeps LastSeenNano fresh) and the
	// cluster stalls until an operator restarts the dead node.
	var fatalExit bool

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
				if fatalExit {
					n.stepDownOnFatalCommitError()
				}
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

		// Cancel the WAL attempt only after every caller in this group has
		// abandoned it. One short request deadline must not abort unrelated
		// writes that happened to be drained into the same group-commit batch.
		batchCtx, batchCancel := context.WithCancel(ctx)
		var abandoned atomic.Int32
		for _, req := range batch {
			r := req
			go func() {
				select {
				case <-r.ctx.Done():
					if abandoned.Add(1) == int32(len(batch)) {
						batchCancel()
					}
				case <-batchCtx.Done():
				}
			}()
		}

		// Assign sequence IDs only to the batch currently being attempted. The
		// IDs are based on the last applied sequence and are not published to
		// followers until WAL append succeeds. A canceled batch can therefore
		// reuse the same IDs without leaving a hole or stale staged entries.
		baseSeq := n.db.Load().LastSequence()
		entries := make([]*wal.Entry, len(batch))
		for i, req := range batch {
			req.entry.ID = baseSeq + int64(i) + 1
			entries[i] = &req.entry
		}

		// Append locally before exposing IDs or payloads to followers. This
		// sacrifices a small amount of fsync/network overlap, but ensures failed
		// attempts never enter the peer replay buffer under reusable IDs.
		err := n.wal.AppendBatch(batchCtx, entries)
		if err == nil && n.peerSrv != nil {
			for _, req := range batch {
				n.peerSrv.Broadcast(&req.entry)
			}

			startRev := batch[0].entry.Sequence()
			maxRev := batch[len(batch)-1].entry.Sequence()
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
		batchCancel() // release watcher goroutines

		// Apply all entries to Pebble as one batch (in order).
		if err == nil {
			dbEntries := make([]wal.Entry, len(batch))
			for i, req := range batch {
				dbEntries[i] = req.entry
			}
			err = n.db.Load().Apply(dbEntries)
			if err == nil {
				n.mu.Lock()
				n.nextSeq = dbEntries[len(dbEntries)-1].Sequence()
				n.mu.Unlock()
				n.maybeRecordRevisionSample(dbEntries)
			}
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
			// Stop accepting writes immediately; the defer fences the node
			// and triggers leader stepdown so followers can take over.
			fatalExit = true
			return
		}
	}
}

// stepDownOnFatalCommitError releases leadership after the commit loop has
// exited due to a fatal WAL/Pebble error. Without this, the node remains the
// cluster's elected leader (watchLoop keeps LastSeenNano fresh, peer server
// keeps the port bound) even though it can no longer accept writes —
// followers cannot win TakeOver and the cluster stalls. Stepping down here
// stops the lock-refresh polling and tears down the peer server so followers
// notice the failure and proceed to election via the standard liveness-TTL
// path.
//
// Idempotent with respect to Node.Close: cancelBg is a sync.Once-style
// context cancel, and grpcSrv.Stop is documented as idempotent.
func (n *Node) stepDownOnFatalCommitError() {
	if n.loadRole() != roleLeader {
		return
	}
	n.log.Warnf("t4: commit loop exited with fatal error — stepping down so followers can take over")
	n.cancelBg()
	n.mu.Lock()
	grpcSrv := n.peerGRPC
	n.mu.Unlock()
	if grpcSrv != nil {
		grpcSrv.Stop()
	}
}

// uploadLocalWALSegments uploads any sealed local WAL segment files that are
// not yet present in S3. This is called when becoming leader so that local
// entries (recovered via replayLocal) are durable in object storage before
// followers can bootstrap.
func uploadLocalWALSegments(ctx context.Context, walDir string, store object.Store, log Logger) error {
	if store == nil {
		return nil
	}
	paths, err := wal.LocalSegments(walDir)
	if err != nil {
		return fmt.Errorf("list local WAL segments: %w", err)
	}
	if len(paths) == 0 {
		return nil
	}

	// Build set of keys already in S3 to skip redundant uploads.
	s3Keys, err := store.List(ctx, "wal/")
	if err != nil {
		return fmt.Errorf("list remote WAL segments: %w", err)
	}
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
			return fmt.Errorf("upload %q to %q: %w", path, objKey, err)
		}
	}
	return nil
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
	seq := n.db.Load().LastSequence()
	if err := n.wal.SealAndFlush(seq + 1); err != nil {
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
		if err := n.cp.WriteWithRegistryAtSequence(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, seq, "", n.sstUploader.Registry(), n.sstUploader.InheritedRegistry()); err != nil {
			n.fenceMu.Unlock()
			n.log.Errorf("t4: startup checkpoint rev=%d: %v", rev, err)
			return
		}
	} else if err := n.cp.WriteAtSequence(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, seq, "", n.cfg.AncestorStore); err != nil {
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
	seq := n.db.Load().LastSequence()
	if err := n.wal.SealAndFlush(seq + 1); err != nil {
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
		if err := n.cp.WriteWithRegistryAtSequence(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, seq, "", n.sstUploader.Registry(), n.sstUploader.InheritedRegistry()); err != nil {
			n.fenceMu.Unlock()
			n.log.Errorf("t4: write checkpoint rev=%d: %v", rev, err)
			return
		}
	} else if err := n.cp.WriteAtSequence(ctx, n.db.Load().Pebble(), n.cfg.ObjectStore, n.term, rev, seq, "", n.cfg.AncestorStore); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: write checkpoint rev=%d: %v", rev, err)
		return
	}
	n.fenceMu.Unlock()
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	n.log.Infof("t4: checkpoint written (rev=%d)", rev)

	// GC WAL segments from S3 that are fully covered by this checkpoint AND
	// that all connected followers have applied. WAL segment boundaries and
	// follower ACKs are sequence-based, while the checkpoint manifest still
	// advertises the user-visible revision.
	gcCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	gcSeq := seq
	if n.peerSrv != nil {
		if minFollower := n.peerSrv.MinFollowerAppliedRev(); minFollower < gcSeq {
			gcSeq = minFollower
		}
	}
	deleted, gcErr := wal.GCSegments(gcCtx, n.cfg.ObjectStore, gcSeq, n.log)
	if gcErr != nil {
		n.log.Warnf("t4: wal gc: %v", gcErr)
	} else if deleted > 0 {
		metrics.WALGCTotal.Add(float64(deleted))
		n.log.Infof("t4: wal gc: deleted %d segments (covered by checkpoint seq=%d)", deleted, gcSeq)
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
