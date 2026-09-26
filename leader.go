package t4

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// becomeLeader transitions this node to leader role.
// Re-opens the WAL with an S3 uploader, starts the peer gRPC server,
// and launches the watchLoop. Must NOT be called with n.mu held.
//
// lockWriteStart is when the lock write that won leadership started; the
// first lease runs from then.
func (n *Node) becomeLeader(bgCtx context.Context, lock *election.Lock, rec *election.LockRecord, lockWriteStart time.Time) error {
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
	peerSrv.SetTerm(rec.Term)
	// A database with the meta keyspace, or a new one about to get it (see
	// initMetaAtGenesis), has WAL entries that followers below format 3 would
	// misapply: refuse them before serving.
	if _, metaOn, err := n.db.Load().MetaGet(metaFormatKey); err != nil {
		_ = w2.Close()
		return fmt.Errorf("t4: read meta format: %w", err)
	} else if metaOn || (n.db.Load().LastSequence() == 0 && !testhook.LegacyNewDatabases.Load()) {
		peerSrv.SetMinFollowerWALFormat(wal.WALFormatVersion)
	}
	lis, err := net.Listen("tcp", n.cfg.PeerListenAddr)
	if err != nil {
		_ = w2.Close()
		return fmt.Errorf("t4: peer listen %s: %w", n.cfg.PeerListenAddr, err)
	}
	serverOpts := append([]grpc.ServerOption{grpc.ForceServerCodec(peer.Codec{})}, peer.ServerOptions()...)
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
	n.extendLease(lockWriteStart, false)
	n.leasePeers.Store(peerSrv)
	n.termStartSeq.Store(nextSeq)
	n.fenceSeq.Store(-1)
	// A term starts with the flag clear, in the lock (TakeOver and
	// TryAcquire never set it) and here.
	n.objectStoreComplete.Store(false)
	n.lockObjectStoreComplete.Store(false)
	n.storeRole(roleLeader)
	n.nextRev = n.db.Load().CurrentRevision() // sync revision counter after any replay
	n.nextSeq = nextSeq
	n.pending = make(map[string]pendingKV)
	n.pendingMeta = make(map[string]pendingMeta)
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

// watchLoop keeps this node's leadership valid and detects when it is lost.
//
// It renews the lock (see lease.go): it reads the lock, then rewrites it with
// the time the renewal started, how long it stays valid and the election
// fence, conditioned on the ETag it read, so a TakeOver in between is
// detected and the node steps down. It renews every LeaderWatchInterval while
// every follower is demonstrably hearing it, every fastRenewInterval
// otherwise, at once when it leaves slow mode, and whenever the commit loop
// needs the fence moved.
func (n *Node) watchLoop(ctx context.Context, lock *election.Lock, term uint64) {
	var disconnectC <-chan struct{}
	if n.peerSrv != nil {
		disconnectC = n.peerSrv.DisconnectC
	}
	check := time.NewTicker(leaderCheckInterval)
	defer check.Stop()
	slowInterval := n.cfg.LeaderWatchInterval

	// stepDown fences writes while cancelling the node's background work,
	// then stops the peer server so followers lose their streams and find
	// the new leader. fenceMu is released before grpcSrv.Stop(): Stop waits
	// for in-flight handlers, which take fenceMu.RLock themselves.
	stepDown := func(reason, why string) {
		n.log.Errorf("t4: leader watch (%s): %s — stepping down", reason, why)
		n.fenceMu.Lock()
		n.cancelBg()
		n.fenceMu.Unlock()
		if grpcSrv := n.peerGRPC; grpcSrv != nil {
			grpcSrv.Stop()
		}
	}

	// renew writes the lock once; it returns false after stepping down. It
	// does not take fenceMu: writes waiting on follower acknowledgements
	// must not delay it. Safety comes from the lease, which every write
	// acknowledgement and linearizable read checks.
	renew := func(reason string, slow bool) bool {
		// A fenced node (Close in flight, commitLoop dead from a fatal
		// error, …) must not assert liveness it cannot back up; whichever
		// path set n.closed tears down the peer server.
		if n.closed.Load() {
			n.log.Debugf("t4: leader watch (%s): node fenced — exiting watch loop", reason)
			return false
		}
		start := time.Now()
		rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rec, etag, err := lock.ReadETag(rCtx)
		cancel()
		if err != nil {
			n.log.Warnf("t4: leader watch (%s): read lock: %v", reason, err)
			return true // transient; the lease lapses if this persists
		}
		if rec == nil || rec.Term != term || rec.NodeID != n.cfg.NodeID {
			stepDown(reason, fmt.Sprintf("lock superseded (current: %+v)", rec))
			return false
		}
		// Position before revision: the revision read afterwards covers it.
		pos := n.db.Load().LastPosition()
		rev := n.db.Load().CurrentRevision()
		if wantSeq, wantRev, wantTerm := n.fenceWanted(); wantSeq > pos.Seq {
			pos.Seq, pos.Term = wantSeq, max(pos.Term, wantTerm)
			if wantRev > rev {
				rev = wantRev
			}
		}
		seq := pos.Seq
		// The flag as the commit loop wants it now; a write waiting on it is
		// acknowledged only once this renewal has recorded it.
		complete := n.objectStoreComplete.Load()
		n.flagWriteStarting(complete)
		ttl := fastTTL
		if slow {
			ttl = n.slowTTL()
		}
		tCtx, tCancel := context.WithTimeout(ctx, 5*time.Second)
		err = lock.Renew(tCtx, term, n.cfg.AdvertisePeerAddr, etag, election.Fence{Rev: rev, Seq: pos.Seq, Term: pos.Term}, complete, start, ttl)
		tCancel()
		if errors.Is(err, object.ErrPreconditionFailed) {
			stepDown(reason, "renewal precondition failed — lock taken")
			return false
		}
		if err != nil {
			n.log.Warnf("t4: leader watch (%s): renew lock: %v", reason, err)
			return true
		}
		n.extendLease(start, slow)
		n.flagWritten(complete)
		n.fenced(seq)
		return true
	}

	var lastAttempt time.Time
	if !renew("start", false) {
		return
	}
	lastAttempt = time.Now()
	for {
		reason := ""
		select {
		case <-check.C:
		case <-disconnectC:
			reason = "disconnect"
		case <-n.fenceReqC:
			reason = "fence"
		case <-ctx.Done():
			return
		}
		now := time.Now()
		slow := n.slowSafe(now)
		g := n.lease.Load()
		interval := fastRenewInterval
		if slow {
			interval = slowInterval
		}
		switch {
		case now.Sub(lastAttempt) >= interval:
			if reason == "" {
				reason = "renew"
			}
		case g != nil && g.slow && !slow:
			// The last renewal was slow but a follower may no longer
			// hear this leader: the lease now runs from that renewal
			// for fastTTL only and may have ended. Renew at once.
			reason = "fast mode"
		case g != nil && !g.slow && slow:
			// Entering slow mode, the next renewal is slowInterval
			// away, but the lease from the last (fast) renewal ends
			// fastTTL after it. Renew now to obtain a slow lease.
			reason = "slow mode"
		case n.fencePending():
			reason = "fence"
		default:
			continue
		}
		if now.Sub(lastAttempt) < leaderCheckInterval {
			continue // at most one attempt per check interval
		}
		lastAttempt = now
		if !renew(reason, slow) {
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
		// A follower's linearizable read relies on this answer, so it
		// needs the same lease a local read does.
		if err := n.checkLease(); err != nil {
			return nil, err
		}
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

	case peer.ForwardMetaPut:
		err := n.MetaPut(ctx, req.Key, req.Value)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardMetaDelete:
		err := n.MetaDelete(ctx, req.Key)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardGetSequence:
		// Every acknowledged write has been applied before its caller is
		// released, so the applied sequence covers all of them. Meta writes
		// have no optimistic pending state, unlike ForwardGetRevision.
		return &peer.ForwardResponse{Revision: n.db.Load().LastSequence(), Succeeded: true}, nil

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

	// Tracks the last durability mode pushed to the WAL so the toggle only
	// fires on transitions rather than on every batch.
	degraded := false

	// Wait mode none opts out of replication durability, and with it of the
	// election fence (lease.go).
	fenceWrites := peer.WaitMode(n.cfg.FollowerWaitMode) != peer.WaitNone

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

	// Per-batch scratch, reused across iterations. Nothing downstream keeps
	// these slices past the batch; see the clear at the bottom of the loop.
	var (
		batch     []*writeReq
		entries   []*wal.Entry
		dbEntries []wal.Entry
	)

	for {
		// Block until at least one request arrives.
		batch = batch[:0]
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
		batchLen := int32(len(batch)) // batch is reused; don't read it from these goroutines
		for _, req := range batch {
			r := req
			go func() {
				select {
				case <-r.ctx.Done():
					if abandoned.Add(1) == batchLen {
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
		entries = entries[:0]
		for i, req := range batch {
			req.entry.ID = baseSeq + int64(i) + 1
			entries = append(entries, &req.entry)
		}

		// Decide this batch's durability before appending it. A quorum ACK is
		// what normally makes a write survive this node, so when too few
		// followers are connected to produce one, the write has to reach
		// object storage before it is acknowledged instead. Without this the
		// leader would keep acknowledging writes whose only copy is a local
		// disk it was never promised would outlive the process.
		if want := n.replicationDegraded(); want != degraded {
			if setter, ok := n.wal.(interface{ SetSyncUpload(bool) }); ok {
				setter.SetSyncUpload(want)
				degraded = want
				metrics.ReplicationDegraded.Set(boolToFloat(want))
				if want {
					n.log.Warnf("t4: replication below ACK target — flushing each batch to object storage before acknowledging")
				} else {
					n.log.Infof("t4: replication restored — resuming asynchronous WAL upload")
				}
			}
		}
		// A batch that is not uploaded before it is acknowledged ends object
		// storage holding every acknowledged write: clear the flag before
		// appending it, and acknowledge it only once the lock says so.
		if !degraded {
			n.objectStoreComplete.Store(false)
		}

		// Append locally before exposing IDs or payloads to followers. This
		// sacrifices a small amount of fsync/network overlap, but ensures failed
		// attempts never enter the peer replay buffer under reusable IDs.
		walStart := time.Now()
		err := n.wal.AppendBatch(batchCtx, entries)
		walEnd := time.Now()
		if err == nil && degraded {
			// A synchronous upload uploads every earlier segment still pending
			// before this one, so object storage now holds every write this
			// leader acknowledged, and this batch.
			n.objectStoreComplete.Store(true)
		}
		var quorumStart, quorumEnd time.Time
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
			quorumStart = time.Now()
			if waitErr := n.peerSrv.WaitForFollowers(ctx, maxRev, peer.WaitMode(n.cfg.FollowerWaitMode)); waitErr != nil {
				err = waitErr
			}
			// Election fence (lease.go): give every connected follower a
			// moment to catch up before resorting to a lock write.
			if err == nil && fenceWrites && n.fenceNeeded(maxRev) {
				n.peerSrv.WaitForAll(ctx, maxRev, fenceAckWait)
			}
			quorumEnd = time.Now()
		}
		batchCancel() // release watcher goroutines

		// Apply all entries to Pebble as one batch (in order).
		if err == nil {
			dbEntries = dbEntries[:0]
			for _, req := range batch {
				dbEntries = append(dbEntries, req.entry)
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
		n.clearPendingBatch(batch, err)

		// A node that may lack this batch must be fenced out of elections
		// before the batch is acknowledged. The batch is committed either
		// way; a failure only withholds the acknowledgement.
		ackErr := err
		if err == nil && fenceWrites && n.peerSrv != nil && (n.fenceNeeded(batch[len(batch)-1].entry.Sequence()) || n.flagStale()) {
			last := batch[len(batch)-1].entry
			ackErr = n.awaitFence(ctx, last.Sequence(), last.Revision, last.Term)
		}

		// Signal all callers, stamping the phase timings first. await turns
		// these into child spans of the caller's span; writing them here and
		// reading them there is safe because the done send below establishes
		// the happens-before.
		for _, req := range batch {
			req.walStart, req.walEnd = walStart, walEnd
			req.quorumStart, req.quorumEnd = quorumStart, quorumEnd
			req.batchSize = len(batch)
			req.done <- ackErr
		}
		// Drop references so reused scratch doesn't keep finished requests
		// and their values alive until the next batch overwrites them.
		clear(batch)
		clear(entries)
		clear(dbEntries)
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

// replicationDegraded reports whether this leader must upload each batch to
// object storage before acknowledging it.
//
// With no follower connected it always must, whatever WALSyncUpload says:
// object storage is then the only other copy of a write, and a candidate
// catches up from it only while it holds every acknowledged write
// (docs/design/takeover-fence.md). Wait mode none opts out of replication
// durability, and with it of this.
//
// With some followers connected but fewer than cfg.FollowerWaitMode's ACK
// target, WALSyncUpload decides: that flag is the existing knob for "block on
// object storage so an acknowledged write survives losing this disk", and an
// operator who turned it off for a durable volume has already answered this
// question. Every write is still acknowledged by a follower then.
func (n *Node) replicationDegraded() bool {
	if n.peerSrv == nil || n.cfg.ObjectStore == nil {
		return false
	}
	mode := peer.WaitMode(n.cfg.FollowerWaitMode)
	if mode != peer.WaitNone && n.peerSrv.ConnectedFollowers() == 0 {
		return true
	}
	if n.cfg.WALSyncUpload == nil || !*n.cfg.WALSyncUpload {
		return false
	}
	return !n.peerSrv.ReplicationSatisfiable(mode)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
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
//
// The List-then-skip below is a bandwidth optimisation, not a correctness
// guard: the outgoing leader's uploadLoop may publish a segment between the
// List and our Put. Safety comes from makeUploader's conditional write, which
// keeps whichever copy was published first.
//
// A segment whose key is taken by different entries is skipped, as a listed
// one is: a follower cuts segments at its own boundaries, so its copy of a key
// can legitimately differ from the leader's. Entries it holds past the
// published object are in Pebble, and the new leader's first checkpoint makes
// them durable.
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
			if errors.Is(err, wal.ErrSegmentConflict) {
				log.Warnf("t4: local WAL segment %q differs from published %q — keeping the published object", path, objKey)
				continue
			}
			return fmt.Errorf("upload %q to %q: %w", path, objKey, err)
		}
	}
	return nil
}

// ── Background checkpoint loop ────────────────────────────────────────────────

// checkpointShutdownWait bounds how long graceful leader shutdown waits for an
// in-flight checkpoint upload before giving up on a clean handoff.
const checkpointShutdownWait = 30 * time.Second

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
	n.checkpointMu.Lock()
	defer n.checkpointMu.Unlock()
	n.runCheckpoint(ctx)
}

// runCheckpoint pins the store content point-in-time under the write fence —
// WAL seal + Pebble flush + Pebble checkpoint copy to a local temp dir — then
// releases the fence and runs the entire object-store upload off that copy.
// The fence window is limited to local I/O: writes admitted after the copy is
// made only extend the WAL beyond the pinned sequence and cannot enter this
// checkpoint, so their latency is unaffected by object-store round trips.
//
// Returns the pinned WAL sequence and true only when a checkpoint was fully
// written; the caller may then GC object-store state covered by that sequence.
//
// The caller must hold checkpointMu. The fence no longer serializes
// checkpoints, so checkpointMu does: pin order == upload order keeps
// manifest/latest monotonic. Once the node is closed no checkpoint starts, so
// gracefulLeaderShutdown only has to wait out the one in flight.
func (n *Node) runCheckpoint(ctx context.Context) (int64, bool) {
	if n.closed.Load() {
		return 0, false
	}
	n.fenceMu.Lock()
	db := n.db.Load()
	rev := db.CurrentRevision()
	if rev == 0 {
		n.fenceMu.Unlock()
		return 0, false
	}
	seq := db.LastSequence()
	if err := n.wal.SealAndFlush(seq + 1); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: checkpoint seal WAL: %v", err)
		return 0, false
	}
	if err := db.Flush(); err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: checkpoint flush pebble: %v", err)
		return 0, false
	}
	// Decided under the fence, so it matches the pinned copy below.
	cpFormat, err := n.checkpointFormat()
	if err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: checkpoint format: %v", err)
		return 0, false
	}
	tmpDir, err := os.MkdirTemp("", "t4-checkpoint-*")
	if err != nil {
		n.fenceMu.Unlock()
		n.log.Errorf("t4: checkpoint mktemp: %v", err)
		return 0, false
	}
	cpDir := filepath.Join(tmpDir, "cp")
	if err := db.Pebble().Checkpoint(cpDir); err != nil {
		n.fenceMu.Unlock()
		_ = os.RemoveAll(tmpDir)
		n.log.Errorf("t4: checkpoint pebble copy: %v", err)
		return 0, false
	}
	// Writes bump entriesSinceCheckpoint while holding fenceMu.RLock, so this
	// is exactly the number of entries the pinned copy covers.
	covered := atomic.LoadInt64(&n.entriesSinceCheckpoint)
	n.fenceMu.Unlock()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	if n.sstUploader != nil {
		n.sstUploader.Wait()
		if err := n.cp.WriteDirWithRegistry(ctx, cpDir, n.cfg.ObjectStore, n.term, rev, seq, "", n.sstUploader.Registry(), n.sstUploader.InheritedRegistry(), cpFormat); err != nil {
			n.log.Errorf("t4: write checkpoint rev=%d: %v", rev, err)
			return 0, false
		}
	} else if err := n.cp.WriteDir(ctx, cpDir, n.cfg.ObjectStore, n.term, rev, seq, "", n.cfg.AncestorStore, cpFormat); err != nil {
		n.log.Errorf("t4: write checkpoint rev=%d: %v", rev, err)
		return 0, false
	}
	// Only discount the covered entries once the checkpoint is durable: a
	// failed upload leaves the counter non-zero so the next tick retries, and
	// writes admitted during the upload still count toward the next one.
	atomic.AddInt64(&n.entriesSinceCheckpoint, -covered)
	metrics.CheckpointsTotal.Inc()
	n.log.Infof("t4: checkpoint written (rev=%d)", rev)
	return seq, true
}

// checkpointFormat returns the checkpoint format needed to represent the
// current store: the meta keyspace requires FormatVersionMeta so that binaries
// predating it refuse the checkpoint rather than restore without that state.
func (n *Node) checkpointFormat() (uint32, error) {
	hasMeta, err := n.db.Load().HasMeta()
	if err != nil {
		return 0, err
	}
	if hasMeta {
		return checkpoint.FormatVersionMeta, nil
	}
	return checkpoint.FormatVersionBase, nil
}

func (n *Node) maybeCheckpoint(ctx context.Context) {
	// Held through GC too, so GC cannot delete SSTs a concurrent checkpoint
	// is about to reference.
	n.checkpointMu.Lock()
	defer n.checkpointMu.Unlock()
	if atomic.LoadInt64(&n.entriesSinceCheckpoint) == 0 {
		return
	}
	seq, ok := n.runCheckpoint(ctx)
	if !ok {
		return
	}

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
