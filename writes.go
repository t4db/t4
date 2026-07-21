package t4

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/peer"
	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/wal"
)

// ── Write path (leader / single-node execute; follower forwards) ──────────────

// Put creates or updates key with value. Returns the new revision.
func (n *Node) Put(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if n.closed.Load() {
		return 0, ErrClosed
	}
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardPut, Key: key, Value: value, Lease: lease})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, ErrClosed
	}
	e, err := n.preparePut(key, value, lease)
	if err != nil {
		n.mu.Unlock()
		return 0, err
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	return n.await(ctx, req, opLabel(e.Op), start, key, e.Revision)
}

// preparePut builds the WAL entry for a Put. Must be called under n.mu.
func (n *Node) preparePut(key string, value []byte, lease int64) (wal.Entry, error) {
	existing, err := n.readKey(key)
	if err != nil {
		return wal.Entry{}, err
	}
	n.nextRev++
	newRev := n.nextRev
	var op wal.Op
	var createRev, prevRev int64
	if existing == nil {
		op, createRev = wal.OpCreate, newRev
	} else {
		op, createRev, prevRev = wal.OpUpdate, existing.CreateRevision, existing.Revision
	}
	n.pending[key] = pendingKV{
		rev: newRev,
		kv: &istore.KeyValue{
			Key: key, Value: value, Revision: newRev,
			CreateRevision: createRev, PrevRevision: prevRev,
			Version: nextPutVersion(existing), Lease: lease,
		},
	}
	version := n.pending[key].kv.Version
	return wal.Entry{
		Revision: newRev, Term: n.term, Op: op,
		Key: key, Value: value, Lease: lease,
		CreateRevision: createRev, PrevRevision: prevRev, Version: version,
	}, nil
}

// Create creates key only if it does not already exist.
func (n *Node) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardCreate, Key: key, Value: value, Lease: lease})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, ErrClosed
	}
	existing, err := n.readKey(key)
	if err != nil {
		n.mu.Unlock()
		return 0, err
	}
	if existing != nil {
		n.mu.Unlock()
		return 0, ErrKeyExists
	}
	n.nextRev++
	newRev := n.nextRev
	n.pending[key] = pendingKV{
		rev: newRev,
		kv: &istore.KeyValue{
			Key: key, Value: value, Revision: newRev,
			CreateRevision: newRev, Version: 1, Lease: lease,
		},
	}
	e := wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpCreate,
		Key: key, Value: value, Lease: lease, CreateRevision: newRev, Version: 1,
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	return n.await(ctx, req, "create", start, key, newRev)
}

// Update updates key only if its current revision matches (CAS).
func (n *Node) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardUpdate, Key: key, Value: value, Revision: revision, Lease: lease})
		if err != nil {
			return 0, nil, false, err
		}
		return resp.Revision, msgToKV(resp.OldKV), resp.Succeeded, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, nil, false, ErrClosed
	}
	existing, err := n.readKey(key)
	if err != nil {
		n.mu.Unlock()
		return 0, nil, false, err
	}
	if existing == nil || existing.Revision != revision {
		curRev := n.db.Load().CurrentRevision()
		n.mu.Unlock()
		return curRev, toKV(existing), false, nil
	}
	n.nextRev++
	newRev := n.nextRev
	n.pending[key] = pendingKV{
		rev: newRev,
		kv: &istore.KeyValue{
			Key: key, Value: value, Revision: newRev,
			CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
			Version: kvVersion(existing) + 1, Lease: lease,
		},
	}
	version := n.pending[key].kv.Version
	e := wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpUpdate,
		Key: key, Value: value, Lease: lease,
		CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision, Version: version,
	}
	oldKV := toKV(existing)
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	newRev, err = n.await(ctx, req, "update", start, key, newRev)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, oldKV, true, nil
}

// Delete removes key unconditionally.
func (n *Node) Delete(ctx context.Context, key string) (int64, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardDeleteIfRevision, Key: key, Revision: 0})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, ErrClosed
	}
	e, err := n.prepareDelete(key)
	if err != nil || e.Key == "" {
		n.mu.Unlock()
		return 0, err // key not found — no-op
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	return n.await(ctx, req, "delete", start, key, e.Revision)
}

// DeleteIfRevision deletes key only if its current revision matches (CAS).
func (n *Node) DeleteIfRevision(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardDeleteIfRevision, Key: key, Revision: revision})
		if err != nil {
			return 0, nil, false, err
		}
		return resp.Revision, msgToKV(resp.OldKV), resp.Succeeded, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, nil, false, ErrClosed
	}
	existing, err := n.readKey(key)
	if err != nil {
		n.mu.Unlock()
		return 0, nil, false, err
	}
	curRev := n.db.Load().CurrentRevision()
	if existing == nil {
		n.mu.Unlock()
		return curRev, nil, false, nil
	}
	if revision != 0 && existing.Revision != revision {
		n.mu.Unlock()
		return curRev, toKV(existing), false, nil
	}
	oldKV := toKV(existing)
	e, err := n.prepareDelete(key)
	if err != nil || e.Key == "" {
		n.mu.Unlock()
		return 0, nil, false, err
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	newRev, err := n.await(ctx, req, "delete", start, key, e.Revision)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, oldKV, true, nil
}

// prepareDelete builds a WAL delete entry for key. Must be called under n.mu.
// Returns a zero-valued entry (Key=="") if the key does not exist.
func (n *Node) prepareDelete(key string) (wal.Entry, error) {
	existing, err := n.readKey(key)
	if err != nil || existing == nil {
		return wal.Entry{}, err
	}
	n.nextRev++
	newRev := n.nextRev
	n.pending[key] = pendingKV{rev: newRev, deleted: true}
	return wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpDelete,
		Key: key, CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
		Version: kvVersion(existing),
	}, nil
}

// readKey returns the current value of key, checking in-flight pending writes
// first, then falling back to Pebble. Must be called under n.mu.
func (n *Node) readKey(key string) (*istore.KeyValue, error) {
	if p, ok := n.pending[key]; ok {
		if p.deleted {
			return nil, nil
		}
		// pending is an optimistic local read path: once a write has been
		// assigned a revision under n.mu, concurrent writes to the same key
		// observe that in-flight state before the batch is durably applied.
		return p.kv, nil
	}
	return n.db.Load().Get(key)
}

// txnCondMatches reports whether existing satisfies cond.
// existing is nil when the key does not exist.
func txnCondMatches(cond TxnCondition, existing *istore.KeyValue) bool {
	var (
		modRev    int64
		createRev int64
		version   int64
		lease     int64
		val       []byte
	)
	if existing != nil {
		modRev = existing.Revision
		createRev = existing.CreateRevision
		version = kvVersion(existing)
		lease = existing.Lease
		val = existing.Value
	}

	var lhs, rhs int64
	switch cond.Target {
	case TxnCondMod:
		lhs, rhs = modRev, cond.ModRevision
	case TxnCondVersion:
		lhs, rhs = version, cond.Version
	case TxnCondCreate:
		lhs, rhs = createRev, cond.CreateRevision
	case TxnCondLease:
		lhs, rhs = lease, cond.Lease
	case TxnCondValue:
		switch cond.Result {
		case TxnCondEqual:
			return string(val) == string(cond.Value)
		case TxnCondNotEqual:
			return string(val) != string(cond.Value)
		default:
			return false // ordering on Value bytes not supported
		}
	default:
		return false
	}
	switch cond.Result {
	case TxnCondEqual:
		return lhs == rhs
	case TxnCondNotEqual:
		return lhs != rhs
	case TxnCondGreater:
		return lhs > rhs
	case TxnCondLess:
		return lhs < rhs
	default:
		return false
	}
}

// Txn executes an atomic multi-key transaction.
//
// All Conditions are evaluated under the write lock in a single atomic step.
// If every condition is satisfied the Success ops are applied and Succeeded is
// true; otherwise the Failure ops are applied and Succeeded is false.
// Either branch may be empty — if no ops need to be written the method returns
// immediately with the current revision.
//
// All write ops within the selected branch share a single revision and are
// committed to the WAL in one entry, ensuring crash-safe atomicity.
func (n *Node) Txn(ctx context.Context, req TxnRequest) (TxnResponse, error) {
	if n.closed.Load() {
		return TxnResponse{}, ErrClosed
	}
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, txnToForwardRequest(req))
		if err != nil {
			return TxnResponse{}, err
		}
		deletedKeys := make(map[string]struct{}, len(resp.DeletedKeys))
		for _, k := range resp.DeletedKeys {
			deletedKeys[k] = struct{}{}
		}
		return TxnResponse{
			Succeeded:   resp.Succeeded,
			Revision:    resp.Revision,
			DeletedKeys: deletedKeys,
		}, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return TxnResponse{}, ErrClosed
	}
	e, succeeded, deletedKeys, err := n.prepareTxn(req)
	if err != nil {
		n.mu.Unlock()
		return TxnResponse{}, err
	}
	if e.Op == 0 {
		// No-op branch: conditions evaluated but no writes needed.
		// Use the committed revision, not nextRev which may be ahead of what
		// the commit loop has fsynced if concurrent writes are in flight.
		curRev := n.db.Load().CurrentRevision()
		n.mu.Unlock()
		return TxnResponse{Succeeded: succeeded, Revision: curRev}, nil
	}
	wr := newWriteReq(ctx, e)
	n.writeC <- wr
	n.mu.Unlock()
	rev, err := n.await(ctx, wr, "txn", start, "", e.Revision)
	if err != nil {
		return TxnResponse{}, err
	}
	return TxnResponse{Succeeded: succeeded, Revision: rev, DeletedKeys: deletedKeys}, nil
}

// txnToForwardRequest converts a TxnRequest into a peer ForwardRequest for
// follower-to-leader forwarding.
func txnToForwardRequest(req TxnRequest) *peer.ForwardRequest {
	conds := make([]peer.TxnCondMsg, len(req.Conditions))
	for i, c := range req.Conditions {
		conds[i] = peer.TxnCondMsg{
			Key:            c.Key,
			Target:         uint8(c.Target),
			Result:         uint8(c.Result),
			ModRevision:    c.ModRevision,
			CreateRevision: c.CreateRevision,
			Version:        c.Version,
			Value:          c.Value,
			Lease:          c.Lease,
		}
	}
	success := txnOpsToMsg(req.Success)
	failure := txnOpsToMsg(req.Failure)
	return &peer.ForwardRequest{
		Op: peer.ForwardTxn,
		TxnReq: &peer.TxnReqMsg{
			Conditions: conds,
			Success:    success,
			Failure:    failure,
		},
	}
}

func txnOpsToMsg(ops []TxnOp) []peer.TxnOpMsg {
	if len(ops) == 0 {
		return nil
	}
	msgs := make([]peer.TxnOpMsg, len(ops))
	for i, op := range ops {
		msgs[i] = peer.TxnOpMsg{
			Type:  uint8(op.Type),
			Key:   op.Key,
			Value: op.Value,
			Lease: op.Lease,
		}
	}
	return msgs
}

// forwardMsgToTxnRequest converts a peer TxnReqMsg back to a TxnRequest.
func forwardMsgToTxnRequest(m *peer.TxnReqMsg) TxnRequest {
	conds := make([]TxnCondition, len(m.Conditions))
	for i, c := range m.Conditions {
		conds[i] = TxnCondition{
			Key:            c.Key,
			Target:         TxnCondTarget(c.Target),
			Result:         TxnCondResult(c.Result),
			ModRevision:    c.ModRevision,
			CreateRevision: c.CreateRevision,
			Version:        c.Version,
			Value:          c.Value,
			Lease:          c.Lease,
		}
	}
	return TxnRequest{
		Conditions: conds,
		Success:    msgToTxnOps(m.Success),
		Failure:    msgToTxnOps(m.Failure),
	}
}

func msgToTxnOps(msgs []peer.TxnOpMsg) []TxnOp {
	if len(msgs) == 0 {
		return nil
	}
	ops := make([]TxnOp, len(msgs))
	for i, m := range msgs {
		ops[i] = TxnOp{
			Type:  TxnOpType(m.Type),
			Key:   m.Key,
			Value: m.Value,
			Lease: m.Lease,
		}
	}
	return ops
}

// prepareTxn evaluates all conditions and prepares the WAL entry for the
// selected branch. Must be called under n.mu.
//
// Returns a zero-valued Entry (Op==0) when the branch has no write ops.
func (n *Node) prepareTxn(req TxnRequest) (wal.Entry, bool, map[string]struct{}, error) {
	// Evaluate conditions.
	succeeded := true
	for _, cond := range req.Conditions {
		existing, err := n.readKey(cond.Key)
		if err != nil {
			return wal.Entry{}, false, nil, err
		}
		if !txnCondMatches(cond, existing) {
			succeeded = false
			break
		}
	}

	ops := req.Success
	if !succeeded {
		ops = req.Failure
	}
	if len(ops) == 0 {
		return wal.Entry{}, succeeded, nil, nil
	}

	// Pre-resolve all ops (read current state) before incrementing nextRev,
	// so that a lookup error cannot leave nextRev in a skipped state.
	type resolvedOp struct {
		walOp          wal.Op
		key            string
		value          []byte
		lease          int64
		createRevision int64
		prevRevision   int64
		version        int64
		skip           bool // delete of a non-existent key
	}
	resolved := make([]resolvedOp, 0, len(ops))
	for _, op := range ops {
		existing, err := n.readKey(op.Key)
		if err != nil {
			return wal.Entry{}, false, nil, err
		}
		switch op.Type {
		case TxnPut:
			var walOp wal.Op
			var createRev, prevRev, version int64
			if existing == nil {
				walOp = wal.OpCreate
				version = 1
			} else {
				walOp = wal.OpUpdate
				createRev = existing.CreateRevision
				prevRev = existing.Revision
				version = kvVersion(existing) + 1
			}
			resolved = append(resolved, resolvedOp{
				walOp: walOp, key: op.Key, value: op.Value, lease: op.Lease,
				createRevision: createRev, prevRevision: prevRev, version: version,
			})
		case TxnDelete:
			if existing == nil {
				resolved = append(resolved, resolvedOp{skip: true})
				continue
			}
			resolved = append(resolved, resolvedOp{
				walOp: wal.OpDelete, key: op.Key,
				createRevision: existing.CreateRevision, prevRevision: existing.Revision,
				version: kvVersion(existing),
			})
		}
	}

	// Drop skipped ops; if nothing remains this is effectively a no-op.
	active := resolved[:0]
	for _, r := range resolved {
		if !r.skip {
			active = append(active, r)
		}
	}
	if len(active) == 0 {
		return wal.Entry{}, succeeded, nil, nil
	}

	if len(active) > 65535 {
		return wal.Entry{}, false, nil, fmt.Errorf("txn: too many ops (%d), maximum is 65535", len(active))
	}

	seen := make(map[string]struct{}, len(active))
	for _, r := range active {
		if _, dup := seen[r.key]; dup {
			return wal.Entry{}, false, nil, fmt.Errorf("txn: duplicate key %q in branch", r.key)
		}
		seen[r.key] = struct{}{}
	}

	n.nextRev++
	newRev := n.nextRev

	var deletedKeys map[string]struct{}
	subOps := make([]wal.TxnSubOp, len(active))
	for i, r := range active {
		cr := r.createRevision
		if r.walOp == wal.OpCreate {
			cr = newRev
		}
		subOps[i] = wal.TxnSubOp{
			Op: r.walOp, Key: r.key, Value: r.value, Lease: r.lease,
			CreateRevision: cr, PrevRevision: r.prevRevision, Version: r.version,
		}
		if r.walOp == wal.OpDelete {
			n.pending[r.key] = pendingKV{rev: newRev, deleted: true}
			if deletedKeys == nil {
				deletedKeys = make(map[string]struct{})
			}
			deletedKeys[r.key] = struct{}{}
		} else {
			n.pending[r.key] = pendingKV{
				rev: newRev,
				kv: &istore.KeyValue{
					Key: r.key, Value: r.value, Revision: newRev,
					CreateRevision: cr, PrevRevision: r.prevRevision,
					Version: r.version, Lease: r.lease,
				},
			}
		}
	}

	return wal.Entry{
		Revision: newRev,
		Term:     n.term,
		Op:       wal.OpTxn,
		Value:    wal.EncodeTxnOps(subOps),
	}, succeeded, deletedKeys, nil
}

func opLabel(op wal.Op) string {
	switch op {
	case wal.OpCreate:
		return "create"
	case wal.OpUpdate:
		return "update"
	case wal.OpDelete:
		return "delete"
	case wal.OpCompact:
		return "compact"
	case wal.OpTxn:
		return "txn"
	default:
		return "unknown"
	}
}

// await waits for a commit-loop result. req must have been sent to writeC
// before n.mu was released. key and rev identify the pending map entry to
// remove once the commit completes (pass key=="" for Compact which has none).
func (n *Node) await(ctx context.Context, req *writeReq, op string, start time.Time, key string, rev int64) (int64, error) {
	cleanPending := func() {
		if key != "" {
			n.mu.Lock()
			if p, ok := n.pending[key]; ok && p.rev == rev {
				delete(n.pending, key)
			}
			n.mu.Unlock()
		}
	}
	var err error
	select {
	case err = <-req.done:
	case <-ctx.Done():
		// Give the commit loop a brief window to react: it may have already
		// detected our cancellation (via batchCtx) and is about to signal us
		// with a meaningful error. If it does, use that result instead of
		// ctx.Err() so callers see a system-level error rather than their own
		// deadline expiry.
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case err = <-req.done:
			// commit loop responded promptly; fall through to normal handling
		case <-timer.C:
			go func() { <-req.done; cleanPending() }()
			return 0, ctx.Err()
		}
	}
	cleanPending()
	if err != nil {
		metrics.WriteErrors.WithLabelValues(op).Inc()
		return 0, fmt.Errorf("t4: commit: %w", err)
	}
	metrics.WritesTotal.WithLabelValues(op).Inc()
	metrics.WriteDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	metrics.CurrentRevision.Set(float64(rev))
	count := atomic.AddInt64(&n.entriesSinceCheckpoint, 1)
	if n.checkpointTriggerC != nil && count >= n.cfg.CheckpointEntries {
		select {
		case n.checkpointTriggerC <- struct{}{}:
		default: // already a pending trigger; don't block
		}
	}
	return rev, nil
}

// clearPendingBatch removes optimistic pending entries for one commit-loop
// batch. If a newer write has already reused the same key, the revision guard
// preserves that newer pending entry.
func (n *Node) clearPendingBatch(batch []*writeReq) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, req := range batch {
		if req.entry.Op == wal.OpTxn {
			// For txn entries the key field is empty; decode sub-ops to clear
			// each affected key from the pending map.
			if ops, err := wal.DecodeTxnOps(req.entry.Value); err == nil {
				for _, op := range ops {
					if p, ok := n.pending[op.Key]; ok && p.rev == req.entry.Revision {
						delete(n.pending, op.Key)
					}
				}
			}
			continue
		}
		if req.entry.Key == "" {
			continue
		}
		if p, ok := n.pending[req.entry.Key]; ok && p.rev == req.entry.Revision {
			delete(n.pending, req.entry.Key)
		}
	}
}

// Compact removes log entries at or below revision.
func (n *Node) Compact(ctx context.Context, revision int64) error {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardCompact, Revision: revision})
		if err != nil {
			return err
		}
		return decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return ErrClosed
	}
	e := wal.Entry{
		Revision: n.nextRev, Term: n.term, Op: wal.OpCompact,
		PrevRevision: revision,
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	_, err := n.await(ctx, req, "compact", start, "", e.Revision)
	return err
}

// ── Forward wire encoding helpers ────────────────────────────────────────────

func encodeErr(err error) (code, msg string) {
	if err == nil {
		return "", ""
	}
	if errors.Is(err, ErrKeyExists) {
		return "key_exists", ""
	}
	return "error", err.Error()
}

func decodeErr(code, msg string) error {
	switch code {
	case "":
		return nil
	case "key_exists":
		return ErrKeyExists
	default:
		return errors.New(msg)
	}
}

func kvToMsg(kv *KeyValue) *peer.KVMsg {
	if kv == nil {
		return nil
	}
	return &peer.KVMsg{
		Key: kv.Key, Value: kv.Value, Revision: kv.Revision,
		CreateRevision: kv.CreateRevision, PrevRevision: kv.PrevRevision,
		Version: kv.Version, Lease: kv.Lease,
	}
}

func msgToKV(m *peer.KVMsg) *KeyValue {
	if m == nil {
		return nil
	}
	return &KeyValue{
		Key: m.Key, Value: m.Value, Revision: m.Revision,
		CreateRevision: m.CreateRevision, PrevRevision: m.PrevRevision,
		Version: m.Version, Lease: m.Lease,
	}
}

func kvVersion(kv *istore.KeyValue) int64 {
	if kv == nil {
		return 0
	}
	if kv.Version > 0 {
		return kv.Version
	}
	return 1
}

func nextPutVersion(existing *istore.KeyValue) int64 {
	if existing == nil {
		return 1
	}
	return kvVersion(existing) + 1
}
