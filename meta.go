package t4

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/t4db/t4/internal/peer"
	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/internal/wal"
)

// The meta keyspace holds unversioned node metadata next to the revisioned
// key-value data: a meta write has no history, produces no watch event, and
// does not advance the revision. It is replicated through the WAL like any
// other write and survives checkpoints, restores, and branches.
//
// Meta writes require WAL format 3, which older binaries refuse to read. Only
// databases created by a release that supports them have the meta keyspace
// enabled (see initMetaAtGenesis); in older databases meta writes fail with
// ErrMetaDisabled.

// metaFormatKey marks a database whose WAL may contain meta ops. It is the
// first entry of such a database and is reserved: user writes to it are
// rejected.
const metaFormatKey = "\x00t4/format"

// MetaKV is one entry of the meta keyspace.
type MetaKV struct {
	Key   string
	Value []byte
}

// MetaPut sets key to value in the meta keyspace. It returns ErrMetaDisabled
// until the database has been upgraded to support meta writes.
func (n *Node) MetaPut(ctx context.Context, key string, value []byte) error {
	if err := validateMetaKey(key); err != nil {
		return err
	}
	return n.writeMeta(ctx, wal.OpMetaPut, key, value)
}

// MetaDelete removes key from the meta keyspace. Deleting a missing key is not
// an error. It returns ErrMetaDisabled until the database has been upgraded to
// support meta writes.
func (n *Node) MetaDelete(ctx context.Context, key string) error {
	if err := validateMetaKey(key); err != nil {
		return err
	}
	return n.writeMeta(ctx, wal.OpMetaDelete, key, nil)
}

// MetaEnabled reports whether this database accepts meta keyspace writes.
func (n *Node) MetaEnabled() (bool, error) {
	if n.closed.Load() {
		return false, ErrClosed
	}
	_, ok, err := n.db.Load().MetaGet(metaFormatKey)
	return ok, err
}

// MetaDisabled returns a TxnCondition that holds while the meta keyspace is
// not enabled. System state that lives in reserved data keys in databases
// created before the meta keyspace, and in meta keys otherwise, can use it to
// pick the keyspace atomically with the write:
//
//	If: MetaDisabled(), Then: data-keyspace ops, Else: meta ops
func MetaDisabled() TxnCondition {
	return TxnCondition{Key: metaFormatKey, Target: TxnCondMetaExists, Result: TxnCondEqual, Version: 0}
}

// initMetaAtGenesis enables the meta keyspace in a database that has no WAL
// entries yet, so that every database created by this release uses it from
// its first write. Databases created by earlier releases keep their format:
// their lease and auth state stays in revisioned keys, and older releases can
// still open them. The format marker is written with writes paused, so it is
// the database's first entry.
//
// It runs on a leader or single node with the commit loop started, before the
// node serves writes (in Open) or right after promotion.
func (n *Node) initMetaAtGenesis(ctx context.Context) (err error) {
	if testhook.LegacyNewDatabases.Load() {
		return nil
	}
	n.fenceMu.Lock()
	defer n.fenceMu.Unlock()
	if n.closed.Load() {
		return ErrClosed
	}
	if n.db.Load().LastSequence() != 0 {
		return nil
	}

	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return ErrClosed
	}
	e, _, _, _, metaToken, err := n.prepareTxn(TxnRequest{Success: []TxnOp{
		{Type: TxnMetaPut, Key: metaFormatKey, Value: []byte(strconv.Itoa(wal.WALFormatVersion))},
	}}, true)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("t4: enable meta keyspace: %w", err)
	}
	wr := newWriteReq(ctx, e)
	wr.metaToken = metaToken
	n.writeC <- wr
	n.mu.Unlock()
	if _, err := n.await(ctx, wr, "txn", start, "", e.Revision); err != nil {
		return fmt.Errorf("t4: enable meta keyspace: %w", err)
	}
	n.log.Infof("t4: new database created with the meta keyspace (WAL format %d)", wal.WALFormatVersion)
	return nil
}

func validateMetaKey(key string) error {
	if key == "" {
		return errors.New("t4: meta key must not be empty")
	}
	if key == metaFormatKey {
		return fmt.Errorf("t4: meta key %q is reserved", key)
	}
	return nil
}

func (n *Node) writeMeta(ctx context.Context, op wal.Op, key string, value []byte) (err error) {
	ctx, span := n.tracer.Start(ctx, "t4."+opLabel(op))
	defer func() { endSpan(span, err) }()

	if n.closed.Load() {
		return ErrClosed
	}
	if n.loadRole() == roleFollower {
		fop := peer.ForwardMetaPut
		if op == wal.OpMetaDelete {
			fop = peer.ForwardMetaDelete
		}
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: fop, Key: key, Value: value})
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
	if err := n.requireMetaLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	// Like Compact, a meta entry carries the last assigned revision without
	// consuming a new one.
	e := wal.Entry{Revision: n.nextRev, Term: n.term, Op: op, Key: key, Value: value}
	metaToken, err := n.trackPendingMetaLocked(&e)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	req := newWriteReq(ctx, e)
	req.metaToken = metaToken
	n.writeC <- req
	n.mu.Unlock()
	_, err = n.await(ctx, req, opLabel(op), start, "", e.Revision)
	return err
}

// requireMetaLocked returns ErrMetaDisabled unless meta writes are enabled.
// Must be called under n.mu on the leader.
func (n *Node) requireMetaLocked() error {
	ok, err := n.metaExistsLocked(metaFormatKey)
	if err != nil {
		return err
	}
	if !ok {
		return ErrMetaDisabled
	}
	return nil
}

// metaExistsLocked reports whether key exists in the meta keyspace, including
// in-flight writes not yet applied. Must be called under n.mu on the leader.
func (n *Node) metaExistsLocked(key string) (bool, error) {
	if p, ok := n.pendingMeta[key]; ok {
		return !p.deleted, nil
	}
	_, ok, err := n.db.Load().MetaGet(key)
	return ok, err
}

// trackPendingMetaLocked records e's meta ops in pendingMeta under a fresh
// token and returns it, or 0 when e has no meta ops. Must be called under n.mu.
func (n *Node) trackPendingMetaLocked(e *wal.Entry) (uint64, error) {
	var ops []wal.TxnSubOp
	switch {
	case e.Op.IsMeta():
		ops = []wal.TxnSubOp{{Op: e.Op, Key: e.Key}}
	case e.Op == wal.OpTxn:
		all, err := wal.DecodeTxnOps(e.Value)
		if err != nil {
			return 0, err
		}
		ops = all
	}
	var token uint64
	for _, op := range ops {
		if !op.Op.IsMeta() {
			continue
		}
		if token == 0 {
			n.metaTokenSeq++
			token = n.metaTokenSeq
		}
		n.pendingMeta[op.Key] = pendingMeta{token: token, deleted: op.Op == wal.OpMetaDelete}
	}
	return token, nil
}

// clearPendingMetaLocked drops key's pendingMeta entry if token still owns it;
// a newer in-flight write to the same key keeps its entry. Must be called
// under n.mu.
func (n *Node) clearPendingMetaLocked(key string, token uint64) {
	if p, ok := n.pendingMeta[key]; ok && p.token == token {
		delete(n.pendingMeta, key)
	}
}

// MetaGet returns the value of key in the meta keyspace from local state; ok
// is false when the key does not exist. On a follower the result may lag the
// leader; use LinearizableMetaGet for an up-to-date read.
func (n *Node) MetaGet(key string) (value []byte, ok bool, err error) {
	if n.closed.Load() {
		return nil, false, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, false, ErrClosed
	}
	return n.db.Load().MetaGet(key)
}

// MetaList returns the meta keyspace entries whose key starts with prefix,
// sorted by key, from local state.
func (n *Node) MetaList(prefix string) ([]MetaKV, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}
	kvs, err := n.db.Load().MetaList(prefix)
	if err != nil {
		return nil, err
	}
	out := make([]MetaKV, len(kvs))
	for i, kv := range kvs {
		out[i] = MetaKV(kv)
	}
	return out, nil
}

// LinearizableMetaGet is MetaGet with linearizability guaranteed: on a
// follower it first catches up with every write the leader has applied.
func (n *Node) LinearizableMetaGet(ctx context.Context, key string) ([]byte, bool, error) {
	ctx, span := n.tracer.Start(ctx, "t4.meta_get")
	defer span.End()
	if err := n.syncMetaWithLeader(ctx); err != nil {
		span.RecordError(err)
		return nil, false, err
	}
	return n.MetaGet(key)
}

// LinearizableMetaList is MetaList with linearizability guaranteed.
func (n *Node) LinearizableMetaList(ctx context.Context, prefix string) ([]MetaKV, error) {
	ctx, span := n.tracer.Start(ctx, "t4.meta_list")
	defer span.End()
	if err := n.syncMetaWithLeader(ctx); err != nil {
		span.RecordError(err)
		return nil, err
	}
	return n.MetaList(prefix)
}

// syncMetaWithLeader is the ReadIndex step for meta reads. Meta writes do not
// advance the revision, so syncWithLeader's revision wait would not observe
// them; this waits for the leader's applied WAL sequence instead.
func (n *Node) syncMetaWithLeader(ctx context.Context) error {
	cli, err := n.readIndexClient()
	if cli == nil || err != nil {
		return err
	}
	resp, err := cli.ForwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardGetSequence})
	if err != nil {
		if isLeaderUnavailable(err) {
			return ErrNoLeader
		}
		return fmt.Errorf("t4: meta read sync: %w", err)
	}
	if err := n.waitForSequence(ctx, resp.Revision); err != nil {
		return fmt.Errorf("t4: meta read sync: wait for local sequence %d: %w", resp.Revision, err)
	}
	return nil
}

func (n *Node) waitForSequence(ctx context.Context, seq int64) error {
	if n.closed.Load() {
		return ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return ErrClosed
	}
	if err := n.db.Load().WaitForSequence(ctx, seq); err != nil {
		if errors.Is(err, istore.ErrClosed) {
			return ErrClosed
		}
		return err
	}
	return nil
}
