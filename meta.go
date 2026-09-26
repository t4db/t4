package t4

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/t4db/t4/internal/peer"
	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/wal"
)

// The meta keyspace holds unversioned node metadata next to the revisioned
// key-value data: a meta write has no history, produces no watch event, and
// does not advance the revision. It is replicated through the WAL like any
// other write and survives checkpoints, restores, and branches.
//
// Meta writes require WAL format 3, which older binaries refuse to read. They
// are therefore rejected with ErrMetaDisabled until the database has been
// explicitly upgraded, which records metaFormatKey.

// metaFormatKey marks a database whose WAL may contain meta ops. It is written
// once, by the upgrade, and is reserved: user writes to it are rejected.
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
	return n.writeMeta(ctx, wal.OpMetaPut, key, value, true)
}

// MetaDelete removes key from the meta keyspace. Deleting a missing key is not
// an error. It returns ErrMetaDisabled until the database has been upgraded to
// support meta writes.
func (n *Node) MetaDelete(ctx context.Context, key string) error {
	if err := validateMetaKey(key); err != nil {
		return err
	}
	return n.writeMeta(ctx, wal.OpMetaDelete, key, nil, true)
}

// MetaEnabled reports whether this database accepts meta keyspace writes.
func (n *Node) MetaEnabled() (bool, error) {
	if n.closed.Load() {
		return false, ErrClosed
	}
	_, ok, err := n.db.Load().MetaGet(metaFormatKey)
	return ok, err
}

// enableMeta records metaFormatKey, after which meta writes are accepted. It
// must run on the leader and is idempotent. Callers are responsible for making
// sure every node that may apply the WAL understands format 3.
func (n *Node) enableMeta(ctx context.Context) error {
	if n.loadRole() == roleFollower {
		return ErrNotLeader
	}
	return n.writeMeta(ctx, wal.OpMetaPut, metaFormatKey, []byte("3"), false)
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

func (n *Node) writeMeta(ctx context.Context, op wal.Op, key string, value []byte, gated bool) (err error) {
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
	if gated {
		if err := n.requireMetaLocked(); err != nil {
			n.mu.Unlock()
			return err
		}
	}
	// Like Compact, a meta entry carries the last assigned revision without
	// consuming a new one.
	e := wal.Entry{Revision: n.nextRev, Term: n.term, Op: op, Key: key, Value: value}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	_, err = n.await(ctx, req, opLabel(op), start, "", e.Revision)
	return err
}

// requireMetaLocked returns ErrMetaDisabled unless meta writes are enabled.
// Must be called under n.mu on the leader.
func (n *Node) requireMetaLocked() error {
	_, ok, err := n.db.Load().MetaGet(metaFormatKey)
	if err != nil {
		return err
	}
	if !ok {
		return ErrMetaDisabled
	}
	return nil
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
