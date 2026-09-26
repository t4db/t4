// Package sysstate stores T4's own bookkeeping (etcd leases, auth users,
// roles, tokens).
//
// Databases created by a release with the meta keyspace keep this state in
// meta keys, where writes consume no revision, as in etcd. Databases created
// by earlier releases keep it in reserved keys of the revisioned data
// keyspace, where every write consumes a revision. A database's mode is fixed
// when it is created (see t4.MetaDisabled); writes still select the keyspace
// with that condition inside the transaction, so a write can never land in
// the wrong one.
package sysstate

import (
	"context"

	"github.com/t4db/t4"
)

// Node is the subset of t4.Node used by this package.
type Node interface {
	Txn(ctx context.Context, req t4.TxnRequest) (t4.TxnResponse, error)
	Get(key string, opts ...t4.ReadOption) (*t4.KeyValue, error)
	List(prefix string, opts ...t4.ReadOption) ([]*t4.KeyValue, error)
	LinearizableGet(ctx context.Context, key string, opts ...t4.ReadOption) (*t4.KeyValue, error)
	LinearizableList(ctx context.Context, prefix string, opts ...t4.ReadOption) ([]*t4.KeyValue, error)
	MetaEnabled() (bool, error)
	MetaGet(key string) ([]byte, bool, error)
	MetaList(prefix string) ([]t4.MetaKV, error)
	LinearizableMetaGet(ctx context.Context, key string) ([]byte, bool, error)
	LinearizableMetaList(ctx context.Context, prefix string) ([]t4.MetaKV, error)
}

// Change is one state write: Value is stored under Key, or Key is removed
// when Delete is set.
type Change struct {
	Key    string
	Value  []byte
	Delete bool
}

// Apply commits ops (ordinary data writes) atomically with the state changes.
func Apply(ctx context.Context, n Node, ops []t4.TxnOp, changes ...Change) error {
	legacy := append([]t4.TxnOp(nil), ops...)
	meta := append([]t4.TxnOp(nil), ops...)
	for _, c := range changes {
		if c.Delete {
			legacy = append(legacy, t4.TxnOp{Type: t4.TxnDelete, Key: c.Key})
			meta = append(meta, t4.TxnOp{Type: t4.TxnMetaDelete, Key: c.Key})
		} else {
			legacy = append(legacy, t4.TxnOp{Type: t4.TxnPut, Key: c.Key, Value: c.Value})
			meta = append(meta, t4.TxnOp{Type: t4.TxnMetaPut, Key: c.Key, Value: c.Value})
		}
	}
	_, err := n.Txn(ctx, t4.TxnRequest{
		Conditions: []t4.TxnCondition{t4.MetaDisabled()},
		Success:    legacy,
		Failure:    meta,
	})
	return err
}

// Put stores value under key.
func Put(ctx context.Context, n Node, key string, value []byte) error {
	return Apply(ctx, n, nil, Change{Key: key, Value: value})
}

// Delete removes key. Removing a missing key is not an error.
func Delete(ctx context.Context, n Node, key string) error {
	return Apply(ctx, n, nil, Change{Key: key, Delete: true})
}

// Create stores value under key only if key does not exist yet. It reports
// whether the value was stored.
func Create(ctx context.Context, n Node, key string, value []byte) (bool, error) {
	for {
		on, err := n.MetaEnabled()
		if err != nil {
			return false, err
		}
		cond := t4.TxnCondition{Key: key, Target: t4.TxnCondVersion, Result: t4.TxnCondEqual, Version: 0}
		op := t4.TxnOp{Type: t4.TxnPut, Key: key, Value: value}
		mode := t4.MetaDisabled()
		if on {
			cond.Target = t4.TxnCondMetaExists
			op.Type = t4.TxnMetaPut
			mode.Result = t4.TxnCondNotEqual
		}
		resp, err := n.Txn(ctx, t4.TxnRequest{
			Conditions: []t4.TxnCondition{mode, cond},
			Success:    []t4.TxnOp{op},
		})
		if err != nil || resp.Succeeded {
			return resp.Succeeded, err
		}
		// The failed condition is either key existing or the mode having
		// changed since it was read; only the latter is worth a retry.
		if now, err := n.MetaEnabled(); err != nil || now == on {
			return false, err
		}
	}
}

// Get returns key's value from local state.
func Get(n Node, key string) ([]byte, bool, error) {
	on, err := n.MetaEnabled()
	if err != nil || on {
		if err != nil {
			return nil, false, err
		}
		return n.MetaGet(key)
	}
	kv, err := n.Get(key)
	if err != nil || kv == nil {
		return nil, false, err
	}
	return kv.Value, true, nil
}

// LinearizableGet returns key's value with linearizability guaranteed.
func LinearizableGet(ctx context.Context, n Node, key string) ([]byte, bool, error) {
	on, err := n.MetaEnabled()
	if err != nil || on {
		if err != nil {
			return nil, false, err
		}
		return n.LinearizableMetaGet(ctx, key)
	}
	kv, err := n.LinearizableGet(ctx, key)
	if err != nil || kv == nil {
		return nil, false, err
	}
	return kv.Value, true, nil
}

// List returns all entries under prefix from local state, sorted by key.
func List(n Node, prefix string) ([]t4.MetaKV, error) {
	on, err := n.MetaEnabled()
	if err != nil || on {
		if err != nil {
			return nil, err
		}
		return n.MetaList(prefix)
	}
	kvs, err := n.List(prefix)
	return fromKVs(kvs), err
}

// LinearizableList returns all entries under prefix with linearizability
// guaranteed, sorted by key.
func LinearizableList(ctx context.Context, n Node, prefix string) ([]t4.MetaKV, error) {
	on, err := n.MetaEnabled()
	if err != nil || on {
		if err != nil {
			return nil, err
		}
		return n.LinearizableMetaList(ctx, prefix)
	}
	kvs, err := n.LinearizableList(ctx, prefix)
	return fromKVs(kvs), err
}

func fromKVs(kvs []*t4.KeyValue) []t4.MetaKV {
	out := make([]t4.MetaKV, len(kvs))
	for i, kv := range kvs {
		out[i] = t4.MetaKV{Key: kv.Key, Value: kv.Value}
	}
	return out
}
