// Package sysstate stores T4's own bookkeeping (etcd leases, auth users,
// roles, tokens).
//
// Databases created by a release with the meta keyspace keep this state in
// meta keys, where writes consume no revision, as in etcd. Databases created
// by earlier releases keep it in reserved keys of the revisioned data
// keyspace, where every write consumes a revision.
//
// A database's mode is fixed before it serves its first write (see
// t4.Node.MetaEnabled), so each call picks the requests for its mode up
// front. In the legacy mode it sends exactly the requests earlier releases
// send: during a rolling upgrade a follower on this release forwards them to
// a leader that may still run an earlier release, which does not understand
// meta ops or meta conditions.
package sysstate

import (
	"context"
	"errors"

	"github.com/t4db/t4"
)

// Node is the subset of t4.Node used by this package.
type Node interface {
	Put(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Delete(ctx context.Context, key string) (int64, error)
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

// Apply commits ops (ordinary puts and deletes) atomically with the state
// changes.
func Apply(ctx context.Context, n Node, ops []t4.TxnOp, changes ...Change) error {
	on, err := n.MetaEnabled()
	if err != nil {
		return err
	}
	all := append([]t4.TxnOp(nil), ops...)
	for _, c := range changes {
		op := t4.TxnOp{Type: t4.TxnPut, Key: c.Key, Value: c.Value}
		switch {
		case c.Delete && on:
			op = t4.TxnOp{Type: t4.TxnMetaDelete, Key: c.Key}
		case c.Delete:
			op = t4.TxnOp{Type: t4.TxnDelete, Key: c.Key}
		case on:
			op.Type = t4.TxnMetaPut
		}
		all = append(all, op)
	}
	_, err = n.Txn(ctx, t4.TxnRequest{Success: all})
	return err
}

// Put stores value under key.
func Put(ctx context.Context, n Node, key string, value []byte) error {
	on, err := n.MetaEnabled()
	if err != nil {
		return err
	}
	if on {
		return Apply(ctx, n, nil, Change{Key: key, Value: value})
	}
	_, err = n.Put(ctx, key, value, 0)
	return err
}

// Delete removes key. Removing a missing key is not an error.
func Delete(ctx context.Context, n Node, key string) error {
	on, err := n.MetaEnabled()
	if err != nil {
		return err
	}
	if on {
		return Apply(ctx, n, nil, Change{Key: key, Delete: true})
	}
	_, err = n.Delete(ctx, key)
	return err
}

// Create stores value under key only if key does not exist yet. It reports
// whether the value was stored.
func Create(ctx context.Context, n Node, key string, value []byte) (bool, error) {
	on, err := n.MetaEnabled()
	if err != nil {
		return false, err
	}
	if !on {
		_, err := n.Create(ctx, key, value, 0)
		if errors.Is(err, t4.ErrKeyExists) {
			return false, nil
		}
		return err == nil, err
	}
	resp, err := n.Txn(ctx, t4.TxnRequest{
		Conditions: []t4.TxnCondition{{Key: key, Target: t4.TxnCondMetaExists, Result: t4.TxnCondEqual, Version: 0}},
		Success:    []t4.TxnOp{{Type: t4.TxnMetaPut, Key: key, Value: value}},
	})
	return resp.Succeeded, err
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
