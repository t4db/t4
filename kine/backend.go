// Package kine provides a kine server.Backend implementation backed by a
// strata Node.
package kine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	kserver "github.com/k3s-io/kine/pkg/server"

	"github.com/makhov/strata"
)

// Backend implements kine's server.Backend interface using a *strata.Node.
type Backend struct {
	node *strata.Node
}

// New creates a Backend wrapping node.
func New(node *strata.Node) *Backend { return &Backend{node: node} }

// Start is a no-op; the node is already started.
func (b *Backend) Start(_ context.Context) error { return nil }

// CurrentRevision returns the current revision.
func (b *Backend) CurrentRevision(_ context.Context) (int64, error) {
	return b.node.CurrentRevision(), nil
}

// Get returns the most recent value for key (or rangeEnd range) at the given
// revision. revision==0 means current.
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *kserver.KeyValue, error) {
	curRev := b.node.CurrentRevision()
	if revision > curRev {
		return curRev, nil, kserver.ErrFutureRev
	}

	kv, err := b.node.Get(key)
	if err != nil {
		return curRev, nil, err
	}
	if kv == nil {
		return curRev, nil, nil
	}
	return curRev, toServerKV(kv, keysOnly), nil
}

// Create creates key only if it does not already exist.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	rev, err := b.node.Create(ctx, key, value, lease)
	if errors.Is(err, strata.ErrKeyExists) {
		return 0, kserver.ErrKeyExists
	}
	return rev, err
}

// Delete deletes key, checking that its revision matches if revision > 0.
func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *kserver.KeyValue, bool, error) {
	newRev, oldKV, deleted, err := b.node.DeleteIfRevision(ctx, key, revision)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, toServerKV(oldKV, false), deleted, nil
}

// List returns keys matching prefix (startKey is a lower bound on the key name).
func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*kserver.KeyValue, error) {
	curRev := b.node.CurrentRevision()
	if revision > curRev {
		return curRev, nil, kserver.ErrFutureRev
	}

	kvs, err := b.node.List(prefix)
	if err != nil {
		return curRev, nil, err
	}

	// Apply startKey filter.
	var out []*kserver.KeyValue
	for _, kv := range kvs {
		if startKey != "" && kv.Key < startKey {
			continue
		}
		out = append(out, toServerKV(kv, keysOnly))
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
	}
	return curRev, out, nil
}

// Count returns the count of live keys with the given prefix.
func (b *Backend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	curRev := b.node.CurrentRevision()
	if revision > curRev {
		return curRev, 0, kserver.ErrFutureRev
	}
	count, err := b.node.Count(prefix)
	if err != nil {
		return curRev, 0, err
	}
	return curRev, count, nil
}

// Update updates key only if its current modRevision matches revision (CAS).
func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *kserver.KeyValue, bool, error) {
	newRev, oldKV, updated, err := b.node.Update(ctx, key, value, revision, lease)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, toServerKV(oldKV, false), updated, nil
}

// Watch streams change events for keys with the given prefix starting from revision+1.
func (b *Backend) Watch(ctx context.Context, key string, revision int64) kserver.WatchResult {
	errCh := make(chan error, 1)
	eventCh := make(chan []*kserver.Event, 64)

	curRev := b.node.CurrentRevision()
	compactRev := b.node.CompactRevision()

	if revision > 0 && revision < compactRev {
		errCh <- kserver.ErrCompacted
		close(errCh)
		close(eventCh)
		return kserver.WatchResult{
			CurrentRevision: curRev,
			CompactRevision: compactRev,
			Events:          eventCh,
			Errorc:          errCh,
		}
	}

	go func() {
		defer close(eventCh)
		defer close(errCh)

		ch, err := b.node.Watch(ctx, key, revision)
		if err != nil {
			errCh <- fmt.Errorf("strata watch: %w", err)
			return
		}
		for ev := range ch {
			se := toServerEvent(&ev)
			select {
			case eventCh <- []*kserver.Event{se}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return kserver.WatchResult{
		CurrentRevision: curRev,
		CompactRevision: compactRev,
		Events:          eventCh,
		Errorc:          errCh,
	}
}

// DbSize returns the approximate on-disk size.
func (b *Backend) DbSize(_ context.Context) (int64, error) {
	cfg := b.node.Config()
	dbDir := filepath.Join(cfg.DataDir, "db")
	var total int64
	err := filepath.Walk(dbDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// Compact compacts the store up to the given revision.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	if err := b.node.Compact(ctx, revision); err != nil {
		return 0, err
	}
	return revision, nil
}

// WaitForSyncTo blocks until the node has applied at least revision.
// On a single-node setup this always returns immediately.
func (b *Backend) WaitForSyncTo(revision int64) {
	_ = b.node.WaitForRevision(context.Background(), revision)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func toServerKV(kv *strata.KeyValue, keysOnly bool) *kserver.KeyValue {
	if kv == nil {
		return nil
	}
	skv := &kserver.KeyValue{
		Key:            kv.Key,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.Revision,
		Lease:          kv.Lease,
	}
	if !keysOnly {
		skv.Value = kv.Value
	}
	return skv
}

func toServerEvent(ev *strata.Event) *kserver.Event {
	se := &kserver.Event{
		Delete: ev.Type == strata.EventDelete,
		Create: ev.Type == strata.EventPut && ev.PrevKV == nil,
		KV:     toServerKV(ev.KV, false),
	}
	if ev.PrevKV != nil {
		se.PrevKV = toServerKV(ev.PrevKV, false)
	}
	return se
}
