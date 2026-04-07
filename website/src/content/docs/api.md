---
title: API Reference
description: Full Go API reference for the T4 embedded library.
---
# API Reference

## Opening a node

```go
func Open(cfg Config) (*Node, error)
```

Opens or creates a node. Blocks until startup (checkpoint restore, WAL replay, leader election) is complete. Returns an error if the data directory is unusable or if the S3 store cannot be reached to read the manifest.

```go
func (n *Node) Close() error
```

Seals the active WAL segment, stops background goroutines, and closes the database. Safe to call multiple times.

---

## Write operations

All writes are rejected with `ErrNotLeader` if the node is a follower and the leader cannot be reached for forwarding.

```go
// Put creates or replaces key unconditionally. Returns the new revision.
Put(ctx context.Context, key string, value []byte, lease int64) (int64, error)

// Create writes key only if it does not exist.
// Returns ErrKeyExists if the key is already present.
Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)

// Update is a compare-and-swap on the key's modification revision.
// revision=0 makes the update unconditional.
// Returns (newRevision, previousKV, succeeded, error).
Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)

// Delete removes key. Returns revision=0 if key did not exist.
Delete(ctx context.Context, key string) (int64, error)

// DeleteIfRevision deletes key only if its current modification revision matches.
// Returns (newRevision, previousKV, succeeded, error).
DeleteIfRevision(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)

// Compact discards the history of all keys up to and including revision.
// The current value of every key is preserved.
Compact(ctx context.Context, revision int64) error
```

---

## Read operations

Reads are always served from the local Pebble instance (leader or follower).

```go
// Get returns the current value for key, or nil if the key does not exist.
Get(key string) (*KeyValue, error)

// List returns all live keys whose names begin with prefix, in lexicographic order.
List(prefix string) ([]*KeyValue, error)

// Count returns the number of live keys whose names begin with prefix.
Count(prefix string) (int64, error)
```

---

## Watch

```go
// Watch streams events for keys matching prefix using etcd semantics:
// startRev=0 means start from the current revision (no history replay),
// startRev=N means replay events from revision N (inclusive).
// The returned channel is closed when ctx is cancelled or the node shuts down.
Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error)
```

### Event

```go
type Event struct {
    Type   EventType // EventPut or EventDelete
    KV     *KeyValue // key/value after the operation
    PrevKV *KeyValue // previous value, nil on first creation or if not available
}

type EventType int

const (
    EventPut    EventType = iota // key was created or updated
    EventDelete                  // key was deleted
)
```

---

## Synchronisation

```go
// WaitForRevision blocks until the node has applied at least rev, then returns nil.
// Returns ctx.Err() if the context is cancelled first.
WaitForRevision(ctx context.Context, rev int64) error
```

Useful when a follower needs to serve a read that is consistent with a write
performed on the leader.

---

## Introspection

```go
CurrentRevision() int64  // highest applied revision
CompactRevision() int64  // compaction watermark (0 if never compacted)
IsLeader() bool          // true for leader and single-node roles
Config() Config          // returns the configuration used to open the node
```

---

## KeyValue

```go
type KeyValue struct {
    Key            string
    Value          []byte
    Revision       int64 // revision at which this value was written
    CreateRevision int64 // revision at which this key was first created
    PrevRevision   int64 // previous modification revision (0 if none)
    Lease          int64 // lease ID (0 = no lease)
}
```

---

## Errors

```go
var ErrKeyExists error  // Create: key already present
var ErrNotLeader error  // write on a follower when the leader is unreachable
```

Both are suitable for use with `errors.Is`.

---

## Point-in-time restore (S3 versioning)

> **Note**: this mechanism requires S3 versioning. For most use cases, prefer [Branches](#branches) which work without versioning.

A `RestorePoint` tells a node to bootstrap from a specific moment captured in S3 via object version IDs, rather than from the latest checkpoint. It is applied once, on first boot, and ignored on subsequent restarts.

```go
type PinnedObject struct {
    Key       string // object key in S3
    VersionID string // S3 version ID of that object
}

type RestorePoint struct {
    // Store to read pinned objects from. Typically the source node's S3 prefix.
    // May differ from Config.ObjectStore (the new node's write prefix).
    Store object.VersionedStore

    // Checkpoint archive to restore from. If Key is empty, no checkpoint is
    // restored and WALSegments are replayed into a fresh database.
    CheckpointArchive PinnedObject

    // WAL segments to replay after the checkpoint, in ascending sequence order.
    WALSegments []PinnedObject
}
```

Set `Config.RestorePoint` to activate:

```go
node, err := t4.Open(t4.Config{
    DataDir:      "/var/lib/t4-branch",
    ObjectStore:  branchStore,
    RestorePoint: &t4.RestorePoint{
        Store:             sourceStore,
        CheckpointArchive: t4.PinnedObject{Key: "...", VersionID: "..."},
        WALSegments: []t4.PinnedObject{
            {Key: "wal/000042.seg", VersionID: "..."},
        },
    },
})
```

### object.VersionedStore

```go
type VersionedStore interface {
    Store
    GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error)
}
```

`object.S3Store` satisfies this interface. S3 versioning must be enabled on the source bucket.

---

## Branches

Branches let you fork a database at any checkpoint with zero S3 data copies. Shared SST files stay in the source prefix and are referenced by the branch's checkpoint index.

### Fork

```go
func Fork(ctx context.Context, sourceStore object.Store, branchID string) (checkpointKey string, err error)
```

Reads the latest checkpoint manifest from `sourceStore`, registers the branch (writes `branches/<branchID>` to the source store), and returns the checkpoint key to pass to `BranchPoint`. Pass `--checkpoint` / a specific key if you want to fork from an earlier revision.

### Unfork

```go
func Unfork(ctx context.Context, sourceStore object.Store, branchID string) error
```

Removes the branch registry entry. The next GC cycle on the source can reclaim any SSTs no longer referenced by any live checkpoint or branch.

### BranchPoint

```go
type BranchPoint struct {
    // SourceStore is the object store of the database being forked.
    SourceStore object.Store

    // CheckpointKey is the manifest.json key returned by Fork.
    CheckpointKey string
}
```

Set `Config.BranchPoint` and `Config.AncestorStore` when starting a branch node for the first time:

```go
sourceStore := object.NewS3Store(object.S3Config{Bucket: "my-bucket", Prefix: "prod/"})
branchStore := object.NewS3Store(object.S3Config{Bucket: "my-bucket", Prefix: "branch-a/"})

cpKey, err := t4.Fork(ctx, sourceStore, "branch-a")

node, err := t4.Open(t4.Config{
    DataDir:       "/var/lib/t4-branch-a",
    ObjectStore:   branchStore,
    AncestorStore: sourceStore,
    BranchPoint: &t4.BranchPoint{
        SourceStore:   sourceStore,
        CheckpointKey: cpKey,
    },
})
```

On first boot the node downloads the SSTs and Pebble metadata from `sourceStore`. On subsequent boots `BranchPoint` is ignored (the local data directory already exists).

When the branch is no longer needed:

```go
if err := t4.Unfork(ctx, sourceStore, "branch-a"); err != nil {
    log.Fatal(err)
}
```
