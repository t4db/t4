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
// Watch streams events for keys matching prefix starting from startRev (inclusive).
// startRev=0 means start from the current revision (no history replay).
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

## Point-in-time restore

A `RestorePoint` tells a node to bootstrap from a specific moment captured
in S3, rather than from the latest checkpoint in its own prefix. It is applied
once, on first boot (when the local data directory does not yet exist), and
ignored on subsequent restarts.

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
node, err := strata.Open(strata.Config{
    DataDir:      "/var/lib/strata-branch",
    ObjectStore:  branchStore,   // new node's own S3 prefix for future writes
    RestorePoint: &strata.RestorePoint{
        Store:             sourceStore,
        CheckpointArchive: strata.PinnedObject{Key: "...", VersionID: "..."},
        WALSegments: []strata.PinnedObject{
            {Key: "wal/000042.seg", VersionID: "..."},
        },
    },
})
```

### object.VersionedStore

`RestorePoint.Store` must implement `object.VersionedStore`:

```go
type VersionedStore interface {
    Store
    GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error)
}
```

`object.S3Store` satisfies this interface automatically. S3 versioning must be
enabled on the source bucket. `GetVersioned` maps directly to `s3:GetObject`
with a `VersionId` — no data is copied within S3.
