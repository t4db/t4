# v1 Compatibility Contract

This page defines the public compatibility promises T4 intends to keep for the v1.x release line.

Anything described here is part of the v1 contract. Internal implementation details that are not described here may
change between minor releases.

## Go API

The exported `github.com/t4db/t4` package API is stable across v1.x, including:

- `Open`, `Node`, `Config`, `KeyValue`, `Event`, watch options, transaction types, restore and branch types.
- Public errors: `ErrKeyExists`, `ErrNotLeader`, `ErrClosed`, and `ErrCompacted`.
- Public enum/string constants for read consistency, follower wait mode, transaction conditions, transaction operations,
  and event types.

V1.x releases may add exported fields, methods, constants, or errors. They will not remove or change the meaning of
existing exported API without a major version bump.

Expert hooks are stable enough to compile across v1.x, but their behavior follows the documented contract rather than
every internal implementation detail:

- `PebbleOptions` customizes Pebble and may affect performance, durability, and compatibility with future Pebble
  versions.
- `WAL`/`WALWriter` is intended for constrained runtimes and tests. Custom implementations must preserve T4's
  write-ahead durability semantics.
- `Logger` and `MetricsRegisterer` are supported embedding hooks.

## Configuration

Existing `Config` fields and CLI flags are stable across v1.x. New fields and flags may be added with defaults that
preserve existing behavior.

Default values are part of the v1 contract unless explicitly documented as tuning guidance:

| Setting               | v1 default                                     |
|-----------------------|------------------------------------------------|
| `ReadConsistency`     | `linearizable`                                 |
| `SegmentMaxSize`      | 50 MiB                                         |
| `SegmentMaxAge`       | 10 s                                           |
| `WALSyncUpload`       | `true`                                         |
| `CheckpointInterval`  | 15 min                                         |
| `CheckpointEntries`   | `0`                                            |
| `AutoCompactMode`     | inferred; effectively `off` unless retention is set |
| `AutoCompactRetention` | `0` (disabled)                                |
| `AutoCompactRevisionRetention` | `0` (disabled)                       |
| `AutoCompactInterval` | `AutoCompactSampleInterval` when enabled       |
| `AutoCompactSampleInterval` | clamp(retention/7, 1 min, 24 h) when enabled |
| `NodeID`              | hostname, or `node-0` if hostname lookup fails |
| `AdvertisePeerAddr`   | `PeerListenAddr`                               |
| `LeaderWatchInterval` | 5 min                                          |
| `FollowerMaxRetries`  | 5                                              |
| `FollowerWaitMode`    | `quorum`                                       |
| `PeerBufferSize`      | 10,000                                         |
| `WatchSendTimeout`    | 30 s                                           |

Environment variables documented in [Configuration](configuration.md) remain supported across v1.x. Command-line flags
take precedence over environment variables.

## Local Data Directory

The local data directory contains:

```text
<data-dir>/
  db/   Pebble database
  wal/  local WAL segment files
```

The directory is owned by T4. Operators should not edit files under `db/` or `wal/`.

V1.x guarantees that a newer v1.x T4 binary can open a data directory created by an older v1.x T4 binary, subject to
normal clean shutdown or WAL recovery. Downgrading to an older binary is best-effort and only supported when no newer
persisted format has been written.

Pebble's internal file format is not a T4 protocol. T4 may upgrade its Pebble dependency in a v1.x release if the new
version can read existing v1 data directories.

## Object Store Layout

When `ObjectStore` is configured, T4 uses the following keys under the configured prefix:

```text
manifest/latest
checkpoint/<term>/<revision>/manifest.json
checkpoint/<term>/<revision>/<meta>
sst/<hash16>/<name>
wal/<term>/<first-revision>
leader-lock
branches/<id>
```

`term` is zero-padded to 10 decimal digits. `revision` and `first-revision` are zero-padded to 20 decimal digits. This
keeps lexicographic order aligned with chronological order.

SST keys use the first 16 hex characters of the file's SHA-256 digest:

```text
sst/<first16hexOfSHA256>/<pebble-sst-filename>
```

## Object Store Interface

Custom object stores must implement `object.Store`:

```go
type Store interface {
Put(ctx context.Context, key string, r io.Reader) error
Get(ctx context.Context, key string) (io.ReadCloser, error)
Delete(ctx context.Context, key string) error
DeleteMany(ctx context.Context, keys []string) error
List(ctx context.Context, prefix string) ([]string, error)
}
```

`List` must return keys in lexicographic order. `Delete` and `DeleteMany` must not fail when the object is already
absent. `Put` must be atomic from readers' perspective.

For production multi-node use, stores should also implement `object.ConditionalStore` so leader election can use atomic
conditional writes:

```go
type ConditionalStore interface {
Store
GetETag(ctx context.Context, key string) (*GetWithETag, error)
PutIfAbsent(ctx context.Context, key string, r io.Reader) error
PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error
}
```

Point-in-time restore requires `object.VersionedStore`:

```go
type VersionedStore interface {
Store
GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error)
}
```

## Encrypted Object Format

When object-store encryption is enabled, object keys keep the same layout but each object body is replaced with an
encrypted stream:

```text
[4 bytes magic "T4E1"][1 byte key id][12 bytes object nonce]
repeated encrypted frames:
  [4 bytes plaintext length][ciphertext || 16 byte GCM tag]
final encrypted frame:
  [4 bytes zero length][16 byte GCM tag]
```

The encryption algorithm is AES-256-GCM. The logical object key, header, frame index, frame length, and final-frame bit
are AEAD associated data. A reader must reject ciphertext copied to a different key, changed frame lengths, missing final
frames, and authentication failures.

The key id byte is reserved for future key rotation. Current implementations write `0` and use the single configured
key provider for all reads and writes.

## Manifest Schema

`manifest/latest` is JSON:

```json
{
  "format_version": 1,
  "checkpoint_key": "checkpoint/0000000001/00000000000000000100/manifest.json",
  "revision": 100,
  "term": 1,
  "last_wal_key": "wal/0000000001/00000000000000000050"
}
```

Fields:

| Field            | Type             | Required | Notes                                                                      |
|------------------|------------------|---------:|----------------------------------------------------------------------------|
| `format_version` | unsigned integer |       no | Missing or `0` means format version 1 for backward compatibility.          |
| `checkpoint_key` | string           |      yes | Object key of the latest checkpoint index.                                 |
| `revision`       | integer          |      yes | Highest revision included in the checkpoint.                               |
| `term`           | unsigned integer |      yes | Leader term that wrote the checkpoint.                                     |
| `last_wal_key`   | string           |       no | Last fully uploaded WAL segment whose last entry is covered by `revision`. |

## Checkpoint Index Schema

Each checkpoint index is stored at `checkpoint/<term>/<revision>/manifest.json`:

```json
{
  "format_version": 1,
  "term": 1,
  "revision": 100,
  "sst_files": [
    "sst/0123456789abcdef/000001.sst"
  ],
  "ancestor_sst_files": [
    "sst/fedcba9876543210/000002.sst"
  ],
  "pebble_meta": [
    "CURRENT",
    "MANIFEST-000001",
    "OPTIONS-000002"
  ]
}
```

Fields:

| Field                | Type             | Required | Notes                                                                          |
|----------------------|------------------|---------:|--------------------------------------------------------------------------------|
| `format_version`     | unsigned integer |       no | Missing or `0` means format version 1.                                         |
| `term`               | unsigned integer |      yes | Leader term that wrote the checkpoint.                                         |
| `revision`           | integer          |      yes | Highest revision included in the checkpoint.                                   |
| `sst_files`          | string array     |      yes | SST object keys in the checkpoint's own store.                                 |
| `ancestor_sst_files` | string array     |       no | SST object keys in the branch ancestor store. Only used by branch checkpoints. |
| `pebble_meta`        | string array     |      yes | Pebble metadata filenames stored next to the checkpoint index.                 |

## Checkpoint Format Versions

Checkpoint format version 1 is the v1 baseline.

Rules for v1.x:

- Adding optional JSON fields with `omitempty` is backward-compatible.
- Missing `format_version` is treated as version 1.
- An incompatible checkpoint format change must increment `format_version`.
- A node must reject checkpoint formats newer than it understands rather than silently restoring corrupt state.
- A newer v1.x node can read checkpoints written by older v1.x nodes.
- Downgrade is only safe if the newer binary has not written a checkpoint format the older binary does not understand.

## Branch Registry Schema

Branch registry entries are stored at `branches/<id>` in the source object store:

```json
{
  "ancestor_checkpoint_key": "checkpoint/0000000001/00000000000000000100/manifest.json"
}
```

Fields:

| Field                     | Type   | Required | Notes                                             |
|---------------------------|--------|---------:|---------------------------------------------------|
| `ancestor_checkpoint_key` | string |      yes | Source checkpoint index key pinned by the branch. |

The branch id is the object key suffix after `branches/`. It is not duplicated inside the JSON body.

## Leader Lock Schema

The leader lock is stored at `leader-lock`:

```json
{
  "node_id": "node-a",
  "term": 42,
  "leader_addr": "node-a.internal:3380",
  "last_seen_nano": 1775684807000000000,
  "committed_rev": 12345
}
```

Fields:

| Field            | Type             | Required | Notes                                                                                            |
|------------------|------------------|---------:|--------------------------------------------------------------------------------------------------|
| `node_id`        | string           |      yes | Stable id of the lock owner.                                                                     |
| `term`           | unsigned integer |      yes | Monotonic leader term.                                                                           |
| `leader_addr`    | string           |      yes | Peer address followers use for WAL streaming.                                                    |
| `last_seen_nano` | integer          |      yes | Unix nanoseconds written by the leader during election or liveness touch.                        |
| `committed_rev`  | integer          |      yes | Highest committed revision known to the leader when writing the lock. Used as an election fence. |

The lock is updated with conditional object-store writes when the store supports `ConditionalStore`.

## WAL Segment Names

Remote WAL segments are stored as:

```text
wal/<term>/<first-revision>
```

Both components are zero-padded decimal strings:

```text
wal/0000000001/00000000000000000042
```

## WAL Entry Frame

Each WAL segment is a sequence of framed entries. Integers are big-endian.

```text
[4 bytes] payload_len uint32
[4 bytes] crc32c uint32 over payload
[N bytes] payload
```

Payload layout:

```text
[1 byte ] op
[8 bytes] revision int64
[8 bytes] term uint64
[8 bytes] lease int64
[8 bytes] create_revision int64
[8 bytes] prev_revision int64
[4 bytes] key_len uint32
[4 bytes] val_len uint32
[key_len bytes] key bytes
[val_len bytes] value bytes
```

Operation codes:

| Code | Operation   |
|-----:|-------------|
|  `1` | create      |
|  `2` | update      |
|  `3` | delete      |
|  `4` | compact     |
|  `5` | transaction |

For transaction entries, the outer entry has an empty key and stores encoded sub-operations in `value`.

## WAL Transaction Payload

Transaction payloads are encoded as:

```text
[4 bytes] count uint32
repeat count times:
  [1 byte ] op
  [8 bytes] lease int64
  [8 bytes] create_revision int64
  [8 bytes] prev_revision int64
  [4 bytes] key_len uint32
  [4 bytes] val_len uint32
  [key_len bytes] key bytes
  [val_len bytes] value bytes
```

Sub-operation `op` must be create, update, or delete.

## etcd Compatibility

The etcd v3 compatibility surface is documented in [Migrating from etcd](etcd-migration.md). Unsupported RPCs and
unsupported transaction forms are part of the v1 contract unless that page is updated in a future minor release.

T4's v1 goal is compatibility with the etcd v3 application surface: KV, Watch, Lease, Auth, and practical multi-key
conditional transactions.

T4 does not aim to emulate etcd's Raft cluster administration or maintenance internals. The following etcd features are
intentional v1 non-goals:

| Feature                                                         | v1 stance         | Rationale                                                                                                                               |
|-----------------------------------------------------------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `Alarm`                                                         | Non-goal          | etcd alarms are tied to etcd backend quota and corruption states. T4 exposes its own health and metrics model instead.                  |
| `Snapshot`                                                      | Non-goal          | T4 uses S3 checkpoints, point-in-time restore, and branching rather than etcd snapshot files.                                           |
| `Hash` / `HashKV`                                               | Non-goal          | These are etcd replica validation tools. T4 may add native consistency diagnostics later, but not etcd hash semantics for v1.           |
| `MemberAdd` / `MemberRemove` / `MemberUpdate` / `MemberPromote` | Non-goal          | T4 has no static Raft membership. Nodes join by sharing the object store and peer configuration.                                        |
| `MoveLeader`                                                    | Non-goal          | T4 has no Raft leadership-transfer operation. A future T4-native stepdown command may make sense, but not etcd-compatible `MoveLeader`. |
| Nested transactions                                             | Non-goal          | Rare in normal application clients and not needed for T4's core compare/put/delete transaction model.                                   |
| Range deletes inside transaction branches                       | Compatibility gap | Omitted from the primary v1 application surface. It can be revisited if real workloads require it.                                      |
