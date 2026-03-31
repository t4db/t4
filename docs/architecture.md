# Architecture

## Overview

```
                   ┌──────────────────────────────────────────────────┐
                   │                    Leader node                    │
 client writes ──► │  Node.Put/Create/… → WAL.Append → store.Apply   │
                   │                          │                        │
                   │                     Broadcast                     │
                   └──────────────────────────┬───────────────────────┘
                                              │  gRPC stream (port 3380)
                        ┌─────────────────────┴──────────────────────┐
                        ▼                                             ▼
                ┌───────────────┐                           ┌───────────────┐
                │  Follower B   │                           │  Follower C   │
                │  WAL.Apply    │                           │  WAL.Apply    │
                │  store.Apply  │                           │  store.Apply  │
                └───────────────┘                           └───────────────┘
                reads: local                                reads: local
                writes: forwarded ──────────────────────►  leader via gRPC

                   ┌──────────────┐
                   │  S3 bucket   │
                   │  leader-lock │  ◄── election / liveness
                   │  wal/…       │  ◄── durability / recovery
                   │  checkpoint/ │  ◄── fast startup
                   │  manifest/   │  ◄── single GET to locate latest state
                   └──────────────┘
```

---

## Storage layout

```
<data-dir>/
  db/          Pebble key-value database
  wal/         Local WAL segment files (*.wal, auto-deleted after S3 upload)

S3 bucket/<prefix>/
  manifest/latest                              JSON pointer to the latest checkpoint
  checkpoint/<term>/<revision>/manifest.json   Checkpoint index (JSON)
  checkpoint/<term>/<revision>/<meta>          Pebble metadata files (MANIFEST-*, OPTIONS-*, CURRENT)
  sst/<hash16>/<name>                          Content-addressed SST files (shared across checkpoints)
  wal/<term>/<first-revision>                  Sealed WAL segment
  leader-lock                                  JSON leader lease record
  branches/<id>                                Branch registry entry (JSON)
```

SST files are keyed by the first 16 hex characters of their SHA-256 content hash. Identical content is stored once regardless of how many checkpoints reference it. Branch nodes add an `AncestorSSTFiles` list to their checkpoint index that points at SSTs in the source prefix — those files are never copied.

---

## Write-ahead log (WAL)

Every write goes through the WAL before it touches the database:

1. The leader assigns the next monotonic revision.
2. The entry (`{revision, op, key, value}`) is appended to the active `.wal` segment and fsynced.
3. The entry is broadcast to all connected followers over gRPC streams.
4. The entry is applied to Pebble.
5. The response is returned to the caller.

WAL segment rotation is triggered by size (default 50 MB) or age (default 10 s). When a segment is sealed it is queued for async upload to S3 and then deleted locally once the upload confirms.

### Segment naming

```
wal/<term>/<first-revision>
```

Both fields are zero-padded to fixed widths so that lexicographic order equals chronological order. This allows recovery to replay segments in the correct sequence with a single S3 list call.

---

## Checkpoints

A checkpoint is a point-in-time Pebble snapshot uploaded to S3. It allows new or recovering nodes to skip WAL replay from the beginning of time.

The checkpoint cycle (triggered by `CheckpointInterval` or `CheckpointEntries`):

1. Seal and upload the current WAL segment.
2. Call `pebble.DB.Checkpoint` to capture a consistent snapshot.
3. Upload each SST file to `sst/<hash16>/<name>` — skipping any that are already present (same content hash = same key, so deduplication is automatic).
4. Upload Pebble metadata files (`MANIFEST-*`, `OPTIONS-*`, `CURRENT`) to `checkpoint/<term>/<revision>/`.
5. Write a `checkpoint/<term>/<revision>/manifest.json` index listing all SST keys and metadata filenames.
6. Write `manifest/latest` pointing to the new index.
7. Delete S3 WAL segments whose last revision is older than the checkpoint.
8. GC old checkpoint directories (keep the two most recent); delete SST objects no longer referenced by any live checkpoint or branch registry entry.

### Content-addressed SSTs

SST files are stored at `sst/<hash16>/<name>` where `<hash16>` is the first 16 hex characters of the file's SHA-256. This means:

- **Deduplication across checkpoints**: an SST that did not change between two checkpoints is uploaded once.
- **Safe sharing across nodes**: multiple nodes restoring from the same checkpoint may produce SST files with the same Pebble filename but different content (due to non-deterministic WAL replay flush boundaries). Content addressing ensures they never collide.

---

## Leader election

Election uses a last-writer-wins S3 object (`leader-lock`) rather than a consensus protocol or TTL polling.

**Acquiring the lock:**
1. Try to read `leader-lock`.
2. If absent (or from a previous term), write a lock record containing `{node_id, term, leader_addr}`.
3. Re-read the lock. If it still contains this node's record, the election is won.

**Liveness detection:**
- Followers detect a dead leader when the WAL gRPC stream fails `FollowerMaxRetries` consecutive times (each attempt has a ~2 s timeout).
- On failure, the follower increments the term and overwrites the lock. It re-reads to confirm it won, then starts serving as the new leader.

**Stepdown:**
- The old leader periodically re-reads the lock (`LeaderWatchInterval`, default 5 min).
- If the lock no longer points to this node, it steps down gracefully.

There is no heartbeat, no TTL, and no ZooKeeper-style session. The only S3 writes for election are at startup and on takeover. A node that is partitioned from S3 will keep serving reads and local writes until it detects the supersession.

---

## Follower replication

Followers connect to the leader's gRPC peer address and open a streaming RPC. The leader pushes WAL entries as they are committed. Each entry contains the revision, operation type, key, and value — enough to apply to the follower's local Pebble instance.

**Catch-up on connect:** when a follower connects, it sends its current revision. The leader replays from that revision using its in-memory ring buffer (`PeerBufferSize` entries, default 10 000). If the follower is too far behind, it must restart from S3.

**Write forwarding:** a client write arriving at a follower is forwarded to the leader via gRPC. The follower returns the leader's response (including the assigned revision) directly to the client.

---

## etcd v3 adapter

The `etcd/` package wraps `*strata.Node` with the etcd v3 gRPC server interfaces (`KVServer`, `WatchServer`, `LeaseServer`, `ClusterServer`, `MaintenanceServer`). The standalone binary registers this adapter and serves on the configured `--listen` address.

Mapping summary:

| etcd operation | Strata call |
|---|---|
| `Range` (single key) | `node.Get` |
| `Range` (prefix) | `node.List` filtered by `RangeEnd` |
| `Put` | `node.Put` |
| `DeleteRange` (single) | `node.Delete` |
| `Txn` (MOD==0 + Put) | `node.Create` |
| `Txn` (MOD==X + Put) | `node.Update` |
| `Txn` (MOD==X + Delete) | `node.DeleteIfRevision` |
| `Watch` | `node.Watch` |
| `Compact` | `node.Compact` |

Lease operations are stubbed (TTL=60, no eviction). Cluster operations return a single synthetic member. These are sufficient for all standard etcd v3 clients.

## Branches

Branching lets you fork a database at a checkpoint without copying SST files in S3.

### How it works

1. **Register** — `Fork(ctx, sourceStore, branchID)` reads the latest (or a specified) checkpoint manifest from the source store and writes a `branches/<id>` registry entry to the source store. This entry records the checkpoint key being forked from.
2. **Start** — Open a new strata node with `BranchPoint{SourceStore, CheckpointKey}`. On first boot, `RestoreBranch` downloads SSTs from the source store and Pebble metadata from the source checkpoint. The branch's own store prefix starts empty.
3. **Diverge** — New SSTs produced by the branch are uploaded to the branch's own prefix. The checkpoint index for the branch records `SSTFiles` (its own SSTs) and `AncestorSSTFiles` (SSTs inherited from the source). Ancestor SSTs are never re-uploaded.
4. **GC coordination** — The source's GC phase reads the branch registry and treats all SST keys referenced by any live branch as pinned. SSTs shared between source and branch are never deleted while the branch is registered.
5. **Unfork** — `Unfork(ctx, sourceStore, branchID)` removes the registry entry. The next source GC cycle can reclaim SSTs that are no longer referenced by any live checkpoint or branch.

### Branch registry

Entries are stored at `branches/<id>` in the source store as JSON:

```json
{
  "id": "my-branch",
  "checkpoint_key": "checkpoint/0000000001/00000000000000000100/manifest.json",
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

## Concurrency model

- **All writes serialised through a single mutex** (`node.mu`). This keeps the revision counter monotonic and makes WAL append + Pebble apply atomic from the node's perspective.
- **Reads are lock-free** — Pebble handles its own read concurrency.
- **Watchers** are registered in a fan-out broadcaster that sends events after each write. Each watcher runs in its own goroutine.
- **WAL background goroutines** (rotation loop, upload loop) share the WAL mutex with write operations but hold it only briefly during segment rotation.
- **Follower streams** run in dedicated goroutines per connected follower; the leader pushes entries from a channel filled under the write mutex.
