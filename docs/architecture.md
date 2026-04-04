# Architecture

## Overview

```
                   ┌──────────────────────────────────────────────────────┐
                   │                     Leader node                       │
 client writes ──► │  Node.Put/… → WAL.Append ──► wait ACK ──► db.Apply  │
                   │                    │               ▲                  │
                   │                Broadcast          ACK                 │
                   └────────────────────┬──────────────┴───────────────────┘
                                        │  gRPC bidi stream (port 3380)
                        ┌───────────────┴────────────────────────────┐
                        ▼                                             ▼
                ┌───────────────┐                           ┌───────────────┐
                │  Follower B   │                           │  Follower C   │
                │  WAL.Write    │                           │  WAL.Write    │
                │  ACK leader   │                           │  ACK leader   │
                │  db.Apply     │                           │  db.Apply     │
                └───────────────┘                           └───────────────┘
                reads: local                                reads: local
                writes: forwarded ──────────────────────►  leader via gRPC

                   ┌──────────────┐
                   │  S3 bucket   │
                   │  leader-lock │  ◄── election / liveness
                   │  wal/…       │  ◄── disaster recovery
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
3. The entry is broadcast to all connected followers over a bidirectional gRPC stream.
4. Each follower fsyncs the entry to its own local WAL, applies it to its own Pebble, then sends an **ACK** back to the leader.
5. The leader waits for ACKs from all connected followers (quorum commit). If followers disconnect mid-wait, the leader proceeds — the entry is already durable in the leader's WAL and will be replayed by followers when they reconnect.
6. The entry is applied to the leader's Pebble.
7. The response is returned to the caller.

In single-node mode (no peers configured) steps 3–5 are skipped.

WAL segment rotation is triggered by size (default 50 MB) or age (default 10 s). Sealed segments are uploaded to S3 and deleted locally once the upload confirms. Upload timing depends on `WALSyncUpload`: in the default (`true`) mode the upload blocks the write acknowledgement, guaranteeing durability even on ephemeral storage; with `WALSyncUpload: false` uploads are asynchronous and run every `SegmentMaxAge`, reducing write latency at the cost of a small durability window. In cluster mode the default is still `true` — the quorum commit already provides durability, but sync upload removes the need for any local persistence guarantee.

### Segment naming

```
wal/<term>/<first-revision>
```

Both fields are zero-padded to fixed widths so that lexicographic order equals chronological order. This allows recovery to replay segments in the correct sequence with a single S3 list call.

---

## Checkpoints

A checkpoint is a point-in-time Pebble snapshot uploaded to S3. It allows new or recovering nodes to skip WAL replay from the beginning of time.

The checkpoint cycle (triggered by `CheckpointInterval` or `CheckpointEntries`):

1. Seal and queue the current WAL segment for async upload to S3.
2. Call `pebble.DB.Checkpoint` to capture a consistent snapshot.
3. Upload each SST file to `sst/<hash16>/<name>` — skipping any that are already present (same content hash = same key, so deduplication is automatic).
4. Upload Pebble metadata files (`MANIFEST-*`, `OPTIONS-*`, `CURRENT`) to `checkpoint/<term>/<revision>/`.
5. Write a `checkpoint/<term>/<revision>/manifest.json` index listing all SST keys and metadata filenames.
6. Write `manifest/latest` pointing to the new index.
7. GC S3 WAL segments fully covered by `min(checkpointRev, minFollowerAppliedRev)` — ensuring no segment is deleted while a connected follower still needs it.
8. GC old checkpoint directories (keep the two most recent); delete SST objects no longer referenced by any live checkpoint or branch registry entry.

### Content-addressed SSTs

SST files are stored at `sst/<hash16>/<name>` where `<hash16>` is the first 16 hex characters of the file's SHA-256. This means:

- **Deduplication across checkpoints**: an SST that did not change between two checkpoints is uploaded once.
- **Safe sharing across nodes**: multiple nodes restoring from the same checkpoint may produce SST files with the same Pebble filename but different content (due to non-deterministic WAL replay flush boundaries). Content addressing ensures they never collide.

> **Implementation note:** Pebble fires a `TableCreated` event when an SST file is first created (while it is still empty). The uploader ignores 0-byte SST files during `Reconcile` to avoid registering an empty file in the content registry and silently skipping the real upload that follows when the flush or compaction completes.

---

## Leader election

Election uses an S3 object (`leader-lock`) with atomic conditional PUT operations rather than a consensus protocol or TTL polling.

**Acquiring the lock:**
1. Read `leader-lock` and capture its ETag.
2. If **absent**: issue `PUT` with `If-None-Match: *` (`PutIfAbsent`) — only one concurrent writer can succeed; all others get a precondition failure.
3. If **present and owned by another node**: become a follower of the lock's recorded address.
4. If the conditional PUT succeeds, the election is won. `LastSeenNano` is set to `now()` on the winning write, so the fresh leader is immediately visible as "alive" to any follower checking liveness.

If the store does not implement the `ConditionalStore` interface (optional; see `pkg/object`), the node falls back to an unconditional write + 100 ms read-back to detect a race. All provided stores (`S3Store`, `Mem`) implement `ConditionalStore`.

**TakeOver (follower promoting itself):**
1. Follower detects a dead leader when the WAL gRPC stream fails `FollowerMaxRetries` consecutive times.
2. Before attempting takeover, the follower reads the current lock. If `LastSeenNano` is younger than `LeaderLivenessTTL` (3 × `FollowerRetryInterval` = 6 s), the leader was recently alive — the follower is an isolated minority and backs off.
3. If the lock has already advanced to a term higher than the follower's `floorTerm` (another candidate already won), the follower backs off and follows the new winner.
4. Otherwise: read the lock ETag, then `PUT` with `If-Match: <etag>`. Only the candidate that read the same ETag wins; all others get a precondition failure and re-read to find the new leader.

**Stepdown:**
- On every follower disconnect the leader immediately fences all writes (`fenceMu` write-lock), reads the S3 lock **with its ETag**, and — if still the owner — writes a **liveness touch** (`LastSeenNano = now()`) using `If-Match: <etag>` (conditional PUT). This closes the Read→Touch race: if a follower won a TakeOver between the leader's Read and Touch, the conditional PUT fails with `ErrPreconditionFailed` and the leader steps down immediately, without a second round-trip.
- Polling (fence + conditional-check + conditional-touch every `FollowerRetryInterval` = 2 s) continues until at least one follower reconnects; once followers are present, polling pauses — liveness is signalled implicitly by the live stream.
- As a backstop, the leader re-reads the lock on the `LeaderWatchInterval` (default 5 min) periodic ticker even when no disconnect has occurred.
- If the lock no longer points to this node at any check (or the conditional touch is rejected), it steps down.

**S3 request budget during a disconnect event:**  
Each poll tick costs 1 GET + 1 PUT (touch). With a `FollowerRetryInterval` of 2 s, that is at most 1 GET + 1 PUT per 2 s while a follower is disconnected. Polling stops as soon as any follower reconnects. Outside of disconnect events (and the periodic ticker) there are zero additional S3 requests on the write path.

There is no heartbeat, no TTL, and no ZooKeeper-style session. The only S3 writes for election outside of disconnect events are at election time and on takeover.

### CAP properties

Strata is an **AP** system (Available + Partition-tolerant) that provides strong durability guarantees in cluster mode:

- **No network partition**: reads are linearizable (followers use the ReadIndex pattern — they sync to the leader's revision before serving). Writes are always routed to the leader.
- **Under network partition**: when a follower is fully isolated (can't reach leader or other followers), it will eventually TakeOver once `LastSeenNano` goes stale. The old leader detects supersession either via its next conditional liveness touch (which fails with `ErrPreconditionFailed` the instant a new leader writes the lock) or within one poll interval (≤ 2 s). **The split-brain window is effectively zero**: the conditional touch means A cannot refresh its liveness after B wins — A's next touch attempt is rejected and triggers immediate stepdown.

**Durability in cluster mode:** quorum commit means every acknowledged write exists on at least two nodes' WALs before the caller sees success. If all followers disconnect, the leader falls back to single-node mode — writes remain available and durable in the leader's local WAL; followers replay missed entries when they reconnect. S3 is disaster-recovery only (both nodes fail simultaneously); WAL uploads are async and do not affect write latency.

---

## Follower replication

Followers connect to the leader's gRPC peer address and open a **bidirectional** streaming RPC. The leader pushes WAL entries; followers send ACKs back.

**Quorum ACK:** after fsyncing each entry to its local WAL and applying it to Pebble, a follower sends an ACK with the entry's revision back to the leader. The leader waits for ACKs from all connected followers before applying the batch to its own Pebble. This guarantees that every acknowledged write exists on at least two nodes' WALs.

**Catch-up on connect:** when a follower connects, it sends its current revision. The leader replays from that revision using its in-memory ring buffer (`PeerBufferSize` entries, default 10 000). If the follower is too far behind (ring buffer miss), the leader returns `ErrResyncRequired` and the follower restores the latest S3 checkpoint then replays remaining WAL entries from S3.

A full resync can be triggered by three conditions, each tracked in the `strata_follower_resyncs_total` metric:

| `reason` label | When it fires |
|---|---|
| `behind_leader_start` | The follower's applied revision is behind the leader's own starting revision — it cannot replay missing entries from the ring buffer because the leader itself began at a higher point |
| `ring_buffer_miss` | The follower's requested revision is older than the oldest entry in the ring buffer — it fell too far behind while connected |
| `stream_gap` | A revision discontinuity was detected mid-stream — the follower missed entries and must restore from a checkpoint |

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
4. **GC coordination** — The source's GC phase reads the branch registry before deleting anything. `GCCheckpoints` keeps the pinned checkpoint directory (manifest + index) intact even if it falls outside the keep-N window. `GCOrphanSSTs` keeps all SST files referenced by any live branch. Both source checkpoint objects and SSTs are preserved for as long as the branch entry exists.
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

- **Revision assignment** is serialised under `node.mu`. This keeps the revision counter monotonic.
- **WAL writes, follower broadcast, and Pebble apply** are serialised through a single `commitLoop` goroutine that drains a write channel. Multiple concurrent callers batch into a single `AppendBatch`+`db.Apply` per drain cycle.
- **Reads are lock-free** — Pebble handles its own read concurrency.
- **Watchers** are registered in a fan-out broadcaster that sends events after each write. Each watcher runs in its own goroutine.
- **WAL background goroutines** (rotation loop, upload loop) share the WAL mutex with write operations but hold it only briefly during segment rotation.
- **Follower streams** each run in a dedicated goroutine; the leader pushes entries from a per-follower buffered channel.

---

## Follower promotion sequence

When a follower wins a takeover election, it goes through the following steps in order:

1. **`becomeLeader`** — reopens the WAL for writing, loads the latest S3 checkpoint, and replays any WAL segments ahead of the checkpoint. `IsLeader()` becomes `true` at the end of this step.
2. **Start `commitLoop` and `checkpointLoop`** — these goroutines are started immediately so that client writes queued in the `writeC` channel are processed right away. Without this, writes would stall until steps 3–4 finish.
3. **`Reconcile`** — uploads any SST files that exist on disk but were never streamed to S3 (followers don't run the SST uploader). `forceCheckpoint` takes a `fenceMu` write-lock during all I/O, briefly pausing new writes — the same behaviour as periodic checkpoints.
4. **`forceCheckpoint`** — writes a startup checkpoint that references all local SSTs, ensuring the next GC cycle on the old leader cannot delete them before they are named in a live checkpoint.
