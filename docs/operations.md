# Operations Guide

## Single-node with S3

The simplest durable deployment. A single node writes WAL segments and checkpoints to S3. If the node is replaced or its disk is lost, it recovers automatically on the next start.

```bash
strata run \
  --data-dir  /var/lib/strata \
  --listen    0.0.0.0:3379  \
  --s3-bucket my-bucket     \
  --s3-prefix strata/
```

AWS credentials are resolved from the standard chain: `AWS_*` environment variables, `~/.aws/credentials`, instance profile (EC2/ECS), workload identity (EKS).

### MinIO or other S3-compatible stores

```bash
strata run \
  --data-dir    /var/lib/strata \
  --listen      0.0.0.0:3379   \
  --s3-bucket   my-bucket      \
  --s3-prefix   strata/        \
  --s3-endpoint http://minio:9000
```

---

## Multi-node cluster

Multi-node mode requires:

1. A shared S3 bucket (leader election lock + WAL archive).
2. Each node has a unique `--node-id` and a `--peer-listen` address reachable by all other nodes.

All nodes run the same command. At startup they race to acquire the S3 leader lock; the winner becomes the leader, the rest become followers.

### Three-node example

```bash
# Node A
strata run \
  --data-dir       /var/lib/strata     \
  --listen         0.0.0.0:3379        \
  --s3-bucket      my-bucket           \
  --s3-prefix      strata/             \
  --node-id        node-a              \
  --peer-listen    0.0.0.0:3380        \
  --advertise-peer node-a.internal:3380

# Node B
strata run \
  --data-dir       /var/lib/strata     \
  --listen         0.0.0.0:3379        \
  --s3-bucket      my-bucket           \
  --s3-prefix      strata/             \
  --node-id        node-b              \
  --peer-listen    0.0.0.0:3380        \
  --advertise-peer node-b.internal:3380

# Node C — same as B, different --node-id and --advertise-peer
```

### Leader election and failover

- On startup each node reads the S3 lock. If absent, it issues an **atomic conditional PUT** (`If-None-Match: *`); only one concurrent writer wins. The winner becomes the leader and records `LastSeenNano = now()` in the lock so followers see it as immediately alive.
- The leader streams WAL entries to all followers over the peer port (default 3380). Followers apply entries and serve local reads.
- A follower that observes `--follower-max-retries` consecutive stream failures (~10 s at default 5 × 2 s) checks the lock's `LastSeenNano`. If stale (older than `LeaderLivenessTTL` = 6 s), it attempts a takeover using `If-Match: <etag>` — only the candidate that read the same ETag wins the race. The new leader records its own address and `LastSeenNano`.
- The former leader periodically re-reads the S3 lock (`--leader-watch-interval-sec`, default 300 s). When it detects the lock no longer points to itself, it steps down. It also re-reads on every follower disconnect and writes a liveness touch while any follower is still connected.
- Writes sent to a follower are automatically forwarded to the current leader and the result is returned to the caller.

Leader election uses atomic conditional PUT (`If-None-Match`/`If-Match` on the `leader-lock` object). There is no quorum requirement and no TTL polling — the only S3 election writes are at startup, on leader takeover, and during liveness touches while followers are disconnected.

### Adding a node to a running cluster

Start a new node with a fresh `--data-dir` and the same S3 bucket. It will:

1. Read the S3 manifest and restore the latest checkpoint.
2. Replay any WAL segments uploaded since the checkpoint.
3. Lose the election (leader already holds the lock) and become a follower.
4. Receive the live WAL stream from the leader to catch up to the current revision.

No manual registration or cluster membership changes are required.

### Scaling down

Close the nodes you want to remove. The remaining nodes continue without any configuration change. If the leader is among the removed nodes, a follower will take over.

---

## mTLS between peers

Provide a shared CA and a per-node certificate/key pair (all PEM format):

```bash
strata run \
  ... \
  --peer-tls-ca   /etc/strata/tls/ca.crt  \
  --peer-tls-cert /etc/strata/tls/node.crt \
  --peer-tls-key  /etc/strata/tls/node.key
```

Both the leader's gRPC server and the follower's gRPC client use these files. The same CA must be used on all nodes. TLS 1.3 is required; mutual authentication is enforced.

### Embedded library

Pass `credentials.TransportCredentials` directly:

```go
serverCreds, clientCreds, err := buildTLS(caFile, certFile, keyFile)

node, err := strata.Open(strata.Config{
    ...
    PeerServerTLS: serverCreds,
    PeerClientTLS: clientCreds,
})
```

---

## Observability

```bash
strata run --metrics-addr 0.0.0.0:9090 ...
```

### Endpoints

| Path | Description |
|---|---|
| `GET /metrics` | Prometheus metrics |
| `GET /healthz` | 200 once the node has started |
| `GET /readyz` | 200 when the node is ready to serve reads |

### Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `strata_writes_total` | counter | `op` | Completed write operations |
| `strata_write_errors_total` | counter | `op` | Write operations that returned an error |
| `strata_write_duration_seconds` | histogram | `op` | Write latency (WAL + apply) |
| `strata_forwarded_writes_total` | counter | `op` | Writes forwarded from follower to leader |
| `strata_forward_duration_seconds` | histogram | `op` | Forwarded write round-trip latency |
| `strata_current_revision` | gauge | — | Latest applied revision |
| `strata_compact_revision` | gauge | — | Compaction watermark |
| `strata_role` | gauge | `role` | 1 for the active role (`leader`/`follower`/`single`) |
| `strata_wal_uploads_total` | counter | — | WAL segments successfully uploaded |
| `strata_wal_upload_errors_total` | counter | — | Failed WAL segment uploads |
| `strata_wal_upload_duration_seconds` | histogram | — | WAL segment upload latency |
| `strata_wal_gc_segments_total` | counter | — | WAL segments deleted from S3 after checkpointing |
| `strata_checkpoints_total` | counter | — | Checkpoints written to S3 |
| `strata_elections_total` | counter | `outcome` | Election attempts (`won`/`lost`) |

`op` label values: `put`, `create`, `update`, `delete`, `compact`.

---

## Durability and recovery

### What is durable

A write is durable when it has been:
- fsynced to the local WAL **and** at least one follower has applied it (cluster mode), **or**
- fsynced to the local WAL **and** the WAL segment has been uploaded to S3 (single-node mode).

In single-node mode without S3, durability depends entirely on local disk.

### Recovery procedure

On startup, Strata always performs:

1. Read `manifest/latest` from S3 → get the latest checkpoint key and revision.
2. If the local Pebble database is absent, restore the checkpoint from S3.
3. Open the local Pebble database.
4. Replay all local WAL segments (`.wal` files in `<data-dir>/wal/`) that are newer than the checkpoint.
5. Replay any WAL segments uploaded to S3 that are newer than the checkpoint and not already replayed locally.
6. Run leader election (cluster mode) or become single-node.

Steps 4–5 ensure that no committed write is lost even if the node is killed between WAL writes and checkpoint creation.

### S3 unavailability

S3 failures are non-blocking on the write path. WAL segment uploads and checkpoints retry in the background. Writes continue to succeed locally. On restart, local WAL segments are replayed first, so no data written to the local WAL is lost even if it was never uploaded.

---

## Branching

Branches let you fork a database at any checkpoint with zero S3 data copies. SST files are content-addressed and shared between the source and all branches — no data is duplicated.

### Requirements

- S3 versioning is **not** required.
- The source database must have at least one checkpoint.

### Creating a branch (CLI)

```bash
# 1. Register the branch against the source store.
#    Prints the checkpoint key — save it.
strata branch fork \
  --s3-bucket my-bucket \
  --s3-prefix strata/ \
  --branch-id my-branch

# Output: checkpoint/0000000001/00000000000000000100/manifest.json

# 2. Start the branch node, pointing it at the source.
strata run \
  --data-dir              /var/lib/strata-branch \
  --listen                0.0.0.0:3379 \
  --s3-bucket             my-bucket \
  --s3-prefix             strata-branch/ \
  --branch-source-bucket  my-bucket \
  --branch-source-prefix  strata/ \
  --branch-checkpoint     checkpoint/0000000001/00000000000000000100/manifest.json
```

On first boot the branch node downloads SSTs and Pebble metadata from the source prefix. On subsequent restarts `--branch-checkpoint` is ignored (the local data directory already exists).

### Creating a branch (Go library)

```go
import "github.com/makhov/strata"
import "github.com/makhov/strata/pkg/object"

sourceStore := object.NewS3Store(object.S3Config{Bucket: "my-bucket", Prefix: "strata/"})
branchStore := object.NewS3Store(object.S3Config{Bucket: "my-bucket", Prefix: "strata-branch/"})

// Register and get the checkpoint key.
cpKey, err := strata.Fork(ctx, sourceStore, "my-branch")
if err != nil {
    log.Fatal(err)
}

// Start the branch node.
node, err := strata.Open(strata.Config{
    DataDir:       "/var/lib/strata-branch",
    ObjectStore:   branchStore,
    AncestorStore: sourceStore,
    BranchPoint: &strata.BranchPoint{
        SourceStore:   sourceStore,
        CheckpointKey: cpKey,
    },
})
```

### Forking from a specific checkpoint

By default `Fork` uses the latest checkpoint. To fork from an earlier revision, call `checkpoint.RegisterBranch` directly with the specific key:

```bash
# CLI
strata branch fork \
  --s3-bucket my-bucket --s3-prefix strata/ \
  --branch-id my-branch \
  --checkpoint checkpoint/0000000001/00000000000000000050/manifest.json
```

```go
// Go — use the internal package directly for a specific key
import "github.com/makhov/strata/internal/checkpoint"

cpKey := "checkpoint/0000000001/00000000000000000050/manifest.json"
if err := checkpoint.RegisterBranch(ctx, sourceStore, "my-branch", cpKey); err != nil {
    log.Fatal(err)
}

### Removing a branch

When the branch is no longer needed, unregister it so the source's GC can reclaim unused SSTs:

```bash
strata branch unfork \
  --s3-bucket my-bucket \
  --s3-prefix strata/ \
  --branch-id my-branch
```

```go
if err := strata.Unfork(ctx, sourceStore, "my-branch"); err != nil {
    log.Fatal(err)
}
```

### Use cases

**Point-in-time recovery** — fork from a checkpoint taken before a bad write, validate, then promote.

**Blue/green migrations** — run a schema migration against a branch with production data, test it, then cut over.

**DR drills** — spin up a replica in a different region from a fork, verify integrity, then shut it down.

**Parallel testing** — fork the same production snapshot for multiple independent test runs.

---

## Point-in-time restore (S3 versioning)

> **Note**: this mechanism requires S3 versioning to be enabled on the bucket. For most use cases, [Branching](#branching) is simpler and does not require versioning.

`RestorePoint` bootstraps a new node from a specific set of S3 object version IDs captured at a past moment. See [api.md — Point-in-time restore](api.md#point-in-time-restore-s3-versioning) for the Go API.

### Requirements

- S3 versioning must be enabled on the bucket **before** the first write.

### Capturing a restore point

```bash
# Find the current checkpoint key.
aws s3 cp s3://my-bucket/source-prefix/manifest/latest - | jq .

# List WAL segments and their version IDs.
aws s3api list-object-versions \
  --bucket my-bucket \
  --prefix source-prefix/wal/ \
  --query 'Versions[?IsLatest==`true`].[Key,VersionId]' \
  --output json
```
