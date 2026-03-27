# Operations Guide

## Single-node with S3

The simplest durable deployment. A single node writes WAL segments and checkpoints to S3. If the node is replaced or its disk is lost, it recovers automatically on the next start.

```bash
strata \
  --data-dir  /var/lib/strata \
  --listen    0.0.0.0:2379  \
  --s3-bucket my-bucket     \
  --s3-prefix strata/
```

AWS credentials are resolved from the standard chain: `AWS_*` environment variables, `~/.aws/credentials`, instance profile (EC2/ECS), workload identity (EKS).

### MinIO or other S3-compatible stores

```bash
strata \
  --data-dir    /var/lib/strata \
  --listen      0.0.0.0:2379   \
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
strata \
  --data-dir       /var/lib/strata     \
  --listen         0.0.0.0:2379        \
  --s3-bucket      my-bucket           \
  --s3-prefix      strata/             \
  --node-id        node-a              \
  --peer-listen    0.0.0.0:2380        \
  --advertise-peer node-a.internal:2380

# Node B
strata \
  --data-dir       /var/lib/strata     \
  --listen         0.0.0.0:2379        \
  --s3-bucket      my-bucket           \
  --s3-prefix      strata/             \
  --node-id        node-b              \
  --peer-listen    0.0.0.0:2380        \
  --advertise-peer node-b.internal:2380

# Node C — same as B, different --node-id and --advertise-peer
```

### Leader election and failover

- On startup each node writes its address to the S3 lock **only if the lock is absent**. The first writer wins and becomes the leader.
- The leader streams WAL entries to all followers over the peer port (default 2380). Followers apply entries and serve local reads.
- A follower that observes `--follower-max-retries` consecutive stream failures (default 5 × ~2 s = ~10 s) overwrites the S3 lock with its own address and becomes the new leader.
- The former leader periodically re-reads the S3 lock (`--leader-watch-interval-sec`, default 300 s). When it detects the lock no longer points to itself, it steps down.
- Writes sent to a follower are automatically forwarded to the current leader and the result is returned to the caller.

There is no quorum requirement. Leader election is a last-writer-wins S3 lock with no TTL polling — the only S3 writes are at startup, on leader takeover, and at each checkpoint.

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
strata \
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
strata --metrics-addr 0.0.0.0:9090 ...
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

## Point-in-time restore

Because all durable state (checkpoints and WAL segments) lives in S3 as
individually versioned objects, you can bootstrap a new node from any past
moment without copying data. The restore point is a lightweight manifest of
S3 version IDs; the new node reads those specific object versions on first
boot, then writes all future WAL segments to its own prefix.

### Requirements

- S3 versioning must be enabled on the bucket **before** the first write.
- Capturing a restore point does not require the source node to be paused.

### Capturing a restore point

External automation lists the current objects and records their S3 version IDs
at the desired moment. Using the AWS CLI:

```bash
# Find the current checkpoint key from the manifest.
aws s3 cp s3://my-bucket/source-prefix/manifest/latest - | jq .

# List WAL segments and their current version IDs.
aws s3api list-object-versions \
  --bucket my-bucket \
  --prefix source-prefix/wal/ \
  --query 'Versions[?IsLatest==`true`].[Key,VersionId]' \
  --output json
```

Using the Go SDK:

```go
manifest, _ := checkpoint.ReadManifest(ctx, sourceStore)
walKeys, _   := sourceStore.List(ctx, "wal/")

// Use s3:ListObjectVersions to resolve a version ID per key, then build:
rp := &strata.RestorePoint{
    Store:             sourceStore,
    CheckpointArchive: strata.PinnedObject{Key: manifest.CheckpointKey, VersionID: cpVersionID},
    WALSegments:       walSegs, // []PinnedObject in ascending sequence order
}
```

### Starting a node from a restore point

```go
node, err := strata.Open(strata.Config{
    DataDir:      "/var/lib/strata-branch",
    ObjectStore:  branchStore,  // separate prefix for this node's own writes
    RestorePoint: rp,
})
```

On first boot the node restores the pinned checkpoint, replays the pinned WAL
segments, then starts writing to `branchStore`. On subsequent restarts
`RestorePoint` is ignored — the node recovers normally from its own prefix.

### Use cases

**Point-in-time recovery** — restore to a known-good revision after a bad
write or accidental deletion.

**Copy-free branching** — two nodes diverge from the same restore point, each
writing to its own S3 prefix. Creating a branch costs one
`ListObjectVersions` call; no bytes are copied in S3.

**Blue/green deployments** — start a shadow node from a restore point, apply
and test a migration against real data, then promote or discard with no
production downtime.

**DR drill** — spin up a replica in a different region from a restore point
captured in the primary region, verify integrity, then shut it down.
