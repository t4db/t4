# Backup and Restore

This guide covers how to back up T4 data, how to restore a cluster after failure, and how to create zero-copy snapshots using branching.

---

## How backups work

T4 writes **checkpoints** to S3 automatically. A checkpoint contains:

- A Pebble database snapshot (SST files + metadata).
- A manifest JSON file pointing to the checkpoint's SST files and the revision at which it was taken.
- An index JSON file with the term and revision.

Checkpoints are written:
- Automatically on a configurable interval (`CheckpointInterval`, default 15 min).
- Optionally after a configurable number of WAL entries (`CheckpointEntries`).
- On leader promotion (startup checkpoint).

SST files are **content-addressed** — identical content is stored once regardless of how many checkpoints reference it. Checkpoints are cheap: only changed SSTs are written.

> **No manual backup steps are needed.** As long as the node can write to S3, backups are continuous and automatic.

---

## Listing available checkpoints

```bash
t4 restore list \
  --s3-bucket my-bucket \
  --s3-prefix t4/
```

Output:

```
CHECKPOINT                                                                  REVISION    TERM
checkpoint/0000000001/00000000000000000050/manifest.json                          50       1
checkpoint/0000000001/00000000000000000100/manifest.json                         100       1  (latest)
```

The `(latest)` marker shows the checkpoint referenced by `manifest/latest`.

---

## Restoring the latest checkpoint

Use this to recover a node that lost its local disk, or to spin up a replacement node.

```bash
t4 restore checkpoint \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --data-dir /var/lib/t4-restored
```

Then start the node normally — it replays any WAL segments on S3 that are newer than the restored checkpoint before joining the cluster:

```bash
t4 run \
  --data-dir  /var/lib/t4-restored \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --listen    0.0.0.0:3379
```

---

## Restoring a specific earlier checkpoint (point-in-time rollback)

Use this when recent writes caused data corruption and you want to roll back to a known-good revision.

```bash
# 1. List checkpoints to find the target revision.
t4 restore list --s3-bucket my-bucket --s3-prefix t4/

# 2. Download the target checkpoint to a fresh directory.
t4 restore checkpoint \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --checkpoint checkpoint/0000000001/00000000000000000050/manifest.json \
  --data-dir /var/lib/t4-pitr

# 3. Start a verification node (no S3 → stays at rev 50, read-only inspection).
t4 run --data-dir /var/lib/t4-pitr --listen 0.0.0.0:3380

# 4. Inspect data, verify correctness.
etcdctl --endpoints=localhost:3380 get --prefix /

# 5. When satisfied, promote to a new production prefix.
t4 run \
  --data-dir  /var/lib/t4-pitr \
  --s3-bucket my-bucket \
  --s3-prefix t4-recovered/ \
  --listen    0.0.0.0:3379
```

> **Caution:** using the **original** S3 prefix in step 5 replays all WAL segments after revision 50 — this is recovery, not rollback. To stay at revision 50, use a fresh prefix as shown above.

---

## Restoring a multi-node cluster

When the entire cluster fails:

1. Restore the latest checkpoint to each node's data directory (or start fresh — nodes recover automatically from S3 on startup).
2. Start all nodes pointing at the same S3 bucket and prefix.
3. They race to acquire the S3 leader lock. One wins, the others follow.

```bash
# On each node (same bucket+prefix, different data-dir and peer address):
t4 run \
  --data-dir  /var/lib/t4 \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --peer-listen 0.0.0.0:3380 \
  --advertise-peer <this-node-ip>:3380 \
  --listen 0.0.0.0:3379
```

No manual restore step is needed — each node runs `t4 restore checkpoint` internally on startup if its local Pebble database is absent.

---

## Zero-copy branching (snapshot without download)

Branching lets you fork a database at a checkpoint without copying SST files in S3. The branch node reads inherited SSTs from the source prefix and writes its own new SSTs to a separate prefix.

```bash
# 1. Register a branch (prints checkpoint key).
t4 branch fork \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --branch-id staging

# 2. Start the branch node with the printed checkpoint key.
t4 run \
  --data-dir  /var/lib/t4-staging \
  --s3-bucket my-bucket \
  --s3-prefix t4-staging/ \
  --branch-prefix t4/ \
  --branch-checkpoint <key-from-step-1> \
  --listen 0.0.0.0:3379

# 3. When done, remove the branch registration so source GC can reclaim space.
t4 branch unfork \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --branch-id staging
```

**Use cases:** test environments, schema migrations, CI data fixtures, analytics read replicas.

---

## Point-in-time restore using S3 versioning

If your S3 bucket has versioning enabled, you can restore to any point in time — not just to a checkpoint boundary.

```bash
# 1. Start a restore-point node (pins S3 object versions at a timestamp).
t4 run \
  --data-dir  /var/lib/t4-pitr \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --restore-point-time "2024-06-01T12:00:00Z" \
  --listen 0.0.0.0:3380
```

See [API reference — RestorePoint](api.md#point-in-time-restore-s3-versioning) for the Go library equivalent.

---

## Backup retention and GC

Old checkpoints and WAL segments accumulate in S3 unless explicitly pruned. Use `t4 gc` to remove objects outside a retention window:

```bash
t4 gc \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --keep 5
```

This keeps the 5 most recent checkpoints and deletes everything else in three passes:

1. Old checkpoint archives beyond the `--keep` window.
2. SST files exclusively referenced by the deleted checkpoints (orphans not needed by any surviving checkpoint or active branch).
3. WAL segments whose entire revision range is covered by the latest surviving checkpoint.

Use `t4 status` first to see current counts:

```bash
t4 status --s3-bucket my-bucket --s3-prefix t4/
```

> **Branch safety:** GC reads the branch registry before deleting. A checkpoint pinned by an active `t4 branch fork` is never deleted, nor are its SST files. Call `t4 branch unfork` only after the branch node is fully decommissioned.

See [Storage management — Garbage collection](operations.md#garbage-collection) for retention recommendations.

---

## Verifying a restore

After restoring, verify that data is intact before promoting the node:

```bash
# Check the revision at which the node is running.
curl -s http://localhost:9090/metrics | grep t4_revision

# Spot-check key data.
etcdctl --endpoints=localhost:3379 get --prefix /your/prefix --limit 100

# Count total keys.
etcdctl --endpoints=localhost:3379 get --prefix / --count-only
```

---

## See also

- [Operations guide](operations.md) — Restore from checkpoint, Point-in-time recovery workflow, Branching
- [Failure scenarios](failure-scenarios.md) — WAL and checkpoint corruption recovery
- [API reference](api.md) — RestorePoint, BranchPoint
