---
title: Troubleshooting
description: Common T4 errors, diagnostics, and fixes — WAL issues, S3 problems, election failures, and performance.
---

## Diagnosing a node

### Check health and readiness

```bash
curl http://localhost:9090/healthz   # 200 = started
curl http://localhost:9090/readyz    # 200 = ready to serve reads
```

### Inspect Prometheus metrics

```bash
curl -s http://localhost:9090/metrics | grep t4_
```

Key metrics to check:

| Metric | What it means |
|---|---|
| `t4_role{role="leader"}` | 1 if this node is the leader |
| `t4_current_revision` | Highest applied revision |
| `t4_wal_upload_errors_total` | Failed S3 WAL uploads (should be 0) |
| `t4_elections_total{outcome="won"}` | How many elections this node has won |
| `t4_write_errors_total` | Write errors by operation type |

### Enable debug logging

```bash
t4 run ... --log-level debug
```

Trace-level logging prints every WAL entry, follower ACK, and S3 operation — useful for diagnosing election or replication issues but very verbose in production.

---

## Startup issues

### `failed to read S3 manifest`

The node can't reach S3 on startup.

**Check:**
- S3 credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or IRSA)
- Bucket name and prefix (`--s3-bucket`, `--s3-prefix`)
- Endpoint URL (`--s3-endpoint` for MinIO/Ceph)
- Network connectivity from the node to S3

```bash
aws s3 ls s3://my-bucket/t4/manifest/
```

### `data directory not empty but no local manifest`

You pointed a new node at a data directory that contains a partial or corrupted state.

**Fix:** either clear the data directory and let the node restore from S3, or investigate what's in it:

```bash
ls -la /var/lib/t4/
rm -rf /var/lib/t4/  # wipe and restore from S3 on next start
```

### Node starts but never becomes ready (`/readyz` returns 503)

The node is stuck in WAL replay or waiting for leader election.

**Check:**
- S3 connectivity (WAL replay reads old segments from S3)
- If multi-node: are other nodes running? A follower won't become ready until it can sync with the leader.
- Disk space: WAL replay writes to local Pebble; if disk is full, replay will fail.

---

## Write errors

### `ErrNotLeader`

A write was sent to a follower and the leader is unreachable for forwarding.

**Causes:**
- Leader is down; a new election is in progress (wait a few seconds)
- Network partition between follower and leader
- Firewall blocking the peer port (default 3380)

**Check:**
```bash
curl http://leader-node:9090/healthz
etcdctl --endpoints=follower:3379 endpoint status
```

### `ErrKeyExists` (from `Create`)

The key already exists. This is expected behavior for `Create` — use `Put` if you want an unconditional write, or `Update` if you want compare-and-swap.

### Writes stall / very high latency (single-node with S3)

`WALSyncUpload=true` (the default) blocks each write until the WAL segment is uploaded to S3. If S3 is slow or unreachable, writes stall.

**Fix options:**
1. Set `--wal-sync-upload=false` if your local storage is durable (e.g. a persistent volume)
2. Check S3 latency: `aws s3 cp /dev/urandom s3://my-bucket/test --expected-size 1 2>&1 | tail -1`
3. Use a cluster — in cluster mode the leader always uses async S3 uploads

### Writes stall in cluster mode (high replication latency)

The leader waits for follower ACKs before returning. High inter-node RTT or a slow follower disk increases write latency.

**Check:**
```bash
# Follower applied revision vs leader current revision
curl -s http://leader:9090/metrics | grep t4_current_revision
curl -s http://follower:9090/metrics | grep t4_current_revision
```

A large gap means the follower is behind. Check:
- Network RTT between nodes
- Follower disk throughput (WAL fsync)
- `t4_wal_upload_errors_total` on followers

---

## Election and leadership issues

### Leader keeps changing (frequent elections)

Symptom: `t4_elections_total` counter is incrementing rapidly.

**Causes:**
- `LeaderLivenessTTL` is too short relative to `FollowerRetryInterval`
- Intermittent S3 connectivity causing leader lock reads to fail
- Network flap between leader and followers

**Check:**
```bash
curl -s http://any-node:9090/metrics | grep t4_elections_total
```

**Fix:**
- Increase `--follower-max-retries` (default 5) to tolerate transient failures
- Check S3 latency and error rates
- Check network stability between nodes

### Follower can't reach leader (peer connection refused)

```
failed to connect to peer: dial tcp node-a:3380: connection refused
```

**Check:**
- `--peer-listen` is set on the leader
- `--advertise-peer` is reachable from the follower (not `0.0.0.0`)
- Firewall rules allow traffic on port 3380
- TLS config matches (both or neither have `--peer-tls-*`)

### Split-brain suspected

T4 prevents split-brain via conditional S3 PUT. If you suspect two nodes both believe they are the leader:

```bash
# Read the current leader lock from S3
aws s3 cp s3://my-bucket/t4/leader-lock - | jq .
```

The lock contains the current leader's address and `LastSeenNano`. Only one node can own the lock at a time — the one whose conditional PUT succeeded last.

---

## WAL and checkpoint issues

### `WAL segment upload failed`

S3 upload is failing. Writes will stall in single-node sync mode.

**Check:**
```bash
curl -s http://localhost:9090/metrics | grep t4_wal_upload_errors_total
```

Common causes:
- S3 bucket does not exist or wrong region
- Credentials expired (IRSA token, static key)
- S3 request throttling (check S3 CloudWatch metrics)

### `checkpoint failed: S3 upload error`

Checkpoints are failing. WAL segments won't be GC'd, and S3 usage will grow unboundedly.

**Fix:** same as WAL upload failures — resolve S3 access. WAL segments are preserved until the next successful checkpoint so no data is lost.

### Disk full

WAL segments accumulate on disk until they're uploaded to S3 and a checkpoint is written. If S3 is unreachable for a long time, local disk fills up.

**Check:**
```bash
du -sh /var/lib/t4/wal/
ls /var/lib/t4/wal/ | wc -l
```

**Fix:** restore S3 connectivity. Once uploads resume, segments are deleted after the next checkpoint.

---

## Data and consistency issues

### Follower returns stale data

By default, follower reads use the `Linearizable` consistency mode — the follower syncs to the leader's current revision before serving the read. If you're seeing stale reads, the follower may be configured with `Serializable` consistency.

**Check:**
```bash
t4 run ... --read-consistency linearizable
```

Or in Go:
```go
node, _ := t4.Open(t4.Config{
    ReadConsistency: t4.ReadConsistencyLinearizable,
})
```

### Key was written but `Get` returns nil

- The key may have been compacted. Check `t4_compact_revision` metric vs the revision of the write.
- If using a follower with `Serializable` consistency, the read may be behind the write.
- If using `Watch`, the event may have already been processed before the `Get`.

### Lost write after node restart

If `WALSyncUpload=false` and the node crashed before the pending WAL segment was uploaded, that segment is lost.

**Recovery:** increase `CheckpointInterval` to reduce the WAL retention window, or enable `WALSyncUpload=true` for single-node deployments without durable local storage.

---

## Performance issues

### Low write throughput (single writer)

T4's reactive group-commit is optimized for concurrent writers. A single serial writer pays the full WAL fsync cost per write (~8 ms on NVMe).

**Options:**
- Batch writes at the application layer
- Use `Put` in parallel goroutines — throughput scales well with concurrency
- If using S3 sync upload, switch to `--wal-sync-upload=false` with a durable local disk

See [Benchmarks](/benchmarks/) for a detailed performance analysis.

### High read latency on followers

Linearizable follower reads require a `ForwardGetRevision` RPC to the leader. Latency = leader RTT + local Pebble lookup.

For workloads that tolerate slightly stale reads, switch to serializable:
```bash
t4 run ... --read-consistency serializable
```

### Memory usage growing

T4 buffers recent WAL entries in memory for follower catch-up (`PeerBufferSize`, default 10,000 entries). If entries are large, this can use significant memory.

Reduce it:
```go
node, _ := t4.Open(t4.Config{
    PeerBufferSize: 1000,
})
```

Trade-off: smaller buffer means slower-to-catch-up followers must restore from S3 checkpoint instead of replaying from memory.
