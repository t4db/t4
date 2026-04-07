---
title: FAQ
description: Frequently asked questions about T4.
---

## General

### When should I use T4 instead of etcd?

Use T4 when you want one or more of:

- **Embedded operation** — run the store inside your binary with no separate process
- **S3-backed disaster recovery** — survive total node loss by recovering from S3
- **High write concurrency** — T4 outperforms etcd at 8+ concurrent writers
- **Zero-copy branching** — fork a database at a checkpoint without copying S3 objects

Use etcd when you need:
- Mature operational tooling and broad ecosystem support
- Lease expiry / TTL-based key eviction
- Very low write latency at 1–4 concurrent writers (etcd's fixed 100 ms batch interval helps here)
- etcd snapshot restore support

Both systems speak the etcd v3 gRPC protocol, so switching between them later is straightforward.

### Does T4 use Raft?

No. Multi-node coordination uses S3 atomic conditional PUTs (`If-None-Match` / `If-Match`) for leader election and a gRPC bidirectional stream for WAL replication. There is no Raft, Paxos, or ZooKeeper.

This makes the cluster topology much simpler — any number of nodes can join or leave without cluster membership changes, and the only shared coordination state lives in S3.

### How many nodes can I run?

There is no hard limit. T4 scales read throughput linearly with replica count (each follower serves local reads). Write throughput is not affected by replica count — all writes go through the leader.

Odd replica counts aren't required (T4 doesn't use Raft quorum). Any number of followers will work. The leader election mechanism is winner-takes-all via S3 conditional PUT.

### Is T4 production-ready?

T4 has a comprehensive test suite including end-to-end, consistency, stress, failure, and fault injection tests, plus a Jepsen correctness suite. The architecture is designed for simplicity and correctness.

That said, it's a newer project than etcd. Evaluate it against your reliability requirements and test your specific workload before deploying to production.

---

## Storage and durability

### What happens if the node loses its disk?

In single-node mode with S3 enabled (`ObjectStore` configured): the node restores from the latest S3 checkpoint and replays WAL segments uploaded since then. Any writes acknowledged before disk loss are preserved.

In cluster mode: a recovering follower restores from S3 and catches up via the live WAL stream from the leader. If the leader's disk fails, a follower wins a new election. No data is lost as long as the S3 uploads of acknowledged writes completed.

Without S3: data is local only. Disk loss = data loss.

### Does T4 require S3 versioning?

No — S3 versioning is only required if you use the [point-in-time restore](/api/#point-in-time-restore-s3-versioning) feature. The [branching](/api/#branches) feature works without versioning and covers most point-in-time restore use cases.

### How much S3 storage does T4 use?

Storage is dominated by WAL segments and checkpoint SST files:

- **WAL segments**: each segment is at most `SegmentMaxSize` (default 50 MB). Segments are deleted from S3 after they are covered by a checkpoint. Under normal operation, only a few segments exist at once.
- **Checkpoint SSTs**: SST files are content-addressed by SHA-256 — unchanged data between checkpoints is stored once. Storage grows with the size of your dataset, not the number of checkpoints.
- **Old checkpoints**: only the two most recent checkpoints are retained (plus any pinned by live branches).

### Can I use T4 without S3?

Yes. Set `ObjectStore: nil` (or omit it) for local-only operation. Data is stored in the local Pebble database only. This is useful for development and for deployments where durability comes from infrastructure (e.g. RAID, persistent volumes, replication at the VM layer).

---

## Clustering

### Do all nodes need to start at the same time?

No. Start as many as you have. The first one to write `leader-lock` to S3 becomes the leader. New nodes can join at any time — they restore the latest S3 checkpoint and start replicating.

### What happens when the leader fails?

A follower detects the leader failure when the WAL gRPC stream fails `FollowerMaxRetries` (default 5) consecutive times. It then:

1. Checks the S3 lock's `LastSeenNano`. If it was written more than `LeaderLivenessTTL` (default 6 s) ago, the leader is considered dead.
2. Attempts to acquire the lock with `If-Match: <etag>`. Only one candidate wins.
3. The winner becomes the new leader, connects to the remaining followers, and resumes writes.

Total failover time: ~10 s at default settings (`FollowerMaxRetries=5`, `FollowerRetryInterval=2s`).

### Can a follower serve reads while the leader is down?

With `Linearizable` consistency (default): no — the follower must sync to the leader's revision before serving reads. If the leader is unreachable, linearizable reads block until failover completes.

With `Serializable` consistency: yes — the follower serves reads from its local state, which may be slightly behind the last leader.

### What if S3 is unavailable?

- **Writes in cluster mode**: not affected — WAL uploads are async. Acknowledged writes are safe on quorum of nodes' WALs. S3 uploads will resume when S3 becomes available.
- **Writes in single-node mode with `WALSyncUpload=true`**: writes will stall until S3 is available.
- **Reads**: not affected — reads are served from local Pebble.
- **New node startup**: blocked until S3 is available (the node needs to read the manifest to find the checkpoint).
- **Leader election**: not affected after initial startup — the leader lock is already held in S3.

---

## Kubernetes

### Does T4 work with Kubernetes?

Yes. The Helm chart deploys T4 as a StatefulSet with persistent volumes, health probes, Prometheus metrics, and optional Envoy proxy for read scale-out.

See the [Kubernetes deployment guide](/deployment/kubernetes/) for details.

### Can T4 replace etcd in a Kubernetes control plane?

Not directly — Kubernetes control planes expect a specific etcd cluster membership API and snapshot restore support that T4 doesn't implement. For embedded Kubernetes (k3s, k0s) using kine as an etcd shim, T4 can serve as the kine backend since kine uses the etcd v3 API.

### How do I expose T4 outside the cluster?

Use a `LoadBalancer` Service or an Ingress (if you have a gRPC-compatible ingress controller):

```yaml
service:
  type: LoadBalancer
```

For internal-only access, use `ClusterIP` (default) and connect via `kubectl port-forward` or a sidecar.

---

## etcd compatibility

### Which etcd client versions work with T4?

Any etcd v3 client. Tested with:
- etcd Go client v3 (`go.etcd.io/etcd/client/v3`)
- `etcdctl` v3.5+
- Python `etcd3` client
- gRPC clients generated from the etcd v3 proto definitions

### Are etcd transactions supported?

Yes — the subset used by standard clients:
- `If(ModRevision == 0).Then(Put)` → `node.Create`
- `If(ModRevision == X).Then(Put)` → `node.Update`
- `If(ModRevision == X).Then(Delete)` → `node.DeleteIfRevision`
- Unconditional `Put` inside a Txn → `node.Put`

Complex multi-key transactions with mixed conditions are not supported.

### Do leases work?

Partially. T4 accepts `LeaseGrant` / `LeaseKeepAlive` / `LeaseRevoke` RPCs and assigns lease IDs, but it does **not** evict keys when a lease expires. If your workload depends on lease expiry to clean up keys (e.g. ephemeral service registrations), delete keys explicitly on shutdown instead.

---

## Branching

### What is a branch?

A branch is a copy-free fork of a T4 database at a specific checkpoint. It shares SST files with the source database in S3 — no data is copied. Each branch writes its new data to its own S3 prefix.

### When would I use branching?

- **Point-in-time recovery**: fork before a bad write, validate the branch, promote it
- **Blue/green migrations**: test a schema change on a branch with production data
- **DR drills**: spin up a replica in a different region and verify integrity
- **Parallel testing**: give each test run its own independent snapshot of production data

### Does branching require S3 versioning?

No. Branching uses content-addressed SSTs and a branch registry entry. No S3 versioning required.

### How long does a fork take?

The `Fork` call itself is near-instant — it only reads the checkpoint manifest and writes a small JSON registry entry. The branch node downloads SSTs from the source on its first boot, which takes time proportional to the size of your dataset.
