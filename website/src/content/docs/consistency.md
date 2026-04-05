---
title: Consistency Model
description: Write durability, linearizable vs serializable reads, CAP classification, split-brain prevention, and behavior under network partitions.
---

# Consistency Model

This document describes the consistency guarantees Strata provides for reads and writes, the CAP classification, and how the system behaves under partitions and failures.

---

## Write durability

A write is **durably acknowledged** when the following conditions are met:

| Mode | Condition |
|---|---|
| **Cluster (default)** | The entry is fsynced to the leader's WAL **and** ACKed by all connected followers before the write returns. The entry exists on at least two nodes' WALs before the caller sees success. |
| **Single-node with S3** | The entry is fsynced to the local WAL **and** the WAL segment has been uploaded to S3 (synchronous upload, default). |
| **Single-node, `WALSyncUpload=false`** | The entry is fsynced to local disk only. The segment is uploaded asynchronously. Up to one segment's worth of acknowledged writes can be lost if the node fails before upload. |

In cluster mode S3 is used only for disaster recovery (all nodes fail simultaneously) and fast startup. Ordinary writes never wait for S3.

### Revision monotonicity

Revisions are assigned under a mutex and are strictly monotonically increasing. No two acknowledged writes ever share a revision. This holds across leader changes — the new leader loads the latest revision from S3 before assigning the next one.

---

## Read consistency

Strata supports two read consistency levels, controlled by `--read-consistency` (CLI) or `ReadConsistency` (library).

### Linearizable (default)

```
--read-consistency linearizable
```

Every read reflects all writes that completed before it, regardless of which node serves it. A follower syncs to the leader's current revision before returning data.

- **Guarantees:** reads are always up-to-date; no stale reads even on followers.
- **Cost:** one gRPC round-trip to the leader per read on a follower (latency overhead is typically single-digit milliseconds on LAN).
- **Single-node mode:** no overhead; the node is always current.

### Serializable

```
--read-consistency serializable
```

Reads are served from the local Pebble state without contacting the leader.

- **Guarantees:** reads are consistent with each other in revision order. A read never observes a partially-applied batch. However a follower may return data that is a few revisions behind the leader.
- **Cost:** zero; no network round-trip.
- **Use case:** analytics, batch scans, or any workload that can tolerate slightly stale data.

---

## CAP classification

Strata is a **CP system** (Consistent + Partition-tolerant) in cluster mode:

- **Consistent:** writes require a quorum of nodes (all connected followers) before being acknowledged. Readers on any node see a consistent revision sequence.
- **Partition-tolerant:** the cluster survives single-node failures and network partitions between nodes.
- **Available trade-off:** if quorum cannot be reached (e.g. all followers disconnect), the leader continues serving writes with single-node durability guarantees. Linearizable reads on partitioned followers return errors until reconnection. The system is not fully available under partition — it favours consistency.

---

## Split-brain prevention

Leader identity is stored as a record in S3 with an ETag-based conditional update mechanism:

1. **Election:** candidates race to write the lock with `If-None-Match: *` (create if absent) or `If-Match: <etag>` (CAS on takeover). The S3 API guarantees only one candidate wins.
2. **Liveness:** while any follower is disconnected the leader refreshes a `LastSeenNano` timestamp in the lock every 2 s. A follower only promotes after `LastSeenNano` has been stale for ≥ 6 s.
3. **Step-down:** the leader re-reads the lock on every follower disconnect event. If the lock no longer names this node (a new leader has taken over), the leader immediately stops serving writes and closes its peer gRPC server.
4. **Fencing on conditional touch:** when the leader's liveness touch is rejected with `ErrPreconditionFailed`, it knows a new leader has written the lock and steps down immediately — no wait required.

**The split-brain window is effectively zero.** At most one node can write the lock at any point in time, and the former leader detects supersession on its very next touch attempt (≤ 2 s).

---

## Behaviour under network partition

### Follower partitioned from leader (leader can still reach S3)

1. Follower loses the peer stream.
2. Leader touches `LastSeenNano` immediately and continues refreshing every 2 s.
3. Follower exhausts its retry budget (~4 s at default settings).
4. Follower reads the S3 lock — `LastSeenNano` ≤ 2 s old → **does not promote**.
5. Partition heals → follower reconnects and replays missed WAL entries.

**Result:** no split-brain, no data loss, writes continue uninterrupted on the leader.

### Leader partitioned from followers and S3

1. Leader stops refreshing `LastSeenNano`.
2. After 6 s the lock goes stale.
3. A follower races to write a new lock with `If-Match: <etag>`. Only one wins.
4. New leader begins serving. Old leader detects supersession on its next fenced check and steps down.
5. Old leader's uncommitted in-flight writes are dropped.

**Result:** automatic failover in ~6 s. Any write that completed quorum ACK before the partition is never lost (exists on ≥ 2 nodes' WALs).

### All followers disconnected (leader isolated from followers, connected to S3)

- Leader falls back to single-node mode — writes are acknowledged with leader-only durability.
- WAL uploads to S3 continue asynchronously.
- When followers reconnect they resync from the leader's ring buffer or from S3.

---

## What can cause data loss

| Scenario | Lost data |
|---|---|
| Leader crash after quorum ACK | None — all followers have the entry in their WALs |
| Leader crash + all followers crash simultaneously | Entries fsynced to leader WAL and uploaded to S3 survive; entries in-flight at crash time may be lost |
| Single-node crash (WALSyncUpload=true) | None — all acknowledged entries are on S3 |
| Single-node crash (WALSyncUpload=false) | Up to one unsealed segment worth of acknowledged writes if the segment was not yet uploaded |
| S3 permanently destroyed (cluster mode) | None if ≥ 1 node survives; full WAL history is on surviving nodes' disks |

---

## See also

- [Configuration reference](configuration) — `ReadConsistency`, `FollowerWaitMode`, `WALSyncUpload`
- [Architecture](architecture) — CAP properties, follower replication, concurrency model
- [Operations guide](operations) — Durability and recovery, network partitions
