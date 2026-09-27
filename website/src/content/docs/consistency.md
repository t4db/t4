---
title: Consistency Model
description: Write durability, linearizable vs serializable reads, CAP classification, split-brain prevention, and behavior under network partitions.
---

# Consistency Model

This document describes the consistency guarantees T4 provides for reads and writes, the CAP classification, and how the system behaves under partitions and failures.

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

T4 supports two read consistency levels, controlled by `--read-consistency` (CLI) or `ReadConsistency` (library).

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

## Linearizability claim and Jepsen evidence

T4 claims **linearizability** for the following operations, with `ReadConsistency: linearizable` (default):

- Single-key `Put`, `Get`, `Delete`.
- Multi-key `Txn` (`If` / `Then` / `Else`) under the same revision-assignment lock as single-key writes. All ops in a transaction commit to the WAL in one entry and apply to Pebble in one batch.
- Watch events: the sequence of events delivered to a single watcher is ordered by revision, and revisions never repeat or regress across leader failover.

The claim is **not** that every read is linearizable regardless of configuration. The following are not linearizable:

- Reads taken with `ReadConsistency: serializable` (consistent prefix, but may be stale relative to the leader's current revision).
- Reads served by a follower whose stream is currently disconnected from the leader during a partition (the follower returns `ErrNotLeader` for linearizable reads rather than serving stale data).
- Lease TTL clocks (wall-clock dependent; see [`api`](api) and the upgrade implications in [`v1-compatibility`](v1-compatibility)).

### Jepsen evidence

The continuous evidence for the linearizability claim is the [Jepsen test suite](https://github.com/t4db/t4/tree/main/jepsen) (`jepsen/src/jepsen/t4/`).

Two workloads, both defined in [`jepsen/src/jepsen/t4/core.clj`](https://github.com/t4db/t4/blob/main/jepsen/src/jepsen/t4/core.clj):

- **`register`** — single-key CAS register (50% reads / 30% writes / 20% CAS). Knossos `cas-register` model.
- **`multi-register`** — three keys driven through the etcd `Txn` API. Every write and CAS is a single Txn covering all three keys, committed in one WAL entry (one revision). The three-tuple `{:a v :b v :c v}` is modelled as a single `cas-register` value, so any non-atomic application would surface as a linearizability violation.

Nemeses (the same for both workloads):

- `partition-halves` (random 2/3 split).
- `kill` (kill+restart a random node).
- `partition-s3` (isolate a node from S3 via iptables).

Every PR that touches `jepsen/**` runs the smoke matrix (`partition-halves` × both workloads). The nightly workflow runs the full matrix (every nemesis × both workloads = 6 jobs). A release tag must cite the most recent green nightly Jepsen run — see [Releasing T4](releasing).

### What Jepsen does not prove (today)

These are real properties of T4, but no Jepsen workload directly exercises them. They are covered by Go tests in this repo. Treat them as **claimed but Jepsen-untested**:

- Watch ordering and replay across leader failover.
- Lease keepalive / revoke under partition.

Filing new Jepsen workloads for these is a v1.x follow-up.

---

## CAP classification

T4 is a **CP system** (Consistent + Partition-tolerant) in cluster mode:

- **Consistent:** writes require a quorum of nodes (all connected followers) before being acknowledged. Readers on any node see a consistent revision sequence.
- **Partition-tolerant:** the cluster survives single-node failures and network partitions between nodes.
- **Available trade-off:** if quorum cannot be reached (e.g. all followers disconnect), the leader continues serving writes with single-node durability guarantees. Linearizable reads on partitioned followers return errors until reconnection. The system is not fully available under partition — it favours consistency.

---

## Split-brain prevention

Leader identity is stored as a record in S3 with an ETag-based conditional update mechanism.

1. **Election:** candidates race to write the lock with `If-None-Match: *` (create if absent) or `If-Match: <etag>` (CAS on takeover). The S3 API guarantees only one candidate wins.
2. **Heartbeats:** the leader and each follower exchange heartbeats on the WAL stream every 500 ms. A follower sends them only while it hears the leader, so when the leader hears a follower, that follower heard the leader moments before.
3. **Renewal:** the leader rewrites the lock with the time the renewal started and how long it stays valid, conditioned on the ETag it read. While it hears every follower it renews every 20 s and the lock stays valid for 60 s (slow mode). When a follower goes silent or leaves, it renews at once and then every 2 s, valid for 6 s (fast mode), for as long as the follower is silent and 5 minutes after.
4. **Takeover:** a follower that heard the leader (its term) until recently may promote once it has not heard it for 7 s and the lock has not been renewed for 6 s: by then a live leader has noticed the silence and renews every 2 s. Any other node waits until the lock's validity has passed. A candidate must also not be behind the committed revision recorded in the lock. The check is made on the very lock record the conditional takeover write replaces.
5. **Lease:** the leader acknowledges writes and serves linearizable reads only while its lease holds: until 2 s before its last renewal's validity ends, and, if the leader is not hearing every follower, no later than 4 s after that renewal started. Otherwise it refuses them with a "no leader" error until it renews again. A write that was already committed when the lease lapsed also gets this error, and may still take effect, as with any etcd write that fails with `Unavailable`.
6. **Election fence:** before acknowledging a write, the leader makes sure that no node lacking it can pass the fence: every connected follower has it, or the committed revision recorded in the lock covers it. It waits up to 1 s for slow followers, then moves the fence with a lock write.
7. **Step-down:** if a renewal is rejected with `ErrPreconditionFailed`, or the lock names another node, the leader steps down immediately.

**No two nodes serve as leader at the same time,** provided the clocks of any two nodes differ by less than the 2 s safety margin.

**S3 cost:** a healthy cluster writes the lock about every 20 s (≈ 4,300 GETs and conditional PUTs per day), plus one write per term, per follower that leaves or falls behind, and 2 s renewals while a follower is silent.

---

## Behaviour under network partition

### Follower partitioned from leader (leader can still reach S3)

1. Follower loses the peer stream. Both sides notice within 2 s through missed heartbeats.
2. Leader switches to fast renewal: it rewrites the lock at once and every 2 s.
3. Follower exhausts its retry budget and reads the lock: it was renewed within the last 2 s → **does not promote**.
4. Partition heals → follower reconnects and replays missed WAL entries.

**Result:** no split-brain, no data loss, writes continue uninterrupted on the leader.

### Leader partitioned from S3 (its followers still reach it)

The leader cannot renew the lock, but it keeps hearing its followers, and none of them may take over while they hear it. It keeps serving until 2 s before its last renewal's validity ends (up to about a minute); followers keep following it. When S3 comes back, the next renewal extends the lease.

### Leader partitioned from S3 and its followers

1. Followers stop hearing the leader, and the leader stops hearing them. The leader's lease falls back to 4 s after its last renewal, which has usually passed already: it stops acknowledging writes and serving linearizable reads.
2. 7 s after they last heard the leader, and once the lock has not been renewed for 6 s, a follower wins a conditional PUT on the lock and becomes leader.
3. The former leader keeps retrying; once it reaches S3 again it sees the new lock and steps down.

**Result:** automatic failover in ~7 s, with no window in which both nodes serve.

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
- [Releasing T4](releasing) — release notes must cite the last green nightly Jepsen run
- [Jepsen suite source](https://github.com/t4db/t4/tree/main/jepsen)
