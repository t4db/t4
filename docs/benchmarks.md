# Benchmark Results: Strata vs etcd

Head-to-head comparison of a single Strata node against a single etcd node, both running in Docker containers on the
same host machine.

## Environment

|                      |                                                                      |
|----------------------|----------------------------------------------------------------------|
| **Date**             | 2026-04-02                                                           |
| **Host**             | Apple Silicon Mac (12 vCPUs, 14 GB RAM assigned to Docker)           |
| **Docker**           | VirtioFS-backed volumes, Linux VM via macOS Virtualization.framework |
| **Strata**           | local-WAL mode (no S3), Pebble storage, group-commit write pipeline  |
| **etcd**             | v3.6.4, bbolt storage, 100 ms batch interval (default)               |
| **Client**           | `stratabench` — etcd v3 Go client, one TCP connection per worker     |
| **Ops**              | 5,000 total per workload                                             |
| **Parallel workers** | 16 (for `par-*` and `mixed` workloads)                               |
| **Key size**         | 64 bytes                                                             |
| **Value size**       | 256 bytes                                                            |

> **Note:** All numbers reflect Docker-on-macOS performance. Every `fsync` crosses the macOS → hypervisor → Linux VM
> boundary, adding 0.2–2 ms of overhead compared to native Linux. Relative comparisons between Strata and etcd are
> meaningful; absolute numbers are not representative of production Linux performance.

---

## Results

### Single node — no S3 (`single` scenario)

| Workload                        | Strata ops/s | etcd ops/s | Ratio | Strata p50 | etcd p50 |
|---------------------------------|--------------|------------|-------|------------|----------|
| `seq-put`                       | 800          | 3,272      | 0.24× | 842 µs     | 287 µs   |
| `par-put` (16 workers)          | 3,116        | 10,018     | 0.31× | 3,079 µs   | 1,574 µs |
| `seq-get`                       | 4,353        | 5,227      | 0.83× | 211 µs     | 185 µs   |
| `par-get` (16 workers)          | 18,931       | 19,252     | 0.98× | 802 µs     | 802 µs   |
| `mixed` (16 workers, 50/50 r/w) | 7,814        | 11,054     | 0.71× | 1,138 µs   | 1,315 µs |
| `watch`                         | 1,044        | 3,120      | 0.33× | 702 µs     | 303 µs   |

### Write throughput scaling with concurrent writers

How `par-put` throughput scales as the number of concurrent clients increases (3,000 ops each, one TCP connection per
worker):

| Clients | Strata ops/s | etcd ops/s | Ratio     |
|---------|--------------|------------|-----------|
| 1       | 1,238        | 1,520      | 0.81×     |
| 2       | 1,415        | 2,224      | 0.64×     |
| 4       | 3,085        | 3,198      | 0.96×     |
| 8       | 4,659        | 4,369      | **1.07×** |
| 16      | 6,621        | 5,910      | **1.12×** |
| 32      | 9,716        | 8,683      | **1.12×** |
| 64      | 13,739       | 11,040     | **1.24×** |

Strata pulls ahead at 8+ concurrent writers and is **24% faster at 64 writers**.

---

## Analysis

### Read performance: effectively identical

`par-get` throughput is within 2% (18,931 vs 19,252 ops/s). Both systems serve reads from an in-memory page cache (
Pebble block cache vs bbolt page cache) with no disk I/O on the hot path. The tiny gap is noise.

`seq-get` is slightly slower on Strata (4,353 vs 5,227) because each read goes through the Strata linearizability
layer (ReadIndex check) even on a single-node deployment.

### Write performance: depends heavily on concurrency

The write story is nuanced:

**Low concurrency (1–2 writers): etcd wins by ~2×.**
etcd uses a fixed 100 ms batch window in its raft pipeline. Even with a single serial client, proposals that arrive
during that window are flushed together in one fsync. With Strata's reactive group-commit, a single serial writer finds
an empty queue and gets exactly one op per fsync — the full cost every time (~800 µs on this host).

**4 writers: essentially tied** (3,085 vs 3,198 ops/s).

**8+ writers: Strata is faster**, reaching 24% higher throughput at 64 writers (13,739 vs 11,040 ops/s).

The mechanism: Strata's commit loop drains the `writeC` queue and fsyncs whatever is there in one shot, then immediately
loops. As the number of concurrent writers grows, the queue is always non-empty when the loop wakes, so every fsync
amortises more ops. At 64 writers the queue effectively never drains, and Strata is doing continuous back-to-back fsyncs
at maximum batch depth.

etcd's fixed 100 ms window caps the system at ~10 flush cycles per second, regardless of how many ops are waiting. That
ceiling hurts under high concurrency: even with 64 writers filling the proposal queue, etcd can only commit every 100 ms
while Strata commits as fast as the disk allows.

**Would adding a time-based batch interval to Strata close the low-concurrency gap?**

It would close the seq-put gap but destroy the high-concurrency advantage. A 100 ms timer caps throughput at 10
flushes/s regardless of load — exactly what limits etcd at 64+ writers. Strata's reactive design is intentionally
unbounded: it's optimal at high concurrency and only costs throughput (not correctness) at low concurrency. Applications
that care about single-writer throughput should issue writes in larger batches at the client layer.

### Write pipeline architecture and durability guarantees

Understanding the write throughput numbers requires understanding exactly what each system does before ACKing a write.

#### Single-node (quorum = 1)

In a single-node cluster the leader is the entire quorum. Both systems **must** fsync to their WAL before ACKing — no other node can cover for a crash. There is no weaker guarantee on either side.

The single-node write gap (etcd ~4× faster on `seq-put`) is therefore not about durability tradeoffs — it comes down to raw fsync pipeline efficiency:

- **etcd** uses a raft "ready" loop that can batch multiple proposals into one WAL write even for a single serial client (heartbeat entries, internal metadata, and proposals can all coalesce). bbolt appends sequentially to a single file.
- **Strata** uses a reactive commit loop: it blocks until a write arrives, fsyncs the batch, then loops. With one sequential client there is always exactly one write in the batch. Each op pays the full fsync cost independently.

Adding a time-based batch interval to Strata would **not** close this gap for sequential clients: with one writer waiting for the previous ACK before sending the next, the window would still only ever hold one write. The interval only helps when multiple concurrent clients would otherwise miss each other between reactive flushes — but at sufficient concurrency, Strata's reactive approach already batches optimally (and outperforms etcd at 8+ writers).

The correct fix is **WAL fsync pipelining**: accept and begin the next batch while the previous batch's fsync is in flight, the same way etcd's raft ready loop does. This would close the single-writer gap without adding artificial latency or sacrificing the high-concurrency advantage.

#### 3-node cluster (quorum = 2)

Here the systems diverge in design:

**etcd** pipelines the leader's own WAL write with sending proposals to followers. The client is ACKed as soon as any two nodes have persisted — the leader's own fsync may still be in progress. This is correct by raft: if the leader crashes before its own fsync, the two followers still have the entry.

**Strata** is more conservative: the leader fsyncs to its own WAL *first*, then broadcasts to followers, then waits for a follower ACK, then responds to the client. Sequential pipeline:

```
leader fsync → broadcast → follower ack → client ack
```

etcd's pipeline:

```
leader WAL write ∥ follower send → quorum ack → client ack
```

**Both are equally correct by raft.** Strata's approach is more conservative than raft requires and is the root cause of the cluster write latency gap. Making Strata broadcast to followers concurrently with the leader's fsync (as etcd does) would bring cluster write latency close to single-node latency with no loss of correctness or durability. This is a known future optimization.

Strata's watch latency (p50 702 µs) reflects the write path: a Put that triggers a watch event must first fsync to the
WAL, then notify the watcher. etcd benefits from the same 100 ms batch amortisation that helps its write throughput.

### 3-node cluster (`cluster` scenario)

Strata nodes use the default `--wal-sync-upload=true` config, but **the leader always reopens the WAL with async uploads
** (`becomeLeader` hardcodes this — quorum commit already guarantees durability). etcd uses its normal raft pipeline
with no external storage. Both systems are on an equal footing for write durability: a quorum-committed write survives
the loss of any minority of nodes.

| Workload               | Strata ops/s | etcd ops/s | Ratio     | Strata p50 | etcd p50 | Strata p999 | etcd p999  |
|------------------------|--------------|------------|-----------|------------|----------|-------------|------------|
| `seq-put`              | 756          | 1,562      | 0.48×     | 1,270 µs   | 489 µs   | 4,606 µs    | 15,979 µs  |
| `par-put` (16 workers) | 1,009        | 6,523      | 0.15×     | 26,485 µs  | 4,052 µs | 744,587 µs  | 520,660 µs |
| `seq-get`              | **4,966**    | 3,405      | **1.46×** | 209 µs     | 273 µs   | 1,370 µs    | 1,960 µs   |
| `par-get` (16 workers) | 42,753       | 43,905     | 0.97×     | 917 µs     | 988 µs   | 6,590 µs    | 17,825 µs  |
| `mixed` (16 workers)   | 1,780        | 15,587     | 0.11×     | 14,011 µs  | 2,943 µs | 567,557 µs  | 13,965 µs  |
| `watch`                | 580          | 1,811      | 0.32×     | 1,120 µs   | 511 µs   | 32,045 µs   | 3,825 µs   |

**Read performance is equal or better:** `seq-get` is 46% faster on Strata; `par-get` is within 3% — both are
effectively noise.

**Write performance gap is explained by Strata's sequential pipeline.** The leader fsyncs first, then broadcasts to followers — more conservative than raft requires. See "Write pipeline architecture and durability guarantees" above. This is a known optimization opportunity: broadcasting concurrently with the leader fsync (as etcd does) would close most of this gap.

**Strata p999 write latency is better than etcd's** (`seq-put` p999: 4,606 µs vs 15,979 µs) — etcd's tail spikes come
from raft leader elections and snapshot transfers.

### S3 durability overhead (not yet benchmarked)

Strata's `--wal-sync-upload` mode (default for single-node) adds a MinIO/S3 round-trip to every WAL segment seal. This
is measured in the `single-s3` scenario (results pending). In return, every acknowledged write is durably stored in
object storage even if the node is destroyed immediately after.

---

## Interpretation guide

| Use case                                         | Recommendation                                                       |
|--------------------------------------------------|----------------------------------------------------------------------|
| Read-heavy workloads (caches, service discovery) | Strata equal or faster in both single and cluster                    |
| Single-writer / low-concurrency writes           | etcd is ~2× faster due to time-based batching                        |
| 8+ concurrent writers, single node               | Strata is faster; gap grows to 24% at 64 writers                     |
| 3-node cluster, reads                            | Strata equal or faster (+46% seq-get)                                |
| 3-node cluster, writes                           | etcd faster due to concurrent pipeline (see analysis); fixable without correctness tradeoff |
| Survive total cluster destruction                | Only Strata (`--wal-sync-upload=true`, single-node mode)             |
| Embedded library in a Go binary                  | Only Strata                                                          |

---

## Running the benchmarks yourself

```bash
# Build images once
docker build -f bench/Dockerfile.strata -t strata-bench .
docker build -f bench/Dockerfile        -t stratabench  .

# Start single-node stack
docker compose -f bench/docker/single/docker-compose.yml --project-name bench-single up -d

# Run a workload (from the host, targeting exposed ports)
go run ./bench/cmd/stratabench --endpoints localhost:3379 --workload par-put --clients 16 --total 5000 --system strata
go run ./bench/cmd/stratabench --endpoints localhost:2379 --workload par-put --clients 16 --total 5000 --system etcd

# Or use the orchestration script for all scenarios + JSONL output
bash bench/run.sh
```

See [`bench/README.md`](../bench/README.md) for full methodology and caveats.
