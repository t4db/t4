---
title: Benchmarks
description: T4 vs etcd benchmark results and analysis — single-node, sync-S3, and 3-node cluster.
---

# Benchmark Results: T4 vs etcd

Head-to-head comparison of T4 and etcd, run from the repository benchmark harness in Docker on the same host.

## Environment

|                      |                                                                      |
|----------------------|----------------------------------------------------------------------|
| **Date**             | 2026-04-12                                                           |
| **Host**             | Apple Silicon Mac (12 vCPUs, 14 GB RAM assigned to Docker)           |
| **Docker**           | VirtioFS-backed volumes, Linux VM via macOS Virtualization.framework |
| **T4**               | Pebble storage, group-commit write pipeline                          |
| **etcd**             | v3.6.4, bbolt storage                                                |
| **Client**           | `t4bench` — etcd v3 Go client, one TCP connection per worker         |
| **Ops**              | 50,000 total per workload                                            |
| **Parallel workers** | 16 (for `par-*` and `mixed` workloads)                               |
| **Key size**         | 64 bytes                                                             |
| **Value size**       | 256 bytes                                                            |

> **Note:** All numbers reflect Docker-on-macOS performance. Every `fsync` crosses the macOS -> hypervisor -> Linux VM
> boundary, adding overhead compared to native Linux. Relative comparisons between T4 and etcd are meaningful; absolute
> numbers are not representative of production Linux performance.

---

## Results

### Single node — no S3 (`single` scenario)

| Workload                        | T4 ops/s | etcd ops/s | Ratio | T4 p50   | etcd p50 |
|---------------------------------|----------|------------|-------|----------|----------|
| `seq-put`                       | 1,180    | 4,532      | 0.26× | 473 µs   | 188 µs   |
| `par-put` (16 workers)          | 5,935    | 13,271     | 0.45× | 1,951 µs | 1,122 µs |
| `seq-get`                       | 7,399    | 9,563      | 0.77× | 116 µs   | 99 µs    |
| `par-get` (16 workers)          | 36,524   | 26,023     | 1.40× | 369 µs   | 297 µs   |
| `mixed` (16 workers, 50/50 r/w) | 11,697   | 16,408     | 0.71× | 693 µs   | 867 µs   |
| `watch`                         | 2,113    | 4,356      | 0.49× | 457 µs   | 200 µs   |

### Single node — synchronous S3 durability (`single-s3` scenario)

| Workload                        | T4 ops/s | etcd ops/s | Ratio | T4 p50   | etcd p50 |
|---------------------------------|----------|------------|-------|----------|----------|
| `seq-put`                       | 642      | 3,011      | 0.21× | 1,494 µs | 200 µs   |
| `par-put` (16 workers)          | 4,327    | 10,532     | 0.41× | 3,200 µs | 1,033 µs |
| `seq-get`                       | 8,151    | 9,940      | 0.82× | 116 µs   | 97 µs    |
| `par-get` (16 workers)          | 36,428   | 26,816     | 1.36× | 376 µs   | 296 µs   |
| `mixed` (16 workers, 50/50 r/w) | 5,337    | 12,881     | 0.41× | 1,771 µs | 875 µs   |
| `watch`                         | 479      | 3,749      | 0.13× | 1,467 µs | 198 µs   |

### 3-node cluster (`cluster` scenario)

T4 cluster nodes use a local S3-compatible server (RustFS) for leader election and WAL/checkpoint archival; etcd uses a standard 3-node raft cluster.

| Workload               | T4 ops/s | etcd ops/s | Ratio | T4 p50   | etcd p50 | T4 p999   | etcd p999 |
|------------------------|----------|------------|-------|----------|----------|-----------|-----------|
| `seq-put`              | 1,440    | 2,000      | 0.72× | 579 µs   | 467 µs   | 4,659 µs  | 2,157 µs  |
| `par-put` (16 workers) | 8,609    | 7,378      | 1.17× | 1,646 µs | 2,074 µs | 13,421 µs | 9,525 µs  |
| `seq-get`              | 5,194    | 3,680      | 1.41× | 199 µs   | 269 µs   | 961 µs    | 1,353 µs  |
| `par-get` (16 workers) | 23,905   | 13,039     | 1.83× | 438 µs   | 687 µs   | 27,859 µs | 48,195 µs |
| `mixed` (16 workers)   | 9,867    | 7,942      | 1.24× | 1,241 µs | 1,725 µs | 20,512 µs | 24,713 µs |
| `watch`                | 1,635    | 1,895      | 0.86× | 576 µs   | 491 µs   | 2,900 µs  | 2,826 µs  |

---

## Analysis

### Single-node writes: etcd still leads

In both `single` and `single-s3`, etcd remains clearly faster for `seq-put`, `par-put`, `mixed`, and `watch`.
On this host, T4's single-node write path is still the main performance gap relative to etcd.

### Single-node reads: T4 scales better on parallel gets

T4 is slightly slower on `seq-get`, but clearly faster on `par-get` in both single-node scenarios. That suggests the
current T4 server + Pebble path scales well under parallel readers even when it does not win the single-request latency
floor.

### S3 durability overhead

Comparing `single` to `single-s3`, T4 read throughput stays nearly flat while write-heavy workloads slow down:

- `seq-put`: 1,180 -> 642 ops/s
- `par-put`: 5,935 -> 4,327 ops/s
- `mixed`: 11,697 -> 5,337 ops/s
- `watch`: 2,113 -> 479 ops/s

This is the expected cost of placing synchronous WAL upload on the write durability path.

### Cluster results are much stronger for T4 than the previous published run

At 16 clients, T4 wins `par-put`, `seq-get`, `par-get`, and `mixed` in the 3-node cluster scenario. etcd still
leads on `seq-put` and is slightly ahead on `watch`, but the gap is small there compared to the earlier numbers.

The updated headline is:

- cluster reads are better on T4
- cluster mixed traffic is better on T4
- cluster parallel writes are better on T4
- single-node writes are still better on etcd
- 
---

## Interpretation guide

| Use case                                  | Recommendation                                               |
|-------------------------------------------|--------------------------------------------------------------|
| Single-node write-heavy workloads         | etcd is faster in this run                                   |
| Single-node read-heavy parallel workloads | T4 is competitive and faster on `par-get`                    |
| Single-node with remote durability        | T4 works, but synchronous S3 upload has a visible write cost |
| 3-node cluster, read-heavy workloads      | T4 is faster in this run                                     |
| 3-node cluster, mixed / parallel traffic  | T4 is faster in this run                                     |
| 3-node cluster, sequential writes         | etcd still leads slightly in this run                        |
| Survive total single-node destruction     | Only T4 with `--wal-sync-upload=true`                        |
| Embedded library in a Go binary           | Only T4                                                      |

---

## Running the benchmarks yourself

```bash
# Build images once
docker build -f bench/Dockerfile.t4 -t t4-bench .
docker build -f bench/Dockerfile    -t t4bench .

# Run all scenarios
./bench/run.sh

# Or a subset
./bench/run.sh single
./bench/run.sh single-s3
./bench/run.sh cluster
```

The raw results from the latest run are written to `bench/results/results.jsonl`.
