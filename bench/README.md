# T4 vs etcd Benchmark

A self-contained benchmark suite that compares T4 against etcd using the
**etcd v3 protocol** — the same wire format both systems speak, measured with
the same Go client (`clientv3 v3.6.4`).

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | 24+ |
| Docker Compose plugin | v2.20+ |
| Go | 1.22+ (only needed to run `t4bench` outside Docker) |

The benchmark builds everything from source — no pre-built binaries required.

## Quick Start

```bash
# from the repository root:
./bench/run.sh
```

This builds both Docker images, runs all three scenarios (≈15–30 min), and
prints a comparison table at the end. Results are written to
`bench/results/results.jsonl`.

To run only a subset of scenarios:

```bash
./bench/run.sh single          # fastest, no S3
./bench/run.sh single-s3       # with MinIO WAL upload
./bench/run.sh cluster         # 3-node both
./bench/run.sh single cluster  # two scenarios
```

Tune ops count and concurrency with environment variables:

```bash
BENCH_TOTAL=10000 BENCH_CLIENTS=8 ./bench/run.sh single
```

## Workloads

| Workload | Description | Key insight |
|----------|-------------|-------------|
| `seq-put` | 1 client, sequential unique Puts | single-writer WAL throughput |
| `par-put` | 16 clients, parallel unique Puts | group-commit batching benefit |
| `seq-get` | 1 client, sequential Gets | read latency floor |
| `par-get` | 16 clients, parallel Gets | read scalability |
| `mixed` | 16 clients, 50 % Put / 50 % Get | realistic mixed workload |
| `watch` | sequential Put → watch event | server-side watch fan-out latency |

Default: 50 000 ops per workload, 64-byte keys, 256-byte values.

## Scenarios

### `single` — local WAL, no remote storage

| Service | Image | Notes |
|---------|-------|-------|
| T4 | built from source | WAL on local disk only, no S3 |
| etcd | `bitnami/etcd:3.6` | single node, default config |

**What it shows:** Raw throughput advantage of T4's group-commit WAL when
remote-durability is not required (e.g. ephemeral caches, dev environments).

**Caveat:** T4's WAL is local-only here. etcd fsyncs to local disk on
every write. These are different durability guarantees — see `single-s3` for
a fairer comparison.

---

### `single-s3` — WAL sync-uploaded to MinIO

| Service | Image | Notes |
|---------|-------|-------|
| T4 | built from source | `--wal-sync-upload=true`, MinIO as S3 |
| etcd | `bitnami/etcd:3.6` | single node, default config |
| MinIO | `minio/minio:latest` | local Docker, S3-compatible |

**What it shows:** T4 with remote durability comparable to etcd's fsync
model. Every WAL segment is uploaded to MinIO before the write ACK is returned.

**Caveat:** MinIO runs on the same machine (Docker bridge), so network latency
is ~sub-millisecond. In a real deployment T4 would upload to S3 over a
LAN/WAN, adding 1–20 ms per segment. etcd's fsync latency depends on the
storage medium (NVMe: ~100 µs, cloud disk: 1–5 ms).

---

### `cluster` — 3-node T4 vs 3-node etcd Raft

| Service | Image | Notes |
|---------|-------|-------|
| t4{1,2,3} | built from source | WAL + leader election via MinIO |
| etcd{1,2,3} | `bitnami/etcd:3.6` | standard Raft cluster |
| MinIO | `minio/minio:latest` | shared S3 for T4 |

**What it shows:** End-to-end quorum write throughput and latency for a
replicated cluster. The benchmark sends to all endpoints; `clientv3` routes
writes to the current leader automatically for both systems.

**Replication model differences:**

| | T4 | etcd |
|-|--------|------|
| Consensus | leader streams WAL; all followers ACK | Raft (quorum = 2/3) |
| Leader election | S3 object lock | Raft election |
| Read linearizability | ReadIndex RPC to leader | ReadIndex RPC to leader |
| Follower serializable reads | yes (`--read-consistency=serializable`) | no (redirect) |
| Durability | WAL archived to S3 | WAL on local disk per node |

---

## Interpreting Results

```
┌─ Scenario: single ──────────────────────────────────────────────────────────
│  workload     system     ops/sec    p50 µs    p90 µs    p99 µs   p999 µs
│ ─────────────────────────────────────────────────────────────────────────
│  seq-put      t4      14 230       340       520     1 100     4 200
│               etcd        11 560       430       680     1 350     5 800
│  par-put      t4      81 400        82       180       620     2 100
│               etcd        38 200       180       390     1 100     4 700
│  seq-get      t4      52 000        92       145       380     1 200
│               etcd        47 800       102       160       410     1 350
...
```

- **`par-put` throughput ratio** is the headline number for T4's
  group-commit advantage: parallel writers share WAL fsyncs.
- **`seq-put` difference** shows single-writer overhead (scheduling, gRPC
  framing) — expect <30 % difference.
- **`seq-get` / `par-get`** are dominated by Pebble vs bbolt read performance.
- **`watch`** measures server-side fan-out latency, not throughput.
- High `p999` relative to `p99` is normal — occasional GC pauses or fsync
  outliers; both systems show this pattern.

## Running `t4bench` Manually

Build outside Docker (requires Go):

```bash
go build -o t4bench ./bench/cmd/t4bench
```

Run against any etcd-compatible endpoint:

```bash
./t4bench \
    --endpoints localhost:2379 \
    --workload par-put \
    --clients 16 \
    --total 100000

./t4bench \
    --endpoints localhost:3379 \
    --workload par-put \
    --clients 16 \
    --total 100000 \
    --scenario single \
    --system t4 \
    --output jsonl >> results/results.jsonl
```

Print a comparison table from a collected JSONL file:

```bash
./t4bench compare --results bench/results/results.jsonl
```

## File Layout

```
bench/
├── cmd/t4bench/main.go   — load generator (workloads + compare command)
├── docker/
│   ├── single/               — 1 T4 + 1 etcd (no S3)
│   ├── single-s3/            — 1 T4 + MinIO + 1 etcd
│   └── cluster/              — 3-node T4 + MinIO + 3-node etcd
├── results/                  — gitignored; written by run.sh
├── Dockerfile                — builds t4bench image
├── Dockerfile.t4         — builds t4-bench image
├── run.sh                    — full benchmark orchestration
└── README.md                 — this file
```
