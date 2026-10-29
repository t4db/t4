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

---

## Collection reads and memory on a CR-heavy keyspace

A Kubernetes cluster holding many custom resources reads whole collections, so
these are measured separately: 40,000 objects of 4 KiB (≈156 MiB of data),
listed unpaginated and in 500-key pages.

Run on 2026-10-29 on the host described above, T4 at `fcb731f`.

### Latency

| Workload           | T4 before | T4 now  | etcd    | T4 : etcd |
|--------------------|-----------|---------|---------|-----------|
| Unpaginated list   | 860.5 ms  | 261.5 ms | 157.3 ms | 5.49× → 1.66× |
| One 500-key page   | 9.84 ms   | 6.40 ms | 3.70 ms | 2.64× → 1.73× |

"Before" is `ee298ac`, i.e. without the log-order fetch. etcd measured
156.2–157.3 ms across three runs, which is what makes the T4 delta readable;
quote that control whenever these are re-run.

### Memory

| Metric                      | T4 before  | T4 now        | etcd          | T4 : etcd  |
|-----------------------------|------------|---------------|---------------|------------|
| Allocated per list          | 1726 MiB   | 454–501 MiB   | 337 MiB       | 1.35–1.49× |
| Heap in use, idle           | 84.8 MiB   | 83.4 MiB      | 62.5–69.0 MiB | 1.25×      |
| Obtained from the OS, idle  | 179.2 MiB  | 179.2 MiB     | 82.6–86.6 MiB | 2.12×      |
| Container resident, idle    | 184.6 MiB  | 185 MiB       | 95.2–97.2 MiB | 1.92×      |

Reproduce with `./bench/memcompare.sh 40000 4096`; the ranges come from
repeated runs of it. etcd's allocation figure landed within 0.15% across three
runs, which is the control to check before reading anything into a T4 delta.

Reading a collection allocates **71% less** than it did, which is the same
change that made it faster: records are read in log order through one iterator
rather than one point lookup per key. The idle rows are unmoved by that work,
as they should be — nothing lists anything while the server sits idle — so they
describe what T4 costs to *hold* 40,000 objects, not to serve them.

**At this object count T4 costs more memory than etcd, on every axis.** Its
~179 MiB from the OS is close to Pebble's 64 MiB block cache plus its
memtables (32 MiB each, up to four): a floor, rather than a function of how
many objects are stored. etcd's per-key `treeIndex` is only a few MB at 40,000
keys, so etcd is cheap here and grows from there while T4 stays flat.

Which means 40,000 objects is the wrong place to ask the question. See the
sweep below: the two cross at roughly 290,000 objects, and this row is on the
side of that where etcd wins.

### Where the two cross

Idle memory against object count, 256-byte values, from
`./bench/indexsweep.sh`:

| Objects   | T4 heap | T4 sys | T4 container | etcd heap | etcd sys | etcd container |
|-----------|---------|--------|--------------|-----------|----------|----------------|
| 100,000   | 103.3   | 151.7  | 151.7        | 46.7      | 70.7     | 75.5           |
| 300,000   | 133.8   | 180.0  | 173.4        | 125.2     | 161.4    | 170.2          |
| 600,000   | 124.5   | 184.0  | 180.4        | 208.3     | 289.1    | 311.6          |
| 1,000,000 | 135.0   | 184.0  | 182.9        | 343.3     | 486.3    | 508.2          |

All MiB. Values are small on purpose: both stores' per-key costs are
independent of value size, while the data is not, and at 4 KiB a million
objects would not fit in the containers' 4 GiB. Take the slope from here and
the floor from the table above.

T4 is flat — 151.7 to 182.9 MiB across nine times the objects — and plateaus
near 184 MiB as Pebble's cache and memtables fill. etcd rises by about **480
bytes per key**. Fitting etcd as `50 + 0.000458 × keys` MiB against T4's flat
184 puts the crossover at **~290,000 objects**, which is where the 300,000 row
has them level.

| Cluster size        | Cheaper store                    |
|---------------------|----------------------------------|
| Under ~290k objects | etcd, by up to 2× at 100k        |
| Around ~290k        | no meaningful difference         |
| Over ~290k          | T4, by 1.7× at 600k and 2.8× at 1M |

This is the structural difference doing what it should: T4 keeps no permanent
per-key index, so a keyspace it can serve from a bounded cache costs bounded
memory, while etcd's in-memory index grows with every key. It also means a
memory comparison at a single object count is close to meaningless — this
file has twice reported a winner from one point and had to take it back.

Lowering T4's floor moves the crossover down, and the floor is Pebble's block
cache plus its memtables. `Config.PebbleOptions` can already change both, but
nothing on the `t4 run` command line reaches it, so a deployment cannot. On
the fit above, a 100 MiB floor would cross at ~110k objects and an 80 MiB one
at ~65k.

> An earlier run reported T4 at 12–15 MiB of heap against etcd's 76 MiB, and
> 517 MiB allocated per list against etcd's 719 MiB. Neither reproduces, and
> some of it appears in commit messages from that work. The heap figure is not
> consistent with Pebble's own cache sizes, so it was most likely sampled
> before those caches filled; the etcd allocation figure is more than double
> what two later runs measured to within 0.1%. Prefer the numbers in this
> section.

### Measuring memory at all

`bench/run.sh` samples container memory once a second. That number moves with
GC timing, the page cache and the sampling phase: asked twice for a paginated
read whose page is smaller than the fetch window — identical work either way —
it answered 381 MiB and 621 MiB. It is fine for spotting an order of magnitude
and misleading for comparing two builds.

Use `go_memstats_alloc_bytes_total` for that instead. It only ever grows, so it
is independent of the collector, and `bench/memcompare.sh` reads it either side
of a known number of reads. Single samples of peak or resident memory have
produced two wrong conclusions in this file's history; a claim about memory
wants a cumulative counter, repeated runs, and etcd measured the same way
alongside as a control.

---

## Interpretation guide

| Use case                                  | Recommendation                                               |
|-------------------------------------------|--------------------------------------------------------------|
| Reading whole collections (many CRs)      | etcd is faster by ~1.7×; on memory it depends on object count |
| Under ~290k objects                       | etcd uses less memory, by up to 2× at 100k                   |
| Over ~290k objects                        | T4 uses less memory, by 1.7× at 600k and 2.8× at 1M          |
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

For the collection-read and memory figures above:

```bash
# Latency, with etcd measured alongside as a control
BENCH_WORKLOADS="list list-paged" BENCH_TOTAL=40000 BENCH_VAL_SIZE=4096 \
  ./bench/run.sh single

# Memory floor at a realistic value size
./bench/memcompare.sh 40000 4096

# How that scales with object count, which is what decides the comparison
./bench/indexsweep.sh 256 100000 300000 600000 1000000
```
