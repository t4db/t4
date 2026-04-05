# Strata Production Readiness Checklist

## Overview
This checklist defines the criteria required for Strata to be considered production-ready.
It is divided into stages and clear pass/fail requirements.

---

## Stage 1 — Internal / Dev Readiness

### Correctness
- [x] No lost acknowledged writes — WAL + quorum ACK; `TestLeaderCrashBeforeWALFlush`, `TestDeletedKeyDurability`
- [x] Monotonic revisions guaranteed — `TestRevisionMonotonicity`, `TestConcurrentCompactPutRevisionUniqueness`
- [x] WAL + checkpoint replay deterministic — `TestWALReplayAfterPartialUpload`, `TestStartupCheckpointCoversLocalWAL`
- [x] No split-brain commits (fencing works) — conditional PUT (`If-None-Match`/`If-Match`); `TestCommitLoopWALErrorFences`

### Basic Operations
- [x] Can start cluster (1 node) — `TestE2EOffline`, `TestE2ESingleNodeS3`
- [x] Can scale 1 → 3 → 1 — `TestScale3To1`, `TestE2EThreeNode`
- [x] Can restart nodes without data loss — `TestNodeRestart`

### Recovery
- [x] Restore from latest checkpoint works — `TestStartupCheckpointCoversLocalWAL`, `TestE2ESingleNodeS3` reopen
- [x] Full cluster restart works — `TestE2EThreeNode`

### Observability
- [x] Basic metrics exposed — `internal/metrics/metrics.go` (Prometheus); `--metrics-addr` flag
- [x] Logs show leader changes and WAL progress — logrus throughout

---

## Stage 2 — Small Production (≤ 50 nodes)

### Consistency Contract (DOCUMENTED)
- [x] Write durability definition — `docs/operations.md` (WAL sync upload, quorum ACK)
- [x] Linearizable vs serializable reads defined — `docs/configuration.md`, `--read-consistency` flag
- [x] Behavior under S3 failure defined — `docs/operations.md` "S3 unavailability" (cluster vs single-node, recovery)
- [x] Behavior under network partition defined — `docs/operations.md` "Network partitions" (split-brain prevention, convergence on heal)

### Failure Testing (AUTOMATED)
- [x] Kill leader during write — `TestLeaderCrashBeforeWALFlush`
- [x] Kill follower during commit — `TestFollowerKilledDuringCommit`
- [x] Kill all nodes and recover — `TestE2EThreeNode`
- [x] S3 temporarily unavailable — `TestObjectStoreUnavailableWritesSucceed`, `TestObjectStoreUnavailableRecovery`
- [x] Network partition scenarios — `TestNetworkPartitionNoSplitBrain` (proxy-based partition, split-brain check, heal + resync)

### Performance
- [ ] Stable p95 write latency — `bench_test.go` exists but not a CI gate
- [ ] Stable watch latency — not measured
- [ ] No stalls under moderate concurrency — `TestLongRunningConsistency` (stress) covers basic case

### Backup / Restore
- [x] Restore CLI implemented — `strata branch fork/unfork`; `RestorePoint` in `restore.go`
- [x] Restore tested end-to-end — `TestRestorePoint`
- [x] Restore from arbitrary checkpoint works — `strata restore list` + `strata restore checkpoint`; documented in `docs/operations.md`

### Operations
- [x] Install documented — `docs/operations.md`
- [x] Upgrade documented — `docs/operations.md` (add node to running cluster)
- [x] GC / compaction documented — `docs/operations.md`

---

## Stage 3 — Production Ready

### Reliability
- [ ] Passes repeated chaos testing
- [ ] No data corruption in long soak tests (≥ 1 week)
- [ ] WAL corruption recovery tested — partial (`TestWALReplayAfterPartialUpload` covers partial upload)
- [x] Checkpoint corruption recovery tested — `TestCheckpointCorruptionManifest`, `TestCheckpointCorruptionArchive`

### Scalability Envelope (DOCUMENTED)
- [ ] Max tested nodes: ______
- [ ] Max tested writes/sec: ______
- [ ] Max watchers: ______
- [ ] Max dataset size: ______

### Tail Latency
- [ ] p99 write latency acceptable
- [ ] p99 watch latency acceptable
- [ ] Failover time measured and stable

### Observability (ADVANCED)
- [x] Leader / follower state metrics — `strata_role` gauge
- [ ] Follower lag metrics — no per-follower lag metric
- [x] WAL throughput metrics — `strata_wal_uploads_total`, `strata_wal_upload_duration_seconds`
- [ ] S3 error + latency metrics — WAL upload errors tracked; no general S3 latency metric
- [ ] Alerting defined

### Upgrade & Compatibility
- [ ] Backward-compatible WAL format
- [ ] Backward-compatible checkpoint format
- [ ] Rolling upgrade supported
- [ ] Downgrade path defined

### Security
- [x] TLS between nodes — peer mTLS (`--peer-tls-*` flags)
- [x] Auth for clients — etcd-compatible RBAC (`--auth-enabled`)
- [x] Encryption for WAL/checkpoints (optional) — AES-256-GCM (`--encryption-key-file`/`--encryption-key-env`); in progress on `enc-at-rest` branch

---

## Stage 4 — External Users / Product

### Documentation
- [x] Architecture doc — `docs/architecture.md`
- [ ] Consistency model doc — partial (scattered across `docs/configuration.md`)
- [ ] Failure scenarios doc — not comprehensive; no dedicated doc
- [ ] Backup / restore guide — no dedicated guide
- [ ] Kubernetes integration guide — not present

### Usability
- [ ] CLI complete (backup, restore, branch, gc, status) — branch CLI done; no gc/status/backup subcommands
- [ ] Helm chart or install script — not present
- [ ] Minimal setup steps — `docs/operations.md` covers basics

### Supportability
- [ ] Runbooks for incidents
- [ ] Troubleshooting guide
- [ ] Metrics dashboard examples

---

## Final Production Gate

Strata can be called production-ready if:

- [ ] All Stage 2 requirements are complete
- [ ] Failure scenarios are tested and reproducible
- [ ] Restore is proven and documented
- [ ] Consistency guarantees are explicit
- [ ] System runs Kubernetes reliably under real workloads
- [ ] Another engineer can deploy and recover the system using only documentation

---

## Notes

- Performance alone does NOT define production readiness.
- Correctness + recovery + operability are the critical factors.
- Prefer staged rollout instead of a binary "production-ready" claim.
