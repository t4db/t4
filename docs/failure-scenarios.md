# Failure Scenarios

This document describes how T4 behaves under various failure conditions, what data is at risk, and how to recover. Each scenario maps to automated tests.

---

## Leader crash

### Before WAL flush

**Scenario:** the leader process dies (kill -9 or OOM) after accepting a write request but before fsyncing it to the local WAL.

**Outcome:**
- The write was never fsynced → it never existed durably. The client connection is broken; the client receives an error or a timeout.
- No data is lost because nothing was durably stored.

**Recovery:**
- A follower wins election in ~6 s (2 × `FollowerRetryInterval` after `FollowerMaxRetries`; ~0.3 s with a graceful shutdown broadcast).
- The new leader loads the latest S3 checkpoint and replays any WAL segments ahead of it.
- Clients retry against the new leader.

**Tested by:** `TestLeaderCrashBeforeWALFlush`

---

### After WAL flush, before ACK to client

**Scenario:** the leader fsyncs the entry to its local WAL and broadcasts to followers, but crashes before returning to the client.

**Outcome:**
- The entry is on the leader's WAL (and on follower WALs if ACKs were received before the crash).
- The client gets a broken connection.

**Recovery:**
- The new leader replays the leader's WAL from S3 and applies all entries up to the highest committed revision.
- The write is visible after recovery. The client may retry safely — a Create (conditional) will fail with ErrExists (already committed), while a Put will be idempotent.

**Tested by:** `TestLeaderCrashBeforeWALFlush` (covers post-flush path)

---

## Follower crash

**Scenario:** a follower process dies while the leader and at least one other follower are healthy.

**Outcome:**
- Leader detects the disconnect immediately.
- With default `FollowerWaitMode=quorum`, the leader needs ACKs from ⌊n/2⌋ + 1 nodes. In a 3-node cluster losing one follower still leaves the leader + 1 follower = quorum. Writes continue.
- The crashed follower's writes that were in-flight are lost locally; but those writes were not quorum-ACKed, so they were not acknowledged to the client.

**Recovery:**
- Restart the follower. On startup it loads the latest S3 checkpoint and replays any missing WAL segments from S3 before reconnecting to the leader.
- If the follower restarted with a fresh disk (container replacement), the S3 path restores the full state.

**Tested by:** `TestFollowerKilledDuringCommit`

---

## Full cluster restart

**Scenario:** all nodes are stopped or fail simultaneously.

**Outcome:**
- No writes are accepted while all nodes are down.
- All nodes restart independently. Each loads the latest S3 checkpoint and replays WAL segments.
- Leader election proceeds via S3 conditional PUT. The first node to successfully write the lock becomes the new leader.

**Recovery:**
```bash
# Restart each node normally — they self-coordinate via S3.
t4 run --data-dir /var/lib/t4 --s3-bucket my-bucket --s3-prefix t4/ \
           --peer-listen 0.0.0.0:3380 ...
```

Any write that completed quorum ACK before the cluster went down exists on all nodes' WALs and survives.

**Tested by:** `TestE2EThreeNode`

---

## S3 unavailability

### Cluster mode

**Scenario:** S3 becomes unreachable while the cluster is running.

**Outcome:**
- Writes continue. WAL segment uploads and checkpoints queue in the background.
- Leader election liveness touches fail with transient errors — the lock is not refreshed, but the `LastSeenNano` timestamp was last set before the outage. The cluster remains stable as long as no new election is triggered during the outage window.
- If S3 is unreachable for > `LeaderLivenessTTL` (6 s) **and** a follower tries to promote, the follower cannot write the new lock either → no election, no split-brain.

**Recovery:**
- When S3 becomes reachable again, queued uploads resume automatically.
- No manual intervention needed if the cluster stayed up during the outage.

**Tested by:** `TestObjectStoreUnavailableWritesSucceed`, `TestObjectStoreUnavailableRecovery`

---

### Single-node mode

**Scenario:** S3 becomes unreachable in single-node mode (where `WALSyncUpload=true`, the default).

**Outcome:**
- Writes succeed until the current open WAL segment fills and a rotation is attempted. On rotation, the seal-and-upload step fails. The node fences itself to prevent unacknowledged data from accumulating silently.
- All writes acknowledged before the outage are durable — their segments are already on S3.

**Recovery:**
1. Restore S3 access.
2. Restart the node. It replays all local WAL segments (including any partial segment) before resuming.

**Tested by:** `TestObjectStoreUnavailableWritesSucceed`

---

## Network partition

### Follower partitioned from leader

**Scenario:** a follower cannot reach the leader or S3, but the leader can still reach S3 and the other followers.

1. Follower exhausts `--follower-max-retries` reconnect attempts.
2. Follower reads the S3 lock — `LastSeenNano` is fresh (≤ 2 s old) → **does not promote**.
3. Leader continues writing with the remaining nodes.
4. When the partition heals, the follower reconnects and resyncs from the leader's ring buffer.

**Result:** no data loss, no split-brain, no service interruption.

---

### Leader partitioned from followers and S3

1. Leader stops refreshing `LastSeenNano`.
2. After 6 s the lock goes stale.
3. A follower races to write a new lock with `If-Match: <etag>`. Only one wins.
4. New leader begins serving. Old leader detects supersession on its next fenced check and steps down.
5. Old leader's uncommitted in-flight writes are dropped.

**Failover time:** ~6 s with default settings (2 × `FollowerRetryInterval` = 4 s, plus election time). ~0.3 s with graceful shutdown broadcast.

**Tested by:** `TestNetworkPartitionNoSplitBrain`, `TestFailoverTime`

---

## WAL corruption

### CRC mismatch mid-segment

**Scenario:** a WAL segment file is partially corrupted (bit flip, torn write, truncated upload).

**Outcome:**
- On replay, `ReadEntry` detects the CRC mismatch and returns an error.
- All entries before the corrupted one are applied. The partially-written entry and all subsequent entries in that segment are discarded.
- A warning is logged: `wal: CRC mismatch at rev=N`.

**Recovery:**
- If only the trailing entry of the last segment is corrupted (typical torn-write scenario on crash), the node recovers all entries up to the corruption point. The last write that failed the CRC was not durably acknowledged (the client received an error), so no data is lost.
- If corruption is deeper, entries after the corruption point in that segment are lost. Restore from a checkpoint (see [Backup and Restore](backup-restore.md)) and replay WAL segments from S3.

**Tested by:** `TestWALCorruptionMidSegment`, `TestWALReplayAfterPartialUpload`

---

### Checkpoint corruption

**Scenario:** a checkpoint manifest or archive on S3 is corrupted or incomplete.

**Outcome:**
- `ReadManifest` or `ReadCheckpointIndex` fails with a JSON parse or integrity error.
- The node logs the error and falls back to the previous checkpoint.
- If no valid checkpoint exists, the node fails to start and prints a clear error.

**Recovery:**
- Use `t4 restore list` to identify the last good checkpoint.
- Use `t4 restore checkpoint` to download it to a fresh `--data-dir`.
- Start the node normally pointing at the new data directory.

**Tested by:** `TestCheckpointCorruptionManifest`, `TestCheckpointCorruptionArchive`

---

## Chaos: random node kills

**Scenario:** random cluster nodes (leaders and followers) are killed and restarted from fresh data directories across multiple rounds of concurrent writes.

**Outcome verified by `TestChaos`:**
- Every write acknowledged before a kill is visible on all surviving nodes after recovery.
- The cluster re-elects a leader after each kill without split-brain.
- Nodes restarted from a fresh disk recover fully via the S3 checkpoint + WAL replay path.

To run extended chaos testing:

```bash
T4_CHAOS_ROUNDS=100 go test -run TestChaos -timeout 600s
```

---

## Failover timing

| Event | Typical time |
|---|---|
| Graceful leader shutdown (broadcast) | ~12 ms |
| Crash failover (default settings) | ~6 s (2 × `FollowerRetryInterval` after `FollowerMaxRetries`) |
| Follower reconnect after network heal | < 1 s |

Tested by `TestFailoverTime`.

---

## See also

- [Consistency model](consistency.md) — durability definitions, CAP properties
- [Operations guide](operations.md) — S3 unavailability, network partitions, recovery procedure
- [Backup and restore](backup-restore.md) — recovering from checkpoint after severe data corruption
