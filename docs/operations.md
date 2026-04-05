# Operations Guide

## Single-node with S3

The simplest durable deployment. A single node writes WAL segments and checkpoints to S3. If the node is replaced or its disk is lost, it recovers automatically on the next start.

```bash
strata run \
  --data-dir  /var/lib/strata \
  --listen    0.0.0.0:3379  \
  --s3-bucket my-bucket     \
  --s3-prefix strata/
```

AWS credentials are resolved from the standard chain: `AWS_*` environment variables, `~/.aws/credentials`, instance profile (EC2/ECS), workload identity (EKS).

### MinIO or other S3-compatible stores

```bash
strata run \
  --data-dir    /var/lib/strata \
  --listen      0.0.0.0:3379   \
  --s3-bucket   my-bucket      \
  --s3-prefix   strata/        \
  --s3-endpoint http://minio:9000
```

---

## Multi-node cluster

Multi-node mode requires:

1. A shared S3 bucket (leader election lock + WAL archive).
2. Each node has a unique `--node-id` and a `--peer-listen` address reachable by all other nodes.

All nodes run the same command. At startup they race to acquire the S3 leader lock; the winner becomes the leader, the rest become followers.

### Three-node example

```bash
# Node A
strata run \
  --data-dir       /var/lib/strata     \
  --listen         0.0.0.0:3379        \
  --s3-bucket      my-bucket           \
  --s3-prefix      strata/             \
  --node-id        node-a              \
  --peer-listen    0.0.0.0:3380        \
  --advertise-peer node-a.internal:3380

# Node B
strata run \
  --data-dir       /var/lib/strata     \
  --listen         0.0.0.0:3379        \
  --s3-bucket      my-bucket           \
  --s3-prefix      strata/             \
  --node-id        node-b              \
  --peer-listen    0.0.0.0:3380        \
  --advertise-peer node-b.internal:3380

# Node C — same as B, different --node-id and --advertise-peer
```

### Leader election and failover

- On startup each node reads the S3 lock. If absent, it issues an **atomic conditional PUT** (`If-None-Match: *`); only one concurrent writer wins. The winner becomes the leader and records `LastSeenNano = now()` in the lock so followers see it as immediately alive.
- The leader streams WAL entries to all followers over the peer port (default 3380). Followers apply entries and serve local reads.
- A follower that observes `--follower-max-retries` consecutive stream failures (~10 s at default 5 × 2 s) checks the lock's `LastSeenNano`. If stale (older than `LeaderLivenessTTL` = 6 s), it attempts a takeover using `If-Match: <etag>` — only the candidate that read the same ETag wins the race. The new leader records its own address and `LastSeenNano`.
- The former leader periodically re-reads the S3 lock (`--leader-watch-interval-sec`, default 300 s) and on every follower disconnect. Each check reads the lock **with its ETag**, then — if still the owner — writes a liveness touch using `If-Match: <etag>`. If the conditional touch is rejected (`ErrPreconditionFailed`), a new leader has taken over between the Read and the Touch: the old leader steps down immediately. This closes the Read→Touch split-brain race without a second round-trip.
- Writes sent to a follower are automatically forwarded to the current leader and the result is returned to the caller.

Leader election uses atomic conditional PUT (`If-None-Match`/`If-Match` on the `leader-lock` object). There is no TTL polling — the only S3 election writes are at startup, on leader takeover, and during liveness touches while followers are disconnected. In cluster mode, writes additionally require quorum ACK from all connected followers before returning to the caller.

### Adding a node to a running cluster

Start a new node with a fresh `--data-dir` and the same S3 bucket. It will:

1. Read the S3 manifest and restore the latest checkpoint.
2. Replay any WAL segments uploaded since the checkpoint.
3. Lose the election (leader already holds the lock) and become a follower.
4. Receive the live WAL stream from the leader to catch up to the current revision.

No manual registration or cluster membership changes are required.

### Scaling down

Close the nodes you want to remove. The remaining nodes continue without any configuration change. If the leader is among the removed nodes, a follower will take over.

---

## mTLS between peers

Provide a shared CA and a per-node certificate/key pair (all PEM format):

```bash
strata run \
  ... \
  --peer-tls-ca   /etc/strata/tls/ca.crt  \
  --peer-tls-cert /etc/strata/tls/node.crt \
  --peer-tls-key  /etc/strata/tls/node.key
```

Both the leader's gRPC server and the follower's gRPC client use these files. The same CA must be used on all nodes. TLS 1.3 is required; mutual authentication is enforced.

### Embedded library

Pass `credentials.TransportCredentials` directly:

```go
serverCreds, clientCreds, err := buildTLS(caFile, certFile, keyFile)

node, err := strata.Open(strata.Config{
    ...
    PeerServerTLS: serverCreds,
    PeerClientTLS: clientCreds,
})
```

---

## Client TLS

By default the etcd gRPC port (3379) is plaintext. Enable TLS to encrypt traffic between clients and the server.

### Server-only TLS (encryption, no client cert required)

```bash
strata run \
  ... \
  --client-tls-cert /etc/strata/tls/server.crt \
  --client-tls-key  /etc/strata/tls/server.key
```

Clients connect with TLS but are not required to present a certificate. Use this when clients are etcd-compatible tools or libraries that support TLS but not mTLS.

```bash
etcdctl --endpoints=https://localhost:3379 \
        --cacert /etc/strata/tls/ca.crt \
        put /hello world
```

### Mutual TLS (mTLS, client cert required)

Add `--client-tls-ca` to require clients to present a certificate signed by the given CA:

```bash
strata run \
  ... \
  --client-tls-cert /etc/strata/tls/server.crt \
  --client-tls-key  /etc/strata/tls/server.key \
  --client-tls-ca   /etc/strata/tls/ca.crt
```

```bash
etcdctl --endpoints=https://localhost:3379 \
        --cacert  /etc/strata/tls/ca.crt  \
        --cert    /etc/strata/tls/client.crt \
        --key     /etc/strata/tls/client.key \
        put /hello world
```

Client TLS and peer mTLS are independent — each uses its own cert/key/CA and can be enabled or disabled separately.

---

## Authentication and RBAC

Strata implements the etcd v3 Auth API: username/password authentication with bearer tokens, and role-based access control scoped to key prefixes. Auth state (users, roles, enabled flag) is stored in Pebble and flows through the WAL, so it is replicated to followers and included in S3 checkpoints. Bearer tokens are persisted to Pebble and survive node restarts — clients do not need to re-authenticate after a restart.

Enable auth with `--auth-enabled`:

```bash
strata run \
  ... \
  --auth-enabled \
  --token-ttl 300   # bearer token lifetime in seconds (default: 300)
```

### Initial setup

Auth cannot be enabled unless a `root` user exists. Bootstrap with `etcdctl`:

```bash
ETCDCTL_API=3 etcdctl --endpoints=localhost:3379 user add root
# Enter password at prompt

ETCDCTL_API=3 etcdctl --endpoints=localhost:3379 auth enable
```

Once enabled, all KV and Watch requests require a valid bearer token. The `root` user has unconditional access to all keys via the built-in `root` role.

> **Note:** The `root` user and `root` role cannot be deleted while auth is enabled.

### Authenticating

```bash
ETCDCTL_API=3 etcdctl --endpoints=localhost:3379 \
  --user root:yourpassword \
  put /hello world
```

The etcd client library handles token acquisition and refresh automatically when `--user` is provided. Tokens expire after `--token-ttl` seconds; the client re-authenticates transparently.

### Managing users

```bash
# Create a user
etcdctl --endpoints=localhost:3379 --user root:pass user add alice

# List users
etcdctl --endpoints=localhost:3379 --user root:pass user list

# Delete a user
etcdctl --endpoints=localhost:3379 --user root:pass user delete alice

# Change password
etcdctl --endpoints=localhost:3379 --user root:pass user passwd alice
```

### Managing roles

```bash
# Create a role
etcdctl --endpoints=localhost:3379 --user root:pass role add reader

# Grant read access to a key prefix
etcdctl --endpoints=localhost:3379 --user root:pass \
  role grant-permission reader read /data/ --prefix

# Grant write access to a specific key
etcdctl --endpoints=localhost:3379 --user root:pass \
  role grant-permission reader write /config/app

# Grant read+write access to a prefix
etcdctl --endpoints=localhost:3379 --user root:pass \
  role grant-permission writer readwrite /app/ --prefix

# Revoke a permission
etcdctl --endpoints=localhost:3379 --user root:pass \
  role revoke-permission reader /data/ --prefix

# List roles
etcdctl --endpoints=localhost:3379 --user root:pass role list

# Inspect a role's permissions
etcdctl --endpoints=localhost:3379 --user root:pass role get reader

# Delete a role
etcdctl --endpoints=localhost:3379 --user root:pass role delete reader
```

### Assigning roles to users

```bash
# Grant a role
etcdctl --endpoints=localhost:3379 --user root:pass \
  user grant-role alice reader

# Revoke a role
etcdctl --endpoints=localhost:3379 --user root:pass \
  user revoke-role alice reader

# List a user's roles
etcdctl --endpoints=localhost:3379 --user root:pass user get alice
```

### RBAC rule evaluation

A request is permitted when the authenticated user has at least one role whose permissions cover the requested key and operation type:

| Operation | Required permission |
|---|---|
| `Range` (Get / List) | `read` |
| `Put` | `write` |
| `DeleteRange` | `write` |
| `Txn` | `write` |
| `Watch` | `read` |

A permission entry covers a key when:
- **Exact key** (`--prefix` omitted): the key matches exactly.
- **Prefix range** (`--prefix`): the key starts with the permission's key prefix (computed as `rangeEnd = prefix[:-1] + chr(ord(prefix[-1])+1)`).
- **Open-ended range** (`rangeEnd = "\x00"`): all keys ≥ the permission key.

The `root` role always passes all checks regardless of the key.

### Auth namespace protection

Keys under the `\x00auth/` prefix are reserved for internal auth storage. Access to these keys via the KV service is blocked for all users, including `root`. Attempting to read or write them returns `PermissionDenied`.

### Rate limiting

To protect against brute-force attacks, Strata enforces a per-username rate limit on failed authentication attempts:

- **5 consecutive failures** within a **5-minute window** triggers a **15-minute lockout** for that username.
- Subsequent `Authenticate` calls during the lockout period return an error without checking the password.
- The lockout state is in-memory only and resets on node restart (intentional: a restart is already a privileged operation).
- All authentication outcomes are recorded in the `strata_auth_attempts_total` metric with a `result` label (`success`, `fail`, `locked`).

### Disabling auth

```bash
etcdctl --endpoints=localhost:3379 --user root:pass auth disable
```

Or restart the node without `--auth-enabled`. Auth state (users, roles) is preserved in Pebble — re-enabling auth later restores the same configuration.

### Full example: read-only service account

```bash
# 1. Create the role with read access to /config/
etcdctl --user root:pass role add config-reader
etcdctl --user root:pass role grant-permission config-reader read /config/ --prefix

# 2. Create the user and assign the role
etcdctl --user root:pass user add svc-account
etcdctl --user root:pass user grant-role svc-account config-reader

# 3. The service account can read /config/ but not write
etcdctl --user svc-account:pass get /config/timeout   # OK
etcdctl --user svc-account:pass put /config/timeout 60s  # PermissionDenied
etcdctl --user svc-account:pass get /secrets/key         # PermissionDenied
```

---

## Observability

```bash
strata run --metrics-addr 0.0.0.0:9090 ...
```

### Endpoints

| Path | Description |
|---|---|
| `GET /metrics` | Prometheus metrics |
| `GET /healthz` | 200 once the node has started |
| `GET /readyz` | 200 when the node is ready to serve reads |

### Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `strata_writes_total` | counter | `op` | Completed write operations |
| `strata_write_errors_total` | counter | `op` | Write operations that returned an error |
| `strata_write_duration_seconds` | histogram | `op` | Write latency (WAL + apply) |
| `strata_forwarded_writes_total` | counter | `op` | Writes forwarded from follower to leader |
| `strata_forward_duration_seconds` | histogram | `op` | Forwarded write round-trip latency |
| `strata_current_revision` | gauge | — | Latest applied revision |
| `strata_compact_revision` | gauge | — | Compaction watermark |
| `strata_role` | gauge | `role` | 1 for the active role (`leader`/`follower`/`single`) |
| `strata_wal_uploads_total` | counter | — | WAL segments successfully uploaded |
| `strata_wal_upload_errors_total` | counter | — | Failed WAL segment uploads |
| `strata_wal_upload_duration_seconds` | histogram | — | WAL segment upload latency |
| `strata_wal_gc_segments_total` | counter | — | WAL segments deleted from S3 after checkpointing |
| `strata_checkpoints_total` | counter | — | Checkpoints written to S3 |
| `strata_elections_total` | counter | `outcome` | Election attempts (`won`/`lost`) |
| `strata_follower_resyncs_total` | counter | `reason` | Full resync events triggered on followers (`behind_leader_start` / `ring_buffer_miss` / `stream_gap`) |
| `strata_follower_lag_revisions` | gauge | `follower_id` | Revisions the follower is behind the leader (0 = fully caught up); absent when no followers connected |
| `strata_auth_attempts_total` | counter | `result` | Authentication attempts (`success` / `fail` / `locked`) |

`op` label values: `put`, `create`, `update`, `delete`, `compact`.

---

## Performance

Numbers are from `go test -bench=. -benchtime=10s` on an Apple M4 Pro (12 cores, NVMe SSD). All tests use in-process loopback — no real network or S3.

### Single-node (no peers, no S3)

Write latency is dominated by a single WAL fsync (~4 ms on NVMe). Concurrent writers are automatically batched by the `commitLoop` into a single fsync per drain cycle (group commit).

| Operation | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| `Put` (serial) | ~231 writes/s | 4.1 ms | 6.3 ms | 8.0 ms |
| `Put` (192 concurrent writers) | ~15,800 writes/s | — | — | — |
| `Get` / `LinearizableGet` (leader) | ~2,260,000 reads/s | 0.44 µs | — | — |
| `List` (100 keys) | ~28,000 ops/s | 35.7 µs | — | — |
| Watch event delivery | — | 4.8 ms | 7.8 ms | 11.1 ms |

### 3-node cluster (localhost loopback)

Write latency = leader WAL fsync + quorum ACK round-trip (follower WAL fsync + network). On loopback, both nodes share the same SSD so each write costs roughly two sequential fsyncs.

| Operation | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| `Put` (serial) | ~70 writes/s | 11.1 ms | 17.0 ms | 21.0 ms |
| `Put` (192 concurrent writers) | ~520 writes/s | — | — | — |
| `LinearizableGet` (follower) | ~20,800 reads/s | 48 µs | — | — |

With group commit, the per-write overhead of the quorum ACK round-trip disappears almost entirely under load — high concurrency improves throughput by batching many writes into one ACK round.

### Impact of real-world latency

Write latency scales with inter-node RTT and S3 latency (single-node only):

| Scenario | Additional latency | Notes |
|---|---|---|
| Cluster, same-host loopback | +15 ms | loopback gRPC + follower fsync |
| Cluster, LAN (1 ms RTT) | +9 ms | ≈ follower fsync + 2× 0.5 ms network |
| Cluster, cross-AZ (5 ms RTT) | +18 ms | ≈ follower fsync + 2× 5 ms network |
| Cluster, cross-region (50 ms RTT) | +108 ms | high-latency links hurt serial throughput most |
| Single-node, S3 upload | +100–500 ms | sync upload per WAL segment — use cluster mode for low latency |

In cluster mode, **S3 uploads are async** (disaster-recovery only) and add zero latency to the write path. Single-node mode uploads each WAL segment to S3 synchronously; write latency is dominated by S3 round-trip, not local fsync. For low-latency single-node deployments without S3, latency is entirely local disk (~8 ms NVMe).

Read latency on a follower includes one `ForwardGetRevision` gRPC call to the leader to obtain the current revision, then a local Pebble lookup. On localhost this costs ~55 µs; on LAN expect ~1–2 ms; on cross-AZ ~10 ms.

---

## Durability and recovery

### What is durable

A write is durable when it has been:
- fsynced to the leader's WAL **and** ACKed by all connected followers (cluster mode) — the entry exists on at least two nodes' WALs before the caller sees success, **or**
- fsynced to the local WAL **and** the WAL segment has been uploaded to S3 (single-node mode).

In cluster mode S3 is disaster-recovery only (both nodes fail simultaneously). WAL uploads are fully async and do not affect write latency. In single-node mode without S3, durability depends entirely on local disk.

### Recovery procedure

On startup, Strata always performs:

1. Read `manifest/latest` from S3 → get the latest checkpoint key and revision.
2. If the local Pebble database is absent, restore the checkpoint from S3.
3. Open the local Pebble database.
4. Replay all local WAL segments (`.wal` files in `<data-dir>/wal/`) that are newer than the checkpoint.
5. Replay any WAL segments uploaded to S3 that are newer than the checkpoint and not already replayed locally.
6. Run leader election (cluster mode) or become single-node.

Steps 4–5 ensure that no committed write is lost even if the node is killed between WAL writes and checkpoint creation.

### S3 unavailability

**Cluster mode**

In cluster mode, S3 uploads are fully async — WAL segments and checkpoints are uploaded in the background without blocking writes. If S3 becomes unavailable:

- **Writes continue.** Durability is backed by the peer WAL + quorum ACK across nodes, not by S3.
- **WAL uploads queue.** Failed uploads are retried; a backlog of unsealed segments accumulates in `<data-dir>/wal/`.
- **Leader election is unaffected** as long as the existing lock record is still readable from S3. If the lock expires or cannot be read, election is blocked until S3 is reachable again.
- **No committed write is lost.** On restart, local WAL segments are replayed before any S3 reads (step 4 of the recovery procedure). Data that was fsynced to local disk is safe regardless of S3 state.

**Single-node mode**

In single-node mode, each WAL segment is uploaded to S3 synchronously before the write is acknowledged. If S3 becomes unavailable:

- **Writes fail** once the in-progress WAL segment fills and a rotation is attempted. The node fences itself to prevent unacknowledged data from accumulating silently.
- **Already-acknowledged writes are safe.** All segments uploaded before the outage are on S3; the current open segment is on local disk.
- **To recover:** repair S3 access and restart the node. On startup it replays all local WAL segments (including any partial segment left on disk), then resumes normal operation.

**After any S3 outage — what is safe**

On startup, Strata replays local WAL segments (step 4) before reading from S3, so no write that was fsynced to local disk is lost even if the segment was never uploaded. In cluster mode with multiple survivors, any write that completed quorum ACK across nodes is never lost even if all S3 state is gone.

---

### Network partitions (cluster mode)

Strata uses S3 as the split-brain arbiter. The leader continuously refreshes a `LastSeenNano` timestamp in the S3 leader lock every `FollowerRetryInterval` (2 s) while followers are disconnected. A follower only promotes itself if it cannot reach the leader **and** the lock is older than `LeaderLivenessTTL` (6 s).

**Follower partitioned from leader, leader can still reach S3:**

1. Follower loses the peer stream; leader detects the disconnect.
2. Leader immediately touches `LastSeenNano` in the lock, then continues refreshing every 2 s.
3. Follower exhausts `--follower-max-retries` reconnect attempts (~4 s at the default of 2 × 2 s).
4. Follower reads the S3 lock — `LastSeenNano` is ≤ 2 s old → backs off, **does not promote**.
5. Follower keeps retrying the peer connection; leader keeps writing.
6. When the partition heals the follower reconnects and resyncs. **No split-brain. No data loss.**

**Leader partitioned from S3 (and from followers):**

1. Leader can no longer touch `LastSeenNano`.
2. After `LeaderLivenessTTL` (6 s) the lock goes stale.
3. A follower that has been retrying attempts a conditional PUT (`If-Match: <etag>`) on the lock — only one candidate wins this atomic race.
4. New leader begins streaming WAL entries.
5. Former leader detects the superseded lock on its next fenced check and steps down.
6. Any write that completed quorum ACK before the partition exists on at least two nodes' WALs and is **never lost**.

**Writes during a follower partition:**

While followers are disconnected, the leader's `WaitForFollowers` returns immediately (0 connected followers → 0 ACKs required). Writes proceed but are acknowledged only by the leader node. WAL segments continue to be uploaded to S3 asynchronously. If the leader fails while the partition persists and before the segments reach S3, those post-partition writes may be lost. For the highest write durability during a known partition, avoid acknowledging client writes until the partition heals, or use a 3-node cluster so quorum (2 of 3) can still be reached with one node partitioned.

---

## Restore from checkpoint

`strata restore` lets you inspect available checkpoints and download one to a local data directory so that `strata run` can boot from it.

### Listing checkpoints

```bash
strata restore list \
  --s3-bucket my-bucket \
  --s3-prefix strata/
```

Output:

```
CHECKPOINT                                                                  REVISION    TERM
checkpoint/0000000001/00000000000000000050/manifest.json                          50       1
checkpoint/0000000001/00000000000000000100/manifest.json                         100       1  (latest)
```

### Restoring a checkpoint

```bash
# Restore the latest checkpoint (default).
strata restore checkpoint \
  --s3-bucket my-bucket \
  --s3-prefix strata/ \
  --data-dir /var/lib/strata-restored

# Restore a specific earlier revision.
strata restore checkpoint \
  --s3-bucket my-bucket \
  --s3-prefix strata/ \
  --checkpoint checkpoint/0000000001/00000000000000000050/manifest.json \
  --data-dir /var/lib/strata-restored
```

The command downloads all SST files and Pebble metadata to `<data-dir>/db/` and prints a summary:

```
Restored checkpoint
  key:       checkpoint/0000000001/00000000000000000050/manifest.json
  revision:  50
  term:      1
  data-dir:  /var/lib/strata-restored

Start the restored node:
  strata run --data-dir /var/lib/strata-restored [--s3-bucket <new-bucket>] --listen 0.0.0.0:3379
```

### Starting the restored node

After the download, run the node pointing at the prepared data directory:

```bash
# Inspect the restored state without connecting to S3 (stays at the restored revision).
strata run \
  --data-dir /var/lib/strata-restored \
  --listen   0.0.0.0:3379

# Or write to a separate S3 prefix so the restored node has its own durable history.
# The node opens the local Pebble database, then replays any WAL segments in
# <new-prefix> that are newer than the restored revision.
strata run \
  --data-dir  /var/lib/strata-restored \
  --s3-bucket my-bucket \
  --s3-prefix strata-restored/ \
  --listen    0.0.0.0:3379
```

> **Note:** If you point `--s3-bucket/prefix` at the **original** cluster's prefix, the node will replay all WAL segments written after the restored checkpoint and arrive at the current state — this is recovery, not a rollback. To stay at the past revision, omit `--s3-bucket` or use a different prefix.

### Point-in-time recovery workflow

```bash
# 1. Find the last good checkpoint.
strata restore list --s3-bucket my-bucket --s3-prefix strata/

# 2. Download it to a local directory.
strata restore checkpoint \
  --s3-bucket my-bucket --s3-prefix strata/ \
  --checkpoint checkpoint/0000000001/00000000000000000050/manifest.json \
  --data-dir /var/lib/strata-pitr

# 3. Start a verification node (no S3 → stays at rev 50).
strata run --data-dir /var/lib/strata-pitr --listen 0.0.0.0:3380

# 4. Validate. If correct, promote by starting with a new S3 prefix.
strata run \
  --data-dir  /var/lib/strata-pitr \
  --s3-bucket my-bucket \
  --s3-prefix strata-recovered/ \
  --listen    0.0.0.0:3379
```

For zero-copy forking (no SST downloads) use `strata branch fork` instead — see [Branching](#branching).

---

## Upgrades and compatibility

### WAL format versioning

Each WAL segment file begins with an 8-byte magic header `"STRATA\x01\n"`. The sixth byte (`\x01`) is the **WAL format version** (currently 1). Readers verify the full magic string on open; a format change bumps this byte, causing old readers to reject new segments with a clear error rather than silently misinterpreting them.

**Current: WAL format version 1.** Entry wire format: CRC32C-framed records with big-endian fixed-width fields (see `internal/wal/entry.go`).

### Checkpoint format versioning

Checkpoint manifests and index files are JSON. Both include a `format_version` field (integer, omitempty). The current version is **1**. Older nodes that do not know this field treat it as version 0 (the original format) — identical to version 1.

When a node reads a manifest or index with `format_version` higher than it knows, it logs a warning and continues. A future incompatible change will increment `format_version` and require the old nodes to be upgraded before they can read new checkpoints.

**Compatibility rule:** adding new JSON fields with `omitempty` is always backward-compatible. Only structural changes that alter how existing fields are interpreted require a version bump.

### Rolling upgrade

A rolling upgrade replaces nodes one at a time without cluster downtime.

```
cluster state: [old, old, old]

1. Add a new node (same S3 prefix, new binary):
   cluster: [old, old, old, new]
   — new node reads old checkpoints (format_version absent → treated as v1 ✓)
   — new node writes format_version=1 (old nodes ignore the field ✓)

2. Transfer leadership to the new node (close one old leader or wait for natural failover).
   cluster: [old, old, new(leader), new]

3. Remove one old node at a time. Repeat until all old nodes are gone.
   cluster: [new, new, new]
```

**Minimum viable rolling upgrade (3-node cluster):**

```bash
# Step 1 — Add node D (new binary, same S3 prefix).
strata run --node-id node-d ... &

# Step 2 — Gracefully shut down node A (old binary).
#           A sends GoodBye; remaining followers elect a new leader from B/C/D.
kill -TERM <pid-A>

# Step 3 — Repeat for B, then C.
kill -TERM <pid-B>
kill -TERM <pid-C>
```

After the last old node is stopped, all writes go through the new binary and all new checkpoints carry `format_version=1`.

### Downgrade path

A downgrade is safe as long as no checkpoint with `format_version > 1` has been written by the new binary.

```bash
# Replace new nodes with old binary, one at a time — same procedure as upgrade.
# Verify no format_version > 1 checkpoint exists first:
strata restore list --s3-bucket my-bucket --s3-prefix strata/
```

If a new format version was introduced (e.g., format_version=2), downgrade to the old binary requires restoring from the last format_version=1 checkpoint instead of the latest one.

---

## Branching

Branches let you fork a database at any checkpoint with zero S3 data copies. SST files are content-addressed and shared between the source and all branches — no data is duplicated.

### Requirements

- S3 versioning is **not** required.
- The source database must have at least one checkpoint.

### Creating a branch (CLI)

```bash
# 1. Register the branch against the source store.
#    Prints the checkpoint key — save it.
strata branch fork \
  --s3-bucket my-bucket \
  --s3-prefix strata/ \
  --branch-id my-branch

# Output: checkpoint/0000000001/00000000000000000100/manifest.json

# 2. Start the branch node, pointing it at the source.
strata run \
  --data-dir              /var/lib/strata-branch \
  --listen                0.0.0.0:3379 \
  --s3-bucket             my-bucket \
  --s3-prefix             strata-branch/ \
  --branch-source-bucket  my-bucket \
  --branch-source-prefix  strata/ \
  --branch-checkpoint     checkpoint/0000000001/00000000000000000100/manifest.json
```

On first boot the branch node downloads SSTs and Pebble metadata from the source prefix. On subsequent restarts `--branch-checkpoint` is ignored (the local data directory already exists).

### Creating a branch (Go library)

```go
import "github.com/strata-db/strata"
import "github.com/strata-db/strata/pkg/object"

sourceStore := object.NewS3Store(object.S3Config{Bucket: "my-bucket", Prefix: "strata/"})
branchStore := object.NewS3Store(object.S3Config{Bucket: "my-bucket", Prefix: "strata-branch/"})

// Register and get the checkpoint key.
cpKey, err := strata.Fork(ctx, sourceStore, "my-branch")
if err != nil {
    log.Fatal(err)
}

// Start the branch node.
node, err := strata.Open(strata.Config{
    DataDir:       "/var/lib/strata-branch",
    ObjectStore:   branchStore,
    AncestorStore: sourceStore,
    BranchPoint: &strata.BranchPoint{
        SourceStore:   sourceStore,
        CheckpointKey: cpKey,
    },
})
```

### Forking from a specific checkpoint

By default `Fork` uses the latest checkpoint. To fork from an earlier revision, call `checkpoint.RegisterBranch` directly with the specific key:

```bash
# CLI
strata branch fork \
  --s3-bucket my-bucket --s3-prefix strata/ \
  --branch-id my-branch \
  --checkpoint checkpoint/0000000001/00000000000000000050/manifest.json
```

```go
// Go — use the internal package directly for a specific key
import "github.com/strata-db/strata/internal/checkpoint"

cpKey := "checkpoint/0000000001/00000000000000000050/manifest.json"
if err := checkpoint.RegisterBranch(ctx, sourceStore, "my-branch", cpKey); err != nil {
    log.Fatal(err)
}

### Removing a branch

When the branch is no longer needed, unregister it so the source's GC can reclaim unused SSTs:

```bash
strata branch unfork \
  --s3-bucket my-bucket \
  --s3-prefix strata/ \
  --branch-id my-branch
```

```go
if err := strata.Unfork(ctx, sourceStore, "my-branch"); err != nil {
    log.Fatal(err)
}
```

### Use cases

**Point-in-time recovery** — fork from a checkpoint taken before a bad write, validate, then promote.

**Blue/green migrations** — run a schema migration against a branch with production data, test it, then cut over.

**DR drills** — spin up a replica in a different region from a fork, verify integrity, then shut it down.

**Parallel testing** — fork the same production snapshot for multiple independent test runs.

---

## Point-in-time restore (S3 versioning)

> **Note**: this mechanism requires S3 versioning to be enabled on the bucket. For most use cases, [Branching](#branching) is simpler and does not require versioning.

`RestorePoint` bootstraps a new node from a specific set of S3 object version IDs captured at a past moment. See [api.md — Point-in-time restore](api.md#point-in-time-restore-s3-versioning) for the Go API.

### Requirements

- S3 versioning must be enabled on the bucket **before** the first write.

### Capturing a restore point

```bash
# Find the current checkpoint key.
aws s3 cp s3://my-bucket/source-prefix/manifest/latest - | jq .

# List WAL segments and their version IDs.
aws s3api list-object-versions \
  --bucket my-bucket \
  --prefix source-prefix/wal/ \
  --query 'Versions[?IsLatest==`true`].[Key,VersionId]' \
  --output json
```
