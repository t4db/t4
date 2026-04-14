---
title: Configuration
description: All t4.Config fields and CLI flags for the T4 standalone binary.
---

# Configuration Reference

## Library: `t4.Config`

| Field                 | Type                               | Default          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|-----------------------|------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DataDir`             | `string`                           | —                | **Required.** Directory for the Pebble database and local WAL segments. Created if absent.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `ObjectStore`         | `object.Store`                     | `nil`            | S3 or compatible store. `nil` = local-only, single-node mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `AncestorStore`       | `object.Store`                     | `nil`            | Source store for a branch node. SSTs referenced by `AncestorSSTFiles` are fetched from here instead of being re-uploaded.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `BranchPoint`         | `*BranchPoint`                     | `nil`            | If set, bootstraps the node from a fork of another database. Applied once on first boot; ignored thereafter. See [Branches](api.md#branches).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `RestorePoint`        | `*RestorePoint`                    | `nil`            | Bootstrap from a specific S3 version (requires S3 versioning). See [Point-in-time restore](api.md#point-in-time-restore-s3-versioning).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `SegmentMaxSize`      | `int64`                            | 50 MB            | WAL segment rotation threshold in bytes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `SegmentMaxAge`       | `time.Duration`                    | 10 s             | WAL segment rotation age threshold. When `WALSyncUpload` is `false` this also controls how often async uploads run.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `WALSyncUpload`       | `*bool`                            | `true`           | Controls WAL segment upload behaviour. `true` (default): each sealed segment is uploaded to S3 synchronously before the write returns — safe when local storage is ephemeral. `false`: uploads happen asynchronously every `SegmentMaxAge`; write latency is lower but up to `SegmentMaxAge` of acknowledged writes can be lost on simultaneous node + S3 failure. Set to `false` when local storage is durable (e.g. a PVC) and low latency matters. **Note: in cluster mode this flag only affects the initial follower WAL. The leader always opens its WAL with async uploads (`becomeLeader` hardcodes this), since quorum commit already provides write durability.** |
| `CheckpointInterval`  | `time.Duration`                    | 15 min           | How often the leader writes a checkpoint to S3. Set to 0 to disable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `CheckpointEntries`   | `int64`                            | 0                | Also trigger a checkpoint after this many WAL entries (0 = disabled).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `NodeID`              | `string`                           | hostname         | Stable unique identifier for this node. Must not change across restarts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `PeerListenAddr`      | `string`                           | `""`             | gRPC listen address for the WAL stream (e.g. `0.0.0.0:3380`). Empty = single-node mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `AdvertisePeerAddr`   | `string`                           | `PeerListenAddr` | Address that followers use to reach this node. Set this when the listen address is not directly routable (container, NAT).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `LeaderWatchInterval` | `time.Duration`                    | 5 min            | How often the leader re-reads the S3 lock to detect supersession by a new leader.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `FollowerMaxRetries`  | `int`                              | 5                | Consecutive stream failures before a follower attempts election takeover.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `FollowerWaitMode`    | `FollowerWaitMode`                 | `quorum`         | How many follower ACKs the leader waits for before acknowledging a write: `none`, `quorum`, or `all`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `PeerBufferSize`      | `int`                              | 10 000           | WAL entries buffered in memory for follower catch-up.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `PeerServerTLS`       | `credentials.TransportCredentials` | `nil`            | mTLS credentials for the peer gRPC server (leader side).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `PeerClientTLS`       | `credentials.TransportCredentials` | `nil`            | mTLS credentials for the peer gRPC client (follower side).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `MetricsAddr`         | `string`                           | `""`             | HTTP address for `/metrics`, `/healthz`, `/readyz`. Empty = disabled.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `ReadConsistency`     | `ReadConsistency`                  | `"linearizable"` | Consistency guarantee for reads served by the etcd adapter. `"linearizable"` (default): each read syncs to the leader's current revision before returning, ensuring no stale reads. `"serializable"`: reads are served from local Pebble state without a round-trip to the leader; lower latency but may return slightly stale data on followers.                                                                                                                                                                                                                                                                                                                           |

### Using a custom S3-compatible store

`object.Store` is a four-method interface (`Put`, `Get`, `Delete`, `List`). Implement it to use any storage backend.

```go
type Store interface {
    Put(ctx context.Context, key string, r io.Reader) error
    Get(ctx context.Context, key string) (io.ReadCloser, error)
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) ([]string, error)
}
```

`pkg/object` provides `NewS3Store` (AWS SDK v2) and `NewMem` (in-memory, for tests).

Optionally implement `ConditionalStore` for atomic election writes:

```go
type ConditionalStore interface {
    Store
    GetETag(ctx context.Context, key string) (*GetWithETag, error)
    PutIfAbsent(ctx context.Context, key string, r io.Reader) error // If-None-Match: *
    PutIfMatch(ctx context.Context, key string, r io.Reader, etag string) error // If-Match: <etag>
}
```

Both `S3Store` and `Mem` implement `ConditionalStore`. If your custom store does not, election falls back to an
unconditional write + read-back (slightly less race-safe under concurrent startup), and liveness touches fall back to
unconditional PUT (the Read→Touch split-brain protection is not available).

---

## CLI: `t4 run`

Start a node. All flags below are sub-flags of the `run` subcommand.

```bash
t4 run [flags]
```

### Environment variables

Most `t4` CLI flags can also be set via `T4_*` environment variables. Command-line flags take precedence over
environment variables. Environment variables are only applied when the corresponding flag was not set explicitly, and
empty environment variable values are ignored.

Required flags such as `--s3-bucket`, `--branch-id`, and `--data-dir` can be satisfied by their documented environment
variables.

For S3 credentials, `t4` fails closed unless you configure credentials explicitly. Use
`T4_S3_ACCESS_KEY_ID` + `T4_S3_SECRET_ACCESS_KEY` for static credentials, or set `--s3-profile` / `T4_S3_PROFILE`
to opt into the AWS shared config chain. If you want the shared `default` profile, set `T4_S3_PROFILE=default`
explicitly.

Example:

```bash
export T4_DATA_DIR=/var/lib/t4
export T4_S3_BUCKET=my-bucket
export T4_S3_PREFIX=prod/cluster-a
export T4_NODE_ID=node-a

t4 run --listen 0.0.0.0:3379
```

#### `t4 run`

| Environment variable           | Equivalent flag               |
|--------------------------------|-------------------------------|
| `T4_DATA_DIR`                  | `--data-dir`                  |
| `T4_LISTEN`                    | `--listen`                    |
| `T4_S3_BUCKET`                 | `--s3-bucket`                 |
| `T4_S3_PREFIX`                 | `--s3-prefix`                 |
| `T4_S3_ENDPOINT`               | `--s3-endpoint`               |
| `T4_S3_REGION`                 | `--s3-region`                 |
| `T4_S3_PROFILE`                | `--s3-profile`                |
| `T4_S3_ACCESS_KEY_ID`          | `--s3-access-key-id`          |
| `T4_S3_SECRET_ACCESS_KEY`      | `--s3-secret-access-key`      |
| `T4_SEGMENT_MAX_SIZE_MB`       | `--segment-max-size-mb`       |
| `T4_SEGMENT_MAX_AGE_SEC`       | `--segment-max-age-sec`       |
| `T4_WAL_SYNC_UPLOAD`           | `--wal-sync-upload`           |
| `T4_CHECKPOINT_INTERVAL_MIN`   | `--checkpoint-interval-min`   |
| `T4_CHECKPOINT_ENTRIES`        | `--checkpoint-entries`        |
| `T4_READ_CONSISTENCY`          | `--read-consistency`          |
| `T4_LOG_LEVEL`                 | `--log-level`                 |
| `T4_NODE_ID`                   | `--node-id`                   |
| `T4_PEER_LISTEN`               | `--peer-listen`               |
| `T4_ADVERTISE_PEER`            | `--advertise-peer`            |
| `T4_LEADER_WATCH_INTERVAL_SEC` | `--leader-watch-interval-sec` |
| `T4_FOLLOWER_MAX_RETRIES`      | `--follower-max-retries`      |
| `T4_FOLLOWER_WAIT_MODE`        | `--follower-wait-mode`        |
| `T4_PEER_TLS_CA`               | `--peer-tls-ca`               |
| `T4_PEER_TLS_CERT`             | `--peer-tls-cert`             |
| `T4_PEER_TLS_KEY`              | `--peer-tls-key`              |
| `T4_CLIENT_TLS_CERT`           | `--client-tls-cert`           |
| `T4_CLIENT_TLS_KEY`            | `--client-tls-key`            |
| `T4_CLIENT_TLS_CA`             | `--client-tls-ca`             |
| `T4_AUTH_ENABLED`              | `--auth-enabled`              |
| `T4_TOKEN_TTL`                 | `--token-ttl`                 |
| `T4_METRICS_ADDR`              | `--metrics-addr`              |
| `T4_BRANCH_PREFIX`             | `--branch-prefix`             |
| `T4_BRANCH_CHECKPOINT`         | `--branch-checkpoint`         |

| Flag                          | Default          | Description                                                                                                                                                                                                                                                                        |
|-------------------------------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--data-dir`                  | `/var/lib/t4`    | Pebble + WAL storage directory                                                                                                                                                                                                                                                     |
| `--listen`                    | `0.0.0.0:3379`   | etcd v3 gRPC listen address                                                                                                                                                                                                                                                        |
| `--s3-bucket`                 | —                | S3 bucket name                                                                                                                                                                                                                                                                     |
| `--s3-prefix`                 | —                | Key prefix inside the bucket (no trailing slash needed)                                                                                                                                                                                                                            |
| `--s3-endpoint`               | —                | Custom S3 endpoint URL (MinIO, Ceph, etc.)                                                                                                                                                                                                                                         |
| `--s3-region`                 | —                | S3 region used for the configured S3 client                                                                                                                                                                                                                                        |
| `--s3-profile`                | —                | Named AWS shared config profile to use. `t4` only enables the AWS shared config chain when this is set explicitly; use `default` to opt in to the default profile.                                                                                                                 |
| `--s3-access-key-id`          | —                | t4 S3 access key ID; when set with `--s3-secret-access-key`, uses static credentials                                                                                                                                                                                               |
| `--s3-secret-access-key`      | —                | S3 secret access key (required when `--s3-access-key-id` is set)                                                                                                                                                                                                                   |
| `--segment-max-size-mb`       | `50`             | WAL segment rotation size threshold (MiB)                                                                                                                                                                                                                                          |
| `--segment-max-age-sec`       | `10`             | WAL segment rotation age (seconds)                                                                                                                                                                                                                                                 |
| `--wal-sync-upload`           | _(default true)_ | Upload WAL segments synchronously before acknowledging writes (`true`/`false`). Applies to single-node mode. In cluster mode the leader always uses async uploads (hardcoded in `becomeLeader`); set to `false` when local storage is durable for lower single-node write latency. |
| `--checkpoint-interval-min`   | `15`             | Checkpoint write interval (minutes)                                                                                                                                                                                                                                                |
| `--checkpoint-entries`        | `0`              | Also checkpoint after N WAL entries (0 = disabled)                                                                                                                                                                                                                                 |
| `--log-level`                 | `info`           | Log level: `trace` `debug` `info` `warn` `error`                                                                                                                                                                                                                                   |
| `--node-id`                   | hostname         | Stable unique node identifier                                                                                                                                                                                                                                                      |
| `--peer-listen`               | —                | Peer WAL stream listen address; enables multi-node mode                                                                                                                                                                                                                            |
| `--advertise-peer`            | `--peer-listen`  | Advertised peer address (use when behind NAT)                                                                                                                                                                                                                                      |
| `--leader-watch-interval-sec` | `300`            | Leader lock re-read interval (seconds)                                                                                                                                                                                                                                             |
| `--follower-max-retries`      | `5`              | Stream failures before a follower attempts takeover                                                                                                                                                                                                                                |
| `--follower-wait-mode`        | `quorum`         | Follower ACK wait policy before leader commit: `none`, `quorum`, or `all`                                                                                                                                                                                                          |
| `--peer-tls-ca`               | —                | CA certificate for peer mTLS (PEM file)                                                                                                                                                                                                                                            |
| `--peer-tls-cert`             | —                | Node certificate for peer mTLS (PEM file)                                                                                                                                                                                                                                          |
| `--peer-tls-key`              | —                | Node private key for peer mTLS (PEM file)                                                                                                                                                                                                                                          |
| `--client-tls-cert`           | —                | Server certificate for client-facing TLS on the etcd port (PEM file)                                                                                                                                                                                                               |
| `--client-tls-key`            | —                | Server private key for client-facing TLS (PEM file)                                                                                                                                                                                                                                |
| `--client-tls-ca`             | —                | CA certificate for client mTLS; omit for server-only TLS (PEM file)                                                                                                                                                                                                                |
| `--auth-enabled`              | `false`          | Enable etcd-compatible authentication and RBAC                                                                                                                                                                                                                                     |
| `--token-ttl`                 | `300`            | Bearer token lifetime in seconds                                                                                                                                                                                                                                                   |
| `--metrics-addr`              | —                | HTTP address for metrics and health endpoints                                                                                                                                                                                                                                      |
| `--branch-prefix`             | —                | S3 prefix of the source database (branch nodes only; uses `--s3-bucket`)                                                                                                                                                                                                           |
| `--branch-checkpoint`         | —                | Checkpoint key to fork from (branch nodes only; omit to use latest)                                                                                                                                                                                                                |

---

## CLI: `t4 branch`

Manage database branches.

### `t4 branch fork`

```bash
t4 branch fork \
  --s3-bucket <bucket> --s3-prefix <source-prefix> \
  --branch-id <id> \
  [--checkpoint <checkpoint-key>]
```

Registers a new branch against the source store. Prints the checkpoint key to stdout. Pass that key as
`--branch-checkpoint` when starting the branch node.

| Flag                     | Description                                               |
|--------------------------|-----------------------------------------------------------|
| `--s3-bucket`            | S3 bucket of the source database                          |
| `--s3-prefix`            | S3 prefix of the source database                          |
| `--s3-endpoint`          | Custom S3 endpoint (optional)                             |
| `--s3-region`            | S3 region (optional)                                      |
| `--s3-profile`           | S3 shared config profile (optional)                       |
| `--s3-access-key-id`     | S3 access key ID (optional)                               |
| `--s3-secret-access-key` | S3 secret access key (optional)                           |
| `--branch-id`            | Unique identifier for the branch                          |
| `--checkpoint`           | Fork from a specific checkpoint key instead of the latest |

Environment variables: `T4_S3_BUCKET`, `T4_S3_PREFIX`, `T4_S3_ENDPOINT`, `T4_S3_REGION`, `T4_S3_PROFILE`,
`T4_S3_ACCESS_KEY_ID`, `T4_S3_SECRET_ACCESS_KEY`, `T4_BRANCH_ID`

### `t4 branch unfork`

```bash
t4 branch unfork \
  --s3-bucket <bucket> --s3-prefix <source-prefix> \
  --branch-id <id>
```

Removes the branch registry entry from the source store. The next GC run on the source may reclaim SSTs that are no
longer needed.

| Flag                     | Description                          |
|--------------------------|--------------------------------------|
| `--s3-bucket`            | S3 bucket of the source database     |
| `--s3-prefix`            | S3 prefix of the source database     |
| `--s3-endpoint`          | Custom S3 endpoint (optional)        |
| `--s3-region`            | AWS region (optional)                |
| `--s3-profile`           | AWS shared config profile (optional) |
| `--s3-access-key-id`     | AWS access key ID (optional)         |
| `--s3-secret-access-key` | AWS secret access key (optional)     |
| `--branch-id`            | Branch identifier to remove          |

Environment variables: `T4_S3_BUCKET`, `T4_S3_PREFIX`, `T4_S3_ENDPOINT`, `T4_S3_REGION`, `T4_S3_PROFILE`,
`T4_S3_ACCESS_KEY_ID`, `T4_S3_SECRET_ACCESS_KEY`, `T4_BRANCH_ID`

---

## CLI: `t4 inspect`

Inspect a local `data-dir` in read-only mode without starting a server.

### `t4 inspect meta`

```bash
t4 inspect meta --data-dir <dir>
```

Shows current revision, compact revision, and total live key count for the local Pebble database.

Environment variable: `T4_DATA_DIR`

### `t4 inspect get`

```bash
t4 inspect get --data-dir <dir> <key>
```

Shows the current value and metadata for one key.

Environment variable: `T4_DATA_DIR`

### `t4 inspect list`

```bash
t4 inspect list --data-dir <dir> [--prefix <prefix>] [--limit <n>]
```

Lists live keys and their metadata, optionally filtered by prefix.

Environment variable: `T4_DATA_DIR`

### `t4 inspect count`

```bash
t4 inspect count --data-dir <dir> [--prefix <prefix>]
```

Counts live keys, optionally filtered by prefix.

Environment variable: `T4_DATA_DIR`

### `t4 inspect history`

```bash
t4 inspect history --data-dir <dir> [--limit <n>] <key>
```

Shows the change history for one key in revision order.

Environment variable: `T4_DATA_DIR`

### `t4 inspect diff`

```bash
t4 inspect diff --data-dir <dir> --from-rev <rev> [--to-rev <rev>] [--prefix <prefix>]
```

Summarizes keys that changed in a revision range, including before/after values.

Environment variable: `T4_DATA_DIR`
