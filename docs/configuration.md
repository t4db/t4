# Configuration Reference

## Library: `strata.Config`

| Field | Type | Default | Description |
|---|---|---|---|
| `DataDir` | `string` | — | **Required.** Directory for the Pebble database and local WAL segments. Created if absent. |
| `ObjectStore` | `object.Store` | `nil` | S3 or compatible store. `nil` = local-only, single-node mode. |
| `AncestorStore` | `object.Store` | `nil` | Source store for a branch node. SSTs referenced by `AncestorSSTFiles` are fetched from here instead of being re-uploaded. |
| `BranchPoint` | `*BranchPoint` | `nil` | If set, bootstraps the node from a fork of another database. Applied once on first boot; ignored thereafter. See [Branches](api.md#branches). |
| `RestorePoint` | `*RestorePoint` | `nil` | Bootstrap from a specific S3 version (requires S3 versioning). See [Point-in-time restore](api.md#point-in-time-restore-s3-versioning). |
| `SegmentMaxSize` | `int64` | 50 MB | WAL segment rotation threshold in bytes. |
| `SegmentMaxAge` | `time.Duration` | 10 s | WAL segment rotation age threshold. When `WALSyncUpload` is `false` this also controls how often async uploads run. |
| `WALSyncUpload` | `*bool` | `true` | Controls WAL segment upload behaviour. `true` (default): each sealed segment is uploaded to S3 synchronously before the write returns — safe when local storage is ephemeral. `false`: uploads happen asynchronously every `SegmentMaxAge`; write latency is lower but up to `SegmentMaxAge` of acknowledged writes can be lost on simultaneous node + S3 failure. Set to `false` when local storage is durable (e.g. a PVC) and low latency matters. |
| `CheckpointInterval` | `time.Duration` | 15 min | How often the leader writes a checkpoint to S3. Set to 0 to disable. |
| `CheckpointEntries` | `int64` | 0 | Also trigger a checkpoint after this many WAL entries (0 = disabled). |
| `NodeID` | `string` | hostname | Stable unique identifier for this node. Must not change across restarts. |
| `PeerListenAddr` | `string` | `""` | gRPC listen address for the WAL stream (e.g. `0.0.0.0:3380`). Empty = single-node mode. |
| `AdvertisePeerAddr` | `string` | `PeerListenAddr` | Address that followers use to reach this node. Set this when the listen address is not directly routable (container, NAT). |
| `LeaderWatchInterval` | `time.Duration` | 5 min | How often the leader re-reads the S3 lock to detect supersession by a new leader. |
| `FollowerMaxRetries` | `int` | 5 | Consecutive stream failures before a follower attempts election takeover. |
| `PeerBufferSize` | `int` | 10 000 | WAL entries buffered in memory for follower catch-up. |
| `PeerServerTLS` | `credentials.TransportCredentials` | `nil` | mTLS credentials for the peer gRPC server (leader side). |
| `PeerClientTLS` | `credentials.TransportCredentials` | `nil` | mTLS credentials for the peer gRPC client (follower side). |
| `MetricsAddr` | `string` | `""` | HTTP address for `/metrics`, `/healthz`, `/readyz`. Empty = disabled. |

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
    PutIfAbsent(ctx context.Context, key string, r io.Reader) error          // If-None-Match: *
    PutIfMatch(ctx context.Context, key string, r io.Reader, etag string) error // If-Match: <etag>
}
```

Both `S3Store` and `Mem` implement `ConditionalStore`. If your custom store does not, election falls back to an unconditional write + read-back (slightly less race-safe under concurrent startup), and liveness touches fall back to unconditional PUT (the Read→Touch split-brain protection is not available).

---

## CLI: `strata run`

Start a node. All flags below are sub-flags of the `run` subcommand.

```bash
strata run [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `/var/lib/strata` | Pebble + WAL storage directory |
| `--listen` | `0.0.0.0:3379` | etcd v3 gRPC listen address |
| `--s3-bucket` | — | S3 bucket name |
| `--s3-prefix` | — | Key prefix inside the bucket (no trailing slash needed) |
| `--s3-endpoint` | — | Custom S3 endpoint URL (MinIO, Ceph, etc.) |
| `--segment-max-size-mb` | `50` | WAL segment rotation size threshold (MiB) |
| `--segment-max-age-sec` | `10` | WAL segment rotation age (seconds) |
| `--wal-sync-upload` | _(default true)_ | Upload WAL segments synchronously before acknowledging writes (`true`/`false`). Set to `false` when local storage is durable (e.g. a PVC) for lower write latency. |
| `--checkpoint-interval-min` | `15` | Checkpoint write interval (minutes) |
| `--checkpoint-entries` | `0` | Also checkpoint after N WAL entries (0 = disabled) |
| `--log-level` | `info` | Log level: `trace` `debug` `info` `warn` `error` |
| `--node-id` | hostname | Stable unique node identifier |
| `--peer-listen` | — | Peer WAL stream listen address; enables multi-node mode |
| `--advertise-peer` | `--peer-listen` | Advertised peer address (use when behind NAT) |
| `--leader-watch-interval-sec` | `300` | Leader lock re-read interval (seconds) |
| `--follower-max-retries` | `5` | Stream failures before a follower attempts takeover |
| `--peer-tls-ca` | — | CA certificate for peer mTLS (PEM file) |
| `--peer-tls-cert` | — | Node certificate for peer mTLS (PEM file) |
| `--peer-tls-key` | — | Node private key for peer mTLS (PEM file) |
| `--metrics-addr` | — | HTTP address for metrics and health endpoints |
| `--branch-source-bucket` | — | S3 bucket of the source database (branch nodes only) |
| `--branch-source-prefix` | — | S3 prefix of the source database (branch nodes only) |
| `--branch-source-endpoint` | — | S3 endpoint of the source database (branch nodes only) |
| `--branch-checkpoint` | — | Checkpoint key to fork from (branch nodes only; omit to use latest) |

---

## CLI: `strata branch`

Manage database branches.

### `strata branch fork`

```bash
strata branch fork \
  --s3-bucket <bucket> --s3-prefix <source-prefix> \
  --branch-id <id> \
  [--checkpoint <checkpoint-key>]
```

Registers a new branch against the source store. Prints the checkpoint key to stdout. Pass that key as `--branch-checkpoint` when starting the branch node.

| Flag | Description |
|---|---|
| `--s3-bucket` | S3 bucket of the source database |
| `--s3-prefix` | S3 prefix of the source database |
| `--s3-endpoint` | Custom S3 endpoint (optional) |
| `--branch-id` | Unique identifier for the branch |
| `--checkpoint` | Fork from a specific checkpoint key instead of the latest |

### `strata branch unfork`

```bash
strata branch unfork \
  --s3-bucket <bucket> --s3-prefix <source-prefix> \
  --branch-id <id>
```

Removes the branch registry entry from the source store. The next GC run on the source may reclaim SSTs that are no longer needed.

| Flag | Description |
|---|---|
| `--s3-bucket` | S3 bucket of the source database |
| `--s3-prefix` | S3 prefix of the source database |
| `--s3-endpoint` | Custom S3 endpoint (optional) |
| `--branch-id` | Branch identifier to remove |
