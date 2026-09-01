# Configuration Reference

## Library: `t4.Config`

| Field                 | Type                               | Default                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|-----------------------|------------------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DataDir`             | `string`                           | —                              | **Required.** Directory for the Pebble database and local WAL segments. Created if absent.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `ObjectStore`         | `object.Store`                     | `nil`                          | S3 or compatible store. `nil` = local-only, single-node mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `AncestorStore`       | `object.Store`                     | `nil`                          | Source store for a branch node. SSTs referenced by `AncestorSSTFiles` are fetched from here instead of being re-uploaded.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `BranchPoint`         | `*BranchPoint`                     | `nil`                          | If set, bootstraps the node from a fork of another database. Applied once on first boot; ignored thereafter. See [Branches](api.md#branches).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `RestorePoint`        | `*RestorePoint`                    | `nil`                          | Bootstrap from a specific S3 version (requires S3 versioning). See [Point-in-time restore](api.md#point-in-time-restore-s3-versioning).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `SegmentMaxSize`      | `int64`                            | 50 MB                          | WAL segment rotation threshold in bytes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `SegmentMaxAge`       | `time.Duration`                    | 10 s                           | WAL segment rotation age threshold. When `WALSyncUpload` is `false` this also controls how often async uploads run.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `WALSyncUpload`       | `*bool`                            | `true`                         | Controls WAL segment upload behaviour. `true` (default): each sealed segment is uploaded to S3 synchronously before the write returns — safe when local storage is ephemeral. `false`: uploads happen asynchronously every `SegmentMaxAge`; write latency is lower but up to `SegmentMaxAge` of acknowledged writes can be lost on simultaneous node + S3 failure. Set to `false` when local storage is durable (e.g. a PVC) and low latency matters. **Note: in cluster mode this flag only affects the initial follower WAL. The leader always opens its WAL with async uploads (`becomeLeader` hardcodes this), since quorum commit already provides write durability.** |
| `CheckpointInterval`  | `time.Duration`                    | 15 min                         | How often the leader writes a checkpoint to S3. Set to 0 to disable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `CheckpointEntries`   | `int64`                            | 0                              | Also trigger a checkpoint after this many WAL entries (0 = disabled).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `AutoCompactMode`     | `AutoCompactMode`                  | inferred/off                   | Automatic history compaction strategy: `off`, `time`, or `revision`. Empty infers `time` when `AutoCompactRetention > 0`, `revision` when `AutoCompactRevisionRetention > 0`, otherwise `off`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `AutoCompactRetention` | `time.Duration`                   | 0                              | Time strategy: retain approximately this much revision history and compact older changes while preserving current key values.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `AutoCompactRevisionRetention` | `int64`                  | 0                              | Revision strategy: retain approximately this many recent revisions and compact older changes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `AutoCompactInterval` | `time.Duration`                    | sample interval / 1 min        | How often the leader checks whether the autocompaction watermark can advance.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `AutoCompactSampleInterval` | `time.Duration`             | clamp(retention/7, 1 min, 24 h) | Time strategy: how often T4 records revision/time samples used to map a retention cutoff to a revision. Coarser values retain up to roughly this much extra history.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `NodeID`              | `string`                           | hostname                       | Stable unique identifier for this node. Must not change across restarts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `PeerListenAddr`      | `string`                           | `""`                           | gRPC listen address for the WAL stream (e.g. `0.0.0.0:3380`). Empty = single-node mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `AdvertisePeerAddr`   | `string`                           | `PeerListenAddr`               | Address that followers use to reach this node. Set this when the listen address is not directly routable (container, NAT).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `LeaderWatchInterval` | `time.Duration`                    | 5 min                          | How often the leader re-reads the S3 lock to detect supersession by a new leader.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `FollowerMaxRetries`  | `int`                              | 5                              | Consecutive stream failures before a follower attempts election takeover.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `FollowerWaitMode`    | `FollowerWaitMode`                 | `quorum`                       | How many follower ACKs the leader waits for before acknowledging a write: `none`, `quorum`, or `all`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `PeerBufferSize`      | `int`                              | 10 000                         | WAL entries buffered in memory for follower catch-up.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `PeerServerTLS`       | `credentials.TransportCredentials` | `nil`                          | mTLS credentials for the peer gRPC server (leader side).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `PeerClientTLS`       | `credentials.TransportCredentials` | `nil`                          | mTLS credentials for the peer gRPC client (follower side).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `MetricsAddr`         | `string`                           | `""`                           | HTTP address for `/metrics`, `/healthz`, `/readyz`. Empty = disabled.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `ReadConsistency`     | `ReadConsistency`                  | `"linearizable"`               | Consistency guarantee for reads served by the etcd adapter. `"linearizable"` (default): each read syncs to the leader's current revision before returning, ensuring no stale reads. `"serializable"`: reads are served from local Pebble state without a round-trip to the leader; lower latency but may return slightly stale data on followers.                                                                                                                                                                                                                                                                                                                           |
| `PebbleOptions`       | `[]func(*pebble.Options)`          | `nil`                          | Expert hook for appending Pebble options before opening the local state machine. Production deployments normally leave this empty.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `WAL`                 | `WALWriter`                        | `nil`                          | Expert hook for replacing the filesystem WAL. Intended for constrained runtimes and tests. Production deployments normally leave this nil.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `Logger`              | `Logger`                           | logrus standard logger         | Receives all T4 log output. Set this in embedded mode to control destination, level, and format. Use `t4.NoopLogger` to silence T4 logs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `MetricsRegisterer`   | `prometheus.Registerer`            | `prometheus.DefaultRegisterer` | Prometheus registerer used for T4 metrics. Pass a custom registry to isolate T4 metrics when embedding.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `TracerProvider`      | `trace.TracerProvider`             | `nil`                          | OpenTelemetry provider for write-path and linearizable-read spans. `nil` falls back to `otel.GetTracerProvider()`, a no-op unless the application configured a global provider. Pass `noop.NewTracerProvider()` to opt out explicitly. Spans never carry key names — see [Distributed tracing](operations.md#distributed-tracing). |
| `ObjectStoreEncryption` | `*ObjectStoreEncryptionConfig`   | `nil`                          | Enables AES-256-GCM encryption for object-store data at rest. Local Pebble files and local WAL files remain plaintext.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

### Using a custom S3-compatible store

`object.Store` is a five-method interface (`Put`, `Get`, `Delete`, `DeleteMany`, `List`). Implement it to use any
storage backend.

```go
type Store interface {
Put(ctx context.Context, key string, r io.Reader) error
Get(ctx context.Context, key string) (io.ReadCloser, error)
Delete(ctx context.Context, key string) error
DeleteMany(ctx context.Context, keys []string) error
List(ctx context.Context, prefix string) ([]string, error)
}
```

`List` must return keys in lexicographic order. `Delete` and `DeleteMany` must not fail when keys are already absent.

Advanced embedders can also replace the WAL by implementing `WALWriter`:

```go
type WALWriter interface {
Open(dir string, term uint64, startRev int64) error
ReplayLocal(db wal.RecoveryStore, afterRev int64) error
Append(e *wal.Entry) error
AppendBatch(ctx context.Context, entries []*wal.Entry) error
SealAndFlush(nextSeq int64) error
Close() error
}
```

Custom WAL implementations must preserve the same durability ordering as the default filesystem WAL.

`pkg/object` provides `NewS3Store` (AWS SDK v2) and `NewMem` (in-memory, for tests).

### Object-store encryption

Set `Config.ObjectStoreEncryption` to encrypt every object-store body written by T4:

```go
keyProvider, err := object.NewStaticKeyProvider(keyBytes) // exactly 32 bytes
node, err := t4.Open(t4.Config{
	DataDir:     "/var/lib/t4",
	ObjectStore: s3Store,
	ObjectStoreEncryption: &t4.ObjectStoreEncryptionConfig{
		KeyProvider: keyProvider,
	},
})
```

The same key must be supplied to every node that shares an S3 prefix. The same key is also required for `RestorePoint`,
branch source stores, and maintenance commands that read encrypted objects.

The standalone CLI supports two key sources:

- `--object-store-encryption-key-file`: file containing a 32-byte key as raw bytes, 64 hex characters, or base64.
- `--object-store-encryption-key-env`: name of an environment variable whose value is a 32-byte key as raw bytes, 64 hex characters, or base64.

The flags are available on `t4 run`, `t4 restore`, `t4 branch`, `t4 gc`, and `t4 status`. Environment variables
`T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` and `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` can set those flag values.

Example:

```bash
openssl rand -base64 32 > /etc/t4/object-key.b64

t4 run \
  --data-dir /var/lib/t4 \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --object-store-encryption-key-file /etc/t4/object-key.b64
```

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

#### CLI environment variables

Generated from the CLI flag definitions in Go. Run `go run ./hack/docgen` after changing CLI flags or env
metadata.

<!-- BEGIN GENERATED: cli-env-vars -->
| Command | Environment variable | Equivalent flag |
|---------|----------------------|-----------------|
| `t4 branch fork` | `T4_BRANCH_ID` | `--branch-id` |
| `t4 branch fork` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 branch fork` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 branch fork` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 branch fork` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 branch fork` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 branch fork` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 branch fork` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 branch fork` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 branch fork` | `T4_S3_REGION` | `--s3-region` |
| `t4 branch fork` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 branch fork` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
| `t4 branch unfork` | `T4_BRANCH_ID` | `--branch-id` |
| `t4 branch unfork` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 branch unfork` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 branch unfork` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 branch unfork` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 branch unfork` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 branch unfork` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 branch unfork` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 branch unfork` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 branch unfork` | `T4_S3_REGION` | `--s3-region` |
| `t4 branch unfork` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 branch unfork` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
| `t4 gc` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 gc` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 gc` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 gc` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 gc` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 gc` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 gc` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 gc` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 gc` | `T4_S3_REGION` | `--s3-region` |
| `t4 gc` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 gc` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
| `t4 inspect count` | `T4_DATA_DIR` | `--data-dir` |
| `t4 inspect diff` | `T4_DATA_DIR` | `--data-dir` |
| `t4 inspect get` | `T4_DATA_DIR` | `--data-dir` |
| `t4 inspect history` | `T4_DATA_DIR` | `--data-dir` |
| `t4 inspect list` | `T4_DATA_DIR` | `--data-dir` |
| `t4 inspect meta` | `T4_DATA_DIR` | `--data-dir` |
| `t4 restore checkpoint` | `T4_DATA_DIR` | `--data-dir` |
| `t4 restore checkpoint` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 restore checkpoint` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 restore checkpoint` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 restore checkpoint` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 restore checkpoint` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 restore checkpoint` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 restore checkpoint` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 restore checkpoint` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 restore checkpoint` | `T4_S3_REGION` | `--s3-region` |
| `t4 restore checkpoint` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 restore checkpoint` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
| `t4 restore list` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 restore list` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 restore list` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 restore list` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 restore list` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 restore list` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 restore list` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 restore list` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 restore list` | `T4_S3_REGION` | `--s3-region` |
| `t4 restore list` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 restore list` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
| `t4 run` | `T4_ADVERTISE_PEER` | `--advertise-peer` |
| `t4 run` | `T4_AUTH_ENABLED` | `--auth-enabled` |
| `t4 run` | `T4_AUTO_COMPACT_INTERVAL` | `--auto-compact-interval` |
| `t4 run` | `T4_AUTO_COMPACT_MODE` | `--auto-compact-mode` |
| `t4 run` | `T4_AUTO_COMPACT_RETAIN_REVISIONS` | `--auto-compact-retain-revisions` |
| `t4 run` | `T4_AUTO_COMPACT_RETENTION` | `--auto-compact-retention` |
| `t4 run` | `T4_AUTO_COMPACT_SAMPLE_INTERVAL` | `--auto-compact-sample-interval` |
| `t4 run` | `T4_BRANCH_CHECKPOINT` | `--branch-checkpoint` |
| `t4 run` | `T4_BRANCH_PREFIX` | `--branch-prefix` |
| `t4 run` | `T4_CHECKPOINT_ENTRIES` | `--checkpoint-entries` |
| `t4 run` | `T4_CHECKPOINT_INTERVAL_MIN` | `--checkpoint-interval-min` |
| `t4 run` | `T4_CLIENT_TLS_CA` | `--client-tls-ca` |
| `t4 run` | `T4_CLIENT_TLS_CERT` | `--client-tls-cert` |
| `t4 run` | `T4_CLIENT_TLS_KEY` | `--client-tls-key` |
| `t4 run` | `T4_DATA_DIR` | `--data-dir` |
| `t4 run` | `T4_FOLLOWER_MAX_RETRIES` | `--follower-max-retries` |
| `t4 run` | `T4_FOLLOWER_WAIT_MODE` | `--follower-wait-mode` |
| `t4 run` | `T4_GRPC_KEEPALIVE_INTERVAL` | `--grpc-keepalive-interval` |
| `t4 run` | `T4_GRPC_KEEPALIVE_MIN_TIME` | `--grpc-keepalive-min-time` |
| `t4 run` | `T4_GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM` | `--grpc-keepalive-permit-without-stream` |
| `t4 run` | `T4_GRPC_KEEPALIVE_TIMEOUT` | `--grpc-keepalive-timeout` |
| `t4 run` | `T4_LEADER_WATCH_INTERVAL_SEC` | `--leader-watch-interval-sec` |
| `t4 run` | `T4_LISTEN` | `--listen` |
| `t4 run` | `T4_LOG_LEVEL` | `--log-level` |
| `t4 run` | `T4_METRICS_ADDR` | `--metrics-addr` |
| `t4 run` | `T4_NODE_ID` | `--node-id` |
| `t4 run` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 run` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 run` | `T4_PEER_LISTEN` | `--peer-listen` |
| `t4 run` | `T4_PEER_TLS_CA` | `--peer-tls-ca` |
| `t4 run` | `T4_PEER_TLS_CERT` | `--peer-tls-cert` |
| `t4 run` | `T4_PEER_TLS_KEY` | `--peer-tls-key` |
| `t4 run` | `T4_READ_CONSISTENCY` | `--read-consistency` |
| `t4 run` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 run` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 run` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 run` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 run` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 run` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 run` | `T4_S3_REGION` | `--s3-region` |
| `t4 run` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 run` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
| `t4 run` | `T4_SEGMENT_MAX_AGE_SEC` | `--segment-max-age-sec` |
| `t4 run` | `T4_SEGMENT_MAX_SIZE_MB` | `--segment-max-size-mb` |
| `t4 run` | `T4_TOKEN_TTL` | `--token-ttl` |
| `t4 run` | `T4_WAL_SYNC_UPLOAD` | `--wal-sync-upload` |
| `t4 run` | `T4_WATCH_SEND_TIMEOUT` | `--watch-send-timeout` |
| `t4 status` | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | `--object-store-encryption-key-env` |
| `t4 status` | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | `--object-store-encryption-key-file` |
| `t4 status` | `T4_S3_ACCESS_KEY_ID` | `--s3-access-key-id` |
| `t4 status` | `T4_S3_BUCKET` | `--s3-bucket` |
| `t4 status` | `T4_S3_CA_BUNDLE` | `--s3-ca-bundle` |
| `t4 status` | `T4_S3_ENDPOINT` | `--s3-endpoint` |
| `t4 status` | `T4_S3_PREFIX` | `--s3-prefix` |
| `t4 status` | `T4_S3_PROFILE` | `--s3-profile` |
| `t4 status` | `T4_S3_REGION` | `--s3-region` |
| `t4 status` | `T4_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` |
| `t4 status` | `T4_S3_SESSION_TOKEN` | `--s3-session-token` |
<!-- END GENERATED: cli-env-vars -->

#### CLI flag reference

Generated from the CLI flag definitions in Go. Run `go run ./hack/docgen` after changing CLI commands or flags.

<!-- BEGIN GENERATED: cli-flags -->
| Command | Flag | Default | Env | Required | Help |
|---------|------|---------|-----|----------|------|
| `t4 branch fork` | `--branch-id` | — | `T4_BRANCH_ID` | Yes | unique identifier for this branch (required) |
| `t4 branch fork` | `--checkpoint` | — | — | No | fork from this specific checkpoint key instead of the latest revision |
| `t4 branch fork` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 branch fork` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 branch fork` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 branch fork` | `--s3-bucket` | — | `T4_S3_BUCKET` | Yes | S3 bucket |
| `t4 branch fork` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 branch fork` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 branch fork` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 branch fork` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 branch fork` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 branch fork` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 branch fork` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 branch unfork` | `--branch-id` | — | `T4_BRANCH_ID` | Yes | unique identifier for this branch (required) |
| `t4 branch unfork` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 branch unfork` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 branch unfork` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 branch unfork` | `--s3-bucket` | — | `T4_S3_BUCKET` | Yes | S3 bucket |
| `t4 branch unfork` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 branch unfork` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 branch unfork` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 branch unfork` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 branch unfork` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 branch unfork` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 branch unfork` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 gc` | `--keep` | `3` | — | No | number of most-recent checkpoints to retain |
| `t4 gc` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 gc` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 gc` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 gc` | `--s3-bucket` | — | `T4_S3_BUCKET` | Yes | S3 bucket |
| `t4 gc` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 gc` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 gc` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 gc` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 gc` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 gc` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 gc` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 inspect count` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory containing local Pebble data |
| `t4 inspect count` | `--json` | `false` | — | No | emit JSON output |
| `t4 inspect count` | `--prefix` | — | — | No | only count keys with this prefix |
| `t4 inspect diff` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory containing local Pebble data |
| `t4 inspect diff` | `--from-rev` | `1` | — | No | start revision, inclusive |
| `t4 inspect diff` | `--json` | `false` | — | No | emit JSON output |
| `t4 inspect diff` | `--prefix` | — | — | No | only include keys with this prefix |
| `t4 inspect diff` | `--to-rev` | `0` | — | No | end revision, inclusive; defaults to current revision |
| `t4 inspect get` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory containing local Pebble data |
| `t4 inspect get` | `--json` | `false` | — | No | emit JSON output |
| `t4 inspect history` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory containing local Pebble data |
| `t4 inspect history` | `--json` | `false` | — | No | emit JSON output |
| `t4 inspect history` | `--limit` | `100` | — | No | maximum number of history events to print; 0 means no limit |
| `t4 inspect list` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory containing local Pebble data |
| `t4 inspect list` | `--json` | `false` | — | No | emit JSON output |
| `t4 inspect list` | `--limit` | `100` | — | No | maximum number of keys to print; 0 means no limit |
| `t4 inspect list` | `--prefix` | — | — | No | only return keys with this prefix |
| `t4 inspect meta` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory containing local Pebble data |
| `t4 inspect meta` | `--json` | `false` | — | No | emit JSON output |
| `t4 restore checkpoint` | `--checkpoint` | — | — | No | checkpoint key to restore (default: latest; use 't4 restore list' to find keys) |
| `t4 restore checkpoint` | `--data-dir` | — | `T4_DATA_DIR` | Yes | local directory to restore into (required; must not already contain a Pebble database) |
| `t4 restore checkpoint` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 restore checkpoint` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 restore checkpoint` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 restore checkpoint` | `--s3-bucket` | — | `T4_S3_BUCKET` | Yes | S3 bucket |
| `t4 restore checkpoint` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 restore checkpoint` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 restore checkpoint` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 restore checkpoint` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 restore checkpoint` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 restore checkpoint` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 restore checkpoint` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 restore list` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 restore list` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 restore list` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 restore list` | `--s3-bucket` | — | `T4_S3_BUCKET` | Yes | S3 bucket |
| `t4 restore list` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 restore list` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 restore list` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 restore list` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 restore list` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 restore list` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 restore list` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 run` | `--advertise-peer` | — | `T4_ADVERTISE_PEER` | No | address followers use to reach this node's peer stream (default: --peer-listen) |
| `t4 run` | `--auth-enabled` | `false` | `T4_AUTH_ENABLED` | No | enable etcd-compatible authentication and RBAC |
| `t4 run` | `--auto-compact-interval` | `0s` | `T4_AUTO_COMPACT_INTERVAL` | No | how often to check autocompaction; 0 defaults by strategy |
| `t4 run` | `--auto-compact-mode` | — | `T4_AUTO_COMPACT_MODE` | No | automatic history compaction strategy: off, time, or revision. Empty infers from retention flags |
| `t4 run` | `--auto-compact-retain-revisions` | `0` | `T4_AUTO_COMPACT_RETAIN_REVISIONS` | No | revision strategy: retain this many recent revisions and compact older changes |
| `t4 run` | `--auto-compact-retention` | `0s` | `T4_AUTO_COMPACT_RETENTION` | No | time strategy: retain revision history for this duration and compact older changes |
| `t4 run` | `--auto-compact-sample-interval` | `0s` | `T4_AUTO_COMPACT_SAMPLE_INTERVAL` | No | time strategy: revision/time sampling interval; 0 defaults to clamp(retention/7, 1m, 24h) when enabled |
| `t4 run` | `--branch-checkpoint` | — | `T4_BRANCH_CHECKPOINT` | No | checkpoint index key returned by 't4 branch fork' (required with --branch-prefix) |
| `t4 run` | `--branch-prefix` | — | `T4_BRANCH_PREFIX` | No | S3 key prefix of the source node to branch from (uses --s3-bucket) |
| `t4 run` | `--checkpoint-entries` | `0` | `T4_CHECKPOINT_ENTRIES` | No | triggers a checkpoint after this many WAL entries regardless of time. 0 means disabled (requires --s3-bucket) |
| `t4 run` | `--checkpoint-interval-min` | `15` | `T4_CHECKPOINT_INTERVAL_MIN` | No | checkpoint interval in minutes (requires --s3-bucket) |
| `t4 run` | `--client-tls-ca` | — | `T4_CLIENT_TLS_CA` | No | CA certificate for client mTLS (PEM); omit for server-only TLS |
| `t4 run` | `--client-tls-cert` | — | `T4_CLIENT_TLS_CERT` | No | server certificate file for client-facing TLS (PEM) |
| `t4 run` | `--client-tls-key` | — | `T4_CLIENT_TLS_KEY` | No | server private key file for client-facing TLS (PEM) |
| `t4 run` | `--data-dir` | `/var/lib/t4` | `T4_DATA_DIR` | No | directory for Pebble data and local WAL segments |
| `t4 run` | `--follower-max-retries` | `5` | `T4_FOLLOWER_MAX_RETRIES` | No | consecutive stream failures before a follower attempts a leader takeover |
| `t4 run` | `--follower-wait-mode` | `quorum` | `T4_FOLLOWER_WAIT_MODE` | No | leader wait policy for follower ACKs before commit: none, quorum, or all |
| `t4 run` | `--grpc-keepalive-interval` | `2h0m0s` | `T4_GRPC_KEEPALIVE_INTERVAL` | No | server keepalive ping interval; how often the server pings idle connections |
| `t4 run` | `--grpc-keepalive-min-time` | `5s` | `T4_GRPC_KEEPALIVE_MIN_TIME` | No | minimum interval the server demands between client keepalive pings |
| `t4 run` | `--grpc-keepalive-permit-without-stream` | `true` | `T4_GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM` | No | accept client pings even when no streams are open; required for etcd v3 client compatibility |
| `t4 run` | `--grpc-keepalive-timeout` | `20s` | `T4_GRPC_KEEPALIVE_TIMEOUT` | No | server keepalive ping ack timeout before declaring the connection dead |
| `t4 run` | `--leader-watch-interval-sec` | `300` | `T4_LEADER_WATCH_INTERVAL_SEC` | No | how often (seconds) the leader reads the lock to detect supersession |
| `t4 run` | `--listen` | `0.0.0.0:3379` | `T4_LISTEN` | No | gRPC listen address (kine/etcd protocol) |
| `t4 run` | `--log-level` | `info` | `T4_LOG_LEVEL` | No | log level (trace/debug/info/warn/error) |
| `t4 run` | `--metrics-addr` | `0.0.0.0:9090` | `T4_METRICS_ADDR` | No | HTTP address for /metrics, /healthz, /readyz (e.g. 0.0.0.0:9090) |
| `t4 run` | `--node-id` | — | `T4_NODE_ID` | No | stable unique node identifier (default: hostname) |
| `t4 run` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 run` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 run` | `--peer-listen` | — | `T4_PEER_LISTEN` | No | address for leader→follower WAL stream (e.g. 0.0.0.0:3380); enables multi-node mode |
| `t4 run` | `--peer-tls-ca` | — | `T4_PEER_TLS_CA` | No | CA certificate file for peer mTLS (PEM) |
| `t4 run` | `--peer-tls-cert` | — | `T4_PEER_TLS_CERT` | No | node certificate file for peer mTLS (PEM) |
| `t4 run` | `--peer-tls-key` | — | `T4_PEER_TLS_KEY` | No | node private key file for peer mTLS (PEM) |
| `t4 run` | `--read-consistency` | `linearizable` | `T4_READ_CONSISTENCY` | No | read consistency for follower nodes: linearizable (ReadIndex, etcd-compatible) or serializable (local, ~115x faster but may be slightly stale) |
| `t4 run` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 run` | `--s3-bucket` | — | `T4_S3_BUCKET` | No | S3 bucket |
| `t4 run` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 run` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 run` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 run` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 run` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 run` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 run` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 run` | `--segment-max-age-sec` | `10` | `T4_SEGMENT_MAX_AGE_SEC` | No | WAL segment rotation age threshold in seconds |
| `t4 run` | `--segment-max-size-mb` | `50` | `T4_SEGMENT_MAX_SIZE_MB` | No | WAL segment rotation size threshold in MiB |
| `t4 run` | `--token-ttl` | `300` | `T4_TOKEN_TTL` | No | bearer token TTL in seconds |
| `t4 run` | `--wal-sync-upload` | — | `T4_WAL_SYNC_UPLOAD` | No | upload WAL segments synchronously before ack (true/false; default true for safety, set false when local storage is durable) |
| `t4 run` | `--watch-send-timeout` | `30s` | `T4_WATCH_SEND_TIMEOUT` | No | cancel a watch whose server-side send queue blocks for longer than this; clients receive a 'mvcc: watcher is slow' cancellation when possible |
| `t4 status` | `--object-store-encryption-key-env` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_ENV` | No | environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 status` | `--object-store-encryption-key-file` | — | `T4_OBJECT_STORE_ENCRYPTION_KEY_FILE` | No | file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 |
| `t4 status` | `--s3-access-key-id` | — | `T4_S3_ACCESS_KEY_ID` | No | t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials |
| `t4 status` | `--s3-bucket` | — | `T4_S3_BUCKET` | Yes | S3 bucket |
| `t4 status` | `--s3-ca-bundle` | — | `T4_S3_CA_BUNDLE` | No | PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for MinIO and other S3-compatible stores running behind a self-signed CA |
| `t4 status` | `--s3-endpoint` | — | `T4_S3_ENDPOINT` | No | custom S3 endpoint URL, e.g. for MinIO |
| `t4 status` | `--s3-prefix` | — | `T4_S3_PREFIX` | No | key prefix inside the S3 bucket |
| `t4 status` | `--s3-profile` | — | `T4_S3_PROFILE` | No | enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile |
| `t4 status` | `--s3-region` | — | `T4_S3_REGION` | No | AWS region |
| `t4 status` | `--s3-secret-access-key` | — | `T4_S3_SECRET_ACCESS_KEY` | No | AWS secret access key |
| `t4 status` | `--s3-session-token` | — | `T4_S3_SESSION_TOKEN` | No | session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key |
| `t4 version` | `--short` | `false` | — | No | Print only the version string |
<!-- END GENERATED: cli-flags -->

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
| `--s3-region`            | AWS region (optional)                                     |
| `--s3-profile`           | AWS shared config profile (optional)                      |
| `--s3-access-key-id`     | AWS access key ID (optional)                              |
| `--s3-secret-access-key` | AWS secret access key (optional)                          |
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
