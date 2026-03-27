# Strata

Strata is an embeddable, S3-durable key-value store with etcd-compatible semantics. It is designed to run either as a library inside your Go program or as a standalone binary serving the [kine](https://github.com/k3s-io/kine) wire protocol (which makes it a drop-in etcd replacement for Kubernetes).

Key properties:

- **Embedded-first** — `strata.Open(cfg)` is the entire API surface; no separate process needed.
- **S3-durable** — WAL segments and periodic Pebble snapshots are uploaded to S3 (or any S3-compatible store). A node that loses its disk recovers automatically.
- **Multi-node** — A leader is elected via an S3 lock. Followers stream the WAL from the leader in real time and transparently forward writes. Leader election is driven by stream liveness, not heartbeat polling.
- **etcd-compatible** — A thin kine adapter makes Strata usable as a Kubernetes datastore without any changes to Kubernetes or kine.

---

## Contents

1. [Embedded usage](#embedded-usage)
2. [Standalone CLI (kine endpoint)](#standalone-cli)
3. [Single-node quickstart](#single-node-quickstart)
4. [Multi-node setup](#multi-node-setup)
5. [mTLS between peers](#mtls-between-peers)
6. [Observability](#observability)
7. [Configuration reference](#configuration-reference)
8. [CLI flag reference](#cli-flag-reference)
9. [API reference](#api-reference)

---

## Embedded usage

```go
import "github.com/makhov/strata"

node, err := strata.Open(strata.Config{
    DataDir: "/var/lib/myapp/strata",
})
if err != nil {
    log.Fatal(err)
}
defer node.Close()

// Write
rev, err := node.Put(ctx, "/config/timeout", []byte("30s"), 0)b

// Read
kv, err := node.Get("/config/timeout")
fmt.Println(string(kv.Value)) // 30s

// Watch
events, _ := node.Watch(ctx, "/config/", 0)
for e := range events {
    fmt.Printf("%s %s=%s\n", e.Type, e.KV.Key, e.KV.Value)
}
```

No external processes, no network ports, no configuration files required for single-node embedded use.

### Adding S3 durability

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/makhov/strata"
    "github.com/makhov/strata/internal/object"
)

awsCfg, _ := config.LoadDefaultConfig(ctx)
s3Client  := s3.NewFromConfig(awsCfg)
store     := object.NewS3Store(s3Client, "my-bucket", "strata/")

node, err := strata.Open(strata.Config{
    DataDir:     "/var/lib/myapp/strata",
    ObjectStore: store,
})
```

With an object store configured, Strata:

- Uploads sealed WAL segments to S3 automatically.
- Writes a Pebble snapshot (checkpoint) to S3 every 15 minutes (configurable).
- Cleans up S3 WAL segments that predate the latest checkpoint.
- On startup, restores from the latest checkpoint and replays any remaining WAL segments.

---

## Standalone CLI

The `strata` binary exposes a node as a kine gRPC endpoint that Kubernetes (or any other kine client) connects to.

```
go install github.com/makhov/strata/cmd/strata@latest
```

---

## Single-node quickstart

```bash
strata \
  --data-dir /var/lib/strata \
  --listen   0.0.0.0:2379
```

That's it. No S3, no peers — pure local durability. Suitable for development or single-node clusters where local disk loss is acceptable.

### With S3

```bash
strata \
  --data-dir  /var/lib/strata \
  --listen    0.0.0.0:2379 \
  --s3-bucket my-bucket \
  --s3-prefix strata/
```

AWS credentials are picked up from the default credential chain (environment variables, `~/.aws/credentials`, instance profile, etc.).

### With MinIO or another S3-compatible store

```bash
strata \
  --data-dir    /var/lib/strata \
  --listen      0.0.0.0:2379 \
  --s3-bucket   my-bucket \
  --s3-prefix   strata/ \
  --s3-endpoint http://minio:9000
```

---

## Multi-node setup

Multi-node mode requires:

1. A shared S3 bucket (used for leader election and WAL archive).
2. Each node to have a unique `--node-id` and a `--peer-listen` address reachable by the other nodes.

All nodes run the same command. They race to acquire the S3 leader lock at startup; the winner becomes the leader, the others become followers.

### Example: three nodes

**Node A** (ends up as leader on first boot):

```bash
strata \
  --data-dir         /var/lib/strata \
  --listen           0.0.0.0:2379 \
  --s3-bucket        my-bucket \
  --s3-prefix        strata/ \
  --node-id          node-a \
  --peer-listen      0.0.0.0:2380 \
  --advertise-peer   node-a.internal:2380
```

**Node B:**

```bash
strata \
  --data-dir         /var/lib/strata \
  --listen           0.0.0.0:2379 \
  --s3-bucket        my-bucket \
  --s3-prefix        strata/ \
  --node-id          node-b \
  --peer-listen      0.0.0.0:2380 \
  --advertise-peer   node-b.internal:2380
```

**Node C:** same as B, with `--node-id node-c` and its own `--advertise-peer`.

### How leader election works

- On startup, each node calls `TryAcquire`, which writes its address to the S3 lock only if the lock is absent.
- The winner starts serving writes and begins streaming WAL entries to followers over port 2380.
- Followers detect a dead leader when the WAL stream fails `--follower-max-retries` times consecutively (default 5). A follower then calls `TakeOver`, which overwrites the S3 lock with its own address and becomes the new leader.
- The former leader's `watchLoop` detects the supersession and steps down (within `--leader-watch-interval-sec`, default 300 s).
- Clients connecting to a follower for writes are automatically forwarded to the current leader; the follower returns the result transparently.

No TTL polling. The only S3 writes are at startup, on TakeOver, and at each checkpoint.

---

## mTLS between peers

Provide a shared CA and per-node certificate/key pair (all in PEM format):

```bash
strata \
  ... \
  --peer-tls-ca   /etc/strata/tls/ca.crt \
  --peer-tls-cert /etc/strata/tls/node.crt \
  --peer-tls-key  /etc/strata/tls/node.key
```

Both the peer gRPC server (leader) and client (follower) use these files. The same CA must be used across all nodes. TLS 1.3 is required; mutual authentication is enforced.

Generating self-signed certificates with `cfssl` or `openssl` is outside the scope of this document, but any standard PKI toolchain works.

---

## Observability

```bash
strata \
  ... \
  --metrics-addr 0.0.0.0:9090
```

Three endpoints are served on that address:

| Path | Description |
|---|---|
| `GET /metrics` | Prometheus metrics |
| `GET /healthz` | Returns 200 once the node has started |
| `GET /readyz` | Returns 200 when the node is ready to serve reads |

### Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `strata_writes_total` | counter | `op` | Completed local write operations |
| `strata_write_errors_total` | counter | `op` | Write operations that returned an error |
| `strata_write_duration_seconds` | histogram | `op` | Local write latency (WAL + apply) |
| `strata_forwarded_writes_total` | counter | `op` | Writes forwarded from follower to leader |
| `strata_forward_duration_seconds` | histogram | `op` | Round-trip latency of forwarded writes |
| `strata_current_revision` | gauge | — | Latest applied revision |
| `strata_compact_revision` | gauge | — | Compaction watermark |
| `strata_role` | gauge | `role` | 1 for the active role (`leader`/`follower`/`single`), 0 otherwise |
| `strata_wal_uploads_total` | counter | — | WAL segments successfully uploaded to S3 |
| `strata_wal_upload_errors_total` | counter | — | Failed WAL segment uploads |
| `strata_wal_upload_duration_seconds` | histogram | — | WAL segment upload latency |
| `strata_wal_gc_segments_total` | counter | — | WAL segments deleted from S3 during GC |
| `strata_checkpoints_total` | counter | — | Checkpoints written to S3 |
| `strata_elections_total` | counter | `outcome` | Leader election attempts by outcome (`won`/`lost`) |

`op` label values: `put`, `create`, `update`, `delete`, `compact`.

---

## Configuration reference

`strata.Config` fields when using the library directly:

| Field | Type | Default | Description |
|---|---|---|---|
| `DataDir` | `string` | — | **Required.** Directory for Pebble data and local WAL segments. |
| `ObjectStore` | `object.Store` | `nil` | S3 (or compatible) store. Nil = local-only, single-node mode. |
| `SegmentMaxSize` | `int64` | 50 MB | WAL segment rotation size threshold in bytes. |
| `SegmentMaxAge` | `time.Duration` | 10 s | WAL segment rotation age threshold. |
| `CheckpointInterval` | `time.Duration` | 15 min | How often the leader writes a checkpoint to S3. |
| `CheckpointEntries` | `int64` | 0 (disabled) | Also checkpoint after this many WAL entries. |
| `NodeID` | `string` | hostname | Stable, unique node identifier. |
| `PeerListenAddr` | `string` | `""` | gRPC listen address for the WAL stream. Empty = single-node. |
| `AdvertisePeerAddr` | `string` | `PeerListenAddr` | Address followers use to reach this node. |
| `LeaderWatchInterval` | `time.Duration` | 5 min | How often the leader re-reads the S3 lock to detect supersession. |
| `FollowerMaxRetries` | `int` | 5 | Consecutive stream failures before a follower attempts a TakeOver. |
| `PeerBufferSize` | `int` | 10 000 | WAL entries buffered in memory for follower catch-up. |
| `PeerServerTLS` | `credentials.TransportCredentials` | `nil` | mTLS credentials for the peer gRPC server. |
| `PeerClientTLS` | `credentials.TransportCredentials` | `nil` | mTLS credentials for the peer gRPC client. |
| `MetricsAddr` | `string` | `""` | HTTP address for `/metrics`, `/healthz`, `/readyz`. |

---

## CLI flag reference

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `/var/lib/strata` | Pebble + WAL storage directory |
| `--listen` | `0.0.0.0:2379` | kine/etcd gRPC listen address |
| `--s3-bucket` | — | S3 bucket name |
| `--s3-prefix` | — | Key prefix inside the bucket |
| `--s3-endpoint` | — | Custom S3 endpoint (MinIO, etc.) |
| `--segment-max-size-mb` | `50` | WAL segment rotation size (MiB) |
| `--segment-max-age-sec` | `10` | WAL segment rotation age (seconds) |
| `--checkpoint-interval-min` | `15` | Checkpoint interval (minutes) |
| `--log-level` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `--node-id` | hostname | Stable unique node identifier |
| `--peer-listen` | — | Peer WAL stream address; enables multi-node mode |
| `--advertise-peer` | `--peer-listen` | Advertised peer address |
| `--leader-watch-interval-sec` | `300` | Leader lock re-read interval (seconds) |
| `--follower-max-retries` | `5` | Stream failures before TakeOver attempt |
| `--peer-tls-ca` | — | CA certificate file for peer mTLS (PEM) |
| `--peer-tls-cert` | — | Node certificate file for peer mTLS (PEM) |
| `--peer-tls-key` | — | Node private key file for peer mTLS (PEM) |
| `--metrics-addr` | — | HTTP address for metrics/health endpoints |

---

## API reference

### Write operations

```go
// Put creates or replaces key. Returns the new revision.
Put(ctx context.Context, key string, value []byte, lease int64) (int64, error)

// Create fails with ErrKeyExists if key already exists.
Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)

// Update is a compare-and-swap on revision. Returns (newRev, oldKV, succeeded, err).
// If revision == 0, the update is unconditional.
Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)

// Delete removes key unconditionally.
Delete(ctx context.Context, key string) (int64, error)

// DeleteIfRevision is a compare-and-swap delete. Returns (newRev, oldKV, succeeded, err).
DeleteIfRevision(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)

// Compact discards history up to and including revision.
Compact(ctx context.Context, revision int64) error
```

### Read operations

```go
// Get returns the current value for key, or nil if not found.
Get(key string) (*KeyValue, error)

// List returns all keys with the given prefix, in lexicographic order.
List(prefix string) ([]*KeyValue, error)

// Count returns the number of keys with the given prefix.
Count(prefix string) (int64, error)
```

### Watch

```go
// Watch streams events for keys matching prefix starting from startRev.
// startRev=0 means start from the current revision.
// The returned channel is closed when ctx is cancelled.
Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error)

// WaitForRevision blocks until the node has applied at least rev.
WaitForRevision(ctx context.Context, rev int64) error
```

### KeyValue

```go
type KeyValue struct {
    Key            string
    Value          []byte
    Revision       int64 // revision at which this key was last modified
    CreateRevision int64 // revision at which this key was created
    PrevRevision   int64 // previous modification revision (0 if none)
    Lease          int64
}
```

### Sentinel errors

```go
var ErrKeyExists  error // returned by Create when the key already exists
var ErrNotLeader  error // returned by writes when forwarding is not available
```

---

## Architecture overview

```
                      ┌──────────────────────────────────────────────────┐
                      │                    Leader node                    │
  client writes ────► │  Node.Put/Create/… → WAL.Append → store.Apply   │
                      │                          │                        │
                      │              peer.Server.Broadcast               │
                      └──────────────────────────┬───────────────────────┘
                                                 │  gRPC stream (port 2380)
                           ┌─────────────────────┴──────────────────────┐
                           ▼                                             ▼
                   ┌───────────────┐                           ┌───────────────┐
                   │  Follower B   │                           │  Follower C   │
                   │  WAL.Append   │                           │  WAL.Append   │
                   │  store.Apply  │                           │  store.Apply  │
                   └───────────────┘                           └───────────────┘
                   reads: local                                reads: local
                   writes: forwarded ──────────────────────►  leader via gRPC

                      ┌──────────────┐
                      │  S3 bucket   │
                      │  leader-lock │  ◄── election / liveness
                      │  wal/…       │  ◄── durability
                      │  checkpoint/ │  ◄── fast recovery
                      └──────────────┘
```
