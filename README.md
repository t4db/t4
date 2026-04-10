# T4

[![CI](https://github.com/t4db/t4/actions/workflows/ci.yml/badge.svg)](https://github.com/t4db/t4/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/t4db/t4.svg)](https://pkg.go.dev/github.com/t4db/t4)
[![Go Report Card](https://goreportcard.com/badge/github.com/t4db/t4)](https://goreportcard.com/report/github.com/t4db/t4)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-t4db.github.io-green)](https://t4db.github.io/t4/)

An embeddable, S3-durable key-value store for Go, with an etcd-compatible standalone server.

- **Embedded-first** — `t4.Open(cfg)` is the entire API. No sidecar, no daemon.
- **S3-durable** — WAL segments and periodic checkpoints are uploaded to S3. A node that loses its disk recovers automatically.
- **Multi-node** — Leader elected via an S3 lock. Followers stream the WAL in real time and forward writes transparently.
- **etcd v3 compatible** — The standalone binary speaks the etcd v3 gRPC protocol, including multi-key transactions.
- **Twelve-factor config** — CLI flags can be supplied through `T4_*` environment variables.
- **Branches** — Fork a database at any checkpoint with zero S3 copies. Each branch writes to its own prefix; shared SST files are deduplicated automatically.

---

## Embedded usage

```go
import "github.com/t4db/t4"

node, err := t4.Open(t4.Config{
    DataDir: "/var/lib/myapp/t4",
})
defer node.Close()

rev, err := node.Put(ctx, "/config/timeout", []byte("30s"), 0)

kv, err := node.Get("/config/timeout")
fmt.Println(string(kv.Value)) // 30s

events, _ := node.Watch(ctx, "/config/", 0)
for e := range events {
    fmt.Printf("%s %s=%s\n", e.Type, e.KV.Key, e.KV.Value)
}
```

### With S3 durability

```go
import (
    "github.com/t4db/t4"
    "github.com/t4db/t4/pkg/object"
)

store, err := object.NewS3StoreFromConfig(ctx, object.S3Config{
    Bucket: "my-bucket",
    Prefix: "t4/",
    Region: "us-east-1",
    // Endpoint: "http://localhost:9000", // MinIO or another S3-compatible store
})
if err != nil {
    return err
}

node, err := t4.Open(t4.Config{
    DataDir:     "/var/lib/myapp/t4",
    ObjectStore: store,
})
```

---

## Standalone binary

The `t4` binary exposes the etcd v3 gRPC protocol. Use `etcdctl`, the official Go client, or any other etcd v3 compatible tool.

```bash
go install github.com/t4db/t4/cmd/t4@latest

# Single node, local only
t4 run --data-dir /var/lib/t4 --listen 0.0.0.0:3379

# Single node with S3
t4 run --data-dir /var/lib/t4 --listen 0.0.0.0:3379 \
           --s3-bucket my-bucket --s3-prefix t4/

# The same configuration can come from environment variables.
T4_DATA_DIR=/var/lib/t4 \
T4_LISTEN=0.0.0.0:3379 \
T4_S3_BUCKET=my-bucket \
T4_S3_PREFIX=t4/ \
T4_S3_REGION=us-east-1 \
t4 run

# Verify
etcdctl --endpoints=localhost:3379 put /hello world
etcdctl --endpoints=localhost:3379 get /hello
```

Multi-node and production setup: see [Operations](https://t4db.github.io/t4/operations/).

---

## Branching

Branches fork a database from an existing S3 checkpoint without copying shared SST files.

```bash
# Register the branch against the source prefix.
checkpoint_key=$(t4 branch fork \
  --s3-bucket my-bucket \
  --s3-prefix t4/ \
  --branch-id experiment)

# Start the branch in its own prefix, using the source prefix as its ancestor.
t4 run \
  --data-dir /var/lib/t4-experiment \
  --listen 0.0.0.0:3379 \
  --s3-bucket my-bucket \
  --s3-prefix t4-experiment/ \
  --branch-prefix t4/ \
  --branch-checkpoint "$checkpoint_key"
```

When the branch is retired, remove its registry entry so future GC can reclaim unneeded source objects:

```bash
t4 branch unfork --s3-bucket my-bucket --s3-prefix t4/ --branch-id experiment
```

---

## Documentation

Full documentation is available at **[t4db.github.io/t4](https://t4db.github.io/t4/)**.

| Document | Contents |
|---|---|
| [Getting Started](https://t4db.github.io/t4/getting-started/) | Quickstart for standalone server and embedded Go library |
| [API Reference](docs/api.md) | Full Go API — methods, types, errors, branching |
| [Configuration](docs/configuration.md) | All config fields and CLI flags |
| [Operations](docs/operations.md) | Multi-node clusters, S3, TLS, authentication, RBAC, observability |
| [Backup and Restore](docs/backup-restore.md) | Checkpoints, point-in-time restore, branching, retention |
| [Security](https://t4db.github.io/t4/security/) | TLS, mTLS, client auth, RBAC setup |
| [Recipes](https://t4db.github.io/t4/recipes/) | Distributed locks, service discovery, common patterns |
| [Kubernetes](https://t4db.github.io/t4/deployment/kubernetes/) | Helm chart, StatefulSet deployment |
| [Docker Compose](https://t4db.github.io/t4/deployment/docker-compose/) | Local, MinIO-backed, and multi-node cluster examples |
| [Architecture](docs/architecture.md) | Internals — WAL, checkpoints, leader election, replication |
| [Benchmarks](docs/benchmarks.md) | T4 vs etcd benchmark results and analysis |
| [Migrating from etcd](https://t4db.github.io/t4/etcd-migration/) | Compatibility table and migration steps |
| [Troubleshooting](https://t4db.github.io/t4/troubleshooting/) | Diagnostics, debug logging, and common fixes |
| [FAQ](https://t4db.github.io/t4/faq/) | Frequently asked questions |
