# Strata

[![CI](https://github.com/makhov/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/makhov/strata/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/makhov/strata.svg)](https://pkg.go.dev/github.com/makhov/strata)
[![Go Report Card](https://goreportcard.com/badge/github.com/makhov/strata)](https://goreportcard.com/report/github.com/makhov/strata)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An embeddable, S3-durable key-value store for Go.

- **Embedded-first** — `strata.Open(cfg)` is the entire API. No sidecar, no daemon.
- **S3-durable** — WAL segments and periodic snapshots are uploaded to S3. A node that loses its disk recovers automatically.
- **Multi-node** — Leader elected via an S3 lock. Followers stream the WAL in real time and forward writes transparently.
- **etcd v3 compatible** — The standalone binary speaks the etcd v3 gRPC protocol. Any etcd client works against it unchanged.
- **Branches** — Fork a database at any checkpoint with zero S3 copies. Each branch writes to its own prefix; shared SST files are deduplicated automatically.

---

## Embedded usage

```go
import "github.com/makhov/strata"

node, err := strata.Open(strata.Config{
    DataDir: "/var/lib/myapp/strata",
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
    awsconfig "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/makhov/strata/pkg/object"
)

awsCfg, _ := awsconfig.LoadDefaultConfig(ctx)
store := object.NewS3Store(s3.NewFromConfig(awsCfg), "my-bucket", "strata/")

node, err := strata.Open(strata.Config{
    DataDir:     "/var/lib/myapp/strata",
    ObjectStore: store,
})
```

---

## Standalone binary

The `strata` binary exposes the etcd v3 gRPC protocol. Use `etcdctl`, the official Go client, or any other etcd v3 compatible tool.

```bash
go install github.com/makhov/strata/cmd/strata@latest

# Single node, local only
strata run --data-dir /var/lib/strata --listen 0.0.0.0:3379

# Single node with S3
strata run --data-dir /var/lib/strata --listen 0.0.0.0:3379 \
           --s3-bucket my-bucket --s3-prefix strata/

# Verify
etcdctl --endpoints=localhost:3379 put /hello world
etcdctl --endpoints=localhost:3379 get /hello
```

Multi-node and production setup: see [docs/operations.md](docs/operations.md).

---

## Documentation

| Document | Contents |
|---|---|
| [docs/api.md](docs/api.md) | Full Go API reference — methods, types, errors, branching |
| [docs/configuration.md](docs/configuration.md) | All config fields and CLI flags |
| [docs/operations.md](docs/operations.md) | Multi-node, S3, mTLS, observability, branching, point-in-time restore |
| [docs/architecture.md](docs/architecture.md) | Internals — WAL, checkpoints, election, replication |
