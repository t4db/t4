---
title: Getting Started
description: Run T4 as an etcd-compatible server or embed it in your Go application.
sidebar:
  order: 0
---

T4 has two modes. Pick the one that fits your use case:

| | Standalone server | Go library |
|---|---|---|
| **Use case** | Drop-in etcd replacement, Kubernetes metadata store, any language | Go apps that want zero-overhead local reads, no sidecar |
| **Client** | Any etcd v3 client (`etcdctl`, Go, Python, Java, …) | Direct Go API calls |
| **Install** | Binary or Docker image | `go get` |

---

## Standalone server

### Install

```bash
go install github.com/t4db/t4/cmd/t4@latest
```

Or use Docker:

```bash
docker pull ghcr.io/t4db/t4:latest
```

### Single node — local only

Good for development or when durability comes from infrastructure (PVC, RAID):

```bash
t4 run --data-dir /var/lib/t4 --listen 0.0.0.0:3379
```

### Single node — S3 durable

WAL segments and checkpoints are uploaded to S3. The node recovers automatically if it loses its disk:

```bash
t4 run \
  --data-dir  /var/lib/t4 \
  --listen    0.0.0.0:3379   \
  --s3-bucket my-bucket      \
  --s3-prefix t4/
```

AWS credentials are resolved from the standard chain: `AWS_*` environment variables, `~/.aws/credentials`, instance profile, or EKS workload identity.

### Use any etcd v3 client

```bash
etcdctl --endpoints=localhost:3379 put /config/timeout 30s
etcdctl --endpoints=localhost:3379 get /config/timeout
etcdctl --endpoints=localhost:3379 watch /config/ --prefix
```

The etcd Go client, Python `etcd3`, Java client, and `etcdctl` all work unchanged.

### Multi-node cluster

All nodes run the same command — no membership config, no initial cluster flag. The first node to write the S3 lock becomes leader; the rest become followers.

```bash
# Node A
t4 run \
  --data-dir       /var/lib/t4      \
  --listen         0.0.0.0:3379         \
  --s3-bucket      my-bucket            \
  --s3-prefix      t4/              \
  --node-id        node-a               \
  --peer-listen    0.0.0.0:3380         \
  --advertise-peer node-a.internal:3380

# Node B (same bucket, same prefix)
t4 run \
  --data-dir       /var/lib/t4      \
  --listen         0.0.0.0:3379         \
  --s3-bucket      my-bucket            \
  --s3-prefix      t4/              \
  --node-id        node-b               \
  --peer-listen    0.0.0.0:3380         \
  --advertise-peer node-b.internal:3380
```

New nodes can join at any time — they restore the latest S3 checkpoint and start replicating. No configuration changes needed on existing nodes.

### MinIO / S3-compatible stores

```bash
t4 run \
  --data-dir    /var/lib/t4 \
  --listen      0.0.0.0:3379   \
  --s3-bucket   my-bucket      \
  --s3-prefix   t4/        \
  --s3-endpoint http://minio:9000
```

### Kubernetes

See the [Kubernetes deployment guide](/deployment/kubernetes/) for the Helm chart, which handles multi-node clustering, PVCs, TLS, IRSA, and Prometheus out of the box.

---

## Go library

Use this when you want the store running inside your binary — no network hop for reads, no process to manage.

### Install

```bash
go get github.com/t4db/t4
```

Requires Go 1.22+.

### Local only

```go
import "github.com/t4db/t4"

node, err := t4.Open(t4.Config{
    DataDir: "/var/lib/myapp/t4",
})
if err != nil {
    log.Fatal(err)
}
defer node.Close()

// Write
rev, err := node.Put(ctx, "/config/timeout", []byte("30s"), 0)

// Read (no network, served from local Pebble)
kv, err := node.Get("/config/timeout")
fmt.Println(string(kv.Value)) // 30s

// List all keys under a prefix
kvs, err := node.List("/config/")

// Watch for changes
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
    "github.com/t4db/t4/pkg/object"
)

awsCfg, _ := awsconfig.LoadDefaultConfig(ctx)
store := object.NewS3Store(s3.NewFromConfig(awsCfg), "my-bucket", "t4/")

node, err := t4.Open(t4.Config{
    DataDir:     "/var/lib/myapp/t4",
    ObjectStore: store,
})
```

### With MinIO

```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/t4db/t4/pkg/object"
)

cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("user", "pass", "")),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...any) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://minio:9000", HostnameImmutable: true}, nil
        }),
    ),
)
store := object.NewS3Store(s3.NewFromConfig(cfg), "my-bucket", "t4/")
```

---

## Next steps

- **[Operations](/operations/)** — Multi-node clusters, TLS, auth, observability, branching
- **[Configuration](/configuration/)** — All CLI flags and `t4.Config` fields
- **[Kubernetes](/deployment/kubernetes/)** — Helm chart for production deployments
- **[API Reference](/api/)** — Full Go embedded library API
- **[Recipes](/recipes/)** — Distributed locks, service discovery, config hot-reload
- **[Migrating from etcd](/etcd-migration/)** — Compatibility guide and migration steps
