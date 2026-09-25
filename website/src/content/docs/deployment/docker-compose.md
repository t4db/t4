---
title: Docker Compose
description: Run T4 with Docker Compose — single node, S3-backed, and 3-node cluster examples.
---

## Single node, local only

The simplest setup — no S3, data on a named volume.

```yaml
# compose.yml
services:
  t4:
    image: ghcr.io/t4db/t4:latest
    command: run --data-dir /var/lib/t4 --listen 0.0.0.0:3379
    ports:
      - "3379:3379"
    volumes:
      - t4-data:/var/lib/t4

volumes:
  t4-data:
```

```bash
docker compose up -d
etcdctl --endpoints=localhost:3379 put /hello world
```

---

## Single node with an S3-compatible store

This example runs [RustFS](https://github.com/rustfs/rustfs) as a local S3-compatible server; any S3-compatible store works the same way via `--s3-endpoint`. The web console is at <http://localhost:9001/rustfs/console/>.

```yaml
# compose.yml
services:
  s3:
    image: rustfs/rustfs:1.0.0
    environment:
      RUSTFS_ACCESS_KEY: t4admin
      RUSTFS_SECRET_KEY: t4admin123
      RUSTFS_CONSOLE_ENABLE: "true"
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - s3-data:/data
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:9000/health"]
      interval: 5s
      retries: 5

  # Creates the bucket (HTTP 200, or 409 if it already exists).
  s3-init:
    image: rustfs/rustfs:1.0.0
    depends_on:
      s3:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        code=$$(curl -s -o /dev/null -w %{http_code} --aws-sigv4 aws:amz:us-east-1:s3 --user t4admin:t4admin123 -X PUT http://s3:9000/t4-data) &&
        echo create bucket: $$code && [ $$code = 200 ] || [ $$code = 409 ]
      "

  t4:
    image: ghcr.io/t4db/t4:latest
    command: >
      run
      --data-dir /var/lib/t4
      --listen 0.0.0.0:3379
      --s3-bucket t4-data
      --s3-prefix data
      --s3-endpoint http://s3:9000
    environment:
      T4_S3_ACCESS_KEY_ID: t4admin
      T4_S3_SECRET_ACCESS_KEY: t4admin123
      T4_S3_REGION: us-east-1
    ports:
      - "3379:3379"
    volumes:
      - t4-data:/var/lib/t4
    depends_on:
      s3-init:
        condition: service_completed_successfully

volumes:
  s3-data:
  t4-data:
```

---

## 3-node cluster with an S3-compatible store

```yaml
# compose.yml
x-t4-common: &t4-common
  image: ghcr.io/t4db/t4:latest
  environment:
    T4_S3_ACCESS_KEY_ID: t4admin
    T4_S3_SECRET_ACCESS_KEY: t4admin123
    T4_S3_REGION: us-east-1
  depends_on:
    s3-init:
      condition: service_completed_successfully

services:
  s3:
    image: rustfs/rustfs:1.0.0
    environment:
      RUSTFS_ACCESS_KEY: t4admin
      RUSTFS_SECRET_KEY: t4admin123
    volumes:
      - s3-data:/data
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:9000/health"]
      interval: 5s
      retries: 5

  # Creates the bucket (HTTP 200, or 409 if it already exists).
  s3-init:
    image: rustfs/rustfs:1.0.0
    depends_on:
      s3:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        code=$$(curl -s -o /dev/null -w %{http_code} --aws-sigv4 aws:amz:us-east-1:s3 --user t4admin:t4admin123 -X PUT http://s3:9000/t4-data) &&
        echo create bucket: $$code && [ $$code = 200 ] || [ $$code = 409 ]
      "

  t4-0:
    <<: *t4-common
    command: >
      run
      --data-dir /var/lib/t4
      --listen 0.0.0.0:3379
      --s3-bucket t4-data
      --s3-prefix cluster
      --s3-endpoint http://s3:9000
      --node-id t4-0
      --peer-listen 0.0.0.0:3380
      --advertise-peer t4-0:3380
      --metrics-addr 0.0.0.0:9090
    ports:
      - "3379:3379"
    volumes:
      - t4-0-data:/var/lib/t4

  t4-1:
    <<: *t4-common
    command: >
      run
      --data-dir /var/lib/t4
      --listen 0.0.0.0:3379
      --s3-bucket t4-data
      --s3-prefix cluster
      --s3-endpoint http://s3:9000
      --node-id t4-1
      --peer-listen 0.0.0.0:3380
      --advertise-peer t4-1:3380
      --metrics-addr 0.0.0.0:9090
    ports:
      - "3380:3379"
    volumes:
      - t4-1-data:/var/lib/t4

  t4-2:
    <<: *t4-common
    command: >
      run
      --data-dir /var/lib/t4
      --listen 0.0.0.0:3379
      --s3-bucket t4-data
      --s3-prefix cluster
      --s3-endpoint http://s3:9000
      --node-id t4-2
      --peer-listen 0.0.0.0:3380
      --advertise-peer t4-2:3380
      --metrics-addr 0.0.0.0:9090
    ports:
      - "3381:3379"
    volumes:
      - t4-2-data:/var/lib/t4

volumes:
  s3-data:
  t4-0-data:
  t4-1-data:
  t4-2-data:
```

```bash
docker compose up -d

# All three nodes are etcd-compatible; write to one, read from another.
etcdctl --endpoints=localhost:3379 put /hello world
etcdctl --endpoints=localhost:3380 get /hello
etcdctl --endpoints=localhost:3381 get /hello
```

---

## Build the image locally

```bash
git clone https://github.com/t4db/t4
cd t4
docker build -t t4:local .
```

The Dockerfile produces a minimal distroless image (~10 MB):

```dockerfile
FROM golang:1.25-bookworm AS builder
# ... builds /t4 binary with CGO_ENABLED=0

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /t4 /t4
EXPOSE 3379 3380 9090
ENTRYPOINT ["/t4"]
CMD ["run"]
```

To use your local build, replace `image: ghcr.io/t4db/t4:latest` with `image: t4:local` in the Compose files above.

---

## Health checks

```yaml
t4:
  # ...
  healthcheck:
    test: ["CMD-SHELL", "wget -qO- http://localhost:9090/healthz || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 3
    start_period: 15s
```

- `/healthz` — returns 200 once the node has started
- `/readyz` — returns 200 when the node is ready to serve reads (after WAL replay and election)
