---
title: Docker Compose
description: Run T4 with Docker Compose — single node, MinIO-backed, and 3-node cluster examples.
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

## Single node with MinIO (S3-compatible)

```yaml
# compose.yml
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/t4
      "

  t4:
    image: ghcr.io/t4db/t4:latest
    command: >
      run
      --data-dir /var/lib/t4
      --listen 0.0.0.0:3379
      --s3-bucket t4
      --s3-prefix data/
      --s3-endpoint http://minio:9000
    environment:
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_DEFAULT_REGION: us-east-1
    ports:
      - "3379:3379"
    volumes:
      - t4-data:/var/lib/t4
    depends_on:
      minio-init:
        condition: service_completed_successfully

volumes:
  minio-data:
  t4-data:
```

---

## 3-node cluster with MinIO

```yaml
# compose.yml
x-t4-common: &t4-common
  image: ghcr.io/t4db/t4:latest
  environment:
    AWS_ACCESS_KEY_ID: minioadmin
    AWS_SECRET_ACCESS_KEY: minioadmin
    AWS_DEFAULT_REGION: us-east-1
  depends_on:
    minio-init:
      condition: service_completed_successfully

services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/t4
      "

  t4-0:
    <<: *t4-common
    command: >
      run
      --data-dir /var/lib/t4
      --listen 0.0.0.0:3379
      --s3-bucket t4
      --s3-prefix cluster/
      --s3-endpoint http://minio:9000
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
      --s3-bucket t4
      --s3-prefix cluster/
      --s3-endpoint http://minio:9000
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
      --s3-bucket t4
      --s3-prefix cluster/
      --s3-endpoint http://minio:9000
      --node-id t4-2
      --peer-listen 0.0.0.0:3380
      --advertise-peer t4-2:3380
      --metrics-addr 0.0.0.0:9090
    ports:
      - "3381:3379"
    volumes:
      - t4-2-data:/var/lib/t4

volumes:
  minio-data:
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
