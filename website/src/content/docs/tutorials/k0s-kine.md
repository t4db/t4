---
title: Use T4 with k0s and kine
description: Run a single-node k0s cluster with T4 as kine's embedded datastore.
---

In this tutorial you will run a single-node k0s cluster with T4 as its datastore. k0s starts kine, kine embeds T4, and the Kubernetes API server continues to use kine's local etcd-compatible endpoint.

This local-only setup is intended for learning and development. At the end, you can optionally add S3 durability.

## Prerequisites

- k0s v1.36.1+k0s.0 or newer installed

## 1. Install k0s

Install k0s v1.36.1+k0s.0 or newer as `/usr/local/bin/k0s`. Follow the [k0s installation guide](https://docs.k0sproject.io/stable/install/) if it is not installed yet.

```bash
k0s version
```

## 2. Configure kine to use T4

Create `/etc/k0s/k0s.yaml`:

```yaml
apiVersion: k0s.k0sproject.io/v1beta1
kind: ClusterConfig
metadata:
  name: k0s
spec:
  storage:
    type: kine
    kine:
      dataSource: t4:///var/lib/k0s/t4
```

The `t4://` scheme selects T4's kine driver. The path after it is T4's local data directory, where Pebble data and WAL segments are stored.

## 3. Install and start the controller

Install k0s as a single-node controller and worker, but do not start it yet:

```bash
sudo k0s install controller --single --config /etc/k0s/k0s.yaml
```

k0s runs kine as the `kube-apiserver` system user. Create a writable data directory for T4, then start k0s:

```bash
sudo install -d -o kube-apiserver -g root -m 0700 /var/lib/k0s/t4
sudo k0s start
```

Wait for the node to become ready:

```bash
sudo k0s status
sudo k0s kubectl get nodes
```

If startup fails, inspect the controller logs:

```bash
sudo journalctl -u k0scontroller --no-pager -n 100
```

## 4. Verify persistence

Create a Kubernetes object, restart k0s, and read the object back:

```bash
sudo k0s kubectl create namespace t4-tutorial
sudo k0s kubectl create configmap hello \
  --namespace t4-tutorial \
  --from-literal=message='Hello from T4'

sudo k0s stop
sudo k0s start

until sudo k0s kubectl get --raw=/readyz >/dev/null 2>&1; do
  sleep 2
done

sudo k0s kubectl get configmap hello \
  --namespace t4-tutorial \
  -o jsonpath='{.data.message}{"\n"}'
```

The final command should print:

```text
Hello from T4
```

Your cluster state now lives in `/var/lib/k0s/t4` instead of k0s-managed etcd.

## Optional: add S3 durability

T4 can upload checkpoints and WAL segments to S3 or an S3-compatible object store. Add kine arguments to the same configuration:

```yaml
spec:
  storage:
    type: kine
    kine:
      dataSource: t4:///var/lib/k0s/t4
      extraArgs:
        s3-bucket: my-k0s-state
        s3-folder: clusters/dev
        s3-region: us-east-1
```

Pass credentials into the k0s service when you install or reinstall it:

```bash
sudo k0s install controller --single \
  --config /etc/k0s/k0s.yaml \
  --force \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

sudo k0s stop
sudo k0s start
```

Use a unique `s3-folder` for every cluster that shares the bucket. For MinIO or another S3-compatible service, also set `s3-endpoint` under `extraArgs`.

This tutorial remains a single-controller setup. A multi-controller deployment also needs T4 peer addresses, shared S3 configuration, and peer TLS.
