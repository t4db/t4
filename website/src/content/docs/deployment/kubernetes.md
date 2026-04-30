---
title: Kubernetes
description: Deploy T4 on Kubernetes using the official Helm chart — single-node and multi-node clusters with S3, TLS, and Prometheus.
---

T4 ships a Helm chart that deploys a StatefulSet with persistent volumes, health probes, Prometheus metrics, and optional Envoy load-balancing proxy.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- An S3 bucket (required for multi-node; optional for single-node)

---

## Quick start — single node

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set s3.bucket=my-bucket \
  --set s3.region=us-east-1
```

This creates a single-replica StatefulSet with a 10 Gi PVC and exposes the etcd-compatible gRPC port on a ClusterIP Service.

### Connect with etcdctl

```bash
kubectl port-forward svc/t4 3379:3379
etcdctl --endpoints=localhost:3379 put /hello world
etcdctl --endpoints=localhost:3379 get /hello
```

---

## Multi-node cluster

Set `replicaCount` to an odd number (3 or 5 recommended). All pods get stable DNS names via the headless Service (`t4-0.t4-headless`, etc.) and race to acquire the S3 leader lock on startup.

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set replicaCount=3 \
  --set s3.bucket=my-bucket \
  --set s3.region=us-east-1
```

Pods are scheduled with `podAntiAffinity` to spread across nodes automatically.

---

## S3 credentials

### IRSA / EKS Workload Identity (recommended)

Create the IAM role with the required S3 permissions, then:

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set s3.bucket=my-bucket \
  --set s3.region=us-east-1 \
  --set s3.useIRSA=true \
  --set s3.iamRoleArn=arn:aws:iam::123456789012:role/t4-s3-role \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::123456789012:role/t4-s3-role
```

### Static credentials via existing Secret

```bash
kubectl create secret generic t4-s3-credentials \
  --from-literal=T4_S3_ACCESS_KEY_ID=AKIA... \
  --from-literal=T4_S3_SECRET_ACCESS_KEY=...

helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set s3.bucket=my-bucket \
  --set s3.existingSecret=t4-s3-credentials
```

### MinIO / S3-compatible stores

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set s3.bucket=my-bucket \
  --set s3.endpoint=http://minio.minio-ns.svc.cluster.local:9000 \
  --set s3.existingSecret=minio-credentials
```

### Built-in MinIO (development / CI)

The chart can deploy a single-node MinIO instance alongside T4 and wire everything up automatically — no external object store needed:

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set minio.enabled=true
```

This creates a MinIO Deployment, PVC, Service, and a post-install Job that creates the `t4` bucket. T4's S3 endpoint, bucket, and credentials are configured automatically.

Customise credentials, bucket name, and storage:

```yaml
# values.yaml
minio:
  enabled: true
  rootUser: myuser
  rootPassword: mypassword  # change this!
  bucket: t4
  persistence:
    size: 20Gi
```

Access the MinIO web console:

```bash
kubectl port-forward svc/t4-minio 9001:9001
open http://localhost:9001
```

> ⚠ **Not for production.** Use a managed S3 service or a dedicated MinIO cluster for production deployments.

---

## Persistence

By default each pod gets a 10 Gi PVC. Adjust size and storage class:

```yaml
# values.yaml
persistence:
  enabled: true
  size: 50Gi
  storageClass: gp3
```

To disable persistence (ephemeral — relies entirely on S3 for recovery):

```yaml
persistence:
  enabled: false
```

---

## TLS

### Client TLS (etcd port)

Enable client-facing TLS. If `secretName` is empty, the chart generates a self-signed Secret automatically:

```yaml
# values.yaml
tls:
  enabled: true
```

To use your own certificate, create a TLS Secret and reference it:

```bash
kubectl create secret tls t4-client-tls \
  --cert=server.crt --key=server.key
```

```yaml
# values.yaml
tls:
  enabled: true
  secretName: t4-client-tls
```

To also require client certificates (mTLS), include `ca.crt` in the Secret and set `clientCertAuth`:

```bash
kubectl create secret generic t4-client-tls \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt
```

```yaml
# values.yaml
tls:
  enabled: true
  secretName: t4-client-tls
  clientCertAuth: true
```

### Peer mTLS (inter-node replication)

```bash
kubectl create secret generic t4-peer-tls \
  --from-file=tls.crt=node.crt \
  --from-file=tls.key=node.key \
  --from-file=ca.crt=ca.crt
```

```yaml
# values.yaml
tls:
  peer:
    enabled: true
    secretName: t4-peer-tls
```

### With cert-manager

```yaml
# issuer.yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: t4-ca
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: t4-peer-tls
spec:
  secretName: t4-peer-tls
  issuerRef:
    name: t4-ca
  dnsNames:
    - t4-0.t4-headless
    - t4-1.t4-headless
    - t4-2.t4-headless
    - t4-headless
  usages:
    - server auth
    - client auth
```

```bash
kubectl apply -f issuer.yaml

helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set tls.peer.enabled=true \
  --set tls.peer.secretName=t4-peer-tls
```

---

## Prometheus metrics

Enable a `ServiceMonitor` for the Prometheus Operator:

```yaml
# values.yaml
serviceMonitor:
  enabled: true
  namespace: monitoring   # namespace where Prometheus Operator watches
  interval: 30s
  labels:
    release: kube-prometheus-stack  # match your Prometheus selector label
```

The ServiceMonitor scrapes the `/metrics` endpoint on port `9090`.

---

## Envoy proxy (read scale-out)

When `replicaCount > 1`, enabling the Envoy proxy routes writes to the leader and load-balances reads across all healthy replicas:

```yaml
# values.yaml
replicaCount: 3
proxy:
  enabled: true
  replicaCount: 2
  lbPolicy: LEAST_REQUEST
```

Clients connect to the proxy Service (`t4-proxy`) instead of `t4` directly. The proxy detects the leader via the `/healthz/leader` endpoint on each pod.

Client TLS through the proxy is terminated at Envoy so the proxy can inspect gRPC method paths and route writes to the leader. Enable `tls` for this path:

```yaml
# values.yaml
proxy:
  enabled: true
tls:
  enabled: true
```

When `proxy.enabled=true`, `tls.enabled=true` terminates TLS at Envoy and Envoy forwards plaintext gRPC to the T4 pods inside the cluster. When `proxy.enabled=false`, the same setting terminates TLS inside T4.

To require client certificates at the proxy, include `ca.crt` in the Secret and set `clientCertAuth`, or omit `secretName` to use the chart-generated CA:

```yaml
# values.yaml
proxy:
  enabled: true
tls:
  enabled: true
  secretName: t4-client-tls
  clientCertAuth: true
```

---

## Full values.yaml example (production 3-node)

```yaml
replicaCount: 3

image:
  repository: ghcr.io/t4db/t4
  tag: "0.11.0"

config:
  walSyncUpload: "false"   # PVC provides durability
  logLevel: info

s3:
  bucket: my-t4-prod
  prefix: k8s/prod
  region: us-east-1
  useIRSA: true
  iamRoleArn: arn:aws:iam::123456789012:role/t4-prod

persistence:
  size: 50Gi
  storageClass: gp3

tls:
  peer:
    enabled: true
    secretName: t4-peer-tls

serviceMonitor:
  enabled: true
  namespace: monitoring
  labels:
    release: kube-prometheus-stack

proxy:
  enabled: true
  replicaCount: 2

resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    memory: 2Gi
```

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 -f values.yaml
```

---

## Upgrading

```bash
helm upgrade t4 oci://ghcr.io/t4db/charts/t4 -f values.yaml
```

The StatefulSet rolls pods one at a time. With `replicaCount >= 3`, quorum is maintained throughout the upgrade. If the leader pod is updated, a follower automatically wins a new election.

---

## Uninstalling

```bash
helm uninstall t4
```

PVCs are **not** deleted automatically. To also remove data:

```bash
kubectl delete pvc -l app.kubernetes.io/name=t4
```
