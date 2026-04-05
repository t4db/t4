# Kubernetes Deployment

This guide covers running Strata on Kubernetes — both single-node and multi-node clusters — using raw manifests or the Helm chart.

---

## Prerequisites

- Kubernetes 1.24+
- An S3 bucket (required for multi-node; optional for single-node ephemeral)
- Helm 3.x (if using the Helm chart)

---

## Quick start with Helm

```bash
# Single node
helm install strata oci://ghcr.io/strata-db/charts/strata \
  --set s3.bucket=my-bucket \
  --set s3.region=us-east-1

# Three-node cluster
helm install strata oci://ghcr.io/strata-db/charts/strata \
  --set replicaCount=3 \
  --set s3.bucket=my-bucket \
  --set s3.region=us-east-1
```

See the [full Kubernetes deployment guide](https://strata-db.github.io/docs/deployment/kubernetes) for Helm values, TLS, IRSA, and Envoy proxy configuration.

---

## Raw manifests

### Single-node deployment

A single-node setup needs:
1. A `Deployment` or `StatefulSet` with one replica.
2. S3 credentials injected as environment variables.
3. A persistent volume for the data directory.
4. A `Service` for client access.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: strata
spec:
  serviceName: strata-headless
  replicas: 1
  selector:
    matchLabels:
      app: strata
  template:
    metadata:
      labels:
        app: strata
    spec:
      containers:
        - name: strata
          image: ghcr.io/strata-db/strata:latest
          args:
            - run
            - --data-dir=/data
            - --listen=0.0.0.0:3379
            - --s3-bucket=$(S3_BUCKET)
            - --s3-prefix=$(S3_PREFIX)
            - --metrics-addr=0.0.0.0:9090
            - --log-level=info
          env:
            - name: S3_BUCKET
              valueFrom:
                configMapKeyRef:
                  name: strata-config
                  key: s3_bucket
            - name: S3_PREFIX
              valueFrom:
                configMapKeyRef:
                  name: strata-config
                  key: s3_prefix
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: strata-s3-credentials
                  key: AWS_ACCESS_KEY_ID
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: strata-s3-credentials
                  key: AWS_SECRET_ACCESS_KEY
          ports:
            - name: etcd
              containerPort: 3379
            - name: metrics
              containerPort: 9090
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9090
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 9090
            initialDelaySeconds: 3
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              cpu: 250m
              memory: 512Mi
            limits:
              memory: 2Gi
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 20Gi
---
apiVersion: v1
kind: Service
metadata:
  name: strata
spec:
  selector:
    app: strata
  ports:
    - name: etcd
      port: 3379
      targetPort: 3379
```

---

### Multi-node cluster

A 3-node cluster requires:
1. A `StatefulSet` with `replicas: 3`.
2. A **headless** `Service` for peer-to-peer gRPC (stable DNS names: `strata-0.strata-headless`, etc.).
3. A regular `Service` for client access.
4. Each pod configured with `--peer-listen` and `--advertise-peer` using its own stable DNS name.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: strata
spec:
  serviceName: strata-headless
  replicas: 3
  podManagementPolicy: Parallel   # start all pods at once; they self-elect
  selector:
    matchLabels:
      app: strata
  template:
    metadata:
      labels:
        app: strata
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
                labelSelector:
                  matchLabels:
                    app: strata
      containers:
        - name: strata
          image: ghcr.io/strata-db/strata:latest
          args:
            - run
            - --data-dir=/data
            - --listen=0.0.0.0:3379
            - --peer-listen=0.0.0.0:3380
            # POD_NAME is injected below; stable DNS: <pod>.strata-headless.<ns>.svc.cluster.local
            - --advertise-peer=$(POD_NAME).strata-headless.$(NAMESPACE).svc.cluster.local:3380
            - --node-id=$(POD_NAME)
            - --s3-bucket=$(S3_BUCKET)
            - --s3-prefix=$(S3_PREFIX)
            - --metrics-addr=0.0.0.0:9090
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: S3_BUCKET
              valueFrom:
                configMapKeyRef:
                  name: strata-config
                  key: s3_bucket
            - name: S3_PREFIX
              valueFrom:
                configMapKeyRef:
                  name: strata-config
                  key: s3_prefix
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: strata-s3-credentials
                  key: AWS_ACCESS_KEY_ID
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: strata-s3-credentials
                  key: AWS_SECRET_ACCESS_KEY
          ports:
            - name: etcd
              containerPort: 3379
            - name: peer
              containerPort: 3380
            - name: metrics
              containerPort: 9090
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9090
            initialDelaySeconds: 10
            periodSeconds: 15
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /readyz
              port: 9090
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              cpu: 500m
              memory: 1Gi
            limits:
              memory: 4Gi
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 50Gi
---
# Headless service — stable DNS for peer gRPC
apiVersion: v1
kind: Service
metadata:
  name: strata-headless
spec:
  clusterIP: None
  selector:
    app: strata
  ports:
    - name: peer
      port: 3380
      targetPort: 3380
---
# Client service
apiVersion: v1
kind: Service
metadata:
  name: strata
spec:
  selector:
    app: strata
  ports:
    - name: etcd
      port: 3379
      targetPort: 3379
```

---

## IAM / S3 permissions

Strata needs the following S3 permissions on the bucket:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::my-bucket",
    "arn:aws:s3:::my-bucket/strata/*"
  ]
}
```

**Recommended:** use EKS IRSA (IAM Roles for Service Accounts) instead of static credentials. Set the `serviceAccountName` in your pod spec and annotate the ServiceAccount with the IAM role ARN.

---

## Ephemeral pods (no PVC)

If you run pods on ephemeral storage (e.g. spot instances), Strata recovers automatically from S3 on every start. Set `WALSyncUpload=true` (the default for single-node mode) to ensure each acknowledged write is on S3 before returning. In cluster mode, WAL uploads are always async; quorum ACK provides the durability guarantee.

```yaml
args:
  - run
  - --data-dir=/tmp/strata   # ephemeral, lost on pod restart
  - --wal-sync-upload=true   # only matters for single-node; ignored by cluster leader
```

> **Note:** without a PVC, startup is slower because every restart restores the latest checkpoint from S3.

---

## Health and readiness probes

| Endpoint | Meaning |
|---|---|
| `GET /healthz` | Node has started and is reachable |
| `GET /readyz` | Node is ready to serve reads |
| `GET /healthz/leader` | Returns 200 only if this pod is the current leader (useful for proxy routing) |

Configure probes with a longer `initialDelaySeconds` on multi-node deployments to allow checkpoint restore and WAL replay to complete before the pod is declared ready.

---

## Rolling upgrade

The StatefulSet controller replaces pods one at a time (`updateStrategy: RollingUpdate`). With 3 replicas, quorum (2 nodes) is maintained throughout.

1. Strata detects when its pod is being replaced (graceful termination signal).
2. If the replaced pod is the leader, it broadcasts a shutdown signal to followers before exiting, triggering immediate re-election (~12 ms failover).
3. The new pod starts, restores state from S3 or PVC, and joins the cluster.

No manual steps are required. See [Operations — Rolling upgrade](operations.md#rolling-upgrade) for the manual procedure if needed.

---

## Prometheus monitoring

Expose a `ServiceMonitor` if using the Prometheus Operator:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: strata
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: strata
  endpoints:
    - port: metrics
      interval: 30s
  namespaceSelector:
    matchNames:
      - default
```

Key metrics to alert on: see [Operations — Alerting](operations.md#alerting).

---

## See also

- [Operations guide](operations.md) — full cluster setup, TLS, auth, rolling upgrade
- [Backup and restore](backup-restore.md) — checkpoint restore, branching
- [Consistency model](consistency.md) — durability guarantees and read modes
