---
title: Security
description: Securing T4 — TLS setup, mTLS between peers, client authentication, and RBAC.
---

## Overview

T4 has two independently configurable TLS surfaces:

| Surface | Flag prefix | What it protects |
|---|---|---|
| **Client TLS** | `--client-tls-*` | etcd gRPC port (3379) — traffic between your application and T4 |
| **Peer mTLS** | `--peer-tls-*` | WAL replication port (3380) — traffic between T4 nodes |

Both use standard PEM-encoded certificates. You can enable either or both independently.

---

## Generating test certificates

For development, use `openssl` to create a self-signed CA and certificates:

```bash
# CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=t4-ca"

# Server key and CSR
openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr \
  -subj "/CN=t4-server"

# Sign with CA, include SANs
openssl x509 -req -days 3650 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

echo "Files: ca.crt, server.crt, server.key"
```

For mTLS (peer-to-peer), generate a second cert for each node, or use a single shared cert if all nodes are behind the same CA.

---

## Client TLS

### Server-only TLS (encryption, no client cert required)

Clients connect with TLS but aren't required to present a certificate. Use this when your clients support TLS but not mTLS.

```bash
t4 run \
  --data-dir /var/lib/t4 \
  --listen 0.0.0.0:3379 \
  --client-tls-cert /etc/t4/tls/server.crt \
  --client-tls-key  /etc/t4/tls/server.key
```

Clients:

```bash
etcdctl --endpoints=https://t4:3379 \
        --cacert /etc/t4/tls/ca.crt \
        put /hello world
```

Go client:

```go
tlsCfg, err := tlsconfig.ClientConfig(tlsconfig.Options{
    CAFile: "/etc/t4/tls/ca.crt",
})
cli, err := clientv3.New(clientv3.Config{
    Endpoints: []string{"https://t4:3379"},
    TLS:       tlsCfg,
})
```

### Mutual TLS (mTLS — client cert required)

Add `--client-tls-ca` to require clients to present a certificate signed by the given CA:

```bash
t4 run \
  --data-dir /var/lib/t4 \
  --listen 0.0.0.0:3379 \
  --client-tls-cert /etc/t4/tls/server.crt \
  --client-tls-key  /etc/t4/tls/server.key \
  --client-tls-ca   /etc/t4/tls/ca.crt
```

Generate a client certificate:

```bash
openssl genrsa -out client.key 4096
openssl req -new -key client.key -out client.csr -subj "/CN=my-app"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt
```

Connect with client cert:

```bash
etcdctl --endpoints=https://t4:3379 \
        --cacert  /etc/t4/tls/ca.crt \
        --cert    /etc/t4/tls/client.crt \
        --key     /etc/t4/tls/client.key \
        put /hello world
```

### Embedded library

Pass `grpc.DialOption` credentials to the etcd client if you're using the etcd-compatible interface, or configure TLS on the gRPC connection directly. For the embedded `*t4.Node`, TLS applies only to the peer port — client reads go directly in-process without any network.

---

## Peer mTLS

Peer mTLS encrypts and authenticates WAL replication streams between nodes. All nodes must use the same CA.

```bash
t4 run \
  --data-dir       /var/lib/t4 \
  --peer-listen    0.0.0.0:3380 \
  --advertise-peer node-a.internal:3380 \
  --peer-tls-ca    /etc/t4/tls/ca.crt \
  --peer-tls-cert  /etc/t4/tls/node.crt \
  --peer-tls-key   /etc/t4/tls/node.key
```

The same cert/key pair can be used on all nodes if the cert includes all peer DNS names in its SANs:

```bash
openssl x509 -req -days 3650 -in node.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out node.crt \
  -extfile <(printf "subjectAltName=DNS:node-a.internal,DNS:node-b.internal,DNS:node-c.internal")
```

Or use separate certs per node — all must be signed by the same CA.

### Embedded library

```go
import "google.golang.org/grpc/credentials"

serverCreds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
clientCreds, err := credentials.NewClientTLSFromFile(caFile, "")

node, err := t4.Open(t4.Config{
    PeerServerTLS: serverCreds,
    PeerClientTLS: clientCreds,
})
```

For mTLS with client cert verification, build `tls.Config` manually:

```go
cert, _ := tls.LoadX509KeyPair(certFile, keyFile)
caCert, _ := os.ReadFile(caFile)
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(caCert)

serverTLS := &tls.Config{
    Certificates: []tls.Certificate{cert},
    ClientCAs:    pool,
    ClientAuth:   tls.RequireAndVerifyClientCert,
}
clientTLS := &tls.Config{
    Certificates: []tls.Certificate{cert},
    RootCAs:      pool,
}

node, err := t4.Open(t4.Config{
    PeerServerTLS: credentials.NewTLS(serverTLS),
    PeerClientTLS: credentials.NewTLS(clientTLS),
})
```

---

## cert-manager (Kubernetes)

Generate peer certificates automatically with cert-manager:

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: t4-ca-issuer
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: t4-ca
  namespace: default
spec:
  isCA: true
  secretName: t4-ca-secret
  commonName: t4-ca
  issuerRef:
    name: t4-ca-issuer
    kind: ClusterIssuer
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: t4-issuer
  namespace: default
spec:
  ca:
    secretName: t4-ca-secret
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: t4-peer-tls
  namespace: default
spec:
  secretName: t4-peer-tls
  issuerRef:
    name: t4-issuer
  dnsNames:
    - t4-0.t4-headless.default.svc.cluster.local
    - t4-1.t4-headless.default.svc.cluster.local
    - t4-2.t4-headless.default.svc.cluster.local
    - t4-headless.default.svc.cluster.local
  usages:
    - server auth
    - client auth
  duration: 8760h    # 1 year
  renewBefore: 720h  # renew 30 days before expiry
```

Then pass the secret to the Helm chart:

```bash
helm install t4 oci://ghcr.io/t4db/charts/t4 \
  --set tls.peer.enabled=true \
  --set tls.peer.secretName=t4-peer-tls
```

---

## Authentication and RBAC

T4 implements the etcd v3 Auth API: username/password auth with bearer tokens and role-based access control.

### Enable auth

Auth requires a `root` user to exist before it can be enabled:

```bash
etcdctl --endpoints=localhost:3379 user add root
# Enter password at prompt

etcdctl --endpoints=localhost:3379 auth enable
```

Once enabled, all KV and Watch requests require authentication.

### Create users and roles

```bash
# Create a read-only role for /config/
etcdctl --endpoints=localhost:3379 --user root:pass \
  role add config-reader

etcdctl --endpoints=localhost:3379 --user root:pass \
  role grant-permission config-reader read /config/ --prefix

# Create a user and assign the role
etcdctl --endpoints=localhost:3379 --user root:pass \
  user add alice

etcdctl --endpoints=localhost:3379 --user root:pass \
  user grant-role alice config-reader
```

### RBAC rules

A request is allowed when the user has at least one role whose permissions cover the key and operation:

| Operation | Required permission |
|---|---|
| `Get` / `List` / `Watch` | `read` |
| `Put` / `Delete` / `Txn` | `write` |

Permission scopes:
- **Exact key**: matches a single key
- **Prefix** (`--prefix`): matches all keys starting with the prefix
- **Open-ended range** (`rangeEnd="\x00"`): matches all keys ≥ the start key

The `root` role bypasses all permission checks.

### Token TTL

Bearer tokens expire after `--token-ttl` seconds (default 300). The etcd Go client handles token refresh automatically when `--user` is provided.

```bash
t4 run ... --auth-enabled --token-ttl 3600
```

---

## S3 bucket security

S3 is used for WAL segments, checkpoints, and the leader lock. The IAM policy for T4's S3 access needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:HeadObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/t4/*"
      ]
    }
  ]
}
```

For leader election, T4 uses conditional PUTs (`If-None-Match`, `If-Match`). These are standard S3 operations and don't require additional permissions.

**Recommendations:**
- Use IRSA / Workload Identity — no static credentials in environment variables or Secrets
- Enable S3 bucket versioning if you use point-in-time restore
- Enable S3 server-side encryption (SSE-S3 or SSE-KMS)
- Restrict bucket access with a bucket policy that denies public access
