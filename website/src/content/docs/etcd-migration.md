---
title: Migrating from etcd
description: How to move an existing etcd workload to T4 — standalone binary replacement and embedded library adoption.
---

T4 implements the core etcd v3 gRPC API — KV, Watch, Lease, and Auth. In most cases, replacing the etcd binary with `t4 run` and pointing your existing clients at the new endpoint is all that's needed. Some Maintenance and Cluster RPCs are not implemented; see the tables below for the full picture.

---

## Compatibility

T4 supports the following etcd v3 operations:

| etcd operation | T4 support |
|---|---|
| `Range` (Get / List / prefix scan) | Full |
| `Put` | Full |
| `DeleteRange` (single key) | Single-key only — range/prefix deletes return `Unimplemented` |
| `Txn` (compare-and-set) | Subset — single-key MOD==0 (create-if-not-exists) and MOD==X (CAS update/delete); multi-key and bare (no-compare) transactions return `Unimplemented` |
| `Watch` | Full (history replay, cancel) |
| `Compact` | Full |
| `LeaseGrant` / `LeaseKeepAlive` / `LeaseRevoke` / `LeaseTimeToLive` / `LeaseLeases` | Full |
| `AuthEnable` / Users / Roles | Full |
| `MemberList` | Returns single synthetic member |
| `MemberAdd` / `MemberRemove` / `MemberUpdate` / `MemberPromote` | Not supported |
| `Snapshot` / `Status` / `Defragment` / `Hash` / `Alarm` / `MoveLeader` | Not supported |
---

## Migrating the standalone binary

### 1. Export data from etcd

```bash
# Snapshot the existing etcd cluster.
etcdctl --endpoints=http://etcd:2379 snapshot save etcd-snapshot.db
```

Alternatively, iterate and re-write all keys after cutover — suitable for small datasets.

### 2. Start T4

```bash
# Single node with S3
t4 run \
  --data-dir /var/lib/t4 \
  --listen 0.0.0.0:3379 \
  --s3-bucket my-bucket \
  --s3-prefix t4/
```

### 3. Import data

T4 does not support etcd snapshot restore directly. Replay keys using `etcdctl` or a migration script:

```bash
# Export all keys from etcd as key-value pairs
etcdctl --endpoints=http://etcd:2379 get / --prefix --print-value-only=false \
  | awk 'NR%2==1{key=$0} NR%2==0{print key, $0}' > keys.txt

# Write them to T4
while read -r key value; do
  etcdctl --endpoints=http://t4:3379 put "$key" "$value"
done < keys.txt
```

For large datasets, write a short Go program using the etcd client library to stream and replay:

```go
import (
    clientv3 "go.etcd.io/etcd/client/v3"
)

func migrate(ctx context.Context, src, dst *clientv3.Client) error {
    resp, err := src.Get(ctx, "/", clientv3.WithPrefix())
    if err != nil {
        return err
    }
    for _, kv := range resp.Kvs {
        if _, err := dst.Put(ctx, string(kv.Key), string(kv.Value)); err != nil {
            return err
        }
    }
    return nil
}
```

### 4. Update client endpoints

Change your application's etcd endpoint from `http://etcd:2379` to `http://t4:3379`. No client code changes needed — the etcd v3 Go client, Java client, Python client, and `etcdctl` all work against T4 unchanged.

---

## Migrating embedded etcd (e.g. k3s / kine)

If you're using etcd embedded in Kubernetes (k3s, k0s) via kine or a direct etcd embed, consider switching to T4's standalone binary as an etcd-compatible backend:

```bash
# k3s example: point the datastore at T4
k3s server --datastore-endpoint=http://t4:3379
```

Or deploy T4 on the cluster itself and point the control plane at its ClusterIP Service.

---

## Adopting the embedded library

If you're currently running the etcd server as a sidecar and connecting via the etcd Go client, you can replace both with the embedded T4 library — eliminating the sidecar process entirely.

### Before (etcd sidecar + etcd client)

```go
// Connecting to a separate etcd sidecar process
cli, err := clientv3.New(clientv3.Config{
    Endpoints: []string{"localhost:2379"},
})

resp, err := cli.Get(ctx, "/config/timeout")
value := string(resp.Kvs[0].Value)

_, err = cli.Put(ctx, "/config/timeout", "30s")
```

### After (embedded T4)

```go
import "github.com/t4db/t4"

// Embedded — no separate process
node, err := t4.Open(t4.Config{
    DataDir:     "/var/lib/myapp/t4",
    ObjectStore: s3Store, // same S3 durability you had before
})

kv, err := node.Get("/config/timeout")
value := string(kv.Value)

_, err = node.Put(ctx, "/config/timeout", []byte("30s"), 0)
```

Key API differences from the etcd v3 Go client:

| etcd client | T4 embedded |
|---|---|
| `cli.Get(ctx, key)` | `node.Get(key)` (no ctx; reads are local) |
| `cli.Get(ctx, prefix, WithPrefix())` | `node.List(prefix)` |
| `cli.Put(ctx, key, value)` | `node.Put(ctx, key, []byte(value), 0)` |
| `cli.Delete(ctx, key)` | `node.Delete(ctx, key)` |
| `cli.Watch(ctx, prefix, WithPrefix())` | `node.Watch(ctx, prefix, 0)` |
| `cli.Txn(ctx).If(...).Then(Put).Commit()` | `node.Create` / `node.Update` / `node.DeleteIfRevision` |
| `cli.Grant(ctx, ttl)` + lease ID on Put | `node.Put(ctx, key, value, leaseID)` — obtain a lease ID from `LeaseGrant` via the etcd gRPC API, or manage leases directly through the embedded server |

---

## Feature gaps to plan for

| etcd feature | T4 behaviour | Workaround |
|---|---|---|
| `DeleteRange` with prefix/range end | Not supported — returns `Unimplemented` | Delete keys individually or iterate with `List` + `Delete` |
| Multi-key / no-compare transactions | Not supported — returns `Unimplemented` | Decompose into individual `Create` / `Update` / `Delete` calls |
| Maintenance RPCs (Status, Defrag, Hash, Alarm, MoveLeader) | Not supported | Not needed for standard application clients |
| `etcdctl snapshot restore` | Not supported | Use `t4 branch fork` for point-in-time copies |
| `MemberAdd` / `MemberRemove` / `MemberUpdate` / `MemberPromote` | Not supported | Not needed for standard clients |
| etcd v2 API | Not supported | Migrate to v3 first |
| gRPC gateway (HTTP/JSON) | Not included | Use a gRPC proxy (e.g. grpc-gateway) in front |
