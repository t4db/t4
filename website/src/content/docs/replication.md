---
title: Replication to etcd
description: Continuously replicate a T4 database into etcd with identical revisions, so kube-apiserver can switch between them without clients noticing.
---

`t4 replicate run` continuously copies a T4 database into an etcd cluster (or
etcd into T4) so that both are at **identical revisions**: after source
revision R is applied, the target is at revision R with the same keys, values,
create and mod revisions, versions and leases.

Kubernetes uses revisions as `resourceVersion`s, for watches and for optimistic
concurrency. Because the revisions match, kube-apiserver can be switched from
the source to the target, for example to fall back from T4 to etcd, without
clients relisting, losing watches or hitting a wave of conflicts.

## Requirements

- **The source must spend revisions like etcd.** A T4 database created with the
  meta keyspace qualifies, as does etcd. A T4 database created by a release before the meta keyspace
  spends revisions on lease and auth bookkeeping. The replicator stops with an
  error on such a source; see [Upgrade](upgrade#databases-created-with-the-meta-keyspace).
- **The target must be empty** (a fresh etcd cluster or T4 database) or have
  been replicated into before. The replicator keeps its cursor on the target
  under `/__t4_replication/`.
- **The source must still have the history the target needs.** The replicator
  starts from the target's cursor, or from the source's first revision for an
  empty target. If the source compacted that history, replication stops with an
  error. kube-apiserver compacts every 5 minutes by default, so start the
  replicator together with the cluster. Bootstrapping a target from a snapshot
  of an existing database is not available yet.
- **Only the replicator may write to the target.** Protect it with etcd
  authentication so the kube-apiserver credentials cannot write until cutover.

## Running

```bash
t4 replicate run \
  --source-endpoints https://t4:3379 \
  --source-cacert ca.pem --source-cert client.pem --source-key client-key.pem \
  --target-endpoints https://etcd-0:2379,https://etcd-1:2379,https://etcd-2:2379 \
  --target-cacert ca.pem --target-cert replicator.pem --target-key replicator-key.pem
```

Run one replicator per target, for example as a single-replica Deployment. A
second replicator cannot corrupt the target: every write is guarded by the
cursor, and the replicator that loses a race stops. All flags and their
`T4_REPLICATE_*` environment variables are listed in
[Configuration](configuration).

The replicator resumes from the cursor after a restart. It retries transient
errors, such as lost connections, and exits with an error only when an operator
must act.

### Leases

Leases are created on the target with the same IDs. Their TTL there is the
source's TTL plus `--lease-ttl-margin` (default 10m). The replicator keeps
them alive while they exist on the source, and revokes them once the source no
longer has them and their keys are gone. Keys attached to a lease are deleted
on the target when the source deletes them, at the same revision.

The target must never expire a lease before the source does: that would
create a revision the source does not have. If the replicator is down longer
than the margin, target leases can expire and replication stops. Set the
margin above the longest downtime you want to tolerate.

### Transaction limits

Each source revision is applied as one target transaction. A source revision
with more operations than etcd's `--max-txn-ops` (default 128) or larger than
`--max-request-bytes` stops replication with an error that names the limit.
Raise the limits on the target etcd if your workload needs it.

## Monitoring

Metrics are served on `--metrics-addr` (default `0.0.0.0:9091`) at `/metrics`,
with a liveness endpoint at `/healthz`.

| Metric                                  | Meaning                                                              |
|-----------------------------------------|----------------------------------------------------------------------|
| `t4_replicate_applied_revision`         | Source revision the target is at.                                    |
| `t4_replicate_source_revision`          | Latest source revision observed.                                     |
| `t4_replicate_lag_revisions`            | Revisions observed but not yet applied.                              |
| `t4_replicate_apply_duration_seconds`   | Time to apply one source revision.                                   |
| `t4_replicate_target_leases`            | Leases mirrored on the target.                                       |
| `t4_replicate_errors_total{op}`         | Retried errors.                                                      |
| `t4_replicate_halted`                   | 1 when replication stopped on an error that needs an operator.       |

Alert on `t4_replicate_halted`, on a growing lag, and on the process not
running.

## When replication stops

The replicator stops instead of letting the target silently diverge. Each
error message names the cause:

| Cause                                                   | What to do                                                   |
|---------------------------------------------------------|--------------------------------------------------------------|
| The target changed behind the replicator's back         | Rebuild the target from an empty cluster.                    |
| The source compacted history the target still needs     | Rebuild the target from an empty cluster.                    |
| The source skipped revisions                            | The source was created before the meta keyspace; it cannot be replicated revision-exactly. |
| The target has data but no cursor                       | Replicate into an empty target.                              |
| A lease expired on the target                           | Raise `--lease-ttl-margin`; rebuild the target.              |
| A source revision exceeds the target's transaction limits | Raise `--max-txn-ops` / `--max-request-bytes` on the target and restart. |

A write that bypasses the replicator is detected when the next source revision
is applied, so the target is already inconsistent at that point. Authentication
on the target prevents this.

## Switching kube-apiserver to the target

1. Check that replication is healthy: `t4_replicate_halted` is 0 and the lag is
   near 0.
2. Stop writes to the source, for example by scaling kube-apiserver down.
3. Wait until `t4_replicate_applied_revision` equals the source's revision, as
   reported by `etcdctl endpoint status` against the source.
4. Stop the replicator.
5. Give the kube-apiserver credentials access to the target.
6. Point kube-apiserver's `--etcd-servers` at the target and start it.

Watches resume from the resourceVersions clients already have. The
`/__t4_replication/` keys stay on the target; kube-apiserver does not read
them.

Moving back replicates in the other direction, into a new, empty database.
That needs the new source's history from its first revision, which a cluster
that has been running for a while no longer has, so in practice it needs
bootstrapping from a snapshot, which is not available yet.
