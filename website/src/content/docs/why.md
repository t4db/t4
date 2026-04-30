---
title: Why T4 Exists
description: The motivation behind building T4 as a key-value database on object storage.
sidebar:
  order: 1
---

Infrastructure systems often need a small, reliable place to keep control-plane state: configuration, service metadata, locks, leases, revision history, and change notifications.

T4 started as an attempt to simplify Kubernetes storage. Kubernetes depends on etcd for its control-plane state, but etcd also brings a full consensus cluster, membership management, snapshots, restores, and operational habits that are not always pleasant in small clusters, edge environments, or experiments. Kubernetes has also moved from a world of big, carefully managed control planes on large servers toward simpler, more disposable hosted control planes (HCPs), where the storage layer should be easier to replace and recover. The initial question was simple: what would Kubernetes-style storage look like if recovery came from object storage instead?

etcd is the standard answer, and it is excellent when you want the full Raft cluster model. But operating a consensus cluster is not always the shape you want. For small platforms, edge deployments, development environments, ephemeral nodes, and systems that already trust object storage, Raft membership can be more machinery than the state store deserves.

T4 explores a different tradeoff: keep the familiar key-value and watch primitives, store hot data locally, and use object storage as the durable recovery layer. Nodes can disappear, lose their disks, or be replaced; the database can rebuild from WAL segments and checkpoints in S3-compatible storage.

Once that model existed, it opened the door to features that are hard to make natural in traditional control-plane storage: copy-free database branches, cheap recovery drills, disposable test copies, and point-in-time migration experiments that reuse existing object-store files instead of cloning whole databases.

## The problem

Many infrastructure projects need a database that is smaller than a general-purpose SQL system but stronger than a plain local file:

- Configuration needs prefix reads, conditional updates, and watch streams.
- Schedulers and control planes need leases, locks, and monotonically increasing revisions.
- Operators need recovery after node loss without hand-copying snapshots.
- Test and migration workflows benefit from point-in-time copies of live state.

The usual answer is to run a separate coordination database. That works, but it also adds an operational object with its own cluster membership, snapshots, restore procedure, failure modes, and performance profile.

## The object-storage bet

Object storage has become the durable substrate many systems already depend on. It is cheap, widely available, regionally durable, and operationally boring in a good way.

T4 uses that substrate directly:

- WAL segments record every change.
- Checkpoints provide fast recovery points.
- Content-addressed SST files avoid re-uploading unchanged data.
- Branches can share existing checkpoint files instead of copying full databases.
- Leader election uses object-store conditional writes instead of a separate coordination service.

The result is not "etcd, but implemented differently." It is a different durability model for workloads where object storage is already the recovery boundary.

## What T4 optimizes for

T4 is built for:

- Durable infrastructure state.
- Simplifying Kubernetes-style control-plane storage.
- Simple replacement of failed or ephemeral nodes.
- Local reads from an embedded or standalone process.
- Existing etcd v3 clients where compatibility matters.
- Copy-free branches for recovery drills, migrations, and tests.

It is not trying to replace every etcd deployment. Use etcd when you want the most mature operational ecosystem, full cluster-management APIs, or the lowest possible latency for small sequential write workloads.

T4 is for the cases where the more interesting question is: if object storage is already the durable source of truth, how much cluster machinery can the database avoid?
