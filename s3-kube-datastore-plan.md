
# S3-Backed Kubernetes Datastore Implementation Plan

## Overview
This document describes the implementation plan for a leader-based key-value datastore
designed to replace etcd for Kubernetes (via kine), using WAL, checkpoints, and object storage
(e.g., S3) for durability and recovery.

Goals:
- No etcd cluster management
- Stateless control plane nodes
- Scale from 1 → 3 nodes → 1 easily
- Embedded / sidecar deployment with kube-apiserver
- Reasonable S3 operation costs
- Kine-compatible backend

---

## Architecture Summary

Leader:
- Persistent DB (maybe use Pebble/RocksDB/Badger)
- WAL writer
- Streams WAL to followers
- Flushes WAL to object storage periodically
- Creates checkpoints periodically

Followers:
- probably In-memory db (maybe again Pebble/RocksDB/Badger)
- Receive WAL stream from leader
- Apply WAL
- Serve reads and watches
- Can become leader if needed

Object Storage:
- WAL archive
- WAL segments
- Checkpoints
- Leader lock

---

## Storage Layout

```
/leader-lock
/wal/{term}/{seq}
/segments/{term}/{segment-id}
/checkpoint/{term}/{revision}
/manifest/latest
```

---

## Core Components

### 1. Leader Election
Use object storage lock with TTL.

Leader lock object contains:
- node_id
- term
- lease_expiry
- last_revision

Leader must renew lease periodically.

If lease expires → new leader election.

---

### 2. WAL System
Each write:
1. Leader assigns revision
2. Append to WAL
3. fsync WAL
4. Stream to followers
5. Wait for follower ACK (optional but recommended)
6. Apply to Pebble
7. Respond to client

WAL flush to object storage:
- Every 5–10 seconds OR
- Every 50 MB

---

### 3. Revision Model
Revision must be global and monotonic.

Recommended:
```
revision = term << 32 | sequence
```

Or simply:
```
revision = global WAL sequence
```

Followers must never generate revisions.

---

### 4. Checkpoints
Checkpoint interval:
- Every 10–15 minutes OR
- Every N WAL entries

Checkpoint contains:
- Full Pebble snapshot
- Last revision
- Term
- WAL position

Recovery always starts from latest checkpoint.

---

### 5. Recovery Procedure
On startup:

1. Download latest checkpoint
2. Restore Pebble
3. Replay WAL from object storage
4. Connect to leader
5. Stream missing WAL
6. Become follower

---

### 6. Read Path
Follower reads allowed if:
```
follower_revision >= requested_revision
```

Otherwise:
- Wait until WAL applied
- Timeout after configurable delay

This provides near-linearizable reads.

---

### 7. Watch Implementation
Watchers subscribe from revision N.

Follower:
- If revision already applied → stream events
- If not → wait for WAL apply
- Events come from WAL entries

Watch events must be ordered by revision.

---

### 8. Compaction
Compaction happens via checkpoints.

Process:
1. Create checkpoint
2. Upload checkpoint
3. Delete old WAL segments before checkpoint revision
4. Update manifest

---

### 9. Scaling Nodes
When scaling up:
1. New node downloads checkpoint
2. Replays WAL
3. Connects to leader stream
4. Becomes follower

When scaling down:
- Nothing required

Cluster membership is implicit.

---

### 10. Durability Rule
A write is considered committed when:
- Leader fsync completed AND
- At least one follower ACK received
OR
- WAL uploaded to object storage

This prevents data loss on leader crash.

---

## Failure Scenarios to Handle

1. Leader crash before WAL upload
2. Leader crash after follower replication
3. Follower lag
4. Split brain leader
5. Corrupted checkpoint
6. WAL gap
7. Network partition
8. Object storage temporary failure
9. No new records - no need for S3 operations, but must still serve reads/watches

Each must be tested.

---

## Implementation Phases

### Phase 1 – Single Node
- Pebble storage
- WAL
- Checkpoints
- Recovery from checkpoint + WAL

### Phase 2 – Leader + Followers
- WAL streaming
- Follower apply
- Read from followers

### Phase 3 – Leader Election
- Object storage lock
- Lease renewal
- Term handling

### Phase 4 – Object Storage WAL
- WAL flush
- Segments
- Recovery from storage

### Phase 5 – Watches + Revisions
- Revision system
- Watch API
- Compaction

### Phase 6 – Kine Driver Integration
- Implement kine backend driver
- Map revisions
- Map transactions
- Watch support

### Phase 7 – Failure Testing
- Kill leader
- Kill followers
- Restart cluster
- Scale 1 → 3 → 1
- Network partitions

---

## Testing Plan

Test scenarios:
- Leader crash during write
- Leader crash before WAL flush
- Follower restart
- New node join
- Split brain simulation
- Object storage unavailable
- Checkpoint corruption
- WAL replay correctness
- Watch consistency
- Kubernetes API integration test

---

## Success Criteria

System must:
- Never lose committed writes
- Maintain monotonic revisions
- Support Kubernetes watches
- Recover from full cluster restart
- Allow scaling nodes up/down freely
- Keep object storage operations low
- Work embedded with k0s

---

## End Goal
A kine-compatible datastore with:
- Stateless control plane
- Object storage durability
- Leader-based replication
- Checkpoints + WAL recovery
- Easy scaling
