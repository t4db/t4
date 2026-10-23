# Group leadership: one leader for many databases

Status: proposal.

`t4 run --db a --db b ...` serves several independent databases from one
process (single node only, so far). This document proposes how such
processes form a cluster: **one leader per group of processes, leading every
database of the group**, instead of one leader election per database.

## Why one leader per group

Every database today has its own leader lock under its S3 prefix. In a
cluster serving N databases per process, that means:

- **N lock renewals.** In steady state each lock costs 1 GET and 1
  conditional PUT every `LeaderWatchInterval` (20 s): about 0.023 USD per
  database per day at AWS prices, so ~70 USD a month per 100 databases and
  ~700 per 1000.
- **N sets of liveness.** Heartbeats, silence detection and slow/fast lease
  modes run per database, per follower.
- **N independent failovers** when one process dies, and no single answer to
  "which process leads".

Systems that run many consensus groups per process stop paying for liveness
per group:

- **LiteFS** holds one lease per cluster, and the primary serves every
  database. Users asked for a primary per database; the workaround is
  running a separate cluster per primary.
- **CockroachDB** has a leaseholder per range, but liveness per node:
  epoch-based leases name the node's liveness epoch, so liveness traffic grows
  with nodes, not ranges. Its newer leader leases rest on store-level
  liveness for the same reason.
- **TiKV** has a leader per region and stops idle regions from sending
  heartbeats, because heartbeats from many regions overloaded the process
  handling them.

Per-database leadership buys the ability to spread leaders across processes.
T4 would not use it at first: the first process to start wins every election.
A group leader keeps T4's single-leader model and the takeover-fence
reasoning it was tested with. If spreading leaders ever matters, the path is
CockroachDB's: per-process liveness, with per-database leadership records that
point to it. The per-database fence this design introduces is the part such a
move would keep.

## What a leader lock does today

A lock record (`internal/election`) carries two kinds of state:

- **Leadership and lease:** `NodeID`, `Term`, `LeaderAddr`, and the renewal
  times `RenewedNano`/`ValidUntilNano` (with `LastSeenNano` for earlier
  releases). The leader serves while its lease holds: from the start of a
  lock write until `leaseSafetyMargin` before anyone may take over
  (`lease.go`). It renews slowly while every follower is hearing it, and fast
  otherwise.
- **Election fence**, about the database's log: `CommittedSeq` and
  `CommittedTerm` (`CommittedRev` for earlier releases), and
  `ObjectStoreComplete`. A candidate whose leader-known position is below the
  fence may lack an acknowledged write and must not take over. It may catch
  up from object storage first, but only while the lock says object storage
  holds every acknowledged write (`ObjectStoreComplete`, or the lock was
  released). The leader moves the fence with a lock write before
  acknowledging a write that a node not blocked by the fence may lack: a
  follower that has not acknowledged it, one that left, or a node never heard
  from (`fenceNeeded`, `awaitFence`).

The first kind is about the process. The second is about one log. A group lock
keeps one copy of the first and one of the second per database.

## Design

### The group lock

One lock at `<s3-prefix>/leader-lock`. With `--db`, every database lives below
`<s3-prefix>/dbs/<name>/`, so the root is free.

```json
{
  "node_id": "node-a", "term": 7, "leader_addr": "10.0.0.1:3380",
  "renewed_nano": ..., "valid_until_nano": ..., "last_seen_nano": ...,
  "committed_rev": 9223372036854775807,
  "committed_seq": 9223372036854775807,
  "committed_term": 18446744073709551615,
  "databases": {
    "tenant-a": {"committed_seq": 120, "committed_term": 7, "committed_rev": 98, "object_store_complete": true},
    "tenant-b": {"committed_seq": 45,  "committed_term": 6, "committed_rev": 40}
  }
}
```

- **Lease fields** are exactly today's, and so is their timing: renewals, slow
  and fast modes, `mayTakeOver`, `checkLease`.
- **`databases`** holds each database's fence and object-storage flag. It's
  additive JSON.
- **The top-level fence is set to the maximum.** `Blocks` then refuses every
  candidate that judges the lock by the top-level fence, which includes every
  existing release and any single-database node pointed at the group prefix
  by mistake. A group lock can be taken over only by a group member.
- **Terms are group terms.** Every database's WAL segments carry the group's
  term. Terms stay monotonic per database, because a database's term only
  ever comes from the group.

### Writing the lock

The leader is the only writer, so one lock write covers every pending fence
move. Each database keeps its own fence state (`fenceSeq`, the want fields,
`awaitFence`), and the group's watch loop writes all of them together. A
follower that leaves usually moves every database's fence at once, so that
costs one lock write, as it does today for one database.

The lock grows by roughly 100 bytes per database, so 1000 databases is about
100 KB per renewal. That's acceptable: renewals are rare (every 20 s in slow
mode).

### Liveness

Followers keep one WAL stream per database. Heartbeats still run per stream,
which keeps `peer.Server`'s logic unchanged. Collapsing them into one
heartbeat per process pair is an optimization for later.

- **Leader, slow mode:** the leader stays in slow mode only while every
  database's peer server is in slow mode (the AND of `SlowSafe`). Any silent
  stream sends the whole group to fast mode.
- **Follower, last heard:** a follower's "last heard the leader of term T" is
  the latest heartbeat of term T on any of its streams. A later time only
  delays a takeover, so this is the conservative choice. Combined with the
  leader's AND, the existing argument for `knownTakeoverDelay` holds: when a
  follower stops hearing the leader, each of its streams goes silent, and the
  leader leaves slow mode within `silenceTimeout` of the first silent stream.

### Takeover

A candidate takes over the group only if, for every database in the lock:

1. the database's fence does not block the candidate's leader-known position
   for it (a database the candidate doesn't have counts as position zero); or
2. the fence blocks it, but that database's `object_store_complete` is set (or
   the lock was released), and catching up from object storage (checkpoint,
   then WAL) brings it past the fence.

That's today's rule applied to each database. The takeover itself stays one
conditional PUT of the whole lock: the new term, plus the candidate's
position as every database's fence.

**Becoming leader** then means doing today's `becomeLeader` for each database
under the new term: open the WAL, start the peer server, register it with the
shared peer port. If any database fails to become leader, the process
relinquishes the group lock, keeping every fence, and steps down. A group
whose leader can't serve some database is worse than a fast failover to a
process that can.

### Peer transport

There's one peer listener per process (`--peer-listen`). It routes `Follow`,
`Forward` and `GoodBye` to the peer server of the database named in the call's
gRPC metadata. A process that doesn't currently lead answers Unavailable, and
the follower re-reads the group lock, as it does after a failover. Followers
add the database name to every call. Sharing one gRPC connection per process
pair is an optimization for later.

### Membership

Every process in a group must serve the same databases (static `--db` lists).

- **A database missing from the lock** (newly added to the list): the leader
  adds it at its next lock write, with a zero fence, which moves on the
  database's first acknowledged write. A process whose list lacks it can't
  pass a non-zero fence for it, so it can't lead.
- **A database in the lock that a process doesn't serve:** at startup that
  process refuses to join the group, with an error naming the database.
  Removing a database from a group needs an explicit operation, which is out
  of scope here.

### Moving a database out of the group

A database below `dbs/<name>/` has no lock of its own. To keep a standalone
cluster (`t4 run --peer-listen ... --s3-prefix <prefix>/dbs/<name>`) from
serving it while the group still leads it, the group leader writes a guard
lock there. It's held by node ID `group:<prefix>`, never renewed, and has the
maximum fence. A standalone node finds the lock held, can't follow the
address it names, and can never pass its fence, so it never serves. Removing
a database from the group (out of scope) deletes the guard lock.

The guard does not stop a single-node `t4 run` (no `--peer-listen`): single-node
mode never reads the leader lock. That's already true today for any prefix a
cluster uses. Making single-node mode refuse a prefix with a live lock would
close it for every database, and is a separate change.

Once the database is standalone, its node takes a term above the one in the
checkpoint it restores (`TryAcquire` uses that as its floor), so terms stay
monotonic.

### Failure isolation

Today a database whose node stops (`Node.Done`) makes `t4 run` exit, so that
it restarts as a follower. In a group, that would fail over every database.
Instead, a member that stops is reopened in place as a follower of the
current term. If the process leads, it relinquishes the group lock first.
That costs one failover for the group, which is what one stopped database
costs today.

## What stays the same

- A process without `--db` is a group of one and writes today's lock format,
  at today's key, with no `databases` field. Rolling upgrades, every existing
  test, and Jepsen are unaffected.
- The on-S3 layout of each database (WAL, checkpoints, manifest) is
  unchanged. Only its lock moves to the group.

## Code structure

`Node` currently owns both kinds of state. The change splits it:

- **group** (new): the election lock, lease (`extendLease`, `leaseDeadline`,
  `checkLease`), mode decision, watch loop, takeover decision, term, and the
  shared peer listener.
- **member** (`Node`): store, WAL, commit loop, its database's peer server,
  fence state, and leader-known and committed positions.

`checkLease` and anything else that asks "may I act as leader" asks the
group.

## Plan

1. **Split `Node` with a group of one.** A pure refactor: same lock format,
   same behavior. The full suite, etcddiff, robustness and Jepsen must pass
   unchanged.
2. **Group lock format and per-database fences**, with unit tests for
   `Blocks` across databases, the maximum top-level fence against
   single-database candidates, and the guard lock.
3. **Groups of N databases on one node**, replacing what #342's `--db` does
   underneath.
4. **Cluster mode:** the shared peer port, and `--peer-listen` with `--db`.
   In-process tests of 3 processes × 2 databases:
   - replication and forwarding per database;
   - killing the leader moves every database;
   - a follower lagging on one database blocks takeover until it catches up
     from object storage, and stays blocked without `object_store_complete`;
   - mismatched `--db` lists are refused.
5. **e2e with the binary on S3**, and a Jepsen workload with several
   databases.

## Open questions

- **Collapsing heartbeats and connections** to one per process pair:
  needed past roughly a few hundred databases?
- **Per-database WAL fsyncs.** Every database has its own WAL and fsyncs on
  the leader's disk. That cost doesn't shrink with group leadership; measure
  it early (step 3).
- **Operations on a single database** (remove from group, restore one
  tenant in place) are separate designs.
- **`t4 status --s3-prefix <prefix>/dbs/<name>`** should report the group
  lock instead of the guard lock.
