---
title: Upgrade and Downgrade
description: What operators need to know to move a running T4 deployment from one v1.x release to another, including fail-closed cases and downgrade safety checks.
---

What operators need to know to move a running T4 deployment from one v1.x release to another. For the underlying compatibility rules see the [v1 Compatibility Contract](/v1-compatibility/).

## What v1.x guarantees

- A newer v1.x binary can open a data directory and an object-store prefix written by any older v1.x binary.
- Newer v1.x can also add fields, flags, default values, exported types, and metrics — but never remove or change the meaning of existing ones without a major version bump.
- Default values listed in the [defaults table](/v1-compatibility/#configuration) do not change across v1.x unless explicitly documented as tuning guidance.

## What fails closed

These are the failure modes the binary refuses rather than silently corrupting state. They protect against an unsupervised downgrade onto data the older binary cannot interpret. None of them is recoverable without action; the binary exits with a clear error and the cluster's other nodes (if any) keep serving.

| Trigger | Behaviour |
|---|---|
| Checkpoint `format_version` newer than this binary understands | Startup refuses to restore. Operator must roll the binary forward. |
| WAL frame magic / version newer than this binary understands | Startup refuses to replay. Same remedy. |
| Follower on an earlier release connecting to the leader of a database created with the meta keyspace | The leader refuses the stream with `wal_format_unsupported`. Upgrade the follower. |
| Follower forwarding a write on an earlier peer forward wire version | The leader refuses it with `forward_wire_version_unsupported` rather than committing a key whose bytes the sender may already have replaced. Keyless forwards (read index, compaction, lease renewal) are unaffected. Upgrade the follower. |
| Follower forwarding a transaction with nested ops or a branch range delete to a leader from before they existed | The leader refuses it with `forward_wire_version_unsupported` rather than applying the transaction with a branch missing, or deleting one key where a range was asked for. Only those transactions are refused; the follower's other writes still go through. Upgrade the leader. |
| RBAC permission range that is not valid UTF-8, read by a release from before those were stored intact | The auth store refuses to load the role rather than enforce a different range: such a range is stored as a `{"base64": "..."}` object, and an earlier binary's string decoder rejects it. Roll the binary forward. Ranges that are valid UTF-8 — all of them in practice — are stored as before and read by either. |
| WAL entry with an op code this binary does not know (local replay, S3 replay, or the leader's peer stream) | Replay or apply is refused. A follower stops replicating and logs an error instead of retrying or attempting a leader takeover; it does not apply the entry. Same remedy. |
| Object store does not enforce the conditional PUTs leader election needs, and `--peer-listen` is set | Startup refuses with `ErrConditionalWritesIgnored`. Use a store that enforces them — on GCS that means `--gcs-bucket` rather than the S3-compatible endpoint — or run a single node. A single node starts with a warning. |
| `--gateway-addr` names an address T4 cannot listen on | Startup refuses with the listen error. An address T4 derived on its own — the port below `--listen` when client TLS is on — is deliberately not fail-closed: T4 warns, falls back to sharing `--listen`, and keeps serving. |
| `manifest/latest` references a checkpoint missing referenced SSTs | Startup error. Recoverable from an earlier checkpoint via `t4 restore`. |
| Branch registry entry points at a checkpoint older than the safe `format_version` | GC refuses to reclaim. Inspect with `t4 inspect`. |

## Rolling upgrade (cluster)

For multi-node clusters, upgrade one node at a time. The leader stays available throughout; the follower being upgraded re-syncs from S3 on restart.

1. Pick the **oldest** node that is currently a follower (avoid the leader on the first pass).
2. Stop the t4 process. Followers resync automatically on restart, so the data directory does not need to be cleared.
3. Install the new binary and restart with the **same** flags as before.
4. Wait for the follower to catch up — `t4 status` shows `current_revision >= leader_current_revision - tolerable_lag` and the leader's metrics show the follower reconnected.
5. Repeat for the remaining followers.
6. Restart the leader last. A follower wins the takeover election (~6 s; see [Consistency](/consistency/#behaviour-under-network-partition)). Promote it gracefully by signalling the leader with `SIGTERM`; the new leader is the highest-CommittedRev follower.

The cluster keeps accepting writes throughout, modulo the ~6 s window when the leader is being restarted.

### Byte-exact peer forwarding

Upgrade **all followers first, then the leader**. Peer forward wire version 2
preserves arbitrary etcd key bytes: valid UTF-8 retains the legacy JSON string;
invalid UTF-8 uses a base64 object that legacy string decoders reject before
mutation. Ordinary UTF-8 forwarding to a legacy leader continues to work.
Binary-key forwarding fails closed until the leader is upgraded; send such
operations only once every node is upgraded.

A new leader rejects a legacy (version 0) or unknown-version forward with
`FailedPrecondition` and `forward_wire_version_unsupported`, before dispatch,
but only when it carries a key: the leader cannot recover bytes a legacy sender
has already replaced. A forward that carries no key — a read index, a
compaction, a lease renewal — has nothing to corrupt and is served as usual, so
upgrading the leader first blocks a legacy follower's writes while leaving its
linearizable reads working. The leader logs the follower to upgrade, at most
once a minute. This gate is independent of WAL and checkpoint formats, which
remain unchanged.

## Single-node upgrade

1. Stop the t4 process.
2. Install the new binary.
3. Start with the same flags. T4 replays any unsealed WAL entries from the local data directory; if the local directory is gone (ephemeral storage), it restores the latest checkpoint from S3.

## The JSON/HTTP gateway opens on upgrade

This release also serves the etcd v3 JSON/HTTP API, and it is on by default. Nothing changes for a gRPC client, but what
a node listens on does:

- **Plaintext `--listen`** — the same port answers both protocols. A connection's first bytes say which it is, so the
  two are separated before either server sees them and gRPC keeps its own transport and its own tuning. No second port
  to publish, and nothing added to the per-request path.
- **Client TLS** — T4 opens a second listener on the port **below** `--listen` (3378 for the default 3379), under the
  same certificate. Sharing a TLS port would mean splitting per request rather than per connection, which roughly
  doubles gRPC request latency. **That port is new.** A firewall rule, Kubernetes Service or load balancer that forwards
  only the client port will not reach it, and JSON clients get nothing until it is published. The Helm chart publishes it
  as the `gateway` port from this version on; a hand-written manifest needs it added.

A derived port is a convenience rather than a requirement, so it cannot stop a node from booting: if the port below
`--listen` is taken, T4 logs a warning and falls back to sharing `--listen`. The upgrade succeeds either way, but it can
land quietly on the slower arrangement — the startup line to look for is `etcd JSON/HTTP gateway listening` with
`derived=true`. Name the address with `--gateway-addr` to decide for yourself; an address you name is required to work,
and startup fails if it cannot be bound.

Browsers are not let in by any of this. `--gateway-cors-origins` is empty by default, which sends no CORS headers, so a
cross-origin call fails its preflight until an operator opts in. See [JSON/HTTP gateway](/http-gateway/) for the
endpoints, the websocket watch, and why grpc-web cannot work.

To keep exactly the pre-upgrade surface — one port, gRPC only — start with `--enable-grpc-gateway=false`.

## `/readyz` requires a linearizable read

`/readyz` used to return 200 whenever the node had a local revision, which is true from the moment it opens — so it
never failed, not even once the node had stopped. It now returns 200 only when the node can complete a linearizable read
within 2 seconds: a leader must hold its lease, and a follower must reach the leader and apply up to it. This is what
etcd's `/readyz` checks.

A follower cut off from its leader now leaves the Kubernetes Service endpoints instead of accepting requests it cannot
answer. The flip side is that a leader outage longer than the probe's failure budget takes every replica out at once.
Deployments that read with `--read-consistency=serializable` and would rather keep serving possibly stale reads through
that can point the probe at `/readyz?exclude=linearizable_read`, as on etcd. `/healthz`, the liveness endpoint, is
unchanged.

## Transaction branches: nesting and range deletes

`Txn` branches gained two things this release, and both are resolved on the
leader in the same atomic step as the transaction's own conditions:

- **A nested `RequestTxn`**, which etcd clients use for a compare-and-set
  whose fallback is itself a compare-and-set — Patroni's leader refresh is the
  common case. Nesting is capped at 128 levels, the depth etcd's default
  `--max-txn-ops` permits.
- **A range delete**, which removes `[key, range_end)` in the transaction's
  single revision. The keys are found under the write lock, so a key a
  concurrent write adds to the range is removed with the others. Two limits
  follow from that lock: a range matching more than 65535 keys is refused
  rather than resolved while writes wait, and a range must start above the
  reserved `\x00` prefix, since the core resolves it without knowing which
  keys are T4's own. Neither applies to a standalone `DeleteRange`, which
  filters reserved keys itself and commits large ranges in batches.

Only the leader can decide branches, so a follower forwards the whole
transaction. A forwarded transaction carrying either feature is stamped **peer
forward wire version 3**, and a leader from an earlier release refuses it with
`FailedPrecondition` and `forward_wire_version_unsupported`. **Upgrade all
followers first, then the leader**, as for wire version 2.

Refusing matters most for the range delete: its op type is an ordinary
single-key delete, so a leader that predates the feature would decode the op,
ignore the range end, and delete one key where the branch asked for a range.

Everything else keeps stamping version 2, so the rest of a follower's writes
go through to a leader that is one release behind: a request carries the
lowest wire version that can represent it, not the newest the sender knows.
Only transactions that nest or delete a range are blocked while a rolling
upgrade is in progress, and they are blocked loudly.

## `MemberList` answers only when client URLs are configured

Before this release `MemberList` returned one synthetic member with an empty
`ClientURLs` list. That is destructive for an etcd client with
`AutoSyncInterval` set: the official Go client replaces its endpoints with
whatever `MemberList` returns, so the first refresh left it with none and every
request after that failed. Such a client connected, worked, and dropped seconds
later.

The RPC now answers with `--advertise-client-urls`, and returns gRPC
`Unimplemented` when that flag is unset rather than a successful empty response.
A syncing client logs the failed sync and keeps the endpoints it was given,
which is the only answer a server can give without guessing an address its
clients may not be able to reach. The flag deliberately does not fall back to
`--listen`: that is `0.0.0.0:3379` out of the box, and a node behind a Service,
NAT or load balancer cannot tell which of its addresses a client used.

So a script that only checked that `member list` succeeded now sees
`Unimplemented` until the flag is set, and the Helm chart leaves
`config.advertiseClientUrls` empty, so chart deployments are included. Name the
address clients reach T4 at — `--advertise-client-urls=https://t4.example.com:443`
— and `member list` and endpoint sync both answer with it.

## Databases created with the meta keyspace

Databases created by this release or later keep T4's own bookkeeping (etcd leases, auth users, roles and tokens) in the
meta keyspace, which is replicated through the WAL but does not consume revisions. As in etcd, granting a lease,
keeping it alive, revoking a lease with no attached keys, and changing auth leave the revision unchanged.

The meta keyspace is enabled when the database is created, as its first WAL entry, and never changes afterwards:

- **Existing databases keep their format.** When this release opens a database created by an earlier one, lease and
  auth state stays in revisioned keys as before, and the database can still be downgraded.
- **New databases cannot be opened by earlier releases.** Their WAL segments containing meta entries use format 3 and
  their checkpoints use format 2, which earlier releases refuse.
- **A leader of a new database refuses followers that run an earlier release.** They are turned away with
  `wal_format_unsupported` before any entry is streamed to them. Create new databases only once every node runs this
  release or later.

## Moving off the GCS S3-compatible endpoint

A deployment configured with `--s3-endpoint https://storage.googleapis.com` is on a store that accepts the conditional
writes leader election depends on and ignores them. Since the release that added the startup check, a **cluster** on that
configuration refuses to start, so it has to move; a single node starts with a warning and should still move.

Switch to the GCS API, which enforces the preconditions:

```bash
# before
--s3-endpoint https://storage.googleapis.com --s3-bucket my-bucket --s3-prefix t4/ \
  --s3-access-key-id ... --s3-secret-access-key ...

# after
--gcs-bucket my-bucket --gcs-prefix t4/
```

The bucket and the object names do not change, so the existing data is read as it stands — no copy or re-upload. What
changes is how T4 authenticates: Application Default Credentials instead of HMAC keys, or `--gcs-credentials-file` for a
service account key. The HMAC keys can then be deleted. `--gcs-bucket` and `--s3-bucket` are mutually exclusive, so
remove the `--s3-*` flags rather than leaving them alongside.

## Downgrade

A downgrade to an older v1.x release is **best-effort**, not guaranteed. The newer binary may have written checkpoint or WAL formats the older binary does not understand.

Check these before downgrading:

1. Is the latest checkpoint in a format the older binary understands? Neither `t4 restore list` nor `t4 inspect meta`
   reports a format version, so read it from the manifest:
   ```bash
   # Absent or 1 → the older binary can restore it. 2 → it will refuse.
   aws s3 cp s3://your-bucket/your-prefix/manifest/latest - | jq '.format_version // 1'
   ```
   Check the local WAL segments the same way — the third byte of each header is the format version:
   ```bash
   for f in /var/lib/t4/wal/*.wal; do
     printf '%s format %d\n' "$(basename "$f")" \
       "$(dd if="$f" bs=1 skip=2 count=1 2>/dev/null | od -An -tu1 | tr -d ' ')"
   done
   ```
   If either is above what the older binary knows, it refuses to start (see [fail-closed table](#what-fails-closed) above).
2. Are any branch registry entries (`branches/<id>` in S3) newer than the older binary's safe `format_version`? List them:
   ```bash
   aws s3 ls s3://your-bucket/your-prefix/branches/
   ```
   Active branches pin their ancestor checkpoint format, so downgrading past that version is unsafe even if the latest checkpoint is older.
3. Has any RBAC permission been granted over a key range that is not valid UTF-8? Only such a range is stored
   differently by this release, and only then does an older binary refuse the role — it fails to decode it rather
   than enforce a different range. Nothing else about auth changes, so a deployment whose permission ranges are
   ordinary text can skip this.

   Whoever granted the permission is the one who knows: ranges like these come from clients that use binary keys,
   such as kine or juicefs, not from `etcdctl`. There is no offline check today — roles live in the meta keyspace,
   which `t4 inspect` cannot list — so read them from a running node with `etcdctl role get`.
4. Has any newly-added `t4.Config` field been used at runtime?
   See `git diff` of `config.go` between the two versions. Embedders relying on a newer field will get a compile error against the older library; the standalone binary silently ignores unknown CLI flags from a newer env-var set, which is a recipe for silent misconfiguration.

If all three answers are "no", the downgrade is safe. If any answer is "yes", **roll forward to a release without the regression instead** — moving forward with a fix is always safer than moving backward.

## What to do when fail-closed fires

The binary exits with an error referencing the schema version it rejected. Steps:

1. Note the version it refused and the corresponding [compatibility contract](/v1-compatibility/) section.
2. Do **not** delete the offending object — it is needed by the newer binary you must roll back to.
3. Roll the binary forward to a version that supports that schema. The data directory and S3 prefix are unchanged by the failed start.

## See also

- [v1 Compatibility Contract](/v1-compatibility/) — schema and API stability rules.
- [Releasing T4](https://github.com/t4db/t4/blob/main/docs/releasing.md) — how releases are produced and gated.
- [Operations](/operations/) — runtime monitoring, log fields, recovery procedures.
