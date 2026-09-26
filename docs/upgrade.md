# Upgrade and Downgrade

What operators need to know to move a running T4 deployment from one v1.x release to another. For the underlying compatibility rules see the [v1 Compatibility Contract](v1-compatibility.md).

## What v1.x guarantees

- A newer v1.x binary can open a data directory and an object-store prefix written by any older v1.x binary.
- Newer v1.x can also add fields, flags, default values, exported types, and metrics — but never remove or change the meaning of existing ones without a major version bump.
- Default values listed in the [defaults table](v1-compatibility.md#configuration) do not change across v1.x unless explicitly documented as tuning guidance.

## What fails closed

These are the failure modes the binary refuses rather than silently corrupting state. They protect against an unsupervised downgrade onto data the older binary cannot interpret. None of them is recoverable without action; the binary exits with a clear error and the cluster's other nodes (if any) keep serving.

| Trigger | Behaviour |
|---|---|
| Checkpoint `format_version` newer than this binary understands | Startup refuses to restore. Operator must roll the binary forward. |
| WAL frame magic / version newer than this binary understands | Startup refuses to replay. Same remedy. |
| WAL entry with an op code this binary does not know (local replay, S3 replay, or the leader's peer stream) | Replay or apply is refused. A follower stops replicating and logs an error instead of retrying or attempting a leader takeover; it does not apply the entry. Same remedy. |
| `manifest/latest` references a checkpoint missing referenced SSTs | Startup error. Recoverable from an earlier checkpoint via `t4 restore`. |
| Branch registry entry points at a checkpoint older than the safe `format_version` | GC refuses to reclaim. Inspect with `t4 inspect`. |

## Rolling upgrade (cluster)

For multi-node clusters, upgrade one node at a time. The leader stays available throughout; the follower being upgraded re-syncs from S3 on restart.

1. Pick the **oldest** node that is currently a follower (avoid the leader on the first pass).
2. Stop the t4 process. Followers resync automatically on restart, so the data directory does not need to be cleared.
3. Install the new binary and restart with the **same** flags as before.
4. Wait for the follower to catch up — `t4 status` shows `current_revision >= leader_current_revision - tolerable_lag` and the leader's metrics show the follower reconnected.
5. Repeat for the remaining followers.
6. Restart the leader last. A follower wins the takeover election (~6 s; see [Consistency](consistency.md#behaviour-under-network-partition)). Promote it gracefully by signalling the leader with `SIGTERM`; the new leader is the highest-CommittedRev follower.

The cluster keeps accepting writes throughout, modulo the ~6 s window when the leader is being restarted.

## Single-node upgrade

1. Stop the t4 process.
2. Install the new binary.
3. Start with the same flags. T4 replays any unsealed WAL entries from the local data directory; if the local directory is gone (ephemeral storage), it restores the latest checkpoint from S3.

## Downgrade

A downgrade to an older v1.x release is **best-effort**, not guaranteed. The newer binary may have written checkpoint or WAL formats the older binary does not understand.

Check these before downgrading:

1. Has the cluster written a new checkpoint since the upgrade?
   ```bash
   t4 inspect meta --data-dir /var/lib/t4
   ```
   If the most recent checkpoint was produced by the newer binary, the older binary may refuse to start (see [fail-closed table](#what-fails-closed) above).
2. Are any branch registry entries (`branches/<id>` in S3) newer than the older binary's safe `format_version`? List them:
   ```bash
   aws s3 ls s3://your-bucket/your-prefix/branches/
   ```
   Active branches pin their ancestor checkpoint format, so downgrading past that version is unsafe even if the latest checkpoint is older.
3. Has any newly-added `t4.Config` field been used at runtime?
   See `git diff` of `config.go` between the two versions. Embedders relying on a newer field will get a compile error against the older library; the standalone binary silently ignores unknown CLI flags from a newer env-var set, which is a recipe for silent misconfiguration.

If all three answers are "no", the downgrade is safe. If any answer is "yes", **roll forward to a release without the regression instead** — moving forward with a fix is always safer than moving backward.

## What to do when fail-closed fires

The binary exits with an error referencing the schema version it rejected. Steps:

1. Note the version it refused and the corresponding [compatibility contract](v1-compatibility.md) section.
2. Do **not** delete the offending object — it is needed by the newer binary you must roll back to.
3. Roll the binary forward to a version that supports that schema. The data directory and S3 prefix are unchanged by the failed start.

## See also

- [v1 Compatibility Contract](v1-compatibility.md) — schema and API stability rules.
- [Releasing T4](releasing.md) — how releases are produced and gated.
- [Operations](operations.md) — runtime monitoring, log fields, recovery procedures.
