# etcd robustness tests

These tests run etcd's own robustness tests against T4: the traffic
generators and validators from `go.etcd.io/etcd/tests/v3/robustness`,
driven against real `t4 run` processes while nodes are killed and restarted.

Validation requires that:

- every result a client observed is linearizable;
- watches deliver one ordered, gap-free history on every node, resumable
  from any revision;
- reads at a past revision return the state at that revision;
- every node ends with the same `HashKV`.

The tests live in their own Go module, so etcd's test framework is not a
dependency of T4. They build `t4` from this checkout, or use
`T4_ROBUSTNESS_BIN`.

```sh
make test-robustness
```

Without S3 they run single-node scenarios only. To also run a three-node
cluster, point them at an S3-compatible store:

```sh
docker run -d --name t4-s3 -p 9000:9000 \
  -e RUSTFS_ACCESS_KEY=t4testadmin -e RUSTFS_SECRET_KEY=t4testadmin \
  rustfs/rustfs:1.0.0
T4_ROBUSTNESS_S3=1 make test-robustness
```

`S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY` and `S3_SECRET_KEY`
override the defaults, which match the command above.
`T4_ROBUSTNESS_SCENARIO` and `T4_ROBUSTNESS_FAILPOINT` narrow a run to one
traffic scenario or failpoint. A failed run keeps its report, with client
histories and node data directories, under `RESULTS_DIR`;
`PERSIST_RESULTS=1` keeps passing runs' reports too. `make test-robustness`
puts node data and reports under `.tmp/robustness` rather than `/tmp`, which
may be RAM-backed; `go test` on its own uses `TMPDIR` and, for reports, the
system temp dir.

## Differences from upstream

etcd validates against the requests it persisted, read from its raft WAL.
T4 has no raft WAL, so the history is rebuilt from a watch over the whole
keyspace: each revision becomes a transaction of its puts and deletes, plus
the lease grants and revokes the model needs. A revision that deletes exactly
the keys of a lease some client revoked, and that no later put uses, is
taken to be that revoke.

That history cannot place a compaction among the revisions, so compaction
traffic is left out.

Failures are injected by the harness rather than etcd's gofail failpoints:
killing a random node, the leader, a follower or every node with SIGKILL, and
restarting the leader gracefully.

T4 does not implement etcd 3.7's `RangeStream`, which the etcd traffic uses
for some reads. Those reads fail and, like any failed read, are left out of
validation.
