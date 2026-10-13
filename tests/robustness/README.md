# etcd robustness tests

These tests run etcd's own robustness tests against T4: the traffic
generators and validators from `go.etcd.io/etcd/tests/v3/robustness`,
driven against real `t4 run` processes while nodes are killed and restarted
or lose their network.

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
traffic scenario or failpoint, and `T4_ROBUSTNESS_SKIP` leaves out a
comma-separated list of `Scenario/Failpoint` pairs. A failed run keeps its report, with client
histories and each node's data directory and log, under `RESULTS_DIR`;
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

Compactions run every 200 ms, but differently from upstream. A compaction
takes no revision, and the rebuilt history cannot place one among them, so a
compaction that succeeded is left out of it: linearization places it by
itself, and the other checks do not need it. The compaction loop:

- never compacts past the revision the history watch has seen, which would
  lose the history if that watch had to resume;
- stops after a compaction whose outcome it does not learn, so that the
  compaction revision at the end tells whether that one took effect;
- now and then targets a revision too far ahead to exist, to be refused as a
  future revision.

When linearization fails, the test also validates without the patching the
rebuilt history feeds, and logs whether that passes: if it does, the rebuilt
history is at fault, not T4. It also replays the history to check the answer
of every read at a past revision.

Failures are injected by the harness rather than etcd's gofail failpoints:
killing a random node, the leader, a follower or every node with SIGKILL, and
restarting the leader gracefully. In a three-node cluster, each node's peer
traffic and its traffic to S3 also pass through a proxy, which can cut the
followers off from the leader, delay the leader's peer traffic, or cut the
leader or a follower off from S3. A cut connection drops what is sent rather
than closing, as on a lossy network.

Like a supervisor, the harness restarts a node whose `t4` process exits on
its own, as a leader does when it steps down after losing S3; the node then
rejoins as a follower.

## CI

Pull requests run the single-node scenarios. The nightly workflow runs the
three-node ones against RustFS.
