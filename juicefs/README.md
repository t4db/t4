# JuiceFS metadata-engine tests against T4

[JuiceFS](https://github.com/juicedata/juicefs) is a POSIX filesystem that
keeps its metadata — inodes, directory entries, chunk and slice records,
sessions — in an external store, and etcd is one of the engines it supports.
T4 serves the etcd v3 API, so JuiceFS can use it instead, and the question
this directory answers is whether it really can: these are JuiceFS's *own*
metadata-engine tests, run against T4 rather than etcd.

That matters because JuiceFS is the client that has found the most T4 bugs.
Issues #420, #421, #422 and #427 all came out of running it, and the shape of
the workload is why: a filesystem rewrites the same small records constantly,
so it churns key history far harder than a config store does, and it reads
and writes through transactions that compare `ModRevision`.

## Running

```bash
./juicefs/run.sh                                   # both etcd suites
./juicefs/run.sh -run 'TestEtcd$' -v ./pkg/meta/   # one suite, verbose
```

Everything runs in Docker: a `t4` binary built from the working tree, a
JuiceFS clone at a pinned tag, and `go test`. Nothing is installed on the
host, no object store is involved, and no FUSE mount is made — these tests
drive the metadata engine directly. T4's log lands in `juicefs/output/`.

Expect the default run to take two to three minutes. Almost all of it is
JuiceFS doing many small transactions against a single node that fsyncs each
write, not any one slow operation.

## How it hooks in

There is nothing to hook. `newKVMeta("etcd", os.Getenv("ETCD_ADDR"), …)` is
how `pkg/meta`'s tests reach the engine, so pointing `$ETCD_ADDR` at T4 is
the entire integration — unlike `patroni/`, this directory needs no
controller shim, and JuiceFS is cloned unpatched.

One thing T4 needs for it to work at all:

- **`--advertise-client-urls`** — `newEtcdClient` sets
  `AutoSyncInterval: time.Minute`, so the etcd client calls `MemberList` on a
  timer, and T4 answers `Unimplemented` until an advertised address is
  configured. `entrypoint.sh` passes the flag.

JuiceFS's etcd engine uses no leases and no watches. It needs `Get` with
range, prefix, limit, keys-only, count-only, serializable and
`MaxModRevision`; `Txn` comparing `ModRevision`; nested `OpTxn`; `Put`; and
delete-by-prefix. Binary keys are the other half of the surface — JuiceFS
keys look like `/juicefs\xfdsetting`, which is why T4 has regression tests
using exactly that.

## What it covers

Two tests, which between them are JuiceFS's whole engine contract:

- **`TestEtcdClient`** runs `testMeta`, the shared suite every JuiceFS
  metadata engine must pass: inode and directory operations, hardlinks,
  renames, chunk and slice bookkeeping, xattrs, quotas, sessions, trash,
  concurrent writers, and the compaction of its own slice lists.
- **`TestEtcd`** runs `testTKV`, the raw key-value and transaction contract
  underneath it: range scans, transaction isolation, and the
  compare-and-set behaviour JuiceFS builds on.

`TestFSOps` in `pkg/meta/random_test.go` is worth adding to a nightly gate
and runs through the same entrypoint, because `-rapid.meta` takes any meta
URL:

```bash
./juicefs/run.sh -run TestFSOps -rapid.meta=etcd://127.0.0.1:3379/jfs \
  -rapid.seed=1 -rapid.checks=30 -rapid.steps=100 -timeout 60m ./pkg/meta/
```

It is a property-based model check of filesystem operations against a
reference model, which is the closest thing upstream has to a differential
test of a metadata engine.

## Caveats

- Not wired into CI. The default run takes minutes, so it belongs in the
  nightly gate next to the Jepsen cells rather than on every PR.
- No fault injection and no object store: T4 runs as a single node, so this
  proves API and semantic compatibility, not durability or failover.
- The filesystem-level suites are not here. JuiceFS's `pjdfstest`, LTP and
  xattr workflows drive a real FUSE mount through
  `.github/scripts/start_meta_engine.sh`, which does know `etcd`, but none of
  those workflows include etcd in their matrix — so upstream has never run
  them on an etcd-shaped engine, and a failure there is as likely to be a
  JuiceFS-on-etcd bug as a T4 one. `rmfiles.yml` and `gateway.yml` *do* run
  on etcd upstream and are the right next step if we want mount coverage.
- The JuiceFS tag is pinned deliberately. The suite is `pkg/meta`'s own test
  helpers, which are not a stable interface, so bump it on purpose and expect
  the test names or their environment variables to be what breaks.
