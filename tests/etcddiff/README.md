# Differential tests against etcd

These tests run the same workload against a real embedded etcd and against
T4's etcd adapter, and require identical results. That covers header
revisions after every operation, responses, per-key metadata (create/mod
revision, version, lease) and the full watch history, lease expiry included.

Revision-exact replication between T4 and etcd depends on this. A replicator
can map T4 revision R to etcd revision R only if both spend revisions on
exactly the same operations.

The tests live in their own Go module, so etcd's server is not a dependency
of T4.

```sh
make test-etcd-diff
```

The workload is randomized and each run logs its seed. To reproduce a
failure:

```sh
cd tests/etcddiff && ETCDDIFF_SEED=<seed> go test -count=1 ./...
```

The tests need a database created with the meta keyspace. In databases
created by earlier releases, lease and auth bookkeeping consumes revisions and
diverges from etcd by design.
