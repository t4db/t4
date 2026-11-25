# Patroni acceptance tests against T4

[Patroni](https://github.com/patroni/patroni) is the standard HA manager for
PostgreSQL. It keeps its cluster state — the leader key, member records,
failover intent — in a DCS, and etcd is the most common choice. T4 serves the
etcd v3 API, so Patroni can use it instead, and the question this directory
answers is whether it really can: these are Patroni's *own* acceptance tests,
run against T4 rather than etcd.

That matters because Patroni is the hardest kind of client to satisfy. It is
not storing data in the DCS, it is deciding which Postgres is allowed to
accept writes — so a lease that outlives its holder, or a compare-and-set
that answers wrongly under contention, promotes a second primary.

## Running

```bash
./patroni/run.sh                                     # the whole suite
./patroni/run.sh features/basic_replication.feature  # one feature
./patroni/run.sh -n 'add the second node'            # one scenario
```

Everything runs in Docker: Postgres, Patroni and `behave` in the container,
with the `t4` binary built from the working tree. Nothing is installed on the
host and no object store is involved — a single T4 node runs with local-only
durability, the same scope as the `etcd --data-dir` it replaces.

Logs, including each Patroni's own log and the T4 node's, land in
`patroni/output/`.

## How it hooks in

Patroni picks its DCS from `$DCS` and resolves the name through a registry it
builds from `AbstractDcsController.get_subclasses()`, where a controller's
name is its class name without the `Controller` suffix. `t4_controller.py`
subclasses the etcd3 controller and overrides only how the process starts;
appending it to `features/environment.py` registers it. Patroni itself is
cloned at a pinned tag and is otherwise unpatched.

Two things T4 needs for this to work at all, both of which it gained for
Patroni's sake:

- **`/version`** — `Etcd3Client` reads it before anything else to choose an
  API prefix, and raises if it is missing.
- **`--advertise-client-urls`** — the client's constructor calls `MemberList`
  through `_load_machines_cache`, and T4 answers `Unimplemented` until an
  advertised address is configured. The controller passes the flag.

## What it covers

Patroni's features exercise the parts no RPC-level test reaches: leader
election under contention, failover and switchover, replication slots,
cascading and standby clusters, `recovery`, `priority_failover`,
`quorum_commit`, and — the one most worth watching for T4 —
`dcs_failsafe_mode`, which SIGSTOPs the DCS to check that a healthy primary
is not demoted when the DCS goes away. `AbstractDcsController.start_outage()`
does that to whatever process the controller started, so T4 gets the test for
free.

## Caveats

- Not wired into CI. It wants Postgres and Python and takes minutes, so it
  belongs in the nightly gate next to the Jepsen cells rather than on every
  PR.
- `DCS=etcd` (the v2 API) is out of scope: T4 does not serve it. Only the
  etcd3-shaped controller applies.
- The Patroni tag is pinned deliberately. The suite drives Patroni's internal
  harness, which is not a stable interface, so bump it on purpose and expect
  the controller's base class to be what breaks.
