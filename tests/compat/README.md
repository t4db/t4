# Compatibility Fixtures

This package verifies that the current T4 binary can read artifacts produced by
older releases.

Fixtures are generated from tagged releases and checked in under `testdata/`.
To refresh them after changing the baseline list:

```bash
tests/compat/generate_fixtures.sh
```

The first baseline is `v0.19.1`, the latest pre-v1 release tag present when the
fixture gate was added.

`testdata/v1.1.11/state-*` holds etcd lease and auth state written by the last
release before the meta keyspace, which keeps it in reserved keys of the
revisioned data keyspace. It is generated separately:

```bash
tests/compat/generate_state_fixture.sh
```
