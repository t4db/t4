#!/usr/bin/env bash
# Starts a single T4 node, runs JuiceFS's pkg/meta tests against it, and
# copies T4's log to the mounted directory. Arguments are passed to
# `go test`; with none, the two etcd suites run.
set -uo pipefail

# --advertise-client-urls is not optional here. newEtcdClient sets
# AutoSyncInterval, so the etcd client calls MemberList on a timer, and T4
# answers Unimplemented until an advertised address is configured.
#
# No object store: one node with local-only durability, the same scope as the
# `etcd --data-dir` this replaces.
t4 run --data-dir /tmp/t4 --listen "$ETCD_ADDR" \
  --advertise-client-urls "http://$ETCD_ADDR" >/tmp/t4.log 2>&1 &
t4_pid=$!

# Wait on the port rather than on a log line, so a change to T4's log format
# cannot hang the run.
for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/${ETCD_ADDR%:*}/${ETCD_ADDR##*:}") 2>/dev/null; then
    break
  fi
  sleep 1
done

if [ "$#" -eq 0 ]; then
  # TestEtcdClient runs JuiceFS's shared metadata-engine contract (testMeta);
  # TestEtcd runs the raw key-value and transaction contract (testTKV).
  # Anchored so TestEtcd does not also match TestEtcdClient.
  set -- -run 'TestEtcd$|TestEtcdClient$' -count=1 -timeout 30m ./pkg/meta/
fi
go test "$@"
rc=$?

kill "$t4_pid" 2>/dev/null
if [ -d /out ]; then
  cp /tmp/t4.log /out/t4.log 2>/dev/null || true
fi

exit "$rc"
