#!/usr/bin/env bash
# Runs behave, then copies its output to the mounted directory.
#
# features/output cannot be the mount point itself: Patroni's before_all hook
# removes and recreates that directory, which fails with EBUSY on a mount.
set -uo pipefail

behave "$@"
rc=$?

if [ -d /out ]; then
  cp -a /patroni/features/output/. /out/ 2>/dev/null || true
fi

exit "$rc"
