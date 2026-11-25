#!/usr/bin/env bash
# Run Patroni's acceptance suite against T4 in Docker.
#
#   ./patroni/run.sh                                    # whole suite
#   ./patroni/run.sh features/basic_replication.feature  # one feature
#
# Logs and Patroni's own output land in patroni/output/.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p patroni/output

docker build -f patroni/Dockerfile -t t4-patroni .
exec docker run --rm -v "$PWD/patroni/output:/out" t4-patroni "$@"
