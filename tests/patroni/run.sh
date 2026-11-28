#!/usr/bin/env bash
# Run Patroni's acceptance suite against T4 in Docker.
#
#   ./tests/patroni/run.sh                                    # whole suite
#   ./tests/patroni/run.sh features/basic_replication.feature  # one feature
#
# Logs and Patroni's own output land in tests/patroni/output/.
set -euo pipefail

cd "$(dirname "$0")/../.."
mkdir -p tests/patroni/output

docker build -f tests/patroni/Dockerfile -t t4-patroni .
exec docker run --rm -v "$PWD/tests/patroni/output:/out" t4-patroni "$@"
