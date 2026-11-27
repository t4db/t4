#!/usr/bin/env bash
# Run JuiceFS's own metadata-engine tests against T4 in Docker.
#
#   ./juicefs/run.sh                                   # both etcd suites
#   ./juicefs/run.sh -run 'TestEtcd$' -v ./pkg/meta/   # one suite, verbose
#
# T4's log lands in juicefs/output/.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p juicefs/output

docker build -f juicefs/Dockerfile -t t4-juicefs .
exec docker run --rm -v "$PWD/juicefs/output:/out" t4-juicefs "$@"
