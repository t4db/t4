#!/usr/bin/env bash
# Run JuiceFS's own metadata-engine tests against T4 in Docker.
#
#   ./tests/juicefs/run.sh                                   # both etcd suites
#   ./tests/juicefs/run.sh -run 'TestEtcd$' -v ./pkg/meta/   # one suite, verbose
#
# T4's log lands in tests/juicefs/output/.
set -euo pipefail

cd "$(dirname "$0")/../.."
mkdir -p tests/juicefs/output

docker build -f tests/juicefs/Dockerfile -t t4-juicefs .
exec docker run --rm -v "$PWD/tests/juicefs/output:/out" t4-juicefs "$@"
