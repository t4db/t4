#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VERSION="${T4_APISERVER_VERSION:-v0.35.4}"
WORKDIR="${T4_APISERVER_WORKDIR:-$ROOT/.tmp/apiserver-$VERSION}"
BIN_DIR="$ROOT/bin/apiserver-tests/$VERSION"
T4_BIN="${T4_APISERVER_T4_BIN:-$ROOT/bin/t4-apiserver-test}"

mkdir -p "$WORKDIR" "$BIN_DIR"
go build -o "$T4_BIN" "$ROOT/cmd/t4"
export T4_APISERVER_T4_BIN="$T4_BIN"

if [[ ! -d "$WORKDIR/.git" ]]; then
  rm -rf "$WORKDIR"
  git clone --depth 1 --branch "$VERSION" https://github.com/kubernetes/apiserver "$WORKDIR"
else
  git -C "$WORKDIR" fetch --depth 1 origin "refs/tags/$VERSION:refs/tags/$VERSION"
  git -C "$WORKDIR" reset --hard "$VERSION"
  git -C "$WORKDIR" clean -xffd
fi

cd "$WORKDIR"

grep -rlF 'k8s.io/apiserver/pkg/storage/etcd3/testserver' pkg/ \
  | xargs sed -i.bak 's|k8s.io/apiserver/pkg/storage/etcd3/testserver|github.com/t4db/t4-apiserver-testserver|'
find pkg -name '*.bak' -delete

go mod edit -replace "github.com/t4db/t4-apiserver-testserver=$ROOT/tests/apiserver/testserver"
go mod tidy
# Each tree gets its own directory: both have a package named metrics.
mkdir -p "$BIN_DIR/etcd3" "$BIN_DIR/cacher"
go test -c -o "$BIN_DIR/etcd3/" ./pkg/storage/etcd3/...
go test -c -o "$BIN_DIR/cacher/" ./pkg/storage/cacher/...

# These expect the watch cache to delegate an exact list at a revision only
# the compact key changed. The cache delegates only while no watch progress
# notification has told it about that revision: etcd sends one every 10
# minutes by default, T4 every second, so with T4 the cache already holds the
# revision and serves the list itself.
skip_cacher='^TestLists$/^ConsistentListFromCache=false,ListFromCacheSnapshot=true$/^List$/resource_version_after_writes'

for testbin in "$BIN_DIR"/etcd3/*.test; do
  "$testbin" -test.count 1 -test.parallel 1 -test.timeout "${T4_APISERVER_TIMEOUT:-15m}"
done

# The cacher tests keep their own parallelism: TestGet runs its cases in
# parallel because one of them waits out the watch cache's 3s timeout, and
# run one by one, the later cases outlive the short lease they read. They fail
# against etcd too that way.
for testbin in "$BIN_DIR"/cacher/*.test; do
  skip=()
  if [[ "$testbin" == "$BIN_DIR/cacher/cacher.test" ]]; then
    skip=(-test.skip "$skip_cacher")
  fi
  "$testbin" -test.count 1 -test.timeout "${T4_APISERVER_TIMEOUT:-15m}" ${skip[@]+"${skip[@]}"}
done
