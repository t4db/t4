#!/usr/bin/env bash
# Runs the Kubernetes conformance tests with Sonobuoy on a two-node k0s
# cluster that keeps its state in T4, built from this tree.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODE="${T4_CONFORMANCE_MODE:-certified-conformance}" # or quick: one test
K0S_VERSION="${T4_CONFORMANCE_K0S_VERSION:-v1.36.4-k0s.1}"
SONOBUOY_VERSION="${T4_CONFORMANCE_SONOBUOY_VERSION:-v0.57.5}"
RESULTS_DIR="${T4_CONFORMANCE_RESULTS:-$ROOT/.tmp/conformance}"
K0S_MEMORY="${T4_CONFORMANCE_K0S_MEMORY:-8g}"

NET=t4-conformance
T4=t4-conformance-t4
K0S=t4-conformance-k0s
WORKER=t4-conformance-worker

remove() {
  docker rm -f "$WORKER" "$K0S" "$T4" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
finish() {
  docker logs "$T4" >"$RESULTS_DIR/t4.log" 2>&1 || true
  docker logs "$K0S" >"$RESULTS_DIR/k0s.log" 2>&1 || true
  docker logs "$WORKER" >"$RESULTS_DIR/worker.log" 2>&1 || true
  # T4_CONFORMANCE_KEEP=1 leaves the cluster up to look into.
  if [[ -z "${T4_CONFORMANCE_KEEP:-}" ]]; then
    remove
  fi
}

mkdir -p "$RESULTS_DIR" "$ROOT/.tmp/bin"
remove
trap finish EXIT

case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
# Sonobuoy runs inside the k0s container, so it is always the Linux build.
SONOBUOY="$ROOT/.tmp/bin/sonobuoy-$SONOBUOY_VERSION-linux-$arch"
if [[ ! -x "$SONOBUOY" ]]; then
  curl -fsSL "https://github.com/vmware-tanzu/sonobuoy/releases/download/$SONOBUOY_VERSION/sonobuoy_${SONOBUOY_VERSION#v}_linux_$arch.tar.gz" \
    | tar -xzO sonobuoy >"$SONOBUOY.tmp"
  chmod +x "$SONOBUOY.tmp"
  mv "$SONOBUOY.tmp" "$SONOBUOY"
fi

docker build -t t4:conformance "$ROOT"
docker network create "$NET" >/dev/null
docker run -d --name "$T4" --network "$NET" --network-alias t4 --memory 2g \
  t4:conformance run >/dev/null
# Kubelet evicts pods once the disk is 85% full by default, which CI runners
# and laptops often already are: evict at a fixed free space instead.
KUBELET_ARGS="--eviction-hard=memory.available<100Mi,nodefs.available<1Gi,imagefs.available<1Gi"
# Not --single: that keeps the cluster state in k0s's own SQLite, not in T4.
docker run -d --name "$K0S" --hostname k0s --network "$NET" \
  --privileged --cgroupns=host --memory "$K0S_MEMORY" --memory-swap "$K0S_MEMORY" \
  -v /var/lib/k0s -v "$ROOT/tests/conformance/k0s.yaml:/etc/k0s/k0s.yaml:ro" \
  "docker.io/k0sproject/k0s:$K0S_VERSION" \
  k0s controller --enable-worker --no-taints \
  --disable-components=konnectivity-server --config=/etc/k0s/k0s.yaml \
  --kubelet-extra-args="$KUBELET_ARGS" >/dev/null
docker cp "$SONOBUOY" "$K0S:/usr/local/bin/sonobuoy"

# The conformance suite needs two nodes: the controller's own and a worker.
for _ in $(seq 1 60); do
  if token="$(docker exec "$K0S" k0s token create --role=worker 2>/dev/null)"; then
    break
  fi
  sleep 5
done
docker run -d --name "$WORKER" --hostname worker --network "$NET" \
  --privileged --cgroupns=host --memory "$K0S_MEMORY" --memory-swap "$K0S_MEMORY" \
  -v /var/lib/k0s "docker.io/k0sproject/k0s:$K0S_VERSION" \
  k0s worker --kubelet-extra-args="$KUBELET_ARGS" "$token" >/dev/null

echo "Waiting for both k0s nodes to be ready"
for _ in $(seq 1 60); do
  ready="$(docker exec "$K0S" k0s kubectl get nodes --no-headers 2>/dev/null | grep -c ' Ready ' || true)"
  if [[ "$ready" == 2 ]]; then
    break
  fi
  sleep 5
done
docker exec "$K0S" k0s kubectl get nodes
docker exec "$K0S" sh -c 'k0s kubeconfig admin >/root/kubeconfig'

sonobuoy() { docker exec "$K0S" sonobuoy --kubeconfig /root/kubeconfig "$@"; }
# Only the e2e plugin: systemd-logs never finishes in a container without
# a systemd journal.
sonobuoy run --plugin e2e --mode "$MODE"
# Sonobuoy's progress counter stays at zero with this conformance image:
# show the tests' own output instead, a dot per test and every failure in
# full as it happens.
for _ in $(seq 1 60); do
  pod="$(docker exec "$K0S" k0s kubectl -n sonobuoy get pods -l sonobuoy-plugin=e2e -o name 2>/dev/null)"
  if [[ -n "$pod" ]] && docker exec "$K0S" k0s kubectl -n sonobuoy logs -f "$pod" -c e2e 2>/dev/null; then
    break
  fi
  sleep 5
done
sonobuoy wait --wait=240 --wait-output=silent
tarball="$(sonobuoy retrieve /root)"
docker cp "$K0S:$tarball" "$RESULTS_DIR/"
summary="$(docker exec "$K0S" sonobuoy results "$tarball" --plugin e2e)"
echo "$summary"
if ! grep -q '^Status: passed' <<<"$summary"; then
  docker exec "$K0S" sonobuoy results "$tarball" --plugin e2e --mode detailed \
    | grep '"status":"failed"' || true
  exit 1
fi
