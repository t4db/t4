#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

helm lint "$chart_dir"

check_urls() {
  local name="$1" expected="$2" rendered actual
  shift 2
  rendered="$(helm template t4 "$chart_dir" --namespace test \
    --show-only templates/statefulset.yaml "$@")"
  # The chart quotes this env value on the line immediately after its name.
  # Inspect only the T4 StatefulSet; proxy and S3 containers are separate.
  # More than one match means the template grew a second setting: the extra
  # line makes the comparison below fail.
  actual="$(awk '
    /^[[:space:]]*- name: T4_ADVERTISE_CLIENT_URLS[[:space:]]*$/ {
      getline
      sub(/^[[:space:]]*value: "/, "")
      sub(/"[[:space:]]*$/, "")
      print
    }
  ' <<< "$rendered")"
  if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL %s: expected <%s>, actual <%s>\n' "$name" "$expected" "$actual" >&2
    return 1
  fi
  printf 'PASS %s: %s\n' "$name" "${actual:-<unset>}"
}

# Unset unless the operator asks: an in-cluster address advertised to a client
# outside the cluster replaces a working endpoint with an unresolvable name.
check_urls defaults ''
check_urls three-replicas '' --set replicaCount=3
check_urls client-tls '' --set tls.enabled=true
check_urls envoy '' --set proxy.enabled=true
check_urls service-port '' --set service.port=12379

cat > "$tmp_dir/external.yaml" <<'YAML'
config:
  advertiseClientUrls: "https://t4.example.com:443"
YAML
check_urls external-url 'https://t4.example.com:443' \
  --values "$tmp_dir/external.yaml" --set proxy.enabled=true --set service.port=12379

cat > "$tmp_dir/multiple.yaml" <<'YAML'
config:
  advertiseClientUrls: "http://node-a:3379,http://node-b:3379,http://node-c:3379"
YAML
check_urls three-urls 'http://node-a:3379,http://node-b:3379,http://node-c:3379' \
  --values "$tmp_dir/multiple.yaml" --set proxy.enabled=true --set tls.enabled=true

cat > "$tmp_dir/in-cluster.yaml" <<'YAML'
config:
  advertiseClientUrls: "http://t4.test.svc.cluster.local:3379"
YAML
check_urls in-cluster 'http://t4.test.svc.cluster.local:3379' \
  --values "$tmp_dir/in-cluster.yaml"
