#!/usr/bin/env bash
# bench/indexsweep.sh — how T4's and etcd's idle memory grow with the number
# of objects stored.
#
# Usage:
#   ./bench/indexsweep.sh [value-size] [count...]
#
# Defaults: 256-byte values at 100k, 300k, 600k and 1M objects.
#
# The two stores grow differently, and a single object count cannot show it:
#
#   etcd  keeps a treeIndex entry per key in memory, so its footprint rises
#         with the number of keys.
#   T4    keeps no permanent per-key index. Its floor is Pebble's block cache
#         plus its memtables — around 190 MiB with the built-in sizes — and
#         that floor does not move with the key count.
#
# So which store is cheaper depends on where those two lines cross, and a
# comparison at one count says nothing about a cluster at another. This sweeps
# the count to find it.
#
# Values are small on purpose. Both effects above are per key and independent
# of value size, while the data itself is not: at 4 KiB a million objects is
# 4 GB and the containers are capped at 4 GiB, so large values would run out
# of room long before the crossover. Read the slope here, and take the floor
# at a realistic value size from memcompare.sh.
set -euo pipefail

VALSIZE="${1:-256}"
shift || true
COUNTS=("$@")
if [ "${#COUNTS[@]}" -eq 0 ]; then
    COUNTS=(100000 300000 600000 1000000)
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="indexsweep"
COMPOSE="docker compose -f $REPO_ROOT/bench/docker/single/docker-compose.yml -p $PROJ"
CLIENTS=16

log() { printf '[sweep] %s\n' "$*" >&2; }

cd "$REPO_ROOT"
log "building images..."
docker build -q -f bench/Dockerfile.t4 -t t4-bench . >/dev/null
docker build -q -f bench/Dockerfile -t t4bench . >/dev/null

metric() { # <system> <metric name>
    local out value tries=0
    while :; do
        if [ "$1" = t4 ]; then
            out="$($COMPOSE exec -T t4 curl -s http://localhost:9090/metrics 2>/dev/null || true)"
        else
            out="$(curl -s http://localhost:2379/metrics 2>/dev/null || true)"
        fi
        value="$(printf '%s\n' "$out" | awk -v n="$2" '$1 == n { printf "%.0f", $2; exit }')"
        if [ -n "$value" ]; then
            printf '%s' "$value"
            return 0
        fi
        tries=$((tries + 1))
        if [ "$tries" -gt 30 ]; then
            log "$1: metric $2 never became readable"
            return 1
        fi
        sleep 1
    done
}

container_bytes() {
    local id
    id="$($COMPOSE ps -q "$1")"
    docker stats --no-stream --format '{{.MemUsage}}' "$id" 2>/dev/null \
        | awk '{
              v = $1; u = $1;
              sub(/[A-Za-z]+$/, "", v); sub(/^[0-9.]+/, "", u);
              m = 1;
              if (u == "KiB") m = 1024;
              else if (u == "MiB") m = 1024 * 1024;
              else if (u == "GiB") m = 1024 * 1024 * 1024;
              else if (u == "TiB") m = 1024 * 1024 * 1024 * 1024;
              printf "%d", v * m;
          }'
}

wait_ready() {
    local tries=0
    until docker run --rm --network "${PROJ}_bench" t4bench \
        --endpoints "$1" --workload seq-put --total 1 >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -gt 60 ]; then
            log "$1 never became ready"
            return 1
        fi
    done
}

mib() { awk -v b="$1" 'BEGIN { printf "%.1f", b / 1048576 }'; }

declare -a ROWS=()

for count in "${COUNTS[@]}"; do
    log "$count objects: starting a clean pair..."
    $COMPOSE down -v >/dev/null 2>&1 || true
    $COMPOSE up -d >/dev/null 2>&1
    wait_ready t4:2379
    wait_ready etcd:2379

    for sys in t4 etcd; do
        log "$count objects: populating $sys..."
        docker run --rm --network "${PROJ}_bench" t4bench \
            --endpoints "$sys:2379" --workload par-put --clients "$CLIENTS" \
            --val-size "$VALSIZE" --total "$count" \
            --output jsonl >/dev/null 2>&1
    done

    # Let flushes, compaction and the uploader settle before reading, so this
    # is the cost of holding the keyspace rather than of having just written
    # it.
    sleep 30

    row="$count"
    for sys in t4 etcd; do
        heap="$(metric "$sys" go_memstats_heap_inuse_bytes)"
        gosys="$(metric "$sys" go_memstats_sys_bytes)"
        rss="$(container_bytes "$sys")"
        row="$row|$(mib "$heap")|$(mib "$gosys")|$(mib "$rss")"
    done
    ROWS+=("$row")
    log "$count objects: $row"
done

$COMPOSE down -v >/dev/null 2>&1 || true

echo
echo "=== idle memory vs object count, ${VALSIZE}-byte values ==="
printf '%10s %24s %24s\n' "" "---------- T4 ----------" "--------- etcd ---------"
printf '%10s %7s %7s %8s %7s %7s %8s\n' objects heap sys container heap sys container
for row in "${ROWS[@]}"; do
    IFS='|' read -r c th tg tr eh eg er <<<"$row"
    printf '%10s %7s %7s %8s %7s %7s %8s\n' "$c" "$th" "$tg" "$tr" "$eh" "$eg" "$er"
done
echo
echo "All figures MiB. Read the slope, not the absolute floor: small values"
echo "keep the data out of the way, so the floor here is lower than it is at a"
echo "realistic value size. Each row is one sample of each server."
