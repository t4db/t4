#!/usr/bin/env bash
# bench/memcompare.sh — compare T4 and etcd memory on a collection read,
# using metrics that do not depend on when the garbage collector happened to
# run.
#
# Usage:
#   ./bench/memcompare.sh [objects] [value-size]
#
# Defaults: 40000 objects of 4096 bytes, the shape of a cluster holding many
# custom resources.
#
# Why this exists alongside run.sh, which already samples memory: run.sh reads
# container memory from `docker stats` once a second, and that number moves
# with GC timing, the page cache and the sampling phase. Measured twice on
# code that cannot behave differently — a paginated read whose page is smaller
# than the fetch window, so identical work either way — it reported 381 MiB and
# 621 MiB. It is fine for spotting an order of magnitude and useless for
# comparing two builds.
#
# What this measures instead:
#
#   allocated per list   go_memstats_alloc_bytes_total, which only ever grows,
#                        so it counts every byte handed out whatever the
#                        collector did. The populate phase shares an invocation
#                        with the reads, so it is cancelled by running a fresh
#                        store at two rep counts and differencing them.
#
#   steady state         heap in use, and bytes obtained from the OS, with the
#                        store populated and idle. This is what a server costs
#                        to hold a keyspace, as opposed to what a read costs to
#                        serve.
#
# Both numbers are reported for etcd too, measured the same way in the same
# run. Trust the T4-to-etcd ratio within one run; do not compare absolute
# figures across runs on different machines.
set -euo pipefail

OBJECTS="${1:-40000}"
VALSIZE="${2:-4096}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="memcompare"
COMPOSE="docker compose -f $REPO_ROOT/bench/docker/single/docker-compose.yml -p $PROJ"

# Rep counts to difference. The gap carries the measurement, so it wants to be
# large enough that one list's allocations dwarf the noise in two whole runs.
LO=3
HI=13

log() { printf '[mem] %s\n' "$*" >&2; }

cd "$REPO_ROOT"
log "building images..."
docker build -q -f bench/Dockerfile.t4 -t t4-bench . >/dev/null
docker build -q -f bench/Dockerfile -t t4bench . >/dev/null

# metric <system> <name> — one go_memstats_* gauge or counter, in bytes.
#
# A server that has just been recreated can accept a connection on its data
# port before its metrics endpoint is serving, so this waits for a reading
# rather than treating the first empty scrape as the answer.
metric() {
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

# container_bytes <service> — resident size of the server process's container.
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

fresh() {
    $COMPOSE down -v >/dev/null 2>&1 || true
    $COMPOSE up -d >/dev/null 2>&1
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

bench_run() { # <endpoint> <workload> <reps>
    docker run --rm --network "${PROJ}_bench" t4bench \
        --endpoints "$1" --workload "$2" --clients 1 \
        --val-size "$VALSIZE" --total "$OBJECTS" --reps "$3" \
        --output jsonl >/dev/null 2>&1
}

mib() { awk -v b="$1" 'BEGIN { printf "%.1f", b / 1048576 }'; }

declare -a ROWS=()

for sys in t4 etcd; do
    ep="$sys:2379"

    log "$sys: allocated per list (reps=$LO)..."
    fresh "$ep"
    lo_before="$(metric "$sys" go_memstats_alloc_bytes_total)"
    bench_run "$ep" list "$LO"
    lo="$(( $(metric "$sys" go_memstats_alloc_bytes_total) - lo_before ))"

    log "$sys: allocated per list (reps=$HI)..."
    fresh "$ep"
    hi_before="$(metric "$sys" go_memstats_alloc_bytes_total)"
    bench_run "$ep" list "$HI"
    hi="$(( $(metric "$sys" go_memstats_alloc_bytes_total) - hi_before ))"

    per_list=$(( (hi - lo) / (HI - LO) ))

    log "$sys: steady state after populate..."
    fresh "$ep"
    bench_run "$ep" seq-put 0
    # Let background compaction and the uploader settle before reading.
    sleep 15
    heap="$(metric "$sys" go_memstats_heap_inuse_bytes)"
    gosys="$(metric "$sys" go_memstats_sys_bytes)"
    rss="$(container_bytes "$sys")"

    ROWS+=("$sys|$(mib "$per_list")|$(mib "$heap")|$(mib "$gosys")|$(mib "$rss")")
done

$COMPOSE down -v >/dev/null 2>&1 || true

echo
echo "=== $OBJECTS objects x $VALSIZE bytes ==="
printf '%-8s %18s %14s %14s %14s\n' system "alloc/list MiB" "heap MiB" "go sys MiB" "container MiB"
printf '%-8s %18s %14s %14s %14s\n' -------- ------------------ -------------- -------------- --------------
for row in "${ROWS[@]}"; do
    IFS='|' read -r s a h g r <<<"$row"
    printf '%-8s %18s %14s %14s %14s\n' "$s" "$a" "$h" "$g" "$r"
done
echo
echo "alloc/list is cumulative and GC-independent: compare it across builds."
echo "heap, go sys and container are a single sample of an idle populated"
echo "server; compare the T4-to-etcd ratio within this run, not across runs."
