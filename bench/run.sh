#!/usr/bin/env bash
# bench/run.sh — orchestrate the full T4 vs etcd benchmark suite.
#
# Usage:
#   ./bench/run.sh [single] [single-s3] [cluster]
#
# With no arguments all three scenarios are run in order. Pass scenario names
# to run a subset, e.g.:  ./bench/run.sh single cluster
#
# Environment overrides:
#   BENCH_TOTAL    total ops per workload (default: 50000)
#   BENCH_CLIENTS  concurrent clients     (default: 16)
#   BENCH_VAL_SIZE value size in bytes    (default: 256)
#   BENCH_KEY_SPACE distinct keys to cycle (default: 0 = always new keys)
#   BENCH_WORKLOADS space-separated list  (default: all six)
#   BENCH_REPS      repetitions of the measured op for list/watch-fanout (default: 20)
#   BENCH_WATCHERS  concurrent watchers for watch-fanout (default: 1000)
#   BENCH_LIST_LIMIT page size for list-paged (default: 500)
#
# Server memory is sampled from outside the containers for every workload and
# appended to the same results file, so the comparison table ends with a
# memory section per scenario. To compare the two stores on a keyspace shaped
# like a cluster full of custom resources:
#
#   BENCH_WORKLOADS="list list-paged watch-fanout" \
#   BENCH_TOTAL=100000 BENCH_VAL_SIZE=4096 ./bench/run.sh single
#
# Mind the container limits in the compose files (4 GiB each): a keyspace much
# past ~300k objects of 4 KiB needs them raised, and raised equally.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/bench"
RESULTS_DIR="$BENCH_DIR/results"
mkdir -p "$RESULTS_DIR"

TOTAL="${BENCH_TOTAL:-50000}"
CLIENTS="${BENCH_CLIENTS:-16}"
VALSIZE="${BENCH_VAL_SIZE:-256}"
KEYSPACE="${BENCH_KEY_SPACE:-0}"
WORKLOADS="${BENCH_WORKLOADS:-seq-put par-put seq-get par-get mixed watch}"
REPS="${BENCH_REPS:-20}"
WATCHERS="${BENCH_WATCHERS:-1000}"
LIST_LIMIT="${BENCH_LIST_LIMIT:-500}"
JSONL="$RESULTS_DIR/results.jsonl"
MEMTMP="$(mktemp -d)"
trap 'rm -rf "$MEMTMP"' EXIT

# ── build images once ─────────────────────────────────────────────────────────

log() { echo "[bench] $*"; }

log "Building t4-bench image..."
docker build -q -f "$BENCH_DIR/Dockerfile.t4" -t t4-bench "$REPO_ROOT"

log "Building t4bench image..."
docker build -q -f "$BENCH_DIR/Dockerfile" -t t4bench "$REPO_ROOT"

# ── helpers ───────────────────────────────────────────────────────────────────

# wait_ready <network> <endpoint>
# Polls the endpoint with a single no-op write until it succeeds or times out.
wait_ready() {
    local network="$1"
    local endpoint="$2"
    local max=40
    local i=0
    printf "[bench]   waiting for %s " "$endpoint"
    while [ $i -lt $max ]; do
        if docker run --rm --network "$network" t4bench \
               --endpoints "$endpoint" --workload seq-put --total 1 \
               --output text >/dev/null 2>&1; then
            echo " ready"
            return 0
        fi
        printf "."
        sleep 3
        i=$((i+1))
    done
    echo " TIMEOUT"
    return 1
}

# mem_bytes <container>... — total memory in use by the given containers.
#
# docker stats reports a cgroup's current usage less its inactive page cache,
# which is the closest thing to "what this server costs the machine" that both
# images can be measured by identically.
mem_bytes() {
    docker stats --no-stream --format '{{.MemUsage}}' "$@" 2>/dev/null \
        | awk '{
              v = $1; u = $1;
              sub(/[A-Za-z]+$/, "", v); sub(/^[0-9.]+/, "", u);
              m = 1;
              if (u == "KiB") m = 1024;
              else if (u == "MiB") m = 1024 * 1024;
              else if (u == "GiB") m = 1024 * 1024 * 1024;
              else if (u == "TiB") m = 1024 * 1024 * 1024 * 1024;
              total += v * m;
          } END { printf "%d %d", total, total }'
}

# start_sampler <outfile> <container>... — poll memory until stop_sampler.
# The file ends up holding "<peak> <last>" in bytes.
start_sampler() {
    local out="$1"; shift
    rm -f "$out" "$out.stop"
    echo "0 0" > "$out"
    (
        local peak=0 cur
        while [ ! -f "$out.stop" ]; do
            cur="$(mem_bytes "$@" | awk '{print $1}')"
            if [ -n "$cur" ] && [ "$cur" -gt "$peak" ] 2>/dev/null; then
                peak="$cur"
            fi
            echo "$peak ${cur:-0}" > "$out"
            sleep 1
        done
    ) &
    SAMPLER_PID=$!
}

# stop_sampler <outfile>
stop_sampler() {
    local out="$1"
    touch "$out.stop"
    wait "$SAMPLER_PID" 2>/dev/null || true
    rm -f "$out.stop"
}

# run_workloads <network> <scenario> <system> <endpoints> <container>...
run_workloads() {
    local network="$1"
    local scenario="$2"
    local system="$3"
    local endpoints="$4"
    shift 4
    local containers=("$@")
    local memfile="$MEMTMP/$scenario-$system.mem"

    for wl in $WORKLOADS; do
        log "  $system / $wl ..."
        start_sampler "$memfile" "${containers[@]}"
        docker run --rm --network "$network" t4bench \
            --endpoints "$endpoints" \
            --workload   "$wl" \
            --clients    "$CLIENTS" \
            --val-size   "$VALSIZE" \
            --key-space  "$KEYSPACE" \
            --total      "$TOTAL" \
            --reps       "$REPS" \
            --watchers   "$WATCHERS" \
            --list-limit "$LIST_LIMIT" \
            --scenario   "$scenario" \
            --system     "$system" \
            --output     jsonl \
            >> "$JSONL"
        stop_sampler "$memfile"

        # Memory lands as its own record for the same cell; compare merges it.
        awk -v s="$scenario" -v sy="$system" -v w="$wl" \
            '{ printf "{\"scenario\":\"%s\",\"system\":\"%s\",\"workload\":\"%s\",\"peak_mem_mb\":%.1f,\"end_mem_mb\":%.1f}\n", \
                 s, sy, w, $1 / 1048576, $2 / 1048576 }' \
            "$memfile" >> "$JSONL"
    done
}

# containers_for <project> <service>... — resolve compose services to ids.
containers_for() {
    local project="$1"; shift
    local ids=()
    local svc id
    for svc in "$@"; do
        id="$(docker ps -q --filter "label=com.docker.compose.project=$project" \
                           --filter "label=com.docker.compose.service=$svc")"
        [ -n "$id" ] && ids+=("$id")
    done
    printf '%s\n' "${ids[@]+"${ids[@]}"}"
}

# run_scenario <name> <compose-file> <t4-ep> <etcd-ep> <t4-services> <etcd-services>
#
# The service lists are what memory is sampled over, so for a cluster scenario
# they name every node and the figure is the cluster's total.
run_scenario() {
    local name="$1"
    local compose="$2"
    local t4_ep="$3"
    local etcd_ep="$4"
    local t4_svcs="$5"
    local etcd_svcs="$6"
    local project="bench-${name}"
    local network="${project}_bench"

    log ""
    log "=== Scenario: $name ==="

    # Bring up the stack (quiet pull, suppress compose output on success).
    docker compose -f "$compose" --project-name "$project" up -d --quiet-pull 2>&1 \
        | grep -v "^$" || true

    # Wait for both targets to be ready.
    wait_ready "$network" "$t4_ep"
    wait_ready "$network" "$etcd_ep"

    # Run all workloads against t4, then etcd.
    # Container ids are hex, so the unquoted expansion that splits them into
    # arguments is safe — and it keeps this working on the bash 3.2 that macOS
    # still ships, which has no mapfile.
    local t4_ids etcd_ids
    t4_ids="$(containers_for "$project" $t4_svcs)"
    etcd_ids="$(containers_for "$project" $etcd_svcs)"
    run_workloads "$network" "$name" "t4"   "$t4_ep"   $t4_ids
    run_workloads "$network" "$name" "etcd" "$etcd_ep" $etcd_ids

    # Tear down and remove volumes so the next scenario starts clean.
    docker compose -f "$compose" --project-name "$project" down -v --timeout 10 \
        >/dev/null 2>&1
    log "  torn down."
}

# ── scenario definitions ──────────────────────────────────────────────────────

do_single() {
    run_scenario "single" \
        "$BENCH_DIR/docker/single/docker-compose.yml" \
        "t4:2379" \
        "etcd:2379" \
        "t4" \
        "etcd"
}

do_single_s3() {
    run_scenario "single-s3" \
        "$BENCH_DIR/docker/single-s3/docker-compose.yml" \
        "t4:2379" \
        "etcd:2379" \
        "t4" \
        "etcd"
}

do_cluster() {
    run_scenario "cluster" \
        "$BENCH_DIR/docker/cluster/docker-compose.yml" \
        "t41:2379,t42:2379,t43:2379" \
        "etcd1:2379,etcd2:2379,etcd3:2379" \
        "t41 t42 t43" \
        "etcd1 etcd2 etcd3"
}

# ── main ──────────────────────────────────────────────────────────────────────

# Truncate results file so each full run starts fresh.
> "$JSONL"

SCENARIOS="${*:-single single-s3 cluster}"
for sc in $SCENARIOS; do
    case "$sc" in
        single)    do_single ;;
        single-s3) do_single_s3 ;;
        cluster)   do_cluster ;;
        *) echo "unknown scenario '$sc'; choose: single single-s3 cluster"; exit 1 ;;
    esac
done

# ── print comparison table ────────────────────────────────────────────────────

log ""
log "=== Results ==="
docker run --rm \
    -v "$RESULTS_DIR:/results:ro" \
    t4bench compare --results /results/results.jsonl
