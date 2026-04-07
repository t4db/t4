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
#   BENCH_WORKLOADS space-separated list  (default: all six)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/bench"
RESULTS_DIR="$BENCH_DIR/results"
mkdir -p "$RESULTS_DIR"

TOTAL="${BENCH_TOTAL:-50000}"
CLIENTS="${BENCH_CLIENTS:-16}"
WORKLOADS="${BENCH_WORKLOADS:-seq-put par-put seq-get par-get mixed watch}"
JSONL="$RESULTS_DIR/results.jsonl"

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

# run_workloads <network> <scenario> <system> <endpoints>
run_workloads() {
    local network="$1"
    local scenario="$2"
    local system="$3"
    local endpoints="$4"
    for wl in $WORKLOADS; do
        log "  $system / $wl ..."
        docker run --rm --network "$network" t4bench \
            --endpoints "$endpoints" \
            --workload   "$wl" \
            --clients    "$CLIENTS" \
            --total      "$TOTAL" \
            --scenario   "$scenario" \
            --system     "$system" \
            --output     jsonl \
            >> "$JSONL"
    done
}

# run_scenario <name> <compose-file> <t4-ep> <etcd-ep>
run_scenario() {
    local name="$1"
    local compose="$2"
    local t4_ep="$3"
    local etcd_ep="$4"
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
    run_workloads "$network" "$name" "t4" "$t4_ep"
    run_workloads "$network" "$name" "etcd"   "$etcd_ep"

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
        "etcd:2379"
}

do_single_s3() {
    run_scenario "single-s3" \
        "$BENCH_DIR/docker/single-s3/docker-compose.yml" \
        "t4:2379" \
        "etcd:2379"
}

do_cluster() {
    run_scenario "cluster" \
        "$BENCH_DIR/docker/cluster/docker-compose.yml" \
        "t41:2379,t42:2379,t43:2379" \
        "etcd1:2379,etcd2:2379,etcd3:2379"
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
