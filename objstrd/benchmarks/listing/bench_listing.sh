#!/bin/bash
# bench_listing.sh -- Benchmark S3 ListObjectsV2 speed across backends.
#
# Compares listing performance of raw, filesystem, and in-memory backends
# served over S3 via objstrd.
#
# Usage:  ./bench_listing.sh <TIER>
#         TIER = 1k | 10k | 100k | 250k
#
# Requires:
#   - Seed image created by create_seed.sh (default: /tmp/bench_seed.raw)
#   - objstrd release binary
#
# Environment:
#   OBJSTRD      Path to objstrd binary  [~/build-objstrd/release/objstrd]
#   RAWOBJSTR    Path to rawobjstr binary [~/build-rawobjstr/release/rawobjstr]
#   SEED_PATH    Seed image path          [/tmp/bench_seed.raw]
#   ITERATIONS   Listing iterations       [3]
#   PARALLEL     Import parallelism       [64]

set -euo pipefail

OBJSTRD="${OBJSTRD:-~/build-objstrd/release/objstrd}"
RAWOBJSTR="${RAWOBJSTR:-~/build-rawobjstr/release/rawobjstr}"
SEED_PATH="${SEED_PATH:-/tmp/bench_seed.raw}"
ITERATIONS="${ITERATIONS:-3}"
PARALLEL="${PARALLEL:-64}"

SEED_PORT=9100
TEST_PORT=9200
PAYLOAD=/tmp/bench_payload_1b

SEED_PID=""
TEST_PID=""

die() { echo "FATAL: $*" >&2; exit 1; }

cleanup() {
    [ -n "$TEST_PID" ] && kill "$TEST_PID" 2>/dev/null && wait "$TEST_PID" 2>/dev/null || true
    [ -n "$SEED_PID" ] && kill "$SEED_PID" 2>/dev/null && wait "$SEED_PID" 2>/dev/null || true
    TEST_PID=""
    SEED_PID=""
    rm -f "$PAYLOAD"
}
trap cleanup EXIT

# -------------------------------------------------------------------
# Parse tier argument
# -------------------------------------------------------------------
TIER="${1:-}"
case "$TIER" in
    1k)    BUCKET=bench-1k;    EXPECTED=1000    ;;
    10k)   BUCKET=bench-10k;   EXPECTED=10000   ;;
    100k)  BUCKET=bench-100k;  EXPECTED=100000  ;;
    250k)  BUCKET=bench-250k;  EXPECTED=250000  ;;
    *)
        echo "Usage: $0 <TIER>"
        echo "  TIER: 1k | 10k | 100k | 250k"
        exit 1
        ;;
esac

# -------------------------------------------------------------------
# Pre-flight
# -------------------------------------------------------------------
[ -x "$OBJSTRD" ]  || die "objstrd not found: $OBJSTRD"
[ -f "$SEED_PATH" ] || die "seed image not found: $SEED_PATH (run create_seed.sh first)"

echo "========================================"
echo "  Listing Benchmark -- tier=$TIER"
echo "  Bucket:     $BUCKET"
echo "  Expected:   $EXPECTED objects"
echo "  Iterations: $ITERATIONS"
echo "  Seed:       $SEED_PATH"
echo "========================================"
echo ""

# -------------------------------------------------------------------
# wait_ready PORT LABEL -- poll /_admin/info until server responds
# -------------------------------------------------------------------
wait_ready() {
    local port=$1 label=$2
    for _ in $(seq 1 120); do
        if curl -s --connect-timeout 2 --max-time 5 "http://localhost:${port}/_admin/info" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    die "$label did not start within 240s on port $port"
}

# -------------------------------------------------------------------
# count_objects PORT BUCKET -- full paginated count
# -------------------------------------------------------------------
count_objects() {
    local port=$1 bucket=$2
    local total=0 token=""
    while true; do
        local url="http://localhost:${port}/${bucket}?list-type=2&max-keys=1000"
        [ -n "$token" ] && url="${url}&continuation-token=${token}"
        local resp
        resp=$(curl -s "$url")
        local kc
        kc=$(echo "$resp" | grep -oP '<KeyCount>\K[0-9]+' || echo "0")
        total=$((total + kc))
        local trunc
        trunc=$(echo "$resp" | grep -c '<IsTruncated>true</IsTruncated>' || true)
        [ "$trunc" -eq 0 ] && break
        token=$(echo "$resp" | grep -oP '<NextContinuationToken>\K[^<]+')
    done
    echo "$total"
}

# -------------------------------------------------------------------
# bench_list PORT BUCKET -- time a full paginated listing (ms)
#   prints: OBJECT_COUNT PAGES ELAPSED_MS
# -------------------------------------------------------------------
bench_list() {
    local port=$1 bucket=$2
    local start_ns end_ns
    start_ns=$(date +%s%N)
    local total=0 pages=0 token=""

    while true; do
        local url="http://localhost:${port}/${bucket}?list-type=2&max-keys=1000"
        [ -n "$token" ] && url="${url}&continuation-token=${token}"
        local resp
        resp=$(curl -s "$url")
        local kc
        kc=$(echo "$resp" | grep -oP '<KeyCount>\K[0-9]+' || echo "0")
        total=$((total + kc))
        pages=$((pages + 1))
        local trunc
        trunc=$(echo "$resp" | grep -c '<IsTruncated>true</IsTruncated>' || true)
        [ "$trunc" -eq 0 ] && break
        token=$(echo "$resp" | grep -oP '<NextContinuationToken>\K[^<]+')
    done

    end_ns=$(date +%s%N)
    local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "$total $pages $elapsed_ms"
}

# -------------------------------------------------------------------
# run_bench LABEL PORT BUCKET -- run ITERATIONS listings, print results
#   returns results in RESULTS_MS array (space-separated string)
# -------------------------------------------------------------------
run_bench() {
    local label=$1 port=$2 bucket=$3
    echo "  Listing benchmark ($ITERATIONS iterations)..."
    local all_ms=""
    for i in $(seq 1 "$ITERATIONS"); do
        local result
        result=$(bench_list "$port" "$bucket")
        local obj_count pages ms
        obj_count=$(echo "$result" | awk '{print $1}')
        pages=$(echo "$result" | awk '{print $2}')
        ms=$(echo "$result" | awk '{print $3}')
        echo "    run $i: ${obj_count} objects, ${pages} pages, ${ms} ms"
        all_ms="${all_ms} ${ms}"
    done
    # compute median (sort and pick middle)
    local sorted
    sorted=$(echo "$all_ms" | tr ' ' '\n' | grep -v '^$' | sort -n)
    local mid=$(( (ITERATIONS + 1) / 2 ))
    local median
    median=$(echo "$sorted" | sed -n "${mid}p")
    echo "  => median: ${median} ms"
    # store result for final table
    eval "RESULT_${label}=${median}"
}

# -------------------------------------------------------------------
# import_from_seed DST_PORT BUCKET -- import objects from seed via S3
# -------------------------------------------------------------------
import_from_seed() {
    local dst_port=$1 bucket=$2
    echo "  Importing $bucket from seed (port $SEED_PORT -> $dst_port)..."

    # create bucket on target
    curl -s -o /dev/null -X PUT "http://localhost:${dst_port}/${bucket}"

    # generate 1-byte payload (content does not matter for listing)
    printf 'x' > "$PAYLOAD"

    local start_s
    start_s=$(date +%s)
    local total=0 token=""

    while true; do
        local url="http://localhost:${SEED_PORT}/${bucket}?list-type=2&max-keys=1000"
        [ -n "$token" ] && url="${url}&continuation-token=${token}"
        local resp
        resp=$(curl -s "$url")

        local keys
        keys=$(echo "$resp" | grep -oP '<Key>\K[^<]+' || true)

        if [ -n "$keys" ]; then
            local batch_n
            batch_n=$(echo "$keys" | wc -l)
            echo "$keys" | xargs -P "$PARALLEL" -I{} \
                curl -s -o /dev/null -X PUT \
                    --data-binary @"$PAYLOAD" \
                    "http://localhost:${dst_port}/${bucket}/{}"
            total=$((total + batch_n))
            echo "    $total objects..."
        fi

        local trunc
        trunc=$(echo "$resp" | grep -c '<IsTruncated>true</IsTruncated>' || true)
        [ "$trunc" -eq 0 ] && break
        token=$(echo "$resp" | grep -oP '<NextContinuationToken>\K[^<]+')
    done

    local elapsed=$(( $(date +%s) - start_s ))
    echo "  Import done: $total objects in ${elapsed}s"
}

# ===================================================================
# Start seed server (needed for fs and mem import)
# ===================================================================
echo "[seed] Starting seed objstrd on port $SEED_PORT..."
"$OBJSTRD" --image "$SEED_PATH" --bucket "$BUCKET" --port "$SEED_PORT" \
    --read-only >/dev/null 2>&1 &
SEED_PID=$!
wait_ready "$SEED_PORT" "seed objstrd"
echo "[seed] Ready (pid $SEED_PID)."
echo ""

# ===================================================================
# Backend 1: RAW -- copy seed image, open directly
# ===================================================================
RAW_IMG="/tmp/bench_raw_${TIER}.raw"
echo "--- [raw] Backend: raw store ---"
echo "  Copying seed image -> $RAW_IMG"
cp "$SEED_PATH" "$RAW_IMG"

echo "  Starting objstrd (raw) on port $TEST_PORT..."
"$OBJSTRD" --image "$RAW_IMG" --bucket "$BUCKET" --port "$TEST_PORT" \
    --read-only >/dev/null 2>&1 &
TEST_PID=$!
wait_ready "$TEST_PORT" "raw objstrd"

# verify count
RAW_COUNT=$(count_objects "$TEST_PORT" "$BUCKET")
echo "  Objects: $RAW_COUNT"

run_bench "RAW" "$TEST_PORT" "$BUCKET"

kill "$TEST_PID" 2>/dev/null; wait "$TEST_PID" 2>/dev/null || true
TEST_PID=""
rm -f "$RAW_IMG"
echo ""

# ===================================================================
# Backend 2: FS -- import from seed
# ===================================================================
FS_ROOT="/tmp/bench_fs_${TIER}"
echo "--- [fs] Backend: filesystem ---"
rm -rf "$FS_ROOT"
mkdir -p "$FS_ROOT"

echo "  Starting objstrd (fs) on port $TEST_PORT..."
"$OBJSTRD" --backend fs --image "$FS_ROOT" --bucket "$BUCKET" \
    --port "$TEST_PORT" >/dev/null 2>&1 &
TEST_PID=$!
wait_ready "$TEST_PORT" "fs objstrd"

import_from_seed "$TEST_PORT" "$BUCKET"

FS_COUNT=$(count_objects "$TEST_PORT" "$BUCKET")
echo "  Objects: $FS_COUNT"

run_bench "FS" "$TEST_PORT" "$BUCKET"

kill "$TEST_PID" 2>/dev/null; wait "$TEST_PID" 2>/dev/null || true
TEST_PID=""
rm -rf "$FS_ROOT"
echo ""

# ===================================================================
# Backend 3: MEM -- import from seed
# ===================================================================
echo "--- [mem] Backend: in-memory ---"
echo "  Starting objstrd (mem) on port $TEST_PORT..."
"$OBJSTRD" --backend mem --bucket "$BUCKET" \
    --port "$TEST_PORT" >/dev/null 2>&1 &
TEST_PID=$!
wait_ready "$TEST_PORT" "mem objstrd"

import_from_seed "$TEST_PORT" "$BUCKET"

MEM_COUNT=$(count_objects "$TEST_PORT" "$BUCKET")
echo "  Objects: $MEM_COUNT"

run_bench "MEM" "$TEST_PORT" "$BUCKET"

kill "$TEST_PID" 2>/dev/null; wait "$TEST_PID" 2>/dev/null || true
TEST_PID=""
