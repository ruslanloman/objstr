#!/bin/bash
# create_seed.sh -- Create a raw object store image with benchmark seed data.
#
# Creates 4 buckets inside one raw image:
#   bench-1k     (1,000 objects, 1 byte each)
#   bench-10k    (10,000 objects, 1 byte each)
#   bench-100k   (100,000 objects, 1 byte each)
#   bench-250k   (250,000 objects, 1 byte each)
#
# The seed image can be backed up and reused to populate other backends.
#
# Usage:  ./create_seed.sh [SEED_PATH]
#         SEED_PATH defaults to /tmp/bench_seed.raw
#
# Requires: objstrd, rawobjstr (pre-built release binaries)

set -euo pipefail

OBJSTRD="${OBJSTRD:-~/build-objstrd/release/objstrd}"
RAWOBJSTR="${RAWOBJSTR:-~/build-rawobjstr/release/rawobjstr}"
SEED_PATH="${1:-/tmp/bench_seed.raw}"
PORT=9100
PARALLEL=64
PAYLOAD=/tmp/bench_payload_1b

# 361k objects * ~8KB on disk = ~2.9GB data, index needs ~128MB
DEVICE_SIZE=$((4 * 1024 * 1024 * 1024))       # 4 GB
INDEX_SLOT_SIZE=$((128 * 1024 * 1024))         # 128 MB (multiple of 16MB)

die() { echo "FATAL: $*" >&2; exit 1; }

cleanup() {
    if [ -n "${OBJSTRD_PID:-}" ]; then
        kill "$OBJSTRD_PID" 2>/dev/null || true
        wait "$OBJSTRD_PID" 2>/dev/null || true
    fi
    rm -f "$PAYLOAD"
}
trap cleanup EXIT

# -------------------------------------------------------------------
# Pre-flight checks
# -------------------------------------------------------------------
[ -x "$OBJSTRD" ]  || die "objstrd not found at $OBJSTRD"
[ -x "$RAWOBJSTR" ] || die "rawobjstr not found at $RAWOBJSTR"

if [ -f "$SEED_PATH" ]; then
    echo "Seed image already exists: $SEED_PATH"
    echo "Remove it first if you want to recreate."
    exit 1
fi

echo "========================================"
echo "  Seed image creation"
echo "  Path:       $SEED_PATH"
echo "  Device:     $((DEVICE_SIZE / 1024 / 1024)) MB"
echo "  Index slot: $((INDEX_SLOT_SIZE / 1024 / 1024)) MB"
echo "  Parallel:   $PARALLEL"
echo "========================================"
echo ""

# -------------------------------------------------------------------
# 1. Format raw image with large index for 1M+ objects
# -------------------------------------------------------------------
echo "[1/5] Formatting raw image..."
"$RAWOBJSTR" format \
    --file "$SEED_PATH" \
    --size "$DEVICE_SIZE" \
    --index-slot-size "$INDEX_SLOT_SIZE"
echo "  done."

# -------------------------------------------------------------------
# 2. Generate 1-byte payload
# -------------------------------------------------------------------
printf 'x' > "$PAYLOAD"

# -------------------------------------------------------------------
# 3. Start objstrd on the seed image
# -------------------------------------------------------------------
echo "[2/5] Starting objstrd on port $PORT..."
"$OBJSTRD" --image "$SEED_PATH" --bucket bench-1k --port "$PORT" >/dev/null 2>&1 &
OBJSTRD_PID=$!

for i in $(seq 1 60); do
    if curl -s --connect-timeout 2 --max-time 5 "http://localhost:${PORT}/_admin/info" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
curl -s --connect-timeout 2 --max-time 5 "http://localhost:${PORT}/_admin/info" >/dev/null 2>&1 \
    || die "objstrd did not start within 120s"
echo "  ready (pid $OBJSTRD_PID)."

# Create the other buckets
curl -s -o /dev/null -X PUT "http://localhost:${PORT}/bench-10k"
curl -s -o /dev/null -X PUT "http://localhost:${PORT}/bench-100k"
curl -s -o /dev/null -X PUT "http://localhost:${PORT}/bench-250k"
echo "  buckets: bench-1k  bench-10k  bench-100k  bench-250k"

# -------------------------------------------------------------------
# populate BUCKET COUNT -- PUT COUNT objects into BUCKET
# -------------------------------------------------------------------
populate() {
    local bucket=$1
    local count=$2
    echo ""
    echo "[populate] $bucket -- $count objects"
    local start_s
    start_s=$(date +%s)
    local batch=10000
    local done_n=0

    while [ "$done_n" -lt "$count" ]; do
        local remain=$((count - done_n))
        local n=$batch
        if [ "$remain" -lt "$batch" ]; then n=$remain; fi
        local from=$((done_n + 1))
        local to=$((done_n + n))

        seq "$from" "$to" \
            | awk '{printf "obj_%07d\n", $1}' \
            | xargs -P "$PARALLEL" -I{} \
                curl -s -o /dev/null -X PUT \
                    --data-binary @"$PAYLOAD" \
                    "http://localhost:${PORT}/${bucket}/{}"

        done_n=$((done_n + n))
        local elapsed=$(( $(date +%s) - start_s ))
        local rate=0
        if [ "$elapsed" -gt 0 ]; then rate=$((done_n / elapsed)); fi
        echo "  $done_n / $count  (${rate} obj/s, ${elapsed}s elapsed)"
    done
}

# -------------------------------------------------------------------
# 4. Populate all three tiers (smallest first)
# -------------------------------------------------------------------
echo ""
echo "[3/5] Populating buckets..."
populate bench-1k    1000
populate bench-10k   10000
populate bench-100k  100000
populate bench-250k  250000

# -------------------------------------------------------------------
# count_objects BUCKET -- full paginated count
# -------------------------------------------------------------------
count_objects() {
    local bucket=$1
    local total=0
    local token=""
    while true; do
        local url="http://localhost:${PORT}/${bucket}?list-type=2&max-keys=1000"
        if [ -n "$token" ]; then
            url="${url}&continuation-token=${token}"
        fi
        local resp
        resp=$(curl -s "$url")
        local kc
        kc=$(echo "$resp" | grep -oP '<KeyCount>\K[0-9]+' || echo "0")
        total=$((total + kc))
        local trunc
        trunc=$(echo "$resp" | grep -c '<IsTruncated>true</IsTruncated>' || true)
        if [ "$trunc" -eq 0 ]; then break; fi
        token=$(echo "$resp" | grep -oP '<NextContinuationToken>\K[^<]+')
    done
    echo "$total"
}

# -------------------------------------------------------------------
# 5. Verify object counts
# -------------------------------------------------------------------
echo ""
echo "[4/5] Verifying object counts..."
C1K=$(count_objects bench-1k)
C10K=$(count_objects bench-10k)
C100K=$(count_objects bench-100k)
C250K=$(count_objects bench-250k)
echo "  bench-1k:   $C1K"
echo "  bench-10k:  $C10K"
echo "  bench-100k: $C100K"
echo "  bench-250k: $C250K"

EXPECT_TOTAL=$((1000 + 10000 + 100000 + 250000))
ACTUAL_TOTAL=$((C1K + C10K + C100K + C250K))
if [ "$ACTUAL_TOTAL" -ne "$EXPECT_TOTAL" ]; then
    echo "  WARNING: expected $EXPECT_TOTAL total, got $ACTUAL_TOTAL"
fi

# -------------------------------------------------------------------
# 6. Stop objstrd (flushes index to disk)
# -------------------------------------------------------------------
echo ""
echo "[5/5] Flushing index and stopping objstrd..."
curl -s -o /dev/null -X POST "http://localhost:${PORT}/_admin/flush"
echo "  flush done."
kill "$OBJSTRD_PID" 2>/dev/null
wait "$OBJSTRD_PID" 2>/dev/null || true
OBJSTRD_PID=""
echo "  stopped."
