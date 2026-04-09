#!/bin/bash
#
# run_startup_repair.sh -- E2E test: under-replication repair after mem shard restart
#
# Scenario:
#   1. Start a 3-shard cluster (2 raw + 1 mem) with RF=2, recovery enabled
#   2. Put objects -- some replicas land on the mem shard
#   3. Restart the server -- mem shard data is lost
#   4. Verify the recovery loop detects under-replicated objects
#   5. Wait for auto-repair to restore RF
#   6. Verify all objects are still readable with correct data
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/s3.sh"

OBJSTRD_BIN="${OBJSTRD_BIN:-$HOME/build-objstrd/release/objstrd}"
PASS=0
FAIL=0
TOTAL=0

# -- helpers ----------------------------------------------------------

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "  FAIL: $1"; }
section() { echo ""; echo "=== [$1] ==="; }

assert_eq() {
    local got="$1" want="$2" msg="$3"
    if [ "$got" = "$want" ]; then
        pass "$msg"
    else
        fail "$msg (got='$got' want='$want')"
    fi
}

assert_ge() {
    local got="$1" want="$2" msg="$3"
    if [ "$got" -ge "$want" ] 2>/dev/null; then
        pass "$msg"
    else
        fail "$msg (got=$got want>=$want)"
    fi
}

json_field() {
    python3 -c "import sys,json; print(json.load(sys.stdin)$1)"
}

admin_get() {
    curl -sf "http://localhost:${PORT}/_admin/$1" 2>/dev/null
}

admin_post() {
    curl -sf -X POST "http://localhost:${PORT}/_admin/$1" 2>/dev/null
}

catalog_count() {
    admin_get "objects?limit=1" | json_field '["total"]'
}

wait_server() {
    local port="$1"
    local i=0
    while [ $i -lt 100 ]; do
        if curl -sf "http://localhost:${port}/_admin/info" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
        i=$((i + 1))
    done
    echo "ERROR: server on port $port did not start within 10s" >&2
    return 1
}

# -- cleanup ----------------------------------------------------------

PIDS_TO_KILL=""
LOG_FILE="/tmp/startup_repair_test.log"

cleanup() {
    for pid in $PIDS_TO_KILL; do
        kill "$pid" 2>/dev/null || true
    done
    rm -f /tmp/startup_repair_s0.raw /tmp/startup_repair_s1.raw /tmp/startup_repair.conf
    echo ""
    echo "Log file: $LOG_FILE"
}
trap cleanup EXIT

# =====================================================================
# Setup: 3-shard cluster (2 raw + 1 mem) RF=2
# =====================================================================

PORT=8350
IMG0="/tmp/startup_repair_s0.raw"
IMG1="/tmp/startup_repair_s1.raw"
CONF="/tmp/startup_repair.conf"

rm -f "$IMG0" "$IMG1" "$CONF" "$LOG_FILE"

cat > "$CONF" <<CONF
cluster    startup-repair-test
bucket     testbucket
size_mb    128

recovery_enabled                     true
recovery_poll_secs                   2
recovery_re_replicate_batch_size     200

primary  rf=2  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  raw  ${IMG0}  size_mb=128
  raw  ${IMG1}  size_mb=128
  mem
CONF

section "Test 1: Start cluster and populate with objects"

"$OBJSTRD_BIN" --config "$CONF" --node primary >>"$LOG_FILE" 2>&1 &
PIDS_TO_KILL="$!"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"
S3_BUCKET="testbucket"

# Put 30 objects
for i in $(seq 1 30); do
    s3_put "startup/obj_$(printf '%03d' $i)" "startup-data-$i"
done

UNIQUE=$(catalog_count)
assert_eq "$UNIQUE" "30" "30 objects stored"

# Record which objects exist on each shard
SHARD_INFO=$(admin_get "shards")
for sid in 0 1 2; do
    COUNT=$(echo "$SHARD_INFO" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data['shards']:
    if s['id'] == $sid:
        print(s['file_count'])
        break
")
    echo "  shard $sid file_count: $COUNT"
done

# Verify all objects readable before restart
BAD=0
for i in $(seq 1 30); do
    KEY="startup/obj_$(printf '%03d' $i)"
    GOT=$(s3_get "$KEY" || echo "FETCH_ERROR")
    WANT="startup-data-$i"
    if [ "$GOT" != "$WANT" ]; then
        BAD=$((BAD + 1))
    fi
done
assert_eq "$BAD" "0" "all 30 objects readable before restart"

# =====================================================================
# Test 2: Restart server -- mem shard is wiped
# =====================================================================

section "Test 2: Restart server (mem shard data lost)"

# Flush raw shard indexes to disk before killing
admin_post "flush" >/dev/null 2>&1 || true

# Kill the server with SIGINT for graceful shutdown
kill -INT $PIDS_TO_KILL 2>/dev/null || true
sleep 2
PIDS_TO_KILL=""

# Restart -- mem shard starts empty, raw shards retain data
"$OBJSTRD_BIN" --config "$CONF" --node primary >>"$LOG_FILE" 2>&1 &
PIDS_TO_KILL="$!"
wait_server "$PORT"

echo "  Server restarted; mem shard is now empty"

# =====================================================================
# Test 3: Verify recovery detects under-replication
# =====================================================================

section "Test 3: Recovery detects under-replicated objects"

# Give the recovery loop a couple of cycles to detect under-replication
sleep 5

RECOVERY=$(admin_get "recovery")
UNDER=$(echo "$RECOVERY" | json_field '["under_replicated_count"]')
POLL_CYCLES=$(echo "$RECOVERY" | json_field '["poll_cycles"]')
echo "  poll_cycles=$POLL_CYCLES  under_replicated_count=$UNDER"

# Even if the under_replicated_count has already been fixed by now (fast repair),
# total_re_replicated should be > 0 if objects were on the mem shard
TOTAL_RE=$(echo "$RECOVERY" | json_field '["total_re_replicated"]')
echo "  total_re_replicated=$TOTAL_RE"

# At least 1 poll cycle should have completed
assert_ge "$POLL_CYCLES" "1" "recovery loop has completed at least 1 poll cycle"

# =====================================================================
# Test 4: Wait for repair to finish and verify
# =====================================================================

section "Test 4: Wait for repair and verify data integrity"

# Wait up to 20 seconds for repair to complete
ATTEMPTS=0
while [ $ATTEMPTS -lt 20 ]; do
    UNDER=$(admin_get "recovery" | json_field '["under_replicated_count"]')
    if [ "$UNDER" = "0" ]; then
        break
    fi
    sleep 1
    ATTEMPTS=$((ATTEMPTS + 1))
done

RECOVERY=$(admin_get "recovery")
UNDER=$(echo "$RECOVERY" | json_field '["under_replicated_count"]')
TOTAL_RE=$(echo "$RECOVERY" | json_field '["total_re_replicated"]')
echo "  final: under_replicated_count=$UNDER  total_re_replicated=$TOTAL_RE"

assert_eq "$UNDER" "0" "no under-replicated objects after recovery"
assert_ge "$TOTAL_RE" "1" "at least 1 object was re-replicated"

# Verify all objects are still readable with correct data
BAD=0
for i in $(seq 1 30); do
    KEY="startup/obj_$(printf '%03d' $i)"
    GOT=$(s3_get "$KEY" || echo "FETCH_ERROR")
    WANT="startup-data-$i"
    if [ "$GOT" != "$WANT" ]; then
        BAD=$((BAD + 1))
        echo "  CORRUPT or LOST: $KEY (got='$GOT')"
    fi
done
assert_eq "$BAD" "0" "all 30 objects readable and correct after repair"

# Catalog count should still be 30
UNIQUE=$(catalog_count)
assert_eq "$UNIQUE" "30" "catalog still has 30 objects after repair"

# =====================================================================
# Test 5: Manual repair-replication confirms nothing left to fix
# =====================================================================

section "Test 5: Manual repair-replication shows clean state"

REPAIR=$(admin_post "repair-replication")
UNDER_REM=$(echo "$REPAIR" | json_field '["under_remaining"]')
echo "  repair-replication under_remaining=$UNDER_REM"
assert_eq "$UNDER_REM" "0" "repair-replication finds nothing left to repair"

# =====================================================================
# Summary
# =====================================================================

echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed (out of $TOTAL)"
echo "=============================="

if [ "$FAIL" -gt 0 ]; then
    echo "SOME TESTS FAILED"
    exit 1
fi
echo "ALL TESTS PASSED"
