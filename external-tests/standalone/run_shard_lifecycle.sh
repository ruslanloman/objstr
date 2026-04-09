#!/bin/bash
#
# run_shard_lifecycle.sh -- E2E tests for shard lifecycle admin operations
#
# Tests scenarios not covered by run_take_offline.sh or run_drain_repair_replication.sh:
#
#   1.  Vacuum after deletes removes markers and reclaims space
#   2.  Vacuum fails gracefully when a shard is offline
#   3.  Redistribute after drain re-balances surviving shards
#   4.  Admin-op-status reflects running/idle state
#   5.  Shard health transitions visible via /_admin/shards
#   6.  Double drain is idempotent (drain an already-drained shard)
#   7.  Delete + vacuum + re-put same key works correctly
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

assert_ne() {
    local got="$1" want="$2" msg="$3"
    if [ "$got" != "$want" ]; then
        pass "$msg"
    else
        fail "$msg (got='$got' should not equal '$want')"
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

assert_le() {
    local got="$1" want="$2" msg="$3"
    if [ "$got" -le "$want" ] 2>/dev/null; then
        pass "$msg"
    else
        fail "$msg (got=$got want<=$want)"
    fi
}

admin_get() {
    curl -sf "http://localhost:${PORT}/_admin/$1" 2>/dev/null
}

admin_post() {
    curl -sf -X POST "http://localhost:${PORT}/_admin/$1" 2>/dev/null
}

admin_post_status() {
    curl -so /dev/null -w "%{http_code}" -X POST "http://localhost:${PORT}/_admin/$1" 2>/dev/null
}

json_field() {
    python3 -c "import sys,json; print(json.load(sys.stdin)$1)"
}

catalog_count() {
    admin_get "objects?limit=1" | json_field '["total"]'
}

catalog_placement() {
    admin_get "objects?limit=500" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    shards = " ".join(str(s) for s in obj["shard_ids"])
    print("%s %s" % (obj["key"], shards))
'
}

shard_health() {
    local sid="$1"
    admin_get "shards" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data['shards']:
    if s['id'] == $sid:
        print(s['health'])
        break
"
}

shard_file_count() {
    local sid="$1"
    admin_get "shards" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data['shards']:
    if s['id'] == $sid:
        print(s['file_count'])
        break
"
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

cleanup() {
    echo ""
    echo "--- Cleanup ---"
    for pid in $PIDS_TO_KILL; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
    rm -f /tmp/lifecycle_test_*.raw /tmp/lifecycle_test_*.conf
    rm -rf /tmp/lifecycle_test_fs_*

    echo ""
    echo "==========================================="
    echo "  Results: $PASS passed, $FAIL failed (of $TOTAL)"
    echo "==========================================="
    if [ "$FAIL" -gt 0 ]; then
        exit 1
    fi
}

trap cleanup EXIT

# =====================================================================
# Setup: 4-shard cluster (mem x4) rf=2
# =====================================================================

section "Setup: 4-shard cluster (mem x4) rf=2"

PORT=8500
CONF="/tmp/lifecycle_test.conf"

rm -f "$CONF"

cat > "$CONF" <<CONF
cluster    lifecycle-test
bucket     testbucket
size_mb    64

primary  rf=2  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  mem
  mem
  mem
  mem
CONF

"$OBJSTRD_BIN" --config "$CONF" --node primary >/dev/null 2>&1 &
PIDS_TO_KILL="$!"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"
S3_BUCKET="testbucket"

s3_bucket_create

# Seed 30 objects
for i in $(seq 1 30); do
    s3_put "lc/obj_$(printf '%03d' $i)" "lifecycle-data-$i"
done

TOTAL_OBJS=$(catalog_count)
assert_eq "$TOTAL_OBJS" "30" "30 objects stored"

# =====================================================================
# Test 1: Vacuum after deletes removes markers
# =====================================================================

section "Test 1: Vacuum after deletes removes markers"

# Delete 10 objects
for i in $(seq 1 10); do
    s3_delete "lc/obj_$(printf '%03d' $i)"
done

# Verify they are gone via S3
for i in 1 5 10; do
    STATUS=$(s3_head "lc/obj_$(printf '%03d' $i)")
    assert_eq "$STATUS" "404" "deleted object $i returns 404"
done

# Remaining count
AFTER_DELETE=$(catalog_count)
assert_eq "$AFTER_DELETE" "20" "20 objects after deleting 10"

# Run vacuum
VACUUM_RESULT=$(admin_post "vacuum")
VACUUM_OK=$(echo "$VACUUM_RESULT" | json_field '["ok"]')
assert_eq "$VACUUM_OK" "True" "vacuum returned ok"

PURGED=$(echo "$VACUUM_RESULT" | json_field '["purged"]')
assert_ge "$PURGED" "1" "vacuum purged at least 1 delete marker (got $PURGED)"

# Deleted objects still 404 after vacuum
for i in 1 5 10; do
    STATUS=$(s3_head "lc/obj_$(printf '%03d' $i)")
    assert_eq "$STATUS" "404" "deleted object $i still 404 after vacuum"
done

# Surviving objects intact
for i in 11 20 30; do
    GOT=$(s3_get "lc/obj_$(printf '%03d' $i)")
    assert_eq "$GOT" "lifecycle-data-$i" "surviving object $i data correct after vacuum"
done

# =====================================================================
# Test 2: Vacuum fails gracefully when shard offline
# =====================================================================

section "Test 2: Vacuum fails when shard offline"

admin_post "take-offline/0" >/dev/null

H0=$(shard_health 0)
assert_eq "$H0" "Detached" "shard 0 is Detached"

VACUUM_STATUS=$(admin_post_status "vacuum")
assert_eq "$VACUUM_STATUS" "409" "vacuum returns 409 Conflict when shard offline"

# Re-attach for subsequent tests
admin_post "attach/0" >/dev/null
sleep 2
H0=$(shard_health 0)
# Health may be Healthy or Syncing briefly
assert_ne "$H0" "Detached" "shard 0 no longer Detached after attach"

# =====================================================================
# Test 3: Redistribute after drain re-balances
# =====================================================================

section "Test 3: Redistribute after drain re-balances"

# First drain shard 3 to create imbalance on remaining shards
DRAIN_RESULT=$(admin_post "drain/3")
DRAIN_ERRORS=$(echo "$DRAIN_RESULT" | json_field '["errors"]')
assert_eq "$DRAIN_ERRORS" "0" "drain shard 3 no errors"

H3=$(shard_health 3)
assert_eq "$H3" "Detached" "shard 3 is Detached after drain"

# All data still accessible
BAD=0
for i in $(seq 11 30); do
    STATUS=$(s3_head "lc/obj_$(printf '%03d' $i)")
    if [ "$STATUS" != "200" ]; then
        BAD=$((BAD + 1))
    fi
done
assert_eq "$BAD" "0" "all 20 objects accessible after drain"

# Run redistribute on surviving shards (0,1,2)
REDIST_RESULT=$(admin_post "redistribute")
REDIST_OK=$(echo "$REDIST_RESULT" | json_field '["ok"]')
REDIST_ERRORS=$(echo "$REDIST_RESULT" | json_field '["errors"]')
assert_eq "$REDIST_OK" "True" "redistribute returned ok"
assert_eq "$REDIST_ERRORS" "0" "redistribute no errors"

# Shard counts should be roughly balanced across shards 0,1,2
for sid in 0 1 2; do
    COUNT=$(catalog_placement | python3 -c "
import sys
count = 0
for line in sys.stdin:
    parts = line.strip().split()
    if '$sid' in parts[1:]:
        count += 1
print(count)
")
    assert_ge "$COUNT" "5" "shard $sid has >= 5 objects after redistribute (got $COUNT)"
done

# All data still intact
BAD=0
for i in $(seq 11 30); do
    GOT=$(s3_get "lc/obj_$(printf '%03d' $i)")
    WANT="lifecycle-data-$i"
    if [ "$GOT" != "$WANT" ]; then
        BAD=$((BAD + 1))
    fi
done
assert_eq "$BAD" "0" "all objects have correct data after redistribute"

# =====================================================================
# Test 4: Admin-op-status reflects idle state
# =====================================================================

section "Test 4: Admin-op-status"

OP_STATUS=$(admin_get "admin-op-status")
OP_RUNNING=$(echo "$OP_STATUS" | json_field '["running"]')
assert_eq "$OP_RUNNING" "False" "admin-op-status idle after operations complete"

# =====================================================================
# Test 5: Shard health transitions visible via /_admin/shards
# =====================================================================

section "Test 5: Shard health transitions"

# Shard 3 should still be Detached from the drain
H3=$(shard_health 3)
assert_eq "$H3" "Detached" "shard 3 still Detached"

# Shards 0,1,2 should be Healthy
for sid in 0 1 2; do
    H=$(shard_health $sid)
    assert_eq "$H" "Healthy" "shard $sid is Healthy"
done

# Take shard 1 offline manually
admin_post "take-offline/1" >/dev/null
H1=$(shard_health 1)
assert_eq "$H1" "Detached" "shard 1 Detached after take-offline"

# Reattach shard 1
admin_post "attach/1" >/dev/null
sleep 2

# After reattach, shard 1 should eventually be Healthy (or Syncing)
H1=$(shard_health 1)
assert_ne "$H1" "Detached" "shard 1 no longer Detached after attach"

# =====================================================================
# Test 6: Double drain is idempotent
# =====================================================================

section "Test 6: Double drain is idempotent"

# Shard 3 is already Detached from test 3
H3=$(shard_health 3)
assert_eq "$H3" "Detached" "shard 3 is Detached (pre-condition)"

# Drain shard 3 again -- should return quickly with 0 moved
DRAIN2_RESULT=$(admin_post "drain/3")
if [ -n "$DRAIN2_RESULT" ]; then
    DRAIN2_MOVED=$(echo "$DRAIN2_RESULT" | json_field '["moved"]')
    DRAIN2_ERRORS=$(echo "$DRAIN2_RESULT" | json_field '["errors"]')
    assert_eq "$DRAIN2_MOVED" "0" "second drain moves 0 objects"
    assert_eq "$DRAIN2_ERRORS" "0" "second drain no errors"
else
    # If endpoint returns error for already-detached shard, that is also acceptable
    pass "drain of already-detached shard handled gracefully"
fi

# Data still intact
TOTAL_AFTER=$(catalog_count)
assert_eq "$TOTAL_AFTER" "20" "still 20 objects after double drain"

# =====================================================================
# Test 7: Delete + vacuum + re-put same key works
# =====================================================================

section "Test 7: Delete + vacuum + re-put same key"

KEY="lc/obj_011"

# Verify object exists
GOT=$(s3_get "$KEY")
assert_eq "$GOT" "lifecycle-data-11" "object exists before delete"

# Delete it
s3_delete "$KEY"
STATUS=$(s3_head "$KEY")
assert_eq "$STATUS" "404" "object 404 after delete"

# Vacuum to clean up markers
admin_post "vacuum" >/dev/null

# Re-put with new data
s3_put "$KEY" "rewritten-data-11"
GOT2=$(s3_get "$KEY")
assert_eq "$GOT2" "rewritten-data-11" "re-put same key has new data"

# Verify it appears in catalog
STATUS2=$(s3_head "$KEY")
assert_eq "$STATUS2" "200" "re-put object returns 200"

# =====================================================================
# Done
# =====================================================================

echo ""
echo "All tests complete."
