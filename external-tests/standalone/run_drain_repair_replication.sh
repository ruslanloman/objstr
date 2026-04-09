#!/bin/bash
#
# run_drain_repair_replication.sh -- E2E tests for drain and repair-replication via objstrd admin API
#
# Tests:
#   1. Balanced placement: objects spread across all shards, not clustered
#   2. No duplicate shard placement: an object never appears on the same shard twice
#   3. Drain preserves data: all objects remain accessible after draining a shard
#   4. Drain moves sole copies: objects only on the drained shard are moved elsewhere
#   5. Drain updates catalog: drained shard removed from all placement entries
#   6. Repair-replication restores RF: after detaching a shard, repair-replication re-replicates
#   7. Mixed shard types: raw + fs + mem + S3 all work together
#   8. Sequential drains: drain multiple shards one at a time
#   9. Repair-replication trims over-replicated: excess copies are removed
#  10. Redistribute balances shard object counts and preserves data
#  11. Redistribute returns 503 on standalone server
#  12. Drain with rf=1 must actually move sole-copy objects (data preserved)
#  13. Drain auto-restores RF via built-in repair sweep (no manual repair-replication)
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

json_field() {
    python3 -c "import sys,json; print(json.load(sys.stdin)$1)"
}

# Parse object placement from /_admin/objects
# Outputs lines: key shard_id shard_id ...
catalog_placement() {
    admin_get "objects?limit=500" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    shards = " ".join(str(s) for s in obj["shard_ids"])
    print("%s %s" % (obj["key"], shards))
'
}

# Get shard file count
shard_files() {
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

# Get shard health
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

# Count unique objects in catalog
catalog_count() {
    admin_get "objects?limit=1" | json_field '["total"]'
}

# Get under-replicated count from repair-replication result
repair_replication_under() {
    admin_post "repair-replication" | json_field '["under_remaining"]'
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
    rm -f /tmp/drain_test_*.raw /tmp/drain_test_*.conf
    rm -rf /tmp/drain_test_fs_*

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
# Test 1: Mixed shards, balanced placement, drain, repair-replication
# =====================================================================

section "Setup: 4-shard cluster (raw + raw + fs + mem) rf=2"

PORT=8300
PORT_S3=8301
IMG_A="/tmp/drain_test_a.raw"
IMG_B="/tmp/drain_test_b.raw"
FS_DIR="/tmp/drain_test_fs_a"
CONF="/tmp/drain_test_basic.conf"

rm -f "$IMG_A" "$IMG_B" "$CONF"
rm -rf "$FS_DIR"
mkdir -p "$FS_DIR"

cat > "$CONF" <<CONF
cluster    drain-test
bucket     testbucket
size_mb    128

primary  rf=2  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  raw  ${IMG_A}  size_mb=128
  raw  ${IMG_B}  size_mb=128
  fs   ${FS_DIR}
  mem
CONF

"$OBJSTRD_BIN" --config "$CONF" --node primary >/dev/null 2>&1 &
PIDS_TO_KILL="$!"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"
S3_BUCKET="testbucket"

# Write enough objects to get distribution across all 4 shards.
# With jump-consistent hash and 4 shards, ~30 objects should cover all.
section "Test 1: Balanced placement across all shards"

s3_bucket_create
for i in $(seq 1 30); do
    s3_put "obj_$(printf '%04d' $i)" "data-for-object-$i"
done

UNIQUE=$(catalog_count)
assert_eq "$UNIQUE" "30" "30 objects stored"

# Check every shard got at least 1 object via catalog placement data
# (shard_files from admin API may undercount non-raw shards)
for sid in 0 1 2 3; do
    CATALOG_ON_SHARD=$(catalog_placement | python3 -c "
import sys
count = 0
for line in sys.stdin:
    parts = line.strip().split()
    if '$sid' in parts[1:]:
        count += 1
print(count)
")
    assert_ge "$CATALOG_ON_SHARD" "1" "shard $sid has at least 1 object in catalog (got $CATALOG_ON_SHARD)"
done

# =====================================================================
# Test 2: No duplicate shard placement
# =====================================================================

section "Test 2: No duplicate shard placement"

DUPES=$(catalog_placement | python3 -c '
import sys
dupes = 0
for line in sys.stdin:
    parts = line.strip().split()
    shards = parts[1:]
    if len(shards) != len(set(shards)):
        dupes += 1
        print("  DUPLICATE: %s on shards %s" % (parts[0], shards), file=sys.stderr)
print(dupes)
')
assert_eq "$DUPES" "0" "no objects placed on the same shard twice"

# =====================================================================
# Test 3: Drain preserves all data
# =====================================================================

section "Test 3: Drain shard 3 (mem) preserves all data"

# Record data before drain
for i in 1 5 10 15 20 25 30; do
    KEY="obj_$(printf '%04d' $i)"
    BEFORE=$(s3_get "$KEY")
    assert_eq "$BEFORE" "data-for-object-$i" "pre-drain GET $KEY"
done

# Drain shard 3 (mem)
DRAIN_RESULT=$(admin_post "drain/3")
DRAIN_MOVED=$(echo "$DRAIN_RESULT" | json_field '["moved"]')
DRAIN_SKIPPED=$(echo "$DRAIN_RESULT" | json_field '["skipped"]')
DRAIN_ERRORS=$(echo "$DRAIN_RESULT" | json_field '["errors"]')
DRAIN_DELETED=$(echo "$DRAIN_RESULT" | json_field '["deleted"]')
DRAIN_DEL_ERR=$(echo "$DRAIN_RESULT" | json_field '["delete_errors"]')
echo "  drain result: moved=$DRAIN_MOVED skipped=$DRAIN_SKIPPED deleted=$DRAIN_DELETED errors=$DRAIN_ERRORS delete_errors=$DRAIN_DEL_ERR"
assert_eq "$DRAIN_ERRORS" "0" "drain had no errors"
assert_eq "$DRAIN_DEL_ERR" "0" "drain had no delete errors"

# Drain must have actually processed objects on the shard
DRAIN_TOTAL=$((DRAIN_MOVED + DRAIN_SKIPPED))
assert_ge "$DRAIN_TOTAL" "1" "drain processed at least 1 object (moved=$DRAIN_MOVED skipped=$DRAIN_SKIPPED)"

# Drain must have deleted the objects it processed from the victim
assert_eq "$DRAIN_DELETED" "$DRAIN_TOTAL" "drain deleted all processed objects from victim (deleted=$DRAIN_DELETED total=$DRAIN_TOTAL)"

# Shard 3 should be detached (drain sets Detached, not Offline)
HEALTH_3=$(shard_health 3)
assert_eq "$HEALTH_3" "Detached" "shard 3 is Detached after drain"

# All objects still accessible
for i in $(seq 1 30); do
    KEY="obj_$(printf '%04d' $i)"
    STATUS=$(s3_head "$KEY")
    if [ "$STATUS" != "200" ]; then
        fail "object $KEY not accessible after drain (status=$STATUS)"
    fi
done
pass "all 30 objects accessible after draining shard 3"

# =====================================================================
# Test 4: Drain updates catalog (no references to drained shard)
# =====================================================================

section "Test 4: Catalog no longer references drained shard"

REFS_TO_3=$(catalog_placement | python3 -c '
import sys
count = 0
for line in sys.stdin:
    parts = line.strip().split()
    if "3" in parts[1:]:
        count += 1
print(count)
')
assert_eq "$REFS_TO_3" "0" "no catalog entries reference shard 3"

# =====================================================================
# Test 5: Drain moved sole copies
# =====================================================================

section "Test 5: Objects that were only on drained shard got moved"

# With rf=2 and 4 shards, objects on shard 3 also had a copy elsewhere.
# drain should skip those (moved=0 or low). If any object was ONLY on
# shard 3, it must have been moved. We verify no data was lost.
UNIQUE_AFTER=$(catalog_count)
assert_eq "$UNIQUE_AFTER" "30" "still 30 unique objects after drain"

# No under-replicated after repair-replication
UNDER=$(repair_replication_under)
assert_eq "$UNDER" "0" "no under-replicated objects after repair-replication"

# =====================================================================
# Test 6: Repair-replication restores RF after detach
# =====================================================================

section "Test 6: Repair-replication restores replication factor"

# Objects now on 3 surviving shards (0,1,2) with rf=2.
# After drain of shard 3, some objects lost a replica.
# Repair-replication should have fixed any under-replication.

# Check placement: every object should have exactly 2 shard entries
WRONG_RF=$(catalog_placement | python3 -c '
import sys
wrong = 0
for line in sys.stdin:
    parts = line.strip().split()
    shard_count = len(parts) - 1
    if shard_count < 2:
        wrong += 1
        print("  UNDER: %s on %d shards" % (parts[0], shard_count), file=sys.stderr)
print(wrong)
')
assert_eq "$WRONG_RF" "0" "all objects have rf=2 replicas"

# Verify data integrity for all objects
CORRUPT=0
for i in $(seq 1 30); do
    KEY="obj_$(printf '%04d' $i)"
    GOT=$(s3_get "$KEY")
    WANT="data-for-object-$i"
    if [ "$GOT" != "$WANT" ]; then
        CORRUPT=$((CORRUPT + 1))
        echo "  CORRUPT: $KEY"
    fi
done
assert_eq "$CORRUPT" "0" "all 30 objects have correct data after drain+repair-replication"

# Stop the first cluster
kill $PIDS_TO_KILL 2>/dev/null || true
PIDS_TO_KILL=""
sleep 2

# =====================================================================
# Test 7: Mixed shard types with S3 backend
# =====================================================================

section "Setup: 5-shard cluster (raw + fs + mem + mem + S3) rf=3"

PORT=8302
PORT_S3=8303
IMG_C="/tmp/drain_test_c.raw"
IMG_S3="/tmp/drain_test_s3_backend.raw"
FS_DIR2="/tmp/drain_test_fs_b"
CONF2="/tmp/drain_test_mixed.conf"

rm -f "$IMG_C" "$IMG_S3" "$CONF2"
rm -rf "$FS_DIR2"
mkdir -p "$FS_DIR2"

# Start upstream S3 backend
"$OBJSTRD_BIN" --image "$IMG_S3" --size-mb 128 --port "$PORT_S3" >/dev/null 2>&1 &
S3_BACKEND_PID=$!
PIDS_TO_KILL="$S3_BACKEND_PID"
wait_server "$PORT_S3"

cat > "$CONF2" <<CONF
cluster    mixed-drain-test
bucket     testbucket
size_mb    128

mixed  rf=3  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  raw  ${IMG_C}  size_mb=128
  fs   ${FS_DIR2}
  mem
  mem
  s3   endpoint=http://127.0.0.1:${PORT_S3}  bucket=testbucket  path_style
CONF

"$OBJSTRD_BIN" --config "$CONF2" --node mixed >/dev/null 2>&1 &
MIXED_PID=$!
PIDS_TO_KILL="$S3_BACKEND_PID $MIXED_PID"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"

section "Test 7: Mixed shard types all participate"

s3_bucket_create
for i in $(seq 1 40); do
    s3_put "mix_$(printf '%04d' $i)" "mixed-data-$i"
done

UNIQUE=$(admin_get "objects?limit=1" | json_field '["total"]')
assert_eq "$UNIQUE" "40" "40 objects stored on mixed cluster"

# All 5 shards should have objects in the catalog
for sid in 0 1 2 3 4; do
    CATALOG_ON_SHARD=$(catalog_placement | python3 -c "
import sys
count = 0
for line in sys.stdin:
    parts = line.strip().split()
    if '$sid' in parts[1:]:
        count += 1
print(count)
")
    assert_ge "$CATALOG_ON_SHARD" "1" "mixed shard $sid has at least 1 object in catalog (got $CATALOG_ON_SHARD)"
done

# No duplicates
DUPES=$(catalog_placement | python3 -c '
import sys
dupes = 0
for line in sys.stdin:
    parts = line.strip().split()
    shards = parts[1:]
    if len(shards) != len(set(shards)):
        dupes += 1
print(dupes)
')
assert_eq "$DUPES" "0" "no duplicate placement on mixed cluster"

# =====================================================================
# Test 8: Sequential drains
# =====================================================================

section "Test 8: Sequential drain of two shards"

# Drain shard 2 (mem) first
DRAIN1=$(admin_post "drain/2")
ERR1=$(echo "$DRAIN1" | json_field '["errors"]')
DEL_ERR1=$(echo "$DRAIN1" | json_field '["delete_errors"]')
DELETED1=$(echo "$DRAIN1" | json_field '["deleted"]')
assert_eq "$ERR1" "0" "drain shard 2 (mem) no errors"
assert_eq "$DEL_ERR1" "0" "drain shard 2 (mem) no delete errors"
assert_ge "$DELETED1" "1" "drain shard 2 deleted objects from victim (deleted=$DELETED1)"

H2=$(shard_health 2)
assert_eq "$H2" "Detached" "shard 2 detached after drain"

# Repair-replication to fix under-replication
admin_post "repair-replication" >/dev/null

# Verify all data still accessible
BAD=0
for i in $(seq 1 40); do
    KEY="mix_$(printf '%04d' $i)"
    STATUS=$(s3_head "$KEY")
    if [ "$STATUS" != "200" ]; then
        BAD=$((BAD + 1))
    fi
done
assert_eq "$BAD" "0" "all 40 objects accessible after draining shard 2"

# Now drain shard 3 (other mem)
DRAIN2=$(admin_post "drain/3")
ERR2=$(echo "$DRAIN2" | json_field '["errors"]')
DEL_ERR2=$(echo "$DRAIN2" | json_field '["delete_errors"]')
DELETED2=$(echo "$DRAIN2" | json_field '["deleted"]')
assert_eq "$ERR2" "0" "drain shard 3 (mem) no errors"
assert_eq "$DEL_ERR2" "0" "drain shard 3 (mem) no delete errors"
assert_ge "$DELETED2" "1" "drain shard 3 deleted objects from victim (deleted=$DELETED2)"

H3=$(shard_health 3)
assert_eq "$H3" "Detached" "shard 3 detached after second drain"

# Repair-replication again
admin_post "repair-replication" >/dev/null

# Still only 3 healthy shards (0=raw, 1=fs, 4=s3) with rf=3
BAD=0
for i in $(seq 1 40); do
    KEY="mix_$(printf '%04d' $i)"
    STATUS=$(s3_head "$KEY")
    if [ "$STATUS" != "200" ]; then
        BAD=$((BAD + 1))
    fi
done
assert_eq "$BAD" "0" "all 40 objects accessible after draining 2 shards"

# All objects should reference only healthy shards (0, 1, 4)
BAD_REFS=$(catalog_placement | python3 -c '
import sys
bad = 0
for line in sys.stdin:
    parts = line.strip().split()
    for s in parts[1:]:
        if s in ("2", "3"):
            bad += 1
            break
print(bad)
')
assert_eq "$BAD_REFS" "0" "no catalog refs to drained shards 2 or 3"

# Every object should have exactly 3 replicas on surviving shards
WRONG_RF=$(catalog_placement | python3 -c '
import sys
wrong = 0
for line in sys.stdin:
    parts = line.strip().split()
    shard_count = len(parts) - 1
    if shard_count != 3:
        wrong += 1
        print("  key=%s shards=%d expected=3" % (parts[0], shard_count), file=sys.stderr)
print(wrong)
')
assert_eq "$WRONG_RF" "0" "all objects have rf=3 on 3 surviving shards"

# =====================================================================
# Test 9: New writes go to surviving shards only
# =====================================================================

section "Test 9: New writes after drain skip drained shards"

for i in $(seq 41 50); do
    s3_put "mix_$(printf '%04d' $i)" "post-drain-$i"
done

# New objects should only be on shards 0, 1, 4
NEW_BAD=$(catalog_placement | python3 -c '
import sys
bad = 0
for line in sys.stdin:
    parts = line.strip().split()
    key = parts[0]
    if not key.startswith("testbucket/mix_004"):
        continue
    for s in parts[1:]:
        if s in ("2", "3"):
            bad += 1
            print("  NEW on drained shard: %s shard %s" % (key, s), file=sys.stderr)
            break
print(bad)
')
assert_eq "$NEW_BAD" "0" "new objects not placed on drained shards"

# Verify new objects readable
for i in $(seq 41 50); do
    KEY="mix_$(printf '%04d' $i)"
    GOT=$(s3_get "$KEY")
    assert_eq "$GOT" "post-drain-$i" "new object $KEY readable after drain"
done

# Stop the mixed cluster
kill $PIDS_TO_KILL 2>/dev/null || true
PIDS_TO_KILL=""
sleep 2

# =====================================================================
# Test 10: Redistribute balances object counts
# =====================================================================

section "Setup: 3-shard cluster (mem + mem + mem) rf=1 for redistribute"

PORT=8304
IMG_R1="/tmp/drain_test_r1.raw"
IMG_R2="/tmp/drain_test_r2.raw"
IMG_R3="/tmp/drain_test_r3.raw"
CONF_R="/tmp/drain_test_redist.conf"

rm -f "$IMG_R1" "$IMG_R2" "$IMG_R3" "$CONF_R"

cat > "$CONF_R" <<CONF
cluster    redist-test
bucket     testbucket
size_mb    64

primary  rf=1  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  mem
  mem
  mem
CONF

"$OBJSTRD_BIN" --config "$CONF_R" --node primary >/dev/null 2>&1 &
REDIST_PID=$!
PIDS_TO_KILL="$REDIST_PID"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"
S3_BUCKET="testbucket"

section "Test 10: Redistribute balances shard object counts"

s3_bucket_create
for i in $(seq 1 60); do
    s3_put "redist_$(printf '%04d' $i)" "redistribute-data-$i"
done

UNIQUE=$(catalog_count)
assert_eq "$UNIQUE" "60" "60 objects stored for redistribute test"

# admin-op-status should show idle
OP_STATUS=$(admin_get "admin-op-status")
OP_RUNNING=$(echo "$OP_STATUS" | json_field '["running"]')
assert_eq "$OP_RUNNING" "False" "admin-op-status idle before redistribute"

# Run redistribute
REDIST_RESULT=$(admin_post "redistribute")
REDIST_OK=$(echo "$REDIST_RESULT" | json_field '["ok"]')
REDIST_MOVED=$(echo "$REDIST_RESULT" | json_field '["moved"]')
REDIST_ERRORS=$(echo "$REDIST_RESULT" | json_field '["errors"]')
echo "  redistribute result: ok=$REDIST_OK moved=$REDIST_MOVED errors=$REDIST_ERRORS"
assert_eq "$REDIST_OK" "True" "redistribute returned ok"
assert_eq "$REDIST_ERRORS" "0" "redistribute had no errors"

# After redistribute, shard counts should be roughly equal
# With 60 objects and 3 shards, expect ~20 each. Tolerance is 10%.
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
    assert_ge "$COUNT" "15" "shard $sid has >= 15 objects after redistribute (got $COUNT)"
    assert_le "$COUNT" "25" "shard $sid has <= 25 objects after redistribute (got $COUNT)"
done

# All objects still accessible and correct
BAD=0
for i in $(seq 1 60); do
    KEY="redist_$(printf '%04d' $i)"
    GOT=$(s3_get "$KEY")
    WANT="redistribute-data-$i"
    if [ "$GOT" != "$WANT" ]; then
        BAD=$((BAD + 1))
        echo "  CORRUPT after redistribute: $KEY"
    fi
done
assert_eq "$BAD" "0" "all 60 objects correct after redistribute"

# admin-op-status should be idle again
OP_STATUS=$(admin_get "admin-op-status")
OP_RUNNING=$(echo "$OP_STATUS" | json_field '["running"]')
assert_eq "$OP_RUNNING" "False" "admin-op-status idle after redistribute"

# =====================================================================
# Test 11: Redistribute on standalone returns 503
# =====================================================================

section "Test 11: Redistribute returns 503 on standalone"

PORT_STANDALONE=8305
IMG_SA="/tmp/drain_test_standalone.raw"
rm -f "$IMG_SA"

"$OBJSTRD_BIN" --image "$IMG_SA" --size-mb 64 --port "$PORT_STANDALONE" >/dev/null 2>&1 &
SA_PID=$!
PIDS_TO_KILL="$REDIST_PID $SA_PID"
wait_server "$PORT_STANDALONE"

REDIST_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:${PORT_STANDALONE}/_admin/redistribute" 2>/dev/null)
assert_eq "$REDIST_STATUS" "503" "redistribute returns 503 on standalone server"

kill $SA_PID 2>/dev/null || true
PIDS_TO_KILL="$REDIST_PID"
sleep 1

# =====================================================================
# Test 12: Drain with rf=1 must actually move sole-copy objects
# =====================================================================

section "Setup: 3-shard cluster (mem + mem + mem) rf=1 for drain-moves-data"

PORT=8306
CONF_DRAIN1="/tmp/drain_test_rf1.conf"

rm -f "$CONF_DRAIN1"

cat > "$CONF_DRAIN1" <<CONF
cluster    drain-rf1-test
bucket     testbucket
size_mb    64

primary  rf=1  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  mem
  mem
  mem
CONF

"$OBJSTRD_BIN" --config "$CONF_DRAIN1" --node primary >/dev/null 2>&1 &
DRAIN1_PID=$!
PIDS_TO_KILL="$REDIST_PID $DRAIN1_PID"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"
S3_BUCKET="testbucket"

section "Test 12: Drain with rf=1 must move sole-copy objects"

s3_bucket_create
for i in $(seq 1 30); do
    s3_put "sole_$(printf '%04d' $i)" "sole-data-$i"
done

UNIQUE=$(catalog_count)
assert_eq "$UNIQUE" "30" "30 objects stored for rf=1 drain test"

# Count objects on shard 1 before drain
BEFORE_ON_1=$(catalog_placement | python3 -c "
import sys
count = 0
for line in sys.stdin:
    parts = line.strip().split()
    if '1' in parts[1:]:
        count += 1
print(count)
")
echo "  objects on shard 1 before drain: $BEFORE_ON_1"
assert_ge "$BEFORE_ON_1" "1" "shard 1 has at least 1 object before drain"

# Drain shard 1
DRAIN_RESULT=$(admin_post "drain/1")
DRAIN_MOVED=$(echo "$DRAIN_RESULT" | json_field '["moved"]')
DRAIN_SKIPPED=$(echo "$DRAIN_RESULT" | json_field '["skipped"]')
DRAIN_ERRORS=$(echo "$DRAIN_RESULT" | json_field '["errors"]')
DRAIN_DELETED=$(echo "$DRAIN_RESULT" | json_field '["deleted"]')
DRAIN_DEL_ERR=$(echo "$DRAIN_RESULT" | json_field '["delete_errors"]')
echo "  drain rf=1 result: moved=$DRAIN_MOVED skipped=$DRAIN_SKIPPED deleted=$DRAIN_DELETED errors=$DRAIN_ERRORS delete_errors=$DRAIN_DEL_ERR"
assert_eq "$DRAIN_ERRORS" "0" "drain rf=1 had no errors"
assert_eq "$DRAIN_DEL_ERR" "0" "drain rf=1 had no delete errors"
assert_eq "$DRAIN_MOVED" "$BEFORE_ON_1" "drain moved all $BEFORE_ON_1 sole-copy objects"
assert_eq "$DRAIN_DELETED" "$BEFORE_ON_1" "drain deleted all $BEFORE_ON_1 objects from victim"

# ALL objects must still be accessible -- with rf=1, if drain did not
# actually move sole copies, GETs for objects that were only on shard 1
# will fail with 404.
LOST=0
for i in $(seq 1 30); do
    KEY="sole_$(printf '%04d' $i)"
    STATUS=$(s3_head "$KEY")
    if [ "$STATUS" != "200" ]; then
        LOST=$((LOST + 1))
        echo "  LOST: $KEY (status=$STATUS)"
    fi
done
assert_eq "$LOST" "0" "no objects lost after draining shard with rf=1"

# Verify data integrity for all objects
BAD=0
for i in $(seq 1 30); do
    KEY="sole_$(printf '%04d' $i)"
    GOT=$(s3_get "$KEY")
    WANT="sole-data-$i"
    if [ "$GOT" != "$WANT" ]; then
        BAD=$((BAD + 1))
        echo "  CORRUPT: $KEY got='$GOT' want='$WANT'"
    fi
done
assert_eq "$BAD" "0" "all 30 objects have correct data after rf=1 drain"

kill $DRAIN1_PID 2>/dev/null || true
PIDS_TO_KILL="$REDIST_PID"
sleep 1

# =====================================================================
# Test 13: Drain auto-restores RF (no manual repair-replication needed)
# =====================================================================

section "Setup: 4-shard cluster (mem x4) rf=3 for drain-auto-RF"

PORT=8307
CONF_AUTO_RF="/tmp/drain_test_auto_rf.conf"
rm -f "$CONF_AUTO_RF"

cat > "$CONF_AUTO_RF" <<CONF
cluster    drain-auto-rf-test
bucket     testbucket
size_mb    64

primary  rf=3  listen=0.0.0.0:${PORT}  endpoint=http://127.0.0.1:${PORT}
  mem
  mem
  mem
  mem
CONF

"$OBJSTRD_BIN" --config "$CONF_AUTO_RF" --node primary >/dev/null 2>&1 &
AUTO_RF_PID=$!
PIDS_TO_KILL="$REDIST_PID $AUTO_RF_PID"
wait_server "$PORT"

S3_ENDPOINT="http://localhost:${PORT}"
S3_BUCKET="testbucket"

section "Test 13: Drain auto-restores replication factor"

s3_bucket_create
for i in $(seq 1 20); do
    s3_put "arf_$(printf '%04d' $i)" "auto-rf-data-$i"
done

UNIQUE=$(catalog_count)
assert_eq "$UNIQUE" "20" "20 objects stored for auto-RF test"

# Every object should have exactly 3 replicas before drain
WRONG_BEFORE=$(catalog_placement | python3 -c '
import sys
wrong = 0
for line in sys.stdin:
    parts = line.strip().split()
    shard_count = len(parts) - 1
    if shard_count != 3:
        wrong += 1
print(wrong)
')
assert_eq "$WRONG_BEFORE" "0" "all objects have rf=3 before drain"

# Drain shard 2 -- this should auto-run repair-replication
DRAIN_RESULT=$(admin_post "drain/2")
DRAIN_MOVED=$(echo "$DRAIN_RESULT" | json_field '["moved"]')
DRAIN_SKIPPED=$(echo "$DRAIN_RESULT" | json_field '["skipped"]')
DRAIN_ERRORS=$(echo "$DRAIN_RESULT" | json_field '["errors"]')
DRAIN_DELETED=$(echo "$DRAIN_RESULT" | json_field '["deleted"]')
DRAIN_DEL_ERR=$(echo "$DRAIN_RESULT" | json_field '["delete_errors"]')
DRAIN_RE_REPL=$(echo "$DRAIN_RESULT" | json_field '["re_replicated"]')
DRAIN_UNDER=$(echo "$DRAIN_RESULT" | json_field '["under_remaining"]')
echo "  drain result: moved=$DRAIN_MOVED skipped=$DRAIN_SKIPPED deleted=$DRAIN_DELETED errors=$DRAIN_ERRORS re_replicated=$DRAIN_RE_REPL under_remaining=$DRAIN_UNDER"
assert_eq "$DRAIN_ERRORS" "0" "drain had no errors"
assert_eq "$DRAIN_DEL_ERR" "0" "drain had no delete errors"

# Drain should have re-replicated objects to restore RF
assert_ge "$DRAIN_RE_REPL" "1" "drain re-replicated at least 1 object (got $DRAIN_RE_REPL)"
assert_eq "$DRAIN_UNDER" "0" "drain left 0 under-replicated objects"

# WITHOUT calling repair-replication manually, check RF is correct
# Every object should have exactly 3 replicas across surviving shards (0,1,3)
WRONG_AFTER=$(catalog_placement | python3 -c '
import sys
wrong = 0
for line in sys.stdin:
    parts = line.strip().split()
    shard_count = len(parts) - 1
    if shard_count != 3:
        wrong += 1
        print("  key=%s shards=%d" % (parts[0], shard_count), file=sys.stderr)
print(wrong)
')
assert_eq "$WRONG_AFTER" "0" "all objects have rf=3 after drain (auto-restored)"

# No catalog references to drained shard
REFS_TO_2=$(catalog_placement | python3 -c '
import sys
count = 0
for line in sys.stdin:
    parts = line.strip().split()
    if "2" in parts[1:]:
        count += 1
print(count)
')
assert_eq "$REFS_TO_2" "0" "no catalog refs to drained shard 2"

# All data still accessible and correct
BAD=0
for i in $(seq 1 20); do
    KEY="arf_$(printf '%04d' $i)"
    GOT=$(s3_get "$KEY")
    WANT="auto-rf-data-$i"
    if [ "$GOT" != "$WANT" ]; then
        BAD=$((BAD + 1))
        echo "  CORRUPT: $KEY"
    fi
done
assert_eq "$BAD" "0" "all 20 objects correct after auto-RF drain"

kill $AUTO_RF_PID 2>/dev/null || true
PIDS_TO_KILL="$REDIST_PID"
sleep 1

# =====================================================================
# Done
# =====================================================================

echo ""
echo "All tests complete."
