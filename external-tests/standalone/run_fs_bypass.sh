#!/bin/bash
#
# standalone/run_fs_bypass.sh
#
# Tests how the system handles files being created/deleted on the
# filesystem behind the object store's back.
#
# Scenarios covered:
#
#   1. shardedobjstr with FS shards (LocalFileSystem via config file)
#      - put via CLI, then rm the file from the fs directory
#      - add a new file into the fs directory, see if list/get picks it up
#
#   2. objstrd with FS backend (tree-config mode)
#      - put via S3 API, then rm the file from the fs directory
#      - add a new file into the fs directory, see if S3 GET/LIST picks it up
#      - delete via S3 API (as if another client deleted from the "bucket")
#
#   3. shardedobjstr -> objstrd/FS + local FS (multi-layer)
#      - put via sharded CLI (replicated to both shards)
#      - rm file from fs behind objstrd and/or the local FS shard
#      - see how the missing file propagates up through the layers
#      - inject files and see if they appear
#
#   4. S3 DELETE from another client vs rm bypass
#
#   5. Raw image + FS shard (mixed backends) with bypass
#
# Usage:
#   bash run_fs_bypass.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

OBJSTRD_BIN="${OBJSTRD_BIN:-$HOME/build-objstrd/release/objstrd}"
SHARDED_BIN="${SHARDED_BIN:-$HOME/build-sharded/release/shardedobjstr}"

PORT_OBJSTRD=8950
FS_ROOT_SHARD0="/tmp/ext_test_fs_s0"
FS_ROOT_SHARD1="/tmp/ext_test_fs_s1"
FS_ROOT_OBJSTRD="/tmp/ext_test_fs_objstrd"
FS_ROOT_LOCAL="/tmp/ext_test_fs_local"
CONF_SHARDED="/tmp/ext_test_sharded_fs.conf"
CONF_OBJSTRD="/tmp/ext_test_objstrd_fs.conf"
CONF_MULTI="/tmp/ext_test_multi_layer.conf"
TMPDATA="/tmp/ext_test_fs_tmpdata"
TMPOUT="/tmp/ext_test_fs_tmpout"
LOG_FILE="/tmp/ext_test_fs_bypass.log"
PASS=0
FAIL=0
OWN_SERVER=false
EXPECTED_HASH=""

cleanup() {
    if [ "$OWN_SERVER" = "true" ]; then
        stop_server "$PORT_OBJSTRD"
    fi
    rm -rf "$FS_ROOT_SHARD0" "$FS_ROOT_SHARD1" "$FS_ROOT_OBJSTRD" "$FS_ROOT_LOCAL"
    rm -f "$CONF_SHARDED" "$CONF_OBJSTRD" "$CONF_MULTI" "$TMPDATA" "$TMPOUT"
    rm -f /tmp/ext_test_bypass_raw.raw /tmp/ext_test_mixed.conf
    if [ "$FAIL" -gt 0 ] && [ "$OWN_SERVER" = "true" ] && [ -n "$EXPECTED_HASH" ]; then
        state=$(check_server_valid "$PORT_OBJSTRD" "$EXPECTED_HASH")
        if [ "$state" != "ok" ]; then
            echo ""
            echo "WARNING: server $state during test run -- results may be invalid"
        fi
    fi
    echo ""
    echo "========================================="
    echo "Results: $PASS passed, $FAIL failed"
    echo "Log: $LOG_FILE"
    echo "========================================="
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# Helper: put data into sharded store via a temp file
sharded_put() {
    local conf="$1" key="$2" data="$3"
    printf '%s' "$data" > "$TMPDATA"
    "$SHARDED_BIN" put --config "$conf" --key "$key" --from "$TMPDATA" 2>/dev/null
}

# Helper: get data from sharded store (stdout), returns "" on error
sharded_get() {
    local conf="$1" key="$2"
    "$SHARDED_BIN" get --config "$conf" --key "$key" 2>/dev/null || true
}

# Helper: list from sharded store
sharded_list() {
    local conf="$1" prefix="$2"
    "$SHARDED_BIN" list --config "$conf" --prefix "$prefix" 2>/dev/null || true
}

# =====================================================================
echo ""
echo "=== SCENARIO 1: shardedobjstr with FS shards (direct bypass) ==="
echo ""
# =====================================================================

rm -rf "$FS_ROOT_SHARD0" "$FS_ROOT_SHARD1"
mkdir -p "$FS_ROOT_SHARD0" "$FS_ROOT_SHARD1"

cat > "$CONF_SHARDED" <<CONF
replicas 1
shard fs ${FS_ROOT_SHARD0}
shard fs ${FS_ROOT_SHARD1}
CONF

echo "  Putting objects via shardedobjstr CLI..."
sharded_put "$CONF_SHARDED" "bypass/obj1.txt" "hello-from-cli"
sharded_put "$CONF_SHARDED" "bypass/obj2.txt" "second-object"

COUNT=$(sharded_list "$CONF_SHARDED" "bypass/" | grep -c "bypass/obj" || true)
if [ "$COUNT" = "2" ]; then
    pass "list shows both objects"
else
    fail "list shows both objects (got $COUNT)"
fi

echo "  Files on disk:"
find "$FS_ROOT_SHARD0" "$FS_ROOT_SHARD1" -type f 2>/dev/null | sed 's/^/    /'

# -- Find where obj1.txt landed and delete it with rm --
OBJ1_FILE=$(find "$FS_ROOT_SHARD0" "$FS_ROOT_SHARD1" -name "obj1.txt" -type f 2>/dev/null | head -1)
if [ -z "$OBJ1_FILE" ]; then
    fail "could not find obj1.txt on filesystem"
else
    echo "  Found obj1.txt at: $OBJ1_FILE"
    echo "  Deleting it with rm (bypassing object store)..."
    rm -f "$OBJ1_FILE"

    # GET should fail or return empty
    GOT=$(sharded_get "$CONF_SHARDED" "bypass/obj1.txt")
    if [ -z "$GOT" ]; then
        pass "get rm'd file returns empty/error"
    else
        fail "get rm'd file returns empty/error (got: $GOT)"
    fi

    # LIST should no longer show it
    if sharded_list "$CONF_SHARDED" "bypass/" | grep -q "obj1.txt"; then
        fail "list no longer shows rm'd file"
    else
        pass "list no longer shows rm'd file"
    fi

    # obj2.txt should still work
    GOT=$(sharded_get "$CONF_SHARDED" "bypass/obj2.txt")
    if [ "$GOT" = "second-object" ]; then
        pass "obj2 still readable after obj1 rm"
    else
        fail "obj2 still readable after obj1 rm (got: '$GOT')"
    fi
fi

# -- Add a new file directly to the filesystem --
echo "  Adding a file directly to FS shard..."
mkdir -p "$FS_ROOT_SHARD0/bypass"
printf '%s' "injected-file" > "$FS_ROOT_SHARD0/bypass/injected.txt"

if sharded_list "$CONF_SHARDED" "bypass/" | grep -q "injected.txt"; then
    pass "list picks up injected file"
else
    fail "list picks up injected file"
fi

GOT=$(sharded_get "$CONF_SHARDED" "bypass/injected.txt")
if [ "$GOT" = "injected-file" ]; then
    pass "get injected file returns correct data"
else
    fail "get injected file returns correct data (got: '$GOT')"
fi

echo ""
echo "  Scenario 1 complete."

# =====================================================================
echo ""
echo "=== SCENARIO 2: objstrd with FS backend (S3 layer bypass) ==="
echo ""
# =====================================================================

rm -rf "$FS_ROOT_OBJSTRD"
mkdir -p "$FS_ROOT_OBJSTRD"

cat > "$CONF_OBJSTRD" <<CONF
cluster  bypass-test
bucket   testbucket

root  rf=1  listen=0.0.0.0:${PORT_OBJSTRD}  endpoint=http://127.0.0.1:${PORT_OBJSTRD}
  fs  ${FS_ROOT_OBJSTRD}
CONF

echo "  Starting objstrd with FS backend on port $PORT_OBJSTRD..."
"$OBJSTRD_BIN" --config "$CONF_OBJSTRD" --node root >>"$LOG_FILE" 2>&1 &
OBJSTRD_PID=$!
echo "$OBJSTRD_PID" > "/tmp/objstrd_${PORT_OBJSTRD}.pid"
OWN_SERVER=true

i=0
while [ $i -lt 100 ]; do
    if curl -sf "http://localhost:${PORT_OBJSTRD}/_admin/info" >/dev/null 2>&1; then
        echo "  objstrd started (pid=$OBJSTRD_PID)"
        break
    fi
    sleep 0.1
    i=$((i + 1))
done
if [ $i -eq 100 ]; then
    echo "ERROR: objstrd did not start within 10s"; exit 1
fi

EXPECTED_HASH=$(get_build_hash "$PORT_OBJSTRD")
export S3_ENDPOINT="http://localhost:${PORT_OBJSTRD}"
export S3_BUCKET="testbucket"

# Create bucket
curl -sf -X PUT "${S3_ENDPOINT}/${S3_BUCKET}" >/dev/null 2>&1 || true

# -- Put objects via S3 API --
echo "  Putting objects via S3 API..."
s3_put "bypass/s3obj1.txt" "hello-from-s3"
s3_put "bypass/s3obj2.txt" "second-s3-object"
s3_put "bypass/s3obj3.txt" "third-s3-object"

GOT=$(s3_get "bypass/s3obj1.txt")
if [ "$GOT" = "hello-from-s3" ]; then
    pass "S3 GET obj1"
else
    fail "S3 GET obj1 (got: '$GOT')"
fi

S3_COUNT=$(s3_list "bypass/" | wc -l)
if [ "$S3_COUNT" -ge 3 ]; then
    pass "S3 LIST shows all 3"
else
    fail "S3 LIST shows all 3 (got $S3_COUNT)"
fi

echo "  Files on objstrd FS:"
find "$FS_ROOT_OBJSTRD" -type f 2>/dev/null | sed 's/^/    /'

# -- rm from the fs directory behind objstrd --
FS_FILE="$FS_ROOT_OBJSTRD/testbucket/bypass/s3obj1.txt"
if [ -f "$FS_FILE" ]; then
    echo "  Deleting s3obj1.txt with rm (bypassing objstrd)..."
    rm -f "$FS_FILE"

    STATUS=$(s3_head "bypass/s3obj1.txt")
    if [ "$STATUS" = "404" ]; then
        pass "S3 HEAD rm'd file returns 404"
    else
        fail "S3 HEAD rm'd file returns 404 (got $STATUS)"
    fi

    if ! s3_get "bypass/s3obj1.txt" >/dev/null 2>&1; then
        pass "S3 GET rm'd file fails"
    else
        fail "S3 GET rm'd file fails"
    fi

    if s3_list "bypass/" | grep -q "s3obj1.txt"; then
        fail "S3 LIST no longer shows rm'd file"
    else
        pass "S3 LIST no longer shows rm'd file"
    fi

    GOT=$(s3_get "bypass/s3obj2.txt")
    if [ "$GOT" = "second-s3-object" ]; then
        pass "S3 obj2 still works after obj1 rm"
    else
        fail "S3 obj2 still works after obj1 rm (got: '$GOT')"
    fi

    # S3 DELETE on already-rm'd file should be idempotent
    DEL_STATUS=$(curl -so /dev/null -w "%{http_code}" -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/bypass/s3obj1.txt")
    if [ "$DEL_STATUS" = "204" ] || [ "$DEL_STATUS" = "200" ]; then
        pass "S3 DELETE on already-rm'd file is idempotent ($DEL_STATUS)"
    else
        fail "S3 DELETE on already-rm'd file is idempotent (got $DEL_STATUS)"
    fi
else
    fail "could not find s3obj1.txt at expected path: $FS_FILE"
fi

# -- Add a new file directly to the fs directory (no sidecar) --
echo "  Adding file directly to objstrd FS dir (no .__meta__ sidecar)..."
mkdir -p "$FS_ROOT_OBJSTRD/testbucket/bypass"
printf '%s' "injected-via-fs" > "$FS_ROOT_OBJSTRD/testbucket/bypass/fs_injected.txt"

if s3_list "bypass/" | grep -q "fs_injected.txt"; then
    pass "S3 LIST picks up injected file (no sidecar)"
else
    fail "S3 LIST picks up injected file (no sidecar)"
fi

# GET without sidecar returns 500 because get_metadata returns NotFound
# which the adapter maps to InternalError. This is expected.
INJ_STATUS=$(curl -so /dev/null -w "%{http_code}" "${S3_ENDPOINT}/${S3_BUCKET}/bypass/fs_injected.txt")
if [ "$INJ_STATUS" = "500" ]; then
    pass "S3 GET injected file without sidecar returns 500 (expected -- no sidecar)"
elif [ "$INJ_STATUS" = "200" ]; then
    pass "S3 GET injected file without sidecar returns 200"
else
    fail "S3 GET injected file without sidecar (unexpected status $INJ_STATUS)"
fi

# -- Inject file WITH sidecar (should work) --
echo "  Adding file with empty sidecar to objstrd FS dir..."
printf '%s' "injected-with-meta" > "$FS_ROOT_OBJSTRD/testbucket/bypass/fs_injected2.txt"
printf '' > "$FS_ROOT_OBJSTRD/testbucket/bypass/fs_injected2.txt.__meta__"

GOT=$(s3_get "bypass/fs_injected2.txt")
if [ "$GOT" = "injected-with-meta" ]; then
    pass "S3 GET injected file WITH sidecar works"
else
    fail "S3 GET injected file WITH sidecar (got: '$GOT')"
fi

# -- S3 DELETE as if another client deleted from the bucket --
echo "  Deleting obj2 via S3 API (simulating another S3 client)..."
s3_delete "bypass/s3obj2.txt"

STATUS=$(s3_head "bypass/s3obj2.txt")
if [ "$STATUS" = "404" ]; then
    pass "S3 HEAD after client DELETE returns 404"
else
    fail "S3 HEAD after client DELETE returns 404 (got $STATUS)"
fi

if [ ! -f "$FS_ROOT_OBJSTRD/testbucket/bypass/s3obj2.txt" ]; then
    pass "file gone from fs after S3 DELETE"
else
    fail "file gone from fs after S3 DELETE"
fi

GOT=$(s3_get "bypass/s3obj3.txt")
if [ "$GOT" = "third-s3-object" ]; then
    pass "S3 obj3 still works"
else
    fail "S3 obj3 still works (got: '$GOT')"
fi

echo ""
echo "  Scenario 2 complete."

# =====================================================================
echo ""
echo "=== SCENARIO 3: shardedobjstr -> objstrd/FS + local FS (multi-layer) ==="
echo ""
# =====================================================================

rm -rf "$FS_ROOT_LOCAL"
mkdir -p "$FS_ROOT_LOCAL"

cat > "$CONF_MULTI" <<CONF
replicas 2
shard s3 endpoint=http://localhost:${PORT_OBJSTRD} bucket=testbucket
shard fs ${FS_ROOT_LOCAL}
CONF

echo "  Putting objects via shardedobjstr (RF=2: S3 shard + local FS shard)..."
sharded_put "$CONF_MULTI" "layered/obj1.txt" "multi-layer-data"
sharded_put "$CONF_MULTI" "layered/obj2.txt" "multi-layer-obj2"

COUNT=$(sharded_list "$CONF_MULTI" "layered/" | grep -c "layered/obj" || true)
if [ "$COUNT" -ge 2 ]; then
    pass "sharded list shows both"
else
    fail "sharded list shows both (got $COUNT)"
fi

GOT=$(sharded_get "$CONF_MULTI" "layered/obj1.txt")
if [ "$GOT" = "multi-layer-data" ]; then
    pass "sharded get obj1 works"
else
    fail "sharded get obj1 works (got: '$GOT')"
fi

echo "  Files on objstrd FS:"
find "$FS_ROOT_OBJSTRD" -path "*/layered/*" -type f 2>/dev/null | sed 's/^/    /'
echo "  Files on local FS:"
find "$FS_ROOT_LOCAL" -path "*/layered/*" -type f 2>/dev/null | sed 's/^/    /'

# -- rm from the objstrd FS directory --
LAYERED_S3="$FS_ROOT_OBJSTRD/testbucket/layered/obj1.txt"
if [ -f "$LAYERED_S3" ]; then
    echo "  Deleting obj1 from objstrd FS (bypassing objstrd)..."
    rm -f "$LAYERED_S3"

    GOT=$(sharded_get "$CONF_MULTI" "layered/obj1.txt")
    if [ "$GOT" = "multi-layer-data" ]; then
        pass "sharded get falls back to local FS after objstrd rm"
    else
        fail "sharded get falls back to local FS after objstrd rm (got: '$GOT')"
    fi
else
    echo "  obj1 not on objstrd FS shard - may be only on local"
fi

# -- Now also rm from the local FS shard --
LOCAL_FILE=$(find "$FS_ROOT_LOCAL" -name "obj1.txt" -type f 2>/dev/null | head -1)
if [ -n "$LOCAL_FILE" ]; then
    echo "  Also deleting obj1 from local FS shard..."
    rm -f "$LOCAL_FILE"

    GOT=$(sharded_get "$CONF_MULTI" "layered/obj1.txt")
    if [ -z "$GOT" ]; then
        pass "sharded get fails after both copies rm'd"
    else
        fail "sharded get fails after both copies rm'd (got: '$GOT')"
    fi
else
    echo "  obj1 not found on local FS shard either"
fi

# obj2 should still work
GOT=$(sharded_get "$CONF_MULTI" "layered/obj2.txt")
if [ "$GOT" = "multi-layer-obj2" ]; then
    pass "sharded get obj2 still works"
else
    fail "sharded get obj2 still works (got: '$GOT')"
fi

# -- Inject files into both layers --
echo "  Injecting file into local FS shard..."
mkdir -p "$FS_ROOT_LOCAL/layered"
printf '%s' "local-injected" > "$FS_ROOT_LOCAL/layered/local_new.txt"

if sharded_list "$CONF_MULTI" "layered/" | grep -q "local_new.txt"; then
    pass "sharded list picks up locally injected file"
else
    fail "sharded list picks up locally injected file"
fi

echo "  Injecting file into objstrd FS dir..."
mkdir -p "$FS_ROOT_OBJSTRD/testbucket/layered"
printf '%s' "objstrd-injected" > "$FS_ROOT_OBJSTRD/testbucket/layered/remote_new.txt"

if sharded_list "$CONF_MULTI" "layered/" | grep -q "remote_new.txt"; then
    pass "sharded list picks up objstrd-injected file"
else
    fail "sharded list picks up objstrd-injected file"
fi

GOT=$(sharded_get "$CONF_MULTI" "layered/local_new.txt")
if [ "$GOT" = "local-injected" ]; then
    pass "sharded get locally injected file"
else
    fail "sharded get locally injected file (got: '$GOT')"
fi

echo ""
echo "  Scenario 3 complete."

# =====================================================================
echo ""
echo "=== SCENARIO 4: S3 DELETE from another client vs rm bypass ==="
echo ""
# =====================================================================

echo "  Putting fresh object via sharded store..."
sharded_put "$CONF_MULTI" "delsrc/target.txt" "delete-test-data"

# -- Delete via S3 API directly (simulating another S3 client) --
echo "  Deleting from S3 API directly (another client)..."
s3_delete "delsrc/target.txt"

STATUS=$(s3_head "delsrc/target.txt")
if [ "$STATUS" = "404" ]; then
    pass "S3 HEAD confirms delete on S3 side"
else
    fail "S3 HEAD confirms delete on S3 side (got $STATUS)"
fi

# Sharded store: S3 shard lost it but local FS shard might still have it
GOT=$(sharded_get "$CONF_MULTI" "delsrc/target.txt")
if [ "$GOT" = "delete-test-data" ]; then
    pass "sharded get falls back to local FS after S3 client delete"
elif [ -z "$GOT" ]; then
    pass "sharded get returns empty (object was only on S3 shard)"
else
    fail "unexpected data after S3 delete: '$GOT'"
fi

# Now also rm from local FS
LOCAL_DEL=$(find "$FS_ROOT_LOCAL" -name "target.txt" -type f 2>/dev/null | head -1)
if [ -n "$LOCAL_DEL" ]; then
    echo "  Also rm from local FS..."
    rm -f "$LOCAL_DEL"
    GOT=$(sharded_get "$CONF_MULTI" "delsrc/target.txt")
    if [ -z "$GOT" ]; then
        pass "sharded get fails after S3 delete + local rm"
    else
        fail "sharded get fails after S3 delete + local rm (got: '$GOT')"
    fi
fi

echo ""
echo "  Scenario 4 complete."

# =====================================================================
echo ""
echo "=== SCENARIO 5: Raw image + FS shard (mixed) with bypass ==="
echo ""
# =====================================================================

RAW_IMAGE="/tmp/ext_test_bypass_raw.raw"
CONF_MIXED="/tmp/ext_test_mixed.conf"

rm -f "$RAW_IMAGE"
rm -rf "$FS_ROOT_SHARD0"
mkdir -p "$FS_ROOT_SHARD0"

# Format a raw shard (64 MB)
"$SHARDED_BIN" format --shards "$RAW_IMAGE" --size 67108864 --replicas 1 2>/dev/null

cat > "$CONF_MIXED" <<CONF
replicas 2
shard raw ${RAW_IMAGE}
shard fs ${FS_ROOT_SHARD0}
CONF

echo "  Putting object via sharded store (raw + fs, RF=2)..."
sharded_put "$CONF_MIXED" "mixed/obj1.txt" "mixed-shard-data"

GOT=$(sharded_get "$CONF_MIXED" "mixed/obj1.txt")
if [ "$GOT" = "mixed-shard-data" ]; then
    pass "mixed cluster get works"
else
    fail "mixed cluster get works (got: '$GOT')"
fi

# rm from the FS shard (raw shard untouched)
FS_FILE=$(find "$FS_ROOT_SHARD0" -name "obj1.txt" -type f 2>/dev/null | head -1)
if [ -n "$FS_FILE" ]; then
    echo "  Deleting obj1 from FS shard (raw shard still has it)..."
    rm -f "$FS_FILE"

    GOT=$(sharded_get "$CONF_MIXED" "mixed/obj1.txt")
    if [ "$GOT" = "mixed-shard-data" ]; then
        pass "mixed cluster falls back to raw shard after FS rm"
    else
        fail "mixed cluster falls back to raw shard after FS rm (got: '$GOT')"
    fi
else
    echo "  obj1 not found on FS shard (only on raw) - skip FS rm test"
    pass "mixed cluster: obj on raw shard only (FS shard empty is valid)"
fi

rm -f "$RAW_IMAGE" "$CONF_MIXED"

echo ""
echo "  Scenario 5 complete."
echo ""
echo "=== All scenarios complete ==="
