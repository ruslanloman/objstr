#!/bin/bash
#
# cluster/run_tree_config_tests.sh
#
# E2E test for the tree config feature with a heterogeneous cluster.
#
# Topology (all on localhost, different ports):
#
#   root (port 8200, rf=2)
#     raw  /tmp/tree_root_0.raw
#     raw  /tmp/tree_root_1.raw
#     leaf-a (port 8201, rf=1)
#       raw  /tmp/tree_leaf_a.raw
#       fs   /tmp/tree_fs_a
#     leaf-b (port 8202, rf=1)
#       raw  /tmp/tree_leaf_b0.raw
#       raw  /tmp/tree_leaf_b1.raw
#
# root has 4 shards: 2 local raw + 2 child nodes (accessed via S3).
# leaf-a has 2 shards: 1 raw + 1 fs.
# leaf-b has 2 shards: 2 raw.
#
# Usage:
#   bash run_tree_config_tests.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/s3.sh"

OBJSTRD_BIN="${OBJSTRD_BIN:-$HOME/build-objstrd/release/objstrd}"

CONF_FILE=/tmp/tree_test_cluster.conf
ROOT_PORT=8200
LEAF_A_PORT=8201
LEAF_B_PORT=8202
SIZE_MB=128
BUCKET=testbucket

PASS=0
FAIL=0
PIDS=()

# ---- helpers ----

cleanup() {
    echo ""
    echo "Cleaning up..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    rm -f /tmp/tree_root_0.raw /tmp/tree_root_1.raw
    rm -f /tmp/tree_leaf_a.raw
    rm -f /tmp/tree_leaf_b0.raw /tmp/tree_leaf_b1.raw
    rm -rf /tmp/tree_fs_a
    rm -f "$CONF_FILE"
    if [ "$FAIL" -gt 0 ]; then
        echo "WARNING: $FAIL test(s) failed"
    fi
    echo "Results: $PASS passed, $FAIL failed"
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1 -- $2"; FAIL=$((FAIL + 1)); }

run() {
    local name="$1"; shift
    if "$@" 2>/dev/null; then
        pass "$name"
    else
        fail "$name" "command failed"
    fi
}

wait_for_server() {
    local port="$1" label="$2" max_secs="${3:-10}"
    local i=0
    while [ $i -lt $((max_secs * 10)) ]; do
        if curl -sf "http://localhost:${port}/_admin/info" >/dev/null 2>&1; then
            echo "  $label ready on port $port"
            return 0
        fi
        sleep 0.1
        i=$((i + 1))
    done
    echo "ERROR: $label did not start within ${max_secs}s on port $port" >&2
    return 1
}

# ---- formatting raw images ----

echo "=== tree config heterogeneous cluster test ==="
echo ""
echo "Formatting raw images..."
RAWOBJSTR_BIN="${RAWOBJSTR_BIN:-$HOME/build-rawobjstr/release/rawobjstr}"
SIZE_BYTES=$((SIZE_MB * 1024 * 1024))
for img in /tmp/tree_root_0.raw /tmp/tree_root_1.raw /tmp/tree_leaf_a.raw /tmp/tree_leaf_b0.raw /tmp/tree_leaf_b1.raw; do
    rm -f "$img"
    "$RAWOBJSTR_BIN" format --file "$img" --size "$SIZE_BYTES"
done

# Create fs directory
rm -rf /tmp/tree_fs_a
mkdir -p /tmp/tree_fs_a

echo "Images formatted."

# ---- write cluster.conf ----

cat > "$CONF_FILE" <<'CONF'
# tree config test cluster
cluster  tree-test
bucket   testbucket

root  rf=2  listen=0.0.0.0:8200  endpoint=http://127.0.0.1:8200
  raw  /tmp/tree_root_0.raw
  raw  /tmp/tree_root_1.raw
  leaf-a  rf=1  listen=0.0.0.0:8201  endpoint=http://127.0.0.1:8201
    raw  /tmp/tree_leaf_a.raw
    fs   /tmp/tree_fs_a
  leaf-b  rf=1  listen=0.0.0.0:8202  endpoint=http://127.0.0.1:8202
    raw  /tmp/tree_leaf_b0.raw
    raw  /tmp/tree_leaf_b1.raw
CONF

echo "Config file written to $CONF_FILE"
cat "$CONF_FILE"
echo ""

# ---- start nodes (leaves first, then root) ----

echo "Starting leaf-b..."
"$OBJSTRD_BIN" --config "$CONF_FILE" --node leaf-b &
PIDS+=($!)
wait_for_server "$LEAF_B_PORT" "leaf-b"

echo "Starting leaf-a..."
"$OBJSTRD_BIN" --config "$CONF_FILE" --node leaf-a &
PIDS+=($!)
wait_for_server "$LEAF_A_PORT" "leaf-a"

echo "Starting root..."
"$OBJSTRD_BIN" --config "$CONF_FILE" --node root &
PIDS+=($!)
wait_for_server "$ROOT_PORT" "root"

echo ""

# ---- S3 tests against root ----

export S3_ENDPOINT="http://localhost:${ROOT_PORT}"
export S3_BUCKET="$BUCKET"

echo "=== S3 CRUD tests against root (rf=2, 4 shards) ==="

# Bucket
run "create bucket" bash -c "curl -sf -X PUT '${S3_ENDPOINT}/${S3_BUCKET}' >/dev/null"
run "head bucket"   bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}')\" = '200' ]"

# PUT + GET
run "put small object"  bash -c "curl -sf -X PUT -d 'hello tree config' '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt' >/dev/null"
run "get small object"  bash -c "[ \"\$(curl -sf '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = 'hello tree config' ]"

# Multiple objects to spread across shards
for i in $(seq 1 20); do
    run "put obj-$i" bash -c "curl -sf -X PUT -d 'data-$i' '${S3_ENDPOINT}/${S3_BUCKET}/spread/obj-${i}.txt' >/dev/null"
done

for i in $(seq 1 20); do
    run "get obj-$i" bash -c "[ \"\$(curl -sf '${S3_ENDPOINT}/${S3_BUCKET}/spread/obj-${i}.txt')\" = 'data-$i' ]"
done

# List
run "list objects" bash -c "curl -sf '${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&prefix=spread/' | grep -q '<Key>'"

# Range read
run "range read" bash -c "[ \"\$(curl -sf -H 'Range: bytes=0-4' '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = 'hello' ]"

# HEAD
run "head object"   bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = '200' ]"
run "head missing"  bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}/no-such.txt')\" = '404' ]"

# DELETE
run "delete object" bash -c "curl -sf -X DELETE '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt' >/dev/null"
run "head deleted"  bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = '404' ]"

# ---- Tests against individual leaf nodes ----

echo ""
echo "=== Direct leaf node tests ==="

# leaf-a should work independently
export S3_ENDPOINT="http://localhost:${LEAF_A_PORT}"
run "leaf-a: create bucket" bash -c "curl -sf -X PUT '${S3_ENDPOINT}/${S3_BUCKET}' >/dev/null"
run "leaf-a: put"           bash -c "curl -sf -X PUT -d 'leaf-a-data' '${S3_ENDPOINT}/${S3_BUCKET}/leaf-test.txt' >/dev/null"
run "leaf-a: get"           bash -c "[ \"\$(curl -sf '${S3_ENDPOINT}/${S3_BUCKET}/leaf-test.txt')\" = 'leaf-a-data' ]"

# leaf-b should work independently
export S3_ENDPOINT="http://localhost:${LEAF_B_PORT}"
run "leaf-b: create bucket" bash -c "curl -sf -X PUT '${S3_ENDPOINT}/${S3_BUCKET}' >/dev/null"
run "leaf-b: put"           bash -c "curl -sf -X PUT -d 'leaf-b-data' '${S3_ENDPOINT}/${S3_BUCKET}/leaf-test.txt' >/dev/null"
run "leaf-b: get"           bash -c "[ \"\$(curl -sf '${S3_ENDPOINT}/${S3_BUCKET}/leaf-test.txt')\" = 'leaf-b-data' ]"

# ---- Admin endpoints ----

echo ""
echo "=== Admin endpoint tests ==="

export S3_ENDPOINT="http://localhost:${ROOT_PORT}"
run "root info"    bash -c "curl -sf '${S3_ENDPOINT}/_admin/info' | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"build_git_hash\" in d'"
run "root shards"  bash -c "curl -sf '${S3_ENDPOINT}/_admin/shards' | python3 -c 'import sys,json; d=json.load(sys.stdin); n=len(d[\"shards\"]); assert n == 4, \"expected 4 shards, got %d\" % n'"

echo ""
echo "=== All tree config tests complete ==="
