#!/bin/bash
#
# standalone/run_basic_s3.sh
#
# Smoke-tests a standalone objstrd node from the outside using curl.
# Covers: bucket create, PUT, GET, HEAD, DELETE, list, range GET.
#
# Usage:
#   bash run_basic_s3.sh
#   S3_ENDPOINT=http://myserver:8000 bash run_basic_s3.sh   # against existing server
#
# If OBJSTRD_BIN is set and S3_ENDPOINT is not already up, the script
# starts its own server on a temp image and stops it afterwards.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

PORT=8900
IMAGE=/tmp/ext_test_standalone.raw
SIZE_MB=256
PASS=0
FAIL=0
OWN_SERVER=false

# ---- server setup ----

if ! curl -sf "http://localhost:${PORT}/_admin/info" >/dev/null 2>&1; then
    rm -f "$IMAGE"
    start_server "$PORT" "$IMAGE" "$SIZE_MB"
    OWN_SERVER=true
fi

EXPECTED_HASH=$(get_build_hash "$PORT")
export S3_ENDPOINT="http://localhost:${PORT}"
export S3_BUCKET="testbucket"

cleanup() {
    if [ "$OWN_SERVER" = "true" ]; then
        stop_server "$PORT"
        rm -f "$IMAGE"
    fi
    # Binary-swap check if any failures
    if [ "$FAIL" -gt 0 ]; then
        state=$(check_server_valid "$PORT" "$EXPECTED_HASH")
        if [ "$state" != "ok" ]; then
            echo ""
            echo "WARNING: server $state during test run -- results may be invalid"
        fi
    fi
    echo ""
    echo "Results: $PASS passed, $FAIL failed"
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

run() {
    local name="$1"; shift
    if "$@"; then
        pass "$name"
    else
        fail "$name"
    fi
}

echo "=== standalone S3 smoke tests ==="
echo "Endpoint: $S3_ENDPOINT  Bucket: $S3_BUCKET"
echo ""

# ---- bucket ----

run "create bucket" bash -c "curl -sf -X PUT '${S3_ENDPOINT}/${S3_BUCKET}' >/dev/null"
run "head bucket"   bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}')\" = '200' ]"

# ---- object CRUD ----

run "put object"    bash -c "curl -sf -X PUT -d 'hello world' '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt' >/dev/null"
run "get object"    bash -c "[ \"\$(curl -sf '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = 'hello world' ]"
run "head object"   bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = '200' ]"
run "head missing"  bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}/test/no-such.txt')\" = '404' ]"

# ---- list ----

run "list bucket"   bash -c "curl -sf '${S3_ENDPOINT}/${S3_BUCKET}?list-type=2' | grep -q '<Key>'"
run "list prefix"   bash -c "curl -sf '${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&prefix=test/' | grep -q 'hello.txt'"

# ---- range read ----

run "range read"    bash -c "[ \"\$(curl -sf -H 'Range: bytes=0-4' '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = 'hello' ]"

# ---- delete ----

run "delete object" bash -c "curl -sf -X DELETE '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt' >/dev/null"
run "head deleted"  bash -c "[ \"\$(curl -so /dev/null -w '%{http_code}' -I '${S3_ENDPOINT}/${S3_BUCKET}/test/hello.txt')\" = '404' ]"

# ---- admin info ----

run "admin info"    bash -c "curl -sf '${S3_ENDPOINT}/_admin/info' | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"build_git_hash\" in d'"
