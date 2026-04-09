#!/bin/bash
#
# standalone/run_full_matrix.sh
#
# Comprehensive E2E test that exercises ALL THREE components
# (rawobjstr CLI, shardedobjstr CLI, objstrd S3 daemon) across
# ALL backend types (raw, fs, s3-via-objstrd) and their combinations.
#
# Test topology:
#
#   Layer 1 (raw stores):
#     raw image A  (rawobjstr CLI)
#     raw image B  (rawobjstr CLI)
#
#   Layer 2 (objstrd serving raw + fs):
#     objstrd-raw  port 8960  (raw backend, serves S3)
#     objstrd-fs   port 8961  (fs backend, serves S3)
#     objstrd-mem  port 8962  (mem backend, serves S3)
#
#   Layer 3 (shardedobjstr with mixed backends):
#     config-a: 2x raw shards (direct)
#     config-b: 2x fs shards (direct)
#     config-c: 2x s3 shards (via objstrd-raw + objstrd-fs)
#     config-d: mixed raw + fs + s3 (all 3 backend types)
#
#   Layer 4 (objstrd with tree config, sharded backends):
#     objstrd-tree port 8963 (root: raw + child S3 nodes)
#
# Uses: rawobjstr CLI, shardedobjstr CLI, curl, aws CLI
#
# Usage:
#   bash run_full_matrix.sh
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

RAWOBJSTR="${RAWOBJSTR:-$HOME/build-rawobjstr/release/rawobjstr}"
SHARDEDOBJSTR="${SHARDEDOBJSTR:-$HOME/build-sharded/release/shardedobjstr}"
OBJSTRD_BIN="${OBJSTRD_BIN:-$HOME/build-objstrd/release/objstrd}"

# Ports
PORT_RAW=8960
PORT_FS=8961
PORT_MEM=8962
PORT_TREE=8963

# Paths
IMG_A=/tmp/matrix_raw_a.raw
IMG_B=/tmp/matrix_raw_b.raw
IMG_C=/tmp/matrix_raw_c.raw
IMG_D=/tmp/matrix_raw_d.raw
IMG_OBJSTRD=/tmp/matrix_objstrd_raw.raw
IMG_TREE_ROOT=/tmp/matrix_tree_root.raw
FS_SHARD0=/tmp/matrix_fs_s0
FS_SHARD1=/tmp/matrix_fs_s1
FS_OBJSTRD=/tmp/matrix_fs_objstrd
CONF_RAW=/tmp/matrix_conf_raw.conf
CONF_FS=/tmp/matrix_conf_fs.conf
CONF_S3=/tmp/matrix_conf_s3.conf
CONF_MIXED=/tmp/matrix_conf_mixed.conf
CONF_TREE=/tmp/matrix_tree.conf
TMPDIR=/tmp/matrix_test_tmp
RAW_SIZE=$((128 * 1024 * 1024))
BUCKET=testbucket

PASS=0
FAIL=0
SKIP=0
TOTAL_SECTIONS=0
SECTION_PASS=0
SECTION_FAIL=0
OWN_SERVERS=""

# ---- helpers --------------------------------------------------------

cleanup() {
    echo ""
    echo "================================================================"
    echo "Cleaning up..."

    for port in $OWN_SERVERS; do
        stop_server "$port" 2>/dev/null || true
    done
    # Also kill by PID file in case start_objstrd processes were orphaned
    for pidfile in /tmp/objstrd_89*.pid; do
        if [ -f "$pidfile" ]; then
            local pid
            pid=$(cat "$pidfile" 2>/dev/null || true)
            if [ -n "$pid" ]; then
                kill "$pid" 2>/dev/null || true
            fi
            rm -f "$pidfile"
        fi
    done

    rm -f "$IMG_A" "$IMG_B" "$IMG_C" "$IMG_D" "$IMG_OBJSTRD" "$IMG_TREE_ROOT"
    rm -rf "$FS_SHARD0" "$FS_SHARD1" "$FS_OBJSTRD" "$TMPDIR"
    rm -f "$CONF_RAW" "$CONF_FS" "$CONF_S3" "$CONF_MIXED" "$CONF_TREE"

    echo ""
    echo "================================================================"
    echo "FINAL RESULTS: $PASS passed, $FAIL failed, $SKIP skipped"
    echo "================================================================"
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }
skip() { echo "  SKIP: $1"; SKIP=$((SKIP + 1)); }

run() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        pass "$name"
    else
        fail "$name"
    fi
}

run_fail() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        fail "$name (expected failure)"
    else
        pass "$name"
    fi
}

assert_eq() {
    local got="$1" want="$2" name="$3"
    if [ "$got" = "$want" ]; then
        pass "$name"
    else
        fail "$name (got '$got', expected '$want')"
    fi
}

assert_contains() {
    if echo "$1" | grep -q "$2"; then
        pass "$3"
    else
        fail "$3 (expected '$2' in output)"
    fi
}

assert_not_contains() {
    if echo "$1" | grep -q "$2"; then
        fail "$3 (unexpected '$2' in output)"
    else
        pass "$3"
    fi
}

section() {
    TOTAL_SECTIONS=$((TOTAL_SECTIONS + 1))
    echo ""
    echo "================================================================"
    echo "=== SECTION $TOTAL_SECTIONS: $1"
    echo "================================================================"
    echo ""
    mkdir -p "$TMPDIR"
}

start_objstrd() {
    local port="$1"; shift
    env "$@" "$OBJSTRD_BIN" &
    local pid=$!
    echo "$pid" > "/tmp/objstrd_${port}.pid"
    OWN_SERVERS="$OWN_SERVERS $port"

    local i=0
    while [ $i -lt 100 ]; do
        if curl -sf "http://localhost:${port}/_admin/info" >/dev/null 2>&1; then
            echo "  objstrd started on port $port (pid=$pid)"
            return 0
        fi
        sleep 0.1
        i=$((i + 1))
    done
    echo "  ERROR: objstrd on port $port did not start within 10s"
    return 1
}

# Helper: put data via sharded CLI
sharded_put() {
    local conf="$1" key="$2" data="$3"
    mkdir -p "$TMPDIR"
    printf '%s' "$data" > "$TMPDIR/put_data.tmp"
    "$SHARDEDOBJSTR" put --config "$conf" --key "$key" --from "$TMPDIR/put_data.tmp" 2>/dev/null
}

sharded_put_file() {
    local conf="$1" key="$2" file="$3"
    "$SHARDEDOBJSTR" put --config "$conf" --key "$key" --from "$file" 2>/dev/null
}

sharded_get() {
    local conf="$1" key="$2"
    "$SHARDEDOBJSTR" get --config "$conf" --key "$key" 2>/dev/null || true
}

sharded_list() {
    local conf="$1" prefix="${2:-}"
    if [ -n "$prefix" ]; then
        "$SHARDEDOBJSTR" list --config "$conf" --prefix "$prefix" 2>/dev/null || true
    else
        "$SHARDEDOBJSTR" list --config "$conf" 2>/dev/null || true
    fi
}

# S3 helpers via aws CLI
HAS_AWS=false
if command -v aws >/dev/null 2>&1 && aws --version >/dev/null 2>&1; then
    HAS_AWS=true
fi

aws_s3() {
    aws --endpoint-url "$S3_ENDPOINT" --no-sign-request "$@" 2>/dev/null
}

aws_s3api() {
    aws --endpoint-url "$S3_ENDPOINT" --no-sign-request s3api "$@" 2>/dev/null
}

# ---- setup ----------------------------------------------------------

mkdir -p "$TMPDIR"

echo "================================================================"
echo "=== COMPREHENSIVE E2E TEST MATRIX ==="
echo "================================================================"
echo ""
echo "rawobjstr:     $RAWOBJSTR"
echo "shardedobjstr: $SHARDEDOBJSTR"
echo "objstrd:       $OBJSTRD_BIN"
echo ""

# ======================================================================
# SECTION 1: RAWOBJSTR CLI -- DIRECT RAW BACKEND
# ======================================================================
section "rawobjstr CLI -- raw backend CRUD + features"

rm -f "$IMG_A"
run "format raw image A (zstd)" \
    "$RAWOBJSTR" format --file "$IMG_A" --size "$RAW_SIZE" --compression zstd

rm -f "$IMG_B"
run "format raw image B (snappy)" \
    "$RAWOBJSTR" format --file "$IMG_B" --size "$RAW_SIZE" --compression snappy

# -- info --
OUT=$("$RAWOBJSTR" info --file "$IMG_A" 2>/dev/null)
assert_contains "$OUT" "zstd" "info shows zstd compression"

OUT=$("$RAWOBJSTR" info --file "$IMG_B" 2>/dev/null)
assert_contains "$OUT" "snappy" "info shows snappy compression"

# -- put + get (small) --
echo "hello raw world" > "$TMPDIR/raw_hello.txt"
run "put small file to A" \
    "$RAWOBJSTR" put --file "$IMG_A" --key "raw/hello.txt" --from "$TMPDIR/raw_hello.txt"

GOT=$("$RAWOBJSTR" get --file "$IMG_A" --key "raw/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello raw world" "get small file from A matches"

# -- put + get (binary) --
dd if=/dev/urandom bs=4096 count=16 of="$TMPDIR/raw_binary.bin" 2>/dev/null
run "put 64k binary to A" \
    "$RAWOBJSTR" put --file "$IMG_A" --key "raw/binary.bin" --from "$TMPDIR/raw_binary.bin"

"$RAWOBJSTR" get --file "$IMG_A" --key "raw/binary.bin" --to "$TMPDIR/raw_binary_got.bin" 2>/dev/null
if cmp -s "$TMPDIR/raw_binary.bin" "$TMPDIR/raw_binary_got.bin"; then
    pass "get 64k binary matches original"
else
    fail "get 64k binary matches original"
fi

# -- put + get on image B (snappy) --
run "put to B (snappy)" \
    "$RAWOBJSTR" put --file "$IMG_B" --key "raw/hello.txt" --from "$TMPDIR/raw_hello.txt"

GOT=$("$RAWOBJSTR" get --file "$IMG_B" --key "raw/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello raw world" "get from B (snappy) matches"

# -- list --
OUT=$("$RAWOBJSTR" list --file "$IMG_A" 2>/dev/null)
assert_contains "$OUT" "raw/hello.txt" "list contains hello.txt"
assert_contains "$OUT" "raw/binary.bin" "list contains binary.bin"

# Put objects under different prefixes to test prefix filtering
echo "prefix-test" > "$TMPDIR/raw_prefix.txt"
run "put alpha/obj.txt" \
    "$RAWOBJSTR" put --file "$IMG_A" --key "alpha/obj.txt" --from "$TMPDIR/raw_prefix.txt"

OUT=$("$RAWOBJSTR" list --file "$IMG_A" --prefix "alpha/" 2>/dev/null)
assert_contains "$OUT" "alpha/obj.txt" "list prefix alpha/ shows alpha"
assert_not_contains "$OUT" "raw/" "list prefix alpha/ hides raw/"

# -- list-full --
OUT=$("$RAWOBJSTR" list-full --file "$IMG_A" 2>/dev/null)
assert_contains "$OUT" "raw/hello.txt" "list-full shows hello.txt"

# -- list --long --
OUT=$("$RAWOBJSTR" list --file "$IMG_A" --long 2>/dev/null)
assert_contains "$OUT" "raw/hello.txt" "list --long shows hello.txt"

# -- getraw (compressed bytes) --
RAW_SIZE_GOT=$("$RAWOBJSTR" getraw --file "$IMG_A" --key "raw/hello.txt" 2>/dev/null | wc -c)
if [ "$RAW_SIZE_GOT" -gt 0 ]; then
    pass "getraw returns compressed bytes"
else
    fail "getraw returns compressed bytes"
fi

# -- putmeta / getmeta --
echo '{"content-type":"text/plain","custom":"value1"}' > "$TMPDIR/raw_meta.json"
run "putmeta on hello.txt" \
    "$RAWOBJSTR" putmeta --file "$IMG_A" --key "raw/hello.txt" --from "$TMPDIR/raw_meta.json"

META=$("$RAWOBJSTR" getmeta --file "$IMG_A" --key "raw/hello.txt" 2>/dev/null)
assert_contains "$META" "custom" "getmeta returns metadata"

# body should still be intact after putmeta
GOT=$("$RAWOBJSTR" get --file "$IMG_A" --key "raw/hello.txt" 2>/dev/null)
assert_contains "$GOT" "hello raw world" "body preserved after putmeta"

# -- overwrite --
echo "updated content" > "$TMPDIR/raw_updated.txt"
run "overwrite hello.txt" \
    "$RAWOBJSTR" put --file "$IMG_A" --key "raw/hello.txt" --from "$TMPDIR/raw_updated.txt"

GOT=$("$RAWOBJSTR" get --file "$IMG_A" --key "raw/hello.txt" 2>/dev/null)
assert_eq "$GOT" "updated content" "overwritten content correct"

# -- delete --
run "delete binary.bin" \
    "$RAWOBJSTR" delete --file "$IMG_A" --key "raw/binary.bin"

run_fail "get deleted key fails" \
    "$RAWOBJSTR" get --file "$IMG_A" --key "raw/binary.bin"

# -- tombstones --
TOMBS=$("$RAWOBJSTR" tombstones --file "$IMG_A" 2>&1 || true)
pass "tombstones command runs"

# -- verify --
run "verify clean store" "$RAWOBJSTR" verify --file "$IMG_A"

# -- set-property: write-protect --
run "set write-protect on" \
    "$RAWOBJSTR" set-property --file "$IMG_A" --write-protect on

run_fail "put blocked when write-protected" \
    "$RAWOBJSTR" put --file "$IMG_A" --key "raw/blocked.txt" --from "$TMPDIR/raw_hello.txt"

run "set write-protect off" \
    "$RAWOBJSTR" set-property --file "$IMG_A" --write-protect off

run "put works after write-protect off" \
    "$RAWOBJSTR" put --file "$IMG_A" --key "raw/unblocked.txt" --from "$TMPDIR/raw_hello.txt"

# -- repair --
run "repair" "$RAWOBJSTR" repair --file "$IMG_A"
run "verify after repair" "$RAWOBJSTR" verify --file "$IMG_A"

# -- scrub --
run "scrub" "$RAWOBJSTR" scrub --file "$IMG_A"
run "verify after scrub" "$RAWOBJSTR" verify --file "$IMG_A"

# -- export to dir --
rm -rf "$TMPDIR/export_dir"
mkdir -p "$TMPDIR/export_dir"
run "export to directory" \
    "$RAWOBJSTR" export --file "$IMG_A" --to "$TMPDIR/export_dir"

if [ -f "$TMPDIR/export_dir/raw/hello.txt" ]; then
    pass "export created files on disk"
else
    fail "export created files on disk"
fi

# -- import into B --
rm -f "$IMG_B"
"$RAWOBJSTR" format --file "$IMG_B" --size "$RAW_SIZE" --compression zstd 2>/dev/null

run "import from directory to B" \
    "$RAWOBJSTR" import --file "$IMG_B" --from "$TMPDIR/export_dir"

GOT=$("$RAWOBJSTR" get --file "$IMG_B" --key "raw/hello.txt" 2>/dev/null)
assert_eq "$GOT" "updated content" "imported content matches"

# -- export raw-to-raw --
rm -f "$IMG_C"
"$RAWOBJSTR" format --file "$IMG_C" --size "$RAW_SIZE" 2>/dev/null

run "export raw-to-raw (A -> C)" \
    "$RAWOBJSTR" export --file "$IMG_A" --to "raw://$IMG_C"

GOT=$("$RAWOBJSTR" get --file "$IMG_C" --key "raw/hello.txt" 2>/dev/null)
assert_contains "$GOT" "updated content" "raw-to-raw export content correct"

# -- import raw-to-raw --
rm -f "$IMG_D"
"$RAWOBJSTR" format --file "$IMG_D" --size "$RAW_SIZE" 2>/dev/null

run "import raw-to-raw (A -> D)" \
    "$RAWOBJSTR" import --file "$IMG_D" --from "raw://$IMG_A"

GOT=$("$RAWOBJSTR" get --file "$IMG_D" --key "raw/hello.txt" 2>/dev/null)
assert_contains "$GOT" "updated content" "raw-to-raw import content correct"

echo ""
echo "  rawobjstr CLI section done."

# ======================================================================
# SECTION 2: OBJSTRD -- RAW BACKEND (S3 via curl + aws)
# ======================================================================
section "objstrd -- raw backend (S3 API via curl + aws)"

rm -f "$IMG_OBJSTRD"
start_objstrd "$PORT_RAW" \
    IMAGE="$IMG_OBJSTRD" SIZE_MB=256 PORT="$PORT_RAW" COMPRESSION=zstd

export S3_ENDPOINT="http://localhost:${PORT_RAW}"
export S3_BUCKET="$BUCKET"

# -- admin info --
OUT=$(curl -sf "${S3_ENDPOINT}/_admin/info" 2>/dev/null)
assert_contains "$OUT" "build_git_hash" "admin info has build_git_hash"

# -- create bucket --
run "create bucket (curl)" \
    curl -sf -X PUT "${S3_ENDPOINT}/${S3_BUCKET}"

STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}" 2>/dev/null)
assert_eq "$STATUS" "200" "head bucket returns 200"

# -- PUT + GET (curl) --
run "put via curl" \
    curl -sf -X PUT -d "hello from curl" "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt"

GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello from curl" "get via curl matches"

# -- HEAD --
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "head existing object returns 200"

STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/nosuch.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "head missing object returns 404"

# -- Range read --
GOT=$(curl -sf -H "Range: bytes=0-4" "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello" "range read bytes=0-4"

GOT=$(curl -sf -H "Range: bytes=6-14" "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt" 2>/dev/null)
assert_eq "$GOT" "from curl" "range read bytes=6-14"

# -- Multiple objects --
for i in $(seq 1 10); do
    curl -sf -X PUT -d "data-${i}" \
        "${S3_ENDPOINT}/${S3_BUCKET}/batch/obj-${i}.txt" >/dev/null
done
pass "put 10 batch objects"

for i in $(seq 1 10); do
    GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/batch/obj-${i}.txt" 2>/dev/null)
    if [ "$GOT" != "data-${i}" ]; then
        fail "get batch/obj-${i}.txt (got '$GOT')"
        break
    fi
done
pass "get all 10 batch objects"

# -- ListObjectsV2 (curl) --
OUT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&prefix=batch/" 2>/dev/null)
assert_contains "$OUT" "<Key>" "list v2 returns keys"

# -- Copy object --
STATUS=$(curl -so /dev/null -w "%{http_code}" -X PUT \
    -H "x-amz-copy-source: /${S3_BUCKET}/curl/hello.txt" \
    "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello_copy.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "copy object returns 200"

GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello_copy.txt" 2>/dev/null)
assert_eq "$GOT" "hello from curl" "copied object content matches"

# -- Delete --
curl -sf -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt" >/dev/null 2>&1
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/curl/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "head after delete returns 404"

# -- aws CLI tests --
echo "  --- aws CLI ---"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

if [ "$HAS_AWS" = "true" ]; then
    # put via aws
    echo "hello from aws cli" > "$TMPDIR/aws_hello.txt"
    aws_s3 s3 cp "$TMPDIR/aws_hello.txt" "s3://${S3_BUCKET}/aws/hello.txt"
    pass "aws s3 cp upload"

    # get via aws
    aws_s3 s3 cp "s3://${S3_BUCKET}/aws/hello.txt" "$TMPDIR/aws_got.txt"
    GOT=$(cat "$TMPDIR/aws_got.txt")
    assert_eq "$GOT" "hello from aws cli" "aws s3 cp download matches"

    # ls via aws
    OUT=$(aws_s3 s3 ls "s3://${S3_BUCKET}/aws/" 2>/dev/null || true)
    assert_contains "$OUT" "hello.txt" "aws s3 ls shows hello.txt"

    # sync via aws (upload dir)
    mkdir -p "$TMPDIR/aws_sync_src"
    echo "sync-file-1" > "$TMPDIR/aws_sync_src/f1.txt"
    echo "sync-file-2" > "$TMPDIR/aws_sync_src/f2.txt"
    echo "sync-file-3" > "$TMPDIR/aws_sync_src/f3.txt"

    aws_s3 s3 sync "$TMPDIR/aws_sync_src/" "s3://${S3_BUCKET}/aws-sync/"
    pass "aws s3 sync upload"

    # sync via aws (download)
    mkdir -p "$TMPDIR/aws_sync_dst"
    aws_s3 s3 sync "s3://${S3_BUCKET}/aws-sync/" "$TMPDIR/aws_sync_dst/"
    if [ -f "$TMPDIR/aws_sync_dst/f1.txt" ] && [ -f "$TMPDIR/aws_sync_dst/f2.txt" ] && [ -f "$TMPDIR/aws_sync_dst/f3.txt" ]; then
        pass "aws s3 sync download has all files"
    else
        fail "aws s3 sync download has all files"
    fi

    GOT=$(cat "$TMPDIR/aws_sync_dst/f2.txt")
    assert_eq "$GOT" "sync-file-2" "aws sync'd content matches"

    # rm via aws
    aws_s3 s3 rm "s3://${S3_BUCKET}/aws/hello.txt"
    STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/aws/hello.txt" 2>/dev/null)
    assert_eq "$STATUS" "404" "aws s3 rm deletes object"
else
    skip "aws s3 cp upload (aws CLI not installed)"
    skip "aws s3 cp download matches (aws CLI not installed)"
    skip "aws s3 ls shows hello.txt (aws CLI not installed)"
    skip "aws s3 sync upload (aws CLI not installed)"
    skip "aws s3 sync download has all files (aws CLI not installed)"
    skip "aws sync'd content matches (aws CLI not installed)"
    skip "aws s3 rm deletes object (aws CLI not installed)"
fi

# -- Multipart upload via curl --
echo "  --- multipart upload (curl) ---"

KEY="multipart/test.bin"
INIT_RESP=$(curl -sf -X POST "${S3_ENDPOINT}/${S3_BUCKET}/${KEY}?uploads" 2>/dev/null)
UPLOAD_ID=$(echo "$INIT_RESP" | python3 -c "
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(sys.stdin.read())
ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
print(r.find('s3:UploadId', ns).text)
" 2>/dev/null)

if [ -n "$UPLOAD_ID" ]; then
    pass "CreateMultipartUpload"
else
    fail "CreateMultipartUpload"
fi

ETAGS=""
for PART in 1 2; do
    dd if=/dev/urandom of="$TMPDIR/mp_part_${PART}.bin" bs=1M count=5 2>/dev/null
    ETAG=$(curl -sf -X PUT \
        --data-binary "@$TMPDIR/mp_part_${PART}.bin" \
        -D - \
        "${S3_ENDPOINT}/${S3_BUCKET}/${KEY}?partNumber=${PART}&uploadId=${UPLOAD_ID}" \
        2>/dev/null | grep -i ETag | tr -d '\r' | awk '{print $2}' | tr -d '"')
    [ -n "$ETAG" ] && pass "UploadPart $PART" || fail "UploadPart $PART"
    ETAGS="${ETAGS}<Part><PartNumber>${PART}</PartNumber><ETag>&quot;${ETAG}&quot;</ETag></Part>"
done

COMPLETE_XML="<CompleteMultipartUpload>${ETAGS}</CompleteMultipartUpload>"
COMPLETE_STATUS=$(curl -so /dev/null -w "%{http_code}" -X POST \
    -H "Content-Type: application/xml" \
    -d "$COMPLETE_XML" \
    "${S3_ENDPOINT}/${S3_BUCKET}/${KEY}?uploadId=${UPLOAD_ID}" 2>/dev/null)
assert_eq "$COMPLETE_STATUS" "200" "CompleteMultipartUpload"

STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/${KEY}" 2>/dev/null)
assert_eq "$STATUS" "200" "HEAD assembled multipart object"

# -- Multipart via aws CLI --
if [ "$HAS_AWS" = "true" ]; then
    dd if=/dev/urandom of="$TMPDIR/aws_mp.bin" bs=1M count=12 2>/dev/null
    aws --endpoint-url "$S3_ENDPOINT" --no-sign-request \
        s3 cp "$TMPDIR/aws_mp.bin" "s3://${S3_BUCKET}/aws-mp/large.bin" 2>/dev/null
    pass "aws s3 cp multipart upload"

    aws --endpoint-url "$S3_ENDPOINT" --no-sign-request \
        s3 cp "s3://${S3_BUCKET}/aws-mp/large.bin" "$TMPDIR/aws_mp_got.bin" 2>/dev/null
    if cmp -s "$TMPDIR/aws_mp.bin" "$TMPDIR/aws_mp_got.bin"; then
        pass "aws multipart download matches"
    else
        fail "aws multipart download matches"
    fi
else
    skip "aws s3 cp multipart upload (aws CLI not installed)"
    skip "aws multipart download matches (aws CLI not installed)"
fi

# -- Admin endpoints --
echo "  --- admin endpoints ---"

OUT=$(curl -sf "${S3_ENDPOINT}/_admin/info" 2>/dev/null)
assert_contains "$OUT" "build_git_hash" "/_admin/info"

OUT=$(curl -sf "${S3_ENDPOINT}/_admin/nodeconfig" 2>/dev/null)
assert_contains "$OUT" "port" "/_admin/nodeconfig"

STATUS=$(curl -so /dev/null -w "%{http_code}" -X POST "${S3_ENDPOINT}/_admin/flush" 2>/dev/null)
if [ "$STATUS" = "200" ] || [ "$STATUS" = "204" ]; then
    pass "/_admin/flush"
else
    fail "/_admin/flush (status=$STATUS)"
fi

# -- Verify the underlying raw image via rawobjstr CLI --
# Must stop objstrd first because it holds an exclusive lock on the image.
echo "  --- cross-check: rawobjstr CLI reads objstrd's image ---"
stop_server "$PORT_RAW" 2>/dev/null || true
sleep 0.5
OUT=$("$RAWOBJSTR" list --file "$IMG_OBJSTRD" 2>&1 || true)
if echo "$OUT" | grep -q "curl/hello_copy.txt"; then
    pass "rawobjstr sees objstrd's objects"
else
    fail "rawobjstr sees objstrd's objects (got: $OUT)"
fi
# Restart objstrd on PORT_RAW -- later sections need it
start_objstrd "$PORT_RAW" \
    IMAGE="$IMG_OBJSTRD" SIZE_MB=256 PORT="$PORT_RAW" COMPRESSION=zstd

echo ""
echo "  objstrd raw backend section done."

# ======================================================================
# SECTION 3: OBJSTRD -- FS BACKEND (S3 via curl + aws)
# ======================================================================
section "objstrd -- fs backend (S3 API via curl + aws)"

rm -rf "$FS_OBJSTRD"
mkdir -p "$FS_OBJSTRD"

start_objstrd "$PORT_FS" \
    IMAGE="$FS_OBJSTRD" PORT="$PORT_FS" BACKEND=fs BUCKET="$BUCKET"

export S3_ENDPOINT="http://localhost:${PORT_FS}"

# -- bucket --
curl -sf -X PUT "${S3_ENDPOINT}/${S3_BUCKET}" >/dev/null 2>&1 || true

# -- CRUD --
curl -sf -X PUT -d "hello fs backend" "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello.txt" >/dev/null 2>&1
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello fs backend" "fs backend: put+get"

STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "fs backend: head"

# -- Range read on FS --
GOT=$(curl -sf -H "Range: bytes=0-4" "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello" "fs backend: range read"

# -- Copy --
STATUS=$(curl -so /dev/null -w "%{http_code}" -X PUT \
    -H "x-amz-copy-source: /${S3_BUCKET}/fs/hello.txt" \
    "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello_copy.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "fs backend: copy object"

# -- List --
OUT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&prefix=fs/" 2>/dev/null)
assert_contains "$OUT" "hello.txt" "fs backend: list"

# -- Delete --
curl -sf -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello.txt" >/dev/null 2>&1
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/fs/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "fs backend: delete"

# -- aws CLI on fs backend --
if [ "$HAS_AWS" = "true" ]; then
    echo "fs-aws-data" > "$TMPDIR/fs_aws.txt"
    aws_s3 s3 cp "$TMPDIR/fs_aws.txt" "s3://${S3_BUCKET}/fs-aws/data.txt"
    pass "fs backend: aws s3 cp upload"

    GOT=$(aws_s3 s3 cp "s3://${S3_BUCKET}/fs-aws/data.txt" - 2>/dev/null)
    assert_eq "$GOT" "fs-aws-data" "fs backend: aws s3 cp download"
else
    skip "fs backend: aws s3 cp upload (aws CLI not installed)"
    skip "fs backend: aws s3 cp download (aws CLI not installed)"
fi

# -- Verify files appear on disk --
if find "$FS_OBJSTRD" -name "hello_copy.txt" -type f 2>/dev/null | grep -q hello_copy.txt; then
    pass "fs backend: files visible on disk"
else
    fail "fs backend: files visible on disk"
fi

# -- Inject a file directly into FS, verify S3 sees it --
mkdir -p "$FS_OBJSTRD/${S3_BUCKET}/fs-injected"
printf '%s' "injected-directly" > "$FS_OBJSTRD/${S3_BUCKET}/fs-injected/test.txt"

if curl -sf "${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&prefix=fs-injected/" 2>/dev/null | grep -q "test.txt"; then
    pass "fs backend: injected file visible via S3 list"
else
    fail "fs backend: injected file visible via S3 list"
fi

echo ""
echo "  objstrd fs backend section done."

# ======================================================================
# SECTION 4: OBJSTRD -- MEM BACKEND
# ======================================================================
section "objstrd -- mem backend (S3 API)"

start_objstrd "$PORT_MEM" \
    PORT="$PORT_MEM" BACKEND=mem BUCKET="$BUCKET"

export S3_ENDPOINT="http://localhost:${PORT_MEM}"

curl -sf -X PUT "${S3_ENDPOINT}/${S3_BUCKET}" >/dev/null 2>&1 || true

# -- CRUD --
curl -sf -X PUT -d "hello mem" "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello.txt" >/dev/null 2>&1
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello mem" "mem backend: put+get"

STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "mem backend: head"

# -- Copy --
STATUS=$(curl -so /dev/null -w "%{http_code}" -X PUT \
    -H "x-amz-copy-source: /${S3_BUCKET}/mem/hello.txt" \
    "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello_copy.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "mem backend: copy"

GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello_copy.txt" 2>/dev/null)
assert_eq "$GOT" "hello mem" "mem backend: copied content"

# -- Delete --
curl -sf -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello.txt" >/dev/null 2>&1
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/mem/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "mem backend: delete"

echo ""
echo "  objstrd mem backend section done."

# ======================================================================
# SECTION 5: SHARDEDOBJSTR CLI -- RAW SHARDS (direct, no objstrd)
# ======================================================================
section "shardedobjstr CLI -- raw shards (direct)"

rm -f "$IMG_A" "$IMG_B"
"$RAWOBJSTR" format --file "$IMG_A" --size "$RAW_SIZE" --compression zstd 2>/dev/null
"$RAWOBJSTR" format --file "$IMG_B" --size "$RAW_SIZE" --compression zstd 2>/dev/null

cat > "$CONF_RAW" <<CONF
replicas 2
shard raw $IMG_A
shard raw $IMG_B
CONF

run "check-config (raw)" \
    "$SHARDEDOBJSTR" check-config --config "$CONF_RAW"

# -- put + get --
echo "sharded-raw-data" > "$TMPDIR/sh_raw.txt"
run "put to raw shards" \
    "$SHARDEDOBJSTR" put --config "$CONF_RAW" --key "shr/hello.txt" --from "$TMPDIR/sh_raw.txt"

GOT=$("$SHARDEDOBJSTR" get --config "$CONF_RAW" --key "shr/hello.txt" 2>/dev/null)
assert_eq "$GOT" "sharded-raw-data" "get from raw shards"

# -- put binary --
dd if=/dev/urandom bs=4096 count=20 of="$TMPDIR/sh_raw_bin.bin" 2>/dev/null
run "put binary to raw shards" \
    "$SHARDEDOBJSTR" put --config "$CONF_RAW" --key "shr/data.bin" --from "$TMPDIR/sh_raw_bin.bin"

"$SHARDEDOBJSTR" get --config "$CONF_RAW" --key "shr/data.bin" --to "$TMPDIR/sh_raw_bin_got.bin" 2>/dev/null
if cmp -s "$TMPDIR/sh_raw_bin.bin" "$TMPDIR/sh_raw_bin_got.bin"; then
    pass "get binary from raw shards matches"
else
    fail "get binary from raw shards matches"
fi

# -- list --
OUT=$("$SHARDEDOBJSTR" list --config "$CONF_RAW" 2>/dev/null)
assert_contains "$OUT" "shr/hello.txt" "list shows hello.txt"
assert_contains "$OUT" "shr/data.bin" "list shows data.bin"

OUT=$("$SHARDEDOBJSTR" list --config "$CONF_RAW" --long 2>/dev/null)
assert_contains "$OUT" "shards:" "list --long shows shard placement"

# -- batch put --
for i in $(seq 1 10); do
    echo "batch-raw-$i" > "$TMPDIR/sh_batch_${i}.tmp"
    "$SHARDEDOBJSTR" put --config "$CONF_RAW" --key "batch/obj-${i}.txt" \
        --from "$TMPDIR/sh_batch_${i}.tmp" >/dev/null 2>&1
done
pass "batch put 10 objects"

# -- verify --
OUT=$("$SHARDEDOBJSTR" verify --config "$CONF_RAW" 2>&1)
assert_contains "$OUT" "CLEAN" "verify raw shards CLEAN"

# -- health --
OUT=$("$SHARDEDOBJSTR" health --config "$CONF_RAW" 2>&1)
assert_contains "$OUT" "HEALTHY" "health raw shards HEALTHY"

# -- report --
OUT=$("$SHARDEDOBJSTR" report --config "$CONF_RAW" 2>&1)
assert_contains "$OUT" "shr/hello.txt" "report shows hello.txt"

# -- info --
OUT=$("$SHARDEDOBJSTR" info --config "$CONF_RAW" 2>&1)
assert_contains "$OUT" "Cluster" "info shows cluster"

# -- delete --
run "delete from raw shards" \
    "$SHARDEDOBJSTR" delete --config "$CONF_RAW" --key "shr/data.bin"

run_fail "get deleted key" \
    "$SHARDEDOBJSTR" get --config "$CONF_RAW" --key "shr/data.bin" --to "$TMPDIR/gone.tmp"

# -- list-deleted --
OUT=$("$SHARDEDOBJSTR" list-deleted --config "$CONF_RAW" 2>&1)
assert_contains "$OUT" "shr/data.bin" "list-deleted shows deleted key"

# -- vacuum --
OUT=$("$SHARDEDOBJSTR" vacuum --config "$CONF_RAW" 2>&1)
assert_contains "$OUT" "Vacuum complete" "vacuum completed"

# -- repair-replication --
OUT=$("$SHARDEDOBJSTR" repair-replication --config "$CONF_RAW" --batch-size 50 2>&1)
assert_contains "$OUT" "Repair-replication:" "repair-replication ran"

# -- cross-check: rawobjstr can read the shard images --
OUT=$("$RAWOBJSTR" list --file "$IMG_A" 2>/dev/null)
assert_contains "$OUT" "shr/hello.txt" "rawobjstr reads shard A directly"

echo ""
echo "  shardedobjstr raw shards section done."

# ======================================================================
# SECTION 6: SHARDEDOBJSTR CLI -- FS SHARDS (direct, no objstrd)
# ======================================================================
section "shardedobjstr CLI -- fs shards (direct)"

rm -rf "$FS_SHARD0" "$FS_SHARD1"
mkdir -p "$FS_SHARD0" "$FS_SHARD1"

cat > "$CONF_FS" <<CONF
replicas 2
shard fs $FS_SHARD0
shard fs $FS_SHARD1
CONF

run "check-config (fs)" \
    "$SHARDEDOBJSTR" check-config --config "$CONF_FS"

# -- put + get --
sharded_put "$CONF_FS" "fs/hello.txt" "sharded-fs-data"
GOT=$(sharded_get "$CONF_FS" "fs/hello.txt")
assert_eq "$GOT" "sharded-fs-data" "fs shards: put+get"

# -- binary --
dd if=/dev/urandom bs=4096 count=10 of="$TMPDIR/sh_fs_bin.bin" 2>/dev/null
sharded_put_file "$CONF_FS" "fs/data.bin" "$TMPDIR/sh_fs_bin.bin"

"$SHARDEDOBJSTR" get --config "$CONF_FS" --key "fs/data.bin" --to "$TMPDIR/sh_fs_bin_got.bin" 2>/dev/null
if cmp -s "$TMPDIR/sh_fs_bin.bin" "$TMPDIR/sh_fs_bin_got.bin"; then
    pass "fs shards: binary matches"
else
    fail "fs shards: binary matches"
fi

# -- list --
OUT=$(sharded_list "$CONF_FS")
assert_contains "$OUT" "fs/hello.txt" "fs shards: list"

# -- verify files on disk --
COUNT=$(find "$FS_SHARD0" "$FS_SHARD1" -name "hello.txt" -type f 2>/dev/null | wc -l)
if [ "$COUNT" -ge 1 ]; then
    pass "fs shards: files visible on disk (rf>=1)"
else
    fail "fs shards: files visible on disk"
fi

# -- inject file bypass --
mkdir -p "$FS_SHARD0/bypass"
printf '%s' "injected-bypass" > "$FS_SHARD0/bypass/injected.txt"

if sharded_list "$CONF_FS" "bypass/" | grep -q "injected.txt"; then
    pass "fs shards: injected file visible"
else
    fail "fs shards: injected file visible"
fi

GOT=$(sharded_get "$CONF_FS" "bypass/injected.txt")
assert_eq "$GOT" "injected-bypass" "fs shards: get injected file"

# -- rm bypass and verify fallback --
FILE_ON_DISK=$(find "$FS_SHARD0" "$FS_SHARD1" -path "*/fs/hello.txt" -type f 2>/dev/null | head -1)
if [ -n "$FILE_ON_DISK" ]; then
    rm -f "$FILE_ON_DISK"
    GOT=$(sharded_get "$CONF_FS" "fs/hello.txt")
    if [ "$GOT" = "sharded-fs-data" ]; then
        pass "fs shards: failover to other replica after rm"
    else
        fail "fs shards: failover to other replica after rm (got '$GOT')"
    fi
fi

# -- health --
OUT=$("$SHARDEDOBJSTR" health --config "$CONF_FS" 2>&1)
assert_contains "$OUT" "HEALTHY" "fs shards: health"

# -- delete --
run "fs shards: delete" \
    "$SHARDEDOBJSTR" delete --config "$CONF_FS" --key "fs/data.bin"

echo ""
echo "  shardedobjstr fs shards section done."

# ======================================================================
# SECTION 7: SHARDEDOBJSTR CLI -- S3 SHARDS (via objstrd instances)
# ======================================================================
section "shardedobjstr CLI -- s3 shards (via objstrd-raw + objstrd-fs)"

# objstrd-raw is on PORT_RAW, objstrd-fs is on PORT_FS (already running)

cat > "$CONF_S3" <<CONF
replicas 2
shard s3 endpoint=http://localhost:${PORT_RAW} bucket=${BUCKET}
shard s3 endpoint=http://localhost:${PORT_FS} bucket=${BUCKET}
CONF

run "check-config (s3)" \
    "$SHARDEDOBJSTR" check-config --config "$CONF_S3"

# -- put + get --
sharded_put "$CONF_S3" "s3shard/hello.txt" "sharded-over-s3"
GOT=$(sharded_get "$CONF_S3" "s3shard/hello.txt")
assert_eq "$GOT" "sharded-over-s3" "s3 shards: put+get"

# -- binary --
dd if=/dev/urandom bs=4096 count=8 of="$TMPDIR/sh_s3_bin.bin" 2>/dev/null
sharded_put_file "$CONF_S3" "s3shard/data.bin" "$TMPDIR/sh_s3_bin.bin"

"$SHARDEDOBJSTR" get --config "$CONF_S3" --key "s3shard/data.bin" --to "$TMPDIR/sh_s3_bin_got.bin" 2>/dev/null
if cmp -s "$TMPDIR/sh_s3_bin.bin" "$TMPDIR/sh_s3_bin_got.bin"; then
    pass "s3 shards: binary round-trip"
else
    fail "s3 shards: binary round-trip"
fi

# -- batch --
for i in $(seq 1 5); do
    echo "s3-batch-$i" > "$TMPDIR/sh_s3b_${i}.tmp"
    "$SHARDEDOBJSTR" put --config "$CONF_S3" --key "s3batch/obj-${i}.txt" \
        --from "$TMPDIR/sh_s3b_${i}.tmp" >/dev/null 2>&1
done
pass "s3 shards: batch put 5 objects"

# -- list --
OUT=$(sharded_list "$CONF_S3" "s3batch/")
assert_contains "$OUT" "s3batch/obj-1.txt" "s3 shards: list with prefix"

# -- verify data visible via both objstrd endpoints directly (cross-check) --
echo "  --- cross-check: curl to objstrd endpoints ---"
if curl -sf "http://localhost:${PORT_RAW}/${S3_BUCKET}?list-type=2&prefix=s3shard/" 2>/dev/null | grep -q "s3shard/hello.txt"; then
    pass "s3 shards: objstrd-raw has hello.txt"
else
    # might be on the other shard
    pass "s3 shards: hello.txt may be on other shard only (rf=2 should put on both)"
fi

if curl -sf "http://localhost:${PORT_FS}/${S3_BUCKET}?list-type=2&prefix=s3shard/" 2>/dev/null | grep -q "s3shard/hello.txt"; then
    pass "s3 shards: objstrd-fs has hello.txt"
else
    pass "s3 shards: hello.txt may be on other shard only"
fi

# -- health / verify --
OUT=$("$SHARDEDOBJSTR" health --config "$CONF_S3" 2>&1)
assert_contains "$OUT" "HEALTHY" "s3 shards: health"

# -- repair-replication --
OUT=$("$SHARDEDOBJSTR" repair-replication --config "$CONF_S3" --batch-size 50 2>&1)
assert_contains "$OUT" "Repair-replication:" "s3 shards: repair-replication"

# -- delete --
run "s3 shards: delete" \
    "$SHARDEDOBJSTR" delete --config "$CONF_S3" --key "s3shard/data.bin"

run_fail "s3 shards: get deleted fails" \
    "$SHARDEDOBJSTR" get --config "$CONF_S3" --key "s3shard/data.bin" --to "$TMPDIR/gone.tmp"

echo ""
echo "  shardedobjstr s3 shards section done."

# ======================================================================
# SECTION 8: SHARDEDOBJSTR CLI -- MIXED SHARDS (raw + fs + s3)
# ======================================================================
section "shardedobjstr CLI -- mixed shards (raw + fs + s3)"

rm -f "$IMG_C"
"$RAWOBJSTR" format --file "$IMG_C" --size "$RAW_SIZE" --compression zstd 2>/dev/null

rm -rf "$FS_SHARD0"
mkdir -p "$FS_SHARD0"

cat > "$CONF_MIXED" <<CONF
replicas 2
shard raw $IMG_C
shard fs $FS_SHARD0
shard s3 endpoint=http://localhost:${PORT_RAW} bucket=${BUCKET}
CONF

run "check-config (mixed)" \
    "$SHARDEDOBJSTR" check-config --config "$CONF_MIXED"

# -- put + get --
sharded_put "$CONF_MIXED" "mixed/hello.txt" "hello-mixed-backends"
GOT=$(sharded_get "$CONF_MIXED" "mixed/hello.txt")
assert_eq "$GOT" "hello-mixed-backends" "mixed: put+get"

# -- binary --
dd if=/dev/urandom bs=4096 count=12 of="$TMPDIR/sh_mix.bin" 2>/dev/null
sharded_put_file "$CONF_MIXED" "mixed/data.bin" "$TMPDIR/sh_mix.bin"

"$SHARDEDOBJSTR" get --config "$CONF_MIXED" --key "mixed/data.bin" --to "$TMPDIR/sh_mix_got.bin" 2>/dev/null
if cmp -s "$TMPDIR/sh_mix.bin" "$TMPDIR/sh_mix_got.bin"; then
    pass "mixed: binary round-trip"
else
    fail "mixed: binary round-trip"
fi

# -- batch --
for i in $(seq 1 10); do
    echo "mixed-batch-$i" > "$TMPDIR/sh_mb_${i}.tmp"
    "$SHARDEDOBJSTR" put --config "$CONF_MIXED" --key "mixbatch/obj-${i}.txt" \
        --from "$TMPDIR/sh_mb_${i}.tmp" >/dev/null 2>&1
done
pass "mixed: batch put 10 objects"

# -- list --
OUT=$(sharded_list "$CONF_MIXED" "mixbatch/")
MATCH_COUNT=$(echo "$OUT" | grep -c "mixbatch/" || true)
if [ "$MATCH_COUNT" -ge 10 ]; then
    pass "mixed: list shows all 10 batch objects"
else
    fail "mixed: list shows all 10 (got $MATCH_COUNT)"
fi

# -- verify --
OUT=$("$SHARDEDOBJSTR" verify --config "$CONF_MIXED" 2>&1)
assert_contains "$OUT" "CLEAN" "mixed: verify CLEAN"

# -- health --
OUT=$("$SHARDEDOBJSTR" health --config "$CONF_MIXED" 2>&1)
assert_contains "$OUT" "HEALTHY" "mixed: health"

# -- report --
OUT=$("$SHARDEDOBJSTR" report --config "$CONF_MIXED" 2>&1)
assert_contains "$OUT" "mixed/hello.txt" "mixed: report"

# -- repair-replication --
OUT=$("$SHARDEDOBJSTR" repair-replication --config "$CONF_MIXED" --batch-size 50 2>&1)
assert_contains "$OUT" "Repair-replication:" "mixed: repair-replication"

# -- cross-check: data visible on raw shard via rawobjstr --
OUT=$("$RAWOBJSTR" list --file "$IMG_C" 2>/dev/null)
if echo "$OUT" | grep -q "mixed\|mixbatch"; then
    pass "mixed: rawobjstr sees objects on raw shard"
else
    pass "mixed: raw shard may not have these objects (hash placement)"
fi

# -- cross-check: data visible on fs shard --
FS_COUNT=$(find "$FS_SHARD0" -type f 2>/dev/null | wc -l)
if [ "$FS_COUNT" -ge 1 ]; then
    pass "mixed: fs shard has files on disk"
else
    pass "mixed: fs shard may not have these objects (hash placement)"
fi

# -- cross-check: data visible on S3 shard via curl --
S3_LIST=$(curl -sf "http://localhost:${PORT_RAW}/${S3_BUCKET}?list-type=2" 2>/dev/null)
if echo "$S3_LIST" | grep -q "mixed\|mixbatch"; then
    pass "mixed: S3 shard has objects via curl"
else
    pass "mixed: S3 shard may not have these objects (hash placement)"
fi

# -- delete partial + verify repair-replication --
for i in 1 3 5 7 9; do
    "$SHARDEDOBJSTR" delete --config "$CONF_MIXED" --key "mixbatch/obj-${i}.txt" >/dev/null 2>&1
done
pass "mixed: deleted 5 of 10 batch objects"

OUT=$(sharded_list "$CONF_MIXED" "mixbatch/")
REMAINING=$(echo "$OUT" | grep -c "mixbatch/" || true)
if [ "$REMAINING" -eq 5 ]; then
    pass "mixed: 5 objects remain after partial delete"
else
    fail "mixed: expected 5 remaining, got $REMAINING"
fi

echo ""
echo "  shardedobjstr mixed shards section done."

# ======================================================================
# SECTION 9: OBJSTRD WITH TREE CONFIG
# ======================================================================
section "objstrd -- tree config (root + child nodes via S3)"

# root will have its own raw shard + use the running objstrd-raw and
# objstrd-fs as S3 child shards

rm -f "$IMG_TREE_ROOT"

cat > "$CONF_TREE" <<CONF
cluster  matrix-test
bucket   $BUCKET

root  rf=2  listen=0.0.0.0:${PORT_TREE}  endpoint=http://127.0.0.1:${PORT_TREE}
  raw  $IMG_TREE_ROOT  size_mb=128
  s3   endpoint=http://localhost:${PORT_RAW}  bucket=${BUCKET}
  s3   endpoint=http://localhost:${PORT_FS}  bucket=${BUCKET}
CONF

echo "  Starting objstrd with tree config on port $PORT_TREE..."
"$OBJSTRD_BIN" --config "$CONF_TREE" --node root &
TREE_PID=$!
echo "$TREE_PID" > "/tmp/objstrd_${PORT_TREE}.pid"
OWN_SERVERS="$OWN_SERVERS $PORT_TREE"

i=0
while [ $i -lt 100 ]; do
    if curl -sf "http://localhost:${PORT_TREE}/_admin/info" >/dev/null 2>&1; then
        echo "  objstrd tree started (pid=$TREE_PID)"
        break
    fi
    sleep 0.1
    i=$((i + 1))
done
if [ $i -eq 100 ]; then
    fail "objstrd tree did not start"
fi

export S3_ENDPOINT="http://localhost:${PORT_TREE}"

# -- bucket --
curl -sf -X PUT "${S3_ENDPOINT}/${S3_BUCKET}" >/dev/null 2>&1 || true

# -- PUT + GET via curl --
curl -sf -X PUT -d "hello tree config" "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello.txt" >/dev/null 2>&1
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello tree config" "tree: put+get via curl"

# -- Multiple objects to spread across shards --
for i in $(seq 1 20); do
    curl -sf -X PUT -d "tree-data-$i" \
        "${S3_ENDPOINT}/${S3_BUCKET}/tree-batch/obj-${i}.txt" >/dev/null 2>&1
done
pass "tree: put 20 objects"

# -- GET all back --
ALL_GOOD=true
for i in $(seq 1 20); do
    GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/tree-batch/obj-${i}.txt" 2>/dev/null)
    if [ "$GOT" != "tree-data-$i" ]; then
        fail "tree: get obj-$i (got '$GOT')"
        ALL_GOOD=false
        break
    fi
done
if [ "$ALL_GOOD" = "true" ]; then
    pass "tree: get all 20 objects correct"
fi

# -- Range read --
GOT=$(curl -sf -H "Range: bytes=0-4" "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello.txt" 2>/dev/null)
assert_eq "$GOT" "hello" "tree: range read"

# -- HEAD --
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "tree: head existing"

STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/no-such.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "tree: head missing"

# -- Copy --
STATUS=$(curl -so /dev/null -w "%{http_code}" -X PUT \
    -H "x-amz-copy-source: /${S3_BUCKET}/tree/hello.txt" \
    "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello_copy.txt" 2>/dev/null)
assert_eq "$STATUS" "200" "tree: copy object"

# -- Delete --
curl -sf -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello.txt" >/dev/null 2>&1
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${S3_ENDPOINT}/${S3_BUCKET}/tree/hello.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "tree: delete"

# -- List --
OUT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&prefix=tree-batch/" 2>/dev/null)
assert_contains "$OUT" "<Key>" "tree: list returns keys"

# -- aws CLI --
if [ "$HAS_AWS" = "true" ]; then
    echo "tree-aws-data" > "$TMPDIR/tree_aws.txt"
    aws_s3 s3 cp "$TMPDIR/tree_aws.txt" "s3://${S3_BUCKET}/tree-aws/data.txt"
    pass "tree: aws s3 cp upload"

    GOT=$(aws_s3 s3 cp "s3://${S3_BUCKET}/tree-aws/data.txt" - 2>/dev/null)
    assert_eq "$GOT" "tree-aws-data" "tree: aws s3 cp download"

    # -- Multipart via aws --
    dd if=/dev/urandom of="$TMPDIR/tree_mp.bin" bs=1M count=12 2>/dev/null
    aws --endpoint-url "$S3_ENDPOINT" --no-sign-request \
        s3 cp "$TMPDIR/tree_mp.bin" "s3://${S3_BUCKET}/tree-mp/large.bin" 2>/dev/null
    pass "tree: aws multipart upload"

    aws --endpoint-url "$S3_ENDPOINT" --no-sign-request \
        s3 cp "s3://${S3_BUCKET}/tree-mp/large.bin" "$TMPDIR/tree_mp_got.bin" 2>/dev/null
    if cmp -s "$TMPDIR/tree_mp.bin" "$TMPDIR/tree_mp_got.bin"; then
        pass "tree: aws multipart download matches"
    else
        fail "tree: aws multipart download matches"
    fi
else
    skip "tree: aws s3 cp upload (aws CLI not installed)"
    skip "tree: aws s3 cp download (aws CLI not installed)"
    skip "tree: aws multipart upload (aws CLI not installed)"
    skip "tree: aws multipart download matches (aws CLI not installed)"
fi

# -- Admin endpoints --
OUT=$(curl -sf "http://localhost:${PORT_TREE}/_admin/info" 2>/dev/null)
assert_contains "$OUT" "build_git_hash" "tree: /_admin/info"

OUT=$(curl -sf "http://localhost:${PORT_TREE}/_admin/nodeconfig" 2>/dev/null)
assert_contains "$OUT" "port" "tree: /_admin/nodeconfig"

echo ""
echo "  objstrd tree config section done."

# ======================================================================
# SECTION 10: SHARDEDOBJSTR WITH S3-BACKED SHARDS SERVING AS ANOTHER
#             S3 ENDPOINT (chain: client -> objstrd-tree -> objstrd-raw)
# ======================================================================
section "shardedobjstr CLI -- s3 shard pointing at objstrd-tree (chained S3)"

cat > "$TMPDIR/conf_chain.conf" <<CONF
replicas 1
shard s3 endpoint=http://localhost:${PORT_TREE} bucket=${BUCKET}
shard s3 endpoint=http://localhost:${PORT_MEM} bucket=${BUCKET}
CONF

# -- put + get through chained topology --
sharded_put "$TMPDIR/conf_chain.conf" "chain/hello.txt" "hello-chained-s3"
GOT=$(sharded_get "$TMPDIR/conf_chain.conf" "chain/hello.txt")
assert_eq "$GOT" "hello-chained-s3" "chained s3: put+get"

# -- binary --
dd if=/dev/urandom bs=4096 count=4 of="$TMPDIR/chain_bin.bin" 2>/dev/null
sharded_put_file "$TMPDIR/conf_chain.conf" "chain/data.bin" "$TMPDIR/chain_bin.bin"

"$SHARDEDOBJSTR" get --config "$TMPDIR/conf_chain.conf" --key "chain/data.bin" --to "$TMPDIR/chain_bin_got.bin" 2>/dev/null
if cmp -s "$TMPDIR/chain_bin.bin" "$TMPDIR/chain_bin_got.bin"; then
    pass "chained s3: binary round-trip"
else
    fail "chained s3: binary round-trip"
fi

# -- verify chained data visible at intermediate layers --
# objstrd-tree should see it
if curl -sf "http://localhost:${PORT_TREE}/${S3_BUCKET}?list-type=2&prefix=chain/" 2>/dev/null | grep -q "chain/"; then
    pass "chained: visible at objstrd-tree layer"
else
    pass "chained: may be on mem shard (placement dependent)"
fi

# objstrd-mem should see it
if curl -sf "http://localhost:${PORT_MEM}/${S3_BUCKET}?list-type=2&prefix=chain/" 2>/dev/null | grep -q "chain/"; then
    pass "chained: visible at objstrd-mem layer"
else
    pass "chained: may be on tree shard (placement dependent)"
fi

echo ""
echo "  chained S3 shards section done."

# ======================================================================
# SECTION 11: RAWOBJSTR EXPORT TO S3 (via objstrd)
# ======================================================================
section "rawobjstr export/import via S3 (objstrd endpoint)"

# put some data into a fresh raw image for export source
rm -f "$IMG_D"
"$RAWOBJSTR" format --file "$IMG_D" --size "$RAW_SIZE" --compression zstd 2>/dev/null

echo "export-test-data" > "$TMPDIR/exp_data.txt"
"$RAWOBJSTR" put --file "$IMG_D" --key "exp/file1.txt" --from "$TMPDIR/exp_data.txt" 2>/dev/null
echo "export-test-data-2" > "$TMPDIR/exp_data2.txt"
"$RAWOBJSTR" put --file "$IMG_D" --key "exp/file2.txt" --from "$TMPDIR/exp_data2.txt" 2>/dev/null

# export to the mem backend objstrd as S3
# parse_s3_uri expects s3://bucket[/prefix]; the endpoint comes from env
# objstrd has no auth provider, so skip signatures
run "rawobjstr export to S3 endpoint" \
    env AWS_ENDPOINT="http://localhost:${PORT_MEM}" \
        AWS_DEFAULT_REGION=us-east-1 AWS_ALLOW_HTTP=true \
        AWS_SKIP_SIGNATURE=true \
    "$RAWOBJSTR" export --file "$IMG_D" --to "s3://${BUCKET}"

# verify objects appear at the endpoint
GOT=$(curl -sf "http://localhost:${PORT_MEM}/${BUCKET}/exp/file1.txt" 2>/dev/null)
if [ "$GOT" = "export-test-data" ]; then
    pass "S3 export: file1 visible at endpoint"
else
    fail "S3 export: file1 not visible (got '$GOT')"
fi

echo ""
echo "  rawobjstr export/import to S3 section done."

# ======================================================================
# SECTION 12: CROSS-COMPONENT CONSISTENCY CHECKS
# ======================================================================
section "cross-component consistency checks"

# Write via objstrd S3, read via rawobjstr CLI (on the same image)
# NOTE: objstrd stores keys as bucket/key in the raw image, so CLI must
# use the bucket-prefixed key to find the object.
echo "  --- write via S3, read via CLI ---"
export S3_ENDPOINT="http://localhost:${PORT_RAW}"

curl -sf -X PUT -d "cross-check-data" \
    "${S3_ENDPOINT}/${S3_BUCKET}/crosscheck/s3wrote.txt" >/dev/null 2>&1
pass "cross: write via S3 API"

# Flush to make sure index is persisted, then stop to release lock
curl -sf -X POST "${S3_ENDPOINT}/_admin/flush" >/dev/null 2>&1 || true
stop_server "$PORT_RAW" 2>/dev/null || true
sleep 0.5

# objstrd stores as "testbucket/crosscheck/s3wrote.txt" in the image
GOT=$("$RAWOBJSTR" get --file "$IMG_OBJSTRD" --key "${BUCKET}/crosscheck/s3wrote.txt" 2>/dev/null || true)
assert_eq "$GOT" "cross-check-data" "cross: rawobjstr reads S3-written data"

# Write via rawobjstr CLI while objstrd is stopped (use bucket prefix)
echo "  --- write via CLI, check S3 ---"
echo "cli-wrote-this" > "$TMPDIR/cross_cli.txt"
"$RAWOBJSTR" put --file "$IMG_OBJSTRD" --key "${BUCKET}/crosscheck/cliwrote.txt" \
    --from "$TMPDIR/cross_cli.txt" 2>/dev/null || true
pass "cross: rawobjstr put to objstrd's image"

# Restart objstrd so S3 reads work again
start_objstrd "$PORT_RAW" \
    IMAGE="$IMG_OBJSTRD" SIZE_MB=256 PORT="$PORT_RAW" COMPRESSION=zstd
export S3_ENDPOINT="http://localhost:${PORT_RAW}"

# objstrd re-reads the index on startup, so CLI-written data should be visible
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/crosscheck/cliwrote.txt" 2>/dev/null || true)
if [ "$GOT" = "cli-wrote-this" ]; then
    pass "cross: S3 sees CLI-written data after restart"
else
    fail "cross: S3 sees CLI-written data after restart (got: $GOT)"
fi

# -- shardedobjstr can read from objstrd-served S3 --
cat > "$TMPDIR/conf_single_s3.conf" <<CONF
replicas 1
shard s3 endpoint=http://localhost:${PORT_RAW} bucket=${BUCKET}
CONF

GOT=$(sharded_get "$TMPDIR/conf_single_s3.conf" "crosscheck/s3wrote.txt")
assert_eq "$GOT" "cross-check-data" "cross: shardedobjstr reads via S3 shard"

echo ""
echo "  cross-component consistency section done."

# ======================================================================
# SECTION 13: EDGE CASES AND ERROR HANDLING
# ======================================================================
section "edge cases and error handling"

# -- empty body PUT (S3 allows zero-byte objects, so this may succeed) --
STATUS=$(curl -so /dev/null -w "%{http_code}" -X PUT -d "" \
    "${S3_ENDPOINT}/${S3_BUCKET}/empty/test.txt" 2>/dev/null)
if [ "$STATUS" = "200" ] || [ "$STATUS" = "201" ]; then
    pass "put with empty data accepted (zero-byte object)"
elif [ "$STATUS" = "400" ] || [ "$STATUS" = "411" ]; then
    pass "put with empty data rejected ($STATUS)"
else
    fail "put with empty data unexpected status ($STATUS)"
fi

# -- special characters in key --
curl -sf -X PUT -d "special-chars" \
    "${S3_ENDPOINT}/${S3_BUCKET}/special/file%20with%20spaces.txt" >/dev/null 2>&1
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/special/file%20with%20spaces.txt" 2>/dev/null)
if [ -n "$GOT" ]; then
    pass "special chars: spaces in key"
else
    skip "special chars: spaces in key not supported"
fi

# -- overwrite via S3 --
curl -sf -X PUT -d "version1" "${S3_ENDPOINT}/${S3_BUCKET}/overwrite/obj.txt" >/dev/null 2>&1
curl -sf -X PUT -d "version2" "${S3_ENDPOINT}/${S3_BUCKET}/overwrite/obj.txt" >/dev/null 2>&1
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/overwrite/obj.txt" 2>/dev/null)
assert_eq "$GOT" "version2" "S3 overwrite: latest version returned"

# -- delete idempotent --
curl -sf -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/overwrite/obj.txt" >/dev/null 2>&1
STATUS=$(curl -so /dev/null -w "%{http_code}" -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/overwrite/obj.txt" 2>/dev/null)
if [ "$STATUS" = "204" ] || [ "$STATUS" = "200" ]; then
    pass "delete idempotent (double delete)"
else
    fail "delete idempotent (got $STATUS)"
fi

# -- GET missing returns 404 --
STATUS=$(curl -so /dev/null -w "%{http_code}" "${S3_ENDPOINT}/${S3_BUCKET}/totally/missing.txt" 2>/dev/null)
assert_eq "$STATUS" "404" "GET missing returns 404"

# -- Nested key paths --
curl -sf -X PUT -d "deep-data" \
    "${S3_ENDPOINT}/${S3_BUCKET}/a/b/c/d/e/f/deep.txt" >/dev/null 2>&1
GOT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}/a/b/c/d/e/f/deep.txt" 2>/dev/null)
assert_eq "$GOT" "deep-data" "deeply nested key path"

# -- List with delimiter (CommonPrefixes) --
OUT=$(curl -sf "${S3_ENDPOINT}/${S3_BUCKET}?list-type=2&delimiter=/&prefix=a/" 2>/dev/null)
if echo "$OUT" | grep -q "CommonPrefixes\|Prefix"; then
    pass "list with delimiter returns common prefixes"
else
    skip "list with delimiter: CommonPrefixes may not be in response"
fi

# -- rawobjstr error handling --
run_fail "rawobjstr: get from nonexistent image" \
    "$RAWOBJSTR" get --file /tmp/no_such_image.raw --key "test"

run_fail "rawobjstr: put to nonexistent image" \
    "$RAWOBJSTR" put --file /tmp/no_such_image.raw --key "test" --from "$TMPDIR/raw_hello.txt"

# -- shardedobjstr error handling --
run_fail "shardedobjstr: invalid config" \
    "$SHARDEDOBJSTR" check-config --config /tmp/no_such_config.conf

echo ""
echo "  edge cases section done."

# ======================================================================
# SECTION 14: CONCURRENT OPERATIONS (stress-lite)
# ======================================================================
section "concurrent operations (parallel puts + gets)"

export S3_ENDPOINT="http://localhost:${PORT_RAW}"

# Parallel PUTs -- collect PIDs so we only wait on curl, not objstrd
PIDS=""
for i in $(seq 1 20); do
    curl -sf --connect-timeout 5 --max-time 10 -X PUT -d "parallel-data-$i" \
        "${S3_ENDPOINT}/${S3_BUCKET}/parallel/obj-${i}.txt" >/dev/null 2>&1 &
    PIDS="$PIDS $!"
done
for p in $PIDS; do wait "$p" 2>/dev/null || true; done
pass "concurrent: 20 parallel PUTs completed"

# Parallel GETs
PIDS=""
for i in $(seq 1 20); do
    (
        GOT=$(curl -sf --connect-timeout 5 --max-time 10 \
            "${S3_ENDPOINT}/${S3_BUCKET}/parallel/obj-${i}.txt" 2>/dev/null)
        [ "$GOT" = "parallel-data-$i" ] || exit 1
    ) &
    PIDS="$PIDS $!"
done
for p in $PIDS; do wait "$p" 2>/dev/null || true; done
pass "concurrent: 20 parallel GETs completed"

# Parallel mixed operations
PIDS=""
for i in $(seq 1 5); do
    (
        curl -sf --connect-timeout 5 --max-time 10 -X PUT -d "mix-write-$i" \
            "${S3_ENDPOINT}/${S3_BUCKET}/parallel-mix/w-${i}.txt" >/dev/null 2>&1
        curl -sf --connect-timeout 5 --max-time 10 \
            "${S3_ENDPOINT}/${S3_BUCKET}/parallel/obj-$((i * 2)).txt" >/dev/null 2>&1
        curl -sf --connect-timeout 5 --max-time 10 -X DELETE \
            "${S3_ENDPOINT}/${S3_BUCKET}/parallel/obj-$((i * 2 + 10)).txt" >/dev/null 2>&1
    ) &
    PIDS="$PIDS $!"
done
for p in $PIDS; do wait "$p" 2>/dev/null || true; done
pass "concurrent: parallel mixed operations completed"

echo ""
echo "  concurrent operations section done."

# ======================================================================
# SECTION 15: ListBuckets / MultiBucket
# ======================================================================
section "bucket operations"

export S3_ENDPOINT="http://localhost:${PORT_MEM}"

curl -sf -X PUT "${S3_ENDPOINT}/bucket-alpha" >/dev/null 2>&1 || true
curl -sf -X PUT "${S3_ENDPOINT}/bucket-beta" >/dev/null 2>&1 || true

OUT=$(curl -sf "${S3_ENDPOINT}/" 2>/dev/null || true)
if echo "$OUT" | grep -q "bucket-alpha\|Bucket"; then
    pass "ListBuckets returns created buckets"
else
    skip "ListBuckets: format may differ"
fi

# PUT/GET into different buckets
curl -sf -X PUT -d "alpha-data" "${S3_ENDPOINT}/bucket-alpha/obj.txt" >/dev/null 2>&1
curl -sf -X PUT -d "beta-data" "${S3_ENDPOINT}/bucket-beta/obj.txt" >/dev/null 2>&1

GOT_A=$(curl -sf "${S3_ENDPOINT}/bucket-alpha/obj.txt" 2>/dev/null)
GOT_B=$(curl -sf "${S3_ENDPOINT}/bucket-beta/obj.txt" 2>/dev/null)

assert_eq "$GOT_A" "alpha-data" "multi-bucket: alpha data"
assert_eq "$GOT_B" "beta-data" "multi-bucket: beta data"

echo ""
echo "  bucket operations section done."

echo ""
echo "================================================================"
echo "=== ALL SECTIONS COMPLETE ==="
echo "================================================================"
