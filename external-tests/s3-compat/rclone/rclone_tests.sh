#!/bin/bash
#
# rclone_tests.sh - rclone S3 conformance tests for objstrd
#
# Each test outputs a JSON line in mint format:
#   {"name":"rclone","function":"<name>","duration":<ms>,"status":"PASS|FAIL","alert":"...","error":"..."}
#
# Usage:
#   ENDPOINT=localhost:8030 ACCESS_KEY=... SECRET_KEY=... BUCKET=rclone-test \
#   LOG_FILE=/tmp/rclone_results/rclone.json DATA_DIR=/tmp/mint_data \
#   bash rclone_tests.sh
#

ENDPOINT="${ENDPOINT:-localhost:8030}"
ACCESS_KEY="${ACCESS_KEY:-AKIAIOSFODNN7EXAMPLE}"
SECRET_KEY="${SECRET_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
BUCKET="${BUCKET:-rclone-test}"
LOG_FILE="${LOG_FILE:-/tmp/rclone_results/rclone.json}"
DATA_DIR="${DATA_DIR:-/tmp/mint_data}"
WORK_DIR="/tmp/rclone_work_$$"

mkdir -p "$(dirname "$LOG_FILE")" "$WORK_DIR"
: > "$LOG_FILE"

PASS=0
FAIL=0

# ---- helpers ----

rclone_env() {
    RCLONE_CONFIG_RAWOBJST_TYPE=s3 \
    RCLONE_CONFIG_RAWOBJST_PROVIDER=Other \
    RCLONE_CONFIG_RAWOBJST_ENDPOINT="http://${ENDPOINT}" \
    RCLONE_CONFIG_RAWOBJST_ACCESS_KEY_ID="${ACCESS_KEY}" \
    RCLONE_CONFIG_RAWOBJST_SECRET_ACCESS_KEY="${SECRET_KEY}" \
    RCLONE_CONFIG_RAWOBJST_FORCE_PATH_STYLE=true \
    rclone "$@"
}

emit() {
    local name="$1" status="$2" duration="$3" alert="$4" error="$5"
    local line
    if [ "$status" = "PASS" ]; then
        line="{\"name\":\"rclone\",\"function\":\"${name}\",\"duration\":${duration},\"status\":\"PASS\"}"
        PASS=$((PASS + 1))
        printf '.'
    else
        # escape newlines and quotes for JSON
        alert_esc=$(echo "$alert" | tr '\n' ' ' | sed 's/"/\\"/g')
        error_esc=$(echo "$error" | tr '\n' ' ' | sed 's/"/\\"/g')
        line="{\"name\":\"rclone\",\"function\":\"${name}\",\"duration\":${duration},\"status\":\"FAIL\",\"alert\":\"${alert_esc}\",\"error\":\"${error_esc}\"}"
        FAIL=$((FAIL + 1))
        printf 'F'
    fi
    echo "$line" >> "$LOG_FILE"
}

run_test() {
    local name="$1"
    local t0
    t0=$(date +%s%3N)
    local out err rc
    out=$(eval "${2}" 2>&1)
    rc=$?
    local t1
    t1=$(date +%s%3N)
    local dur=$(( t1 - t0 ))
    if [ $rc -eq 0 ]; then
        emit "$name" "PASS" "$dur" "" ""
    else
        emit "$name" "FAIL" "$dur" "exit code $rc" "$out"
    fi
}

run_test_with_check() {
    # like run_test but also checks $CHECK_CMD output
    local name="$1" cmd="$2" check_cmd="$3"
    local t0
    t0=$(date +%s%3N)
    local out rc
    out=$(eval "$cmd" 2>&1)
    rc=$?
    local t1
    t1=$(date +%s%3N)
    local dur=$(( t1 - t0 ))
    if [ $rc -ne 0 ]; then
        emit "$name" "FAIL" "$dur" "exit code $rc on main cmd" "$out"
        return
    fi
    local check_out check_rc
    check_out=$(eval "$check_cmd" 2>&1)
    check_rc=$?
    if [ $check_rc -eq 0 ]; then
        emit "$name" "PASS" "$dur" "" ""
    else
        emit "$name" "FAIL" "$dur" "check failed" "$check_out"
    fi
}

# ---- setup ----

echo "=== rclone S3 Tests ==="
echo "Endpoint:  http://${ENDPOINT}"
echo "Bucket:    ${BUCKET}"
echo "Data dir:  ${DATA_DIR}"
echo "Log file:  ${LOG_FILE}"
echo ""

# Create bucket (idempotent)
rclone_env mkdir "rawobjstr:${BUCKET}" 2>/dev/null || true

echo "Running tests..."

# ---- 1. Bucket / listing ----

run_test "test_list_buckets" \
    "rclone_env lsd rawobjstr: 2>&1 | grep -q '.'"

run_test "test_list_empty_bucket" \
    "rclone_env size rawobjstr:${BUCKET}/ 2>&1 | grep -q 'Total objects: 0'"

# ---- 2. Basic upload / download ----

run_test "test_copy_small_file" \
    "echo 'hello rclone' > ${WORK_DIR}/hello.txt && rclone_env copy ${WORK_DIR}/hello.txt rawobjstr:${BUCKET}/rclone-test/"

run_test_with_check "test_list_after_upload" \
    "rclone_env ls rawobjstr:${BUCKET}/rclone-test/" \
    "rclone_env ls rawobjstr:${BUCKET}/rclone-test/ 2>&1 | grep -q 'hello.txt'"

run_test_with_check "test_download_small_file" \
    "rclone_env copy rawobjstr:${BUCKET}/rclone-test/hello.txt ${WORK_DIR}/dl/" \
    "diff ${WORK_DIR}/hello.txt ${WORK_DIR}/dl/hello.txt"

# ---- 3. Checksum verification ----

run_test "test_check_small_file" \
    "rclone_env check ${WORK_DIR}/hello.txt rawobjstr:${BUCKET}/rclone-test/ 2>&1"

# ---- 4. 1MB file ----

run_test "test_copy_1mb_file" \
    "rclone_env copy ${DATA_DIR}/datafile-1-MB rawobjstr:${BUCKET}/rclone-test/"

run_test_with_check "test_download_1mb_file" \
    "rclone_env copy rawobjstr:${BUCKET}/rclone-test/datafile-1-MB ${WORK_DIR}/dl1mb/" \
    "diff ${DATA_DIR}/datafile-1-MB ${WORK_DIR}/dl1mb/datafile-1-MB"

run_test "test_check_1mb_file" \
    "rclone_env check ${DATA_DIR}/datafile-1-MB rawobjstr:${BUCKET}/rclone-test/ 2>&1"

# ---- 5. 10MB file ----

run_test "test_copy_10mb_file" \
    "rclone_env copy ${DATA_DIR}/datafile-10-MB rawobjstr:${BUCKET}/rclone-test/"

run_test_with_check "test_download_10mb_file" \
    "rclone_env copy rawobjstr:${BUCKET}/rclone-test/datafile-10-MB ${WORK_DIR}/dl10mb/" \
    "diff ${DATA_DIR}/datafile-10-MB ${WORK_DIR}/dl10mb/datafile-10-MB"

run_test "test_check_10mb_file" \
    "rclone_env check ${DATA_DIR}/datafile-10-MB rawobjstr:${BUCKET}/rclone-test/ 2>&1"

# ---- 6. Multipart (11MB triggers rclone multipart) ----

run_test "test_copy_11mb_multipart" \
    "rclone_env copy ${DATA_DIR}/datafile-11-MB rawobjstr:${BUCKET}/rclone-mp/"

run_test_with_check "test_download_11mb_multipart" \
    "rclone_env copy rawobjstr:${BUCKET}/rclone-mp/datafile-11-MB ${WORK_DIR}/dl11mb/" \
    "diff ${DATA_DIR}/datafile-11-MB ${WORK_DIR}/dl11mb/datafile-11-MB"

run_test "test_check_11mb_multipart" \
    "rclone_env check ${DATA_DIR}/datafile-11-MB rawobjstr:${BUCKET}/rclone-mp/ 2>&1"

# ---- 7. Directory sync ----

mkdir -p "${WORK_DIR}/sync_src"
cp "${DATA_DIR}/datafile-1-kB" "${WORK_DIR}/sync_src/"
cp "${DATA_DIR}/datafile-10-kB" "${WORK_DIR}/sync_src/"
cp "${DATA_DIR}/datafile-100-kB" "${WORK_DIR}/sync_src/"

run_test "test_sync_upload_dir" \
    "rclone_env sync ${WORK_DIR}/sync_src/ rawobjstr:${BUCKET}/rclone-sync/"

run_test "test_check_synced_dir" \
    "rclone_env check ${WORK_DIR}/sync_src/ rawobjstr:${BUCKET}/rclone-sync/ 2>&1"

# Modify one file and re-sync, verify updated
echo "modified" > "${WORK_DIR}/sync_src/datafile-1-kB"
run_test "test_sync_update_modified" \
    "rclone_env sync ${WORK_DIR}/sync_src/ rawobjstr:${BUCKET}/rclone-sync/"

run_test "test_check_after_sync_update" \
    "rclone_env check ${WORK_DIR}/sync_src/ rawobjstr:${BUCKET}/rclone-sync/ 2>&1"

# Delete one file locally and re-sync (should delete remote too)
rm "${WORK_DIR}/sync_src/datafile-10-kB"
run_test "test_sync_delete_propagates" \
    "rclone_env sync ${WORK_DIR}/sync_src/ rawobjstr:${BUCKET}/rclone-sync/"

run_test_with_check "test_deleted_file_gone_remote" \
    "rclone_env ls rawobjstr:${BUCKET}/rclone-sync/" \
    "! rclone_env ls rawobjstr:${BUCKET}/rclone-sync/ 2>&1 | grep -q 'datafile-10-kB'"

# ---- 8. lsf / lsjson listing ----

run_test "test_lsf_listing" \
    "rclone_env lsf rawobjstr:${BUCKET}/rclone-test/ 2>&1 | grep -q 'hello.txt'"

run_test "test_lsjson_listing" \
    "rclone_env lsjson rawobjstr:${BUCKET}/rclone-test/ 2>&1 | grep -q '\"Name\"'"

# ---- 9. Delete ----

run_test "test_delete_single_object" \
    "rclone_env delete rawobjstr:${BUCKET}/rclone-test/hello.txt"

run_test_with_check "test_deleted_object_not_listed" \
    "rclone_env ls rawobjstr:${BUCKET}/rclone-test/ 2>&1" \
    "! rclone_env ls rawobjstr:${BUCKET}/rclone-test/ 2>&1 | grep -q 'hello.txt'"

# ---- 10. Purge ----

run_test "test_purge_bucket_prefix" \
    "rclone_env purge rawobjstr:${BUCKET}/"

run_test "test_bucket_empty_after_purge" \
    "rclone_env mkdir rawobjstr:${BUCKET} 2>/dev/null || true; rclone_env size rawobjstr:${BUCKET}/ 2>&1 | grep -q 'Total objects: 0'"

# ---- summary ----

echo ""
echo ""
TOTAL=$((PASS + FAIL))
echo "=== Results ==="
echo "PASS: ${PASS}  FAIL: ${FAIL}  TOTAL: ${TOTAL}"

rm -rf "$WORK_DIR"
[ "$FAIL" -eq 0 ]
