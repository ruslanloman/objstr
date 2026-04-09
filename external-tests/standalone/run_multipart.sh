#!/bin/bash
#
# standalone/run_multipart.sh
#
# Tests multipart upload against a standalone objstrd via raw S3 XML API.
# No aws-cli or boto3 required -- pure curl.
#
# Covers: CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
#         ListParts, AbortMultipartUpload.
#
# Usage:
#   bash run_multipart.sh
#   S3_ENDPOINT=http://myserver:8000 bash run_multipart.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

PORT=8901
IMAGE=/tmp/ext_test_multipart.raw
SIZE_MB=512
PASS=0
FAIL=0
OWN_SERVER=false

if ! curl -sf "http://localhost:${PORT}/_admin/info" >/dev/null 2>&1; then
    rm -f "$IMAGE"
    start_server "$PORT" "$IMAGE" "$SIZE_MB"
    OWN_SERVER=true
fi

EXPECTED_HASH=$(get_build_hash "$PORT")
export S3_ENDPOINT="http://localhost:${PORT}"
export S3_BUCKET="testbucket"
BASE="${S3_ENDPOINT}/${S3_BUCKET}"

cleanup() {
    [ "$OWN_SERVER" = "true" ] && stop_server "$PORT" && rm -f "$IMAGE"
    if [ "$FAIL" -gt 0 ]; then
        state=$(check_server_valid "$PORT" "$EXPECTED_HASH")
        [ "$state" = "ok" ] || echo "WARNING: server $state during test run"
    fi
    echo ""
    echo "Results: $PASS passed, $FAIL failed"
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

echo "=== multipart upload tests ==="
echo "Endpoint: $S3_ENDPOINT"
echo ""

KEY="mp/testobj.bin"

# ---- create multipart upload ----
echo "Starting multipart upload..."
INIT_RESP=$(curl -sf -X POST "${BASE}/${KEY}?uploads")
UPLOAD_ID=$(echo "$INIT_RESP" | python3 -c "
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(sys.stdin.read())
ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
print(r.find('s3:UploadId', ns).text)
")
echo "  UploadId: $UPLOAD_ID"
[ -n "$UPLOAD_ID" ] && pass "CreateMultipartUpload" || { fail "CreateMultipartUpload"; exit 1; }

# ---- upload parts (5 MB each -- minimum S3 part size) ----
PART_SIZE=$((5 * 1024 * 1024))
ETAGS=""
for PART in 1 2 3; do
    PART_FILE="/tmp/ext_test_mp_part_${PART}.bin"
    dd if=/dev/urandom of="$PART_FILE" bs=1M count=5 2>/dev/null
    ETAG=$(curl -sf -X PUT \
        -H "Content-Length: $PART_SIZE" \
        --data-binary "@${PART_FILE}" \
        -D - \
        "${BASE}/${KEY}?partNumber=${PART}&uploadId=${UPLOAD_ID}" \
        | grep -i ETag | tr -d '\r' | awk '{print $2}' | tr -d '"')
    rm -f "$PART_FILE"
    [ -n "$ETAG" ] && pass "UploadPart $PART" || fail "UploadPart $PART"
    ETAGS="${ETAGS}<Part><PartNumber>${PART}</PartNumber><ETag>&quot;${ETAG}&quot;</ETag></Part>"
done

# ---- list parts ----
LIST_RESP=$(curl -sf "${BASE}/${KEY}?uploadId=${UPLOAD_ID}")
PART_COUNT=$(echo "$LIST_RESP" | python3 -c "
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(sys.stdin.read())
ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
print(len(r.findall('s3:Part', ns)))
")
[ "$PART_COUNT" = "3" ] && pass "ListParts (3 parts)" || fail "ListParts expected 3 got $PART_COUNT"

# ---- complete ----
COMPLETE_XML="<CompleteMultipartUpload>${ETAGS}</CompleteMultipartUpload>"
COMPLETE_STATUS=$(curl -so /dev/null -w "%{http_code}" -X POST \
    -H "Content-Type: application/xml" \
    -d "$COMPLETE_XML" \
    "${BASE}/${KEY}?uploadId=${UPLOAD_ID}")
[ "$COMPLETE_STATUS" = "200" ] && pass "CompleteMultipartUpload" || fail "CompleteMultipartUpload status=$COMPLETE_STATUS"

# ---- verify assembled object ----
STATUS=$(curl -so /dev/null -w "%{http_code}" -I "${BASE}/${KEY}")
[ "$STATUS" = "200" ] && pass "HEAD after complete" || fail "HEAD after complete status=$STATUS"

# ---- abort test: start and abort ----
AB_RESP=$(curl -sf -X POST "${BASE}/mp/toabort.bin?uploads")
AB_ID=$(echo "$AB_RESP" | python3 -c "
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(sys.stdin.read())
ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
print(r.find('s3:UploadId', ns).text)
")
AB_STATUS=$(curl -so /dev/null -w "%{http_code}" -X DELETE \
    "${BASE}/mp/toabort.bin?uploadId=${AB_ID}")
[ "$AB_STATUS" = "204" ] && pass "AbortMultipartUpload" || fail "AbortMultipartUpload status=$AB_STATUS"
