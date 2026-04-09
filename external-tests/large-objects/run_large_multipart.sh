#!/bin/bash
#
# large-objects/run_large_multipart.sh
#
# Multipart upload of a large object using aws-cli.
# Uploads PARTS x PART_MB chunks, completes the upload, then verifies
# total size via HEAD and a byte-range GET.
#
# Requires: aws-cli v2, dd, /dev/urandom
#
# Usage:
#   bash run_large_multipart.sh
#   PARTS=10 PART_MB=1024 bash run_large_multipart.sh   # 10 x 1 GB = 10 GB
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

PARTS="${PARTS:-10}"
PART_MB="${PART_MB:-512}"
PORT="${PORT:-8902}"
IMAGE="${IMAGE:-/tmp/large_mpu_test.raw}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
BUCKET="${BUCKET:-testbucket}"
KEY="large-multipart-${PARTS}x${PART_MB}mb"
TOTAL_BYTES=$(( PARTS * PART_MB * 1024 * 1024 ))
TOTAL_GB=$(( TOTAL_BYTES / 1024 / 1024 / 1024 ))

if [ "$MANAGE_SERVER" = "1" ]; then
  IMAGE_SIZE_MB=$(( TOTAL_BYTES / 1024 / 1024 + 2048 ))
  rm -f "$IMAGE"
  start_server "$PORT" "$IMAGE" "$IMAGE_SIZE_MB"
fi

export S3_ENDPOINT="http://localhost:${PORT}"
export S3_BUCKET="$BUCKET"

BUILD_HASH=$(get_build_hash "$PORT")
echo "=== Large multipart upload: ${PARTS} x ${PART_MB} MB = ${TOTAL_GB} GB ==="
echo "Endpoint: $S3_ENDPOINT  Key: $KEY  Build: $BUILD_HASH"

TMPDIR_PARTS=$(mktemp -d)
trap '
  RC=$?
  rm -rf "$TMPDIR_PARTS"
  if [ $RC -ne 0 ]; then
    STATE=$(check_server_valid '"'"'$PORT'"'"' '"'"'$BUILD_HASH'"'"')
    if [ "$STATE" != "ok" ]; then
      echo "WARNING: server $STATE -- failure may not be a real bug"
    fi
  fi
  [ "'"'"'$MANAGE_SERVER'"'"'" = "1" ] && stop_server '"'"'$PORT'"'"'
  exit $RC
' EXIT

s3_bucket_create

# aws-cli endpoint config (no SSL verification needed for local server)
export AWS_ACCESS_KEY_ID="${ACCESS_KEY:-AKIAIOSFODNN7EXAMPLE}"
export AWS_SECRET_ACCESS_KEY="${SECRET_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
export AWS_DEFAULT_REGION="${REGION:-us-east-1}"
AWS_OPTS="--endpoint-url $S3_ENDPOINT --no-verify-ssl"

echo "Generating ${PARTS} part files (${PART_MB} MB each)..."
for i in $(seq 1 "$PARTS"); do
  dd if=/dev/urandom bs=1M count="$PART_MB" of="$TMPDIR_PARTS/part${i}" 2>/dev/null
done

echo "Creating multipart upload..."
UPLOAD_ID=$(aws s3api create-multipart-upload $AWS_OPTS \
  --bucket "$BUCKET" --key "$KEY" \
  --query 'UploadId' --output text)

echo "Upload ID: $UPLOAD_ID"

PARTS_JSON="\"Parts\":["
for i in $(seq 1 "$PARTS"); do
  echo "  Uploading part ${i}/${PARTS}..."
  ETAG=$(aws s3api upload-part $AWS_OPTS \
    --bucket "$BUCKET" --key "$KEY" \
    --upload-id "$UPLOAD_ID" \
    --part-number "$i" \
    --body "$TMPDIR_PARTS/part${i}" \
    --query 'ETag' --output text)
  [ "$i" -gt 1 ] && PARTS_JSON="${PARTS_JSON},"
  PARTS_JSON="${PARTS_JSON}{\"PartNumber\":${i},\"ETag\":${ETAG}}"
done
PARTS_JSON="${PARTS_JSON}]"

echo "Completing multipart upload..."
aws s3api complete-multipart-upload $AWS_OPTS \
  --bucket "$BUCKET" --key "$KEY" \
  --upload-id "$UPLOAD_ID" \
  --multipart-upload "{${PARTS_JSON}}" > /dev/null

echo "Verifying total size via HEAD..."
CONTENT_LENGTH=$(aws s3api head-object $AWS_OPTS \
  --bucket "$BUCKET" --key "$KEY" \
  --query 'ContentLength' --output text)
assert_eq "$CONTENT_LENGTH" "$TOTAL_BYTES" "Content-Length"

echo "Byte-range read (last 1 MB)..."
START=$(( TOTAL_BYTES - 1048576 ))
aws s3api get-object $AWS_OPTS \
  --bucket "$BUCKET" --key "$KEY" \
  --range "bytes=${START}-$((TOTAL_BYTES-1))" \
  "$TMPDIR_PARTS/last_chunk" > /dev/null
CHUNK_SIZE=$(wc -c < "$TMPDIR_PARTS/last_chunk")
assert_eq "$CHUNK_SIZE" "1048576" "Last chunk size"

echo ""
echo "PASS: large multipart upload (${TOTAL_GB} GB) succeeded"
