#!/bin/bash
#
# large-objects/run_large_put.sh
#
# Tests large object handling: PUT a multi-GB body, verify via HEAD and
# byte-range GET.  Requires Linux (needs /dev/urandom or dd).
#
# Usage:
#   bash run_large_put.sh            # defaults: 2 GB, port 8900
#   SIZE_GB=5 bash run_large_put.sh  # custom size
#
# The server must already be running (standalone or cluster node).
# Set S3_ENDPOINT if not localhost:8900.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

SIZE_GB="${SIZE_GB:-2}"
SIZE_BYTES=$(( SIZE_GB * 1024 * 1024 * 1024 ))
PORT="${PORT:-8900}"
IMAGE="${IMAGE:-/tmp/large_put_test.raw}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"  # set to 0 if server is already running
BUCKET="${BUCKET:-testbucket}"
KEY="large-object-${SIZE_GB}gb"

if [ "$MANAGE_SERVER" = "1" ]; then
  # Need a larger image for multi-GB objects
  IMAGE_SIZE_MB=$(( (SIZE_GB + 2) * 1024 ))
  rm -f "$IMAGE"
  start_server "$PORT" "$IMAGE" "$IMAGE_SIZE_MB"
fi

export S3_ENDPOINT="http://localhost:${PORT}"
export S3_BUCKET="$BUCKET"

BUILD_HASH=$(get_build_hash "$PORT")
echo "=== Large PUT test: ${SIZE_GB} GB object ==="
echo "Endpoint: $S3_ENDPOINT  Key: $KEY  Build: $BUILD_HASH"

trap '
  RC=$?
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

echo "Uploading ${SIZE_GB} GB via stdin stream..."
dd if=/dev/urandom bs=1M count=$(( SIZE_GB * 1024 )) 2>/dev/null \
  | s3_put_stream "$KEY" "$SIZE_BYTES"

echo "Verifying size via HEAD..."
CONTENT_LENGTH=$(curl -sI "$(_object_url "$KEY")" | grep -i '^Content-Length:' | tr -d '\r' | awk '{print $2}')
assert_eq "$CONTENT_LENGTH" "$SIZE_BYTES" "Content-Length"

echo "Checking byte-range GET (first 1 MB)..."
RANGE_RESP=$(s3_get_range "$KEY" "0-1048575")
RANGE_LEN=${#RANGE_RESP}
if [ "$RANGE_LEN" -ne 1048576 ]; then
  echo "FAIL: expected 1048576 bytes, got $RANGE_LEN"
  exit 1
fi

echo "Checking byte-range GET (last 1 MB)..."
START=$(( SIZE_BYTES - 1048576 ))
END=$(( SIZE_BYTES - 1 ))
s3_get_range "$KEY" "${START}-${END}" > /dev/null

echo ""
echo "PASS: Large PUT/HEAD/range-GET all succeeded for ${SIZE_GB} GB object"
