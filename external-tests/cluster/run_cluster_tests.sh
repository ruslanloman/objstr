#!/bin/bash
#
# cluster/run_cluster_tests.sh
#
# Black-box S3 tests against a running cluster.
# Sources /tmp/cluster_state.sh (written by setup_3node.sh).
#
# Tests each node independently first, then (once coordinator/sharding is
# implemented) will verify cross-node data visibility.
#
# Usage:
#   bash run_cluster_tests.sh
#   BUCKET=mytest bash run_cluster_tests.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

STATE_FILE=/tmp/cluster_state.sh
if [ ! -f "$STATE_FILE" ]; then
  echo "ERROR: $STATE_FILE not found. Run setup_3node.sh first."
  exit 1
fi
source "$STATE_FILE"

BUCKET="${BUCKET:-testbucket}"
PASS=0
FAIL=0

run_test() {
  local name="$1"
  local fn="$2"
  if $fn 2>&1; then
    echo "PASS: $name"
    PASS=$((PASS+1))
  else
    echo "FAIL: $name"
    FAIL=$((FAIL+1))
  fi
}

# ---------------------------------------------------------------------------
# Per-node smoke tests
# ---------------------------------------------------------------------------

test_node_basic() {
  local url="$1"
  local label="$2"
  export S3_ENDPOINT="$url"
  export S3_BUCKET="$BUCKET"

  s3_bucket_create
  s3_put "test-key-${label}" "hello from $label"
  local body
  body=$(s3_get "test-key-${label}")
  assert_eq "$body" "hello from $label" "GET body"
  s3_delete "test-key-${label}"
}

echo "=== Cluster tests: 3 nodes ==="
echo "NODE0=${NODE0_URL}  NODE1=${NODE1_URL}  NODE2=${NODE2_URL}"
echo ""

for i in 0 1 2; do
  eval "URL=\$NODE${i}_URL"
  eval "HASH=\$NODE${i}_HASH"
  run_test "node${i} basic smoke (${URL})" "test_node_basic $URL node${i}"
  # Extract port from URL for binary-swap check
  NODE_PORT=$(echo "$URL" | sed 's|.*:\([0-9]*\)$|\1|')
  VALID=$(check_server_valid "$NODE_PORT" "$HASH" 2>/dev/null || echo "ok")
  if [ "$VALID" != "ok" ]; then
    echo "WARNING: node${i} binary may have been swapped ($VALID)"
  fi
done

# ---------------------------------------------------------------------------
# Cross-node read (future: once sharding/coordinator is live)
# ---------------------------------------------------------------------------
# The intent: write to NODE0, read from NODE1 and NODE2.
# TODO: uncomment when the coordinator distributes objects across the cluster.
#
# echo "=== Cross-node visibility (requires coordinator) ==="
# export S3_ENDPOINT="$NODE0_URL" S3_BUCKET="$BUCKET"
# s3_put "cross-node-key" "shared data"
#
# export S3_ENDPOINT="$NODE1_URL"
# run_test "cross-node read from NODE1" "s3_get cross-node-key"
#
# export S3_ENDPOINT="$NODE2_URL"
# run_test "cross-node read from NODE2" "s3_get cross-node-key"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
