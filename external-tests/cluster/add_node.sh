#!/bin/bash
#
# cluster/add_node.sh
#
# Example: how to add a 4th node to the running 3-node cluster.
#
# This demonstrates the operational workflow:
#   1. Start a new objstrd instance on a new image
#   2. Verify it is healthy via /_admin/info
#   3. Update the state file
#
# When ROLE=node/coordinator is implemented this script will also:
#   - Register the node with the coordinator
#   - Trigger repair-replication
#
# Usage:
#   bash add_node.sh [port] [image] [size_mb]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"

PORT="${1:-8103}"
IMAGE="${2:-/tmp/cluster_node3.raw}"
SIZE_MB="${3:-512}"
STATE_FILE=/tmp/cluster_state.sh

echo "=== Adding node on port $PORT ==="

rm -f "$IMAGE"
start_server "$PORT" "$IMAGE" "$SIZE_MB"

HASH=$(get_build_hash "$PORT")

echo "NODE_URL_NEW=http://localhost:${PORT}" >> "$STATE_FILE"
echo "NODE_HASH_NEW=${HASH}" >> "$STATE_FILE"

echo ""
echo "Node added: http://localhost:${PORT}  hash=$HASH"
echo ""
echo "TODO (once ROLE=coordinator is live):"
echo "  curl -X POST 'http://localhost:9000/_coord/nodes' \\"
echo "    -d '{\"address\":\"http://localhost:${PORT}\",\"failure_group\":\"site-1\"}'"
