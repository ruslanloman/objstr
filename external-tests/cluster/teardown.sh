#!/bin/bash
#
# cluster/teardown.sh
#
# Stops all cluster nodes started by setup_3node.sh.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"

BASE_PORT=8100
NODE_COUNT=3

echo "=== Stopping cluster nodes ==="
for i in $(seq 0 $((NODE_COUNT - 1))); do
    port=$((BASE_PORT + i))
    stop_server "$port"
    rm -f "/tmp/cluster_node${i}.raw"
    echo "  node$i (port $port) stopped"
done
rm -f /tmp/cluster_state.sh
echo "Done."
