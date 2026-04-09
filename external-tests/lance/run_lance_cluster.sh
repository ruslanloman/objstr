#!/bin/bash
#
# lance/run_lance_cluster.sh
#
# Run the LanceDB-on-cluster example against a running cluster.
# This builds and runs shardedobjstr/examples/lance_on_cluster.rs
# with the `lance` feature enabled.
#
# The example registers a cluster:// URI scheme with LanceDB so that
# Arrow/Lance tables are striped across cluster nodes.
#
# Prerequisites:
#   - Running objstrd cluster (or single node)
#   - Rust toolchain with `cargo` in PATH
#   - lance_on_cluster.rs still present in shardedobjstr/examples/
#     (or rebuild from external-tests/lance/lance_on_cluster.rs)
#
# Usage:
#   bash run_lance_cluster.sh
#   CLUSTER_HOSTS="http://localhost:8100,http://localhost:8101" bash run_lance_cluster.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLUSTER_HOSTS="${CLUSTER_HOSTS:-http://localhost:8100,http://localhost:8101,http://localhost:8102}"
CARGO="${CARGO:-cargo}"

echo "=== LanceDB on cluster example ==="
echo "Cluster: $CLUSTER_HOSTS"
echo ""

# Build and run the example with the lance feature gate
CLUSTER_HOSTS="$CLUSTER_HOSTS" \
  "$CARGO" run \
    --manifest-path "$REPO_ROOT/shardedobjstr/Cargo.toml" \
    --features lance \
    --example lance_on_cluster \
    --release

echo ""
echo "PASS: lance_on_cluster example completed"
