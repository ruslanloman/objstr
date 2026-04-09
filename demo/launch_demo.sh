#!/bin/bash
#
# Launch the multi-backend demo cluster.
# Must be run as root (needs /dev/sdb access).
#
# Usage:  sudo bash launch_demo.sh
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OBJSTRD="${OBJSTRD:-$HOME/build-objstrd/release/objstrd}"
VM_IP="${VM_IP:-111.111.111.111}"

# Stop any existing demo
pkill -f objstrd 2>/dev/null || true
sleep 1
rm -f /tmp/demo_s3_backend.raw
mkdir -p /tmp/demo_fs

# Backend S3 instance (port 8001) -- standalone raw image store
# The main demo node uses this as one of its shard backends.
RUST_LOG="${RUST_LOG:-debug}" $OBJSTRD --image /tmp/demo_s3_backend.raw --size-mb 2048 --port 8001 &
BACKEND_PID=$!
echo "Backend S3 started on port 8001 (PID $BACKEND_PID)"
sleep 2

# Main demo instance (port 8000) -- multi-backend node with 4 shards:
#   shard 0: raw block device (/dev/sdb)
#   shard 1: filesystem backend (/tmp/demo_fs)
#   shard 2: in-memory backend
#   shard 3: S3 proxy to the backend on port 8001
RUST_LOG="${RUST_LOG:-debug}" $OBJSTRD --config "$SCRIPT_DIR/demo_cluster.conf" --node demo &
DEMO_PID=$!
echo "Demo server started on port 8000 (PID $DEMO_PID)"
sleep 3

echo ""
echo "=== Demo running ==="
echo "Main UI:    http://${VM_IP}:8000/ui.html"
echo "Server:     http://${VM_IP}:8000/server.html"
echo "Viz:        http://${VM_IP}:8000/viz.html"
echo "Backend S3: http://${VM_IP}:8001/ui.html"
echo ""
echo "PIDs: backend=$BACKEND_PID demo=$DEMO_PID"
echo "Stop with: sudo kill $BACKEND_PID $DEMO_PID"
