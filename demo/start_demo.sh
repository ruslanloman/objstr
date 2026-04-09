#!/bin/bash
set -e

OBJSTRD="${OBJSTRD:-$HOME/build-objstrd/release/objstrd}"
VM_IP="${VM_IP:-127.0.0.1}"

pkill -f objstrd 2>/dev/null || true
sleep 1
rm -f /tmp/demo_s3_backend.raw 2>/dev/null || true
mkdir -p /tmp/demo_fs

# Backend S3 instance (port 8001)
RUST_LOG="${RUST_LOG:-debug}" nohup $OBJSTRD --image /tmp/demo_s3_backend.raw --size-mb 2048 --port 8001 > /tmp/demo_backend.log 2>&1 &
BACKEND_PID=$!
echo "Backend S3 started on port 8001 (PID $BACKEND_PID)"
sleep 2

# Main demo instance (port 8000)
RUST_LOG="${RUST_LOG:-debug}" nohup $OBJSTRD --config "$HOME/objstr/demo/demo_cluster.conf" --node demo > /tmp/demo_main.log 2>&1 &
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
echo "Stop with: kill $BACKEND_PID $DEMO_PID"
