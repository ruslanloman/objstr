#!/bin/bash
set -e

echo "vmpassword" | sudo -S pkill -f objstrd 2>/dev/null || true
sleep 1
echo "vmpassword" | sudo -S rm -f /tmp/demo_s3_backend.raw /tmp/demo_backend.log /tmp/demo_main.log 2>/dev/null || true
echo "vmpassword" | sudo -S mkdir -p /tmp/demo_fs

OBJSTRD="$HOME/build-objstrd/release/objstrd"
VM_IP="127.0.0.1"

# Backend S3 instance (port 8001)
echo "vmpassword" | sudo -S bash -c "RUST_LOG=debug nohup $OBJSTRD --image /tmp/demo_s3_backend.raw --size-mb 2048 --port 8001 > /tmp/demo_backend.log 2>&1 &"
echo "Backend S3 started on port 8001"
sleep 2

# Main demo instance (port 8000)
echo "vmpassword" | sudo -S bash -c "RUST_LOG=debug nohup $OBJSTRD --config $HOME/objstr/demo/demo_cluster.conf --node demo > /tmp/demo_main.log 2>&1 &"
echo "Demo server started on port 8000"
sleep 3

echo ""
echo "=== Demo running ==="
echo "Main UI:    http://${VM_IP}:8000/ui.html"
echo "Server:     http://${VM_IP}:8000/server.html"
echo "Viz:        http://${VM_IP}:8000/viz.html"
echo "Backend S3: http://${VM_IP}:8001/ui.html"
