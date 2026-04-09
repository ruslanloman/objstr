#!/bin/bash
set -e

echo "vmpassword" | sudo -S pkill -f objstrd 2>/dev/null || true
sleep 1
echo "vmpassword" | sudo -S rm -f /tmp/demo_s3_backend.raw /tmp/demo_backend.log /tmp/demo_main.log 2>/dev/null || true
echo "vmpassword" | sudo -S mkdir -p /tmp/demo_fs

OBJSTRD="$HOME/build-objstrd/release/objstrd"

echo "vmpassword" | sudo -S bash -c "RUST_LOG=debug nohup $OBJSTRD --image /tmp/demo_s3_backend.raw --size-mb 2048 --port 8001 > /tmp/demo_backend.log 2>&1 &"
echo "Backend S3 started on port 8001"
sleep 2

echo "vmpassword" | sudo -S bash -c "RUST_LOG=debug nohup $OBJSTRD --config $HOME/objstr/demo/demo_cluster.conf --node demo > /tmp/demo_main.log 2>&1 &"
echo "Demo server started on port 8000"
sleep 3

curl -s http://127.0.0.1:8000/_admin/info | python3 -m json.tool | grep -E 'build_date|replication|shard_count'
