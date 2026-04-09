#!/bin/bash
echo "=== Shard Health ==="
curl -s http://127.0.0.1:8000/_admin/shards 2>&1 | python3 -m json.tool 2>&1 | head -100

echo ""
echo "=== Under-replicated objects ==="
# Every object is on [0,2,3,4]. Shard 3 was drained (Offline).
# rf=4 means we need 4 healthy copies.
# healthy shards with copies: 0, 2, 4 = 3 copies
# shard 1 has 0 files = never gets anything
# So 3 < 4 = under-replicated. Recovery should copy to shard 1.
curl -s 'http://127.0.0.1:8000/_admin/objects?limit=20' 2>&1 | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    print(f"  {obj[\"key\"]}: shards={obj[\"shard_ids\"]}")
'

echo ""
echo "=== Try manual repair-replication ==="
curl -s -X POST http://127.0.0.1:8000/_admin/repair-replication 2>&1 | python3 -m json.tool

echo ""
echo "=== Objects after repair-replication ==="
curl -s 'http://127.0.0.1:8000/_admin/objects?limit=20' 2>&1 | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    print(f"  {obj[\"key\"]}: shards={obj[\"shard_ids\"]}")
'

echo ""
echo "=== Shard file counts ==="
curl -s http://127.0.0.1:8000/_admin/shards 2>&1 | python3 -c '
import sys, json
data = json.load(sys.stdin)
for s in data:
    print(f"  shard {s[\"id\"]} ({s[\"label\"]}): files={s[\"file_count\"]} health={s[\"health\"]} data={s[\"data_bytes\"]}")
'
