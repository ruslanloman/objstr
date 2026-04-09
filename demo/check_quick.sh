#!/bin/bash
BASE="http://127.0.0.1:8000"

echo "=== Recovery status ==="
curl -s "$BASE/_admin/recovery" | python3 -c '
import sys, json
d = json.load(sys.stdin)
for k, v in sorted(d.items()):
    print("  %s = %s" % (k, v))
'

echo ""
echo "=== Shard file counts ==="
curl -s "$BASE/_admin/shards" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for s in data["shards"]:
    sid = s["id"]
    name = s["name"]
    fc = s["file_count"]
    h = s["health"]
    print("  shard %d %s: files=%d health=%s" % (sid, name, fc, h))
'

echo ""
echo "=== Object placement ==="
curl -s "$BASE/_admin/objects?limit=20" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    print("  %s: shards=%s" % (obj["key"], obj["shard_ids"]))
print("  total unique: %d" % data["total"])
'
