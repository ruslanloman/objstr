#!/bin/bash
set -e

BASE="http://127.0.0.1:8000"

echo "=== BEFORE DRAIN ==="
echo "Shard file counts:"
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
echo "Object placement:"
curl -s "$BASE/_admin/objects?limit=20" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    print("  %s: shards=%s" % (obj["key"], obj["shard_ids"]))
'

echo ""
echo "Recovery status:"
curl -s "$BASE/_admin/recovery" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("  under_replicated=%d over_replicated=%d" % (d["under_replicated_count"], d["over_replicated_count"]))
'

echo ""
echo "=== DRAINING SHARD 3 (mem) ==="
DRAIN_RESULT=$(curl -s -X POST "$BASE/_admin/drain/3")
echo "Drain result: $DRAIN_RESULT"

echo ""
echo "=== AFTER DRAIN ==="
echo "Shard file counts:"
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
echo "Object placement:"
curl -s "$BASE/_admin/objects?limit=20" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    print("  %s: shards=%s" % (obj["key"], obj["shard_ids"]))
'

echo ""
echo "Recovery status:"
curl -s "$BASE/_admin/recovery" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("  under_replicated=%d over_replicated=%d" % (d["under_replicated_count"], d["over_replicated_count"]))
'

echo ""
echo "=== VERIFY: can we still GET every object via S3? ==="
KEYS=$(curl -s "$BASE/_admin/objects?limit=20" | python3 -c '
import sys, json
data = json.load(sys.stdin)
for obj in data["objects"]:
    print(obj["key"])
')
for key in $KEYS; do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/$key")
    if [ "$STATUS" = "200" ]; then
        echo "  GET /$key -> $STATUS OK"
    else
        echo "  GET /$key -> $STATUS FAIL!"
    fi
done

echo ""
echo "=== DONE ==="
