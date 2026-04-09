# Example: Change Feed -- Watching for New Objects

The `/_admin/objects` endpoint supports a `since_txn` query parameter that
returns only objects created after a given transaction ID. Combined with the
event socket, this gives you a simple change feed.

## Querying New Objects via HTTP

```bash
# Get the current txn_id
TXN=$(curl -s http://localhost:8000/_admin/info | jq '.txn_id')

# ... time passes, writes happen ...

# Fetch only objects written since that txn
curl -s "http://localhost:8000/_admin/objects?since_txn=$TXN" | jq '.objects[].key'

# Filter by prefix too
curl -s "http://localhost:8000/_admin/objects?since_txn=$TXN&prefix=images/" | jq '.objects[].key'
```

## Parameters for `/_admin/objects`

| Param | Default | Description |
|-------|---------|-------------|
| `since_txn` | 0 | Only return objects with `created_txn > since_txn` |
| `prefix` | (none) | Filter keys containing this substring |
| `offset` | 0 | Pagination offset |
| `limit` | 100 | Max results per page (capped at 1000) |

## Shell Script: Watch via Event Socket

```bash
#!/bin/bash
# watch-new-objects.sh -- prints new object keys as they are written
#
# Requires: socat, curl, jq
#
# Usage:
#   WRITER on port 8000 with --event-socket /tmp/objstrd.sock
#   READER on port 8001 with --read-only --event-socket /tmp/objstrd.sock
#   ./watch-new-objects.sh /tmp/objstrd.sock mysecretvalue http://localhost:8001

SOCKET="${1:?usage: $0 <socket> <secret> <base-url>}"
SECRET="${2:?usage: $0 <socket> <secret> <base-url>}"
BASE="${3:?usage: $0 <socket> <secret> <base-url>}"

# Get initial txn_id
LAST_TXN=$(curl -s "$BASE/_admin/info" | jq -r '.txn_id')
echo "starting at txn_id=$LAST_TXN"

# Connect to the event socket, authenticate, then loop
{
  echo "SECRET $SECRET"
  # Read the OK response
  read -r REPLY
  if [ "$REPLY" != "OK" ]; then
    echo "ERROR: auth failed: $REPLY" >&2
    exit 1
  fi
  # Events: PUT <key>, DELETE <key>, FLUSH <shard_id> <txn_id>
  while read -r CMD REST; do
    case "$CMD" in
      PUT)
        echo "PUT $REST"
        ;;
      DELETE)
        echo "DELETE $REST"
        ;;
      FLUSH)
        SHARD=$(echo "$REST" | cut -d' ' -f1)
        TXN=$(echo "$REST" | cut -d' ' -f2)
        OBJECTS=$(curl -s "$BASE/_admin/objects?since_txn=$LAST_TXN&limit=1000")
        COUNT=$(echo "$OBJECTS" | jq '.total')
        if [ "$COUNT" -gt 0 ]; then
          echo "--- flush shard=$SHARD txn=$TXN: $COUNT new objects ---"
          echo "$OBJECTS" | jq -r '.objects[] | "\(.created_txn) \(.size) \(.key)"'
        fi
        LAST_TXN=$TXN
        ;;
    esac
  done
} < <(socat - UNIX-CONNECT:"$SOCKET")
```
