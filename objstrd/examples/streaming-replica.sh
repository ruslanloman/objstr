#!/bin/bash
#
# streaming-replica.sh
#
# Demonstrates streaming read-only replicas using three objstrd instances:
#
#   1. WRITER   -- normal read-write objstrd with an event socket
#   2. READER1  -- read-only replica via Unix socket (same machine)
#   3. READER2  -- read-only replica via socat TCP bridge (cross-machine)
#
# All three share the same backing shard (same filesystem path).  The
# readers never scan the shard on their own -- they rebuild the catalog
# once at startup, then apply PUT/DELETE events from the writer in
# real time.
#
# READER1 connects directly to the writer's Unix event socket.
# READER2 connects over TCP via socat, which bridges the Unix socket
# to a TCP port.  In a real deployment the socat listener would run
# on the writer machine, and READER2 could be on any host that can
# reach it over the network.
#
# This pattern is useful for:
#   - Scaling reads across multiple nodes
#   - Building a change feed (fire a Lambda, webhook, or pipeline
#     whenever an object is written or deleted)
#   - Cross-datacenter replication over a TCP link
#
# NOTE: Currently limited to a single backend shard (rf=1).  Multi-shard
# streaming will be added in a future release.
#
# Requirements: objstrd, socat, curl
#
# Usage:
#   OBJSTRD_BIN=/path/to/objstrd bash streaming-replica.sh
#
set -euo pipefail

OBJSTRD="${OBJSTRD_BIN:-~/build-objstrd/release/objstrd}"
SHARD_DIR="/tmp/streaming-replica-test"
WRITER_PORT=8801
READER1_PORT=8802
READER2_PORT=8803
SOCAT_PORT=9090
EVENT_SOCK="/tmp/writer-events.sock"
EVENT_SECRET="replica-test-secret"
WRITER_CONF="/tmp/streaming-writer.conf"
READER1_CONF="/tmp/streaming-reader1.conf"
READER2_CONF="/tmp/streaming-reader2.conf"
WRITER_PID=""
READER1_PID=""
READER2_PID=""
SOCAT_PID=""

cleanup() {
    echo ""
    echo "--- cleanup ---"
    [ -n "$READER2_PID" ] && kill "$READER2_PID" 2>/dev/null && wait "$READER2_PID" 2>/dev/null || true
    [ -n "$READER1_PID" ] && kill "$READER1_PID" 2>/dev/null && wait "$READER1_PID" 2>/dev/null || true
    [ -n "$SOCAT_PID" ]   && kill "$SOCAT_PID"   2>/dev/null && wait "$SOCAT_PID"   2>/dev/null || true
    [ -n "$WRITER_PID" ]  && kill "$WRITER_PID"  2>/dev/null && wait "$WRITER_PID"  2>/dev/null || true
    rm -rf "$SHARD_DIR" "$EVENT_SOCK" "$WRITER_CONF" "$READER1_CONF" "$READER2_CONF"
}
trap cleanup EXIT

mkdir -p "$SHARD_DIR"

# ---- check prerequisites ----

if ! command -v socat >/dev/null 2>&1; then
    echo "ERROR: socat is required for this example (apt install socat)"
    exit 1
fi

# ---- generate config files ----

cat > "$WRITER_CONF" <<EOF
# Writer config -- serves S3 on port $WRITER_PORT, broadcasts events
cluster        streaming-demo
bucket         testbucket

event_socket   $EVENT_SOCK
event_secret   $EVENT_SECRET

writer  rf=1  listen=0.0.0.0:$WRITER_PORT  endpoint=http://127.0.0.1:$WRITER_PORT
  fs  $SHARD_DIR
EOF

cat > "$READER1_CONF" <<EOF
# Reader 1 -- connects via Unix socket (same machine as writer)
cluster        streaming-demo
bucket         testbucket

event_source   $EVENT_SOCK
event_secret   $EVENT_SECRET

reader1  rf=1  listen=0.0.0.0:$READER1_PORT  endpoint=http://127.0.0.1:$READER1_PORT
  fs  $SHARD_DIR
EOF

cat > "$READER2_CONF" <<EOF
# Reader 2 -- connects via TCP (socat bridge, simulates cross-machine)
cluster        streaming-demo
bucket         testbucket

event_source   tcp:127.0.0.1:$SOCAT_PORT
event_secret   $EVENT_SECRET

reader2  rf=1  listen=0.0.0.0:$READER2_PORT  endpoint=http://127.0.0.1:$READER2_PORT
  fs  $SHARD_DIR
EOF

echo "=== Config files ==="
echo ""
echo "Writer ($WRITER_CONF):"
cat "$WRITER_CONF"
echo ""
echo "Reader 1 - Unix socket ($READER1_CONF):"
cat "$READER1_CONF"
echo ""
echo "Reader 2 - TCP/socat ($READER2_CONF):"
cat "$READER2_CONF"
echo ""

# ---- start writer ----

echo "Starting writer on port $WRITER_PORT ..."
"$OBJSTRD" --config "$WRITER_CONF" --node writer &
WRITER_PID=$!

for i in $(seq 1 30); do
    if curl -sf "http://localhost:${WRITER_PORT}/_admin/info" >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done
echo "Writer is up (PID $WRITER_PID)"

# ---- start socat bridge ----
#
# In a real cross-machine setup, this runs on the WRITER machine:
#   socat TCP-LISTEN:9090,reuseaddr,fork UNIX-CONNECT:/path/to/events.sock
#
# Readers on other machines then use:
#   event_source  tcp:<writer-ip>:9090

echo "Starting socat TCP bridge on port $SOCAT_PORT ..."
socat TCP-LISTEN:${SOCAT_PORT},reuseaddr,fork UNIX-CONNECT:"${EVENT_SOCK}" &
SOCAT_PID=$!
sleep 0.3
echo "Socat bridge is up (PID $SOCAT_PID)"

# ---- start reader 1 (Unix socket) ----

echo "Starting reader 1 (Unix socket) on port $READER1_PORT ..."
"$OBJSTRD" --config "$READER1_CONF" --node reader1 --read-only &
READER1_PID=$!

for i in $(seq 1 30); do
    if curl -sf "http://localhost:${READER1_PORT}/_admin/info" >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done
echo "Reader 1 is up (PID $READER1_PID)"

# ---- start reader 2 (TCP via socat) ----

echo "Starting reader 2 (TCP/socat) on port $READER2_PORT ..."
"$OBJSTRD" --config "$READER2_CONF" --node reader2 --read-only &
READER2_PID=$!

for i in $(seq 1 30); do
    if curl -sf "http://localhost:${READER2_PORT}/_admin/info" >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done
echo "Reader 2 is up (PID $READER2_PID)"

# ---- verify all nodes report correct state ----

echo ""
echo "=== Node info ==="
for PORT_LABEL in "writer:$WRITER_PORT" "reader1:$READER1_PORT" "reader2:$READER2_PORT"; do
    LABEL="${PORT_LABEL%%:*}"
    PORT="${PORT_LABEL##*:}"
    RO=$(curl -sf "http://localhost:${PORT}/_admin/info" | grep -o '"read_only":[a-z]*' || echo '"read_only":?')
    ES=$(curl -sf "http://localhost:${PORT}/_admin/info" | grep -o '"event_source":"[^"]*"' || echo "none")
    echo "  $LABEL (port $PORT): $RO  $ES"
done

# ---- test: write via writer, read via both readers ----

echo ""
echo "=== PUT via writer ==="
curl -sf -X PUT "http://localhost:${WRITER_PORT}/testbucket/hello.txt" \
    -d "Hello from the writer" \
    -o /dev/null -w "  PUT /testbucket/hello.txt -> HTTP %{http_code}\n"

# Give events a moment to propagate to both readers
sleep 0.5

echo ""
echo "=== GET via reader 1 (Unix socket) ==="
BODY1=$(curl -sf "http://localhost:${READER1_PORT}/testbucket/hello.txt")
echo "  Body: $BODY1"
if [ "$BODY1" = "Hello from the writer" ]; then
    echo "  PASS: reader 1 returned correct content"
else
    echo "  FAIL: expected 'Hello from the writer', got '$BODY1'"
    exit 1
fi

echo ""
echo "=== GET via reader 2 (TCP/socat) ==="
BODY2=$(curl -sf "http://localhost:${READER2_PORT}/testbucket/hello.txt")
echo "  Body: $BODY2"
if [ "$BODY2" = "Hello from the writer" ]; then
    echo "  PASS: reader 2 returned correct content"
else
    echo "  FAIL: expected 'Hello from the writer', got '$BODY2'"
    exit 1
fi

# ---- test: delete via writer, verify gone on both readers ----

echo ""
echo "=== DELETE via writer ==="
curl -sf -X DELETE "http://localhost:${WRITER_PORT}/testbucket/hello.txt" \
    -o /dev/null -w "  DELETE /testbucket/hello.txt -> HTTP %{http_code}\n"

sleep 0.5

echo ""
echo "=== HEAD via reader 1 (should 404) ==="
HTTP1=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:${READER1_PORT}/testbucket/hello.txt" || true)
if [ "$HTTP1" = "404" ]; then
    echo "  PASS: reader 1 returned 404 after delete"
else
    echo "  FAIL: expected 404, got $HTTP1"
    exit 1
fi

echo ""
echo "=== HEAD via reader 2 (should 404) ==="
HTTP2=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:${READER2_PORT}/testbucket/hello.txt" || true)
if [ "$HTTP2" = "404" ]; then
    echo "  PASS: reader 2 returned 404 after delete"
else
    echo "  FAIL: expected 404, got $HTTP2"
    exit 1
fi

echo ""
echo "=== All tests passed (1 writer + 2 readers) ==="
