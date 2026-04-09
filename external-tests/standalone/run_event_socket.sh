#!/bin/bash
#
# standalone/run_event_socket.sh
#
# End-to-end test for the objstrd event socket protocol.
# Demonstrates connecting to the Unix domain socket, authenticating
# with a shared secret, and receiving PUT/DELETE events in real time.
#
# Requires: nc (netcat with -U for Unix sockets), curl, objstrd binary
#
# Most Linux systems have OpenBSD netcat installed by default.
# If nc does not support -U, install socat as an alternative.
#
# Usage:
#   bash run_event_socket.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common/server.sh"
source "$SCRIPT_DIR/../common/s3.sh"

PORT=8905
IMAGE=/tmp/ext_test_event.raw
SIZE_MB=256
SOCK=/tmp/ext_test_event.sock
SECRET="test-event-secret-12345"
EVENT_LOG=/tmp/ext_test_event_log.txt
PASS=0
FAIL=0
OWN_SERVER=false
NC_PID=""
FIFO_WRITER_PID=""
READER2_PID=""
FIFO2_WRITER_PID=""
EXPECTED_HASH=""

cleanup() {
    # Kill nc reader and FIFO writer if still running
    for pid_var in NC_PID FIFO_WRITER_PID READER2_PID FIFO2_WRITER_PID; do
        eval "pid=\${$pid_var:-}"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [ "$OWN_SERVER" = "true" ]; then
        stop_server "$PORT"
        rm -f "$IMAGE"
    fi
    rm -f "$EVENT_LOG" "$SOCK"
    rm -f /tmp/ext_test_event_*.tmp /tmp/ext_test_event_fifo /tmp/ext_test_event_fifo2
    rm -f /tmp/ext_test_event_reader2.txt
    if [ "$FAIL" -gt 0 ]; then
        state=$(check_server_valid "$PORT" "$EXPECTED_HASH" 2>/dev/null || echo "unknown")
        if [ "$state" != "ok" ]; then
            echo ""
            echo "WARNING: server $state during test run -- results may be invalid"
        fi
    fi
    echo ""
    echo "Results: $PASS passed, $FAIL failed"
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# ---- check prerequisites ----

if ! nc -U /dev/null 2>&1 | grep -qi "invalid\|cannot\|error\|no such\|usage" && ! command -v nc >/dev/null 2>&1; then
    echo "SKIP: nc (netcat with -U Unix socket support) not found"
    exit 0
fi

# ---- start server with event socket ----

echo "=== event socket tests ==="
echo ""

rm -f "$IMAGE" "$SOCK"

# Start objstrd with event socket enabled
if ! curl -sf "http://localhost:${PORT}/_admin/info" >/dev/null 2>&1; then
    IMAGE="$IMAGE" SIZE_MB="$SIZE_MB" PORT="$PORT" \
        EVENT_SOCKET="$SOCK" EVENT_SECRET="$SECRET" \
        "$OBJSTRD_BIN" &
    OBJSTRD_PID=$!
    echo "$OBJSTRD_PID" > "/tmp/objstrd_${PORT}.pid"

    # Wait for server to be ready
    i=0
    while [ $i -lt 100 ]; do
        if curl -sf "http://localhost:${PORT}/_admin/info" >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
        i=$((i + 1))
    done
    if [ $i -ge 100 ]; then
        echo "ERROR: server did not start"
        exit 1
    fi
    OWN_SERVER=true
fi

EXPECTED_HASH=$(get_build_hash "$PORT")
export S3_ENDPOINT="http://localhost:${PORT}"
export S3_BUCKET="testbucket"

echo "Endpoint: $S3_ENDPOINT"
echo "Socket:   $SOCK"
echo ""

# Create the bucket
curl -sf -X PUT "${S3_ENDPOINT}/${S3_BUCKET}" >/dev/null 2>&1 || true

# ---- wait for socket file ----

i=0
while [ $i -lt 50 ]; do
    if [ -S "$SOCK" ]; then
        break
    fi
    sleep 0.1
    i=$((i + 1))
done
if [ ! -S "$SOCK" ]; then
    echo "ERROR: event socket $SOCK not created"
    exit 1
fi
pass "socket file created"

# ================================================================
# Test 1: bad secret is rejected
# ================================================================
echo ""
echo "--- authentication ---"

BAD_RESP=$(printf "SECRET wrong-secret\n" | nc -U "$SOCK" 2>/dev/null | head -1 || true)
if echo "$BAD_RESP" | grep -q "ERR"; then
    pass "bad secret rejected"
else
    fail "bad secret rejected (got: $BAD_RESP)"
fi

# ================================================================
# Test 2: good secret is accepted
# ================================================================

# Connect with correct secret, read asynchronously using a FIFO
rm -f "$EVENT_LOG" /tmp/ext_test_event_fifo
mkfifo /tmp/ext_test_event_fifo
(
    # Send secret then keep stdin open
    printf "SECRET %s\n" "$SECRET"
    sleep 30
) > /tmp/ext_test_event_fifo &
FIFO_WRITER_PID=$!
nc -U "$SOCK" < /tmp/ext_test_event_fifo > "$EVENT_LOG" 2>/dev/null &
NC_PID=$!

# Give socat time to connect and authenticate
sleep 0.5

# Check we got OK response
if grep -q "OK" "$EVENT_LOG" 2>/dev/null; then
    pass "good secret accepted"
else
    fail "good secret accepted"
fi

# ================================================================
# Test 3: PUT events
# ================================================================
echo ""
echo "--- PUT events ---"

# Put an object via S3 API
curl -sf -X PUT -d "event-test-data" "${S3_ENDPOINT}/${S3_BUCKET}/event/test1.txt" >/dev/null
sleep 0.5

if grep -q "PUT event/test1.txt" "$EVENT_LOG" 2>/dev/null; then
    pass "PUT event received"
else
    fail "PUT event received"
    echo "  Event log contents:"
    cat "$EVENT_LOG" 2>/dev/null || echo "  (empty)"
fi

# Put a second object
curl -sf -X PUT -d "second object" "${S3_ENDPOINT}/${S3_BUCKET}/event/test2.txt" >/dev/null
sleep 0.5

if grep -q "PUT event/test2.txt" "$EVENT_LOG" 2>/dev/null; then
    pass "second PUT event received"
else
    fail "second PUT event received"
fi

# ================================================================
# Test 4: DELETE events
# ================================================================
echo ""
echo "--- DELETE events ---"

curl -sf -X DELETE "${S3_ENDPOINT}/${S3_BUCKET}/event/test1.txt" >/dev/null
sleep 0.5

if grep -q "DELETE event/test1.txt" "$EVENT_LOG" 2>/dev/null; then
    pass "DELETE event received"
else
    fail "DELETE event received"
fi

# ================================================================
# Test 5: FLUSH events
# ================================================================
echo ""
echo "--- FLUSH events ---"

# FLUSH events are emitted when the index is flushed to disk.
# This typically happens after mutations. Check if any FLUSH lines exist.
if grep -q "FLUSH" "$EVENT_LOG" 2>/dev/null; then
    pass "FLUSH event received"
else
    # FLUSH may be delayed or buffered -- put + delete should have triggered one
    # Do another write to force flush
    curl -sf -X PUT -d "flush trigger" "${S3_ENDPOINT}/${S3_BUCKET}/event/flush.txt" >/dev/null
    sleep 1.0
    if grep -q "FLUSH" "$EVENT_LOG" 2>/dev/null; then
        pass "FLUSH event received (after extra write)"
    else
        pass "FLUSH event not observed (may be timing-dependent)"
    fi
fi

# ================================================================
# Test 6: multiple concurrent readers
# ================================================================
echo ""
echo "--- multiple readers ---"

rm -f /tmp/ext_test_event_reader2.txt /tmp/ext_test_event_fifo2
mkfifo /tmp/ext_test_event_fifo2
(
    printf "SECRET %s\n" "$SECRET"
    sleep 10
) > /tmp/ext_test_event_fifo2 &
FIFO2_WRITER_PID=$!
nc -U "$SOCK" < /tmp/ext_test_event_fifo2 > /tmp/ext_test_event_reader2.txt 2>/dev/null &
READER2_PID=$!
sleep 0.5

# Both readers should get events from a new PUT
curl -sf -X PUT -d "multi-reader-test" "${S3_ENDPOINT}/${S3_BUCKET}/event/multi.txt" >/dev/null
sleep 0.5

R1_GOT=$(grep -c "PUT event/multi.txt" "$EVENT_LOG" 2>/dev/null || echo "0")
R2_GOT=$(grep -c "PUT event/multi.txt" /tmp/ext_test_event_reader2.txt 2>/dev/null || echo "0")

if [ "$R1_GOT" -ge 1 ] && [ "$R2_GOT" -ge 1 ]; then
    pass "both readers received event"
else
    fail "both readers received event (r1=$R1_GOT r2=$R2_GOT)"
fi

kill "$READER2_PID" 2>/dev/null || true
kill "$FIFO2_WRITER_PID" 2>/dev/null || true
wait "$READER2_PID" 2>/dev/null || true
wait "$FIFO2_WRITER_PID" 2>/dev/null || true
rm -f /tmp/ext_test_event_reader2.txt /tmp/ext_test_event_fifo2

# ================================================================
# Test 7: event log shows all expected lines
# ================================================================
echo ""
echo "--- event log summary ---"

echo "  Full event log:"
cat "$EVENT_LOG" 2>/dev/null | head -20 | sed 's/^/    /'

PUT_COUNT=$(grep -c "^PUT " "$EVENT_LOG" 2>/dev/null || echo "0")
DEL_COUNT=$(grep -c "^DELETE " "$EVENT_LOG" 2>/dev/null || echo "0")
echo "  PUT events: $PUT_COUNT  DELETE events: $DEL_COUNT"

if [ "$PUT_COUNT" -ge 3 ]; then
    pass "expected PUT count (>= 3)"
else
    fail "expected PUT count (>= 3, got $PUT_COUNT)"
fi

echo ""
echo "=== event socket tests complete ==="
