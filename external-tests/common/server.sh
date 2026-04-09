#!/bin/bash
#
# common/server.sh - objstrd lifecycle helpers
#
# Source this file in test scripts:
#   source "$(dirname "$0")/../common/server.sh"
#
# Provides:
#   start_server <port> <image> <size_mb> [EXTRA_ENV=val ...]
#     Starts objstrd in the background, writes PID to /tmp/objstrd_<port>.pid
#     Waits until /_admin/info responds (max 10 s).
#     Prints the build_git_hash so callers can detect binary swaps.
#     Extra arguments after size_mb must be VAR=VALUE environment variable
#     assignments (they are prepended to the command line).
#
#   stop_server <port>
#     Kills the process by PID file.
#
#   get_build_hash <port>
#     Returns the build_git_hash from /_admin/info.
#
#   check_server_valid <port> <expected_hash>
#     Prints "ok", "crashed", or "replaced".
#

OBJSTRD_BIN="${OBJSTRD_BIN:-$HOME/build-objstrd/release/objstrd}"

# require_port_free <port>
#   Fails with an error if the given port is already in use.
#   Use this before start_server to avoid confusing bind failures.
require_port_free() {
    local port="$1"
    if command -v lsof >/dev/null 2>&1; then
        local pid
        pid=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -n "$pid" ]; then
            echo "ERROR: port $port already in use (pid $pid)" >&2
            return 1
        fi
    elif command -v ss >/dev/null 2>&1; then
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            echo "ERROR: port $port already in use" >&2
            return 1
        fi
    fi
    return 0
}

start_server() {
    local port="$1"
    local image="$2"
    local size_mb="$3"
    shift 3
    local pid_file="/tmp/objstrd_${port}.pid"

    IMAGE="$image" SIZE_MB="$size_mb" PORT="$port" "$@" "$OBJSTRD_BIN" &
    local pid=$!
    echo "$pid" > "$pid_file"

    # Wait up to 10 s for the server to be ready
    local i=0
    while [ $i -lt 100 ]; do
        if curl -sf "http://localhost:${port}/_admin/info" >/dev/null 2>&1; then
            local hash
            hash=$(get_build_hash "$port")
            echo "objstrd started on port $port (pid=$pid hash=$hash)"
            return 0
        fi
        sleep 0.1
        i=$((i + 1))
    done

    echo "ERROR: objstrd on port $port did not start within 10 s" >&2
    kill "$pid" 2>/dev/null || true
    return 1
}

stop_server() {
    local port="$1"
    local pid_file="/tmp/objstrd_${port}.pid"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        kill "$pid" 2>/dev/null || true
        rm -f "$pid_file"
    fi
}

get_build_hash() {
    local port="$1"
    curl -sf "http://localhost:${port}/_admin/info" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('build_git_hash','unknown'))" \
        2>/dev/null || echo "unknown"
}

check_server_valid() {
    local port="$1"
    local expected_hash="$2"
    local actual
    actual=$(get_build_hash "$port" 2>/dev/null) || actual=""
    if [ -z "$actual" ] || [ "$actual" = "unknown" ]; then
        echo "crashed"
    elif [ "$actual" = "$expected_hash" ]; then
        echo "ok"
    else
        echo "replaced"
    fi
}
