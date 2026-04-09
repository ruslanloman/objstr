#!/bin/bash
#
# s3-compat/rclone/run_rclone_tests.sh
#
# rclone S3 compatibility test runner for objstrd.
#
# Starts a objstrd instance, runs rclone operations, reports results in
# mint-compatible JSON format.
#
# Prerequisites:
#   - rclone must be installed (see note below)
#   - Data files in MINT_DATA_DIR (run ../mint/create_data_files.sh first)
#
# NOTE: apt install rclone on Ubuntu 22.04 gives v1.53.3-DEV which is broken.
# Install from the official script instead:
#   curl https://rclone.org/install.sh | sudo bash
# Or to ~/.local/bin without sudo:
#   mkdir -p ~/.local/bin
#   curl -L https://downloads.rclone.org/rclone-current-linux-amd64.zip -o /tmp/rclone.zip
#   unzip -j /tmp/rclone.zip '*/rclone' -d ~/.local/bin && chmod +x ~/.local/bin/rclone
#
# Usage:
#   bash run_rclone_tests.sh
#
set -euo pipefail

if [ -x "$HOME/.local/bin/rclone" ]; then
    export PATH="$HOME/.local/bin:$PATH"
fi

if ! command -v rclone >/dev/null 2>&1; then
    echo "ERROR: rclone not found on PATH or in ~/.local/bin."
    echo ""
    echo "Install from the official script (requires sudo):"
    echo "  curl https://rclone.org/install.sh | sudo bash"
    echo ""
    echo "Or install to ~/.local/bin without sudo:"
    echo "  mkdir -p ~/.local/bin"
    echo "  curl -L https://downloads.rclone.org/rclone-current-linux-amd64.zip -o /tmp/rclone.zip"
    echo "  unzip -j /tmp/rclone.zip '*/rclone' -d ~/.local/bin && chmod +x ~/.local/bin/rclone"
    echo ""
    echo "Skipping rclone tests."
    exit 0
fi

RCLONE_VERSION=$(rclone version 2>&1 | head -1)
echo "rclone: ${RCLONE_VERSION}"
echo ""

BINARY="${BINARY:-$HOME/build-objstrd/release/objstrd}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8040}"
IMAGE_SIZE_MB=512
RESULTS_DIR=/tmp/rclone_results
MINT_DATA_DIR="${MINT_DATA_DIR:-/tmp/mint_data}"
ACCESS_KEY="${ACCESS_KEY:-AKIAIOSFODNN7EXAMPLE}"
SECRET_KEY="${SECRET_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
BUCKET="${BUCKET:-rclone-test-$$}"

echo "=== rclone S3 tests ==="
echo "Server:   http://localhost:${PORT}"
echo "Results:  ${RESULTS_DIR}"
echo ""

if [ ! -f "${MINT_DATA_DIR}/datafile-10-MB" ]; then
    echo "Data files not found in ${MINT_DATA_DIR}."
    echo "Creating data files..."
    bash "$SCRIPT_DIR/../mint/create_data_files.sh" "$MINT_DATA_DIR"
fi

cleanup() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill -9 "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f /tmp/rclone_test_$$.raw
}
trap cleanup EXIT

pid=$(lsof -ti :$PORT 2>/dev/null || true)
[ -n "$pid" ] && kill -9 $pid 2>/dev/null || true

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

IMAGE=/tmp/rclone_test_$$.raw SIZE_MB=$IMAGE_SIZE_MB PORT=$PORT \
    ACCESS_KEY=$ACCESS_KEY SECRET_KEY=$SECRET_KEY \
    RUST_LOG=warn \
    "$BINARY" > "$RESULTS_DIR/server.log" 2>&1 &
SERVER_PID=$!

echo "Waiting for server..."
for attempt in $(seq 1 15); do
    if curl -s -o /dev/null "http://localhost:${PORT}/" 2>/dev/null; then
        break
    fi
    if [ $attempt -eq 15 ]; then
        echo "ERROR: Server failed to start"
        cat "$RESULTS_DIR/server.log"
        exit 1
    fi
    sleep 1
done
echo "Server ready"
echo ""

start_time=$(date +%s)

ENDPOINT="localhost:${PORT}" \
ACCESS_KEY="$ACCESS_KEY" \
SECRET_KEY="$SECRET_KEY" \
BUCKET="$BUCKET" \
LOG_FILE="$RESULTS_DIR/rclone.json" \
DATA_DIR="$MINT_DATA_DIR" \
bash "$SCRIPT_DIR/rclone_tests.sh" 2>&1 | tee "$RESULTS_DIR/output.txt"

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "Duration: ${duration}s"
echo "Results:  ${RESULTS_DIR}/rclone.json"
