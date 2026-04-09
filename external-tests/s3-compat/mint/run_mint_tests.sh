#!/bin/bash
#
# s3-compat/mint/run_mint_tests.sh
#
# Parallel mint-style S3 test runner for objstrd.
#
# Starts N objstrd instances on separate ports, runs the Python test suite
# against each, and combines results.
#
# Usage: ./run_mint_tests.sh [workers] [extra_args...]
#   workers: number of parallel workers (default: 1)
#
# Output: /tmp/mint_results/ (per-worker logs + combined JSON)
#
set -euo pipefail

WORKERS=${1:-1}
shift 2>/dev/null || true
EXTRA_ARGS="$*"

BINARY="${BINARY:-$HOME/build-objstrd/release/objstrd}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_PORT="${BASE_PORT:-8020}"
IMAGE_SIZE_MB=1024
RESULTS_DIR=/tmp/mint_results
MINT_DATA_DIR="${MINT_DATA_DIR:-/tmp/mint_data}"

MAIN_AK=AKIAIOSFODNN7EXAMPLE
MAIN_SK=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

echo "=== Mint S3 tests parallel runner ==="
echo "Workers: $WORKERS | Ports: $BASE_PORT-$((BASE_PORT + WORKERS - 1))"
echo "Results: $RESULTS_DIR"

echo "Creating data files..."
bash "$SCRIPT_DIR/create_data_files.sh" "$MINT_DATA_DIR"
echo ""

cleanup() {
    echo "Cleaning up..."
    for i in $(seq 0 $((WORKERS - 1))); do
        local pid_file="$RESULTS_DIR/worker${i}.pid"
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    pid=$(lsof -ti :$port 2>/dev/null || true)
    [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
done

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    image="/tmp/mint_worker${i}.raw"
    logfile="$RESULTS_DIR/server${i}.log"

    echo "Worker $i: port=$port"

    rm -f "$image"

    IMAGE="$image" SIZE_MB=$IMAGE_SIZE_MB PORT=$port \
        ACCESS_KEY=$MAIN_AK SECRET_KEY=$MAIN_SK \
        RUST_LOG=warn \
        "$BINARY" > "$logfile" 2>&1 &
    echo $! > "$RESULTS_DIR/worker${i}.pid"
done

echo "Waiting for servers..."
sleep 2
for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    for attempt in $(seq 1 15); do
        if curl -s -o /dev/null "http://localhost:$port/" 2>/dev/null; then
            break
        fi
        if [ $attempt -eq 15 ]; then
            echo "ERROR: Worker $i (port $port) failed to start"
            cat "$RESULTS_DIR/server${i}.log"
            exit 1
        fi
        sleep 1
    done
done
echo "All servers ready"
echo ""

echo "Starting test execution..."
start_time=$(date +%s)

TEST_PIDS=""
for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    log_file="$RESULTS_DIR/mint_w${i}.json"
    out_file="$RESULTS_DIR/output_w${i}.txt"

    (
        SERVER_ENDPOINT="localhost:$port" \
        ACCESS_KEY="$MAIN_AK" \
        SECRET_KEY="$MAIN_SK" \
        MINT_DATA_DIR="$MINT_DATA_DIR" \
        LOG_FILE="$log_file" \
        TEST_BUCKET="mint-w${i}-$(date +%s)" \
        RUN_ON_FAIL=1 \
        python3 "$SCRIPT_DIR/s3_mint_tests.py" > "$out_file" 2>&1 || true
        echo "Worker $i done"
    ) &
    TEST_PIDS="$TEST_PIDS $!"
done

echo "Waiting for all workers to finish..."
for pid in $TEST_PIDS; do
    wait "$pid" 2>/dev/null || true
done

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "============================================"
echo "=== Combined Results ==="
echo "============================================"

total_pass=0
total_fail=0
total_na=0

for i in $(seq 0 $((WORKERS - 1))); do
    log_file="$RESULTS_DIR/mint_w${i}.json"
    out_file="$RESULTS_DIR/output_w${i}.txt"

    if [ ! -f "$log_file" ]; then
        echo "Worker $i: NO RESULTS (log file missing)"
        if [ -f "$out_file" ]; then
            echo "  stdout:"
            tail -5 "$out_file"
        fi
        continue
    fi

    p=$(grep -c '"status":"PASS"' "$log_file" || true)
    f=$(grep -c '"status":"FAIL"' "$log_file" || true)
    n=$(grep -c '"status":"NA"' "$log_file" || true)
    p=${p:-0}; f=${f:-0}; n=${n:-0}

    echo "Worker $i: $p passed, $f failed, $n skipped"

    total_pass=$((total_pass + p))
    total_fail=$((total_fail + f))
    total_na=$((total_na + n))
done

total=$((total_pass + total_fail + total_na))

echo "--------------------------------------------"
echo "TOTAL: $total_pass passed, $total_fail failed, $total_na skipped ($total tests in ${duration}s)"
echo "============================================"

cat "$RESULTS_DIR"/mint_w*.json > "$RESULTS_DIR/log.json" 2>/dev/null || true

echo ""
echo "Detailed results: $RESULTS_DIR/log.json"
echo "Parse with: python3 $SCRIPT_DIR/parse_mint_results.py $RESULTS_DIR/log.json"
