#!/bin/bash
#
# s3-compat/ceph/run_ceph_tests.sh
#
# Parallel ceph s3-tests runner for objstrd.
#
# Starts N objstrd instances on separate ports, splits the test suite
# across them, runs in parallel, and concatenates results.
#
# Usage: ./run_ceph_tests.sh [workers] [extra_pytest_args...]
#   workers: number of parallel workers (default: 6)
#
# Output: /tmp/s3s_ceph_results/  (per-worker + combined results)
#
# Prerequisites:
#   - ceph s3-tests cloned at S3TESTS_DIR (default: ~/s3-tests)
#   - Python venv set up inside s3-tests: cd s3-tests && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
#   - objstrd binary built at BINARY (default: ~/build-objstrd/release/objstrd)
#
set -euo pipefail

WORKERS=${1:-6}
shift 2>/dev/null || true
EXTRA_PYTEST_ARGS="$*"

BINARY="${BINARY:-$HOME/build-objstrd/release/objstrd}"
S3TESTS_DIR="${S3TESTS_DIR:-~/s3-tests}"
BASE_PORT="${BASE_PORT:-8010}"
IMAGE_SIZE_MB=128
RESULTS_DIR=/tmp/s3s_ceph_results
TEMPLATE_CONF=$S3TESTS_DIR/s3tests.conf

# credentials (same for all instances)
MAIN_AK="${MAIN_AK:-AKIAIOSFODNN7EXAMPLE}"
MAIN_SK="${MAIN_SK:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
ALT_AK="${ALT_AK:-AKIAI44QH8DHBEXAMPLE}"
ALT_SK="${ALT_SK:-je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY}"
TENANT_AK="${TENANT_AK:-AKIAIOSFODNN7TENANT1}"
TENANT_SK="${TENANT_SK:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYTENANTKEY}"
IAM_AK="${IAM_AK:-AKIAIOSFODNN7IAMEXAM}"
IAM_SK="${IAM_SK:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYIAMKEYEXAM}"

echo "=== Ceph s3-tests parallel runner ==="
echo "Workers: $WORKERS | Ports: $BASE_PORT-$((BASE_PORT + WORKERS - 1))"
echo "Results: $RESULTS_DIR"

# clean up any previous run
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

# kill any stale objstrd on our ports
for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    pid=$(lsof -ti :$port 2>/dev/null || true)
    [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
done

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

# --- Collect test names ---
echo "Collecting tests..."
cd "$S3TESTS_DIR"
source venv/bin/activate

S3TEST_CONF=$TEMPLATE_CONF python -m pytest s3tests/functional/test_s3.py \
    --collect-only -q 2>/dev/null \
    | grep '::' > "$RESULTS_DIR/all_tests.txt"

TOTAL=$(wc -l < "$RESULTS_DIR/all_tests.txt")
PER_WORKER=$(( (TOTAL + WORKERS - 1) / WORKERS ))
echo "Total tests: $TOTAL | ~$PER_WORKER per worker"

# split test list
split -l "$PER_WORKER" -d -a 1 "$RESULTS_DIR/all_tests.txt" "$RESULTS_DIR/chunk_"

# --- Start workers ---
for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    image="/tmp/s3s_worker${i}.raw"
    logfile="$RESULTS_DIR/server${i}.log"
    conffile="$RESULTS_DIR/s3tests_w${i}.conf"
    chunkfile="$RESULTS_DIR/chunk_${i}"

    if [ ! -f "$chunkfile" ]; then
        echo "Worker $i: no tests (chunk file missing), skipping"
        continue
    fi

    ntest=$(wc -l < "$chunkfile")
    echo "Worker $i: port=$port, $ntest tests"

    # create per-worker s3tests.conf (only port differs)
    sed "s/^port = .*/port = $port/" "$TEMPLATE_CONF" > "$conffile"

    # clean old data
    rm -f "$image"

    # start objstrd
    IMAGE="$image" SIZE_MB=$IMAGE_SIZE_MB BUCKET=testbucket PORT=$port \
        ACCESS_KEY=$MAIN_AK SECRET_KEY=$MAIN_SK \
        ACCESS_KEY_1=$ALT_AK SECRET_KEY_1=$ALT_SK \
        ACCESS_KEY_2=$TENANT_AK SECRET_KEY_2=$TENANT_SK \
        ACCESS_KEY_3=$IAM_AK SECRET_KEY_3=$IAM_SK \
        RUST_LOG=warn \
        "$BINARY" > "$logfile" 2>&1 &
    echo $! > "$RESULTS_DIR/worker${i}.pid"
done

# wait for all servers to be ready
echo "Waiting for servers..."
sleep 2
for i in $(seq 0 $((WORKERS - 1))); do
    port=$((BASE_PORT + i))
    chunkfile="$RESULTS_DIR/chunk_${i}"
    [ ! -f "$chunkfile" ] && continue

    for attempt in $(seq 1 10); do
        if curl -s -o /dev/null "http://localhost:$port/" 2>/dev/null; then
            break
        fi
        if [ $attempt -eq 10 ]; then
            echo "ERROR: Worker $i (port $port) failed to start"
            cat "$RESULTS_DIR/server${i}.log"
            exit 1
        fi
        sleep 1
    done
done
echo "All servers ready"

# --- Run tests in parallel ---
echo "Starting test execution..."
for i in $(seq 0 $((WORKERS - 1))); do
    chunkfile="$RESULTS_DIR/chunk_${i}"
    [ ! -f "$chunkfile" ] && continue

    conffile="$RESULTS_DIR/s3tests_w${i}.conf"
    resultfile="$RESULTS_DIR/result_w${i}.txt"
    junitfile="$RESULTS_DIR/junit_w${i}.xml"

    (
        cd "$S3TESTS_DIR"
        S3TEST_CONF="$conffile" python -m pytest \
            $(cat "$chunkfile" | tr '\n' ' ') \
            --tb=no -q \
            --junit-xml="$junitfile" \
            > "$resultfile" 2>&1 || true
        echo "Worker $i done: $(tail -1 "$resultfile")"
    ) &
done

echo "Waiting for all workers to finish..."
wait

# --- Combine results ---
echo ""
echo "============================================"
echo "=== Combined Results ==="
echo "============================================"

total_passed=0
total_failed=0
total_error=0
total_skipped=0

for i in $(seq 0 $((WORKERS - 1))); do
    resultfile="$RESULTS_DIR/result_w${i}.txt"
    [ ! -f "$resultfile" ] && continue

    summary=$(tail -1 "$resultfile")
    echo "Worker $i: $summary"

    p=$(echo "$summary" | grep -oP '\d+ passed' | grep -oP '\d+' || echo 0)
    f=$(echo "$summary" | grep -oP '\d+ failed' | grep -oP '\d+' || echo 0)
    e=$(echo "$summary" | grep -oP '\d+ error' | grep -oP '\d+' || echo 0)
    s=$(echo "$summary" | grep -oP '\d+ skipped' | grep -oP '\d+' || echo 0)

    total_passed=$((total_passed + p))
    total_failed=$((total_failed + f))
    total_error=$((total_error + e))
    total_skipped=$((total_skipped + s))
done

echo "--------------------------------------------"
echo "TOTAL: $total_passed passed, $total_failed failed, $total_error errors, $total_skipped skipped"
echo "============================================"

# Build combined per-test results file
echo "Building combined results..."
{
    echo "# Ceph s3-tests results - $(date -Iseconds)"
    echo "# $total_passed passed, $total_failed failed, $total_error errors, $total_skipped skipped"
    echo ""
    for i in $(seq 0 $((WORKERS - 1))); do
        resultfile="$RESULTS_DIR/result_w${i}.txt"
        [ ! -f "$resultfile" ] && continue
        cat "$resultfile"
    done
} > "$RESULTS_DIR/combined.txt"

# Build a simple failures list
{
    for i in $(seq 0 $((WORKERS - 1))); do
        resultfile="$RESULTS_DIR/result_w${i}.txt"
        [ ! -f "$resultfile" ] && continue
        grep -E '^(FAILED|ERROR) ' "$resultfile" || true
    done
} > "$RESULTS_DIR/failures.txt"

nfail=$(wc -l < "$RESULTS_DIR/failures.txt")
echo "Failure details: $RESULTS_DIR/failures.txt ($nfail lines)"
echo "Full output: $RESULTS_DIR/combined.txt"
echo "JUnit XML: $RESULTS_DIR/junit_w*.xml"

# Cleanup servers
for i in $(seq 0 $((WORKERS - 1))); do
    pid_file="$RESULTS_DIR/worker${i}.pid"
    if [ -f "$pid_file" ]; then
        kill -9 "$(cat "$pid_file")" 2>/dev/null || true
    fi
    rm -f "/tmp/s3s_worker${i}.raw"
done

echo "Done."
