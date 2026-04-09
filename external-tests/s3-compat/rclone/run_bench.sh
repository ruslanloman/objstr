#!/bin/bash
#
# run_bench.sh - rclone throughput benchmark for objstrd (multi-backend)
#
# Measures upload/download throughput across multiple storage backends:
#
#   raw-file        RawObjectStore on a 2 GB image file  (no O_DIRECT)
#   raw-file-dio    RawObjectStore on a 2 GB image file  (O_DIRECT)
#   raw-dev         RawObjectStore on /dev/sdb block device (no O_DIRECT)
#   raw-dev-dio     RawObjectStore on /dev/sdb block device (O_DIRECT)
#   raw-loop        RawObjectStore on a loop device (file-backed) (no O_DIRECT)
#   raw-loop-dio    RawObjectStore on a loop device (file-backed) (O_DIRECT)
#   fs              LocalFileSystem (object_store file-per-object backend)
#   mem             InMemory (in-process memory backend - theoretical ceiling)
#
# Phases per backend:
#   P1   small (4Ki/1Mi/8Mi)          1 thread   single PUT
#   P2   small (4Ki/1Mi/8Mi)          4 threads  single PUT
#   P3   large (64Mi/128Mi/256Mi)     1 thread   single PUT
#   P4   large (64Mi/128Mi/256Mi)     4 threads  single PUT
#   P5   large (64Mi/128Mi/256Mi)     1 thread   multipart (16Mi chunks)
#   P6   large (64Mi/128Mi/256Mi)     4 threads  multipart (16Mi chunks)
#   P7   mixed (tiny+medium+large)    concurrent (3 rclone processes, 30s)
#        Exercises simultaneous small and large I/O with flush pressure.
#        flush interval = 2 s for all raw backends (hits ~15 flushes in 30s).
#
# O_DIRECT flag is stored in the RawObjectStore superblock at format time.
# Each test pre-formats its image/device; open() detects DIO from superblock.
#
# Prerequisites:
#   - rclone v1.73+ at ~/.local/bin/rclone  (apt version on Ubuntu 22.04 is broken)
#   - objstrd at BINARY path (built with BACKEND= support)
#   - rawobjstr CLI at RAWOBJST_CLI path for device formatting
#   - /dev/sdb must be available (world read-write) for raw-dev* backends
#   - sudo required for loop device backends (losetup)
#
# Usage:
#   bash run_bench.sh [--quick] [--backends COMMA-LIST|all]
#
#   --quick             5s/phase, file-cap 1 for large  (~5 min total)
#   default             15s/phase, file-cap 2 for large (~20 min total)
#   --backends all      run all 8 backends (default)
#   --backends raw-file,mem   run only those two backends
#
set -euo pipefail

# ---- arg parsing ----
QUICK=false
BACKENDS_ARG="all"
while [ $# -gt 0 ]; do
    case "$1" in
        --quick)   QUICK=true ;;
        --backends) shift; BACKENDS_ARG="${1:-all}" ;;
        *) ;;
    esac
    shift
done

if [ "$BACKENDS_ARG" = "all" ]; then
    BACKENDS="raw-file raw-file-dio raw-dev raw-dev-dio raw-loop raw-loop-dio fs mem"
else
    BACKENDS="${BACKENDS_ARG//,/ }"
fi

# ---- Check rclone ----
if [ -x "$HOME/.local/bin/rclone" ]; then
    export PATH="$HOME/.local/bin:$PATH"
fi
if ! command -v rclone >/dev/null 2>&1; then
    echo "ERROR: rclone not found. Install to ~/.local/bin (apt version is broken):"
    echo "  curl -L https://downloads.rclone.org/rclone-current-linux-amd64.zip -o /tmp/rclone.zip"
    echo "  unzip -j /tmp/rclone.zip '*/rclone' -d ~/.local/bin && chmod +x ~/.local/bin/rclone"
    exit 1
fi
RCLONE_VERSION=$(rclone version 2>&1 | head -1)

# ---- Paths ----
BINARY=~/build-objstrd/release/objstrd
RAWOBJST_CLI=~/build-rawobjstr/release/rawobjstr
PORT=8050
RESULTS_BASE=/tmp/rclone_bench
DEV_SDB=/dev/sdb
IMAGE_SIZE_MB=2048

ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Flush interval for all RawObjectStore backends: 2 s so a 30-s mixed test
# hits ~15 flushes.
FLUSH_SECS=2

if $QUICK; then
    TEST_TIME=5s
    SMALL_CAP=10
    LARGE_CAP=1
    MIXED_TIME=30s   # long enough for ~15 flushes even in quick mode
else
    TEST_TIME=15s
    SMALL_CAP=50
    LARGE_CAP=2
    MIXED_TIME=60s
fi

# ---- Disk space check ----
AVAIL_KB=$(df --output=avail / | tail -1)
AVAIL_GB=$((AVAIL_KB / 1024 / 1024))
echo "Disk free on /: ${AVAIL_GB} GB"
if [ "$AVAIL_GB" -lt 5 ]; then
    echo "WARNING: less than 5 GB free. Consider cleaning /tmp or build dirs."
fi

# ---- Verify binaries ----
[ -x "$BINARY" ]      || { echo "ERROR: $BINARY not found"; exit 1; }
[ -x "$RAWOBJST_CLI" ] || { echo "ERROR: $RAWOBJST_CLI not found"; exit 1; }

rm -rf "$RESULTS_BASE"
mkdir -p "$RESULTS_BASE"

echo "=== objstrd multi-backend benchmark ==="
echo "rclone:     ${RCLONE_VERSION}"
echo "Server:     ${BINARY}"
echo "Backends:   ${BACKENDS}"
echo "Test time:  ${TEST_TIME}/phase, mixed=${MIXED_TIME}"
echo "Flush:      ${FLUSH_SECS}s (raw backends)"
echo ""

# ================================================================
# Helper functions
# ================================================================

sep() {
    echo ""
    echo "================================================================"
    echo "  $*"
    echo "================================================================"
    echo ""
}

# wait_for_server PORT - poll until HTTP responds or fail
wait_for_server() {
    local p="$1"
    for attempt in $(seq 1 30); do
        if curl -s -o /dev/null "http://localhost:${p}/" 2>/dev/null; then
            return 0
        fi
        if [ "$attempt" -eq 30 ]; then
            echo "ERROR: server on port $p failed to start"
            return 1
        fi
        sleep 1
    done
}

# configure rclone remote for PORT
setup_rclone() {
    local p="$1"
    export RCLONE_CONFIG_BENCH_TYPE=s3
    export RCLONE_CONFIG_BENCH_PROVIDER=Other
    export RCLONE_CONFIG_BENCH_ENDPOINT="http://localhost:${p}"
    export RCLONE_CONFIG_BENCH_ACCESS_KEY_ID="${ACCESS_KEY}"
    export RCLONE_CONFIG_BENCH_SECRET_ACCESS_KEY="${SECRET_KEY}"
    export RCLONE_CONFIG_BENCH_FORCE_PATH_STYLE=true
}

# run_phases RESULTS_DIR BUCKET - run all 7 phases against an already-running server
run_phases() {
    local rdir="$1"
    local bucket="$2"

    mkdir -p "$rdir"

    # pre-create bucket
    rclone mkdir "bench:${bucket}" 2>/dev/null || true

    # ---------- Phase 1 ----------
    sep "P1  small (4Ki/1Mi/8Mi)  1t  single PUT"
    rclone test speed "bench:${bucket}" \
        --small 4Ki --medium 1Mi --large 8Mi \
        --test-time "${TEST_TIME}" --file-cap "${SMALL_CAP}" \
        --transfers 1 --s3-upload-cutoff 5Gi \
        2>&1 | tee "${rdir}/p1_small_t1.txt"

    # ---------- Phase 2 ----------
    sep "P2  small (4Ki/1Mi/8Mi)  4t  single PUT"
    rclone test speed "bench:${bucket}" \
        --small 4Ki --medium 1Mi --large 8Mi \
        --test-time "${TEST_TIME}" --file-cap "${SMALL_CAP}" \
        --transfers 4 --s3-upload-cutoff 5Gi \
        2>&1 | tee "${rdir}/p2_small_t4.txt"

    # ---------- Phase 3 ----------
    sep "P3  large (64Mi/128Mi/256Mi)  1t  single PUT"
    rclone test speed "bench:${bucket}" \
        --small 64Mi --medium 128Mi --large 256Mi \
        --test-time "${TEST_TIME}" --file-cap "${LARGE_CAP}" \
        --transfers 1 --s3-upload-cutoff 5Gi \
        2>&1 | tee "${rdir}/p3_large_nomp_t1.txt"

    # ---------- Phase 4 ----------
    sep "P4  large (64Mi/128Mi/256Mi)  4t  single PUT"
    rclone test speed "bench:${bucket}" \
        --small 64Mi --medium 128Mi --large 256Mi \
        --test-time "${TEST_TIME}" --file-cap "${LARGE_CAP}" \
        --transfers 4 --s3-upload-cutoff 5Gi \
        2>&1 | tee "${rdir}/p4_large_nomp_t4.txt"

    # ---------- Phase 5 ----------
    sep "P5  large (64Mi/128Mi/256Mi)  1t  multipart"
    rclone test speed "bench:${bucket}" \
        --small 64Mi --medium 128Mi --large 256Mi \
        --test-time "${TEST_TIME}" --file-cap "${LARGE_CAP}" \
        --transfers 1 \
        --s3-upload-cutoff 5Mi --s3-chunk-size 16Mi --s3-upload-concurrency 4 \
        2>&1 | tee "${rdir}/p5_large_mp_t1.txt"

    # ---------- Phase 6 ----------
    sep "P6  large (64Mi/128Mi/256Mi)  4t  multipart"
    rclone test speed "bench:${bucket}" \
        --small 64Mi --medium 128Mi --large 256Mi \
        --test-time "${TEST_TIME}" --file-cap "${LARGE_CAP}" \
        --transfers 4 \
        --s3-upload-cutoff 5Mi --s3-chunk-size 16Mi --s3-upload-concurrency 4 \
        2>&1 | tee "${rdir}/p6_large_mp_t4.txt"

    # ---------- Phase 7 ----------
    # Mixed concurrent workload: tiny (4Ki x16t) + medium (1Mi x8t) + large
    # (64Mi x2t multipart) all running simultaneously for MIXED_TIME.
    # This is the stress test: thousands of tiny requests mixing with large
    # streaming uploads and downloads while the flush timer fires repeatedly.
    sep "P7  MIXED concurrent  tiny+medium+large  (${MIXED_TIME})"
    local tf_tiny="${rdir}/p7_mix_tiny.txt"
    local tf_med="${rdir}/p7_mix_med.txt"
    local tf_large="${rdir}/p7_mix_large.txt"

    (rclone test speed "bench:${bucket}" \
        --small 4Ki --medium 4Ki --large 4Ki \
        --test-time "${MIXED_TIME}" --file-cap 200 \
        --transfers 16 --s3-upload-cutoff 5Gi \
        2>&1 | tee "${tf_tiny}") &
    PID_TINY=$!

    (rclone test speed "bench:${bucket}" \
        --small 1Mi --medium 1Mi --large 1Mi \
        --test-time "${MIXED_TIME}" --file-cap 50 \
        --transfers 8 --s3-upload-cutoff 5Gi \
        2>&1 | tee "${tf_med}") &
    PID_MED=$!

    (rclone test speed "bench:${bucket}" \
        --small 64Mi --medium 128Mi --large 256Mi \
        --test-time "${MIXED_TIME}" --file-cap 2 \
        --transfers 2 \
        --s3-upload-cutoff 5Mi --s3-chunk-size 16Mi --s3-upload-concurrency 4 \
        2>&1 | tee "${tf_large}") &
    PID_LARGE=$!

    wait $PID_TINY $PID_MED $PID_LARGE || true

    # Cleanup bench bucket
    rclone purge "bench:${bucket}" 2>/dev/null || true
}

# ================================================================
# Per-backend logic
# ================================================================

run_backend() {
    local name="$1"
    local rdir="${RESULTS_BASE}/${name}"
    local bucket="bench-${name}-$$"
    local server_pid=""
    local loop_dev=""
    local image_path=""

    sep "BACKEND: ${name}"

    # ---- Setup ----
    case "$name" in

        raw-file)
            image_path="/tmp/rclone_bench_${name}_$$.raw"
            echo "Formatting ${IMAGE_SIZE_MB} MB image (no DIO): ${image_path}"
            "$RAWOBJST_CLI" format --file "$image_path" --size $((IMAGE_SIZE_MB * 1024 * 1024))
            IMAGE="$image_path" PORT="$PORT" \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                FLUSH_INTERVAL_SECS="$FLUSH_SECS" RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        raw-file-dio)
            image_path="/tmp/rclone_bench_${name}_$$.raw"
            echo "Formatting ${IMAGE_SIZE_MB} MB image (O_DIRECT): ${image_path}"
            "$RAWOBJST_CLI" format --file "$image_path" --size $((IMAGE_SIZE_MB * 1024 * 1024)) --direct-io
            IMAGE="$image_path" PORT="$PORT" \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                FLUSH_INTERVAL_SECS="$FLUSH_SECS" RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        raw-dev)
            echo "Formatting /dev/sdb (no DIO)"
            "$RAWOBJST_CLI" format --file "$DEV_SDB"
            IMAGE="$DEV_SDB" PORT="$PORT" \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                FLUSH_INTERVAL_SECS="$FLUSH_SECS" RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        raw-dev-dio)
            echo "Formatting /dev/sdb (O_DIRECT)"
            "$RAWOBJST_CLI" format --file "$DEV_SDB" --direct-io
            IMAGE="$DEV_SDB" PORT="$PORT" \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                FLUSH_INTERVAL_SECS="$FLUSH_SECS" RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        raw-loop)
            image_path="/tmp/rclone_bench_${name}_$$.raw"
            echo "Creating ${IMAGE_SIZE_MB} MB backing file for loop device"
            dd if=/dev/zero of="$image_path" bs=1M count="$IMAGE_SIZE_MB" status=progress 2>&1
            loop_dev=$(sudo losetup --find --show "$image_path")
            echo "Loop device: ${loop_dev}"
            "$RAWOBJST_CLI" format --file "$loop_dev"
            IMAGE="$loop_dev" PORT="$PORT" \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                FLUSH_INTERVAL_SECS="$FLUSH_SECS" RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        raw-loop-dio)
            image_path="/tmp/rclone_bench_${name}_$$.raw"
            echo "Creating ${IMAGE_SIZE_MB} MB backing file for loop device (DIO)"
            dd if=/dev/zero of="$image_path" bs=1M count="$IMAGE_SIZE_MB" status=progress 2>&1
            loop_dev=$(sudo losetup --find --show "$image_path")
            echo "Loop device: ${loop_dev}"
            "$RAWOBJST_CLI" format --file "$loop_dev" --direct-io
            IMAGE="$loop_dev" PORT="$PORT" \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                FLUSH_INTERVAL_SECS="$FLUSH_SECS" RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        fs)
            image_path="/tmp/rclone_bench_${name}_$$"
            mkdir -p "$image_path"
            IMAGE="$image_path" PORT="$PORT" BACKEND=fs \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        mem)
            PORT="$PORT" BACKEND=mem \
                ACCESS_KEY="$ACCESS_KEY" SECRET_KEY="$SECRET_KEY" \
                RUST_LOG=warn \
                "$BINARY" > "${RESULTS_BASE}/${name}_server.log" 2>&1 &
            server_pid=$!
            ;;

        *)
            echo "ERROR: unknown backend '${name}'"
            return 1
            ;;
    esac

    # ---- Wait for server ready ----
    echo "Waiting for server (PID ${server_pid})..."
    if ! wait_for_server "$PORT"; then
        echo "Server log:"
        cat "${RESULTS_BASE}/${name}_server.log" 2>/dev/null
        kill "$server_pid" 2>/dev/null || true
        return 1
    fi
    echo "Server ready"

    # ---- Configure rclone and run phases ----
    setup_rclone "$PORT"
    run_phases "$rdir" "$bucket" || true

    # ---- Teardown ----
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
    [ -n "$loop_dev" ] && sudo losetup -d "$loop_dev" 2>/dev/null || true
    [ -n "$image_path" ] && rm -rf "$image_path" 2>/dev/null || true

    echo "Backend ${name} done. Results in ${rdir}/"
}

# ================================================================
# Main loop
# ================================================================

# Kill any stale server on our port
stale=$(lsof -ti :$PORT 2>/dev/null || true)
[ -n "$stale" ] && kill -9 $stale 2>/dev/null || true

FAILED=""
for backend in $BACKENDS; do
    if run_backend "$backend"; then
        echo "  OK: ${backend}"
    else
        echo "  FAILED: ${backend}"
        FAILED="${FAILED} ${backend}"
        # Kill server if still running
        stale=$(lsof -ti :$PORT 2>/dev/null || true)
        [ -n "$stale" ] && kill -9 $stale 2>/dev/null || true
    fi
    sleep 2
done

# ================================================================
# Summary
# ================================================================

echo ""
echo "================================================================"
echo "SUMMARY -- upload MiB/s (256Mi large, 4t, single PUT)"
echo "================================================================"
for backend in $BACKENDS; do
    f="${RESULTS_BASE}/${backend}/p4_large_nomp_t4.txt"
    if [ -f "$f" ]; then
        # rclone outputs bytes per run; last Upload/Download lines are the 256Mi run
        up=$(grep 'Upload' "$f" | tail -1 | awk '{print $NF}' 2>/dev/null || true)
        dn=$(grep 'Download' "$f" | tail -1 | awk '{print $NF}' 2>/dev/null || true)
        printf "  %-18s  upload=%-12s  download=%s\n" "$backend" "${up:-N/A}" "${dn:-N/A}"
    else
        printf "  %-18s  (skipped or failed)\n" "$backend"
    fi
done

echo ""
echo "================================================================"
echo "SUMMARY -- mixed workload (tiny+medium+large concurrent)"
echo "================================================================"
for backend in $BACKENDS; do
    rdir="${RESULTS_BASE}/${backend}"
    for mix_f in "${rdir}/p7_mix_tiny.txt" "${rdir}/p7_mix_med.txt" "${rdir}/p7_mix_large.txt"; do
        label=$(basename "$mix_f" .txt | sed 's/p7_mix_//')
        if [ -f "$mix_f" ]; then
            up_line=$(grep 'Upload' "$mix_f" | tail -1 2>/dev/null || echo "N/A")
            dn_line=$(grep 'Download' "$mix_f" | tail -1 2>/dev/null || echo "N/A")
            printf "  %-18s  %-8s  %s | %s\n" "$backend" "$label" "$up_line" "$dn_line"
        fi
    done
done

echo ""
echo "Full results: ${RESULTS_BASE}/"
if [ -n "$FAILED" ]; then
    echo "FAILED backends:${FAILED}"
fi
echo "================================================================"
