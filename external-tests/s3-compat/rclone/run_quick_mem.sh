#!/bin/bash
# Quick wrapper - runs bench with mem+fs+raw-file backends only (~5 min)
# Useful for a fast smoke-check of relative performance.
#
# Usage: bash run_quick_mem.sh
#
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
pkill -f objstrd 2>/dev/null || true
sleep 1
bash "$SCRIPT_DIR/run_bench.sh" --quick --backends mem,fs,raw-file 2>&1 | tee /tmp/bench_multi.log
echo "exit=$?" >> /tmp/bench_multi.log
