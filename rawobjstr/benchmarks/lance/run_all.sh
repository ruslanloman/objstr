#!/bin/bash
# Run all Lance benchmarks on RawObjectStore.
#
# Usage:
#   bash run_all.sh              # image-file backends only
#   bash run_all.sh /dev/sdb     # also test raw block device (needs sudo)
#
# Prerequisites:
#   source ~/rawobjstr/.venv/bin/activate
#   pip install pylance pyarrow numpy
#   cd ~/rawobjstr/rawobjstr/python
#   CARGO_TARGET_DIR=~/build-pyrawobjstr maturin develop --release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DEVICE_FLAG=""
if [ "${1:-}" != "" ]; then
    DEVICE_FLAG="--device $1"
    echo "=== Including raw device: $1 ==="
fi

echo ""
echo "=========================================="
echo "  Lance Write + Scan Benchmark"
echo "=========================================="
python bench_write_scan.py $DEVICE_FLAG

echo ""
echo "=========================================="
echo "  Lance Random Access Benchmark"
echo "=========================================="
python bench_random_access.py $DEVICE_FLAG

echo ""
echo "=========================================="
echo "  Lance Vector Index Benchmark"
echo "=========================================="
python bench_vector_search.py $DEVICE_FLAG

echo ""
echo "=========================================="
echo "  All benchmarks complete."
echo "=========================================="
