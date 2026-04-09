#!/bin/bash
# bench_catalog.sh -- Benchmark catalog persistence (rebuild, load, save).
#
# Measures catalog rebuild from raw index, JSON/bincode load, clean
# close, and dirty close times for a raw image with >300k objects.
#
# Uses the shardedobjstr bench_catalog example binary for precise timing
# (no HTTP/S3 overhead).
#
# Usage:  ./bench_catalog.sh [IMAGE_PATH] [ITERATIONS]
#         IMAGE_PATH  defaults to /tmp/bench_seed.raw
#         ITERATIONS  defaults to 3
#
# Requires:
#   - Seed image created by ../listing/create_seed.sh
#   - shardedobjstr crate (will be built automatically)
#
# Environment:
#   SEED_PATH    Seed image path  [/tmp/bench_seed.raw]
#   ITERATIONS   Timing runs      [3]

set -euo pipefail

SEED_PATH="${1:-${SEED_PATH:-/tmp/bench_seed.raw}}"
ITERATIONS="${2:-${ITERATIONS:-3}}"
BUILD_DIR="${CARGO_TARGET_DIR:-~/build-sharded}"
CARGO="${CARGO:-~/.cargo/bin/cargo}"

die() { echo "FATAL: $*" >&2; exit 1; }

[ -f "$SEED_PATH" ] || die "seed image not found: $SEED_PATH (run ../listing/create_seed.sh first)"

echo "Building bench_catalog example..."
cd ~/objstr
CARGO_TARGET_DIR="$BUILD_DIR" $CARGO build -p shardedobjstr --release --example bench_catalog 2>&1 | tail -5

BENCH="$BUILD_DIR/release/examples/bench_catalog"
[ -x "$BENCH" ] || die "bench_catalog binary not found: $BENCH"

echo ""
exec "$BENCH" "$SEED_PATH" "$ITERATIONS"
