#!/bin/bash
# Run all listing benchmark tiers, saving output to a file.
set -euo pipefail
export OBJSTRD=~/build-objstrd/release/objstrd
export RAWOBJSTR=~/build-rawobjstr/release/rawobjstr
export SEED_PATH=/tmp/bench_seed.raw
OUTFILE=/tmp/bench_listing_results.txt

cd ~/objstr

exec > "$OUTFILE" 2>&1

for tier in 1k 10k 100k 250k; do
    echo ""
    echo "============================================"
    echo "  TIER: $tier"
    echo "============================================"
    bash objstrd/benchmarks/listing/bench_listing.sh "$tier"
done

echo ""
echo "ALL DONE"
