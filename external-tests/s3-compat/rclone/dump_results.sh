#!/bin/bash
for b in mem fs raw-file; do
    echo
    echo "=== $b ==="
    for pf in /tmp/rclone_bench/$b/p*.txt; do
        [ -f "$pf" ] || continue
        phase=$(basename "$pf" .txt)
        echo "-- $phase --"
        grep -E 'Upload|Download' "$pf" | tail -6
    done
done
