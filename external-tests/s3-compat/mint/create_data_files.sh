#!/bin/bash
#
# create_data_files.sh - Generate test data files for mint-style S3 tests
#
# Creates random data files of various sizes in $MINT_DATA_DIR (default: /tmp/mint_data).
# These match the sizes used by the MinIO mint test suite.
# Skips files that already exist with the correct size.
#
# Usage: ./create_data_files.sh [data_dir]
#
set -euo pipefail

MINT_DATA_DIR="${1:-/tmp/mint_data}"

declare -A data_file_map
data_file_map["datafile-0-b"]="0"
data_file_map["datafile-1-b"]="1"
data_file_map["datafile-1-kB"]="1K"
data_file_map["datafile-10-kB"]="10K"
data_file_map["datafile-33-kB"]="33K"
data_file_map["datafile-100-kB"]="100K"
data_file_map["datafile-1-MB"]="1M"
data_file_map["datafile-1.03-MB"]="1056K"
data_file_map["datafile-5-MB"]="5M"
data_file_map["datafile-5243880-b"]="5243880"
data_file_map["datafile-6-MB"]="6M"
data_file_map["datafile-10-MB"]="10M"
data_file_map["datafile-11-MB"]="11M"
data_file_map["datafile-65-MB"]="65M"
data_file_map["datafile-129-MB"]="129M"

mkdir -p "$MINT_DATA_DIR"

for filename in "${!data_file_map[@]}"; do
    size="${data_file_map[$filename]}"
    filepath="$MINT_DATA_DIR/$filename"

    if [ -f "$filepath" ]; then
        echo "  exists: $filename"
        continue
    fi

    echo "creating $filename ($size)"
    if [ "$size" = "0" ]; then
        touch "$filepath"
    elif echo "$size" | grep -qE '^[0-9]+$'; then
        # Pure byte count (no suffix)
        dd if=/dev/urandom of="$filepath" bs=1 count="$size" 2>/dev/null
    elif echo "$size" | grep -qE '^[0-9]+K$'; then
        count="${size%K}"
        dd if=/dev/urandom of="$filepath" bs=1024 count="$count" 2>/dev/null
    elif echo "$size" | grep -qE '^[0-9]+M$'; then
        count="${size%M}"
        dd if=/dev/urandom of="$filepath" bs=1048576 count="$count" 2>/dev/null
    else
        echo "ERROR: unknown size format: $size" >&2
        exit 1
    fi
done

echo "Data files ready in $MINT_DATA_DIR"
ls -lhS "$MINT_DATA_DIR"
