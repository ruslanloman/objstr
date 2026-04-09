# S3 Listing Benchmark

Compares ListObjectsV2 listing speed across three objstrd backends:
**raw** (block device / image file), **filesystem**, and **in-memory**.

The raw object store can be a practical S3 substitute for local
testing -- especially for large object counts where listings can be
significantly faster than a filesystem backend.

## Test Environment

| Parameter | Value |
|-----------|-------|
| VM | Hyper-V, 8 vCPU, 4 GB RAM, 4 GB swap |
| OS | Ubuntu 24.04 (linux 6.8) |
| Root disk | /dev/sda1 ext4, 97 GB (virtual SCSI) |
| Raw device | /dev/sdb 20 GB (virtual SCSI) |
| objstrd | 0.1.0 (S3 daemon, s3s 0.13, hyper 1) |
| rawobjstr | 0.1.0 (raw object store engine) |

## Quick Start

```bash
# 1. Build objstrd and rawobjstr (on the VM)
cd ~/objstr
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo build -p objstrd --release
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo build -p rawobjstr --release

# 2. Create the seed image (takes a while for 250k objects)
bash objstrd/benchmarks/listing/create_seed.sh

# 3. Run benchmarks per tier
bash objstrd/benchmarks/listing/bench_listing.sh 1k
bash objstrd/benchmarks/listing/bench_listing.sh 10k
bash objstrd/benchmarks/listing/bench_listing.sh 100k
bash objstrd/benchmarks/listing/bench_listing.sh 250k
```

## Prerequisites

- Linux (tested on Ubuntu)
- `objstrd` and `rawobjstr` release binaries (see Quick Start)
- ~4 GB free disk space (seed image + working copies)
- `curl`, `grep` (GNU with `-P`), `awk`

## How It Works

1. **create_seed.sh** -- Formats a raw image (4 GB, 128 MB index) and
   populates 4 buckets over S3:

   | Bucket       | Objects | 
   |--------------|---------|
   | bench-1k     | 1,000   |
   | bench-10k    | 10,000  |
   | bench-100k   | 100,000 |
   | bench-250k   | 250,000 |.

2. **bench_listing.sh TIER** -- For a given tier (1k, 10k, 100k, 250k),
   tests all three backends:

   - **raw**: copies the seed image and opens it directly (no import).
   - **fs**: starts a fresh filesystem backend and imports objects from
     the seed via paginated S3 list + parallel PUT.
   - **mem**: starts a fresh in-memory backend and imports the same way.

   Runs `ITERATIONS` (default 3) full paginated ListObjectsV2 listings
   per backend and reports the median time.

## Environment Variables

| Variable    | Default                                        | Description              |
|-------------|------------------------------------------------|--------------------------|
| OBJSTRD     | ~/build-objstrd/release/objstrd       | Path to objstrd binary   |
| RAWOBJSTR   | ~/build-rawobjstr/release/rawobjstr   | Path to rawobjstr binary |
| SEED_PATH   | /tmp/bench_seed.raw                            | Seed image location      |
| ITERATIONS  | 3                                              | Listing runs per backend |
| PARALLEL    | 64                                             | Import parallelism       |

## Results

All times are median of 3 iterations, full paginated ListObjectsV2 (max-keys=1000).

### 1k objects

| Backend | Objects | List (ms) |
|---------|---------|-----------|
| raw     | 1000    | 149       |
| fs      | 1000    | 105       |
| mem     | 1000    | 35        |

### 10k objects

| Backend | Objects | List (ms) |
|---------|---------|-----------|
| raw     | 10000   | 1686      |
| fs      | 10000   | 2679      |
| mem     | 10000   | 535       |

### 100k objects

| Backend | Objects | List (ms) |
|---------|---------|-----------|
| raw     | 100000  | 26654     |
| fs      | 100000  | 161892    |
| mem     | 100000  | 27805     |

### 250k objects

| Backend | Objects | List (ms) |
|---------|---------|-----------|
| raw     | 250000  | 91805     |
| fs      | 250000  | 972367    |
| mem     | 250000  | 186609    |

## Notes

- The raw backend copies the full seed image (all 4 buckets), so it has
  a larger index than the fs/mem backends which only hold one tier. This
  slightly disadvantages raw in the comparison.
- Listing uses `max-keys=1000` per page (the S3 default).
- Import puts a fresh 1-byte payload for each key, not the original seed
  data. Content is irrelevant for listing benchmarks.
- **Why mem slows down at scale:** every ListObjectsV2 page re-lists all
  objects from the backend, filters by continuation token, sorts, and
  returns 1000. This is O(n) per page, so a full listing is O(n^2) --
  250k objects over 250 pages means 62.5M items processed. The raw
  backend has the same algorithmic cost but a lower per-item constant
  because its index scan is cheaper than iterating the `InMemory`
  BTreeMap through the `object_store` trait. This is not a memory/swap
  issue -- 250k 1-byte objects fit comfortably in RAM.
