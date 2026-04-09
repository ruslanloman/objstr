# Catalog Persistence Benchmark

Measures catalog rebuild, load, and save times for raw object store images
containing hundreds of thousands of objects.

## Test Environment

| Parameter | Value |
|-----------|-------|
| VM | Hyper-V, 8 vCPU, 4 GB RAM, 4 GB swap |
| OS | Ubuntu 24.04 (linux 6.8) |
| Root disk | /dev/sda1 ext4, 97 GB (virtual SCSI) |
| objstrd | 0.1.0 (S3 daemon, s3s 0.13, hyper 1) |
| rawobjstr | 0.1.0 (raw object store engine) |
| shardedobjstr | 0.1.0 (sharded object store with catalog persistence) |

## Quick Start

```bash
# 1. Create the seed image (if not already done)
bash objstrd/benchmarks/listing/create_seed.sh

# 2. Run the benchmark
bash objstrd/benchmarks/catalog/bench_catalog.sh
```

## What It Measures

| Phase | Description |
|-------|-------------|
| Rebuild from index | Scan the raw store index and build the in-memory catalog |
| JSON load | Load catalog from a JSON file (with CRC32c validation) |
| JSON clean close | `save_if_dirty` when no mutations -- should be ~0 ms |
| JSON dirty close | `save_if_dirty` after one mutation -- full re-serialize + write |
| Bincode load | Load catalog from a bincode file (with CRC32c validation) |
| Bincode clean close | `save_if_dirty` when no mutations -- should be ~0 ms |
| Bincode dirty close | `save_if_dirty` after one mutation -- full re-serialize + write |

Each phase also includes opening the raw store (read superblock +
validate index), so the timings include that fixed overhead. The relative
differences show the pure catalog I/O cost.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SEED_PATH` | `/tmp/bench_seed.raw` | Path to the seed raw image |
| `ITERATIONS` | `3` | Number of timing iterations per phase |

## Results

All times are median of 3 iterations. 361,000 objects across 4 buckets in
a single raw image (the same seed used by the listing benchmark).

### Summary

| Mode | File Size | Load (ms) | Clean Close (ms) | Dirty Close (ms) |
|------|-----------|-----------|-------------------|-------------------|
| rebuild (index) | -- | 350 | -- | -- |
| json | 41.0 MB | 2,216 | 0 | 2,684 |
| bincode | 32.7 MB | 250 | 0 | 321 |

### Detailed Phase Results

#### Phase 1: Rebuild from index (no persistence)

| Run | Time (ms) |
|-----|-----------|
| 1 | 379 |
| 2 | 339 |
| 3 | 350 |
| **median** | **350** |

#### Phase 2-3: JSON load

| Run | Time (ms) |
|-----|-----------|
| 1 | 2,432 |
| 2 | 2,216 |
| 3 | 1,890 |
| **median** | **2,216** |

Initial JSON save: 2,764 ms (41.0 MB file).

#### Phase 5: JSON dirty close

| Run | Time (ms) |
|-----|-----------|
| 1 | 2,684 |
| 2 | 2,365 |
| 3 | 2,776 |
| **median** | **2,684** |

#### Phase 6-7: Bincode load

| Run | Time (ms) |
|-----|-----------|
| 1 | 280 |
| 2 | 248 |
| 3 | 250 |
| **median** | **250** |

Initial bincode save: 298 ms (32.7 MB file).

#### Phase 9: Bincode dirty close

| Run | Time (ms) |
|-----|-----------|
| 1 | 368 |
| 2 | 315 |
| 3 | 321 |
| **median** | **321** |

## Notes

- **Bincode is ~9x faster** than JSON for load (250 ms vs 2,216 ms)
  and ~8x faster for save (321 ms vs 2,684 ms).
- **Rebuilding from the raw index** (350 ms) is comparable to bincode
  load (250 ms). The index scan is fast because it reads a contiguous
  memory-mapped region on a local raw store. For filesystem or S3 shards,
  rebuild requires listing every object over the network or filesystem,
  which is orders of magnitude slower (see listing benchmark results). This
  is where catalog persistence provides the biggest win by avoiding a
  full shard rescan on startup.
- **Clean close is 0 ms** for both formats. The dirty tracking flag
  skips the write entirely when no mutations have occurred.
- **JSON is 21% larger** on disk (41 MB vs 33 MB) due to key names and
  formatting overhead.
- For clusters with >100k objects, bincode is strongly recommended.
  JSON is useful for debugging and manual inspection of small catalogs.
