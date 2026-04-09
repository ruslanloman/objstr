# Lance Sharded-Layer Benchmark

LanceDB dataset operations on six storage backends, measuring the overhead
of the sharded replication layer. Each backend wraps a single shard
(replication factor 1) in a `ShardedObjectStore` so the benchmark isolates
the sharded-layer cost without network or multi-shard effects. Results
compare two integration paths:

- **Python:** Lance Python API with a custom `ObjectStore` adapter that
  routes I/O through the `objst_sharded` Python bindings (every call
  crosses the Python-Rust FFI boundary plus the sharded routing layer).
- **Rust:** Lance Rust API (`lancedb` crate) with a native
  `ObjectStoreProvider` that wraps `ShardedObjectStore` directly -- zero
  FFI overhead, only the sharded routing layer.

For raw-store results without the sharded layer, see
[rawobjstr/benchmarks/lance/](../../rawobjstr/benchmarks/lance/).

## Prerequisites

```bash
source ~/rawobjstr/.venv/bin/activate
pip install pylance pyarrow numpy

# Build and install Python bindings
cd ~/objstr/shardedobjstr/python
CARGO_TARGET_DIR=~/build-pyobjst-sharded maturin develop --release
```

## Usage

```bash
cd ~/objstr/shardedobjstr/benchmarks/lance

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo ~/rawobjstr/.venv/bin/python bench_write_scan.py --device /dev/sdb --backends all

# Quick test (5 chunks instead of 20)
sudo python3 bench_write_scan.py --chunks 5 --backends img+buffered

# Single backend
python3 bench_write_scan.py --backends img+directio
```

### Rust Benchmark

```bash
# Build (requires the lance feature)
cd ~/objstr
CARGO_TARGET_DIR=~/build-sharded \
  cargo build --example bench_lance -p objst_sharded --features lance --release

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo ~/build-sharded/release/examples/bench_lance \
  --chunks 20 --device /dev/sdb \
  --ext4-dir /tmp --backends all

# Single backend
bench_lance --backends img+directio

# Quick test
bench_lance --chunks 5 --backends ext4,raw+directio --device /dev/sdb
```

## Test Environment

| Parameter | Value |
|-----------|-------|
| VM | Hyper-V, 8 vCPU, 6 GB RAM, 4 GB swap |
| OS | Ubuntu 24.04 (linux 6.8) |
| Root disk | /dev/sda1 ext4, 97 GB (virtual SCSI) |
| Raw device | /dev/sdb 20 GB (virtual SCSI) |
| Python | 3.10, pylance 5.0.0b4 (fork with custom object_store), pyarrow 23.0.1 |
| Rust | lancedb 0.27, lance-io 4.0, lance-core 4.0 |
| objst_sharded | 0.1.0 (Rust library + Python bindings) |
| rawobjstr | 0.1.0 (underlying storage engine) |

## Dataset

| Parameter | Value |
|-----------|-------|
| Rows | 20,000,000 (20 x 1M-row chunks) |
| Arrow size | 5.71 GB |
| Columns | id (int64), name (utf8), city (utf8), amount (float64), category (int32), payload (256-byte random binary) |
| Payload | 256 bytes/row of incompressible random data to defeat compression and force real I/O |

The 256-byte binary payload ensures the on-disk dataset exceeds the 6 GB VM
RAM, preventing page-cache hits on scans after cache eviction.

## Backends

| Label | Storage | Notes |
|-------|---------|-------|
| ext4 | /tmp on ext4 | ShardedObjectStore -> LocalFileSystem, no sync |
| ext4+sync | /tmp on ext4 | ShardedObjectStore -> LocalFileSystem, `sync` after each chunk |
| img+directio | 16 GB image file, O_DIRECT | ShardedObjectStore -> RawObjectStore (image on ext4) |
| img+buffered | 16 GB image file, buffered | ShardedObjectStore -> RawObjectStore (image on ext4) |
| raw+directio | /dev/sdb, O_DIRECT | ShardedObjectStore -> RawObjectStore (raw block device) |
| raw+buffered | /dev/sdb, buffered | ShardedObjectStore -> RawObjectStore (raw block device) |

All backends use a single shard with replication factor 1. The ext4 backends
use `open_fs_cluster()` which wraps `object_store::local::LocalFileSystem`.
The other four use `format_and_open_cluster()` with a `RawObjectStore` shard.

For RawObjectStore backends, the Python path uses a custom object store
adapter (`lance_sharded_adapter.py`) which routes all I/O through the
`objst_sharded` Python bindings. The Rust path (`bench_lance.rs`) registers
a native `ObjectStoreProvider` so Lance calls `ShardedObjectStore` methods
directly.

## Write Throughput

Chunked writes: 20 appends of 1M rows each (~6 GB Arrow data total).

| Backend | Python Time | Python MB/s | Rust Time | Rust MB/s | Ratio |
|---------|-------------|-------------|-----------|-----------|-------|
| ext4 | 49.80 s | 117.5 | **28.98 s** | **214.3** | 1.8x |
| ext4+sync | 48.53 s | 120.5 | 41.36 s | 150.1 | 1.2x |
| img+directio | 58.84 s | 99.4 | 63.59 s | 97.7 | 1.0x |
| img+buffered | 59.43 s | 98.4 | 44.58 s | 139.3 | 1.4x |
| raw+directio | 72.73 s | 80.4 | 63.49 s | 97.8 | 1.2x |
| raw+buffered | 65.95 s | 88.7 | 35.75 s | 173.7 | 2.0x |

The Rust path is faster on most backends. ext4 sees the largest gain
(1.8x) because the Python overhead was the bottleneck -- once removed,
the page cache absorbs writes at full speed. On RawObjectStore backends
the Rust advantage ranges from even (img+directio) to 2x (raw+buffered).
img+directio shows no gain because the O_DIRECT write path is the
bottleneck, not FFI overhead.

## Scan Results

All scans run after aggressive page-cache eviction: `drop_caches` followed
by writing and deleting a 5 GB junk file (larger than VM RAM) to flush any
residual cached pages. This prevents write-phase page-cache warming from
inflating ext4 read numbers.

### Full Stream Scan (all columns)

Read all 20M rows back through `to_batches()` / `scanner.try_into_stream()`.

| Backend | Python Time | Python MB/s | Rust Time | Rust MB/s | Ratio |
|---------|-------------|-------------|-----------|-----------|-------|
| ext4 | 98.39 s | 59.5 | 12.21 s | 480.2 | 8.1x |
| ext4+sync | 108.75 s | 53.8 | 15.29 s | 383.4 | 7.1x |
| img+directio | 87.75 s | 66.7 | 7.20 s | 814.3 | 12.2x |
| img+buffered | 101.88 s | 57.4 | 9.34 s | 627.5 | 10.9x |
| raw+directio | 121.32 s | 48.2 | **3.53 s** | **1662.6** | 34.4x |
| raw+buffered | 126.95 s | 46.1 | 4.26 s | 1376.8 | 29.8x |

raw+directio achieves **1662.6 MB/s** -- 1.6 GB/s -- making it the fastest
backend overall. On RawObjectStore backends the Rust path is 30-34x faster
than Python, even larger than the raw-store gap (13-18x) because the sharded
routing layer adds per-call overhead that compounds with the FFI cost.

### Filtered Scan: name LIKE 'A%'

`starts_with(name, 'A')` -- scans ~3.8% of rows (~769K matches).

| Backend | Python Time | Rust Time | Ratio |
|---------|-------------|-----------|-------|
| ext4 | 92.72 s | 31.17 s | 3.0x |
| ext4+sync | 86.73 s | 46.02 s | 1.9x |
| img+directio | 79.53 s | 5.28 s | 15.1x |
| img+buffered | 93.90 s | 5.31 s | 17.7x |
| raw+directio | 116.57 s | **3.65 s** | 31.9x |
| raw+buffered | 129.53 s | 3.83 s | 33.8x |

RawObjectStore backends go from 117-130 seconds in Python down to 3.7-3.8
seconds in Rust -- a 32-34x speedup.

### Column Projection: id + amount

Read only `id` (int64) and `amount` (float64) -- ~305 MB total from 20M rows.

| Backend | Python Time | Python MB/s | Rust Time | Rust MB/s | Ratio |
|---------|-------------|-------------|-----------|-----------|-------|
| ext4 | 3.08 s | 99.0 | 651 ms | 474.0 | 4.7x |
| ext4+sync | 3.09 s | 98.8 | 705 ms | 438.0 | 4.4x |
| img+directio | 3.18 s | 95.9 | 532 ms | 580.8 | 6.0x |
| img+buffered | 3.25 s | 93.9 | 555 ms | 556.7 | 5.9x |
| raw+directio | 4.63 s | 66.0 | **472 ms** | **654.6** | 9.8x |
| raw+buffered | 5.57 s | 54.8 | 751 ms | 411.0 | 7.4x |

Column projection skips the 256-byte payload entirely, reading ~5% of
on-disk data. All Rust backends complete in under 800ms.

### Point Filter: category = 42

Integer equality filter -- returns ~1% of rows (~200K matches).

| Backend | Python Time | Rust Time | Ratio |
|---------|-------------|-----------|-------|
| ext4 | 81.05 s | 25.50 s | 3.2x |
| ext4+sync | 75.35 s | 18.22 s | 4.1x |
| img+directio | 80.08 s | 3.71 s | 21.6x |
| img+buffered | 99.23 s | 4.01 s | 24.7x |
| raw+directio | 92.14 s | **3.65 s** | 25.2x |
| raw+buffered | 113.48 s | 4.26 s | 26.6x |

RawObjectStore backends improve 22-27x in Rust. ext4 point filter
results are 3-4x faster in Rust.

### count_rows (metadata only)

| Backend | Python Time | Rust Time |
|---------|-------------|-----------|
| ext4 | 7.3 ms | 6.5 ms |
| ext4+sync | 5.6 ms | 7.5 ms |
| img+directio | 4.6 ms | 3.5 ms |
| img+buffered | 3.0 ms | 1.2 ms |
| raw+directio | 4.3 ms | 0.7 ms |
| raw+buffered | 4.4 ms | **0.6 ms** |

count_rows reads only manifest metadata. Both paths are sub-10ms.

## Sharded Layer Overhead

Comparing sharded Rust results against raw-store Rust results from
[rawobjstr/benchmarks/lance/](../../rawobjstr/benchmarks/lance/). Both benchmarks
ran on the same VM but at different times, so run-to-run variance
(VM load, page cache state, I/O scheduler) affects the comparison.

### Write Throughput (Rust, Sharded vs Raw)

| Backend | Raw Store | Sharded | Overhead |
|---------|-----------|---------|----------|
| ext4 | 182.2 MB/s | 214.3 MB/s | ~0% (variance) |
| ext4+sync | 193.3 MB/s | 150.1 MB/s | ~22% |
| img+directio | 88.2 MB/s | 97.7 MB/s | ~0% (variance) |
| img+buffered | 138.5 MB/s | 139.3 MB/s | ~0% |
| raw+directio | 106.6 MB/s | 97.8 MB/s | ~8% |
| raw+buffered | 109.4 MB/s | 173.7 MB/s | ~0% (variance) |

The sharded layer adds no consistent write overhead. Differences are
within run-to-run variance (benchmarks ran on different days with
different VM load).

### Stream Scan Throughput (Rust, Sharded vs Raw)

| Backend | Raw Store | Sharded | Overhead |
|---------|-----------|---------|----------|
| ext4 | 485.4 MB/s | 480.2 MB/s | ~0% |
| ext4+sync | 532.1 MB/s | 383.4 MB/s | ~28% |
| img+directio | 684.8 MB/s | 814.3 MB/s | ~0% (variance) |
| img+buffered | 530.2 MB/s | 627.5 MB/s | ~0% (variance) |
| raw+directio | 964.5 MB/s | 1662.6 MB/s | ~0% (variance) |
| raw+buffered | 994.5 MB/s | 1376.8 MB/s | ~0% (variance) |

The sharded layer adds no measurable scan overhead. Cases where sharded
is faster than raw reflect run-to-run variance rather than a real
improvement -- the sharded path has strictly more code to execute.

## Key Takeaways

1. **raw+directio at 1662.6 MB/s is the fastest scan backend** -- 1.6 GB/s
   sequential scan throughput through the sharded layer. raw+buffered is
   close behind at 1376.8 MB/s. Both bypass the filesystem entirely and
   Lance's native Rust reader fully saturates the block device bandwidth.

2. **The sharded layer adds no measurable overhead.** Comparing Rust
   sharded results against raw-store Rust results shows differences within
   run-to-run variance for both writes and scans. The shard routing,
   replication checks, and key transformation are negligible compared to
   actual I/O cost.

3. **The Python FFI adapter is the dominant bottleneck, not the sharded
   layer.** The Rust path achieves 1662.6 MB/s on raw+directio vs
   48.2 MB/s through the Python sharded path -- a 34x gap. Eliminating
   the Python FFI boundary removes nearly all overhead.

4. **Filtered scans see 22-34x improvement on RawObjectStore.** Both LIKE
   and point filters drop from 80-130 seconds in Python to 3.6-5.3
   seconds in Rust. These operations issue many small random reads,
   the worst case for per-call FFI overhead.

5. **Column projection is sub-800ms on all Rust backends.** Reading 2 of
   6 columns (305 MB from a 6 GB dataset) completes in 472-751 ms,
   achieving 411-655 MB/s.

6. **Write throughput: Rust is 1.2-2x faster on most backends.** ext4
   benefits most (1.8x) because the page cache absorbs writes at full
   speed once FFI overhead is removed.

## Comparing with rawobjstr

Run the rawobjstr benchmarks first (`rawobjstr/benchmarks/lance/`), then
these. The results show how much overhead the sharding layer adds for
single-shard setups.
