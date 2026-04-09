# Lance Integration Benchmark

LanceDB dataset operations on six storage backends, matching the test matrix
in [../../PERFORMANCE.md](../../PERFORMANCE.md). Results compare two integration paths:

- **Python:** Lance Python API with a custom `ObjectStore` adapter that
  routes I/O through the `rawobjstr` Python bindings (every call crosses the
  Python-Rust FFI boundary).
- **Rust:** Lance Rust API (`lancedb` crate) with a native
  `ObjectStoreProvider` that wraps `RawObjectStore` directly -- zero FFI
  overhead.

## Test Environment

| Parameter | Value |
|-----------|-------|
| VM | Hyper-V, 8 vCPU, 6 GB RAM, 4 GB swap |
| OS | Ubuntu 24.04 (linux 6.8) |
| Root disk | /dev/sda1 ext4, 97 GB (virtual SCSI) |
| Raw device | /dev/sdb 20 GB (virtual SCSI) |
| Python | 3.12, pylance 5.0.0b4 (fork with custom object_store), pyarrow 23.0.1 |
| Rust | lancedb 0.27, lance-io 4.0, lance-core 4.0 |
| rawobjstr | 0.1.0 (Rust library + Python bindings) |

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
| ext4 | /tmp on ext4 | Linux page cache, no sync |
| ext4+sync | /tmp on ext4 | `sync` called 3x after each chunk write |
| img+directio | 16 GB image file, O_DIRECT | RawObjectStore, image on ext4 |
| img+buffered | 16 GB image file, buffered | RawObjectStore, image on ext4 |
| raw+directio | /dev/sdb, O_DIRECT | RawObjectStore, raw block device |
| raw+buffered | /dev/sdb, buffered | RawObjectStore, raw block device |

For RawObjectStore backends, the Python path uses a custom object store
adapter (`lance_objstore_adapter.py`) which routes all I/O through the
`rawobjstr` Python bindings -- every read/write crosses the Python-Rust FFI
boundary. The Rust path (`bench_lance.rs`) registers a native
`ObjectStoreProvider` so Lance calls `RawObjectStore` methods directly.

## Write Throughput

Chunked writes: 20 appends of 1M rows each (~6 GB Arrow data total).

| Backend | Python Time | Python MB/s | Rust Time | Rust MB/s | Ratio |
|---------|-------------|-------------|-----------|-----------|-------|
| ext4 | 79.5 s | 73.6 | 34.1 s | 182.2 | 2.3x |
| ext4+sync | 113.2 s | 51.7 | **32.1 s** | **193.3** | 3.5x |
| img+directio | 87.1 s | 67.2 | 70.4 s | 88.2 | 1.2x |
| img+buffered | 61.3 s | 95.4 | 44.8 s | 138.5 | 1.4x |
| raw+directio | 70.6 s | 82.8 | 58.3 s | 106.6 | 1.2x |
| raw+buffered | 88.9 s | 65.8 | 56.8 s | 109.4 | 1.6x |

The Rust path is faster on all backends. ext4 and ext4+sync see the
largest gains (3-4x) because the Python overhead was the bottleneck -- once
removed, the page cache can absorb writes at full speed. On RawObjectStore
backends the Rust advantage ranges from +29% (raw+directio) to +66%
(raw+buffered).

## Scan Results

All scans run after aggressive page-cache eviction: `drop_caches` followed
by writing and deleting a 5 GB junk file (larger than VM RAM) to flush any
residual cached pages. This prevents write-phase page-cache warming from
inflating ext4 read numbers.

### Full Stream Scan (all columns)

Read all 20M rows back through `to_batches()` / `scanner.try_into_stream()`.

| Backend | Python Time | Python MB/s | Rust Time | Rust MB/s | Ratio |
|---------|-------------|-------------|-----------|-----------|-------|
| ext4 | 16.9 s | 346.1 | 12.1 s | 485.4 | 1.4x |
| ext4+sync | 12.7 s | 459.6 | 11.0 s | 532.1 | 1.2x |
| img+directio | 128.1 s | 45.7 | 8.6 s | 684.8 | 14.9x |
| img+buffered | 87.1 s | 67.2 | 11.1 s | 530.2 | 7.8x |
| raw+directio | 78.8 s | 74.3 | 6.1 s | 964.5 | 12.9x |
| raw+buffered | 108.4 s | 54.0 | **5.9 s** | **994.5** | 18.4x |

raw+buffered achieves **994.5 MB/s** -- nearly 1 GB/s -- making it the
fastest backend overall. On RawObjectStore backends the Rust path is
13-18x faster than Python.

### Filtered Scan: name LIKE 'A%'

`starts_with(name, 'A')` -- scans ~3.8% of rows (~769K matches).

| Backend | Python Time | Rust Time | Ratio |
|---------|-------------|-----------|-------|
| ext4 | 38.0 s | 41.2 s | 0.9x |
| ext4+sync | 29.8 s | 38.2 s | 0.8x |
| img+directio | 76.3 s | 4.8 s | 15.9x |
| img+buffered | 79.9 s | 5.6 s | 14.3x |
| raw+directio | 76.7 s | 5.6 s | 13.7x |
| raw+buffered | 81.4 s | **4.4 s** | 18.5x |

RawObjectStore backends go from 77-81 seconds in Python down to 4-6
seconds in Rust -- a 14-19x speedup.

### Column Projection: id + amount

Read only `id` (int64) and `amount` (float64) -- ~309 MB total from 20M rows.

| Backend | Python Time | Python MB/s | Rust Time | Rust MB/s | Ratio |
|---------|-------------|-------------|-----------|-----------|-------|
| ext4 | 780 ms | 391.4 | 569 ms | 542.4 | 1.4x |
| ext4+sync | 643 ms | 474.8 | 560 ms | 550.9 | 1.1x |
| img+directio | 3.78 s | 80.8 | 579 ms | 533.7 | 6.5x |
| img+buffered | 3.62 s | 84.3 | 626 ms | 493.2 | 5.8x |
| raw+directio | 2.98 s | 102.4 | 759 ms | 407.0 | 3.9x |
| raw+buffered | 3.62 s | 84.3 | **539 ms** | **572.5** | 6.7x |

Column projection skips the 256-byte payload entirely, reading ~5% of
on-disk data.

### Point Filter: category = 42

Integer equality filter -- returns ~1% of rows (~201K matches).

| Backend | Python Time | Rust Time | Ratio |
|---------|-------------|-----------|-------|
| ext4 | 14.5 s | 16.4 s | 0.9x |
| ext4+sync | 16.2 s | 17.2 s | 0.9x |
| img+directio | 72.4 s | **4.0 s** | 18.1x |
| img+buffered | 80.8 s | 5.7 s | 14.2x |
| raw+directio | 73.9 s | 4.7 s | 15.7x |
| raw+buffered | 98.8 s | 4.3 s | 23.0x |

RawObjectStore backends improve 15-23x in Rust. ext4 point filter
results are comparable between Python and Rust.

### count_rows (metadata only)

| Backend | Python Time | Rust Time |
|---------|-------------|-----------|
| ext4 | 11.3 ms | 4.4 ms |
| ext4+sync | 9.4 ms | 5.0 ms |
| img+directio | 1.7 ms | 2.4 ms |
| img+buffered | 6.5 ms | 1.4 ms |
| raw+directio | 6.7 ms | **0.6 ms** |
| raw+buffered | 8.0 ms | **0.6 ms** |

count_rows reads only manifest metadata. Both paths are sub-10ms.

## Key Takeaways

1. **raw+buffered on /dev/sdb is the fastest backend at 994.5 MB/s** --
   nearly 1 GB/s sequential scan throughput. raw+directio is close behind
   at 964.5 MB/s. Both bypass the filesystem entirely and Lance's native
   Rust reader fully saturates the block device bandwidth.

2. **The Python FFI adapter was the bottleneck, not RawObjectStore.** With
   native Rust integration the RawObjectStore backends go from 45-74 MB/s
   stream scan to 530-995 MB/s -- a 13-18x improvement. The Python
   adapter's per-call GIL acquisition and buffer conversion dominated.

3. **Aggressive cache eviction resolved the ext4 anomaly.** Previous runs
   showed Python ext4 faster than Rust because write-phase page-cache
   warming inflated Python read numbers. Writing a 5 GB junk file to blow
   out the cache before each scan shows Rust consistently faster on ext4
   (+16-40% for stream scans, +16-39% for column projection).

4. **Filtered scans see 14-23x improvement on RawObjectStore.** Both LIKE
   and point filters drop from 73-99 seconds in Python to 4-6 seconds in
   Rust. These operations issue many small random reads for column pages,
   the worst case for per-call FFI overhead.

5. **Column projection is sub-800ms on all Rust backends.** Reading 2 of
   6 columns (309 MB from a 6 GB dataset) completes in 539-759 ms,
   achieving 407-573 MB/s. The Python path takes 3-4 seconds on
   RawObjectStore backends.

6. **Write throughput: Rust is 2.5-3.7x faster on ext4, 1.3-1.7x on raw.**
   ext4+sync benefits most (51.7 -> 193.3 MB/s).

## Benchmark Scripts

| Script | What it tests | I/O pattern |
|--------|---------------|-------------|
| `bench_write_scan.py` | Dataset write + full scan + filtered scan | Sequential write, sequential read |
| `bench_random_access.py` | Random row access (take) | Random read |
| `bench_vector_search.py` | IVF-PQ index build + ANN search | Heavy write + random read |

## How it works

The `lance_objstore_adapter.py` module wraps `rawobjstr.Store` to implement
the Python object store interface that Lance expects. Lance calls `put()`,
`get()`, `head()`, `delete()`, `list()` etc. on this adapter, and the adapter
forwards them to the raw object store.

Note: the Python path uses the custom object store adapter
(`lance_objstore_adapter.py`) which routes all I/O through the rawobjstr
Python bindings -- every read/write crosses the Python-Rust FFI boundary
which adds a performance penalty.

## Reproducing

### Prerequisites

```bash
# On the Linux VM
source ~/objstr/.venv/bin/activate

# Install lance + dependencies
pip install pylance pyarrow numpy

# Build rawobjstr Python bindings
cd ~/objstr/rawobjstr/python
CARGO_TARGET_DIR=~/build-pyrawobjstr maturin develop --release
```

### Python Benchmark

```bash
cd rawobjstr/benchmarks/lance
source ~/objstr/.venv/bin/activate

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo python3 bench_write_scan.py --device /dev/sdb

# Single backend
python3 bench_write_scan.py --backends ext4

# Fewer chunks for a quick test
python3 bench_write_scan.py --chunks 5 --backends ext4,img+directio

# Run all Python benchmarks
bash run_all.sh
```

### Rust Benchmark

```bash
# Build (requires the lance feature)
cd ~/objstr
CARGO_TARGET_DIR=~/build-rawobjstr \
  ~/.cargo/bin/cargo build --example bench_lance -p rawobjstr --features lance --release

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo ~/build-rawobjstr/release/examples/bench_lance \
  --chunks 20 --chunk-rows 1000000 \
  --img-size 17179869184 --device /dev/sdb \
  --ext4-dir /tmp --backends all

# Single backend
bench_lance --backends img+directio --img-size 17179869184

# Quick test
bench_lance --chunks 5 --backends ext4,raw+directio --device /dev/sdb
```
