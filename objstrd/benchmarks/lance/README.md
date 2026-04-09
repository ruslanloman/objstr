# Lance S3 HTTP Layer Benchmark

LanceDB dataset operations on six storage backends, measuring the overhead
of the S3 HTTP protocol layer. Each backend runs an `objstrd` instance
serving an S3-compatible endpoint, and LanceDB connects to it via standard
S3 storage options. This isolates the cost of the HTTP stack (s3s framework,
XML codec, hyper server, TCP loopback) on top of the sharded object store.

For sharded-layer results without the HTTP layer, see
[shardedobjstr/benchmarks/lance/](../../../shardedobjstr/benchmarks/lance/).
For raw-store results without either layer, see
[rawobjstr/benchmarks/lance/](../../../rawobjstr/benchmarks/lance/).

## Client-Side Tuning

The following client-side optimizations are applied to reduce HTTP overhead.
These are standard `object_store` S3 storage options passed through LanceDB:

| Setting | Value | Effect |
|---------|-------|--------|
| `skip_signature` | `true` | Skip SigV4 auth entirely (objstrd started without `--access-key`/`--secret-key`). Removes per-request HMAC-SHA256 computation and canonical request serialization. |
| `unsigned_payload` | `true` | Skip payload checksum in signature. Combined with `skip_signature`, eliminates all auth overhead. |
| `pool_max_idle_per_host` | `32` | Keep 32 idle HTTP connections to objstrd. Default is ~10. Allows more concurrent range reads without connection setup overhead. |
| `LANCE_IO_THREADS` | `32` | Lance I/O thread pool size (env var). Default is 8. More threads issue more concurrent S3 GetObject range reads during scans. |

These settings are realistic for local/trusted deployments where objstrd
runs on the same host or trusted network. For production with untrusted
clients, re-enable SigV4 auth (adds ~10-15% overhead to writes, ~3-7x to
scans due to per-request signing cost).

## Test Environment

| Parameter | Value |
|-----------|-------|
| VM | Hyper-V, 8 vCPU, 6 GB RAM, 4 GB swap |
| OS | Ubuntu 24.04 (linux 6.8) |
| Root disk | /dev/sda1 ext4, 97 GB (virtual SCSI) |
| Raw device | /dev/sdb 20 GB (virtual SCSI) |
| Python | 3.12, pylance, pyarrow |
| Rust | lancedb 0.27, lance-io 4.0, lance-core 4.0 |
| objstrd | 0.1.0 (S3 daemon, s3s 0.13, hyper 1) |

## Dataset

| Parameter | Disk Backends | mem Backend |
|-----------|---------------|-------------|
| Rows | 10,000,000 (10 x 1M-row chunks) | 5,000,000 (5 x 1M-row chunks) |
| Arrow size | ~3.03 GB | ~1.52 GB |
| Columns | id (int64), name (utf8), city (utf8), amount (float64), category (int32), payload (256-byte random binary) | same |
| Payload | 256 bytes/row of incompressible random data to defeat compression and force real I/O | same |

The mem backend uses fewer chunks to fit in RAM. All disk backends
use 10 chunks. Page cache is evicted before scans on disk backends.

## Backends

Each backend starts a dedicated `objstrd` process without auth
(`--access-key`/`--secret-key` omitted). LanceDB connects via
`s3://testbucket/lance-bench` with `skip_signature=true`. The "fs"
backend is equivalent to the "ext4" backend in the rawobjstr and
shardedobjstr benchmarks.

| Label | objstrd Command | Notes |
|-------|-----------------|-------|
| fs | `objstrd --backend fs --image /tmp/lance_s3_fs --port 8900` | Filesystem via S3 (equiv to "ext4" in other benches) |
| mem | `objstrd --backend mem --port 8900` | In-memory store, isolates pure HTTP overhead |
| img+directio | `objstrd --image /tmp/lance_s3_dio.img --size-mb 16384 --direct-io --port 8900` | O_DIRECT image file via S3 |
| img+buffered | `objstrd --image /tmp/lance_s3_buf.img --size-mb 16384 --port 8900` | Buffered image file via S3 |
| raw+directio | `objstrd --image /dev/sdb --direct-io --port 8900` | Raw block device via S3, O_DIRECT |
| raw+buffered | `objstrd --image /dev/sdb --port 8900` | Raw block device via S3, buffered |

All backends use objstrd in standalone mode with a single shard (default
replication factor 1). The benchmark script starts objstrd, waits for the
`/_admin/info` endpoint to respond, runs the workload, then kills the
process and cleans up.

## Write Throughput

Chunked writes: 10 appends of 1M rows each (~3 GB) for disk backends,
5 appends (~1.5 GB) for mem.

| Backend | Rust Time | Rust MB/s | Data |
|---------|-----------|-----------|------|
| fs | 39.10 s | 79.4 | 3.03 GB |
| mem | 12.75 s | 121.7 | 1.52 GB |
| img+directio | 40.81 s | 76.1 | 3.03 GB |
| **img+buffered** | **26.98 s** | **115.1** | 3.03 GB |
| raw+directio | 32.15 s | 96.6 | 3.03 GB |
| raw+buffered | 31.58 s | 98.3 | 3.03 GB |

## Scan Results

All scans run after page-cache eviction (`drop_caches`). The mem backend
is not affected since all data is in the objstrd process heap.

### Full Stream Scan (all columns)

Read all rows back (10M for disk, 5M for mem).

| Backend | Rust Time | Rust MB/s |
|---------|-----------|----------|
| fs | 6.30 s | 465.5 |
| mem | 1.53 s | 957.3 |
| img+directio | 8.48 s | 345.6 |
| **img+buffered** | **3.88 s** | **755.5** |
| raw+directio | 4.69 s | 625.0 |
| raw+buffered | 4.64 s | 631.8 |

### Filtered Scan: name LIKE 'A%'

`starts_with(name, 'A')` -- scans ~3.8% of rows.

| Backend | Rust Time | Matching Rows |
|---------|-----------|---------------|
| fs | 5.57 s | 384,395 |
| mem | 815.2 ms | 192,095 |
| **img+directio** | **3.02 s** | 384,395 |
| img+buffered | 3.27 s | 384,395 |
| raw+directio | 4.17 s | 384,395 |
| raw+buffered | 5.25 s | 384,395 |

### Column Projection: id + amount

Read only `id` (int64) and `amount` (float64) -- ~154 MB from 10M rows
(~77 MB from 5M rows for mem).

| Backend | Rust Time | Rust MB/s |
|---------|-----------|----------|
| fs | 463.8 ms | 332.8 |
| mem | 218.2 ms | 353.7 |
| **img+directio** | **339.0 ms** | **455.3** |
| img+buffered | 384.0 ms | 402.0 |
| raw+directio | 458.5 ms | 336.7 |
| raw+buffered | 367.2 ms | 420.4 |

### Point Filter: category = 42

Integer equality filter -- returns ~1% of rows.

| Backend | Rust Time | Matching Rows |
|---------|-----------|---------------|
| fs | 7.23 s | 100,650 |
| mem | 610.9 ms | 50,213 |
| **img+directio** | **2.67 s** | 100,650 |
| img+buffered | 2.74 s | 100,650 |
| raw+directio | 2.83 s | 100,650 |
| raw+buffered | 2.76 s | 100,650 |

### count_rows (metadata only)

| Backend | Rust Time |
|---------|-----------|
| fs | 55.4 ms |
| mem | 3.4 ms |
| img+directio | 7.9 ms |
| **img+buffered** | **5.4 ms** |
| raw+directio | 7.1 ms |
| raw+buffered | 8.1 ms |

## S3 HTTP Layer Overhead

Comparing Rust results through the S3 HTTP layer (this benchmark) against
direct Rust results from
[shardedobjstr/benchmarks/lance/](../../../shardedobjstr/benchmarks/lance/).  The
sharded benchmark accesses `ShardedObjectStore` directly in-process. This
benchmark goes through the full HTTP stack: LanceDB S3 client -> HTTP
request -> s3s XML parsing -> objstrd adapter -> ShardedObjectStore
-> backend I/O -> response serialization -> HTTP response.

The S3 HTTP results use client-side tuning (skip_signature, 32 connections,
32 I/O threads). See "Client-Side Tuning" section above.

### Write Throughput (Rust, S3 HTTP vs Direct Sharded)

| Backend | Direct Sharded | S3 HTTP | Ratio |
|---------|----------------|---------|-------|
| fs (ext4) | 214.3 MB/s | 79.4 MB/s | 2.7x slower |
| img+directio | 97.7 MB/s | 76.1 MB/s | 1.3x slower |
| img+buffered | 139.3 MB/s | 115.1 MB/s | 1.2x slower |
| raw+directio | 97.8 MB/s | 96.6 MB/s | 1.0x (parity) |
| raw+buffered | 173.7 MB/s | 98.3 MB/s | 1.8x slower |

### Stream Scan Throughput (Rust, S3 HTTP vs Direct Sharded)

| Backend | Direct Sharded | S3 HTTP | Ratio |
|---------|----------------|---------|-------|
| fs (ext4) | 480.2 MB/s | 465.5 MB/s | 1.0x (parity) |
| img+directio | 814.3 MB/s | 345.6 MB/s | 2.4x slower |
| img+buffered | 627.5 MB/s | 755.5 MB/s | 1.2x faster |
| raw+directio | 1662.6 MB/s | 625.0 MB/s | 2.7x slower |
| raw+buffered | 1376.8 MB/s | 631.8 MB/s | 2.2x slower |

## Key Takeaways

1. **Client-side tuning dramatically reduces HTTP overhead.** Skipping
   SigV4 auth, increasing the connection pool to 32, and using 32 I/O
   threads improved stream scan throughput by 2-7x compared to default
   settings. img+buffered went from 105.9 to 755.5 MB/s -- actually
   exceeding direct sharded access (627.5 MB/s) due to kernel read-ahead
   benefiting the concurrent HTTP read pattern.

2. **Write overhead is now 1.0-2.7x** (was 1.4-3.0x before tuning).
   raw+directio reached near-parity (96.6 vs 97.8 MB/s direct). The
   remaining overhead on fs (2.7x) comes from the filesystem backing
   being much faster when accessed directly.

3. **Stream scan overhead is 1.0-2.7x** (was 3.6-9.2x before tuning).
   fs scans reached parity (465.5 vs 480.2 MB/s). img+buffered actually
   beat direct access. The biggest remaining gap is raw+directio (2.7x)
   where the direct path achieves 1.6 GB/s via zero-copy DMA.

4. **The mem backend sets the HTTP ceiling.** 121.7 MB/s writes and
   957.3 MB/s stream scans represent the maximum throughput through the
   S3 HTTP stack with zero storage I/O cost.

5. **img+buffered is the overall best performer** for scans at 755.5 MB/s
   stream throughput. The kernel's read-ahead and page cache work well
   with the concurrent HTTP range read pattern. img+directio is slower
   (345.6 MB/s) because O_DIRECT bypasses read-ahead.

6. **Connection pool and I/O threads matter more than auth.** The jump
   from default (~10 connections, 8 threads) to 32/32 enables many more
   concurrent range reads, which is how Lance reads column chunks. This
   was the single biggest factor in scan improvement.

7. **count_rows remains sub-10ms** on rawobjstr backends (3.4-8.1 ms)
   but 55 ms on fs due to filesystem stat overhead.

## Reproducing

### Python Benchmark

```bash
cd objstrd/benchmarks/lance
source /path/to/.venv/bin/activate

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo python3 bench_write_scan.py --device /dev/sdb --backends all

# Single backend
python3 bench_write_scan.py --backends fs

# In-memory only (no root needed, fast smoke test)
python3 bench_write_scan.py --backends mem --chunks 5

# Image-only (no root needed for device)
python3 bench_write_scan.py --backends img+directio,img+buffered
```

### Rust Benchmark

```bash
# Build (requires the lance feature)
cd /path/to/objstr
CARGO_TARGET_DIR=~/build-objstrd \
  ~/.cargo/bin/cargo build --example bench_lance -p objstrd \
  --features lance --release

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo ~/build-objstrd/release/examples/bench_lance \
  --chunks 20 --device /dev/sdb --backends all

# Single backend
bench_lance --backends fs

# Quick test
bench_lance --chunks 5 --backends mem
```
