# RawObjectStore Performance Benchmarks

Benchmarks comparing six storage backends using the `bench_storage` example binary.

> **Caveat:** These benchmarks were run inside a Linux VM on a laptop
> (Hyper-V on Windows 10), not on dedicated server hardware. The virtual disk
> sits on a laptop NVMe behind the hypervisor's I/O stack and virtual SCSI driver, so 
> throughput numbers are lower than what woud be expected on real 
> hardware would achieve and do not belive that the O_DIRECT is being honnered by the host machine.

## Test Environment

| Component | Detail |
|-----------|--------|
| **Hypervisor** | Hyper-V on Windows 11 |
| **vCPUs** | 8 |
| **RAM** | 3.1 GB (intentionally small to ensure reads hit disk, not page cache) |
| **Swap** | 4 GB |
| **Root disk** | 97 GB virtual SCSI, ext4 (`/dev/sda1`) |
| **Test disk** | 20 GB virtual SCSI (`/dev/sdb`), no filesystem, raw block |
| **OS** | Ubuntu (kernel 5.x / 6.x) |

The 3.1 GB RAM is key: with a ~15 GB dataset the kernel page cache is
completely blown out during cold-cache read phases, so the numbers reflect
real I/O to the virtual disk rather than memory-speed cache hits.

## Backends

| Backend | Description |
|---------|-------------|
| **ext4** | Linux ext4 filesystem via `LocalFileSystem` (writes go to page cache) |
| **ext4+sync** | Same as ext4 but calls `sync;sync;sync` at flush intervals (forces writes to disk) |
| **img+directio** | `RawObjectStore` on a 20 GB loopback image file, O_DIRECT enabled |
| **img+buffered** | `RawObjectStore` on a 20 GB loopback image file, buffered I/O (no O_DIRECT) |
| **raw+directio** | `RawObjectStore` on `/dev/sdb` (20 GB block device), O_DIRECT enabled |
| **raw+buffered** | `RawObjectStore` on `/dev/sdb` (20 GB block device), buffered I/O |

## Workload

- **~63,512 files** totalling **~15 GB** (exceeds 3.1 GB RAM by ~5x)
  - 50,000 small (512 B - 64 KB) = ~1.6 GB
  - 1,500 medium (128 KB - 2 MB) = ~1.6 GB
  - 2,000 large (1 MB - 8 MB) = ~9.0 GB
  - 10,000 tiny (256 B - 4 KB) = ~21 MB
  - 12 huge (1 MB - 512 MB) = ~3 GB
- Writes in 5 batches with periodic flush/sync
- Caches dropped (`echo 3 > /proc/sys/vm/drop_caches`) before every read phase
- Optimize consolidates 60,000 small+tiny files into contiguous 4 MB chunks
- 5,000 random reads per phase (from the full file set)
- Deterministic workload (seed = 42) - identical data across all backends

## Results (MB/s)

TOTAL_WRITE is the aggregate throughput: total bytes written across all 5 write
phases divided by the total wall-clock time of those phases. It is **not** an
average of the per-phase MB/s values - it is weighted by actual data volume and
duration, so the large file phase (9 GB) dominates.

| Phase | ext4 | ext4+sync | img+directio | img+buffered | raw+directio | raw+buffered |
|-------|-----:|----------:|-------------:|-------------:|-------------:|-------------:|
| write_small | 179.1 | 125.7 | 147.7 | 282.7 | 123.9 | 319.6 |
| write_medium | 1,039.4 | 592.2 | 244.2 | 277.2 | 233.2 | 403.0 |
| write_large | 953.0 | 458.6 | 330.4 | 298.4 | 471.5 | 363.2 |
| write_tiny | 15.3 | 13.8 | 12.6 | 68.9 | 10.0 | 37.0 |
| write_huge | 422.2 | 465.9 | 179.1 | 379.1 | 242.0 | 350.3 |
| TOTAL_WRITE | 540.8 | 354.9 | 242.9 | 305.1 | 287.0 | 355.1 |
| read_seq (cold) | 407.9 | 317.6 | 708.6 | 790.9 | 618.6 | 681.7 |
| read_random (cold) | 320.0 | 269.2 | 461.7 | 397.6 | 481.0 | 299.1 |
| list (ops/s) | 121,206 | 128,567 | 2,268,286 | 1,587,800 | 2,190,069 | 1,764,222 |
| optimize | 71.4 | 75.1 | 206.9 | 338.1 | 214.8 | 413.5 |
| read_seq (post-opt) | 856.6 | 908.4 | 873.6 | 784.4 | 861.0 | 707.7 |
| read_random (post-opt) | 829.7 | 944.8 | 702.3 | 573.8 | 757.1 | 558.9 |

### Detailed timing

| Phase | ext4 | ext4+sync | img+directio | img+buffered | raw+directio | raw+buffered |
|-------|-----:|----------:|-------------:|-------------:|-------------:|-------------:|
| write_small (50K, 1.6 GB) | 8.8s | 12.5s | 10.6s | 5.6s | 12.7s | 4.9s |
| write_medium (1.5K, 1.6 GB) | 1.5s | 2.7s | 6.6s | 5.8s | 6.9s | 4.0s |
| write_large (2K, 9.0 GB) | 9.5s | 19.8s | 27.4s | 30.4s | 19.2s | 24.9s |
| write_tiny (10K, 21 MB) | 1.4s | 1.5s | 1.6s | 0.3s | 2.1s | 0.6s |
| write_huge (12, 2.9 GB) | 6.8s | 6.2s | 16.0s | 7.6s | 11.8s | 8.2s |
| **TOTAL_WRITE (63,512, 14.8 GB)** | **28.0s** | **42.6s** | **62.3s** | **49.6s** | **52.7s** | **42.6s** |
| read_seq cold (63,512) | 37.1s | 47.6s | 21.3s | 19.1s | 24.4s | 22.2s |
| read_random cold (5,000) | 3.7s | 4.4s | 2.5s | 3.0s | 2.4s | 3.9s |
| optimize (60K -> 396) | 44.6s | 42.4s | 15.4s | 9.4s | 14.8s | 7.7s |
| read_seq post-opt (3,908) | 17.7s | 16.6s | 17.3s | 19.3s | 17.6s | 21.4s |
| read_random post-opt (3,908) | 18.2s | 16.0s | 21.5s | 26.4s | 20.0s | 27.1s |

## Analysis

### Write performance

- **ext4** posts the highest total write throughput (541 MB/s) but this is misleading -
  writes land in the Linux page cache and return immediately. The data is **not on disk**
  until a later background writeback (typically 5-30 seconds later). The large/medium
  numbers (953/1,039 MB/s) are essentially memory bandwidth, not storage speed.
- **ext4+sync** forces writes to disk at the same flush intervals where RawObjectStore
  calls `flush_index()`. At **355 MB/s** total it is a fairer comparison - and
  remarkably close to raw+buffered (also 355 MB/s).
- **raw+buffered** matches ext4+sync at **355 MB/s** total. Small file writes are the
  fastest of any backend (320 MB/s / 10,177 ops/s) because RawObjectStore's contiguous
  layout works well with the page cache.
- **raw+directio** writes at **287 MB/s**. Every write goes straight to the block device
  with no page cache - this is the most honest write number. Data is durable the moment
  `write()` returns.
- **img+directio** is slowest overall (243 MB/s) because writes go through the filesystem
  layer to the image file, adding overhead on top of O_DIRECT.
- **Tiny file writes** (256 B - 4 KB): buffered backends dominate. img+buffered leads
  at 69 MB/s (33K ops/s) while direct-IO backends are 10-13 MB/s due to 4 KB alignment
  overhead per write.

### Read performance (cold cache)

With 15 GB of data and 3.1 GB RAM, cold reads genuinely hit storage.

- **img+buffered leads sequential cold reads** at 791 MB/s, followed by img+directio
  (709 MB/s) and raw+buffered (682 MB/s). RawObjectStore's contiguous extents enable
  highly effective kernel read-ahead.
- **ext4 is slowest** for cold sequential reads: 408 MB/s (ext4) and 318 MB/s
  (ext4+sync). 63,512 individual files scattered across ext4 block groups defeats
  read-ahead. That is **1.7x slower** than img+directio.
- **Random cold reads**: raw+directio leads at 481 MB/s, closely followed by
  img+directio (462 MB/s). Direct-IO avoids page cache overhead for random access.
  ext4 trails at 320/269 MB/s.
- **Post-optimize**: after consolidating 60,000 small+tiny files into 396 chunks of
  ~4 MB, ext4 catches up dramatically (857-945 MB/s) because it now has only ~3,900
  large files that fit well in the filesystem. All backends converge to 700-945 MB/s
  post-optimize.

### List performance

RawObjectStore keeps a `HashMap<Path, ...>` in memory, so listing is essentially free:
~2 million ops/s for direct-IO modes, ~1.6-1.8 million for buffered. This is
**~18x faster** than ext4's `readdir` (~125K ops/s).

### Optimize performance

Consolidating 60,000 small+tiny files into 396 chunks of ~4 MB:
- **raw+buffered**: 413.5 MB/s (7.7s) - fastest, page cache helps both reads and writes.
- **raw+directio**: 214.8 MB/s (14.8s) - each small file read is a direct-IO `pread`.
- **ext4**: 71-75 MB/s (42-45s) - **5.4x slower** than raw+buffered. Reading 60,000
  scattered small files from ext4 is the bottleneck; the filesystem overhead per file
  (open/read/close/unlink) dominates.


## Running the benchmarks

```bash
# Build
cargo build --release --example bench_storage

# Individual backends
cargo run --release --example bench_storage -- ext4
cargo run --release --example bench_storage -- ext4sync
cargo run --release --example bench_storage -- img
cargo run --release --example bench_storage -- img_buf
cargo run --release --example bench_storage -- raw
cargo run --release --example bench_storage -- raw_buf

# All six backends with comparison table
cargo run --release --example bench_storage -- all
```

Requirements: Linux, `/dev/sdb` accessible (for raw modes), `sudo` for `drop_caches`.
