# Index Write Amplification Benchmarks

> **Historical benchmarks** Only format v4 (256-shard index) is
> supported by the current code. The v2 and v3 numbers are retained to show
> why the sharded index was introduced. Results may vary with different
> hardware or code versions.

> **Note:** Format v2 is obsolete and no longer supported by the code (only v4
> is accepted).  The v2 and v3 numbers below are retained as historical
> context showing why the sharded index was introduced.  v4 uses the 256-shard
> index layout with per-extent compression support.

Measurements of index flush overhead comparing the full-index-rewrite design (format v2)
with the 256-shard index (hash-partitioned by `crc32c(key) % 256`). The v2
baseline uses a single monolithic index blob rewritten in full on every `flush_index()`;
the sharded index writes only the dirty shards, reducing write amplification from O(n) to near-constant.

All benchmarks run on a Hyper-V VM (4 vCPU, 8 GB RAM). Object keys are 200 characters.
Payload is 4 KB unless noted. Two configurations tested:

| Config | Device | I/O mode | Size |
|--------|--------|----------|------|
| **Loopback + Buffered** | Temp file on ext4 | Buffered (page cache) | 512 MB -- 1 GB |
| **Raw Device + O_DIRECT** | /dev/sdb | O_DIRECT (bypasses page cache) | 20 GB |

Run with:
```bash
# Loopback + Buffered (default)
CARGO_TARGET_DIR=~/build-rawobjstr cargo test --release \
  --test index_write_amplification -- --nocapture --ignored bench_all_workloads

# Raw device + O_DIRECT
RAW_DEVICE=/dev/sdb CARGO_TARGET_DIR=~/build-rawobjstr cargo test --release \
  --test index_write_amplification -- --nocapture --ignored bench_all_workloads
```

---

## Key Findings

- **282 bytes per entry** in the serialized index (200-char keys, bincode format)
- **92--99% of wall time** is spent in `flush_index()` for flush-per-put workloads
- **344x write amplification** at 10,000 objects with flush-per-put (13.4 GB of index I/O for 39 MB of data)
- **O_DIRECT is 19--56% faster** than buffered for flush-heavy workloads (fsync doesn't need to flush page cache)
- Write amplification ratios are identical between modes (same bytes written, just faster with O_DIRECT)
- Flush time per call is 2--12 ms (O_DIRECT) vs 5--16 ms (buffered)
- Batching flushes reduces amplification linearly but increases the crash-vulnerability window
- **Reopen time is 2.2--55x slower** on raw device with O_DIRECT (no page cache for extent scan)

---

## W1: Sequential Append (flush per PUT)

Inserts N objects (4 KB each, 200-char keys) with `flush_index()` after every single PUT.
This is the worst case for the current design -- every flush rewrites the entire index.

| Objects | Total time | Flush time | Avg flush | Index final | Cumulative index I/O | Write amp |
|--------:|------------|------------|----------:|------------:|---------------------:|----------:|
| 100 | 502 ms | 499 ms | 4.99 ms | 27.6 KB | 1.4 MB | 3.5x |
| 500 | 2,581 ms | 2,557 ms | 5.11 ms | 137.7 KB | 33.7 MB | 17.3x |
| 1,000 | 5,455 ms | 5,408 ms | 5.41 ms | 275.4 KB | 134.6 MB | 34.5x |
| 5,000 | 51,980 ms | 51,527 ms | 10.31 ms | 1,377.0 KB | 3,362.5 MB | 172.2x |

**Raw Device + O_DIRECT:**

| Objects | Total time | Flush time | Avg flush | Index final | Cumulative index I/O | Write amp |
|--------:|------------|------------|----------:|------------:|---------------------:|----------:|
| 100 | 237 ms | 217 ms | 2.17 ms | 27.6 KB | 1.4 MB | 3.5x |
| 500 | 1,515 ms | 1,382 ms | 2.76 ms | 137.7 KB | 33.7 MB | 17.3x |
| 1,000 | 3,341 ms | 3,084 ms | 3.08 ms | 275.4 KB | 134.6 MB | 34.5x |
| 5,000 | 37,510 ms | 35,457 ms | 7.09 ms | 1,377.0 KB | 3,362.6 MB | 172.2x |

O_DIRECT is **28--56% faster** on wall-clock time. The avg flush drops from
5--10 ms to 2--7 ms because fsync on O_DIRECT doesn't need to flush the page cache.
Write amplification ratios are identical -- the same bytes are written either way.

---

## W2: Batch Append (10,000 objects, varying flush interval)

Same 10,000 objects (4 KB, 200-char keys) but flush every N operations instead
of every PUT. Shows the amortization effect of batching.

| Flush every N | Flushes | Total time | Flush time | Avg flush | Cumulative index I/O | Write amp | Ops/sec |
|--------------:|--------:|------------|------------|----------:|---------------------:|----------:|--------:|
| 1 | 10,000 | 159,704 ms | 158,296 ms | 15.83 ms | 13,448.5 MB | 344.3x | 63 |
| 10 | 1,000 | 14,145 ms | 13,864 ms | 13.86 ms | 1,346.0 MB | 34.5x | 707 |
| 50 | 200 | 3,211 ms | 3,030 ms | 15.15 ms | 270.3 MB | 6.9x | 3,115 |
| 100 | 100 | 1,694 ms | 1,524 ms | 15.24 ms | 135.8 MB | 3.5x | 5,903 |
| 500 | 20 | 530 ms | 369 ms | 18.42 ms | 28.2 MB | 0.7x | 18,862 |

**Raw Device + O_DIRECT:**

| Flush every N | Flushes | Total time | Flush time | Avg flush | Cumulative index I/O | Write amp | Ops/sec |
|--------------:|--------:|------------|------------|----------:|---------------------:|----------:|--------:|
| 1 | 10,000 | 129,164 ms | 122,519 ms | 12.25 ms | 13,448.5 MB | 344.3x | 77 |
| 10 | 1,000 | 13,539 ms | 10,569 ms | 10.57 ms | 1,346.0 MB | 34.5x | 739 |
| 50 | 200 | 4,048 ms | 1,924 ms | 9.62 ms | 270.3 MB | 6.9x | 2,470 |
| 100 | 100 | 3,405 ms | 1,093 ms | 10.93 ms | 135.8 MB | 3.5x | 2,937 |
| 500 | 20 | 1,912 ms | 212 ms | 10.57 ms | 28.2 MB | 0.7x | 5,230 |

Flush-every-500 is **300x faster** than flush-every-1 for the same 10,000 objects.
With batching at N=500, the write amplification drops below 1x (the 28 MB of index
I/O is less than the 39 MB of data). However, up to 500 objects are at risk in a crash.

**Note on O_DIRECT at high batch sizes:** With large batches (flush/50--500) buffered
I/O is actually *faster* on total wall time. O_DIRECT flush time is lower (no page
cache writeback) but the non-flush portion (data writes) is slower because every
data PUT also bypasses the page cache. At flush/1 the index rewrite dominates and
O_DIRECT wins; at flush/500 the 10,000 data writes dominate and buffered wins.

---

## W3: Mixed Object Sizes (flush per PUT)

500 objects per size category (512 B, 4 KB, 64 KB, 1 MB) = 2,000 total objects.
Flush after every PUT. Demonstrates that flush cost is determined by object *count*,
not object *size*.

| Metric | Loopback+Buffered | Raw+O_DIRECT |
|--------|------------------:|--------------:|
| Objects | 2,000 | 2,000 |
| Total time | 16,182 ms | 16,403 ms |
| Flush time | 14,955 ms (92%) | 13,280 ms (81%) |
| Avg flush | 7.48 ms | 6.64 ms |
| Index final | 550.8 KB | 550.8 KB |
| Cumulative index I/O | 538.2 MB | 538.2 MB |
| Data written | 533.4 MB | 533.4 MB |
| Write amp | 1.0x | 1.0x |

The write amplification is only 1.0x because the large objects (1 MB each) dominate
the data volume. But the flush time per operation is the same ~7.5 ms regardless of
whether the PUT is 512 B or 1 MB -- the index serialization cost depends only on
object count.

---

## W4: Overwrite-Heavy (constant index size)

Pre-populate 1,000 objects, then overwrite random existing keys 2,000 times with
flush after each overwrite. Index size stays constant at ~1,000 entries.

| Metric | Loopback+Buffered | Raw+O_DIRECT |
|--------|------------------:|--------------:|
| Objects (constant) | 1,000 | 1,000 |
| Overwrite ops | 2,000 | 2,000 |
| Total time | 14,168 ms | 10,195 ms |
| Flush time | 14,055 ms (99%) | 9,412 ms (92%) |
| Avg flush | 7.03 ms | 4.71 ms |
| Index final | 275.5 KB | 275.4 KB |
| Cumulative index I/O | 538.0 MB | 538.0 MB |
| Write amp | 68.9x | 68.9x |

O_DIRECT is **28% faster** here. Each flush rewrites 275 KB of index for a 4 KB
data change -- a 69:1 ratio.

---

## W5: Delete-Heavy (shrinking index)

Pre-populate 2,000 objects, then delete one at a time with flush after each delete.

| Metric | Loopback+Buffered | Raw+O_DIRECT |
|--------|------------------:|--------------:|
| Initial objects | 2,000 | 2,000 |
| Delete ops | 2,000 | 2,000 |
| Total time | 14,572 ms | 9,790 ms |
| Flush time | 14,503 ms (99%) | 9,714 ms (99%) |
| Avg flush | 7.25 ms | 4.86 ms |
| Index at start | ~564 KB | ~564 KB |
| Index at end | 48 B (empty) | 48 B (empty) |
| Cumulative index I/O | 537.7 MB | 537.7 MB |

The cumulative index I/O is nearly identical to W4 (same number of flushes, similar
average index size). With the sharded index, deletes rewrite only the affected
shard (~1/256th of the index) instead of the full blob.

---

## W6: Mixed CRUD (realistic workload)

Start with 1,000 objects. Execute 10,000 random operations (50% PUT-new, 20%
overwrite, 20% GET, 10% DELETE) and flush every 10 operations.

| Metric | Loopback+Buffered | Raw+O_DIRECT |
|--------|------------------:|--------------:|
| Final objects | 5,048 | 5,048 |
| Total ops | 10,000 | 10,000 |
| Flushes | 1,001 | 1,001 |
| Total time | 13,295 ms | 10,915 ms |
| Flush time | 13,002 ms (98%) | 7,888 ms (72%) |
| Avg flush | 12.99 ms | 7.88 ms |
| Index final | 1,390.2 KB | 1,390.2 KB |
| Cumulative index I/O | 818.7 MB | 818.7 MB |
| Data written | 27.7 MB | 27.7 MB |
| Write amp | 29.6x | 29.6x |

O_DIRECT drops flush from 98% to 72% of wall time. The 18% total improvement
comes from faster fsync (no page cache writeback).

---

## W7: Reopen / Recovery Time

Populate N objects, flush, close, then measure `open()` time (index deserialization +
integrity scan).

| Objects | Index size | Loopback open | Raw+O_DIRECT open |
|--------:|----------:|-------------:|---------:|
| 100 | 27.6 KB | 0.5 ms | 14.3 ms |
| 1,000 | 275.4 KB | 3.5 ms | 192.8 ms |
| 5,000 | 1,377.0 KB | 36.6 ms | 969.3 ms |
| 10,000 | 2,753.9 KB | 42.6 ms | 2,258.7 ms |

O_DIRECT reopen is **28--55x slower** than buffered. The open path reads block 0 of
every extent for the integrity scan -- without the page cache, each read is a physical
disk I/O. On a 20 GB device with O_DIRECT, scanning 10,000 extents takes 2.3 seconds.
This is an important consideration for production use on raw devices.

---

## W8: Index Size Validation

Measures serialized index size for varying object counts (200-char keys, 4 KB objects).

| Objects | Index size | Bytes/entry |
|--------:|-----------:|------------:|
| 100 | 27.6 KB | 282 |
| 500 | 137.7 KB | 282 |
| 1,000 | 275.4 KB | 282 |
| 5,000 | 1,377.0 KB | 282 |
| 10,000 | 2,753.9 KB | 282 |

Exactly 282 bytes per entry -- consistent across all sizes. This is the bincode
serialization of: 8-byte HashMap length prefix (amortized) + 8-byte string length +
200-byte key + 48-byte ExtentInfo + overhead. At this rate:

| Object count | Index size | Fits in 16 MB slot? |
|-------------:|-----------:|:-------------------:|
| 10,000 | 2.7 MB | Yes |
| 50,000 | 13.8 MB | Yes |
| 58,000 | 16.0 MB | Barely |
| 100,000 | 27.5 MB | No (need 32 MB slot) |

---

## Summary Table

### Loopback + Buffered I/O

| Workload | Objects | Flushes | Total(ms) | Flush(ms) | IdxFinal(KB) | IdxIO(MB) | WrAmp |
|----------|--------:|--------:|----------:|----------:|--------------:|----------:|------:|
| W1: seq_append(100) | 100 | 100 | 502 | 499 | 27.6 | 1.4 | 3.5x |
| W1: seq_append(500) | 500 | 500 | 2,581 | 2,557 | 137.7 | 33.7 | 17.3x |
| W1: seq_append(1,000) | 1,000 | 1,000 | 5,455 | 5,408 | 275.4 | 134.6 | 34.5x |
| W1: seq_append(5,000) | 5,000 | 5,000 | 51,980 | 51,527 | 1,377.0 | 3,362.5 | 172.2x |
| W2: batch(flush/1) | 10,000 | 10,000 | 159,704 | 158,296 | 2,753.9 | 13,448.5 | 344.3x |
| W2: batch(flush/10) | 10,000 | 1,000 | 14,145 | 13,864 | 2,753.9 | 1,346.0 | 34.5x |
| W2: batch(flush/50) | 10,000 | 200 | 3,211 | 3,030 | 2,753.9 | 270.3 | 6.9x |
| W2: batch(flush/100) | 10,000 | 100 | 1,694 | 1,524 | 2,753.9 | 135.8 | 3.5x |
| W2: batch(flush/500) | 10,000 | 20 | 530 | 369 | 2,753.9 | 28.2 | 0.7x |
| W3: mixed_sizes | 2,000 | 2,000 | 16,182 | 14,955 | 550.8 | 538.2 | 1.0x |
| W4: overwrite(1K base) | 1,000 | 2,000 | 14,168 | 14,055 | 275.5 | 538.0 | 68.9x |
| W5: delete(2K->0) | 0 | 2,000 | 14,572 | 14,503 | 0.0 | 537.7 | -- |
| W6: mixed_crud | 5,048 | 1,001 | 13,295 | 13,002 | 1,390.2 | 818.7 | 29.6x |
| W7: reopen(100) | 100 | 0 | 0.5 | -- | 27.6 | -- | -- |
| W7: reopen(1K) | 1,000 | 0 | 3.5 | -- | 275.4 | -- | -- |
| W7: reopen(5K) | 5,000 | 0 | 36.6 | -- | 1,377.0 | -- | -- |
| W7: reopen(10K) | 10,000 | 0 | 42.6 | -- | 2,753.9 | -- | -- |

### Raw Device + O_DIRECT

| Workload | Objects | Flushes | Total(ms) | Flush(ms) | IdxFinal(KB) | IdxIO(MB) | WrAmp |
|----------|--------:|--------:|----------:|----------:|--------------:|----------:|------:|
| W1: seq_append(100) | 100 | 100 | 237 | 217 | 27.6 | 1.4 | 3.5x |
| W1: seq_append(500) | 500 | 500 | 1,515 | 1,382 | 137.7 | 33.7 | 17.3x |
| W1: seq_append(1,000) | 1,000 | 1,000 | 3,341 | 3,084 | 275.4 | 134.6 | 34.5x |
| W1: seq_append(5,000) | 5,000 | 5,000 | 37,510 | 35,457 | 1,377.0 | 3,362.6 | 172.2x |
| W2: batch(flush/1) | 10,000 | 10,000 | 129,164 | 122,519 | 2,753.9 | 13,448.5 | 344.3x |
| W2: batch(flush/10) | 10,000 | 1,000 | 13,539 | 10,569 | 2,753.9 | 1,346.0 | 34.5x |
| W2: batch(flush/50) | 10,000 | 200 | 4,048 | 1,924 | 2,753.9 | 270.3 | 6.9x |
| W2: batch(flush/100) | 10,000 | 100 | 3,405 | 1,093 | 2,753.9 | 135.8 | 3.5x |
| W2: batch(flush/500) | 10,000 | 20 | 1,912 | 212 | 2,753.9 | 28.2 | 0.7x |
| W3: mixed_sizes | 2,000 | 2,000 | 16,403 | 13,280 | 550.8 | 538.2 | 1.0x |
| W4: overwrite(1K base) | 1,000 | 2,000 | 10,195 | 9,412 | 275.4 | 538.0 | 68.9x |
| W5: delete(2K->0) | 0 | 2,000 | 9,790 | 9,714 | 0.0 | 537.7 | -- |
| W6: mixed_crud | 5,048 | 1,001 | 10,915 | 7,888 | 1,390.2 | 818.7 | 29.6x |
| W7: reopen(100) | 100 | 0 | 14.3 | -- | 27.6 | -- | -- |
| W7: reopen(1K) | 1,000 | 0 | 192.8 | -- | 275.4 | -- | -- |
| W7: reopen(5K) | 5,000 | 0 | 969.3 | -- | 1,377.0 | -- | -- |
| W7: reopen(10K) | 10,000 | 0 | 2,258.7 | -- | 2,753.9 | -- | -- |

### Mode Comparison (% change: O_DIRECT vs Buffered)

| Workload | Buffered Total | O_DIRECT Total | Delta | Write Amp |
|----------|---------------:|---------------:|------:|----------:|
| W1: 5K append | 51,980 ms | 37,510 ms | **-28%** | identical |
| W2: flush/1 | 159,704 ms | 129,164 ms | **-19%** | identical |
| W2: flush/10 | 14,145 ms | 13,539 ms | **-4%** | identical |
| W4: overwrite | 14,168 ms | 10,195 ms | **-28%** | identical |
| W5: delete | 14,572 ms | 9,790 ms | **-33%** | identical |
| W6: mixed | 13,295 ms | 10,915 ms | **-18%** | identical |
| W7: reopen 10K | 42.6 ms | 2,258.7 ms | **+5,202%** | N/A |

---

## v3 Sharded Index Results (Loopback + Buffered)

After implementing the 256-shard index (format version 3), each `flush_index()` writes
only the dirty shards (~1/256th per affected key) instead of the entire index. The
free list is no longer persisted (rebuilt from gap analysis on open).

### v3 Summary Table

| Workload | Objects | Flushes | Total(ms) | Flush(ms) | IdxFinal(KB) | IdxIO(MB) | WrAmp |
|----------|--------:|--------:|----------:|----------:|--------------:|----------:|------:|
| W1: seq_append(100) | 100 | 100 | 2,079 | 2,030 | 31.5 | 0.8 | 2.1x |
| W1: seq_append(500) | 500 | 500 | 7,872 | 7,743 | 141.7 | 4.1 | 2.1x |
| W1: seq_append(1,000) | 1,000 | 1,000 | 15,645 | 15,391 | 279.4 | 8.5 | 2.2x |
| W1: seq_append(5,000) | 5,000 | 5,000 | 60,502 | 59,213 | 1,381.0 | 53.0 | 2.7x |
| W2: batch(flush/1) | 10,000 | 10,000 | 94,486 | 92,814 | 2,757.9 | 132.3 | 3.4x |
| W2: batch(flush/10) | 10,000 | 1,000 | 14,979 | 14,642 | 2,757.9 | 62.0 | 1.6x |
| W2: batch(flush/50) | 10,000 | 200 | 7,131 | 6,925 | 2,757.9 | 55.8 | 1.4x |
| W2: batch(flush/100) | 10,000 | 100 | 5,709 | 5,529 | 2,757.9 | 50.7 | 1.3x |
| W2: batch(flush/500) | 10,000 | 20 | 2,932 | 2,768 | 2,757.9 | 28.5 | 0.7x |
| W3: mixed_sizes | 2,000 | 2,000 | 16,096 | 14,717 | 554.8 | 18.1 | 0.0x |
| W4: overwrite(1K base) | 1,000 | 2,000 | 11,531 | 11,397 | 279.4 | 18.0 | 2.3x |
| W5: delete(2K->0) | 0 | 2,000 | 11,617 | 11,538 | 4.0 | 17.7 | -- |
| W6: mixed_crud | 5,048 | 1,001 | 10,984 | 10,715 | 1,394.1 | 34.4 | 1.2x |
| W7: reopen(100) | 100 | 0 | 0.6 | -- | 31.5 | -- | -- |
| W7: reopen(1K) | 1,000 | 0 | 3.3 | -- | 279.4 | -- | -- |
| W7: reopen(5K) | 5,000 | 0 | 20.2 | -- | 1,380.9 | -- | -- |
| W7: reopen(10K) | 10,000 | 0 | 42.8 | -- | 2,757.9 | -- | -- |

### v2 -> v3 Comparison (Index I/O and Write Amplification)

| Workload | v2 IdxIO | v2 WrAmp | v3 IdxIO | v3 WrAmp | I/O Reduction |
|----------|--------:|--------:|--------:|--------:|--------:|
| W1: seq_append(100) | 1.4 MB | 3.5x | 0.8 MB | 2.1x | **43%** |
| W1: seq_append(500) | 33.7 MB | 17.3x | 4.1 MB | 2.1x | **88%** |
| W1: seq_append(1,000) | 134.6 MB | 34.5x | 8.5 MB | 2.2x | **94%** |
| W1: seq_append(5,000) | 3,362.5 MB | 172.2x | 53.0 MB | 2.7x | **98.4%** |
| W2: batch(flush/1) 10K | 13,448.5 MB | 344.3x | 132.3 MB | 3.4x | **99.0%** |
| W2: batch(flush/10) 10K | 1,346.0 MB | 34.5x | 62.0 MB | 1.6x | **95.4%** |
| W2: batch(flush/50) 10K | 270.3 MB | 6.9x | 55.8 MB | 1.4x | **79.4%** |
| W2: batch(flush/100) 10K | 135.8 MB | 3.5x | 50.7 MB | 1.3x | **62.7%** |
| W2: batch(flush/500) 10K | 28.2 MB | 0.7x | 28.5 MB | 0.7x | ~same |
| W3: mixed_sizes | 538.2 MB | 1.0x | 18.1 MB | 0.0x | **96.6%** |
| W4: overwrite(1K) | 538.0 MB | 68.9x | 18.0 MB | 2.3x | **96.7%** |
| W5: delete(2K->0) | 537.7 MB | -- | 17.7 MB | -- | **96.7%** |
| W6: mixed_crud | 818.7 MB | 29.6x | 34.4 MB | 1.2x | **95.8%** |

### Key findings

- **Write amplification is nearly constant** at 2--3x regardless of object count,
  compared to v2's O(n) growth (3.5x at 100 objects -> 344x at 10,000).
- At 10K objects with flush-per-put: **344x -> 3.4x** (101x improvement).
- Overwrite-heavy workload: **68.9x -> 2.3x** (30x improvement).
- Mixed CRUD: **29.6x -> 1.2x** (25x improvement).
- Batch flush/500 is unchanged (already writing full index at each flush).
- Reopen time is essentially the same (~42 ms for 10K objects) -- loading 256 small
  shard blobs is roughly the same speed as loading one large blob.
- Index size per entry is slightly higher at ~282 bytes (vs ~275 in v2) due to
  256x shard container overhead, but the difference is negligible.
- Total wall time for flush-per-put is *higher* in v3 (per-shard iteration overhead),
  but index I/O is dramatically lower. The wall time increase is due to
  more syscalls (one pwrite per dirty shard vs one pwrite for the whole index).
