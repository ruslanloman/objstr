# Storage Format

This document describes the on-disk layout of a rawobjstr device (format version 4).

## Overview

The device is divided into three regions:

```
 0                    8 KB                                     device_size - 2*slot              device_size
 +---------------------+------------------------------------------+-----------------------------------+
 |  Superblock Region  |             Data Region                  |          Index Region             |
 |      (8 KB)         |   (device_size - 8 KB - 2*slot)         |       (2 * index_slot_size)       |
 +------+--------------+------------------------------------------+----------------+------------------+
 | SB-A |     SB-B     |  Extent . Extent . Extent . ...  . Free |   Shards A     |    Shards B      |
 | 4 KB |     4 KB     |  (4 KB-aligned allocations)              |   (slot_size)  |    (slot_size)   |
 +------+--------------+------------------------------------------+----------------+------------------+
```

All byte offsets and sizes are aligned to 4 KB (4096 bytes).

---

## Superblock Region (8 KB)

Two identical 4 KB superblock copies at offsets 0 and 4096. Both are written on every `flush_index()` with an `fsync` between each write. If one is corrupt on open, the other is used. The valid superblock with the highest `txn_id` wins.

To view the first superblock:

```
 hexdump -s 0 -n 72 -e '8/1 "%c" " | " 1/4 "v%d " 1/4 "f%d | " 1/8 "%u " 1/8 "%u " 1/8 "%u " 1/8 "%u " 1/4 "%08x " 1/4 "%08x " 1/8 "%u\n"' device.raw
```

The second superblock:

```
hexdump -s 4096 -n 72 -e '8/1 "%c" " | " 2/4 "v%u " " | " 4/8 "%u " " | " 1/8 "Txn:%u " 2/4 "Crc:%08x " 1/8 "Prev:%u\n"' device.raw
```

Eg:

```
# Magic(8s) | Ver(4) | Flags(4) | DevSize(8) | Align(8) | IdxOff(8) | IdxSz(8) | Txn(8) | IdxCrc(4) | SbCrc(4) | PrevOff(8)

RAWOBJST | v4 v0 | 536870912 4096 503316480 24 | Txn:1 Crc:00000000 Crc:cd6d47ddPrev:503316480
```

The IdxCrc field is unused (always 0) - per-shard CRCs are stored in the
shard directory later in the superblock.

### Serialization

The superblock is serialized with bincode in field declaration order, then zero-padded to 4096 bytes. A CRC32c checksum covers the entire 4096-byte buffer with the checksum field itself zeroed during computation.

### Field layout

The superblock stores both device geometry and the shard directory (256 entries of
per-shard metadata). With bincode serialization the shard directory adds ~2,312 bytes,
fitting comfortably in the 4 KB superblock alongside the ~100 bytes of fixed fields.

```
Offset  Size  Field                   Description
------  ----  -----                   -----------
 0       8    magic                   b"RAWOBJST" - identifies a formatted device
 8       4    version                 Format version (currently 4)
12       4    flags                   Bit 0 = O_DIRECT enabled
16       8    device_size             Total device size in bytes
24       8    block_alignment         Allocation alignment (4096)
32       8    index_region_offset     Byte offset of index region A
40       8    index_region_size       Sum of all active shard sizes (informational)
48       8    txn_id                  Monotonic transaction counter (incremented on each flush)
56       4    index_checksum          (Legacy, unused - per-shard CRCs used instead)
60       4    superblock_checksum     CRC32c of this 4096-byte buffer (field zeroed during computation)
64       8    prev_index_offset       Previous index region offset (reserved)
72       8    prev_index_size         Previous index region size (reserved)
80       8    prev_txn_id             Previous transaction ID
88       4    prev_index_checksum     Previous index CRC32c (reserved)
92       8    index_slot_capacity     Max capacity of each index slot (multiple of 16 MB; default 16 MB)
100    2312   shard_slots[256]        Per-shard directory (256 x ShardSlotMeta, see below)
~2412     4   max_key_length          Maximum allowed key length in bytes (stored as u32). 0 = use DEFAULT_MAX_KEY_LENGTH (1024). Hard ceiling: 65536 (64 KB).
~2416     1   compression             Compression algorithm (0=none, 1=zstd, 2=snappy, 3..12=gzip0..gzip9). Set at format time, applies to all objects.
~2417  ~1679  (zero padding)          Pad to 4096 bytes
```

#### ShardSlotMeta (9 bytes each, 256 entries)

Each shard has a metadata entry in the superblock:

| Field | Size | Description |
|-------|------|-------------|
| `active_slot` | 1 byte | Which region holds the active copy: 0 = region A, 1 = region B |
| `size` | 4 bytes | Serialized size in bytes of this shard's blob |
| `crc` | 4 bytes | CRC32c of the serialized shard blob |

### Validation on open

1. Read both 4 KB superblocks (offsets 0 and 4096).
2. For each: zero the checksum field, compute CRC32c over the full 4096 bytes, compare to stored checksum.
3. Pick the valid superblock with the highest `txn_id`.
4. If both are corrupt, the device cannot be opened (must be reformatted).

---

## Data Region (extents)

Starts at offset 8192 (after the superblock region) and extends to `device_size - 2 * index_slot_size`.

Files are stored as **extents** - contiguous allocations. Each extent is divided into
4 KB blocks, where every block carries a 4-byte CRC32c prefix protecting 4092 bytes of
data. The payload is split across CRC-protected blocks with no per-extent header:

```
Block 0                          Block 1                          Block N (last)
+------+------------------------+   +------+------------------------+   +------+-------------+------+
| CRC0 | Payload                |   | CRC1 | Payload (cont.)        |   | CRCn | Payload     | Zero |
| 4 B  | 4092 B                 |   | 4 B  | 4092 B                 |   | 4 B  | remaining   | pad  |
+------+------------------------+   +------+------------------------+   +------+-------------+------+
<------- 4096 bytes -------------> <------- 4096 bytes -------------> <------- 4096 bytes --------->
```

Each 4-byte CRC32c covers the full 4092-byte data region that follows it (including
any zero-padding in the last block). This enables:

- **Per-block integrity** - corruption is detected at read time without reading the
  entire file; only the requested blocks are verified.
- **True partial reads** - range requests fetch and verify only the blocks that
  overlap the requested byte range.
- **Defrag-friendly** - blocks are self-contained (CRC travels with data), so they
  can be relocated without recomputing checksums.

All extent metadata (size, payload CRC, timestamps) lives in the catalog (index)
as `ExtentInfo` entries. There is no per-extent header on disk.

### Size calculation

An extent holding `N` payload bytes occupies `ceil(N / 4092) * 4096` bytes on
device. Each 4 KB block carries 4 bytes of CRC overhead, leaving 4092 bytes for data:

| Payload size | Blocks | Padded extent size |
|-------------|--------|--------------------|
| 1 byte | 1 | 4 KB |
| 4,092 bytes | 1 | 4 KB (exact fit) |
| 4,093 bytes | 2 | 8 KB |
| 8,184 bytes | 2 | 8 KB (exact fit) |
| 1 MB | 256 | 1,048,576 B |

Zero-byte objects are rejected at the API level (`EmptyPayload` error).
The minimum object size is 1 byte, which occupies one 4 KB block on disk.

The CRC overhead is 4 bytes per 4092 data bytes (0.098%).

### Allocation

A first-fit allocator manages a sorted free list of `(offset, size)` pairs. When a file is deleted, its extent is returned to the free list and merged with adjacent free regions (coalescing). When a file is overwritten, the old extent is freed and a new one is allocated.

---

## Object Metadata

Objects can carry an opaque metadata suffix stored alongside the body in the same
extent. There is no separate on-disk structure for metadata - it is concatenated
to the end of the body bytes within the payload:

```
Payload layout (before CRC-block encoding):

+----------------------------------+----------------------------+
|          Body bytes              |     Metadata suffix        |
|   [0 .. size - meta_len)        |   [size - meta_len .. size)|
+----------------------------------+----------------------------+
<-------- ExtentInfo.size (on-disk payload size) --------------->
```

The `meta_len` field in `ExtentInfo` (u16) records how many trailing bytes are
metadata. When `meta_len` is 0 the object has no metadata. The body size visible
to callers (S3 `Content-Length`, `ObjectStore::head`) is `size - meta_len` (or
`uncompressed_size - meta_len` when compressed).

### Limits

`meta_len` is a `u16`, so the maximum metadata size is **65,535 bytes** (64 KB - 1).
Attempts to store larger metadata return a `MetadataTooLarge` error.

### Compression interaction

When device-level compression is enabled, the entire payload (body + metadata) is
compressed as a single unit. The `meta_len` field always refers to the
*uncompressed* metadata length - decompression is applied first, then the suffix
is split out.

### How metadata is used

The S3 adapter (`objstrd`) uses the metadata suffix to persist per-object S3
headers (Content-Type, Content-Encoding, user-defined `x-amz-meta-*` headers,
etc.) as opaque bytes. The raw store itself treats the metadata as an opaque
blob and never interprets its contents.

### API methods

| Method | Description |
|--------|-------------|
| `put_with_meta(path, body, metadata)` | Write body + metadata as one extent |
| `get_metadata(path)` | Read only the metadata suffix (suffix read of `meta_len` bytes) |
| `head_with_meta(path)` | Return `ObjectMeta` + `meta_len` (index-only, no data I/O) |
| `set_meta_len(path, len)` | Update `meta_len` in the index for an existing object |

---

## Index Region (Sharded)

The last `2 * index_slot_size` bytes of the device hold two index regions, each
subdivided into 256 **shard slots**. Each shard independently uses A/B double-buffering
- its active copy lives in either region A or region B, tracked by the per-shard
`active_slot` field in the superblock.

```
                  Region A (slot_size)                              Region B (slot_size)
+----+----+----+----+------+----+----+           +----+----+----+----+------+----+----+
| S0 | S1 | S2 | S3 | .... |S254|S255|           | S0 | S1 | S2 | S3 | .... |S254|S255|
+----+----+----+----+------+----+----+           +----+----+----+----+------+----+----+
 <-- shard_slot_size each -->                      <-- shard_slot_size each -->

shard_slot_size = index_slot_capacity / 256
```

With the default 16 MB slot size, each shard slot is **64 KB**. Shard `i`'s region A
offset is `index_region_a + i * shard_slot_size`; region B is
`index_region_b + i * shard_slot_size`.

### Shard assignment

Each key is assigned to a shard by:

```
shard_id = crc32c(key_bytes) & 0xFF    // low 8 bits of CRC32c -> 0..255
```

This distributes keys roughly uniformly across the 256 shards. With 10,000 objects
the average shard holds ~39 entries.

### ShardData structure

Each shard is independently bincode-serialized and CRC32c-protected:

| Field | Type | Description |
|-------|------|-------------|
| `files` | `HashMap<String, ExtentInfo>` | Keys in this shard mapped to their extent metadata |
| `tombstones` | `HashMap<String, TombstoneInfo>` | Tombstones for objects removed during integrity scanning |

### ExtentInfo

Each entry in the file map:

| Field | Type | Description |
|-------|------|-------------|
| `offset` | `u64` | Byte offset on device where the extent starts |
| `size` | `u64` | On-disk payload size (compressed if compression was applied) |
| `padded_size` | `u64` | Total extent size on disk (= ceil(size / 4092) * 4096) |
| `crc32c` | `u32` | CRC32c of the on-disk (possibly compressed) payload |
| `created_txn` | `u64` | Transaction ID when written |
| `last_modified` | `DateTime<Utc>` | Timestamp of last write |
| `meta_len` | `u16` | Length of the metadata suffix appended after the body (0 = no metadata) |
| `uncompressed_size` | `u64` | Original uncompressed payload size. 0 means the extent is not compressed (either compression is disabled for the device, the payload was too small, or compression did not reduce size). When non-zero, `size` is the compressed on-disk size and `uncompressed_size` is the logical size returned to callers. |

### Integrity

Each shard's serialized blob is protected by a CRC32c stored in the superblock's
`shard_slots[i].crc` field. On open, all 256 shards are deserialized and CRC-verified.
If any shard fails verification, the device cannot be opened.

### Double-buffering (per-shard)

Each shard independently alternates between its region A and region B slot.
`flush_index()` writes only **dirty shards** to the opposite slot from their current
`active_slot`, then updates the superblock atomically. This means:

- A PUT touching 1 key rewrites only that key's shard (~1/256th of the index)
- Crash safety: each shard's previous copy remains intact in the other slot
- The superblock commit is the atomic point - it flips all dirty shards' `active_slot`
  values at once

### Flush sequence

1. For each dirty shard: collect matching entries, serialize, write to opposite slot
2. `fsync` all shard data
3. Update superblock (`txn_id`, `shard_slots`, `index_region_size`)
4. Write primary superblock + `fsync`
5. Write backup superblock + `fsync`
6. Clear dirty flags

### Free list

The free list (allocator state) is **not** persisted. On every `open()`, the
allocator is rebuilt from gap analysis: sort all extents by offset, compute the gaps
between them. This eliminates free-list serialization overhead and guarantees
consistency (no stale free-list entries after a crash).

---

## Compression

Compression is configured at format time via the `compression` field in the superblock.
Once set, the algorithm applies to all objects written to the device. The algorithm
cannot be changed without reformatting.

### Supported algorithms

| Value | Name | Notes |
|-------|------|-------|
| 0 | none | No compression (default) |
| 1 | zstd | Zstandard (default level 3) |
| 2 | snappy | Google Snappy (fast, moderate ratio) |
| 3-12 | gzip0-gzip9 | Gzip with explicit level 0-9 |

### Behavior

- **Minimum payload threshold:** Payloads smaller than 4096 bytes are never compressed
  (overhead outweighs savings at small sizes).
- **Try-then-compare:** The payload is compressed, and the compressed result is only
  used if it is strictly smaller than the original. If compression does not reduce size,
  the original uncompressed payload is stored and `uncompressed_size` is set to 0.
- **CRC covers on-disk bytes:** The per-block CRC32c and the payload CRC32c in the
  index both cover the on-disk (compressed) payload. Decompression happens after CRC
  verification.
- **Logical size reporting:** `ObjectStore` callers (S3 clients, `Content-Length`) see
  the uncompressed size. The on-disk size is internal to the store.

### Full GET streaming

For full object reads (`get()` with no range), compressed objects are streamed
directly to the caller without buffering the decompressed form in memory:

- **zstd** and **gzip** (all levels): the CRC-protected blocks are read in 1 MB
  batches from disk and piped through a streaming decoder (`zstd::stream::read::Decoder`
  or `flate2::read::GzDecoder`).  Decompressed chunks (256 KB each) are yielded
  directly to the `GetResult` stream.  Peak RAM: ~1 MB block buffer + decoder
  working state, regardless of object size.
- **snappy**: the `snap` crate has no streaming `Read` API (raw format limitation).
  The full compressed payload is read into memory (the compressed, smaller form) and
  decompressed in one call.  Peak RAM: compressed size + decompressed size.

### Range reads on compressed objects

When a range read is requested on a compressed object, the entire compressed extent
must be read and decompressed before the requested byte range can be extracted.  This
is unavoidable - the decompressor must process all preceding bytes to reconstruct
any given offset.  To prevent unbounded memory usage:

- Objects with `uncompressed_size <= 1 GB`: range reads are allowed. The full extent
  is decompressed into memory and the requested range is sliced out.
- Objects with `uncompressed_size > 1 GB`: range reads are rejected with an error.
  The caller must fetch the whole object instead.

This limitation is documented in the `FormatOptions` struct and in error messages
returned to callers.

---

## Full Device Map

Example: 256 MB device with default 16 MB index slots (shard_slot_size = 64 KB):

```
Byte offset         Size        Contents
-----------         ----        --------
0x0000_0000           4 KB      Superblock A (primary, includes shard directory)
0x0000_1000           4 KB      Superblock B (backup)
0x0000_2000     ~222.0 MB      Data extents (first-fit allocated, 4 KB aligned)
0x0E00_2000        16 MB       Index Region A  (256 shard slots x 64 KB each)
0x0F00_2000        16 MB       Index Region B  (256 shard slots x 64 KB each)
0x1000_0000         ---        End of device (256 MB)
```

With 32 MB index slots on the same device, the data area shrinks to ~190 MB and the two index regions occupy 64 MB total.

---

## Index Capacity by Slot Size

Each shard holds a subset of files (~1/256th). The per-shard serialized size depends
on path length and entry count. With 282 bytes per entry (200-char keys, measured)
and 256 shards, each shard averages ~282 x N/256 bytes.

The bottleneck is the shard_slot_size (= slot_size / 256). A shard overflows if its
serialized data exceeds its 64 KB slot (with default 16 MB index slots). In practice
hash distribution means some shards hold more entries than average.

| Slot Size | Shard Slot | Approx. Max Files (200-char keys) | Approx. Max Files (25-char keys) |
|-----------|-----------|-----------------------------------|----------------------------------|
| **16 MB** (default) | 64 KB | ~50,000 | ~150,000 |
| **32 MB** | 128 KB | ~100,000 | ~300,000 |
| **48 MB** | 192 KB | ~150,000 | ~450,000 |
| **64 MB** | 256 KB | ~200,000 | ~600,000 |

To choose a slot size at format time:

```rust
use rawobjstr::store::{RawObjectStore, FormatOptions};
use rawobjstr::Compression;

let store = RawObjectStore::format_with_options(path, FormatOptions {
    device_size,
    direct_io: true,
    index_slot_size: 32 * 1024 * 1024, // 32 MB per slot (64 MB total)
    max_key_length: 1024,
    compression: Compression::Zstd,     // or Compression::None
})?;
```

---

## Limits

| Parameter | Value | Notes |
|-----------|-------|-------|
| Minimum device size | ~32 MB + 12 KB (default) | `SUPERBLOCK_REGION + BLOCK_ALIGNMENT + 2 * index_slot_size` |
| Maximum device size | No hard limit | Tested up to 20 GB; limited by available disk |
| Index slot size | 16 MB default, configurable | Must be a multiple of 16 MB; set at format time via FormatOptions |
| Block alignment | 4 KB | All extents are padded to 4 KB boundaries |
| Block data size | 4,092 bytes | 4 KB block minus 4-byte CRC prefix |
| Max file size | Device size - overhead | Single file can use the entire data area |
| Max path length | ~65 KB | Limited by bincode serialization (practical limit: thousands of chars) |
| Concurrent access | Single writer (flock), multi-thread | `Arc<RwLock>` internally; `flock(LOCK_EX)` prevents multiple writer processes; readers use `pread` without lock |

---

## Crash Safety

> **Warning: rawobjstr is NOT durable-by-default. You MUST call `flush_index()` to persist state. Everything between flush calls is lost on crash, power loss, or kill -9.**

### Write path

1. `put()` and `copy()` write data payloads to the device immediately but only update an **in-memory** index. `delete()` only frees the extent in the in-memory allocator (no device write). The on-disk index is unchanged until `flush_index()` is called.
2. `flush_index()` atomically persists the in-memory index:
   - Serializes only **dirty shards** and writes each to the opposite slot from its current `active_slot`.
   - Updates both superblocks (primary + backup) with the new `txn_id`, shard slot metadata, and CRC32c checksums.
   - `fsync`s after shard data, after the primary superblock, and after the backup superblock.
3. On `open()` after a crash, the store reads both superblocks, picks the valid one with the highest `txn_id`, and loads the index from the region it points to.

### What is lost on crash?

| Crash timing | Survives | Lost | Device state |
|-------------|----------|------|--------------|
| Before any `flush_index()` | Nothing | All puts since format | `open()` returns empty store. Unreferenced extents leak space. |
| Between flushes | Last successful flush | Puts/deletes/copies after that flush | Orphaned extents leak space. "Deleted" files reappear. |
| During `flush_index()` | Previous checkpoint | In-progress flush | Worst case: one superblock corrupt, other valid. `open()` picks the valid one. |
| After `flush_index()` | Everything | Nothing | Clean state. |

**Space leaks:** Orphaned extents (written but never indexed) cannot be reclaimed without reformatting. Use `repair` to rebuild the free list after a crash, or reformat via export -> format -> import.

### Stale extent detection (the crash-recovery gap)

Between flushes, `put()` frees the old extent in the in-memory allocator and immediately reuses the space for new writes.  If the process crashes before the next `flush_index()`, the on-disk index still references the old offset  -  which now contains a **different object's data**.

Reading a stale extent returns wrong bytes silently unless the CRC is checked against the index.  During normal operation this never happens: the in-memory index is always authoritative.  The problem only arises when the store is reopened after a crash and the persisted index is rolled back to the last checkpoint.

**How many objects can be affected?**

- If you flush after every write: at most **one** object can be stale.
- If you batch N writes between flushes: up to **N** stale entries are possible.

### Open-time integrity scan (`OpenMode`)

To protect against stale extents, the store performs an integrity scan every time a device is opened.  The scan mode is controlled by the `OpenMode` enum passed to `open_with_mode()`.  The default `open()` uses `OpenMode::Default`.

| Mode | I/O cost | What it catches |
|------|----------|-----------------|
| `Default` | One 4 KB pread per object | Full overwrites (block 0 CRC mismatch) |
| `FullVerify` | Reads every block of every object | Full and partial overwrites (payload CRC recomputed) |
| `SkipVerify` | Zero extra I/O | Nothing  -  caller accepts the risk |

**Default (fast block 0 scan):**

For each entry in the persisted index:
1. Read block 0 (4 KB) at the entry's offset.
2. Verify block 0's per-block CRC (first 4 bytes vs CRC of remaining 4,092 bytes).
3. If the CRC check fails, the entry is **removed from the index** and a `WARNING` is printed to stderr.

This catches the common case: the extent was fully overwritten by a new object, so block 0's CRC no longer matches.  It does **not** catch partial overwrites where block 0 survived but later data blocks were rewritten by a different, smaller object.

**FullVerify (complete payload scan):**

For each entry in the persisted index:
1. Read all blocks of the extent (padded to 4 KB boundaries).
2. Verify every per-block CRC via `decode_blocks()`.
3. Recompute `crc32c(payload)` over the decoded payload bytes.
4. Compare against `index.crc32c`.
5. If any step fails, the entry is removed and a `WARNING` is printed.

This catches both full and partial overwrites at the cost of reading the entire data region.

**SkipVerify:**

No integrity check is performed.  Use only when you are certain the store was cleanly shut down (e.g. immediately after a successful `flush_index()`).

**After the scan**, the allocator is rebuilt from the cleaned-up index.  Space previously occupied by removed stale entries becomes free for reuse  -  no reformatting required.

### `flush_index()` API contract

`flush_index()` is the **only** way to persist state.  All writes between flush calls are crash-volatile.

The flush sequence (per-shard double-buffered index):
1. For each dirty shard: serialize its `ShardData` (file map + tombstones) with bincode, compute CRC32c.
2. Write each dirty shard's blob to the **opposite slot** from its current `active_slot` (0 -> region B, 1 -> region A).
3. `fsync` the device (all shard writes durable).
4. Update the **primary** superblock (offset 0) with the new `txn_id`, per-shard `active_slot`/`size`/`crc` metadata.
5. `fsync`.
6. Update the **backup** superblock (offset 4096) identically.
7. `fsync`.
8. Clear dirty flags.

If a crash interrupts this sequence:
- Before step 3: shard writes may be incomplete.  The old superblocks still reference the previous (valid) shard slots.  No data loss beyond unflushed changes.
- Between steps 4 and 6: primary superblock is updated, backup may be stale.  `open()` reads both and picks the one with the highest valid `txn_id`.
- After step 7: fully committed.

### Verify / fsck

The `verify` CLI command (and `verify_all()` API) performs a comprehensive offline audit:
- Reads and verifies every block of every indexed extent (per-block CRC + payload CRC).
- Checks for overlapping extents in the index.
- Validates free list consistency (used + free = data region).
- Reports per-file status: OK, CRC mismatch, out of bounds, read error.

The `repair` CLI command rebuilds the free list from the index, recovering any leaked space from orphaned extents.  Both `verify` and `repair` open the store with `OpenMode::FullVerify` to ensure stale entries are removed before analysis.

### CLI open modes

| Command | OpenMode | Rationale |
|---------|----------|-----------|
| `verify` | `FullVerify` | Comprehensive audit requires full payload CRC check |
| `repair` | `FullVerify` | Must detect and remove stale entries before rebuilding free list |
| `info` | `FullVerify` | Reports accurate file counts and space usage |
| `list`, `get`, `put`, `delete` | `Default` | Fast block 0 CRC scan is sufficient for normal operations |
| `export` | `Default` | Each file is CRC-checked on read anyway |
| `import` | N/A | Opens with `Default`; imported data is written fresh |
| `demo` | `Default` | Interactive workload, fast open preferred |
| `tombstones` | `FullVerify` | Must detect stale entries to populate tombstone list |
| `del-tombstone` | `FullVerify` | Consistent with other admin commands |
| `scrub` | `FullVerify` | Stale entries must be removed before zeroing free space |

### Tombstones

When the integrity scan removes a stale entry, it creates a **tombstone** in the index rather than silently discarding the record.  Tombstones preserve:

- The original object path (key)
- Payload size and CRC32c (from the last-flushed index)
- Last modified timestamp
- Reason for removal (e.g. "block CRC mismatch")
- Transaction ID when the tombstone was created

Tombstones are persisted in the index alongside live file entries and survive across flushes and reopens.  They serve two purposes:

1. **Visibility:** Operators can list tombstones (`rawobjstr tombstones`) to see exactly what was lost after a crash.
2. **Orchestration:** A cluster controller can read tombstones to know which objects need to be restored from a redundant backup or replica, then remove the tombstone once the object has been re-uploaded.

**API:**
- `list_tombstones()`  -  returns all tombstone entries sorted by path.
- `delete_tombstone(path)`  -  removes a single tombstone by path. Returns `false` if the path is a live file or not a tombstone. Only tombstones can be deleted via this method (safety guard).
- `clear_tombstones()`  -  removes all tombstones.

**CLI:**
- `rawobjstr tombstones --file <path>`  -  list all tombstones.
- `rawobjstr del-tombstone --file <path> --key <object-path>`  -  remove one tombstone and flush.

Tombstones do **not** occupy disk space in the data region (the stale extent's space is already free).  They only consume space in the serialized index (~120 bytes per tombstone).

### Free-space scrub

After cleanup, stale data bytes remain on disk in free regions until overwritten by new objects.  For sensitive data, use `scrub_free_space()` (or `rawobjstr scrub`) to zero all free regions immediately.

The scrub writes zeros in 1 MB chunks across every free region in the allocator, then calls `fsync`.  It does **not** modify live file data or the index  -  only free space is touched.

```bash
# Zero all free space (sensitive data erasure)
rawobjstr scrub --file /dev/nvme0n1p5
```

### Lance / LanceDB interop

Lance manages its own transaction log (`_versions/*.manifest`, `_transactions/*.txn`). On crash, Lance sees the last committed manifest on reopen. Uncommitted transactions are absent. The combination is safe: rawobjstr reverts to its last checkpoint, Lance reverts to its last committed version within that checkpoint.

**Open-mode recommendation for embedded use:**

When an application like LanceDB holds the store open for its lifetime and controls the shutdown sequence, the open-time integrity scan is unnecessary overhead:

| Scenario | Recommended OpenMode | Why |
|----------|----------------------|-----|
| Long-lived embedded process (LanceDB, CLI tool) | `SkipVerify` | Process owns the store; clean shutdown guaranteed |
| Short-lived process or after unclean shutdown | `Default` | 1 block-per-object read to catch stale extents |
| Maintenance / audit | `FullVerify` | Full payload CRC scan; use for `verify`/`repair` only |

For LanceDB specifically: Lance performs its own MVCC-style transaction isolation, so it never reads a half-written file  -  it always reads from the last committed manifest version.  This makes the rawobjstr integrity scan redundant during normal operation.  Use `open_with_mode(path, OpenMode::SkipVerify)` to eliminate the per-object pread cost on every open.

If the host process crashed without calling `flush_index()`, the recommended recovery sequence is:

```rust
// First open after unclean shutdown: run a full scan to catch stale extents.
let store = RawObjectStore::open_with_mode(path, OpenMode::FullVerify)?;
store.flush_index()?;  // persist cleaned-up state
// Subsequent opens in the same session can skip the scan.
let store = RawObjectStore::open_with_mode(path, OpenMode::SkipVerify)?;
```
