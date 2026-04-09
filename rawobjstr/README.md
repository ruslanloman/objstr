# rawobjstr

An [`ObjectStore`](https://docs.rs/object_store) implementation that writes directly to a raw block device or loopback file  -  no filesystem, no journal, no page cache overhead.

## Why?

Traditional filesystems (ext4, XFS) add overhead that's redundant when the consumer already manages its own data layout:

```
your app -> ObjectStore -> ext4 (journal, inodes, bitmaps) -> block device
                              ^ unnecessary for flat file stores
```

`rawobjstr` eliminates the middleman:

```
your app -> ObjectStore -> rawobjstr -> block device
```

### Single contiguous reads (no block reassembly)

Traditional object stores split objects into fixed-size blocks internally. Ceph RADOS shards every object into 4 MB blocks scattered across OSDs. SeaweedFS chunks large objects into 64 MB pieces. Even filesystem-backed stores like MinIO inherit ext4/XFS block scattering - the filesystem decides where each 4 KB block lands, and after normal churn the blocks of a single file can be spread across the disk.

On read, these systems must gather blocks from multiple locations, reassemble them in memory, and hand you the result. For a 128 MB Parquet file that means 32 RADOS reads, or dozens of scattered filesystem reads stitched together.

`rawobjstr` does none of that. Every object is stored as a **single contiguous extent** on the device. A 128 MB file occupies one 128 MB region - one offset, one length. Reading it back is a single `pread()` call into that contiguous region. Range reads (e.g. fetching one column chunk from a Parquet file) are also a single `pread()` into the relevant byte range, no scatter-gather, no reassembly, no extra copies.

This matters in both I/O modes:

| Mode | Why contiguous layout helps |
|------|----------------------------|
| **Buffered I/O** (loopback file) | Sequential disk layout triggers kernel read-ahead - the OS prefetches the next blocks before you ask for them. Scattered blocks defeat read-ahead entirely. |
| **O_DIRECT** (block device) | Bypasses the page cache, so there is no kernel read-ahead. But contiguous layout means the kernel submits one large sequential NVMe/SCSI command instead of many small random I/Os. The drive controller processes a single bulk transfer at full device bandwidth rather than hopping across the platter or flash translation layer. |

See [PERFORMANCE.md](PERFORMANCE.md) and [benchmarks/lance/](benchmarks/lance/) for benchmarks.

```
Traditional object store          rawobjstr
+---+ +---+ +---+ +---+          +------------------+
| 1 | | 2 | | 3 | | 4 |  blocks  |  1 2 3 4 5 6 ... |  single extent
+---+ +---+ +---+ +---+          +------------------+
  |     |     |     |                      |
 scattered across disk             one contiguous region
  |     |     |     |                      |
  v     v     v     v                      v
 4 pread() + reassemble            1 pread() -- done
```

## Features

- **Single contiguous extents**  -  every object is one contiguous region on disk; reads are a single `pread()` with no block reassembly (see above)

- **Full `ObjectStore` trait**  -  `put`, `get`, `delete`, `list`, `list_with_delimiter`, `copy`, `rename`, `head`, byte-range reads, multipart uploads
- **CRC32c checksums**  -  per-block and per-extent checksums (hardware-accelerated on x86 SSE4.2 / ARM)
- **Compression**  -  zstd, snappy, gzip (levels 0-9); selected at format time, transparent decompression on read; skipped when compressed form is not smaller
- **Crash-safe**  -  double-buffered index with superblock redundancy; recovers to last `flush_index()` checkpoint on power loss or kill -9
- **O_DIRECT support**  -  bypass the kernel page cache entirely (Linux)
- **Block device or loopback file**  -  works with `/dev/sdX`, NVMe, or a regular file
- **CLI tool**  -  format, put, get, list, delete, verify, import, export, repair, set-property, scrub, vacuum, tombstones, and more
- **Read-only mode**  -  open with `open_readonly()` for safe concurrent readers; all write operations return errors
- **Write-protect flag**  -  persist a write-protect bit in the superblock via `modify_flags()`; blocks writes even after reopen
- **Import / Export**  -  bulk-copy between any two ObjectStores (local dir, S3, raw device)  -  pack or unpack LanceDB tables as self-contained image files
- **Metadata suffix**  -  up to 65 KB of opaque metadata per object, readable without fetching the body
- **Free space scrubbing**  -  zero all free regions for secure erasure of deleted data
- **Writer exclusion**  -  `flock(LOCK_EX)` prevents multiple writer processes on the same device; readers are safe via `pread` with no lock
- **Event socket**  -  optional Unix domain socket broadcasts `PUT <key>`, `DELETE <key>`, and `FLUSH <shard_id> <txn_id>` events so readers can react in real-time instead of polling
- **Streaming block I/O**  -  batched 1 MB encode/decode for memory-efficient large object writes and reads
- **Device layout map**  -  `layout_map()` returns a full visualization of the device (superblocks, extents, free regions, index slots)
- **Python bindings**  -  native [PyO3](https://pyo3.rs) package (`rawobjstr`) with sync and async APIs, fsspec integration, and DuckDB/PyArrow/pandas support. See [python/README.md](python/README.md)

---

## Use Cases

- **Simple object store**  -  store arbitrary files in a single image, manage with CLI tools (put/get/list/delete), no database needed
- **Portable table snapshots**  -  pack a LanceDB table into a `.raw` image, ship it (scp, S3, USB), unpack or query in-place on the other end
- **S3 <-> local migration**  -  import from S3 into an image, export from image to S3
- **Embedded / IoT**  -  minimal write amplification on flash
- **Analytics appliances**  -  dedicate a disk to a single dataset; Parquet/Arrow column chunks land in contiguous extents so byte-range reads are a single `pread()` at full device bandwidth
- **Backup / archival**  -  `aws s3 sync` (or `import`) pulls thousands of small files from S3 into the image in one pass; then you copy one file instead of thousands when moving the backup to cold storage, tape, or another host. CRC32c checksums are computed per-block at write time and verified on read, so corruption is caught automatically without a separate integrity step.
- **ML model serving**  -  store model shards on a dedicated NVMe with O_DIRECT; model loads bypass the page cache and don't evict hot working data from other processes
- **CI/CD golden datasets**  -  pack test fixtures into a `.raw` image, scp to CI runners; exact byte-for-byte reproducibility with no S3 dependency at runtime
- **Air-gapped data transfer**  -  export to image, copy to USB or tape, import on the other side; CRC checksums verify integrity end-to-end without any network
- **Forensic imaging / write-once archival**  -  populate the device, set the write-protect flag, run `verify`; further writes are rejected even after reopen, preserving the bitwise state for audit
- **Live data pipelines**  -  writer ingests objects; downstream consumers subscribe via the event socket and react to `PUT`/`DELETE`/`FLUSH` events in real-time without polling
- **Feature store**  -  per-entity feature vectors stored as objects keyed by entity ID; range reads fetch specific feature subsets without loading the full record; no external database required


## Requirements

- Rust 1.70+
- Linux for O_DIRECT and block device support (loopback files work anywhere)
- `object_store` 0.12

---

## Quickstart

Clone and build the CLI tool with a single command:

```bash
git clone https://github.com/sysadminmike/objstr.git
cd objstr/rawobjstr
chmod +x build.sh
./build.sh --location ~/build-raw
```

This checks for Rust/Cargo and a C linker, builds the `rawobjstr` binary, and prints its path when done. Use `--debug` instead of the default `--release` for faster compile times during development.

Once built:

```bash
# Format a store image and put/list files
rawobjstr format --file /tmp/store.raw --size 268435456
rawobjstr put --file /tmp/store.raw --key hello.txt --from hello.txt
rawobjstr list --file /tmp/store.raw
rawobjstr get --file /tmp/store.raw --key hello.txt
```

For an S3-compatible server on top of a raw store, see the [`objstrd`](../objstrd/) crate.

To remove everything: `rm -rf ~/build-raw rawobjstr`

---

## Using It as a Simple Object Store (CLI)

The `rawobjstr` CLI binary provides a full command-line interface.
Think of it like a self-contained key-value store where the "keys" are file paths
and the "values" are arbitrary blobs  -  all packed into a single image file or block device.

### Create a store

```bash
# Create a 1 GB image file
rawobjstr format --file /tmp/my_store.raw --size 1073741824

# Or format a block device (auto-detects size)
sudo rawobjstr format --file /dev/sdb --direct-io
```

### Put files in

```bash
# Store a single file
rawobjstr put --file /tmp/my_store.raw --key docs/readme.md --from ./README.md

# Store anything  -  parquet files, images, CSVs, binaries
rawobjstr put --file /tmp/my_store.raw --key data/2024/sales.parquet --from /data/sales.parquet
```

### Get files out

```bash
# Print to stdout
rawobjstr get --file /tmp/my_store.raw --key docs/readme.md

# Save to a local file
rawobjstr get --file /tmp/my_store.raw --key data/2024/sales.parquet --to ./sales.parquet
```

### List, delete, inspect

```bash
# List all files
rawobjstr list --file /tmp/my_store.raw --long

# List files under a prefix (directory-like)
rawobjstr list --file /tmp/my_store.raw --prefix data/2024

# Delete a file
rawobjstr delete --file /tmp/my_store.raw --key docs/readme.md

# Device info (capacity, free space, file count)
rawobjstr info --file /tmp/my_store.raw

# Full integrity check (reads every extent, verifies CRCs)
rawobjstr verify --file /tmp/my_store.raw

# Rebuild free list after corruption
rawobjstr repair --file /tmp/my_store.raw
```

### All CLI commands

| Command | Description |
|---------|-------------|
| `format` | Initialize a new device/image (superblock + index regions) |
| `info` | Show device metadata: size, txn ID, file count, free space |
| `list` / `ls` | List files, optionally filtered by `--prefix`, with `--long` for sizes |
| `list-full` | List with full extent details (offset, size, CRC, compression) |
| `get` | Read a file to stdout or `--to` a local path |
| `getraw` | Read raw on-disk bytes (no decompression) |
| `getmeta` | Read only the metadata suffix of an object |
| `put` | Write a local file into the store |
| `putmeta` | Update the metadata suffix of an existing object |
| `delete` / `del` | Remove a file, return space to free list |
| `verify` | Full fsck: CRC verification, free list consistency, overlap detection |
| `import` | Bulk import from local dir, another raw device, or S3 |
| `export` | Bulk export to local dir, another raw device, or S3 |
| `repair` | Rebuild free list from index, reclaim leaked space |
| `tombstones` | List objects removed during open-time integrity scan |
| `del-tombstone` | Remove a specific tombstone entry |
| `scrub` | Zero all free regions (secure erase of deleted data) |
| `set-property` | Change device flags (write-protect, direct-io) without opening the store |
| `vacuum` | Remove stale delete markers (`__deleted__/*` keys) |
| `list-deleted` | List all delete marker keys with timestamps |

See [CLI.md](CLI.md) for the full CLI reference (all options, example output, S3 auth, workflows).

---

## Rust API

See [API.md](API.md) for the full API reference (constructors, store operations, ObjectStore trait, OpenMode, metadata extensions, and all public types).

### Quick Start  -  loopback file

```rust
use std::sync::Arc;
use rawobjstr::store::RawObjectStore;
use object_store::{ObjectStore, PutPayload, path::Path};
use bytes::Bytes;

// Format a 1 GB loopback file
let store = RawObjectStore::format_with_size(
    std::path::Path::new("/tmp/test.raw"),
    1_073_741_824,  // 1 GB
    false,          // O_DIRECT off
).unwrap();

// Use it like any ObjectStore
let rt = tokio::runtime::Runtime::new().unwrap();
rt.block_on(async {
    store.put(
        &Path::from("data/myfile.txt"),
        PutPayload::from(Bytes::from("hello")),
    ).await.unwrap();

    let result = store.get(&Path::from("data/myfile.txt")).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("hello"));
});

// Persist the index to disk
store.flush_index().unwrap();
```

See [API.md](API.md) for block device usage, Arrow/Parquet integration, and DataFusion examples.

### With LanceDB

**rawobjstr** was originally built to bypass ext4 overhead for a project built on LanceDB, `rawobjstr` proved useful enough on its own that the distributed layer (`shardedobjstr`) and the S3-compatible daemon (`objstrd`) followed naturally.

See [LANCEDB.md](LANCEDB.md) for the full guide: setup, Rust integration, crash safety, and packing/shipping tables as portable image files.


## On-Disk Layout

Three regions: superblock (8 KB), data extents (bulk of device), double-buffered index (end of device). All allocations are 4 KB aligned. CRC32c checksums on every extent and on the index.

```
 +--------+--------+-----------------------------+----------+----------+
 |  SB-A  |  SB-B  |     Data Extents            | Index A  | Index B  |
 |  4 KB  |  4 KB  |  (first-fit, 4 KB aligned)  | (16 MB)  | (16 MB)  |
 +--------+--------+-----------------------------+----------+----------+
```

See [STORAGE-FORMAT.md](STORAGE-FORMAT.md) for the full byte-level specification (superblock fields, extent block layout, index structure, crash safety details).

### Delete Markers

When a standalone `RawObjectStore` is used as a shard inside a
`ShardedObjectStore`, the cluster layer writes **delete marker** objects under
the `__deleted__/` prefix to coordinate deletes across replicas. For example,
deleting `data/foo.bin` creates a marker at `__deleted__/data/foo.bin` whose
body is the deletion timestamp (RFC 3339).

A standalone `RawObjectStore` has no replication, so deletes take effect
immediately -- no marker is needed at the raw layer. However, markers may
still exist on a device that was previously used as a shard. Two CLI commands
handle them:

```bash
# List all delete markers on a device
rawobjstr list-deleted --file /dev/sdb

# Remove stale delete markers
rawobjstr vacuum --file /dev/sdb
```

`list-deleted` prints a table of original keys, deletion timestamps, and
marker body sizes. `vacuum` deletes every `__deleted__/*` key and flushes
the index.

### Event Socket (Writer-to-Reader Push)

Optional Unix domain socket that broadcasts `PUT`, `DELETE`, and `FLUSH` events from a writer process to read-only subscribers in real time. See [EVENT-SOCKET.md](EVENT-SOCKET.md) for the wire protocol, Rust API, Python example, and shell script example.

## Limits

| Parameter | Value |
|-----------|-------|
| Minimum device size | ~32 MB + 12 KB (default 16 MB index slots) |
| Index slot size | 16 MB default, configurable (multiple of 16 MB) |
| Index capacity | ~150k files at 16 MB, ~600k at 64 MB |
| Block alignment | 4 KB |
| Max object size | Largest contiguous free extent (see below) |
| Concurrent access | Single writer (`flock`-enforced), multi-reader, multi-thread (`Arc<RwLock>`) |

### Object Size Limits

The maximum size of a single object is determined by the **largest contiguous free extent** in the allocator, not the total free space. On a freshly formatted device:

```
max_object ~ device_size - superblocks (8 KB) - index regions (2 x slot_size) - 4 KB alignment
```

For example, a fresh 4 GB device with default 16 MB index slots can hold a single object up to **~3,998 MB**.

Several factors constrain object size in practice:

| Constraint | Reason |
|------------|--------|
| **Contiguous free space** | The first-fit allocator requires a single contiguous extent. After many put/delete cycles, fragmentation may leave only small holes even if total free space is large. A device with 500 MB free might only have a largest hole of 128 MB. |
| **RAM (writes)** | For objects >= 8 MB (without compression), the write path uses streaming block I/O with incremental CRC -- peak heap is ~2 MB regardless of object size. For smaller objects or compressed writes, the payload is buffered in memory. The `ObjectStore::put()` trait requires the full `PutPayload` in memory, but `put_with_meta_from_file()` streams from disk with ~1 MB peak heap. Range reads only load the blocks that overlap the requested byte range. |
| **Index slot capacity** | Each file entry is ~100-200 bytes serialized (path + metadata). The 16 MB default index slot holds ~150K entries. A single huge object won't hit this, but many objects will. |

There is no artificial size cap in the code -- `ExtentInfo.size` is a `u64`.

## Limitations

- **No defragmentation** - the first-fit extent allocator never moves or compacts extents. After heavy churn (many put + delete cycles), free space becomes fragmented. Total free space may be large but no single contiguous region is big enough for a new object. The `repair` command rebuilds the free list but does not move data.

- **Single-writer enforcement** - the store uses `flock(LOCK_EX)` to prevent concurrent writer processes on the same device. A second read-write open returns `DeviceLocked`. Multiple read-only opens are allowed alongside the writer. On filesystems that do not support `flock` (e.g. some network filesystems), concurrent writers could corrupt the index.

- **Index flush is explicit** - writes are only durable after `flush_index()`. Unflushed writes are lost on crash (the data extents survive on disk but the index doesn't reference them until `repair` reclaims them). The store warns on Drop if there are unflushed changes.

- **Multipart complete() RAM** - individual `put_part()` calls spool to disk (O(part) RAM). `complete()` streams parts back one at a time, computing a CRC32C incrementally and writing blocks to the device. Memory during `complete()` is O(part_size), not O(total_payload). **Exception:** the Snappy codec must buffer the entire uncompressed payload in RAM before compressing; for large objects, prefer Zstd or no compression.

## Future Improvements

### Online defragmentation / repacking

The most impactful improvement would be an offline or online **repack** operation that compacts all live extents to the front of the data region, eliminating fragmentation and restoring a single large free extent.

**Offline repack** (simplest):
1. Format a new image: `format --file /tmp/new.raw --size <size>`
2. Import from the old one: `import --file /tmp/new.raw --from raw:///tmp/old.raw`
3. All extents are written contiguously into the fresh image with no gaps.

**Online defrag via sharded repair-replication**:

When a raw device is used as a shard in a `ShardedObjectStore` cluster, you can defragment it without downtime by using the repair-replication mechanism:
1. Temporarily increase the replication factor or add a spare shard
2. Drain the fragmented shard  -  repair-replication moves all its objects to other shards
3. Format the now-empty device fresh (`rawobjstr format`)
4. Re-add the shard and repair-replication again  -  objects flow back as contiguous extents on a clean device
5. Remove the spare shard if one was added

This leverages the cluster's ability to redistribute objects across shards and avoids any single-device crash-safety complexity  -  repair-replication is already idempotent and resumable. See the `shardedobjstr` [README](../shardedobjstr/README.md) for repair-replication details.

## Crash Safety & Durability

**Not durable-by-default.** You must call `flush_index()` to persist state. Everything between flush calls is lost on crash. Double-buffered index + redundant superblocks ensure the last successful flush always survives.

Call `flush_index()` periodically (e.g. every 30 seconds) to bound the data-loss window. After a crash, use `repair` to reclaim leaked space from unflushed writes.
