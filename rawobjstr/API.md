# API Reference

## RawObjectStore

The main type. Implements the [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) trait from `object_store` 0.12.



## Rust API

### Quick Start  -  loopback file

```rust
use std::sync::Arc;
use rawobjstr::store::RawObjectStore;
use object_store::{ObjectStore, PutPayload, path::Path};
use bytes::Bytes;

// Format a 1 GB loopback file
let store = RawObjectStore::format_with_size(
    std::path::Path::new("/tmp/demo.raw"),
    1_073_741_824,  // 1 GB
    false,          // O_DIRECT off
).unwrap();

// Use it like any ObjectStore
let rt = tokio::runtime::Runtime::new().unwrap();
rt.block_on(async {
    store.put(
        &Path::from("data/test.txt"),
        PutPayload::from(Bytes::from("hello")),
    ).await.unwrap();

    let result = store.get(&Path::from("data/test.txt")).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("hello"));
});

// Persist the index to disk
store.flush_index().unwrap();
```

### With a block device

```rust
use rawobjstr::store::RawObjectStore;

// Format /dev/sdb (auto-detects size via BLKGETSIZE64 ioctl)
let store = RawObjectStore::format(
    std::path::Path::new("/dev/sdb"),
    true,  // O_DIRECT on  -  recommended for block devices
).unwrap();

// Later, reopen (O_DIRECT flag is persisted in superblock)
let store = RawObjectStore::open(std::path::Path::new("/dev/sdb")).unwrap();
```

### With Arrow / Parquet (write + read back)

```rust
use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, path::Path};
use rawobjstr::store::RawObjectStore;

let store = Arc::new(
    RawObjectStore::open(std::path::Path::new("/dev/sdb")).unwrap()
);

// Build an Arrow RecordBatch
let batch = RecordBatch::try_from_iter(vec![
    ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
    ("name", Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as _),
]).unwrap();

// Write as Parquet
let mut buf = Vec::new();
let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
writer.write(&batch).unwrap();
writer.close().unwrap();
store.put(&Path::from("data.parquet"), PutPayload::from(Bytes::from(buf))).await.unwrap();
store.flush_index().unwrap();

// Read it back
let bytes = store.get(&Path::from("data.parquet")).await.unwrap().bytes().await.unwrap();
let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1024).unwrap();
for batch in reader {
    println!("{:?}", batch.unwrap());
}
```

### With Arrow IPC (streaming format)

```rust
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;

// Write
let mut buf = Vec::new();
let mut writer = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
writer.write(&batch).unwrap();
writer.finish().unwrap();
store.put(&Path::from("data.arrow"), PutPayload::from(Bytes::from(buf))).await.unwrap();

// Read
let bytes = store.get(&Path::from("data.arrow")).await.unwrap().bytes().await.unwrap();
let reader = FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
for batch in reader {
    println!("{:?}", batch.unwrap());
}
```

### With DataFusion

Register the store under a custom URL scheme and query Parquet files with SQL:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use url::Url;
use rawobjstr::store::RawObjectStore;

// Open or format a store, then wrap in Arc for DataFusion
let store = RawObjectStore::open(std::path::Path::new("/tmp/store.raw")).unwrap();
let store = Arc::new(store);

let ctx = SessionContext::new();

// The URL must have a host (e.g. "localhost") - scheme-only URLs won't work
let url = Url::parse("raw://localhost").unwrap();
ctx.register_object_store(&url, store.clone());

// Register a Parquet file stored in the raw device as a table
ctx.register_parquet("my_table", "raw://localhost/data.parquet",
    ParquetReadOptions::default()).await.unwrap();

let df = ctx.sql("SELECT * FROM my_table WHERE id > 1").await.unwrap();
df.show().await.unwrap();
```

This requires `datafusion`, `url`, and `parquet` as dependencies.

### Construction

| Method | Description |
|--------|-------------|
| `RawObjectStore::format(path, direct_io)` | Format a block device (auto-detect size via `BLKGETSIZE64` ioctl) |
| `RawObjectStore::format_with_size(path, size, direct_io)` | Format a loopback file of given size (default 16 MB index slots) |
| `RawObjectStore::format_with_options(path, opts)` | Format with explicit `FormatOptions` (custom index slot size) |
| `RawObjectStore::open(path)` | Open an existing formatted device/image with `OpenMode::Default` |
| `RawObjectStore::open_with_mode(path, mode)` | Open with an explicit `OpenMode` (skip scan, fast scan, or full verify) |
| `RawObjectStore::open_readonly(path)` | Open in read-only mode (`OpenMode::Default`). All write operations return `ReadOnly` |
| `RawObjectStore::open_readonly_with_mode(path, mode)` | Open read-only with an explicit `OpenMode` |
| `RawObjectStore::modify_flags(path, set, clear)` | Set/clear superblock flags on a closed device. Returns the new flags value |

### Store Operations

| Method | Description |
|--------|-------------|
| `store.flush_index()` | Persist in-memory index to disk (crash-safe, double-buffered) |
| `store.reload_index()` | Re-read on-disk index if a writer has flushed since last load. Returns `Ok(true)` if refreshed |
| `store.add_flush_callback(cb)` | Register a callback invoked after each `flush_index()` with the new `txn_id` |
| `store.device_info()` | Get device statistics and metadata (`DeviceInfo`) |
| `store.device_path()` | Return the path of the device or image file |
| `store.needs_flush()` | Whether the store has dirty (unflushed) changes |
| `store.is_read_only()` | Returns `true` if the store was opened via `open_readonly` / `open_readonly_with_mode` |
| `store.max_key_length()` | Return the enforced maximum key length in bytes |
| `store.compression()` | Return the compression algorithm configured for this device |
| `store.verify_all()` | Full integrity check: CRC verification, overlap detection, free list consistency |
| `store.repair()` | Rebuild free list from index and re-flush |
| `store.import_from(&source, prefix)` | Import files from any ObjectStore (parallel, up to 8 concurrent fetches). Returns `ImportReport` |
| `store.export_to(&target)` | Export all files to another ObjectStore (parallel, up to 8 concurrent). Returns `ExportReport` |
| `store.list_tombstones()` | Return all tombstone entries created during the last open-time integrity scan |
| `store.delete_tombstone(path)` | Remove a single tombstone (returns `false` if path is still a live file) |
| `store.clear_tombstones()` | Remove all tombstones; returns the count removed |
| `store.scrub_free_space()` | Zero all free regions on device in 1 MB chunks then fsync (`ScrubReport`) |
| `store.get_raw(&path)` | Read raw on-disk bytes without decompression (`RawGetResult`) |
| `store.list_full(prefix)` | Return full extent info (`ObjectFullInfo`) for all objects (index-only, zero data reads) |
| `store.layout_map()` | Return full device layout for visualization (`DeviceLayout`) |

### Metadata-Aware Extensions

These methods are used by `objstrd` to store per-object S3 metadata (ETag, content type)
as an opaque suffix appended to the body. Direct callers can also use them for
application-specific metadata.

| Method | Description |
|--------|-------------|
| `store.put_with_meta(&path, body, metadata)` | Store body + opaque metadata bytes as a single extent |
| `store.put_with_meta_from_file(&path, file, meta_len)` | Store body + metadata from a file that already contains both |
| `store.head_with_meta(&path)` | Return `ObjectMeta` and `meta_len` for an object (index-only, no data I/O) |
| `store.list_with_meta(prefix)` | List objects with their `meta_len` values (index-only, zero data reads) |
| `store.get_metadata(&path)` | Read only the metadata suffix of an object |
| `store.set_meta_len(&path, meta_len)` | Update the `meta_len` in the index for an existing object |
| `store.update_metadata(&path, metadata)` | Rewrite an existing object with new metadata bytes, preserving the body |

### ObjectStore Trait Methods

All standard `ObjectStore` methods are implemented:

| Method | Description |
|--------|-------------|
| `put(location, payload)` | Write an object (overwrite mode) |
| `put_opts(location, payload, opts)` | Write with options: `Create` (fail if exists), `Overwrite`, `Update` |
| `put_multipart_opts(location, opts)` | Start a multipart upload (parts spooled to disk, assembled on complete) |
| `get(location)` | Read a full object |
| `get_opts(location, options)` | Read with byte-range support (`Bounded`, `Offset`, `Suffix`) |
| `head(location)` | Get object metadata without reading data |
| `delete(location)` | Delete an object (returns `Ok(())` if not found) |
| `list(prefix)` | List objects matching prefix (path-segment aware) |
| `list_with_delimiter(prefix)` | List with directory-like grouping |
| `copy(from, to)` | Copy via raw block copy (no decode/re-encode) |
| `copy_if_not_exists(from, to)` | Atomic copy-if-absent |
| `rename(from, to)` | O(1) index-only rename (no data movement) |
| `rename_if_not_exists(from, to)` | Atomic rename-if-absent |

### OpenMode

Controls the extent integrity check performed when opening an existing device.

```rust
pub enum OpenMode {
    /// Fast block-0 scan (default).  Reads the first 4 KB block of every
    /// indexed extent and verifies the per-block CRC32c.
    /// Cost: one 4 KB pread per object.  Catches full overwrites.
    Default,

    /// Full payload verification.  Reads every block of every extent,
    /// verifies per-block CRCs, then recomputes the payload CRC and
    /// compares it against the index.  Catches partial overwrites too.
    /// Use for maintenance / post-crash audit.
    FullVerify,

    /// Skip all integrity checks on open (zero extra I/O).
    /// Recommended for long-lived embedded processes (e.g. LanceDB) that
    /// own the store for their lifetime and guarantee clean shutdown.
    SkipVerify,
}
```

| Mode | Cost | Use when |
|------|------|----------|
| `Default` | 1 pread per object (4 KB each) | Normal reopens, short-lived processes |
| `FullVerify` | All blocks of every object | Post-crash audit, maintenance tools |
| `SkipVerify` | Zero | Long-lived embedded use (LanceDB, servers) |

Objects that fail the integrity check during `Default` or `FullVerify` are removed
from the live index and recorded as **tombstones**.  They do not appear in `list()`
or `get()` but are accessible via `list_tombstones()`.

### FormatOptions

```rust
pub struct FormatOptions {
    pub device_size: u64,       // Total device size in bytes
    pub direct_io: bool,        // Use O_DIRECT (Linux only)
    pub index_slot_size: u64,   // Index slot capacity (multiple of 16 MB)
    pub max_key_length: usize,  // Maximum object key length in bytes.
                                // Default: 1024 (S3 compatible). Hard ceiling: 65536 (64 KB).
                                // Values above shard_slot_size - 98 are silently clamped.
    pub compression: Compression, // Compression algorithm (None, Zstd, Snappy, Gzip0..Gzip9).
                                  // Applied to all objects >= 4096 bytes. Default: None.
}
```

### Compression

```rust
#[repr(u8)]
pub enum Compression {
    None   = 0,
    Zstd   = 1,
    Snappy = 2,
    Gzip0  = 3,   // gzip level 0 (store only)
    Gzip1  = 4,
    Gzip2  = 5,
    Gzip3  = 6,
    Gzip4  = 7,
    Gzip5  = 8,
    Gzip6  = 9,   // gzip default
    Gzip7  = 10,
    Gzip8  = 11,
    Gzip9  = 12,  // gzip max
}
```

Compression is set at format time and applies to all objects on the device.
Objects smaller than 4096 bytes are never compressed. If compression does not
reduce the size, the object is stored uncompressed (indicated by
`ExtentInfo.uncompressed_size == 0`).

| Method | Description |
|--------|-------------|
| `Compression::from_u8(v)` | Parse from the on-disk byte value |
| `Compression::from_str_name(s)` | Parse from a name string (e.g. `"zstd"`, `"gzip9"`) |
| `compression.as_str()` | Return the name string |
| `compression.compress(data)` | Compress a byte slice |
| `compression.decompress(data)` | Decompress a byte slice |

### RawGetResult

Returned by `store.get_raw(&path)`.

| Field | Type | Description |
|-------|------|-------------|
| `data` | `Bytes` | Exact bytes stored on disk (compressed if applicable) |
| `uncompressed_size` | `u64` | Original size before compression. 0 = not compressed |
| `compression` | `Compression` | Compression algorithm configured on this device |

### TombstoneEntry

Returned by `store.list_tombstones()`.  Each entry represents an object that was
removed from the live index during the open-time integrity scan.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `String` | Original ObjectStore path |
| `size` | `u64` | Original payload size in bytes |
| `crc32c` | `u32` | Original CRC32c of the payload |
| `last_modified` | `DateTime<Utc>` | When the object was last written |
| `reason` | `String` | Why the entry was removed (e.g. "block 0 CRC mismatch") |
| `tombstone_txn` | `u64` | Transaction ID when the tombstone was created |

Tombstones are persisted in the index across reopens (via `flush_index()`).  Writing
a new object at a tombstoned path automatically removes the tombstone.

### ScrubReport

Returned by `store.scrub_free_space()`.

| Field | Type | Description |
|-------|------|-------------|
| `regions_scrubbed` | `usize` | Number of free regions zeroed |
| `bytes_scrubbed` | `u64` | Total bytes written (all zero) |

Scrub only zeros the free-space allocator regions.  Live object data is never touched.

### ObjectFullInfo

Returned by `store.list_full(prefix)`.  Each entry provides full extent-level
detail for a stored object (index-only, zero data reads).

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | ObjectStore key (path) |
| `body_size` | `u64` | Body size in bytes (minus metadata suffix) |
| `meta_len` | `u16` | Metadata suffix size in bytes |
| `last_modified` | `DateTime<Utc>` | When the object was last written |
| `created_txn` | `u64` | Transaction ID when the object was created |
| `offset` | `u64` | Byte offset of extent on device |
| `padded_size` | `u64` | On-disk size including padding |

### LayoutExtent

A single extent in the device layout map returned by `store.layout_map()`.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | ObjectStore key |
| `offset` | `u64` | Byte offset on device |
| `size` | `u64` | On-disk payload size (compressed if applicable) |
| `padded_size` | `u64` | On-disk size including header + padding |
| `created_txn` | `u64` | Transaction ID when the object was created |
| `last_modified` | `DateTime<Utc>` | When the object was last written |
| `uncompressed_size` | `u64` | Original uncompressed size (0 = not compressed) |

### DeviceLayout

Complete device layout for visualization, returned by `store.layout_map()`.

| Field | Type | Description |
|-------|------|-------------|
| `device_size` | `u64` | Total device size |
| `data_region_start` | `u64` | Byte offset where the data region begins |
| `data_region_end` | `u64` | Byte offset where the data region ends |
| `index_region_a` | `u64` | Byte offset of index region A |
| `index_region_b` | `u64` | Byte offset of index region B |
| `active_index_region` | `u64` | Currently active index region (A or B) |
| `txn_id` | `u64` | Current transaction counter |
| `extents` | `Vec<LayoutExtent>` | Used extents sorted by offset |
| `free_regions` | `Vec<(u64, u64)>` | Free regions sorted by offset: (offset, size) |

### VerifyReport

Returned by `store.verify_all()`.

| Field | Type | Description |
|-------|------|-------------|
| `files_checked` | `usize` | Total files verified |
| `files_ok` | `usize` | Files that passed verification |
| `errors` | `Vec<ExtentVerifyResult>` | Files that failed verification |
| `ok_files` | `Vec<ExtentVerifyResult>` | Files that passed (for detailed inspection) |
| `overlapping_extents` | `Vec<(String, String)>` | Pairs of keys whose extents overlap on disk |
| `free_list_consistent` | `bool` | Whether the free list matches the used extents |
| `space_accounted` | `bool` | Whether used + free = total data region |
| `total_data_region` | `u64` | Total data region size in bytes |
| `total_used` | `u64` | Total bytes occupied by extents |
| `total_free` | `u64` | Total bytes in the free list |

### ExtentVerifyResult

Result of verifying a single file's extent.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `String` | ObjectStore key |
| `offset` | `u64` | Byte offset of extent on device |
| `expected_size` | `u64` | Expected extent payload size |
| `status` | `VerifyStatus` | `Ok`, `CrcMismatch`, `BlockCorrupt`, `OutOfBounds`, or `ReadError` |

### RepairReport

Returned by `store.repair()`.

| Field | Type | Description |
|-------|------|-------------|
| `free_list_rebuilt` | `bool` | Whether the free list was rebuilt |
| `old_free_entries` | `usize` | Free list entries before repair |
| `new_free_entries` | `usize` | Free list entries after repair |
| `old_free_space` | `u64` | Free space before repair |
| `new_free_space` | `u64` | Free space after repair |
| `flushed` | `bool` | Whether the index was flushed after repair |
| `files_found` | `usize` | Number of files found in the index |
| `used_extents` | `Vec<(String, u64, u64)>` | (path, offset, padded_size) sorted by offset |
| `new_free_list` | `Vec<(u64, u64)>` | (offset, size) sorted by offset |

### ImportReport

Returned by `store.import_from()`.

| Field | Type | Description |
|-------|------|-------------|
| `files_imported` | `usize` | Number of files successfully imported |
| `bytes_imported` | `u64` | Total bytes imported |
| `errors` | `Vec<(String, String)>` | (path, error message) for files that failed |

### ExportReport

Returned by `store.export_to()`.

| Field | Type | Description |
|-------|------|-------------|
| `files_exported` | `usize` | Number of files successfully exported |
| `bytes_exported` | `u64` | Total bytes exported |
| `errors` | `Vec<(String, String)>` | (path, error message) for files that failed |

### DeviceInfo

Returned by `store.device_info()`:

| Field | Type | Description |
|-------|------|-------------|
| `device_path` | `String` | Path to the device/file |
| `device_size` | `u64` | Total device size |
| `format_version` | `u32` | Superblock version (currently 4) |
| `flags` | `u32` | Feature flags (e.g. `FLAG_DIRECT_IO`, `FLAG_WRITE_PROTECT`) |
| `direct_io` | `bool` | Whether O_DIRECT is active |
| `txn_id` | `u64` | Current transaction counter |
| `index_slot_capacity` | `u64` | Maximum bytes available per index slot (A or B) |
| `index_serialized_bytes` | `u64` | Sum of all active shard sizes on disk |
| `index_region_a` | `u64` | Byte offset of index region A |
| `index_region_b` | `u64` | Byte offset of index region B |
| `active_index_region` | `u64` | Byte offset of the currently-active index region |
| `file_count` | `usize` | Number of stored objects |
| `data_bytes_stored` | `u64` | Total payload bytes |
| `device_bytes_used` | `u64` | Total on-disk bytes (including headers/padding) |
| `free_space` | `u64` | Total free space in data region |
| `free_fragments` | `usize` | Number of free list entries |
| `largest_free_extent` | `u64` | Size of largest contiguous hole |
| `data_region_start` | `u64` | Byte offset where the data region begins |
| `data_region_end` | `u64` | Byte offset where the data region ends |
| `last_flush_bytes` | `u64` | Bytes written in the most recent `flush_index()` call |
| `max_key_length` | `usize` | Maximum allowed key length in bytes (as stored in the superblock; may be lower than the requested value if clamped to the shard slot ceiling) |
| `shard_sizes` | `Vec<u32>` | Serialized size in bytes of each of the 256 index shards |
| `shard_slot_size` | `u64` | Maximum capacity of a single shard slot (`index_slot_capacity / NUM_SHARDS`) |
| `compression` | `Compression` | Compression algorithm configured for this device |

---

## Behavioral Notes

### Tombstones and Open-time Integrity

When opening with `OpenMode::Default` or `OpenMode::FullVerify`, the store scans every
indexed extent for evidence of a stale pointer - the disk content no longer matches
what the index recorded (lost because the process crashed between the write and the
next `flush_index()`).

Any object that fails the check is:
1. Removed from the live files map (it will not appear in `list()` or be readable via `get()`).
2. Inserted into the **tombstone map** with a reason string and the current transaction ID.

Tombstones survive across `flush_index()` + close + reopen cycles.  They are visible via
`list_tombstones()` so an orchestrator can restore the affected objects from a backup or
replica before clearing them.

```rust
// Inspect what was lost after a crash recovery
let tombstones = store.list_tombstones();
for t in &tombstones {
    println!("{}: {} bytes -- {}", t.path, t.size, t.reason);
}

// Restore from backup, then clear the tombstone
store.put(&Path::from(&t.path), payload).await?;
// put() at a tombstoned path automatically removes the tombstone.

// Or discard all tombstones without restoring
let n = store.clear_tombstones();
store.flush_index()?;
```

**`delete_tombstone(path)`** returns `false` if `path` is a live file (safety guard --
calling it on an existing object is a no-op).

### scrub_free_space()

After objects are deleted or declared stale by the integrity scan, their raw bytes remain
on disk in the free regions until a new object overwrites them.  For sensitive data, call
`scrub_free_space()` to zero every free region immediately:

```rust
let report = store.scrub_free_space()?;
println!("Scrubbed {} regions, {} bytes", report.regions_scrubbed, report.bytes_scrubbed);
```

Scrub does not modify live object data, does not update the index, and calls `fsync`
once after zeroing all regions.  CLI equivalent: `rawobjstr scrub --file <path>`.

---

### flush_index() and Drop

The store tracks dirty shards internally. Any mutation (put, delete, copy, rename, multipart complete/abort) marks the affected shard(s) as dirty. Calling `flush_index()` writes only the dirty shards to disk (256 shards partitioned by `crc32c(key) % 256`), then updates the superblock. After flushing, `device_info().last_flush_bytes` reports the actual bytes written.

**If the store is dropped while dirty, a warning is printed to stderr.** This prevents silent data loss from forgetting to flush. (Stores opened in read-only mode skip the dirty warning on drop.)

```
WARNING: RawObjectStore(/tmp/store.raw) dropped with unflushed changes.
Call flush_index() before dropping to persist data.
```

### rename() -- O(1) index-only operation

Unlike the default ObjectStore `rename()` (which does copy + delete), our `rename()` is O(1): it moves the key in the in-memory index without any disk I/O for data. The extent stays at the same physical offset. This makes rename effectively free for any object size.

`rename_if_not_exists()` provides the atomic variant that fails if the destination already exists.

**Both require a subsequent `flush_index()` to persist the rename.**

### Read-only Mode

Open a store in read-only mode with `open_readonly()` or `open_readonly_with_mode()`.
The device file is opened with `O_RDONLY`  -  no writes hit disk, not even during drop.

```rust
let store = RawObjectStore::open_readonly(std::path::Path::new("/tmp/store.raw")).unwrap();
assert!(store.is_read_only());

// All reads work normally
let data = store.get(&Path::from("key")).await.unwrap().bytes().await.unwrap();
let files: Vec<_> = store.list(None).try_collect().await.unwrap();
let meta = store.head(&Path::from("key")).await.unwrap();
let info = store.device_info();
let report = store.verify_all();
let tombstones = store.list_tombstones();

// All writes return RawStoreError::ReadOnly
store.put(&path, payload).await.unwrap_err();       // ReadOnly
store.delete(&path).await.unwrap_err();              // ReadOnly
store.copy(&from, &to).await.unwrap_err();           // ReadOnly
store.rename(&from, &to).await.unwrap_err();         // ReadOnly
store.flush_index().unwrap_err();                     // ReadOnly
store.repair().unwrap_err();                          // ReadOnly
store.scrub_free_space().unwrap_err();                // ReadOnly
store.delete_tombstone("key").unwrap_err();           // ReadOnly
store.clear_tombstones().unwrap_err();                // ReadOnly
```

**Key behaviors in read-only mode:**
- The allocator is not loaded (saves memory and startup I/O).
- `FullVerify` still scans all extents but does **not** record tombstones on disk.
  Corrupt entries are removed from the in-memory index only.
- Drop does not print a dirty warning, even if the in-memory index was modified.
- Multiple read-only opens on the same device work concurrently (since no writes occur).

### Write-Protect Flag

The `FLAG_WRITE_PROTECT` superblock flag prevents any read-write open:

```rust
use rawobjstr::{FLAG_WRITE_PROTECT};

// Set write-protect on a closed device
RawObjectStore::modify_flags(path, FLAG_WRITE_PROTECT, 0).unwrap();

// RW open now fails
RawObjectStore::open(path).unwrap_err();                   // WriteProtected
RawObjectStore::open_with_mode(path, mode).unwrap_err();   // WriteProtected

// RO open still works
let store = RawObjectStore::open_readonly(path).unwrap();

// Clear write-protect
RawObjectStore::modify_flags(path, 0, FLAG_WRITE_PROTECT).unwrap();
RawObjectStore::open(path).unwrap();  // works again
```

### modify_flags()

`modify_flags(path, set_flags, clear_flags)` atomically reads the superblock, applies
flag changes, and writes both superblock copies. The store must not be open. Returns
the new flags value.

```rust
// Set write-protect
let flags = RawObjectStore::modify_flags(path, FLAG_WRITE_PROTECT, 0).unwrap();
assert_ne!(flags & FLAG_WRITE_PROTECT, 0);

// Clear direct-io
let flags = RawObjectStore::modify_flags(path, 0, FLAG_DIRECT_IO).unwrap();

// Query current flags without changing anything
let flags = RawObjectStore::modify_flags(path, 0, 0).unwrap();
```

Only bits in `FLAG_KNOWN_MASK` (`FLAG_DIRECT_IO | FLAG_WRITE_PROTECT`) are accepted.
Unknown bits return an error.

### Multipart Upload

Multipart uploads spool each part to a temporary on-disk extent (prefixed with `__raw_multipart_tmp/`). This means:
- Individual `put_part()` calls write to disk immediately
- Memory usage during upload: O(single part size), not O(total upload size)
- On `complete()`: parts are read back, concatenated, and written as the final object
- On `abort()`: temporary extents are freed
- Temporary part keys are invisible to `list()` and `list_with_delimiter()`

The assembly during `complete()` streams parts to disk using `StreamingBlockWriter`,
so memory usage is O(single part size) - not O(total upload size). See
**I/O Streaming Behavior** below for details on compressed vs. uncompressed paths.

### delete() returns Ok(()) for missing keys

Per the ObjectStore trait contract, `delete()` returns `Ok(())` even if the object does not exist. This is idempotent - deleting the same key twice is not an error.

### list() prefix matching

Prefix matching uses path-segment semantics (via `Path::prefix_matches`):
- `list(Some("data"))` matches `data/foo` and `data/bar/baz` but NOT `datafile`
- `list(None)` returns all objects

### Index validation on open

When opening an existing device, the index is validated:
- Entries pointing outside the data region are removed with a warning
- Overlapping extents are detected; the later extent (by offset) is removed
- A free list is rebuilt from the validated entries

---

### Zero-byte Objects

Zero-byte objects are not supported. Calling `put()` with an empty payload returns
`RawStoreError::EmptyPayload`. Multipart uploads with zero parts also return this error.

This is a deliberate design decision: the on-disk format requires at least one 4 KB
block per object (for CRC protection), and zero-byte objects would be degenerate entries
with no blocks and no CRC. Rejecting them simplifies validation.

**S3 compatibility note:** The `objstrd` S3 adapter stores per-object metadata (ETag,
content type) as a suffix appended to the body via `put_with_meta()`. A zero-body S3
PUT still results in a non-empty payload on disk (the metadata suffix), so S3 clients
will never see this error. Only direct `RawObjectStore` callers can encounter it.

---

### I/O Streaming Behavior

#### Writes (put)

The `put()` path receives the full payload as a `Bytes` value, then writes it to disk
in 1 MB batches of CRC-protected 4 KB blocks. This keeps peak heap usage to roughly
`payload_size + 1 MB` (the batch buffer). The `ObjectStore` trait requires the full
payload up front, so there is no way to avoid materializing it.

For multipart uploads, each `put_part()` writes its part to a temporary on-disk extent
immediately. On `complete()`, parts are read back and streamed to the final extent
using a `StreamingBlockWriter` that processes 1 MB at a time - peak memory during
assembly is O(single part) + 1 MB, not O(total object).

#### Reads (get)

**Uncompressed objects:** `get()` returns a lazy stream backed by a
`StreamingBlockReader`. Blocks are read and CRC-verified in 1 MB batches as the
consumer pulls from the stream. The full object is never buffered in memory - each
chunk is yielded and can be dropped before the next is read. This applies to objects
of any size.

**Range reads on uncompressed objects** are truly partial: only the blocks overlapping
the requested byte range are read from disk.

**Compressed objects:** The entire compressed payload must be read and decompressed
before any bytes can be returned, because the compression frame spans the whole object.
For range reads on compressed objects larger than 1 GB (`COMPRESSED_RANGE_READ_MAX`),
the request is rejected to prevent unbounded memory usage.

---

### Compression Behavior

Compression is set at format time and applies transparently to all objects >= 4096 bytes.
Objects smaller than 4096 bytes are always stored uncompressed. If compression does not
reduce size, the object is stored uncompressed (indicated by `uncompressed_size == 0`
in the index entry).

#### Compression during put

The full payload is compressed **in memory** before writing. Peak memory during a
compressed put is roughly `2 * payload_size` (original + compressed buffer). There is
no streaming compression for single puts - the entire payload must fit in RAM twice.

#### Decompression during get

The full compressed payload is read from disk and decompressed **in memory** before
being returned as a single-chunk stream. Unlike uncompressed gets, there is no lazy
streaming - the entire decompressed object is materialized.

#### Multipart complete with compression

When compression is enabled, `complete()` stream-compresses parts into a temporary file
on the OS filesystem (via `tempfile::tempfile()`):

- **Zstd and Gzip:** Parts are read one at a time, decompressed if individually
  compressed, and fed through a streaming encoder to the temp file. Peak memory:
  O(single part).
- **Snappy:** All parts must be buffered in memory (snappy has no streaming API).
  Peak memory: O(total uncompressed size).

After compression, the temp file is streamed to the device in 1 MB batches. The temp
file is automatically deleted when the operation completes or fails.

#### Recommendation for large objects

Compression is not designed for very large objects (10 GB+). For puts, the full payload
must be held in memory during compression. For gets, the full payload must be
decompressed before any bytes are returned.

**Recommended pattern for large files:** pre-compress the data before storing it
(e.g. with `zstd` on the command line) and use a store formatted with
`compression: None`. This lets the store stream data in and out without buffering,
and avoids the temp-file overhead during multipart assembly. The application controls
the compression and can stream-decompress on read.

If the speed penalty and temp-file disk usage are acceptable for your workload, the
built-in compression works for large files - but pre-compression gives the best
throughput and lowest memory usage.

---

## Public API Surface

The public API is intentionally minimal:

| Module | Public Items |
|--------|-------------|
| `store` | `RawObjectStore`, `OpenMode`, `FormatOptions`, `DeviceInfo`, `ObjectFullInfo`, `LayoutExtent`, `DeviceLayout`, `VerifyReport`, `RepairReport`, `ImportReport`, `ExportReport`, `VerifyStatus`, `ExtentVerifyResult`, `TombstoneEntry`, `ScrubReport`, `RawGetResult` |
| `lib` | `RawStoreError`, `Compression`, constants (`DATA_START`, `BLOCK_ALIGNMENT`, `INDEX_REGION_SIZE`, `FLAG_DIRECT_IO`, `FLAG_WRITE_PROTECT`, `FLAG_KNOWN_MASK`, `COMPRESSED_RANGE_READ_MAX`, `BUILD_GIT_HASH`, `BUILD_DATE`, `VERSION`, etc.) |
| `event` | `StoreEvent`, `EventBus`, `parse_event()`, `subscribe_events_tcp()`, `subscribe_events_auto()` |
| `event::unix` | `EventServer`, `EventLogFn`, `subscribe_events()` (Unix-only, `#[cfg(unix)]`) |
| `extent` | `padded_extent_size()` (useful for capacity planning) |
| `io` | `DeviceIo` (for benchmarks and advanced usage) |
| `superblock` | `Superblock`, `ShardSlotMeta` (for low-level inspection) |

Internal types (`ExtentAllocator`, `DeviceIndex`, `ExtentInfo`, block encoding functions) are `pub(crate)` and not part of the stable API.
