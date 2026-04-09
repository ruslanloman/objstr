use std::collections::HashSet;
use std::fmt;
use std::path::Path as StdPath;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutMode, PutOptions, PutPayload, PutResult,
};
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::allocator::ExtentAllocator;
use crate::extent::padded_extent_size;
use crate::index::{DeviceIndex, ExtentInfo, ShardData, TombstoneInfo, shard_for_key};
use crate::io::DeviceIo;
use crate::superblock::Superblock;
use crate::{
    BLOCK_ALIGNMENT, Compression, COMPRESSED_RANGE_READ_MAX, DATA_START,
    FLAG_DIRECT_IO, INDEX_REGION_SIZE, NUM_SHARDS,
};

/// Store name used in `object_store::Error::Generic` messages.
const STORE_NAME: &str = "RawObjectStore";

/// Prefix for temporary multipart upload extents stored on disk.
/// These keys are filtered from list() and list_with_delimiter() results.
const MULTIPART_TMP_PREFIX: &str = "__raw_multipart_tmp/";

/// Build a free list as the complement of the valid used extents
/// in `[DATA_START, data_end)`.  Returns `(used_sorted, free_list)`.
fn rebuild_free_list(
    index: &DeviceIndex,
    data_end: u64,
) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
    let mut used: Vec<(u64, u64)> = index
        .files
        .values()
        .filter(|e| {
            e.offset >= DATA_START
                && e.padded_size > 0
                && e.offset.saturating_add(e.padded_size) <= data_end
        })
        .map(|e| (e.offset, e.padded_size))
        .collect();
    used.sort_by_key(|(o, _)| *o);

    let mut free = Vec::new();
    let mut cursor = DATA_START;
    for &(offset, psize) in &used {
        if offset > cursor {
            free.push((cursor, offset - cursor));
        }
        cursor = offset + psize;
    }
    if cursor < data_end {
        free.push((cursor, data_end - cursor));
    }
    (used, free)
}

/// All mutable state protected by a single mutex, eliminating lock-ordering
/// and TOCTOU issues.
struct Inner {
    io: Arc<DeviceIo>,
    index: DeviceIndex,
    /// `None` when opened in read-only mode (no allocations needed).
    allocator: Option<ExtentAllocator>,
    superblock: Superblock,
    dirty: bool,
    /// Whether the store was opened in read-only mode.
    read_only: bool,
    /// Per-shard dirty flags (sharded index).
    shard_dirty: Vec<bool>,
    /// Bytes written to disk in the most recent flush_index() call.
    last_flush_bytes: u64,
    /// Enforced maximum key length in bytes (from superblock).
    max_key_length: usize,
    /// Compression algorithm for this device (from superblock).
    compression: Compression,
}

impl Inner {
    /// Return a mutable reference to the allocator, or `ReadOnly` if in RO mode.
    fn allocator_mut(&mut self) -> crate::Result<&mut ExtentAllocator> {
        self.allocator.as_mut().ok_or(crate::RawStoreError::ReadOnly)
    }

    /// Mark the shard containing `key` as dirty.
    fn mark_shard_dirty(&mut self, key: &str) {
        let shard = shard_for_key(key) as usize;
        self.shard_dirty[shard] = true;
    }

    /// Write payload to device and update the in-memory index.
    /// On I/O failure the allocated extent is freed (no leak).
    fn do_put(&mut self, key: String, payload: Bytes) -> crate::Result<()> {
        self.do_put_with_meta(key, payload, 0)
    }

    /// Payloads at or above this size use the streaming write path
    /// (incremental CRC + StreamingBlockWriter) so peak heap stays ~1 MB
    /// regardless of object size.  Below this threshold the simpler
    /// single-pass path is used.
    const STREAMING_PUT_THRESHOLD: usize = 8 * 1024 * 1024; // 8 MB

    /// Write payload (body + metadata already concatenated) to device and update
    /// the in-memory index with the given `meta_len`.
    fn do_put_with_meta(&mut self, key: String, payload: Bytes, meta_len: u16) -> crate::Result<()> {
        if self.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        if payload.is_empty() {
            return Err(crate::RawStoreError::EmptyPayload);
        }
        if key.len() > self.max_key_length {
            return Err(crate::RawStoreError::KeyTooLong {
                len: key.len(),
                max: self.max_key_length,
            });
        }

        // Attempt compression if the device has a compression algorithm set
        // and the payload is large enough to benefit (>= 4096 bytes).
        let original_size = payload.len() as u64;
        let (write_payload, uncompressed_size) = if self.compression != Compression::None
            && original_size >= 4096
        {
            if let Some(compressed) = self.compression.compress(&payload) {
                if (compressed.len() as u64) < original_size {
                    // Compressed form is smaller -- use it
                    (Bytes::from(compressed), original_size)
                } else {
                    // Compression didn't help -- store original
                    (payload, 0u64)
                }
            } else {
                (payload, 0u64)
            }
        } else {
            (payload, 0u64)
        };

        let payload_size = write_payload.len() as u64;
        let padded = padded_extent_size(payload_size)?;

        // Allocate extent
        let offset = self.allocator_mut()?.alloc(padded)?;

        // For large payloads use streaming write: incremental CRC +
        // StreamingBlockWriter so we avoid a separate full-payload CRC
        // scan and the encode_and_write_batched buffer allocation.
        let crc = if write_payload.len() >= Self::STREAMING_PUT_THRESHOLD {
            let mut writer = crate::extent::StreamingBlockWriter::new(&self.io, offset);
            let mut crc: u32 = 0;
            let chunk_size = 1024 * 1024; // 1 MB chunks
            let mut pos = 0;
            while pos < write_payload.len() {
                let end = (pos + chunk_size).min(write_payload.len());
                let chunk = &write_payload[pos..end];
                crc = crc32c::crc32c_append(crc, chunk);
                if let Err(e) = writer.write_chunk(chunk) {
                    let _ = self.allocator_mut().map(|a| a.free(offset, padded));
                    return Err(e);
                }
                pos = end;
            }
            if let Err(e) = writer.finish() {
                let _ = self.allocator_mut().map(|a| a.free(offset, padded));
                return Err(e);
            }
            crc
        } else {
            // Small payload: single-pass CRC + batched write.
            let crc = crc32c::crc32c(&write_payload);
            if let Err(e) = crate::extent::encode_and_write_batched(
                &self.io, offset, &write_payload,
            ) {
                self.allocator_mut()?.free(offset, padded);
                return Err(e);
            }
            crc
        };

        // Free old extent if overwriting
        if let Some(old) = self.index.files.remove(&key) {
            self.allocator_mut()?.free(old.offset, old.padded_size);
        }

        // If a tombstone existed for this path, clear it: the object is alive again.
        self.index.tombstones.remove(&key);

        self.mark_shard_dirty(&key);
        self.index.files.insert(
            key,
            ExtentInfo {
                offset,
                size: payload_size,
                padded_size: padded,
                crc32c: crc,
                created_txn: self.superblock.txn_id,
                last_modified: Utc::now(),
                meta_len,
                uncompressed_size,
            },
        );

        self.dirty = true;
        Ok(())
    }

    /// Read payload from device and verify per-block CRCs.
    fn do_get(&self, key: &str) -> crate::Result<(Bytes, ExtentInfo)> {
        let info = self
            .index
            .files
            .get(key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.to_string()))?
            .clone();

        let content = crate::extent::read_and_decode_batched(
            &self.io, info.offset, info.size,
        )
        .map_err(|e| match e {
            crate::RawStoreError::DataCorruption { expected, actual, .. } =>
                crate::RawStoreError::DataCorruption {
                    path: key.to_string(),
                    expected,
                    actual,
                },
            other => other,
        })?;

        // Decompress if needed
        let data = if info.uncompressed_size > 0 {
            let decompressed = self.compression.decompress(&content)?;
            Bytes::from(decompressed)
        } else {
            Bytes::from(content)
        };
        Ok((data, info))
    }

    /// Read the raw on-disk payload of an object **without** decompression.
    ///
    /// Returns the exact bytes stored in the extent (compressed if the
    /// device uses compression and the object was large enough to compress)
    /// together with the `ExtentInfo` so the caller can inspect
    /// `uncompressed_size`.
    fn do_get_raw(&self, key: &str) -> crate::Result<(Bytes, ExtentInfo)> {
        let info = self
            .index
            .files
            .get(key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.to_string()))?
            .clone();

        let content = crate::extent::read_and_decode_batched(
            &self.io, info.offset, info.size,
        )
        .map_err(|e| match e {
            crate::RawStoreError::DataCorruption { expected, actual, .. } =>
                crate::RawStoreError::DataCorruption {
                    path: key.to_string(),
                    expected,
                    actual,
                },
            other => other,
        })?;

        Ok((Bytes::from(content), info))
    }

    /// Read a range of payload bytes, only fetching the needed blocks.
    ///
    /// **Compressed extents:** When the extent is compressed, the entire
    /// compressed payload must be read and decompressed before the byte
    /// range can be extracted.  For objects whose uncompressed size exceeds
    /// `COMPRESSED_RANGE_READ_MAX` (1 GB), this method returns an error
    /// to prevent unbounded memory usage -- callers should fetch the whole
    /// object instead.
    ///
    /// **S3 layer:** The objstrd adapter maps this error to S3
    /// `InternalError`.  Clients receiving a 500 on a range request for
    /// a large compressed object should retry as a full GET.
    fn do_get_range(&self, key: &str, range: std::ops::Range<u64>) -> crate::Result<(Bytes, ExtentInfo)> {
        let info = self
            .index
            .files
            .get(key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.to_string()))?
            .clone();

        // Compressed extent: must read + decompress everything, then slice.
        if info.uncompressed_size > 0 {
            if info.uncompressed_size > COMPRESSED_RANGE_READ_MAX {
                return Err(crate::RawStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "range read on compressed object '{}' rejected: uncompressed size {} \
                         exceeds limit {} (1 GB). Fetch the whole object instead.",
                        key, info.uncompressed_size, COMPRESSED_RANGE_READ_MAX,
                    ),
                )));
            }
            let (full_data, info) = self.do_get(key)?;
            let start = range.start as usize;
            let end = (range.end as usize).min(full_data.len());
            let start = start.min(end);
            return Ok((full_data.slice(start..end), info));
        }

        // Uncompressed extent: efficient partial read.
        // Payload byte offsets map directly to block content offsets.
        let content_start = range.start;
        let content_end = range.end;

        // Which blocks cover this content range?
        let first_block = content_start / crate::extent::BLOCK_DATA_SIZE as u64;
        let last_block = (content_end.saturating_sub(1)) / crate::extent::BLOCK_DATA_SIZE as u64;

        let block_byte_offset = first_block.checked_mul(BLOCK_ALIGNMENT)
            .ok_or_else(|| crate::RawStoreError::ExtentInvalid {
                reason: "range read: block offset overflow".into(),
            })?;
        let disk_offset = info.offset.checked_add(block_byte_offset)
            .ok_or_else(|| crate::RawStoreError::ExtentInvalid {
                reason: "range read: disk offset overflow".into(),
            })?;
        let disk_len = (last_block - first_block + 1).checked_mul(BLOCK_ALIGNMENT)
            .ok_or_else(|| crate::RawStoreError::ExtentInvalid {
                reason: "range read: disk length overflow".into(),
            })? as usize;
        let raw = self.io.pread(disk_offset, disk_len)?;

        let data = crate::extent::decode_block_range(
            &raw,
            first_block,
            content_start as usize..content_end as usize,
        )
        .map_err(|e| match e {
            crate::RawStoreError::DataCorruption { expected, actual, .. } =>
                crate::RawStoreError::DataCorruption {
                    path: key.to_string(),
                    expected,
                    actual,
                },
            other => other,
        })?;

        Ok((Bytes::from(data), info))
    }

    /// Copy raw encoded blocks from one key to another without
    /// decoding + re-encoding.  Skips the CRC decode/encode round-trip.
    ///
    /// Copies in 1 MB batches to avoid buffering the entire extent in
    /// memory for large objects.
    fn do_copy(&mut self, from_key: &str, to_key: String) -> crate::Result<()> {
        if self.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        if to_key.len() > self.max_key_length {
            return Err(crate::RawStoreError::KeyTooLong {
                len: to_key.len(),
                max: self.max_key_length,
            });
        }
        let info = self
            .index
            .files
            .get(from_key)
            .ok_or_else(|| crate::RawStoreError::NotFound(from_key.to_string()))?
            .clone();

        // Allocate new extent first
        let new_offset = self.allocator_mut()?.alloc(info.padded_size)?;

        // Copy raw blocks in 1 MB batches (256 blocks of 4 KB each).
        const BATCH_SIZE: u64 = 256 * crate::BLOCK_ALIGNMENT;
        let total = info.padded_size;
        let mut copied: u64 = 0;
        while copied < total {
            let chunk = (total - copied).min(BATCH_SIZE) as usize;
            let src_off = info.offset.checked_add(copied)
                .ok_or_else(|| crate::RawStoreError::ExtentInvalid {
                    reason: "copy: source offset overflow".into(),
                })?;
            let dst_off = new_offset.checked_add(copied)
                .ok_or_else(|| crate::RawStoreError::ExtentInvalid {
                    reason: "copy: destination offset overflow".into(),
                })?;
            let raw = match self.io.pread(src_off, chunk) {
                Ok(r) => r,
                Err(e) => {
                    let _ = self.allocator_mut().map(|a| a.free(new_offset, info.padded_size));
                    return Err(e);
                }
            };
            if let Err(e) = self.io.pwrite(dst_off, &raw) {
                let _ = self.allocator_mut().map(|a| a.free(new_offset, info.padded_size));
                return Err(e);
            }
            copied += chunk as u64;
        }

        // Free old extent if overwriting
        if let Some(old) = self.index.files.remove(&to_key) {
            self.allocator_mut()?.free(old.offset, old.padded_size);
        }

        self.mark_shard_dirty(&to_key);
        self.index.files.insert(
            to_key,
            ExtentInfo {
                offset: new_offset,
                size: info.size,
                padded_size: info.padded_size,
                crc32c: info.crc32c,
                created_txn: self.superblock.txn_id,
                last_modified: Utc::now(),
                meta_len: info.meta_len,
                uncompressed_size: info.uncompressed_size,
            },
        );

        self.dirty = true;
        Ok(())
    }
}

// -- Main store struct is defined below, after the diagnostic types. --

// -- Open mode / integrity check -------------------------------------

/// Controls the extent integrity check performed when opening a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Fast block-0 scan (default).  Reads the first 4 KB block of every
    /// indexed extent and verifies the per-block CRC32c.
    /// Cost: one 4 KB pread per object.  Catches full overwrites.
    Default,

    /// Full payload verification.  Reads every block of every extent,
    /// verifies per-block CRCs, then recomputes the payload CRC and
    /// compares it against the index.  Catches partial overwrites too.
    FullVerify,

    /// Skip all integrity checks on open (fastest, no extra I/O).
    SkipVerify,
}

// -- Tool / diagnostic types -----------------------------------------

/// Full information about a single stored object, including extent layout.
/// Returned by `RawObjectStore::list_full()`.
pub struct ObjectFullInfo {
    /// ObjectStore key (path).
    pub key: String,
    /// Body size in bytes (total payload minus metadata suffix).
    pub body_size: u64,
    /// Metadata suffix size in bytes.
    pub meta_len: u16,
    /// When the object was last written.
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// Transaction ID when the object was written.
    pub created_txn: u64,
    /// Byte offset of the extent on the device.
    pub offset: u64,
    /// Padded on-disk size (block-aligned).
    pub padded_size: u64,
}

/// Result of a raw (no-decompression) read via [`RawObjectStore::get_raw`].
pub struct RawGetResult {
    /// The exact payload bytes stored on disk.
    /// If the object was compressed, this is the compressed form.
    pub data: Bytes,
    /// Original uncompressed size.  Zero means the object is stored
    /// uncompressed (either compression is disabled, the object was
    /// too small to compress, or compression did not shrink it).
    pub uncompressed_size: u64,
    /// Compression algorithm configured on this device.
    pub compression: Compression,
}

/// Device statistics and metadata.
pub struct DeviceInfo {
    pub device_path: String,
    pub device_size: u64,
    pub format_version: u32,
    pub flags: u32,
    pub direct_io: bool,
    pub txn_id: u64,
    pub index_slot_capacity: u64,
    pub index_serialized_bytes: u64,
    pub index_region_a: u64,
    pub index_region_b: u64,
    pub active_index_region: u64,
    pub file_count: usize,
    pub data_bytes_stored: u64,
    pub device_bytes_used: u64,
    pub free_space: u64,
    pub free_fragments: usize,
    pub largest_free_extent: u64,
    pub data_region_start: u64,
    pub data_region_end: u64,
    /// Bytes written to disk in the most recent flush_index() call.
    pub last_flush_bytes: u64,
    /// Enforced maximum key length in bytes.
    pub max_key_length: usize,
    /// Serialized size in bytes for each shard slot (256 entries, 0 = empty).
    pub shard_sizes: Vec<u32>,
    /// Capacity of each shard slot in bytes.
    pub shard_slot_size: u64,
    /// Compression algorithm configured for this device.
    pub compression: Compression,
}

/// Status of a single extent verification.
pub enum VerifyStatus {
    Ok,
    CrcMismatch { expected: u32, actual: u32 },
    BlockCorrupt(String),
    OutOfBounds,
    ReadError(String),
}

/// Result of verifying a single file's extent.
pub struct ExtentVerifyResult {
    pub path: String,
    pub offset: u64,
    pub expected_size: u64,
    pub status: VerifyStatus,
}

/// Summary of a full device verification.
pub struct VerifyReport {
    pub files_checked: usize,
    pub files_ok: usize,
    pub errors: Vec<ExtentVerifyResult>,
    pub ok_files: Vec<ExtentVerifyResult>,
    pub overlapping_extents: Vec<(String, String)>,
    pub free_list_consistent: bool,
    pub space_accounted: bool,
    pub total_data_region: u64,
    pub total_used: u64,
    pub total_free: u64,
}

/// Summary of a repair operation.
pub struct RepairReport {
    pub free_list_rebuilt: bool,
    pub old_free_entries: usize,
    pub new_free_entries: usize,
    pub old_free_space: u64,
    pub new_free_space: u64,
    pub flushed: bool,
    /// Number of files found in the index.
    pub files_found: usize,
    /// Used extents: (path, offset, padded_size) sorted by offset.
    pub used_extents: Vec<(String, u64, u64)>,
    /// Rebuilt free list: (offset, size) sorted by offset.
    pub new_free_list: Vec<(u64, u64)>,
}

/// Summary of an import operation.
pub struct ImportReport {
    pub files_imported: usize,
    pub bytes_imported: u64,
    pub errors: Vec<(String, String)>,
}

/// A tombstone entry visible to callers — records an object that was
/// removed during open-time integrity scanning.
pub struct TombstoneEntry {
    /// Original ObjectStore path.
    pub path: String,
    /// Original payload size in bytes.
    pub size: u64,
    /// Original CRC32c of the payload.
    pub crc32c: u32,
    /// When the object was last written.
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// Why the entry was removed.
    pub reason: String,
    /// Transaction ID when the tombstone was created.
    pub tombstone_txn: u64,
}

/// A single extent in the device layout map.
pub struct LayoutExtent {
    /// ObjectStore key.
    pub key: String,
    /// Byte offset on device.
    pub offset: u64,
    /// On-disk payload size in bytes (compressed if compression applied).
    pub size: u64,
    /// On-disk size including header + padding.
    pub padded_size: u64,
    /// Transaction ID when written.
    pub created_txn: u64,
    /// When this extent was last written.
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// Original uncompressed payload size (0 = not compressed).
    pub uncompressed_size: u64,
}

/// Complete device layout for visualization.
pub struct DeviceLayout {
    pub device_size: u64,
    pub data_region_start: u64,
    pub data_region_end: u64,
    /// Byte offset of index region A (= data_region_end).
    pub index_region_a: u64,
    /// Byte offset of index region B.
    pub index_region_b: u64,
    /// Byte offset of the currently active index region (A or B).
    pub active_index_region: u64,
    pub txn_id: u64,
    /// Used extents sorted by offset.
    pub extents: Vec<LayoutExtent>,
    /// Free regions sorted by offset: (offset, size).
    pub free_regions: Vec<(u64, u64)>,
}

/// Summary of a free-space scrub operation.
pub struct ScrubReport {
    /// Number of free regions zeroed.
    pub regions_scrubbed: usize,
    /// Total bytes zeroed.
    pub bytes_scrubbed: u64,
}

/// Summary of an export operation.
pub struct ExportReport {
    pub files_exported: usize,
    pub bytes_exported: u64,
    pub errors: Vec<(String, String)>,
}

// -- Core types ------------------------------------------------------

/// Options for formatting a new device.
pub struct FormatOptions {
    /// Total device size in bytes.
    pub device_size: u64,
    /// Whether to use O_DIRECT (Linux only).
    pub direct_io: bool,
    /// Capacity of each index slot.  Must be a multiple of 16 MB (minimum 16 MB).
    /// Default: 16 MB.  Larger values allow more files at the cost of usable data space.
    pub index_slot_size: u64,
    /// Maximum allowed key length in bytes.  Enforced on `put()` and `copy()`.
    /// Default: 1024 (matching AWS S3).
    ///
    /// May be set up to `(index_slot_size / NUM_SHARDS) - 98` bytes
    /// (the per-shard slot size minus serialization overhead).  Values above
    /// this ceiling are clamped silently because a single key that large
    /// would overflow the shard slot on flush.  Even below the ceiling,
    /// very long keys reduce the number of objects that can share one shard.
    pub max_key_length: usize,
    /// Compression algorithm applied to all objects on this device.
    /// Default: `Compression::None`.
    ///
    /// When set to a non-`None` algorithm, objects >= 4096 bytes are
    /// compressed before writing.  If the compressed form is not smaller
    /// than the original, the object is stored uncompressed.
    ///
    /// **Range reads on compressed objects** require decompressing the
    /// entire extent first.  Objects with an uncompressed size > 1 GB
    /// will reject range reads -- callers must fetch the whole object.
    pub compression: Compression,
}

/// A raw block device (or loopback file) [`ObjectStore`] implementation.
///
/// Bypasses the filesystem layer entirely, writing objects directly to a
/// block device with a contiguous extent allocator, per-block CRC32c
/// protection, and a crash-safe sharded index.
///
/// Create a new store with [`format`](Self::format) /
/// [`format_with_size`](Self::format_with_size) /
/// [`format_with_options`](Self::format_with_options), or open an existing
/// one with [`open`](Self::open).
pub struct RawObjectStore {
    inner: Arc<RwLock<Inner>>,
    /// Shared I/O handle for reads outside the lock (pread is thread-safe).
    io: Arc<DeviceIo>,
    device_path: String,
    /// Byte offset of index region A
    index_region_a: u64,
    /// Byte offset of index region B
    index_region_b: u64,
    /// Per-slot capacity (read from superblock on open)
    index_slot_capacity: u64,
    /// Size of each shard slot within a region: index_slot_capacity / NUM_SHARDS
    shard_slot_size: u64,
    /// Monotonic counter for unique multipart upload IDs
    next_upload_id: AtomicU64,
    /// Callbacks invoked after each `flush_index()` with the new txn_id.
    /// The sharded layer registers closures here to emit FLUSH events.
    flush_callbacks: parking_lot::Mutex<Vec<Arc<dyn Fn(u64) + Send + Sync>>>,
}

impl fmt::Debug for RawObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawObjectStore")
            .field("device", &self.device_path)
            .finish()
    }
}

impl fmt::Display for RawObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawObjectStore({})", self.device_path)
    }
}

impl RawObjectStore {
    /// Format a block device or loopback file, auto-detecting the size.
    ///
    /// For block devices (`/dev/sdX`) the size is queried via ioctl.
    /// For regular files the existing file size is used.
    /// Use [`format_with_size`] when you need to create a new loopback file
    /// of a specific size.
    pub fn format(path: &StdPath, direct_io: bool) -> crate::Result<Self> {
        // Open without O_DIRECT to probe size (and to do the initial writes).
        let probe = DeviceIo::open(path, false)?;
        let device_size = probe.size()?;
        drop(probe);
        Self::format_with_size(path, device_size, direct_io)
    }

    /// Format a new device (or loopback file) of a given size and open it.
    ///
    /// Uses the default index slot size (16 MB per slot, 32 MB total).
    /// For larger index capacity use [`format_with_options`].
    pub fn format_with_size(path: &StdPath, device_size: u64, direct_io: bool) -> crate::Result<Self> {
        Self::format_with_options(path, FormatOptions {
            device_size,
            direct_io,
            index_slot_size: INDEX_REGION_SIZE,
            max_key_length: crate::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        })
    }

    /// Format a new device with explicit options including index slot size.
    ///
    /// `opts.index_slot_size` must be a multiple of 16 MB (minimum 16 MB).
    /// Larger slots allow more files at the cost of usable data space.
    pub fn format_with_options(path: &StdPath, opts: FormatOptions) -> crate::Result<Self> {
        let FormatOptions { device_size, direct_io, index_slot_size, max_key_length, compression } = opts;

        crate::validate_index_slot_size(index_slot_size)?;

        if max_key_length > crate::MAX_KEY_LENGTH_HARD_CEILING {
            return Err(crate::RawStoreError::MaxKeyLengthTooLarge {
                len: max_key_length,
                max: crate::MAX_KEY_LENGTH_HARD_CEILING,
            });
        }

        let min_size = crate::min_device_size(index_slot_size);
        if device_size < min_size {
            return Err(crate::RawStoreError::DeviceTooSmall {
                size: device_size,
                minimum: min_size,
            });
        }

        // Format with buffered I/O (set_len needs it), then reopen direct.
        let io = DeviceIo::create(path, device_size, false)?;

        // Index regions at the end of the device
        let index_total = index_slot_size * 2;
        let index_region_a = device_size - index_total;
        let index_region_b = device_size - index_slot_size;
        let data_end = index_region_a;
        let shard_slot_size = index_slot_size / NUM_SHARDS as u64;

        // Clamp max_key_length to the absolute ceiling: shard_slot_size - 98 bytes
        // of bincode/CRC overhead.  A key at this limit can only be stored when it
        // is the sole occupant of its shard.
        let shard_overhead: usize = 98;
        let ceiling = (shard_slot_size as usize).saturating_sub(shard_overhead);
        let effective_max_key = max_key_length.min(ceiling);

        // Write empty shards to region A
        let empty_shard = ShardData::default();
        let (shard_bytes, shard_crc) = empty_shard.to_bytes()?;

        let mut sb = Superblock::new(device_size, BLOCK_ALIGNMENT, index_region_a, index_slot_size, effective_max_key as u32, compression);
        for i in 0..NUM_SHARDS {
            let offset = index_region_a + i as u64 * shard_slot_size;
            io.pwrite(offset, &shard_bytes)?;
            sb.shard_slots[i].active_slot = 0;
            sb.shard_slots[i].size = shard_bytes.len() as u32;
            sb.shard_slots[i].crc = shard_crc;
        }

        sb.index_region_offset = index_region_a;
        sb.index_region_size = shard_bytes.len() as u64 * NUM_SHARDS as u64;
        sb.index_checksum = 0;
        sb.txn_id = 1;
        if direct_io {
            sb.flags |= FLAG_DIRECT_IO;
        }

        let sb_bytes = sb.to_bytes()?;
        io.pwrite(0, &sb_bytes)?;
        io.pwrite(crate::SUPERBLOCK_SIZE, &sb_bytes)?;
        io.sync()?;

        // Reopen with O_DIRECT if requested.
        // We must drop the old io (and its flock) before calling DeviceIo::open,
        // because the RHS is evaluated before the old value is dropped on
        // reassignment.  Wrap in Option so the borrow checker allows
        // explicit drop + reassign.
        let mut io_opt = Some(io);
        if direct_io {
            io_opt.take(); // drops old DeviceIo, releasing flock
            io_opt = Some(DeviceIo::open(path, true)?);
        }
        let io = io_opt.unwrap();

        let index = DeviceIndex::new();
        let allocator = ExtentAllocator::new(DATA_START, data_end, BLOCK_ALIGNMENT);

        let io = Arc::new(io);
        let result = Ok(Self {
            inner: Arc::new(RwLock::new(Inner {
                io: Arc::clone(&io),
                index,
                allocator: Some(allocator),
                superblock: sb,
                dirty: false,
                read_only: false,
                shard_dirty: vec![false; NUM_SHARDS],
                last_flush_bytes: 0,
                max_key_length: effective_max_key,
                compression,
            })),
            io,
            device_path: path.to_string_lossy().into_owned(),
            index_region_a,
            index_region_b,
            index_slot_capacity: index_slot_size,
            shard_slot_size,
            next_upload_id: AtomicU64::new(0),
            flush_callbacks: parking_lot::Mutex::new(Vec::new()),
        });
        info!(
            device = %path.display(),
            device_size,
            direct_io,
            compression = %compression,
            index_slot_size,
            max_key_length = effective_max_key,
            "formatted new raw store"
        );
        result
    }

    /// Open an existing formatted device with the default integrity check
    /// (fast header scan).
    ///
    /// If the superblock has the `FLAG_DIRECT_IO` flag set the device
    /// is automatically reopened with `O_DIRECT`.
    pub fn open(path: &StdPath) -> crate::Result<Self> {
        Self::open_impl(path, OpenMode::Default, false)
    }

    /// Open an existing formatted device with the specified integrity check.
    ///
    /// See [`OpenMode`] for available modes.
    pub fn open_with_mode(path: &StdPath, mode: OpenMode) -> crate::Result<Self> {
        Self::open_impl(path, mode, false)
    }

    /// Open an existing formatted device in **read-only** mode with the
    /// default integrity check (fast header scan).
    ///
    /// The file is opened with `O_RDONLY`.  All mutating operations
    /// (`put`, `delete`, `copy`, `rename`, `flush_index`, …) will return
    /// `RawStoreError::ReadOnly`.  The allocator and free-list are not
    /// built — only the index is loaded.
    pub fn open_readonly(path: &StdPath) -> crate::Result<Self> {
        Self::open_impl(path, OpenMode::Default, true)
    }

    /// Open an existing formatted device in **read-only** mode with the
    /// specified integrity check.
    pub fn open_readonly_with_mode(path: &StdPath, mode: OpenMode) -> crate::Result<Self> {
        Self::open_impl(path, mode, true)
    }

    /// Shared open implementation.
    fn open_impl(path: &StdPath, mode: OpenMode, read_only: bool) -> crate::Result<Self> {
        // Always read superblocks with buffered I/O first.
        let io = if read_only {
            DeviceIo::open_readonly(path, false)?
        } else {
            DeviceIo::open(path, false)?
        };
        let device_size = io.size()?;

        // Read both superblocks, pick valid one with highest txn_id
        let sb0_bytes = io.pread(0, crate::SUPERBLOCK_SIZE as usize)?;
        let sb1_bytes = io.pread(crate::SUPERBLOCK_SIZE, crate::SUPERBLOCK_SIZE as usize)?;

        let sb0 = Superblock::from_bytes(&sb0_bytes);
        let sb1 = Superblock::from_bytes(&sb1_bytes);

        let sb = match (sb0, sb1) {
            (Ok(a), Ok(b)) => {
                if a.txn_id >= b.txn_id {
                    a
                } else {
                    b
                }
            }
            (Ok(a), Err(_)) => a,
            (Err(_), Ok(b)) => b,
            (Err(_), Err(_)) => return Err(crate::RawStoreError::SuperblockCorrupt),
        };

        // Derive index slot capacity from superblock (defensive fallback for
        // zero-valued field; v4 always sets this, but guard against corruption)
        let index_slot_capacity = if sb.index_slot_capacity > 0 {
            sb.index_slot_capacity
        } else {
            INDEX_REGION_SIZE
        };

        let min_size = crate::min_device_size(index_slot_capacity);
        if device_size < min_size {
            return Err(crate::RawStoreError::DeviceTooSmall {
                size: device_size,
                minimum: min_size,
            });
        }

        let index_total = index_slot_capacity * 2;
        let index_region_a = device_size - index_total;
        let index_region_b = device_size - index_slot_capacity;
        let data_end = index_region_a;
        let shard_slot_size = index_slot_capacity / NUM_SHARDS as u64;

        // Read all shards and merge into a single DeviceIndex
        let mut index = DeviceIndex::new();
        for i in 0..NUM_SHARDS {
            let meta = &sb.shard_slots[i];
            if meta.size == 0 {
                continue;
            }
            let offset = if meta.active_slot == 0 {
                index_region_a + i as u64 * shard_slot_size
            } else {
                index_region_b + i as u64 * shard_slot_size
            };
            let shard_bytes = io.pread(offset, meta.size as usize)?;
            let shard = ShardData::from_bytes(&shard_bytes, meta.crc)?;
            index.files.extend(shard.files);
            index.tombstones.extend(shard.tombstones);
        }

        // Track whether the integrity scan modifies the index so we can
        // mark the affected shards dirty after constructing Inner.
        let mut scan_modified_keys: Vec<String> = Vec::new();

        // Validate index entries: remove out-of-bounds and overlapping extents
        {
            let mut bad_keys = Vec::new();
            for (key, info) in &index.files {
                if info.offset < DATA_START
                    || info.padded_size == 0
                    || info.offset.saturating_add(info.padded_size) > data_end
                {
                    warn!(
                        key = key.as_str(),
                        offset = info.offset,
                        padded_size = info.padded_size,
                        data_end,
                        "integrity: removing index entry, extent out of bounds"
                    );
                    bad_keys.push(key.clone());
                }
            }
            for key in &bad_keys {
                index.files.remove(key);
            }
            scan_modified_keys.extend(bad_keys);

            // Detect overlapping extents (sorted by offset)
            let mut sorted: Vec<_> = index
                .files
                .iter()
                .map(|(k, v)| (k.clone(), v.offset, v.padded_size))
                .collect();
            sorted.sort_by_key(|(_, o, _)| *o);
            let mut overlap_keys = Vec::new();
            for w in sorted.windows(2) {
                if w[0].1 + w[0].2 > w[1].1 {
                    warn!(
                        first = w[0].0.as_str(),
                        first_offset = w[0].1,
                        first_size = w[0].2,
                        second = w[1].0.as_str(),
                        second_offset = w[1].1,
                        "integrity: overlapping extents, removing latter"
                    );
                    overlap_keys.push(w[1].0.clone());
                }
            }
            for key in &overlap_keys {
                index.files.remove(key);
            }
            scan_modified_keys.extend(overlap_keys);
        }

        // -- Integrity scan (crash-recovery protection) --------------
        //
        // Between flushes, old freed extents can be reused by new writes.
        // If the process crashes before the next flush_index(), the
        // on-disk index still points to the old offset -- which now
        // contains a different object's data.  We detect this by
        // comparing the on-disk block 0 CRC as a quick sanity check.
        //
        // Stale entries are removed from the files map and recorded as
        // tombstones so that an orchestrator can restore them from backup.
        let next_txn = sb.txn_id + 1;

        /// Remove stale entries from the index, optionally creating tombstones,
        /// and append their keys to `modified_keys`.
        fn apply_stale_removals(
            stale: Vec<(String, String)>,
            index: &mut DeviceIndex,
            modified_keys: &mut Vec<String>,
            read_only: bool,
            next_txn: u64,
        ) {
            for (key, reason) in &stale {
                if let Some(info) = index.files.remove(key) {
                    if !read_only {
                        index.tombstones.insert(key.clone(), TombstoneInfo {
                            size: info.size,
                            crc32c: info.crc32c,
                            last_modified: info.last_modified,
                            reason: reason.clone(),
                            tombstone_txn: next_txn,
                        });
                    }
                }
            }
            modified_keys.extend(stale.into_iter().map(|(k, _)| k));
        }

        match mode {
            OpenMode::Default => {
                // Fast scan: read block 0 of each extent, verify its CRC.
                let mut stale: Vec<(String, String)> = Vec::new();
                for (key, info) in &index.files {
                    let raw = match io.pread(info.offset, BLOCK_ALIGNMENT as usize) {
                        Ok(r) => r,
                        Err(_) => {
                            warn!(
                                key = key.as_str(),
                                offset = info.offset,
                                "integrity: cannot read block 0, removing"
                            );
                            stale.push((key.clone(), "unreadable block 0".into()));
                            continue;
                        }
                    };

                    // Verify block 0 CRC
                    let stored_block_crc = match raw[..4].try_into() {
                        Ok(b) => u32::from_le_bytes(b),
                        Err(_) => {
                            stale.push((key.clone(), "block 0 too short for CRC".into()));
                            continue;
                        }
                    };
                    let actual_block_crc = crc32c::crc32c(&raw[4..]);
                    if stored_block_crc != actual_block_crc {
                        warn!(
                            key = key.as_str(),
                            offset = info.offset,
                            expected = format_args!("{:#010x}", stored_block_crc),
                            actual = format_args!("{:#010x}", actual_block_crc),
                            "integrity: block 0 CRC mismatch, removing"
                        );
                        stale.push((key.clone(), format!(
                            "block 0 CRC mismatch (expected {:#010x}, got {:#010x})",
                            stored_block_crc, actual_block_crc
                        )));
                    }
                }
                apply_stale_removals(stale, &mut index, &mut scan_modified_keys, read_only, next_txn);
            }
            OpenMode::FullVerify => {
                // Full verification: read all blocks, verify per-block
                // CRCs, then recompute payload CRC vs index.
                let mut stale: Vec<(String, String)> = Vec::new();
                for (key, info) in &index.files {
                    let disk_len = match padded_extent_size(info.size) {
                        Ok(v) => v as usize,
                        Err(_) => {
                            stale.push((key.clone(), "payload size overflow".into()));
                            continue;
                        }
                    };
                    let raw = match io.pread(info.offset, disk_len) {
                        Ok(r) => r,
                        Err(_) => {
                            warn!(
                                key = key.as_str(),
                                offset = info.offset,
                                "integrity: cannot read extent, removing"
                            );
                            stale.push((key.clone(), "unreadable extent".into()));
                            continue;
                        }
                    };

                    let payload = match crate::extent::decode_blocks(&raw, info.size as usize) {
                        Ok(c) => c,
                        Err(_) => {
                            warn!(
                                key = key.as_str(),
                                offset = info.offset,
                                "integrity: block CRC failure, removing"
                            );
                            stale.push((key.clone(), "block CRC failure".into()));
                            continue;
                        }
                    };

                    // Verify payload CRC
                    let actual_crc = crc32c::crc32c(&payload);
                    if actual_crc != info.crc32c {
                        warn!(
                            key = key.as_str(),
                            offset = info.offset,
                            computed = format_args!("{:#010x}", actual_crc),
                            indexed = format_args!("{:#010x}", info.crc32c),
                            "integrity: payload CRC mismatch, data overwritten after last flush, removing"
                        );
                        stale.push((key.clone(), format!(
                            "payload CRC {:#010x} != index CRC {:#010x}",
                            actual_crc, info.crc32c
                        )));
                    }
                }
                apply_stale_removals(stale, &mut index, &mut scan_modified_keys, read_only, next_txn);
            }
            OpenMode::SkipVerify => {
                // No integrity check -- caller accepts the risk.
            }
        }

        // If write-protect flag is set and caller requested read-write, reject.
        if !read_only && (sb.flags & crate::FLAG_WRITE_PROTECT != 0) {
            return Err(crate::RawStoreError::WriteProtected);
        }

        // Purge orphaned multipart temporary extents left over from a crash.
        // These are hidden from list() but waste disk space.  Removing them
        // before the allocator rebuild returns their extents to the free list.
        if !read_only {
            let multipart_keys: Vec<String> = index.files.keys()
                .filter(|k| k.starts_with(MULTIPART_TMP_PREFIX))
                .cloned()
                .collect();
            if !multipart_keys.is_empty() {
                info!(
                    count = multipart_keys.len(),
                    "purging orphaned multipart temporary extents"
                );
                for key in &multipart_keys {
                    index.files.remove(key);
                    scan_modified_keys.push(key.clone());
                }
            }
        }

        // Rebuild allocator (skip in read-only mode -- no allocations needed).
        // Shards do not persist the free list; always rebuild from gaps.
        let allocator = if read_only {
            None
        } else {
            let (_used, free) = rebuild_free_list(&index, data_end);
            Some(ExtentAllocator::from_free_list(free, DATA_START, data_end))
        };

        // Reopen with O_DIRECT if the superblock says so.
        // Drop the old io first to release flock before acquiring it on the new fd.
        let use_direct = sb.flags & FLAG_DIRECT_IO != 0;
        let mut io_opt = Some(io);
        if use_direct {
            io_opt.take(); // drops old DeviceIo, releasing flock
            io_opt = Some(if read_only {
                DeviceIo::open_readonly(path, true)?
            } else {
                DeviceIo::open(path, true)?
            });
        }
        let io = io_opt.unwrap();

        // Pre-compute shard dirty flags from integrity-scan modifications so
        // the next flush persists corrections made during open.
        let mut initial_shard_dirty = vec![false; NUM_SHARDS];
        for key in &scan_modified_keys {
            initial_shard_dirty[shard_for_key(key) as usize] = true;
        }
        let scan_dirty = !scan_modified_keys.is_empty();

        let max_key_length = sb.max_key_length as usize;
        if max_key_length == 0 {
            return Err(crate::RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "superblock has max_key_length=0 (pre-v4 image); reformat required",
            )));
        }

        let compression = Compression::from_u8(sb.compression)?;
        let sb_txn_id = sb.txn_id;

        let io = Arc::new(io);
        let result = Ok(Self {
            inner: Arc::new(RwLock::new(Inner {
                io: Arc::clone(&io),
                index,
                allocator,
                superblock: sb,
                dirty: scan_dirty,
                read_only,
                shard_dirty: initial_shard_dirty,
                last_flush_bytes: 0,
                max_key_length,
                compression,
            })),
            io,
            device_path: path.to_string_lossy().into_owned(),
            index_region_a,
            index_region_b,
            index_slot_capacity,
            shard_slot_size,
            next_upload_id: AtomicU64::new(0),
            flush_callbacks: parking_lot::Mutex::new(Vec::new()),
        });
        let file_count = result.as_ref().map(|s| {
            let inner = s.inner.read();
            inner.index.files.len()
        }).unwrap_or(0);
        if result.is_ok() {
            info!(
                device = %path.display(),
                device_size,
                file_count,
                direct_io = use_direct,
                compression = %compression,
                read_only,
                scan_repairs = scan_modified_keys.len(),
                txn_id = sb_txn_id,
                "opened raw store"
            );
        }
        result
    }

    /// Flush the in-memory index to disk (sharded, crash-safe).
    ///
    /// Only dirty shards are rewritten.  Each dirty shard is written to the
    /// opposite A/B slot, then the superblock is updated atomically.
    pub fn flush_index(&self) -> crate::Result<()> {
        let mut inner = self.inner.write();
        if inner.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }

        let new_txn = inner.superblock.txn_id + 1;
        let mut bytes_written = 0u64;

        // Pre-bucket files and tombstones by shard so each dirty shard
        // only iterates its own entries instead of scanning all keys.
        let dirty_count = inner.shard_dirty.iter().filter(|&&d| d).count();
        let mut shard_files: Vec<ShardData> = (0..NUM_SHARDS)
            .map(|_| ShardData::default())
            .collect();

        if dirty_count > 0 {
            for (key, info) in &inner.index.files {
                let s = shard_for_key(key) as usize;
                if inner.shard_dirty[s] {
                    shard_files[s].files.insert(key.clone(), info.clone());
                }
            }
            for (key, info) in &inner.index.tombstones {
                let s = shard_for_key(key) as usize;
                if inner.shard_dirty[s] {
                    shard_files[s].tombstones.insert(key.clone(), info.clone());
                }
            }
        }

        // Write only dirty shards
        for shard_idx in 0..NUM_SHARDS {
            if !inner.shard_dirty[shard_idx] {
                continue;
            }

            let shard = std::mem::take(&mut shard_files[shard_idx]);

            let (shard_bytes, shard_crc) = shard.to_bytes()?;

            if shard_bytes.len() as u64 > self.shard_slot_size {
                return Err(crate::RawStoreError::ShardOverflow {
                    shard: shard_idx,
                    size: shard_bytes.len() as u64,
                    capacity: self.shard_slot_size,
                });
            }

            // Write to the opposite slot
            let cur_slot = inner.superblock.shard_slots[shard_idx].active_slot;
            let new_slot = 1 - cur_slot;
            let write_offset = if new_slot == 0 {
                self.index_region_a + shard_idx as u64 * self.shard_slot_size
            } else {
                self.index_region_b + shard_idx as u64 * self.shard_slot_size
            };

            inner.io.pwrite(write_offset, &shard_bytes)?;
            bytes_written += shard_bytes.len() as u64;

            inner.superblock.shard_slots[shard_idx].active_slot = new_slot;
            inner.superblock.shard_slots[shard_idx].size = shard_bytes.len() as u32;
            inner.superblock.shard_slots[shard_idx].crc = shard_crc;
        }

        // Sync shard data before committing the superblock
        inner.io.sync()?;

        // Update superblock
        inner.superblock.prev_txn_id = inner.superblock.txn_id;
        inner.superblock.txn_id = new_txn;
        // index_region_size = total of all active shards (informational)
        inner.superblock.index_region_size = inner.superblock.shard_slots.iter()
            .map(|s| s.size as u64)
            .sum();

        let sb_bytes = inner.superblock.to_bytes()?;
        inner.io.pwrite(0, &sb_bytes)?;
        inner.io.sync()?;
        inner.io.pwrite(crate::SUPERBLOCK_SIZE, &sb_bytes)?;
        inner.io.sync()?;
        bytes_written += 2 * crate::SUPERBLOCK_SIZE;

        // Clear dirty flags
        for flag in inner.shard_dirty.iter_mut() {
            *flag = false;
        }
        inner.dirty = false;
        inner.last_flush_bytes = bytes_written;

        debug!(
            txn_id = new_txn,
            dirty_shards = dirty_count,
            bytes_written,
            "flush_index complete"
        );

        // Invoke flush callbacks (used by the event bus to emit FLUSH events).
        {
            let txn_id = inner.superblock.txn_id;
            let cbs = self.flush_callbacks.lock();
            for cb in cbs.iter() {
                cb(txn_id);
            }
        }

        Ok(())
    }

    /// Re-read the on-disk index if the writer has flushed since we last loaded.
    ///
    /// Reads the superblock and compares the `txn_id`.  If unchanged, returns
    /// `Ok(false)`.  If the transaction ID advanced, the method re-reads only
    /// the shards whose `active_slot` or `crc` differ, merges the new data
    /// into the in-memory [`DeviceIndex`], and optionally rebuilds the
    /// allocator free list (for read-write stores).
    ///
    /// No integrity scan is performed -- the writer's `flush_index()` already
    /// committed a consistent snapshot.
    ///
    /// Returns `Ok(true)` when the index was refreshed, `Ok(false)` when it
    /// was already up-to-date.
    pub fn reload_index(&self) -> crate::Result<bool> {
        let mut inner = self.inner.write();

        // Read the primary superblock (offset 0).
        let sb_bytes = inner.io.pread(0, crate::SUPERBLOCK_SIZE as usize)?;
        let new_sb = match Superblock::from_bytes(&sb_bytes) {
            Ok(s) => s,
            Err(_) => {
                // Primary failed -- try backup.
                let sb1_bytes = inner.io.pread(crate::SUPERBLOCK_SIZE, crate::SUPERBLOCK_SIZE as usize)?;
                Superblock::from_bytes(&sb1_bytes)?
            }
        };

        if new_sb.txn_id == inner.superblock.txn_id {
            return Ok(false);
        }

        // Determine which shards changed.
        // Clone old slots to avoid borrowing inner.superblock during mutation.
        let old_slots = inner.superblock.shard_slots.clone();
        let new_slots = &new_sb.shard_slots;

        // Remove entries belonging to changed shards, then insert new data.
        for shard_idx in 0..NUM_SHARDS {
            let old = &old_slots[shard_idx];
            let new = &new_slots[shard_idx];
            if old.active_slot == new.active_slot && old.crc == new.crc && old.size == new.size {
                continue; // shard unchanged
            }

            // Remove old entries for this shard from in-memory index.
            let keys_to_remove: Vec<String> = inner.index.files
                .keys()
                .filter(|k| shard_for_key(k) as usize == shard_idx)
                .cloned()
                .collect();
            for k in &keys_to_remove {
                inner.index.files.remove(k);
            }
            let tomb_keys_to_remove: Vec<String> = inner.index.tombstones
                .keys()
                .filter(|k| shard_for_key(k) as usize == shard_idx)
                .cloned()
                .collect();
            for k in &tomb_keys_to_remove {
                inner.index.tombstones.remove(k);
            }

            // Read the new shard from disk.
            if new.size == 0 {
                continue;
            }
            let offset = if new.active_slot == 0 {
                self.index_region_a + shard_idx as u64 * self.shard_slot_size
            } else {
                self.index_region_b + shard_idx as u64 * self.shard_slot_size
            };
            let shard_bytes = inner.io.pread(offset, new.size as usize)?;
            let shard = ShardData::from_bytes(&shard_bytes, new.crc)?;
            inner.index.files.extend(shard.files);
            inner.index.tombstones.extend(shard.tombstones);
        }

        // Update superblock and derived fields.
        inner.superblock = new_sb;
        inner.max_key_length = if inner.superblock.max_key_length > 0 {
            inner.superblock.max_key_length as usize
        } else {
            crate::DEFAULT_MAX_KEY_LENGTH
        };

        // Rebuild allocator free list if in read-write mode.
        if !inner.read_only {
            let data_end = self.index_region_a;
            let (_used, free) = rebuild_free_list(&inner.index, data_end);
            inner.allocator = Some(ExtentAllocator::from_free_list(free, DATA_START, data_end));
        }

        // Clear dirty flags since the in-memory state now matches disk.
        for flag in inner.shard_dirty.iter_mut() {
            *flag = false;
        }
        inner.dirty = false;

        debug!(
            new_txn_id = inner.superblock.txn_id,
            file_count = inner.index.files.len(),
            "reload_index: index refreshed from disk"
        );

        Ok(true)
    }

    /// Register a callback invoked after each `flush_index()` with the
    /// new `txn_id`.
    ///
    /// The sharded event layer uses this to emit `FLUSH` events without
    /// the raw store needing to know about the event bus directly.
    pub fn add_flush_callback(&self, cb: Arc<dyn Fn(u64) + Send + Sync>) {
        self.flush_callbacks.lock().push(cb);
    }

    /// Return the path of the device or image file.
    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    /// Whether the store has dirty (unflushed) changes.
    pub fn needs_flush(&self) -> bool {
        self.inner.read().dirty
    }

    /// Modify superblock flags on a device without fully opening it.
    ///
    /// This is a static utility that opens the file read/write with buffered
    /// I/O, reads both superblock copies, applies the flag changes, and
    /// rewrites both copies.  It does NOT load the index, build the
    /// allocator, or verify extents.
    ///
    /// Use this to toggle `FLAG_DIRECT_IO` or `FLAG_WRITE_PROTECT` without
    /// going through the normal `open()` path (which would fail if the
    /// device is write-protected).
    ///
    /// `set_flags` bits are OR'd in; `clear_flags` bits are AND-NOT'd out.
    /// Only known flag bits (`FLAG_KNOWN_MASK`) may be set.
    ///
    /// Returns the resulting flags value.
    pub fn modify_flags(path: &StdPath, set_flags: u32, clear_flags: u32) -> crate::Result<u32> {
        // Validate: only known bits
        if set_flags & !crate::FLAG_KNOWN_MASK != 0 {
            return Err(crate::RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown flag bits in set_flags: {:#010x}", set_flags & !crate::FLAG_KNOWN_MASK),
            )));
        }

        let io = DeviceIo::open(path, false)?;

        // Read both superblocks, pick valid one with highest txn_id
        let sb0_bytes = io.pread(0, crate::SUPERBLOCK_SIZE as usize)?;
        let sb1_bytes = io.pread(crate::SUPERBLOCK_SIZE, crate::SUPERBLOCK_SIZE as usize)?;

        let sb0 = Superblock::from_bytes(&sb0_bytes);
        let sb1 = Superblock::from_bytes(&sb1_bytes);

        let mut sb = match (sb0, sb1) {
            (Ok(a), Ok(b)) => {
                if a.txn_id >= b.txn_id { a } else { b }
            }
            (Ok(a), Err(_)) => a,
            (Err(_), Ok(b)) => b,
            (Err(_), Err(_)) => return Err(crate::RawStoreError::SuperblockCorrupt),
        };

        // Apply flag changes
        sb.flags = (sb.flags | set_flags) & !clear_flags;

        // Rewrite both superblock copies
        let sb_bytes = sb.to_bytes()?;
        io.pwrite(0, &sb_bytes)?;
        io.sync()?;
        io.pwrite(crate::SUPERBLOCK_SIZE, &sb_bytes)?;
        io.sync()?;

        Ok(sb.flags)
    }

    /// Whether this store instance was opened in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.inner.read().read_only
    }

    // -- Tool / diagnostic methods ------------------------------------

    /// Get device statistics and metadata.
    pub fn device_info(&self) -> DeviceInfo {
        // Snapshot all needed fields under the read lock, then release it
        // before any further computation so concurrent writers aren't stalled.
        let (sizes, device_size, format_version, flags, txn_id, index_serialized_bytes,
             active_index_region, file_count, free_space, free_fragments, largest_free,
             last_flush_bytes, max_key_length, shard_sizes, compression)
        = {
            let inner = self.inner.read();
            let sizes: Vec<(u64, u64)> = inner.index.files.values()
                .map(|e| (e.size, e.padded_size))
                .collect();
            let (free_space, free_fragments, largest_free) = match &inner.allocator {
                Some(alloc) => {
                    let fl = alloc.free_list();
                    let largest = fl.iter().map(|(_, s)| *s).max().unwrap_or(0);
                    (alloc.free_space(), fl.len(), largest)
                }
                None => (0, 0, 0),
            };
            (
                sizes,
                inner.superblock.device_size,
                inner.superblock.version,
                inner.superblock.flags,
                inner.superblock.txn_id,
                inner.superblock.index_region_size,
                inner.superblock.index_region_offset,
                inner.index.files.len(),
                free_space,
                free_fragments,
                largest_free,
                inner.last_flush_bytes,
                inner.max_key_length,
                inner.superblock.shard_slots.iter().map(|s| s.size).collect::<Vec<_>>(),
                inner.compression,
            )
        }; // read lock dropped here

        let data_bytes: u64 = sizes.iter().map(|(s, _)| s).sum();
        let device_bytes: u64 = sizes.iter().map(|(_, p)| p).sum();
        let data_end = self.index_region_a;

        DeviceInfo {
            device_path: self.device_path.clone(),
            device_size,
            format_version,
            flags,
            direct_io: flags & FLAG_DIRECT_IO != 0,
            txn_id,
            index_slot_capacity: self.index_slot_capacity,
            index_serialized_bytes,
            index_region_a: self.index_region_a,
            index_region_b: self.index_region_b,
            active_index_region,
            file_count,
            data_bytes_stored: data_bytes,
            device_bytes_used: device_bytes,
            free_space,
            free_fragments,
            largest_free_extent: largest_free,
            data_region_start: DATA_START,
            data_region_end: data_end,
            last_flush_bytes,
            max_key_length,
            shard_sizes,
            shard_slot_size: self.shard_slot_size,
            compression,
        }
    }

    /// Return the enforced maximum key length in bytes for this device.
    pub fn max_key_length(&self) -> usize {
        self.inner.read().max_key_length
    }

    /// Return the compression algorithm configured for this device.
    pub fn compression(&self) -> Compression {
        self.inner.read().compression
    }

    /// Return the full device layout for visualization: all extents + free regions.
    pub fn layout_map(&self) -> DeviceLayout {
        // Hold the read lock only long enough to snapshot the raw data.
        // Sorting and free-list construction happen after the lock is released
        // so that concurrent writers are not stalled on large indexes.
        let (mut extents, device_size, active_index_region, txn_id) = {
            let inner = self.inner.read();
            let data_end = self.index_region_a;
            let extents: Vec<LayoutExtent> = inner
                .index
                .files
                .iter()
                .filter(|(_, e)| {
                    e.offset >= DATA_START
                        && e.padded_size > 0
                        && e.offset.saturating_add(e.padded_size) <= data_end
                })
                .map(|(k, e)| LayoutExtent {
                    key: k.clone(),
                    offset: e.offset,
                    size: e.size,
                    padded_size: e.padded_size,
                    created_txn: e.created_txn,
                    last_modified: e.last_modified,
                    uncompressed_size: e.uncompressed_size,
                })
                .collect();
            (
                extents,
                inner.superblock.device_size,
                inner.superblock.index_region_offset,
                inner.superblock.txn_id,
            )
        }; // read lock dropped here

        let data_end = self.index_region_a;
        extents.sort_by_key(|e| e.offset);

        // Build free list from gaps
        let mut free = Vec::new();
        let mut cursor = DATA_START;
        for ext in &extents {
            if ext.offset > cursor {
                free.push((cursor, ext.offset - cursor));
            }
            cursor = ext.offset + ext.padded_size;
        }
        if cursor < data_end {
            free.push((cursor, data_end - cursor));
        }

        DeviceLayout {
            device_size,
            data_region_start: DATA_START,
            data_region_end: data_end,
            index_region_a: self.index_region_a,
            index_region_b: self.index_region_b,
            active_index_region,
            txn_id,
            extents,
            free_regions: free,
        }
    }

    /// Verify all extents on the device -- reads every file's header and payload CRC.
    pub fn verify_all(&self) -> VerifyReport {
        let inner = self.inner.read();
        let data_end = self.index_region_a;

        let mut files_ok = 0usize;
        let mut errors = Vec::new();
        let mut ok_files = Vec::new();

        for (path, info) in &inner.index.files {
            // Bounds check
            if info.offset < DATA_START
                || info.offset.saturating_add(info.padded_size) > data_end
            {
                errors.push(ExtentVerifyResult {
                    path: path.clone(),
                    offset: info.offset,
                    expected_size: info.size,
                    status: VerifyStatus::OutOfBounds,
                });
                continue;
            }

            // Read all blocks for this extent
            let disk_len = match padded_extent_size(info.size) {
                Ok(v) => v as usize,
                Err(e) => {
                    errors.push(ExtentVerifyResult {
                        path: path.clone(),
                        offset: info.offset,
                        expected_size: info.size,
                        status: VerifyStatus::ReadError(e.to_string()),
                    });
                    continue;
                }
            };
            let raw = match inner.io.pread(info.offset, disk_len) {
                Ok(d) => d,
                Err(e) => {
                    errors.push(ExtentVerifyResult {
                        path: path.clone(),
                        offset: info.offset,
                        expected_size: info.size,
                        status: VerifyStatus::ReadError(e.to_string()),
                    });
                    continue;
                }
            };

            // Verify per-block CRCs and decode payload
            let content = match crate::extent::decode_blocks(&raw, info.size as usize) {
                Ok(c) => c,
                Err(crate::RawStoreError::DataCorruption { expected, actual, .. }) => {
                    errors.push(ExtentVerifyResult {
                        path: path.clone(),
                        offset: info.offset,
                        expected_size: info.size,
                        status: VerifyStatus::CrcMismatch { expected, actual },
                    });
                    continue;
                }
                Err(e) => {
                    errors.push(ExtentVerifyResult {
                        path: path.clone(),
                        offset: info.offset,
                        expected_size: info.size,
                        status: VerifyStatus::ReadError(e.to_string()),
                    });
                    continue;
                }
            };

            // Verify whole-file payload CRC (stored in index)
            let actual_crc = crc32c::crc32c(&content);
            if actual_crc != info.crc32c {
                errors.push(ExtentVerifyResult {
                    path: path.clone(),
                    offset: info.offset,
                    expected_size: info.size,
                    status: VerifyStatus::CrcMismatch {
                        expected: info.crc32c,
                        actual: actual_crc,
                    },
                });
                continue;
            }

            files_ok += 1;
            ok_files.push(ExtentVerifyResult {
                path: path.clone(),
                offset: info.offset,
                expected_size: info.size,
                status: VerifyStatus::Ok,
            });
        }

        // Check for overlapping extents
        let mut sorted: Vec<_> = inner
            .index
            .files
            .iter()
            .map(|(k, v)| (k.clone(), v.offset, v.padded_size))
            .collect();
        sorted.sort_by_key(|(_, o, _)| *o);

        let mut overlapping = Vec::new();
        for w in sorted.windows(2) {
            if w[0].1 + w[0].2 > w[1].1 {
                overlapping.push((w[0].0.clone(), w[1].0.clone()));
            }
        }

        // Space accounting
        let total_data_region = data_end - DATA_START;
        let total_used: u64 = inner.index.files.values().map(|e| e.padded_size).sum();
        let (total_free, space_accounted, free_list_consistent) = match &inner.allocator {
            Some(alloc) => {
                let tf = alloc.free_space();
                let accounted = total_used + tf == total_data_region;

                let fl = alloc.free_list();
                let mut consistent = true;
                for i in 0..fl.len() {
                    let (off, sz) = fl[i];
                    if off < DATA_START || off.saturating_add(sz) > data_end || sz == 0 {
                        consistent = false;
                        break;
                    }
                    if i + 1 < fl.len() && off + sz > fl[i + 1].0 {
                        consistent = false;
                        break;
                    }
                }
                (tf, accounted, consistent)
            }
            None => {
                // Read-only mode: no allocator, compute from index
                let computed_free = total_data_region.saturating_sub(total_used);
                (computed_free, true, true)
            }
        };

        VerifyReport {
            files_checked: inner.index.files.len(),
            files_ok,
            errors,
            ok_files,
            overlapping_extents: overlapping,
            free_list_consistent,
            space_accounted,
            total_data_region,
            total_used,
            total_free,
        }
    }

    /// Repair the device: rebuild free list from index and flush.
    pub fn repair(&self) -> crate::Result<RepairReport> {
        let (old_free_entries, old_free_space, files_found, used_extents, new_free_list_copy) = {
            let mut inner = self.inner.write();
            if inner.read_only {
                return Err(crate::RawStoreError::ReadOnly);
            }
            let data_end = self.index_region_a;

            let old_entries = inner.allocator.as_ref().map(|a| a.free_list().len()).unwrap_or(0);
            let old_space = inner.allocator.as_ref().map(|a| a.free_space()).unwrap_or(0);

            // Rebuild free list as complement of used extents
            let (_used, new_free) = rebuild_free_list(&inner.index, data_end);

            // Build (path, offset, padded_size) for report
            let mut used_with_names: Vec<(String, u64, u64)> = inner
                .index
                .files
                .iter()
                .filter(|(_, e)| {
                    e.offset >= DATA_START
                        && e.padded_size > 0
                        && e.offset.saturating_add(e.padded_size) <= data_end
                })
                .map(|(k, e)| (k.clone(), e.offset, e.padded_size))
                .collect();
            used_with_names.sort_by_key(|(_, o, _)| *o);

            let files_count = inner.index.files.len();

            let new_free_copy = new_free.clone();
            inner.allocator = Some(ExtentAllocator::from_free_list(new_free, DATA_START, data_end));
            (old_entries, old_space, files_count, used_with_names, new_free_copy)
        };

        // Flush persists the repaired state (also re-writes both superblocks)
        self.flush_index()?;

        let inner = self.inner.read();
        Ok(RepairReport {
            free_list_rebuilt: true,
            old_free_entries,
            new_free_entries: inner.allocator.as_ref().map(|a| a.free_list().len()).unwrap_or(0),
            old_free_space,
            new_free_space: inner.allocator.as_ref().map(|a| a.free_space()).unwrap_or(0),
            flushed: true,
            files_found,
            used_extents,
            new_free_list: new_free_list_copy,
        })
    }

    // -- Tombstone management -----------------------------------------

    /// List all tombstone entries (objects removed during integrity scan).
    pub fn list_tombstones(&self) -> Vec<TombstoneEntry> {
        let inner = self.inner.read();
        let mut entries: Vec<TombstoneEntry> = inner
            .index
            .tombstones
            .iter()
            .map(|(path, t)| TombstoneEntry {
                path: path.clone(),
                size: t.size,
                crc32c: t.crc32c,
                last_modified: t.last_modified,
                reason: t.reason.clone(),
                tombstone_txn: t.tombstone_txn,
            })
            .collect();
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        entries
    }

    /// Delete a single tombstone by path.  Returns `Ok(true)` if removed,
    /// `Ok(false)` if the path was not a tombstone.
    ///
    /// This only removes the tombstone record — it cannot remove a live
    /// file.  Call `flush_index()` to persist.
    pub fn delete_tombstone(&self, path: &str) -> crate::Result<bool> {
        let mut inner = self.inner.write();
        if inner.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        // Refuse to operate if the path is a live file (safety guard).
        if inner.index.files.contains_key(path) {
            return Ok(false);
        }
        if inner.index.tombstones.remove(path).is_some() {
            inner.mark_shard_dirty(path);
            inner.dirty = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete all tombstones.  Returns the number removed.
    /// Call `flush_index()` to persist.
    pub fn clear_tombstones(&self) -> crate::Result<usize> {
        let mut inner = self.inner.write();
        if inner.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        let count = inner.index.tombstones.len();
        if count > 0 {
            // Mark only shards that actually contain tombstones
            let dirty_shards: Vec<usize> = inner.index.tombstones.keys()
                .map(|key| shard_for_key(key) as usize)
                .collect();
            for s in dirty_shards {
                inner.shard_dirty[s] = true;
            }
            inner.index.tombstones.clear();
            inner.dirty = true;
        }
        Ok(count)
    }

    // -- Free-space scrub ---------------------------------------------

    /// Zero all free regions on the device.  Use this to erase residual
    /// data from deleted or overwritten objects (sensitive data scrub).
    ///
    /// Writes are done in 1 MB chunks to limit memory usage.
    /// Calls `fsync` once at the end.
    pub fn scrub_free_space(&self) -> crate::Result<ScrubReport> {
        let inner = self.inner.read();
        if inner.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        let free_list = inner.allocator.as_ref()
            .map(|a| a.free_list().to_vec())
            .unwrap_or_default();
        let mut regions_scrubbed = 0usize;
        let mut bytes_scrubbed = 0u64;

        const CHUNK: usize = 1024 * 1024; // 1 MB
        let zeros = vec![0u8; CHUNK];

        for &(offset, size) in &free_list {
            let mut cursor = offset;
            let end = offset + size;
            while cursor < end {
                let len = std::cmp::min((end - cursor) as usize, CHUNK);
                inner.io.pwrite(cursor, &zeros[..len])?;
                cursor += len as u64;
            }
            regions_scrubbed += 1;
            bytes_scrubbed += size;
        }

        inner.io.sync()?;

        Ok(ScrubReport {
            regions_scrubbed,
            bytes_scrubbed,
        })
    }

    /// Import all objects from another ObjectStore into this device.
    pub async fn import_from(
        &self,
        source: &dyn ObjectStore,
        prefix: Option<&Path>,
    ) -> crate::Result<ImportReport> {
        let files: Vec<ObjectMeta> = source
            .list(prefix)
            .try_collect()
            .await
            .map_err(|e| {
                crate::RawStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        let mut imported = 0usize;
        let mut bytes = 0u64;
        let mut errors = Vec::new();

        // Fetch from source in parallel (up to 8 concurrent), then put locally
        // one by one (local puts need the write lock).
        let fetched: Vec<(Path, Result<Bytes, String>)> =
            futures::stream::iter(files.into_iter())
                .map(|meta| async move {
                    let loc = meta.location;
                    let res = async {
                        let r = source.get(&loc).await.map_err(|e| e.to_string())?;
                        r.bytes().await.map_err(|e| e.to_string())
                    }
                    .await;
                    (loc, res)
                })
                .buffered(8)
                .collect()
                .await;

        for (loc, result) in fetched {
            match result {
                Ok(data) => {
                    let size = data.len() as u64;
                    match self.put(&loc, PutPayload::from(data)).await {
                        Ok(_) => {
                            imported += 1;
                            bytes += size;
                        }
                        Err(e) => errors.push((loc.to_string(), e.to_string())),
                    }
                }
                Err(e) => errors.push(("fetch".to_string(), e)),
            }
        }

        self.flush_index()?;

        Ok(ImportReport {
            files_imported: imported,
            bytes_imported: bytes,
            errors,
        })
    }

    /// Export all objects from this device to another ObjectStore.
    pub async fn export_to(
        &self,
        target: &dyn ObjectStore,
    ) -> crate::Result<ExportReport> {
        let files: Vec<ObjectMeta> = self
            .list(None)
            .try_collect()
            .await
            .map_err(|e| {
                crate::RawStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        let mut exported = 0usize;
        let mut bytes = 0u64;
        let mut errors = Vec::new();

        // Read + write in parallel (up to 8 concurrent).  Each task does
        // a local get (read-lock) then a remote put -- both are independent.
        let results: Vec<(Path, Result<u64, String>)> =
            futures::stream::iter(files.into_iter())
                .map(|meta| async move {
                    let loc = meta.location;
                    let res = async {
                        let r = self.get(&loc).await.map_err(|e| e.to_string())?;
                        let data = r.bytes().await.map_err(|e| e.to_string())?;
                        let size = data.len() as u64;
                        target
                            .put(&loc, PutPayload::from(data))
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok::<_, String>(size)
                    }
                    .await;
                    (loc, res)
                })
                .buffered(8)
                .collect()
                .await;

        for (loc, result) in results {
            match result {
                Ok(size) => {
                    exported += 1;
                    bytes += size;
                }
                Err(e) => errors.push((loc.to_string(), e)),
            }
        }

        Ok(ExportReport {
            files_exported: exported,
            bytes_exported: bytes,
            errors,
        })
    }

    // -- Metadata-aware extensions -----------------------------------

    /// Store body + opaque metadata bytes as a single extent.
    ///
    /// The raw store concatenates `body` and `metadata` into one payload and
    /// records `meta_len` in the index so the caller can split them later.
    /// Returns `PutResult` on success.
    pub fn put_with_meta(
        &self,
        location: &Path,
        body: Bytes,
        metadata: &[u8],
    ) -> crate::Result<()> {
        if metadata.len() > u16::MAX as usize {
            return Err(crate::RawStoreError::MetadataTooLarge {
                len: metadata.len(),
                max: u16::MAX as usize,
            });
        }
        let key = location.to_string();
        let meta_len = metadata.len() as u16;

        // Concatenate body + metadata into a single payload
        let mut payload = Vec::with_capacity(body.len() + metadata.len());
        payload.extend_from_slice(&body);
        payload.extend_from_slice(metadata);
        let payload = Bytes::from(payload);

        let mut inner = self.inner.write();
        inner.do_put_with_meta(key, payload, meta_len)
    }

    /// Store body + metadata from a file that already contains both.
    ///
    /// The file must contain the body bytes followed by exactly
    /// `meta_len` bytes of metadata.  The file is read from the current
    /// position (caller should seek to 0 if needed).
    ///
    /// When the device has no compression enabled, this method streams
    /// the file through in 1 MB chunks using [`StreamingBlockWriter`],
    /// so peak heap usage is ~1 MB regardless of file size.  With
    /// compression enabled, the full file is read into memory for the
    /// compression pass (same as [`put_with_meta`]).
    pub fn put_with_meta_from_file(
        &self,
        location: &Path,
        file: &mut std::fs::File,
        meta_len: u16,
    ) -> crate::Result<()> {
        use std::io::Read;
        let key = location.to_string();
        let file_len = file.metadata()
            .map(|m| m.len() as usize)
            .unwrap_or(0);

        let mut inner = self.inner.write();

        // When compression is enabled we need the full payload in memory
        // for the compression pass -- fall back to the buffered path.
        if inner.compression != Compression::None {
            let mut payload = Vec::with_capacity(file_len);
            file.read_to_end(&mut payload)
                .map_err(crate::RawStoreError::Io)?;
            return inner.do_put_with_meta(key, Bytes::from(payload), meta_len);
        }

        // -- Streaming path (no compression) --
        if inner.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        if file_len == 0 {
            return Err(crate::RawStoreError::EmptyPayload);
        }
        if key.len() > inner.max_key_length {
            return Err(crate::RawStoreError::KeyTooLong {
                len: key.len(),
                max: inner.max_key_length,
            });
        }

        let payload_size = file_len as u64;
        let padded = padded_extent_size(payload_size)?;
        let offset = inner.allocator_mut()?.alloc(padded)?;

        // Stream file contents through CRC hasher + block writer.
        // Peak heap: one 1 MB read buffer + ~1 MB inside the writer.
        let mut writer = crate::extent::StreamingBlockWriter::new(&inner.io, offset);
        let mut crc: u32 = 0;
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    // I/O error reading the source file -- free the extent.
                    let _ = inner.allocator_mut().map(|a| a.free(offset, padded));
                    return Err(crate::RawStoreError::Io(e));
                }
            };
            crc = crc32c::crc32c_append(crc, &buf[..n]);
            if let Err(e) = writer.write_chunk(&buf[..n]) {
                let _ = inner.allocator_mut().map(|a| a.free(offset, padded));
                return Err(e);
            }
        }
        if let Err(e) = writer.finish() {
            let _ = inner.allocator_mut().map(|a| a.free(offset, padded));
            return Err(e);
        }

        // Free old extent if overwriting
        if let Some(old) = inner.index.files.remove(&key) {
            inner.allocator_mut()?.free(old.offset, old.padded_size);
        }

        // Clear any tombstone -- the object is alive again.
        inner.index.tombstones.remove(&key);

        let txn_id = inner.superblock.txn_id;
        let now = Utc::now();
        inner.mark_shard_dirty(&key);
        inner.index.files.insert(
            key,
            ExtentInfo {
                offset,
                size: payload_size,
                padded_size: padded,
                crc32c: crc,
                created_txn: txn_id,
                last_modified: now,
                meta_len,
                uncompressed_size: 0,
            },
        );

        inner.dirty = true;
        Ok(())
    }

    /// Return the `ObjectMeta` and `meta_len` for an object (index-only, no data I/O).
    pub fn head_with_meta(
        &self,
        location: &Path,
    ) -> crate::Result<(ObjectMeta, u16)> {
        let key = location.to_string();
        let inner = self.inner.read();
        let info = inner
            .index
            .files
            .get(&key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.clone()))?;
        let meta = Self::meta_for(location, info);
        Ok((meta, info.meta_len))
    }

    /// List objects with their `meta_len` values (index-only, zero data reads).
    pub fn list_with_meta(
        &self,
        prefix: Option<&Path>,
    ) -> Vec<(ObjectMeta, u16)> {
        let inner = self.inner.read();
        inner
            .index
            .files
            .iter()
            .filter(|(k, _)| !k.starts_with(MULTIPART_TMP_PREFIX))
            .filter(|(k, _)| {
                match prefix {
                    Some(p) => {
                        let key_path =
                            Path::parse(k.as_str()).unwrap_or_else(|_| Path::from(k.as_str()));
                        key_path.prefix_matches(p)
                    }
                    None => true,
                }
            })
            .map(|(k, info)| {
                let path = Path::parse(k.as_str()).unwrap_or_else(|_| Path::from(k.as_str()));
                (Self::meta_for(&path, info), info.meta_len)
            })
            .collect()
    }

    /// Read only the metadata suffix of an object (suffix read of `meta_len` bytes).
    ///
    /// Returns empty `Bytes` if `meta_len` is 0.
    pub fn get_metadata(
        &self,
        location: &Path,
    ) -> crate::Result<Bytes> {
        let key = location.to_string();
        let inner = self.inner.read();
        let info = inner
            .index
            .files
            .get(&key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.clone()))?
            .clone();
        if info.meta_len == 0 {
            return Ok(Bytes::new());
        }
        let logical_size = if info.uncompressed_size > 0 { info.uncompressed_size } else { info.size };
        let start = logical_size.saturating_sub(info.meta_len as u64);
        let end = logical_size;
        let (data, _) = inner.do_get_range(&key, start..end)?;
        Ok(data)
    }

    /// Update the `meta_len` in the index for an existing object.
    ///
    /// This is needed after multipart upload completion, where the S3 adapter
    /// appends metadata as the final part and then records the length.
    pub fn set_meta_len(
        &self,
        location: &Path,
        meta_len: u16,
    ) -> crate::Result<()> {
        let key = location.to_string();
        let mut inner = self.inner.write();
        let info = inner
            .index
            .files
            .get_mut(&key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.clone()))?;
        info.meta_len = meta_len;
        inner.mark_shard_dirty(&key);
        inner.dirty = true;
        Ok(())
    }

    /// Read the raw on-disk payload of an object without decompression.
    ///
    /// Returns a [`RawGetResult`] containing:
    /// - `data`: the exact bytes stored on disk (compressed if applicable)
    /// - `uncompressed_size`: the original size before compression (0 if not compressed)
    /// - `compression`: the compression algorithm configured on this device
    ///
    /// Callers can check `uncompressed_size > 0` to determine whether the
    /// returned bytes are compressed. When compressed, the device's compression
    /// algorithm (available via `compression` or `device_info().compression`)
    /// was used. Callers who store pre-compressed data and track this in their
    /// own metadata can use `getraw` to avoid a redundant decompress-recompress
    /// cycle.
    pub fn get_raw(
        &self,
        location: &Path,
    ) -> crate::Result<RawGetResult> {
        let key = location.to_string();
        let inner = self.inner.read();
        let (data, info) = inner.do_get_raw(&key)?;
        Ok(RawGetResult {
            data,
            uncompressed_size: info.uncompressed_size,
            compression: inner.compression,
        })
    }

    /// Return full extent information for all objects (index-only, zero data reads).
    ///
    /// Results are sorted by key. Multipart temporary keys are excluded.
    pub fn list_full(&self, prefix: Option<&Path>) -> Vec<ObjectFullInfo> {
        let inner = self.inner.read();
        let mut results: Vec<ObjectFullInfo> = inner
            .index
            .files
            .iter()
            .filter(|(k, _)| !k.starts_with(MULTIPART_TMP_PREFIX))
            .filter(|(k, _)| match prefix {
                Some(p) => {
                    let key_path =
                        Path::parse(k.as_str()).unwrap_or_else(|_| Path::from(k.as_str()));
                    key_path.prefix_matches(p)
                }
                None => true,
            })
            .map(|(k, info)| {
                let logical_size = if info.uncompressed_size > 0 { info.uncompressed_size } else { info.size };
                let body_size = logical_size.saturating_sub(info.meta_len as u64);
                ObjectFullInfo {
                    key: k.clone(),
                    body_size,
                    meta_len: info.meta_len,
                    last_modified: info.last_modified,
                    created_txn: info.created_txn,
                    offset: info.offset,
                    padded_size: info.padded_size,
                }
            })
            .collect();
        results.sort_by(|a, b| a.key.cmp(&b.key));
        results
    }

    /// Rewrite an existing object with new metadata bytes, preserving the body.
    ///
    /// For **uncompressed** extents the body is streamed from the old extent
    /// to a new extent in 1 MB chunks, so peak heap usage is ~1 MB regardless
    /// of object size.  For **compressed** extents the full payload must be
    /// decompressed first, so the buffered path is used.
    ///
    /// The old extent is freed and the index is updated atomically under a
    /// single write lock.
    pub fn update_metadata(&self, location: &Path, metadata: Bytes) -> crate::Result<()> {
        if metadata.len() > u16::MAX as usize {
            return Err(crate::RawStoreError::MetadataTooLarge {
                len: metadata.len(),
                max: u16::MAX as usize,
            });
        }
        let key = location.to_string();
        let meta_len = metadata.len() as u16;

        let mut inner = self.inner.write();

        let info = inner
            .index
            .files
            .get(&key)
            .ok_or_else(|| crate::RawStoreError::NotFound(key.clone()))?
            .clone();

        // Compressed extents must be fully decompressed to extract the body,
        // so fall back to the buffered path.
        if info.uncompressed_size > 0 {
            let logical_size = info.uncompressed_size;
            let body_size = logical_size.saturating_sub(info.meta_len as u64);
            let body = if body_size == 0 {
                Bytes::new()
            } else {
                let (data, _) = inner.do_get(&key)?;
                data.slice(..body_size as usize)
            };
            let mut payload = Vec::with_capacity(body.len() + metadata.len());
            payload.extend_from_slice(&body);
            payload.extend_from_slice(&metadata);
            return inner.do_put_with_meta(key, Bytes::from(payload), meta_len);
        }

        // -- Streaming path for uncompressed extents --
        if inner.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }

        let body_size = info.size.saturating_sub(info.meta_len as u64);
        let new_payload_size = body_size + metadata.len() as u64;
        if new_payload_size == 0 {
            return Err(crate::RawStoreError::EmptyPayload);
        }

        let padded = padded_extent_size(new_payload_size)?;
        let new_offset = inner.allocator_mut()?.alloc(padded)?;

        // Stream body from old extent to new extent in 1 MB chunks.
        let io_clone = Arc::clone(&inner.io);
        let mut reader = crate::extent::StreamingBlockReader::new(
            io_clone, info.offset, info.size,
        );
        let mut writer = crate::extent::StreamingBlockWriter::new(&inner.io, new_offset);
        let mut crc: u32 = 0;
        let mut body_remaining = body_size as usize;

        while body_remaining > 0 {
            let chunk = match reader.next_chunk() {
                Ok(Some(c)) => c,
                Ok(None) => break,
                Err(e) => {
                    let _ = inner.allocator_mut().map(|a| a.free(new_offset, padded));
                    return Err(e);
                }
            };
            let take = chunk.len().min(body_remaining);
            crc = crc32c::crc32c_append(crc, &chunk[..take]);
            if let Err(e) = writer.write_chunk(&chunk[..take]) {
                let _ = inner.allocator_mut().map(|a| a.free(new_offset, padded));
                return Err(e);
            }
            body_remaining -= take;
        }

        // Append new metadata.
        if !metadata.is_empty() {
            crc = crc32c::crc32c_append(crc, &metadata);
            if let Err(e) = writer.write_chunk(&metadata) {
                let _ = inner.allocator_mut().map(|a| a.free(new_offset, padded));
                return Err(e);
            }
        }

        if let Err(e) = writer.finish() {
            let _ = inner.allocator_mut().map(|a| a.free(new_offset, padded));
            return Err(e);
        }

        // Free old extent.
        if let Some(old) = inner.index.files.remove(&key) {
            let _ = inner.allocator_mut().map(|a| a.free(old.offset, old.padded_size));
        }

        inner.index.tombstones.remove(&key);
        inner.mark_shard_dirty(&key);
        let txn_id = inner.superblock.txn_id;
        inner.index.files.insert(
            key,
            ExtentInfo {
                offset: new_offset,
                size: new_payload_size,
                padded_size: padded,
                crc32c: crc,
                created_txn: txn_id,
                last_modified: Utc::now(),
                meta_len,
                uncompressed_size: 0,
            },
        );

        inner.dirty = true;
        Ok(())
    }

    /// Build ObjectMeta for a file.
    ///
    /// Reports the **body-only** size (logical size minus metadata suffix)
    /// so `ObjectStore` callers (including S3 clients) see
    /// `Content-Length` matching the actual body they will receive.
    fn meta_for(location: &Path, info: &ExtentInfo) -> ObjectMeta {
        let logical_size = if info.uncompressed_size > 0 {
            info.uncompressed_size
        } else {
            info.size
        };
        let body_size = logical_size.saturating_sub(info.meta_len as u64);
        ObjectMeta {
            location: location.clone(),
            last_modified: info.last_modified,
            size: body_size,
            e_tag: None,
            version: None,
        }
    }
}

/// Convert our error type to object_store::Error.
fn to_os_error(e: crate::RawStoreError) -> object_store::Error {
    match e {
        crate::RawStoreError::NotFound(ref path) => object_store::Error::NotFound {
            path: path.clone(),
            source: Box::new(e),
        },
        crate::RawStoreError::AlreadyExists(ref path) => object_store::Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(e),
        },
        _ => object_store::Error::Generic {
            store: STORE_NAME,
            source: Box::new(e),
        },
    }
}

#[async_trait]
impl ObjectStore for RawObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let data: Bytes = payload.into();
        let key = location.to_string();
        let mut inner = self.inner.write();

        // Check under the same lock that protects the write -- no TOCTOU
        match opts.mode {
            PutMode::Create => {
                if inner.index.files.contains_key(&key) {
                    return Err(object_store::Error::AlreadyExists {
                        path: key.clone(),
                        source: Box::new(crate::RawStoreError::AlreadyExists(
                            key,
                        )),
                    });
                }
            }
            PutMode::Overwrite => {}
            PutMode::Update(update) => {
                // Validate the precondition: the caller's e_tag/version must
                // match the current state.  Since we don't generate e_tags,
                // any Update with a non-None precondition must fail.
                if update.e_tag.is_some() || update.version.is_some() {
                    return Err(object_store::Error::Precondition {
                        path: key.clone(),
                        source: Box::new(crate::RawStoreError::Io(
                            std::io::Error::new(
                                std::io::ErrorKind::Unsupported,
                                "conditional update (e_tag/version) is not supported",
                            ),
                        )),
                    });
                }
                // No precondition provided — treat as overwrite
            }
        }

        inner
            .do_put(key, data)
            .map_err(|e| to_os_error(e))?;

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let upload_id = self.next_upload_id.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(RawMultipartUpload {
            inner: Arc::clone(&self.inner),
            location: location.clone(),
            upload_id,
            part_count: 0,
            part_keys: Vec::new(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let key = location.to_string();

        // --- Ranged requests: buffered path under the lock -----------------
        if let Some(ref get_range) = options.range {
            let inner = self.inner.read();
            let (data, info, range) = match get_range {
                GetRange::Bounded(r) => {
                    let ei = inner.index.files.get(&key)
                        .ok_or_else(|| to_os_error(
                            crate::RawStoreError::NotFound(key.clone()),
                        ))?;
                    let logical = if ei.uncompressed_size > 0 { ei.uncompressed_size as usize } else { ei.size as usize };
                    let total_size = logical.saturating_sub(ei.meta_len as usize);
                    let start = (r.start as usize).min(total_size);
                    let end = (r.end as usize).min(total_size);
                    let start = start.min(end);
                    let (data, info) = inner.do_get_range(&key, start as u64..end as u64)
                        .map_err(|e| to_os_error(e))?;
                    (data, info, start as u64..end as u64)
                }
                GetRange::Offset(o) => {
                    let ei = inner.index.files.get(&key)
                        .ok_or_else(|| to_os_error(
                            crate::RawStoreError::NotFound(key.clone()),
                        ))?;
                    let logical = if ei.uncompressed_size > 0 { ei.uncompressed_size as usize } else { ei.size as usize };
                    let total_size = logical.saturating_sub(ei.meta_len as usize);
                    let start = (*o as usize).min(total_size);
                    let (data, info) = inner.do_get_range(&key, start as u64..total_size as u64)
                        .map_err(|e| to_os_error(e))?;
                    (data, info, start as u64..total_size as u64)
                }
                GetRange::Suffix(s) => {
                    let ei = inner.index.files.get(&key)
                        .ok_or_else(|| to_os_error(
                            crate::RawStoreError::NotFound(key.clone()),
                        ))?;
                    let logical = if ei.uncompressed_size > 0 { ei.uncompressed_size as usize } else { ei.size as usize };
                    let total_size = logical.saturating_sub(ei.meta_len as usize);
                    let start = total_size.saturating_sub(*s as usize);
                    let (data, info) = inner.do_get_range(&key, start as u64..total_size as u64)
                        .map_err(|e| to_os_error(e))?;
                    (data, info, start as u64..total_size as u64)
                }
            };
            let meta = Self::meta_for(location, &info);
            let stream = futures::stream::once(async move { Ok(data) }).boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range,
                attributes: Default::default(),
            });
        }

        // --- Full GET: look up info under the lock, then release -----------
        let (info, compression) = {
            let inner = self.inner.read();
            let info = inner.index.files.get(&key)
                .ok_or_else(|| to_os_error(
                    crate::RawStoreError::NotFound(key.clone()),
                ))?
                .clone();
            let compression = inner.compression;
            (info, compression)
        };
        // Lock released.

        let meta = Self::meta_for(location, &info);

        if info.uncompressed_size > 0 {
            // Compressed object: stream blocks from disk through decompressor.
            // For zstd and gzip we pipe BlockReaderIo (a std::io::Read adapter
            // over the CRC-protected block reader) into a streaming decoder and
            // yield decompressed chunks.  Peak RAM: 1 MB block buffer + decoder
            // working memory, regardless of object size -- no full-object buffer.
            //
            // Snappy has no streaming Read API so it still reads the full
            // compressed payload into memory and decompresses in one call;
            // however the buffer holds the compressed (smaller) form, not the
            // decompressed form.
            let io = Arc::clone(&self.io);
            let body_size = info.uncompressed_size.saturating_sub(info.meta_len as u64);

            let stream: BoxStream<'static, object_store::Result<Bytes>> = match compression {
                crate::Compression::Snappy => {
                    // Snappy: buffer compressed payload, decompress in one shot.
                    let compressed = crate::extent::read_and_decode_batched(
                        &io, info.offset, info.size,
                    ).map_err(|e| match e {
                        crate::RawStoreError::DataCorruption { expected, actual, .. } =>
                            to_os_error(crate::RawStoreError::DataCorruption {
                                path: key.clone(), expected, actual,
                            }),
                        other => to_os_error(other),
                    })?;
                    let data = compression.decompress(&compressed)
                        .map_err(|e| to_os_error(e))?;
                    // Slice to body_size to exclude the metadata suffix.
                    let end = (body_size as usize).min(data.len());
                    let body = Bytes::from(data).slice(..end);
                    futures::stream::once(async move { Ok(body) }).boxed()
                }
                crate::Compression::Zstd => {
                    // Zstd: wrap BlockReaderIo in a zstd streaming decoder.
                    let block_reader = crate::extent::BlockReaderIo::new(
                        io, info.offset, info.size,
                    );
                    let decoder = zstd::stream::read::Decoder::new(block_reader)
                        .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                    let remaining = body_size as usize;
                    let stream_state = (decoder, false, remaining); // (decoder, done, remaining_body_bytes)
                    futures::stream::unfold(stream_state, |(mut dec, done, remaining)| async move {
                        if done || remaining == 0 { return None; }
                        use std::io::Read;
                        let read_size = remaining.min(256 * 1024);
                        let mut buf = vec![0u8; read_size];
                        match dec.read(&mut buf) {
                            Ok(0) => None,
                            Ok(n) => {
                                buf.truncate(n);
                                let consumed = n.min(remaining);
                                buf.truncate(consumed);
                                Some((Ok(Bytes::from(buf)), (dec, false, remaining - consumed)))
                            }
                            Err(e) => Some((Err(object_store::Error::Generic {
                                store: STORE_NAME,
                                source: Box::new(e),
                            }), (dec, true, 0)))
                        }
                    }).fuse().boxed()
                }
                _ => {
                    // Gzip variants: wrap BlockReaderIo in flate2 GzDecoder.
                    let block_reader = crate::extent::BlockReaderIo::new(
                        io, info.offset, info.size,
                    );
                    let decoder = flate2::read::GzDecoder::new(block_reader);
                    let remaining = body_size as usize;
                    let stream_state = (decoder, false, remaining);
                    futures::stream::unfold(stream_state, |(mut dec, done, remaining)| async move {
                        if done || remaining == 0 { return None; }
                        use std::io::Read;
                        let read_size = remaining.min(256 * 1024);
                        let mut buf = vec![0u8; read_size];
                        match dec.read(&mut buf) {
                            Ok(0) => None,
                            Ok(n) => {
                                buf.truncate(n);
                                let consumed = n.min(remaining);
                                buf.truncate(consumed);
                                Some((Ok(Bytes::from(buf)), (dec, false, remaining - consumed)))
                            }
                            Err(e) => Some((Err(object_store::Error::Generic {
                                store: STORE_NAME,
                                source: Box::new(e),
                            }), (dec, true, 0)))
                        }
                    }).fuse().boxed()
                }
            };

            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: 0..body_size,
                attributes: Default::default(),
            });
        }

        // Uncompressed object: true streaming GET (no full-object buffer).
        let body_size = info.size.saturating_sub(info.meta_len as u64);
        if body_size == 0 {
            // Zero-byte file: empty stream.
            let stream = futures::stream::once(async { Ok(Bytes::new()) }).boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: 0..0,
                attributes: Default::default(),
            });
        }

        let io = Arc::clone(&self.io);
        let reader = crate::extent::StreamingBlockReader::new(
            io, info.offset, body_size,
        );

        let stream = futures::stream::unfold(
            reader,
            |mut reader| async move {
                match reader.next_chunk() {
                    Ok(Some(chunk)) if !chunk.is_empty() => {
                        Some((Ok(Bytes::from(chunk)), reader))
                    }
                    Ok(_) => None,
                    Err(e) => Some((Err(to_os_error(e)), reader)),
                }
            },
        ).fuse().boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: 0..body_size,
            attributes: Default::default(),
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let key = location.to_string();
        let mut inner = self.inner.write();

        if inner.read_only {
            return Err(to_os_error(crate::RawStoreError::ReadOnly));
        }

        if let Some(info) = inner.index.files.remove(&key) {
            if let Some(alloc) = inner.allocator.as_mut() {
                alloc.free(info.offset, info.padded_size);
            }
            inner.mark_shard_dirty(&key);
            inner.dirty = true;
        }
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix_path = prefix.cloned();

        // Snapshot raw key+info pairs under the read lock, then release it
        // before building Path / ObjectMeta (avoids blocking writers during
        // Path::parse and string allocation for large indexes).
        let snapshot: Vec<(String, crate::index::ExtentInfo)> = {
            let inner = self.inner.read();
            inner.index.files.iter()
                .filter(|(k, _)| !k.starts_with(MULTIPART_TMP_PREFIX))
                .map(|(k, info)| (k.clone(), info.clone()))
                .collect()
        };

        let results: Vec<object_store::Result<ObjectMeta>> = snapshot
            .into_iter()
            .filter(|(k, _)| {
                match &prefix_path {
                    Some(p) => {
                        let key_path = Path::parse(k.as_str())
                            .unwrap_or_else(|_| Path::from(k.as_str()));
                        key_path.prefix_matches(p)
                    }
                    None => true,
                }
            })
            .map(|(k, info)| {
                let path = Path::parse(k.as_str()).unwrap_or_else(|_| Path::from(k.as_str()));
                Ok(Self::meta_for(&path, &info))
            })
            .collect();

        futures::stream::iter(results).boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();

        // Snapshot raw key+info pairs under the read lock, then release it
        // before Path::parse / ObjectMeta construction.
        let snapshot: Vec<(String, crate::index::ExtentInfo)> = {
            let inner = self.inner.read();
            inner.index.files.iter()
                .filter(|(k, _)| !k.starts_with(MULTIPART_TMP_PREFIX))
                .map(|(k, info)| (k.clone(), info.clone()))
                .collect()
        };

        let mut objects = Vec::new();
        let mut common_prefixes: HashSet<Path> = HashSet::new();

        for (k, info) in &snapshot {
            let matches = if prefix_str.is_empty() {
                true
            } else {
                k.starts_with(&prefix_str)
                    && (k.len() == prefix_str.len()
                        || k.as_bytes().get(prefix_str.len()) == Some(&b'/'))
            };

            if !matches {
                continue;
            }

            // Strip prefix to get relative path
            let relative = if prefix_str.is_empty() {
                k.as_str()
            } else if k.len() > prefix_str.len() {
                &k[prefix_str.len() + 1..]
            } else {
                continue;
            };

            // Check if there's a delimiter (/) in the relative path
            if let Some(slash_pos) = relative.find('/') {
                // It's a "directory" -- add as common prefix
                let dir = if prefix_str.is_empty() {
                    &relative[..slash_pos]
                } else {
                    &k[..prefix_str.len() + 1 + slash_pos]
                };
                common_prefixes.insert(Path::parse(dir).unwrap_or_else(|_| Path::from(dir)));
            } else {
                // Direct child -- add as object
                let path = Path::parse(k.as_str()).unwrap_or_else(|_| Path::from(k.as_str()));
                objects.push(Self::meta_for(&path, info));
            }
        }

        let mut prefixes: Vec<Path> = common_prefixes.into_iter().collect();
        prefixes.sort();
        objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

        Ok(ListResult {
            objects,
            common_prefixes: prefixes,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_key = from.to_string();
        let to_key = to.to_string();
        let mut inner = self.inner.write();

        inner
            .do_copy(&from_key, to_key)
            .map_err(|e| to_os_error(e))?;

        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_key = from.to_string();
        let to_key = to.to_string();
        let mut inner = self.inner.write();

        // Check + copy under the same lock -- no TOCTOU
        if inner.index.files.contains_key(&to_key) {
            return Err(object_store::Error::AlreadyExists {
                path: to_key.clone(),
                source: Box::new(crate::RawStoreError::AlreadyExists(to_key)),
            });
        }

        inner
            .do_copy(&from_key, to_key)
            .map_err(|e| to_os_error(e))?;

        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_key = from.to_string();
        let to_key = to.to_string();
        let mut inner = self.inner.write();

        if inner.read_only {
            return Err(to_os_error(crate::RawStoreError::ReadOnly));
        }
        if to_key.len() > inner.max_key_length {
            return Err(to_os_error(crate::RawStoreError::KeyTooLong {
                len: to_key.len(),
                max: inner.max_key_length,
            }));
        }

        let info = inner.index.files.remove(&from_key).ok_or_else(|| {
            object_store::Error::NotFound {
                path: from_key.clone(),
                source: Box::new(crate::RawStoreError::NotFound(from_key.clone())),
            }
        })?;

        // Free old extent if overwriting
        if let Some(old) = inner.index.files.remove(&to_key) {
            if let Some(alloc) = inner.allocator.as_mut() {
                alloc.free(old.offset, old.padded_size);
            }
        }

        inner.mark_shard_dirty(&from_key);
        inner.mark_shard_dirty(&to_key);
        inner.index.files.insert(to_key, info);
        inner.dirty = true;
        Ok(())
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_key = from.to_string();
        let to_key = to.to_string();
        let mut inner = self.inner.write();

        if inner.read_only {
            return Err(to_os_error(crate::RawStoreError::ReadOnly));
        }
        if to_key.len() > inner.max_key_length {
            return Err(to_os_error(crate::RawStoreError::KeyTooLong {
                len: to_key.len(),
                max: inner.max_key_length,
            }));
        }

        if inner.index.files.contains_key(&to_key) {
            return Err(object_store::Error::AlreadyExists {
                path: to_key.clone(),
                source: Box::new(crate::RawStoreError::AlreadyExists(to_key)),
            });
        }

        let info = inner.index.files.remove(&from_key).ok_or_else(|| {
            object_store::Error::NotFound {
                path: from_key.clone(),
                source: Box::new(crate::RawStoreError::NotFound(from_key.clone())),
            }
        })?;

        inner.mark_shard_dirty(&from_key);
        inner.mark_shard_dirty(&to_key);
        inner.index.files.insert(to_key, info);
        inner.dirty = true;
        Ok(())
    }
}

impl Drop for RawObjectStore {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.try_read() {
            if inner.dirty && !inner.read_only {
                warn!(
                    device = self.device_path.as_str(),
                    "RawObjectStore dropped with unflushed changes; call flush_index() before dropping"
                );
            }
        }
    }
}

/// Multipart upload: spools parts to temporary on-disk extents so
/// individual parts do not accumulate in RAM.  On complete(), parts
/// are streamed one at a time to the final extent -- peak RAM is
/// bounded to the size of a single part plus a 1 MB encoding buffer,
/// regardless of total object size.
///
/// If compression is enabled, parts are stream-compressed to a
/// temporary file first, then the compressed data is streamed to the
/// device.  Peak RAM for zstd/gzip: one part + compressor buffers +
/// 1 MB.  Snappy requires buffering all parts (raw format limitation).
struct RawMultipartUpload {
    inner: Arc<RwLock<Inner>>,
    location: Path,
    upload_id: u64,
    part_count: usize,
    part_keys: Vec<String>,
}

impl fmt::Debug for RawMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawMultipartUpload")
            .field("location", &self.location)
            .field("upload_id", &self.upload_id)
            .field("parts", &self.part_count)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for RawMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        let data: Bytes = payload.into();
        let key = format!("{}{}/{}", MULTIPART_TMP_PREFIX, self.upload_id, self.part_count);
        self.part_count += 1;
        self.part_keys.push(key.clone());
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner
                .write()
                .do_put(key, data)
                .map_err(|e| to_os_error(e))?;
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        use std::io::{Read, Seek, SeekFrom, Write};

        if self.part_keys.is_empty() {
            return Err(to_os_error(crate::RawStoreError::EmptyPayload));
        }

        let key = self.location.to_string();
        let mut inner = self.inner.write();

        // Collect part info from the index.  Each part may have been
        // individually compressed by do_put, so we track the ExtentInfo
        // to know whether decompression is needed when reading back.
        let mut part_infos: Vec<(String, ExtentInfo)> = Vec::with_capacity(self.part_keys.len());
        for pk in &self.part_keys {
            let info = inner.index.files.get(pk)
                .ok_or_else(|| to_os_error(crate::RawStoreError::NotFound(pk.clone())))?
                .clone();
            part_infos.push((pk.clone(), info));
        }
        // Total logical (uncompressed) size of all parts
        let total_uncompressed: u64 = part_infos.iter()
            .map(|(_, info)| {
                if info.uncompressed_size > 0 { info.uncompressed_size } else { info.size }
            })
            .sum();

        let compression = inner.compression;
        let needs_compression = compression != Compression::None
            && total_uncompressed >= 4096;

        // Determine what we're writing: compressed (via temp file) or uncompressed
        let (write_size, uncompressed_size, temp_source) = if needs_compression {
            let mut tmp = tempfile::tempfile()
                .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;

            // Stream-compress parts into the temp file
            match compression {
                Compression::Zstd => {
                    let mut enc = zstd::stream::write::Encoder::new(&mut tmp, 3)
                        .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                    for (_, part_info) in &part_infos {
                        let raw = crate::extent::read_and_decode_batched(
                            &inner.io, part_info.offset, part_info.size,
                        ).map_err(|e| to_os_error(e))?;
                        let content = if part_info.uncompressed_size > 0 {
                            compression.decompress(&raw).map_err(|e| to_os_error(e))?
                        } else {
                            raw
                        };
                        enc.write_all(&content)
                            .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                    }
                    let _ = enc.finish()
                        .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                }
                Compression::Snappy => {
                    // Raw snappy requires full input in memory
                    let mut all = Vec::with_capacity(total_uncompressed as usize);
                    for (_, part_info) in &part_infos {
                        let raw = crate::extent::read_and_decode_batched(
                            &inner.io, part_info.offset, part_info.size,
                        ).map_err(|e| to_os_error(e))?;
                        let content = if part_info.uncompressed_size > 0 {
                            compression.decompress(&raw).map_err(|e| to_os_error(e))?
                        } else {
                            raw
                        };
                        all.extend_from_slice(&content);
                    }
                    let mut enc = snap::raw::Encoder::new();
                    let compressed = enc.compress_vec(&all).map_err(|e| {
                        to_os_error(crate::RawStoreError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("snappy compress failed: {}", e),
                        )))
                    })?;
                    drop(all);
                    tmp.write_all(&compressed)
                        .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                }
                Compression::None => unreachable!(),
                _ => {
                    // gzip0..gzip9
                    let level = (compression as u8)
                        .saturating_sub(Compression::Gzip0 as u8);
                    let mut enc = flate2::write::GzEncoder::new(
                        &mut tmp,
                        flate2::Compression::new(level as u32),
                    );
                    for (_, part_info) in &part_infos {
                        let raw = crate::extent::read_and_decode_batched(
                            &inner.io, part_info.offset, part_info.size,
                        ).map_err(|e| to_os_error(e))?;
                        let content = if part_info.uncompressed_size > 0 {
                            compression.decompress(&raw).map_err(|e| to_os_error(e))?
                        } else {
                            raw
                        };
                        enc.write_all(&content)
                            .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                    }
                    let _ = enc.finish()
                        .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                }
            }

            let compressed_size = tmp.seek(SeekFrom::End(0))
                .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;

            if compressed_size < total_uncompressed {
                // Compression helped -- use the temp file
                tmp.seek(SeekFrom::Start(0))
                    .map_err(|e| to_os_error(crate::RawStoreError::Io(e)))?;
                (compressed_size, total_uncompressed, Some(tmp))
            } else {
                // Compression did not help -- write uncompressed from parts
                drop(tmp);
                (total_uncompressed, 0u64, None)
            }
        } else {
            (total_uncompressed, 0u64, None)
        };

        // Allocate extent on device
        let padded = padded_extent_size(write_size)
            .map_err(|e| to_os_error(e))?;
        let offset = inner.allocator_mut()
            .map_err(|e| to_os_error(e))?
            .alloc(padded)
            .map_err(|e| to_os_error(e))?;

        // Write CRC-protected blocks and compute payload CRC.
        // We clone the Arc<DeviceIo> so the StreamingBlockWriter borrows
        // the clone, leaving `inner` free for later mutable use.
        let crc_result: crate::Result<u32> = if let Some(mut tmp) = temp_source {
            // Stream from compressed temp file to device
            let io = Arc::clone(&inner.io);
            let mut writer = crate::extent::StreamingBlockWriter::new(&io, offset);
            let mut crc = 0u32;
            let mut buf = vec![0u8; 1024 * 1024];
            let mut write_err: Option<crate::RawStoreError> = None;
            loop {
                let n = match tmp.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => { write_err = Some(crate::RawStoreError::Io(e)); break; }
                };
                crc = crc32c::crc32c_append(crc, &buf[..n]);
                if let Err(e) = writer.write_chunk(&buf[..n]) {
                    write_err = Some(e);
                    break;
                }
            }
            if write_err.is_none() {
                if let Err(e) = writer.finish() {
                    write_err = Some(e);
                }
            }
            match write_err {
                Some(e) => Err(e),
                None => Ok(crc),
            }
        } else {
            // Stream from on-disk parts directly to device (uncompressed)
            let io = Arc::clone(&inner.io);
            let mut writer = crate::extent::StreamingBlockWriter::new(&io, offset);
            let mut crc = 0u32;
            let mut write_err: Option<crate::RawStoreError> = None;
            for (_, part_info) in &part_infos {
                let raw = match crate::extent::read_and_decode_batched(
                    &io, part_info.offset, part_info.size,
                ) {
                    Ok(c) => c,
                    Err(e) => { write_err = Some(e); break; }
                };
                // Decompress if the individual part was compressed
                let content = if part_info.uncompressed_size > 0 {
                    match compression.decompress(&raw) {
                        Ok(d) => d,
                        Err(e) => { write_err = Some(e); break; }
                    }
                } else {
                    raw
                };
                crc = crc32c::crc32c_append(crc, &content);
                if let Err(e) = writer.write_chunk(&content) {
                    write_err = Some(e);
                    break;
                }
            }
            if write_err.is_none() {
                if let Err(e) = writer.finish() {
                    write_err = Some(e);
                }
            }
            match write_err {
                Some(e) => Err(e),
                None => Ok(crc),
            }
        };

        let crc_state = match crc_result {
            Ok(crc) => crc,
            Err(e) => {
                if let Ok(alloc) = inner.allocator_mut() {
                    alloc.free(offset, padded);
                }
                return Err(to_os_error(e));
            }
        };

        // Free old extent if overwriting
        if let Some(old) = inner.index.files.remove(&key) {
            if let Some(alloc) = inner.allocator.as_mut() {
                alloc.free(old.offset, old.padded_size);
            }
            inner.mark_shard_dirty(&key);
        }

        // Clear tombstone if one exists
        inner.index.tombstones.remove(&key);

        // Insert final extent into index
        inner.mark_shard_dirty(&key);
        let txn_id = inner.superblock.txn_id;
        inner.index.files.insert(
            key,
            ExtentInfo {
                offset,
                size: write_size,
                padded_size: padded,
                crc32c: crc_state,
                created_txn: txn_id,
                last_modified: Utc::now(),
                meta_len: 0,
                uncompressed_size,
            },
        );

        inner.dirty = true;

        // Clean up temporary part extents
        for part_key in &self.part_keys {
            if let Some(info) = inner.index.files.remove(part_key) {
                if let Some(alloc) = inner.allocator.as_mut() {
                    alloc.free(info.offset, info.padded_size);
                }
                inner.mark_shard_dirty(part_key);
            }
        }
        self.part_keys.clear();

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut inner = self.inner.write();
        for part_key in &self.part_keys {
            if let Some(info) = inner.index.files.remove(part_key) {
                if let Some(alloc) = inner.allocator.as_mut() {
                    alloc.free(info.offset, info.padded_size);
                }
                inner.mark_shard_dirty(part_key);
            }
        }
        self.part_keys.clear();
        inner.dirty = true;
        Ok(())
    }
}