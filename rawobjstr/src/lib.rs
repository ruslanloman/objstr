//! # rawobjstr
//!
//! An [`ObjectStore`](object_store::ObjectStore) implementation that writes
//! directly to a raw block device or loopback image file, bypassing the
//! filesystem entirely for 2-5x faster I/O.
//!
//! Each object is stored as a single contiguous extent with per-block CRC32c
//! protection, a sharded on-disk index, and dual-buffered crash-safe
//! superblocks. The store supports transparent compression (zstd, snappy,
//! gzip), multipart uploads, read-only mode, and import/export to other
//! `ObjectStore` backends.
//!
//! ## Platform
//!
//! Full functionality (O_DIRECT, block device ioctl) requires **Linux**.
//! The crate compiles on other platforms but some options are unavailable.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::path::Path;
//! use rawobjstr::store::RawObjectStore;
//!
//! // Format a 1 GB loopback image and open it
//! let store = RawObjectStore::format_with_size(
//!     Path::new("/tmp/my_store.raw"),
//!     1_073_741_824,
//!     false, // no O_DIRECT
//! ).unwrap();
//! ```
//!
//! ## Feature flags
//!
//! - **`aws`** -- enables the `object_store/aws` feature for S3 import/export.

#[doc(hidden)]
pub mod allocator;
#[doc(hidden)]
pub mod extent;
#[doc(hidden)]
pub mod index;
pub mod event;
pub mod io;
pub mod store;
pub mod superblock;

/// Git commit hash baked in at build time (e.g. `a1b2c3d` or `a1b2c3d-dirty`).
pub const BUILD_GIT_HASH: &str = env!("BUILD_GIT_HASH");
/// UTC timestamp when the crate was compiled.
pub const BUILD_DATE: &str = env!("BUILD_DATE");
/// Crate version from Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use thiserror::Error;

/// All errors returned by the raw object store.
#[derive(Error, Debug)]
pub enum RawStoreError {
    #[error("device not formatted (bad superblock magic)")]
    NotFormatted,

    #[error("superblock corrupt: both copies failed checksum")]
    SuperblockCorrupt,

    #[error("index corrupt: checksum mismatch")]
    IndexCorrupt,

    #[error("data corruption at path {path}: expected CRC {expected:#010x}, got {actual:#010x}")]
    DataCorruption {
        path: String,
        expected: u32,
        actual: u32,
    },

    #[error("invalid extent: {reason}")]
    ExtentInvalid { reason: String },

    #[error("no space left on device (need {needed} bytes, largest free {available})")]
    NoSpace { needed: u64, available: u64 },

    #[error("path not found: {0}")]
    NotFound(String),

    #[error("path already exists: {0}")]
    AlreadyExists(String),

    #[error("device too small: {size} bytes (minimum {minimum})")]
    DeviceTooSmall { size: u64, minimum: u64 },

    #[error("invalid index slot size: {size} bytes (must be a multiple of 16 MB, minimum 16 MB)")]
    InvalidIndexSlotSize { size: u64 },

    #[error("key too long: {len} bytes exceeds maximum {max}")]
    KeyTooLong { len: usize, max: usize },

    #[error("index shard {shard} overflow: serialized size {size} bytes exceeds slot capacity {capacity}")]
    ShardOverflow { shard: usize, size: u64, capacity: u64 },

    #[error("max_key_length {len} exceeds the hard ceiling of {max} bytes (64 KB); lower the value or use a larger index slot size")]
    MaxKeyLengthTooLarge { len: usize, max: usize },

    #[error("metadata too large: {len} bytes exceeds maximum {max} (u16::MAX)")]
    MetadataTooLarge { len: usize, max: usize },

    #[error("zero-byte objects are not supported")]
    EmptyPayload,

    #[error("device is open in read-only mode")]
    ReadOnly,

    #[error("device has write-protect flag set; clear it with `set-property --write-protect off` first")]
    WriteProtected,

    #[error("device is already locked by another process: {path}")]
    DeviceLocked { path: String },

    #[error("insufficient writes for {path}: required {required}, got {actual}")]
    InsufficientWrites { path: String, required: usize, actual: usize },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub type Result<T> = std::result::Result<T, RawStoreError>;

/// Superblock size: 4 KB (one sector)
pub const SUPERBLOCK_SIZE: u64 = 4096;

/// Two superblock copies: primary at offset 0, backup at offset 4096
/// Total reserved: 8 KB
pub const SUPERBLOCK_REGION: u64 = SUPERBLOCK_SIZE * 2;

/// Data starts right after superblocks
pub const DATA_START: u64 = SUPERBLOCK_REGION;

/// Block alignment for all allocations
pub const BLOCK_ALIGNMENT: u64 = 4096;

/// Index region size – default slot capacity (each of the two regions)
pub const INDEX_REGION_SIZE: u64 = 16 * 1024 * 1024; // 16 MB

/// Total index reservation with default slot size (two regions for double-buffering)
pub const INDEX_TOTAL_SIZE: u64 = INDEX_REGION_SIZE * 2;

/// Minimum device size with default (16 MB) index slots
pub const MIN_DEVICE_SIZE: u64 = SUPERBLOCK_REGION + BLOCK_ALIGNMENT + INDEX_TOTAL_SIZE;

/// Compute the minimum device size for a given index slot capacity.
pub fn min_device_size(index_slot_size: u64) -> u64 {
    SUPERBLOCK_REGION + BLOCK_ALIGNMENT + index_slot_size * 2
}

/// Validate that an index slot size is acceptable (>= 16 MB, multiple of 16 MB).
pub fn validate_index_slot_size(slot_size: u64) -> Result<()> {
    if slot_size < INDEX_REGION_SIZE || slot_size % INDEX_REGION_SIZE != 0 {
        return Err(RawStoreError::InvalidIndexSlotSize { size: slot_size });
    }
    Ok(())
}

/// Superblock magic
pub const SUPERBLOCK_MAGIC: &[u8; 8] = b"RAWOBJST";

/// Number of index shards.
pub const NUM_SHARDS: usize = 256;

/// Superblock flag: device was formatted with O_DIRECT
pub const FLAG_DIRECT_IO: u32 = 1 << 0;

/// Superblock flag: device is write-protected.  When set, `open()` (read-write)
/// will fail with `WriteProtected`; only `open_readonly()` succeeds.
pub const FLAG_WRITE_PROTECT: u32 = 1 << 1;

/// Default maximum object key length in bytes.
/// AWS S3 limits keys to 1024 bytes; we match that for interop.
/// Override at format time via `FormatOptions::max_key_length`.
///
/// **Caveat:** The absolute ceiling is the shard slot size (index_slot_size /
/// NUM_SHARDS) minus ~98 bytes of bincode/CRC overhead.  With the default
/// 16 MB index slot that is ~65 438 bytes.  Setting `max_key_length` higher
/// than this will be accepted at format time but a single key near the limit
/// may fail at `flush_index()` when other keys share the same shard.
pub const MAX_KEY_LENGTH: usize = 1024;

/// Alias for clarity: this is the default when no custom limit is specified.
pub const DEFAULT_MAX_KEY_LENGTH: usize = MAX_KEY_LENGTH;

/// Absolute hard ceiling for `FormatOptions::max_key_length`.
/// Format rejects any value strictly above this; the 64 KB bound is safe
/// for any index slot size >= 16 MB  (shard_slot = 64 KB − overhead).
pub const MAX_KEY_LENGTH_HARD_CEILING: usize = 65_536; // 64 KB

/// Mask of all known superblock flag bits.
pub const FLAG_KNOWN_MASK: u32 = FLAG_DIRECT_IO | FLAG_WRITE_PROTECT;

/// Maximum uncompressed object size (in bytes) for which range reads are
/// supported on compressed extents.  Above this threshold `get_range()`
/// returns an error -- callers should fetch the whole object instead.
///
/// Rationale: a compressed extent must be fully decompressed before a byte
/// range can be extracted.  For very large objects this would buffer the
/// entire decompressed payload in RAM, negating the benefit of range reads.
pub const COMPRESSED_RANGE_READ_MAX: u64 = 1024 * 1024 * 1024; // 1 GB

/// Compression algorithm applied to all objects on a device.
///
/// Chosen at format time and stored in the superblock.  Individual objects
/// may still be stored uncompressed if the compressed form is not smaller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum Compression {
    None   = 0,
    Zstd   = 1,
    Snappy = 2,
    Gzip0  = 3,
    Gzip1  = 4,
    Gzip2  = 5,
    Gzip3  = 6,
    Gzip4  = 7,
    Gzip5  = 8,
    Gzip6  = 9,
    Gzip7  = 10,
    Gzip8  = 11,
    Gzip9  = 12,
}

impl Compression {
    /// Convert from a u8 stored on disk.
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0  => Ok(Self::None),
            1  => Ok(Self::Zstd),
            2  => Ok(Self::Snappy),
            3  => Ok(Self::Gzip0),
            4  => Ok(Self::Gzip1),
            5  => Ok(Self::Gzip2),
            6  => Ok(Self::Gzip3),
            7  => Ok(Self::Gzip4),
            8  => Ok(Self::Gzip5),
            9  => Ok(Self::Gzip6),
            10 => Ok(Self::Gzip7),
            11 => Ok(Self::Gzip8),
            12 => Ok(Self::Gzip9),
            _  => Err(RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown compression algorithm: {}", v),
            ))),
        }
    }

    /// Parse from a human-readable string (case-insensitive).
    pub fn from_str_name(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none"  => Ok(Self::None),
            "zstd"  => Ok(Self::Zstd),
            "snappy" => Ok(Self::Snappy),
            "gzip0" => Ok(Self::Gzip0),
            "gzip1" => Ok(Self::Gzip1),
            "gzip2" => Ok(Self::Gzip2),
            "gzip3" => Ok(Self::Gzip3),
            "gzip4" => Ok(Self::Gzip4),
            "gzip5" => Ok(Self::Gzip5),
            "gzip6" => Ok(Self::Gzip6),
            "gzip7" => Ok(Self::Gzip7),
            "gzip8" => Ok(Self::Gzip8),
            "gzip9" => Ok(Self::Gzip9),
            _ => Err(RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unknown compression '{}'; valid: none, zstd, snappy, gzip0..gzip9",
                    s
                ),
            ))),
        }
    }

    /// Human-readable name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None   => "none",
            Self::Zstd   => "zstd",
            Self::Snappy => "snappy",
            Self::Gzip0  => "gzip0",
            Self::Gzip1  => "gzip1",
            Self::Gzip2  => "gzip2",
            Self::Gzip3  => "gzip3",
            Self::Gzip4  => "gzip4",
            Self::Gzip5  => "gzip5",
            Self::Gzip6  => "gzip6",
            Self::Gzip7  => "gzip7",
            Self::Gzip8  => "gzip8",
            Self::Gzip9  => "gzip9",
        }
    }

    /// Compress `data` using the configured algorithm.
    /// Returns `None` for `Compression::None`.
    pub fn compress(&self, data: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::None => None,
            Self::Zstd => Some(match zstd::bulk::compress(data, 3) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(codec = "zstd", len = data.len(), error = %e, "compression failed, storing uncompressed");
                    data.to_vec()
                }
            }),
            Self::Snappy => {
                let mut enc = snap::raw::Encoder::new();
                Some(match enc.compress_vec(data) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(codec = "snappy", len = data.len(), error = %e, "compression failed, storing uncompressed");
                        data.to_vec()
                    }
                })
            }
            _ => {
                // gzip0..gzip9
                use std::io::Write;
                let level = (*self as u8).saturating_sub(Self::Gzip0 as u8);
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::new(level as u32),
                );
                if let Err(e) = encoder.write_all(data) {
                    tracing::warn!(codec = %self.as_str(), len = data.len(), error = %e, "compression write failed, storing uncompressed");
                    return Some(data.to_vec());
                }
                Some(match encoder.finish() {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(codec = %self.as_str(), len = data.len(), error = %e, "compression finish failed, storing uncompressed");
                        data.to_vec()
                    }
                })
            }
        }
    }

    /// Decompress `data` using the configured algorithm.
    ///
    /// Used for partial range reads (which require the full decompressed
    /// payload to slice from) and internal helpers.  Full streaming GETs
    /// use `BlockReaderIo` + a streaming decoder instead of this method.
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::None => Ok(data.to_vec()),
            Self::Zstd => {
                // Use streaming decode so there is no hardcoded output size
                // limit -- zstd::bulk::decompress would panic on objects larger
                // than the supplied max_output_size.
                use std::io::Read;
                let mut decoder = zstd::stream::read::Decoder::new(data)
                    .map_err(|e| RawStoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("zstd decoder init failed: {}", e),
                    )))?;
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)
                    .map_err(|e| RawStoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("zstd decompress failed: {}", e),
                    )))?;
                Ok(out)
            }
            Self::Snappy => {
                let mut dec = snap::raw::Decoder::new();
                dec.decompress_vec(data).map_err(|e| RawStoreError::Io(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("snappy decompress failed: {}", e),
                    ),
                ))
            }
            _ => {
                use std::io::Read;
                let mut decoder = flate2::read::GzDecoder::new(data);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out).map_err(|e| RawStoreError::Io(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("gzip decompress failed: {}", e),
                    ),
                ))?;
                Ok(out)
            }
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

/// Align a value up to the given alignment
pub fn align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_basic() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4095, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn min_device_size_matches_constant() {
        assert_eq!(min_device_size(INDEX_REGION_SIZE), MIN_DEVICE_SIZE);
    }

    #[test]
    fn min_device_size_scales_with_slot() {
        let double = min_device_size(INDEX_REGION_SIZE * 2);
        assert_eq!(double, SUPERBLOCK_REGION + BLOCK_ALIGNMENT + INDEX_REGION_SIZE * 4);
    }

    #[test]
    fn validate_index_slot_size_accepts_valid() {
        assert!(validate_index_slot_size(INDEX_REGION_SIZE).is_ok());
        assert!(validate_index_slot_size(INDEX_REGION_SIZE * 2).is_ok());
        assert!(validate_index_slot_size(INDEX_REGION_SIZE * 4).is_ok());
    }

    #[test]
    fn validate_index_slot_size_rejects_invalid() {
        assert!(validate_index_slot_size(0).is_err());
        assert!(validate_index_slot_size(INDEX_REGION_SIZE - 1).is_err());
        assert!(validate_index_slot_size(INDEX_REGION_SIZE + 1).is_err());
    }

    #[test]
    fn compression_from_u8_roundtrip() {
        for v in 0..=12u8 {
            let c = Compression::from_u8(v).unwrap();
            assert_eq!(c as u8, v);
        }
    }

    #[test]
    fn compression_from_u8_invalid() {
        assert!(Compression::from_u8(13).is_err());
        assert!(Compression::from_u8(255).is_err());
    }

    #[test]
    fn compression_from_str_name_roundtrip() {
        let names = [
            "none", "zstd", "snappy",
            "gzip0", "gzip1", "gzip2", "gzip3", "gzip4",
            "gzip5", "gzip6", "gzip7", "gzip8", "gzip9",
        ];
        for name in names {
            let c = Compression::from_str_name(name).unwrap();
            assert_eq!(c.as_str(), name);
        }
    }

    #[test]
    fn compression_from_str_name_case_insensitive() {
        assert!(Compression::from_str_name("ZSTD").is_ok());
        assert!(Compression::from_str_name("Snappy").is_ok());
        assert!(Compression::from_str_name("GZIP6").is_ok());
    }

    #[test]
    fn compression_from_str_name_invalid() {
        assert!(Compression::from_str_name("lz4").is_err());
        assert!(Compression::from_str_name("").is_err());
        assert!(Compression::from_str_name("gzip10").is_err());
    }

    #[test]
    fn compression_display() {
        assert_eq!(format!("{}", Compression::None), "none");
        assert_eq!(format!("{}", Compression::Zstd), "zstd");
        assert_eq!(format!("{}", Compression::Gzip6), "gzip6");
    }

    #[test]
    fn compression_default_is_none() {
        assert_eq!(Compression::default(), Compression::None);
    }

    #[test]
    fn compression_compress_none_returns_none() {
        assert!(Compression::None.compress(b"hello").is_none());
    }

    #[test]
    fn compression_decompress_none_returns_copy() {
        let data = b"hello world";
        let out = Compression::None.decompress(data).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn compression_zstd_roundtrip() {
        let data = vec![0xAAu8; 8192];
        let compressed = Compression::Zstd.compress(&data).unwrap();
        let decompressed = Compression::Zstd.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compression_snappy_roundtrip() {
        let data = vec![0xBBu8; 8192];
        let compressed = Compression::Snappy.compress(&data).unwrap();
        let decompressed = Compression::Snappy.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compression_gzip_roundtrip() {
        let data = vec![0xCCu8; 8192];
        let compressed = Compression::Gzip6.compress(&data).unwrap();
        let decompressed = Compression::Gzip6.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn error_display_includes_details() {
        let e = RawStoreError::NoSpace { needed: 1024, available: 512 };
        let msg = format!("{}", e);
        assert!(msg.contains("1024"));
        assert!(msg.contains("512"));
    }

    #[test]
    fn error_display_data_corruption() {
        let e = RawStoreError::DataCorruption {
            path: "test/file.bin".into(),
            expected: 0xDEADBEEF,
            actual: 0x12345678,
        };
        let msg = format!("{}", e);
        assert!(msg.contains("test/file.bin"));
        assert!(msg.contains("0xdeadbeef"));
        assert!(msg.contains("0x12345678"));
    }
}
