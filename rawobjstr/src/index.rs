use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Result, RawStoreError};

/// Determine which shard a key belongs to (0..255).
pub(crate) fn shard_for_key(key: &str) -> u8 {
    (crc32c::crc32c(key.as_bytes()) & 0xFF) as u8
}

/// Data for a single index shard, serialized independently to disk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct ShardData {
    pub files: HashMap<String, ExtentInfo>,
    #[serde(default)]
    pub tombstones: HashMap<String, TombstoneInfo>,
}

impl ShardData {
    /// Serialize this shard and compute its CRC32c.
    pub fn to_bytes(&self) -> Result<(Vec<u8>, u32)> {
        let data = bincode::serialize(self)?;
        let crc = crc32c::crc32c(&data);
        Ok((data, crc))
    }

    /// Deserialize a shard blob and verify its CRC32c.
    pub fn from_bytes(data: &[u8], expected_crc: u32) -> Result<Self> {
        let actual_crc = crc32c::crc32c(data);
        if actual_crc != expected_crc {
            return Err(RawStoreError::IndexCorrupt);
        }
        let shard: ShardData = bincode::deserialize(data)?;
        Ok(shard)
    }
}

/// In-memory device index: maps ObjectStore paths to physical extents.
///
/// In the sharded format this is purely in-memory -- persistence is
/// handled by `ShardData` + `Superblock`.  The free list is always
/// rebuilt from extent gaps on open, and the transaction counter lives
/// in the superblock.
#[derive(Debug, Clone, Default)]
pub(crate) struct DeviceIndex {
    /// Map from ObjectStore path (string) to extent info
    pub files: HashMap<String, ExtentInfo>,
    /// Tombstones: objects removed during open-time integrity scan.
    /// Key is the original ObjectStore path.
    pub tombstones: HashMap<String, TombstoneInfo>,
}

/// Information about a single stored file/extent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExtentInfo {
    /// Byte offset on device where the extent starts (including header)
    pub offset: u64,
    /// Actual payload size (excluding header and padding)
    pub size: u64,
    /// Total size including header + alignment padding
    pub padded_size: u64,
    /// CRC32c checksum of the payload
    pub crc32c: u32,
    /// Transaction ID when this extent was written
    pub created_txn: u64,
    /// When this extent was last written
    pub last_modified: DateTime<Utc>,
    /// Length of opaque metadata bytes appended after the body within the
    /// payload.  The body occupies `[0 .. size - meta_len)` and the metadata
    /// occupies `[size - meta_len .. size)`.  Default 0 = no metadata.
    #[serde(default)]
    pub meta_len: u16,
    /// Uncompressed payload size in bytes.  0 means the extent is stored
    /// uncompressed (either compression is `None` or the compressed form
    /// was not smaller than the original).  When > 0, `size` holds the
    /// on-disk (compressed) payload size and this field holds the original
    /// uncompressed size.
    #[serde(default)]
    pub uncompressed_size: u64,
}

/// A tombstone records an object that was removed during open-time integrity
/// scanning.  The original data is lost (overwritten on disk), but the
/// tombstone preserves the key name and metadata so that an orchestrator can
/// restore from a redundant backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TombstoneInfo {
    /// Original payload size in bytes.
    pub size: u64,
    /// Original CRC32c of the payload (from the index entry).
    pub crc32c: u32,
    /// When the object was last written.
    pub last_modified: DateTime<Utc>,
    /// Why the entry was removed (human-readable).
    pub reason: String,
    /// Transaction ID when the tombstone was created.
    pub tombstone_txn: u64,
}

impl DeviceIndex {
    /// Create a new empty index.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            tombstones: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_round_trip() {
        let mut shard = ShardData::default();
        shard.files.insert(
            "data/00000.db".to_string(),
            ExtentInfo {
                offset: 8192,
                size: 1024,
                padded_size: 4096,
                crc32c: 0x12345678,
                created_txn: 1,
                last_modified: Utc::now(),
                meta_len: 0,
                uncompressed_size: 0,
            },
        );

        let (data, crc) = shard.to_bytes().unwrap();
        let shard2 = ShardData::from_bytes(&data, crc).unwrap();
        assert_eq!(shard2.files.len(), 1);
    }

    #[test]
    fn shard_corrupt_detection() {
        let shard = ShardData::default();
        let (mut data, crc) = shard.to_bytes().unwrap();
        data[0] ^= 0xFF;
        assert!(ShardData::from_bytes(&data, crc).is_err());
    }

    #[test]
    fn shard_for_key_deterministic() {
        let a = shard_for_key("foo/bar");
        let b = shard_for_key("foo/bar");
        assert_eq!(a, b);
        // Verify a different key doesn't panic (return type is u8, always in range)
        let _c = shard_for_key("baz/qux");
    }
}
