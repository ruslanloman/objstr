use serde::{Deserialize, Serialize};

use crate::{Compression, RawStoreError, Result, SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE, NUM_SHARDS};

/// Per-shard slot metadata stored in the superblock.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSlotMeta {
    /// Which region holds the active copy: 0 = region A, 1 = region B.
    pub active_slot: u8,
    /// Serialized size in bytes of this shard's blob.
    pub size: u32,
    /// CRC32c of the serialized shard blob.
    pub crc: u32,
}

impl Default for ShardSlotMeta {
    fn default() -> Self {
        Self { active_slot: 0, size: 0, crc: 0 }
    }
}

/// On-disk superblock structure, stored at offset 0 (primary) and 4096 (backup).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Superblock {
    pub magic: [u8; 8],
    pub version: u32,
    pub flags: u32,
    pub device_size: u64,
    pub block_alignment: u64,
    pub index_region_offset: u64,
    pub index_region_size: u64,
    pub txn_id: u64,
    pub index_checksum: u32,
    pub superblock_checksum: u32,
    /// Previous index offset (reserved for crash rollback — not yet used).
    #[allow(dead_code)]
    pub prev_index_offset: u64,
    /// Previous index size (reserved for crash rollback — not yet used).
    #[allow(dead_code)]
    pub prev_index_size: u64,
    /// Previous transaction ID (reserved for crash rollback — not yet used).
    #[allow(dead_code)]
    pub prev_txn_id: u64,
    /// Previous index checksum (reserved for crash rollback — not yet used).
    #[allow(dead_code)]
    pub prev_index_checksum: u32,
    /// Maximum capacity (in bytes) of each index slot.
    /// Must be a multiple of 16 MB.  Stored at format time so that
    /// `open()` can derive the region offsets without hardcoded constants.
    pub index_slot_capacity: u64,
    /// Per-shard slot metadata.  Length == NUM_SHARDS (256).
    #[serde(default)]
    pub shard_slots: Vec<ShardSlotMeta>,

    /// Maximum allowed key length in bytes; enforced on put/copy.
    pub max_key_length: u32,

    /// Compression algorithm applied to all objects on this device.
    /// See `Compression::from_u8()`.
    pub compression: u8,
}

impl Superblock {
    /// Create a fresh superblock for a new device.
    pub fn new(device_size: u64, block_alignment: u64, index_a_offset: u64, index_slot_capacity: u64, max_key_length: u32, compression: Compression) -> Self {
        Self {
            magic: *SUPERBLOCK_MAGIC,
            version: 4,
            flags: 0,
            device_size,
            block_alignment,
            index_region_offset: index_a_offset,
            index_region_size: 0,
            txn_id: 0,
            index_checksum: 0,
            superblock_checksum: 0,
            prev_index_offset: index_a_offset,
            prev_index_size: 0,
            prev_txn_id: 0,
            prev_index_checksum: 0,
            index_slot_capacity,
            shard_slots: vec![ShardSlotMeta::default(); NUM_SHARDS],
            max_key_length,
            compression: compression as u8,
        }
    }

    /// Serialize to a fixed-size byte buffer (SUPERBLOCK_SIZE bytes).
    ///
    /// The checksum is computed over the entire padded buffer (with the
    /// checksum field itself zeroed), ensuring both write and read use
    /// an identical byte sequence for CRC computation.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut sb = self.clone();
        sb.superblock_checksum = 0;
        let data = bincode::serialize(&sb)?;

        // Build the final padded buffer with checksum field zeroed
        let mut buf = vec![0u8; SUPERBLOCK_SIZE as usize];
        if data.len() > buf.len() {
            return Err(RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "serialized superblock ({}) exceeds SUPERBLOCK_SIZE ({})",
                    data.len(),
                    SUPERBLOCK_SIZE
                ),
            )));
        }
        buf[..data.len()].copy_from_slice(&data);

        // Compute checksum over the entire padded buffer (checksum field is zero)
        let checksum = crc32c::crc32c(&buf);

        // Locate the superblock_checksum field by searching for the 4 zero
        // bytes that remain after we zeroed sb.superblock_checksum before
        // serializing.  We compute the offset programmatically: serialize the
        // struct *with* a known non-zero sentinel in the checksum field, and
        // find where it differs from the zeroed version.
        let checksum_offset = {
            let mut probe = sb.clone();
            probe.superblock_checksum = 0xDEAD_BEEF;
            let probe_data = bincode::serialize(&probe)?;
            let mut off = None;
            for i in 0..data.len() {
                if data[i] != probe_data[i] {
                    off = Some(i);
                    break;
                }
            }
            off.ok_or_else(|| RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot locate superblock_checksum field in serialized data",
            )))?
        };

        buf[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());
        Ok(buf)
    }

    /// Deserialize from a byte buffer and validate the checksum.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let sb: Superblock = bincode::deserialize(buf)?;

        if sb.magic != *SUPERBLOCK_MAGIC {
            return Err(RawStoreError::NotFormatted);
        }

        // Reject unknown versions
        if sb.version != 4 {
            return Err(RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported superblock version: {} (expected 4)", sb.version),
            )));
        }

        // Validate shard_slots length
        if sb.shard_slots.len() != NUM_SHARDS {
            return Err(RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("shard_slots length {} != expected {}", sb.shard_slots.len(), NUM_SHARDS),
            )));
        }

        // Validate basic field sanity
        if sb.block_alignment == 0 {
            return Err(RawStoreError::SuperblockCorrupt);
        }

        // Re-compute checksum: zero the checksum field and CRC the padded buffer.
        // Locate the field the same way to_bytes does.
        let checksum_offset = {
            let mut probe_sb = sb.clone();
            probe_sb.superblock_checksum = 0;
            let zero_data = bincode::serialize(&probe_sb)?;
            probe_sb.superblock_checksum = 0xDEAD_BEEF;
            let probe_data = bincode::serialize(&probe_sb)?;
            let mut off = None;
            for i in 0..zero_data.len() {
                if zero_data[i] != probe_data[i] {
                    off = Some(i);
                    break;
                }
            }
            off.ok_or_else(|| RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot locate superblock_checksum field in serialized data",
            )))?
        };

        let mut check_buf = vec![0u8; SUPERBLOCK_SIZE as usize];
        let copy_len = buf.len().min(SUPERBLOCK_SIZE as usize);
        check_buf[..copy_len].copy_from_slice(&buf[..copy_len]);
        check_buf[checksum_offset..checksum_offset + 4].copy_from_slice(&0u32.to_le_bytes());
        let expected = crc32c::crc32c(&check_buf);

        if expected != sb.superblock_checksum {
            return Err(RawStoreError::SuperblockCorrupt);
        }

        Ok(sb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let sb = Superblock::new(1024 * 1024 * 1024, 4096, 900 * 1024 * 1024, 16 * 1024 * 1024, 1024, Compression::None);
        let bytes = sb.to_bytes().unwrap();
        assert_eq!(bytes.len(), SUPERBLOCK_SIZE as usize);
        let sb2 = Superblock::from_bytes(&bytes).unwrap();
        assert_eq!(sb2.magic, *SUPERBLOCK_MAGIC);
        assert_eq!(sb2.device_size, 1024 * 1024 * 1024);
        assert_eq!(sb2.txn_id, 0);
        assert_eq!(sb2.index_slot_capacity, 16 * 1024 * 1024);
        assert_eq!(sb2.max_key_length, 1024);
    }

    #[test]
    fn corrupt_detection() {
        let sb = Superblock::new(1024 * 1024 * 1024, 4096, 900 * 1024 * 1024, 16 * 1024 * 1024, 1024, Compression::None);
        let mut bytes = sb.to_bytes().unwrap();
        bytes[10] ^= 0xFF; // corrupt a byte
        assert!(Superblock::from_bytes(&bytes).is_err());
    }
}
