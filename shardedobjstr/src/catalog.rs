use std::collections::HashMap;
use std::path::Path as StdPath;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::ObjectMeta;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::ShardId;

/// Where a single object lives across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementEntry {
    /// Shard IDs that hold a copy. The first entry is the hash-selected
    /// shard; reads are round-robin balanced across all entries.
    pub shards: Vec<ShardId>,
    /// Object body size in bytes (excludes metadata suffix).
    pub size: u64,
    /// CRC32c of the canonical payload (for cross-shard verification).
    pub crc32c: Option<u32>,
    /// When the catalog entry was last updated.
    pub updated: DateTime<Utc>,
    /// Length of the metadata suffix stored after the body on raw shards.
    /// Zero for objects without metadata or on non-raw shards.
    #[serde(default)]
    pub meta_len: u16,
}

/// In-memory placement catalog: maps object paths to the shards that hold them.
///
/// POC: kept entirely in memory. Production would persist this to a dedicated
/// metadata shard or a small RawObjectStore device.
pub struct Catalog {
    inner: RwLock<HashMap<String, PlacementEntry>>,
    /// True when the catalog has been mutated since the last save.
    dirty: AtomicBool,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            dirty: AtomicBool::new(false),
        }
    }

    /// Returns true if the catalog has been mutated since the last
    /// `clear_dirty()` call.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Mark the catalog as clean (called after a successful save).
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    /// Mark the catalog as dirty (called after mutations).
    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Record a full placement (used by put).
    pub fn put(&self, key: String, shards: Vec<ShardId>, size: u64, crc32c: Option<u32>, meta_len: u16) {
        self.inner.write().insert(
            key,
            PlacementEntry {
                shards,
                size,
                crc32c,
                updated: Utc::now(),
                meta_len,
            },
        );
        self.mark_dirty();
    }

    /// Add a single replica to an existing entry (used during rebuild / replication).
    ///
    /// When `meta_len` is provided it is stored on the entry.  For
    /// existing entries `meta_len` is only updated when the caller
    /// passes a non-zero value (avoids clobbering a known meta_len
    /// with zero from a shard that does not track it).
    pub fn add_replica(&self, key: &str, shard_id: ShardId, size: u64, meta_len: u16) {
        let mut map = self.inner.write();
        let entry = map.entry(key.to_string()).or_insert_with(|| PlacementEntry {
            shards: Vec::new(),
            size,
            crc32c: None,
            updated: Utc::now(),
            meta_len,
        });
        if meta_len > 0 {
            entry.meta_len = meta_len;
        }
        if !entry.shards.contains(&shard_id) {
            entry.shards.push(shard_id);
        }
        self.mark_dirty();
    }

    /// Look up placement for an object.
    pub fn get(&self, key: &str) -> Option<PlacementEntry> {
        self.inner.read().get(key).cloned()
    }

    /// Atomically insert a placement entry only if the key does not already exist.
    /// Returns `true` if inserted, `false` if the key was already present.
    pub fn try_insert(&self, key: String, shards: Vec<ShardId>, size: u64) -> bool {
        use std::collections::hash_map::Entry;
        let mut map = self.inner.write();
        match map.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(PlacementEntry {
                    shards,
                    size,
                    crc32c: None,
                    updated: Utc::now(),
                    meta_len: 0,
                });
                self.mark_dirty();
                true
            }
        }
    }

    /// Remove an entry, returning it if it existed.
    pub fn remove(&self, key: &str) -> Option<PlacementEntry> {
        let removed = self.inner.write().remove(key);
        if removed.is_some() {
            self.mark_dirty();
        }
        removed
    }

    /// Clear the entire catalog (used before rebuild).
    pub fn clear(&self) {
        self.inner.write().clear();
        self.mark_dirty();
    }

    /// Atomically replace the entire catalog contents.
    pub fn replace(&self, new_map: HashMap<String, PlacementEntry>) {
        *self.inner.write() = new_map;
        self.mark_dirty();
    }

    /// Remove a specific shard from an entry's shard list.
    /// If no shards remain, the entry is removed entirely.
    pub fn remove_shard(&self, key: &str, shard_id: crate::ShardId) {
        let mut map = self.inner.write();
        if let Some(entry) = map.get_mut(key) {
            entry.shards.retain(|&s| s != shard_id);
            if entry.shards.is_empty() {
                map.remove(key);
            }
            self.mark_dirty();
        }
    }

    /// Remove all catalog entries that reference the given shard.
    ///
    /// For each entry: if the shard is the only replica the entry is removed
    /// entirely; otherwise the shard is just stripped from the shard list.
    /// Returns the number of entries that were modified or removed.
    pub fn remove_all_for_shard(&self, shard_id: crate::ShardId) -> usize {
        let mut map = self.inner.write();
        let mut affected = 0usize;
        let mut to_remove: Vec<String> = Vec::new();
        for (key, entry) in map.iter_mut() {
            if entry.shards.contains(&shard_id) {
                entry.shards.retain(|&s| s != shard_id);
                affected += 1;
                if entry.shards.is_empty() {
                    to_remove.push(key.clone());
                }
            }
        }
        for key in to_remove {
            map.remove(&key);
        }
        if affected > 0 {
            self.mark_dirty();
        }
        affected
    }

    /// Return all catalog entries that reference the given shard.
    ///
    /// Returns `(key, PlacementEntry)` pairs where the shard appears in
    /// the entry's shard list.
    pub fn entries_for_shard(&self, shard_id: crate::ShardId) -> Vec<(String, PlacementEntry)> {
        let map = self.inner.read();
        map.iter()
            .filter(|(_, entry)| entry.shards.contains(&shard_id))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Number of objects tracked.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return all catalog entries as `(key, PlacementEntry)` pairs.
    pub fn all_entries(&self) -> Vec<(String, PlacementEntry)> {
        let map = self.inner.read();
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Iterate over the raw entry map without cloning.
    ///
    /// The provided closure receives a shared reference to the inner
    /// `HashMap` while the read lock is held.  Use this instead of
    /// `all_entries()` in hot paths to avoid copying the entire map.
    pub fn with_entries<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<String, PlacementEntry>) -> R,
    {
        let map = self.inner.read();
        f(&map)
    }

    /// Replace the catalog contents with the data from `other`.
    ///
    /// Used by `load_catalog()` so it can work with `&self` (no need
    /// to replace the `Arc<Catalog>` pointer).
    /// Does NOT mark dirty -- caller just loaded from disk.
    pub fn load_into(&self, other: Catalog) {
        let new_data = other.inner.into_inner();
        *self.inner.write() = new_data;
        self.dirty.store(false, Ordering::Relaxed);
    }

    /// List all objects, optionally filtered by a path prefix.
    /// Returns deduplicated ObjectMeta entries.
    pub fn list(&self, prefix: Option<&str>) -> Vec<ObjectMeta> {
        let map = self.inner.read();
        map.iter()
            .filter(|(k, _)| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .map(|(k, entry)| ObjectMeta {
                location: Path::from(k.as_str()),
                last_modified: entry.updated,
                size: entry.size,
                e_tag: None,
                version: None,
            })
            .collect()
    }

    /// Serialize the catalog to JSON bytes (for persistence).
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        let map = self.inner.read();
        serde_json::to_vec(&*map)
    }

    /// Deserialize a catalog from JSON bytes.
    pub fn from_json(data: &[u8]) -> serde_json::Result<Self> {
        let map: HashMap<String, PlacementEntry> = serde_json::from_slice(data)?;
        Ok(Self {
            inner: RwLock::new(map),
            dirty: AtomicBool::new(false),
        })
    }

    /// Save the catalog to a JSON file on disk.
    /// Uses write-to-temp + atomic rename to avoid corruption on crash.
    pub fn save_to_file(&self, path: &StdPath) -> std::io::Result<()> {
        let data = self.to_json().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, &data)?;
        std::fs::rename(&tmp_path, path)
    }

    /// Load a catalog from a JSON file on disk.
    /// Returns an empty catalog if the file does not exist.
    pub fn load_from_file(path: &StdPath) -> std::io::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let data = std::fs::read(path)?;
        Self::from_json(&data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })
    }

    /// Serialize the catalog to bincode bytes (compact binary, fast for large catalogs).
    pub fn to_bincode(&self) -> std::io::Result<Vec<u8>> {
        let map = self.inner.read();
        bincode::serialize(&*map).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
        })
    }

    /// Deserialize a catalog from bincode bytes.
    pub fn from_bincode(data: &[u8]) -> std::io::Result<Self> {
        let map: HashMap<String, PlacementEntry> = bincode::deserialize(data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;
        Ok(Self {
            inner: RwLock::new(map),
            dirty: AtomicBool::new(false),
        })
    }
}

// -- CatalogPersistence ------------------------------------------------------

/// JSON wrapper that embeds a CRC32c checksum alongside the catalog data.
#[derive(Serialize, Deserialize)]
struct JsonEnvelope {
    /// CRC32c of the JSON-encoded catalog `data` field.
    checksum: u32,
    /// The catalog entries.
    data: HashMap<String, PlacementEntry>,
}

/// Bincode on-disk format: [4-byte little-endian CRC32c] [bincode payload].
/// The CRC covers only the bincode payload bytes.
const BINCODE_CRC_LEN: usize = 4;

/// Strategy for persisting the placement catalog to disk.
#[derive(Debug, Clone)]
pub enum CatalogPersistence {
    /// No persistence -- catalog lives only in memory.
    None,
    /// Human-readable JSON file (with embedded checksum).
    Json { path: PathBuf },
    /// Compact binary (bincode) -- fast load for millions of objects.
    /// File format: [4-byte LE CRC32c][bincode payload].
    Bincode { path: PathBuf },
}

impl Default for CatalogPersistence {
    fn default() -> Self {
        Self::None
    }
}

impl CatalogPersistence {
    /// Convenience constructor for JSON persistence.
    pub fn json(path: impl Into<PathBuf>) -> Self {
        Self::Json { path: path.into() }
    }

    /// Convenience constructor for bincode persistence.
    pub fn bincode(path: impl Into<PathBuf>) -> Self {
        Self::Bincode { path: path.into() }
    }

    /// The file path (if any).
    pub fn path(&self) -> Option<&StdPath> {
        match self {
            Self::None => Option::None,
            Self::Json { path } | Self::Bincode { path } => Some(path),
        }
    }

    /// Check if persistence is disabled.
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Save a catalog to disk using the configured strategy.
    /// The checksum is embedded inside the file (JSON envelope or bincode
    /// prefix) so no separate .crc file is needed.
    /// No-op for `None`.
    pub fn save(&self, catalog: &Catalog) -> std::io::Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Json { path } => {
                let map = catalog.inner.read();
                let entries = map.len();
                // Serialize via serde_json::Value for deterministic key order.
                let value = serde_json::to_value(&*map).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                let payload = serde_json::to_vec(&value).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                let checksum = crc32c::crc32c(&payload);
                drop(map);
                let envelope = serde_json::json!({
                    "checksum": checksum,
                    "data": value,
                });
                let data = serde_json::to_vec(&envelope).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })?;
                atomic_write(path, &data)?;
                debug!(format = "json", entries, bytes = data.len(), path = %path.display(), "catalog persisted");
                Ok(())
            }
            Self::Bincode { path } => {
                let entries = catalog.len();
                let payload = catalog.to_bincode()?;
                let checksum = crc32c::crc32c(&payload);
                let mut data = Vec::with_capacity(BINCODE_CRC_LEN + payload.len());
                data.extend_from_slice(&checksum.to_le_bytes());
                data.extend_from_slice(&payload);
                atomic_write(path, &data)?;
                debug!(format = "bincode", entries, bytes = data.len(), path = %path.display(), "catalog persisted");
                Ok(())
            }
        }
    }

    /// Load a catalog from disk using the configured strategy.
    /// Validates the embedded checksum; returns an error if it does not match.
    /// Returns an empty catalog if the file does not exist or persistence is `None`.
    pub fn load(&self) -> std::io::Result<Catalog> {
        match self {
            Self::None => Ok(Catalog::new()),
            Self::Json { path } => {
                if !path.exists() {
                    debug!(format = "json", path = %path.display(), "catalog file not found, starting empty");
                    return Ok(Catalog::new());
                }
                let raw = std::fs::read(path)?;
                // Try new envelope format first.
                if let Ok(envelope) = serde_json::from_slice::<JsonEnvelope>(&raw) {
                    // Re-serialize via Value for deterministic key order.
                    let value = serde_json::to_value(&envelope.data).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, e)
                    })?;
                    let payload = serde_json::to_vec(&value).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, e)
                    })?;
                    let actual = crc32c::crc32c(&payload);
                    if actual != envelope.checksum {
                        warn!(format = "json", path = %path.display(),
                              expected = format_args!("{:#010X}", envelope.checksum),
                              actual = format_args!("{:#010X}", actual),
                              "catalog checksum mismatch");
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "catalog checksum mismatch: expected {:#010X}, got {:#010X}",
                                envelope.checksum, actual
                            ),
                        ));
                    }
                    let entries = envelope.data.len();
                    debug!(format = "json", entries, bytes = raw.len(), path = %path.display(), "catalog loaded");
                    return Ok(Catalog {
                        inner: RwLock::new(envelope.data),
                        dirty: AtomicBool::new(false),
                    });
                }
                // Fall back to legacy format (plain JSON without envelope).
                let cat = Catalog::from_json(&raw).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })?;
                debug!(format = "json-legacy", entries = cat.len(), path = %path.display(), "catalog loaded");
                Ok(cat)
            }
            Self::Bincode { path } => {
                if !path.exists() {
                    debug!(format = "bincode", path = %path.display(), "catalog file not found, starting empty");
                    return Ok(Catalog::new());
                }
                let raw = std::fs::read(path)?;
                if raw.len() < BINCODE_CRC_LEN {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "catalog file too short for checksum header",
                    ));
                }
                let stored_crc = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                let payload = &raw[BINCODE_CRC_LEN..];
                let actual_crc = crc32c::crc32c(payload);
                if stored_crc != actual_crc {
                    warn!(format = "bincode", path = %path.display(),
                          expected = format_args!("{:#010X}", stored_crc),
                          actual = format_args!("{:#010X}", actual_crc),
                          "catalog checksum mismatch");
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "catalog checksum mismatch: expected {:#010X}, got {:#010X}",
                            stored_crc, actual_crc
                        ),
                    ));
                }
                let cat = Catalog::from_bincode(payload)?;
                debug!(format = "bincode", entries = cat.len(), bytes = raw.len(), path = %path.display(), "catalog loaded");
                Ok(cat)
            }
        }
    }
}

/// Write data to a temp file then atomically rename to the target path.
fn atomic_write(path: &StdPath, data: &[u8]) -> std::io::Result<()> {
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_preserves_meta_len() {
        let cat = Catalog::new();
        cat.put("obj/a".into(), vec![0, 1], 1024, Some(0xDEAD), 42);

        let entry = cat.get("obj/a").unwrap();
        assert_eq!(entry.meta_len, 42);
        assert_eq!(entry.size, 1024);
        assert_eq!(entry.crc32c, Some(0xDEAD));
    }

    #[test]
    fn meta_len_zero_when_not_specified() {
        let cat = Catalog::new();
        cat.put("obj/b".into(), vec![0], 512, None, 0);

        let entry = cat.get("obj/b").unwrap();
        assert_eq!(entry.meta_len, 0);
    }

    #[test]
    fn meta_len_survives_json_roundtrip() {
        let cat = Catalog::new();
        cat.put("obj/a".into(), vec![0, 1], 1024, Some(0xBEEF), 99);
        cat.put("obj/b".into(), vec![2], 512, None, 0);

        let json = cat.to_json().unwrap();
        let restored = Catalog::from_json(&json).unwrap();

        let a = restored.get("obj/a").unwrap();
        assert_eq!(a.meta_len, 99);
        assert_eq!(a.crc32c, Some(0xBEEF));

        let b = restored.get("obj/b").unwrap();
        assert_eq!(b.meta_len, 0);
    }

    #[test]
    fn meta_len_default_zero_for_old_json() {
        // Simulate a catalog entry from before meta_len was added
        // (no "meta_len" field in JSON). serde(default) should give 0.
        let old_json = br#"{"obj/old":{"shards":[0],"size":256,"crc32c":null,"updated":"2025-01-01T00:00:00Z"}}"#;
        let cat = Catalog::from_json(old_json).unwrap();
        let entry = cat.get("obj/old").unwrap();
        assert_eq!(entry.meta_len, 0, "missing meta_len should default to 0");
    }

    #[test]
    fn meta_len_survives_bincode_roundtrip() {
        let cat = Catalog::new();
        cat.put("obj/x".into(), vec![0], 2048, Some(0xCAFE), 200);

        let data = cat.to_bincode().unwrap();
        let restored = Catalog::from_bincode(&data).unwrap();

        let entry = restored.get("obj/x").unwrap();
        assert_eq!(entry.meta_len, 200);
        assert_eq!(entry.crc32c, Some(0xCAFE));
    }

    #[test]
    fn add_replica_defaults_meta_len_zero() {
        let cat = Catalog::new();
        cat.add_replica("obj/r", 0, 512, 0);

        let entry = cat.get("obj/r").unwrap();
        assert_eq!(entry.meta_len, 0, "add_replica should default meta_len to 0");
    }

    #[test]
    fn try_insert_defaults_meta_len_zero() {
        let cat = Catalog::new();
        cat.try_insert("obj/t".into(), vec![0, 1], 1024);

        let entry = cat.get("obj/t").unwrap();
        assert_eq!(entry.meta_len, 0, "try_insert should default meta_len to 0");
    }
}
