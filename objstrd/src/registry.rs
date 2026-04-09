//! Store registry: tracks one or more backend object stores.
//!
//! Each store entry carries the generic `Arc<dyn ObjectStore>` handle
//! that the adapter and distribution layer use for all I/O, plus an
//! optional strongly-typed `Arc<RawObjectStore>` reference when the
//! backend is a raw block device (needed for metadata-aware APIs,
//! flush, and visualization endpoints).

use std::fmt;
use std::sync::Arc;

use object_store::ObjectStore;
use rawobjstr::store::RawObjectStore;

// -- StoreKind ---------------------------------------------------------------

/// Discriminator for the backend type behind a store entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    /// `RawObjectStore` -- raw block device or loopback file.
    Raw,
    /// Apache `object_store` AWS S3 backend.
    S3,
    /// `object_store::local::LocalFileSystem`.
    LocalFs,
    /// `object_store::memory::InMemory` (testing only).
    InMemory,
    /// Any other `ObjectStore` implementation.
    Other,
}

impl fmt::Display for StoreKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreKind::Raw => write!(f, "raw"),
            StoreKind::S3 => write!(f, "s3"),
            StoreKind::LocalFs => write!(f, "fs"),
            StoreKind::InMemory => write!(f, "mem"),
            StoreKind::Other => write!(f, "other"),
        }
    }
}

// -- StoreEntry --------------------------------------------------------------

/// A single backend store known to the daemon.
pub struct StoreEntry {
    /// Human-readable name (e.g. "shard-0", "s3-archive").
    pub name: String,
    /// Generic object store handle used by the adapter for all I/O.
    pub store: Arc<dyn ObjectStore>,
    /// What kind of backend this is.
    pub kind: StoreKind,
    /// Strongly-typed reference, populated only when `kind == Raw`.
    /// Used for `put_with_meta`, `head_with_meta`, `list_with_meta`,
    /// `get_metadata`, `set_meta_len`, `flush_index`, device_info, etc.
    pub raw_ref: Option<Arc<RawObjectStore>>,
}

impl fmt::Debug for StoreEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreEntry")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("has_raw_ref", &self.raw_ref.is_some())
            .finish()
    }
}

// -- StoreRegistry -----------------------------------------------------------

/// Ordered collection of backend stores.
///
/// The index into `stores` is the *shard ID* used by the distribution
/// layer -- shard 0 is `stores[0]`, etc.
pub struct StoreRegistry {
    stores: Vec<StoreEntry>,
}

impl StoreRegistry {
    /// Build a registry from a list of store entries.
    ///
    /// Panics if `stores` is empty -- the daemon requires at least one store.
    pub fn new(stores: Vec<StoreEntry>) -> Self {
        assert!(!stores.is_empty(), "StoreRegistry requires at least one store");
        Self { stores }
    }

    /// Convenience: build a single-store registry from a RawObjectStore.
    pub fn single_raw(name: &str, store: Arc<RawObjectStore>) -> Self {
        let entry = StoreEntry {
            name: name.to_string(),
            store: store.clone() as Arc<dyn ObjectStore>,
            kind: StoreKind::Raw,
            raw_ref: Some(store),
        };
        Self::new(vec![entry])
    }

    /// Number of stores (== number of shards for distribution).
    pub fn shard_count(&self) -> usize {
        self.stores.len()
    }

    /// Access a store by shard index.
    pub fn get(&self, index: usize) -> Option<&StoreEntry> {
        self.stores.get(index)
    }

    /// Access a store by name.
    pub fn get_by_name(&self, name: &str) -> Option<&StoreEntry> {
        self.stores.iter().find(|e| e.name == name)
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = &StoreEntry> {
        self.stores.iter()
    }

    /// Return all raw store references (for flush-all, shutdown, etc.).
    pub fn all_raw(&self) -> Vec<Arc<RawObjectStore>> {
        self.stores
            .iter()
            .filter_map(|e| e.raw_ref.clone())
            .collect()
    }

    /// Return the first store's generic handle.
    ///
    /// Useful when operating in single-store mode or when the caller
    /// does not care which store is used (e.g. bucket scan).
    pub fn first_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.stores[0].store)
    }

    /// Return all store names and kinds (for diagnostics / info endpoint).
    pub fn summary(&self) -> Vec<(String, StoreKind)> {
        self.stores
            .iter()
            .map(|e| (e.name.clone(), e.kind))
            .collect()
    }
}

impl fmt::Debug for StoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreRegistry")
            .field("count", &self.stores.len())
            .field("stores", &self.stores)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn make_mem_entry(name: &str) -> StoreEntry {
        StoreEntry {
            name: name.to_string(),
            store: Arc::new(InMemory::new()),
            kind: StoreKind::InMemory,
            raw_ref: None,
        }
    }

    #[test]
    fn single_store_registry() {
        let reg = StoreRegistry::new(vec![make_mem_entry("mem0")]);
        assert_eq!(reg.shard_count(), 1);
        assert!(reg.get(0).is_some());
        assert!(reg.get(1).is_none());
        assert_eq!(reg.get_by_name("mem0").unwrap().kind, StoreKind::InMemory);
    }

    #[test]
    fn multi_store_registry() {
        let reg = StoreRegistry::new(vec![
            make_mem_entry("shard-0"),
            make_mem_entry("shard-1"),
            make_mem_entry("shard-2"),
        ]);
        assert_eq!(reg.shard_count(), 3);
        assert_eq!(reg.get(2).unwrap().name, "shard-2");
        assert!(reg.get_by_name("shard-1").is_some());
        assert!(reg.get_by_name("missing").is_none());
        assert_eq!(reg.all_raw().len(), 0);
    }

    #[test]
    fn summary() {
        let reg = StoreRegistry::new(vec![
            make_mem_entry("a"),
            make_mem_entry("b"),
        ]);
        let s = reg.summary();
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].0, "a");
        assert_eq!(s[1].1, StoreKind::InMemory);
    }

    #[test]
    #[should_panic(expected = "at least one store")]
    fn empty_registry_panics() {
        StoreRegistry::new(vec![]);
    }
}
