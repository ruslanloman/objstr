//! # shardedobjstr
//!
//! Distributed [`ObjectStore`](object_store::ObjectStore) that shards objects
//! across multiple raw block devices (or any `ObjectStore` backend) with
//! configurable replication. Includes a persistent catalog, health probing,
//! and repair-replication utilities.

pub mod catalog;
pub mod config;
pub mod event;
pub mod metadata;
pub mod repair;
pub mod replication;
pub mod tlv;

/// Git commit hash baked in at build time (e.g. `a1b2c3d` or `a1b2c3d-dirty`).
pub const BUILD_GIT_HASH: &str = env!("BUILD_GIT_HASH");
/// UTC timestamp when the crate was compiled.
pub const BUILD_DATE: &str = env!("BUILD_DATE");
/// Crate version from Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Reserved key prefix for delete markers.  Objects under this prefix are
/// invisible to normal LIST/GET/HEAD operations and must not be written by
/// clients.  The sharded store creates them during DELETE and removes them
/// during vacuum.
pub const DELETE_MARKER_PREFIX: &str = "__deleted__/";

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::RwLock;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use catalog::{Catalog, CatalogPersistence, PlacementEntry};
use replication::ReplicationPolicy;

// -- Offline placeholder store ---------------------------------------

/// A dummy `ObjectStore` that returns errors for every operation.
/// Used as a slot-holder for shards that are not yet attached.
#[derive(Debug)]
struct OfflinePlaceholderStore {
    shard_id: usize,
}

impl std::fmt::Display for OfflinePlaceholderStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OfflinePlaceholderStore(shard={})", self.shard_id)
    }
}

impl OfflinePlaceholderStore {
    fn err(&self) -> object_store::Error {
        object_store::Error::Generic {
            store: "OfflinePlaceholderStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("shard {} is offline", self.shard_id),
            )),
        }
    }
}

#[async_trait]
impl ObjectStore for OfflinePlaceholderStore {
    async fn put(&self, _: &Path, _: PutPayload) -> object_store::Result<PutResult> {
        Err(self.err())
    }
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> object_store::Result<PutResult> {
        Err(self.err())
    }
    async fn put_multipart(&self, _: &Path) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(self.err())
    }
    async fn put_multipart_opts(&self, _: &Path, _: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(self.err())
    }
    async fn get(&self, _: &Path) -> object_store::Result<GetResult> {
        Err(self.err())
    }
    async fn get_opts(&self, _: &Path, _: GetOptions) -> object_store::Result<GetResult> {
        Err(self.err())
    }
    async fn get_range(&self, _: &Path, _: std::ops::Range<u64>) -> object_store::Result<Bytes> {
        Err(self.err())
    }
    async fn head(&self, _: &Path) -> object_store::Result<ObjectMeta> {
        Err(self.err())
    }
    async fn delete(&self, _: &Path) -> object_store::Result<()> {
        Err(self.err())
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        Box::pin(futures::stream::once(async { Err(object_store::Error::Generic {
            store: "OfflinePlaceholderStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "shard is offline",
            )),
        }) }))
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        Err(self.err())
    }
    async fn copy(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        Err(self.err())
    }
    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        Err(self.err())
    }
}

// -- Error types -----------------------------------------------------

#[derive(Error, Debug)]
pub enum ShardError {
    #[error("no shards available")]
    NoShards,

    #[error("object not found: {0}")]
    NotFound(String),

    #[error("all replicas failed for {path}: {errors:?}")]
    AllReplicasFailed {
        path: String,
        errors: Vec<String>,
    },

    #[error("insufficient writes for {path}: required {required}, got {actual}: {errors:?}")]
    InsufficientWrites {
        path: String,
        required: usize,
        actual: usize,
        errors: Vec<String>,
    },

    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("store is read-only")]
    ReadOnly,

    #[error("vacuum requires all shards healthy, but shard {shard_id} is offline")]
    VacuumOfflineShard { shard_id: usize },

    #[error("vacuum is already running")]
    VacuumAlreadyRunning,
}

pub type Result<T> = std::result::Result<T, ShardError>;

// -- Core types ------------------------------------------------------

/// Unique identifier for a shard (index into the shards vec).
pub type ShardId = usize;

/// Health status of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardHealth {
    Healthy,
    Degraded,
    Offline,
    /// Shard is being re-synced after coming back online.
    Syncing,
    /// Shard was manually taken offline (admin action).
    /// The recovery loop will NOT auto-reattach it.
    /// Use `attach_shard()` or `POST /_admin/attach/{id}` to bring it back.
    Detached,
}

impl ShardHealth {
    /// Returns true if the shard can accept writes (Healthy or Degraded).
    pub fn is_writable(&self) -> bool {
        matches!(self, ShardHealth::Healthy | ShardHealth::Degraded)
    }

    /// Returns true if the shard is completely unavailable for I/O
    /// (Offline, Detached, or Syncing-but-not-yet-readable).
    /// Use this instead of `== ShardHealth::Offline` when the intent is
    /// "skip this shard because it cannot serve requests".
    pub fn is_unavailable(&self) -> bool {
        matches!(self, ShardHealth::Offline | ShardHealth::Detached)
    }
}

/// Reason a shard was taken offline or detached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachReason {
    /// Admin manually took the shard offline via the API.
    Manual,
    /// Consecutive health probe failures exceeded threshold.
    ProbeFailure,
    /// Backing device/file disappeared from disk.
    DeviceMissing,
    /// Shard was drained (data evacuated to survivors).
    Drain,
}

/// Read preference policy controlling the order in which replicas are
/// tried when reading an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadPreference {
    /// Round-robin rotation across replicas (default).
    RoundRobin,
    /// Try shards in config order (0, 1, 2, ...).  First healthy shard
    /// wins.  Ideal for heterogeneous mirrors (e.g. NVMe first, then S3).
    Ordered,
}

/// Per-shard metadata tracked by the cluster.
pub struct ShardInfo {
    pub id: ShardId,
    pub store: Arc<dyn ObjectStore>,
    pub health: ShardHealth,
    /// Free space in bytes (updated periodically).
    pub free_space: u64,
    /// When this shard was marked offline (None if healthy/degraded).
    pub offline_since: Option<DateTime<Utc>>,
    /// Cumulative CRC error count for this shard (resets on daemon restart,
    /// but survives SIGHUP / config reload).
    pub crc_error_count: AtomicU64,
    /// Why this shard was detached (None if not detached).
    pub detach_reason: Option<DetachReason>,
    /// When true, re-replication sweeps skip this shard's grace-period
    /// check -- objects are NOT proactively re-replicated to survivors.
    pub suppress_replication: bool,
}

/// Report returned by `invalidate_shard()`.
#[derive(Debug, Clone)]
pub struct InvalidateReport {
    /// Shard that was invalidated.
    pub shard_id: ShardId,
    /// Number of catalog entries purged before re-scan.
    pub entries_purged: usize,
    /// Number of objects found on the shard after re-scan.
    pub entries_restored: usize,
    /// Keys that were in the catalog before but are missing from the shard
    /// after re-scan (potential data loss -- other replicas may still hold them).
    pub missing_keys: Vec<String>,
    /// Whether the re-scan succeeded. If false, the shard remains Degraded.
    pub scan_ok: bool,
}

/// Result of verifying a single object's replicas.
#[derive(Debug, Clone)]
pub struct VerifyObjectReport {
    /// Object key that was verified.
    pub key: String,
    /// Expected CRC32c from the catalog (None if not recorded).
    pub catalog_crc: Option<u32>,
    /// Per-shard results: (shard_id, computed_crc, size, matches_catalog).
    /// `matches_catalog` is None when catalog has no CRC to compare against.
    pub replicas: Vec<VerifyReplicaResult>,
    /// True if all readable replicas agree on CRC (even if catalog CRC is unknown).
    pub replicas_consistent: bool,
}

/// CRC verification result for one replica of an object.
#[derive(Debug, Clone)]
pub struct VerifyReplicaResult {
    pub shard_id: ShardId,
    /// Computed CRC32c from reading the replica, or None if the read failed.
    pub crc32c: Option<u32>,
    /// Size in bytes, or None if the read failed.
    pub size: Option<u64>,
    /// True if CRC matches catalog; None if catalog CRC unknown or read failed.
    pub matches_catalog: Option<bool>,
    /// Error message if the read failed.
    pub error: Option<String>,
}

/// Aggregate report from verifying all objects (or a subset).
#[derive(Debug, Clone, Default)]
pub struct VerifyReport {
    /// Total objects checked.
    pub objects_checked: usize,
    /// Objects where all replicas are consistent and match catalog CRC.
    pub objects_ok: usize,
    /// Objects with at least one CRC mismatch (catalog vs replica or inter-replica).
    pub objects_mismatched: usize,
    /// Objects with at least one replica read failure.
    pub objects_with_errors: usize,
    /// Detailed per-object reports for mismatched or errored objects only.
    pub details: Vec<VerifyObjectReport>,
}

// -- Cross-shard MD5 verification types ------------------------------

/// MD5 digest and metadata for a single object on a single shard.
#[derive(Debug, Clone)]
pub struct ShardObjectDigest {
    pub shard_id: ShardId,
    /// Hex-encoded MD5 digest of the object body.
    pub md5_hex: String,
    /// Object size in bytes.
    pub size: u64,
    /// Last-modified timestamp reported by the shard.
    pub last_modified: DateTime<Utc>,
}

/// Report from cross-verifying a single object across all its replicas.
#[derive(Debug, Clone)]
pub struct CrossVerifyReport {
    /// Object key that was verified.
    pub key: String,
    /// Expected CRC32c from the catalog (for informational display).
    pub catalog_crc: Option<u32>,
    /// Per-shard digest results (one per readable replica).
    pub shards: Vec<ShardObjectDigest>,
    /// True if all readable replicas produced the same MD5.
    pub consistent: bool,
    /// Per-shard errors for replicas that could not be read.
    pub errors: Vec<(ShardId, String)>,
}

/// Aggregate report from cross-verifying all objects (or a subset).
#[derive(Debug, Clone, Default)]
pub struct CrossVerifyAllReport {
    /// Total objects checked (only objects with RF > 1).
    pub objects_checked: u64,
    /// Objects where all replicas agree on MD5.
    pub objects_ok: u64,
    /// Objects with at least one MD5 mismatch between replicas.
    pub objects_mismatched: u64,
    /// Objects where at least one replica could not be read.
    pub objects_with_errors: u64,
    /// Objects skipped because they have only 1 replica.
    pub objects_skipped_single_replica: u64,
    /// Detailed per-object reports for mismatched or errored objects only.
    pub details: Vec<CrossVerifyReport>,
}

/// Tracked in-flight multipart upload for expiry purposes.
struct TrackedUpload {
    /// Object path being uploaded.
    location: Path,
    /// When the upload was created.
    created_at: std::time::Instant,
    /// Primary shard that holds the parts.
    primary_shard_id: ShardId,
}

/// Default expiry for abandoned multipart uploads: 24 hours.
const DEFAULT_MULTIPART_EXPIRY: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

// -- Sharded store --------------------------------------------------

/// A sharded object store that distributes objects across multiple
/// `RawObjectStore` shards with configurable replication.
///
/// This is the main entry point. It implements `ObjectStore` so it can
/// be used as a drop-in replacement anywhere the trait is expected.
pub struct ShardedObjectStore {
    shards: RwLock<Vec<ShardInfo>>,
    catalog: Arc<Catalog>,
    replication: ReplicationPolicy,
    persistence: RwLock<CatalogPersistence>,
    /// Round-robin counter for read load-balancing across replicas.
    read_counter: AtomicU64,
    /// Read preference policy for replica ordering.
    read_preference: RwLock<ReadPreference>,
    /// Optional event bus for broadcasting PUT/DELETE/FLUSH events.
    event_bus: parking_lot::Mutex<Option<Arc<rawobjstr::event::EventBus>>>,
    /// When true, all write operations (put/delete/copy) are rejected.
    read_only: bool,
    /// Registry of in-flight multipart uploads for expiry tracking.
    multipart_uploads: Arc<parking_lot::Mutex<HashMap<u64, TrackedUpload>>>,
    /// Monotonic ID counter for multipart upload tracking.
    next_upload_id: AtomicU64,
    /// Configurable expiry duration for abandoned multipart uploads.
    multipart_expiry: std::time::Duration,
    /// Number of background read-repair tasks spawned (fire-and-forget).
    read_repair_count: Arc<AtomicU64>,
    /// Number of background read-repair tasks that completed successfully.
    read_repair_success: Arc<AtomicU64>,
    /// Number of background read-repair tasks that failed.
    read_repair_failed: Arc<AtomicU64>,
    /// Bounds concurrent read-repair I/O (prevents unbounded task explosion).
    repair_semaphore: Arc<tokio::sync::Semaphore>,
    /// Minimum number of successful shard writes before a put returns Ok.
    /// Default: max(replication_factor - 1, 1).
    min_writes: usize,
    /// When true, deletes also require `min_writes` successful shard
    /// deletions before returning Ok. Default: false (best-effort).
    delete_requires_min_writes: bool,
    /// Optional raw-reference registry for metadata-aware operations.
    /// When present, read-repair, copy, replication, and multipart
    /// complete preserve metadata across shard types.
    raw_refs: parking_lot::Mutex<Option<Arc<crate::metadata::RawRefRegistry>>>,
    /// Tracks whether a vacuum operation is already running.
    vacuum_in_progress: std::sync::atomic::AtomicBool,
}

impl ShardedObjectStore {
    /// Create a new cluster from a set of shard stores.
    ///
    /// Each store should be a `RawObjectStore` (or any `ObjectStore` impl).
    /// `replication_factor` controls how many copies of each object are kept.
    pub fn new(
        stores: Vec<Arc<dyn ObjectStore>>,
        replication_factor: usize,
    ) -> Self {
        let shards: Vec<ShardInfo> = stores
            .into_iter()
            .enumerate()
            .map(|(id, store)| ShardInfo {
                id,
                store,
                health: ShardHealth::Healthy,
                free_space: u64::MAX, // unknown until first health check
                offline_since: None,
                crc_error_count: AtomicU64::new(0),
                detach_reason: None,
                suppress_replication: false,
            })
            .collect();

        let shard_count = shards.len();

        Self {
            shards: RwLock::new(shards),
            catalog: Arc::new(Catalog::new()),
            replication: ReplicationPolicy::new(replication_factor, shard_count),
            persistence: RwLock::new(CatalogPersistence::None),
            read_counter: AtomicU64::new(0),
            read_preference: RwLock::new(ReadPreference::RoundRobin),
            event_bus: parking_lot::Mutex::new(None),
            read_only: false,
            multipart_uploads: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            next_upload_id: AtomicU64::new(1),
            multipart_expiry: DEFAULT_MULTIPART_EXPIRY,
            read_repair_count: Arc::new(AtomicU64::new(0)),
            read_repair_success: Arc::new(AtomicU64::new(0)),
            read_repair_failed: Arc::new(AtomicU64::new(0)),
            repair_semaphore: Arc::new(tokio::sync::Semaphore::new(32)),
            // Use clamped RF so min_writes never exceeds shard_count.
            min_writes: replication_factor.min(shard_count).saturating_sub(1).max(1),
            delete_requires_min_writes: false,
            raw_refs: parking_lot::Mutex::new(None),
            vacuum_in_progress: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create a cluster where some shards may be unavailable at startup.
    ///
    /// Each entry in `stores` is either `Some(store)` (available) or
    /// `None` (offline). Offline slots get a placeholder and are marked
    /// `ShardHealth::Offline`. The cluster starts in degraded mode and
    /// can serve reads/writes on the available shards. Offline shards
    /// can be attached later with `attach_shard()`.
    ///
    /// The shard count (and thus hash ring) is determined by the length
    /// of `stores`, so slot positions are stable.
    pub fn new_with_offline(
        stores: Vec<Option<Arc<dyn ObjectStore>>>,
        replication_factor: usize,
    ) -> Self {
        let shards: Vec<ShardInfo> = stores
            .into_iter()
            .enumerate()
            .map(|(id, maybe_store)| match maybe_store {
                Some(store) => ShardInfo {
                    id,
                    store,
                    health: ShardHealth::Healthy,
                    free_space: u64::MAX,
                    offline_since: None,
                    crc_error_count: AtomicU64::new(0),
                    detach_reason: None,
                    suppress_replication: false,
                },
                None => ShardInfo {
                    id,
                    store: Arc::new(OfflinePlaceholderStore { shard_id: id }),
                    health: ShardHealth::Offline,
                    free_space: 0,
                    offline_since: Some(Utc::now()),
                    crc_error_count: AtomicU64::new(0),
                    detach_reason: None,
                    suppress_replication: false,
                },
            })
            .collect();

        let shard_count = shards.len();

        Self {
            shards: RwLock::new(shards),
            catalog: Arc::new(Catalog::new()),
            replication: ReplicationPolicy::new(replication_factor, shard_count),
            persistence: RwLock::new(CatalogPersistence::None),
            read_counter: AtomicU64::new(0),
            read_preference: RwLock::new(ReadPreference::RoundRobin),
            event_bus: parking_lot::Mutex::new(None),
            read_only: false,
            multipart_uploads: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            next_upload_id: AtomicU64::new(1),
            multipart_expiry: DEFAULT_MULTIPART_EXPIRY,
            read_repair_count: Arc::new(AtomicU64::new(0)),
            read_repair_success: Arc::new(AtomicU64::new(0)),
            read_repair_failed: Arc::new(AtomicU64::new(0)),
            repair_semaphore: Arc::new(tokio::sync::Semaphore::new(32)),
            // Use clamped RF so min_writes never exceeds shard_count.
            min_writes: replication_factor.min(shard_count).saturating_sub(1).max(1),
            delete_requires_min_writes: false,
            raw_refs: parking_lot::Mutex::new(None),
            vacuum_in_progress: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Number of shards in the cluster.
    pub fn shard_count(&self) -> usize {
        self.shards.read().len()
    }

    /// Attach an event bus for broadcasting PUT/DELETE/FLUSH events.
    pub fn set_event_bus(&self, bus: Arc<rawobjstr::event::EventBus>) {
        *self.event_bus.lock() = Some(bus);
    }

    /// Attach a raw-reference registry for metadata-aware operations.
    ///
    /// When set, read-repair, copy, replication, multipart complete,
    /// and rebuild-catalog will preserve metadata across shard types.
    pub fn set_raw_refs(&self, refs: Arc<crate::metadata::RawRefRegistry>) {
        *self.raw_refs.lock() = Some(refs);
    }

    /// Get a clone of the raw-reference registry, if one is attached.
    pub fn raw_refs(&self) -> Option<Arc<crate::metadata::RawRefRegistry>> {
        self.raw_refs.lock().clone()
    }

    /// Emit an event if an event bus is attached.
    pub(crate) fn emit_event(&self, event: rawobjstr::event::StoreEvent) {
        let bus = self.event_bus.lock();
        if let Some(ref b) = *bus {
            b.emit(event);
        }
    }

    /// Get a clone of a shard's underlying store.
    pub fn shard_store(&self, id: ShardId) -> Option<Arc<dyn ObjectStore>> {
        self.shards.read().get(id).map(|s| s.store.clone())
    }

    /// Return the placement entry for an object, if it exists.
    pub fn placement(&self, path: &str) -> Option<PlacementEntry> {
        self.catalog.get(path)
    }

    /// Configured replication factor.
    pub fn replication_factor(&self) -> usize {
        self.replication.factor()
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Get the health status of a shard.
    pub fn shard_health(&self, id: ShardId) -> Option<ShardHealth> {
        self.shards.read().get(id).map(|s| s.health)
    }

    /// Get the offline_since timestamp for a shard (None if not offline).
    pub fn shard_offline_since(&self, id: ShardId) -> Option<DateTime<Utc>> {
        self.shards.read().get(id).and_then(|s| s.offline_since)
    }

    /// Get free space for a shard (in bytes).
    pub fn shard_free_space(&self, id: ShardId) -> Option<u64> {
        self.shards.read().get(id).map(|s| s.free_space)
    }

    /// Get cumulative CRC error count for a shard.
    pub fn shard_crc_error_count(&self, id: ShardId) -> u64 {
        self.shards
            .read()
            .get(id)
            .map(|s| s.crc_error_count.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Update the cached free space for a shard.
    pub fn set_shard_free_space(&self, id: ShardId, free: u64) {
        let mut shards = self.shards.write();
        if let Some(s) = shards.get_mut(id) {
            s.free_space = free;
        }
    }

    /// Get the current read preference policy.
    pub fn read_preference(&self) -> ReadPreference {
        self.read_preference.read().clone()
    }

    /// Set the read preference policy.
    pub fn set_read_preference(&self, pref: ReadPreference) {
        *self.read_preference.write() = pref;
    }

    /// Set the health status of a shard.
    ///
    /// Returns the previous health status, or `None` if the shard ID is
    /// out of range.  The daemon uses this to mark shards Degraded or
    /// Offline based on periodic health checks, CRC failure counters,
    /// or manual admin commands.
    pub fn set_shard_health(&self, id: ShardId, health: ShardHealth) -> Option<ShardHealth> {
        let mut shards = self.shards.write();
        if let Some(shard) = shards.get_mut(id) {
            let prev = shard.health;
            if prev != health {
                info!(shard_id = id, from = ?prev, to = ?health, "shard health changed");
            }
            shard.health = health;
            // Track offline/degraded/detached timestamp.  These states
            // need a timestamp so the re-replication sweep can apply the
            // grace period.
            if health.is_unavailable() || health == ShardHealth::Degraded {
                if !prev.is_unavailable() && prev != ShardHealth::Degraded {
                    shard.offline_since = Some(Utc::now());
                }
            } else if health == ShardHealth::Healthy {
                shard.offline_since = None;
                shard.detach_reason = None;
                shard.suppress_replication = false;
            }
            Some(prev)
        } else {
            None
        }
    }

    /// Set the catalog persistence strategy.
    pub fn set_persistence(&self, p: CatalogPersistence) {
        *self.persistence.write() = p;
    }

    /// Get a clone of the current persistence strategy.
    pub fn persistence(&self) -> CatalogPersistence {
        self.persistence.read().clone()
    }

    /// Builder: set persistence strategy and return self.
    pub fn with_persistence(self, p: CatalogPersistence) -> Self {
        *self.persistence.write() = p;
        self
    }

    /// Returns true if the cluster is in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Builder: set read-only mode and return self.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Builder: set multipart upload expiry duration and return self.
    pub fn with_multipart_expiry(mut self, expiry: std::time::Duration) -> Self {
        self.multipart_expiry = expiry;
        self
    }

    /// Get current multipart expiry duration.
    pub fn multipart_expiry(&self) -> std::time::Duration {
        self.multipart_expiry
    }

    /// Builder: set minimum successful writes before a put returns Ok.
    ///
    /// Clamped to `[1, replication_factor]`. Default is
    /// `max(replication_factor - 1, 1)`.
    pub fn with_min_writes(mut self, min_writes: usize) -> Self {
        self.min_writes = min_writes.max(1).min(self.replication.factor());
        self
    }

    /// Minimum number of successful shard writes required for a put to
    /// return Ok. Writes that land on fewer shards than this threshold
    /// return `ShardError::InsufficientWrites`.
    pub fn min_writes(&self) -> usize {
        self.min_writes
    }

    /// Builder: require `min_writes` successful deletions before a
    /// delete returns Ok. Default is `false` (best-effort).
    pub fn with_delete_requires_min_writes(mut self, enabled: bool) -> Self {
        self.delete_requires_min_writes = enabled;
        self
    }

    /// Whether deletes require `min_writes` quorum.
    pub fn delete_requires_min_writes(&self) -> bool {
        self.delete_requires_min_writes
    }

    /// Register a multipart upload for expiry tracking.
    /// Returns a tracking ID.
    fn register_multipart(&self, location: &Path, primary_shard_id: ShardId) -> u64 {
        let id = self.next_upload_id.fetch_add(1, Ordering::Relaxed);
        self.multipart_uploads.lock().insert(id, TrackedUpload {
            location: location.clone(),
            created_at: std::time::Instant::now(),
            primary_shard_id,
        });
        id
    }

    /// Return the number of in-flight multipart uploads.
    pub fn multipart_upload_count(&self) -> usize {
        self.multipart_uploads.lock().len()
    }

    /// List in-flight multipart uploads.
    ///
    /// Returns `(tracking_id, location, age_secs, primary_shard_id)` tuples.
    pub fn list_multipart_uploads(&self) -> Vec<(u64, String, u64, ShardId)> {
        let uploads = self.multipart_uploads.lock();
        let now = std::time::Instant::now();
        uploads.iter().map(|(&id, u)| {
            let age = now.duration_since(u.created_at).as_secs();
            (id, u.location.to_string(), age, u.primary_shard_id)
        }).collect()
    }

    /// Purge multipart uploads older than the configured expiry.
    ///
    /// For each stale upload, aborts via a DELETE on the primary shard's
    /// temp path (best-effort). Returns the number of uploads purged.
    pub async fn purge_stale_multiparts(&self) -> usize {
        let expiry = self.multipart_expiry;
        let now = std::time::Instant::now();

        // Collect stale upload IDs and their details under lock.
        let stale: Vec<(u64, Path, ShardId)> = {
            let uploads = self.multipart_uploads.lock();
            uploads.iter()
                .filter(|(_, u)| now.duration_since(u.created_at) > expiry)
                .map(|(&id, u)| (id, u.location.clone(), u.primary_shard_id))
                .collect()
        };

        if stale.is_empty() {
            return 0;
        }

        let mut purged = 0usize;
        for (id, location, primary_shard_id) in &stale {
            // Best-effort cleanup: delete any partial data on the primary shard.
            let store = {
                let shards = self.shards.read();
                shards.get(*primary_shard_id).map(|s| s.store.clone())
            };
            if let Some(store) = store {
                let _ = store.delete(location).await;
            }
            // Remove from registry regardless of cleanup outcome.
            self.multipart_uploads.lock().remove(id);
            purged += 1;
        }

        if purged > 0 {
            tracing::info!(purged, "purged stale multipart uploads");
        }

        purged
    }

    /// Return an ObjectStore error for read-only mode.
    fn read_only_err() -> object_store::Error {
        object_store::Error::Generic {
            store: "ShardedObjectStore",
            source: Box::new(ShardError::ReadOnly),
        }
    }

    /// Save the catalog using the configured persistence strategy.
    pub fn save_catalog(&self) -> std::io::Result<()> {
        let entries = self.catalog.len();
        self.persistence.read().save(&self.catalog)?;
        self.catalog.clear_dirty();
        debug!(entries, "catalog saved to disk");
        Ok(())
    }

    /// Save the catalog only if it has been modified since the last save.
    /// Returns `Ok(true)` if saved, `Ok(false)` if nothing to save.
    pub fn save_catalog_if_dirty(&self) -> std::io::Result<bool> {
        if !self.catalog.is_dirty() {
            return Ok(false);
        }
        self.save_catalog()?;
        Ok(true)
    }

    /// Returns true if the catalog has unsaved changes.
    pub fn catalog_is_dirty(&self) -> bool {
        self.catalog.is_dirty()
    }

    /// Load the catalog using the configured persistence strategy.
    /// Replaces the current in-memory catalog.
    pub fn load_catalog(&self) -> std::io::Result<()> {
        let loaded = self.persistence.read().load()?;
        let count = loaded.len();
        self.catalog.load_into(loaded);
        debug!(entries = count, "catalog loaded from disk");
        Ok(())
    }

    /// Return the shard IDs that should hold a new object at `path`.
    ///
    /// Health-aware: skips Offline and Syncing shards, backfilling from
    /// the next positions on the ring to maintain the replication factor.
    pub fn target_shards(&self, path: &Path) -> Vec<ShardId> {
        self.select_targets(path)
    }

    /// Return the shard order for reading an existing object.
    ///
    /// Checks the catalog first; falls back to all non-offline shards.
    /// The order is load-balanced across replicas.
    pub fn read_shard_order(&self, path: &Path) -> Vec<ShardId> {
        let (order, _fallback) = self.resolve_shards(path.as_ref());
        order
    }

    /// Select which shards should hold a new object (internal).
    ///
    /// Uses jump consistent hash to determine a "home" shard, then picks
    /// rf consecutive shards clockwise from home, skipping offline and
    /// syncing shards.  Jump hash guarantees only ~1/N keys change home
    /// when N changes, keeping placement stable across shard additions.
    fn select_targets(&self, path: &Path) -> Vec<ShardId> {
        let rf = self.replication.factor();
        let shards = self.shards.read();
        if shards.is_empty() {
            return vec![];
        }

        let n = shards.len();

        // Collect writable status for each shard.
        let writable: Vec<bool> = (0..n)
            .map(|id| {
                !shards[id].health.is_unavailable()
                    && shards[id].health != ShardHealth::Syncing
            })
            .collect();

        let writable_count = writable.iter().filter(|&&w| w).count();
        if writable_count <= rf {
            // Not enough candidates to be picky -- use all writable shards.
            return (0..n).filter(|&id| writable[id]).collect();
        }

        // Jump consistent hash determines the "home" shard for this key.
        // Pick rf shards clockwise from home, skipping offline/syncing.
        let hash = crc32c::crc32c(path.as_ref().as_bytes()) as u64;
        let home = replication::jump_consistent_hash(hash, n as u32) as usize;

        let mut targets = Vec::with_capacity(rf);
        for offset in 0..n {
            let shard_id = (home + offset) % n;
            if writable[shard_id] {
                targets.push(shard_id);
                if targets.len() == rf {
                    break;
                }
            }
        }

        targets
    }

    /// Resolve the shard list for an existing object and whether we are in
    /// fallback mode (catalog miss). Returns a preference-ordered shard list.
    fn resolve_shards(&self, key: &str) -> (Vec<ShardId>, bool) {
        let entry = self.catalog.get(key);
        let is_fallback = entry.is_none();
        let shards = self.shards.read();
        let raw: Vec<ShardId> = match entry {
            Some(e) => e.shards.into_iter()
                .filter(|&id| id < shards.len())
                .filter(|&id| !shards[id].health.is_unavailable())
                .collect(),
            None => {
                // No catalog entry -- try all non-offline shards, but order
                // them by jump hash distance so the most likely home shard
                // is tried first (avoids scanning every shard on miss).
                let hash = crc32c::crc32c(key.as_bytes()) as u64;
                let n = shards.len();
                let home = replication::jump_consistent_hash(hash, n as u32) as u64;
                let mut ids: Vec<ShardId> = (0..n)
                    .filter(|&id| !shards[id].health.is_unavailable())
                    .collect();
                ids.sort_by_key(|&id| (id as u64 + n as u64 - home) % n as u64);
                ids
            }
        };
        drop(shards);
        let pref = self.read_preference.read().clone();
        let ordered = match pref {
            ReadPreference::Ordered => raw, // config order (0, 1, 2, ...)
            ReadPreference::RoundRobin => self.balanced_shard_order(&raw),
        };
        (ordered, is_fallback)
    }

    fn not_found_err(location: &Path) -> object_store::Error {
        object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(ShardError::NotFound(location.to_string())),
        }
    }

    /// Check if a read error is a data corruption (CRC mismatch).
    /// If so, increment the per-shard CRC error counter and remove
    /// the corrupt replica from the catalog so repair-replication will
    /// recreate it from a healthy copy.
    ///
    /// Returns `true` if the error was a CRC corruption (caller should
    /// schedule a read repair from a healthy replica).
    fn handle_read_error(&self, shard_id: ShardId, key: &str, err: &object_store::Error) -> bool {
        // DataCorruption errors from RawObjectStore arrive wrapped in
        // object_store::Error::Generic with the display containing
        // "data corruption".
        let msg = err.to_string();
        if !msg.contains("data corruption") {
            return false;
        }
        let count = {
            let shards = self.shards.read();
            if let Some(info) = shards.get(shard_id) {
                info.crc_error_count.fetch_add(1, Ordering::Relaxed) + 1
            } else {
                return true;
            }
        };
        error!(
            shard_id,
            key,
            crc_errors = count,
            "CRC mismatch on shard -- removing corrupt replica from catalog"
        );
        // Remove this replica from the catalog so the background
        // repair-replication sweep will re-replicate from a healthy copy.
        self.catalog.remove_shard(key, shard_id);
        true
    }

    /// Read metadata for a source object using the attached raw_refs.
    ///
    /// Checks catalog `meta_len` first to skip I/O when the object has
    /// no metadata.  For S3Like sources, metadata is extracted from the
    /// already-captured `attributes` (avoiding a redundant GET).
    ///
    /// Returns empty Vec when no metadata is available or no raw_refs
    /// are attached.
    async fn read_object_metadata(
        &self,
        location: &Path,
        attributes: &object_store::Attributes,
    ) -> Vec<u8> {
        let refs = match self.raw_refs() {
            Some(r) => r,
            None => return Vec::new(),
        };

        // Check catalog meta_len to skip metadata reads for data-only objects.
        if let Some(entry) = self.catalog.get(location.as_ref()) {
            if entry.meta_len == 0 {
                // For S3Like shards meta_len is always 0 in the catalog,
                // but metadata may still exist in attributes.
                let has_s3like = entry.shards.iter().any(|&sid| {
                    refs.kind(sid) == crate::metadata::ShardKind::S3Like
                });
                if !has_s3like {
                    return Vec::new();
                }
            }
        }

        match crate::metadata::get_metadata(self, &refs, location).await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => {
                // Fallback: check attributes directly (S3Like source).
                let meta_map = crate::metadata::attributes_to_meta(attributes);
                if meta_map.is_empty() {
                    Vec::new()
                } else {
                    crate::tlv::encode_metadata(&meta_map).unwrap_or_default()
                }
            }
        }
    }

    /// Schedule a background read repair: copy good data from
    /// `good_shard` to `corrupt_shard`, overwriting the corrupt copy
    /// and restoring the catalog entry.
    ///
    /// This is fire-and-forget: the spawned task logs success or failure
    /// via `tracing` but does not retry.  If the repair fails the
    /// corrupt replica stays removed from the catalog (it was already
    /// purged by `handle_read_error`) and a subsequent repair-replication
    /// sweep will re-replicate from a healthy copy.
    ///
    /// Use `read_repair_count()` to observe how many repairs were
    /// attempted.
    fn schedule_read_repair(
        &self,
        key: String,
        good_shard: ShardId,
        corrupt_shard: ShardId,
    ) {
        self.read_repair_count.fetch_add(1, Ordering::Relaxed);
        let good_store = {
            let shards = self.shards.read();
            match shards.get(good_shard) {
                Some(s) if !s.health.is_unavailable() => s.store.clone(),
                _ => return,
            }
        };
        let bad_store = {
            let shards = self.shards.read();
            match shards.get(corrupt_shard) {
                Some(s) if !s.health.is_unavailable() => s.store.clone(),
                _ => return,
            }
        };
        let catalog = Arc::clone(&self.catalog);
        let sem = Arc::clone(&self.repair_semaphore);
        let success_ctr = Arc::clone(&self.read_repair_success);
        let failed_ctr = Arc::clone(&self.read_repair_failed);
        let raw_refs = self.raw_refs();
        tokio::spawn(async move {
            // Acquire a semaphore permit to bound concurrent repair I/O.
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return, // semaphore closed
            };
            let path = Path::from(key.as_str());
            let result = good_store.get(&path).await;
            match result {
                Ok(get_result) => {
                    // Capture S3 attributes before consuming the result.
                    let attributes = get_result.attributes.clone();
                    match get_result.bytes().await {
                        Ok(data) => {
                            let size = data.len() as u64;

                            // Read metadata from the good shard.
                            let meta_bytes: Vec<u8> = if let Some(ref refs) = raw_refs {
                                crate::metadata::read_metadata_from_shard(
                                    good_store.as_ref(), good_shard, &path,
                                    &attributes, refs,
                                ).await.unwrap_or_default()
                            } else {
                                Vec::new()
                            };

                            // Write to corrupt shard, preserving metadata.
                            let write_ok = if !meta_bytes.is_empty() {
                                if let Some(ref refs) = raw_refs {
                                    repair::write_with_meta_to_shard(
                                        refs, corrupt_shard, &bad_store,
                                        &path, data, &meta_bytes,
                                    ).await.is_ok()
                                } else {
                                    bad_store.put(&path, PutPayload::from(data)).await.is_ok()
                                }
                            } else {
                                bad_store.put(&path, PutPayload::from(data)).await.is_ok()
                            };

                            if !write_ok {
                                tracing::warn!(
                                    key = key.as_str(),
                                    corrupt_shard,
                                    "read repair write failed"
                                );
                                failed_ctr.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                            // Restore catalog entry for the repaired shard.
                            let ml = if meta_bytes.is_empty() { 0u16 } else { meta_bytes.len() as u16 };
                            catalog.add_replica(&key, corrupt_shard, size, ml);
                            success_ctr.fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                key = key.as_str(),
                                good_shard,
                                corrupt_shard,
                                "read repair complete"
                            );
                        }
                        Err(e) => {
                            failed_ctr.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                key = key.as_str(),
                                "read repair: failed to read bytes from good shard: {e}"
                            );
                        }
                    }
                }
                Err(e) => {
                    failed_ctr.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        key = key.as_str(),
                        "read repair: failed to get from good shard: {e}"
                    );
                }
            }
        });
    }

    /// Number of background read-repair tasks spawned since startup.
    pub fn read_repair_count(&self) -> u64 {
        self.read_repair_count.load(Ordering::Relaxed)
    }

    /// Number of background read-repair tasks that completed successfully.
    pub fn read_repair_success(&self) -> u64 {
        self.read_repair_success.load(Ordering::Relaxed)
    }

    /// Number of background read-repair tasks that failed.
    pub fn read_repair_failed(&self) -> u64 {
        self.read_repair_failed.load(Ordering::Relaxed)
    }

    /// Rotate a shard list for read load-balancing so that successive
    /// reads start from different replicas.
    fn balanced_shard_order(&self, shards: &[ShardId]) -> Vec<ShardId> {
        if shards.len() <= 1 {
            return shards.to_vec();
        }
        let idx = self.read_counter.fetch_add(1, Ordering::Relaxed) as usize % shards.len();
        let mut out = Vec::with_capacity(shards.len());
        out.extend_from_slice(&shards[idx..]);
        out.extend_from_slice(&shards[..idx]);
        out
    }

    /// Write payload to the specified shards, returning which shards succeeded.
    /// Does not touch the catalog.
    ///
    /// **Partial-write behaviour:** When the write fans out to N shards and
    /// some succeed but fewer than `min_writes`, this method returns
    /// `InsufficientWrites`.  However, the shards that *did* accept the
    /// payload already hold the data.  If the caller retries, the object
    /// may land on a different set of targets, producing temporary
    /// over-replication.  This is safe: the catalog records actual
    /// placements, so `over_replication_trim()` / `repair_replication_sweep()`
    /// will remove the extras.  Callers that need exactly-once semantics
    /// should check the catalog before retrying.
    async fn write_to_shards(
        &self,
        location: &Path,
        data: Bytes,
        targets: &[ShardId],
    ) -> object_store::Result<(PutResult, Vec<ShardId>)> {
        let (result, placed) = self
            .write_to_shards_inner(location, data, targets, self.min_writes, true)
            .await?;
        let result = result.expect("at least one shard succeeded so PutResult is Some");
        Ok((result, placed))
    }

    /// Mark shard health after a fan-out write attempt and return retry
    /// targets if all initial targets failed.
    ///
    /// * All targets failed -> marks them Offline, returns fresh retry
    ///   target IDs (excluding `tried`).
    /// * Partial failure -> marks failed shards Degraded, returns empty.
    /// * All succeeded -> returns empty.
    ///
    /// Shared by `write_to_shards_inner` and `metadata::put_with_meta` /
    /// `put_with_meta_from_file` so health-marking logic lives in one
    /// place.
    pub(crate) fn mark_write_failures(
        &self,
        placed: &[ShardId],
        failed: &[ShardId],
        tried: &std::collections::HashSet<ShardId>,
        location: &Path,
    ) -> Vec<ShardId> {
        if placed.is_empty() && !failed.is_empty() {
            for &sid in failed {
                self.set_shard_health(sid, ShardHealth::Offline);
                warn!(
                    shard_id = sid,
                    key = %location,
                    "all targets failed, marking Offline for retry"
                );
            }
            self.select_targets(location)
                .into_iter()
                .filter(|sid| !tried.contains(sid))
                .collect()
        } else if !failed.is_empty() {
            for &sid in failed {
                if self.shard_health(sid) == Some(ShardHealth::Healthy) {
                    self.set_shard_health(sid, ShardHealth::Degraded);
                    warn!(shard_id = sid, "write partial failure, marking Degraded");
                }
            }
            vec![]
        } else {
            vec![]
        }
    }

    /// Best-effort cleanup of orphaned data from shards that accepted a
    /// payload when quorum was not met.
    ///
    /// Only cleans up for NEW keys (catalog miss).  For overwrites the
    /// old data was already replaced in-place so deleting would destroy
    /// the only surviving copy.
    ///
    /// Shared by `write_to_shards_inner` and `metadata::put_with_meta` /
    /// `put_with_meta_from_file`.
    pub(crate) async fn cleanup_orphaned_writes(
        &self,
        location: &Path,
        placed: &[ShardId],
    ) {
        let is_new_key = self.catalog.get(location.as_ref()).is_none();
        if !is_new_key {
            return;
        }
        let raw_refs = self.raw_refs();
        let cleanup_stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = placed
            .iter()
            .filter_map(|&sid| self.shard_store(sid).map(|s| (sid, s)))
            .collect();
        let cleanup_futs: Vec<_> = cleanup_stores
            .into_iter()
            .map(|(sid, store)| {
                let loc = location.clone();
                let refs = raw_refs.clone();
                async move {
                    let body_res = store.delete(&loc).await;
                    let kind = refs
                        .as_ref()
                        .map(|r| r.kind(sid))
                        .unwrap_or(crate::metadata::ShardKind::Sidecar);
                    crate::metadata::cleanup_sidecar(store.as_ref(), &loc, kind).await;
                    (sid, body_res)
                }
            })
            .collect();
        for (sid, outcome) in futures::future::join_all(cleanup_futs).await {
            if let Err(e) = outcome {
                warn!(
                    shard_id = sid,
                    path = %location,
                    error = %e,
                    "failed to clean up orphaned data after InsufficientWrites"
                );
            }
        }
    }

    /// Core fan-out write shared by `write_to_shards` and
    /// `write_to_shards_best_effort`.
    ///
    /// Health marking, retry-on-all-fail, and orphan cleanup are
    /// delegated to `mark_write_failures` and `cleanup_orphaned_writes`
    /// which are shared with `metadata::put_with_meta`.
    ///
    /// * `min_required` -- minimum number of successful puts (e.g.
    ///   `self.min_writes` for normal writes, `1` for best-effort).
    /// * `cleanup_on_fail` -- when true, delete orphaned data from
    ///   shards that accepted the payload if the quorum was not met.
    async fn write_to_shards_inner(
        &self,
        location: &Path,
        data: Bytes,
        targets: &[ShardId],
        min_required: usize,
        cleanup_on_fail: bool,
    ) -> object_store::Result<(Option<PutResult>, Vec<ShardId>)> {
        debug!(
            key = %location,
            size = data.len(),
            targets = ?targets,
            min_required,
            "write_to_shards_inner: starting fan-out"
        );
        // Clone stores under lock, then release before I/O.
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            targets
                .iter()
                .filter(|&&shard_id| shard_id < shards.len())
                .map(|&shard_id| (shard_id, shards[shard_id].store.clone()))
                .collect()
        };
        let futs: Vec<_> = stores
            .into_iter()
            .map(|(shard_id, store)| {
                let loc = location.clone();
                let payload = PutPayload::from(data.clone());
                async move { (shard_id, store.put(&loc, payload).await) }
            })
            .collect();
        let outcomes = futures::future::join_all(futs).await;

        let mut result: Option<PutResult> = None;
        let mut errors: Vec<String> = Vec::new();
        let mut placed: Vec<ShardId> = Vec::new();
        let mut failed: Vec<ShardId> = Vec::new();

        for (shard_id, outcome) in outcomes {
            match outcome {
                Ok(r) => {
                    placed.push(shard_id);
                    if result.is_none() {
                        result = Some(r);
                    }
                }
                Err(e) => {
                    errors.push(format!("shard {shard_id}: {e}"));
                    failed.push(shard_id);
                }
            }
        }

        if placed.is_empty() && !failed.is_empty() {
            // All initial targets failed.  Mark Offline and retry with
            // freshly selected targets via the shared helper.
            let tried: std::collections::HashSet<ShardId> =
                targets.iter().copied().collect();
            let retry_shard_ids =
                self.mark_write_failures(&placed, &failed, &tried, location);

            let retry_stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
                let shards = self.shards.read();
                retry_shard_ids
                    .iter()
                    .filter(|&&sid| sid < shards.len())
                    .map(|&sid| (sid, shards[sid].store.clone()))
                    .collect()
            };

            if !retry_stores.is_empty() {
                let retry_futs: Vec<_> = retry_stores
                    .into_iter()
                    .map(|(shard_id, store)| {
                        let loc = location.clone();
                        let payload = PutPayload::from(data.clone());
                        async move { (shard_id, store.put(&loc, payload).await) }
                    })
                    .collect();
                for (shard_id, outcome) in futures::future::join_all(retry_futs).await {
                    match outcome {
                        Ok(r) => {
                            placed.push(shard_id);
                            if result.is_none() {
                                result = Some(r);
                            }
                        }
                        Err(e) => {
                            errors.push(format!("shard {shard_id} (retry): {e}"));
                        }
                    }
                }
            }

            if placed.is_empty() {
                error!(
                    key = %location,
                    errors = ?errors,
                    "write_to_shards_inner: all replicas failed (including retry)"
                );
                return Err(object_store::Error::Generic {
                    store: "ShardedObjectStore",
                    source: Box::new(ShardError::AllReplicasFailed {
                        path: location.to_string(),
                        errors: errors.clone(),
                    }),
                });
            }
        } else {
            // Partial failure (or full success): mark Degraded if needed.
            let tried: std::collections::HashSet<ShardId> =
                targets.iter().copied().collect();
            self.mark_write_failures(&placed, &failed, &tried, location);
        }

        // Enforce min_required: enough replicas must have landed.
        if placed.len() < min_required {
            if cleanup_on_fail {
                self.cleanup_orphaned_writes(location, &placed).await;
            }

            return Err(object_store::Error::Generic {
                store: "ShardedObjectStore",
                source: Box::new(ShardError::InsufficientWrites {
                    path: location.to_string(),
                    required: min_required,
                    actual: placed.len(),
                    errors,
                }),
            });
        }

        Ok((result, placed))
    }

    /// Rebuild the catalog by scanning every shard's list() concurrently.
    /// Use after a crash or when bringing a cluster back online.
    ///
    /// When `raw_refs` is attached via `set_raw_refs()`, raw shards are
    /// scanned with `list_with_meta` to recover `meta_len` for each
    /// object. Without it, `meta_len` defaults to 0.
    pub async fn rebuild_catalog(&self) -> Result<usize> {
        info!("rebuild_catalog: starting full scan");
        let raw_refs = self.raw_refs();

        // Snapshot stores under lock, then release before I/O.
        let shard_stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            shards
                .iter()
                .filter(|s| !s.health.is_unavailable())
                .map(|s| (s.id, s.store.clone()))
                .collect()
        };
        let futs: Vec<_> = shard_stores
            .into_iter()
            .map(|(id, store)| {
                let refs = raw_refs.clone();
                async move {
                    // For raw shards with raw_refs, use list_with_meta to capture meta_len.
                    if let Some(ref r) = refs {
                        if let Some(raw) = r.get(id) {
                            let items = raw.list_with_meta(None);
                            let results: Vec<(ObjectMeta, u16)> = items;
                            return (id, Ok(results));
                        }
                    }
                    // Non-raw or no raw_refs: use standard list, meta_len=0.
                    let result: std::result::Result<Vec<ObjectMeta>, object_store::Error> =
                        store.list(None).try_collect().await;
                    match result {
                        Ok(objects) => (id, Ok(objects.into_iter().map(|m| (m, 0u16)).collect())),
                        Err(e) => (id, Err(e)),
                    }
                }
            })
            .collect();
        let results = futures::future::join_all(futs).await;

        let mut new_map: HashMap<String, PlacementEntry> = HashMap::new();
        let mut total = 0usize;
        for (shard_id, result) in results {
            match result {
                Ok(objects) => {
                    for (meta, ml) in objects {
                        let key = meta.location.to_string();
                        if key.ends_with(".__meta__") {
                            continue;
                        }
                        let entry = new_map.entry(key).or_insert_with(|| PlacementEntry {
                            shards: Vec::new(),
                            size: meta.size,
                            crc32c: None,
                            updated: meta.last_modified,
                            meta_len: ml,
                        });
                        // Preserve the highest meta_len seen across replicas.
                        if ml > entry.meta_len {
                            entry.meta_len = ml;
                        }
                        if !entry.shards.contains(&shard_id) {
                            entry.shards.push(shard_id);
                        }
                        total += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!("rebuild_catalog: shard {shard_id} scan failed: {e}");
                }
            }
        }

        // Atomically swap the catalog contents.
        self.catalog.replace(new_map);
        // Rebuild reflects ground truth from shards -- nothing unsaved.
        self.catalog.clear_dirty();
        info!(entries = total, "rebuild_catalog: complete");
        Ok(total)
    }

    /// Rebuild catalog entries for a single shard by scanning its contents.
    ///
    /// Unlike `rebuild_catalog()` which replaces the entire catalog, this
    /// only adds/updates entries for objects found on the specified shard.
    /// Does not remove existing entries for other shards.
    ///
    /// When `raw_refs` is attached via `set_raw_refs()`, raw shards are
    /// scanned with `list_with_meta` to recover `meta_len`.
    ///
    /// Returns the number of objects found on the shard.
    pub async fn rebuild_catalog_for_shard(&self, shard_id: ShardId) -> Result<usize> {
        let store = {
            let shards = self.shards.read();
            let shard = shards.get(shard_id).ok_or(ShardError::NoShards)?;
            shard.store.clone()
        };

        // Purge existing catalog entries for this shard first so stale
        // entries (objects no longer on disk) are removed.
        self.catalog.remove_all_for_shard(shard_id);

        // Try raw path first for meta_len recovery.
        if let Some(ref refs) = self.raw_refs() {
            if let Some(raw) = refs.get(shard_id) {
                let items = raw.list_with_meta(None);
                let count = items.len();
                for (meta, ml) in items {
                    let key = meta.location.to_string();
                    self.catalog.add_replica(&key, shard_id, meta.size, ml);
                }
                return Ok(count);
            }
        }

        let objects: Vec<ObjectMeta> = store.list(None).try_collect().await
            .map_err(ShardError::ObjectStore)?;

        let count = objects.iter()
            .filter(|m| !m.location.as_ref().ends_with(".__meta__"))
            .count();
        for meta in &objects {
            if meta.location.as_ref().ends_with(".__meta__") {
                continue;
            }
            let key = meta.location.to_string();
            self.catalog.add_replica(&key, shard_id, meta.size, 0);
        }
        Ok(count)
    }

    /// Invalidate a shard: mark it Degraded, purge its catalog entries,
    /// re-scan just that shard, and restore catalog entries for objects
    /// that are still present.
    ///
    /// Use after a crash, detected corruption, or manual admin command.
    /// Returns an `InvalidateReport` describing what happened.
    pub async fn invalidate_shard(&self, shard_id: ShardId) -> Result<InvalidateReport> {
        {
            let shards = self.shards.read();
            if shard_id >= shards.len() {
                return Err(ShardError::NoShards);
            }
        }

        // 1. Mark shard Degraded during rescan.
        {
            let mut shards = self.shards.write();
            shards[shard_id].health = ShardHealth::Degraded;
        }

        // 2. Snapshot keys currently attributed to this shard, then purge.
        let old_entries = self.catalog.entries_for_shard(shard_id);
        let old_keys: std::collections::HashSet<String> =
            old_entries.iter().map(|(k, _)| k.clone()).collect();
        let entries_purged = self.catalog.remove_all_for_shard(shard_id);

        // 3. Re-scan the shard and add fresh entries.
        let scan_result = self.rebuild_catalog_for_shard(shard_id).await;

        let (entries_restored, scan_ok) = match scan_result {
            Ok(n) => (n, true),
            Err(e) => {
                tracing::warn!(
                    "invalidate_shard: re-scan of shard {} failed: {}",
                    shard_id, e
                );
                (0, false)
            }
        };

        // 4. Determine missing keys (were in catalog before, not found after).
        let new_entries = self.catalog.entries_for_shard(shard_id);
        let new_keys: std::collections::HashSet<String> =
            new_entries.iter().map(|(k, _)| k.clone()).collect();
        let missing_keys: Vec<String> = old_keys
            .difference(&new_keys)
            .cloned()
            .collect();

        // 5. If scan succeeded, mark shard Healthy again.
        if scan_ok {
            let mut shards = self.shards.write();
            shards[shard_id].health = ShardHealth::Healthy;
            shards[shard_id].offline_since = None;
        }

        Ok(InvalidateReport {
            shard_id,
            entries_purged,
            entries_restored,
            missing_keys,
            scan_ok,
        })
    }

    /// Scan the catalog and return objects whose replica count is below
    /// the configured replication factor.
    ///
    /// Returns `(key, current_replica_count)` pairs. The caller can then
    /// use `replicate_object()` to restore RF. This is computed on the
    /// fly from the catalog -- no persistent under-replication tracking
    /// is needed.
    pub fn find_under_replicated(&self) -> Vec<(String, usize)> {
        let rf = self.replication.factor();
        let shards = self.shards.read();
        self.catalog.with_entries(|map| {
            map.iter()
                .filter_map(|(key, entry)| {
                    // Only count shards that are Healthy or Syncing.
                    // Degraded and Offline shards are not counted as
                    // available replicas because their data may be
                    // corrupt or unreachable.
                    let healthy_count = entry.shards.iter()
                        .filter(|&&sid| {
                            shards.get(sid)
                                .map(|s| matches!(s.health,
                                    ShardHealth::Healthy | ShardHealth::Syncing))
                                .unwrap_or(false)
                        })
                        .count();
                    if healthy_count < rf {
                        Some((key.clone(), healthy_count))
                    } else {
                        None
                    }
                })
                .collect()
        })
    }

    /// Attach a store to an offline shard slot, mark it Healthy, and
    /// rebuild its catalog entries.
    ///
    /// Use `force = true` to skip invalidation -- the shard's existing
    /// objects are trusted and added to the catalog without purging
    /// first. This is the right choice when reattaching a known-good
    /// device (e.g., reconnecting after a network outage or growing
    /// the device). Use `force = false` to invalidate first (purge +
    /// re-scan) when the shard's contents may be stale.
    ///
    /// Returns the number of objects found on the newly attached shard.
    pub async fn attach_shard(
        &self,
        shard_id: ShardId,
        store: Arc<dyn ObjectStore>,
        force: bool,
    ) -> Result<usize> {
        {
            let mut shards = self.shards.write();
            if shard_id >= shards.len() {
                return Err(ShardError::NoShards);
            }
            // Swap in the real store but keep Syncing until catalog
            // rebuild completes -- this prevents writes from targeting
            // the shard before we know what objects it holds.
            shards[shard_id].store = store;
            shards[shard_id].health = ShardHealth::Syncing;
            shards[shard_id].free_space = u64::MAX;
            shards[shard_id].offline_since = None;
            shards[shard_id].suppress_replication = false;
            shards[shard_id].detach_reason = None;
        }

        let result = if force {
            // Trust existing data: just scan and add to catalog.
            self.rebuild_catalog_for_shard(shard_id).await
        } else {
            // Full invalidation: purge, re-scan, detect missing keys.
            self.invalidate_shard(shard_id).await.map(|r| r.entries_restored)
        };

        // On success, mark Healthy; on failure, revert to Offline so
        // the shard is not stuck in Syncing forever.
        {
            let mut shards = self.shards.write();
            if shard_id < shards.len() {
                if result.is_ok() {
                    shards[shard_id].health = ShardHealth::Healthy;
                } else {
                    shards[shard_id].health = ShardHealth::Offline;
                    shards[shard_id].offline_since = Some(Utc::now());
                }
            }
        }

        result
    }

    /// Detach a shard: replace it with an offline placeholder and mark
    /// it `Offline`. Catalog entries referencing this shard are NOT
    /// purged -- they remain so the system knows the shard's objects
    /// exist but are currently unreachable. When the shard is
    /// reattached, `attach_shard(force=true)` will re-verify.
    ///
    /// Returns the previous health status.
    pub fn detach_shard(&self, shard_id: ShardId) -> Option<ShardHealth> {
        let mut shards = self.shards.write();
        if shard_id >= shards.len() {
            return None;
        }
        let prev = shards[shard_id].health;
        shards[shard_id].store = Arc::new(OfflinePlaceholderStore { shard_id });
        shards[shard_id].health = ShardHealth::Offline;
        shards[shard_id].free_space = 0;
        if !prev.is_unavailable() {
            shards[shard_id].offline_since = Some(Utc::now());
        }
        Some(prev)
    }

    /// Manually take a shard offline ("detach") so the recovery loop
    /// will NOT auto-reattach it.  The shard is replaced with an
    /// offline placeholder and marked `Detached`.
    ///
    /// When `suppress_replication` is true, re-replication sweeps will
    /// skip this shard's grace-period check -- objects are NOT
    /// proactively copied to survivors.  Use this when you plan to
    /// bring the shard back soon (e.g. raw device repair).
    ///
    /// Returns the previous health status.
    pub fn hold_offline(
        &self,
        shard_id: ShardId,
        suppress_replication: bool,
        reason: DetachReason,
    ) -> Option<ShardHealth> {
        let mut shards = self.shards.write();
        if shard_id >= shards.len() {
            return None;
        }
        let prev = shards[shard_id].health;
        shards[shard_id].store = Arc::new(OfflinePlaceholderStore { shard_id });
        shards[shard_id].health = ShardHealth::Detached;
        shards[shard_id].free_space = 0;
        shards[shard_id].detach_reason = Some(reason);
        shards[shard_id].suppress_replication = suppress_replication;
        if !prev.is_unavailable() {
            shards[shard_id].offline_since = Some(Utc::now());
        }
        info!(
            shard_id,
            suppress_replication,
            "shard manually taken offline (Detached)"
        );
        Some(prev)
    }

    /// Clear the held-offline flag so the shard becomes eligible for
    /// `attach_shard()`.  This does NOT reattach -- call `attach_shard`
    /// separately after `release_hold`.
    ///
    /// Returns true if the shard was previously held (Detached).
    pub fn release_hold(&self, shard_id: ShardId) -> bool {
        let mut shards = self.shards.write();
        if let Some(s) = shards.get_mut(shard_id) {
            if s.health == ShardHealth::Detached {
                s.health = ShardHealth::Offline;
                s.detach_reason = None;
                s.suppress_replication = false;
                return true;
            }
        }
        false
    }

    /// Get the detach reason for a shard (None if not detached).
    pub fn shard_detach_reason(&self, id: ShardId) -> Option<DetachReason> {
        self.shards.read().get(id).and_then(|s| s.detach_reason)
    }

    /// Check whether re-replication is suppressed for a shard.
    pub fn shard_suppress_replication(&self, id: ShardId) -> bool {
        self.shards.read().get(id).map(|s| s.suppress_replication).unwrap_or(false)
    }

    /// Set the detach reason for a shard (e.g. after `detach_shard`).
    ///
    /// This is used by the recovery loop to annotate why a shard was
    /// automatically taken offline (probe failure vs device missing).
    pub fn set_detach_reason(&self, id: ShardId, reason: DetachReason) {
        let mut shards = self.shards.write();
        if let Some(s) = shards.get_mut(id) {
            s.detach_reason = Some(reason);
        }
    }

    /// Copy a single object from one shard to another to restore
    /// replication factor. Reads from `from_shard`, writes to
    /// `to_shard`, and updates the catalog.
    ///
    /// When `raw_refs` is provided, metadata (TLV suffix on raw shards,
    /// sidecar files, or S3 attributes) is preserved on the target.
    /// Without it the copy is data-only (legacy behaviour).
    ///
    /// Returns the size of the replicated object in bytes.
    pub async fn replicate_object(
        &self,
        key: &str,
        from_shard: ShardId,
        to_shard: ShardId,
        raw_refs: Option<&crate::metadata::RawRefRegistry>,
    ) -> Result<u64> {
        let (from_store, to_store) = {
            let shards = self.shards.read();
            if from_shard >= shards.len() || to_shard >= shards.len() {
                return Err(ShardError::NoShards);
            }
            (shards[from_shard].store.clone(), shards[to_shard].store.clone())
        };

        let path = Path::from(key);

        // Read from source shard, capturing S3 attributes before consuming.
        let get_result = from_store.get(&path).await
            .map_err(ShardError::ObjectStore)?;
        let attributes = get_result.attributes.clone();
        let data = get_result.bytes().await.map_err(ShardError::ObjectStore)?;
        let size = data.len() as u64;

        // Try to read metadata from the source shard.
        let meta_bytes = if let Some(refs) = raw_refs {
            crate::metadata::read_metadata_from_shard(
                from_store.as_ref(), from_shard, &path, &attributes, refs,
            ).await
        } else {
            None
        };

        // Write to target shard, preserving metadata when available.
        if let Some(ref meta) = meta_bytes {
            if let Some(refs) = raw_refs {
                repair::write_with_meta_to_shard(
                    refs, to_shard, &to_store, &path, data, meta,
                ).await.map_err(|e| ShardError::ObjectStore(
                    object_store::Error::Generic {
                        store: "replicate_object",
                        source: e,
                    },
                ))?;
            } else {
                to_store.put(&path, PutPayload::from(data)).await
                    .map_err(ShardError::ObjectStore)?;
            }
        } else {
            to_store.put(&path, PutPayload::from(data)).await
                .map_err(ShardError::ObjectStore)?;
        }

        // Update catalog.
        let ml = meta_bytes.as_ref().map_or(0u16, |m| m.len() as u16);
        self.catalog.add_replica(key, to_shard, size, ml);
        Ok(size)
    }

    /// Find the least-full healthy shard that does not already hold
    /// the given object.
    ///
    /// Used by the re-replication sweep: when a shard is permanently
    /// offline the system needs to copy under-replicated objects to a
    /// *different* healthy shard to restore RF.  Picking the shard
    /// with the most free space keeps utilization balanced.
    ///
    /// Returns `None` if no suitable target exists (all healthy shards
    /// already hold the object or no shards are healthy).
    pub fn find_replication_target(&self, key: &str) -> Option<ShardId> {
        let existing: std::collections::HashSet<ShardId> = self
            .catalog
            .get(key)
            .map(|e| e.shards.into_iter().collect())
            .unwrap_or_default();

        let shards = self.shards.read();
        (0..shards.len())
            .filter(|&sid| {
                !existing.contains(&sid)
                    && shards[sid].health == ShardHealth::Healthy
            })
            .max_by_key(|&sid| shards[sid].free_space)
    }

    /// Count objects per healthy shard by iterating the catalog.
    ///
    /// Returns `(shard_id, object_count)` pairs for every shard whose
    /// health is `Healthy`.  This is used by the redistribute sweep to
    /// determine which shards are over- or under-populated.
    pub fn shard_object_counts(&self) -> Vec<(ShardId, usize)> {
        let shards = self.shards.read();
        let healthy_ids: Vec<ShardId> = shards.iter()
            .filter(|s| s.health == ShardHealth::Healthy)
            .map(|s| s.id)
            .collect();
        let mut counts: Vec<(ShardId, usize)> = healthy_ids.iter()
            .map(|&sid| (sid, 0usize))
            .collect();
        self.catalog.with_entries(|map| {
            for (_key, entry) in map.iter() {
                for &sid in &entry.shards {
                    if let Some(pair) = counts.iter_mut().find(|(id, _)| *id == sid) {
                        pair.1 += 1;
                    }
                }
            }
        });
        counts
    }

    /// Scan the catalog and return objects whose total replica count
    /// that are usable (Healthy or Degraded) exceeds the replication
    /// factor.  Offline and Syncing shards are excluded because they
    /// cannot serve reads -- trimming a usable copy while replicas
    /// are still syncing could reduce availability.  Once a syncing
    /// shard finishes, the next sweep will detect the extra copy.
    ///
    /// Returns `(key, usable_replica_count)` pairs.
    pub fn find_over_replicated(&self) -> Vec<(String, usize)> {
        let rf = self.replication.factor();
        let shards = self.shards.read();
        self.catalog.with_entries(|map| {
            map.iter()
                .filter_map(|(key, entry)| {
                    let count = entry.shards.iter()
                        .filter(|&&sid| {
                            shards.get(sid)
                                .map(|s| {
                                    !s.health.is_unavailable()
                                        && s.health != ShardHealth::Syncing
                                })
                                .unwrap_or(false)
                        })
                        .count();
                    if count > rf {
                        Some((key.clone(), count))
                    } else {
                        None
                    }
                })
                .collect()
        })
    }

    /// For an over-replicated object, pick the shard to shed: the one
    /// with the *least* free space among healthy shards holding it.
    /// Removing from the fullest shard frees up the most constrained
    /// resource.
    ///
    /// Returns `None` if the object has <= rf copies or no suitable
    /// shard can be removed.
    pub fn pick_excess_shard(&self, key: &str) -> Option<ShardId> {
        let rf = self.replication.factor();
        let entry = self.catalog.get(key)?;
        if entry.shards.len() <= rf {
            return None;
        }
        let shards = self.shards.read();
        entry.shards.iter()
            .copied()
            .filter(|&sid| {
                shards.get(sid)
                    .map(|s| s.health == ShardHealth::Healthy)
                    .unwrap_or(false)
            })
            // Only remove if we would still have >= rf copies
            // after removing this candidate.
            .filter(|&candidate| {
                let remaining = entry.shards.iter()
                    .filter(|&&s| s != candidate)
                    .filter(|&&s| shards.get(s)
                        .map(|sh| sh.health == ShardHealth::Healthy)
                        .unwrap_or(false))
                    .count();
                remaining >= rf
            })
            .min_by_key(|&sid| shards[sid].free_space)
    }

    /// Delete a single replica of an object from a specific shard and
    /// update the catalog.  Used to trim over-replicated objects.
    pub async fn remove_replica(
        &self,
        key: &str,
        shard_id: ShardId,
    ) -> Result<()> {
        let store = {
            let shards = self.shards.read();
            if shard_id >= shards.len() {
                return Err(ShardError::NoShards);
            }
            shards[shard_id].store.clone()
        };
        let path = Path::from(key);
        crate::metadata::cleanup_sidecar_maybe(
            store.as_ref(), &path, self.raw_refs().as_deref(), shard_id,
        ).await;
        store.delete(&path).await.map_err(ShardError::ObjectStore)?;
        self.catalog.remove_shard(key, shard_id);
        Ok(())
    }

    /// Probe each non-offline shard with a small LIST to validate it is
    /// accessible. Returns a vec of `(shard_id, accessible)` for every
    /// shard in the cluster.
    ///
    /// This is useful after construction to detect shards that are locked
    /// by another process (flock), have gone away, or are otherwise
    /// unreachable. Offline/Syncing shards are skipped (reported as false).
    pub async fn validate_shard_access(&self) -> Vec<(ShardId, bool)> {
        let shard_info: Vec<(ShardId, ShardHealth, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            shards.iter().map(|s| (s.id, s.health, s.store.clone())).collect()
        };

        let futs: Vec<_> = shard_info.into_iter().map(|(id, health, store)| {
            async move {
                if health.is_unavailable() {
                    return (id, false);
                }
                let ok = crate::repair::probe_store(
                    &store,
                    std::time::Duration::from_secs(5),
                ).await;
                (id, ok)
            }
        }).collect();

        futures::future::join_all(futs).await
    }

    /// Verify a single object by reading every replica, computing CRC32c,
    /// and comparing against the catalog and each other.
    pub async fn verify_object(&self, key: &str) -> Result<VerifyObjectReport> {
        let entry = self.catalog.get(key)
            .ok_or_else(|| ShardError::NotFound(key.to_string()))?;

        let catalog_crc = entry.crc32c;
        let path = Path::from(key);

        // Collect stores for all shards listed in the catalog entry.
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            entry.shards.iter()
                .filter_map(|&sid| {
                    shards.get(sid).map(|s| (sid, s.store.clone()))
                })
                .collect()
        };

        // Read each replica concurrently.
        let futs: Vec<_> = stores.into_iter().map(|(sid, store)| {
            let p = path.clone();
            async move {
                match store.get(&p).await {
                    Ok(result) => match result.bytes().await {
                        Ok(data) => {
                            let crc = crc32c::crc32c(&data);
                            let size = data.len() as u64;
                            let matches = catalog_crc.map(|c| c == crc);
                            (sid, Some(crc), Some(size), matches, None)
                        }
                        Err(e) => (sid, None, None, None, Some(e.to_string())),
                    },
                    Err(e) => (sid, None, None, None, Some(e.to_string())),
                }
            }
        }).collect();

        let outcomes = futures::future::join_all(futs).await;

        let mut replicas = Vec::with_capacity(outcomes.len());
        for (sid, crc, size, matches, error) in outcomes {
            replicas.push(VerifyReplicaResult {
                shard_id: sid,
                crc32c: crc,
                size,
                matches_catalog: matches,
                error,
            });
        }

        // Check inter-replica consistency: all readable replicas have the same CRC.
        let readable_crcs: Vec<u32> = replicas.iter()
            .filter_map(|r| r.crc32c)
            .collect();
        let replicas_consistent = if readable_crcs.len() <= 1 {
            true
        } else {
            readable_crcs.windows(2).all(|w| w[0] == w[1])
        };

        Ok(VerifyObjectReport {
            key: key.to_string(),
            catalog_crc,
            replicas,
            replicas_consistent,
        })
    }

    /// Verify all objects in the catalog (or a subset by prefix).
    ///
    /// Returns an aggregate report. Only objects with mismatches or errors
    /// appear in `details` to keep the report compact.
    pub async fn verify_all(&self, prefix: Option<&str>) -> VerifyReport {
        let entries = self.catalog.all_entries();
        let keys: Vec<String> = entries.into_iter()
            .filter(|(k, _)| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .map(|(k, _)| k)
            .collect();

        let mut report = VerifyReport::default();

        // Process in parallel batches to avoid overwhelming backends.
        for chunk in keys.chunks(64) {
            let futs: Vec<_> = chunk.iter()
                .map(|key| self.verify_object(key))
                .collect();
            let results = join_all(futs).await;
            for result in results {
                report.objects_checked += 1;
                match result {
                    Ok(obj_report) => {
                        let has_errors = obj_report.replicas.iter().any(|r| r.error.is_some());
                        let has_mismatch = !obj_report.replicas_consistent
                            || obj_report.replicas.iter().any(|r| r.matches_catalog == Some(false));

                        if has_mismatch {
                            report.objects_mismatched += 1;
                            report.details.push(obj_report);
                        } else if has_errors {
                            report.objects_with_errors += 1;
                            report.details.push(obj_report);
                        } else {
                            report.objects_ok += 1;
                        }
                    }
                    Err(_) => {
                        report.objects_with_errors += 1;
                    }
                }
            }
        }

        report
    }

    // -- Cross-shard MD5 verification ---------------------------------

    /// Get the MD5 digest of a single object on a single shard.
    ///
    /// For S3-type stores whose `head()` returns an ETag, the ETag is
    /// used directly (no body download).  For raw and filesystem stores
    /// the full body is read (which forces internal CRC verification on
    /// raw stores) and MD5 is computed from the bytes.
    async fn get_shard_md5(
        store: Arc<dyn ObjectStore>,
        shard_id: ShardId,
        key: &str,
    ) -> std::result::Result<ShardObjectDigest, (ShardId, String)> {
        let path = Path::from(key);

        // Try head first -- if the store provides an ETag we can use it.
        match store.head(&path).await {
            Ok(meta) => {
                if let Some(etag) = &meta.e_tag {
                    // Strip surrounding quotes and any `W/` weak prefix.
                    let hex = etag
                        .trim_start_matches("W/")
                        .trim_matches('"')
                        .to_string();
                    // Validate it looks like a plain MD5 hex (32 hex chars).
                    // Multipart S3 ETags contain a dash (e.g. "abc-3") and
                    // should fall through to the body-read path.
                    if hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                        return Ok(ShardObjectDigest {
                            shard_id,
                            md5_hex: hex,
                            size: meta.size as u64,
                            last_modified: meta.last_modified,
                        });
                    }
                }
            }
            Err(e) => {
                return Err((shard_id, format!("head failed: {}", e)));
            }
        }

        // No ETag or non-standard ETag: read the full body and compute MD5.
        // For raw stores this triggers per-block CRC + payload CRC verification.
        match store.get(&path).await {
            Ok(result) => {
                let meta = result.meta.clone();
                match result.bytes().await {
                    Ok(data) => {
                        let digest = md5::compute(&data);
                        Ok(ShardObjectDigest {
                            shard_id,
                            md5_hex: format!("{:x}", digest),
                            size: data.len() as u64,
                            last_modified: meta.last_modified,
                        })
                    }
                    Err(e) => Err((shard_id, format!("body read failed: {}", e))),
                }
            }
            Err(e) => Err((shard_id, format!("get failed: {}", e))),
        }
    }

    /// Cross-verify a single object by comparing the MD5 digest of every
    /// replica.  Requires the object to be stored on at least 2 shards.
    ///
    /// Each shard's copy is read (or its ETag queried for S3 backends),
    /// and the resulting MD5 hex strings are compared.  The report
    /// includes per-shard MD5, size, and timestamp so the operator can
    /// determine which copy (if any) is the outlier.
    pub async fn cross_verify_object(&self, key: &str) -> Result<CrossVerifyReport> {
        let entry = self.catalog.get(key)
            .ok_or_else(|| ShardError::NotFound(key.to_string()))?;

        let catalog_crc = entry.crc32c;

        // Collect stores for all shards listed in the catalog entry.
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            entry.shards.iter()
                .filter_map(|&sid| {
                    shards.get(sid).map(|s| (sid, s.store.clone()))
                })
                .collect()
        };

        if stores.len() < 2 {
            return Err(ShardError::AllReplicasFailed {
                path: key.to_string(),
                errors: vec!["cross-verify requires at least 2 replicas".to_string()],
            });
        }

        // Query each shard concurrently.
        let futs: Vec<_> = stores.into_iter().map(|(sid, store)| {
            let k = key.to_string();
            async move { Self::get_shard_md5(store, sid, &k).await }
        }).collect();

        let outcomes = join_all(futs).await;

        let mut digests = Vec::new();
        let mut errors = Vec::new();
        for outcome in outcomes {
            match outcome {
                Ok(d) => digests.push(d),
                Err(e) => errors.push(e),
            }
        }

        // Check if all readable replicas agree on MD5.
        let consistent = if digests.len() < 2 {
            // Cannot determine consistency with fewer than 2 readable replicas.
            digests.len() <= 1 && errors.is_empty()
        } else {
            digests.windows(2).all(|w| w[0].md5_hex == w[1].md5_hex)
        };

        Ok(CrossVerifyReport {
            key: key.to_string(),
            catalog_crc,
            shards: digests,
            consistent,
            errors,
        })
    }

    /// Cross-verify all objects in the catalog (or a prefix subset) by
    /// comparing MD5 digests across replicas.
    ///
    /// Objects stored on only 1 shard are skipped (counted in
    /// `objects_skipped_single_replica`).  Processing is batched (64 at
    /// a time) to avoid overwhelming backends.
    pub async fn cross_verify_all(
        &self,
        prefix: Option<&str>,
        progress: Option<&crate::repair::ProgressSink>,
    ) -> CrossVerifyAllReport {
        let entries = self.catalog.all_entries();
        let keys: Vec<(String, usize)> = entries.into_iter()
            .filter(|(k, _)| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .map(|(k, e)| (k, e.shards.len()))
            .collect();

        let mut report = CrossVerifyAllReport::default();

        let (multi_keys, single_count): (Vec<_>, usize) = {
            let mut multi = Vec::new();
            let mut single = 0usize;
            for (k, shard_count) in keys {
                if shard_count >= 2 {
                    multi.push(k);
                } else {
                    single += 1;
                }
            }
            (multi, single)
        };
        report.objects_skipped_single_replica = single_count as u64;

        let total = multi_keys.len();
        if let Some(tx) = progress {
            let _ = tx.send(format!("cross-verify: {total} objects to check ({single_count} skipped, single replica)"));
        }

        for chunk in multi_keys.chunks(64) {
            let futs: Vec<_> = chunk.iter()
                .map(|key| self.cross_verify_object(key))
                .collect();
            let results = join_all(futs).await;
            for result in results {
                report.objects_checked += 1;
                match result {
                    Ok(obj_report) => {
                        let has_errors = !obj_report.errors.is_empty();
                        if !obj_report.consistent {
                            if let Some(tx) = progress {
                                let _ = tx.send(format!("MISMATCH {}", obj_report.key));
                            }
                            report.objects_mismatched += 1;
                            report.details.push(obj_report);
                        } else if has_errors {
                            if let Some(tx) = progress {
                                let _ = tx.send(format!("ERROR {}", obj_report.key));
                            }
                            report.objects_with_errors += 1;
                            report.details.push(obj_report);
                        } else {
                            report.objects_ok += 1;
                        }
                    }
                    Err(_) => {
                        report.objects_with_errors += 1;
                    }
                }
            }
            if let Some(tx) = progress {
                let _ = tx.send(format!("progress: {}/{total} objects verified", report.objects_checked));
            }
        }

        report
    }

    // -- Delete marker helpers ----------------------------------------

    /// Build the delete-marker key for a given object key.
    fn delete_marker_key(key: &str) -> String {
        format!("{DELETE_MARKER_PREFIX}{key}")
    }

    /// Return `true` if `key` lives under the delete-marker namespace.
    pub fn is_delete_marker(key: &str) -> bool {
        key.starts_with(DELETE_MARKER_PREFIX)
    }

    /// Write a delete marker for `key` to **all** healthy shards.
    ///
    /// Unlike normal puts (which target `replication_factor` shards),
    /// markers go to every healthy shard so that recovery sync on any
    /// returning shard will see the marker locally.  The marker body
    /// is the deletion timestamp as an RFC 3339 string.
    ///
    /// When `delete_requires_min_writes` is false (default), the marker
    /// write succeeds as long as at least one shard accepts it.  When
    /// true, the normal `min_writes` quorum is enforced.
    async fn put_delete_marker(&self, key: &str) -> object_store::Result<DateTime<Utc>> {
        let marker_key = Self::delete_marker_key(key);
        let now = Utc::now();
        let body = Bytes::from(now.to_rfc3339());
        let path = Path::from(marker_key.as_str());

        // Collect ALL healthy shards (not just RF targets).
        let all_healthy: Vec<ShardId> = {
            let shards = self.shards.read();
            (0..shards.len())
                .filter(|&id| !shards[id].health.is_unavailable())
                .collect()
        };
        if all_healthy.is_empty() {
            return Err(object_store::Error::Generic {
                store: "ShardedObjectStore",
                source: Box::new(ShardError::NoShards),
            });
        }

        if self.delete_requires_min_writes {
            // Strict mode: enforce min_writes quorum (same as puts).
            let (_, placed) = self.write_to_shards(&path, body, &all_healthy).await?;
            let size = now.to_rfc3339().len() as u64;
            self.catalog.put(marker_key, placed, size, None, 0);
        } else {
            // Best-effort: succeed if at least one shard accepts.
            let placed = self.write_to_shards_best_effort(&path, body, &all_healthy).await?;
            let size = now.to_rfc3339().len() as u64;
            self.catalog.put(marker_key, placed, size, None, 0);
        }
        Ok(now)
    }

    /// Write payload to the specified shards, succeeding if at least one
    /// shard accepts.  Does not enforce `min_writes`.  Used for delete
    /// markers in best-effort mode.
    async fn write_to_shards_best_effort(
        &self,
        location: &Path,
        data: Bytes,
        targets: &[ShardId],
    ) -> object_store::Result<Vec<ShardId>> {
        let (_result, placed) = self
            .write_to_shards_inner(location, data, targets, 1, false)
            .await?;
        Ok(placed)
    }

    /// Remove a delete marker for `key` from all shards.  Best-effort:
    /// silently ignores NotFound.
    async fn remove_delete_marker(&self, key: &str) -> object_store::Result<()> {
        let marker_key = Self::delete_marker_key(key);
        let path = Path::from(marker_key.as_str());
        // Use the raw trait delete (no marker recursion).
        self.delete_raw(&path).await
    }

    /// Read the delete marker for `key`, returning its deletion timestamp
    /// if one exists.
    pub async fn get_delete_marker(&self, key: &str) -> Option<DateTime<Utc>> {
        let marker_key = Self::delete_marker_key(key);
        let path = Path::from(marker_key.as_str());
        let result = self.get_raw(&path).await.ok()?;
        let data = result.bytes().await.ok()?;
        let s = std::str::from_utf8(&data).ok()?;
        DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.with_timezone(&Utc))
    }

    /// List all current delete markers across the cluster.
    /// Returns `(original_key, deleted_at)` pairs.
    pub async fn list_delete_markers(&self) -> Vec<(String, DateTime<Utc>)> {
        let prefix = Path::from(DELETE_MARKER_PREFIX);
        let entries = self.catalog.list(Some(prefix.as_ref()));
        let original_keys: Vec<String> = entries.iter().filter_map(|meta| {
            let marker_key = meta.location.to_string();
            marker_key.strip_prefix(DELETE_MARKER_PREFIX).map(|k| k.to_string())
        }).collect();
        let futs: Vec<_> = original_keys.iter()
            .map(|key| self.get_delete_marker(key))
            .collect();
        let results = join_all(futs).await;
        original_keys.into_iter().zip(results)
            .filter_map(|(key, ts)| ts.map(|t| (key, t)))
            .collect()
    }

    /// Vacuum stale delete markers.
    ///
    /// Requires all shards to be healthy -- returns an error if any shard
    /// is offline because the marker may still be needed when that shard
    /// comes back.
    ///
    /// For each marker:
    /// - If no live object exists for the key: marker fully applied,
    ///   remove it.
    /// - If a live object has `last_modified > marker_timestamp`: object
    ///   was re-PUT after deletion, marker is stale, remove it.
    /// - If a live object has `last_modified <= marker_timestamp`: object
    ///   should have been deleted but was not (missed delete), delete the
    ///   object then remove the marker.
    ///
    /// Returns `(markers_purged, stale_objects_cleaned)`.
    pub async fn vacuum_delete_markers(
        &self,
        progress: Option<&crate::repair::ProgressSink>,
    ) -> Result<(usize, usize)> {
        // Prevent concurrent vacuum runs.
        if self.vacuum_in_progress.swap(true, Ordering::AcqRel) {
            return Err(ShardError::VacuumAlreadyRunning);
        }

        let result = self.vacuum_delete_markers_inner(progress).await;

        self.vacuum_in_progress.store(false, Ordering::Release);
        result
    }

    async fn vacuum_delete_markers_inner(
        &self,
        progress: Option<&crate::repair::ProgressSink>,
    ) -> Result<(usize, usize)> {
        // Safety: refuse if any shard is offline or degraded.
        // Offline shards may hold pre-delete data that would reappear
        // once they come back.  Degraded shards may have partially
        // corrupt data.  Syncing shards are actively rebuilding and
        // their sync process replays delete markers, so they are safe.
        {
            let shards = self.shards.read();
            for (idx, s) in shards.iter().enumerate() {
                if s.health.is_unavailable()
                    || s.health == ShardHealth::Degraded
                {
                    return Err(ShardError::VacuumOfflineShard { shard_id: idx });
                }
            }
        }

        let markers = self.list_delete_markers().await;
        let marker_count = markers.len();
        if let Some(tx) = progress {
            let _ = tx.send(format!("vacuum: {marker_count} delete markers to process"));
        }
        let mut purged: usize = 0;
        let mut cleaned: usize = 0;

        for (key, deleted_at) in &markers {
            let path = Path::from(key.as_str());

            // Check if the live object still exists on any shard.
            match self.head_raw(&path).await {
                Ok(meta) => {
                    if meta.last_modified > *deleted_at {
                        // Object was re-PUT after the delete -- marker is stale.
                        let _ = self.remove_delete_marker(key).await;
                        purged += 1;
                        if let Some(tx) = progress {
                            let _ = tx.send(format!("vacuum: purged stale marker for {key}"));
                        }
                    } else {
                        // Missed delete: remove the stale object, then the marker.
                        let _ = self.delete_raw(&path).await;
                        let _ = self.remove_delete_marker(key).await;
                        purged += 1;
                        cleaned += 1;
                        if let Some(tx) = progress {
                            let _ = tx.send(format!("vacuum: cleaned stale object + marker for {key}"));
                        }
                    }
                }
                Err(_) => {
                    // Object already gone everywhere -- marker fully applied.
                    let _ = self.remove_delete_marker(key).await;
                    purged += 1;
                    if let Some(tx) = progress {
                        let _ = tx.send(format!("vacuum: purged applied marker for {key}"));
                    }
                }
            }
        }

        Ok((purged, cleaned))
    }

    /// Low-level GET that does NOT filter delete markers.  Used
    /// internally so the marker-aware layer can read marker payloads.
    async fn get_raw(&self, location: &Path) -> object_store::Result<GetResult> {
        let key = location.to_string();
        let (shard_ids, is_fallback) = self.resolve_shards(&key);
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            shard_ids.iter().map(|&id| (id, shards[id].store.clone())).collect()
        };
        let mut last_err = None;
        for (shard_id, store) in &stores {
            match store.get(location).await {
                Ok(result) => {
                    if is_fallback {
                        let size = match store.head(location).await {
                            Ok(meta) => meta.size,
                            Err(_) => 0,
                        };
                        self.catalog.add_replica(&key, *shard_id, size, 0);
                    }
                    return Ok(result);
                }
                Err(e) => { last_err = Some(e); }
            }
        }
        Err(last_err.unwrap_or_else(|| Self::not_found_err(location)))
    }

    /// Low-level HEAD that does NOT filter delete markers.
    async fn head_raw(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let key = location.to_string();
        let (shard_ids, is_fallback) = self.resolve_shards(&key);
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            shard_ids.iter().map(|&id| (id, shards[id].store.clone())).collect()
        };
        let mut last_err = None;
        for (shard_id, store) in &stores {
            match store.head(location).await {
                Ok(meta) => {
                    if is_fallback { self.catalog.add_replica(&key, *shard_id, meta.size, 0); }
                    return Ok(meta);
                }
                Err(e) => { last_err = Some(e); }
            }
        }
        Err(last_err.unwrap_or_else(|| Self::not_found_err(location)))
    }

    /// Low-level DELETE that does NOT create a marker.  Used for
    /// removing markers themselves and for vacuum cleanup.
    async fn delete_raw(&self, location: &Path) -> object_store::Result<()> {
        let key = location.to_string();
        let entry = self.catalog.get(&key);

        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            let shard_ids: Vec<ShardId> = match &entry {
                Some(e) => e.shards.iter()
                    .copied()
                    .filter(|&id| id < shards.len())
                    .filter(|&id| !shards[id].health.is_unavailable())
                    .collect(),
                None => (0..shards.len())
                    .filter(|&id| !shards[id].health.is_unavailable())
                    .collect(),
            };
            shard_ids.iter()
                .map(|&id| (id, shards[id].store.clone()))
                .collect()
        };

        let raw_refs = self.raw_refs();
        let futs: Vec<_> = stores
            .into_iter()
            .map(|(shard_id, store)| {
                let loc = location.clone();
                let refs = raw_refs.clone();
                async move {
                    crate::metadata::cleanup_sidecar_maybe(
                        store.as_ref(), &loc, refs.as_deref(), shard_id,
                    ).await;
                    (shard_id, store.delete(&loc).await)
                }
            })
            .collect();
        let outcomes = futures::future::join_all(futs).await;

        let mut last_err = None;
        // NOTE: `succeeded` includes shards that returned NotFound --
        // from the delete perspective, the object is gone either way.
        let mut succeeded: Vec<ShardId> = Vec::new();
        let mut failed: Vec<ShardId> = Vec::new();
        for (shard_id, outcome) in outcomes {
            match outcome {
                Ok(()) => succeeded.push(shard_id),
                Err(e) => {
                    if matches!(e, object_store::Error::NotFound { .. }) {
                        succeeded.push(shard_id);
                    } else {
                        failed.push(shard_id);
                        last_err = Some(e);
                    }
                }
            }
        }

        if failed.is_empty() {
            self.catalog.remove(&key);
            Ok(())
        } else if self.delete_requires_min_writes {
            // Strict mode: require min_writes quorum for deletes.
            if succeeded.len() >= self.min_writes {
                for &shard_id in &succeeded {
                    self.catalog.remove_shard(&key, shard_id);
                }
                Ok(())
            } else {
                for &shard_id in &succeeded {
                    self.catalog.remove_shard(&key, shard_id);
                }
                Err(last_err.unwrap_or_else(|| object_store::Error::Generic {
                    store: "ShardedObjectStore",
                    source: Box::from("all target shards failed to delete"),
                }))
            }
        } else {
            // Best-effort: succeed if at least one shard deleted.
            for &shard_id in &succeeded {
                self.catalog.remove_shard(&key, shard_id);
            }
            if succeeded.is_empty() {
                Err(last_err.unwrap_or_else(|| object_store::Error::Generic {
                    store: "ShardedObjectStore",
                    source: Box::from("all target shards failed to delete"),
                }))
            } else {
                if !failed.is_empty() {
                    warn!(
                        key = %key,
                        failed_shards = ?failed,
                        succeeded_shards = ?succeeded,
                        "partial delete: object removed from catalog but may still \
                         exist on failed shards; rebuild_catalog will re-discover it"
                    );
                }
                Ok(())
            }
        }
    }
}

// -- ObjectStore trait impl ------------------------------------------

impl std::fmt::Display for ShardedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardedObjectStore({} shards)", self.shards.read().len())
    }
}

impl std::fmt::Debug for ShardedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedObjectStore")
            .field("shard_count", &self.shards.read().len())
            .field("replication_factor", &self.replication.factor())
            .finish()
    }
}

/// Shared read-with-fallback logic for get/get_opts/get_range.
///
/// Handles: delete-marker hiding, shard resolution, CRC error tracking,
/// fallback catalog repair, and read-repair scheduling.
macro_rules! read_with_fallback {
    ($self:ident, $location:expr, |$store:ident| $op:expr) => {{
        let key = $location.to_string();
        if Self::is_delete_marker(&key) {
            return Err(Self::not_found_err($location));
        }
        let (shard_ids, is_fallback) = $self.resolve_shards(&key);
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = $self.shards.read();
            shard_ids
                .iter()
                .map(|&id| (id, shards[id].store.clone()))
                .collect()
        };
        let mut last_err = None;
        let mut corrupted_shards: Vec<ShardId> = Vec::new();
        let fallback_refs = if is_fallback { $self.raw_refs() } else { None };
        for (shard_id, $store) in &stores {
            match ($op).await {
                Ok(result) => {
                    if is_fallback {
                        // Recover size and meta_len.  For Raw shards
                        // use head_with_meta so the catalog captures
                        // the correct meta_len instead of defaulting
                        // to 0 (which would hide metadata until a
                        // rebuild_catalog).
                        let raw_hit = fallback_refs.as_ref()
                            .and_then(|refs| {
                                refs.get(*shard_id).and_then(|raw| {
                                    raw.head_with_meta($location).ok()
                                })
                            });
                        let (size, ml) = if let Some((meta, ml)) = raw_hit {
                            (meta.size, ml)
                        } else {
                            // Non-raw shard or no raw_refs: async head,
                            // meta_len stays 0 (metadata lives in S3
                            // attributes or sidecar, not in body).
                            let size = match $store.head($location).await {
                                Ok(meta) => meta.size,
                                Err(_) => 0,
                            };
                            (size, 0u16)
                        };
                        $self.catalog.add_replica(&key, *shard_id, size, ml);
                    }
                    for bad in &corrupted_shards {
                        $self.schedule_read_repair(key.clone(), *shard_id, *bad);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    let is_crc = $self.handle_read_error(*shard_id, &key, &e);
                    if is_crc {
                        corrupted_shards.push(*shard_id);
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Self::not_found_err($location)))
    }};
}

#[async_trait]
impl ObjectStore for ShardedObjectStore {
    /// Write an object, replicating to N shards.
    ///
    /// If all initial targets fail, marks them offline and retries with
    /// freshly selected healthy shards.
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        if self.read_only {
            return Err(Self::read_only_err());
        }
        let targets = self.select_targets(location);
        if targets.is_empty() {
            return Err(object_store::Error::Generic {
                store: "ShardedObjectStore",
                source: Box::new(ShardError::NoShards),
            });
        }

        let data: Bytes = payload.into();
        let size = data.len() as u64;
        let crc = crc32c::crc32c(&data);

        match self.write_to_shards(location, data.clone(), &targets).await {
            Ok((result, placed)) => {
                self.catalog.put(location.to_string(), placed, size, Some(crc), 0);
                self.emit_event(rawobjstr::event::StoreEvent::Put { key: location.to_string() });
                // Best-effort: remove any stale delete marker for this key.
                let _ = self.remove_delete_marker(&location.to_string()).await;
                Ok(result)
            }
            Err(first_err) => {
                // InsufficientWrites means some writes landed; do NOT
                // offline the successful shards or retry.
                if let object_store::Error::Generic { ref source, .. } = first_err {
                    if source.downcast_ref::<ShardError>().map_or(false, |se| {
                        matches!(se, ShardError::InsufficientWrites { .. })
                    }) {
                        return Err(first_err);
                    }
                }
                // Mark all failed targets offline and retry.
                for &sid in &targets {
                    self.set_shard_health(sid, ShardHealth::Offline);
                }
                let retry_targets = self.select_targets(location);
                if retry_targets.is_empty() || retry_targets == targets {
                    return Err(object_store::Error::Generic {
                        store: "ShardedObjectStore",
                        source: Box::new(ShardError::NoShards),
                    });
                }
                let (result, placed) =
                    self.write_to_shards(location, data, &retry_targets).await?;
                self.catalog.put(location.to_string(), placed, size, Some(crc), 0);
                self.emit_event(rawobjstr::event::StoreEvent::Put { key: location.to_string() });
                // Best-effort: remove any stale delete marker for this key.
                let _ = self.remove_delete_marker(&location.to_string()).await;
                Ok(result)
            }
        }
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.read_only {
            return Err(Self::read_only_err());
        }
        match opts.mode {
            PutMode::Create => {
                // Atomic insert: try_insert guarantees only one caller wins.
                let targets = self.select_targets(location);
                if targets.is_empty() {
                    return Err(object_store::Error::Generic {
                        store: "ShardedObjectStore",
                        source: Box::new(ShardError::NoShards),
                    });
                }

                let data: Bytes = payload.into();
                let size = data.len() as u64;

                if !self.catalog.try_insert(location.to_string(), targets.clone(), size) {
                    return Err(object_store::Error::AlreadyExists {
                        path: location.to_string(),
                        source: "key already exists in catalog".into(),
                    });
                }

                let crc = crc32c::crc32c(&data);
                match self.write_to_shards(location, data.clone(), &targets).await {
                    Ok((result, placed)) => {
                        self.catalog.put(location.to_string(), placed, size, Some(crc), 0);
                        self.emit_event(rawobjstr::event::StoreEvent::Put { key: location.to_string() });
                        // Best-effort: remove any stale delete marker.
                        let _ = self.remove_delete_marker(&location.to_string()).await;
                        Ok(result)
                    }
                    Err(first_err) => {
                        // InsufficientWrites means some writes landed; do NOT
                        // offline the successful shards or retry.
                        if let object_store::Error::Generic { ref source, .. } = first_err {
                            if source.downcast_ref::<ShardError>().map_or(false, |se| {
                                matches!(se, ShardError::InsufficientWrites { .. })
                            }) {
                                // Clean up the placeholder catalog entry.
                                if let Some(entry) = self.catalog.get(&location.to_string()) {
                                    if entry.crc32c.is_none() {
                                        self.catalog.remove(&location.to_string());
                                    }
                                }
                                return Err(first_err);
                            }
                        }
                        // Mark failed targets offline and retry.
                        for &sid in &targets {
                            self.set_shard_health(sid, ShardHealth::Offline);
                        }
                        let retry_targets = self.select_targets(location);
                        if retry_targets.is_empty() || retry_targets == targets {
                            // Only remove our placeholder (crc32c=None), not
                            // a real entry written by a concurrent put().
                            if let Some(entry) = self.catalog.get(&location.to_string()) {
                                if entry.crc32c.is_none() {
                                    self.catalog.remove(&location.to_string());
                                }
                            }
                            return Err(object_store::Error::Generic {
                                store: "ShardedObjectStore",
                                source: Box::new(ShardError::NoShards),
                            });
                        }
                        match self.write_to_shards(location, data, &retry_targets).await {
                            Ok((result, placed)) => {
                                self.catalog.put(location.to_string(), placed, size, Some(crc), 0);
                                self.emit_event(rawobjstr::event::StoreEvent::Put { key: location.to_string() });
                                // Best-effort: remove any stale delete marker.
                                let _ = self.remove_delete_marker(&location.to_string()).await;
                                Ok(result)
                            }
                            Err(e) => {
                                // Only remove our placeholder (crc32c=None), not
                                // a real entry written by a concurrent put().
                                if let Some(entry) = self.catalog.get(&location.to_string()) {
                                    if entry.crc32c.is_none() {
                                        self.catalog.remove(&location.to_string());
                                    }
                                }
                                Err(e)
                            }
                        }
                    }
                }
            }
            PutMode::Overwrite => self.put(location, payload).await,
            PutMode::Update(ref update) => {
                // The sharded store does not generate e_tags or versions.
                if update.e_tag.is_some() || update.version.is_some() {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: "e_tag/version preconditions not supported".into(),
                    });
                }
                self.put(location, payload).await
            }
        }
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default()).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.read_only {
            return Err(Self::read_only_err());
        }
        let targets = self.select_targets(location);
        if targets.is_empty() {
            return Err(object_store::Error::Generic {
                store: "ShardedObjectStore",
                source: Box::new(ShardError::NoShards),
            });
        }

        // Clone stores under lock.
        let (primary_id, primary_store, replica_stores) = {
            let shards = self.shards.read();
            let primary_id = targets[0];
            let primary_store = shards[primary_id].store.clone();
            let replica_stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = targets[1..]
                .iter()
                .filter_map(|&id| shards.get(id).map(|s| (id, s.store.clone())))
                .collect();
            (primary_id, primary_store, replica_stores)
        };

        // Primary shard owns the multipart upload (spools parts to disk).
        // If the primary fails to start, mark it offline and retry.
        match primary_store.put_multipart_opts(location, opts.clone()).await {
            Ok(primary_upload) => {
                let tid = self.register_multipart(location, primary_id);
                Ok(Box::new(ShardedMultipartUpload {
                    location: location.clone(),
                    primary_upload,
                    primary_shard_id: primary_id,
                    primary_store,
                    replica_stores,
                    catalog: Arc::clone(&self.catalog),
                    event_bus: self.event_bus.lock().clone(),
                    tracking_id: tid,
                    multipart_registry: Arc::clone(&self.multipart_uploads),
                    min_writes: self.min_writes,
                    raw_refs: self.raw_refs(),
                }))
            }
            Err(_first_err) => {
                self.set_shard_health(primary_id, ShardHealth::Offline);
                let retry_targets = self.select_targets(location);
                if retry_targets.is_empty() {
                    return Err(object_store::Error::Generic {
                        store: "ShardedObjectStore",
                        source: Box::new(ShardError::NoShards),
                    });
                }
                let (retry_primary_id, retry_primary_store, retry_replicas) = {
                    let shards = self.shards.read();
                    let pid = retry_targets[0];
                    let ps = shards[pid].store.clone();
                    let rs: Vec<(ShardId, Arc<dyn ObjectStore>)> = retry_targets[1..]
                        .iter()
                        .filter_map(|&id| shards.get(id).map(|s| (id, s.store.clone())))
                        .collect();
                    (pid, ps, rs)
                };
                let primary_upload = retry_primary_store
                    .put_multipart_opts(location, opts)
                    .await?;
                let tid = self.register_multipart(location, retry_primary_id);
                Ok(Box::new(ShardedMultipartUpload {
                    location: location.clone(),
                    primary_upload,
                    primary_shard_id: retry_primary_id,
                    primary_store: retry_primary_store,
                    replica_stores: retry_replicas,
                    catalog: Arc::clone(&self.catalog),
                    event_bus: self.event_bus.lock().clone(),
                    tracking_id: tid,
                    multipart_registry: Arc::clone(&self.multipart_uploads),
                    min_writes: self.min_writes,
                    raw_refs: self.raw_refs(),
                }))
            }
        }
    }

    /// Read an object -- load-balances across replicas, falls back on error.
    /// On CRC corruption, tries the next replica and schedules a background
    /// read repair to overwrite the corrupt copy from a healthy one.
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        read_with_fallback!(self, location, |store| store.get(location))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        read_with_fallback!(self, location, |store| store.get_opts(location, options.clone()))
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<u64>) -> object_store::Result<Bytes> {
        read_with_fallback!(self, location, |store| store.get_range(location, range.clone()))
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let key = location.to_string();
        // Hide delete markers from external callers.
        if Self::is_delete_marker(&key) {
            return Err(Self::not_found_err(location));
        }
        let (shard_ids, is_fallback) = self.resolve_shards(&key);
        let stores: Vec<(ShardId, Arc<dyn ObjectStore>)> = {
            let shards = self.shards.read();
            shard_ids.iter().map(|&id| (id, shards[id].store.clone())).collect()
        };
        let fallback_refs = if is_fallback { self.raw_refs() } else { None };
        let mut last_err = None;
        for (shard_id, store) in &stores {
            match store.head(location).await {
                Ok(meta) => {
                    if is_fallback {
                        // For Raw shards, recover meta_len via the
                        // synchronous index lookup so the catalog
                        // entry is correct from the start.
                        let ml = fallback_refs.as_ref()
                            .and_then(|refs| {
                                refs.get(*shard_id).and_then(|raw| {
                                    raw.head_with_meta(location).ok()
                                        .map(|(_, ml)| ml)
                                })
                            })
                            .unwrap_or(0u16);
                        self.catalog.add_replica(&key, *shard_id, meta.size, ml);
                    }
                    return Ok(meta);
                }
                Err(e) => { last_err = Some(e); }
            }
        }
        Err(last_err.unwrap_or_else(|| Self::not_found_err(location)))
    }

    /// Delete from all shards that hold the object.
    ///
    /// First writes a delete marker (`__deleted__/<key>`) containing the
    /// deletion timestamp, then removes the real object.  The marker
    /// prevents resurrection during recovery sync on offline shards.
    /// If the real delete fails the marker is retained (not rolled
    /// back) so that recovery sync can still prevent resurrection.
    /// Stale markers are cleaned by vacuum or by a subsequent PUT.
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        if self.read_only {
            return Err(Self::read_only_err());
        }
        let key = location.to_string();

        // Skip marker creation for internal marker keys (avoid recursion).
        if Self::is_delete_marker(&key) {
            return self.delete_raw(location).await;
        }

        // Step 1: write the delete marker.
        if let Err(e) = self.put_delete_marker(&key).await {
            error!(key, error = %e, "failed to write delete marker");
            return Err(e);
        }

        // Step 2: delete the actual object from all shards.
        match self.delete_raw(location).await {
            Ok(()) => {
                self.emit_event(rawobjstr::event::StoreEvent::Delete {
                    key,
                });
                Ok(())
            }
            Err(e) => {
                // Do NOT roll back the marker.  A stale marker is safe
                // (vacuum or a subsequent PUT cleans it up), but a
                // missing marker after partial deletion lets a
                // returning shard resurrect the object.
                Err(e)
            }
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // Return from catalog -- deduplicated across shards.
        // Filter out delete markers so they are invisible to clients.
        let entries = self.catalog.list(prefix.map(|p| p.as_ref()));
        let stream = futures::stream::iter(
            entries.into_iter()
                .filter(|m| !Self::is_delete_marker(m.location.as_ref()))
                .map(Ok),
        );
        Box::pin(stream)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let all: Vec<ObjectMeta> = self
            .list(prefix)
            .try_collect()
            .await?;

        let prefix_str = prefix.map(|p| p.as_ref()).unwrap_or("");
        let mut common_prefixes = std::collections::BTreeSet::new();
        let mut objects = Vec::new();

        for meta in all {
            let path_str = meta.location.as_ref();
            let relative = path_str
                .strip_prefix(prefix_str)
                .unwrap_or(path_str)
                .trim_start_matches('/');

            if let Some(slash_pos) = relative.find('/') {
                let dir = if prefix_str.is_empty() {
                    format!("{}/", &relative[..slash_pos])
                } else {
                    let base = prefix_str.trim_end_matches('/');
                    format!("{}/{}/", base, &relative[..slash_pos])
                };
                common_prefixes.insert(dir);
            } else {
                objects.push(meta);
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().map(|p| Path::from(p.as_str())).collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        if self.read_only {
            return Err(Self::read_only_err());
        }
        // Read source, capturing S3 attributes for metadata preservation.
        let get_result = self.get(from).await?;
        let attributes = get_result.attributes.clone();
        let data = get_result.bytes().await?;

        // Try to read metadata from the source object.
        let meta_bytes = self.read_object_metadata(from, &attributes).await;

        if !meta_bytes.is_empty() {
            if let Some(ref refs) = self.raw_refs() {
                crate::metadata::put_with_meta(
                    self, refs, to, data, &meta_bytes,
                ).await.map_err(|e| object_store::Error::Generic {
                    store: "ShardedObjectStore",
                    source: Box::new(e),
                })?;
                return Ok(());
            }
        }
        self.put(to, PutPayload::from(data)).await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        if self.read_only {
            return Err(Self::read_only_err());
        }
        // Check destination first to fail fast.
        if self.catalog.get(&to.to_string()).is_some() {
            return Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "object already exists".into(),
            });
        }

        // Read source, capturing S3 attributes for metadata preservation.
        let get_result = self.get(from).await?;
        let attributes = get_result.attributes.clone();
        let data = get_result.bytes().await?;
        let size = data.len() as u64;

        let targets = self.select_targets(to);
        if !self.catalog.try_insert(to.to_string(), targets.clone(), size) {
            return Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "object already exists".into(),
            });
        }

        // Try to read metadata from the source object.
        let meta_bytes = self.read_object_metadata(from, &attributes).await;

        if !meta_bytes.is_empty() {
            if let Some(ref refs) = self.raw_refs() {
                match crate::metadata::put_with_meta(
                    self, refs, to, data, &meta_bytes,
                ).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        self.catalog.remove(&to.to_string());
                        return Err(object_store::Error::Generic {
                            store: "ShardedObjectStore",
                            source: Box::new(e),
                        });
                    }
                }
            }
        }

        let crc = crc32c::crc32c(&data);

        // Write directly to shards (bypassing put() to avoid catalog overwrite).
        match self.write_to_shards(to, data, &targets).await {
            Ok((_, placed)) => {
                // Always update catalog with actual placement + CRC.
                self.catalog.put(to.to_string(), placed, size, Some(crc), 0);
                Ok(())
            }
            Err(e) => {
                self.catalog.remove(&to.to_string());
                Err(e)
            }
        }
    }

    /// Rename by copy-then-delete.  This is NOT atomic: if the source
    /// delete fails after a successful copy, the object will exist at
    /// both paths.  A warning is logged in that case.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_if_not_exists(from, to).await?;
        if let Err(e) = self.delete(from).await {
            tracing::warn!(
                "rename_if_not_exists: source delete of '{}' failed: {}; object now exists at both '{}' and '{}'",
                from, e, from, to
            );
            return Err(e);
        }
        Ok(())
    }
}

// -- Sharded multipart upload ----------------------------------------

/// Multipart upload that delegates part storage to the primary shard's
/// native `MultipartUpload` (which spools parts to disk, not RAM).
/// On `complete()`, the primary shard assembles the object, then we
/// read it back and replicate to the remaining target shards.
struct ShardedMultipartUpload {
    location: Path,
    /// The primary shard's native multipart upload handle.
    primary_upload: Box<dyn MultipartUpload>,
    /// Primary shard ID for catalog tracking.
    primary_shard_id: ShardId,
    /// Primary shard store -- used to read the assembled object back.
    primary_store: Arc<dyn ObjectStore>,
    /// Remaining (non-primary) target stores for replication.
    replica_stores: Vec<(ShardId, Arc<dyn ObjectStore>)>,
    catalog: Arc<Catalog>,
    /// Optional event bus for emitting PUT events on complete().
    event_bus: Option<Arc<rawobjstr::event::EventBus>>,
    /// Tracking ID in the parent's multipart registry.
    tracking_id: u64,
    /// Reference to the parent's multipart registry for deregistration.
    multipart_registry: Arc<parking_lot::Mutex<HashMap<u64, TrackedUpload>>>,
    /// Minimum successful writes (including primary) before complete returns Ok.
    min_writes: usize,
    /// Optional raw-ref registry for metadata-aware replication.
    raw_refs: Option<Arc<crate::metadata::RawRefRegistry>>,
}

impl std::fmt::Debug for ShardedMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedMultipartUpload")
            .field("location", &self.location)
            .field("primary_shard", &self.primary_shard_id)
            .field("replicas", &self.replica_stores.len())
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for ShardedMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        // Delegate directly to the primary shard's multipart --
        // parts are spooled to disk, not held in memory.
        self.primary_upload.put_part(payload)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        // 1. Complete the primary shard's multipart (assembles on disk).
        let primary_result = self.primary_upload.complete().await?;

        // Deregister from the multipart tracking registry immediately
        // after the primary commit succeeds.  This MUST happen before
        // replication so that a later purge_stale_multiparts() does
        // not delete committed data if replication fails.
        self.multipart_registry.lock().remove(&self.tracking_id);

        let mut placed = vec![self.primary_shard_id];

        // 2. If there are replica targets, read the assembled object
        //    back from the primary shard and write to each replica.
        if !self.replica_stores.is_empty() {
            let get_result = self.primary_store.get(&self.location).await?;
            let attributes = get_result.attributes.clone();
            let data = get_result.bytes().await?;
            let size = data.len() as u64;
            let crc = crc32c::crc32c(&data);

            // Read metadata from the primary shard so replicas get it too.
            let meta_bytes: Vec<u8> = if let Some(ref refs) = self.raw_refs {
                crate::metadata::read_metadata_from_shard(
                    self.primary_store.as_ref(), self.primary_shard_id,
                    &self.location, &attributes, refs,
                ).await.unwrap_or_default()
            } else {
                Vec::new()
            };
            let meta_len = meta_bytes.len() as u16;

            let futs: Vec<_> = self.replica_stores
                .iter()
                .map(|(shard_id, store)| {
                    let store = store.clone();
                    let loc = self.location.clone();
                    let payload_data = data.clone();
                    let sid = *shard_id;
                    let meta = meta_bytes.clone();
                    let refs = self.raw_refs.clone();
                    async move {
                        if !meta.is_empty() {
                            if let Some(ref refs) = refs {
                                return (sid, repair::write_with_meta_to_shard(
                                    refs, sid, &store, &loc, payload_data, &meta,
                                ).await.map_err(|e| object_store::Error::Generic {
                                    store: "multipart_replicate",
                                    source: e,
                                }));
                            }
                        }
                        (sid, store.put(&loc, PutPayload::from(payload_data)).await.map(|_| ()))
                    }
                })
                .collect();
            let outcomes = futures::future::join_all(futs).await;

            let mut errors: Vec<String> = Vec::new();
            for (shard_id, outcome) in outcomes {
                match outcome {
                    Ok(_) => { placed.push(shard_id); }
                    Err(e) => { errors.push(format!("shard {shard_id}: {e}")); }
                }
            }

            if !errors.is_empty() {
                tracing::warn!(
                    path = %self.location,
                    placed = placed.len(),
                    failed = errors.len(),
                    "multipart complete: replica replication partially failed: {}",
                    errors.join("; ")
                );
            }

            // Enforce min_writes: enough replicas must have landed.
            if placed.len() < self.min_writes {
                // Clean up orphaned data from shards that did succeed
                // (including the primary).  Without this, data sits on
                // disk with no catalog entry -- unreachable and wasting
                // space.  This mirrors write_to_shards_inner().
                for (shard_id, store) in self.replica_stores.iter() {
                    if placed.contains(shard_id) {
                        let _ = store.delete(&self.location).await;
                    }
                }
                // Always delete from primary (it always succeeded).
                let _ = self.primary_store.delete(&self.location).await;

                return Err(object_store::Error::Generic {
                    store: "ShardedObjectStore",
                    source: Box::new(ShardError::InsufficientWrites {
                        path: self.location.to_string(),
                        required: self.min_writes,
                        actual: placed.len(),
                        errors,
                    }),
                });
            }

            self.catalog.put(self.location.to_string(), placed, size, Some(crc), meta_len);
            if let Some(ref bus) = self.event_bus {
                bus.emit(rawobjstr::event::StoreEvent::Put { key: self.location.to_string() });
            }
        } else {
            // Single-shard case: compute size + CRC without buffering
            // the full object.  Stream through the data in chunks.
            let get_result = self.primary_store.get(&self.location).await?;
            let size = get_result.meta.size as u64;
            let mut stream = get_result.into_stream();
            let mut crc: u32 = 0;
            while let Some(chunk) = stream.try_next().await? {
                crc = crc32c::crc32c_append(crc, &chunk);
            }

            // Read metadata from the primary shard for catalog tracking.
            let single_meta_len: u16 = if let Some(ref refs) = self.raw_refs {
                match refs.kind(self.primary_shard_id) {
                    crate::metadata::ShardKind::Raw => {
                        refs.get(self.primary_shard_id)
                            .and_then(|raw| raw.get_metadata(&self.location).ok())
                            .map(|b| b.len() as u16)
                            .unwrap_or(0)
                    }
                    _ => 0,
                }
            } else {
                0
            };
            self.catalog.put(self.location.to_string(), placed, size, Some(crc), single_meta_len);
            if let Some(ref bus) = self.event_bus {
                bus.emit(rawobjstr::event::StoreEvent::Put { key: self.location.to_string() });
            }
        }

        Ok(primary_result)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.multipart_registry.lock().remove(&self.tracking_id);
        self.primary_upload.abort().await
    }
}
