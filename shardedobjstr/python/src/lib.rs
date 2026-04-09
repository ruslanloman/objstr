use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, MultipartUpload, ObjectStore, PutPayload};
use parking_lot::RwLock;
use pyo3::exceptions::{PyFileExistsError, PyFileNotFoundError, PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rawobjstr::store::{FormatOptions, OpenMode, RawObjectStore};
use rawobjstr::{Compression, RawStoreError};
use tokio::runtime::Runtime;

use shardedobjstr::{ReadPreference, ShardedObjectStore};
use shardedobjstr::metadata::{
    RawRefRegistry, ShardKind,
    put_with_meta, put_with_meta_from_file, head_with_meta, get_metadata,
    list_with_meta, set_meta_len,
};

// -- Error conversion --------------------------------------------------------

fn raw_err(e: RawStoreError) -> PyErr {
    match &e {
        RawStoreError::NotFound(_) => PyFileNotFoundError::new_err(e.to_string()),
        RawStoreError::AlreadyExists(_) => PyFileExistsError::new_err(e.to_string()),
        RawStoreError::NoSpace { .. } => PyIOError::new_err(e.to_string()),
        RawStoreError::ShardOverflow { .. } => PyIOError::new_err(e.to_string()),
        RawStoreError::NotFormatted => PyValueError::new_err(e.to_string()),
        RawStoreError::SuperblockCorrupt => PyValueError::new_err(e.to_string()),
        RawStoreError::IndexCorrupt => PyValueError::new_err(e.to_string()),
        _ => PyIOError::new_err(e.to_string()),
    }
}

fn obj_err(e: object_store::Error) -> PyErr {
    match &e {
        object_store::Error::NotFound { .. } => PyFileNotFoundError::new_err(e.to_string()),
        object_store::Error::AlreadyExists { .. } => PyFileExistsError::new_err(e.to_string()),
        _ => PyIOError::new_err(e.to_string()),
    }
}

fn io_err(e: std::io::Error) -> PyErr {
    PyIOError::new_err(e.to_string())
}

// -- Shared data classes -----------------------------------------------------

#[pyclass(frozen)]
#[derive(Clone)]
struct ObjectMeta {
    #[pyo3(get)]
    location: String,
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    last_modified: String,
    #[pyo3(get)]
    e_tag: Option<String>,
}

#[pymethods]
impl ObjectMeta {
    fn __repr__(&self) -> String {
        format!("ObjectMeta(location={:?}, size={})", self.location, self.size)
    }
}

fn convert_meta(m: &object_store::ObjectMeta) -> ObjectMeta {
    ObjectMeta {
        location: m.location.to_string(),
        size: m.size,
        last_modified: m.last_modified.to_rfc3339(),
        e_tag: m.e_tag.clone(),
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct ListResult {
    #[pyo3(get)]
    objects: Vec<ObjectMeta>,
    #[pyo3(get)]
    common_prefixes: Vec<String>,
}

// -- Placement info ----------------------------------------------------------

#[pyclass(frozen)]
#[derive(Clone)]
struct PlacementInfo {
    #[pyo3(get)]
    shards: Vec<usize>,
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    crc32c: Option<u32>,
    #[pyo3(get)]
    updated: String,
}

#[pymethods]
impl PlacementInfo {
    fn __repr__(&self) -> String {
        format!(
            "PlacementInfo(shards={:?}, size={})",
            self.shards, self.size
        )
    }
}

// -- Shard health ------------------------------------------------------------

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
enum ShardHealth {
    Healthy,
    Degraded,
    Offline,
    Syncing,
    Detached,
}

fn convert_health(h: shardedobjstr::ShardHealth) -> ShardHealth {
    match h {
        shardedobjstr::ShardHealth::Healthy => ShardHealth::Healthy,
        shardedobjstr::ShardHealth::Degraded => ShardHealth::Degraded,
        shardedobjstr::ShardHealth::Offline => ShardHealth::Offline,
        shardedobjstr::ShardHealth::Syncing => ShardHealth::Syncing,
        shardedobjstr::ShardHealth::Detached => ShardHealth::Detached,
    }
}

fn to_rust_health(h: &ShardHealth) -> shardedobjstr::ShardHealth {
    match h {
        ShardHealth::Healthy => shardedobjstr::ShardHealth::Healthy,
        ShardHealth::Degraded => shardedobjstr::ShardHealth::Degraded,
        ShardHealth::Offline => shardedobjstr::ShardHealth::Offline,
        ShardHealth::Syncing => shardedobjstr::ShardHealth::Syncing,
        ShardHealth::Detached => shardedobjstr::ShardHealth::Detached,
    }
}

// -- Invalidate report -------------------------------------------------------

#[pyclass(frozen)]
#[derive(Clone)]
struct InvalidateReport {
    #[pyo3(get)]
    shard_id: usize,
    #[pyo3(get)]
    entries_purged: usize,
    #[pyo3(get)]
    entries_restored: usize,
    #[pyo3(get)]
    missing_keys: Vec<String>,
    #[pyo3(get)]
    scan_ok: bool,
}

#[pymethods]
impl InvalidateReport {
    fn __repr__(&self) -> String {
        format!(
            "InvalidateReport(shard={}, purged={}, restored={}, missing={}, ok={})",
            self.shard_id, self.entries_purged, self.entries_restored,
            self.missing_keys.len(), self.scan_ok
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct RepairReplicationResult {
    #[pyo3(get)]
    re_replicated: usize,
    #[pyo3(get)]
    trimmed: usize,
    #[pyo3(get)]
    under_remaining: usize,
    #[pyo3(get)]
    over_remaining: usize,
}

#[pymethods]
impl RepairReplicationResult {
    fn __repr__(&self) -> String {
        format!(
            "RepairReplicationResult(re_replicated={}, trimmed={}, under_remaining={}, over_remaining={})",
            self.re_replicated, self.trimmed, self.under_remaining, self.over_remaining
        )
    }
}

/// Result of a redistribute sweep.
#[pyclass(frozen)]
#[derive(Clone)]
struct RedistributeResult {
    #[pyo3(get)]
    moved: usize,
    #[pyo3(get)]
    skipped: usize,
    #[pyo3(get)]
    errors: usize,
    /// List of (shard_id, count) tuples after the sweep.
    #[pyo3(get)]
    shard_counts: Vec<(usize, usize)>,
}

#[pymethods]
impl RedistributeResult {
    fn __repr__(&self) -> String {
        format!(
            "RedistributeResult(moved={}, skipped={}, errors={}, shards={})",
            self.moved, self.skipped, self.errors, self.shard_counts.len()
        )
    }
}

// -- Verify report types -----------------------------------------------------

#[pyclass(frozen)]
#[derive(Clone)]
struct VerifyReplicaResult {
    #[pyo3(get)]
    shard_id: usize,
    #[pyo3(get)]
    crc32c: Option<u32>,
    #[pyo3(get)]
    size: Option<u64>,
    #[pyo3(get)]
    matches_catalog: Option<bool>,
    #[pyo3(get)]
    error: Option<String>,
}

#[pymethods]
impl VerifyReplicaResult {
    fn __repr__(&self) -> String {
        format!(
            "VerifyReplicaResult(shard={}, crc={:?}, ok={:?})",
            self.shard_id, self.crc32c, self.matches_catalog
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct VerifyObjectReport {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    catalog_crc: Option<u32>,
    #[pyo3(get)]
    replicas: Vec<VerifyReplicaResult>,
    #[pyo3(get)]
    replicas_consistent: bool,
}

#[pymethods]
impl VerifyObjectReport {
    fn __repr__(&self) -> String {
        format!(
            "VerifyObjectReport(key={:?}, consistent={}, replicas={})",
            self.key, self.replicas_consistent, self.replicas.len()
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct VerifyReport {
    #[pyo3(get)]
    objects_checked: usize,
    #[pyo3(get)]
    objects_ok: usize,
    #[pyo3(get)]
    objects_mismatched: usize,
    #[pyo3(get)]
    objects_with_errors: usize,
    #[pyo3(get)]
    details: Vec<VerifyObjectReport>,
}

#[pymethods]
impl VerifyReport {
    fn __repr__(&self) -> String {
        format!(
            "VerifyReport(checked={}, ok={}, mismatched={}, errors={})",
            self.objects_checked, self.objects_ok,
            self.objects_mismatched, self.objects_with_errors
        )
    }
}

// -- Cross-verify report types -----------------------------------------------

#[pyclass(frozen)]
#[derive(Clone)]
struct CrossVerifyShardDigest {
    #[pyo3(get)]
    shard_id: usize,
    #[pyo3(get)]
    md5_hex: String,
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    last_modified: String,
}

#[pymethods]
impl CrossVerifyShardDigest {
    fn __repr__(&self) -> String {
        format!(
            "CrossVerifyShardDigest(shard={}, md5={:?}, size={})",
            self.shard_id, self.md5_hex, self.size
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct CrossVerifyObjectReport {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    catalog_crc: Option<u32>,
    #[pyo3(get)]
    shards: Vec<CrossVerifyShardDigest>,
    #[pyo3(get)]
    consistent: bool,
    #[pyo3(get)]
    errors: Vec<(usize, String)>,
}

#[pymethods]
impl CrossVerifyObjectReport {
    fn __repr__(&self) -> String {
        format!(
            "CrossVerifyObjectReport(key={:?}, consistent={}, shards={}, errors={})",
            self.key, self.consistent, self.shards.len(), self.errors.len()
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct CrossVerifyReport {
    #[pyo3(get)]
    objects_checked: u64,
    #[pyo3(get)]
    objects_ok: u64,
    #[pyo3(get)]
    objects_mismatched: u64,
    #[pyo3(get)]
    objects_with_errors: u64,
    #[pyo3(get)]
    objects_skipped_single_replica: u64,
    #[pyo3(get)]
    details: Vec<CrossVerifyObjectReport>,
}

#[pymethods]
impl CrossVerifyReport {
    fn __repr__(&self) -> String {
        format!(
            "CrossVerifyReport(checked={}, ok={}, mismatched={}, errors={}, skipped={})",
            self.objects_checked, self.objects_ok,
            self.objects_mismatched, self.objects_with_errors,
            self.objects_skipped_single_replica
        )
    }
}

// -- Repair-replication plan types -------------------------------------------

#[pyclass(frozen)]
#[derive(Clone)]
struct PlannedAction {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    current_count: usize,
    #[pyo3(get)]
    target_rf: usize,
    #[pyo3(get)]
    action_type: String,
    #[pyo3(get)]
    source_shard: Option<usize>,
    #[pyo3(get)]
    target_shard: Option<usize>,
    #[pyo3(get)]
    trim_shard: Option<usize>,
}

#[pymethods]
impl PlannedAction {
    fn __repr__(&self) -> String {
        match self.action_type.as_str() {
            "replicate" => format!(
                "PlannedAction(replicate {:?} from {} to {})",
                self.key,
                self.source_shard.unwrap_or(0),
                self.target_shard.unwrap_or(0),
            ),
            _ => format!(
                "PlannedAction(trim {:?} from shard {})",
                self.key,
                self.trim_shard.unwrap_or(0),
            ),
        }
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct RepairReplicationPlan {
    #[pyo3(get)]
    replications: Vec<PlannedAction>,
    #[pyo3(get)]
    trims: Vec<PlannedAction>,
    #[pyo3(get)]
    unrepairable: usize,
    #[pyo3(get)]
    untrimmable: usize,
}

#[pymethods]
impl RepairReplicationPlan {
    fn __repr__(&self) -> String {
        format!(
            "RepairReplicationPlan(replications={}, trims={}, unrepairable={}, untrimmable={})",
            self.replications.len(), self.trims.len(),
            self.unrepairable, self.untrimmable
        )
    }
}

// -- ClusterStore wrapper ----------------------------------------------------

/// A sharded object store backed by multiple RawObjectStore shards.
///
/// Create with `cluster_from_paths()` or `cluster_from_formatted_paths()`.
/// Implements the same put/get/delete/list API as the base `rawobjstr.Store`.
#[pyclass]
struct ClusterStore {
    inner: Arc<RwLock<ShardedObjectStore>>,
    rt: Runtime,
    /// Keep raw shard references so we can flush_index() on close.
    /// Slots may be None for offline (degraded) shards.
    raw_shards: RwLock<Vec<Option<Arc<RawObjectStore>>>>,
}

impl ClusterStore {
    /// Internal: track a newly attached raw shard.
    fn raw_shards_attach(&self, shard_id: usize, store: Arc<RawObjectStore>) {
        let mut shards = self.raw_shards.write();
        if shard_id < shards.len() {
            shards[shard_id] = Some(store);
        }
    }

    /// Internal: remove a raw shard reference on detach.
    fn raw_shards_detach(&self, shard_id: usize) {
        let mut shards = self.raw_shards.write();
        if shard_id < shards.len() {
            shards[shard_id] = None;
        }
    }

    /// Build a `RawRefRegistry` from the current raw shard references.
    /// All Python-constructed shards are raw block devices.
    fn build_raw_refs(&self) -> RawRefRegistry {
        let shards = self.raw_shards.read();
        let refs: Vec<Option<Arc<RawObjectStore>>> = shards.clone();
        let kinds = vec![ShardKind::Raw; refs.len()];
        RawRefRegistry::new(refs, kinds)
    }
}

#[pymethods]
impl ClusterStore {
    // ---- Write ----

    /// Write an object, replicating across N shards.
    fn put(&self, py: Python<'_>, key: &str, data: &[u8]) -> PyResult<()> {
        let path = Path::from(key);
        let payload = PutPayload::from(Bytes::copy_from_slice(data));
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.put(&path, payload))
                .map_err(obj_err)?;
            Ok(())
        })
    }

    /// Write an object only if the key does not already exist.
    /// Raises FileExistsError if the key is already present.
    fn put_if_not_exists(&self, py: Python<'_>, key: &str, data: &[u8]) -> PyResult<()> {
        let path = Path::from(key);
        let payload = PutPayload::from(Bytes::copy_from_slice(data));
        let opts = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..object_store::PutOptions::default()
        };
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.put_opts(&path, payload, opts))
                .map_err(|e| match &e {
                    object_store::Error::AlreadyExists { path, .. } => {
                        PyFileExistsError::new_err(format!("key already exists: {}", path))
                    }
                    _ => obj_err(e),
                })?;
            Ok(())
        })
    }

    /// Write an object via multipart upload (delegates to primary shard).
    fn put_multipart(&self, py: Python<'_>, key: &str, data: &[u8]) -> PyResult<()> {
        let path = Path::from(key);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let mut upload = inner.put_multipart(&path).await.map_err(obj_err)?;
                upload
                    .put_part(PutPayload::from(Bytes::copy_from_slice(data)))
                    .await
                    .map_err(obj_err)?;
                upload.complete().await.map_err(obj_err)?;
                Ok(())
            })
        })
    }

    // ---- Read ----

    /// Read a full object, or a byte range if `range=(start, end)` is given.
    #[pyo3(signature = (key, *, range=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let path = Path::from(key);
        let opts = match range {
            Some((start, end)) => GetOptions {
                range: Some(GetRange::Bounded(start..end)),
                ..GetOptions::default()
            },
            None => GetOptions::default(),
        };
        let inner = self.inner.read();
        let data = py.allow_threads(|| {
            self.rt.block_on(async {
                let result = inner.get_opts(&path, opts).await.map_err(obj_err)?;
                let data = result.bytes().await.map_err(obj_err)?;
                Ok::<_, PyErr>(data)
            })
        })?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Get object metadata without reading the data.
    fn head(&self, py: Python<'_>, key: &str) -> PyResult<ObjectMeta> {
        let path = Path::from(key);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let meta = inner.head(&path).await.map_err(obj_err)?;
                Ok(convert_meta(&meta))
            })
        })
    }

    // ---- Metadata-aware I/O ----

    /// Write an object with raw metadata bytes, replicating across shards.
    ///
    /// ``metadata`` is the raw TLV-encoded metadata (or arbitrary bytes)
    /// that will be stored alongside the object body.
    fn put_with_meta<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        data: &[u8],
        metadata: &[u8],
    ) -> PyResult<()> {
        let path = Path::from(key);
        let payload = Bytes::copy_from_slice(data);
        let meta = metadata.to_vec();
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            self.rt.block_on(async {
                put_with_meta(&*inner, &raw_refs, &path, payload, &meta)
                    .await
                    .map_err(raw_err)
            })
        })
    }

    /// Write an object from a local file that already contains
    /// payload followed by metadata bytes.
    ///
    /// When all target shards are Raw, the file is streamed directly
    /// without reading the entire body into memory (~1 MB peak heap).
    /// ``meta_len`` is the number of trailing metadata bytes in the file.
    fn put_with_meta_from_file(
        &self,
        py: Python<'_>,
        key: &str,
        file_path: &str,
        meta_len: u16,
    ) -> PyResult<()> {
        let path = Path::from(key);
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            let mut file = std::fs::File::open(file_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            self.rt.block_on(async {
                put_with_meta_from_file(&*inner, &raw_refs, &path, &mut file, meta_len)
                    .await
                    .map_err(raw_err)
            })
        })
    }

    /// Get object metadata (ObjectMeta + meta_len) without reading the body.
    ///
    /// Returns ``(ObjectMeta, meta_len)`` where ``meta_len`` is the number
    /// of trailing metadata bytes stored with the object.
    fn head_with_meta(&self, py: Python<'_>, key: &str) -> PyResult<(ObjectMeta, u16)> {
        let path = Path::from(key);
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let (meta, meta_len) = head_with_meta(&*inner, &raw_refs, &path)
                    .await
                    .map_err(raw_err)?;
                Ok((convert_meta(&meta), meta_len))
            })
        })
    }

    /// Read the raw metadata bytes for an object.
    ///
    /// Returns empty ``bytes`` if the object has no metadata.
    fn get_metadata<'py>(
        &self,
        py: Python<'py>,
        key: &str,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let path = Path::from(key);
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        let data = py.allow_threads(|| {
            self.rt.block_on(async {
                get_metadata(&*inner, &raw_refs, &path)
                    .await
                    .map_err(raw_err)
            })
        })?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// List objects with their metadata lengths.
    ///
    /// Returns a list of ``(ObjectMeta, meta_len)`` tuples.
    #[pyo3(signature = (prefix=None))]
    fn list_with_meta(
        &self,
        py: Python<'_>,
        prefix: Option<&str>,
    ) -> PyResult<Vec<(ObjectMeta, u16)>> {
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let prefix_path = prefix.map(Path::from);
                let items = list_with_meta(&*inner, &raw_refs, prefix_path.as_ref()).await;
                Ok(items
                    .iter()
                    .map(|(m, ml)| (convert_meta(m), *ml))
                    .collect())
            })
        })
    }

    /// Set the metadata length for an object on its raw shard.
    ///
    /// No-op for non-raw shards.
    fn set_meta_len(&self, py: Python<'_>, key: &str, meta_len: u16) -> PyResult<()> {
        let path = Path::from(key);
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            self.rt.block_on(async {
                set_meta_len(&*inner, &raw_refs, &path, meta_len)
                    .await
                    .map_err(raw_err)
            })
        })
    }

    // ---- Delete ----

    fn delete(&self, py: Python<'_>, key: &str) -> PyResult<()> {
        let path = Path::from(key);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.delete(&path))
                .map_err(obj_err)
        })
    }

    /// List all current delete markers across the cluster.
    ///
    /// Returns a list of ``(original_key, deleted_at_rfc3339)`` tuples.
    fn list_delete_markers(&self, py: Python<'_>) -> PyResult<Vec<(String, String)>> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let markers = inner.list_delete_markers().await;
                Ok(markers
                    .into_iter()
                    .map(|(key, ts)| (key, ts.to_rfc3339()))
                    .collect())
            })
        })
    }

    /// Vacuum stale delete markers.
    ///
    /// Requires all shards to be healthy -- raises an error if any shard
    /// is offline.
    ///
    /// Returns ``(markers_purged, stale_objects_cleaned)``.
    fn vacuum_delete_markers(&self, py: Python<'_>) -> PyResult<(usize, usize)> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.vacuum_delete_markers(None))
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    // ---- Listing ----

    /// List objects matching an optional prefix.
    #[pyo3(signature = (prefix=None))]
    fn list(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<Vec<ObjectMeta>> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let prefix_path = prefix.map(Path::from);
                let items: Vec<object_store::ObjectMeta> = inner
                    .list(prefix_path.as_ref())
                    .try_collect()
                    .await
                    .map_err(obj_err)?;
                Ok(items.iter().map(convert_meta).collect())
            })
        })
    }

    /// List with directory-like grouping.
    #[pyo3(signature = (prefix=None))]
    fn list_with_delimiter(
        &self,
        py: Python<'_>,
        prefix: Option<&str>,
    ) -> PyResult<ListResult> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let prefix_path = prefix.map(Path::from);
                let result = inner
                    .list_with_delimiter(prefix_path.as_ref())
                    .await
                    .map_err(obj_err)?;
                Ok(ListResult {
                    objects: result.objects.iter().map(convert_meta).collect(),
                    common_prefixes: result
                        .common_prefixes
                        .iter()
                        .map(|p| p.to_string())
                        .collect(),
                })
            })
        })
    }

    // ---- Copy & Rename ----

    fn copy(&self, py: Python<'_>, from: &str, to: &str) -> PyResult<()> {
        let from_p = Path::from(from);
        let to_p = Path::from(to);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.copy(&from_p, &to_p))
                .map_err(obj_err)
        })
    }

    fn copy_if_not_exists(&self, py: Python<'_>, from: &str, to: &str) -> PyResult<()> {
        let from_p = Path::from(from);
        let to_p = Path::from(to);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.copy_if_not_exists(&from_p, &to_p))
                .map_err(obj_err)
        })
    }

    fn rename(&self, py: Python<'_>, from: &str, to: &str) -> PyResult<()> {
        let from_p = Path::from(from);
        let to_p = Path::from(to);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.rename(&from_p, &to_p))
                .map_err(obj_err)
        })
    }

    fn rename_if_not_exists(&self, py: Python<'_>, from: &str, to: &str) -> PyResult<()> {
        let from_p = Path::from(from);
        let to_p = Path::from(to);
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.rename_if_not_exists(&from_p, &to_p))
                .map_err(obj_err)
        })
    }

    // ---- Cluster management ----

    /// Number of shards in the cluster.
    fn shard_count(&self) -> usize {
        self.inner.read().shard_count()
    }

    /// Configured replication factor.
    fn replication_factor(&self) -> usize {
        self.inner.read().replication_factor()
    }

    /// Configured minimum writes required for a put to succeed.
    fn min_writes(&self) -> usize {
        self.inner.read().min_writes()
    }

    /// Whether deletes must also satisfy min_writes.
    fn delete_requires_min_writes(&self) -> bool {
        self.inner.read().delete_requires_min_writes()
    }

    /// Whether the cluster is in read-only mode.
    #[getter]
    fn read_only(&self) -> bool {
        self.inner.read().is_read_only()
    }

    /// Health of a shard by index.
    fn shard_health(&self, id: usize) -> Option<ShardHealth> {
        self.inner.read().shard_health(id).map(convert_health)
    }

    /// Set the health of a shard. Returns the previous health.
    fn set_shard_health(&self, id: usize, health: &ShardHealth) -> Option<ShardHealth> {
        self.inner
            .read()
            .set_shard_health(id, to_rust_health(health))
            .map(convert_health)
    }

    /// Get the current read preference ("round-robin" or "ordered").
    fn read_preference(&self) -> String {
        match self.inner.read().read_preference() {
            ReadPreference::RoundRobin => "round-robin".to_string(),
            ReadPreference::Ordered => "ordered".to_string(),
        }
    }

    /// Set the read preference. Accepts "round-robin" or "ordered".
    fn set_read_preference(&self, pref: &str) -> PyResult<()> {
        let rp = match pref {
            "ordered" => ReadPreference::Ordered,
            "round-robin" | "round_robin" => ReadPreference::RoundRobin,
            _ => return Err(PyValueError::new_err(
                format!("unknown read preference: {pref:?} (expected 'round-robin' or 'ordered')")
            )),
        };
        self.inner.read().set_read_preference(rp);
        Ok(())
    }

    /// Invalidate a shard: mark Degraded, purge catalog entries,
    /// re-scan and restore. Returns an InvalidateReport.
    fn invalidate_shard(&self, py: Python<'_>, shard_id: usize) -> PyResult<InvalidateReport> {
        py.allow_threads(|| {
            self.rt.block_on(async {
                let inner = self.inner.read();
                let report = inner
                    .invalidate_shard(shard_id)
                    .await
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                Ok(InvalidateReport {
                    shard_id: report.shard_id,
                    entries_purged: report.entries_purged,
                    entries_restored: report.entries_restored,
                    missing_keys: report.missing_keys,
                    scan_ok: report.scan_ok,
                })
            })
        })
    }

    /// Return all catalog entries that reference the given shard.
    fn entries_for_shard(&self, shard_id: usize) -> Vec<(String, PlacementInfo)> {
        self.inner
            .read()
            .catalog()
            .entries_for_shard(shard_id)
            .into_iter()
            .map(|(k, e)| {
                (
                    k,
                    PlacementInfo {
                        shards: e.shards,
                        size: e.size,
                        crc32c: e.crc32c,
                        updated: e.updated.to_rfc3339(),
                    },
                )
            })
            .collect()
    }

    /// Remove all catalog entries for a shard. Returns entries affected.
    fn remove_all_for_shard(&self, shard_id: usize) -> usize {
        self.inner.read().catalog().remove_all_for_shard(shard_id)
    }

    /// Placement info for an object (which shards hold it).
    fn placement(&self, key: &str) -> Option<PlacementInfo> {
        self.inner.read().placement(key).map(|e| PlacementInfo {
            shards: e.shards,
            size: e.size,
            crc32c: e.crc32c,
            updated: e.updated.to_rfc3339(),
        })
    }

    /// Total number of objects tracked by the catalog.
    fn catalog_len(&self) -> usize {
        self.inner.read().catalog().len()
    }

    /// Save catalog to a file.
    ///
    /// Args:
    ///     path: File path to save to.
    ///     format: "json" (default) or "bincode".
    #[pyo3(signature = (path, *, format="json"))]
    fn save_catalog(&self, path: &str, format: &str) -> PyResult<()> {
        let p = std::path::Path::new(path);
        let inner = self.inner.read();
        let persistence = match format {
            "json" => shardedobjstr::catalog::CatalogPersistence::json(p),
            "bincode" => shardedobjstr::catalog::CatalogPersistence::bincode(p),
            _ => return Err(PyValueError::new_err(
                format!("unknown catalog format '{}'; valid: json, bincode", format),
            )),
        };
        persistence.save(inner.catalog()).map_err(io_err)
    }

    /// Rebuild catalog by scanning all shards.  Returns number of objects found.
    fn rebuild_catalog(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.rebuild_catalog())
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    /// Rebuild catalog entries for a single shard.  Returns number of objects found.
    fn rebuild_catalog_for_shard(&self, py: Python<'_>, shard_id: usize) -> PyResult<usize> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.rebuild_catalog_for_shard(shard_id))
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    /// Find objects whose replica count is below the target replication factor.
    ///
    /// Returns a list of `(key, current_replica_count)` tuples.
    /// Computed on the fly from the catalog -- no persistent state needed.
    fn find_under_replicated(&self) -> Vec<(String, usize)> {
        self.inner.read().find_under_replicated()
    }

    /// Attach a store to an offline shard slot.
    ///
    /// Args:
    ///     shard_id: Index of the shard slot to attach.
    ///     path: Path to the raw shard image or block device.
    ///     force: If True, trust existing data (scan only).
    ///         If False, invalidate first (purge + re-scan).
    ///
    /// Returns the number of objects found on the shard.
    #[pyo3(signature = (shard_id, path, *, force=true))]
    fn attach_shard(
        &self,
        py: Python<'_>,
        shard_id: usize,
        path: &str,
        force: bool,
    ) -> PyResult<usize> {
        let store = open_raw_shard(path, false, false)?;
        self.raw_shards_attach(shard_id, Arc::clone(&store));
        let obj_store: Arc<dyn ObjectStore> = store;
        py.allow_threads(|| {
            self.rt.block_on(async {
                let inner = self.inner.read();
                inner
                    .attach_shard(shard_id, obj_store, force)
                    .await
                    .map_err(|e| PyIOError::new_err(e.to_string()))
            })
        })
    }

    /// Detach a shard: replace with offline placeholder, mark Offline.
    /// Catalog entries are preserved (not purged).
    ///
    /// Returns the previous health status as a string, or None if shard_id
    /// is out of range.
    fn detach_shard(&self, shard_id: usize) -> Option<ShardHealth> {
        self.raw_shards_detach(shard_id);
        self.inner
            .read()
            .detach_shard(shard_id)
            .map(convert_health)
    }

    /// Take a shard offline and hold it in Detached state so the recovery
    /// loop will not auto-reattach it.  Returns the previous health.
    ///
    /// If ``suppress_replication`` is true, the re-replication sweep will
    /// skip this shard (objects will not be re-replicated elsewhere).
    #[pyo3(signature = (shard_id, suppress_replication=false))]
    fn hold_offline(
        &self,
        shard_id: usize,
        suppress_replication: bool,
    ) -> Option<ShardHealth> {
        self.raw_shards_detach(shard_id);
        self.inner.read().hold_offline(shard_id, suppress_replication, shardedobjstr::DetachReason::Manual)
            .map(convert_health)
    }

    /// Release the Detached hold on a shard, making it eligible for manual
    /// attach.  Returns true if the shard was held.
    fn release_hold(&self, shard_id: usize) -> bool {
        self.inner.read().release_hold(shard_id)
    }

    /// Check whether re-replication is suppressed for a shard.
    ///
    /// Returns true if ``hold_offline()`` was called with
    /// ``suppress_replication=True``.
    fn shard_suppress_replication(&self, shard_id: usize) -> bool {
        self.inner.read().shard_suppress_replication(shard_id)
    }

    /// Return why a shard was detached, or None if healthy/not detached.
    ///
    /// Possible values: "Manual", "ProbeFailure", "DeviceMissing", "Drain".
    fn shard_detach_reason(&self, shard_id: usize) -> Option<String> {
        self.inner.read().shard_detach_reason(shard_id).map(|r| format!("{:?}", r))
    }

    /// Annotate a detached shard with a reason.
    ///
    /// Args:
    ///     shard_id: Index of the shard.
    ///     reason: One of "Manual", "ProbeFailure", "DeviceMissing", "Drain".
    fn set_detach_reason(&self, shard_id: usize, reason: &str) -> PyResult<()> {
        let r = match reason {
            "Manual" => shardedobjstr::DetachReason::Manual,
            "ProbeFailure" => shardedobjstr::DetachReason::ProbeFailure,
            "DeviceMissing" => shardedobjstr::DetachReason::DeviceMissing,
            "Drain" => shardedobjstr::DetachReason::Drain,
            other => return Err(PyValueError::new_err(format!(
                "unknown detach reason: {other:?}; expected Manual, ProbeFailure, DeviceMissing, or Drain"
            ))),
        };
        self.inner.read().set_detach_reason(shard_id, r);
        Ok(())
    }

    /// Replicate a single object from one shard to another.
    ///
    /// Returns the size of the replicated object in bytes.
    fn replicate_object(
        &self,
        py: Python<'_>,
        key: &str,
        from_shard: usize,
        to_shard: usize,
    ) -> PyResult<u64> {
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.replicate_object(key, from_shard, to_shard, Some(&raw_refs)))
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    /// Find objects whose replica count exceeds the replication factor.
    ///
    /// Returns a list of `(key, current_replica_count)` tuples.
    fn find_over_replicated(&self) -> Vec<(String, usize)> {
        self.inner.read().find_over_replicated()
    }

    /// Pick the best shard from which to remove an excess replica of `key`.
    ///
    /// Prefers the fullest healthy shard that holds a copy.
    /// Returns a shard id, or None if nothing to trim.
    fn pick_excess_shard(&self, key: &str) -> Option<usize> {
        self.inner.read().pick_excess_shard(key)
    }

    /// Remove a single replica of `key` from `shard_id`.
    fn remove_replica(&self, py: Python<'_>, key: &str, shard_id: usize) -> PyResult<()> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt
                .block_on(inner.remove_replica(key, shard_id))
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    /// Find the best target shard for replicating `key`.
    ///
    /// Picks a healthy shard that does not already hold a copy, preferring
    /// shards with more free space.  Returns a shard id or None.
    fn find_replication_target(&self, key: &str) -> Option<usize> {
        self.inner.read().find_replication_target(key)
    }

    /// Run a full repair-replication sweep: repair under-replicated
    /// objects then trim over-replicated ones.
    ///
    /// Args:
    ///     batch_size: Max objects to process per phase (default 100).
    ///
    /// Returns a dict with keys: re_replicated, trimmed,
    /// under_remaining, over_remaining.
    #[pyo3(signature = (*, batch_size=100))]
    fn repair_replication(&self, py: Python<'_>, batch_size: usize) -> PyResult<RepairReplicationResult> {
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            // Rebuild catalog first so we see the true state of all shards.
            let _ = self.rt.block_on(inner.rebuild_catalog());
            let result = self.rt.block_on(
                shardedobjstr::repair::repair_replication_sweep(&inner, batch_size, Some(&raw_refs), None),
            );
            Ok(RepairReplicationResult {
                re_replicated: result.re_replicated,
                trimmed: result.trimmed,
                under_remaining: result.under_remaining,
                over_remaining: result.over_remaining,
            })
        })
    }

    /// Run a re-replication sweep for under-replicated objects.
    ///
    /// Args:
    ///     batch_size: Max objects to process (default 100).
    ///
    /// Returns the number of objects re-replicated.
    #[pyo3(signature = (*, batch_size=100))]
    fn re_replication_sweep(
        &self,
        py: Python<'_>,
        batch_size: usize,
    ) -> PyResult<usize> {
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            Ok(self.rt.block_on(
                shardedobjstr::repair::re_replication_sweep(&inner, batch_size, Some(&raw_refs)),
            ))
        })
    }

    /// Trim over-replicated objects (excess replicas beyond RF).
    ///
    /// Args:
    ///     batch_size: Max objects to process (default 100).
    ///
    /// Returns the number of excess replicas removed.
    #[pyo3(signature = (*, batch_size=100))]
    fn over_replication_trim(
        &self,
        py: Python<'_>,
        batch_size: usize,
    ) -> PyResult<usize> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            Ok(self.rt.block_on(
                shardedobjstr::repair::over_replication_trim(&inner, batch_size),
            ))
        })
    }

    /// Drain all objects from a shard by replicating single-copy objects
    /// to surviving shards, then detaching the shard and running a
    /// repair-replication sweep.
    ///
    /// Objects already replicated on other healthy shards are skipped.
    /// Objects whose only copy lives on the victim are replicated to
    /// the least-full healthy shard before the victim goes offline.
    ///
    /// Args:
    ///     shard_id: The shard index to drain.
    ///     batch_size: Max objects per repair-replication pass (default 10000).
    ///
    /// Returns a RepairReplicationResult.
    #[pyo3(signature = (shard_id, *, batch_size=10000))]
    fn drain_shard(
        &self,
        py: Python<'_>,
        shard_id: usize,
        batch_size: usize,
    ) -> PyResult<RepairReplicationResult> {
        let inner = self.inner.read();
        if shard_id >= inner.shard_count() {
            return Err(PyValueError::new_err("shard_id out of range"));
        }
        let raw_refs = self.build_raw_refs();
        // Phase 1: replicate objects whose only copy lives on the victim.
        let pre_moved = py.allow_threads(|| {
            self.rt.block_on(async {
                let entries = inner.catalog().all_entries();
                let mut moved = 0usize;
                for (key, entry) in &entries {
                    if !entry.shards.contains(&shard_id) {
                        continue;
                    }
                    let other_healthy = entry.shards.iter().any(|&s| {
                        s != shard_id
                            && inner.shard_health(s)
                                == Some(shardedobjstr::ShardHealth::Healthy)
                    });
                    if other_healthy {
                        continue;
                    }
                    if let Some(target) = inner.find_replication_target(key) {
                        if inner.replicate_object(key, shard_id, target, Some(&raw_refs)).await.is_ok() {
                            moved += 1;
                        }
                    }
                }
                moved
            })
        });
        // Phase 2: detach the shard and run a repair-replication sweep.
        inner.detach_shard(shard_id);
        py.allow_threads(|| {
            let result = self.rt.block_on(
                shardedobjstr::repair::repair_replication_sweep(&inner, batch_size, Some(&raw_refs), None),
            );
            // Rebuild catalog to remove stale entries for the drained shard.
            let _ = self.rt.block_on(inner.rebuild_catalog());
            Ok(RepairReplicationResult {
                re_replicated: pre_moved + result.re_replicated,
                trimmed: result.trimmed,
                under_remaining: result.under_remaining,
                over_remaining: result.over_remaining,
            })
        })
    }

    /// Redistribute objects to balance shard object counts.
    ///
    /// Moves objects from the fullest shard to the emptiest until the
    /// difference is within the tolerance percentage of the mean.
    /// Ensures RF is met (replicate then delete) for each moved object.
    ///
    /// Args:
    ///     batch_size: Max objects to move per call (default 500).
    ///     tolerance_pct: Stop when (max-min)/mean <= this (default 0.10).
    ///
    /// Returns a RedistributeResult.
    #[pyo3(signature = (*, batch_size=500, tolerance_pct=0.10))]
    fn redistribute(
        &self,
        py: Python<'_>,
        batch_size: usize,
        tolerance_pct: f64,
    ) -> PyResult<RedistributeResult> {
        let inner = self.inner.read();
        let raw_refs = self.build_raw_refs();
        py.allow_threads(|| {
            let result = self.rt.block_on(
                shardedobjstr::repair::redistribute_sweep(&inner, batch_size, tolerance_pct, Some(&raw_refs), None),
            );
            Ok(RedistributeResult {
                moved: result.moved,
                skipped: result.skipped,
                errors: result.errors,
                shard_counts: result.shard_counts,
            })
        })
    }

    /// Return the CRC error count for a shard.
    ///
    /// This counter increments when a read detects data corruption.
    /// It survives SIGHUP (config reload) but resets on process restart.
    fn crc_error_count(&self, shard_id: usize) -> u64 {
        self.inner.read().shard_crc_error_count(shard_id)
    }

    /// Return when a shard went offline, or None if it is healthy.
    fn shard_offline_since(&self, shard_id: usize) -> Option<f64> {
        self.inner
            .read()
            .shard_offline_since(shard_id)
            .map(|dt| dt.timestamp() as f64)
    }

    /// Return the cached free space for a shard, or None if not set.
    fn shard_free_space(&self, shard_id: usize) -> Option<u64> {
        self.inner.read().shard_free_space(shard_id)
    }

    /// Update the cached free space for a shard.
    fn set_shard_free_space(&self, shard_id: usize, free: u64) {
        self.inner.read().set_shard_free_space(shard_id, free);
    }

    /// Number of background read-repair tasks triggered so far.
    fn read_repair_count(&self) -> u64 {
        self.inner.read().read_repair_count()
    }

    /// Number of read-repair tasks that succeeded.
    fn read_repair_success(&self) -> u64 {
        self.inner.read().read_repair_success()
    }

    /// Number of read-repair tasks that failed.
    fn read_repair_failed(&self) -> u64 {
        self.inner.read().read_repair_failed()
    }

    /// Return the multipart upload expiry duration in seconds.
    fn multipart_expiry(&self) -> f64 {
        self.inner.read().multipart_expiry().as_secs_f64()
    }

    // ---- Verify replicas ----

    /// Verify a single object by reading every replica and comparing CRC32c.
    ///
    /// Returns a VerifyObjectReport with per-replica results and whether
    /// all replicas are consistent.
    fn verify_object(&self, py: Python<'_>, key: &str) -> PyResult<VerifyObjectReport> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let report = inner.verify_object(key).await
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                Ok(VerifyObjectReport {
                    key: report.key,
                    catalog_crc: report.catalog_crc,
                    replicas: report.replicas.into_iter().map(|r| VerifyReplicaResult {
                        shard_id: r.shard_id,
                        crc32c: r.crc32c,
                        size: r.size,
                        matches_catalog: r.matches_catalog,
                        error: r.error,
                    }).collect(),
                    replicas_consistent: report.replicas_consistent,
                })
            })
        })
    }

    /// Verify all objects in the catalog (or filtered by prefix).
    ///
    /// Returns a VerifyReport. Only mismatched/errored objects appear
    /// in the details list.
    #[pyo3(signature = (*, prefix=None))]
    fn verify_all(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<VerifyReport> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let report = inner.verify_all(prefix).await;
                Ok(VerifyReport {
                    objects_checked: report.objects_checked,
                    objects_ok: report.objects_ok,
                    objects_mismatched: report.objects_mismatched,
                    objects_with_errors: report.objects_with_errors,
                    details: report.details.into_iter().map(|d| VerifyObjectReport {
                        key: d.key,
                        catalog_crc: d.catalog_crc,
                        replicas: d.replicas.into_iter().map(|r| VerifyReplicaResult {
                            shard_id: r.shard_id,
                            crc32c: r.crc32c,
                            size: r.size,
                            matches_catalog: r.matches_catalog,
                            error: r.error,
                        }).collect(),
                        replicas_consistent: d.replicas_consistent,
                    }).collect(),
                })
            })
        })
    }

    // ---- Cross-verify (MD5-based) ----

    /// Cross-verify a single object by comparing MD5 digests across replicas.
    ///
    /// Returns a CrossVerifyObjectReport with per-shard MD5, size, and
    /// timestamps, plus a ``consistent`` flag.
    fn cross_verify_object(&self, py: Python<'_>, key: &str) -> PyResult<CrossVerifyObjectReport> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let report = inner.cross_verify_object(key).await
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                Ok(CrossVerifyObjectReport {
                    key: report.key,
                    catalog_crc: report.catalog_crc,
                    shards: report.shards.into_iter().map(|d| CrossVerifyShardDigest {
                        shard_id: d.shard_id,
                        md5_hex: d.md5_hex,
                        size: d.size,
                        last_modified: d.last_modified.to_rfc3339(),
                    }).collect(),
                    consistent: report.consistent,
                    errors: report.errors,
                })
            })
        })
    }

    /// Cross-verify all objects by comparing MD5 digests across replicas.
    ///
    /// Objects with only one replica are skipped. Returns a
    /// CrossVerifyReport with aggregate counts and per-object details
    /// for mismatched or errored objects.
    #[pyo3(signature = (*, prefix=None))]
    fn cross_verify_all(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<CrossVerifyReport> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                let report = inner.cross_verify_all(prefix, None).await;
                Ok(CrossVerifyReport {
                    objects_checked: report.objects_checked,
                    objects_ok: report.objects_ok,
                    objects_mismatched: report.objects_mismatched,
                    objects_with_errors: report.objects_with_errors,
                    objects_skipped_single_replica: report.objects_skipped_single_replica,
                    details: report.details.into_iter().map(|d| CrossVerifyObjectReport {
                        key: d.key,
                        catalog_crc: d.catalog_crc,
                        shards: d.shards.into_iter().map(|s| CrossVerifyShardDigest {
                            shard_id: s.shard_id,
                            md5_hex: s.md5_hex,
                            size: s.size,
                            last_modified: s.last_modified.to_rfc3339(),
                        }).collect(),
                        consistent: d.consistent,
                        errors: d.errors,
                    }).collect(),
                })
            })
        })
    }

    // ---- Plan repair-replication (dry-run) ----

    /// Compute what a repair-replication sweep would do without
    /// performing any I/O.
    ///
    /// Returns a RepairReplicationPlan listing planned copy and trim
    /// operations.
    #[pyo3(signature = (*, batch_size=100))]
    fn plan_repair_replication(&self, batch_size: usize) -> RepairReplicationPlan {
        let inner = self.inner.read();
        let plan = shardedobjstr::repair::plan_repair_replication(&inner, batch_size);

        fn convert_action(a: &shardedobjstr::repair::PlannedAction) -> PlannedAction {
            match &a.action {
                shardedobjstr::repair::PlannedActionKind::Replicate { source_shard, target_shard } => {
                    PlannedAction {
                        key: a.key.clone(),
                        current_count: a.current_count,
                        target_rf: a.target_rf,
                        action_type: "replicate".to_string(),
                        source_shard: Some(*source_shard),
                        target_shard: Some(*target_shard),
                        trim_shard: None,
                    }
                }
                shardedobjstr::repair::PlannedActionKind::Trim { shard } => {
                    PlannedAction {
                        key: a.key.clone(),
                        current_count: a.current_count,
                        target_rf: a.target_rf,
                        action_type: "trim".to_string(),
                        source_shard: None,
                        target_shard: None,
                        trim_shard: Some(*shard),
                    }
                }
            }
        }

        RepairReplicationPlan {
            replications: plan.replications.iter().map(convert_action).collect(),
            trims: plan.trims.iter().map(convert_action).collect(),
            unrepairable: plan.unrepairable,
            untrimmable: plan.untrimmable,
        }
    }

    // ---- Shard access validation ----

    /// Probe each shard to verify it is accessible.
    ///
    /// Returns a list of (shard_id, accessible) tuples.
    /// Useful after construction to detect shards locked by another process.
    fn validate_shard_access(&self, py: Python<'_>) -> PyResult<Vec<(usize, bool)>> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                Ok(inner.validate_shard_access().await)
            })
        })
    }

    // ---- Multipart upload management ----

    /// Number of in-flight multipart uploads.
    fn multipart_upload_count(&self) -> usize {
        self.inner.read().multipart_upload_count()
    }

    /// List in-flight multipart uploads.
    ///
    /// Returns a list of (tracking_id, location, age_secs, primary_shard_id).
    fn list_multipart_uploads(&self) -> Vec<(u64, String, u64, usize)> {
        self.inner.read().list_multipart_uploads()
    }

    /// Purge multipart uploads older than the expiry threshold.
    ///
    /// Returns the number of uploads purged.
    fn purge_stale_multiparts(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = self.inner.read();
        py.allow_threads(|| {
            self.rt.block_on(async {
                Ok(inner.purge_stale_multiparts().await)
            })
        })
    }

    fn __repr__(&self) -> String {
        let inner = self.inner.read();
        format!(
            "ClusterStore(shards={}, replication={}, min_writes={}, delete_requires_min_writes={})",
            inner.shard_count(),
            inner.replication_factor(),
            inner.min_writes(),
            inner.delete_requires_min_writes(),
        )
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_val: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        if let Err(e) = self.flush_all() {
            // If an exception is already propagating, log the flush error
            // rather than replacing the original exception.
            if _exc_type.is_some() {
                eprintln!("WARNING: flush_all failed during __exit__: {}", e);
            } else {
                return Err(e);
            }
        }
        Ok(false)
    }

    /// Flush the index of every shard so data is persisted to disk.
    fn flush_all(&self) -> PyResult<()> {
        for shard in self.raw_shards.read().iter().flatten() {
            shard.flush_index().map_err(raw_err)?;
        }
        Ok(())
    }
}

// -- Module-level functions --------------------------------------------------

fn build_runtime() -> PyResult<Runtime> {
    Runtime::new()
        .map_err(|e| PyIOError::new_err(format!("failed to create tokio runtime: {}", e)))
}

fn open_raw_shard(path: &str, direct_io: bool, read_only: bool) -> PyResult<Arc<RawObjectStore>> {
    let std_path = std::path::Path::new(path);
    let store = if read_only {
        RawObjectStore::open_readonly_with_mode(std_path, OpenMode::Default)
    } else {
        RawObjectStore::open_with_mode(std_path, OpenMode::Default)
    };
    let store = store.map_err(|e| match &e {
        RawStoreError::DeviceLocked { .. } => {
            PyIOError::new_err(format!(
                "cannot open '{}': {}",
                path, e
            ))
        }
        RawStoreError::NotFormatted => {
            PyValueError::new_err(format!(
                "could not open '{}': not formatted. Use format_shard() first.",
                path
            ))
        }
        _ => raw_err(e),
    })?;
    let _ = direct_io;
    Ok(Arc::new(store))
}

/// Format a shard at the given path and return nothing.
/// For block devices, size can be omitted (auto-detected).
/// For image files, size must be provided.
#[pyfunction]
#[pyo3(signature = (path, *, size=None, direct_io=false))]
fn format_shard(path: &str, size: Option<u64>, direct_io: bool) -> PyResult<()> {
    let std_path = std::path::Path::new(path);
    if let Some(sz) = size {
        RawObjectStore::format_with_size(std_path, sz, direct_io).map_err(raw_err)?;
    } else {
        RawObjectStore::format(std_path, direct_io).map_err(raw_err)?;
    }
    Ok(())
}

/// Open a cluster from a list of already-formatted shard paths.
///
/// Args:
///     paths: List of paths to shard image files or block devices.
///     replication_factor: How many copies of each object to keep.
///         Clamped to len(paths). Default 1.
///     direct_io: Open all shards with O_DIRECT. Default False.
///     catalog_path: Optional path to a catalog file for persistence.
///         If the file exists it is loaded automatically.
///     catalog_format: "json" (default) or "bincode". Controls the
///         serialization format used for the catalog file.
#[pyfunction]
#[pyo3(signature = (paths, *, replication_factor=1, min_writes=None, delete_requires_min_writes=false, direct_io=false, read_only=false, catalog_path=None, catalog_format="json"))]
fn open_cluster(
    paths: Vec<String>,
    replication_factor: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
    direct_io: bool,
    read_only: bool,
    catalog_path: Option<&str>,
    catalog_format: &str,
) -> PyResult<ClusterStore> {
    if paths.is_empty() {
        return Err(PyValueError::new_err("paths must not be empty"));
    }
    let raw_shards: Vec<Arc<RawObjectStore>> = paths
        .iter()
        .map(|p| open_raw_shard(p, direct_io, read_only))
        .collect::<PyResult<_>>()?;
    let stores: Vec<Arc<dyn ObjectStore>> = raw_shards
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();

    let mut cluster = ShardedObjectStore::new(stores, replication_factor)
        .with_read_only(read_only);
    if let Some(mw) = min_writes {
        cluster = cluster.with_min_writes(mw);
    }
    if delete_requires_min_writes {
        cluster = cluster.with_delete_requires_min_writes(true);
    }

    if let Some(cp) = catalog_path {
        let persistence = match catalog_format {
            "json" => shardedobjstr::catalog::CatalogPersistence::json(cp),
            "bincode" => shardedobjstr::catalog::CatalogPersistence::bincode(cp),
            _ => return Err(PyValueError::new_err(
                format!("unknown catalog_format '{}'; valid: json, bincode", catalog_format),
            )),
        };
        cluster.set_persistence(persistence);
        cluster.load_catalog().map_err(io_err)?;
    }

    let rt = build_runtime()?;
    Ok(ClusterStore {
        inner: Arc::new(RwLock::new(cluster)),
        rt,
        raw_shards: RwLock::new(raw_shards.into_iter().map(Some).collect()),
    })
}

/// Format a set of shards and open as a cluster in one step.
///
/// All shards are formatted (any existing content is destroyed).
/// Each shard must have a size specified as `(path, size_bytes)`.
///
/// Args:
///     shards: List of (path, size_bytes) tuples.
///     replication_factor: How many copies of each object to keep. Default 1.
///     direct_io: Use O_DIRECT on all shards. Default False.
#[pyfunction]
#[pyo3(signature = (shards, *, replication_factor=1, min_writes=None, delete_requires_min_writes=false, direct_io=false, compression=None))]
fn format_and_open_cluster(
    shards: Vec<(String, u64)>,
    replication_factor: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
    direct_io: bool,
    compression: Option<&str>,
) -> PyResult<ClusterStore> {
    if shards.is_empty() {
        return Err(PyValueError::new_err("shards must not be empty"));
    }
    let comp = match compression {
        Some(name) => Compression::from_str_name(name)
            .map_err(|e| PyValueError::new_err(format!("unknown compression: {e}")))?,
        None => Compression::None,
    };

    let raw_shards: Vec<Arc<RawObjectStore>> = shards
        .iter()
        .map(|(path, size)| {
            let std_path = std::path::Path::new(path.as_str());
            let store = if comp == Compression::None {
                RawObjectStore::format_with_size(std_path, *size, direct_io).map_err(raw_err)?
            } else {
                RawObjectStore::format_with_options(
                    std_path,
                    FormatOptions {
                        device_size: *size,
                        direct_io,
                        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
                        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                        compression: comp,
                    },
                )
                .map_err(raw_err)?
            };
            Ok(Arc::new(store))
        })
        .collect::<PyResult<_>>()?;
    let stores: Vec<Arc<dyn ObjectStore>> = raw_shards
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();

    let mut cluster = ShardedObjectStore::new(stores, replication_factor);
    if let Some(mw) = min_writes {
        cluster = cluster.with_min_writes(mw);
    }
    if delete_requires_min_writes {
        cluster = cluster.with_delete_requires_min_writes(true);
    }
    let rt = build_runtime()?;
    Ok(ClusterStore {
        inner: Arc::new(RwLock::new(cluster)),
        rt,
        raw_shards: RwLock::new(raw_shards.into_iter().map(Some).collect()),
    })
}

/// Open a cluster backed by filesystem (LocalFileSystem) shards.
///
/// Each entry in `roots` is a directory path. The directory must already
/// exist. This wraps `object_store::local::LocalFileSystem` as the backend.
///
/// Args:
///     roots: List of directory paths for each shard.
///     replication_factor: Target number of copies per object. Default 1.
///
/// Returns:
///     A `ClusterStore` backed by filesystem shards.
#[pyfunction]
#[pyo3(signature = (roots, *, replication_factor=1, min_writes=None, delete_requires_min_writes=false))]
fn open_fs_cluster(
    roots: Vec<String>,
    replication_factor: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
) -> PyResult<ClusterStore> {
    if roots.is_empty() {
        return Err(PyValueError::new_err("roots must not be empty"));
    }
    let stores: Vec<Arc<dyn ObjectStore>> = roots
        .iter()
        .map(|root| {
            let fs = object_store::local::LocalFileSystem::new_with_prefix(root)
                .map_err(|e| PyIOError::new_err(format!("failed to open fs shard at {}: {}", root, e)))?;
            Ok(Arc::new(fs) as Arc<dyn ObjectStore>)
        })
        .collect::<PyResult<_>>()?;

    let mut cluster = ShardedObjectStore::new(stores, replication_factor);
    if let Some(mw) = min_writes {
        cluster = cluster.with_min_writes(mw);
    }
    if delete_requires_min_writes {
        cluster = cluster.with_delete_requires_min_writes(true);
    }
    let rt = build_runtime()?;
    Ok(ClusterStore {
        inner: Arc::new(RwLock::new(cluster)),
        rt,
        raw_shards: RwLock::new(vec![None; roots.len()]),
    })
}

/// Open a cluster where some shards may be offline (degraded startup).
///
/// Each entry in `shard_paths` is either a path string (available shard)
/// or None (offline slot). Offline slots get a placeholder store and are
/// marked Offline. The cluster can serve reads/writes on available shards.
/// Use `attach_shard()` on the returned ClusterStore to bring offline
/// shards online later.
///
/// Args:
///     shard_paths: List of path strings or None for each shard slot.
///     replication_factor: Target number of copies per object. Default 1.
///     direct_io: Open available shards with O_DIRECT. Default False.
///     catalog_path: Optional catalog persistence file path.
///     catalog_format: "json" (default) or "bincode".
#[pyfunction]
#[pyo3(signature = (shard_paths, *, replication_factor=1, min_writes=None, delete_requires_min_writes=false, direct_io=false, read_only=false, catalog_path=None, catalog_format="json"))]
fn open_cluster_degraded(
    shard_paths: Vec<Option<String>>,
    replication_factor: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
    direct_io: bool,
    read_only: bool,
    catalog_path: Option<&str>,
    catalog_format: &str,
) -> PyResult<ClusterStore> {
    if shard_paths.is_empty() {
        return Err(PyValueError::new_err("shard_paths must not be empty"));
    }
    let mut raw_shards: Vec<Option<Arc<RawObjectStore>>> = Vec::with_capacity(shard_paths.len());
    let mut obj_stores: Vec<Option<Arc<dyn ObjectStore>>> = Vec::with_capacity(shard_paths.len());
    for maybe_path in &shard_paths {
        match maybe_path {
            Some(path) => {
                let store = open_raw_shard(path, direct_io, read_only)?;
                obj_stores.push(Some(Arc::clone(&store) as Arc<dyn ObjectStore>));
                raw_shards.push(Some(store));
            }
            None => {
                obj_stores.push(None);
                raw_shards.push(None);
            }
        }
    }

    let mut cluster = ShardedObjectStore::new_with_offline(obj_stores, replication_factor)
        .with_read_only(read_only);
    if let Some(mw) = min_writes {
        cluster = cluster.with_min_writes(mw);
    }
    if delete_requires_min_writes {
        cluster = cluster.with_delete_requires_min_writes(true);
    }

    if let Some(cp) = catalog_path {
        let persistence = match catalog_format {
            "json" => shardedobjstr::catalog::CatalogPersistence::json(cp),
            "bincode" => shardedobjstr::catalog::CatalogPersistence::bincode(cp),
            _ => return Err(PyValueError::new_err(
                format!("unknown catalog_format '{}'; valid: json, bincode", catalog_format),
            )),
        };
        cluster.set_persistence(persistence);
        cluster.load_catalog().map_err(io_err)?;
    }

    let rt = build_runtime()?;
    Ok(ClusterStore {
        inner: Arc::new(RwLock::new(cluster)),
        rt,
        raw_shards: RwLock::new(raw_shards),
    })
}

/// Format a shard with full options and open as a single-shard cluster.
#[pyfunction]
#[pyo3(signature = (path, *, size, direct_io=false, index_slot_size=None, max_key_length=None, compression=None))]
fn format_shard_with_options(
    path: &str,
    size: u64,
    direct_io: bool,
    index_slot_size: Option<u64>,
    max_key_length: Option<usize>,
    compression: Option<&str>,
) -> PyResult<()> {
    let std_path = std::path::Path::new(path);
    let max_key_len = max_key_length.unwrap_or(rawobjstr::DEFAULT_MAX_KEY_LENGTH);
    let comp = match compression {
        Some(name) => Compression::from_str_name(name)
            .map_err(|e| PyValueError::new_err(format!("unknown compression: {e}")))?,
        None => Compression::None,
    };
    RawObjectStore::format_with_options(
        std_path,
        FormatOptions {
            device_size: size,
            direct_io,
            index_slot_size: index_slot_size
                .unwrap_or(rawobjstr::INDEX_REGION_SIZE),
            max_key_length: max_key_len,
            compression: comp,
        },
    )
    .map_err(raw_err)?;
    Ok(())
}

// -- Config file support -----------------------------------------------------

/// Load and validate a cluster config file.
///
/// Returns a dict with the parsed config. Raises ValueError on parse errors.
///
/// Example:
///     conf = shardedobjstr.load_config("cluster.conf")
///     print(conf["replicas"], conf["shards"])
#[pyfunction]
fn load_config(path: &str) -> PyResult<pyo3::Py<pyo3::types::PyDict>> {
    use shardedobjstr::config as cc;

    let conf = cc::load_cluster_conf(std::path::Path::new(path))
        .map_err(|e| PyValueError::new_err(format!("config error: {e}")))?;

    pyo3::Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new_bound(py);
        dict.set_item("replicas", conf.replicas)?;
        dict.set_item("min_writes", conf.min_writes)?;
        dict.set_item("delete_requires_min_writes", conf.delete_requires_min_writes)?;
        dict.set_item("direct_io", conf.direct_io)?;
        dict.set_item("read_only", conf.read_only)?;
        dict.set_item("catalog", conf.catalog.as_deref())?;
        dict.set_item("read_prefer", conf.read_prefer.as_deref())?;
        dict.set_item("compression", conf.compression.as_deref())?;
        dict.set_item("size_mb", conf.size_mb)?;

        let shard_list = pyo3::types::PyList::empty_bound(py);
        for s in &conf.shards {
            let sd = pyo3::types::PyDict::new_bound(py);
            sd.set_item("type", s.type_name())?;
            match s {
                cc::ShardConf::Raw { path, read_only, compression, direct_io, size_mb } => {
                    sd.set_item("path", path)?;
                    sd.set_item("read_only", *read_only)?;
                    sd.set_item("compression", compression.as_deref())?;
                    sd.set_item("direct_io", *direct_io)?;
                    sd.set_item("size_mb", *size_mb)?;
                }
                cc::ShardConf::Fs { root, read_only } => {
                    sd.set_item("root", root)?;
                    sd.set_item("read_only", *read_only)?;
                }
                cc::ShardConf::S3 { endpoint, bucket, region, access_key, secret_key, path_style } => {
                    sd.set_item("endpoint", endpoint)?;
                    sd.set_item("bucket", bucket)?;
                    sd.set_item("region", region.as_deref())?;
                    sd.set_item("access_key", access_key.as_deref())?;
                    sd.set_item("secret_key", secret_key.as_deref())?;
                    sd.set_item("path_style", *path_style)?;
                }
                cc::ShardConf::Mem => {}
                cc::ShardConf::Node(name) => {
                    sd.set_item("node", name)?;
                }
            }
            shard_list.append(sd)?;
        }
        dict.set_item("shards", shard_list)?;
        Ok(dict.unbind())
    })
}

/// Validate a cluster config file and return a list of diagnostic messages.
///
/// Each item is a dict with "level" ("ERROR", "WARN", "INFO") and "message".
#[pyfunction]
fn check_config(path: &str) -> PyResult<pyo3::Py<pyo3::types::PyList>> {
    use shardedobjstr::config as cc;

    let conf = cc::load_cluster_conf(std::path::Path::new(path))
        .map_err(|e| PyValueError::new_err(format!("config error: {e}")))?;

    let diags = cc::validate_cluster_conf(&conf);

    pyo3::Python::with_gil(|py| {
        let list = pyo3::types::PyList::empty_bound(py);
        for d in &diags {
            let item = pyo3::types::PyDict::new_bound(py);
            item.set_item("level", format!("{}", d.level))?;
            item.set_item("message", &d.message)?;
            list.append(item)?;
        }
        Ok(list.unbind())
    })
}

/// Open a cluster from a config file.
///
/// Reads shard paths, replicas, catalog settings from the file and opens
/// the cluster. CLI-style overrides can be passed as keyword arguments.
///
/// Args:
///     path: Path to the .conf file.
///     direct_io: Override the config's direct_io setting.
///     read_only: Override the config's read_only setting.
#[pyfunction]
#[pyo3(signature = (path, *, min_writes=None, direct_io=None, read_only=None))]
fn open_cluster_from_config(
    path: &str,
    min_writes: Option<usize>,
    direct_io: Option<bool>,
    read_only: Option<bool>,
) -> PyResult<ClusterStore> {
    use shardedobjstr::config as cc;

    let conf = cc::load_cluster_conf(std::path::Path::new(path))
        .map_err(|e| PyValueError::new_err(format!("config error: {e}")))?;

    let dio = direct_io.unwrap_or(conf.direct_io);
    let ro = read_only.unwrap_or(conf.read_only);

    if conf.shards.is_empty() {
        return Err(PyValueError::new_err("config has no shards"));
    }

    let raw_shards: Vec<Arc<RawObjectStore>> = conf.shards
        .iter()
        .map(|s| {
            match s {
                cc::ShardConf::Raw { path, read_only, direct_io: shard_dio, .. } => {
                    let shard_ro = *read_only || ro;
                    let shard_dio = shard_dio.unwrap_or(dio);
                    open_raw_shard(path, shard_dio, shard_ro)
                }
                other => Err(PyValueError::new_err(
                    format!("open_cluster_from_config only supports raw shards, got '{}'", other.type_name()),
                )),
            }
        })
        .collect::<PyResult<_>>()?;

    let stores: Vec<Arc<dyn object_store::ObjectStore>> = raw_shards
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn object_store::ObjectStore>)
        .collect();

    let mut cluster = ShardedObjectStore::new(stores, conf.replicas)
        .with_read_only(ro);
    let effective_mw = min_writes.or(conf.min_writes);
    if let Some(mw) = effective_mw {
        cluster = cluster.with_min_writes(mw);
    }

    if let Some(ref cat) = conf.catalog {
        let persistence = if cat == "none" {
            None
        } else if let Some(p) = cat.strip_prefix("json:") {
            Some(shardedobjstr::catalog::CatalogPersistence::json(p))
        } else if let Some(p) = cat.strip_prefix("bincode:") {
            Some(shardedobjstr::catalog::CatalogPersistence::bincode(p))
        } else {
            // Bare path = JSON
            Some(shardedobjstr::catalog::CatalogPersistence::json(cat))
        };
        if let Some(p) = persistence {
            cluster.set_persistence(p);
            let _ = cluster.load_catalog();
        }
    }

    let rt = build_runtime()?;
    Ok(ClusterStore {
        inner: Arc::new(RwLock::new(cluster)),
        rt,
        raw_shards: RwLock::new(raw_shards.into_iter().map(Some).collect()),
    })
}

// -- Python module -----------------------------------------------------------

#[pymodule]
fn _shardedobjstr(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(format_shard, m)?)?;
    m.add_function(wrap_pyfunction!(format_shard_with_options, m)?)?;
    m.add_function(wrap_pyfunction!(open_cluster, m)?)?;
    m.add_function(wrap_pyfunction!(open_cluster_degraded, m)?)?;
    m.add_function(wrap_pyfunction!(format_and_open_cluster, m)?)?;
    m.add_function(wrap_pyfunction!(open_fs_cluster, m)?)?;
    m.add_function(wrap_pyfunction!(load_config, m)?)?;
    m.add_function(wrap_pyfunction!(check_config, m)?)?;
    m.add_function(wrap_pyfunction!(open_cluster_from_config, m)?)?;
    m.add_class::<ClusterStore>()?;
    m.add_class::<ObjectMeta>()?;
    m.add_class::<ListResult>()?;
    m.add_class::<PlacementInfo>()?;
    m.add_class::<ShardHealth>()?;
    m.add_class::<InvalidateReport>()?;
    m.add_class::<RepairReplicationResult>()?;
    m.add_class::<RedistributeResult>()?;
    m.add_class::<VerifyReplicaResult>()?;
    m.add_class::<VerifyObjectReport>()?;
    m.add_class::<VerifyReport>()?;
    m.add_class::<PlannedAction>()?;
    m.add_class::<RepairReplicationPlan>()?;
    m.add_class::<CrossVerifyShardDigest>()?;
    m.add_class::<CrossVerifyObjectReport>()?;
    m.add_class::<CrossVerifyReport>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add(
        "__build_info__",
        std::format!(
            "{} (git {}, built {})",
            env!("CARGO_PKG_VERSION"),
            env!("BUILD_GIT_HASH"),
            env!("BUILD_DATE"),
        ),
    )?;
    Ok(())
}
