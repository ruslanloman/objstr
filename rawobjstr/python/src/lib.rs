use std::path::Path as StdPath;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectStore, PutMode, PutOptions, PutPayload,
};
use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIOError, PyOSError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rawobjstr::store::{
    DeviceInfo, ExportReport, FormatOptions, ImportReport, ObjectFullInfo,
    OpenMode, RawObjectStore, RepairReport, TombstoneEntry, VerifyReport,
};
use rawobjstr::{Compression, RawStoreError};
use tokio::runtime::Runtime;

// -- Error conversion --------------------------------------------------------

fn raw_err(e: RawStoreError) -> PyErr {
    match &e {
        RawStoreError::NotFound(_) => PyFileNotFoundError::new_err(e.to_string()),
        RawStoreError::AlreadyExists(_) => PyFileExistsError::new_err(e.to_string()),
        RawStoreError::NoSpace { .. } => PyOSError::new_err(e.to_string()),
        RawStoreError::ShardOverflow { .. } => PyOSError::new_err(e.to_string()),
        RawStoreError::NotFormatted => PyValueError::new_err(e.to_string()),
        RawStoreError::SuperblockCorrupt => PyValueError::new_err(e.to_string()),
        RawStoreError::IndexCorrupt => PyValueError::new_err(e.to_string()),
        RawStoreError::EmptyPayload => PyValueError::new_err(e.to_string()),
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

// -- Python-visible data classes ---------------------------------------------

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
        format!(
            "ObjectMeta(location={:?}, size={})",
            self.location, self.size
        )
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

#[pyclass(frozen)]
#[derive(Clone)]
struct PyDeviceInfo {
    #[pyo3(get)]
    device_path: String,
    #[pyo3(get)]
    device_size: u64,
    #[pyo3(get)]
    format_version: u32,
    #[pyo3(get)]
    flags: u32,
    #[pyo3(get)]
    direct_io: bool,
    #[pyo3(get)]
    txn_id: u64,
    #[pyo3(get)]
    file_count: usize,
    #[pyo3(get)]
    data_bytes_stored: u64,
    #[pyo3(get)]
    device_bytes_used: u64,
    #[pyo3(get)]
    free_space: u64,
    #[pyo3(get)]
    free_fragments: usize,
    #[pyo3(get)]
    largest_free_extent: u64,
    #[pyo3(get)]
    last_flush_bytes: u64,
    #[pyo3(get)]
    max_key_length: usize,
    #[pyo3(get)]
    index_slot_capacity: u64,
    #[pyo3(get)]
    compression: String,
}

#[pymethods]
impl PyDeviceInfo {
    fn __repr__(&self) -> String {
        format!(
            "DeviceInfo(path={:?}, files={}, used={}, free={})",
            self.device_path, self.file_count, self.data_bytes_stored, self.free_space
        )
    }
}

fn convert_device_info(d: &DeviceInfo) -> PyDeviceInfo {
    PyDeviceInfo {
        device_path: d.device_path.clone(),
        device_size: d.device_size,
        format_version: d.format_version,
        flags: d.flags,
        direct_io: d.direct_io,
        txn_id: d.txn_id,
        file_count: d.file_count,
        data_bytes_stored: d.data_bytes_stored,
        device_bytes_used: d.device_bytes_used,
        free_space: d.free_space,
        free_fragments: d.free_fragments,
        largest_free_extent: d.largest_free_extent,
        last_flush_bytes: d.last_flush_bytes,
        max_key_length: d.max_key_length,
        index_slot_capacity: d.index_slot_capacity,
        compression: d.compression.as_str().to_string(),
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyTombstoneEntry {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    crc32c: u32,
    #[pyo3(get)]
    last_modified: String,
    #[pyo3(get)]
    reason: String,
    #[pyo3(get)]
    tombstone_txn: u64,
}

#[pymethods]
impl PyTombstoneEntry {
    fn __repr__(&self) -> String {
        format!(
            "TombstoneEntry(path={:?}, size={}, reason={:?})",
            self.path, self.size, self.reason
        )
    }
}

fn convert_tombstone(t: &TombstoneEntry) -> PyTombstoneEntry {
    PyTombstoneEntry {
        path: t.path.clone(),
        size: t.size,
        crc32c: t.crc32c,
        last_modified: t.last_modified.to_rfc3339(),
        reason: t.reason.clone(),
        tombstone_txn: t.tombstone_txn,
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyScrubReport {
    #[pyo3(get)]
    regions_scrubbed: usize,
    #[pyo3(get)]
    bytes_scrubbed: u64,
}

#[pymethods]
impl PyScrubReport {
    fn __repr__(&self) -> String {
        format!(
            "ScrubReport(regions={}, bytes={})",
            self.regions_scrubbed, self.bytes_scrubbed
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyVerifyReport {
    #[pyo3(get)]
    files_checked: usize,
    #[pyo3(get)]
    files_ok: usize,
    #[pyo3(get)]
    error_count: usize,
    #[pyo3(get)]
    free_list_consistent: bool,
    #[pyo3(get)]
    space_accounted: bool,
    #[pyo3(get)]
    total_data_region: u64,
    #[pyo3(get)]
    total_used: u64,
    #[pyo3(get)]
    total_free: u64,
}

#[pymethods]
impl PyVerifyReport {
    fn __repr__(&self) -> String {
        format!(
            "VerifyReport(checked={}, ok={}, errors={})",
            self.files_checked, self.files_ok, self.error_count
        )
    }
}

fn convert_verify_report(r: &VerifyReport) -> PyVerifyReport {
    PyVerifyReport {
        files_checked: r.files_checked,
        files_ok: r.files_ok,
        error_count: r.errors.len(),
        free_list_consistent: r.free_list_consistent,
        space_accounted: r.space_accounted,
        total_data_region: r.total_data_region,
        total_used: r.total_used,
        total_free: r.total_free,
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyRepairReport {
    #[pyo3(get)]
    free_list_rebuilt: bool,
    #[pyo3(get)]
    old_free_entries: usize,
    #[pyo3(get)]
    new_free_entries: usize,
    #[pyo3(get)]
    old_free_space: u64,
    #[pyo3(get)]
    new_free_space: u64,
    #[pyo3(get)]
    flushed: bool,
    #[pyo3(get)]
    files_found: usize,
}

#[pymethods]
impl PyRepairReport {
    fn __repr__(&self) -> String {
        format!(
            "RepairReport(rebuilt={}, files={}, old_free={}, new_free={})",
            self.free_list_rebuilt, self.files_found, self.old_free_space, self.new_free_space
        )
    }
}

fn convert_repair_report(r: &RepairReport) -> PyRepairReport {
    PyRepairReport {
        free_list_rebuilt: r.free_list_rebuilt,
        old_free_entries: r.old_free_entries,
        new_free_entries: r.new_free_entries,
        old_free_space: r.old_free_space,
        new_free_space: r.new_free_space,
        flushed: r.flushed,
        files_found: r.files_found,
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyImportReport {
    #[pyo3(get)]
    files_imported: usize,
    #[pyo3(get)]
    bytes_imported: u64,
    #[pyo3(get)]
    errors: Vec<(String, String)>,
}

#[pymethods]
impl PyImportReport {
    fn __repr__(&self) -> String {
        format!(
            "ImportReport(imported={}, bytes={}, errors={})",
            self.files_imported, self.bytes_imported, self.errors.len()
        )
    }
}

fn convert_import_report(r: &ImportReport) -> PyImportReport {
    PyImportReport {
        files_imported: r.files_imported,
        bytes_imported: r.bytes_imported,
        errors: r.errors.clone(),
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyExportReport {
    #[pyo3(get)]
    files_exported: usize,
    #[pyo3(get)]
    bytes_exported: u64,
    #[pyo3(get)]
    errors: Vec<(String, String)>,
}

#[pymethods]
impl PyExportReport {
    fn __repr__(&self) -> String {
        format!(
            "ExportReport(exported={}, bytes={}, errors={})",
            self.files_exported, self.bytes_exported, self.errors.len()
        )
    }
}

fn convert_export_report(r: &ExportReport) -> PyExportReport {
    PyExportReport {
        files_exported: r.files_exported,
        bytes_exported: r.bytes_exported,
        errors: r.errors.clone(),
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
struct PyObjectFullInfo {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    body_size: u64,
    #[pyo3(get)]
    meta_len: u16,
    #[pyo3(get)]
    last_modified: String,
    #[pyo3(get)]
    created_txn: u64,
    #[pyo3(get)]
    offset: u64,
    #[pyo3(get)]
    padded_size: u64,
}

#[pymethods]
impl PyObjectFullInfo {
    fn __repr__(&self) -> String {
        format!(
            "ObjectFullInfo(key={:?}, body={}, meta={})",
            self.key, self.body_size, self.meta_len
        )
    }
}

fn convert_object_full_info(i: &ObjectFullInfo) -> PyObjectFullInfo {
    PyObjectFullInfo {
        key: i.key.clone(),
        body_size: i.body_size,
        meta_len: i.meta_len,
        last_modified: i.last_modified.to_rfc3339(),
        created_txn: i.created_txn,
        offset: i.offset,
        padded_size: i.padded_size,
    }
}

// -- Store wrapper -----------------------------------------------------------

#[pyclass]
struct Store {
    inner: RawObjectStore,
    rt: Runtime,
}

#[pymethods]
impl Store {
    // ---- Write operations ----

    /// Put an object.  `data` can be bytes, bytearray, or memoryview.
    #[pyo3(signature = (key, data, *, mode="overwrite"))]
    fn put(&self, py: Python<'_>, key: &str, data: &[u8], mode: &str) -> PyResult<()> {
        let path = Path::from(key);
        let payload = PutPayload::from(Bytes::copy_from_slice(data));
        let put_mode = match mode {
            "overwrite" => PutMode::Overwrite,
            "create" => PutMode::Create,
            _ => return Err(PyValueError::new_err("mode must be 'overwrite' or 'create'")),
        };
        let opts = PutOptions {
            mode: put_mode,
            ..PutOptions::default()
        };
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.put_opts(&path, payload, opts))
                .map_err(obj_err)?;
            Ok(())
        })
    }

    // ---- Read operations ----

    /// Read a full object, or a byte range if `range` is specified.
    /// Returns bytes.
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
        let data = py.allow_threads(|| {
            self.rt.block_on(async {
                let result = self.inner.get_opts(&path, opts).await.map_err(obj_err)?;
                let data = result.bytes().await.map_err(obj_err)?;
                Ok::<_, PyErr>(data)
            })
        })?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Read the raw on-disk bytes of an object without decompression.
    ///
    /// Returns a dict with:
    ///   - ``data`` (bytes): the exact bytes stored on disk
    ///   - ``uncompressed_size`` (int): original size (0 if not compressed)
    ///   - ``compression`` (str): compression algorithm (e.g. "none", "zstd", "gzip9")
    ///
    /// Callers can check ``uncompressed_size > 0`` to determine if the
    /// returned data is in compressed form.
    fn getraw<'py>(&self, py: Python<'py>, key: &str) -> PyResult<PyObject> {
        let path = Path::from(key);
        let result = py.allow_threads(|| {
            self.inner.get_raw(&path).map_err(raw_err)
        })?;
        let dict = pyo3::types::PyDict::new_bound(py);
        dict.set_item("data", PyBytes::new_bound(py, &result.data))?;
        dict.set_item("uncompressed_size", result.uncompressed_size)?;
        dict.set_item("compression", result.compression.as_str())?;
        Ok(dict.into())
    }

    /// Get object metadata without reading the data.
    fn head(&self, py: Python<'_>, key: &str) -> PyResult<ObjectMeta> {
        let path = Path::from(key);
        py.allow_threads(|| {
            self.rt.block_on(async {
                let meta = self.inner.head(&path).await.map_err(obj_err)?;
                Ok(convert_meta(&meta))
            })
        })
    }

    // ---- Delete ----

    fn delete(&self, py: Python<'_>, key: &str) -> PyResult<()> {
        let path = Path::from(key);
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.delete(&path))
                .map_err(obj_err)
        })
    }

    // ---- Listing ----

    /// List objects matching an optional prefix.
    #[pyo3(signature = (prefix=None))]
    fn list(&self, py: Python<'_>, prefix: Option<&str>) -> PyResult<Vec<ObjectMeta>> {
        py.allow_threads(|| {
            self.rt.block_on(async {
                let prefix_path = prefix.map(Path::from);
                let stream = self.inner.list(prefix_path.as_ref());
                let items: Vec<object_store::ObjectMeta> =
                    stream.try_collect().await.map_err(obj_err)?;
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
        py.allow_threads(|| {
            self.rt.block_on(async {
                let prefix_path = prefix.map(Path::from);
                let result = self
                    .inner
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

    fn copy(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dst);
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.copy(&from_path, &to_path))
                .map_err(obj_err)
        })
    }

    fn copy_if_not_exists(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dst);
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.copy_if_not_exists(&from_path, &to_path))
                .map_err(obj_err)
        })
    }

    fn rename(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dst);
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.rename(&from_path, &to_path))
                .map_err(obj_err)
        })
    }

    fn rename_if_not_exists(&self, py: Python<'_>, src: &str, dst: &str) -> PyResult<()> {
        let from_path = Path::from(src);
        let to_path = Path::from(dst);
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.rename_if_not_exists(&from_path, &to_path))
                .map_err(obj_err)
        })
    }

    // ---- Multipart Upload ----

    /// Start a multipart upload.  Returns a MultipartUpload handle.
    fn multipart(&self, py: Python<'_>, key: &str) -> PyResult<PyMultipartUpload> {
        let path = Path::from(key);
        let upload = py.allow_threads(|| {
            self.rt.block_on(async {
                self.inner
                    .put_multipart(&path)
                    .await
                    .map_err(obj_err)
            })
        })?;
        Ok(PyMultipartUpload {
            upload: Some(upload),
        })
    }

    // ---- Maintenance ----

    /// Persist the in-memory index to disk (crash-safe, double-buffered).
    fn flush_index(&self) -> PyResult<()> {
        self.inner.flush_index().map_err(raw_err)
    }

    /// Whether the store was opened in read-only mode.
    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// Get device statistics and metadata.
    fn device_info(&self) -> PyDeviceInfo {
        convert_device_info(&self.inner.device_info())
    }

    /// Full integrity check.
    fn verify_all(&self) -> PyVerifyReport {
        convert_verify_report(&self.inner.verify_all())
    }

    /// Rebuild free list from index and re-flush.
    fn repair(&self) -> PyResult<PyRepairReport> {
        self.inner
            .repair()
            .map(|r| convert_repair_report(&r))
            .map_err(raw_err)
    }

    /// Return all tombstone entries.
    fn list_tombstones(&self) -> Vec<PyTombstoneEntry> {
        self.inner
            .list_tombstones()
            .iter()
            .map(convert_tombstone)
            .collect()
    }

    /// Remove a single tombstone.  Returns False if path is a live file.
    fn delete_tombstone(&self, path: &str) -> PyResult<bool> {
        self.inner.delete_tombstone(path).map_err(raw_err)
    }

    /// Remove all tombstones.  Returns the count removed.
    fn clear_tombstones(&self) -> PyResult<usize> {
        self.inner.clear_tombstones().map_err(raw_err)
    }

    /// Import all objects from another store into this one.
    /// If ``prefix`` is given, only import keys matching that prefix.
    #[pyo3(signature = (source, *, prefix=None))]
    fn import_from(
        &self,
        py: Python<'_>,
        source: &Store,
        prefix: Option<&str>,
    ) -> PyResult<PyImportReport> {
        let prefix_path = prefix.map(Path::from);
        py.allow_threads(|| {
            self.rt.block_on(async {
                let report = self
                    .inner
                    .import_from(&source.inner, prefix_path.as_ref())
                    .await
                    .map_err(raw_err)?;
                Ok(convert_import_report(&report))
            })
        })
    }

    /// Export all objects from this store to another store.
    fn export_to(&self, py: Python<'_>, target: &Store) -> PyResult<PyExportReport> {
        py.allow_threads(|| {
            self.rt.block_on(async {
                let report = self
                    .inner
                    .export_to(&target.inner)
                    .await
                    .map_err(raw_err)?;
                Ok(convert_export_report(&report))
            })
        })
    }

    /// Zero all free regions on device.
    fn scrub_free_space(&self) -> PyResult<PyScrubReport> {
        let r = self.inner.scrub_free_space().map_err(raw_err)?;
        Ok(PyScrubReport {
            regions_scrubbed: r.regions_scrubbed,
            bytes_scrubbed: r.bytes_scrubbed,
        })
    }

    // ---- Metadata extensions ----

    /// List all objects with full extent details (index-only, no I/O).
    /// Returns a list sorted by key.
    #[pyo3(signature = (prefix=None))]
    fn list_full(&self, prefix: Option<&str>) -> Vec<PyObjectFullInfo> {
        let prefix_path = prefix.map(Path::from);
        self.inner
            .list_full(prefix_path.as_ref())
            .iter()
            .map(convert_object_full_info)
            .collect()
    }

    /// Read only the metadata suffix bytes for an object.
    /// Returns empty bytes if the object has no metadata.
    fn get_metadata<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyBytes>> {
        let path = Path::from(key);
        let data = self.inner.get_metadata(&path).map_err(raw_err)?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Replace the metadata suffix of an object without re-uploading the body.
    /// The existing body is preserved; only the metadata bytes change.
    fn update_metadata(&self, py: Python<'_>, key: &str, metadata: &[u8]) -> PyResult<()> {
        let path = Path::from(key);
        let meta = Bytes::copy_from_slice(metadata);
        py.allow_threads(|| self.inner.update_metadata(&path, meta).map_err(raw_err))
    }

    /// Store an object together with metadata bytes in a single call.
    /// The metadata is appended to the body and tracked via meta_len.
    fn put_with_meta(&self, py: Python<'_>, key: &str, data: &[u8], metadata: &[u8]) -> PyResult<()> {
        let path = Path::from(key);
        let body = Bytes::copy_from_slice(data);
        py.allow_threads(|| self.inner.put_with_meta(&path, body, metadata).map_err(raw_err))
    }

    /// Store body + metadata from a file on disk.
    /// The file must contain body bytes followed by exactly `meta_len` bytes
    /// of metadata at the end.
    fn put_with_meta_from_file(&self, py: Python<'_>, key: &str, file_path: &str, meta_len: u16) -> PyResult<()> {
        let path = Path::from(key);
        let std_path = StdPath::new(file_path);
        let mut file = std::fs::File::open(std_path)
            .map_err(|e| PyIOError::new_err(format!("failed to open file: {}", e)))?;
        py.allow_threads(|| self.inner.put_with_meta_from_file(&path, &mut file, meta_len).map_err(raw_err))
    }

    /// Return (ObjectMeta, meta_len) for an object (index-only, no data I/O).
    fn head_with_meta(&self, key: &str) -> PyResult<(ObjectMeta, u16)> {
        let path = Path::from(key);
        let (meta, meta_len) = self.inner.head_with_meta(&path).map_err(raw_err)?;
        Ok((convert_meta(&meta), meta_len))
    }

    /// List objects with their meta_len values (index-only, zero data reads).
    /// Returns a list of (ObjectMeta, meta_len) tuples.
    #[pyo3(signature = (prefix=None))]
    fn list_with_meta(&self, prefix: Option<&str>) -> Vec<(ObjectMeta, u16)> {
        let prefix_path = prefix.map(Path::from);
        self.inner
            .list_with_meta(prefix_path.as_ref())
            .into_iter()
            .map(|(m, ml)| (convert_meta(&m), ml))
            .collect()
    }

    /// Update the meta_len in the index for an existing object without
    /// touching the data.  Useful after multipart uploads that append
    /// metadata as the final part.
    fn set_meta_len(&self, key: &str, meta_len: u16) -> PyResult<()> {
        let path = Path::from(key);
        self.inner.set_meta_len(&path, meta_len).map_err(raw_err)
    }

    // ---- Index management ----

    /// Re-read the on-disk index.  Useful for read-only readers tracking a
    /// live writer.  Returns True if the index was refreshed, False if it
    /// was already up-to-date.
    fn reload_index(&self) -> PyResult<bool> {
        self.inner.reload_index().map_err(raw_err)
    }

    /// Whether the store has dirty (unflushed) changes.
    fn needs_flush(&self) -> bool {
        self.inner.needs_flush()
    }

    /// Return the full device layout for visualization: all extents + free regions.
    ///
    /// Returns a dict with keys:
    ///   - device_size, data_region_start, data_region_end (int)
    ///   - index_region_a, index_region_b, active_index_region (int)
    ///   - txn_id (int)
    ///   - extents: list of dicts (key, offset, size, padded_size, created_txn,
    ///     last_modified, uncompressed_size)
    ///   - free_regions: list of (offset, size) tuples
    fn layout_map<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let layout = self.inner.layout_map();
        let dict = pyo3::types::PyDict::new_bound(py);
        dict.set_item("device_size", layout.device_size)?;
        dict.set_item("data_region_start", layout.data_region_start)?;
        dict.set_item("data_region_end", layout.data_region_end)?;
        dict.set_item("index_region_a", layout.index_region_a)?;
        dict.set_item("index_region_b", layout.index_region_b)?;
        dict.set_item("active_index_region", layout.active_index_region)?;
        dict.set_item("txn_id", layout.txn_id)?;

        let extents_list = pyo3::types::PyList::empty_bound(py);
        for ext in &layout.extents {
            let ed = pyo3::types::PyDict::new_bound(py);
            ed.set_item("key", &ext.key)?;
            ed.set_item("offset", ext.offset)?;
            ed.set_item("size", ext.size)?;
            ed.set_item("padded_size", ext.padded_size)?;
            ed.set_item("created_txn", ext.created_txn)?;
            ed.set_item("last_modified", ext.last_modified.to_rfc3339())?;
            ed.set_item("uncompressed_size", ext.uncompressed_size)?;
            extents_list.append(ed)?;
        }
        dict.set_item("extents", extents_list)?;

        let free_list = pyo3::types::PyList::empty_bound(py);
        for (offset, size) in &layout.free_regions {
            free_list.append((*offset, *size))?;
        }
        dict.set_item("free_regions", free_list)?;

        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        let info = self.inner.device_info();
        format!(
            "Store(path={:?}, files={}, size={})",
            info.device_path, info.file_count, info.device_size
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
        // Flush on clean exit (no exception), best-effort
        if _exc_type.is_none() {
            let _ = self.inner.flush_index();
        }
        Ok(false) // do not suppress exceptions
    }
}

// -- Multipart upload --------------------------------------------------------

#[pyclass]
struct PyMultipartUpload {
    upload: Option<Box<dyn object_store::MultipartUpload>>,
}

#[pymethods]
impl PyMultipartUpload {
    /// Upload a part.
    fn put_part(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("upload already completed or aborted"))?;
        let payload = PutPayload::from(Bytes::copy_from_slice(data));
        py.allow_threads(|| {
            // MultipartUpload::put_part takes &mut self
            // We need a runtime to block_on since put_part returns a future
            let rt = Runtime::new()
                .map_err(|e| PyIOError::new_err(format!("failed to create runtime: {}", e)))?;
            rt.block_on(upload.put_part(payload)).map_err(obj_err)
        })
    }

    /// Complete the upload, assembling all parts into the final object.
    fn complete(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| PyValueError::new_err("upload already completed or aborted"))?;
        py.allow_threads(|| {
            let rt = Runtime::new()
                .map_err(|e| PyIOError::new_err(format!("failed to create runtime: {}", e)))?;
            rt.block_on(upload.complete()).map_err(obj_err)?;
            Ok(())
        })
    }

    /// Abort the upload, freeing temporary parts.
    fn abort(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| PyValueError::new_err("upload already completed or aborted"))?;
        py.allow_threads(|| {
            let rt = Runtime::new()
                .map_err(|e| PyIOError::new_err(format!("failed to create runtime: {}", e)))?;
            rt.block_on(upload.abort()).map_err(obj_err)
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_val: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        if self.upload.is_some() {
            if exc_type.is_some() {
                let _ = self.abort(py);
            } else {
                self.complete(py)?;
            }
        }
        Ok(false)
    }
}

// -- Module-level functions --------------------------------------------------

fn build_runtime() -> PyResult<Runtime> {
    Runtime::new().map_err(|e| PyIOError::new_err(format!("failed to create tokio runtime: {}", e)))
}

/// Format a new store on a file or block device.
///
/// Args:
///     path: Path to the image file or block device.
///     size: Size in bytes (required for files, optional for block devices).
///     direct_io: Use O_DIRECT (Linux only). Default False.
///     index_slot_size: Index slot size in bytes (multiple of 16 MB). Default 16 MB.
///     max_key_length: Maximum object key length in bytes. Default 1024 (S3 compatible).
///         Hard ceiling is 65536 (64 KB). Values above the shard slot ceiling
///         (index_slot_size / 256 - 98) are silently clamped down.
///         ``size`` is required when this option is set.
///     compression: Compression algorithm name (``"none"``, ``"zstd"``, ``"snappy"``,
///         ``"gzip0"`` .. ``"gzip9"``). Default ``"none"``.
#[pyfunction]
#[pyo3(signature = (path, *, size=None, direct_io=false, index_slot_size=None, max_key_length=None, compression=None))]
fn format(
    path: &str,
    size: Option<u64>,
    direct_io: bool,
    index_slot_size: Option<u64>,
    max_key_length: Option<usize>,
    compression: Option<&str>,
) -> PyResult<Store> {
    let std_path = StdPath::new(path);
    let rt = build_runtime()?;

    let max_key_len = max_key_length.unwrap_or(rawobjstr::DEFAULT_MAX_KEY_LENGTH);
    let comp = match compression {
        Some(name) => Compression::from_str_name(name)
            .map_err(|e| PyValueError::new_err(format!("unknown compression: {e}")))?,
        None => Compression::None,
    };

    let store = if index_slot_size.is_some() || comp != Compression::None {
        let device_size = size.ok_or_else(|| {
            PyValueError::new_err("size is required when index_slot_size or compression is specified")
        })?;
        RawObjectStore::format_with_options(
            std_path,
            FormatOptions {
                device_size,
                direct_io,
                index_slot_size: index_slot_size
                    .unwrap_or(rawobjstr::INDEX_REGION_SIZE),
                max_key_length: max_key_len,
                compression: comp,
            },
        )
        .map_err(raw_err)?
    } else if let Some(sz) = size {
        if max_key_len != rawobjstr::DEFAULT_MAX_KEY_LENGTH {
            RawObjectStore::format_with_options(
                std_path,
                FormatOptions {
                    device_size: sz,
                    direct_io,
                    index_slot_size: rawobjstr::INDEX_REGION_SIZE,
                    max_key_length: max_key_len,
                    compression: comp,
                },
            )
            .map_err(raw_err)?
        } else {
            RawObjectStore::format_with_size(std_path, sz, direct_io).map_err(raw_err)?
        }
    } else {
        if max_key_length.is_some() {
            return Err(PyValueError::new_err(
                "size is required when max_key_length is specified",
            ));
        }
        RawObjectStore::format(std_path, direct_io).map_err(raw_err)?
    };

    Ok(Store { inner: store, rt })
}

/// Open an existing store.
///
/// Args:
///     path: Path to the image file or block device.
///     mode: Integrity check mode. One of "default", "full_verify", "skip_verify".
///     readonly: Open in read-only mode. Default False.
#[pyfunction]
#[pyo3(signature = (path, *, mode="default", readonly=false))]
fn open(path: &str, mode: &str, readonly: bool) -> PyResult<Store> {
    let std_path = StdPath::new(path);
    let open_mode = match mode {
        "default" => OpenMode::Default,
        "full_verify" => OpenMode::FullVerify,
        "skip_verify" => OpenMode::SkipVerify,
        _ => {
            return Err(PyValueError::new_err(
                "mode must be 'default', 'full_verify', or 'skip_verify'",
            ))
        }
    };
    let rt = build_runtime()?;

    let store = if readonly {
        RawObjectStore::open_readonly_with_mode(std_path, open_mode).map_err(raw_err)?
    } else {
        RawObjectStore::open_with_mode(std_path, open_mode).map_err(raw_err)?
    };

    Ok(Store { inner: store, rt })
}

/// Modify superblock flags on a device without fully opening it.
///
/// This reads both superblock copies, applies the flag changes, and rewrites
/// both copies.  It does NOT load the index or verify extents.
///
/// Use this to toggle ``FLAG_DIRECT_IO`` or ``FLAG_WRITE_PROTECT`` without
/// going through the normal ``open()`` path (which would fail if the device
/// is write-protected).
///
/// Args:
///     path: Path to the image file or block device.
///     set_flags: Flag bits to set (OR'd in). Default 0.
///     clear_flags: Flag bits to clear (AND-NOT'd out). Default 0.
///
/// Returns the resulting flags value.
#[pyfunction]
#[pyo3(signature = (path, *, set_flags=0, clear_flags=0))]
fn modify_flags(path: &str, set_flags: u32, clear_flags: u32) -> PyResult<u32> {
    let std_path = StdPath::new(path);
    RawObjectStore::modify_flags(std_path, set_flags, clear_flags).map_err(raw_err)
}

// -- Python module -----------------------------------------------------------

#[pymodule]
fn _rawobjstr(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(format, m)?)?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(modify_flags, m)?)?;
    m.add_class::<Store>()?;
    m.add_class::<ObjectMeta>()?;
    m.add_class::<ListResult>()?;
    m.add_class::<PyDeviceInfo>()?;
    m.add_class::<PyTombstoneEntry>()?;
    m.add_class::<PyScrubReport>()?;
    m.add_class::<PyVerifyReport>()?;
    m.add_class::<PyRepairReport>()?;
    m.add_class::<PyObjectFullInfo>()?;
    m.add_class::<PyImportReport>()?;
    m.add_class::<PyExportReport>()?;
    m.add_class::<PyMultipartUpload>()?;
    m.add("FLAG_DIRECT_IO", rawobjstr::FLAG_DIRECT_IO)?;
    m.add("FLAG_WRITE_PROTECT", rawobjstr::FLAG_WRITE_PROTECT)?;
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
