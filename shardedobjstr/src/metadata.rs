//! Raw-metadata routing for sharded mode.
//!
//! When operating in cluster mode, S3 metadata operations (Content-Type,
//! user-defined headers, etc.) need the raw-store API (`put_with_meta`,
//! `head_with_meta`, `get_metadata`, `list_with_meta`, `set_meta_len`).
//! `ShardedObjectStore` does not expose these because it operates at the
//! generic `ObjectStore` trait level.
//!
//! `RawRefRegistry` is a thin index from shard ID to `Arc<RawObjectStore>`
//! that the adapter uses alongside `ShardedObjectStore` for metadata ops.
//!
//! Metadata strategy per shard type:
//!
//! - **Raw** -- body + TLV metadata stored together in a single extent.
//!   `meta_len` in the index tells the reader where body ends.
//! - **S3 / Node** -- metadata is stored as native S3 attributes via
//!   `put_opts(path, body, PutOptions { attributes })`.  On read,
//!   `GetResult.attributes` carries the metadata back.
//! - **Fs / Mem** -- `LocalFileSystem` rejects attributes, so metadata
//!   is stored in a sidecar object at `{path}.__meta__` containing raw
//!   TLV bytes.

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use futures::future::join_all;
use object_store::path::Path;
use object_store::{Attribute, Attributes};
use object_store::{ObjectMeta, PutOptions, PutPayload};
use object_store::ObjectStore as ObjStoreTrait;

use rawobjstr::store::RawObjectStore;
use rawobjstr::RawStoreError;
use tracing::warn;

use crate::{ShardedObjectStore, ShardId};
use crate::tlv::{encode_metadata, decode_metadata};

/// How a shard stores metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardKind {
    /// Raw block-device shard (metadata in extent, tracked by `RawRefRegistry`).
    Raw,
    /// S3 / R2 / child Node shard -- uses native S3 attributes.
    S3Like,
    /// Local filesystem or in-memory shard -- uses `__meta__` sidecar file.
    Sidecar,
}

/// Maps shard IDs to their `RawObjectStore` references and kinds.
///
/// Only shards backed by raw block devices have `Some` in `refs`.
/// The `kinds` vec tells metadata functions which strategy to use
/// for each shard slot.
pub struct RawRefRegistry {
    refs: Vec<Option<Arc<RawObjectStore>>>,
    kinds: Vec<ShardKind>,
}

impl RawRefRegistry {
    /// Build from lists of optional raw references and shard kinds.
    pub fn new(refs: Vec<Option<Arc<RawObjectStore>>>, kinds: Vec<ShardKind>) -> Self {
        assert_eq!(refs.len(), kinds.len(), "refs and kinds must have same length");
        Self { refs, kinds }
    }

    /// Build from one raw store (single-shard convenience).
    pub fn single(raw: Arc<RawObjectStore>) -> Self {
        Self {
            refs: vec![Some(raw)],
            kinds: vec![ShardKind::Raw],
        }
    }

    /// Get the raw reference for a shard, if any.
    pub fn get(&self, shard_id: ShardId) -> Option<&Arc<RawObjectStore>> {
        self.refs.get(shard_id).and_then(|r| r.as_ref())
    }

    /// Return the shard kind for a given shard ID.
    pub fn kind(&self, shard_id: ShardId) -> ShardKind {
        self.kinds.get(shard_id).copied().unwrap_or(ShardKind::Sidecar)
    }

    /// Return all raw store references (for flush-all, shutdown, etc.).
    pub fn all_raw(&self) -> Vec<Arc<RawObjectStore>> {
        self.refs.iter().filter_map(|r| r.clone()).collect()
    }

    /// Return the first raw store reference, if any.
    pub fn first_raw(&self) -> Option<Arc<RawObjectStore>> {
        self.refs.iter().find_map(|r| r.clone())
    }

    /// Number of shard slots (including non-raw ones).
    pub fn shard_count(&self) -> usize {
        self.refs.len()
    }
}

// -- Helper conversions ------------------------------------------------------

/// Path for the metadata sidecar file on non-raw shards.
pub fn meta_sidecar_path(location: &Path) -> Path {
    Path::from(format!("{}.__meta__", location))
}

/// Best-effort delete of the sidecar metadata file for a single shard.
/// No-op if the shard does not use sidecar storage.
pub async fn cleanup_sidecar(store: &dyn ObjStoreTrait, location: &Path, kind: ShardKind) {
    if kind == ShardKind::Sidecar {
        let meta_path = meta_sidecar_path(location);
        let _ = store.delete(&meta_path).await;
    }
}

/// Best-effort delete of a sidecar metadata file.  When `refs` is `None`,
/// assumes the shard might be Sidecar and attempts the delete anyway
/// (harmless no-op on non-sidecar shards).
pub async fn cleanup_sidecar_maybe(
    store: &dyn ObjStoreTrait,
    location: &Path,
    refs: Option<&RawRefRegistry>,
    shard_id: crate::ShardId,
) {
    let maybe_sidecar = refs
        .map(|r| r.kind(shard_id) == ShardKind::Sidecar)
        .unwrap_or(true);
    if maybe_sidecar {
        let meta_path = meta_sidecar_path(location);
        let _ = store.delete(&meta_path).await;
    }
}

/// Read metadata bytes from a specific shard, dispatching by shard kind.
///
/// Returns `Some(bytes)` when non-empty metadata is found, `None` otherwise.
/// For `Raw` shards reads from the `RawObjectStore` index, for `S3Like`
/// encodes attributes into TLV, and for `Sidecar` reads the `.__meta__` file.
pub async fn read_metadata_from_shard(
    store: &dyn ObjStoreTrait,
    shard_id: crate::ShardId,
    location: &Path,
    attributes: &object_store::Attributes,
    refs: &RawRefRegistry,
) -> Option<Vec<u8>> {
    match refs.kind(shard_id) {
        ShardKind::Raw => {
            refs.get(shard_id)
                .and_then(|raw| raw.get_metadata(location).ok())
                .map(|b| b.to_vec())
                .filter(|b| !b.is_empty())
        }
        ShardKind::S3Like => {
            let meta_map = attributes_to_meta(attributes);
            if meta_map.is_empty() {
                None
            } else {
                encode_metadata(&meta_map).ok()
                    .filter(|b| !b.is_empty())
            }
        }
        ShardKind::Sidecar => {
            let meta_path = meta_sidecar_path(location);
            match store.get(&meta_path).await {
                Ok(r) => r.bytes().await.ok()
                    .map(|b| b.to_vec())
                    .filter(|b| !b.is_empty()),
                Err(_) => None,
            }
        }
    }
}

/// Convert an `object_store::Error` into a `RawStoreError`.
fn obj_err(e: object_store::Error) -> RawStoreError {
    match e {
        object_store::Error::NotFound { path, .. } => RawStoreError::NotFound(path),
        other => RawStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            other.to_string(),
        )),
    }
}

/// Convert a metadata HashMap (from TLV decoding) into object_store Attributes.
pub fn meta_to_attributes(meta: &HashMap<String, String>) -> Attributes {
    let mut attrs = Attributes::new();
    for (key, value) in meta {
        let attr = match key.as_str() {
            "content-type" => Some(Attribute::ContentType),
            "cache-control" => Some(Attribute::CacheControl),
            "content-disposition" => Some(Attribute::ContentDisposition),
            "content-encoding" => Some(Attribute::ContentEncoding),
            "content-language" => Some(Attribute::ContentLanguage),
            k if k.starts_with("x-amz-meta-") => {
                let suffix = &k["x-amz-meta-".len()..];
                Some(Attribute::Metadata(Cow::Owned(suffix.to_string())))
            }
            other => {
                tracing::debug!(
                    key = other,
                    "dropping unrecognized metadata key for S3Like shard"
                );
                None
            }
        };
        if let Some(a) = attr {
            attrs.insert(a, value.clone().into());
        }
    }
    attrs
}

/// Convert object_store Attributes back into a metadata HashMap.
pub fn attributes_to_meta(attrs: &Attributes) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (attr, value) in attrs.iter() {
        let key = match attr {
            Attribute::ContentType => "content-type".to_string(),
            Attribute::CacheControl => "cache-control".to_string(),
            Attribute::ContentDisposition => "content-disposition".to_string(),
            Attribute::ContentEncoding => "content-encoding".to_string(),
            Attribute::ContentLanguage => "content-language".to_string(),
            Attribute::Metadata(k) => format!("x-amz-meta-{}", k),
            _ => continue,
        };
        map.insert(key, value.as_ref().to_string());
    }
    map
}

// -- Metadata-aware operations -----------------------------------------------

/// Put payload + metadata, replicating to target shards.
///
/// - Raw shards: payload + metadata in one extent via `put_with_meta`.
/// - S3Like shards: payload with native attributes via `put_opts`.
/// - Sidecar shards: payload at path, metadata at `path.__meta__`.
///
/// SYNC WARNING: This is one of two write paths.  The sibling is
/// `ShardedObjectStore::write_to_shards_inner()` in lib.rs.  Changes
/// to quorum, min_writes enforcement, shard health marking, or
/// cleanup-on-failure must be mirrored there.  See AGENTS.md
/// "Two Write Paths" for the full checklist.
///
/// **Partial-write behaviour:** When the write fans out to N shards and
/// some succeed but fewer than `min_writes`, this function returns
/// `InsufficientWrites`.  However, the shards that *did* accept the
/// payload already hold the data.  If the caller retries, the object
/// may land on a different set of targets, producing temporary
/// over-replication.  This is safe: the catalog records actual
/// placements, so `over_replication_trim()` / `repair_replication_sweep()`
/// will remove the extras.  Callers that need exactly-once semantics
/// should check the catalog before retrying.
pub async fn put_with_meta(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &Path,
    payload: Bytes,
    metadata: &[u8],
) -> Result<(), RawStoreError> {
    if cluster.is_read_only() {
        return Err(RawStoreError::ReadOnly);
    }
    let catalog = cluster.catalog();

    let targets = cluster.target_shards(location);

    // Fan out writes concurrently, matching lib.rs write_to_shards.
    let futs: Vec<_> = targets.iter().map(|&shard_id| {
        let pay = payload.clone();
        let loc = location.clone();
        async move {
            (shard_id, try_put_shard(cluster, raw_refs, shard_id, &loc, &pay, metadata).await)
        }
    }).collect();
    let outcomes = join_all(futs).await;

    let mut placed = Vec::new();
    let mut failed_shards = Vec::new();
    let mut last_err = None;

    for (shard_id, outcome) in outcomes {
        match outcome {
            Ok(()) => placed.push(shard_id),
            Err(e) => {
                failed_shards.push(shard_id);
                last_err = Some(e);
            }
        }
    }

    // Mark health and get retry targets via the shared helper.
    let tried: HashSet<ShardId> = targets.iter().copied().collect();
    let retry_shard_ids =
        cluster.mark_write_failures(&placed, &failed_shards, &tried, location);

    // If all writes failed, retry with fresh targets.
    if placed.is_empty() && !retry_shard_ids.is_empty() {
        let retry_futs: Vec<_> = retry_shard_ids.iter()
            .map(|&shard_id| {
                let pay = payload.clone();
                let loc = location.clone();
                async move {
                    (shard_id, try_put_shard(cluster, raw_refs, shard_id, &loc, &pay, metadata).await)
                }
            })
            .collect();
        for (shard_id, outcome) in join_all(retry_futs).await {
            match outcome {
                Ok(()) => placed.push(shard_id),
                Err(e) => last_err = Some(e),
            }
        }
    }

    if placed.is_empty() {
        return Err(last_err.unwrap_or_else(|| {
            RawStoreError::NotFound(location.to_string())
        }));
    }

    // Enforce min_writes: enough replicas must have landed.
    let required = cluster.min_writes();
    if placed.len() < required {
        cluster.cleanup_orphaned_writes(location, &placed).await;

        return Err(RawStoreError::InsufficientWrites {
            path: location.to_string(),
            required,
            actual: placed.len(),
        });
    }

    catalog.put(
        location.as_ref().to_string(),
        placed,
        payload.len() as u64,
        Some(crc32c::crc32c(&payload)),
        metadata.len() as u16,
    );
    cluster.emit_event(rawobjstr::event::StoreEvent::Put {
        key: location.as_ref().to_string(),
    });
    Ok(())
}

/// Try to write payload + metadata to a single shard.
async fn try_put_shard(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    shard_id: ShardId,
    location: &Path,
    payload: &Bytes,
    metadata: &[u8],
) -> Result<(), RawStoreError> {
    match raw_refs.kind(shard_id) {
        ShardKind::Raw => {
            if let Some(raw) = raw_refs.get(shard_id) {
                raw.put_with_meta(location, payload.clone(), metadata)?;
                Ok(())
            } else {
                Err(RawStoreError::NotFound(format!(
                    "shard {shard_id}: no raw ref"
                )))
            }
        }
        ShardKind::S3Like => {
            if let Some(store) = cluster.shard_store(shard_id) {
                let meta_map = decode_metadata(metadata);
                let attrs = meta_to_attributes(&meta_map);
                let opts = PutOptions {
                    attributes: attrs,
                    ..Default::default()
                };
                store
                    .put_opts(location, PutPayload::from(payload.to_vec()), opts)
                    .await
                    .map_err(obj_err)?;
                Ok(())
            } else {
                Err(RawStoreError::NotFound(format!(
                    "shard {shard_id}: no store"
                )))
            }
        }
        ShardKind::Sidecar => {
            if let Some(store) = cluster.shard_store(shard_id) {
                store
                    .put(location, PutPayload::from(payload.to_vec()))
                    .await
                    .map_err(obj_err)?;
                if !metadata.is_empty() {
                    let meta_path = meta_sidecar_path(location);
                    store
                        .put(&meta_path, PutPayload::from(metadata.to_vec()))
                        .await
                        .map_err(obj_err)?;
                }
                Ok(())
            } else {
                Err(RawStoreError::NotFound(format!(
                    "shard {shard_id}: no store"
                )))
            }
        }
    }
}

/// Head with meta_len, routed to the correct shard via catalog.
///
/// For raw shards, `meta_len` is read from the extent index on disk.
/// For non-raw shards (S3Like, Sidecar), `meta_len` is retrieved from
/// the catalog where `put_with_meta` stored it. Falls back to 0 when
/// the catalog has no entry (e.g. plain `put` without metadata).
pub async fn head_with_meta(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &Path,
) -> Result<(ObjectMeta, u16), RawStoreError> {
    // Hide delete markers from external callers.
    if ShardedObjectStore::is_delete_marker(location.as_ref()) {
        return Err(RawStoreError::NotFound(location.to_string()));
    }
    let read_order = cluster.read_shard_order(location);
    let mut last_err = None;

    for shard_id in &read_order {
        if let Some(raw) = raw_refs.get(*shard_id) {
            match raw.head_with_meta(location) {
                Ok(result) => return Ok(result),
                Err(RawStoreError::NotFound(_)) => continue,
                Err(e) => last_err = Some(e),
            }
        } else if let Some(store) = cluster.shard_store(*shard_id) {
            // Non-raw shard: head via ObjectStore, read meta_len from catalog.
            match store.head(location).await {
                Ok(obj_meta) => {
                    let meta_len = cluster
                        .catalog()
                        .get(location.as_ref())
                        .map(|e| e.meta_len)
                        .unwrap_or(0);
                    return Ok((obj_meta, meta_len));
                }
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => last_err = Some(obj_err(e)),
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        RawStoreError::NotFound(location.to_string())
    }))
}

/// Read just the metadata bytes for an object.
///
/// - Raw shards: read from the trailing metadata in the extent.
/// - S3Like shards: GET the object, extract native attributes, encode as TLV.
/// - Sidecar shards: read from the `__meta__` sidecar file.
pub async fn get_metadata(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &Path,
) -> Result<Bytes, RawStoreError> {
    // Hide delete markers from external callers.
    if ShardedObjectStore::is_delete_marker(location.as_ref()) {
        return Err(RawStoreError::NotFound(location.to_string()));
    }
    let read_order = cluster.read_shard_order(location);

    for shard_id in &read_order {
        if let Some(raw) = raw_refs.get(*shard_id) {
            match raw.get_metadata(location) {
                Ok(data) => return Ok(data),
                Err(RawStoreError::NotFound(_)) => continue,
                Err(e) => {
                    warn!(
                        shard_id,
                        key = location.as_ref(),
                        error = %e,
                        "get_metadata: raw shard error, trying next"
                    );
                    continue;
                }
            }
        } else if let Some(store) = cluster.shard_store(*shard_id) {
            match raw_refs.kind(*shard_id) {
                ShardKind::S3Like => {
                    // GET the object and extract native attributes
                    match store.get(location).await {
                        Ok(result) => {
                            let meta = attributes_to_meta(&result.attributes);
                            if meta.is_empty() {
                                return Ok(Bytes::new());
                            }
                            let encoded = encode_metadata(&meta).unwrap_or_default();
                            return Ok(Bytes::from(encoded));
                        }
                        Err(object_store::Error::NotFound { .. }) => continue,
                        Err(e) => {
                            warn!(
                                shard_id,
                                key = location.as_ref(),
                                error = %e,
                                "get_metadata: S3Like shard error, trying next"
                            );
                            continue;
                        }
                    }
                }
                ShardKind::Sidecar => {
                    let meta_path = meta_sidecar_path(location);
                    match store.get(&meta_path).await {
                        Ok(result) => {
                            let bytes = result.bytes().await.map_err(|e| {
                                RawStoreError::Io(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e.to_string(),
                                ))
                            })?;
                            return Ok(bytes);
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            // Sidecar file missing -- try next shard
                            // in case a replica has the metadata.
                            continue;
                        }
                        Err(e) => {
                            warn!(
                                shard_id,
                                key = location.as_ref(),
                                error = %e,
                                "get_metadata: sidecar shard error, trying next"
                            );
                            continue;
                        }
                    }
                }
                ShardKind::Raw => unreachable!(),
            }
        }
    }

    Err(RawStoreError::NotFound(location.to_string()))
}

/// List objects under a prefix with meta_len, merged across all shards.
///
/// - Raw shards: uses `list_with_meta` for meta_len.
/// - S3Like shards: list via ObjectStore (no sidecar filtering).
/// - Sidecar shards: list via ObjectStore, `__meta__` entries excluded.
pub async fn list_with_meta(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    prefix: Option<&Path>,
) -> Vec<(ObjectMeta, u16)> {
    let mut seen = HashSet::new();
    let mut results = Vec::new();

    for shard_id in 0..raw_refs.shard_count() {
        if let Some(raw) = raw_refs.get(shard_id) {
            for item in raw.list_with_meta(prefix) {
                let key = item.0.location.as_ref().to_string();
                // Hide delete markers from external callers.
                if ShardedObjectStore::is_delete_marker(&key) {
                    continue;
                }
                if seen.insert(key) {
                    results.push(item);
                }
            }
        } else if let Some(store) = cluster.shard_store(shard_id) {
            let stream = store.list(prefix);
            let items: Vec<ObjectMeta> = stream.try_collect().await.unwrap_or_default();
            let filter_sidecars = raw_refs.kind(shard_id) == ShardKind::Sidecar;
            for item in items {
                let key = item.location.as_ref().to_string();
                if filter_sidecars && key.ends_with(".__meta__") {
                    continue;
                }
                // Hide delete markers from external callers.
                if ShardedObjectStore::is_delete_marker(&key) {
                    continue;
                }
                if seen.insert(key) {
                    results.push((item, 0));
                }
            }
        }
    }

    results
}

/// Set meta_len on the shard that holds the object.
/// No-op for S3Like and Sidecar shards (metadata is external).
pub async fn set_meta_len(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &Path,
    meta_len: u16,
) -> Result<(), RawStoreError> {
    if cluster.is_read_only() {
        return Err(RawStoreError::ReadOnly);
    }
    let read_order = cluster.read_shard_order(location);

    for shard_id in &read_order {
        if let Some(raw) = raw_refs.get(*shard_id) {
            if raw.set_meta_len(location, meta_len).is_ok() {
                return Ok(());
            }
        } else if cluster.shard_store(*shard_id).is_some() {
            // Non-raw shard: meta_len not tracked (sidecar is the truth)
            return Ok(());
        }
    }

    Err(RawStoreError::NotFound(location.to_string()))
}

/// Put payload + metadata from an open file, replicating to target shards.
///
/// The file must already contain payload bytes followed by metadata bytes.
///
/// When all target shards are Raw, the file is passed directly to each
/// store's streaming `put_with_meta_from_file` (peak heap ~1 MB).
/// For mixed or non-raw shards the file is read into memory and split
/// into payload and metadata for the per-shard `put_with_meta` calls.
pub async fn put_with_meta_from_file(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &Path,
    file: &mut std::fs::File,
    meta_len: u16,
) -> Result<(), RawStoreError> {
    use std::io::{Read, Seek, SeekFrom};

    let targets = cluster.target_shards(location);

    // Fast path: all target shards are Raw -- stream from the file
    // without reading the whole thing into memory.
    let all_raw = targets.iter().all(|&sid| raw_refs.kind(sid) == ShardKind::Raw);
    if all_raw {
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let body_len = (file_len as usize).saturating_sub(meta_len as usize);

        // Read the metadata suffix for the catalog CRC+size computation.
        // Metadata is small (< 64 KB), so this is fine.
        file.seek(SeekFrom::Start(body_len as u64)).map_err(RawStoreError::Io)?;
        let mut meta_buf = vec![0u8; meta_len as usize];
        file.read_exact(&mut meta_buf).map_err(RawStoreError::Io)?;

        // Compute CRC over the body portion for the catalog by reading
        // in 1 MB chunks.  This avoids buffering the full payload.
        file.seek(SeekFrom::Start(0)).map_err(RawStoreError::Io)?;
        let mut crc: u32 = 0;
        let mut remaining = body_len;
        let mut crc_buf = vec![0u8; 1024 * 1024];
        while remaining > 0 {
            let to_read = remaining.min(crc_buf.len());
            let n = file.read(&mut crc_buf[..to_read]).map_err(RawStoreError::Io)?;
            if n == 0 { break; }
            crc = crc32c::crc32c_append(crc, &crc_buf[..n]);
            remaining -= n;
        }

        let mut placed = Vec::new();
        let mut failed_shards = Vec::new();
        let mut last_err = None;

        for &shard_id in &targets {
            if let Some(raw) = raw_refs.get(shard_id) {
                file.seek(SeekFrom::Start(0)).map_err(RawStoreError::Io)?;
                match raw.put_with_meta_from_file(location, file, meta_len) {
                    Ok(()) => placed.push(shard_id),
                    Err(e) => {
                        failed_shards.push(shard_id);
                        last_err = Some(e);
                    }
                }
            }
        }

        // Mark health via the shared helper and retry with fresh targets
        // if all initial writes failed.
        let tried: HashSet<ShardId> = targets.iter().copied().collect();
        let retry_shard_ids =
            cluster.mark_write_failures(&placed, &failed_shards, &tried, location);

        if placed.is_empty() && !retry_shard_ids.is_empty() {
            for &shard_id in &retry_shard_ids {
                if let Some(raw) = raw_refs.get(shard_id) {
                    file.seek(SeekFrom::Start(0)).map_err(RawStoreError::Io)?;
                    match raw.put_with_meta_from_file(location, file, meta_len) {
                        Ok(()) => placed.push(shard_id),
                        Err(e) => { last_err = Some(e); }
                    }
                }
            }
        }

        if placed.is_empty() {
            return Err(last_err.unwrap_or_else(|| {
                RawStoreError::NotFound(location.to_string())
            }));
        }

        // Enforce min_writes: enough replicas must have landed.
        let required = cluster.min_writes();
        if placed.len() < required {
            cluster.cleanup_orphaned_writes(location, &placed).await;
            return Err(RawStoreError::InsufficientWrites {
                path: location.to_string(),
                required,
                actual: placed.len(),
            });
        }

        cluster.catalog().put(
            location.as_ref().to_string(),
            placed,
            body_len as u64,
            Some(crc),
            meta_len,
        );
        cluster.emit_event(rawobjstr::event::StoreEvent::Put {
            key: location.as_ref().to_string(),
        });
        return Ok(());
    }

    // Fallback: non-raw shards need payload + metadata separated.
    // This reads the entire file into memory. Warn for large files.
    file.seek(SeekFrom::Start(0)).map_err(RawStoreError::Io)?;
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if file_len > 64 * 1024 * 1024 {
        warn!(
            path = location.as_ref(),
            size_mb = file_len / (1024 * 1024),
            "put_with_meta_from_file: falling back to full-file read for mixed-shard cluster"
        );
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(RawStoreError::Io)?;

    let payload_end = buf.len().saturating_sub(meta_len as usize);
    let payload = Bytes::from(buf[..payload_end].to_vec());
    let metadata = &buf[payload_end..];

    put_with_meta(cluster, raw_refs, location, payload, metadata).await
}

/// Delete the metadata sidecar for an object on Sidecar shards.
/// Skipped for Raw and S3Like shards (no sidecar exists).
/// Best-effort: errors are silently ignored (sidecar may not exist).
///
/// NOTE: This does NOT enforce min_writes because it is ancillary cleanup
/// called alongside the object deletion path (put_delete_marker in lib.rs),
/// which already enforces min_writes / delete_requires_min_writes.
pub async fn delete_sidecar(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &Path,
) {
    if cluster.is_read_only() {
        return;
    }
    let targets = cluster.target_shards(location);
    let meta_path = meta_sidecar_path(location);
    for &shard_id in &targets {
        if raw_refs.kind(shard_id) == ShardKind::Sidecar {
            if let Some(store) = cluster.shard_store(shard_id) {
                let _ = store.delete(&meta_path).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn meta_to_attributes_known_fields() {
        let mut meta = HashMap::new();
        meta.insert("content-type".to_string(), "application/json".to_string());
        meta.insert("cache-control".to_string(), "max-age=3600".to_string());
        meta.insert("content-disposition".to_string(), "inline".to_string());
        meta.insert("content-encoding".to_string(), "gzip".to_string());
        meta.insert("content-language".to_string(), "en".to_string());

        let attrs = meta_to_attributes(&meta);

        assert_eq!(
            attrs.get(&Attribute::ContentType).map(|v| v.as_ref()),
            Some("application/json"),
        );
        assert_eq!(
            attrs.get(&Attribute::CacheControl).map(|v| v.as_ref()),
            Some("max-age=3600"),
        );
        assert_eq!(
            attrs.get(&Attribute::ContentDisposition).map(|v| v.as_ref()),
            Some("inline"),
        );
        assert_eq!(
            attrs.get(&Attribute::ContentEncoding).map(|v| v.as_ref()),
            Some("gzip"),
        );
        assert_eq!(
            attrs.get(&Attribute::ContentLanguage).map(|v| v.as_ref()),
            Some("en"),
        );
    }

    #[test]
    fn meta_to_attributes_custom_amz_meta() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-meta-color".to_string(), "blue".to_string());
        meta.insert("x-amz-meta-version".to_string(), "42".to_string());

        let attrs = meta_to_attributes(&meta);

        assert_eq!(
            attrs.get(&Attribute::Metadata(Cow::Borrowed("color"))).map(|v| v.as_ref()),
            Some("blue"),
        );
        assert_eq!(
            attrs.get(&Attribute::Metadata(Cow::Borrowed("version"))).map(|v| v.as_ref()),
            Some("42"),
        );
    }

    #[test]
    fn meta_to_attributes_drops_unrecognized_keys() {
        let mut meta = HashMap::new();
        meta.insert("x-custom-header".to_string(), "value".to_string());
        meta.insert("content-type".to_string(), "text/plain".to_string());

        let attrs = meta_to_attributes(&meta);

        // Only content-type should survive; x-custom-header is dropped.
        assert_eq!(
            attrs.get(&Attribute::ContentType).map(|v| v.as_ref()),
            Some("text/plain"),
        );
        // Count attributes: should be exactly 1
        assert_eq!(attrs.iter().count(), 1);
    }

    #[test]
    fn attributes_to_meta_known_fields() {
        let mut attrs = Attributes::new();
        attrs.insert(Attribute::ContentType, "text/html".into());
        attrs.insert(Attribute::CacheControl, "no-cache".into());
        attrs.insert(Attribute::ContentDisposition, "attachment".into());
        attrs.insert(Attribute::ContentEncoding, "br".into());
        attrs.insert(Attribute::ContentLanguage, "fr".into());

        let meta = attributes_to_meta(&attrs);

        assert_eq!(meta.get("content-type").map(|s| s.as_str()), Some("text/html"));
        assert_eq!(meta.get("cache-control").map(|s| s.as_str()), Some("no-cache"));
        assert_eq!(meta.get("content-disposition").map(|s| s.as_str()), Some("attachment"));
        assert_eq!(meta.get("content-encoding").map(|s| s.as_str()), Some("br"));
        assert_eq!(meta.get("content-language").map(|s| s.as_str()), Some("fr"));
        assert_eq!(meta.len(), 5);
    }

    #[test]
    fn attributes_to_meta_custom_metadata() {
        let mut attrs = Attributes::new();
        attrs.insert(Attribute::Metadata(Cow::Owned("color".to_string())), "red".into());
        attrs.insert(Attribute::Metadata(Cow::Owned("size".to_string())), "large".into());

        let meta = attributes_to_meta(&attrs);

        assert_eq!(meta.get("x-amz-meta-color").map(|s| s.as_str()), Some("red"));
        assert_eq!(meta.get("x-amz-meta-size").map(|s| s.as_str()), Some("large"));
        assert_eq!(meta.len(), 2);
    }

    #[test]
    fn roundtrip_meta_to_attributes_to_meta() {
        let mut original = HashMap::new();
        original.insert("content-type".to_string(), "image/png".to_string());
        original.insert("x-amz-meta-author".to_string(), "alice".to_string());

        let attrs = meta_to_attributes(&original);
        let roundtripped = attributes_to_meta(&attrs);

        assert_eq!(original, roundtripped);
    }

    #[test]
    fn empty_meta_to_attributes() {
        let meta = HashMap::new();
        let attrs = meta_to_attributes(&meta);
        assert_eq!(attrs.iter().count(), 0);
    }

    #[test]
    fn empty_attributes_to_meta() {
        let attrs = Attributes::new();
        let meta = attributes_to_meta(&attrs);
        assert!(meta.is_empty());
    }

    #[test]
    fn meta_sidecar_path_format() {
        let loc = Path::from("data/file.db");
        let sidecar = meta_sidecar_path(&loc);
        assert_eq!(sidecar.as_ref(), "data/file.db.__meta__");
    }
}
