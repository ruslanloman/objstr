//! Adapter: implements s3s::S3 trait backed by one or more ObjectStores.
//!
//! This module bridges the s3s framework (full S3 protocol) with
//! backend stores via the `StoreBackend` enum.  When backed by a
//! single `RawObjectStore` or a `ShardedObjectStore` (which wraps
//! multiple raw stores) the raw metadata-aware APIs are used.

use std::collections::{HashMap, HashSet};
use std::io::Seek;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Timelike;
use futures::{TryStreamExt, StreamExt};
use object_store::{GetOptions, GetRange, ObjectStore};
use object_store::path::Path;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3Request, S3Response, S3Result, S3};

use time::OffsetDateTime;

use rawobjstr::store::RawObjectStore;
use rawobjstr::RawStoreError;

use shardedobjstr::ShardedObjectStore;
use shardedobjstr::metadata::RawRefRegistry;

// -- SyncStream helper -------------------------------------------------------

/// Wraps a `Send`-only stream so it can satisfy `Send + Sync` bounds.
///
/// This is safe because `Stream` is only polled through `Pin<&mut Self>`,
/// so `Sync` is never actually exercised at runtime.  The s3s crate
/// requires `Send + Sync` on `StreamingBlob::wrap`, even though the
/// stream is always consumed by a single task.
struct SyncStream<S>(S);

// SAFETY: SyncStream is only used for streams consumed by a single task.
// The Sync bound is required by the s3s API but never used across threads.
unsafe impl<S: Send> Sync for SyncStream<S> {}

impl<S: futures::Stream + Unpin + Send> futures::Stream for SyncStream<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.0).poll_next(cx)
    }
}

// -- StoreBackend ------------------------------------------------------------

/// Unified backend the adapter uses for all I/O.
///
/// In single-store mode the adapter wraps one `RawObjectStore`.
/// In cluster mode a `ShardedObjectStore` routes across many stores
/// while a `RawRefRegistry` provides raw-metadata access.
pub enum StoreBackend {
    /// Single raw block-device store.
    Raw(Arc<RawObjectStore>),
    /// Sharded cluster store + raw references for metadata ops.
    Sharded(Arc<ShardedObjectStore>, Arc<RawRefRegistry>),
}

impl StoreBackend {
    /// Return a reference suitable for generic `ObjectStore` operations.
    pub fn obj_store(&self) -> &dyn ObjectStore {
        match self {
            StoreBackend::Raw(raw) => raw.as_ref(),
            StoreBackend::Sharded(cluster, _) => cluster.as_ref(),
        }
    }

    /// Put body + metadata using the raw metadata API.
    pub async fn put_with_meta(
        &self,
        path: &Path,
        body: Bytes,
        meta: &[u8],
    ) -> Result<(), RawStoreError> {
        match self {
            StoreBackend::Raw(raw) => raw.put_with_meta(path, body, meta),
            StoreBackend::Sharded(cluster, refs) => {
                shardedobjstr::metadata::put_with_meta(cluster, refs, path, body, meta).await
            }
        }
    }

    /// Head with metadata length.
    pub async fn head_with_meta(
        &self,
        path: &Path,
    ) -> Result<(object_store::ObjectMeta, u16), RawStoreError> {
        match self {
            StoreBackend::Raw(raw) => raw.head_with_meta(path),
            StoreBackend::Sharded(cluster, refs) => {
                shardedobjstr::metadata::head_with_meta(cluster, refs, path).await
            }
        }
    }

    /// Read raw metadata bytes for an object.
    pub async fn get_metadata(&self, path: &Path) -> Result<Bytes, RawStoreError> {
        match self {
            StoreBackend::Raw(raw) => raw.get_metadata(path),
            StoreBackend::Sharded(cluster, refs) => {
                shardedobjstr::metadata::get_metadata(cluster, refs, path).await
            }
        }
    }

    /// List objects under a prefix with per-object meta_len.
    pub async fn list_with_meta(
        &self,
        prefix: Option<&Path>,
    ) -> Vec<(object_store::ObjectMeta, u16)> {
        match self {
            StoreBackend::Raw(raw) => raw.list_with_meta(prefix),
            StoreBackend::Sharded(cluster, refs) => {
                shardedobjstr::metadata::list_with_meta(cluster, refs, prefix).await
            }
        }
    }

    /// Set meta_len in the index for a given path.
    pub async fn set_meta_len(
        &self,
        path: &Path,
        meta_len: u16,
    ) -> Result<(), RawStoreError> {
        match self {
            StoreBackend::Raw(raw) => raw.set_meta_len(path, meta_len),
            StoreBackend::Sharded(cluster, refs) => {
                shardedobjstr::metadata::set_meta_len(cluster, refs, path, meta_len).await
            }
        }
    }

    /// Put body + metadata from an already-open file.
    ///
    /// The file must contain body bytes followed by metadata bytes
    /// (total = body + meta_len bytes).  Avoids the extra allocation
    /// that `put_with_meta` incurs when concatenating body + metadata.
    pub async fn put_with_meta_from_file(
        &self,
        path: &Path,
        file: &mut std::fs::File,
        meta_len: u16,
    ) -> Result<(), RawStoreError> {
        match self {
            StoreBackend::Raw(raw) => raw.put_with_meta_from_file(path, file, meta_len),
            StoreBackend::Sharded(cluster, refs) => {
                shardedobjstr::metadata::put_with_meta_from_file(cluster, refs, path, file, meta_len).await
            }
        }
    }

    /// Delete metadata sidecar files for an object (non-raw shards only).
    pub async fn delete_sidecar(&self, path: &Path) {
        if let StoreBackend::Sharded(cluster, refs) = self {
            shardedobjstr::metadata::delete_sidecar(cluster, refs, path).await;
        }
    }
}

// -- Internal metadata helpers -----------------------------------------------

/// Metadata is stored as variable-length TLV bytes appended after the object
/// body.  The raw store records `meta_len` in its index so the split point
/// is always known without markers or fixed sizes.

/// Construct a path without percent-encoding the input.
///
/// `Path::from` percent-encodes characters like `%`, `[`, `~`, etc.
/// This causes round-trip mismatches: a key `b%ar` gets stored as
/// `b%25ar`, listed as `b%25ar`, and then a delete for `b%25ar`
/// double-encodes to `b%2525ar` — not found.
///
/// `Path::parse` stores the string verbatim (no encoding), which
/// ensures keys survive a put → list → delete round-trip.
fn raw_path(s: String) -> object_store::path::Path {
    object_store::path::Path::parse(&s).unwrap_or_else(|_| object_store::path::Path::from(s))
}

/// Sentinel appended to keys that end with `/` so the trailing slash is not
/// stripped by `object_store::path::Path::parse`.
const DIR_MARK: &str = "__DIRMARK__";

/// Precomputed suffix used to detect DIR_MARK in stored keys.
const DIR_MARK_SUFFIX: &str = "/__DIRMARK__";

fn obj_key(bucket: &str, key: &str) -> object_store::path::Path {
    if key.ends_with('/') {
        raw_path(format!("{bucket}/{key}{DIR_MARK}"))
    } else {
        raw_path(format!("{bucket}/{key}"))
    }
}

fn compute_md5(data: &[u8]) -> String {
    format!("{:x}", md5::compute(data))
}

/// Reject keys that contain path-traversal components (`..`).
///
/// S3 keys are flat strings, but when the backend is a local filesystem
/// a `..` component could escape the prefix directory.  `object_store`'s
/// `Path::parse` normalizes `/` but does NOT strip `..`, so we reject
/// them explicitly at the S3 protocol boundary.
fn validate_key(key: &str) -> S3Result<()> {
    for component in key.split('/') {
        if component == ".." {
            return Err(s3_error!(InvalidArgument, "key must not contain '..' path components"));
        }
        if component == DIR_MARK {
            return Err(s3_error!(InvalidArgument, "key must not contain reserved component '__DIRMARK__'"));
        }
    }
    Ok(())
}

// -- TLV metadata encoding (re-exported) -------------------------------------

pub(crate) use shardedobjstr::tlv::encode_metadata;
pub use shardedobjstr::tlv::decode_metadata;

// -- PartMeta ----------------------------------------------------------------

#[derive(Clone, Debug)]
struct PartMeta {
    size: usize,
    etag: String,
    /// Path to the temp file holding this part's data on the local filesystem.
    temp_path: std::path::PathBuf,
}

// -- Upload ------------------------------------------------------------------

#[derive(Debug)]
struct Upload {
    bucket: String,
    key: String,
    metadata: HashMap<String, String>,
    parts: HashMap<i32, PartMeta>,
    /// When this upload was created (for staleness expiry).
    created_at: std::time::Instant,
}

impl Drop for Upload {
    fn drop(&mut self) {
        // Clean up temp files when an upload is removed (complete, abort, or expiry).
        for (_, part) in self.parts.drain() {
            let _ = std::fs::remove_file(&part.temp_path);
        }
    }
}

/// Maximum number of concurrent in-progress multipart uploads.
/// Prevents memory exhaustion from clients that create uploads without
/// completing or aborting them.
const MAX_CONCURRENT_UPLOADS: usize = 1000;

/// Uploads older than this are considered stale and automatically purged
/// when a new upload is created (lazy reaper).
const UPLOAD_EXPIRY: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Default maximum object body size accepted by put_object (5 GB, matching
/// S3's single-PUT limit).  Also used as the per-part cap in upload_part
/// and upload_part_copy.
const DEFAULT_MAX_BODY_SIZE: u64 = 5 * 1024 * 1024 * 1024;

// -- ObjectStoreS3Adapter ----------------------------------------------------

/// S3 adapter wrapping one or more object stores.
pub struct ObjectStoreS3Adapter {
    pub backend: StoreBackend,
    /// In-memory bucket registry (Arc-wrapped for external access).
    buckets: Arc<RwLock<HashSet<String>>>,
    /// In-progress multipart uploads: upload_id -> Upload.
    uploads: RwLock<HashMap<String, Upload>>,
    /// Per-bucket location constraint (from CreateBucket).
    locations: RwLock<HashMap<String, String>>,
    /// Cached per-bucket stats for HEAD bucket: (object_count, bytes_used, computed_at).
    /// Avoids scanning all objects on every HEAD request.  Entries older than
    /// `BUCKET_STATS_TTL` are recomputed on next access.
    bucket_stats_cache: RwLock<HashMap<String, (usize, u64, std::time::Instant)>>,
    /// When `true`, unauthenticated requests may call `ListBuckets`.
    /// Set this when the server has no access-key auth configured so that
    /// plain HTTP clients (and tests) can list buckets without credentials.
    allow_anon_list_buckets: bool,
    /// Optional event bus for broadcasting PUT/DELETE events.
    event_bus: Option<Arc<rawobjstr::event::EventBus>>,
    /// Maximum body size (bytes) accepted by put_object / upload_part.
    /// Set to 0 for unlimited.
    max_body_size: u64,
    /// How long before an in-progress multipart upload is considered stale.
    /// Defaults to `UPLOAD_EXPIRY` (24 h).  Override with
    /// `set_upload_expiry` for integration tests.
    upload_expiry: std::time::Duration,
}

impl ObjectStoreS3Adapter {
    fn build(backend: StoreBackend, buckets: HashSet<String>) -> Self {
        Self {
            backend,
            buckets: Arc::new(RwLock::new(buckets)),
            uploads: RwLock::new(HashMap::new()),
            locations: RwLock::new(HashMap::new()),
            bucket_stats_cache: RwLock::new(HashMap::new()),
            allow_anon_list_buckets: false,
            event_bus: None,
            max_body_size: DEFAULT_MAX_BODY_SIZE,
            upload_expiry: UPLOAD_EXPIRY,
        }
    }

    pub fn new(store: Arc<RawObjectStore>) -> Self {
        Self::build(StoreBackend::Raw(store), HashSet::new())
    }

    /// Create the adapter with an initial bucket pre-registered.
    pub fn with_bucket(store: Arc<RawObjectStore>, bucket: &str) -> Self {
        Self::build(StoreBackend::Raw(store), HashSet::from([bucket.to_string()]))
    }

    /// Create the adapter backed by a sharded cluster store.
    pub fn new_sharded(cluster: Arc<ShardedObjectStore>, raw_refs: Arc<RawRefRegistry>) -> Self {
        Self::build(StoreBackend::Sharded(cluster, raw_refs), HashSet::new())
    }

    /// Create the adapter backed by a sharded cluster store with a default bucket.
    pub fn with_bucket_sharded(
        cluster: Arc<ShardedObjectStore>,
        raw_refs: Arc<RawRefRegistry>,
        bucket: &str,
    ) -> Self {
        Self::build(StoreBackend::Sharded(cluster, raw_refs), HashSet::from([bucket.to_string()]))
    }

    /// Allow unauthenticated `ListBuckets` requests to return the full list.
    ///
    /// Call this when the server runs without access-key auth so that plain
    /// HTTP clients (e.g. tests, local dev) can see the bucket list.
    pub fn set_allow_anon_list_buckets(&mut self, allow: bool) {
        self.allow_anon_list_buckets = allow;
    }

    /// Set the event bus for broadcasting PUT/DELETE events.
    pub fn set_event_bus(&mut self, bus: Arc<rawobjstr::event::EventBus>) {
        self.event_bus = Some(bus);
    }

    /// Override the multipart upload expiry duration (default 24 h).
    /// Intended for integration tests that need to verify the lazy reaper.
    pub fn set_upload_expiry(&mut self, dur: std::time::Duration) {
        self.upload_expiry = dur;
    }

    /// Return a shared handle to the bucket registry.
    pub fn bucket_registry(&self) -> Arc<RwLock<HashSet<String>>> {
        Arc::clone(&self.buckets)
    }

    /// Replace the entire in-memory bucket set.
    pub async fn set_buckets(&self, new: HashSet<String>) {
        *self.buckets.write().await = new;
    }

    /// Discover bucket names by scanning the ObjectStore index.
    ///
    /// Every top-level path component that is not an internal prefix
    /// (`__buckets__`) is treated as a bucket.
    pub async fn rebuild_buckets_from_index(&self) -> HashSet<String> {
        let mut buckets = HashSet::new();
        let mut listing = self.backend.obj_store().list(None);
        while let Some(item) = match listing.try_next().await {
            Ok(v) => v,
            Err(e) => {
                warn!("rebuild_buckets_from_index listing error: {e}");
                None
            }
        } {
            let key = item.location.as_ref();
            if let Some(first) = key.split('/').next() {
                if !first.is_empty()
                    && first != "__buckets__"
                {
                    buckets.insert(first.to_string());
                }
            }
        }
        buckets
    }

    async fn ensure_bucket(&self, bucket: &str) -> S3Result<()> {
        let buckets = self.buckets.read().await;
        if buckets.contains(bucket) {
            Ok(())
        } else {
            Err(s3_error!(NoSuchBucket))
        }
    }

    /// Compute (object_count, bytes_used) from the index for a bucket.
    async fn compute_bucket_stats(&self, bucket: &str) -> (usize, u64) {
        let prefix = raw_path(format!("{bucket}/"));
        let bucket_pfx = format!("{bucket}/");
        let listing = self.backend.list_with_meta(Some(&prefix)).await;
        let mut obj_count: usize = 0;
        let mut bytes_used: u64 = 0;
        for (item, _meta_len) in &listing {
            let full = item.location.as_ref();
            let key = full.strip_prefix(&bucket_pfx).unwrap_or(full);
            if !key.starts_with("__buckets__/") {
                obj_count += 1;
                bytes_used += item.size;
            }
        }
        (obj_count, bytes_used)
    }

    /// Load metadata from the stored object using the raw store's metadata API.
    async fn load_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Option<HashMap<String, String>> {
        let path = obj_key(bucket, key);
        match self.backend.get_metadata(&path).await {
            Ok(data) => {
                if data.is_empty() {
                    Some(HashMap::new())
                } else {
                    Some(decode_metadata(&data))
                }
            }
            Err(_) => None,
        }
    }

    /// Build a metadata map from individual S3 standard headers and user
    /// metadata.  Shared by `put_object`, `copy_object` REPLACE, and
    /// `create_multipart_upload`.
    fn build_s3_metadata(
        content_type: Option<&str>,
        cache_control: Option<&str>,
        content_disposition: Option<&str>,
        content_encoding: Option<&str>,
        content_language: Option<&str>,
        expires: Option<&Timestamp>,
        user_metadata: Option<&HashMap<String, String>>,
    ) -> HashMap<String, String> {
        let mut meta = HashMap::new();
        if let Some(ct) = content_type {
            meta.insert("content-type".to_string(), ct.to_string());
        }
        if let Some(cc) = cache_control {
            meta.insert("cache-control".to_string(), cc.to_string());
        }
        if let Some(cd) = content_disposition {
            meta.insert("content-disposition".to_string(), cd.to_string());
        }
        if let Some(ce) = content_encoding {
            meta.insert("content-encoding".to_string(), ce.to_string());
        }
        if let Some(cl) = content_language {
            meta.insert("content-language".to_string(), cl.to_string());
        }
        if let Some(exp) = expires {
            let mut buf = Vec::new();
            if exp.format(TimestampFormat::DateTime, &mut buf).is_ok() {
                if let Ok(s) = std::str::from_utf8(&buf) {
                    meta.insert("expires".to_string(), s.to_string());
                }
            }
        }
        if let Some(user_meta) = user_metadata {
            for (k, v) in user_meta {
                meta.insert(format!("x-amz-meta-{k}"), v.clone());
            }
        }
        meta
    }

    /// Build a metadata map from PutObject headers.
    fn build_put_metadata(input: &PutObjectInput) -> HashMap<String, String> {
        Self::build_s3_metadata(
            input.content_type.as_deref(),
            input.cache_control.as_deref(),
            input.content_disposition.as_deref(),
            input.content_encoding.as_deref(),
            input.content_language.as_deref(),
            input.expires.as_ref(),
            input.metadata.as_ref(),
        )
    }

    /// Extract user metadata (x-amz-meta-*) from the stored metadata map.
    fn extract_user_metadata(
        meta: &HashMap<String, String>,
    ) -> Option<HashMap<String, String>> {
        let user: HashMap<String, String> = meta
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-meta-"))
            .map(|(k, v)| (k.strip_prefix("x-amz-meta-").unwrap().to_string(), v.clone()))
            .collect();
        if user.is_empty() {
            None
        } else {
            Some(user)
        }
    }

    /// Set x-amz-meta-* headers directly on the response HeaderMap using
    /// Latin-1 (ISO-8859-1) encoded bytes.  HTTP/1.1 header values are
    /// decoded as Latin-1 by Python's http.client (and therefore boto3),
    /// so each Unicode codepoint U+0000..U+00FF must be sent as a single
    /// byte.  This also bypasses s3s's `add_opt_metadata()` which RFC
    /// 2047-encodes non-ASCII values -- breaking clients like boto3 that
    /// do not decode RFC 2047.
    fn add_user_meta_headers(
        headers: &mut hyper::HeaderMap,
        user_metadata: Option<HashMap<String, String>>,
    ) {
        if let Some(meta) = user_metadata {
            for (key, val) in &meta {
                let header_name = format!("x-amz-meta-{}", key);
                if let Ok(name) = hyper::header::HeaderName::from_bytes(header_name.as_bytes()) {
                    // Encode as Latin-1: each char -> single byte.
                    // Characters in U+0000..U+00FF map 1:1 to Latin-1 bytes.
                    // Reject values with chars outside Latin-1 range.
                    if val.chars().any(|c| c as u32 > 0xFF) {
                        warn!(key = %key, "metadata value contains non-Latin-1 characters, skipping header");
                        continue;
                    }
                    let latin1: Vec<u8> = val.chars().map(|c| c as u8).collect();
                    if let Ok(value) = hyper::header::HeaderValue::from_bytes(&latin1) {
                        headers.insert(name, value);
                    } else {
                        warn!(key = %key, "metadata value cannot be encoded as HTTP header, skipping");
                    }
                } else {
                    warn!(key = %key, "metadata key cannot be encoded as HTTP header name, skipping");
                }
            }
        }
    }

    /// Read an object's body, raw metadata bytes, and ETag from the source path.
    ///
    /// Used by `copy_object` to fetch the source before writing to the
    /// destination.  Returns `(body, raw_meta_bytes, etag)`.
    async fn read_source_object(&self, path: &Path) -> S3Result<(Bytes, Bytes, String)> {
        let (_, src_meta_len) = self.backend.head_with_meta(path).await.map_err(|e| match e {
            RawStoreError::NotFound(_) => s3_error!(NoSuchKey),
            _ => { warn!("copy_object head error: {e}"); s3_error!(InternalError) }
        })?;
        let body = self.backend.obj_store().get(path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => s3_error!(NoSuchKey),
            _ => { warn!("copy_object read error: {e}"); s3_error!(InternalError) }
        })?.bytes().await.map_err(|e| { warn!("copy_object bytes error: {e}"); s3_error!(InternalError) })?;
        let (raw_meta, etag) = if src_meta_len > 0 {
            let raw_meta = self.backend.get_metadata(path).await.map_err(|e| {
                warn!("copy_object metadata read error: {e}");
                s3_error!(InternalError)
            })?;
            let decoded = decode_metadata(&raw_meta);
            let etag = decoded.get("etag").cloned()
                .unwrap_or_else(|| compute_md5(&body));
            (raw_meta, etag)
        } else {
            (Bytes::new(), compute_md5(&body))
        };
        Ok((body, raw_meta, etag))
    }
}

// -- S3 trait implementation -------------------------------------------------

#[async_trait]
impl S3 for ObjectStoreS3Adapter {
    // -- Bucket operations ---------------------------------------------------

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        debug!(bucket = %bucket, "create_bucket");

        // Additional DNS-name validation: reject .- and -. sequences
        // (s3s built-in check doesn't cover these)
        if bucket.contains(".-") || bucket.contains("-.") {
            return Err(s3_error!(InvalidBucketName));
        }

        let mut buckets = self.buckets.write().await;
        if buckets.contains(bucket.as_str()) {
            // Idempotent: same owner re-creating an existing bucket returns OK
            let output = CreateBucketOutput {
                location: Some(format!("/{bucket}")),
                ..Default::default()
            };
            return Ok(S3Response::new(output));
        }
        buckets.insert(bucket.clone());

        // Store location constraint if provided
        if let Some(ref config) = input.create_bucket_configuration {
            if let Some(ref loc) = config.location_constraint {
                let loc_str: &str = loc.as_str();
                if !loc_str.is_empty() {
                    self.locations
                        .write()
                        .await
                        .insert(bucket.clone(), loc_str.to_string());
                }
            }
        }

        let output = CreateBucketOutput {
            location: Some(format!("/{bucket}")),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = &req.input.bucket;
        debug!(bucket = %bucket, "delete_bucket");

        // Check if bucket has objects
        let prefix = raw_path(format!("{bucket}/"));
        let mut listing = self.backend.obj_store().list(Some(&prefix));
        if listing.try_next().await.ok().flatten().is_some() {
            return Err(s3_error!(BucketNotEmpty));
        }

        let mut buckets = self.buckets.write().await;
        if !buckets.remove(bucket.as_str()) {
            return Err(s3_error!(NoSuchBucket));
        }

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = &req.input.bucket;
        debug!(bucket = %bucket, "head_bucket");
        self.ensure_bucket(bucket).await?;

        // Return cached stats if fresh (avoids scanning all objects per HEAD).
        const BUCKET_STATS_TTL: std::time::Duration = std::time::Duration::from_secs(5);

        // Fast path: check freshness under a read lock so concurrent HEAD
        // requests for already-cached buckets are not blocked.
        {
            let cache = self.bucket_stats_cache.read().await;
            if let Some(&(count, bytes, ref when)) = cache.get(bucket.as_str()) {
                if when.elapsed() < BUCKET_STATS_TTL {
                    drop(cache);
                    let mut resp = S3Response::new(HeadBucketOutput::default());
                    resp.headers.insert(
                        "x-rgw-object-count",
                        hyper::header::HeaderValue::from(count as u64),
                    );
                    resp.headers.insert(
                        "x-rgw-bytes-used",
                        hyper::header::HeaderValue::from(bytes),
                    );
                    resp.headers.insert(
                        "x-rgw-quota-max-buckets",
                        hyper::header::HeaderValue::from_static("1000"),
                    );
                    resp.headers.insert(
                        "x-amz-bucket-region",
                        hyper::header::HeaderValue::from_static("us-east-1"),
                    );
                    return Ok(resp);
                }
            }
        }

        // Cache miss or stale -- recompute outside any lock to avoid
        // blocking concurrent reads on the expensive listing scan.
        let (obj_count, bytes_used) = self.compute_bucket_stats(bucket).await;

        {
            let mut cache = self.bucket_stats_cache.write().await;
            cache.insert(bucket.clone(), (obj_count, bytes_used, std::time::Instant::now()));
        }

        let mut resp = S3Response::new(HeadBucketOutput::default());
        resp.headers.insert(
            "x-rgw-object-count",
            hyper::header::HeaderValue::from(obj_count as u64),
        );
        resp.headers.insert(
            "x-rgw-bytes-used",
            hyper::header::HeaderValue::from(bytes_used),
        );
        resp.headers.insert(
            "x-rgw-quota-max-buckets",
            hyper::header::HeaderValue::from_static("1000"),
        );
        resp.headers.insert(
            "x-amz-bucket-region",
            hyper::header::HeaderValue::from_static("us-east-1"),
        );
        Ok(resp)
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let input = req.input;
        debug!("list_buckets");
        // Anonymous request: return empty list unless the server explicitly
        // permits anon listing (i.e. no access-key auth is configured).
        if req.credentials.is_none() && !self.allow_anon_list_buckets {
            return Ok(S3Response::new(ListBucketsOutput {
                buckets: Some(vec![]),
                owner: None,
                ..Default::default()
            }));
        }
        let buckets_set = self.buckets.read().await;
        let mut names: Vec<&String> = buckets_set.iter().collect();
        names.sort();

        // Apply continuation token filter
        let cont_token = input.continuation_token.as_deref().unwrap_or("");
        if !cont_token.is_empty() {
            names.retain(|n| n.as_str() > cont_token);
        }

        let max_buckets = input.max_buckets.unwrap_or(i32::MAX) as usize;
        let truncated = names.len() > max_buckets;
        let page: Vec<&String> = names.into_iter().take(max_buckets).collect();

        let next_token = if truncated {
            page.last().map(|n| (*n).clone())
        } else {
            None
        };

        let buckets: Vec<Bucket> = page
            .iter()
            .map(|name| Bucket {
                name: Some((*name).clone()),
                creation_date: Some(Timestamp::from(
                    std::time::SystemTime::now(),
                )),
                bucket_region: Some("us-east-1".to_string()),
            })
            .collect();

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Owner {
                display_name: Some("rawobjstr".to_string()),
                id: Some("rawobjstr".to_string()),
            }),
            continuation_token: next_token,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // -- Object operations ---------------------------------------------------

    // TODO: put_object is ~160 lines with a confusing early-return for large
    // objects.  Consider splitting into put_small / put_large helpers.
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "put_object");
        validate_key(key)?;

        self.ensure_bucket(bucket).await?;

        // Reject early if declared content-length exceeds the cap.
        if self.max_body_size > 0 {
            if let Some(cl) = input.content_length {
                if cl > 0 && cl as u64 > self.max_body_size {
                    return Err(s3_error!(EntityTooLarge, "Object size exceeds server limit"));
                }
            }
        }

        // Save metadata sidecar before consuming body
        let meta = Self::build_put_metadata(&input);

        // For small objects (known content-length <= 8 MB), collect directly
        // into a pre-allocated Vec to avoid tempfile syscall overhead.
        // For large or unknown-size objects, spool through a tempfile to cap
        // peak memory at ~1x the object size (instead of ~2x from Vec doubling).
        const SPOOL_THRESHOLD: i64 = 8 * 1024 * 1024;

        let (body_data, md5) = match input.body {
            Some(body) => {
                let content_len = input.content_length.unwrap_or(-1);
                let mut stream = body.into_stream();
                let mut md5_ctx = md5::Context::new();

                if content_len >= 0 && content_len <= SPOOL_THRESHOLD {
                    // Small object fast path: collect directly into Vec
                    let mut buf = Vec::with_capacity(content_len as usize);
                    while let Some(chunk) = stream.try_next().await.map_err(|e| {
                        warn!("failed to read put_object body: {e}");
                        s3_error!(InternalError)
                    })? {
                        md5_ctx.consume(&chunk);
                        buf.extend_from_slice(&chunk);
                    }
                    (Bytes::from(buf), format!("{:x}", md5_ctx.compute()))
                } else {
                    // Large / unknown-size object: spool through tempfile
                    let tmp_std = tempfile::tempfile().map_err(|e| {
                        warn!("put_object: failed to create temp file: {e}");
                        s3_error!(InternalError)
                    })?;
                    let mut tmp = tokio::fs::File::from_std(tmp_std);
                    let mut total_bytes: u64 = 0;
                    while let Some(chunk) = stream.try_next().await.map_err(|e| {
                        warn!("failed to read put_object body: {e}");
                        s3_error!(InternalError)
                    })? {
                        total_bytes += chunk.len() as u64;
                        if self.max_body_size > 0 && total_bytes > self.max_body_size {
                            warn!(total_bytes, max = self.max_body_size, "put_object: body exceeds size limit");
                            return Err(s3_error!(EntityTooLarge, "Object size exceeds server limit"));
                        }
                        md5_ctx.consume(&chunk);
                        tmp.write_all(&chunk).await.map_err(|e| {
                            warn!("put_object: failed to spool body to temp file: {e}");
                            s3_error!(InternalError)
                        })?;
                    }
                    let md5 = format!("{:x}", md5_ctx.compute());

                    // Save metadata sidecar and compute encoded metadata
                    let mut meta = meta.clone();
                    meta.insert("etag".to_string(), md5.clone());
                    let meta_bytes = encode_metadata(&meta).map_err(|e| {
                        warn!("put_object: metadata too large: {e}");
                        s3_error!(InvalidArgument)
                    })?;
                    let meta_len = meta_bytes.len() as u16;

                    // Append metadata to the same temp file (body + meta in one file)
                    tmp.write_all(&meta_bytes).await.map_err(|e| {
                        warn!("put_object: failed to write metadata to temp file: {e}");
                        s3_error!(InternalError)
                    })?;
                    tmp.flush().await.map_err(|e| {
                        warn!("put_object: failed to flush temp file: {e}");
                        s3_error!(InternalError)
                    })?;

                    // Convert to std::fs::File and pass to backend -- avoids
                    // reading the entire file back into memory.
                    let mut std_file = tmp.into_std().await;
                    std_file.seek(std::io::SeekFrom::Start(0)).map_err(|e| {
                        warn!("put_object: failed to seek temp file: {e}");
                        s3_error!(InternalError)
                    })?;

                    let path = obj_key(bucket, key);
                    self.backend.put_with_meta_from_file(&path, &mut std_file, meta_len).await.map_err(|e| {
                        warn!("put_object store error: {e}");
                        s3_error!(InternalError)
                    })?;

                    // Emit PUT event
                    if let Some(ref bus) = self.event_bus {
                        bus.emit_put(key);
                    }

                    // Invalidate cached bucket stats
                    self.bucket_stats_cache.write().await.remove(bucket.as_str());

                    let output = PutObjectOutput {
                        e_tag: Some(ETag::Strong(md5)),
                        ..Default::default()
                    };
                    return Ok(S3Response::new(output));
                }
            }
            None => (Bytes::new(), compute_md5(&[])),
        };

        // Write data with metadata via the raw store
        let path = obj_key(bucket, key);
        let mut meta = meta;
        meta.insert("etag".to_string(), md5.clone());
        let meta_bytes = encode_metadata(&meta).map_err(|e| {
            warn!("put_object: metadata too large: {e}");
            s3_error!(InvalidArgument)
        })?;
        self.backend.put_with_meta(&path, body_data, &meta_bytes).await.map_err(|e| {
            warn!("put_object store error: {e}");
            s3_error!(InternalError)
        })?;

        // Emit PUT event
        if let Some(ref bus) = self.event_bus {
            bus.emit_put(key);
        }

        // Invalidate cached bucket stats (object count/bytes changed)
        self.bucket_stats_cache.write().await.remove(bucket.as_str());

        let output = PutObjectOutput {
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// S3 GetObject handler.
    ///
    /// **Compressed range reads:** The underlying raw store rejects range
    /// reads on compressed objects larger than 1 GB (`COMPRESSED_RANGE_READ_MAX`)
    /// because the entire object must be decompressed in memory to serve a
    /// byte slice.  When this happens the store returns an I/O error which is
    /// mapped to S3 `InternalError`.  Clients should fetch the whole object
    /// instead, or store large objects uncompressed.
    // TODO: get_object is ~300 lines with deeply nested conditional header
    // logic.  Consider extracting check_conditional_headers(), compute_range(),
    // and stream_response() helpers.
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "get_object");
        validate_key(key)?;

        self.ensure_bucket(bucket).await?;

        let path = obj_key(bucket, key);

        // Index-only lookup: no data I/O.
        let (head_meta, _meta_len) = self.backend.head_with_meta(&path).await.map_err(|e| match e {
            RawStoreError::NotFound(_) => s3_error!(NoSuchKey),
            _ => {
                warn!("get_object store error: {e}");
                s3_error!(InternalError)
            }
        })?;

        let obj_mtime = head_meta.last_modified;
        let last_modified = Timestamp::from(std::time::SystemTime::from(obj_mtime));
        // head_meta.size is already body-only (meta_for subtracts meta_len).
        let body_size = head_meta.size as u64;

        // Read metadata (suffix read for raw shards, sidecar for non-raw).
        // Missing sidecar file is not an error -- the object simply has no metadata.
        let meta = match self.backend.get_metadata(&path).await {
            Ok(data) if !data.is_empty() => decode_metadata(&data),
            Ok(_) => HashMap::new(),
            Err(RawStoreError::NotFound(_)) => HashMap::new(),
            Err(e) => {
                warn!("get_object metadata read error: {e}");
                return Err(s3_error!(InternalError));
            }
        };

        // S3 returns 416 InvalidRange for unsatisfiable Range requests
        if let Some(ref range) = input.range {
            match range {
                Range::Int { first, .. } if *first >= body_size => {
                    return Err(s3_error!(InvalidRange));
                }
                _ => {}
            }
        }

        // Truncate obj_mtime to second precision for conditional checks.
        // HTTP dates (Last-Modified, If-Modified-Since) have only second granularity,
        // so sub-second components must be removed before comparison.
        let obj_mtime_secs = obj_mtime.with_nanosecond(0).unwrap_or(obj_mtime);

        // Check If-Modified-Since: 304 if object was NOT modified after the given date
        if let Some(ims) = &input.if_modified_since {
            let ims_stime: chrono::DateTime<chrono::Utc> =
                chrono::DateTime::from(std::time::SystemTime::from(OffsetDateTime::from(ims.clone())));
            if obj_mtime_secs <= ims_stime {
                let stored_etag = meta.get("etag").cloned();
                let mut header_map = hyper::HeaderMap::new();
                if let Some(etag) = stored_etag {
                    let etag_str = format!("\"{}\"", etag);
                    if let Ok(v) = hyper::header::HeaderValue::from_str(&etag_str) {
                        header_map.insert(hyper::header::ETAG, v);
                    }
                }
                let mut err = s3_error!(NotModified);
                err.set_headers(header_map);
                return Err(err);
            }
        }

        // Check If-Unmodified-Since: 412 if object WAS modified after the given date
        if let Some(ius) = &input.if_unmodified_since {
            let ius_stime: chrono::DateTime<chrono::Utc> =
                chrono::DateTime::from(std::time::SystemTime::from(OffsetDateTime::from(ius.clone())));
            if obj_mtime_secs > ius_stime {
                return Err(s3_error!(PreconditionFailed));
            }
        }

        let etag = meta.get("etag").cloned()
            .map(ETag::Strong)
            .unwrap_or_else(|| ETag::Strong("d41d8cd98f00b204e9800998ecf8427e".to_string()));

        // Check conditional request headers
        if let Some(if_match) = &input.if_match {
            let matches = match if_match {
                ETagCondition::Any => true,
                ETagCondition::ETag(cond_etag) => cond_etag == &etag,
            };
            if !matches {
                return Err(s3_error!(PreconditionFailed));
            }
        }
        if let Some(if_none_match) = &input.if_none_match {
            let matches = match if_none_match {
                ETagCondition::Any => true,
                ETagCondition::ETag(cond_etag) => cond_etag == &etag,
            };
            if matches {
                let etag_str = match &etag {
                    ETag::Strong(s) => format!("\"{}\"", s),
                    ETag::Weak(s) => format!("W/\"{}\"", s),
                };
                let mut header_map = hyper::HeaderMap::new();
                if let Ok(v) = hyper::header::HeaderValue::from_str(&etag_str) {
                    header_map.insert(hyper::header::ETAG, v);
                }
                let mut err = s3_error!(NotModified);
                err.set_headers(header_map);
                return Err(err);
            }
        }

        // Compute range to read from the backend.  Only the requested
        // byte range is fetched -- no full-object read.
        let (read_start, read_end, content_length, content_range) = match &input.range {
            Some(range) => {
                let (start, end) = match range {
                    Range::Int { first, last } => {
                        let s = *first as usize;
                        let e = last.map(|l| (l as usize) + 1).unwrap_or(body_size as usize).min(body_size as usize);
                        (s, e)
                    }
                    Range::Suffix { length } => {
                        let len = (*length as usize).min(body_size as usize);
                        (body_size as usize - len, body_size as usize)
                    }
                };
                let range_str = format!("bytes {}-{}/{}", start, end - 1, body_size);
                (start as u64, end as u64, (end - start) as i64, Some(range_str))
            }
            None => (0u64, body_size, body_size as i64, None),
        };

        // Read the body, streaming when possible to avoid buffering
        // the entire object in memory.
        //
        // Full GETs (no Range header): use a streaming GET from the
        // backend.  If meta_len > 0, the stream is truncated at
        // body_size to exclude the trailing metadata bytes.
        //
        // Ranged GETs: use a bounded range read (already excludes
        // metadata by construction).

        let is_range_request = content_range.is_some();

        if is_range_request {
            // Ranged read: bounded range already targets only the requested bytes.
            let body_bytes = if read_end > read_start {
                let opts = GetOptions {
                    range: Some(GetRange::Bounded(read_start..read_end)),
                    ..Default::default()
                };
                self.backend.obj_store().get_opts(&path, opts).await.map_err(|e| {
                    warn!("get_object range read error: {e}");
                    s3_error!(InternalError)
                })?.bytes().await.map_err(|e| {
                    warn!("get_object body read error: {e}");
                    s3_error!(InternalError)
                })?
            } else {
                Bytes::new()
            };

            let content_type = meta.get("content-type").cloned();
            let cache_control = meta.get("cache-control").cloned();
            let content_encoding = meta.get("content-encoding").cloned();
            let content_disposition = meta.get("content-disposition").cloned();
            let content_language = meta.get("content-language").cloned();
            let user_metadata = Self::extract_user_metadata(&meta);

            let body_stream = futures::stream::once(async { Ok::<_, std::io::Error>(body_bytes) });
            let body = StreamingBlob::wrap(body_stream);

            let output = GetObjectOutput {
                body: Some(body),
                content_length: Some(content_length),
                content_range,
                content_type,
                cache_control,
                content_encoding,
                content_disposition,
                content_language,
                last_modified: Some(last_modified),
                e_tag: Some(etag),
                metadata: None,
                accept_ranges: Some("bytes".to_string()),
                ..Default::default()
            };
            let mut resp = S3Response::new(output);
            Self::add_user_meta_headers(&mut resp.headers, user_metadata);
            return Ok(resp);
        }

        // Full GET: stream the response body to avoid buffering the
        // entire object in memory.
        let get_result = self.backend.obj_store()
            .get_opts(&path, GetOptions::default())
            .await
            .map_err(|e| {
                warn!("get_object streaming read error: {e}");
                s3_error!(InternalError)
            })?;

        // The underlying stream now yields body-only bytes (get_opts
        // excludes the metadata suffix), so we just forward it.
        let raw_stream = get_result.into_stream();

        let inner: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(raw_stream.map(|r| r.map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
            })));
        // SyncStream adds the Sync bound required by StreamingBlob::wrap.
        let body = StreamingBlob::wrap(SyncStream(inner));

        let content_type = meta.get("content-type").cloned();
        let cache_control = meta.get("cache-control").cloned();
        let content_encoding = meta.get("content-encoding").cloned();
        let content_disposition = meta.get("content-disposition").cloned();
        let content_language = meta.get("content-language").cloned();
        let user_metadata = Self::extract_user_metadata(&meta);

        let output = GetObjectOutput {
            body: Some(body),
            content_length: Some(content_length),
            content_range,
            content_type,
            cache_control,
            content_encoding,
            content_disposition,
            content_language,
            last_modified: Some(last_modified),
            e_tag: Some(etag),
            metadata: None,
            accept_ranges: Some("bytes".to_string()),
            ..Default::default()
        };
        let mut resp = S3Response::new(output);
        Self::add_user_meta_headers(&mut resp.headers, user_metadata);
        Ok(resp)
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "head_object");
        validate_key(key)?;

        self.ensure_bucket(bucket).await?;

        let path = obj_key(bucket, key);
        let (head, _meta_len) = self.backend.head_with_meta(&path).await.map_err(|e| match e {
            RawStoreError::NotFound(_) => s3_error!(NoSuchKey),
            _ => {
                warn!("head_object store error: {e}");
                s3_error!(InternalError)
            }
        })?;

        // head.size is already body-only (meta_for subtracts meta_len).
        let content_length = head.size as i64;
        let last_modified = Timestamp::from(std::time::SystemTime::from(head.last_modified));

        // Read metadata (suffix read for raw shards, sidecar for non-raw).
        let meta = match self.backend.get_metadata(&path).await {
            Ok(data) if !data.is_empty() => decode_metadata(&data),
            _ => HashMap::new(),
        };
        let content_type = meta.get("content-type").cloned();
        let cache_control = meta.get("cache-control").cloned();
        let content_disposition = meta.get("content-disposition").cloned();
        let content_encoding = meta.get("content-encoding").cloned();
        let content_language = meta.get("content-language").cloned();
        let expires = meta.get("expires").cloned()
            .and_then(|s| Timestamp::parse(TimestampFormat::DateTime, &s).ok());
        let user_metadata = Self::extract_user_metadata(&meta);
        let e_tag = meta.get("etag").cloned().map(ETag::Strong);

        let output = HeadObjectOutput {
            content_length: Some(content_length),
            content_type,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            expires,
            last_modified: Some(last_modified),
            metadata: None,
            e_tag,
            ..Default::default()
        };
        let mut resp = S3Response::new(output);
        Self::add_user_meta_headers(&mut resp.headers, user_metadata);
        Ok(resp)
    }

    // NOTE: ObjectParts (multipart part manifest) is not currently persisted
    // during CompleteMultipartUpload, so requesting ObjectParts will return
    // None.  ETag, ObjectSize, StorageClass, and LastModified are fully
    // supported.
    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "get_object_attributes");
        validate_key(key)?;

        self.ensure_bucket(bucket).await?;

        let path = obj_key(bucket, key);
        let (head, _meta_len) = self.backend.head_with_meta(&path).await.map_err(|e| match e {
            RawStoreError::NotFound(_) => s3_error!(NoSuchKey),
            _ => {
                warn!("get_object_attributes store error: {e}");
                s3_error!(InternalError)
            }
        })?;

        let requested: HashSet<&str> = input
            .object_attributes
            .iter()
            .map(|a| a.as_str())
            .collect();

        // Read stored metadata only if we need ETag or StorageClass.
        let meta = if requested.contains(ObjectAttributes::ETAG)
            || requested.contains(ObjectAttributes::STORAGE_CLASS)
            || requested.contains(ObjectAttributes::CHECKSUM)
        {
            match self.backend.get_metadata(&path).await {
                Ok(data) if !data.is_empty() => decode_metadata(&data),
                _ => HashMap::new(),
            }
        } else {
            HashMap::new()
        };

        let e_tag = if requested.contains(ObjectAttributes::ETAG) {
            meta.get("etag").cloned().map(ETag::Strong)
        } else {
            None
        };

        let object_size = if requested.contains(ObjectAttributes::OBJECT_SIZE) {
            Some(head.size as i64)
        } else {
            None
        };

        let storage_class = if requested.contains(ObjectAttributes::STORAGE_CLASS) {
            let sc = meta.get("x-amz-storage-class")
                .cloned()
                .unwrap_or_else(|| "STANDARD".to_string());
            Some(StorageClass::from(sc))
        } else {
            None
        };

        let last_modified = Timestamp::from(std::time::SystemTime::from(head.last_modified));

        let output = GetObjectAttributesOutput {
            e_tag,
            object_size,
            storage_class,
            last_modified: Some(last_modified),
            checksum: None,
            delete_marker: None,
            object_parts: None,
            request_charged: None,
            version_id: None,
        };
        Ok(S3Response::new(output))
    }

    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "get_object_acl");

        self.ensure_bucket(bucket).await?;

        let path = obj_key(bucket, key);
        self.backend.obj_store().head(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => s3_error!(NoSuchKey),
            _ => {
                warn!("get_object_acl store error: {e}");
                s3_error!(InternalError)
            }
        })?;

        let owner = Owner {
            id: Some("testuser".to_string()),
            display_name: Some("Test User".to_string()),
        };
        let grantee = Grantee {
            id: Some("testuser".to_string()),
            display_name: Some("Test User".to_string()),
            type_: Type::from_static(Type::CANONICAL_USER),
            email_address: None,
            uri: None,
        };
        let grant = Grant {
            grantee: Some(grantee),
            permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
        };
        let output = GetObjectAclOutput {
            owner: Some(owner),
            grants: Some(vec![grant]),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "delete_object");
        validate_key(key)?;

        self.ensure_bucket(bucket).await?;

        let path = obj_key(bucket, key);
        // Clean up metadata sidecar (non-raw shards only, best-effort)
        self.backend.delete_sidecar(&path).await;
        match self.backend.obj_store().delete(&path).await {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) => {
                // S3 DELETE on non-existent key returns 204 (idempotent)
            }
            Err(e) => {
                warn!("delete_object store error: {e}");
                return Err(s3_error!(InternalError));
            }
        }

        // Always emit DELETE event (even for NotFound -- matches sharded
        // backend behaviour and keeps streaming replicas in sync).
        if let Some(ref bus) = self.event_bus {
            bus.emit_delete(key);
        }

        // Invalidate cached bucket stats (object count/bytes changed)
        self.bucket_stats_cache.write().await.remove(bucket.as_str());

        let output = DeleteObjectOutput::default();
        Ok(S3Response::new(output))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        debug!(bucket = %bucket, "delete_objects");

        self.ensure_bucket(bucket).await?;

        // S3 limits batch deletes to 1000 objects
        if input.delete.objects.len() > 1000 {
            return Err(s3_error!(MalformedXML, "You may not request more than 1000 items in a batch delete"));
        }

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for obj in input.delete.objects {
            let key = obj.key;
            if validate_key(&key).is_err() {
                errors.push(Error {
                    code: Some("InvalidArgument".to_string()),
                    key: Some(key),
                    message: Some("key must not contain '..' path components".to_string()),
                    version_id: None,
                });
                continue;
            }
            let path = obj_key(bucket, &key);
            self.backend.delete_sidecar(&path).await;
            match self.backend.obj_store().delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    // Always emit DELETE event (even for NotFound -- matches
                    // sharded backend behaviour and keeps replicas in sync).
                    if let Some(ref bus) = self.event_bus {
                        bus.emit_delete(&key);
                    }
                    deleted.push(DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    });
                }
                Err(e) => {
                    errors.push(Error {
                        code: Some("InternalError".to_string()),
                        key: Some(key),
                        message: Some(e.to_string()),
                        version_id: None,
                    });
                }
            }
        }

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let dst_bucket = &input.bucket;
        let dst_key = &input.key;

        let (src_bucket, src_key) = match &input.copy_source {
            CopySource::Bucket { ref bucket, ref key, .. } => (bucket.as_ref(), key.as_ref()),
            _ => return Err(s3_error!(NotImplemented)),
        };

        debug!(
            src_bucket = %src_bucket, src_key = %src_key,
            dst_bucket = %dst_bucket, dst_key = %dst_key,
            "copy_object"
        );

        validate_key(src_key)?;
        validate_key(dst_key)?;

        self.ensure_bucket(src_bucket).await?;
        self.ensure_bucket(dst_bucket).await?;

        // S3 forbids copying an object to itself without metadata change
        let is_replace = input.metadata_directive.as_ref()
            .map(|d| d.as_str() == MetadataDirective::REPLACE)
            .unwrap_or(false);
        if src_bucket == dst_bucket.as_str() && src_key == dst_key.as_str() && !is_replace {
            return Err(s3_error!(InvalidRequest,
                "This copy request is illegal because it is trying to copy an object to itself \
                 without changing the object's metadata, storage class, website redirect location \
                 or encryption attributes."));
        }

        // With meta_len in the index, store.copy() copies both data and metadata
        // in one operation (meta_len propagated). For REPLACE metadata, we re-read,
        // split by meta_len, and re-write with new metadata.
        let src_path = obj_key(src_bucket, src_key);
        let dst_path = obj_key(dst_bucket, dst_key);

        // Read source body + metadata (shared by both REPLACE and COPY modes).
        let (body, raw_meta, etag) = self.read_source_object(&src_path).await?;

        if is_replace {
            // REPLACE mode: discard source metadata, attach caller-supplied metadata.
            let mut meta = Self::build_s3_metadata(
                input.content_type.as_deref(),
                input.cache_control.as_deref(),
                input.content_disposition.as_deref(),
                input.content_encoding.as_deref(),
                input.content_language.as_deref(),
                input.expires.as_ref(),
                input.metadata.as_ref(),
            );
            meta.insert("etag".to_string(), etag.clone());
            let meta_bytes = encode_metadata(&meta).map_err(|e| {
                warn!("copy_object: metadata too large: {e}");
                s3_error!(InvalidArgument)
            })?;
            self.backend.put_with_meta(&dst_path, body, &meta_bytes).await
                .map_err(|e| { warn!("copy_object write error: {e}"); s3_error!(InternalError) })?;
        } else {
            // COPY mode: preserve body + metadata as-is.
            let meta_bytes = if raw_meta.is_empty() {
                // Source had no metadata; store at least the etag so that
                // HeadObject on the copy returns the same etag as the
                // CopyObject response.
                let mut meta = HashMap::new();
                meta.insert("etag".to_string(), etag.clone());
                encode_metadata(&meta).map_err(|e| {
                    warn!("copy_object: metadata too large: {e}");
                    s3_error!(InvalidArgument)
                })?
            } else {
                raw_meta.to_vec()
            };
            self.backend.put_with_meta(&dst_path, body, &meta_bytes).await.map_err(|e| {
                warn!("copy_object write error: {e}"); s3_error!(InternalError)
            })?;
        }

        // Emit PUT event (for event-based streaming replication)
        if let Some(ref bus) = self.event_bus {
            bus.emit_put(dst_key);
        }

        // Invalidate cached bucket stats (object count/bytes changed)
        self.bucket_stats_cache.write().await.remove(dst_bucket.as_str());

        let copy_result = CopyObjectResult {
            e_tag: Some(ETag::Strong(etag)),
            last_modified: Some(Timestamp::from(std::time::SystemTime::now())),
            ..Default::default()
        };
        let output = CopyObjectOutput {
            copy_object_result: Some(copy_result),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // -- Listing operations --------------------------------------------------

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        // Ceph extension: allow-unordered=true combined with delimiter is invalid.
        let allow_unordered = req
            .uri
            .query()
            .map(|q| q.contains("allow-unordered=true"))
            .unwrap_or(false);
        if allow_unordered && req.input.delimiter.is_some() {
            return Err(s3_error!(InvalidArgument, "allow-unordered may not be used with delimiter"));
        }

        let has_delimiter = req.input.delimiter.as_deref().is_some_and(|d| !d.is_empty());
        // Delegate to list_objects_v2 and convert
        let v2_resp = self.list_objects_v2(req.map_input(|v1: ListObjectsInput| {
            let mut v2: ListObjectsV2Input = v1.into();
            // V1 list_objects always returns Owner; force fetch_owner on.
            v2.fetch_owner = Some(true);
            v2
        })).await?;
        Ok(v2_resp.map_output(|v2| {
            // NextMarker: set when truncated and delimiter is used, to the
            // last key/prefix in the result (per S3 spec).
            let next_marker = if v2.is_truncated == Some(true) && has_delimiter {
                // last common prefix or last object key, whichever is greater
                let last_obj = v2
                    .contents
                    .as_deref()
                    .and_then(|c| c.last())
                    .and_then(|o| o.key.as_deref());
                let last_pfx = v2
                    .common_prefixes
                    .as_deref()
                    .and_then(|p| p.last())
                    .and_then(|p| p.prefix.as_deref());
                match (last_obj, last_pfx) {
                    (Some(ok), Some(pk)) => Some(if ok > pk { ok } else { pk }.to_string()),
                    (Some(ok), None) => Some(ok.to_string()),
                    (None, Some(pk)) => Some(pk.to_string()),
                    (None, None) => None,
                }
            } else {
                None
            };
            ListObjectsOutput {
                contents: v2.contents,
                common_prefixes: v2.common_prefixes,
                delimiter: v2.delimiter,
                encoding_type: v2.encoding_type,
                name: v2.name,
                prefix: v2.prefix,
                max_keys: v2.max_keys,
                is_truncated: v2.is_truncated,
                next_marker,
                ..Default::default()
            }
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = &input.bucket;
        debug!(bucket = %bucket, prefix = ?input.prefix, delimiter = ?input.delimiter, "list_objects_v2");

        self.ensure_bucket(bucket).await?;

        let prefix_str = input.prefix.as_deref().unwrap_or("");
        // Use the directory component of the prefix for the object-store listing.
        // `Path::prefix_matches` is component-based, so listing at e.g.
        // "buck/ba" would miss "buck/bar".  Instead list at the parent directory
        // (everything up to and including the last '/') so all candidates are
        // returned, then apply the exact string-prefix filter below.
        let dir_prefix = if let Some(slash_pos) = prefix_str.rfind('/') {
            format!("{bucket}/{}", &prefix_str[..=slash_pos])
        } else {
            format!("{bucket}/")
        };
        let store_prefix = raw_path(dir_prefix);
        let max_keys = input.max_keys.unwrap_or(1000);
        let use_url_encoding = input.encoding_type.as_ref().map(|e| e.as_str() == "url").unwrap_or(false);
        let fetch_owner = input.fetch_owner.unwrap_or(false);

        // Treat empty delimiter as no delimiter (S3 spec: empty delimiter is ignored)
        let delimiter = input.delimiter.as_deref().filter(|d| !d.is_empty());

        // Collect all objects under this bucket+prefix (index-only, zero data reads)
        let listing = self.backend.list_with_meta(Some(&store_prefix)).await;

        let bucket_prefix = format!("{bucket}/");
        let mut objects: Vec<Object> = Vec::new();
        let mut common_prefixes_set = std::collections::BTreeSet::new();

        for (item, _meta_len) in &listing {
            let full_key = item.location.as_ref();
            // Strip bucket prefix to get the S3 key
            let Some(raw_key) = full_key.strip_prefix(&bucket_prefix) else {
                continue;
            };

            // Decode trailing-slash directory marker: keys ending in "/" were
            // stored as "key/__DIRMARK__" to preserve the slash through
            // object_store::Path parsing (which strips trailing slashes).
            let key: std::borrow::Cow<str> = if raw_key.ends_with(DIR_MARK_SUFFIX) {
                // Strip DIR_MARK to recover the original "key/" form.
                std::borrow::Cow::Owned(raw_key[..raw_key.len() - DIR_MARK.len()].to_string())
            } else if raw_key == DIR_MARK || raw_key.ends_with(&format!("/{DIR_MARK}/")) {
                // Stray marker component — skip.
                continue;
            } else {
                std::borrow::Cow::Borrowed(raw_key)
            };
            let key: &str = &key;

            // Skip internal keys (bucket registry)
            if key.starts_with("__buckets__/") {
                continue;
            }

            // Filter: key must start with the requested prefix
            if !key.starts_with(prefix_str) {
                continue;
            }

            // Apply continuation-token / start-after filter
            if let Some(ref start_after) = input.start_after {
                if key <= start_after.as_str() {
                    continue;
                }
            }
            if let Some(ref token) = input.continuation_token {
                if key <= token.as_str() {
                    continue;
                }
            }

            // Handle delimiter grouping
            if let Some(delim) = delimiter {
                let remainder = &key[prefix_str.len()..];
                if let Some(pos) = remainder.find(delim) {
                    // This is a common prefix
                    let cp = format!("{}{}", prefix_str, &remainder[..pos + delim.len()]);
                    // Only include this common prefix if it is strictly after any
                    // continuation token / start-after (avoids re-emitting prefixes
                    // from previous pages).
                    let after_start = input.start_after.as_deref()
                        .map(|sa| cp.as_str() > sa)
                        .unwrap_or(true);
                    let after_token = input.continuation_token.as_deref()
                        .map(|ct| cp.as_str() > ct)
                        .unwrap_or(true);
                    if after_start && after_token {
                        common_prefixes_set.insert(cp);
                    }
                    continue;
                }
            }

            let owner = if fetch_owner {
                Some(Owner {
                    id: Some("testuser".to_string()),
                    display_name: Some("Test User".to_string()),
                })
            } else {
                None
            };

            // Defer metadata/etag loading until after pagination (avoid N+1
            // reads for objects that will be truncated by max_keys).
            let obj = Object {
                key: Some(key.to_string()),
                last_modified: Some(Timestamp::from(std::time::SystemTime::from(item.last_modified))),
                // item.size is already body-only (meta_for subtracts meta_len).
                size: Some(item.size as i64),
                owner,
                e_tag: None,
                ..Default::default()
            };
            objects.push(obj);
        }

        // Sort objects by key
        objects.sort_by(|a, b| {
            let ak = a.key.as_deref().unwrap_or("");
            let bk = b.key.as_deref().unwrap_or("");
            ak.cmp(bk)
        });

        // Merge objects and common prefixes, apply max_keys limit
        let common_prefixes_list: Vec<CommonPrefix> = common_prefixes_set
            .into_iter()
            .map(|p| CommonPrefix {
                prefix: Some(p),
            })
            .collect();

        let mut result_objects = Vec::new();
        let mut result_prefixes = Vec::new();
        let mut total_count = 0usize;
        let max_keys_usize = max_keys.max(0) as usize;
        let mut obj_idx = 0;
        let mut prefix_idx = 0;

        while total_count < max_keys_usize {
            let obj_key_str = objects.get(obj_idx).and_then(|o| o.key.as_deref());
            let pfx_key_str = common_prefixes_list
                .get(prefix_idx)
                .and_then(|p| p.prefix.as_deref());

            match (obj_key_str, pfx_key_str) {
                (Some(ok), Some(pk)) => {
                    if ok < pk {
                        result_objects.push(objects[obj_idx].clone());
                        obj_idx += 1;
                    } else {
                        result_prefixes.push(common_prefixes_list[prefix_idx].clone());
                        prefix_idx += 1;
                    }
                    total_count += 1;
                }
                (Some(_), None) => {
                    result_objects.push(objects[obj_idx].clone());
                    obj_idx += 1;
                    total_count += 1;
                }
                (None, Some(_)) => {
                    result_prefixes.push(common_prefixes_list[prefix_idx].clone());
                    prefix_idx += 1;
                    total_count += 1;
                }
                (None, None) => break,
            }
        }

        // is_truncated: only true if there are remaining items AND max_keys > 0
        let is_truncated = max_keys > 0
            && (obj_idx < objects.len() || prefix_idx < common_prefixes_list.len());
        let key_count = total_count as i32;

        // NextContinuationToken: the first key of the next page (last included key works too)
        let next_continuation_token = if is_truncated {
            // The continuation token is the last key returned (clients use it as exclusive start)
            let last_obj_key = result_objects.last().and_then(|o| o.key.as_deref());
            let last_pfx_key = result_prefixes.last().and_then(|p| p.prefix.as_deref());
            match (last_obj_key, last_pfx_key) {
                (Some(ok), Some(pk)) => Some(if ok > pk { ok } else { pk }.to_string()),
                (Some(ok), None) => Some(ok.to_string()),
                (None, Some(pk)) => Some(pk.to_string()),
                (None, None) => None,
            }
        } else {
            None
        };

        // Load etags only for the paginated result objects (not the full listing).
        // Uses parallel fetches to avoid sequential N metadata reads.
        // Batched in chunks of 32 to avoid unbounded fan-out on large listings.
        {
            let keys: Vec<String> = result_objects.iter()
                .filter_map(|obj| obj.key.clone())
                .collect();
            let mut etag_results = Vec::with_capacity(keys.len());
            for chunk in keys.chunks(32) {
                let futs: Vec<_> = chunk.iter()
                    .map(|key| self.load_metadata(bucket, key))
                    .collect();
                etag_results.extend(futures::future::join_all(futs).await);
            }
            for (obj, meta) in result_objects.iter_mut().zip(etag_results) {
                obj.e_tag = meta
                    .and_then(|m| m.get("etag").cloned())
                    .map(ETag::Strong);
            }
        }

        // Apply URL encoding to keys and prefixes if requested
        let url_encode = |s: &str| -> String {
            if use_url_encoding {
                s.chars()
                    .flat_map(|c| {
                        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '/') {
                            vec![c]
                        } else {
                            let mut buf = [0u8; 4];
                            let bytes = c.encode_utf8(&mut buf).as_bytes();
                            bytes
                                .iter()
                                .flat_map(|b| {
                                    format!("%{b:02X}").chars().collect::<Vec<_>>()
                                })
                                .collect()
                        }
                    })
                    .collect()
            } else {
                s.to_string()
            }
        };

        let encoded_objects: Vec<Object> = if use_url_encoding {
            result_objects
                .into_iter()
                .map(|mut o| {
                    o.key = o.key.map(|k| url_encode(&k));
                    o
                })
                .collect()
        } else {
            result_objects
        };

        let encoded_prefixes: Vec<CommonPrefix> = if use_url_encoding {
            result_prefixes
                .into_iter()
                .map(|mut p| {
                    p.prefix = p.prefix.map(|k| url_encode(&k));
                    p
                })
                .collect()
        } else {
            result_prefixes
        };

        // Only include delimiter in output if it was non-empty in the request
        let output_delimiter = input.delimiter.filter(|d| !d.is_empty());

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(max_keys),
            is_truncated: Some(is_truncated),
            next_continuation_token,
            continuation_token: input.continuation_token,
            contents: if encoded_objects.is_empty() {
                None
            } else {
                Some(encoded_objects)
            },
            common_prefixes: if encoded_prefixes.is_empty() {
                None
            } else {
                Some(encoded_prefixes)
            },
            delimiter: output_delimiter,
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            prefix: input.prefix,
            start_after: input.start_after,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // -- Multipart upload operations -----------------------------------------

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        debug!(bucket = %bucket, key = %key, "create_multipart_upload");

        self.ensure_bucket(bucket).await?;

        let upload_id = uuid::Uuid::new_v4().to_string();

        // Build metadata from request headers (same fields as PutObject).
        let metadata = Self::build_s3_metadata(
            input.content_type.as_deref(),
            input.cache_control.as_deref(),
            input.content_disposition.as_deref(),
            input.content_encoding.as_deref(),
            input.content_language.as_deref(),
            input.expires.as_ref(),
            input.metadata.as_ref(),
        );

        let upload = Upload {
            bucket: bucket.clone(),
            key: key.clone(),
            metadata,
            parts: HashMap::new(),
            created_at: std::time::Instant::now(),
        };

        {
            let mut uploads = self.uploads.write().await;

            // Lazy reaper: purge stale uploads before checking the cap.
            let now = std::time::Instant::now();
            let stale_ids: Vec<String> = uploads
                .iter()
                .filter(|(_, u)| now.checked_duration_since(u.created_at).unwrap_or_default() > self.upload_expiry)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale_ids {
                uploads.remove(id);
            }
            if !stale_ids.is_empty() {
                debug!(purged = stale_ids.len(), "purged stale multipart uploads");
            }

            // Enforce concurrent upload cap
            if uploads.len() >= MAX_CONCURRENT_UPLOADS {
                warn!(
                    count = uploads.len(),
                    max = MAX_CONCURRENT_UPLOADS,
                    "too many concurrent multipart uploads"
                );
                return Err(s3_error!(SlowDown, "too many concurrent multipart uploads"));
            }

            uploads.insert(upload_id.clone(), upload);
        }

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            upload_id: Some(upload_id),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;
        let part_number = input.part_number;
        if part_number < 1 || part_number > 10_000 {
            return Err(s3_error!(InvalidArgument, "part number must be between 1 and 10000"));
        }
        debug!(upload_id = %upload_id, part_number = part_number, "upload_part");

        // Spool part body to a named temp file on the local filesystem.
        // Parts can be up to 5 GB; keeping them on disk avoids buffering
        // in RAM and avoids writing to the backend store twice (once for
        // upload_part, again during complete_multipart).
        let named_tmp = tempfile::NamedTempFile::new().map_err(|e| {
            warn!("upload_part: failed to create temp file: {e}");
            s3_error!(InternalError)
        })?;
        let temp_path = named_tmp.path().to_path_buf();
        // Detach from auto-delete so the file outlives this function.
        // Upload::drop will clean it up later.
        let (tmp_std, _kept) = named_tmp.keep().map_err(|e| {
            warn!("upload_part: failed to persist temp file: {e}");
            s3_error!(InternalError)
        })?;
        let mut tmp = tokio::fs::File::from_std(tmp_std);
        let mut md5_ctx = md5::Context::new();
        let mut size: usize = 0;

        let result: S3Result<S3Response<UploadPartOutput>> = async {
            match input.body {
                Some(body) => {
                    let mut stream = body.into_stream();
                    while let Some(chunk) = stream.try_next().await.map_err(|e| {
                        warn!("upload_part: stream read error: {e}");
                        s3_error!(InternalError)
                    })? {
                        md5_ctx.consume(&chunk);
                        size += chunk.len();
                        if self.max_body_size > 0 && size as u64 > self.max_body_size {
                            warn!(size, max = self.max_body_size, "upload_part: body exceeds size limit");
                            return Err(s3_error!(EntityTooLarge, "Part size exceeds server limit"));
                        }
                        tmp.write_all(&chunk).await.map_err(|e| {
                            warn!("upload_part: write error: {e}");
                            s3_error!(InternalError)
                        })?;
                    }
                }
                None => {}
            }
            tmp.flush().await.map_err(|e| {
                warn!("upload_part: flush error: {e}");
                s3_error!(InternalError)
            })?;
            drop(tmp);

            let md5 = format!("{:x}", md5_ctx.compute());

            // Track part in upload registry
            {
                let mut uploads = self.uploads.write().await;
                let upload = uploads.get_mut(upload_id.as_str()).ok_or_else(|| {
                    s3_error!(NoSuchUpload)
                })?;
                upload.parts.insert(part_number, PartMeta {
                    size,
                    etag: md5.clone(),
                    temp_path: temp_path.clone(),
                });
            }

            let output = UploadPartOutput {
                e_tag: Some(ETag::Strong(md5)),
                ..Default::default()
            };
            Ok(S3Response::new(output))
        }.await;

        // If we failed before registering the part, clean up the temp file.
        if result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
        }
        result
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;
        let part_number = input.part_number;
        if part_number < 1 || part_number > 10_000 {
            return Err(s3_error!(InvalidArgument, "part number must be between 1 and 10000"));
        }

        let (src_bucket, src_key) = match &input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.as_ref().to_string(), key.as_ref().to_string()),
            _ => return Err(s3_error!(NotImplemented, "AccessPoint CopySource not supported")),
        };

        debug!(src_bucket = %src_bucket, src_key = %src_key, upload_id = %upload_id, part_number = part_number, "upload_part_copy");

        // Get source object size and meta_len (index-only, no data I/O).
        let src_path = obj_key(&src_bucket, &src_key);
        let (src_head, _src_meta_len) = self.backend.head_with_meta(&src_path).await.map_err(|e| match e {
            RawStoreError::NotFound(_) => s3_error!(NoSuchKey),
            _ => s3_error!(InternalError),
        })?;
        // head_meta.size is already body-only (meta_for subtracts meta_len).
        let src_body_size = src_head.size as u64;

        // Determine the byte range to copy.
        let (copy_start, copy_end) = if let Some(range_str) = &input.copy_source_range {
            // Must be "bytes=start-end"
            let range_str = range_str.trim();
            let bytes_range = range_str.strip_prefix("bytes=")
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid CopySourceRange format"))?;
            let (start_str, end_str) = bytes_range.split_once('-')
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid CopySourceRange format"))?;
            // Validate that start and end are pure numeric (no trailing chars)
            if start_str.chars().any(|c| !c.is_ascii_digit()) || end_str.chars().any(|c| !c.is_ascii_digit()) {
                return Err(s3_error!(InvalidArgument, "Invalid CopySourceRange format"));
            }
            let start: u64 = start_str.parse().map_err(|_| s3_error!(InvalidArgument, "Invalid CopySourceRange"))?;
            let end: u64 = end_str.parse().map_err(|_| s3_error!(InvalidArgument, "Invalid CopySourceRange"))?;
            if start > end {
                return Err(s3_error!(InvalidRange, "CopySourceRange start > end"));
            }
            if start >= src_body_size || end >= src_body_size {
                return Err(s3_error!(InvalidRange, "CopySourceRange out of bounds"));
            }
            (start, end + 1)
        } else {
            (0, src_body_size)
        };

        let copy_size = copy_end - copy_start;

        // Enforce max body size.
        if self.max_body_size > 0 && copy_size > self.max_body_size {
            return Err(s3_error!(EntityTooLarge, "Copy source size exceeds server limit"));
        }

        // Stream source data directly to a temp file instead of buffering
        // the entire copy in memory.
        let named_tmp = tempfile::NamedTempFile::new().map_err(|e| {
            warn!("upload_part_copy: failed to create temp file: {e}");
            s3_error!(InternalError)
        })?;
        let temp_path = named_tmp.path().to_path_buf();
        let (tmp_std, _kept) = named_tmp.keep().map_err(|e| {
            warn!("upload_part_copy: failed to persist temp file: {e}");
            s3_error!(InternalError)
        })?;
        let mut tmp = tokio::fs::File::from_std(tmp_std);

        let opts = GetOptions {
            range: Some(GetRange::Bounded(copy_start..copy_end)),
            ..Default::default()
        };
        let get_result = self.backend.obj_store()
            .get_opts(&src_path, opts)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => s3_error!(NoSuchKey),
                _ => { warn!("upload_part_copy: get_opts error: {e}"); s3_error!(InternalError) }
            })?;

        let mut stream = get_result.into_stream();
        let mut md5_ctx = md5::Context::new();
        let mut size: usize = 0;

        while let Some(chunk) = stream.try_next().await.map_err(|e| {
            warn!("upload_part_copy: stream read error: {e}");
            s3_error!(InternalError)
        })? {
            md5_ctx.consume(&chunk);
            size += chunk.len();
            tmp.write_all(&chunk).await.map_err(|e| {
                warn!("upload_part_copy: write error: {e}");
                s3_error!(InternalError)
            })?;
        }

        tmp.flush().await.map_err(|e| {
            warn!("upload_part_copy: flush error: {e}");
            s3_error!(InternalError)
        })?;
        drop(tmp);

        let md5 = format!("{:x}", md5_ctx.compute());

        // Track part in upload registry
        {
            let mut uploads = self.uploads.write().await;
            let upload = uploads.get_mut(upload_id.as_str()).ok_or_else(|| {
                s3_error!(NoSuchUpload)
            })?;
            upload.parts.insert(part_number, PartMeta {
                size,
                etag: md5.clone(),
                temp_path,
            });
        }

        let copy_part_result = CopyPartResult {
            e_tag: Some(ETag::Strong(md5)),
            last_modified: Some(Timestamp::from(std::time::SystemTime::now())),
            ..Default::default()
        };

        let output = UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        let key = &input.key;
        let upload_id = &input.upload_id;
        debug!(bucket = %bucket, key = %key, upload_id = %upload_id, "complete_multipart_upload");

        let multipart_upload = input.multipart_upload.ok_or_else(|| {
            s3_error!(InvalidRequest, "missing multipart upload body")
        })?;

        // Extract upload info and remove from registry
        let upload = {
            let mut uploads = self.uploads.write().await;
            uploads.remove(upload_id.as_str())
        };

        let upload = match upload {
            Some(u) => u,
            None => {
                // Already completed: if object exists return success (idempotent re-complete)
                let obj_path = obj_key(bucket, key);
                match self.backend.obj_store().head(&obj_path).await {
                    Ok(_) => {
                        // Use stored ETag from metadata (avoids reading the entire object)
                        let etag = self.load_metadata(bucket, key).await
                            .and_then(|m| m.get("etag").cloned())
                            .unwrap_or_default();
                        let output = CompleteMultipartUploadOutput {
                            bucket: Some(bucket.clone()),
                            key: Some(key.clone()),
                            e_tag: Some(ETag::Strong(etag)),
                            ..Default::default()
                        };
                        return Ok(S3Response::new(output));
                    }
                    Err(_) => return Err(s3_error!(NoSuchUpload)),
                }
            }
        };

        // Assemble parts from local temp files into a single temp file,
        // computing MD5 as we go, then write once to the backend via
        // put_with_meta.  This is a single write to the block device
        // regardless of backend type (Raw or Sharded).
        let obj_path = obj_key(bucket, key);
        let parts = multipart_upload.parts.unwrap_or_default();
        if parts.is_empty() {
            return Err(s3_error!(InvalidPart, "multipart upload must contain at least one part"));
        }

        let tmp_std = tempfile::tempfile().map_err(|e| {
            warn!("complete_multipart: failed to create temp file: {e}");
            s3_error!(InternalError)
        })?;
        let mut tmp = tokio::fs::File::from_std(tmp_std);
        let mut md5_ctx = md5::Context::new();

        for part in &parts {
            let part_number = part.part_number.ok_or_else(|| {
                s3_error!(InvalidRequest, "missing part number")
            })?;
            let part_meta = upload.parts.get(&part_number).ok_or_else(|| {
                s3_error!(InvalidPart)
            })?;
            // Stream part temp file to assembly file in 1 MB chunks
            // to avoid buffering entire parts in memory.
            let mut part_file = tokio::fs::File::open(&part_meta.temp_path).await.map_err(|e| {
                warn!("complete_multipart: failed to open part {part_number} temp file: {e}");
                s3_error!(InternalError)
            })?;
            let mut buf = vec![0u8; 1024 * 1024];
            loop {
                let n = part_file.read(&mut buf).await.map_err(|e| {
                    warn!("complete_multipart: failed to read part {part_number}: {e}");
                    s3_error!(InternalError)
                })?;
                if n == 0 { break; }
                md5_ctx.consume(&buf[..n]);
                tmp.write_all(&buf[..n]).await.map_err(|e| {
                    warn!("complete_multipart: failed to write part to temp file: {e}");
                    s3_error!(InternalError)
                })?;
            }
        }
        tmp.flush().await.map_err(|e| {
            warn!("complete_multipart: failed to flush temp file: {e}");
            s3_error!(InternalError)
        })?;

        let md5 = format!("{:x}", md5_ctx.compute());
        let mut meta = upload.metadata.clone();
        meta.insert("etag".to_string(), md5.clone());
        let meta_bytes = encode_metadata(&meta).map_err(|e| {
            warn!("complete_multipart: metadata too large: {e}");
            s3_error!(InvalidArgument)
        })?;
        let meta_len = meta_bytes.len() as u16;

        // Append metadata to the assembly temp file (body + meta in one file)
        tmp.write_all(&meta_bytes).await.map_err(|e| {
            warn!("complete_multipart: failed to write metadata to temp file: {e}");
            s3_error!(InternalError)
        })?;
        tmp.flush().await.map_err(|e| {
            warn!("complete_multipart: failed to flush temp file: {e}");
            s3_error!(InternalError)
        })?;

        // Convert to std::fs::File and pass to backend -- avoids
        // reading the entire assembled file back into memory.
        let mut std_file = tmp.into_std().await;
        std_file.seek(std::io::SeekFrom::Start(0)).map_err(|e| {
            warn!("complete_multipart: failed to seek temp file: {e}");
            s3_error!(InternalError)
        })?;

        if let Err(e) = self.backend.put_with_meta_from_file(
            &obj_path,
            &mut std_file,
            meta_len,
        ).await {
            warn!("complete_multipart put_with_meta error: {e}");
            // Re-insert so the client can retry CompleteMultipartUpload
            // without having to re-upload all parts.
            self.uploads.write().await.insert(upload_id.to_string(), upload);
            return Err(s3_error!(InternalError));
        }
        // upload drops here -- its Drop impl removes part temp files.

        // Emit PUT event (for event-based streaming replication)
        if let Some(ref bus) = self.event_bus {
            bus.emit_put(key);
        }

        // Invalidate cached bucket stats (object count/bytes changed)
        self.bucket_stats_cache.write().await.remove(bucket.as_str());

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: Some(ETag::Strong(md5)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;
        debug!(upload_id = %upload_id, "abort_multipart_upload");

        // Remove upload from registry
        let upload = {
            let mut uploads = self.uploads.write().await;
            uploads.remove(upload_id.as_str())
        };

        if let Some(upload) = upload {
            // Temp files are cleaned up by Upload::drop.
            drop(upload);
        } else {
            return Err(s3_error!(NoSuchUpload));
        }

        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        debug!(bucket = %bucket, "list_multipart_uploads");

        self.ensure_bucket(bucket).await?;

        let uploads = self.uploads.read().await;
        let mut result: Vec<MultipartUpload> = Vec::new();

        for (upload_id, upload) in uploads.iter() {
            if upload.bucket == *bucket {
                result.push(MultipartUpload {
                    key: Some(upload.key.clone()),
                    upload_id: Some(upload_id.clone()),
                    ..Default::default()
                });
            }
        }

        let output = ListMultipartUploadsOutput {
            bucket: Some(bucket.clone()),
            uploads: if result.is_empty() {
                None
            } else {
                Some(result)
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;
        debug!(upload_id = %upload_id, "list_parts");

        let uploads = self.uploads.read().await;
        let upload = uploads.get(upload_id.as_str()).ok_or_else(|| {
            s3_error!(NoSuchUpload)
        })?;

        let mut parts: Vec<Part> = upload
            .parts
            .iter()
            .map(|(num, meta)| Part {
                part_number: Some(*num),
                size: Some(meta.size as i64),
                e_tag: Some(ETag::Strong(meta.etag.clone())),
                ..Default::default()
            })
            .collect();
        parts.sort_by_key(|p| p.part_number);

        let output = ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(input.upload_id),
            parts: Some(parts),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // -- Stub operations -----------------------------------------------------

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let bucket = &req.input.bucket;
        self.ensure_bucket(bucket).await?;
        let locations = self.locations.read().await;
        let loc = locations.get(bucket.as_str()).cloned();
        let output = GetBucketLocationOutput {
            location_constraint: loc.map(|s| BucketLocationConstraint::from(s)),
        };
        Ok(S3Response::new(output))
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        self.ensure_bucket(&req.input.bucket).await?;
        Ok(S3Response::new(GetBucketVersioningOutput::default()))
    }

    async fn get_bucket_acl(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        self.ensure_bucket(&req.input.bucket).await?;

        let output = GetBucketAclOutput {
            owner: Some(Owner {
                display_name: Some("rawobjstr".to_string()),
                id: Some("rawobjstr".to_string()),
            }),
            grants: Some(vec![Grant {
                grantee: Some(Grantee {
                    display_name: Some("rawobjstr".to_string()),
                    id: Some("rawobjstr".to_string()),
                    type_: "CanonicalUser".to_string().into(),
                    email_address: None,
                    uri: None,
                }),
                permission: Some("FULL_CONTROL".to_string().into()),
            }]),
        };
        Ok(S3Response::new(output))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        self.ensure_bucket(&req.input.bucket).await?;
        // Accept but ignore versioning config
        Ok(S3Response::new(PutBucketVersioningOutput {}))
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let input = req.input;
        let bucket = &input.bucket;
        debug!(bucket = %bucket, "list_object_versions");

        self.ensure_bucket(bucket).await?;

        let prefix_str = input.prefix.as_deref().unwrap_or("");
        let store_prefix = raw_path(format!("{bucket}/{prefix_str}"));
        let max_keys = input.max_keys.unwrap_or(1000) as usize;
        let key_marker = input.key_marker.as_deref().unwrap_or("");

        let listing = self.backend.list_with_meta(Some(&store_prefix)).await;

        let bucket_prefix = format!("{bucket}/");
        let mut versions: Vec<ObjectVersion> = Vec::new();

        for (item, _meta_len) in &listing {
            let full_key = item.location.as_ref();
            let Some(raw_key) = full_key.strip_prefix(&bucket_prefix) else {
                continue;
            };

            // Decode trailing-slash directory marker (same logic as list_objects_v2).
            let key: std::borrow::Cow<str> = if raw_key.ends_with(DIR_MARK_SUFFIX) {
                std::borrow::Cow::Owned(raw_key[..raw_key.len() - DIR_MARK.len()].to_string())
            } else if raw_key == DIR_MARK || raw_key.ends_with(&format!("/{DIR_MARK}/")) {
                continue;
            } else {
                std::borrow::Cow::Borrowed(raw_key)
            };
            let key: &str = &key;

            if key.starts_with("__buckets__/") {
                continue;
            }
            if !key.starts_with(prefix_str) {
                continue;
            }
            if !key_marker.is_empty() && key <= key_marker {
                continue;
            }

            versions.push(ObjectVersion {
                key: Some(key.to_string()),
                version_id: Some("null".to_string()),
                is_latest: Some(true),
                last_modified: Some(Timestamp::from(std::time::SystemTime::from(
                    item.last_modified,
                ))),
                // item.size is already body-only (meta_for subtracts meta_len).
                size: Some(item.size as i64),
                storage_class: Some("STANDARD".to_string().into()),
                owner: Some(Owner {
                    display_name: Some("rawobjstr".to_string()),
                    id: Some("rawobjstr".to_string()),
                }),
                ..Default::default()
            });
        }

        versions.sort_by(|a, b| {
            let ak = a.key.as_deref().unwrap_or("");
            let bk = b.key.as_deref().unwrap_or("");
            ak.cmp(bk)
        });

        let is_truncated = versions.len() > max_keys;
        let versions: Vec<ObjectVersion> = versions.into_iter().take(max_keys).collect();
        let next_key_marker = if is_truncated {
            versions.last().and_then(|v| v.key.clone())
        } else {
            None
        };

        let output = ListObjectVersionsOutput {
            name: Some(input.bucket),
            prefix: input.prefix,
            key_marker: input.key_marker,
            max_keys: Some(max_keys as i32),
            is_truncated: Some(is_truncated),
            versions: if versions.is_empty() {
                None
            } else {
                Some(versions)
            },
            next_key_marker,
            // next_version_id_marker must be set when truncated to avoid botocore
            // passing VersionIdMarker=None to the next page request.
            next_version_id_marker: if is_truncated {
                Some("".to_string())
            } else {
                None
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
