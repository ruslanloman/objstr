//! VizService -- admin dashboard and device visualization endpoints.
//!
//! Wraps the S3 service and intercepts `/_admin/` routes for the
//! device visualization and management UIs.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectStore;

use crate::adapter::decode_metadata;
use crate::logging::{LogBuffer, LogEntry, LogFilter};
use crate::recovery::{RepairReplicationStatusHandle, RecoveryStatusHandle};

use rawobjstr::store::RawObjectStore;
use shardedobjstr::metadata::ShardKind;
use shardedobjstr::repair::sync_and_reattach;
use shardedobjstr::{ShardHealth, ShardedObjectStore};

use s3s::service::S3Service;

// -- Static HTML assets baked in at compile time ----------------------

const VIZ_HTML:     &str = include_str!(concat!(env!("OUT_DIR"), "/viz.html"));
const UI_HTML:      &str = include_str!(concat!(env!("OUT_DIR"), "/ui.html"));
const SERVER_HTML:  &str = include_str!(concat!(env!("OUT_DIR"), "/server.html"));
const CLUSTER_HTML: &str = include_str!(concat!(env!("OUT_DIR"), "/cluster.html"));
const LOGS_HTML:    &str = include_str!(concat!(env!("OUT_DIR"), "/logs.html"));
const CONFIG_HTML:  &str = include_str!(concat!(env!("OUT_DIR"), "/config.html"));

// -- Helpers ----------------------------------------------------------

/// Returns true if `name` is a real user-created bucket, not an internal
/// prefix used by the adapter for the bucket registry.
pub fn is_user_bucket(name: &str) -> bool {
    !name.is_empty() && name != "__buckets__"
}

/// Returns true if `key` is an internal metadata key (not a user object).
fn is_internal_key(key: &str) -> bool {
    key.starts_with("__buckets__")
        || key.starts_with("__deleted__")
        || key.ends_with(".__meta__")
}

/// Extract a percent-decoded query parameter value from a URL query string.
pub fn qparam(query: Option<&str>, key: &str) -> Option<String> {
    query.and_then(|q| {
        q.split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(k, _)| *k == key)
            .map(|(_, v)| {
                percent_encoding::percent_decode_str(v)
                    .decode_utf8_lossy()
                    .into_owned()
            })
    })
}

/// Scan the ObjectStore for top-level bucket prefixes, returning a set
/// that always includes `default_bucket`.
///
/// Uses `list_with_delimiter` to fetch only top-level prefixes in O(buckets)
/// instead of iterating every object in the store.
pub async fn scan_bucket_names(
    store: &dyn object_store::ObjectStore,
    default_bucket: &str,
) -> HashSet<String> {
    let mut buckets = HashSet::<String>::new();
    match store.list_with_delimiter(None).await {
        Ok(result) => {
            for prefix in &result.common_prefixes {
                let name = prefix.as_ref().trim_end_matches('/');
                if is_user_bucket(name) {
                    buckets.insert(name.to_string());
                }
            }
        }
        Err(e) => {
            tracing::warn!("bucket scan listing error: {e}");
        }
    }
    buckets.insert(default_bucket.to_string());
    buckets
}

// -- OfflineVizStore --------------------------------------------------

/// Tiny placeholder ObjectStore returned by viz endpoints for offline shards.
///
/// Every method returns a "shard is offline" error. The `list` stream
/// yields a single error item so callers that `try_next()` also see
/// the failure.
#[derive(Debug)]
pub struct OfflineVizStore;

impl std::fmt::Display for OfflineVizStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OfflineVizStore")
    }
}

fn _offline_err() -> object_store::Error {
    object_store::Error::Generic {
        store: "OfflineVizStore",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "shard is offline",
        )),
    }
}

#[async_trait::async_trait]
impl ObjectStore for OfflineVizStore {
    async fn put(&self, _loc: &object_store::path::Path, _payload: object_store::PutPayload) -> object_store::Result<object_store::PutResult> { Err(_offline_err()) }
    async fn put_opts(&self, _loc: &object_store::path::Path, _payload: object_store::PutPayload, _opts: object_store::PutOptions) -> object_store::Result<object_store::PutResult> { Err(_offline_err()) }
    async fn put_multipart_opts(&self, _loc: &object_store::path::Path, _opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> { Err(_offline_err()) }
    async fn get(&self, _loc: &object_store::path::Path) -> object_store::Result<object_store::GetResult> { Err(_offline_err()) }
    async fn get_opts(&self, _loc: &object_store::path::Path, _opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> { Err(_offline_err()) }
    async fn get_range(&self, _loc: &object_store::path::Path, _range: std::ops::Range<u64>) -> object_store::Result<bytes::Bytes> { Err(_offline_err()) }
    async fn head(&self, _loc: &object_store::path::Path) -> object_store::Result<object_store::ObjectMeta> { Err(_offline_err()) }
    async fn delete(&self, _loc: &object_store::path::Path) -> object_store::Result<()> { Err(_offline_err()) }
    fn list(&self, _: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        Box::pin(futures::stream::once(async { Err(_offline_err()) }))
    }
    async fn list_with_delimiter(&self, _prefix: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> { Err(_offline_err()) }
    async fn copy(&self, _from: &object_store::path::Path, _to: &object_store::path::Path) -> object_store::Result<()> { Err(_offline_err()) }
    async fn copy_if_not_exists(&self, _from: &object_store::path::Path, _to: &object_store::path::Path) -> object_store::Result<()> { Err(_offline_err()) }
}

// -- VizService -------------------------------------------------------

/// Identifies which exclusive admin operation is currently running.
/// Only one of these may run at a time.
#[derive(Debug, Clone, Copy)]
pub enum AdminOperation {
    Drain,
    RepairReplication,
    Redistribute,
    CrossVerify,
}

impl std::fmt::Display for AdminOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drain => write!(f, "drain"),
            Self::RepairReplication => write!(f, "repair-replication"),
            Self::Redistribute => write!(f, "redistribute"),
            Self::CrossVerify => write!(f, "cross-verify"),
        }
    }
}

/// Shared lock that prevents concurrent admin operations (drain,
/// repair-replication, redistribute).
pub type AdminOpLock = Arc<tokio::sync::Mutex<Option<AdminOperation>>>;

#[derive(Clone)]
pub struct VizService {
    pub s3: S3Service,
    /// All raw stores (only raw-backed shards). Use raw_index_map for lookup.
    pub raw_stores: Vec<Arc<RawObjectStore>>,
    /// All object stores, indexed by global shard ID.
    pub obj_stores: Vec<Arc<dyn ObjectStore>>,
    /// Human-readable name for each shard (indexed by global shard ID).
    pub shard_names: Vec<String>,
    /// Kind of each shard (indexed by global shard ID).
    pub shard_kinds: Vec<ShardKind>,
    /// Maps global shard ID -> index into raw_stores (None for non-raw).
    pub raw_index_map: Vec<Option<usize>>,
    /// True when running with multiple shards (CONFIG_FILE / STORE_N_*).
    pub cluster_mode: bool,
    /// Replication factor (1 = no replication, only meaningful in cluster mode).
    pub replication_factor: usize,
    /// Shared bucket registry (for rebuild-index / clear-bucket-cache)
    pub bucket_registry: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
    pub bucket: String,
    pub port: u16,
    pub bind: String,
    pub read_only: bool,
    /// Timestamp when the process first started (never resets).
    pub process_start_time: std::time::Instant,
    /// Timestamp when the config was last loaded/reloaded (resets on SIGHUP).
    pub config_loaded_time: std::time::Instant,
    /// Bearer token required for /_admin/* endpoints (None = unrestricted)
    pub admin_token: Option<String>,
    /// Allowed CORS origin for /_admin/* responses (None = no CORS headers)
    pub cors_origin: Option<String>,
    /// Server role: standalone, node, or coordinator
    pub role: String,
    /// Structured log buffer for REST API and web UI
    pub log_buffer: LogBuffer,
    /// Notify signal to trigger config reload from HTTP endpoint
    pub reload_signal: Arc<tokio::sync::Notify>,
    /// Optional cluster reference for querying shard health.
    pub cluster: Option<Arc<ShardedObjectStore>>,
    /// Live recovery status from the background task.
    pub recovery_status: Option<RecoveryStatusHandle>,
    pub repair_replication_status: Option<RepairReplicationStatusHandle>,
    /// Exclusive lock preventing concurrent admin operations.
    pub admin_op_lock: AdminOpLock,
    /// CLI command used to start the server.
    pub cli_command: String,
    /// Content of the config file (if --config was used).
    pub config_content: Option<String>,
    /// Per-shard stats cache: shard_id -> (file_count, data_bytes, when).
    pub stats_cache: Arc<std::sync::Mutex<std::collections::HashMap<usize, (u64, u64, std::time::Instant)>>>,
    /// Event socket path (None if no event socket is running).
    pub event_socket_path: Option<String>,
    /// Live subscriber count for the event socket (None if no socket).
    pub event_socket_connections: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// Event source address this node subscribes to (None if not a streaming replica).
    pub event_source: Option<String>,
    /// Human-readable node name (from --node CLI or bind:port fallback).
    pub node_name: String,
    /// Original stores for each shard (kept for reattach after detach).
    pub original_stores: Vec<Option<Arc<dyn ObjectStore>>>,
    /// Backing device/file paths per shard (for raw store reopen).
    pub raw_device_paths: Vec<Option<String>>,
    /// S3/node endpoint URLs per shard (None for non-S3 shards).
    pub shard_endpoints: Vec<Option<String>>,
    /// Broadcast channel for streaming operation progress to SSE clients.
    pub op_log_tx: tokio::sync::broadcast::Sender<String>,
}

/// RAII guard that releases the admin operation lock when dropped.
/// Ensures the lock is released even if the spawned task panics.
pub struct AdminOpGuard {
    lock: AdminOpLock,
}

impl AdminOpGuard {
    fn new(lock: AdminOpLock) -> Self {
        Self { lock }
    }
}

impl Drop for AdminOpGuard {
    fn drop(&mut self) {
        // Use try_lock to avoid blocking in the destructor.
        // In Tokio, this is safe because the mutex is not contended
        // during drop (only one admin op runs at a time).
        if let Ok(mut guard) = self.lock.try_lock() {
            *guard = None;
        }
    }
}

impl VizService {
    /// Cache TTL for non-raw store stats.
    const STATS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

    /// Try to acquire the admin operation lock. Returns an RAII guard
    /// that releases the lock on drop (including on panic). Returns
    /// `Err(response)` with a 409 Conflict if another operation is
    /// already running.
    async fn try_acquire_admin_op(
        &self,
        op: AdminOperation,
    ) -> std::result::Result<AdminOpGuard, http::Response<s3s::Body>> {
        let mut guard = self.admin_op_lock.lock().await;
        if let Some(current) = *guard {
            let body = serde_json::json!({
                "error": format!("{current} is already running"),
                "current_operation": current.to_string(),
            }).to_string();
            return Err(http::Response::builder()
                .status(409)
                .header("content-type", "application/json")
                .body(s3s::Body::from(body))
                .expect("static response"));
        }
        *guard = Some(op);
        Ok(AdminOpGuard::new(Arc::clone(&self.admin_op_lock)))
    }

    /// List all objects in a non-raw ObjectStore and return (file_count, data_bytes).
    /// Results are cached per shard for STATS_CACHE_TTL.
    async fn list_store_stats(&self, shard_id: usize) -> (u64, u64) {
        // Check cache first.
        {
            let cache = self.stats_cache.lock().unwrap();
            if let Some(&(fc, db, when)) = cache.get(&shard_id) {
                if when.elapsed() < Self::STATS_CACHE_TTL {
                    return (fc, db);
                }
            }
        }

        use futures::StreamExt;
        let store = &self.obj_stores[shard_id];
        let mut file_count: u64 = 0;
        let mut data_bytes: u64 = 0;
        let mut stream = store.list(None);
        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let key = meta.location.to_string();
                if is_internal_key(&key) { continue; }
                file_count += 1;
                data_bytes += meta.size as u64;
            }
        }

        // Store in cache.
        {
            let mut cache = self.stats_cache.lock().unwrap();
            cache.insert(shard_id, (file_count, data_bytes, std::time::Instant::now()));
        }

        (file_count, data_bytes)
    }

    /// Return the shard type string for a given ShardKind.
    fn shard_type_str(kind: &ShardKind, name: &str) -> &'static str {
        match kind {
            ShardKind::Raw => "raw",
            ShardKind::S3Like => {
                if name.starts_with("node:") { "node" }
                else { "s3" }
            }
            ShardKind::Sidecar => {
                if name.starts_with("fs:") { "fs" }
                else { "mem" }
            }
        }
    }

    /// Get filesystem stats for a path: (total_bytes, free_bytes, fs_type).
    /// Uses `stat -f` and `/proc/mounts` on Linux.
    fn fs_disk_stats(path: &str) -> (Option<u64>, Option<u64>, Option<String>) {
        // Use stat -f to get block size, total blocks, free blocks
        let output = std::process::Command::new("stat")
            .args(["-f", "-c", "%S %b %a", path])
            .output();
        let (total, free) = match output {
            Ok(o) if o.status.success() => {
                let s = String::from_utf8_lossy(&o.stdout);
                parse_stat_output(&s)
            }
            _ => (None, None),
        };

        // Read /proc/mounts for filesystem type
        let fs_type = std::fs::read_to_string("/proc/mounts")
            .ok()
            .and_then(|mounts| {
                let real_path = std::fs::canonicalize(path)
                    .unwrap_or_else(|_| std::path::PathBuf::from(path));
                let real_str = real_path.to_string_lossy();
                parse_mounts_fstype(&mounts, &real_str)
            });

        (total, free, fs_type)
    }

    fn json_response(body: String, cors_origin: Option<&str>) -> http::Response<s3s::Body> {
        let mut builder = http::Response::builder()
            .status(200)
            .header("content-type", "application/json");
        if let Some(origin) = cors_origin {
            builder = builder.header("access-control-allow-origin", origin);
        }
        builder.body(s3s::Body::from(body)).expect("static response")
    }

    fn forbidden() -> http::Response<s3s::Body> {
        http::Response::builder()
            .status(403)
            .header("content-type", "application/json")
            .body(s3s::Body::from(r#"{"error":"forbidden"}"#.to_string()))
            .expect("static response")
    }

    fn preflight(cors_origin: Option<&str>) -> http::Response<s3s::Body> {
        let mut builder = http::Response::builder()
            .status(204)
            .header("access-control-allow-methods", "GET, POST, OPTIONS")
            .header("access-control-allow-headers", "authorization, content-type")
            .header("access-control-max-age", "86400");
        if let Some(origin) = cors_origin {
            builder = builder.header("access-control-allow-origin", origin);
        }
        builder.body(s3s::Body::empty()).expect("static response")
    }

    fn html_response(body: &'static str) -> http::Response<s3s::Body> {
        http::Response::builder()
            .status(200)
            .header("content-type", "text/html; charset=utf-8")
            .body(s3s::Body::from(bytes::Bytes::from(body)))
            .expect("static response")
    }

    fn not_found() -> http::Response<s3s::Body> {
        http::Response::builder()
            .status(404)
            .header("content-type", "text/plain")
            .body(s3s::Body::from("not found".to_string()))
            .expect("static response")
    }

    fn unavailable() -> http::Response<s3s::Body> {
        http::Response::builder()
            .status(503)
            .header("content-type", "application/json")
            .body(s3s::Body::from(r#"{"error":"visualization requires BACKEND=raw"}"#.to_string()))
            .expect("static response")
    }

    async fn handle_admin(&self, path: &str, query: Option<&str>) -> http::Response<s3s::Body> {
        // Per-shard routes: /_admin/shard/{id}/{sub}
        if let Some(rest) = path.strip_prefix("/_admin/shard/") {
            let mut parts = rest.splitn(2, '/');
            let shard_id = match parts.next().and_then(|s| s.parse::<usize>().ok()) {
                Some(id) if id < self.obj_stores.len() => id,
                _ => return Self::not_found(),
            };
            let sub = parts.next().unwrap_or("viz");
            let kind = self.shard_kinds.get(shard_id).copied().unwrap_or(ShardKind::Sidecar);

            // For raw shards, use the full device endpoints
            if let Some(raw_idx) = self.raw_index_map.get(shard_id).copied().flatten() {
                let store = &self.raw_stores[raw_idx];
                return match sub {
                    "viz" => Self::html_response(VIZ_HTML),
                    "ui" => Self::html_response(UI_HTML),
                    "server" => Self::html_response(SERVER_HTML),
                    "info" => {
                        let info = store.device_info();
                        let json = serde_json::json!({
                            "shard_id":               shard_id,
                            "shard_name":             self.shard_names.get(shard_id).cloned().unwrap_or_default(),
                            "type":                   "raw",
                            "device_path":            info.device_path,
                            "device_size":            info.device_size,
                            "format_version":         info.format_version,
                            "direct_io":              info.direct_io,
                            "txn_id":                 info.txn_id,
                            "file_count":             info.file_count,
                            "data_bytes_stored":      info.data_bytes_stored,
                            "device_bytes_used":      info.device_bytes_used,
                            "free_space":             info.free_space,
                            "free_fragments":         info.free_fragments,
                            "largest_free_extent":    info.largest_free_extent,
                            "index_serialized_bytes": info.index_serialized_bytes,
                            "index_slot_capacity":    info.index_slot_capacity,
                            "shard_sizes":            info.shard_sizes,
                            "shard_slot_size":        info.shard_slot_size,
                            "compression":            info.compression.as_str(),
                        });
                        Self::json_response(json.to_string(), self.cors_origin.as_deref())
                    }
                    _ => self.handle_device_endpoint(store, sub, query),
                };
            }

            // Non-raw shard: html pages + info + objects
            let name = self.shard_names.get(shard_id).cloned().unwrap_or_default();
            let stype = Self::shard_type_str(&kind, &name);
            return match sub {
                "ui" => Self::html_response(UI_HTML),
                "server" => Self::html_response(SERVER_HTML),
                "info" => {
                    let (file_count, data_bytes) = self.list_store_stats(shard_id).await;
                    let json = serde_json::json!({
                        "shard_id":          shard_id,
                        "shard_name":        name,
                        "type":              stype,
                        "file_count":        file_count,
                        "data_bytes_stored": data_bytes,
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                }
                "objects" => {
                    use futures::StreamExt;
                    let prefix = qparam(query, "prefix").unwrap_or_default();
                    let offset: usize = qparam(query, "offset").and_then(|v| v.parse().ok()).unwrap_or(0);
                    let limit: usize = qparam(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(100).min(1000);
                    let store = &self.obj_stores[shard_id];
                    let mut all: Vec<serde_json::Value> = Vec::new();
                    let mut stream = store.list(None);
                    while let Some(item) = stream.next().await {
                        if let Ok(meta) = item {
                            let key = meta.location.to_string();
                            if is_internal_key(&key) { continue; }
                            if !prefix.is_empty() && !key.contains(&prefix) { continue; }
                            all.push(serde_json::json!({
                                "key": key,
                                "size": meta.size,
                                "padded_size": meta.size,
                                "offset": 0,
                                "created_txn": 0,
                                "last_modified": meta.last_modified.to_rfc3339(),
                                "uncompressed_size": 0,
                            }));
                        }
                    }
                    all.sort_by(|a, b| {
                        let at = a["last_modified"].as_str().unwrap_or("");
                        let bt = b["last_modified"].as_str().unwrap_or("");
                        bt.cmp(at)
                    });
                    let total = all.len();
                    let page: Vec<_> = all.into_iter().skip(offset).take(limit).collect();
                    let json = serde_json::json!({
                        "total": total,
                        "offset": offset,
                        "limit": limit,
                        "objects": page,
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                }
                _ => Self::not_found(),
            };
        }

        match path {
            "/_admin/" | "/_admin/cluster" => Self::html_response(CLUSTER_HTML),

            "/_admin/viz" => {
                    Self::html_response(VIZ_HTML)
            }

            "/_admin/config" => Self::html_response(CONFIG_HTML),

            "/_admin/ui" => Self::html_response(UI_HTML),

            "/_admin/server" => Self::html_response(SERVER_HTML),

            "/_admin/logs/ui" => Self::html_response(LOGS_HTML),

            "/_admin/logs" => {
                let filter = LogFilter::from_query(query);
                let result = self.log_buffer.query(&filter);
                let json = serde_json::to_string(&result).unwrap_or_else(|_| "{}".into());
                Self::json_response(json, self.cors_origin.as_deref())
            }

            // /_admin/recovery -- live recovery status and config
            "/_admin/recovery" => {
                if let Some(ref handle) = self.recovery_status {
                    let st = handle.lock().clone();
                    let under_count = self.cluster.as_ref()
                        .map(|c| c.find_under_replicated().len())
                        .unwrap_or(0);
                    let over_count = self.cluster.as_ref()
                        .map(|c| c.find_over_replicated().len())
                        .unwrap_or(0);
                    let dm_count = self.cluster.as_ref()
                        .map(|c| c.catalog().with_entries(|m| {
                            m.keys().filter(|k| k.starts_with(
                                shardedobjstr::DELETE_MARKER_PREFIX)).count()
                        }))
                        .unwrap_or(0);
                    let json = serde_json::json!({
                        "enabled":                  st.config.enabled,
                        "poll_interval_secs":       st.config.poll_interval_secs,
                        "probe_timeout_secs":       st.config.probe_timeout_secs,
                        "failure_threshold":        st.config.failure_threshold,
                        "re_replicate_batch_size":  st.config.re_replicate_batch_size,
                        "poll_cycles":              st.poll_cycles,
                        "total_re_replicated":      st.total_re_replicated,
                        "last_sweep_count":         st.last_sweep_count,
                        "under_replicated_count":   under_count,
                        "over_replicated_count":    over_count,
                        "delete_marker_count":      dm_count,
                        "total_trimmed":            st.total_trimmed,
                        "last_trim_count":          st.last_trim_count,
                        "last_poll_at":             st.last_poll_at,
                        "last_sweep_at":            st.last_sweep_at,
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                } else {
                    let json = serde_json::json!({
                        "enabled": false,
                        "note": "recovery task not active (single-store mode)"
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                }
            }

            // /_admin/repair-replication-status -- live repair-replication task status
            "/_admin/repair-replication-status" => {
                if let Some(ref handle) = self.repair_replication_status {
                    let st = handle.lock();
                    let json = serde_json::json!({
                        "enabled":                    st.enabled,
                        "interval_secs":              st.interval_secs,
                        "batch_size":                 st.batch_size,
                        "phase":                      st.phase,
                        "cycles_completed":           st.cycles_completed,
                        "total_objects_replicated":   st.total_objects_replicated,
                        "total_objects_trimmed":      st.total_objects_trimmed,
                        "last_catalog_size":          st.last_catalog_size,
                        "remaining_under_replicated": st.remaining_under_replicated,
                        "last_completed_at":          st.last_completed_at,
                        "last_started_at":            st.last_started_at,
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                } else {
                    let json = serde_json::json!({
                        "enabled": false,
                        "note": "repair-replication task not active"
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                }
            }

            // /_admin/admin-op-status -- check if an exclusive admin op is running
            "/_admin/admin-op-status" => {
                let guard = self.admin_op_lock.lock().await;
                let json = if let Some(op) = *guard {
                    serde_json::json!({
                        "running": true,
                        "operation": op.to_string(),
                    })
                } else {
                    serde_json::json!({
                        "running": false,
                    })
                };
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            // /_admin/nodeconfig -- raw node config for the config viewer page
            "/_admin/nodeconfig" => {
                let shard_count = self.obj_stores.len();
                let mut shards_json = Vec::new();
                for i in 0..shard_count {
                    let name = self.shard_names.get(i).cloned().unwrap_or_else(|| format!("shard-{i}"));
                    let kind = self.shard_kinds.get(i);
                    let stype = kind.map(|k| Self::shard_type_str(k, &name)).unwrap_or("unknown");
                    shards_json.push(serde_json::json!({
                        "id": i,
                        "name": name,
                        "type": stype,
                    }));
                }
                let recovery = if let Some(ref handle) = self.recovery_status {
                    let st = handle.lock();
                    serde_json::json!({
                        "enabled":                 st.config.enabled,
                        "poll_interval_secs":      st.config.poll_interval_secs,
                        "probe_timeout_secs":      st.config.probe_timeout_secs,
                        "failure_threshold":       st.config.failure_threshold,
                        "re_replicate_batch_size": st.config.re_replicate_batch_size,
                    })
                } else {
                    serde_json::json!({"enabled": false})
                };
                let json = serde_json::json!({
                    "role":                self.role,
                    "port":                self.port,
                    "bind":                self.bind,
                    "bucket":              self.bucket,
                    "cluster_mode":        self.cluster_mode,
                    "replication_factor":  self.replication_factor,
                    "shard_count":         shard_count,
                    "shards":              shards_json,
                    "recovery":            recovery,
                    "cli_command":         self.cli_command,
                    "config_content":      self.config_content,
                });
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            "/_admin/shards" => {
                let mut shards = Vec::new();
                for (i, kind) in self.shard_kinds.iter().enumerate() {
                    let name = self.shard_names.get(i).cloned().unwrap_or_else(|| format!("shard-{i}"));
                    let stype = Self::shard_type_str(kind, &name);

                    // Query real shard health from the cluster (if available).
                    let health_str = self.cluster.as_ref()
                        .and_then(|c| c.shard_health(i))
                        .map(|h| match h {
                            ShardHealth::Healthy  => "Healthy",
                            ShardHealth::Degraded => "Degraded",
                            ShardHealth::Offline  => "Offline",
                            ShardHealth::Syncing  => "Syncing",
                            ShardHealth::Detached => "Detached",
                        })
                        .unwrap_or("Healthy");

                    let crc_errors = self.cluster.as_ref()
                        .map(|c| c.shard_crc_error_count(i))
                        .unwrap_or(0);

                    let detach_reason = self.cluster.as_ref()
                        .and_then(|c| c.shard_detach_reason(i))
                        .map(|r| match r {
                            shardedobjstr::DetachReason::Manual => "manual",
                            shardedobjstr::DetachReason::ProbeFailure => "probe_failure",
                            shardedobjstr::DetachReason::DeviceMissing => "device_missing",
                            shardedobjstr::DetachReason::Drain => "drain",
                        });

                    let suppress_replication = self.cluster.as_ref()
                        .map(|c| c.shard_suppress_replication(i))
                        .unwrap_or(false);

                    let shard_json = if health_str == "Offline" || health_str == "Detached" {
                        // Offline/Detached shard: show config info but no live stats.
                        serde_json::json!({
                            "id":                i,
                            "name":              name,
                            "type":              stype,
                            "health":            health_str,
                            "crc_error_count":   crc_errors,
                            "detach_reason":     detach_reason,
                            "suppress_replication": suppress_replication,
                            "device_path":       serde_json::Value::Null,
                            "device_size":       serde_json::Value::Null,
                            "file_count":        0,
                            "data_bytes_stored": 0,
                            "device_bytes_used": 0,
                            "free_space":        serde_json::Value::Null,
                            "free_fragments":    serde_json::Value::Null,
                            "direct_io":         false,
                            "compression":       "none",
                        })
                    } else if let Some(raw_idx) = self.raw_index_map.get(i).copied().flatten() {
                        let info = self.raw_stores[raw_idx].device_info();
                        serde_json::json!({
                            "id":                i,
                            "name":              name,
                            "type":              stype,
                            "health":            health_str,
                            "crc_error_count":   crc_errors,
                            "detach_reason":     detach_reason,
                            "suppress_replication": suppress_replication,
                            "device_path":       info.device_path,
                            "device_size":       info.device_size,
                            "file_count":        info.file_count,
                            "data_bytes_stored": info.data_bytes_stored,
                            "device_bytes_used": info.device_bytes_used,
                            "free_space":        info.free_space,
                            "free_fragments":    info.free_fragments,
                            "direct_io":         info.direct_io,
                            "compression":       info.compression.as_str(),
                        })
                    } else {
                        let (file_count, data_bytes) = self.list_store_stats(i).await;
                        let endpoint = self.shard_endpoints.get(i).cloned().flatten();

                        // For FS shards, get disk usage and filesystem type
                        let (disk_total, disk_free, fs_type) = if stype == "fs" {
                            if let Some(ref path) = self.raw_device_paths.get(i).and_then(|p| p.as_ref()) {
                                Self::fs_disk_stats(path)
                            } else {
                                (None, None, None)
                            }
                        } else {
                            (None, None, None)
                        };
                        let disk_used = match (disk_total, disk_free) {
                            (Some(t), Some(f)) => Some(t.saturating_sub(f)),
                            _ => None,
                        };

                        serde_json::json!({
                            "id":                i,
                            "name":              name,
                            "type":              stype,
                            "health":            health_str,
                            "crc_error_count":   crc_errors,
                            "detach_reason":     detach_reason,
                            "suppress_replication": suppress_replication,
                            "device_path":       serde_json::Value::Null,
                            "device_size":       disk_total,
                            "file_count":        file_count,
                            "data_bytes_stored": data_bytes,
                            "device_bytes_used": disk_used.unwrap_or(data_bytes),
                            "free_space":        disk_free,
                            "free_fragments":    serde_json::Value::Null,
                            "direct_io":         false,
                            "compression":       "none",
                            "endpoint":          endpoint,
                            "fs_type":           fs_type,
                        })
                    };
                    shards.push(shard_json);
                }
                let json = serde_json::json!({
                    "mode":               if self.cluster_mode { "sharded" } else { "single" },
                    "replication_factor":  self.replication_factor,
                    "shard_count":         self.obj_stores.len(),
                    "shards":             shards,
                });
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            "/_admin/sysinfo" => {
                let process_uptime_secs = self.process_start_time.elapsed().as_secs();
                let config_uptime_secs = self.config_loaded_time.elapsed().as_secs();

                let sys_uptime_secs = std::fs::read_to_string("/proc/uptime")
                    .ok()
                    .map(|s| parse_proc_uptime(&s))
                    .unwrap_or(0);

                let (load1, load5, load15) = std::fs::read_to_string("/proc/loadavg")
                    .ok()
                    .map(|s| parse_proc_loadavg(&s))
                    .unwrap_or((0.0, 0.0, 0.0));

                let (mem_total_kb, mem_free_kb, mem_available_kb) =
                    std::fs::read_to_string("/proc/meminfo")
                        .ok()
                        .map(|s| parse_proc_meminfo(&s))
                        .unwrap_or((0, 0, 0));

                // Process memory from /proc/self/status (VmRSS)
                let process_rss_kb = std::fs::read_to_string("/proc/self/status")
                    .ok()
                    .map(|s| parse_proc_status_rss(&s))
                    .unwrap_or(0);

                let json = serde_json::json!({
                    "process_uptime_secs": process_uptime_secs,
                    "config_uptime_secs":  config_uptime_secs,
                    "system_uptime_secs":  sys_uptime_secs,
                    "load_avg_1":          load1,
                    "load_avg_5":          load5,
                    "load_avg_15":         load15,
                    "mem_total_kb":        mem_total_kb,
                    "mem_free_kb":         mem_free_kb,
                    "mem_available_kb":    mem_available_kb,
                    "process_rss_kb":      process_rss_kb,
                });
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            "/_admin/info" => {
                let json = if let Some(store) = self.raw_stores.first() {
                    let info = store.device_info();
                    let mut obj = serde_json::json!({
                        "pid":                    std::process::id(),
                        "build_git_hash":         env!("BUILD_GIT_HASH"),
                        "build_date":             env!("BUILD_DATE"),
                        "rawobjst_version":    rawobjstr::VERSION,
                        "rawobjst_git_hash":   rawobjstr::BUILD_GIT_HASH,
                        "rawobjst_build_date": rawobjstr::BUILD_DATE,
                        "shardedobjstr_version":    shardedobjstr::VERSION,
                        "shardedobjstr_git_hash":   shardedobjstr::BUILD_GIT_HASH,
                        "shardedobjstr_build_date": shardedobjstr::BUILD_DATE,
                        "role":                   self.role,
                        "node_name":              self.node_name,
                        "port":                   self.port,
                        "bind":                   self.bind,
                        "bucket":                 self.bucket,
                        "backend":                "raw",
                        "has_raw":                true,
                        "cluster_mode":           self.cluster_mode,
                        "shard_count":            self.obj_stores.len(),
                        "replication_factor":     self.replication_factor,
                        "device_path":            info.device_path,
                        "device_size":            info.device_size,
                        "format_version":         info.format_version,
                        "direct_io":              info.direct_io,
                        "read_only":              self.read_only,
                        "txn_id":                 info.txn_id,
                        "file_count":             info.file_count,
                        "data_bytes_stored":      info.data_bytes_stored,
                        "device_bytes_used":      info.device_bytes_used,
                        "free_space":             info.free_space,
                        "free_fragments":         info.free_fragments,
                        "largest_free_extent":    info.largest_free_extent,
                        "index_serialized_bytes": info.index_serialized_bytes,
                        "index_slot_capacity":    info.index_slot_capacity,
                        "shard_sizes":            info.shard_sizes,
                        "shard_slot_size":        info.shard_slot_size,
                        "compression":            info.compression.as_str(),
                    });
                    // In cluster mode, add aggregate totals across all shards
                    if self.cluster_mode && self.obj_stores.len() > 1 {
                        let mut total_files = info.file_count;
                        let mut total_data = info.data_bytes_stored;
                        let mut total_used = info.device_bytes_used;
                        let mut total_free = info.free_space;
                        let mut total_size = info.device_size;
                        for s in self.raw_stores.iter().skip(1) {
                            let si = s.device_info();
                            total_files += si.file_count;
                            total_data += si.data_bytes_stored;
                            total_used += si.device_bytes_used;
                            total_free += si.free_space;
                            total_size += si.device_size;
                        }
                        // Include non-raw shards in totals
                        for (i, kind) in self.shard_kinds.iter().enumerate() {
                            if !matches!(kind, ShardKind::Raw) {
                                let (fc, db) = self.list_store_stats(i).await;
                                total_files += fc as usize;
                                total_data += db;
                                total_used += db;
                            }
                        }
                        obj["total_file_count"] = serde_json::json!(total_files);
                        obj["total_data_bytes_stored"] = serde_json::json!(total_data);
                        obj["total_device_bytes_used"] = serde_json::json!(total_used);
                        obj["total_free_space"] = serde_json::json!(total_free);
                        obj["total_device_size"] = serde_json::json!(total_size);
                    }
                    if let Some(ref p) = self.event_socket_path {
                        obj["event_socket"] = serde_json::json!(p);
                        let count = self.event_socket_connections.as_ref()
                            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                            .unwrap_or(0);
                        obj["event_socket_subscribers"] = serde_json::json!(count);
                    }
                    if let Some(ref src) = self.event_source {
                        obj["event_source"] = serde_json::json!(src);
                    }
                    obj
                } else {
                    let (fc, db) = self.list_store_stats(0).await;
                    let mut obj = serde_json::json!({
                        "pid":            std::process::id(),
                        "build_git_hash": env!("BUILD_GIT_HASH"),
                        "build_date":     env!("BUILD_DATE"),
                        "rawobjst_version":    rawobjstr::VERSION,
                        "rawobjst_git_hash":   rawobjstr::BUILD_GIT_HASH,
                        "rawobjst_build_date": rawobjstr::BUILD_DATE,
                        "shardedobjstr_version":    shardedobjstr::VERSION,
                        "shardedobjstr_git_hash":   shardedobjstr::BUILD_GIT_HASH,
                        "shardedobjstr_build_date": shardedobjstr::BUILD_DATE,
                        "role":           self.role,
                        "node_name":      self.node_name,
                        "port":           self.port,
                        "bind":           self.bind,
                        "bucket":         self.bucket,
                        "backend":        "non-raw",
                        "has_raw":        false,
                        "cluster_mode":   self.cluster_mode,
                        "shard_count":    self.obj_stores.len(),
                        "replication_factor": self.replication_factor,
                        "read_only":      self.read_only,
                        "file_count":     fc,
                        "data_bytes_stored": db,
                    });
                    if let Some(ref p) = self.event_socket_path {
                        obj["event_socket"] = serde_json::json!(p);
                        let count = self.event_socket_connections.as_ref()
                            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                            .unwrap_or(0);
                        obj["event_socket_subscribers"] = serde_json::json!(count);
                    }
                    if let Some(ref src) = self.event_source {
                        obj["event_source"] = serde_json::json!(src);
                    }
                    obj
                };
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            "/_admin/buckets" => {
                let reg = self.bucket_registry.try_read()
                    .map(|g| g.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                let mut list = reg;
                list.sort();
                Self::json_response(serde_json::json!({ "buckets": list }).to_string(), self.cors_origin.as_deref())
            }

            "/_admin/objects" => {
                if self.obj_stores.len() > 1 {
                    // Multi-shard: aggregate objects from all shards, consolidating
                    // replicas of the same key into a single entry with shard_ids.
                    use futures::StreamExt;
                    let prefix = qparam(query, "prefix").unwrap_or_default();
                    let offset: usize = qparam(query, "offset").and_then(|v| v.parse().ok()).unwrap_or(0);
                    let limit: usize = qparam(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(100).min(1000);

                    // Keyed accumulator: key -> (metadata from first shard, shard_ids)
                    struct ObjEntry {
                        size: u64,
                        padded_size: u64,
                        offset: u64,
                        created_txn: u64,
                        last_modified: String,
                        uncompressed_size: u64,
                        shard_ids: Vec<usize>,
                    }
                    let mut by_key: std::collections::HashMap<String, ObjEntry> = std::collections::HashMap::new();

                    for (sid, _kind) in self.shard_kinds.iter().enumerate() {
                        // Skip unavailable shards (Offline or Detached)
                        if let Some(c) = &self.cluster {
                            if matches!(c.shard_health(sid), Some(h) if h.is_unavailable()) {
                                continue;
                            }
                        }
                        if let Some(raw_idx) = self.raw_index_map.get(sid).copied().flatten() {
                            let layout = self.raw_stores[raw_idx].layout_map();
                            for e in &layout.extents {
                                if is_internal_key(&e.key) { continue; }
                                if !prefix.is_empty() && !e.key.contains(&prefix) { continue; }
                                let entry = by_key.entry(e.key.clone()).or_insert_with(|| ObjEntry {
                                    size: e.size,
                                    padded_size: e.padded_size,
                                    offset: e.offset,
                                    created_txn: e.created_txn,
                                    last_modified: e.last_modified.to_rfc3339(),
                                    uncompressed_size: e.uncompressed_size,
                                    shard_ids: Vec::new(),
                                });
                                if !entry.shard_ids.contains(&sid) {
                                    entry.shard_ids.push(sid);
                                }
                            }
                        } else {
                            let store = &self.obj_stores[sid];
                            let mut stream = store.list(None);
                            while let Some(item) = stream.next().await {
                                if let Ok(meta) = item {
                                    let key = meta.location.to_string();
                                    if is_internal_key(&key) { continue; }
                                    if !prefix.is_empty() && !key.contains(&prefix) { continue; }
                                    let entry = by_key.entry(key).or_insert_with(|| ObjEntry {
                                        size: meta.size as u64,
                                        padded_size: meta.size as u64,
                                        offset: 0,
                                        created_txn: 0,
                                        last_modified: meta.last_modified.to_rfc3339(),
                                        uncompressed_size: 0,
                                        shard_ids: Vec::new(),
                                    });
                                    if !entry.shard_ids.contains(&sid) {
                                        entry.shard_ids.push(sid);
                                    }
                                }
                            }
                        }
                    }

                    let mut all_objects: Vec<serde_json::Value> = by_key
                        .into_iter()
                        .map(|(key, e)| {
                            let mut ids = e.shard_ids;
                            ids.sort_unstable();
                            serde_json::json!({
                                "key": key,
                                "size": e.size,
                                "padded_size": e.padded_size,
                                "offset": e.offset,
                                "created_txn": e.created_txn,
                                "last_modified": e.last_modified,
                                "uncompressed_size": e.uncompressed_size,
                                "shard_ids": ids,
                            })
                        })
                        .collect();

                    all_objects.sort_by(|a, b| {
                        let at = a["last_modified"].as_str().unwrap_or("");
                        let bt = b["last_modified"].as_str().unwrap_or("");
                        bt.cmp(at)
                    });
                    let total = all_objects.len();
                    let page: Vec<_> = all_objects.into_iter()
                        .skip(offset)
                        .take(limit)
                        .collect();
                    let json = serde_json::json!({
                        "total": total,
                        "offset": offset,
                        "limit": limit,
                        "shard_count": self.obj_stores.len(),
                        "objects": page,
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                } else if let Some(store) = self.raw_stores.first() {
                    self.handle_device_endpoint(store, "objects", query)
                } else {
                    // Non-raw standalone: list via ObjectStore trait
                    use futures::StreamExt;
                    let prefix = qparam(query, "prefix").unwrap_or_default();
                    let offset: usize = qparam(query, "offset").and_then(|v| v.parse().ok()).unwrap_or(0);
                    let limit: usize = qparam(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(100).min(1000);

                    let store = &self.obj_stores[0];
                    let mut objects: Vec<serde_json::Value> = Vec::new();
                    let mut stream = store.list(None);
                    while let Some(item) = stream.next().await {
                        if let Ok(meta) = item {
                            let key = meta.location.to_string();
                            if is_internal_key(&key) { continue; }
                            if !prefix.is_empty() && !key.contains(&prefix) { continue; }
                            objects.push(serde_json::json!({
                                "key": key,
                                "size": meta.size as u64,
                                "padded_size": meta.size as u64,
                                "offset": 0,
                                "created_txn": 0,
                                "last_modified": meta.last_modified.to_rfc3339(),
                                "uncompressed_size": 0,
                            }));
                        }
                    }
                    objects.sort_by(|a, b| {
                        let at = a["last_modified"].as_str().unwrap_or("");
                        let bt = b["last_modified"].as_str().unwrap_or("");
                        bt.cmp(at)
                    });
                    let total = objects.len();
                    let page: Vec<_> = objects.into_iter().skip(offset).take(limit).collect();
                    let json = serde_json::json!({
                        "total": total,
                        "offset": offset,
                        "limit": limit,
                        "objects": page,
                    });
                    Self::json_response(json.to_string(), self.cors_origin.as_deref())
                }
            }

            "/_admin/heatmap" | "/_admin/region" | "/_admin/extent_meta"
            | "/_admin/getraw" => {
                let Some(store) = self.raw_stores.first() else {
                    return Self::unavailable();
                };
                let endpoint = path.strip_prefix("/_admin/").unwrap_or(path);
                self.handle_device_endpoint(store, endpoint, query)
            }

            _ => Self::not_found(),
        }
    }

    /// Handle device-specific endpoints for a given raw store.
    ///
    /// This is called both from top-level routes (using the primary store)
    /// and from per-shard routes (using the shard-specific store).
    fn handle_device_endpoint(
        &self,
        store: &RawObjectStore,
        endpoint: &str,
        query: Option<&str>,
    ) -> http::Response<s3s::Body> {
        match endpoint {
            "heatmap" => {
                let chunks: usize = qparam(query, "chunks")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(256)
                    .min(4096)
                    .max(16);
                let layout = store.layout_map();
                let device_size = layout.device_size;
                let chunk_size = (device_size + chunks as u64 - 1) / chunks as u64;

                let mut file_counts = vec![0u32; chunks];
                let mut used_bytes  = vec![0u64; chunks];
                let mut free_bytes  = vec![0u64; chunks];

                for ext in &layout.extents {
                    let start = ext.offset;
                    let end = ext.offset + ext.padded_size;
                    let c0 = (start / chunk_size) as usize;
                    let c1 = ((end.saturating_sub(1)) / chunk_size) as usize;
                    for c in c0..=c1.min(chunks - 1) {
                        let cb_start = c as u64 * chunk_size;
                        let cb_end = cb_start + chunk_size;
                        let overlap_start = start.max(cb_start);
                        let overlap_end = end.min(cb_end);
                        if overlap_start < overlap_end {
                            used_bytes[c] += overlap_end - overlap_start;
                            if c == c0 { file_counts[c] += 1; }
                        }
                    }
                }

                for &(off, sz) in &layout.free_regions {
                    let start = off;
                    let end = off + sz;
                    let c0 = (start / chunk_size) as usize;
                    let c1 = ((end.saturating_sub(1)) / chunk_size) as usize;
                    for c in c0..=c1.min(chunks - 1) {
                        let cb_start = c as u64 * chunk_size;
                        let cb_end = cb_start + chunk_size;
                        let overlap_start = start.max(cb_start);
                        let overlap_end = end.min(cb_end);
                        if overlap_start < overlap_end {
                            free_bytes[c] += overlap_end - overlap_start;
                        }
                    }
                }

                let dead_bytes: Vec<u64> = (0..chunks).map(|c| {
                    let cb_start = c as u64 * chunk_size;
                    let cb_end = (cb_start + chunk_size).min(device_size);
                    let csize = cb_end - cb_start;
                    csize.saturating_sub(used_bytes[c] + free_bytes[c])
                }).collect();

                let json = serde_json::json!({
                    "device_size": device_size,
                    "chunk_size": chunk_size,
                    "chunks": chunks,
                    "data_region_start": layout.data_region_start,
                    "data_region_end": layout.data_region_end,
                    "index_region_a": layout.index_region_a,
                    "index_region_b": layout.index_region_b,
                    "active_index_region": layout.active_index_region,
                    "txn_id": layout.txn_id,
                    "total_extents": layout.extents.len(),
                    "total_free_regions": layout.free_regions.len(),
                    "file_counts": file_counts,
                    "used_bytes": used_bytes,
                    "free_bytes": free_bytes,
                    "dead_bytes": dead_bytes,
                });
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            "region" => {
                let layout = store.layout_map();
                let from: u64 = qparam(query, "from").and_then(|v| v.parse().ok()).unwrap_or(0);
                let to: u64 = qparam(query, "to").and_then(|v| v.parse().ok()).unwrap_or(layout.device_size);

                let extents: Vec<serde_json::Value> = layout.extents.iter()
                    .filter(|e| e.offset + e.padded_size > from && e.offset < to)
                    .map(|e| serde_json::json!({
                        "key": e.key,
                        "offset": e.offset,
                        "size": e.size,
                        "padded_size": e.padded_size,
                        "created_txn": e.created_txn,
                        "last_modified": e.last_modified.to_rfc3339(),
                        "uncompressed_size": e.uncompressed_size,
                    }))
                    .collect();
                let free: Vec<serde_json::Value> = layout.free_regions.iter()
                    .filter(|&&(off, sz)| off + sz > from && off < to)
                    .map(|&(off, sz)| serde_json::json!({ "offset": off, "size": sz }))
                    .collect();
                let json = serde_json::json!({
                    "from": from,
                    "to": to,
                    "extents": extents,
                    "free_regions": free,
                });
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            "extent_meta" => {
                let key = match qparam(query, "key") {
                    Some(k) if !k.is_empty() => k,
                    _ => return Self::json_response("{}".into(), self.cors_origin.as_deref()),
                };
                let path = object_store::path::Path::parse(&key)
                    .unwrap_or_else(|_| object_store::path::Path::from(key));
                match store.get_metadata(&path) {
                    Ok(data) if !data.is_empty() => {
                        let meta = decode_metadata(&data);
                        let json = serde_json::to_string(&meta).unwrap_or_else(|_| "{}".into());
                        Self::json_response(json, self.cors_origin.as_deref())
                    }
                    _ => Self::json_response("{}".into(), self.cors_origin.as_deref()),
                }
            }

            "getraw" => {
                let key = match qparam(query, "key") {
                    Some(k) if !k.is_empty() => k,
                    _ => {
                        return http::Response::builder()
                            .status(400)
                            .header("content-type", "text/plain")
                            .body(s3s::Body::from("missing ?key= parameter".to_string()))
                            .unwrap();
                    }
                };
                let path = object_store::path::Path::parse(&key)
                    .unwrap_or_else(|_| object_store::path::Path::from(key));
                match store.get_raw(&path) {
                    Ok(raw_result) => {
                        http::Response::builder()
                            .status(200)
                            .header("content-type", "application/octet-stream")
                            .header("x-raw-uncompressed-size", raw_result.uncompressed_size.to_string())
                            .header("x-raw-compression", raw_result.compression.as_str())
                            .body(s3s::Body::from(raw_result.data))
                            .unwrap()
                    }
                    Err(rawobjstr::RawStoreError::NotFound(_)) => {
                        http::Response::builder()
                            .status(404)
                            .body(s3s::Body::from("not found".to_string()))
                            .unwrap()
                    }
                    Err(e) => {
                        http::Response::builder()
                            .status(500)
                            .header("content-type", "text/plain")
                            .body(s3s::Body::from(format!("getraw failed: {e}")))
                            .unwrap()
                    }
                }
            }

            "objects" => {
                let layout = store.layout_map();
                let mut extents: Vec<&_> = layout.extents.iter()
                    .filter(|e| !is_internal_key(&e.key))
                    .collect();

                let prefix = qparam(query, "prefix").unwrap_or_default();
                if !prefix.is_empty() {
                    extents.retain(|e| e.key.contains(&prefix));
                }

                let since_txn: u64 = qparam(query, "since_txn")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                if since_txn > 0 {
                    extents.retain(|e| e.created_txn >= since_txn);
                }

                let total = extents.len();
                extents.sort_by(|a, b| b.created_txn.cmp(&a.created_txn));

                let offset: usize = qparam(query, "offset").and_then(|v| v.parse().ok()).unwrap_or(0);
                let limit: usize = qparam(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(100).min(1000);
                let page_items: Vec<serde_json::Value> = extents.iter()
                    .skip(offset)
                    .take(limit)
                    .map(|e| serde_json::json!({
                        "key": e.key,
                        "size": e.size,
                        "padded_size": e.padded_size,
                        "offset": e.offset,
                        "created_txn": e.created_txn,
                        "last_modified": e.last_modified.to_rfc3339(),
                        "uncompressed_size": e.uncompressed_size,
                    }))
                    .collect();

                let json = serde_json::json!({
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "txn_id": layout.txn_id,
                    "objects": page_items,
                });
                Self::json_response(json.to_string(), self.cors_origin.as_deref())
            }

            _ => Self::not_found(),
        }
    }
}

impl hyper::service::Service<http::Request<hyper::body::Incoming>> for VizService {
    type Response = http::Response<s3s::Body>;
    type Error = s3s::HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: http::Request<hyper::body::Incoming>) -> Self::Future {
        let path = req.uri().path().to_owned();
        if path.starts_with("/_admin/") || path == "/_admin" {
            // OPTIONS preflight for /_admin/* endpoints
            if req.method() == http::Method::OPTIONS {
                let cors = self.cors_origin.clone();
                return Box::pin(async move { Ok(Self::preflight(cors.as_deref())) });
            }

            // Admin token authentication for /_admin/* endpoints
            if let Some(expected) = &self.admin_token {
                let authorized = req
                    .headers()
                    .get(http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
                    .map(|token| {
                        // Constant-time comparison to avoid timing side-channels
                        let a = token.as_bytes();
                        let b = expected.as_bytes();
                        if a.len() != b.len() { return false; }
                        let mut acc = 0u8;
                        for (x, y) in a.iter().zip(b.iter()) {
                            acc |= x ^ y;
                        }
                        acc == 0
                    })
                    .unwrap_or(false);
                // Fall back to ?token= query param (needed for EventSource / SSE)
                let authorized = authorized || req.uri().query()
                    .and_then(|q| q.split('&').find(|p| p.starts_with("token=")))
                    .map(|p| {
                        let token = p.trim_start_matches("token=");
                        let a = token.as_bytes();
                        let b = expected.as_bytes();
                        if a.len() != b.len() { return false; }
                        let mut acc = 0u8;
                        for (x, y) in a.iter().zip(b.iter()) {
                            acc |= x ^ y;
                        }
                        acc == 0
                    })
                    .unwrap_or(false);
                if !authorized {
                    return Box::pin(async { Ok(Self::forbidden()) });
                }
            }

            let full_path = if path == "/_admin" { "/_admin/".to_owned() } else { path };

            // POST /_admin/flush -- force an index checkpoint
            if req.method() == http::Method::POST && full_path == "/_admin/flush" {
                let stores = self.raw_stores.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                return Box::pin(async move {
                    let start = std::time::Instant::now();
                    let resp = if stores.is_empty() {
                        http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(r#"{"error":"requires BACKEND=raw"}"#.to_string()))
                            .expect("static response")
                    } else {
                        let result = tokio::task::spawn_blocking(move || {
                            let mut errors = Vec::new();
                            for s in &stores {
                                if let Err(e) = s.flush_index() {
                                    errors.push(format!("{e}"));
                                }
                            }
                            errors
                        }).await;
                        match result {
                            Ok(ref errors) if errors.is_empty() => {
                                Self::json_response(r#"{"ok":true}"#.to_string(), cors.as_deref())
                            }
                            Ok(errors) => {
                                tracing::error!("/_admin/flush failed: {:?}", errors);
                                http::Response::builder()
                                    .status(500)
                                    .header("content-type", "application/json")
                                    .body(s3s::Body::from(r#"{"ok":false,"error":"flush failed"}"#.to_string()))
                                    .expect("static response")
                            }
                            Err(_) => {
                                tracing::error!("/_admin/flush task panicked");
                                http::Response::builder()
                                    .status(500)
                                    .header("content-type", "application/json")
                                    .body(s3s::Body::from(r#"{"ok":false,"error":"internal error"}"#.to_string()))
                                    .expect("static response")
                            }
                        }
                    };
                    let elapsed_ms = start.elapsed().as_millis();
                    let status = resp.status().as_u16();
                    let level = if status >= 500 { "error" } else { "info" };
                    log_buf.push(LogEntry::new(level, "admin", "admin",
                        format!("POST /_admin/flush {} {}ms", status, elapsed_ms)));
                    Ok(resp)
                });
            }

            // POST /_admin/rebuild-index -- reload binary index from device, rebuild bucket registry
            if req.method() == http::Method::POST && full_path == "/_admin/rebuild-index" {
                let stores = self.raw_stores.clone();
                let reg    = Arc::clone(&self.bucket_registry);
                let bucket = self.bucket.clone();
                let cors   = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                return Box::pin(async move {
                    let start = std::time::Instant::now();
                    if stores.is_empty() {
                        return Ok(Self::unavailable());
                    }
                    let stores2 = stores.clone();
                    let reload_msg = match tokio::task::spawn_blocking(move || {
                        let mut msgs = Vec::new();
                        for (i, s) in stores2.iter().enumerate() {
                            match s.reload_index() {
                                Ok(changed) => msgs.push(format!("shard {i}: changed={changed}")),
                                Err(e) => msgs.push(format!("shard {i}: error: {e}")),
                            }
                        }
                        msgs.join("; ")
                    }).await {
                        Ok(msg) => msg,
                        Err(_) => {
                            tracing::error!("/_admin/rebuild-index task panicked");
                            "internal error".to_string()
                        }
                    };
                    let buckets = scan_bucket_names(stores[0].as_ref(), &bucket).await;
                    let count = buckets.len();
                    *reg.write().await = buckets;
                    let elapsed_ms = start.elapsed().as_millis();
                    log_buf.push(LogEntry::new("info", "admin", "admin",
                        format!("POST /_admin/rebuild-index 200 {}ms ({reload_msg})", elapsed_ms)));
                    let body = serde_json::json!({
                        "ok": true, "buckets": count, "detail": reload_msg,
                    }).to_string();
                    Ok(Self::json_response(body, cors.as_deref()))
                });
            }

            // POST /_admin/clear-bucket-cache -- delete __buckets__/ objects, rebuild registry
            if req.method() == http::Method::POST && full_path == "/_admin/clear-bucket-cache" {
                let Some(store_arc) = self.raw_stores.first().cloned() else {
                    return Box::pin(async { Ok(Self::unavailable()) });
                };
                let reg    = Arc::clone(&self.bucket_registry);
                let bucket = self.bucket.clone();
                let cors   = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                return Box::pin(async move {
                    let start = std::time::Instant::now();
                    let prefix = object_store::path::Path::from("__buckets__");
                    let mut listing = store_arc.list(Some(&prefix));
                    let mut deleted = 0u32;
                    let mut errors  = 0u32;
                    while let Some(item) = match listing.try_next().await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("clear-bucket-cache listing error: {e}");
                            None
                        }
                    } {
                        match store_arc.delete(&item.location).await {
                            Ok(())  => deleted += 1,
                            Err(_)  => errors  += 1,
                        }
                    }
                    drop(listing);
                    let buckets = scan_bucket_names(store_arc.as_ref(), &bucket).await;
                    let count = buckets.len();
                    *reg.write().await = buckets;
                    let elapsed_ms = start.elapsed().as_millis();
                    log_buf.push(LogEntry::new("info", "admin", "admin",
                        format!("POST /_admin/clear-bucket-cache 200 {}ms (deleted={deleted}, errors={errors})", elapsed_ms)));
                    let body = serde_json::json!({
                        "ok": true, "deleted": deleted, "errors": errors, "buckets": count,
                    }).to_string();
                    Ok(Self::json_response(body, cors.as_deref()))
                });
            }

            // POST /_admin/reload -- trigger config reload (same as SIGHUP)
            if req.method() == http::Method::POST && full_path == "/_admin/reload" {
                let cors = self.cors_origin.clone();
                let signal = Arc::clone(&self.reload_signal);
                let log_buf = self.log_buffer.clone();
                return Box::pin(async move {
                    signal.notify_one();
                    log_buf.push(LogEntry::new("info", "admin", "admin",
                        "POST /_admin/reload 200 (config reload initiated)".to_string()));
                    let body = r#"{"ok":true,"message":"reload initiated"}"#;
                    Ok(Self::json_response(body.to_string(), cors.as_deref()))
                });
            }

            // GET /_admin/op-log/stream -- SSE stream of admin operation progress
            if req.method() == http::Method::GET && full_path == "/_admin/op-log/stream" {
                let mut rx = self.op_log_tx.subscribe();
                let cors = self.cors_origin.clone();
                return Box::pin(async move {
                    let stream = async_stream::stream! {
                        // Send an SSE comment immediately so the HTTP response
                        // headers are flushed and EventSource.onopen fires.
                        yield Ok::<_, std::convert::Infallible>(
                            hyper::body::Frame::data(bytes::Bytes::from(": connected\n\n")),
                        );
                        loop {
                            match rx.recv().await {
                                Ok(msg) => {
                                    if msg == "__done__" {
                                        yield Ok::<_, std::convert::Infallible>(
                                            hyper::body::Frame::data(bytes::Bytes::from("event: done\ndata: {}\n\n")),
                                        );
                                        break;
                                    }
                                    let escaped = msg.replace('\\', "\\\\").replace('"', "\\\"");
                                    let payload = format!(
                                        "data: {{\"message\":\"{escaped}\"}}\n\n"
                                    );
                                    yield Ok::<_, std::convert::Infallible>(
                                        hyper::body::Frame::data(bytes::Bytes::from(payload)),
                                    );
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                    let payload = format!(
                                        "data: {{\"message\":\"[skipped {n} messages]\"}}\n\n"
                                    );
                                    yield Ok::<_, std::convert::Infallible>(
                                        hyper::body::Frame::data(bytes::Bytes::from(payload)),
                                    );
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    };
                    let body = s3s::Body::http_body_unsync(
                        http_body_util::StreamBody::new(stream),
                    );
                    let mut builder = http::Response::builder()
                        .status(200)
                        .header("content-type", "text/event-stream")
                        .header("cache-control", "no-cache")
                        .header("connection", "keep-alive");
                    if let Some(ref origin) = cors {
                        builder = builder.header("access-control-allow-origin", origin.as_str());
                    }
                    Ok(builder.body(body).expect("static response"))
                });
            }

            // POST /_admin/cross-verify -- run cross-verify across all shards
            if req.method() == http::Method::POST && full_path == "/_admin/cross-verify" {
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let svc = self.clone();
                let op_tx = self.op_log_tx.clone();
                return Box::pin(async move {
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    let _admin_guard = match svc.try_acquire_admin_op(AdminOperation::CrossVerify).await {
                        Ok(g) => g,
                        Err(resp) => return Ok(resp),
                    };
                    // Spawn the work so it cannot be cancelled by HTTP disconnection.
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    tokio::spawn(async move {
                        let _guard = _admin_guard; // move guard into task; dropped on exit or panic
                        let start = std::time::Instant::now();
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            "POST /_admin/cross-verify started".to_string()));
                        let _ = op_tx.send("cross-verify started".to_string());
                        let _ = cluster.rebuild_catalog().await;
                        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<String>();
                        let op_tx2 = op_tx.clone();
                        let fwd = tokio::spawn(async move {
                            while let Some(msg) = prx.recv().await {
                                let _ = op_tx2.send(msg);
                            }
                        });
                        let report = cluster.cross_verify_all(None, Some(&ptx)).await;
                        drop(ptx);
                        let _ = fwd.await;
                        drop(_guard); // release admin op lock
                        let elapsed_ms = start.elapsed().as_millis();
                        let summary = format!("cross-verify completed {}ms: checked={}, ok={}, mismatched={}, errors={}, skipped_single={}",
                            elapsed_ms, report.objects_checked, report.objects_ok,
                            report.objects_mismatched, report.objects_with_errors,
                            report.objects_skipped_single_replica);
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            format!("POST /_admin/{summary}")));
                        let _ = op_tx.send(summary);
                        let _ = op_tx.send("__done__".to_string());
                        let body = serde_json::json!({
                            "ok": true,
                            "objects_checked": report.objects_checked,
                            "objects_ok": report.objects_ok,
                            "objects_mismatched": report.objects_mismatched,
                            "objects_with_errors": report.objects_with_errors,
                            "objects_skipped_single_replica": report.objects_skipped_single_replica,
                        }).to_string();
                        let _ = tx.send(body);
                    });
                    match rx.await {
                        Ok(body) => Ok(Self::json_response(body, cors.as_deref())),
                        Err(_) => Ok(http::Response::builder()
                            .status(500)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(r#"{"error":"task failed"}"#.to_string()))
                            .expect("static response")),
                    }
                });
            }

            // POST /_admin/repair-replication -- run one repair-replication sweep (under+over)
            if req.method() == http::Method::POST && full_path == "/_admin/repair-replication" {
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let svc = self.clone();
                let op_tx = self.op_log_tx.clone();
                return Box::pin(async move {
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    let _admin_guard = match svc.try_acquire_admin_op(AdminOperation::RepairReplication).await {
                        Ok(g) => g,
                        Err(resp) => return Ok(resp),
                    };
                    // Spawn the work so it cannot be cancelled by HTTP disconnection.
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    tokio::spawn(async move {
                        let _guard = _admin_guard; // move guard into task; dropped on exit or panic
                        let start = std::time::Instant::now();
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            "POST /_admin/repair-replication started".to_string()));
                        let _ = op_tx.send("repair-replication started".to_string());
                        let _ = cluster.rebuild_catalog().await;
                        let raw_refs = cluster.raw_refs();
                        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<String>();
                        let op_tx2 = op_tx.clone();
                        let fwd = tokio::spawn(async move {
                            while let Some(msg) = prx.recv().await {
                                let _ = op_tx2.send(msg);
                            }
                        });
                        let result = shardedobjstr::repair::repair_replication_sweep(
                            &cluster, 500, raw_refs.as_deref(), Some(&ptx),
                        ).await;
                        drop(ptx);
                        let _ = fwd.await;
                        drop(_guard); // release admin op lock
                        let elapsed_ms = start.elapsed().as_millis();
                        let summary = format!("repair-replication completed {}ms: re_replicated={}, trimmed={}, under_remaining={}, over_remaining={}",
                            elapsed_ms, result.re_replicated, result.trimmed,
                            result.under_remaining, result.over_remaining);
                        log_buf.push(LogEntry::new("info", "replication", "admin",
                            format!("POST /_admin/{summary}")));
                        let _ = op_tx.send(summary);
                        let _ = op_tx.send("__done__".to_string());
                        let body = serde_json::json!({
                            "ok": true,
                            "re_replicated": result.re_replicated,
                            "trimmed": result.trimmed,
                            "under_remaining": result.under_remaining,
                            "over_remaining": result.over_remaining,
                        }).to_string();
                        let _ = tx.send(body);
                    });
                    match rx.await {
                        Ok(body) => Ok(Self::json_response(body, cors.as_deref())),
                        Err(_) => Ok(http::Response::builder()
                            .status(500)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(r#"{"error":"task failed"}"#.to_string()))
                            .expect("static response")),
                    }
                });
            }

            // POST /_admin/redistribute -- balance object counts across shards
            if req.method() == http::Method::POST && full_path == "/_admin/redistribute" {
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let read_only = self.read_only;
                let svc = self.clone();
                let op_tx = self.op_log_tx.clone();
                return Box::pin(async move {
                    if read_only {
                        return Ok(http::Response::builder()
                            .status(403)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"server is read-only"}"#.to_string(),
                            ))
                            .expect("static response"));
                    }
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    let _admin_guard = match svc.try_acquire_admin_op(AdminOperation::Redistribute).await {
                        Ok(g) => g,
                        Err(resp) => return Ok(resp),
                    };
                    // Spawn the work so it cannot be cancelled by HTTP disconnection.
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    tokio::spawn(async move {
                        let _guard = _admin_guard; // move guard into task; dropped on exit or panic
                        let start = std::time::Instant::now();
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            "POST /_admin/redistribute started".to_string()));
                        let _ = op_tx.send("redistribute started".to_string());
                        let _ = cluster.rebuild_catalog().await;
                        let under = cluster.find_under_replicated();
                        if !under.is_empty() {
                            drop(_guard); // release admin op lock
                            let body = serde_json::json!({
                                "error": format!("{} under-replicated objects -- run repair-replication first", under.len()),
                                "under_replicated": under.len(),
                            }).to_string();
                            let _ = tx.send(Err(body));
                            return;
                        }
                        let raw_refs = cluster.raw_refs();
                        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<String>();
                        let op_tx2 = op_tx.clone();
                        let fwd = tokio::spawn(async move {
                            while let Some(msg) = prx.recv().await {
                                let _ = op_tx2.send(msg);
                            }
                        });
                        let result = shardedobjstr::repair::redistribute_sweep(
                            &cluster, 500, 0.10, raw_refs.as_deref(), Some(&ptx),
                        ).await;
                        drop(ptx);
                        let _ = fwd.await;
                        drop(_guard); // release admin op lock
                        let elapsed_ms = start.elapsed().as_millis();
                        let summary = format!("redistribute completed {}ms: moved={}, skipped={}, errors={}",
                            elapsed_ms, result.moved, result.skipped, result.errors);
                        log_buf.push(LogEntry::new("info", "replication", "admin",
                            format!("POST /_admin/{summary}")));
                        let _ = op_tx.send(summary);
                        let _ = op_tx.send("__done__".to_string());
                        let shard_counts: Vec<serde_json::Value> = result.shard_counts.iter()
                            .map(|(sid, cnt)| serde_json::json!({"shard_id": sid, "count": cnt}))
                            .collect();
                        let body = serde_json::json!({
                            "ok": true,
                            "moved": result.moved,
                            "skipped": result.skipped,
                            "errors": result.errors,
                            "shard_counts": shard_counts,
                        }).to_string();
                        let _ = tx.send(Ok(body));
                    });
                    match rx.await {
                        Ok(Ok(body)) => Ok(Self::json_response(body, cors.as_deref())),
                        Ok(Err(body)) => Ok(http::Response::builder()
                            .status(409)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(body))
                            .expect("static response")),
                        Err(_) => Ok(http::Response::builder()
                            .status(500)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(r#"{"error":"task failed"}"#.to_string()))
                            .expect("static response")),
                    }
                });
            }

            // POST /_admin/vacuum -- purge stale delete markers
            if req.method() == http::Method::POST && full_path == "/_admin/vacuum" {
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let op_tx = self.op_log_tx.clone();
                return Box::pin(async move {
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    let start = std::time::Instant::now();
                    log_buf.push(LogEntry::new("info", "admin", "admin",
                        "POST /_admin/vacuum started".to_string()));
                    let _ = op_tx.send("vacuum started".to_string());
                    let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<String>();
                    let op_tx2 = op_tx.clone();
                    let fwd = tokio::spawn(async move {
                        while let Some(msg) = prx.recv().await {
                            let _ = op_tx2.send(msg);
                        }
                    });
                    match cluster.vacuum_delete_markers(Some(&ptx)).await {
                        Ok((purged, cleaned)) => {
                            drop(ptx);
                            let _ = fwd.await;
                            let elapsed_ms = start.elapsed().as_millis();
                            let summary = format!("vacuum completed {elapsed_ms}ms: purged={purged}, cleaned={cleaned}");
                            log_buf.push(LogEntry::new("info", "admin", "admin",
                                format!("POST /_admin/{summary}")));
                            let _ = op_tx.send(summary);
                            let _ = op_tx.send("__done__".to_string());
                            let body = serde_json::json!({
                                "ok": true,
                                "purged": purged,
                                "cleaned": cleaned,
                            }).to_string();
                            Ok(Self::json_response(body, cors.as_deref()))
                        }
                        Err(e) => {
                            drop(ptx);
                            let _ = fwd.await;
                            let elapsed_ms = start.elapsed().as_millis();
                            let summary = format!("vacuum failed {elapsed_ms}ms: {e}");
                            log_buf.push(LogEntry::new("error", "admin", "admin",
                                format!("POST /_admin/{summary}")));
                            let _ = op_tx.send(summary);
                            let _ = op_tx.send("__done__".to_string());
                            let body = serde_json::json!({
                                "ok": false,
                                "error": format!("{e}"),
                            }).to_string();
                            Ok(http::Response::builder()
                                .status(409)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(body))
                                .expect("static response"))
                        }
                    }
                });
            }

            // POST /_admin/take-offline/<shard_id>[?suppress_replication=true]
            // Manually detach a shard so the recovery loop will NOT auto-reattach it.
            if req.method() == http::Method::POST
                && full_path.starts_with("/_admin/take-offline/")
            {
                let shard_id_str = full_path.trim_start_matches("/_admin/take-offline/");
                let shard_id: usize = match shard_id_str.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        return Box::pin(async move {
                            Ok(http::Response::builder()
                                .status(400)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(
                                    r#"{"error":"invalid shard_id"}"#.to_string(),
                                ))
                                .expect("static response"))
                        });
                    }
                };
                let suppress = req.uri().query()
                    .and_then(|q| q.split('&').find(|p| p.starts_with("suppress_replication=")))
                    .map(|p| p.trim_start_matches("suppress_replication=") == "true")
                    .unwrap_or(false);
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let raw_stores = self.raw_stores.clone();
                let raw_index_map = self.raw_index_map.clone();
                return Box::pin(async move {
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    // Flush raw index before detaching so on-disk state is
                    // consistent for manual repair.
                    if let Some(Some(raw_idx)) = raw_index_map.get(shard_id) {
                        if let Some(raw) = raw_stores.get(*raw_idx) {
                            if let Err(e) = raw.flush_index() {
                                log_buf.push(LogEntry::new("warn", "admin", "admin",
                                    format!("take-offline shard {shard_id}: failed to flush raw index: {e}")));
                            }
                        }
                    }
                    let prev = cluster.hold_offline(
                        shard_id,
                        suppress,
                        shardedobjstr::DetachReason::Manual,
                    );
                    let prev_str = match prev {
                        Some(h) => format!("{h:?}"),
                        None => {
                            return Ok(http::Response::builder()
                                .status(404)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(
                                    r#"{"error":"shard not found"}"#.to_string(),
                                ))
                                .expect("static response"));
                        }
                    };
                    log_buf.push(LogEntry::new("info", "admin", "admin",
                        format!("POST /_admin/take-offline/{shard_id}: {prev_str} -> Detached (suppress_replication={suppress})")));
                    let body = serde_json::json!({
                        "ok": true,
                        "shard_id": shard_id,
                        "previous_health": prev_str,
                        "suppress_replication": suppress,
                    }).to_string();
                    Ok(Self::json_response(body, cors.as_deref()))
                });
            }

            // POST /_admin/attach/<shard_id> -- reattach a Detached shard
            // Reopens the store, rescans the catalog, marks Healthy.
            if req.method() == http::Method::POST
                && full_path.starts_with("/_admin/attach/")
            {
                let shard_id_str = full_path.trim_start_matches("/_admin/attach/");
                let shard_id: usize = match shard_id_str.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        return Box::pin(async move {
                            Ok(http::Response::builder()
                                .status(400)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(
                                    r#"{"error":"invalid shard_id"}"#.to_string(),
                                ))
                                .expect("static response"))
                        });
                    }
                };
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let original_stores = self.original_stores.clone();
                let raw_stores = self.raw_stores.clone();
                let raw_index_map = self.raw_index_map.clone();
                let admin_op_lock = self.admin_op_lock.clone();
                return Box::pin(async move {
                    // Reject attach if a bulk admin op (drain, redistribute) is running.
                    {
                        let guard = admin_op_lock.lock().await;
                        if let Some(current) = *guard {
                            let body = serde_json::json!({
                                "error": format!("cannot attach while {current} is running"),
                                "current_operation": current.to_string(),
                            }).to_string();
                            return Ok(http::Response::builder()
                                .status(409)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(body))
                                .expect("static response"));
                        }
                    }
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    // Only allow attaching Detached or Offline shards.
                    let health = cluster.shard_health(shard_id);
                    match health {
                        Some(ShardHealth::Detached) | Some(ShardHealth::Offline) => {}
                        Some(h) => {
                            let body = format!(
                                r#"{{"error":"shard is {h:?}, must be Detached or Offline"}}"#
                            );
                            return Ok(http::Response::builder()
                                .status(409)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(body))
                                .expect("static response"));
                        }
                        None => {
                            return Ok(http::Response::builder()
                                .status(404)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(
                                    r#"{"error":"shard not found"}"#.to_string(),
                                ))
                                .expect("static response"));
                        }
                    }
                    // For raw shards, reload the index from disk (catches
                    // any changes made during offline maintenance) without
                    // reopening the file -- the original store still holds
                    // the device lock.
                    let _store: Option<Arc<dyn ObjectStore>> = if let Some(Some(raw_idx)) = raw_index_map.get(shard_id) {
                        if let Some(raw) = raw_stores.get(*raw_idx) {
                            if let Err(e) = raw.reload_index() {
                                let body = format!(
                                    r#"{{"error":"failed to reload raw index: {e}"}}"#
                                );
                                return Ok(http::Response::builder()
                                    .status(500)
                                    .header("content-type", "application/json")
                                    .body(s3s::Body::from(body))
                                    .expect("static response"));
                            }
                            log_buf.push(LogEntry::new("info", "admin", "admin",
                                format!("shard {shard_id}: reloaded raw index from disk")));
                            Some(raw.clone() as Arc<dyn ObjectStore>)
                        } else {
                            original_stores.get(shard_id).cloned().flatten()
                        }
                    } else {
                        // Non-raw shard: use the original store.
                        original_stores.get(shard_id).cloned().flatten()
                    };
                    if _store.is_none() {
                        return Ok(http::Response::builder()
                            .status(500)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"no original store for shard"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    let start = std::time::Instant::now();
                    let raw_refs = cluster.raw_refs();
                    // If the shard was drained, skip the sync -- just
                    // attach directly.  Syncing would copy all objects
                    // back from healthy shards, undoing the drain.
                    let was_drained = cluster.shard_detach_reason(shard_id)
                        == Some(shardedobjstr::DetachReason::Drain);
                    if was_drained {
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            format!("POST /_admin/attach/{shard_id} started (drained -- skip sync)")));
                        let original = original_stores.get(shard_id)
                            .and_then(|o| o.as_ref())
                            .map(Arc::clone);
                        if let Some(store) = original {
                            let _ = cluster.attach_shard(shard_id, store, true).await;
                        } else {
                            log_buf.push(LogEntry::new("error", "admin", "admin",
                                format!("POST /_admin/attach/{shard_id} failed: no original store")));
                            cluster.set_shard_health(shard_id, shardedobjstr::ShardHealth::Offline);
                        }
                    } else {
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            format!("POST /_admin/attach/{shard_id} started (sync + reattach)")));
                        sync_and_reattach(
                            &cluster,
                            &original_stores,
                            shard_id,
                            raw_refs.as_deref(),
                        ).await;
                    }
                    let elapsed_ms = start.elapsed().as_millis();
                    let new_health = cluster.shard_health(shard_id);
                    match new_health {
                        Some(ShardHealth::Healthy) => {
                            let count = cluster.catalog().entries_for_shard(shard_id).len();
                            log_buf.push(LogEntry::new("info", "health", "admin",
                                format!("shard {shard_id}: attached, {count} objects found ({elapsed_ms}ms)")));
                            let body = serde_json::json!({
                                "ok": true,
                                "shard_id": shard_id,
                                "objects_found": count,
                            }).to_string();
                            Ok(Self::json_response(body, cors.as_deref()))
                        }
                        _ => {
                            log_buf.push(LogEntry::new("error", "admin", "admin",
                                format!("POST /_admin/attach/{shard_id} failed ({elapsed_ms}ms): shard is {new_health:?}")));
                            let body = format!(r#"{{"error":"attach failed: shard is {new_health:?}"}}"#);
                            Ok(http::Response::builder()
                                .status(500)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(body))
                                .expect("static response"))
                        }
                    }
                });
            }

            // POST /_admin/drain/<shard_id> -- drain all objects off a shard
            if req.method() == http::Method::POST
                && full_path.starts_with("/_admin/drain/")
            {
                let shard_id_str = full_path.trim_start_matches("/_admin/drain/");
                let shard_id: usize = match shard_id_str.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        return Box::pin(async move {
                            Ok(http::Response::builder()
                                .status(400)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(
                                    r#"{"error":"invalid shard_id"}"#.to_string(),
                                ))
                                .expect("static response"))
                        });
                    }
                };
                let cluster = self.cluster.clone();
                let cors = self.cors_origin.clone();
                let log_buf = self.log_buffer.clone();
                let svc = self.clone();
                let op_tx = self.op_log_tx.clone();
                return Box::pin(async move {
                    let Some(cluster) = cluster else {
                        return Ok(http::Response::builder()
                            .status(503)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(
                                r#"{"error":"requires cluster mode"}"#.to_string(),
                            ))
                            .expect("static response"));
                    };
                    let _admin_guard = match svc.try_acquire_admin_op(AdminOperation::Drain).await {
                        Ok(g) => g,
                        Err(resp) => return Ok(resp),
                    };
                    // Grab the victim store BEFORE detaching so we can
                    // still read data from it during the drain.
                    let victim_store = match cluster.shard_store(shard_id) {
                        Some(s) => s,
                        None => {
                            drop(_admin_guard); // release admin op lock
                            return Ok(http::Response::builder()
                                .status(404)
                                .header("content-type", "application/json")
                                .body(s3s::Body::from(
                                    r#"{"error":"shard not found"}"#.to_string(),
                                ))
                                .expect("static response"));
                        }
                    };
                    // Spawn the work so it cannot be cancelled by HTTP disconnection.
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    tokio::spawn(async move {
                        let _guard = _admin_guard; // move guard into task; dropped on exit or panic
                        let start = std::time::Instant::now();
                        log_buf.push(LogEntry::new("info", "admin", "admin",
                            format!("POST /_admin/drain/{shard_id} started")));
                        let _ = op_tx.send(format!("drain shard {shard_id} started"));
                        // Mark the shard Detached so new writes skip it AND
                        // the recovery loop does not auto-reattach it.
                        cluster.hold_offline(shard_id, false, shardedobjstr::DetachReason::Drain);
                        log_buf.push(LogEntry::new("info", "health", "admin",
                            format!("shard {shard_id}: detached for drain (Detached)")));
                        // Drain reads from the saved victim_store and writes
                        // to the cluster (which now routes to survivors).
                        let raw_refs = cluster.raw_refs();
                        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<String>();
                        let op_tx2 = op_tx.clone();
                        let fwd = tokio::spawn(async move {
                            while let Some(msg) = prx.recv().await {
                                let _ = op_tx2.send(msg);
                            }
                        });
                        let report = shardedobjstr::repair::drain_shard(
                            &cluster, &cluster, shard_id, &victim_store,
                            raw_refs.as_deref(), Some(&ptx),
                        ).await;
                        drop(ptx);
                        let _ = fwd.await;
                        // For raw shards, flush the index so deletions persist
                        // to disk. Without this, reload_index on re-attach
                        // would resurrect the deleted files.
                        if let Some(Some(raw_idx)) = svc.raw_index_map.get(shard_id) {
                            if let Some(raw) = svc.raw_stores.get(*raw_idx) {
                                if let Err(e) = raw.flush_index() {
                                    log_buf.push(LogEntry::new("warn", "admin", "admin",
                                        format!("drain shard {shard_id}: failed to flush raw index: {e}")));
                                }
                            }
                        }
                        drop(_guard); // release admin op lock
                        let elapsed_ms = start.elapsed().as_millis();
                        let summary = format!("drain shard {shard_id} completed {}ms: moved={}, skipped={}, deleted={}, errors={}, delete_errors={}, re_replicated={}, under_remaining={}",
                            elapsed_ms, report.moved, report.skipped, report.deleted, report.errors, report.delete_errors,
                            report.re_replicated, report.under_remaining);
                        log_buf.push(LogEntry::new("info", "replication", "admin",
                            format!("POST /_admin/{summary}")));
                        let _ = op_tx.send(summary);
                        let _ = op_tx.send("__done__".to_string());
                        let body = serde_json::json!({
                            "ok": true,
                            "shard_id": shard_id,
                            "moved": report.moved,
                            "skipped": report.skipped,
                            "errors": report.errors,
                            "deleted": report.deleted,
                            "delete_errors": report.delete_errors,
                            "re_replicated": report.re_replicated,
                            "under_remaining": report.under_remaining,
                        }).to_string();
                        let _ = tx.send(body);
                    });
                    match rx.await {
                        Ok(body) => Ok(Self::json_response(body, cors.as_deref())),
                        Err(_) => Ok(http::Response::builder()
                            .status(500)
                            .header("content-type", "application/json")
                            .body(s3s::Body::from(r#"{"error":"task failed"}"#.to_string()))
                            .expect("static response")),
                    }
                });
            }

            let query_str = req.uri().query().map(|q| q.to_owned());
            let admin_path = full_path.clone();
            let svc = self.clone();
            Box::pin(async move {
                let start = std::time::Instant::now();
                let resp = svc.handle_admin(&admin_path, query_str.as_deref()).await;
                let elapsed_ms = start.elapsed().as_millis();
                let status = resp.status().as_u16();
                // Don't log the log endpoint itself to avoid feedback loops
                if !admin_path.starts_with("/_admin/logs") {
                    let msg = format!("GET {} {} {}ms", admin_path, status, elapsed_ms);
                    svc.log_buffer.push(LogEntry::new("info", "admin", "admin", msg));
                }
                Ok(resp)
            })
        } else {
            // Convert Incoming body to s3s::Body and forward to the S3 service.
            // Clone is cheap (S3Service uses Arc internally).
            let s3 = self.s3.clone();
            let method = req.method().to_string();
            let req_path = req.uri().path_and_query()
                .map(|pq| pq.to_string())
                .unwrap_or_else(|| req.uri().path().to_string());
            let log_buf = self.log_buffer.clone();
            let (parts, body) = req.into_parts();
            let s3_body = s3s::Body::from(body);
            let s3_req = http::Request::from_parts(parts, s3_body);
            let start = std::time::Instant::now();
            Box::pin(async move {
                let resp = s3.call(s3_req).await;
                let elapsed_ms = start.elapsed().as_millis();
                let status = match &resp {
                    Ok(r) => r.status().as_u16(),
                    Err(_) => 500,
                };
                let msg = format!("{} {} {} {}ms", method, req_path, status, elapsed_ms);
                let level = if status >= 500 { "error" } else if status >= 400 { "warn" } else { "info" };
                log_buf.push(LogEntry::new(level, "requests", "s3", msg));
                resp
            })
        }
    }
}

// -- Extracted /proc and stat parsers (testable without filesystem) --------

/// Parse the output of `stat -f -c "%S %b %a"` into (total_bytes, free_bytes).
fn parse_stat_output(s: &str) -> (Option<u64>, Option<u64>) {
    let parts: Vec<&str> = s.trim().split_whitespace().collect();
    if parts.len() == 3 {
        let bsize: u64 = parts[0].parse().unwrap_or(0);
        let blocks: u64 = parts[1].parse().unwrap_or(0);
        let avail: u64 = parts[2].parse().unwrap_or(0);
        (Some(blocks * bsize), Some(avail * bsize))
    } else {
        (None, None)
    }
}

/// Find the filesystem type for `real_path` from the contents of /proc/mounts.
fn parse_mounts_fstype(mounts: &str, real_path: &str) -> Option<String> {
    let mut best: Option<(&str, &str)> = None;
    for line in mounts.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3 {
            let mount_point = parts[1];
            if real_path.starts_with(mount_point) {
                if best.is_none() || mount_point.len() > best.unwrap().0.len() {
                    best = Some((mount_point, parts[2]));
                }
            }
        }
    }
    best.map(|(_, t)| t.to_string())
}

/// Parse /proc/uptime content into whole seconds of system uptime.
fn parse_proc_uptime(s: &str) -> u64 {
    s.trim()
        .split_whitespace()
        .next()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v as u64)
        .unwrap_or(0)
}

/// Parse /proc/loadavg content into (load1, load5, load15).
fn parse_proc_loadavg(s: &str) -> (f64, f64, f64) {
    let mut p = s.split_whitespace();
    let l1: f64 = p.next().and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let l5: f64 = p.next().and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let l15: f64 = p.next().and_then(|v| v.parse().ok()).unwrap_or(0.0);
    (l1, l5, l15)
}

/// Parse /proc/meminfo content into (total_kb, free_kb, available_kb).
fn parse_proc_meminfo(s: &str) -> (u64, u64, u64) {
    let mut total = 0u64;
    let mut free = 0u64;
    let mut avail = 0u64;
    for line in s.lines() {
        if line.starts_with("MemTotal:") {
            total = line.split_whitespace().nth(1)
                .and_then(|v| v.parse().ok()).unwrap_or(0);
        } else if line.starts_with("MemFree:") {
            free = line.split_whitespace().nth(1)
                .and_then(|v| v.parse().ok()).unwrap_or(0);
        } else if line.starts_with("MemAvailable:") {
            avail = line.split_whitespace().nth(1)
                .and_then(|v| v.parse().ok()).unwrap_or(0);
        }
    }
    (total, free, avail)
}

/// Parse VmRSS from /proc/self/status content, in kB.
fn parse_proc_status_rss(s: &str) -> u64 {
    for line in s.lines() {
        if line.starts_with("VmRSS:") {
            return line.split_whitespace().nth(1)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- parse_stat_output --------------------------------------------

    #[test]
    fn stat_output_normal() {
        let (total, free) = parse_stat_output("4096 1000000 500000\n");
        assert_eq!(total, Some(4096 * 1_000_000));
        assert_eq!(free, Some(4096 * 500_000));
    }

    #[test]
    fn stat_output_empty() {
        let (total, free) = parse_stat_output("");
        assert!(total.is_none());
        assert!(free.is_none());
    }

    #[test]
    fn stat_output_too_few_fields() {
        let (total, free) = parse_stat_output("4096 1000000");
        assert!(total.is_none());
        assert!(free.is_none());
    }

    #[test]
    fn stat_output_non_numeric() {
        // unwrap_or(0) means non-numeric fields produce 0, not None
        let (total, free) = parse_stat_output("abc def ghi");
        assert_eq!(total, Some(0));
        assert_eq!(free, Some(0));
    }

    #[test]
    fn stat_output_extra_whitespace() {
        let (total, free) = parse_stat_output("  4096   100   50  \n");
        assert_eq!(total, Some(4096 * 100));
        assert_eq!(free, Some(4096 * 50));
    }

    // -- parse_mounts_fstype -----------------------------------------

    #[test]
    fn mounts_finds_longest_match() {
        let mounts = "\
/dev/sda1 / ext4 rw 0 0
/dev/sdb1 /data xfs rw 0 0
/dev/sdc1 /data/images btrfs rw 0 0";
        assert_eq!(
            parse_mounts_fstype(mounts, "/data/images/foo.img"),
            Some("btrfs".into())
        );
    }

    #[test]
    fn mounts_root_fallback() {
        let mounts = "/dev/sda1 / ext4 rw 0 0\n";
        assert_eq!(
            parse_mounts_fstype(mounts, "/tmp/file"),
            Some("ext4".into())
        );
    }

    #[test]
    fn mounts_no_match() {
        let mounts = "/dev/sda1 /data ext4 rw 0 0\n";
        assert_eq!(parse_mounts_fstype(mounts, "/other/path"), None);
    }

    #[test]
    fn mounts_empty() {
        assert_eq!(parse_mounts_fstype("", "/foo"), None);
    }

    #[test]
    fn mounts_malformed_lines() {
        let mounts = "garbage\nonly_one_field\n/dev/sda /mnt\n";
        // Line with <3 fields should be skipped; "/dev/sda /mnt" has only 2 fields
        assert_eq!(parse_mounts_fstype(mounts, "/mnt/foo"), None);
    }

    // -- parse_proc_uptime -------------------------------------------

    #[test]
    fn uptime_normal() {
        assert_eq!(parse_proc_uptime("12345.67 9876.54\n"), 12345);
    }

    #[test]
    fn uptime_empty() {
        assert_eq!(parse_proc_uptime(""), 0);
    }

    #[test]
    fn uptime_garbage() {
        assert_eq!(parse_proc_uptime("not_a_number idle"), 0);
    }

    // -- parse_proc_loadavg ------------------------------------------

    #[test]
    fn loadavg_normal() {
        let (l1, l5, l15) = parse_proc_loadavg("0.50 1.25 2.10 3/450 12345\n");
        assert!((l1 - 0.50).abs() < f64::EPSILON);
        assert!((l5 - 1.25).abs() < f64::EPSILON);
        assert!((l15 - 2.10).abs() < f64::EPSILON);
    }

    #[test]
    fn loadavg_empty() {
        assert_eq!(parse_proc_loadavg(""), (0.0, 0.0, 0.0));
    }

    #[test]
    fn loadavg_partial() {
        let (l1, l5, l15) = parse_proc_loadavg("1.0");
        assert!((l1 - 1.0).abs() < f64::EPSILON);
        assert_eq!(l5, 0.0);
        assert_eq!(l15, 0.0);
    }

    #[test]
    fn loadavg_garbage() {
        assert_eq!(parse_proc_loadavg("x y z"), (0.0, 0.0, 0.0));
    }

    // -- parse_proc_meminfo ------------------------------------------

    #[test]
    fn meminfo_normal() {
        let content = "\
MemTotal:       16384000 kB
MemFree:         2048000 kB
MemAvailable:    8192000 kB
Buffers:          512000 kB
Cached:          4096000 kB
";
        let (total, free, avail) = parse_proc_meminfo(content);
        assert_eq!(total, 16384000);
        assert_eq!(free, 2048000);
        assert_eq!(avail, 8192000);
    }

    #[test]
    fn meminfo_empty() {
        assert_eq!(parse_proc_meminfo(""), (0, 0, 0));
    }

    #[test]
    fn meminfo_missing_fields() {
        let content = "MemTotal:       1000 kB\n";
        let (total, free, avail) = parse_proc_meminfo(content);
        assert_eq!(total, 1000);
        assert_eq!(free, 0);
        assert_eq!(avail, 0);
    }

    #[test]
    fn meminfo_non_numeric() {
        let content = "MemTotal:       abc kB\nMemFree:       def kB\n";
        assert_eq!(parse_proc_meminfo(content), (0, 0, 0));
    }

    // -- parse_proc_status_rss ---------------------------------------

    #[test]
    fn status_rss_normal() {
        let content = "\
Name:   objstrd
Pid:    12345
VmPeak: 500000 kB
VmSize: 400000 kB
VmRSS:  123456 kB
VmData: 200000 kB
";
        assert_eq!(parse_proc_status_rss(content), 123456);
    }

    #[test]
    fn status_rss_missing() {
        let content = "Name: foo\nPid: 1\n";
        assert_eq!(parse_proc_status_rss(content), 0);
    }

    #[test]
    fn status_rss_empty() {
        assert_eq!(parse_proc_status_rss(""), 0);
    }

    #[test]
    fn status_rss_non_numeric() {
        let content = "VmRSS:  abc kB\n";
        assert_eq!(parse_proc_status_rss(content), 0);
    }
}
