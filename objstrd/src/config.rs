//! Configuration parsing for multi-store setups.
//!
//! Supports three modes:
//!
//! 1. **Tree config file** -- `--config cluster.conf --node top` with an
//!    indentation-based tree describing the full cluster topology.
//!
//! 2. **JSON config file** -- `CONFIG_FILE=cluster.json` with a flat cluster
//!    definition including replication factor and store list.
//!
//! 3. **Multi-store env vars** -- `STORE_0_TYPE=raw`, `STORE_0_IMAGE=/dev/sda`,
//!    `STORE_1_TYPE=s3`, `STORE_1_ENDPOINT=...`, etc.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

// -- StoreConfig -------------------------------------------------------------

/// Configuration for a single backend store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Human-readable name (auto-generated if omitted).
    pub name: Option<String>,
    /// Backend type: "raw", "s3", "fs", "mem".
    #[serde(rename = "type")]
    pub store_type: String,

    // -- raw backend fields --
    /// Path to raw image file or block device.
    pub image: Option<String>,
    /// Image size in MB when creating new (default: 256).
    pub size_mb: Option<u64>,
    /// Enable O_DIRECT when formatting.
    pub direct_io: Option<bool>,
    /// Open in read-only mode.
    pub read_only: Option<bool>,

    // -- fs backend fields --
    /// Root directory for LocalFileSystem backend.
    pub root: Option<String>,

    // -- s3 backend fields --
    /// Upstream S3 endpoint URL.
    pub endpoint: Option<String>,
    /// Upstream S3 bucket name.
    pub s3_bucket: Option<String>,
    /// AWS region.
    pub region: Option<String>,
    /// S3 access key.
    pub access_key: Option<String>,
    /// S3 secret key.
    pub secret_key: Option<String>,
    /// Use path-style requests.
    pub force_path_style: Option<bool>,
    /// Allow non-TLS endpoint.
    pub allow_http: Option<bool>,
}

// -- ClusterConfig -----------------------------------------------------------

/// Top-level cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// List of backend stores (at least one required).
    pub stores: Vec<StoreConfig>,
    /// Replication factor (default: 1 = no replication).
    #[serde(default = "default_replication_factor")]
    pub replication_factor: usize,
    /// Minimum successful writes before acknowledging Ok.
    /// None means default (max(replication_factor - 1, 1)).
    #[serde(default)]
    pub min_writes: Option<usize>,
    /// When true, delete-marker writes must also meet `min_writes`.
    #[serde(default)]
    pub delete_requires_min_writes: bool,
    /// Default S3 bucket name (default: "testbucket").
    #[serde(default = "default_bucket")]
    pub default_bucket: String,
}

fn default_replication_factor() -> usize {
    1
}

fn default_bucket() -> String {
    "testbucket".to_string()
}

// -- Parsing -----------------------------------------------------------------

/// Parse cluster configuration from environment variables and/or config file.
///
/// Priority:
/// 1. If `CONFIG_FILE` env var is set, load JSON from that path.
/// 2. Else if `STORE_0_TYPE` env var is set, parse numbered `STORE_N_*` vars.
///
/// Returns `None` if no stores could be configured (caller should exit).
pub fn parse_config() -> Option<ClusterConfig> {
    // 1. Config file
    if let Ok(path) = std::env::var("CONFIG_FILE") {
        return load_config_file(Path::new(&path));
    }

    // 2. Multi-store env vars
    if std::env::var("STORE_0_TYPE").is_ok() {
        return parse_multi_store_env();
    }

    None
}

/// Maximum config file size (10 MB). Rejects obviously oversized files.
const MAX_CONFIG_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// Load a cluster config from a JSON file.
fn load_config_file(path: &Path) -> Option<ClusterConfig> {
    match std::fs::metadata(path) {
        Ok(m) if m.len() > MAX_CONFIG_FILE_SIZE => {
            warn!(path = %path.display(), size = m.len(), max = MAX_CONFIG_FILE_SIZE,
                  "config file too large");
            return None;
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to stat config file");
            return None;
        }
        _ => {}
    }
    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to read config file");
            return None;
        }
    };
    match serde_json::from_str::<ClusterConfig>(&data) {
        Ok(cfg) => {
            if cfg.stores.is_empty() {
                warn!(path = %path.display(), "config file has no stores defined");
                return None;
            }
            Some(cfg)
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to parse config file");
            None
        }
    }
}

/// Parse `STORE_0_TYPE`, `STORE_0_IMAGE`, `STORE_1_TYPE`, ... env vars.
///
/// Scans all indices 0..64 so gaps (e.g. STORE_0, STORE_2 with no STORE_1)
/// are detected and warned about rather than silently dropping higher stores.
fn parse_multi_store_env() -> Option<ClusterConfig> {
    let mut stores = Vec::new();
    let mut last_found: Option<usize> = None;
    let mut gaps: Vec<usize> = Vec::new();

    for i in 0..64 {
        let type_var = format!("STORE_{i}_TYPE");
        let store_type = match std::env::var(&type_var) {
            Ok(t) => t.to_lowercase(),
            Err(_) => continue,
        };

        // Detect gaps in numbering.
        if let Some(prev) = last_found {
            for gap_idx in (prev + 1)..i {
                gaps.push(gap_idx);
            }
        }
        last_found = Some(i);

        let name = std::env::var(format!("STORE_{i}_NAME"))
            .ok()
            .or_else(|| Some(format!("store-{i}")));

        let cfg = StoreConfig {
            name,
            store_type,
            image: std::env::var(format!("STORE_{i}_IMAGE")).ok(),
            size_mb: std::env::var(format!("STORE_{i}_SIZE_MB"))
                .ok()
                .and_then(|v| v.parse().ok()),
            direct_io: std::env::var(format!("STORE_{i}_DIRECT_IO"))
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            read_only: std::env::var(format!("STORE_{i}_READ_ONLY"))
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            root: std::env::var(format!("STORE_{i}_ROOT")).ok(),
            endpoint: std::env::var(format!("STORE_{i}_ENDPOINT")).ok(),
            s3_bucket: std::env::var(format!("STORE_{i}_S3_BUCKET")).ok(),
            region: std::env::var(format!("STORE_{i}_REGION")).ok(),
            access_key: std::env::var(format!("STORE_{i}_ACCESS_KEY")).ok(),
            secret_key: std::env::var(format!("STORE_{i}_SECRET_KEY")).ok(),
            force_path_style: std::env::var(format!("STORE_{i}_FORCE_PATH_STYLE"))
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true")),
            allow_http: std::env::var(format!("STORE_{i}_ALLOW_HTTP"))
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true")),
        };
        stores.push(cfg);
    }

    if !gaps.is_empty() {
        let gap_str: Vec<String> = gaps.iter().map(|g| g.to_string()).collect();
        warn!(
            gaps = gap_str.join(", ").as_str(),
            "gap(s) in STORE_N numbering, check for misnumbered env vars"
        );
    }

    if stores.is_empty() {
        return None;
    }

    let replication_factor: usize = std::env::var("REPLICATION_FACTOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let min_writes: Option<usize> = std::env::var("MIN_WRITES")
        .ok()
        .and_then(|v| v.parse().ok());

    if let Some(mw) = min_writes {
        if mw > replication_factor {
            warn!(
                min_writes = mw,
                replication_factor,
                "MIN_WRITES > REPLICATION_FACTOR; writes will always fail, clamping"
            );
        }
        if mw > stores.len() {
            warn!(
                min_writes = mw,
                store_count = stores.len(),
                "MIN_WRITES > number of stores; writes will always fail"
            );
        }
    }

    let delete_requires_min_writes: bool = std::env::var("DELETE_REQUIRES_MIN_WRITES")
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let default_bucket = std::env::var("BUCKET").unwrap_or_else(|_| "testbucket".to_string());

    Some(ClusterConfig {
        stores,
        replication_factor,
        min_writes,
        delete_requires_min_writes,
        default_bucket,
    })
}

// -- Tree config -------------------------------------------------------------

/// A shard entry within a node: either a local backend or a reference to a
/// child node (which becomes a remote S3 shard).
#[derive(Debug, Clone)]
pub enum TreeShard {
    /// Local RawObjectStore -- path to image file or block device.
    Raw {
        path: String,
        read_only: bool,
        /// Compression algorithm (e.g. "zstd", "snappy", "none").
        /// Only used when formatting a new image.  Overrides the global
        /// `compression` directive for this shard.
        compression: Option<String>,
        /// Enable O_DIRECT.  Only used when formatting a new image.
        /// Overrides the global `direct_io` directive for this shard.
        direct_io: Option<bool>,
        /// Image size in MB when creating a new image.
        /// Overrides the global `size_mb` directive for this shard.
        size_mb: Option<u64>,
    },
    /// Local filesystem directory.
    Fs { root: String, read_only: bool },
    /// Direct S3-compatible backend (AWS S3, Cloudflare R2, etc.).
    S3 {
        endpoint: String,
        bucket: String,
        region: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        path_style: bool,
    },
    /// In-memory store (testing only).
    Mem,
    /// Reference to a child node by name.
    Node(String),
}

/// A node in the cluster tree. Each node runs as a separate objstrd process.
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// Node name (unique within the cluster).
    pub name: String,
    /// Bind address, e.g. "0.0.0.0:8000".
    pub listen: String,
    /// How other nodes reach this one, e.g. "http://10.0.1.10:8000".
    pub endpoint: String,
    /// Replication factor for this node's ShardedObjectStore.
    pub replication_factor: usize,
    /// Minimum successful writes before acknowledging Ok.
    /// None means default (max(replication_factor - 1, 1)).
    pub min_writes: Option<usize>,
    /// When true, delete-marker writes must also meet `min_writes`.
    pub delete_requires_min_writes: bool,
    /// Ordered list of shards and child nodes.
    pub shards: Vec<TreeShard>,
    /// Child nodes (inline, same indentation level).
    pub children: Vec<TreeNode>,
}

/// Top-level tree config parsed from a .conf file.
#[derive(Debug, Clone)]
pub struct TreeConfig {
    /// Cluster name (informational).
    pub cluster_name: String,
    /// Default bucket name.
    pub default_bucket: String,
    /// Root node of the tree.
    pub root: TreeNode,
    /// Read preference policy (None = use default RoundRobin).
    pub read_prefer: Option<String>,
    /// Recovery config overrides (all optional; None = use default).
    pub recovery_enabled: Option<bool>,
    pub recovery_poll_secs: Option<u64>,
    pub recovery_probe_timeout_secs: Option<u64>,
    pub recovery_failure_threshold: Option<u32>,
    pub recovery_re_replicate_batch_size: Option<usize>,
    /// Repair-replication task interval in seconds (0 = disabled).
    pub repair_replication_interval_secs: Option<u64>,
    /// Repair-replication task batch size.
    pub repair_replication_batch_size: Option<usize>,

    // -- Global shard defaults (per-shard values override these) ----------
    /// Default compression for new raw images.
    pub compression: Option<String>,
    /// Default O_DIRECT setting for new raw images.
    pub direct_io: Option<bool>,
    /// Default image size in MB for new raw images.
    pub size_mb: Option<u64>,

    // -- Daemon options (objstrd only; shardedobjstr ignores) ------
    /// Periodic flush interval in seconds for raw shards.
    pub flush_interval: Option<u64>,
    /// Bearer token for /_admin/* admin endpoints.
    pub admin_token: Option<String>,
    /// S3 access key for auth and child-node connections.
    pub access_key: Option<String>,
    /// S3 secret key for auth and child-node connections.
    pub secret_key: Option<String>,
    /// CORS origin for admin endpoints.
    pub cors_origin: Option<String>,
    /// Path to structured log file.
    pub log_file: Option<String>,
    /// In-memory log ring buffer size.
    pub log_buffer_size: Option<usize>,
    /// Unix domain socket path for event broadcasting.
    pub event_socket: Option<String>,
    /// Authentication secret for event socket connections.
    pub event_secret: Option<String>,
    /// Maximum concurrent event socket readers.
    pub max_readers: Option<usize>,
    /// Event source address for streaming replica mode.
    ///
    /// When set on a read-only node, the daemon connects to the
    /// writer's event stream and keeps its catalog up to date via
    /// PUT/DELETE events (no FLUSH required).  Accepts a Unix
    /// socket path or `tcp:host:port`.
    pub event_source: Option<String>,

    // -- Catalog persistence options ------------------------------------
    /// Path to the catalog persistence file.
    /// When set, the catalog is saved to / loaded from this file.
    pub catalog_path: Option<String>,
    /// Catalog format: "json" or "bincode" (default: "json").
    pub catalog_format: Option<String>,
    /// Periodic catalog flush interval in seconds (0 = disabled).
    /// Only flushes if the catalog has been modified since last flush.
    pub catalog_flush_interval: Option<u64>,
}

impl TreeConfig {
    /// Find a node by name anywhere in the tree.
    pub fn find_node(&self, name: &str) -> Option<&TreeNode> {
        find_node_recursive(&self.root, name)
    }
}

fn find_node_recursive<'a>(node: &'a TreeNode, name: &str) -> Option<&'a TreeNode> {
    if node.name == name {
        return Some(node);
    }
    for child in &node.children {
        if let Some(found) = find_node_recursive(child, name) {
            return Some(found);
        }
    }
    None
}

/// Parse a tree config file (.conf format).
///
/// Format:
/// ```text
/// cluster  my-cluster
/// bucket   testbucket
///
/// root-node  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
///   raw  /dev/nvme0n1
///   child-node  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.20:8000
///     raw  /data/shard.raw
///   s3  endpoint=https://s3.amazonaws.com  bucket=archive  region=us-east-1
/// ```
pub fn parse_tree_config(text: &str) -> Result<TreeConfig, String> {
    let mut cluster_name = String::new();
    let mut default_bucket = "testbucket".to_string();
    let mut read_prefer: Option<String> = None;

    // Recovery config overrides (all optional).
    let mut recovery_enabled: Option<bool> = None;
    let mut recovery_poll_secs: Option<u64> = None;
    let mut recovery_probe_timeout_secs: Option<u64> = None;
    let mut recovery_failure_threshold: Option<u32> = None;
    let mut recovery_re_replicate_batch_size: Option<usize> = None;
    let mut repair_replication_interval_secs: Option<u64> = None;
    let mut repair_replication_batch_size: Option<usize> = None;

    // Global shard defaults.
    let mut compression: Option<String> = None;
    let mut direct_io: Option<bool> = None;
    let mut size_mb: Option<u64> = None;

    // Daemon options.
    let mut flush_interval: Option<u64> = None;
    let mut admin_token: Option<String> = None;
    let mut access_key: Option<String> = None;
    let mut secret_key: Option<String> = None;
    let mut cors_origin: Option<String> = None;
    let mut log_file: Option<String> = None;
    let mut log_buffer_size: Option<usize> = None;
    let mut event_socket: Option<String> = None;
    let mut event_secret: Option<String> = None;
    let mut max_readers: Option<usize> = None;
    let mut event_source: Option<String> = None;

    // Catalog persistence options.
    let mut catalog_path: Option<String> = None;
    let mut catalog_format: Option<String> = None;
    let mut catalog_flush_interval: Option<u64> = None;

    // Collect non-empty, non-comment lines with their indent level.
    let mut lines: Vec<(usize, &str)> = Vec::new();
    for raw_line in text.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = raw_line.len() - raw_line.trim_start().len();
        lines.push((indent, trimmed));
    }

    // Extract top-level directives (cluster, bucket, recovery_*) --
    // they have indent 0 and do not contain rf=.
    let mut remaining: Vec<(usize, &str)> = Vec::new();
    for &(indent, line) in &lines {
        if indent == 0 && line.starts_with("cluster ") {
            cluster_name = line.strip_prefix("cluster").unwrap().trim().to_string();
        } else if indent == 0 && line.starts_with("bucket ") {
            default_bucket = line.strip_prefix("bucket").unwrap().trim().to_string();
        } else if indent == 0 && line.starts_with("read_prefer ") {
            read_prefer = Some(line.strip_prefix("read_prefer").unwrap().trim().to_string());
        } else if indent == 0 && line.starts_with("recovery_") {
            let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
            if parts.len() == 2 {
                let val = parts[1].trim();
                match parts[0] {
                    "recovery_enabled" => {
                        recovery_enabled = Some(val == "true" || val == "1");
                    }
                    "recovery_poll_secs" => {
                        recovery_poll_secs = Some(val.parse::<u64>()
                            .map_err(|_| format!("invalid recovery_poll_secs: {val}"))?);
                    }
                    "recovery_probe_timeout_secs" => {
                        recovery_probe_timeout_secs = Some(val.parse::<u64>()
                            .map_err(|_| format!("invalid recovery_probe_timeout_secs: {val}"))?);
                    }
                    "recovery_failure_threshold" => {
                        recovery_failure_threshold = Some(val.parse::<u32>()
                            .map_err(|_| format!("invalid recovery_failure_threshold: {val}"))?);
                    }
                    "recovery_re_replicate_after_secs" => {
                        // Deprecated: silently ignored (grace period removed).
                    }
                    "recovery_re_replicate_batch_size" => {
                        recovery_re_replicate_batch_size = Some(val.parse::<usize>()
                            .map_err(|_| format!("invalid recovery_re_replicate_batch_size: {val}"))?);
                    }
                    _ => {} // ignore unknown recovery_* keys
                }
            }
        } else if indent == 0 {
            // Try to parse as a known top-level directive.
            let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
            let (key, val) = if parts.len() == 2 {
                (parts[0], Some(parts[1].trim()))
            } else {
                (parts[0], None)
            };
            match key {
                "compression" => {
                    compression = val.map(|v| v.to_string());
                }
                "direct_io" => {
                    direct_io = Some(val.map_or(true, |v| v == "true" || v == "1"));
                }
                "size_mb" => {
                    if let Some(v) = val {
                        size_mb = Some(v.parse::<u64>()
                            .map_err(|_| format!("invalid size_mb: {v}"))?);
                    }
                }
                "flush_interval" => {
                    if let Some(v) = val {
                        flush_interval = Some(v.parse::<u64>()
                            .map_err(|_| format!("invalid flush_interval: {v}"))?);
                    }
                }
                "admin_token" => {
                    admin_token = val.map(|v| v.to_string());
                }
                "access_key" => {
                    access_key = val.map(|v| v.to_string());
                }
                "secret_key" => {
                    secret_key = val.map(|v| v.to_string());
                }
                "cors_origin" => {
                    cors_origin = val.map(|v| v.to_string());
                }
                "log_file" => {
                    log_file = val.map(|v| v.to_string());
                }
                "log_buffer_size" => {
                    if let Some(v) = val {
                        log_buffer_size = Some(v.parse::<usize>()
                            .map_err(|_| format!("invalid log_buffer_size: {v}"))?);
                    }
                }
                "event_socket" => {
                    event_socket = val.map(|v| v.to_string());
                }
                "event_secret" => {
                    event_secret = val.map(|v| v.to_string());
                }
                "max_readers" => {
                    if let Some(v) = val {
                        max_readers = Some(v.parse::<usize>()
                            .map_err(|_| format!("invalid max_readers: {v}"))?);
                    }
                }
                "event_source" => {
                    event_source = val.map(|v| v.to_string());
                }
                "repair_replication_interval" => {
                    if let Some(v) = val {
                        repair_replication_interval_secs = Some(v.parse::<u64>()
                            .map_err(|_| format!("invalid repair_replication_interval: {v}"))?);
                    }
                }
                "repair_replication_batch_size" => {
                    if let Some(v) = val {
                        repair_replication_batch_size = Some(v.parse::<usize>()
                            .map_err(|_| format!("invalid repair_replication_batch_size: {v}"))?);
                    }
                }
                "catalog_path" => {
                    catalog_path = val.map(|v| v.to_string());
                }
                "catalog_format" => {
                    catalog_format = val.map(|v| v.to_string());
                }
                "catalog_flush_interval" => {
                    if let Some(v) = val {
                        catalog_flush_interval = Some(v.parse::<u64>()
                            .map_err(|_| format!("invalid catalog_flush_interval: {v}"))?);
                    }
                }
                // CLI-only or flat-config directives -- silently skip so
                // the same config file works for both the daemon and the CLI.
                "read_only" | "catalog" | "replicas"
                | "min_writes" | "delete_requires_min_writes" => {}
                _ => {
                    // Not a known directive -- must be a node line.
                    remaining.push((indent, line));
                }
            }
        } else {
            remaining.push((indent, line));
        }
    }

    if remaining.is_empty() {
        return Err("no nodes defined in tree config".to_string());
    }

    // The first remaining line at the smallest indent is the root node.
    let root = parse_node_block(&remaining, &mut 0)?;

    if cluster_name.is_empty() {
        cluster_name = root.name.clone();
    }

    Ok(TreeConfig {
        cluster_name,
        default_bucket,
        root,
        read_prefer,
        recovery_enabled,
        recovery_poll_secs,
        recovery_probe_timeout_secs,
        recovery_failure_threshold,
        recovery_re_replicate_batch_size,
        compression,
        direct_io,
        size_mb,
        flush_interval,
        admin_token,
        access_key,
        secret_key,
        cors_origin,
        log_file,
        log_buffer_size,
        event_socket,
        event_secret,
        max_readers,
        event_source,
        repair_replication_interval_secs,
        repair_replication_batch_size,
        catalog_path,
        catalog_format,
        catalog_flush_interval,
    })
}

/// Parse a node and its children from lines[*pos..], advancing *pos.
fn parse_node_block(
    lines: &[(usize, &str)],
    pos: &mut usize,
) -> Result<TreeNode, String> {
    if *pos >= lines.len() {
        return Err("unexpected end of config".to_string());
    }

    let (node_indent, node_line) = lines[*pos];
    let node = parse_node_line(node_line)?;
    *pos += 1;

    let mut shards = Vec::new();
    let mut children = Vec::new();

    while *pos < lines.len() {
        let (indent, line) = lines[*pos];
        if indent <= node_indent {
            // Back to parent or sibling level -- stop.
            break;
        }

        // Check if this line is a child node (has rf=) or a shard.
        if is_node_line(line) {
            let child = parse_node_block(lines, pos)?;
            // Add a Node reference shard for this child, then store the child.
            shards.push(TreeShard::Node(child.name.clone()));
            children.push(child);
        } else {
            let shard = parse_shard_line(line)?;
            shards.push(shard);
            *pos += 1;
        }
    }

    Ok(TreeNode {
        name: node.name,
        listen: node.listen,
        endpoint: node.endpoint,
        replication_factor: node.replication_factor,
        min_writes: node.min_writes,
        delete_requires_min_writes: node.delete_requires_min_writes,
        shards,
        children,
    })
}

/// Returns true if a trimmed line looks like a node definition (has rf=).
fn is_node_line(line: &str) -> bool {
    // Node lines have rf= somewhere. Shard lines start with raw/fs/s3/mem.
    let first = line.split_whitespace().next().unwrap_or("");
    !matches!(first, "raw" | "fs" | "s3" | "mem") && line.contains("rf=")
}

/// Split a config line into tokens on whitespace, respecting quoted values.
///
/// Supports double quotes and single quotes so that key=value pairs with
/// spaces in the value are kept intact:
///   `s3  endpoint=https://host  secret_key="my secret"  bucket=test`
/// yields: ["s3", "endpoint=https://host", "secret_key=my secret", "bucket=test"]
fn tokenize_kv_line(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;

    for ch in line.chars() {
        match in_quote {
            Some(q) => {
                if ch == q {
                    in_quote = None;
                } else {
                    current.push(ch);
                }
            }
            None => {
                if ch == '"' || ch == '\'' {
                    in_quote = Some(ch);
                } else if ch.is_whitespace() {
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                } else {
                    current.push(ch);
                }
            }
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Helper struct for partially parsed node line.
struct ParsedNodeLine {
    name: String,
    listen: String,
    endpoint: String,
    replication_factor: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
}

/// Parse a node line like:
///   `top  rf=3  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000`
fn parse_node_line(line: &str) -> Result<ParsedNodeLine, String> {
    let parts = tokenize_kv_line(line);
    if parts.is_empty() {
        return Err("empty node line".to_string());
    }

    let name = parts[0].clone();
    let mut rf = 1usize;
    let mut mw: Option<usize> = None;
    let mut del_req_mw = false;
    let mut listen = String::new();
    let mut endpoint = String::new();
    let mut seen_keys: Vec<String> = Vec::new();

    for part in &parts[1..] {
        // Extract key for duplicate detection.
        let key = part.split('=').next().unwrap_or(part).to_string();
        if seen_keys.contains(&key) {
            return Err(format!("node '{}': duplicate key '{}'", name, key));
        }
        seen_keys.push(key);

        if let Some(val) = part.strip_prefix("rf=") {
            rf = val.parse::<usize>().map_err(|_| format!("invalid rf value: {val}"))?;
        } else if let Some(val) = part.strip_prefix("min_writes=") {
            let v = val.parse::<usize>().map_err(|_| format!("invalid min_writes value: {val}"))?;
            if v == 0 {
                return Err(format!("node '{}': min_writes must be >= 1", name));
            }
            mw = Some(v);
        } else if part == "delete_requires_min_writes"
               || part == "delete_requires_min_writes=true"
               || part == "delete_requires_min_writes=1" {
            del_req_mw = true;
        } else if part == "delete_requires_min_writes=false"
               || part == "delete_requires_min_writes=0" {
            del_req_mw = false;
        } else if let Some(val) = part.strip_prefix("listen=") {
            listen = val.to_string();
        } else if let Some(val) = part.strip_prefix("endpoint=") {
            endpoint = val.to_string();
        }
    }

    if listen.is_empty() {
        return Err(format!("node '{}': missing listen= field", name));
    }
    if endpoint.is_empty() {
        return Err(format!("node '{}': missing endpoint= field", name));
    }

    if let Some(v) = mw {
        if v > rf {
            return Err(format!("node '{}': min_writes ({v}) must be <= rf ({rf})", name));
        }
    }

    Ok(ParsedNodeLine {
        name,
        listen,
        endpoint,
        replication_factor: rf,
        min_writes: mw,
        delete_requires_min_writes: del_req_mw,
    })
}

/// Parse a shard line like:
///   `raw  /dev/nvme0n1`
///   `fs   /mnt/nfs-share`
///   `s3   endpoint=https://s3.amazonaws.com  bucket=archive  region=us-east-1`
///   `s3   endpoint=https://host  secret_key="my secret"  bucket=test`
///   `mem`
fn parse_shard_line(line: &str) -> Result<TreeShard, String> {
    let parts = tokenize_kv_line(line);
    if parts.is_empty() {
        return Err("empty shard line".to_string());
    }

    match parts[0].as_str() {
        "raw" => {
            let path = parts.get(1)
                .ok_or_else(|| "raw shard: missing path".to_string())?;
            let mut read_only = false;
            let mut compression = None;
            let mut direct_io = None;
            let mut size_mb = None;
            for p in parts.iter().skip(2) {
                if p == "readonly" || p == "read_only" || p == "read-only" {
                    read_only = true;
                } else if p == "direct_io" || p == "direct-io" {
                    direct_io = Some(true);
                } else if let Some(val) = p.strip_prefix("direct_io=").or_else(|| p.strip_prefix("direct-io=")) {
                    direct_io = Some(val == "true" || val == "1");
                } else if let Some(val) = p.strip_prefix("compression=") {
                    compression = Some(val.to_string());
                } else if let Some(val) = p.strip_prefix("size_mb=").or_else(|| p.strip_prefix("size-mb=")) {
                    size_mb = Some(val.parse::<u64>()
                        .map_err(|_| format!("raw shard: invalid size_mb: {val}"))?);
                }
            }
            Ok(TreeShard::Raw { path: path.to_string(), read_only, compression, direct_io, size_mb })
        }
        "fs" => {
            let root = parts.get(1)
                .ok_or_else(|| "fs shard: missing root path".to_string())?;
            let read_only = parts.iter().skip(2).any(|p| p == "readonly" || p == "read_only" || p == "read-only");
            Ok(TreeShard::Fs { root: root.to_string(), read_only })
        }
        "mem" => Ok(TreeShard::Mem),
        "s3" => {
            let mut endpoint = String::new();
            let mut bucket = String::new();
            let mut region = None;
            let mut access_key = None;
            let mut secret_key = None;
            let mut path_style = false;
            let mut seen_keys: Vec<String> = Vec::new();

            for part in &parts[1..] {
                // Extract key for duplicate detection.
                let key = part.split('=').next().unwrap_or(part).to_string();
                if seen_keys.contains(&key) {
                    return Err(format!("s3 shard: duplicate key '{}'", key));
                }
                seen_keys.push(key);

                if let Some(val) = part.strip_prefix("endpoint=") {
                    endpoint = val.to_string();
                } else if let Some(val) = part.strip_prefix("bucket=") {
                    bucket = val.to_string();
                } else if let Some(val) = part.strip_prefix("region=") {
                    region = Some(val.to_string());
                } else if let Some(val) = part.strip_prefix("access_key=") {
                    access_key = Some(val.to_string());
                } else if let Some(val) = part.strip_prefix("secret_key=") {
                    secret_key = Some(val.to_string());
                } else if part == "path_style=true" || part == "path_style" {
                    path_style = true;
                }
            }

            if endpoint.is_empty() {
                return Err("s3 shard: missing endpoint= field".to_string());
            }
            if bucket.is_empty() {
                return Err("s3 shard: missing bucket= field".to_string());
            }

            Ok(TreeShard::S3 {
                endpoint,
                bucket,
                region,
                access_key,
                secret_key,
                path_style,
            })
        }
        other => {
            Err(format!("unknown shard type: '{other}' (expected raw, fs, s3, mem)"))
        }
    }
}

/// Load and parse a tree config from a file path.
pub fn load_tree_config(path: &Path) -> Result<TreeConfig, String> {
    match std::fs::metadata(path) {
        Ok(m) if m.len() > MAX_CONFIG_FILE_SIZE => {
            return Err(format!("config file '{}' too large ({} bytes, max {})",
                path.display(), m.len(), MAX_CONFIG_FILE_SIZE));
        }
        Err(e) => {
            return Err(format!("failed to stat '{}': {e}", path.display()));
        }
        _ => {}
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read '{}': {e}", path.display()))?;
    parse_tree_config(&text)
}

// -- Config validation -------------------------------------------------------

/// A single diagnostic message from config validation.
#[derive(Debug)]
pub struct ConfigDiag {
    pub level: DiagLevel,
    pub message: String,
}

/// Severity level for a config diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagLevel {
    Error,
    Warning,
    Info,
}

impl std::fmt::Display for DiagLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiagLevel::Error => write!(f, "ERROR"),
            DiagLevel::Warning => write!(f, "WARN"),
            DiagLevel::Info => write!(f, "INFO"),
        }
    }
}

/// Validate a parsed TreeConfig and return diagnostics.
///
/// Checks:
/// - Node names are unique
/// - listen/endpoint fields are well-formed
/// - raw shard paths exist (warning if not, since they may be auto-created)
/// - fs shard root directories exist
/// - compression values are recognized
/// - replication factors are sane (not larger than shard count)
/// - child node references resolve
/// - numeric values are within reasonable ranges
pub fn validate_tree_config(
    tree: &TreeConfig,
    node_name: Option<&str>,
) -> Vec<ConfigDiag> {
    let mut diags = Vec::new();

    // Collect all node names for uniqueness + reference checks.
    let mut all_names: Vec<String> = Vec::new();
    collect_node_names(&tree.root, &mut all_names);
    let mut seen = std::collections::HashSet::new();
    for name in &all_names {
        if !seen.insert(name.as_str()) {
            diags.push(ConfigDiag {
                level: DiagLevel::Error,
                message: format!("duplicate node name '{name}'"),
            });
        }
    }

    // Check requested node exists.
    if let Some(name) = node_name {
        if tree.find_node(name).is_none() {
            diags.push(ConfigDiag {
                level: DiagLevel::Error,
                message: format!("node '{name}' not found in config"),
            });
        }
    }

    // Validate global compression if set.
    if let Some(ref comp) = tree.compression {
        if rawobjstr::Compression::from_str_name(comp).is_err() {
            diags.push(ConfigDiag {
                level: DiagLevel::Error,
                message: format!("unknown compression algorithm '{comp}'"),
            });
        }
    }

    // -- Global S3 duplicate check: same (endpoint, bucket) anywhere in the
    //    tree means two shards write to the same backend.
    {
        let mut s3_pairs: Vec<(String, String, String)> = Vec::new(); // (endpoint, bucket, node_name)
        collect_s3_shards(&tree.root, &mut s3_pairs);
        let mut seen_s3: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
        for (ep, bkt, nname) in &s3_pairs {
            if !seen_s3.insert((ep.clone(), bkt.clone())) {
                diags.push(ConfigDiag {
                    level: DiagLevel::Error,
                    message: format!(
                        "duplicate S3 backend: endpoint='{}' bucket='{}' \
                         appears on multiple shards (node '{}') -- \
                         replication to the same backend is pointless",
                        ep, bkt, nname,
                    ),
                });
            }
        }
    }

    // -- Per-machine raw/fs duplicate check: nodes on the same host
    //    (same endpoint hostname) must not use the same device or mount.
    {
        let mut nodes_flat: Vec<&TreeNode> = Vec::new();
        collect_nodes(&tree.root, &mut nodes_flat);

        // Group nodes by hostname (strip scheme + port).
        let mut by_host: std::collections::HashMap<String, Vec<&TreeNode>> =
            std::collections::HashMap::new();
        for node in &nodes_flat {
            let host = endpoint_hostname(&node.endpoint);
            by_host.entry(host).or_default().push(node);
        }

        for (host, group) in &by_host {
            // Collect (path, node_name) across all nodes on this host.
            let mut raw_paths: Vec<(String, String)> = Vec::new();
            let mut fs_roots: Vec<(String, String)> = Vec::new();
            for node in group {
                for shard in &node.shards {
                    match shard {
                        TreeShard::Raw { path, .. } => {
                            raw_paths.push((path.clone(), node.name.clone()));
                        }
                        TreeShard::Fs { root, .. } => {
                            fs_roots.push((root.clone(), node.name.clone()));
                        }
                        _ => {}
                    }
                }
            }

            let mut seen_raw: std::collections::HashSet<String> = std::collections::HashSet::new();
            for (path, nname) in &raw_paths {
                if !seen_raw.insert(path.clone()) {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Error,
                        message: format!(
                            "duplicate raw device '{}' on host '{}' (node '{}') -- \
                             multiple shards on the same machine must not share a device",
                            path, host, nname,
                        ),
                    });
                }
            }

            let mut seen_fs: std::collections::HashSet<String> = std::collections::HashSet::new();
            for (root, nname) in &fs_roots {
                if !seen_fs.insert(root.clone()) {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Error,
                        message: format!(
                            "duplicate fs root '{}' on host '{}' (node '{}') -- \
                             multiple shards on the same machine must not share a mount",
                            root, host, nname,
                        ),
                    });
                }
            }
        }
    }

    // Walk all nodes.
    validate_node(&tree.root, tree, &all_names, &mut diags);

    // Info summary.
    let node_count = all_names.len();
    let shard_count = count_shards(&tree.root);
    diags.push(ConfigDiag {
        level: DiagLevel::Info,
        message: format!(
            "cluster '{}': {} node(s), {} shard(s), bucket '{}'",
            tree.cluster_name, node_count, shard_count, tree.default_bucket,
        ),
    });

    diags
}

fn collect_node_names(node: &TreeNode, out: &mut Vec<String>) {
    out.push(node.name.clone());
    for child in &node.children {
        collect_node_names(child, out);
    }
}

/// Collect all S3 shard (endpoint, bucket, node_name) tuples across the tree.
fn collect_s3_shards<'a>(node: &'a TreeNode, out: &mut Vec<(String, String, String)>) {
    for shard in &node.shards {
        if let TreeShard::S3 { endpoint, bucket, .. } = shard {
            out.push((endpoint.clone(), bucket.clone(), node.name.clone()));
        }
    }
    for child in &node.children {
        collect_s3_shards(child, out);
    }
}

/// Collect references to all nodes in the tree.
fn collect_nodes<'a>(node: &'a TreeNode, out: &mut Vec<&'a TreeNode>) {
    out.push(node);
    for child in &node.children {
        collect_nodes(child, out);
    }
}

/// Extract the hostname from an endpoint URL, stripping scheme and port.
/// e.g. "http://10.0.1.10:8000" -> "10.0.1.10"
fn endpoint_hostname(endpoint: &str) -> String {
    let without_scheme = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .unwrap_or(endpoint);
    // Strip port and path.
    without_scheme
        .split(':')
        .next()
        .unwrap_or(without_scheme)
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .to_string()
}

fn count_shards(node: &TreeNode) -> usize {
    let local: usize = node.shards.iter().filter(|s| !matches!(s, TreeShard::Node(_))).count();
    let child_shards: usize = node.children.iter().map(|c| count_shards(c)).sum();
    local + child_shards
}

fn validate_node(
    node: &TreeNode,
    tree: &TreeConfig,
    all_names: &[String],
    diags: &mut Vec<ConfigDiag>,
) {
    // listen should be host:port.
    if !node.listen.contains(':') {
        diags.push(ConfigDiag {
            level: DiagLevel::Warning,
            message: format!("node '{}': listen '{}' missing port (expected host:port)", node.name, node.listen),
        });
    }

    // endpoint should be a URL.
    if !node.endpoint.starts_with("http://") && !node.endpoint.starts_with("https://") {
        diags.push(ConfigDiag {
            level: DiagLevel::Warning,
            message: format!("node '{}': endpoint '{}' does not start with http:// or https://", node.name, node.endpoint),
        });
    }

    // RF should not exceed non-Node shard count.
    let local_shard_count = node.shards.iter().filter(|s| !matches!(s, TreeShard::Node(_))).count()
        + node.shards.iter().filter(|s| matches!(s, TreeShard::Node(_))).count();
    if node.replication_factor > local_shard_count && local_shard_count > 0 {
        diags.push(ConfigDiag {
            level: DiagLevel::Warning,
            message: format!(
                "node '{}': rf={} exceeds shard count {} (will be clamped)",
                node.name, node.replication_factor, local_shard_count,
            ),
        });
    }

    // Validate each shard.
    for (i, shard) in node.shards.iter().enumerate() {
        match shard {
            TreeShard::Raw { path, compression, size_mb, .. } => {
                let p = std::path::Path::new(path);
                if !p.exists() {
                    if size_mb.is_some() || tree.size_mb.is_some() {
                        diags.push(ConfigDiag {
                            level: DiagLevel::Info,
                            message: format!(
                                "node '{}' shard {}: raw '{}' does not exist (will be auto-created)",
                                node.name, i, path,
                            ),
                        });
                    } else {
                        diags.push(ConfigDiag {
                            level: DiagLevel::Warning,
                            message: format!(
                                "node '{}' shard {}: raw '{}' does not exist and no size_mb set",
                                node.name, i, path,
                            ),
                        });
                    }
                }
                // Check parent directory exists.
                if let Some(parent) = p.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        diags.push(ConfigDiag {
                            level: DiagLevel::Error,
                            message: format!(
                                "node '{}' shard {}: parent directory '{}' does not exist",
                                node.name, i, parent.display(),
                            ),
                        });
                    }
                }
                if let Some(ref comp) = compression {
                    if rawobjstr::Compression::from_str_name(comp).is_err() {
                        diags.push(ConfigDiag {
                            level: DiagLevel::Error,
                            message: format!(
                                "node '{}' shard {}: unknown compression '{comp}'",
                                node.name, i,
                            ),
                        });
                    }
                }
            }
            TreeShard::Fs { root, .. } => {
                let p = std::path::Path::new(root);
                if !p.exists() {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Error,
                        message: format!(
                            "node '{}' shard {}: fs root '{}' does not exist",
                            node.name, i, root,
                        ),
                    });
                } else if !p.is_dir() {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Error,
                        message: format!(
                            "node '{}' shard {}: fs root '{}' is not a directory",
                            node.name, i, root,
                        ),
                    });
                }
            }
            TreeShard::S3 { endpoint, bucket, .. } => {
                if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Warning,
                        message: format!(
                            "node '{}' shard {}: S3 endpoint '{}' does not start with http(s)://",
                            node.name, i, endpoint,
                        ),
                    });
                }
                if bucket.is_empty() {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Error,
                        message: format!(
                            "node '{}' shard {}: S3 bucket is empty",
                            node.name, i,
                        ),
                    });
                }
            }
            TreeShard::Node(ref_name) => {
                if !all_names.iter().any(|n| n == ref_name) {
                    diags.push(ConfigDiag {
                        level: DiagLevel::Error,
                        message: format!(
                            "node '{}' shard {}: references unknown child node '{ref_name}'",
                            node.name, i,
                        ),
                    });
                }
            }
            TreeShard::Mem => {}
        }
    }

    // Recurse into children.
    for child in &node.children {
        validate_node(child, tree, all_names, diags);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_config() {
        let json = r#"{
            "stores": [
                { "type": "raw", "name": "shard-0", "image": "/dev/sdz", "size_mb": 1024 },
                { "type": "mem", "name": "shard-1" }
            ],
            "replication_factor": 2,
            "default_bucket": "mybucket"
        }"#;
        let cfg: ClusterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.stores.len(), 2);
        assert_eq!(cfg.stores[0].store_type, "raw");
        assert_eq!(cfg.stores[0].name.as_deref(), Some("shard-0"));
        assert_eq!(cfg.stores[0].image.as_deref(), Some("/dev/sdz"));
        assert_eq!(cfg.stores[1].store_type, "mem");
        assert_eq!(cfg.replication_factor, 2);
        assert_eq!(cfg.default_bucket, "mybucket");
    }

    #[test]
    fn default_values() {
        let json = r#"{ "stores": [{ "type": "mem" }] }"#;
        let cfg: ClusterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.replication_factor, 1);
        assert_eq!(cfg.default_bucket, "testbucket");
    }

    // -- Tree config tests ---------------------------------------------------

    #[test]
    fn tree_config_simple() {
        let text = r#"
cluster  test-cluster
bucket   mybucket

root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.cluster_name, "test-cluster");
        assert_eq!(cfg.default_bucket, "mybucket");
        assert_eq!(cfg.root.name, "root");
        assert_eq!(cfg.root.listen, "0.0.0.0:8000");
        assert_eq!(cfg.root.endpoint, "http://10.0.1.10:8000");
        assert_eq!(cfg.root.replication_factor, 2);
        assert_eq!(cfg.root.shards.len(), 2);
        assert!(matches!(&cfg.root.shards[0], TreeShard::Raw { path, read_only: false, .. } if path == "/dev/nvme0n1"));
        assert!(matches!(&cfg.root.shards[1], TreeShard::Raw { path, read_only: false, .. } if path == "/dev/nvme1n1"));
        assert!(cfg.root.children.is_empty());
    }

    #[test]
    fn tree_config_nested() {
        let text = r#"
# full heterogeneous cluster
cluster  hetero-test
bucket   testbucket

top  rf=3  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1
  inner-left  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.20:8000
    raw  /data/shard0.raw
    inner-sub  rf=1  listen=0.0.0.0:8001  endpoint=http://10.0.1.20:8001
      raw  /dev/sda
      fs   /mnt/nfs-share
  inner-right  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.30:8000
    raw  /data/shard3.raw
    raw  /data/shard4.raw
  s3   endpoint=https://s3.amazonaws.com  bucket=my-archive  region=us-east-1
  s3   endpoint=https://abc123.r2.cloudflarestorage.com  bucket=hot-cache
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.cluster_name, "hetero-test");
        let top = &cfg.root;
        assert_eq!(top.name, "top");
        assert_eq!(top.replication_factor, 3);

        // top has 6 shards: 2 raw + 2 child nodes + 2 S3
        assert_eq!(top.shards.len(), 6);
        assert!(matches!(&top.shards[0], TreeShard::Raw { path, read_only: false, .. } if path == "/dev/nvme0n1"));
        assert!(matches!(&top.shards[1], TreeShard::Raw { path, read_only: false, .. } if path == "/dev/nvme1n1"));
        assert!(matches!(&top.shards[2], TreeShard::Node(n) if n == "inner-left"));
        assert!(matches!(&top.shards[3], TreeShard::Node(n) if n == "inner-right"));
        assert!(matches!(&top.shards[4], TreeShard::S3 { bucket, .. } if bucket == "my-archive"));
        assert!(matches!(&top.shards[5], TreeShard::S3 { bucket, .. } if bucket == "hot-cache"));

        // 2 direct child nodes
        assert_eq!(top.children.len(), 2);

        let left = &top.children[0];
        assert_eq!(left.name, "inner-left");
        assert_eq!(left.shards.len(), 2);
        assert!(matches!(&left.shards[0], TreeShard::Raw { .. }));
        assert!(matches!(&left.shards[1], TreeShard::Node(n) if n == "inner-sub"));
        assert_eq!(left.children.len(), 1);

        let sub = &left.children[0];
        assert_eq!(sub.name, "inner-sub");
        assert_eq!(sub.shards.len(), 2);
        assert!(matches!(&sub.shards[0], TreeShard::Raw { .. }));
        assert!(matches!(&sub.shards[1], TreeShard::Fs { root, .. } if root == "/mnt/nfs-share"));

        let right = &top.children[1];
        assert_eq!(right.name, "inner-right");
        assert_eq!(right.shards.len(), 2);
        assert!(right.children.is_empty());

        // find_node
        assert!(cfg.find_node("top").is_some());
        assert!(cfg.find_node("inner-sub").is_some());
        assert_eq!(cfg.find_node("inner-sub").unwrap().endpoint, "http://10.0.1.20:8001");
        assert!(cfg.find_node("nonexistent").is_none());
    }

    #[test]
    fn tree_config_s3_fields() {
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  s3   endpoint=https://s3.amazonaws.com  bucket=archive  region=eu-west-1  access_key=AKIA  secret_key=secret  path_style
"#;
        let cfg = parse_tree_config(text).unwrap();
        match &cfg.root.shards[0] {
            TreeShard::S3 { endpoint, bucket, region, access_key, secret_key, path_style } => {
                assert_eq!(endpoint, "https://s3.amazonaws.com");
                assert_eq!(bucket, "archive");
                assert_eq!(region.as_deref(), Some("eu-west-1"));
                assert_eq!(access_key.as_deref(), Some("AKIA"));
                assert_eq!(secret_key.as_deref(), Some("secret"));
                assert!(*path_style);
            }
            other => panic!("expected S3 shard, got {:?}", other),
        }
    }

    #[test]
    fn tree_config_mem_shard() {
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  mem
  raw  /dev/sda
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.root.shards.len(), 2);
        assert!(matches!(&cfg.root.shards[0], TreeShard::Mem));
        assert!(matches!(&cfg.root.shards[1], TreeShard::Raw { .. }));
    }

    #[test]
    fn tree_config_comments_and_blanks() {
        let text = r#"
# this is a comment
cluster  my-cluster

# another comment
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000

  # indented comment
  raw  /dev/sda

"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.cluster_name, "my-cluster");
        assert_eq!(cfg.root.shards.len(), 1);
    }

    #[test]
    fn tree_config_missing_listen() {
        let text = "node  rf=1  endpoint=http://host:9000\n  raw  /dev/sda\n";
        let err = parse_tree_config(text).unwrap_err();
        assert!(err.contains("missing listen="), "got: {err}");
    }

    #[test]
    fn tree_config_no_nodes() {
        let text = "cluster foo\nbucket bar\n";
        let err = parse_tree_config(text).unwrap_err();
        assert!(err.contains("no nodes"), "got: {err}");
    }

    #[test]
    fn tree_config_default_cluster_name() {
        let text = "node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000\n  raw  /dev/sda\n";
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.cluster_name, "node");
    }

    // -- Tokenizer tests ----------------------------------------------------

    #[test]
    fn tokenize_basic() {
        let tokens = tokenize_kv_line("s3  endpoint=https://host  bucket=test");
        assert_eq!(tokens, vec!["s3", "endpoint=https://host", "bucket=test"]);
    }

    #[test]
    fn tokenize_double_quotes() {
        let tokens = tokenize_kv_line(
            r#"s3  endpoint=https://host  secret_key="my secret"  bucket=test"#,
        );
        assert_eq!(
            tokens,
            vec!["s3", "endpoint=https://host", "secret_key=my secret", "bucket=test"]
        );
    }

    #[test]
    fn tokenize_single_quotes() {
        let tokens = tokenize_kv_line("s3  endpoint=https://host  secret_key='key with spaces'");
        assert_eq!(
            tokens,
            vec!["s3", "endpoint=https://host", "secret_key=key with spaces"]
        );
    }

    // -- Duplicate key detection tests --------------------------------------

    #[test]
    fn tree_config_duplicate_shard_key() {
        let text = "node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000\n  s3  endpoint=https://s3.example.com  bucket=a  bucket=b\n";
        let err = parse_tree_config(text).unwrap_err();
        assert!(err.contains("duplicate key 'bucket'"), "got: {err}");
    }

    #[test]
    fn tree_config_duplicate_node_key() {
        let text = "node  rf=1  rf=2  listen=0.0.0.0:9000  endpoint=http://host:9000\n  raw  /dev/sda\n";
        let err = parse_tree_config(text).unwrap_err();
        assert!(err.contains("duplicate key 'rf'"), "got: {err}");
    }

    // -- Quoted value integration test --------------------------------------

    #[test]
    fn tree_config_quoted_secret() {
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  s3  endpoint=https://s3.example.com  bucket=test  secret_key="my secret key"
"#;
        let cfg = parse_tree_config(text).unwrap();
        match &cfg.root.shards[0] {
            TreeShard::S3 { secret_key, .. } => {
                assert_eq!(secret_key.as_deref(), Some("my secret key"));
            }
            other => panic!("expected S3 shard, got {:?}", other),
        }
    }

    // -- Per-shard options tests --------------------------------------------

    #[test]
    fn tree_config_raw_shard_options() {
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  raw  /dev/nvme0n1  compression=zstd  direct_io  size_mb=4096
  raw  /dev/nvme1n1  readonly  compression=snappy
  raw  /dev/nvme2n1
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.root.shards.len(), 3);

        match &cfg.root.shards[0] {
            TreeShard::Raw { path, read_only, compression, direct_io, size_mb } => {
                assert_eq!(path, "/dev/nvme0n1");
                assert!(!read_only);
                assert_eq!(compression.as_deref(), Some("zstd"));
                assert_eq!(*direct_io, Some(true));
                assert_eq!(*size_mb, Some(4096));
            }
            other => panic!("expected Raw shard, got {:?}", other),
        }

        match &cfg.root.shards[1] {
            TreeShard::Raw { path, read_only, compression, direct_io, size_mb } => {
                assert_eq!(path, "/dev/nvme1n1");
                assert!(read_only);
                assert_eq!(compression.as_deref(), Some("snappy"));
                assert_eq!(*direct_io, None);
                assert_eq!(*size_mb, None);
            }
            other => panic!("expected Raw shard, got {:?}", other),
        }

        match &cfg.root.shards[2] {
            TreeShard::Raw { path, read_only, compression, direct_io, size_mb } => {
                assert_eq!(path, "/dev/nvme2n1");
                assert!(!read_only);
                assert!(compression.is_none());
                assert!(direct_io.is_none());
                assert!(size_mb.is_none());
            }
            other => panic!("expected Raw shard, got {:?}", other),
        }
    }

    #[test]
    fn tree_config_fs_readonly() {
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  fs  /mnt/data  readonly
  fs  /mnt/rw
"#;
        let cfg = parse_tree_config(text).unwrap();
        match &cfg.root.shards[0] {
            TreeShard::Fs { root, read_only } => {
                assert_eq!(root, "/mnt/data");
                assert!(read_only);
            }
            other => panic!("expected Fs shard, got {:?}", other),
        }
        match &cfg.root.shards[1] {
            TreeShard::Fs { root, read_only } => {
                assert_eq!(root, "/mnt/rw");
                assert!(!read_only);
            }
            other => panic!("expected Fs shard, got {:?}", other),
        }
    }

    #[test]
    fn tree_config_grace_secs_silently_ignored() {
        // grace_secs on shard lines is now deprecated and silently ignored.
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  raw  /dev/nvme0n1  grace_secs=300
  raw  /dev/nvme1n1  grace_secs=0
  raw  /dev/nvme2n1
  fs   /mnt/data  grace_secs=120
  fs   /mnt/rw
  s3   endpoint=http://host:9001  bucket=bk  grace_secs=600
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.root.shards.len(), 6);
    }

    // -- Global shard defaults tests ----------------------------------------

    #[test]
    fn tree_config_global_shard_defaults() {
        let text = r#"
cluster  test
compression  zstd
direct_io    true
size_mb      2048

node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  raw  /dev/sda
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.compression.as_deref(), Some("zstd"));
        assert_eq!(cfg.direct_io, Some(true));
        assert_eq!(cfg.size_mb, Some(2048));
    }

    // -- Daemon options tests -----------------------------------------------

    #[test]
    fn tree_config_daemon_options() {
        let text = r#"
cluster  test
bucket   mybucket

flush_interval   10
admin_token      secret123
access_key       AKIA1234
secret_key       shhh5678
cors_origin      *
log_file         /var/log/objstrd.log
log_buffer_size  20000
event_socket     /tmp/objstrd.sock
event_secret     evtsecret
max_readers      32

node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  raw  /dev/sda
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.flush_interval, Some(10));
        assert_eq!(cfg.admin_token.as_deref(), Some("secret123"));
        assert_eq!(cfg.access_key.as_deref(), Some("AKIA1234"));
        assert_eq!(cfg.secret_key.as_deref(), Some("shhh5678"));
        assert_eq!(cfg.cors_origin.as_deref(), Some("*"));
        assert_eq!(cfg.log_file.as_deref(), Some("/var/log/objstrd.log"));
        assert_eq!(cfg.log_buffer_size, Some(20000));
        assert_eq!(cfg.event_socket.as_deref(), Some("/tmp/objstrd.sock"));
        assert_eq!(cfg.event_secret.as_deref(), Some("evtsecret"));
        assert_eq!(cfg.max_readers, Some(32));
    }

    #[test]
    fn tree_config_daemon_options_absent() {
        let text = r#"
node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  raw  /dev/sda
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert!(cfg.flush_interval.is_none());
        assert!(cfg.admin_token.is_none());
        assert!(cfg.access_key.is_none());
        assert!(cfg.secret_key.is_none());
        assert!(cfg.cors_origin.is_none());
        assert!(cfg.log_file.is_none());
        assert!(cfg.log_buffer_size.is_none());
        assert!(cfg.event_socket.is_none());
        assert!(cfg.event_secret.is_none());
        assert!(cfg.max_readers.is_none());
        assert!(cfg.compression.is_none());
        assert!(cfg.direct_io.is_none());
        assert!(cfg.size_mb.is_none());
    }

    #[test]
    fn tree_config_direct_io_flag_alone() {
        let text = r#"
direct_io

node  rf=1  listen=0.0.0.0:9000  endpoint=http://host:9000
  raw  /dev/sda  direct_io
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.direct_io, Some(true));
        match &cfg.root.shards[0] {
            TreeShard::Raw { direct_io, .. } => assert_eq!(*direct_io, Some(true)),
            other => panic!("expected Raw shard, got {:?}", other),
        }
    }

    #[test]
    fn validate_tree_config_basic() {
        let text = r#"
cluster  test
node  rf=2  listen=0.0.0.0:8000  endpoint=http://host:8000
  mem
  mem
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, Some("node"));
        // Should have no errors, just info.
        for d in &diags {
            assert_ne!(d.level, DiagLevel::Error, "unexpected error: {}", d.message);
        }
        // Should have an info summary.
        assert!(diags.iter().any(|d| d.level == DiagLevel::Info && d.message.contains("2 shard(s)")));
    }

    #[test]
    fn validate_tree_config_missing_node() {
        let text = r#"
node  rf=1  listen=0.0.0.0:8000  endpoint=http://host:8000
  mem
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, Some("nonexistent"));
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("not found")));
    }

    #[test]
    fn validate_tree_config_bad_compression() {
        let text = r#"
compression  bogus

node  rf=1  listen=0.0.0.0:8000  endpoint=http://host:8000
  mem
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("unknown compression")));
    }

    #[test]
    fn validate_tree_config_rf_exceeds_shards() {
        let text = r#"
node  rf=5  listen=0.0.0.0:8000  endpoint=http://host:8000
  mem
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Warning
            && d.message.contains("exceeds shard count")));
    }

    // -- Duplicate shard validation tests -----------------------------------

    #[test]
    fn validate_duplicate_s3_global() {
        // Same endpoint + bucket on two different nodes = Error.
        let text = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  s3  endpoint=https://s3.amazonaws.com  bucket=mydata  region=us-east-1
  child  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.20:8000
    s3  endpoint=https://s3.amazonaws.com  bucket=mydata  region=us-east-1
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate S3 backend")),
            "expected duplicate S3 error, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_s3_same_bucket_different_endpoint() {
        // Same bucket but different endpoints (R2 vs AWS) = OK.
        let text = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  s3  endpoint=https://abc.r2.cloudflarestorage.com  bucket=mydata
  s3  endpoint=https://s3.us-east-1.amazonaws.com  bucket=mydata  region=us-east-1
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(!diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate S3 backend")),
            "should not flag different endpoints with same bucket, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_duplicate_s3_same_node() {
        // Same endpoint + bucket within one node = Error.
        let text = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  s3  endpoint=https://s3.amazonaws.com  bucket=data  region=us-east-1
  s3  endpoint=https://s3.amazonaws.com  bucket=data  region=us-east-1
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate S3 backend")),
            "expected duplicate S3 error, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_duplicate_raw_same_machine() {
        // Two nodes on the same host (same endpoint hostname), same raw path = Error.
        let text = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1  size_mb=64
  child  rf=1  listen=0.0.0.0:9000  endpoint=http://10.0.1.10:9000
    raw  /dev/nvme0n1  size_mb=64
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate raw device")),
            "expected duplicate raw error for same machine, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_duplicate_raw_different_machines() {
        // Two nodes on different hosts, same raw path = OK (different physical devices).
        let text = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1  size_mb=64
  child  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.20:8000
    raw  /dev/nvme0n1  size_mb=64
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(!diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate raw device")),
            "should not flag same path on different machines, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_duplicate_raw_same_node() {
        // Same raw path within one node = Error.
        let text = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1  size_mb=64
  raw  /dev/nvme0n1  size_mb=64
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate raw device")),
            "expected duplicate raw error within same node, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_duplicate_fs_same_machine() {
        // Two nodes on same host, same fs root = Error.
        let text_fs = r#"
root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  fs  /tmp
  child  rf=1  listen=0.0.0.0:9000  endpoint=http://10.0.1.10:9000
    fs  /tmp
"#;
        let cfg = parse_tree_config(text_fs).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Error
            && d.message.contains("duplicate fs root")),
            "expected duplicate fs error for same machine, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn validate_endpoint_hostname_extraction() {
        assert_eq!(endpoint_hostname("http://10.0.1.10:8000"), "10.0.1.10");
        assert_eq!(endpoint_hostname("https://s3.amazonaws.com"), "s3.amazonaws.com");
        assert_eq!(endpoint_hostname("http://myhost:9000/path"), "myhost");
        assert_eq!(endpoint_hostname("bare-host"), "bare-host");
    }

    // -- Malformed / edge case tree config tests ----------------------------

    #[test]
    fn tree_config_empty_input() {
        let text = "";
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_comments_only() {
        let text = "# this is a comment\n# another comment\n";
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_node_without_shards() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert!(cfg.root.shards.is_empty());
        assert!(cfg.root.children.is_empty());
    }

    #[test]
    fn tree_config_raw_missing_path() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_fs_missing_root() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  fs
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_unknown_shard_type() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  blobstore  /dev/sda
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_invalid_rf_value() {
        let text = r#"
root  rf=abc  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_s3_missing_endpoint() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  s3  bucket=mybucket
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_s3_missing_bucket() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  s3  endpoint=https://s3.amazonaws.com
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_s3_duplicate_key() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  s3  endpoint=https://s3.amazonaws.com  bucket=a  endpoint=https://other.com
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_invalid_size_mb() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1  size_mb=not_a_number
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_invalid_recovery_poll_secs() {
        let text = r#"
recovery_poll_secs  notanum
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
"#;
        assert!(parse_tree_config(text).is_err());
    }

    #[test]
    fn tree_config_deeply_nested() {
        let text = r#"
cluster deep-test
a  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.1:8000
  b  rf=1  listen=0.0.0.0:8001  endpoint=http://10.0.1.2:8001
    c  rf=1  listen=0.0.0.0:8002  endpoint=http://10.0.1.3:8002
      d  rf=1  listen=0.0.0.0:8003  endpoint=http://10.0.1.4:8003
        raw  /dev/sda
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.root.name, "a");
        assert_eq!(cfg.root.children.len(), 1);
        let b = &cfg.root.children[0];
        assert_eq!(b.name, "b");
        let c = &b.children[0];
        assert_eq!(c.name, "c");
        let d = &c.children[0];
        assert_eq!(d.name, "d");
        assert_eq!(d.shards.len(), 1);
        assert!(matches!(&d.shards[0], TreeShard::Raw { path, .. } if path == "/dev/sda"));
    }

    #[test]
    fn tree_config_validate_rf_exceeds_shards() {
        let text = r#"
root  rf=5  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1
"#;
        let cfg = parse_tree_config(text).unwrap();
        let diags = validate_tree_config(&cfg, None);
        assert!(diags.iter().any(|d| d.level == DiagLevel::Warning
            || d.level == DiagLevel::Error),
            "expected warning/error for rf > shard count, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>());
    }

    #[test]
    fn tree_config_raw_with_all_options() {
        let text = r#"
root  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1  readonly  compression=zstd  direct_io=true  size_mb=512
"#;
        let cfg = parse_tree_config(text).unwrap();
        match &cfg.root.shards[0] {
            TreeShard::Raw { path, read_only, compression, direct_io, size_mb } => {
                assert_eq!(path, "/dev/nvme0n1");
                assert!(read_only);
                assert_eq!(compression.as_deref(), Some("zstd"));
                assert_eq!(*direct_io, Some(true));
                assert_eq!(*size_mb, Some(512));
            }
            other => panic!("expected Raw shard, got: {:?}", other),
        }
    }

    #[test]
    fn tree_config_multiple_roots_uses_first() {
        // Only the first node at root indent is parsed as root.
        // The second node at the same indent level would be a sibling
        // but parse_node_block stops at same-indent, so it is ignored.
        let text = r#"
root1  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
root2  rf=1  listen=0.0.0.0:8001  endpoint=http://10.0.1.20:8001
  raw  /dev/nvme1n1
"#;
        let cfg = parse_tree_config(text).unwrap();
        assert_eq!(cfg.root.name, "root1");
    }

    // -- Config file priority tests -----------------------------------------

    #[test]
    fn load_config_file_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.json");
        std::fs::write(&path, r#"{
            "stores": [
                { "type": "raw", "image": "/dev/sda" },
                { "type": "mem" }
            ],
            "replication_factor": 2,
            "default_bucket": "from-file"
        }"#).unwrap();
        let cfg = load_config_file(&path).expect("should parse valid JSON");
        assert_eq!(cfg.stores.len(), 2);
        assert_eq!(cfg.replication_factor, 2);
        assert_eq!(cfg.default_bucket, "from-file");
    }

    #[test]
    fn load_config_file_missing_file() {
        let result = load_config_file(Path::new("/nonexistent/path/config.json"));
        assert!(result.is_none(), "missing file should return None");
    }

    #[test]
    fn load_config_file_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not valid json {{{").unwrap();
        let result = load_config_file(&path);
        assert!(result.is_none(), "invalid JSON should return None");
    }

    #[test]
    fn load_config_file_empty_stores() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");
        std::fs::write(&path, r#"{ "stores": [] }"#).unwrap();
        let result = load_config_file(&path);
        assert!(result.is_none(), "empty stores should return None");
    }

    #[test]
    fn load_config_file_too_large() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.json");
        // Write a file just over 10 MB
        let data = "x".repeat(10 * 1024 * 1024 + 1);
        std::fs::write(&path, data).unwrap();
        let result = load_config_file(&path);
        assert!(result.is_none(), "oversized file should return None");
    }

    /// Verify CONFIG_FILE takes priority over STORE_N_* env vars.
    /// parse_config() checks CONFIG_FILE first and returns early if present.
    #[test]
    fn parse_config_file_takes_priority_over_env() {
        // Create a valid JSON config file
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("priority.json");
        std::fs::write(&path, r#"{
            "stores": [{ "type": "mem", "name": "from-file" }],
            "default_bucket": "filebucket"
        }"#).unwrap();

        // Set both CONFIG_FILE and STORE_0_TYPE. CONFIG_FILE should win.
        // Use a unique prefix to avoid conflicts with parallel tests.
        std::env::set_var("CONFIG_FILE", path.to_str().unwrap());
        std::env::set_var("STORE_0_TYPE", "mem");
        std::env::set_var("STORE_0_NAME", "from-env");

        let cfg = parse_config().expect("parse_config should succeed");

        // Clean up env vars immediately
        std::env::remove_var("CONFIG_FILE");
        std::env::remove_var("STORE_0_TYPE");
        std::env::remove_var("STORE_0_NAME");

        // The config should come from the JSON file, not env vars
        assert_eq!(cfg.default_bucket, "filebucket",
            "CONFIG_FILE should take priority over STORE_N_* env vars");
        assert_eq!(cfg.stores[0].name.as_deref(), Some("from-file"),
            "store name should come from JSON file, not env var");
    }
}
