//! Config file parser for ShardedObjectStore.
//!
//! Supports all shard types (raw, fs, s3, mem) and accepts the same config
//! format as `objstrd`.  Daemon-specific keywords (`cluster`, `bucket`,
//! `flush_interval`, `admin_token`, `access_key`, `secret_key`, `cors_origin`,
//! `log_file`, `log_buffer_size`, `event_socket`, `event_secret`, `max_readers`,
//! and `recovery_*`) are silently ignored so that a single config file can be
//! shared between the daemon and the CLI.
//!
//! # Format
//!
//! ```text
//! # cluster.conf -- comments start with #
//! replicas  2
//! catalog   json:/tmp/catalog.json
//! read_prefer  round-robin
//! direct_io    true
//! compression  zstd
//! size_mb      1024
//! read_only    false
//!
//! shard  raw  /dev/nvme0n1
//! shard  raw  /dev/nvme1n1  size_mb=512  compression=none
//! shard  fs   /mnt/data
//! shard  s3   endpoint=https://s3.amazonaws.com  bucket=my-bucket  region=us-east-1
//! shard  mem
//! ```
//!
//! Global directives set defaults; per-shard key=value pairs override them.

use std::path::Path;

/// Configuration for a single shard.
#[derive(Debug, Clone)]
pub enum ShardConf {
    /// Local raw block device or loopback image.
    Raw {
        path: String,
        read_only: bool,
        compression: Option<String>,
        direct_io: Option<bool>,
        size_mb: Option<u64>,
    },
    /// Local filesystem directory.
    Fs {
        root: String,
        read_only: bool,
    },
    /// S3-compatible endpoint.
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
    /// Reference to a child node by name (tree configs only).
    /// The child node is itself a `ShardedObjectStore` composed from its own
    /// shards and replication factor.
    Node(String),
}

impl ShardConf {
    /// Human-readable type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            ShardConf::Raw { .. } => "raw",
            ShardConf::Fs { .. } => "fs",
            ShardConf::S3 { .. } => "s3",
            ShardConf::Mem => "mem",
            ShardConf::Node(_) => "node",
        }
    }
}

// -- Tree config (nested clusters) -------------------------------------------

/// A node in a cluster tree.  Each node becomes its own `ShardedObjectStore`
/// with its own replication factor.  Child nodes are referenced as `Node`
/// shards in the parent.
///
/// This struct does NOT contain daemon-specific fields like `listen` or
/// `endpoint` -- those belong in `objstrd`.
#[derive(Debug, Clone)]
pub struct TreeNode {
    /// Node name (unique within the cluster).
    pub name: String,
    /// Replication factor for this node's ShardedObjectStore.
    pub replication_factor: usize,
    /// Minimum successful writes before a put returns Ok.
    /// None means use default (max(rf - 1, 1)).
    pub min_writes: Option<usize>,
    /// Ordered list of shards (raw, fs, s3, mem, or child node refs).
    pub shards: Vec<ShardConf>,
    /// Child nodes (inline, defined by indentation).
    pub children: Vec<TreeNode>,
}

impl TreeNode {
    /// Find a child node by name (recursive).
    pub fn find_node(&self, name: &str) -> Option<&TreeNode> {
        if self.name == name {
            return Some(self);
        }
        for child in &self.children {
            if let Some(found) = child.find_node(name) {
                return Some(found);
            }
        }
        None
    }
}

/// Top-level tree config parsed from a .conf file.
///
/// Contains only the storage topology (nodes, shards, replication).
/// Daemon-specific fields (listen, endpoint, recovery, flush, auth, logging)
/// are NOT included -- `objstrd` parses those separately from the same file.
#[derive(Debug, Clone)]
pub struct TreeConf {
    /// Cluster name (informational).
    pub cluster_name: String,
    /// Root node of the tree.
    pub root: TreeNode,
    /// Catalog persistence spec.
    pub catalog: Option<String>,
    /// Read preference policy.
    pub read_prefer: Option<String>,
    /// Global default: compression for new raw images.
    pub compression: Option<String>,
    /// Global default: O_DIRECT setting.
    pub direct_io: bool,
    /// Global default: image size in MB for new raw images.
    pub size_mb: Option<u64>,
    /// Global default: open shards read-only.
    pub read_only: bool,
}

impl TreeConf {
    /// Find a node by name anywhere in the tree.
    pub fn find_node(&self, name: &str) -> Option<&TreeNode> {
        self.root.find_node(name)
    }
}

/// Top-level cluster configuration parsed from a .conf file.
#[derive(Debug, Clone)]
pub struct ClusterConf {
    /// Replication factor (default 1).
    pub replicas: usize,
    /// Minimum successful writes before a put returns Ok.
    /// None means use default (max(replicas - 1, 1)).
    pub min_writes: Option<usize>,
    /// Catalog persistence spec: "none", "json:<path>", "bincode:<path>", or bare path.
    pub catalog: Option<String>,
    /// Read preference: "round-robin" or "ordered".
    pub read_prefer: Option<String>,
    /// Global default: open raw shards with O_DIRECT.
    pub direct_io: bool,
    /// Global default: compression for new raw images.
    pub compression: Option<String>,
    /// Global default: image size in MB for new raw images.
    pub size_mb: Option<u64>,
    /// Global default: open raw/fs shards read-only.
    pub read_only: bool,
    /// When true, deletes require min_writes quorum (default: false).
    pub delete_requires_min_writes: bool,
    /// Ordered list of shards (raw, fs, s3, or mem).
    pub shards: Vec<ShardConf>,
}

/// Daemon-only directives that we silently ignore when parsing.
const DAEMON_DIRECTIVES: &[&str] = &[
    "cluster", "bucket", "flush_interval", "admin_token",
    "access_key", "secret_key", "cors_origin", "log_file",
    "log_buffer_size", "event_socket", "event_secret", "max_readers",
    "event_source", "catalog_path", "catalog_format", "catalog_flush_interval",
];

/// Parse a cluster config from text.
///
/// Daemon-specific directives and indented tree-config lines (child nodes)
/// are silently ignored so the same file works for both `objstrd` and the
/// `shardedobjstr` CLI.
///
/// Returns a descriptive error string on failure.
pub fn parse_cluster_conf(text: &str) -> Result<ClusterConf, String> {
    let mut replicas: usize = 1;
    let mut min_writes: Option<usize> = None;
    let mut catalog: Option<String> = None;
    let mut read_prefer: Option<String> = None;
    let mut direct_io = false;
    let mut compression: Option<String> = None;
    let mut size_mb: Option<u64> = None;
    let mut read_only = false;
    let mut delete_requires_min_writes = false;
    let mut shards: Vec<ShardConf> = Vec::new();

    for (line_num, raw_line) in text.lines().enumerate() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Skip indented lines (tree-config child nodes / nested shards).
        let indent = raw_line.len() - raw_line.trim_start().len();
        if indent > 0 {
            continue;
        }

        let line_label = line_num + 1;

        let tokens = tokenize(trimmed)
            .map_err(|e| format!("line {}: {}", line_label, e))?;
        if tokens.is_empty() {
            continue;
        }

        match tokens[0].as_str() {
            "replicas" => {
                let val = require_value(&tokens, "replicas", line_label)?;
                replicas = val.parse::<usize>()
                    .map_err(|_| format!("line {}: invalid replicas '{val}'", line_label))?;
                if replicas == 0 {
                    return Err(format!("line {}: replicas must be >= 1", line_label));
                }
            }
            "min_writes" => {
                let val = require_value(&tokens, "min_writes", line_label)?;
                let mw = val.parse::<usize>()
                    .map_err(|_| format!("line {}: invalid min_writes '{val}'", line_label))?;
                if mw == 0 {
                    return Err(format!("line {}: min_writes must be >= 1", line_label));
                }
                min_writes = Some(mw);
            }
            "catalog" => {
                catalog = Some(require_value(&tokens, "catalog", line_label)?.to_string());
            }
            "read_prefer" => {
                let val = require_value(&tokens, "read_prefer", line_label)?;
                match val {
                    "round-robin" | "ordered" => {}
                    _ => return Err(format!(
                        "line {}: unknown read_prefer '{}' (expected round-robin or ordered)",
                        line_label, val,
                    )),
                }
                read_prefer = Some(val.to_string());
            }
            "direct_io" => {
                let val = tokens.get(1).map(|s| s.as_str()).unwrap_or("true");
                direct_io = val == "true" || val == "1";
            }
            "compression" => {
                compression = Some(require_value(&tokens, "compression", line_label)?.to_string());
            }
            "size_mb" => {
                let val = require_value(&tokens, "size_mb", line_label)?;
                size_mb = Some(val.parse::<u64>()
                    .map_err(|_| format!("line {}: invalid size_mb '{val}'", line_label))?);
            }
            "read_only" => {
                let val = tokens.get(1).map(|s| s.as_str()).unwrap_or("true");
                read_only = val == "true" || val == "1";
            }
            "delete_requires_min_writes" => {
                let val = tokens.get(1).map(|s| s.as_str()).unwrap_or("true");
                delete_requires_min_writes = val == "true" || val == "1";
            }
            "shard" => {
                let shard = parse_shard_tokens(&tokens[1..], line_label)?;
                shards.push(shard);
            }
            // Daemon-only directives -- silently ignored.
            key if DAEMON_DIRECTIVES.contains(&key) => {}
            // recovery_* and repair_replication_* directives -- silently ignored.
            key if key.starts_with("recovery_") || key.starts_with("repair_replication_") => {}
            // Tree-config node lines (contain rf=) -- silently ignored.
            _ if trimmed.contains("rf=") => {}
            other => {
                return Err(format!(
                    "line {}: unknown directive '{other}' (expected replicas, min_writes, catalog, \
                     read_prefer, direct_io, compression, size_mb, read_only, or shard)",
                    line_label,
                ));
            }
        }
    }

    if shards.is_empty() {
        return Err("no shards defined".to_string());
    }

    if let Some(mw) = min_writes {
        if mw > replicas {
            return Err(format!(
                "min_writes ({mw}) must be <= replicas ({replicas})"
            ));
        }
    }

    Ok(ClusterConf {
        replicas,
        min_writes,
        catalog,
        read_prefer,
        direct_io,
        compression,
        size_mb,
        read_only,
        delete_requires_min_writes,
        shards,
    })
}

/// Load and parse a cluster config file.
pub fn load_cluster_conf(path: &Path) -> Result<ClusterConf, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read '{}': {e}", path.display()))?;
    parse_cluster_conf(&text)
}

// -- Tree config parser ------------------------------------------------------

/// Daemon-only directives that we silently skip in tree configs.
const TREE_DAEMON_DIRECTIVES: &[&str] = &[
    "bucket", "flush_interval", "admin_token",
    "access_key", "secret_key", "cors_origin", "log_file",
    "log_buffer_size", "event_socket", "event_secret", "max_readers",
    "event_source", "catalog_path", "catalog_format", "catalog_flush_interval",
];

/// Parse a tree config from text.
///
/// Tree configs use indentation to define nested clusters.  Each node line
/// contains a name and `rf=N`.  Shards are listed under their parent node.
/// Daemon-specific fields (`listen=`, `endpoint=`, `recovery_*`, etc.) are
/// silently ignored so that the same config file works for both `objstrd`
/// and the `shardedobjstr` CLI.
///
/// ```text
/// cluster  my-cluster
/// compression  zstd
///
/// root  rf=2
///   raw  /dev/nvme0n1
///   raw  /dev/nvme1n1
///   child  rf=1
///     fs  /mnt/data
///     s3  endpoint=https://s3.amazonaws.com  bucket=backup
/// ```
pub fn parse_tree_conf(text: &str) -> Result<TreeConf, String> {
    let mut cluster_name = String::new();
    let mut catalog: Option<String> = None;
    let mut read_prefer: Option<String> = None;
    let mut compression: Option<String> = None;
    let mut direct_io = false;
    let mut size_mb: Option<u64> = None;
    let mut read_only = false;

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

    // Extract top-level directives (indent 0, not node lines).
    let mut remaining: Vec<(usize, &str)> = Vec::new();
    for &(indent, line) in &lines {
        if indent == 0 {
            let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
            let key = parts[0];
            let val = parts.get(1).map(|v| v.trim());

            match key {
                "cluster" => {
                    cluster_name = val.unwrap_or("").to_string();
                }
                "catalog" => {
                    catalog = val.map(|v| v.to_string());
                }
                "read_prefer" => {
                    if let Some(v) = val {
                        match v {
                            "round-robin" | "ordered" => {}
                            _ => return Err(format!(
                                "unknown read_prefer '{}' (expected round-robin or ordered)", v,
                            )),
                        }
                        read_prefer = Some(v.to_string());
                    }
                }
                "replicas" => {
                    // In tree config "replicas" at indent 0 is ambiguous --
                    // the rf is per-node.  Accept it as a top-level hint but
                    // it does not override per-node rf values.
                    // Silently skip.
                }
                "compression" => {
                    compression = val.map(|v| v.to_string());
                }
                "direct_io" => {
                    direct_io = val.map_or(true, |v| v == "true" || v == "1");
                }
                "size_mb" => {
                    if let Some(v) = val {
                        size_mb = Some(v.parse::<u64>()
                            .map_err(|_| format!("invalid size_mb: {v}"))?);
                    }
                }
                "read_only" => {
                    read_only = val.map_or(true, |v| v == "true" || v == "1");
                }
                // Daemon-only directives -- silently skip.
                k if TREE_DAEMON_DIRECTIVES.contains(&k) => {}
                // recovery_* and repair_replication_* directives -- silently skip.
                k if k.starts_with("recovery_") || k.starts_with("repair_replication_") => {}
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
    let root = parse_tree_node_block(&remaining, &mut 0)?;

    if cluster_name.is_empty() {
        cluster_name = root.name.clone();
    }

    Ok(TreeConf {
        cluster_name,
        root,
        catalog,
        read_prefer,
        compression,
        direct_io,
        size_mb,
        read_only,
    })
}

/// Load and parse a tree config file.
pub fn load_tree_conf(path: &Path) -> Result<TreeConf, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read '{}': {e}", path.display()))?;
    parse_tree_conf(&text)
}

/// Returns true if a config file contains tree-style node definitions.
///
/// A tree config has at least one non-indented line containing `rf=` that
/// does not start with a shard keyword.  This distinguishes it from a flat
/// config which uses `shard` lines and `replicas`.
pub fn is_tree_config(text: &str) -> bool {
    for raw_line in text.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = raw_line.len() - raw_line.trim_start().len();
        if indent == 0 && is_tree_node_line(trimmed) {
            return true;
        }
    }
    false
}

/// Auto-detect and parse: if the config has tree-style nodes, parse as tree;
/// otherwise parse as flat.  Returns the tree conf in either case.
///
/// For a flat config the result is a single-node tree where the root's
/// shards and replication factor come from the flat config.
pub fn load_auto_conf(path: &Path) -> Result<TreeConf, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read '{}': {e}", path.display()))?;
    if is_tree_config(&text) {
        parse_tree_conf(&text)
    } else {
        let flat = parse_cluster_conf(&text)?;
        Ok(TreeConf {
            cluster_name: String::new(),
            root: TreeNode {
                name: "root".to_string(),
                replication_factor: flat.replicas,
                min_writes: flat.min_writes,
                shards: flat.shards,
                children: Vec::new(),
            },
            catalog: flat.catalog,
            read_prefer: flat.read_prefer,
            compression: flat.compression,
            direct_io: flat.direct_io,
            size_mb: flat.size_mb,
            read_only: flat.read_only,
        })
    }
}

/// Parse a node block and its children from lines[*pos..], advancing *pos.
fn parse_tree_node_block(
    lines: &[(usize, &str)],
    pos: &mut usize,
) -> Result<TreeNode, String> {
    if *pos >= lines.len() {
        return Err("unexpected end of config".to_string());
    }

    let (node_indent, node_line) = lines[*pos];
    let (name, rf, mw) = parse_tree_node_line(node_line)?;
    *pos += 1;

    let mut shards = Vec::new();
    let mut children = Vec::new();

    while *pos < lines.len() {
        let (indent, line) = lines[*pos];
        if indent <= node_indent {
            break;
        }

        if is_tree_node_line(line) {
            let child = parse_tree_node_block(lines, pos)?;
            shards.push(ShardConf::Node(child.name.clone()));
            children.push(child);
        } else {
            let shard = parse_tree_shard_line(line)?;
            shards.push(shard);
            *pos += 1;
        }
    }

    Ok(TreeNode {
        name,
        replication_factor: rf,
        min_writes: mw,
        shards,
        children,
    })
}

/// Returns true if a trimmed line looks like a node definition (has rf=).
fn is_tree_node_line(line: &str) -> bool {
    let first = line.split_whitespace().next().unwrap_or("");
    !matches!(first, "raw" | "fs" | "s3" | "mem" | "shard") && line.contains("rf=")
}

/// Parse a node header line like:
///   `root  rf=3  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000`
///
/// Extracts only the node name and replication factor.  Daemon-specific
/// fields (`listen=`, `endpoint=`) are silently ignored.
fn parse_tree_node_line(line: &str) -> Result<(String, usize, Option<usize>), String> {
    let parts = tokenize(line)?;
    if parts.is_empty() {
        return Err("empty node line".to_string());
    }

    let name = parts[0].clone();
    let mut rf = 1usize;
    let mut mw: Option<usize> = None;

    for part in &parts[1..] {
        if let Some(val) = part.strip_prefix("rf=") {
            rf = val.parse::<usize>()
                .map_err(|_| format!("node '{}': invalid rf value: {val}", name))?;
            if rf == 0 {
                return Err(format!("node '{}': rf must be >= 1", name));
            }
        } else if let Some(val) = part.strip_prefix("min_writes=") {
            let v = val.parse::<usize>()
                .map_err(|_| format!("node '{}': invalid min_writes value: {val}", name))?;
            if v == 0 {
                return Err(format!("node '{}': min_writes must be >= 1", name));
            }
            mw = Some(v);
        }
        // listen=, endpoint=, and other daemon keys are silently skipped.
    }

    if let Some(v) = mw {
        if v > rf {
            return Err(format!("node '{}': min_writes ({v}) must be <= rf ({rf})", name));
        }
    }

    Ok((name, rf, mw))
}

/// Parse a shard line within a tree config (same syntax as flat config but
/// without the leading "shard" keyword).
fn parse_tree_shard_line(line: &str) -> Result<ShardConf, String> {
    let parts = tokenize(line)?;
    if parts.is_empty() {
        return Err("empty shard line".to_string());
    }

    match parts[0].as_str() {
        "raw" => parse_raw_shard(&parts[1..], 0),
        "fs" => parse_fs_shard(&parts[1..], 0),
        "s3" => parse_s3_shard(&parts[1..], 0),
        "mem" => Ok(ShardConf::Mem),
        // Bare path (implicit raw type).
        _ if parts[0].starts_with('/') || parts[0].contains('.') => {
            parse_raw_shard(&parts, 0)
        }
        other => Err(format!("unknown shard type: '{other}' (expected raw, fs, s3, mem)")),
    }
}

/// A single diagnostic from config validation.
#[derive(Debug)]
pub struct ClusterDiag {
    pub level: ClusterDiagLevel,
    pub message: String,
}

/// Severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterDiagLevel {
    Error,
    Warning,
    Info,
}

impl std::fmt::Display for ClusterDiagLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterDiagLevel::Error => write!(f, "ERROR"),
            ClusterDiagLevel::Warning => write!(f, "WARN"),
            ClusterDiagLevel::Info => write!(f, "INFO"),
        }
    }
}

/// Validate a parsed cluster config and return diagnostics.
pub fn validate_cluster_conf(conf: &ClusterConf) -> Vec<ClusterDiag> {
    let mut diags = Vec::new();

    if conf.replicas > conf.shards.len() {
        diags.push(ClusterDiag {
            level: ClusterDiagLevel::Warning,
            message: format!(
                "replicas ({}) exceeds shard count ({}) -- will be clamped",
                conf.replicas, conf.shards.len(),
            ),
        });
    }

    if let Some(ref comp) = conf.compression {
        if rawobjstr::Compression::from_str_name(comp).is_err() {
            diags.push(ClusterDiag {
                level: ClusterDiagLevel::Error,
                message: format!("unknown global compression '{comp}'"),
            });
        }
    }

    for (i, shard) in conf.shards.iter().enumerate() {
        match shard {
            ShardConf::Raw { path, compression, .. } => {
                let p = std::path::Path::new(path);
                if !p.exists() {
                    let has_size = match shard {
                        ShardConf::Raw { size_mb, .. } => size_mb.is_some(),
                        _ => false,
                    } || conf.size_mb.is_some();
                    if has_size {
                        diags.push(ClusterDiag {
                            level: ClusterDiagLevel::Info,
                            message: format!(
                                "shard {} (raw): '{}' does not exist (will be formatted)",
                                i, path,
                            ),
                        });
                    } else {
                        diags.push(ClusterDiag {
                            level: ClusterDiagLevel::Warning,
                            message: format!(
                                "shard {} (raw): '{}' does not exist and no size_mb set",
                                i, path,
                            ),
                        });
                    }
                }
                if let Some(parent) = p.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        diags.push(ClusterDiag {
                            level: ClusterDiagLevel::Error,
                            message: format!(
                                "shard {} (raw): parent directory '{}' does not exist",
                                i, parent.display(),
                            ),
                        });
                    }
                }
                if let Some(ref comp) = compression {
                    if rawobjstr::Compression::from_str_name(comp).is_err() {
                        diags.push(ClusterDiag {
                            level: ClusterDiagLevel::Error,
                            message: format!("shard {} (raw): unknown compression '{comp}'", i),
                        });
                    }
                }
            }
            ShardConf::Fs { root, .. } => {
                let p = std::path::Path::new(root);
                if !p.exists() {
                    diags.push(ClusterDiag {
                        level: ClusterDiagLevel::Warning,
                        message: format!(
                            "shard {} (fs): directory '{}' does not exist",
                            i, root,
                        ),
                    });
                }
            }
            ShardConf::S3 { endpoint, bucket, .. } => {
                if endpoint.is_empty() {
                    diags.push(ClusterDiag {
                        level: ClusterDiagLevel::Error,
                        message: format!("shard {} (s3): empty endpoint", i),
                    });
                }
                if bucket.is_empty() {
                    diags.push(ClusterDiag {
                        level: ClusterDiagLevel::Error,
                        message: format!("shard {} (s3): empty bucket", i),
                    });
                }
            }
            ShardConf::Mem => {}
            ShardConf::Node(_) => {}
        }
    }

    let shard_count = conf.shards.len();
    diags.push(ClusterDiag {
        level: ClusterDiagLevel::Info,
        message: format!(
            "{} shard(s), replicas={}, catalog={}",
            shard_count,
            conf.replicas,
            conf.catalog.as_deref().unwrap_or("none"),
        ),
    });

    diags
}

// -- Internal helpers --------------------------------------------------------

fn require_value<'a>(tokens: &'a [String], key: &str, line: usize) -> Result<&'a str, String> {
    tokens.get(1)
        .map(|s| s.as_str())
        .ok_or_else(|| format!("line {}: '{}' requires a value", line, key))
}

fn tokenize(line: &str) -> Result<Vec<String>, String> {
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

    if in_quote.is_some() {
        return Err(format!("unterminated quote in: {}", line));
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

/// Parse shard tokens after the "shard" keyword.
///
/// Accepted formats:
///   `raw  /path/to/image  [readonly] [direct_io] [compression=alg] [size_mb=N]`
///   `/path/to/image  [key=val ...]`  (implicit raw type)
///   `fs   /path/to/dir  [readonly]`
///   `s3   endpoint=URL  bucket=NAME  [region=R] [access_key=K] [secret_key=S] [path_style]`
///   `mem`
fn parse_shard_tokens(tokens: &[String], line: usize) -> Result<ShardConf, String> {
    if tokens.is_empty() {
        return Err(format!("line {}: shard line has no type or path", line));
    }

    match tokens[0].as_str() {
        "raw" => parse_raw_shard(&tokens[1..], line),
        "fs" => parse_fs_shard(&tokens[1..], line),
        "s3" => parse_s3_shard(&tokens[1..], line),
        "mem" => Ok(ShardConf::Mem),
        // Bare path (implicit raw type).
        _ => parse_raw_shard(tokens, line),
    }
}

fn parse_raw_shard(tokens: &[String], line: usize) -> Result<ShardConf, String> {
    if tokens.is_empty() {
        return Err(format!("line {}: raw shard missing path", line));
    }

    // First token can be a bare path or path=<value>.
    let path = tokens[0].strip_prefix("path=").unwrap_or(&tokens[0]).to_string();
    let mut read_only = false;
    let mut compression: Option<String> = None;
    let mut direct_io: Option<bool> = None;
    let mut size_mb: Option<u64> = None;

    for tok in &tokens[1..] {
        if tok == "readonly" || tok == "read_only" || tok == "read-only" {
            read_only = true;
        } else if tok == "direct_io" || tok == "direct-io" {
            direct_io = Some(true);
        } else if let Some(val) = tok.strip_prefix("compression=") {
            compression = Some(val.to_string());
        } else if let Some(val) = tok.strip_prefix("size_mb=").or_else(|| tok.strip_prefix("size-mb=")) {
            size_mb = Some(val.parse::<u64>()
                .map_err(|_| format!("line {}: invalid size_mb '{val}'", line))?);
        } else if let Some(val) = tok.strip_prefix("direct_io=").or_else(|| tok.strip_prefix("direct-io=")) {
            direct_io = Some(val == "true" || val == "1");
        } else {
            return Err(format!("line {}: unknown raw shard option '{tok}'", line));
        }
    }

    Ok(ShardConf::Raw { path, read_only, compression, direct_io, size_mb })
}

fn parse_fs_shard(tokens: &[String], line: usize) -> Result<ShardConf, String> {
    if tokens.is_empty() {
        return Err(format!("line {}: fs shard missing root path", line));
    }
    // First token can be a bare path or root=<value>.
    let root = tokens[0].strip_prefix("root=").unwrap_or(&tokens[0]).to_string();
    let mut read_only = false;
    for tok in tokens.iter().skip(1) {
        if tok == "readonly" || tok == "read_only" || tok == "read-only" {
            read_only = true;
        } else {
            return Err(format!("line {}: unknown fs shard option '{tok}'", line));
        }
    }
    Ok(ShardConf::Fs { root, read_only })
}

fn parse_s3_shard(tokens: &[String], line: usize) -> Result<ShardConf, String> {
    let mut endpoint = String::new();
    let mut bucket = String::new();
    let mut region = None;
    let mut access_key = None;
    let mut secret_key = None;
    let mut path_style = false;

    for tok in tokens {
        if let Some(val) = tok.strip_prefix("endpoint=") {
            endpoint = val.to_string();
        } else if let Some(val) = tok.strip_prefix("bucket=") {
            bucket = val.to_string();
        } else if let Some(val) = tok.strip_prefix("region=") {
            region = Some(val.to_string());
        } else if let Some(val) = tok.strip_prefix("access_key=") {
            access_key = Some(val.to_string());
        } else if let Some(val) = tok.strip_prefix("secret_key=") {
            secret_key = Some(val.to_string());
        } else if tok == "path_style" || tok == "path_style=true" {
            path_style = true;
        } else {
            return Err(format!("line {}: unknown s3 shard option '{tok}'", line));
        }
    }

    if endpoint.is_empty() {
        return Err(format!("line {}: s3 shard missing endpoint=", line));
    }
    if bucket.is_empty() {
        return Err(format!("line {}: s3 shard missing bucket=", line));
    }

    Ok(ShardConf::S3 { endpoint, bucket, region, access_key, secret_key, path_style })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let text = "shard raw /dev/sda\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.replicas, 1);
        assert_eq!(conf.shards.len(), 1);
        assert!(matches!(&conf.shards[0], ShardConf::Raw { path, read_only, .. }
            if path == "/dev/sda" && !read_only));
        assert!(conf.catalog.is_none());
    }

    #[test]
    fn parse_full_config() {
        let text = r#"
# Full cluster config
replicas  3
catalog   json:/tmp/catalog.json
read_prefer  round-robin
direct_io    true
compression  zstd
size_mb      1024
read_only    false

shard  raw  /dev/nvme0n1
shard  raw  /dev/nvme1n1  compression=none
shard  /tmp/extra.raw  size_mb=512  direct_io
"#;
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.replicas, 3);
        assert_eq!(conf.catalog.as_deref(), Some("json:/tmp/catalog.json"));
        assert_eq!(conf.read_prefer.as_deref(), Some("round-robin"));
        assert!(conf.direct_io);
        assert_eq!(conf.compression.as_deref(), Some("zstd"));
        assert_eq!(conf.size_mb, Some(1024));
        assert!(!conf.read_only);

        assert_eq!(conf.shards.len(), 3);
        assert!(matches!(&conf.shards[0], ShardConf::Raw { path, .. } if path == "/dev/nvme0n1"));
        assert!(matches!(&conf.shards[1], ShardConf::Raw { compression, .. }
            if compression.as_deref() == Some("none")));
        assert!(matches!(&conf.shards[2], ShardConf::Raw { path, size_mb, direct_io, .. }
            if path == "/tmp/extra.raw" && *size_mb == Some(512) && *direct_io == Some(true)));
    }

    #[test]
    fn parse_implicit_raw_type() {
        let text = "shard /dev/sda\nshard /dev/sdb\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.shards.len(), 2);
        assert!(matches!(&conf.shards[0], ShardConf::Raw { path, .. } if path == "/dev/sda"));
        assert!(matches!(&conf.shards[1], ShardConf::Raw { path, .. } if path == "/dev/sdb"));
    }

    #[test]
    fn parse_shard_readonly() {
        let text = "shard raw /dev/sda readonly\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert!(matches!(&conf.shards[0], ShardConf::Raw { read_only: true, .. }));
    }

    #[test]
    fn parse_fs_shard() {
        let text = "shard fs /mnt/data\nshard fs /mnt/backup readonly\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.shards.len(), 2);
        assert!(matches!(&conf.shards[0], ShardConf::Fs { root, read_only }
            if root == "/mnt/data" && !read_only));
        assert!(matches!(&conf.shards[1], ShardConf::Fs { root, read_only }
            if root == "/mnt/backup" && *read_only));
    }

    #[test]
    fn parse_s3_shard() {
        let text = "shard s3 endpoint=https://s3.example.com bucket=my-data region=us-east-1\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.shards.len(), 1);
        assert!(matches!(&conf.shards[0], ShardConf::S3 { endpoint, bucket, region, .. }
            if endpoint == "https://s3.example.com" && bucket == "my-data"
            && region.as_deref() == Some("us-east-1")));
    }

    #[test]
    fn parse_s3_with_credentials() {
        let text = r#"shard s3 endpoint=http://localhost:9000 bucket=test access_key=admin secret_key="password" path_style"#;
        let conf = parse_cluster_conf(&format!("{text}\n")).unwrap();
        assert!(matches!(&conf.shards[0], ShardConf::S3 {
            endpoint, bucket, access_key, secret_key, path_style, ..
        } if endpoint == "http://localhost:9000" && bucket == "test"
            && access_key.as_deref() == Some("admin")
            && secret_key.as_deref() == Some("password")
            && *path_style));
    }

    #[test]
    fn parse_mem_shard() {
        let text = "shard mem\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert!(matches!(&conf.shards[0], ShardConf::Mem));
    }

    #[test]
    fn parse_mixed_shards() {
        let text = "\
replicas 2
shard raw /dev/sda
shard fs /mnt/data
shard s3 endpoint=https://s3.example.com bucket=archive
shard mem
";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.shards.len(), 4);
        assert!(matches!(&conf.shards[0], ShardConf::Raw { .. }));
        assert!(matches!(&conf.shards[1], ShardConf::Fs { .. }));
        assert!(matches!(&conf.shards[2], ShardConf::S3 { .. }));
        assert!(matches!(&conf.shards[3], ShardConf::Mem));
    }

    #[test]
    fn daemon_directives_ignored() {
        let text = "\
cluster  test-cluster
bucket   testbucket
flush_interval  30
admin_token  secret123
access_key  AKID
secret_key  SKEY
cors_origin  *
log_file  /tmp/log
log_buffer_size  1000
event_socket  /tmp/events.sock
event_secret  shh
max_readers  16
event_source  tcp:localhost:9090
catalog_path  /tmp/catalog.json
catalog_format  json
catalog_flush_interval  30
recovery_enabled  true
recovery_poll_secs  10
repair_replication_interval  300
repair_replication_batch_size  500
replicas  2
shard raw /dev/sda
shard raw /dev/sdb
";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.replicas, 2);
        assert_eq!(conf.shards.len(), 2);
    }

    #[test]
    fn tree_config_node_lines_ignored() {
        let text = "\
cluster  prod
replicas  2
shard raw /dev/sda
shard raw /dev/sdb
top  rf=3  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
";
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.shards.len(), 2);
    }

    #[test]
    fn indented_lines_ignored() {
        let text = "\
replicas  2
shard raw /dev/sda
shard raw /dev/sdb
top  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/sdc
  s3  endpoint=https://s3.example.com  bucket=test
";
        let conf = parse_cluster_conf(text).unwrap();
        // Only the top-level shard lines are parsed.
        assert_eq!(conf.shards.len(), 2);
    }

    #[test]
    fn error_no_shards() {
        let text = "replicas 2\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("no shards"), "got: {err}");
    }

    #[test]
    fn error_unknown_directive() {
        let text = "bogus 42\nshard raw /dev/sda\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("unknown directive 'bogus'"), "got: {err}");
    }

    #[test]
    fn error_invalid_replicas() {
        let text = "replicas abc\nshard raw /dev/sda\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("invalid replicas"), "got: {err}");
    }

    #[test]
    fn error_zero_replicas() {
        let text = "replicas 0\nshard raw /dev/sda\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("replicas must be >= 1"), "got: {err}");
    }

    #[test]
    fn error_invalid_read_prefer() {
        let text = "read_prefer bogus\nshard raw /dev/sda\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("unknown read_prefer"), "got: {err}");
    }

    #[test]
    fn error_shard_unknown_option() {
        let text = "shard raw /dev/sda foobar\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("unknown raw shard option"), "got: {err}");
    }

    #[test]
    fn error_s3_missing_endpoint() {
        let text = "shard s3 bucket=test\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("missing endpoint"), "got: {err}");
    }

    #[test]
    fn error_s3_missing_bucket() {
        let text = "shard s3 endpoint=https://s3.example.com\n";
        let err = parse_cluster_conf(text).unwrap_err();
        assert!(err.contains("missing bucket"), "got: {err}");
    }

    #[test]
    fn comments_and_blank_lines() {
        let text = r#"
# This is a comment
replicas  2

# Another comment
shard  raw  /dev/sda
shard  raw  /dev/sdb
"#;
        let conf = parse_cluster_conf(text).unwrap();
        assert_eq!(conf.replicas, 2);
        assert_eq!(conf.shards.len(), 2);
    }

    #[test]
    fn direct_io_flag_alone() {
        let text = "direct_io\nshard raw /dev/sda\n";
        let conf = parse_cluster_conf(text).unwrap();
        assert!(conf.direct_io);
    }

    #[test]
    fn validate_basic() {
        let text = "shard raw /dev/sda\nshard raw /dev/sdb\n";
        let conf = parse_cluster_conf(text).unwrap();
        let diags = validate_cluster_conf(&conf);
        for d in &diags {
            assert_ne!(d.level, ClusterDiagLevel::Error, "unexpected error: {}", d.message);
        }
    }

    #[test]
    fn validate_rf_exceeds_shards() {
        let conf = ClusterConf {
            replicas: 5,
            min_writes: None,
            catalog: None,
            read_prefer: None,
            direct_io: false,
            compression: None,
            size_mb: None,
            read_only: false,
            delete_requires_min_writes: false,
            shards: vec![
                ShardConf::Raw { path: "/dev/sda".into(), read_only: false,
                            compression: None, direct_io: None, size_mb: None },
            ],
        };
        let diags = validate_cluster_conf(&conf);
        assert!(diags.iter().any(|d| d.level == ClusterDiagLevel::Warning
            && d.message.contains("exceeds shard count")));
    }

    // -- Tree config tests ---------------------------------------------------

    #[test]
    fn tree_simple() {
        let text = r#"
cluster  test-cluster

root  rf=2
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1
"#;
        let conf = parse_tree_conf(text).unwrap();
        assert_eq!(conf.cluster_name, "test-cluster");
        assert_eq!(conf.root.name, "root");
        assert_eq!(conf.root.replication_factor, 2);
        assert_eq!(conf.root.shards.len(), 2);
        assert!(matches!(&conf.root.shards[0], ShardConf::Raw { path, .. }
            if path == "/dev/nvme0n1"));
        assert!(matches!(&conf.root.shards[1], ShardConf::Raw { path, .. }
            if path == "/dev/nvme1n1"));
        assert!(conf.root.children.is_empty());
    }

    #[test]
    fn tree_nested() {
        let text = r#"
cluster  nested-test

top  rf=2
  raw  /dev/nvme0n1
  inner  rf=1
    raw  /dev/sda
    fs   /mnt/data
  s3   endpoint=https://s3.amazonaws.com  bucket=backup  region=us-east-1
"#;
        let conf = parse_tree_conf(text).unwrap();
        let top = &conf.root;
        assert_eq!(top.name, "top");
        assert_eq!(top.replication_factor, 2);
        // 3 shards: raw + Node(inner) + s3
        assert_eq!(top.shards.len(), 3);
        assert!(matches!(&top.shards[0], ShardConf::Raw { path, .. }
            if path == "/dev/nvme0n1"));
        assert!(matches!(&top.shards[1], ShardConf::Node(name) if name == "inner"));
        assert!(matches!(&top.shards[2], ShardConf::S3 { endpoint, bucket, .. }
            if endpoint == "https://s3.amazonaws.com" && bucket == "backup"));

        // Child node
        assert_eq!(top.children.len(), 1);
        let inner = &top.children[0];
        assert_eq!(inner.name, "inner");
        assert_eq!(inner.replication_factor, 1);
        assert_eq!(inner.shards.len(), 2);
        assert!(matches!(&inner.shards[0], ShardConf::Raw { path, .. }
            if path == "/dev/sda"));
        assert!(matches!(&inner.shards[1], ShardConf::Fs { root, .. }
            if root == "/mnt/data"));
    }

    #[test]
    fn tree_deeply_nested() {
        let text = r#"
top  rf=2
  raw  /dev/nvme0n1
  mid  rf=2
    raw  /dev/sda
    leaf  rf=1
      fs   /mnt/leaf
      mem
"#;
        let conf = parse_tree_conf(text).unwrap();
        assert_eq!(conf.cluster_name, "top"); // defaults to root name
        let mid = &conf.root.children[0];
        assert_eq!(mid.name, "mid");
        assert_eq!(mid.replication_factor, 2);
        let leaf = &mid.children[0];
        assert_eq!(leaf.name, "leaf");
        assert_eq!(leaf.replication_factor, 1);
        assert_eq!(leaf.shards.len(), 2);
        assert!(matches!(&leaf.shards[0], ShardConf::Fs { root, .. }
            if root == "/mnt/leaf"));
        assert!(matches!(&leaf.shards[1], ShardConf::Mem));
    }

    #[test]
    fn tree_ignores_daemon_fields() {
        let text = r#"
cluster  prod
bucket   data
flush_interval  30
admin_token  secret
access_key  AKID
secret_key  SKEY
cors_origin  *
log_file  /tmp/log
log_buffer_size  1000
event_socket  /tmp/events.sock
event_secret  shh
max_readers  16
event_source  tcp:localhost:9090
catalog_path  /tmp/catalog.json
catalog_format  json
catalog_flush_interval  30
recovery_enabled  true
recovery_poll_secs  10
repair_replication_interval  300
repair_replication_batch_size  500
compression  zstd

root  rf=2  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1
"#;
        let conf = parse_tree_conf(text).unwrap();
        assert_eq!(conf.cluster_name, "prod");
        assert_eq!(conf.compression.as_deref(), Some("zstd"));
        assert_eq!(conf.root.name, "root");
        assert_eq!(conf.root.replication_factor, 2);
        assert_eq!(conf.root.shards.len(), 2);
    }

    #[test]
    fn tree_find_node() {
        let text = r#"
top  rf=2
  raw  /dev/nvme0n1
  mid  rf=1
    raw  /dev/sda
    leaf  rf=1
      fs  /mnt/data
"#;
        let conf = parse_tree_conf(text).unwrap();
        assert!(conf.find_node("top").is_some());
        assert!(conf.find_node("mid").is_some());
        assert!(conf.find_node("leaf").is_some());
        assert!(conf.find_node("nonexistent").is_none());

        let leaf = conf.find_node("leaf").unwrap();
        assert_eq!(leaf.shards.len(), 1);
    }

    #[test]
    fn tree_is_tree_config() {
        assert!(is_tree_config("root  rf=2\n  raw  /dev/sda\n"));
        assert!(!is_tree_config("shard raw /dev/sda\n"));
        assert!(!is_tree_config("replicas 2\nshard raw /dev/sda\n"));
        // Indented node lines don't count (they need indent 0).
        assert!(!is_tree_config("  child  rf=1\n"));
    }

    #[test]
    fn tree_error_no_nodes() {
        let text = "cluster foo\ncompression zstd\n";
        let err = parse_tree_conf(text).unwrap_err();
        assert!(err.contains("no nodes"), "got: {err}");
    }

    #[test]
    fn tree_error_zero_rf() {
        let text = "root  rf=0\n  raw  /dev/sda\n";
        let err = parse_tree_conf(text).unwrap_err();
        assert!(err.contains("rf must be >= 1"), "got: {err}");
    }

    #[test]
    fn tree_global_defaults() {
        let text = r#"
catalog   json:/tmp/cat.json
read_prefer  ordered
compression  zstd
direct_io    true
size_mb      1024
read_only    true

root  rf=1
  raw  /dev/sda
"#;
        let conf = parse_tree_conf(text).unwrap();
        assert_eq!(conf.catalog.as_deref(), Some("json:/tmp/cat.json"));
        assert_eq!(conf.read_prefer.as_deref(), Some("ordered"));
        assert_eq!(conf.compression.as_deref(), Some("zstd"));
        assert!(conf.direct_io);
        assert_eq!(conf.size_mb, Some(1024));
        assert!(conf.read_only);
    }
}
