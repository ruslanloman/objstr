//! CLI tools for ShardedObjectStore.
//!
//! ## Commands
//!
//! ```bash
//! # Format a 3-shard cluster from loopback files (replication factor 2)
//! shardedobjstr format --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
//!     --size 1073741824 --replicas 2
//!
//! # Show cluster and per-shard info
//! shardedobjstr info --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw
//!
//! # List all files (shows shard placement)
//! shardedobjstr list --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --long
//!
//! # Put a file (replicated across shards automatically)
//! shardedobjstr put --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
//!     --replicas 2 --key data/example.txt --from ./example.txt
//!
//! # Get a file (round-robin across replicas, auto failover)
//! shardedobjstr get --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
//!     --replicas 2 --key data/example.txt --to ./out.example.txt
//!
//! # Delete a file (removed from all shards)
//! shardedobjstr delete --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
//!     --replicas 2 --key data/example.txt
//!
//! # Verify all shards + cross-shard CRC consistency
//! shardedobjstr verify --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
//!     --replicas 2
//!
//! ```

use std::io::Write;
use std::path::Path as StdPath;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::{FormatOptions, RawObjectStore, VerifyStatus};
use rawobjstr::Compression;

use shardedobjstr::ShardedObjectStore;
use shardedobjstr::config as cluster_conf;
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "--version" | "-V" | "version" => {
            println!("{}", version_string());
        }
        "check-config" => cmd_check_config(&args[2..]),
        "format" => cmd_format(&args[2..]),
        "info" => cmd_info(&args[2..]),
        "list" | "ls" => cmd_list(&args[2..]),
        "get" => cmd_get(&args[2..]),
        "put" => cmd_put(&args[2..]),
        "delete" | "del" => cmd_delete(&args[2..]),
        "verify" => cmd_verify(&args[2..]),
        "cross-verify" => cmd_cross_verify(&args[2..]),
        "health" => cmd_health(&args[2..]),
        "add-shard" => cmd_add_shard(&args[2..]),
        "remove-shard" => cmd_remove_shard(&args[2..]),
        "repair-replication" => cmd_repair_replication(&args[2..]),
        "report" => cmd_report(&args[2..]),
        "vacuum" => cmd_vacuum(&args[2..]),
        "list-deleted" => cmd_list_deleted(&args[2..]),
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }
}

fn version_string() -> String {
    format!(
        "shardedobjstr {} (git {}, built {})",
        shardedobjstr::VERSION,
        shardedobjstr::BUILD_GIT_HASH,
        shardedobjstr::BUILD_DATE,
    )
}

fn print_usage() {
    eprintln!("{}", version_string());
    eprintln!();
    eprintln!(
        "Usage:
  shardedobjstr check-config --config <path>       Validate a config file and exit
  shardedobjstr format  --shards <path,path,...> --size <bytes> [--replicas N] [--direct-io]
  shardedobjstr info    --shards <path,path,...>   (or --config <path>)
  shardedobjstr list    --shards <path,path,...> [--replicas N] [--prefix <prefix>] [--long]
  shardedobjstr get     --shards <path,path,...> --replicas N --key <object-path> [--to <file>]
  shardedobjstr put     --shards <path,path,...> --replicas N --key <object-path> --from <file>
  shardedobjstr delete  --shards <path,path,...> --replicas N --key <object-path>
  shardedobjstr verify  --shards <path,path,...> [--replicas N]
  shardedobjstr cross-verify --shards <path,path,...> --replicas N [--prefix <prefix>] [--key <key>]
  shardedobjstr health  --shards <path,path,...>
  shardedobjstr add-shard    --shards <existing,...> --new-shard <path> --size <bytes> [--replicas N]
  shardedobjstr remove-shard --shards <all-including-victim,...> --remove <path> [--replicas N]
  shardedobjstr repair-replication --shards <path,path,...> --replicas N [--batch-size N]
  shardedobjstr report  --shards <path,path,...> --replicas N [--under-replicated]
  shardedobjstr vacuum  --shards <path,path,...> --replicas N
  shardedobjstr list-deleted --shards <path,path,...> --replicas N

  --config      Path to a cluster .conf file (alternative to --shards).
                When used, shard paths, replicas, catalog, etc. come from the file.
  --shards      Comma-separated paths to raw devices or loopback image files.
  --replicas    Replication factor (default: 1). Clamped to shard count.
  --size        Size in bytes for format (each shard gets this size).
  --direct-io   Use O_DIRECT (Linux only).
  --long / -l   Show sizes and shard placements in list output.
  --catalog     Catalog persistence: 'none', 'json:<path>', or 'bin:<path>'.
                Default: none (rebuild from shard indexes each time).
  --new-shard   Path for the new shard to format and add (add-shard).
  --remove      Path of the shard to drain and remove (remove-shard).
  --batch-size  Max objects per sweep (default: 100). Used by repair-replication.
  --under-replicated  Filter to objects below target replication factor (report)."
    );
}

// -- Arg parsing -----------------------------------------------------

struct CliArgs {
    #[allow(dead_code)]
    config: Option<String>,
    /// Shard configs (from --config or synthesized from --shards).
    shard_confs: Vec<cluster_conf::ShardConf>,
    replicas: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
    size: Option<u64>,
    direct_io: bool,
    key: Option<String>,
    prefix: Option<String>,
    to: Option<String>,
    from: Option<String>,
    long: bool,
    new_shard: Option<String>,
    remove: Option<String>,
    catalog: Option<String>,
    batch_size: Option<usize>,
    under_replicated: bool,
    _node: Option<String>,
    tree_conf: Option<cluster_conf::TreeConf>,
}

impl CliArgs {
    /// Extract raw shard paths (only for commands that need local raw stores).
    fn raw_shard_paths(&self) -> Vec<String> {
        self.shard_confs.iter().filter_map(|s| match s {
            cluster_conf::ShardConf::Raw { path, .. } => Some(path.clone()),
            _ => None,
        }).collect()
    }
}

fn parse_args(args: &[String]) -> CliArgs {
    let mut config: Option<String> = None;
    let mut shard_paths: Vec<String> = Vec::new();
    let mut replicas = 1usize;
    let mut min_writes: Option<usize> = None;
    let mut delete_requires_min_writes = false;
    let mut size = None;
    let mut direct_io = false;
    let mut key = None;
    let mut prefix = None;
    let mut to = None;
    let mut from = None;
    let mut long = false;
    let mut new_shard = None;
    let mut remove = None;
    let mut catalog = None;
    let mut batch_size = None;
    let mut under_replicated = false;
    let mut node: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        macro_rules! next_val {
            ($flag:expr) => {{
                i += 1;
                if i >= args.len() {
                    eprintln!("{} requires a value", $flag);
                    std::process::exit(1);
                }
                &args[i]
            }};
        }
        match args[i].as_str() {
            "--config" => {
                config = Some(next_val!("--config").clone());
            }
            "--shards" => {
                shard_paths = next_val!("--shards").split(',').map(|s| s.trim().to_string()).collect();
            }
            "--replicas" => {
                replicas = next_val!("--replicas").parse().expect("invalid --replicas");
            }
            "--min-writes" => {
                min_writes = Some(next_val!("--min-writes").parse().expect("invalid --min-writes"));
            }
            "--delete-requires-min-writes" => delete_requires_min_writes = true,
            "--size" => {
                size = Some(next_val!("--size").parse::<u64>().expect("invalid --size"));
            }
            "--direct-io" => direct_io = true,
            "--key" => {
                key = Some(next_val!("--key").clone());
            }
            "--prefix" => {
                prefix = Some(next_val!("--prefix").clone());
            }
            "--to" => {
                to = Some(next_val!("--to").clone());
            }
            "--from" => {
                from = Some(next_val!("--from").clone());
            }
            "--long" | "-l" => long = true,
            "--new-shard" => {
                new_shard = Some(next_val!("--new-shard").clone());
            }
            "--remove" => {
                remove = Some(next_val!("--remove").clone());
            }
            "--catalog" => {
                catalog = Some(next_val!("--catalog").clone());
            }
            "--batch-size" => {
                batch_size = Some(next_val!("--batch-size").parse().expect("invalid --batch-size"));
            }
            "--under-replicated" => under_replicated = true,
            "--node" => {
                node = Some(next_val!("--node").clone());
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Build shard_confs: from --config or from --shards (as raw).
    let mut shard_confs: Vec<cluster_conf::ShardConf> = Vec::new();
    let mut tree_conf: Option<cluster_conf::TreeConf> = None;

    if let Some(ref conf_path) = config {
        let text = std::fs::read_to_string(conf_path)
            .unwrap_or_else(|e| {
                eprintln!("ERROR: failed to read '{}': {e}", conf_path);
                std::process::exit(1);
            });

        if cluster_conf::is_tree_config(&text) {
            // Tree config -- build nested topology.
            let tc = cluster_conf::parse_tree_conf(&text)
                .unwrap_or_else(|e| {
                    eprintln!("ERROR: {e}");
                    std::process::exit(1);
                });

            // Select the node to operate on.
            let target_name = node.as_deref().unwrap_or(&tc.root.name);
            let target = tc.find_node(target_name)
                .unwrap_or_else(|| {
                    eprintln!("ERROR: node '{}' not found in config", target_name);
                    std::process::exit(1);
                });

            shard_confs = target.shards.clone();
            if !args.iter().any(|a| a == "--replicas") {
                replicas = target.replication_factor;
            }
            if min_writes.is_none() {
                min_writes = target.min_writes;
            }
            if catalog.is_none() {
                catalog = tc.catalog.clone();
            }
            if !direct_io {
                direct_io = tc.direct_io;
            }
            tree_conf = Some(tc);
        } else {
            // Flat config.
            let conf = cluster_conf::parse_cluster_conf(&text)
                .unwrap_or_else(|e| {
                    eprintln!("ERROR: {e}");
                    std::process::exit(1);
                });
            if shard_paths.is_empty() {
                shard_confs = conf.shards;
            }
            if !args.iter().any(|a| a == "--replicas") {
                replicas = conf.replicas;
            }
            if min_writes.is_none() {
                min_writes = conf.min_writes;
            }
            if catalog.is_none() {
                catalog = conf.catalog;
            }
            if !direct_io {
                direct_io = conf.direct_io;
            }
            if !delete_requires_min_writes {
                delete_requires_min_writes = conf.delete_requires_min_writes;
            }
        }
    }

    // --shards always means raw shards and overrides config shards.
    if !shard_paths.is_empty() {
        shard_confs = shard_paths.iter().map(|p| cluster_conf::ShardConf::Raw {
            path: p.clone(),
            read_only: false,
            compression: None,
            direct_io: None,
            size_mb: None,
        }).collect();
    }

    if shard_confs.is_empty() {
        eprintln!("--shards or --config required");
        std::process::exit(1);
    }

    CliArgs {
        config,
        shard_confs,
        replicas,
        min_writes,
        delete_requires_min_writes,
        size,
        direct_io,
        key,
        prefix,
        to,
        from,
        long,
        new_shard,
        remove,
        catalog,
        batch_size,
        under_replicated,
        _node: node,
        tree_conf,
    }
}

// -- Helpers ---------------------------------------------------------

/// Parse a `--catalog` value into an optional file path for JSON persistence.
///
/// Accepted formats:
///   `none`           -> None
///   `json:<path>`    -> Json persistence
///   `bincode:<path>` -> Bincode persistence
///   `<path>`         -> Json persistence (bare path)
fn parse_catalog_persistence(s: &str) -> Option<shardedobjstr::catalog::CatalogPersistence> {
    if s == "none" {
        return None;
    }
    if let Some(path) = s.strip_prefix("json:") {
        return Some(shardedobjstr::catalog::CatalogPersistence::json(path));
    }
    if let Some(path) = s.strip_prefix("bincode:").or_else(|| s.strip_prefix("bin:")) {
        return Some(shardedobjstr::catalog::CatalogPersistence::bincode(path));
    }
    // Bare path -- treat as JSON
    Some(shardedobjstr::catalog::CatalogPersistence::json(s))
}

/// Open all shards and build a ShardedObjectStore.
/// Rebuilds the catalog from each shard's persisted index.
fn open_cluster(confs: &[cluster_conf::ShardConf], replicas: usize, min_writes: Option<usize>, delete_requires_min_writes: bool, tree: Option<&cluster_conf::TreeConf>) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    open_cluster_with_catalog_and_tree(confs, replicas, min_writes, delete_requires_min_writes, None, tree)
}

/// Open a shard config entry into an ObjectStore backend.
fn open_shard(conf: &cluster_conf::ShardConf, index: usize) -> Arc<dyn ObjectStore> {
    match conf {
        cluster_conf::ShardConf::Raw { path, .. } => {
            Arc::new(
                RawObjectStore::open(StdPath::new(path))
                    .unwrap_or_else(|e| panic!("failed to open raw shard {}: {}: {}", index, path, e)),
            )
        }
        cluster_conf::ShardConf::Fs { root, .. } => {
            Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(root)
                    .unwrap_or_else(|e| panic!("failed to open fs shard {}: {}: {}", index, root, e)),
            )
        }
        #[cfg(feature = "s3-backend")]
        cluster_conf::ShardConf::S3 { endpoint, bucket, region, access_key, secret_key, path_style } => {
            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_bucket_name(bucket)
                .with_region(region.as_deref().unwrap_or("us-east-1"));
            if let Some(k) = access_key {
                builder = builder.with_access_key_id(k);
            }
            if let Some(k) = secret_key {
                builder = builder.with_secret_access_key(k);
            }
            if access_key.is_none() && secret_key.is_none() {
                builder = builder.with_skip_signature(true);
            }
            if *path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
            Arc::new(builder.build()
                .unwrap_or_else(|e| panic!("failed to open s3 shard {}: {}", index, e)))
        }
        #[cfg(not(feature = "s3-backend"))]
        cluster_conf::ShardConf::S3 { .. } => {
            eprintln!("ERROR: shard {} is type s3 but this binary was built without s3-backend feature", index);
            std::process::exit(1);
        }
        cluster_conf::ShardConf::Mem => {
            Arc::new(object_store::memory::InMemory::new())
        }
        cluster_conf::ShardConf::Node(name) => {
            panic!("shard {}: Node('{}') cannot be opened as a standalone shard -- use tree config", index, name);
        }
    }
}

/// Open a cluster with optional catalog persistence.
fn open_cluster_with_catalog(
    confs: &[cluster_conf::ShardConf],
    replicas: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
    catalog: Option<&str>,
    tree: Option<&cluster_conf::TreeConf>,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    open_cluster_with_catalog_and_tree(confs, replicas, min_writes, delete_requires_min_writes, catalog, tree)
}

/// Open a tree node recursively, building nested ShardedObjectStore instances
/// for any Node shard references.
fn open_tree_node(
    node: &cluster_conf::TreeNode,
) -> (Arc<ShardedObjectStore>, Vec<Arc<RawObjectStore>>) {
    let mut stores: Vec<Arc<dyn ObjectStore>> = Vec::new();
    let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();

    for (i, conf) in node.shards.iter().enumerate() {
        if let cluster_conf::ShardConf::Node(child_name) = conf {
            // Find the child and recursively build it.
            let child = node.children.iter()
                .find(|c| &c.name == child_name)
                .unwrap_or_else(|| panic!("child node '{}' not found", child_name));
            let (child_store, child_raws) = open_tree_node(child);
            stores.push(child_store);
            raw_stores.extend(child_raws);
        } else if let cluster_conf::ShardConf::Raw { path, .. } = conf {
            let raw = Arc::new(
                RawObjectStore::open(StdPath::new(path))
                    .unwrap_or_else(|e| panic!("failed to open raw shard {}: {}: {}", i, path, e)),
            );
            stores.push(raw.clone());
            raw_stores.push(raw);
        } else {
            stores.push(open_shard(conf, i));
        }
    }

    let mut cluster = ShardedObjectStore::new(stores, node.replication_factor);
    if let Some(mw) = node.min_writes {
        cluster = cluster.with_min_writes(mw);
    }
    let cluster = Arc::new(cluster);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let count = rt.block_on(cluster.rebuild_catalog()).expect("rebuild_catalog failed");
    if count > 0 {
        eprintln!("Node '{}': catalog rebuilt: {} objects across {} shards",
            node.name, count, node.shards.len());
    }

    (cluster, raw_stores)
}

/// Open a cluster with optional catalog persistence, with optional tree conf
/// for resolving Node shard references.
fn open_cluster_with_catalog_and_tree(
    confs: &[cluster_conf::ShardConf],
    replicas: usize,
    min_writes: Option<usize>,
    delete_requires_min_writes: bool,
    catalog: Option<&str>,
    tree: Option<&cluster_conf::TreeConf>,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let mut stores: Vec<Arc<dyn ObjectStore>> = Vec::new();
    let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();

    for (i, conf) in confs.iter().enumerate() {
        if let cluster_conf::ShardConf::Node(child_name) = conf {
            // Find the child node in the tree and recursively build it.
            let tree = tree.unwrap_or_else(|| {
                eprintln!("ERROR: shard {} references node '{}' but no tree config loaded", i, child_name);
                std::process::exit(1);
            });
            let target = tree.find_node(child_name)
                .unwrap_or_else(|| {
                    eprintln!("ERROR: child node '{}' not found in tree config", child_name);
                    std::process::exit(1);
                });
            let (child_store, child_raws) = open_tree_node(target);
            stores.push(child_store);
            raw_stores.extend(child_raws);
        } else if let cluster_conf::ShardConf::Raw { path, .. } = conf {
            let raw = Arc::new(
                RawObjectStore::open(StdPath::new(path))
                    .unwrap_or_else(|e| panic!("failed to open raw shard {}: {}: {}", i, path, e)),
            );
            stores.push(raw.clone());
            raw_stores.push(raw);
        } else {
            stores.push(open_shard(conf, i));
        }
    }

    let shard_count = confs.len();
    let mut cluster = ShardedObjectStore::new(stores, replicas);
    if let Some(mw) = min_writes {
        cluster = cluster.with_min_writes(mw);
    }
    if delete_requires_min_writes {
        cluster = cluster.with_delete_requires_min_writes(true);
    }

    if let Some(cat) = catalog {
        if let Some(persistence) = parse_catalog_persistence(cat) {
            cluster.set_persistence(persistence);
        }
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    // If a catalog path is configured, try loading from file first.
    // Otherwise just rebuild from shards.
    let has_catalog_path = catalog.is_some() && parse_catalog_persistence(catalog.unwrap()).is_some();
    if has_catalog_path {
        if let Err(_) = cluster.load_catalog() {
            // File doesn't exist yet or is corrupt -- rebuild instead
            let count = rt.block_on(cluster.rebuild_catalog()).expect("rebuild_catalog failed");
            if count > 0 {
                eprintln!("Catalog rebuilt: {} objects found across {} shards", count, shard_count);
            }
        } else if cluster.catalog().len() > 0 {
            eprintln!("Catalog loaded: {} objects", cluster.catalog().len());
        }
    } else {
        let count = rt.block_on(cluster.rebuild_catalog()).expect("rebuild_catalog failed");
        if count > 0 {
            eprintln!("Catalog rebuilt: {} objects found across {} shards", count, shard_count);
        }
    }

    (cluster, raw_stores)
}

/// Build a `RawRefRegistry` from the CLI's raw stores (all shards are Raw).
fn build_raw_ref_registry(raw_stores: &[Arc<RawObjectStore>]) -> RawRefRegistry {
    let refs: Vec<Option<Arc<RawObjectStore>>> = raw_stores
        .iter()
        .map(|s| Some(s.clone()))
        .collect();
    let kinds = vec![ShardKind::Raw; raw_stores.len()];
    RawRefRegistry::new(refs, kinds)
}

/// Flush every raw shard's index to disk (non-raw shards are skipped).
fn flush_all(raw_stores: &[Arc<RawObjectStore>]) {
    for (i, store) in raw_stores.iter().enumerate() {
        store
            .flush_index()
            .unwrap_or_else(|e| panic!("flush shard {} failed: {}", i, e));
    }
}

// -- check-config ----------------------------------------------------

fn cmd_check_config(args: &[String]) {
    let mut conf_path: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--config requires a value");
                    std::process::exit(1);
                }
                conf_path = Some(args[i].clone());
            }
            other => {
                eprintln!("unknown arg for check-config: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    let path = conf_path.unwrap_or_else(|| {
        eprintln!("check-config requires --config <path>");
        std::process::exit(1);
    });

    eprintln!("Checking config: {path}");

    // Read the file and auto-detect format.
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("cannot read '{}': {e}", path);
        std::process::exit(1);
    });

    let is_tree = cluster_conf::is_tree_config(&text);
    if is_tree {
        eprintln!("  format: tree config");
    } else {
        eprintln!("  format: flat config");
    }

    // Parse the config (auto-detect handles both).
    let tree = match cluster_conf::load_auto_conf(StdPath::new(&path)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("PARSE ERROR: {e}");
            std::process::exit(1);
        }
    };

    // For flat configs we still have the richer validate_cluster_conf.
    let diags = if !is_tree {
        match cluster_conf::load_cluster_conf(StdPath::new(&path)) {
            Ok(flat) => cluster_conf::validate_cluster_conf(&flat),
            Err(_) => Vec::new(), // already parsed above, won't fail
        }
    } else {
        // Basic tree-config diagnostics.
        let mut diags = Vec::new();
        fn walk_tree(node: &cluster_conf::TreeNode, depth: usize, diags: &mut Vec<cluster_conf::ClusterDiag>) {
            let indent = "  ".repeat(depth);
            let shard_count = node.shards.len();
            let child_count = node.children.len();
            if shard_count > 0 && node.replication_factor > shard_count {
                diags.push(cluster_conf::ClusterDiag {
                    level: cluster_conf::ClusterDiagLevel::Warning,
                    message: format!(
                        "{}node '{}': rf={} exceeds shard count ({}) -- will be clamped",
                        indent, node.name, node.replication_factor, shard_count,
                    ),
                });
            }
            if shard_count == 0 && child_count == 0 {
                diags.push(cluster_conf::ClusterDiag {
                    level: cluster_conf::ClusterDiagLevel::Warning,
                    message: format!(
                        "{}node '{}': no shards and no children",
                        indent, node.name,
                    ),
                });
            }
            diags.push(cluster_conf::ClusterDiag {
                level: cluster_conf::ClusterDiagLevel::Info,
                message: format!(
                    "{}node '{}': rf={}, {} shard(s), {} child(ren)",
                    indent, node.name, node.replication_factor, shard_count, child_count,
                ),
            });
            for child in &node.children {
                walk_tree(child, depth + 1, diags);
            }
        }
        walk_tree(&tree.root, 0, &mut diags);
        diags
    };
    let mut has_error = false;
    for d in &diags {
        let prefix = match d.level {
            cluster_conf::ClusterDiagLevel::Error => { has_error = true; "  ERROR" },
            cluster_conf::ClusterDiagLevel::Warning => "  WARN ",
            cluster_conf::ClusterDiagLevel::Info => "  INFO ",
        };
        eprintln!("{prefix}  {}", d.message);
    }
    if has_error {
        eprintln!("\nConfig has errors.");
        std::process::exit(1);
    } else {
        eprintln!("\nConfig OK.");
        std::process::exit(0);
    }
}

// -- format ----------------------------------------------------------

fn cmd_format(args: &[String]) {
    let cli = parse_args(args);
    let size = cli.size.expect("--size required for format");
    let raw_paths = cli.raw_shard_paths();
    if raw_paths.is_empty() {
        eprintln!("format requires raw shards (--shards or raw entries in config)");
        std::process::exit(1);
    }

    println!(
        "Formatting {} raw shards ({} MB each, replicas={})...",
        raw_paths.len(),
        size / (1024 * 1024),
        cli.replicas,
    );

    for (i, path) in raw_paths.iter().enumerate() {
        print!("  shard {}: {} ... ", i, path);
        let store = RawObjectStore::format_with_options(
            StdPath::new(path),
            FormatOptions {
                device_size: size,
                direct_io: cli.direct_io,
                index_slot_size: 16 * 1024 * 1024,
                max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                compression: Compression::None,
            },
        )
        .unwrap_or_else(|e| panic!("format shard {} failed: {}", i, e));
        store
            .flush_index()
            .unwrap_or_else(|e| panic!("flush shard {} failed: {}", i, e));
        println!("OK");
    }

    println!("Done. {} shards formatted and ready.", raw_paths.len());
}

// -- info ------------------------------------------------------------

fn cmd_info(args: &[String]) {
    let cli = parse_args(args);
    let raw_paths = cli.raw_shard_paths();
    let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();

    for path in &raw_paths {
        raw_stores.push(Arc::new(
            RawObjectStore::open(StdPath::new(path))
                .unwrap_or_else(|e| panic!("failed to open {}: {}", path, e)),
        ));
    }

    // Cluster summary
    let total_capacity: u64 = raw_stores.iter().map(|s| s.device_info().device_size).sum();
    let total_files: usize = raw_stores.iter().map(|s| s.device_info().file_count).sum();
    let total_data: u64 = raw_stores.iter().map(|s| s.device_info().data_bytes_stored).sum();
    let total_free: u64 = raw_stores.iter().map(|s| s.device_info().free_space).sum();

    println!("Cluster: {} shards", raw_stores.len());
    println!(
        "  Total capacity:    {:.1} MB",
        total_capacity as f64 / 1_048_576.0
    );
    println!("  Total files:       {} (sum across shards, includes replicas)", total_files);
    println!(
        "  Total data stored: {:.1} MB",
        total_data as f64 / 1_048_576.0
    );
    println!(
        "  Total free space:  {:.1} MB",
        total_free as f64 / 1_048_576.0
    );
    println!();

    // Per-shard details
    for (i, store) in raw_stores.iter().enumerate() {
        let info = store.device_info();
        println!("Shard {}:  {}", i, info.device_path);
        println!(
            "  Size:       {:.1} MB   Files: {}   Data: {:.1} MB   Free: {:.1} MB ({} fragments)",
            info.device_size as f64 / 1_048_576.0,
            info.file_count,
            info.data_bytes_stored as f64 / 1_048_576.0,
            info.free_space as f64 / 1_048_576.0,
            info.free_fragments,
        );
        println!(
            "  Txn: {}   O_DIRECT: {}   Index: {:.1}% of {} MB slot",
            info.txn_id,
            info.direct_io,
            info.index_serialized_bytes as f64 / info.index_slot_capacity as f64 * 100.0,
            info.index_slot_capacity / (1024 * 1024),
        );
    }
}

// -- list ------------------------------------------------------------

fn cmd_list(args: &[String]) {
    let cli = parse_args(args);
    let (cluster, _raw) = open_cluster_with_catalog(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.catalog.as_deref(), cli.tree_conf.as_ref());

    let prefix = cli.prefix.as_deref().map(Path::from);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let files: Vec<_> = cluster.list(prefix.as_ref()).try_collect().await.unwrap();
        let mut sorted: Vec<_> = files.iter().collect();
        sorted.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

        if cli.long {
            for f in &sorted {
                // Show which shards hold this file
                let placement = cluster
                    .placement(f.location.as_ref())
                    .map(|e| {
                        e.shards
                            .iter()
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_else(|| "?".to_string());
                println!("{:>10}  [shards: {}]  {}", f.size, placement, f.location);
            }
        } else {
            for f in &sorted {
                println!("{}", f.location);
            }
        }
        println!("\n{} objects", sorted.len());
    });
}

// -- get -------------------------------------------------------------

fn cmd_get(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().expect("--key required for get");
    let (cluster, _raw) = open_cluster_with_catalog(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.catalog.as_deref(), cli.tree_conf.as_ref());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        let result = cluster
            .get(&Path::from(key))
            .await
            .unwrap_or_else(|e| panic!("get failed: {}", e));
        result
            .bytes()
            .await
            .unwrap_or_else(|e| panic!("read bytes failed: {}", e))
    });

    // Show which shard it came from
    if let Some(entry) = cluster.placement(key) {
        eprintln!(
            "Read {} bytes from shards [{}]",
            data.len(),
            entry
                .shards
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    if let Some(to_path) = &cli.to {
        std::fs::write(to_path, &data).expect("failed to write output file");
        eprintln!("{} bytes -> {}", data.len(), to_path);
    } else {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        out.write_all(&data).expect("write to stdout failed");
    }
}

// -- put -------------------------------------------------------------

fn cmd_put(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().expect("--key required for put");

    let (data, source_label): (Vec<u8>, String) = if let Some(from_path) = cli.from.as_deref() {
        let d = std::fs::read(from_path).expect("failed to read input file");
        (d, from_path.to_string())
    } else {
        use std::io::Read;
        let mut buf = Vec::new();
        std::io::stdin().lock().read_to_end(&mut buf).expect("failed to read stdin");
        (buf, "stdin".to_string())
    };
    let len = data.len();

    let (cluster, raw_stores) = open_cluster_with_catalog(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.catalog.as_deref(), cli.tree_conf.as_ref());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(&Path::from(key), PutPayload::from(Bytes::from(data)))
            .await
            .unwrap_or_else(|e| panic!("put failed: {}", e));
    });
    flush_all(&raw_stores);

    // Save catalog if persistence is configured
    cluster.save_catalog().expect("save catalog failed");

    // Show placement
    if let Some(entry) = cluster.placement(key) {
        eprintln!(
            "{} bytes <- {} -> {} (shards [{}])",
            len,
            source_label,
            key,
            entry
                .shards
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

// -- delete ----------------------------------------------------------

fn cmd_delete(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().expect("--key required for delete");

    let (cluster, raw_stores) = open_cluster_with_catalog(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.catalog.as_deref(), cli.tree_conf.as_ref());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .delete(&Path::from(key))
            .await
            .unwrap_or_else(|e| panic!("delete failed: {}", e));
    });
    flush_all(&raw_stores);

    // Save catalog if persistence is configured
    cluster.save_catalog().expect("save catalog failed");
    eprintln!("deleted {} from all shards", key);
}

// -- verify ----------------------------------------------------------

// NOTE: Per-shard verification (verify_all) is specific to RawObjectStore and
// checks block-level integrity (CRC, extent overlaps, free-list consistency).
// The cross-shard CRC consistency check below works through the generic
// ObjectStore trait and would apply to any backend.  When the CLI gains
// support for mixed store types (S3, filesystem), per-shard checks should
// be skipped for non-raw backends with an informational message.

fn cmd_verify(args: &[String]) {
    let cli = parse_args(args);
    let raw_paths = cli.raw_shard_paths();
    let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();

    for path in &raw_paths {
        raw_stores.push(Arc::new(
            RawObjectStore::open(StdPath::new(path))
                .unwrap_or_else(|e| panic!("failed to open {}: {}", path, e)),
        ));
    }

    let mut all_clean = true;

    // Per-shard verification
    for (i, store) in raw_stores.iter().enumerate() {
        let report = store.verify_all();
        let clean = report.errors.is_empty()
            && report.overlapping_extents.is_empty()
            && report.free_list_consistent
            && report.space_accounted;

        println!(
            "Shard {}:  {} files checked, {} OK, {} errors  {}",
            i,
            report.files_checked,
            report.files_ok,
            report.errors.len(),
            if clean { "CLEAN" } else { "ISSUES" },
        );

        if !report.errors.is_empty() {
            for e in &report.errors {
                let status_str = match &e.status {
                    VerifyStatus::Ok => "ok".to_string(),
                    VerifyStatus::CrcMismatch { expected, actual } => {
                        format!("CRC mismatch: expected {:#010x}, got {:#010x}", expected, actual)
                    }
                    VerifyStatus::BlockCorrupt(msg) => format!("block corrupt: {}", msg),
                    VerifyStatus::OutOfBounds => "extent out of bounds".to_string(),
                    VerifyStatus::ReadError(msg) => format!("read error: {}", msg),
                };
                println!("    {} (0x{:x}, {} bytes): {}", e.path, e.offset, e.expected_size, status_str);
            }
            all_clean = false;
        }
        if !report.overlapping_extents.is_empty() {
            println!("    Overlapping extents:");
            for (a, b) in &report.overlapping_extents {
                println!("      {} <-> {}", a, b);
            }
            all_clean = false;
        }
    }

    // Cross-shard CRC consistency check
    println!();
    println!("Cross-shard consistency check...");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mismatches = rt.block_on(check_cross_shard_crcs(&raw_stores));
    if mismatches.is_empty() {
        println!("  All replicas consistent.");
    } else {
        println!("  {} mismatches found:", mismatches.len());
        for (path, details) in &mismatches {
            println!("    {}: {}", path, details);
        }
        all_clean = false;
    }

    println!();
    if all_clean {
        println!("Cluster is clean.");
    } else {
        println!("Cluster has issues.");
        std::process::exit(1);
    }
}

// -- health ----------------------------------------------------------

fn cmd_health(args: &[String]) {
    let cli = parse_args(args);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let timeout = std::time::Duration::from_secs(5);
    let mut all_healthy = true;

    println!("Probing {} shards...", cli.shard_confs.len());
    println!();

    for (i, conf) in cli.shard_confs.iter().enumerate() {
        match conf {
            cluster_conf::ShardConf::Raw { path, .. } => {
                let store = match RawObjectStore::open(StdPath::new(path)) {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        println!("Shard {}: FAILED  raw:{}  -- {}", i, path, e);
                        all_healthy = false;
                        continue;
                    }
                };
                let dyn_store: Arc<dyn ObjectStore> = store.clone();
                let reachable = rt.block_on(
                    shardedobjstr::repair::probe_store(&dyn_store, timeout),
                );
                let info = store.device_info();
                if reachable {
                    let usage_pct = if info.device_size > 0 {
                        (info.data_bytes_stored as f64 / info.device_size as f64) * 100.0
                    } else {
                        0.0
                    };
                    println!(
                        "Shard {}: HEALTHY  raw:{}  ({} files, {:.1}% used, {:.1} MB free)",
                        i, info.device_path, info.file_count, usage_pct,
                        info.free_space as f64 / 1_048_576.0,
                    );
                } else {
                    println!("Shard {}: UNREACHABLE  raw:{}", i, path);
                    all_healthy = false;
                }
            }
            other => {
                let label = match other {
                    cluster_conf::ShardConf::Fs { root, .. } => format!("fs:{}", root),
                    cluster_conf::ShardConf::S3 { endpoint, bucket, .. } => format!("s3:{}/{}", endpoint, bucket),
                    cluster_conf::ShardConf::Mem => "mem".to_string(),
                    cluster_conf::ShardConf::Node(name) => format!("node:{}", name),
                    _ => unreachable!(),
                };
                if let cluster_conf::ShardConf::Node(child_name) = other {
                    // Resolve node via tree config.
                    if let Some(ref tc) = cli.tree_conf {
                        if let Some(child_node) = tc.find_node(child_name) {
                            let (child_store, _raws) = open_tree_node(child_node);
                            let reachable = rt.block_on(
                                shardedobjstr::repair::probe_store(
                                    &(child_store as Arc<dyn ObjectStore>),
                                    timeout,
                                ),
                            );
                            if reachable {
                                println!("Shard {}: HEALTHY  {}", i, label);
                            } else {
                                println!("Shard {}: UNREACHABLE  {}", i, label);
                                all_healthy = false;
                            }
                        } else {
                            println!("Shard {}: ERROR  node '{}' not found in tree", i, child_name);
                            all_healthy = false;
                        }
                    } else {
                        println!("Shard {}: ERROR  node '{}' requires tree config", i, child_name);
                        all_healthy = false;
                    }
                } else {
                    let store = open_shard(other, i);
                    let reachable = rt.block_on(
                        shardedobjstr::repair::probe_store(&store, timeout),
                    );
                    if reachable {
                        println!("Shard {}: HEALTHY  {}", i, label);
                    } else {
                        println!("Shard {}: UNREACHABLE  {}", i, label);
                        all_healthy = false;
                    }
                }
            }
        }
    }

    println!();
    if all_healthy {
        println!("All {} shards healthy.", cli.shard_confs.len());
    } else {
        println!("Some shards have issues.");
        std::process::exit(1);
    }
}

/// Compare CRC32c of every object that exists on multiple shards.
async fn check_cross_shard_crcs(
    raw_stores: &[Arc<RawObjectStore>],
) -> Vec<(String, String)> {
    // Build a map: path -> vec of (shard_id, size)
    let mut file_map: std::collections::HashMap<String, Vec<(usize, u64)>> =
        std::collections::HashMap::new();

    for (shard_id, store) in raw_stores.iter().enumerate() {
        let files: Vec<_> = store.list(None).try_collect().await.unwrap_or_default();
        for f in files {
            file_map
                .entry(f.location.to_string())
                .or_default()
                .push((shard_id, f.size as u64));
        }
    }

    let mut mismatches = Vec::new();

    for (path, shards) in &file_map {
        if shards.len() < 2 {
            continue;
        }

        // Check sizes first (fast)
        let first_size = shards[0].1;
        for &(shard_id, size) in &shards[1..] {
            if size != first_size {
                mismatches.push((
                    path.clone(),
                    format!(
                        "size mismatch: shard {} has {} bytes, shard {} has {} bytes",
                        shards[0].0, first_size, shard_id, size
                    ),
                ));
            }
        }

        // Full CRC check: read from each shard and compare
        let mut crcs: Vec<(usize, u32)> = Vec::new();
        for &(shard_id, _) in shards {
            match raw_stores[shard_id].get(&Path::from(path.as_str())).await {
                Ok(result) => match result.bytes().await {
                    Ok(data) => {
                        crcs.push((shard_id, crc32c::crc32c(&data)));
                    }
                    Err(e) => {
                        mismatches.push((
                            path.clone(),
                            format!("shard {} read error: {}", shard_id, e),
                        ));
                    }
                },
                Err(e) => {
                    mismatches.push((
                        path.clone(),
                        format!("shard {} get error: {}", shard_id, e),
                    ));
                }
            }
        }

        if crcs.len() >= 2 {
            let first_crc = crcs[0].1;
            for &(shard_id, crc) in &crcs[1..] {
                if crc != first_crc {
                    mismatches.push((
                        path.clone(),
                        format!(
                            "CRC mismatch: shard {} = {:#010x}, shard {} = {:#010x}",
                            crcs[0].0, first_crc, shard_id, crc
                        ),
                    ));
                }
            }
        }
    }

    mismatches
}

// -- cross-verify (MD5-based cross-shard verification) ---------------

fn cmd_cross_verify(args: &[String]) {
    let cli = parse_args(args);

    if cli.replicas < 2 {
        eprintln!("ERROR: cross-verify requires --replicas >= 2 (got {})", cli.replicas);
        std::process::exit(1);
    }

    let (cluster, _raw_stores) = open_cluster(
        &cli.shard_confs,
        cli.replicas,
        cli.min_writes,
        cli.delete_requires_min_writes,
        cli.tree_conf.as_ref(),
    );

    let rt = tokio::runtime::Runtime::new().unwrap();

    if let Some(ref key) = cli.key {
        // Single-object mode.
        let report = rt.block_on(cluster.cross_verify_object(key))
            .unwrap_or_else(|e| {
                eprintln!("ERROR: {}", e);
                std::process::exit(1);
            });
        print_cross_verify_report(&report);
        if !report.consistent {
            std::process::exit(1);
        }
    } else {
        // All-objects mode.
        let report = rt.block_on(cluster.cross_verify_all(cli.prefix.as_deref(), None));

        println!("Cross-shard MD5 verification");
        println!("============================");
        println!("  Objects checked:                {}", report.objects_checked);
        println!("  Objects OK:                     {}", report.objects_ok);
        println!("  Objects with MD5 mismatch:      {}", report.objects_mismatched);
        println!("  Objects with read errors:        {}", report.objects_with_errors);
        println!("  Objects skipped (single replica): {}", report.objects_skipped_single_replica);
        println!();

        if !report.details.is_empty() {
            println!("Details:");
            println!();
            for obj in &report.details {
                print_cross_verify_report(obj);
                println!();
            }
        }

        if report.objects_mismatched > 0 || report.objects_with_errors > 0 {
            println!("RESULT: MISMATCHES FOUND");
            std::process::exit(1);
        } else {
            println!("RESULT: ALL REPLICAS CONSISTENT");
        }
    }
}

fn print_cross_verify_report(r: &shardedobjstr::CrossVerifyReport) {
    let status = if r.consistent { "OK" } else { "MISMATCH" };
    println!("  {} [{}]", r.key, status);
    if let Some(crc) = r.catalog_crc {
        println!("    catalog CRC32c: {:#010x}", crc);
    }
    for d in &r.shards {
        println!(
            "    shard {:>2}:  md5={}  size={:>10}  modified={}",
            d.shard_id, d.md5_hex, d.size, d.last_modified.to_rfc3339(),
        );
    }
    for (sid, err) in &r.errors {
        println!("    shard {:>2}:  ERROR: {}", sid, err);
    }
}

// -- add-shard -------------------------------------------------------

fn cmd_add_shard(args: &[String]) {
    let cli = parse_args(args);
    let new_path = cli.new_shard.as_deref().expect("--new-shard required for add-shard");
    let size = cli.size.expect("--size required for add-shard");

    // 1. Format the new shard
    println!("Formatting new shard: {} ({} MB)...", new_path, size / (1024 * 1024));
    let new_store = RawObjectStore::format_with_options(
        StdPath::new(new_path),
        FormatOptions {
            device_size: size,
            direct_io: cli.direct_io,
            index_slot_size: 16 * 1024 * 1024,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        },
    )
    .unwrap_or_else(|e| panic!("format new shard failed: {}", e));
    new_store.flush_index().unwrap();
    drop(new_store); // release file lock before reopening below
    println!("  Formatted OK");

    // 2. Open the expanded cluster (existing shards + new one)
    let mut all_confs = cli.shard_confs.clone();
    all_confs.push(cluster_conf::ShardConf::Raw {
        path: new_path.to_string(),
        read_only: false,
        compression: None,
        direct_io: None,
        size_mb: None,
    });
    let (_cluster, raw_stores) = open_cluster(&all_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.tree_conf.as_ref());

    println!("\nCluster expanded: {} -> {} shards", cli.shard_confs.len(), all_confs.len());

    // Show per-shard summary
    println!();
    for (i, store) in raw_stores.iter().enumerate() {
        let info = store.device_info();
        let tag = if i == raw_stores.len() - 1 { " (NEW)" } else { "" };
        println!(
            "  Shard {}{}: {} -- {:.1} MB, {} files, {:.1} MB free",
            i,
            tag,
            info.device_path,
            info.device_size as f64 / 1_048_576.0,
            info.file_count,
            info.free_space as f64 / 1_048_576.0,
        );
    }
    println!("\nNew objects will be hashed across all {} shards.", all_confs.len());
    println!("Existing objects stay where they are (run repair-replication to fix RF).");
}

// -- remove-shard ----------------------------------------------------

fn cmd_remove_shard(args: &[String]) {
    let cli = parse_args(args);
    let victim_path = cli.remove.as_deref().expect("--remove required for remove-shard");

    // Find the victim shard index (matched by raw path)
    let victim_idx = cli.shard_confs.iter().position(|c| match c {
        cluster_conf::ShardConf::Raw { path, .. } => path == victim_path,
        _ => false,
    });
    let victim_idx = match victim_idx {
        Some(idx) => idx,
        None => {
            eprintln!("Error: '{}' is not a raw shard in the cluster", victim_path);
            std::process::exit(1);
        }
    };

    // Guard: must keep at least 3 shards after removal
    let remaining = cli.shard_confs.len() - 1;
    if remaining < 3 {
        eprintln!(
            "Error: cannot remove shard -- only {} shards would remain (minimum 3 required).",
            remaining
        );
        std::process::exit(1);
    }

    // Guard: remaining shards must be >= replication factor
    if remaining < cli.replicas {
        eprintln!(
            "Error: cannot remove shard -- {} remaining shards < replication factor {}.",
            remaining, cli.replicas
        );
        std::process::exit(1);
    }

    // Open the full cluster (including victim) -- this locks all shard files.
    let (cluster, raw_stores) = open_cluster(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.tree_conf.as_ref());

    // Build the survivor cluster from already-open stores (no re-locking).
    let survivor_stores: Vec<Arc<dyn ObjectStore>> = (0..cli.shard_confs.len())
        .filter(|&i| i != victim_idx)
        .map(|i| cluster.shard_store(i).expect("shard store"))
        .collect();
    let survivor_cluster = ShardedObjectStore::new(survivor_stores, cli.replicas);
    let rt_surv = tokio::runtime::Runtime::new().unwrap();
    let _ = rt_surv.block_on(survivor_cluster.rebuild_catalog());

    // Collect survivor raw stores for flushing (skip the victim).
    let survivor_raw: Vec<Arc<RawObjectStore>> = raw_stores
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != victim_idx)
        .map(|(_, s)| s.clone())
        .collect();

    // List all objects on the victim shard
    let victim_store = cluster.shard_store(victim_idx)
        .expect("victim shard store not found");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let victim_files: Vec<object_store::ObjectMeta> = victim_store
            .list(None)
            .try_collect()
            .await
            .unwrap_or_default();

        if victim_files.is_empty() {
            println!("Shard {} ({}) has no objects -- nothing to drain.", victim_idx, victim_path);
        } else {
            println!(
                "Draining {} objects from shard {} ({}) to surviving shards...",
                victim_files.len(),
                victim_idx,
                victim_path,
            );

            // Build a RawRefRegistry so drain preserves TLV metadata.
            let registry = build_raw_ref_registry(&raw_stores);

            let report = shardedobjstr::repair::drain_shard(
                &cluster,
                &survivor_cluster,
                victim_idx,
                &victim_store,
                Some(&registry),
                None,
            )
            .await;

            println!(
                "\nDrain complete: {} moved, {} skipped (already replicated), {} deleted, {} errors",
                report.moved, report.skipped, report.deleted, report.errors
            );
            if report.re_replicated > 0 || report.under_remaining > 0 {
                println!(
                    "Repair sweep: {} re-replicated, {} still under-replicated",
                    report.re_replicated, report.under_remaining
                );
            }

            if report.errors > 0 {
                eprintln!("Warning: {} objects could not be drained!", report.errors);
                eprintln!("The victim shard still has data. Fix errors and retry.");
                std::process::exit(1);
            }
            if report.delete_errors > 0 {
                eprintln!("Warning: {} objects could not be deleted from victim shard!", report.delete_errors);
            }
        }
    });

    // Flush survivors
    flush_all(&survivor_raw);

    let surviving_count = cli.shard_confs.len() - 1;
    println!("\nShard {} ({}) drained successfully.", victim_idx, victim_path);
    println!("Cluster shrunk: {} -> {} shards", cli.shard_confs.len(), surviving_count);
    println!("\nYou can now safely delete the removed shard file: {}", victim_path);
}

// -- repair-replication ----------------------------------------------

fn cmd_repair_replication(args: &[String]) {
    let cli = parse_args(args);
    let (cluster, raw_stores) = open_cluster_with_catalog(
        &cli.shard_confs,
        cli.replicas,
        cli.min_writes,
        cli.delete_requires_min_writes,
        cli.catalog.as_deref(),
        cli.tree_conf.as_ref(),
    );

    let batch_size = cli.batch_size.unwrap_or(100);

    println!(
        "Repair-replication: {} shards, rf={}, batch_size={}",
        cluster.shard_count(),
        cluster.replication_factor(),
        batch_size,
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    let registry = build_raw_ref_registry(&raw_stores);
    let result = rt.block_on(async {
        shardedobjstr::repair::repair_replication_sweep(&cluster, batch_size, Some(&registry), None).await
    });

    flush_all(&raw_stores);

    println!("Re-replicated:     {}", result.re_replicated);
    println!("Trimmed:           {}", result.trimmed);
    println!("Under-replicated:  {} remaining", result.under_remaining);
    println!("Over-replicated:   {} remaining", result.over_remaining);

    if result.under_remaining > 0 || result.over_remaining > 0 {
        println!("\nNote: run repair-replication again to continue (batch_size={}).", batch_size);
    } else {
        println!("\nCluster replication is healthy.");
    }
}

// -- report ----------------------------------------------------------

fn cmd_report(args: &[String]) {
    let cli = parse_args(args);
    let (cluster, _raw) = open_cluster_with_catalog(
        &cli.shard_confs,
        cli.replicas,
        cli.min_writes,
        cli.delete_requires_min_writes,
        cli.catalog.as_deref(),
        cli.tree_conf.as_ref(),
    );

    if cli.under_replicated {
        let under = cluster.find_under_replicated();
        if under.is_empty() {
            println!("All objects meet replication factor {}", cluster.replication_factor());
        } else {
            println!(
                "{} under-replicated objects (RF = {}):\n",
                under.len(),
                cluster.replication_factor(),
            );
            println!("{:<60} {:>8}", "KEY", "REPLICAS");
            println!("{}", "-".repeat(70));
            for (key, count) in &under {
                println!("{:<60} {:>8}", key, count);
            }
            println!("\nTotal: {} under-replicated", under.len());
        }
        return;
    }

    // Full report: all objects with placement details
    let mut entries = cluster.catalog().all_entries();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    println!(
        "Cluster report: {} shards, RF = {}, {} objects\n",
        cluster.shard_count(),
        cluster.replication_factor(),
        entries.len(),
    );

    if entries.is_empty() {
        println!("(no objects)");
        return;
    }

    println!(
        "{:<60} {:>10} {:>8} {:>10}  {}",
        "KEY", "SIZE", "REPLICAS", "CRC32C", "SHARDS"
    );
    println!("{}", "-".repeat(100));

    let rf = cluster.replication_factor();
    let mut under_count = 0usize;
    for (key, entry) in &entries {
        let shards_str = entry
            .shards
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let crc_str = entry
            .crc32c
            .map(|c| format!("{:08x}", c))
            .unwrap_or_else(|| "-".to_string());
        let count = entry.shards.len();
        let marker = if count < rf { " *" } else { "" };
        if count < rf {
            under_count += 1;
        }
        println!(
            "{:<60} {:>10} {:>8} {:>10}  [{}]{}",
            key, entry.size, count, crc_str, shards_str, marker,
        );
    }

    println!("\n{} objects total", entries.len());
    if under_count > 0 {
        println!(
            "{} under-replicated (marked with *)",
            under_count,
        );
    }
}

// -- vacuum ----------------------------------------------------------

fn cmd_vacuum(args: &[String]) {
    let cli = parse_args(args);
    let (cluster, raw_stores) = open_cluster_with_catalog(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.catalog.as_deref(), cli.tree_conf.as_ref());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        match cluster.vacuum_delete_markers(None).await {
            Ok((purged, cleaned)) => {
                println!("Vacuum complete: {} markers purged, {} stale objects cleaned", purged, cleaned);
            }
            Err(e) => {
                eprintln!("Vacuum failed: {e}");
                std::process::exit(1);
            }
        }
    });

    flush_all(&raw_stores);
}

// -- list-deleted ----------------------------------------------------

fn cmd_list_deleted(args: &[String]) {
    let cli = parse_args(args);
    let (cluster, _raw_stores) = open_cluster_with_catalog(&cli.shard_confs, cli.replicas, cli.min_writes, cli.delete_requires_min_writes, cli.catalog.as_deref(), cli.tree_conf.as_ref());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let markers = cluster.list_delete_markers().await;
        if markers.is_empty() {
            println!("No delete markers found.");
            return;
        }
        println!("{:<60} {}", "KEY", "DELETED AT");
        println!("{:-<60} {:-<30}", "", "");
        for (key, ts) in &markers {
            println!("{:<60} {}", key, ts.to_rfc3339());
        }
        println!("\n{} delete marker(s) total.", markers.len());
    });
}
