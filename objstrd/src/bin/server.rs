//! objstrd -- distributed object store daemon
//!
//! S3-compatible HTTP server backed by RawObjectStore, LocalFileSystem, or
//! InMemory -- selectable at runtime via the BACKEND env var.
//!
//! All settings can be specified via CLI flags or environment variables.
//! CLI flags take priority. Run `objstrd --help` for the full list.
//!
//! Key options:
//!   --role <ROLE>          Server role: standalone, node, coordinator
//!                          [env ROLE, default standalone]
//!   --config <PATH>        Tree config file for heterogeneous cluster
//!                          [env CONFIG]
//!   --node <NAME>          This node's name in the config
//!                          [env NODE]
//!   --port <PORT>          Listen port [env PORT, default 8000]
//!   --bind <ADDR>          Bind address [env BIND, default 0.0.0.0]
//!   --bucket <NAME>        Bucket name [env BUCKET, default testbucket]
//!   --backend <TYPE>       raw|fs|mem|s3 [env BACKEND, default raw]
//!   --access-key <KEY>     SigV4 access key [env ACCESS_KEY]
//!   --secret-key <KEY>     SigV4 secret key [env SECRET_KEY]
//!   --admin-token <TOKEN>  Bearer token for /_admin/* [env ADMIN_TOKEN]
//!   --cors-origin <ORIGIN> CORS origin for /_admin/* [env ADMIN_CORS_ORIGIN]
//!   --image <PATH>         Image/device path [env IMAGE, default store.raw]
//!   --size-mb <MB>         Image size (new) [env SIZE_MB, default 256]
//!   --direct-io            Enable O_DIRECT [env DIRECT_IO=1]
//!   --compression <ALG>    none|zstd|snappy|gzip [env COMPRESSION, default none]
//!   --read-only            Open read-only (all modes) [env READ_ONLY=1]
//!   --flush-interval <S>   Flush interval [env FLUSH_INTERVAL_SECS, default 5]
//!   --event-socket <PATH>  Event socket path [env EVENT_SOCKET]
//!   --event-secret <SEC>   Event socket auth secret [env EVENT_SECRET]
//!   --max-readers <N>      Max event socket readers [env MAX_READERS, default 16]
//!   --log-file <PATH>     Append text logs to file [env LOG_FILE]
//!   --log-buffer-size <N> In-memory ring buffer size [env LOG_BUFFER_SIZE, default 10000]
//!   --recovery-enabled <bool>  Enable/disable recovery [env RECOVERY_ENABLED, default true]
//!   --recovery-poll-secs <N>   Health poll interval [env RECOVERY_POLL_SECS, default 10]
//!   --recovery-probe-timeout <N> Probe timeout [env RECOVERY_PROBE_TIMEOUT_SECS, default 5]
//!   --recovery-failure-threshold <N> Failures before detach [env RECOVERY_FAILURE_THRESHOLD, default 3]
//!   --recovery-re-replicate-batch <N> Max objects per sweep [env RECOVERY_RE_REPLICATE_BATCH_SIZE, default 100]

use std::path::Path;
use std::sync::Arc;
use objstrd::adapter::ObjectStoreS3Adapter;
use objstrd::config::{parse_config, load_tree_config, TreeShard};
use objstrd::logging::{LogBuffer, LogEntry};
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};

use rawobjstr::store::{RawObjectStore, FormatOptions};
use rawobjstr::Compression;
use object_store::ObjectStore;
use shardedobjstr::ShardedObjectStore;

use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::info;
use tracing::warn;
use tracing::debug;

/// Spawn a periodic flush task for a raw store.  Returns the JoinHandle.
fn spawn_flush_task(
    store: Arc<RawObjectStore>,
    interval_secs: u64,
    cancel_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let device = store.device_path().to_string();
    debug!(device = device.as_str(), interval_secs, "flush task started");
    let mut cancel = cancel_rx;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(interval_secs),
        );
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if store.needs_flush() {
                        match store.flush_index() {
                            Ok(()) => {
                                debug!(device = device.as_str(), "periodic flush complete");
                            }
                            Err(e) => {
                                tracing::error!(device = device.as_str(), "periodic flush failed: {e}");
                            }
                        }
                    }
                }
                _ = cancel.changed() => break,
            }
        }
        debug!(device = device.as_str(), "flush task stopped");
    })
}

/// Spawn a periodic catalog flush task.  Only writes to disk when the
/// catalog has been modified since the last save.  Returns the JoinHandle.
fn spawn_catalog_flush_task(
    cluster: Arc<ShardedObjectStore>,
    interval_secs: u64,
    cancel_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    debug!(interval_secs, "catalog flush task started");
    let mut cancel = cancel_rx;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(interval_secs),
        );
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match cluster.save_catalog_if_dirty() {
                        Ok(true) => {
                            tracing::info!(entries = cluster.catalog().len(), "periodic catalog flush");
                        }
                        Ok(false) => {} // not dirty, nothing to do
                        Err(e) => {
                            tracing::error!("periodic catalog flush failed: {e}");
                        }
                    }
                }
                _ = cancel.changed() => break,
            }
        }
        debug!("catalog flush task stopped");
    })
}

struct AllowAnonListBuckets;

#[async_trait::async_trait]
impl S3Access for AllowAnonListBuckets {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> s3s::S3Result<()> {
        match cx.credentials() {
            Some(_) => Ok(()),
            None => {
                if cx.s3_op().name() == "ListBuckets" {
                    Ok(())
                } else {
                    Err(s3s::s3_error!(AccessDenied, "Authentication required"))
                }
            }
        }
    }
}

use objstrd::viz::{VizService, OfflineVizStore, scan_bucket_names};

/// Build a `RecoveryConfig` by merging CLI/env overrides > tree config > defaults.
fn build_recovery_config(
    cli_enabled: Option<bool>,
    cli_poll_secs: Option<u64>,
    cli_probe_timeout: Option<u64>,
    cli_failure_threshold: Option<u32>,
    cli_re_replicate_batch: Option<usize>,
    cli_repair_replication_interval: Option<u64>,
    cli_repair_replication_batch: Option<usize>,
    tree: Option<&objstrd::config::TreeConfig>,
) -> objstrd::recovery::RecoveryConfig {
    let defaults = objstrd::recovery::RecoveryConfig::default();
    objstrd::recovery::RecoveryConfig {
        enabled: cli_enabled
            .or_else(|| tree.and_then(|t| t.recovery_enabled))
            .unwrap_or(defaults.enabled),
        poll_interval_secs: cli_poll_secs
            .or_else(|| tree.and_then(|t| t.recovery_poll_secs))
            .unwrap_or(defaults.poll_interval_secs)
            .max(1),
        probe_timeout_secs: cli_probe_timeout
            .or_else(|| tree.and_then(|t| t.recovery_probe_timeout_secs))
            .unwrap_or(defaults.probe_timeout_secs)
            .max(1),
        failure_threshold: cli_failure_threshold
            .or_else(|| tree.and_then(|t| t.recovery_failure_threshold))
            .unwrap_or(defaults.failure_threshold)
            .max(1),
        re_replicate_batch_size: cli_re_replicate_batch
            .or_else(|| tree.and_then(|t| t.recovery_re_replicate_batch_size))
            .unwrap_or(defaults.re_replicate_batch_size)
            .max(1)
            .min(10_000),
        repair_replication_interval_secs: cli_repair_replication_interval
            .or_else(|| tree.and_then(|t| t.repair_replication_interval_secs))
            .unwrap_or(defaults.repair_replication_interval_secs),
        repair_replication_batch_size: cli_repair_replication_batch
            .or_else(|| tree.and_then(|t| t.repair_replication_batch_size))
            .unwrap_or(defaults.repair_replication_batch_size)
            .max(1)
            .min(10_000),
    }
}

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;
    let env_filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() {
    setup_tracing();

    // Parse CLI args: --port, --bind, --bucket, --backend, --image, --size-mb,
    //   --direct-io, --compression, --read-only, --access-key, --secret-key
    // Falls back to environment variables, then defaults.
    let cli_args: Vec<String> = std::env::args().collect();
    let cli_get = |flag: &str| -> Option<String> {
        cli_args.iter().position(|a| a == flag).and_then(|i| cli_args.get(i + 1).cloned())
    };
    let cli_flag = |flag: &str| -> bool {
        cli_args.iter().any(|a| a == flag)
    };

    // Show help/version
    if cli_flag("--help") || cli_flag("-h") {
        eprintln!("objstrd - S3-compatible object store daemon");
        eprintln!();
        eprintln!("Usage: objstrd [OPTIONS]");
        eprintln!();
        eprintln!("Options (also settable via env vars):");
        eprintln!("  --port <PORT>              Listen port      [env PORT, default 8000]");
        eprintln!("  --bind <ADDR>              Bind address     [env BIND, default 0.0.0.0]");
        eprintln!("  --bucket <NAME>            Bucket name      [env BUCKET, default testbucket]");
        eprintln!("  --backend <TYPE>           raw|fs|mem|s3    [env BACKEND, default raw]");
        eprintln!("  --image <PATH>             Image/device (raw) or root dir (fs)  [env IMAGE, default store.raw]");
        eprintln!("  --size-mb <MB>             Image size (new) [env SIZE_MB, default 256]");
        eprintln!("  --direct-io                Enable O_DIRECT  [env DIRECT_IO=1]");
        eprintln!("  --compression <ALG>        none|zstd|snappy|gzip0..gzip9  [env COMPRESSION, default none]");
        eprintln!("  --read-only                Open read-only   [env READ_ONLY=1]");
        eprintln!("  --access-key <KEY>         SigV4 access key [env ACCESS_KEY]");
        eprintln!("  --secret-key <KEY>         SigV4 secret key [env SECRET_KEY]");
        eprintln!("  --admin-token <TOKEN>      Admin API token  [env ADMIN_TOKEN]");
        eprintln!("  --cors-origin <ORIGIN>     CORS origin for /_admin/*  [env ADMIN_CORS_ORIGIN]");
        eprintln!("  --flush-interval <SECS>    Flush interval   [env FLUSH_INTERVAL_SECS, default 5]");
        eprintln!("  --catalog-path <PATH>      Catalog persistence file  [env CATALOG_PATH]");
        eprintln!("  --catalog-format <FMT>     json|bincode  [env CATALOG_FORMAT, default json]");
        eprintln!("  --catalog-flush-interval <SECS> Catalog flush interval (0=disabled)  [env CATALOG_FLUSH_INTERVAL_SECS]");
        eprintln!("  --event-socket <PATH>      Event socket (all modes)  [env EVENT_SOCKET]");
        eprintln!("  --event-secret <SECRET>    Event socket auth secret  [env EVENT_SECRET]");
        eprintln!("  --max-readers <N>          Max event socket readers  [env MAX_READERS, default 16]");
        eprintln!("  --event-source <URL>       Streaming replica source (forces read-only)  [env EVENT_SOURCE]");
        eprintln!("  --read-prefer <MODE>       Read preference: ordered|round-robin  [env READ_PREFER]");
        eprintln!("  --role <ROLE>              standalone|node|coordinator  [env ROLE, default standalone]");
        eprintln!("  --log-file <PATH>          Append text logs to file  [env LOG_FILE]");
        eprintln!("  --log-buffer-size <N>      In-memory log ring buffer size  [env LOG_BUFFER_SIZE, default 10000]");
        eprintln!();
        eprintln!("S3 backend options (--backend s3):");
        eprintln!("  --s3-endpoint <URL>        S3 endpoint URL         [env S3_ENDPOINT]");
        eprintln!("  --s3-bucket <NAME>         Upstream S3 bucket      [env S3_BUCKET]");
        eprintln!("  --s3-region <REGION>       S3 region               [env S3_REGION, default us-east-1]");
        eprintln!("  --s3-access-key <KEY>      Upstream S3 access key  [env S3_ACCESS_KEY]");
        eprintln!("  --s3-secret-key <KEY>      Upstream S3 secret key  [env S3_SECRET_KEY]");
        eprintln!("  --s3-path-style            Use path-style requests [env S3_PATH_STYLE=1]");
        eprintln!();
        eprintln!("Recovery options:");
        eprintln!("  --recovery-enabled <bool>  Enable/disable auto-recovery  [env RECOVERY_ENABLED, default true]");
        eprintln!("  --recovery-poll-secs <N>   Health poll interval (secs)  [env RECOVERY_POLL_SECS, default 10]");
        eprintln!("  --recovery-probe-timeout <N>  Probe timeout (secs)  [env RECOVERY_PROBE_TIMEOUT_SECS, default 5]");
        eprintln!("  --recovery-failure-threshold <N>  Failures before detach  [env RECOVERY_FAILURE_THRESHOLD, default 3]");
        eprintln!("  --recovery-re-replicate-batch <N>  Max objects per sweep  [env RECOVERY_RE_REPLICATE_BATCH_SIZE, default 100]");
        eprintln!();
        eprintln!("Repair-replication options:");
        eprintln!("  --repair-replication-interval-secs <N>  Periodic repair-replication interval (0=off)  [env REPAIR_REPLICATION_INTERVAL_SECS, default 0]");
        eprintln!("  --repair-replication-batch-size <N>     Max objects per repair-replication sweep  [env REPAIR_REPLICATION_BATCH_SIZE, default 500]");
        eprintln!();
        eprintln!("Tree config mode:");
        eprintln!("  --config <PATH>            Cluster config file  [env CONFIG]");
        eprintln!("  --node <NAME>              This node's name     [env NODE]");
        eprintln!("  --check-config             Validate config and exit (requires --config)");
        std::process::exit(0);
    }
    if cli_flag("--version") || cli_flag("-V") {
        eprintln!("objstrd {} ({})", env!("BUILD_GIT_HASH"), env!("BUILD_DATE"));
        std::process::exit(0);
    }

    // -- Check-config mode: validate and exit -------------------------
    if cli_flag("--check-config") {
        let conf_path = cli_get("--config")
            .or_else(|| std::env::var("CONFIG").ok())
            .unwrap_or_else(|| {
                eprintln!("ERROR: --check-config requires --config <path>");
                std::process::exit(1);
            });
        let node_name = cli_get("--node")
            .or_else(|| std::env::var("NODE").ok());

        eprintln!("Checking config: {conf_path}");
        let tree = match load_tree_config(Path::new(&conf_path)) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("PARSE ERROR: {e}");
                std::process::exit(1);
            }
        };
        let diags = objstrd::config::validate_tree_config(&tree, node_name.as_deref());
        let mut has_error = false;
        for d in &diags {
            let prefix = match d.level {
                objstrd::config::DiagLevel::Error => { has_error = true; "  ERROR" },
                objstrd::config::DiagLevel::Warning => "  WARN ",
                objstrd::config::DiagLevel::Info => "  INFO ",
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

    let mut port: u16 = cli_get("--port")
        .or_else(|| std::env::var("PORT").ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(8000);

    let mut bind = cli_get("--bind")
        .or_else(|| std::env::var("BIND").ok())
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let bucket = cli_get("--bucket")
        .or_else(|| std::env::var("BUCKET").ok())
        .unwrap_or_else(|| "testbucket".to_string());
    let backend = cli_get("--backend")
        .or_else(|| std::env::var("BACKEND").ok())
        .unwrap_or_else(|| "raw".to_string());

    let access_key = cli_get("--access-key").or_else(|| std::env::var("ACCESS_KEY").ok());
    let secret_key = cli_get("--secret-key").or_else(|| std::env::var("SECRET_KEY").ok());

    let mut global_read_only = cli_flag("--read-only")
        || std::env::var("READ_ONLY")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

    let read_only = backend == "raw" && global_read_only;

    let role = cli_get("--role")
        .or_else(|| std::env::var("ROLE").ok())
        .unwrap_or_else(|| "standalone".to_string());
    match role.as_str() {
        "standalone" | "node" | "coordinator" => {}
        other => {
            eprintln!("ERROR: unknown ROLE '{other}' -- expected standalone, node, or coordinator");
            std::process::exit(1);
        }
    }

    if role == "coordinator" {
        // Coordinator mode is not yet implemented. Accept the flag so the
        // startup path is wired, but exit with a clear message.
        eprintln!("ERROR: coordinator mode is not yet implemented (Phase D).");
        eprintln!("Use --role standalone or --role node.");
        std::process::exit(1);
    }

    info!(role = %role, "server role");

    // -- Structured logging: ring buffer + optional file --
    // Peek at tree config for log_file/log_buffer_size fallback.
    let (tree_log_file, tree_log_buffer_size) = cli_get("--config")
        .or_else(|| std::env::var("CONFIG").ok())
        .and_then(|conf_path| load_tree_config(Path::new(&conf_path)).ok())
        .map(|t| (t.log_file.clone(), t.log_buffer_size))
        .unwrap_or((None, None));
    let log_buffer_size: usize = cli_get("--log-buffer-size")
        .or_else(|| std::env::var("LOG_BUFFER_SIZE").ok())
        .and_then(|v| v.parse().ok())
        .or(tree_log_buffer_size)
        .unwrap_or(10_000)
        .max(100)
        .min(1_000_000);
    let log_file_path = cli_get("--log-file")
        .or_else(|| std::env::var("LOG_FILE").ok())
        .or(tree_log_file)
        .map(std::path::PathBuf::from);
    let log_buffer = LogBuffer::new(log_buffer_size, log_file_path.clone());
    if let Some(ref p) = log_file_path {
        info!(path = %p.display(), "logging to file");
    }
    info!(buffer_size = log_buffer_size, "structured log buffer initialized");

    // Log a lifecycle entry for startup
    log_buffer.push(LogEntry::new(
        "info", "lifecycle", "internal",
        format!("objstrd starting role={} backend={}", role, backend),
    ));

    // -- Recovery config (CLI > env > tree config > defaults) --
    let cli_recovery_enabled: Option<bool> = cli_get("--recovery-enabled")
        .or_else(|| std::env::var("RECOVERY_ENABLED").ok())
        .map(|v| v == "true" || v == "1");
    let cli_recovery_poll_secs: Option<u64> = cli_get("--recovery-poll-secs")
        .or_else(|| std::env::var("RECOVERY_POLL_SECS").ok())
        .and_then(|v| v.parse().ok());
    let cli_recovery_probe_timeout: Option<u64> = cli_get("--recovery-probe-timeout")
        .or_else(|| std::env::var("RECOVERY_PROBE_TIMEOUT_SECS").ok())
        .and_then(|v| v.parse().ok());
    let cli_recovery_failure_threshold: Option<u32> = cli_get("--recovery-failure-threshold")
        .or_else(|| std::env::var("RECOVERY_FAILURE_THRESHOLD").ok())
        .and_then(|v| v.parse().ok());
    let cli_recovery_re_replicate_batch: Option<usize> = cli_get("--recovery-re-replicate-batch")
        .or_else(|| std::env::var("RECOVERY_RE_REPLICATE_BATCH_SIZE").ok())
        .and_then(|v| v.parse().ok());

    // -- Repair-replication config (CLI > env > tree config > defaults) --
    let cli_repair_replication_interval: Option<u64> = cli_get("--repair-replication-interval-secs")
        .or_else(|| std::env::var("REPAIR_REPLICATION_INTERVAL_SECS").ok())
        .and_then(|v| v.parse().ok());
    let cli_repair_replication_batch: Option<usize> = cli_get("--repair-replication-batch-size")
        .or_else(|| std::env::var("REPAIR_REPLICATION_BATCH_SIZE").ok())
        .and_then(|v| v.parse().ok());

    let cli_read_prefer: Option<String> = cli_get("--read-prefer")
        .or_else(|| std::env::var("READ_PREFER").ok());

    // -- Extract config path for reload support --
    let config_path: Option<String> = cli_get("--config")
        .or_else(|| std::env::var("CONFIG").ok());
    let node_name_opt: Option<String> = cli_get("--node")
        .or_else(|| std::env::var("NODE").ok());
    if config_path.is_some() && node_name_opt.is_none() {
        eprintln!("ERROR: --config requires --node <name>");
        std::process::exit(1);
    }

    // Initial bind/port determination from tree config (if applicable).
    if let Some(ref conf_path) = config_path {
        let tree = load_tree_config(Path::new(conf_path)).unwrap_or_else(|e| {
            eprintln!("ERROR: {e}");
            std::process::exit(1);
        });
        let node_name = node_name_opt.as_ref().unwrap();
        let my_node = tree.find_node(node_name).unwrap_or_else(|| {
            eprintln!("ERROR: node '{}' not found in config", node_name);
            std::process::exit(1);
        });
        let listen_parts: Vec<&str> = my_node.listen.splitn(2, ':').collect();
        bind = listen_parts[0].to_string();
        port = listen_parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(8000);
    }

    // Bind listener once (survives reloads so clients queue in TCP backlog)
    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind to {addr}: {e}"));
    let local_addr = listener.local_addr().unwrap();
    let http_server = ConnBuilder::new(TokioExecutor::new());

    // SIGHUP signal for config reload
    let mut sighup = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::hangup(),
    ).expect("failed to register SIGHUP handler");

    let process_start_time = std::time::Instant::now();

    'reload: loop {

    // -- Build the object store and optionally a RawObjectStore reference for flush --

    // raw stores to flush on shutdown
    let mut shutdown_raws: Vec<Arc<RawObjectStore>> = Vec::new();
    // Signal for waking the bucket-scan task immediately on notify events
    let bucket_rescan_signal = Arc::new(tokio::sync::Notify::new());
    // Cancellation channel for flush timer tasks
    let (flush_cancel_tx, flush_cancel_rx) = watch::channel(false);
    let mut flush_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    // Background tasks that must be aborted on reload (recovery, bucket rescan)
    let mut bg_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    // Reload signal for HTTP-triggered reload
    let reload_signal = Arc::new(tokio::sync::Notify::new());

    // These are produced by either the cluster-config path or the single-store path.
    let mut adapter: Option<ObjectStoreS3Adapter> = None;
    let mut viz_raw_stores: Vec<Arc<RawObjectStore>> = vec![];
    let mut viz_obj_stores: Vec<Arc<dyn ObjectStore>> = vec![];
    let mut viz_shard_names: Vec<String> = vec![];
    let mut viz_shard_kinds: Vec<ShardKind> = vec![];
    let mut viz_raw_index_map: Vec<Option<usize>> = vec![];
    let mut viz_original_stores: Vec<Option<Arc<dyn ObjectStore>>> = vec![];
    let mut viz_device_paths: Vec<Option<String>> = vec![];
    let mut viz_shard_endpoints: Vec<Option<String>> = vec![];
    let mut is_cluster_mode: bool = false;
    let viz_replication_factor: usize;
    let mut viz_cluster: Option<Arc<ShardedObjectStore>> = None;
    let mut is_read_only: bool = false;
    let mut recovery_status_handle: Option<objstrd::recovery::RecoveryStatusHandle> = None;
    let mut repair_replication_status_handle: Option<objstrd::recovery::RepairReplicationStatusHandle> = None;
    let admin_op_lock: objstrd::viz::AdminOpLock = Arc::new(tokio::sync::Mutex::new(None));

    // Keep the event server alive for the duration of the HTTP server loop.
    // Without this, the EventServer would be dropped at the end of the
    // if/else branch that created it, removing the socket file.
    #[cfg(unix)]
    let mut _event_server_keep_alive: Option<rawobjstr::event::unix::EventServer>;
    #[cfg(unix)]
    {
        _event_server_keep_alive = None;
    }
    // Log callback for event socket events (connect, disconnect, auth fail).
    #[cfg(unix)]
    let event_log_cb: Option<rawobjstr::event::unix::EventLogFn> = {
        let lb = log_buffer.clone();
        Some(Arc::new(move |level: &str, message: &str| {
            lb.push(LogEntry::new(level, "events", "internal", message.to_string()));
        }))
    };
    // Track event socket info for the admin dashboard.
    let mut viz_event_socket_path: Option<String> = None;
    let mut viz_event_socket_connections: Option<Arc<std::sync::atomic::AtomicUsize>> = None;
    // Event bus for standalone PUT/DELETE events (set inside "raw" arm, used after match).
    #[cfg(unix)]
    let mut event_bus_single: Option<Arc<rawobjstr::event::EventBus>> = None;

    // -- (Re-)parse config file ---------------------------------------
    let tree_config = if let Some(ref conf_path) = config_path {
        let node_name = node_name_opt.as_ref().unwrap();
        let tree = load_tree_config(Path::new(conf_path)).unwrap_or_else(|e| {
            eprintln!("ERROR: {e}");
            std::process::exit(1);
        });
        let my_node = tree.find_node(node_name).unwrap_or_else(|| {
            eprintln!("ERROR: node '{}' not found in config", node_name);
            std::process::exit(1);
        }).clone();
        Some((tree, my_node))
    } else {
        None
    };

    let cluster_config = if tree_config.is_none() { parse_config() } else { None };

    // Extract daemon-level overrides from tree config (if present) so
    // they can serve as fallback for CLI/env.
    let tree_event_socket = tree_config.as_ref().and_then(|(t, _)| t.event_socket.clone());
    let tree_event_secret = tree_config.as_ref().and_then(|(t, _)| t.event_secret.clone());
    let tree_max_readers = tree_config.as_ref().and_then(|(t, _)| t.max_readers);
    let tree_event_source = tree_config.as_ref().and_then(|(t, _)| t.event_source.clone());
    let tree_admin_token = tree_config.as_ref().and_then(|(t, _)| t.admin_token.clone());
    let tree_access_key = tree_config.as_ref().and_then(|(t, _)| t.access_key.clone());
    let tree_secret_key = tree_config.as_ref().and_then(|(t, _)| t.secret_key.clone());
    let tree_cors_origin = tree_config.as_ref().and_then(|(t, _)| t.cors_origin.clone());

    // Apply tree config fallback for access_key/secret_key (CLI > env > config).
    let access_key = access_key.clone().or(tree_access_key.clone());
    let secret_key = secret_key.clone().or(tree_secret_key.clone());

    // -- Event socket config (available to all modes) -----------------
    let event_socket_path = cli_get("--event-socket")
        .or_else(|| std::env::var("EVENT_SOCKET").ok())
        .or(tree_event_socket);
    let event_secret = cli_get("--event-secret")
        .or_else(|| std::env::var("EVENT_SECRET").ok())
        .or(tree_event_secret);
    let max_readers_global: usize = cli_get("--max-readers")
        .or_else(|| std::env::var("MAX_READERS").ok())
        .and_then(|v| v.parse().ok())
        .or(tree_max_readers)
        .unwrap_or(16);
    let event_source = cli_get("--event-source")
        .or_else(|| std::env::var("EVENT_SOURCE").ok())
        .or(tree_event_source);

    // A streaming replica is always read-only.
    if event_source.is_some() {
        global_read_only = true;
    }

    if let Some((tree, my_node)) = tree_config {
        // -- Tree config mode ------------------------------------------

        let flush_interval_secs: u64 = cli_get("--flush-interval")
            .or_else(|| std::env::var("FLUSH_INTERVAL_SECS").ok())
            .and_then(|v| v.parse().ok())
            .or(tree.flush_interval)
            .unwrap_or(5);

        // Catalog persistence config (tree config + env + CLI).
        let catalog_path: Option<String> = cli_get("--catalog-path")
            .or_else(|| std::env::var("CATALOG_PATH").ok())
            .or_else(|| tree.catalog_path.clone());
        let catalog_format: String = cli_get("--catalog-format")
            .or_else(|| std::env::var("CATALOG_FORMAT").ok())
            .or_else(|| tree.catalog_format.clone())
            .unwrap_or_else(|| "json".to_string());
        let catalog_flush_interval: u64 = cli_get("--catalog-flush-interval")
            .or_else(|| std::env::var("CATALOG_FLUSH_INTERVAL_SECS").ok())
            .and_then(|v| v.parse().ok())
            .or(tree.catalog_flush_interval)
            .unwrap_or(0);

        let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();
        let mut raw_refs: Vec<Option<Arc<RawObjectStore>>> = Vec::new();
        let mut shard_kinds: Vec<ShardKind> = Vec::new();
        let mut obj_stores: Vec<Option<Arc<dyn ObjectStore>>> = Vec::new();
        let mut shard_names: Vec<String> = Vec::new();
        let mut device_paths: Vec<Option<String>> = Vec::new();
        let mut shard_endpoints: Vec<Option<String>> = Vec::new();

        for (i, shard) in my_node.shards.iter().enumerate() {
            match shard {
                TreeShard::Raw { path, read_only: shard_ro, compression: shard_comp,
                                  direct_io: shard_dio, size_mb: shard_sz } => {
                    let image_path = std::path::Path::new(path);
                    let open_ro = *shard_ro || global_read_only;
                    let opened = if !image_path.exists() && !open_ro {
                        // Image does not exist -- try to format a new one.
                        let sz = shard_sz.or(tree.size_mb).unwrap_or(256);
                        let dio = shard_dio.or(tree.direct_io).unwrap_or(false);
                        let comp_str = shard_comp.as_deref()
                            .or(tree.compression.as_deref())
                            .unwrap_or("none");
                        let comp = Compression::from_str_name(comp_str)
                            .unwrap_or_else(|_| {
                                eprintln!("ERROR: shard {i} unknown compression '{comp_str}'");
                                std::process::exit(1);
                            });
                        info!(shard = i, image = %path, size_mb = sz, direct_io = dio,
                              compression = %comp, "formatting new raw shard");
                        match RawObjectStore::format_with_options(image_path, FormatOptions {
                            device_size: sz * 1024 * 1024,
                            direct_io: dio,
                            index_slot_size: rawobjstr::INDEX_REGION_SIZE,
                            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                            compression: comp,
                        }) {
                            Ok(store) => Some(Arc::new(store)),
                            Err(e) => {
                                warn!(shard = i, path = %path, error = %e, "failed to format raw shard, marking offline");
                                None
                            }
                        }
                    } else if !image_path.exists() {
                        warn!(shard = i, path = %path, "raw device does not exist, marking offline");
                        None
                    } else if open_ro {
                        info!(shard = i, image = %path, "opening raw shard (read-only)");
                        match RawObjectStore::open_readonly(image_path) {
                            Ok(store) => Some(Arc::new(store)),
                            Err(e) => {
                                warn!(shard = i, path = %path, error = %e, "failed to open raw shard, marking offline");
                                None
                            }
                        }
                    } else {
                        info!(shard = i, image = %path, "opening raw shard");
                        match RawObjectStore::open(image_path) {
                            Ok(store) => Some(Arc::new(store)),
                            Err(e) => {
                                warn!(shard = i, path = %path, error = %e, "failed to open raw shard, marking offline");
                                None
                            }
                        }
                    };
                    if let Some(ref arc) = opened {
                        obj_stores.push(Some(arc.clone() as Arc<dyn ObjectStore>));
                        raw_refs.push(Some(arc.clone()));
                        raw_stores.push(arc.clone());
                    } else {
                        obj_stores.push(None);
                        raw_refs.push(None);
                    }
                    shard_names.push(format!("raw:{}", path));
                    shard_kinds.push(ShardKind::Raw);
                    device_paths.push(Some(path.to_string()));
                    shard_endpoints.push(None);
                }
                TreeShard::Fs { root, read_only: _fs_ro } => {
                    let fs_path = std::path::Path::new(root);
                    if !fs_path.exists() {
                        warn!(shard = i, root = %root, "fs shard directory does not exist, marking offline");
                        obj_stores.push(None);
                        raw_refs.push(None);
                    } else {
                        info!(shard = i, root = %root, "opening fs shard");
                        match object_store::local::LocalFileSystem::new_with_prefix(root) {
                            Ok(store) => {
                                obj_stores.push(Some(Arc::new(store) as Arc<dyn ObjectStore>));
                                raw_refs.push(None);
                            }
                            Err(e) => {
                                warn!(shard = i, root = %root, error = %e, "failed to open fs shard, marking offline");
                                obj_stores.push(None);
                                raw_refs.push(None);
                            }
                        }
                    }
                    shard_names.push(format!("fs:{}", root));
                    shard_kinds.push(ShardKind::Sidecar);
                    device_paths.push(Some(root.to_string()));
                    shard_endpoints.push(None);
                }
                TreeShard::Mem => {
                    info!(shard = i, "opening in-memory shard");
                    obj_stores.push(Some(Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>));
                    raw_refs.push(None);
                    shard_names.push("mem".to_string());
                    shard_kinds.push(ShardKind::Sidecar);
                    device_paths.push(None);
                    shard_endpoints.push(None);
                }
                TreeShard::S3 { endpoint, bucket: s3_bucket, region, access_key: ak, secret_key: sk, path_style } => {
                    #[cfg(feature = "s3-backend")]
                    {
                        info!(shard = i, endpoint = %endpoint, bucket = %s3_bucket, "opening direct S3 shard");
                        let mut builder = object_store::aws::AmazonS3Builder::new()
                            .with_endpoint(endpoint)
                            .with_bucket_name(s3_bucket)
                            .with_region(region.as_deref().unwrap_or("us-east-1"));
                        if let Some(k) = ak {
                            builder = builder.with_access_key_id(k);
                        }
                        if let Some(k) = sk {
                            builder = builder.with_secret_access_key(k);
                        }
                        if ak.is_none() && sk.is_none() {
                            builder = builder.with_skip_signature(true);
                        }
                        if *path_style {
                            builder = builder.with_virtual_hosted_style_request(false);
                        }
                        if endpoint.starts_with("http://") {
                            builder = builder.with_allow_http(true);
                        }
                        let store = builder.build().unwrap_or_else(|e| {
                            eprintln!("ERROR: shard {i} S3 '{}': {e}", endpoint);
                            std::process::exit(1);
                        });
                        obj_stores.push(Some(Arc::new(store) as Arc<dyn ObjectStore>));
                        raw_refs.push(None);
                        shard_names.push(format!("s3:{}", s3_bucket));
                        shard_kinds.push(ShardKind::S3Like);
                        device_paths.push(None);
                        shard_endpoints.push(Some(endpoint.clone()));
                    }
                    #[cfg(not(feature = "s3-backend"))]
                    {
                        let _ = (endpoint, s3_bucket, region, ak, sk, path_style);
                        eprintln!("ERROR: shard {i} is S3 but objstrd was not built with --features s3-backend");
                        std::process::exit(1);
                    }
                }
                TreeShard::Node(name) => {
                    let child = tree.find_node(name).unwrap_or_else(|| {
                        eprintln!("ERROR: child node '{}' not found in config", name);
                        std::process::exit(1);
                    });
                    #[cfg(feature = "s3-backend")]
                    {
                        let child_bucket = &tree.default_bucket;
                        info!(shard = i, node = %name, endpoint = %child.endpoint, "opening node shard");
                        let mut builder = object_store::aws::AmazonS3Builder::new()
                            .with_endpoint(&child.endpoint)
                            .with_bucket_name(child_bucket)
                            .with_region("us-east-1")
                            .with_virtual_hosted_style_request(false);
                        if child.endpoint.starts_with("http://") {
                            builder = builder.with_allow_http(true);
                        }
                        if let Some(ref ak) = access_key {
                            builder = builder.with_access_key_id(ak);
                        }
                        if let Some(ref sk) = secret_key {
                            builder = builder.with_secret_access_key(sk);
                        }
                        if access_key.is_none() && secret_key.is_none() {
                            builder = builder.with_skip_signature(true);
                        }
                        let store = builder.build().unwrap_or_else(|e| {
                            eprintln!("ERROR: shard {i} node '{}': {e}", name);
                            std::process::exit(1);
                        });
                        obj_stores.push(Some(Arc::new(store) as Arc<dyn ObjectStore>));
                        raw_refs.push(None);
                        shard_names.push(format!("node:{}", name));
                        shard_kinds.push(ShardKind::S3Like);
                        device_paths.push(None);
                        shard_endpoints.push(Some(child.endpoint.clone()));
                    }
                    #[cfg(not(feature = "s3-backend"))]
                    {
                        let _ = child;
                        eprintln!("ERROR: node shard '{}' requires --features s3-backend", name);
                        std::process::exit(1);
                    }
                }
            }
        }

        // Log offline shard summary
        let offline_count = obj_stores.iter().filter(|s| s.is_none()).count();
        if offline_count > 0 {
            warn!(offline_count, total = obj_stores.len(), "some shards are OFFLINE -- serving in degraded mode");
        }

        // Keep a clone of original stores for the recovery task.
        let original_stores: Vec<Option<Arc<dyn ObjectStore>>> = obj_stores.iter().map(|o| o.as_ref().map(Arc::clone)).collect();
        // Keep a clone for viz (obj_stores is consumed by ShardedObjectStore::new_with_offline).
        let viz_obj_stores_vec: Vec<Option<Arc<dyn ObjectStore>>> = obj_stores.iter().map(|o| o.as_ref().map(Arc::clone)).collect();
        // Keep clones for the VizService attach endpoint.
        viz_original_stores = original_stores.clone();
        viz_device_paths = device_paths.clone();

        let mut cluster_builder =
            ShardedObjectStore::new_with_offline(obj_stores, my_node.replication_factor)
                .with_read_only(global_read_only);
        if let Some(mw) = my_node.min_writes {
            cluster_builder = cluster_builder.with_min_writes(mw);
        }
        if my_node.delete_requires_min_writes {
            cluster_builder = cluster_builder.with_delete_requires_min_writes(true);
        }
        let cluster = Arc::new(cluster_builder);

        // Apply read preference from tree config.
        if let Some(ref pref) = tree.read_prefer {
            match pref.as_str() {
                "ordered" => cluster.set_read_preference(shardedobjstr::ReadPreference::Ordered),
                "round-robin" | "round_robin" => cluster.set_read_preference(shardedobjstr::ReadPreference::RoundRobin),
                other => info!("unknown read_prefer '{}', using round-robin", other),
            }
        }

        // Catalog persistence: configure, load if file exists.
        if let Some(ref cat_path) = catalog_path {
            let persistence = match catalog_format.as_str() {
                "bincode" | "bin" => shardedobjstr::catalog::CatalogPersistence::bincode(cat_path),
                _ => shardedobjstr::catalog::CatalogPersistence::json(cat_path),
            };
            cluster.set_persistence(persistence);
            match cluster.load_catalog() {
                Ok(()) => {
                    let n = cluster.catalog().len();
                    if n > 0 {
                        info!(entries = n, path = cat_path, "loaded catalog from disk");
                    } else {
                        info!(path = cat_path, "catalog file missing or empty, will rebuild from shards");
                    }
                }
                Err(e) => {
                    warn!(error = %e, path = cat_path, "catalog file failed checksum or parse, ignoring and rebuilding from shards");
                }
            }

            // Strip offline and ephemeral (mem) shards from the catalog so
            // stale entries do not inflate replica counts.  This makes
            // find_under_replicated() report accurate numbers from the
            // very first poll cycle.
            for sid in 0..cluster.shard_count() {
                let strip = if let Some(h) = cluster.shard_health(sid) {
                    h.is_unavailable()
                } else {
                    false
                };
                // mem shards always lose data on restart, so strip
                // regardless of health.
                let is_mem = shard_names.get(sid).map(|n| n == "mem").unwrap_or(false);
                if strip || is_mem {
                    let removed = cluster.catalog().remove_all_for_shard(sid);
                    if removed > 0 {
                        let reason = if is_mem { "ephemeral mem shard" } else { "offline" };
                        info!(shard_id = sid, entries = removed, reason, "stripped shard from catalog");
                    }
                }
            }
        }

        // Build raw_index_map: maps global shard ID to raw_stores index
        // Only count shards that actually opened (raw_refs[i] is Some).
        let mut rim: Vec<Option<usize>> = Vec::new();
        let mut raw_idx = 0usize;
        for (idx, kind) in shard_kinds.iter().enumerate() {
            if matches!(kind, ShardKind::Raw) && raw_refs.get(idx).and_then(|r| r.as_ref()).is_some() {
                rim.push(Some(raw_idx));
                raw_idx += 1;
            } else {
                rim.push(None);
            }
        }

        let registry = Arc::new(RawRefRegistry::new(raw_refs, shard_kinds.clone()));

        // Periodic flush for raw shards (cancellable for reload)
        for raw in &raw_stores {
            shutdown_raws.push(Arc::clone(raw));
            flush_handles.push(spawn_flush_task(
                Arc::clone(raw),
                flush_interval_secs,
                flush_cancel_rx.clone(),
            ));
        }

        // Periodic catalog flush (cancellable for reload)
        if catalog_flush_interval > 0 && catalog_path.is_some() {
            flush_handles.push(spawn_catalog_flush_task(
                Arc::clone(&cluster),
                catalog_flush_interval,
                flush_cancel_rx.clone(),
            ));
        }

        // Event socket (tree-config mode).
        //
        // Read-write: start an EventServer that broadcasts PUT/DELETE/FLUSH.
        // Read-only:  subscribe to a writer's socket and reload_index on
        //             FLUSH.  NOTE: only flushes from RawObjectStore shards
        //             are captured -- non-raw shards (S3, fs, mem) do not
        //             emit FLUSH events, so in mixed setups not every
        //             update will trigger a reload.
        #[cfg(unix)]
        if let Some(ref sock_path) = event_socket_path {
            let es = event_secret.as_deref().unwrap_or_else(|| {
                eprintln!("ERROR: EVENT_SOCKET set but EVENT_SECRET missing");
                std::process::exit(1);
            });
            if global_read_only {
                let reload_stores: Vec<Arc<RawObjectStore>> = rim.iter()
                    .filter_map(|raw_idx_opt| {
                        raw_idx_opt.map(|ri| Arc::clone(&raw_stores[ri]))
                    })
                    .collect();
                let rescan_signal = Arc::clone(&bucket_rescan_signal);
                let subscribe_handle: tokio::task::JoinHandle<()> =
                    shardedobjstr::event::subscribe_store_events(
                        Path::new(sock_path),
                        es,
                        move |event| {
                            if matches!(event, rawobjstr::event::StoreEvent::Flush { .. }) {
                                for s in &reload_stores {
                                    if let Err(e) = s.reload_index() {
                                        tracing::error!("read-only reload_index failed: {e}");
                                    }
                                }
                                rescan_signal.notify_one();
                            }
                        },
                    )
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!("ERROR: failed to subscribe to event socket: {e}");
                        std::process::exit(1);
                    });
                bg_handles.push(subscribe_handle);
                info!(socket = %sock_path, "subscribed to event socket (tree read-only)");
            } else {
                let shard_pairs: Vec<(usize, Arc<RawObjectStore>)> = rim.iter().enumerate()
                    .filter_map(|(sid, raw_idx_opt)| {
                        raw_idx_opt.map(|ri| (sid, Arc::clone(&raw_stores[ri])))
                    })
                    .collect();
                let (_bus, server) = shardedobjstr::event::setup_event_socket(
                    &cluster,
                    &shard_pairs,
                    Path::new(sock_path),
                    es,
                    max_readers_global,
                    event_log_cb.clone(),
                ).unwrap_or_else(|e| {
                    eprintln!("ERROR: failed to start event socket: {e}");
                    std::process::exit(1);
                });
                info!(socket = %sock_path, "event socket started (tree mode)");
                _event_server_keep_alive = Some(server);
            }
        }

        // Streaming replica mode (tree-config, read-only).
        //
        // If event_source is set (from config or --event-source), connect
        // to the writer's event stream and keep the in-memory catalog
        // up to date via PUT/DELETE events + HEAD calls.  This replaces
        // the FLUSH-based reload_index approach and works with any shard
        // backend (S3, fs, mem, raw).
        if let Some(ref src) = event_source {
            let es = event_secret.as_deref().unwrap_or_else(|| {
                eprintln!("ERROR: EVENT_SOURCE set but EVENT_SECRET missing");
                std::process::exit(1);
            });
            let replica_handle = shardedobjstr::event::subscribe_streaming_replica(
                src,
                es,
                Arc::clone(&cluster),
            )
            .await
            .unwrap_or_else(|e| {
                eprintln!("ERROR: failed to start streaming replica: {e}");
                std::process::exit(1);
            });
            bg_handles.push(replica_handle);
            info!(event_source = %src, "streaming replica subscriber started");
        }

        // Rebuild catalog from shards at startup so that
        // find_under_replicated() has accurate data from the first
        // poll cycle.  This is especially important when the catalog
        // was NOT loaded from a file, or when ephemeral shards (mem)
        // lost data since the last run.
        if cluster.shard_count() > 1 {
            let before = cluster.catalog().len();
            match cluster.rebuild_catalog().await {
                Ok(n) => {
                    info!(entries = n, previous = before, "startup catalog rebuild complete");
                }
                Err(e) => {
                    warn!(error = %e, "startup catalog rebuild failed");
                }
            }
        }

        // Spawn background recovery task (health polling + auto-sync).
        // Skip for streaming replicas -- they get data from the event stream,
        // not from recovery sweeps.
        if event_source.is_none() {
            let (recovery_handle, recovery_st) = objstrd::recovery::spawn_recovery_task(
                Arc::clone(&cluster),
                original_stores,
                device_paths,
                build_recovery_config(
                    cli_recovery_enabled,
                    cli_recovery_poll_secs,
                    cli_recovery_probe_timeout,
                    cli_recovery_failure_threshold,
                    cli_recovery_re_replicate_batch,
                    cli_repair_replication_interval,
                    cli_repair_replication_batch,
                    Some(&tree),
                ),
                log_buffer.clone(),
                Some(Arc::clone(&registry)),
                Arc::clone(&admin_op_lock),
            );
            bg_handles.push(recovery_handle);
            recovery_status_handle = Some(recovery_st);
        }

        // Spawn background repair-replication task if configured.
        {
            let rc = build_recovery_config(
                cli_recovery_enabled,
                cli_recovery_poll_secs,
                cli_recovery_probe_timeout,
                cli_recovery_failure_threshold,
                cli_recovery_re_replicate_batch,
                cli_repair_replication_interval,
                cli_repair_replication_batch,
                Some(&tree),
            );
            if rc.repair_replication_interval_secs > 0 {
                let (reb_handle, reb_st) = objstrd::recovery::spawn_repair_replication_task(
                    Arc::clone(&cluster),
                    &rc,
                    log_buffer.clone(),
                    Some(Arc::clone(&registry)),
                    Arc::clone(&admin_op_lock),
                );
                bg_handles.push(reb_handle);
                repair_replication_status_handle = Some(reb_st);
                info!(interval_secs = rc.repair_replication_interval_secs, batch_size = rc.repair_replication_batch_size, "repair-replication task started");
            }
        }

        // Spawn free-space tracker: periodically update cached free_space
        // on the cluster from actual device info so that replication
        // target selection and over-replication trimming are capacity-aware.
        {
            let fs_cluster = Arc::clone(&cluster);
            let fs_raws = raw_stores.clone();
            let fs_rim = rim.clone();
            debug!(interval_secs = 30, "free-space tracker started");
            bg_handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_secs(30),
                );
                loop {
                    interval.tick().await;
                    for (shard_id, raw_idx_opt) in fs_rim.iter().enumerate() {
                        if let Some(raw_idx) = raw_idx_opt {
                            let info = fs_raws[*raw_idx].device_info();
                            fs_cluster.set_shard_free_space(shard_id, info.free_space);
                        }
                    }
                }
            }));
        }

        let actual_bucket = if bucket != "testbucket" { bucket.clone() } else { tree.default_bucket.clone() };

        viz_cluster = Some(Arc::clone(&cluster));
        adapter = Some(ObjectStoreS3Adapter::with_bucket_sharded(
            cluster, registry, &actual_bucket,
        ));

        viz_raw_stores = raw_stores;
        viz_obj_stores = viz_obj_stores_vec.into_iter().map(|o| o.unwrap_or_else(|| Arc::new(OfflineVizStore) as Arc<dyn ObjectStore>)).collect();
        viz_shard_names = shard_names;
        viz_shard_kinds = shard_kinds;
        viz_raw_index_map = rim;
        viz_shard_endpoints = shard_endpoints;
        is_cluster_mode = true;
        viz_replication_factor = my_node.replication_factor;
        is_read_only = global_read_only;

        info!(
            node = %my_node.name,
            shards = my_node.shards.len(),
            rf = my_node.replication_factor,
            cluster = %tree.cluster_name,
            "tree config: node ready"
        );

    } else if let Some(cfg) = cluster_config {
        // -- Multi-shard mode (CONFIG_FILE or STORE_N_* env vars) ------

        let flush_interval_secs: u64 = cli_get("--flush-interval")
            .or_else(|| std::env::var("FLUSH_INTERVAL_SECS").ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        // Catalog persistence config (env + CLI only in multi-shard mode).
        let catalog_path: Option<String> = cli_get("--catalog-path")
            .or_else(|| std::env::var("CATALOG_PATH").ok());
        let catalog_format: String = cli_get("--catalog-format")
            .or_else(|| std::env::var("CATALOG_FORMAT").ok())
            .unwrap_or_else(|| "json".to_string());
        let catalog_flush_interval: u64 = cli_get("--catalog-flush-interval")
            .or_else(|| std::env::var("CATALOG_FLUSH_INTERVAL_SECS").ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();
        let mut raw_refs: Vec<Option<Arc<RawObjectStore>>> = Vec::new();
        let mut obj_stores: Vec<Option<Arc<dyn ObjectStore>>> = Vec::new();
        let mut shard_names: Vec<String> = Vec::new();
        let mut device_paths: Vec<Option<String>> = Vec::new();

        for (i, sc) in cfg.stores.iter().enumerate() {
            if sc.store_type != "raw" {
                eprintln!(
                    "ERROR: store {i} type '{}' -- only 'raw' is supported in cluster mode",
                    sc.store_type
                );
                std::process::exit(1);
            }
            let name = sc.name.clone().unwrap_or_else(|| format!("shard-{i}"));
            let image = sc.image.clone()
                .unwrap_or_else(|| format!("shard-{i}.raw"));
            let image_path = std::path::Path::new(&image);
            let size_mb = sc.size_mb.unwrap_or(256);
            let direct_io = sc.direct_io.unwrap_or(false);
            let shard_read_only = sc.read_only.unwrap_or(false);

            let maybe_store = if image_path.exists() {
                if shard_read_only {
                    info!(shard = i, image = %image, name = %name, "opening existing shard (read-only)");
                    match RawObjectStore::open_readonly(image_path) {
                        Ok(s) => Some(s),
                        Err(e) => {
                            warn!(shard = i, image = %image, error = %e, "failed to open shard -- marking OFFLINE");
                            None
                        }
                    }
                } else {
                    info!(shard = i, image = %image, name = %name, "opening existing shard");
                    match RawObjectStore::open(image_path) {
                        Ok(s) => Some(s),
                        Err(e) => {
                            warn!(shard = i, image = %image, error = %e, "failed to open shard -- marking OFFLINE");
                            None
                        }
                    }
                }
            } else {
                info!(shard = i, image = %image, name = %name, size_mb, "formatting new shard");
                match RawObjectStore::format_with_size(image_path, size_mb * 1024 * 1024, direct_io) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        warn!(shard = i, image = %image, error = %e, "failed to format shard -- marking OFFLINE");
                        None
                    }
                }
            };

            match maybe_store {
                Some(store) => {
                    let arc = Arc::new(store);
                    obj_stores.push(Some(arc.clone() as Arc<dyn ObjectStore>));
                    raw_refs.push(Some(arc.clone()));
                    raw_stores.push(arc);
                    shard_names.push(name);
                    device_paths.push(Some(image));
                }
                None => {
                    obj_stores.push(None);
                    raw_refs.push(None);
                    shard_names.push(name);
                    device_paths.push(Some(image));
                }
            }
        }

        // Log offline shard summary
        let offline_count = obj_stores.iter().filter(|s| s.is_none()).count();
        if offline_count > 0 {
            warn!(offline_count, total = obj_stores.len(), "some shards are OFFLINE -- serving in degraded mode");
        }

        // Keep a clone of original stores for the recovery task.
        let original_stores: Vec<Option<Arc<dyn ObjectStore>>> = obj_stores.iter().map(|o| o.as_ref().map(Arc::clone)).collect();
        // Keep a clone for viz.
        let viz_obj_stores_vec: Vec<Option<Arc<dyn ObjectStore>>> = obj_stores.iter().map(|o| o.as_ref().map(Arc::clone)).collect();
        // Keep clones for the VizService attach endpoint.
        viz_original_stores = original_stores.clone();
        viz_device_paths = device_paths.clone();

        let mut cluster_builder =
            ShardedObjectStore::new_with_offline(obj_stores, cfg.replication_factor)
                .with_read_only(global_read_only);
        if let Some(mw) = cfg.min_writes {
            cluster_builder = cluster_builder.with_min_writes(mw);
        }
        if cfg.delete_requires_min_writes {
            cluster_builder = cluster_builder.with_delete_requires_min_writes(true);
        }
        let cluster = Arc::new(cluster_builder);

        // Apply read preference from CLI / env var.
        if let Some(ref pref) = cli_read_prefer {
            match pref.as_str() {
                "ordered" => cluster.set_read_preference(shardedobjstr::ReadPreference::Ordered),
                "round-robin" | "round_robin" => cluster.set_read_preference(shardedobjstr::ReadPreference::RoundRobin),
                other => info!("unknown read_prefer '{}', using round-robin", other),
            }
        }

        // Catalog persistence: configure, load if file exists.
        if let Some(ref cat_path) = catalog_path {
            let persistence = match catalog_format.as_str() {
                "bincode" | "bin" => shardedobjstr::catalog::CatalogPersistence::bincode(cat_path),
                _ => shardedobjstr::catalog::CatalogPersistence::json(cat_path),
            };
            cluster.set_persistence(persistence);
            match cluster.load_catalog() {
                Ok(()) => {
                    let n = cluster.catalog().len();
                    if n > 0 {
                        info!(entries = n, path = cat_path, "loaded catalog from disk");
                    } else {
                        info!(path = cat_path, "catalog file missing or empty, will rebuild from shards");
                    }
                }
                Err(e) => {
                    warn!(error = %e, path = cat_path, "catalog file failed checksum or parse, ignoring and rebuilding from shards");
                }
            }

            // Strip offline and ephemeral (mem) shards from the catalog so
            // stale entries do not inflate replica counts.  This makes
            // find_under_replicated() report accurate numbers from the
            // very first poll cycle.
            for sid in 0..cluster.shard_count() {
                let strip = if let Some(h) = cluster.shard_health(sid) {
                    h.is_unavailable()
                } else {
                    false
                };
                let is_mem = shard_names.get(sid).map(|n| n == "mem").unwrap_or(false);
                if strip || is_mem {
                    let removed = cluster.catalog().remove_all_for_shard(sid);
                    if removed > 0 {
                        let reason = if is_mem { "ephemeral mem shard" } else { "offline" };
                        info!(shard_id = sid, entries = removed, reason, "stripped shard from catalog");
                    }
                }
            }
        }

        let shard_kinds = vec![ShardKind::Raw; raw_refs.len()];
        let registry = Arc::new(RawRefRegistry::new(raw_refs, shard_kinds.clone()));

        // Periodic flush for all shards (cancellable for reload)
        for raw in &raw_stores {
            shutdown_raws.push(Arc::clone(raw));
            flush_handles.push(spawn_flush_task(
                Arc::clone(raw),
                flush_interval_secs,
                flush_cancel_rx.clone(),
            ));
        }

        // Periodic catalog flush (cancellable for reload)
        if catalog_flush_interval > 0 && catalog_path.is_some() {
            flush_handles.push(spawn_catalog_flush_task(
                Arc::clone(&cluster),
                catalog_flush_interval,
                flush_cancel_rx.clone(),
            ));
        }

        // Event socket (multi-shard mode).
        //
        // Read-write: start an EventServer broadcasting PUT/DELETE/FLUSH.
        // Read-only:  subscribe to a writer's socket and reload_index on
        //             FLUSH.  All shards are raw in multi-shard mode so
        //             every flush is captured.
        #[cfg(unix)]
        if let Some(ref sock_path) = event_socket_path {
            let es = event_secret.as_deref().unwrap_or_else(|| {
                eprintln!("ERROR: EVENT_SOCKET set but EVENT_SECRET missing");
                std::process::exit(1);
            });
            if global_read_only {
                let reload_stores: Vec<Arc<RawObjectStore>> = raw_stores.iter()
                    .map(|s| Arc::clone(s))
                    .collect();
                let rescan_signal = Arc::clone(&bucket_rescan_signal);
                let subscribe_handle: tokio::task::JoinHandle<()> =
                    shardedobjstr::event::subscribe_store_events(
                        Path::new(sock_path),
                        es,
                        move |event| {
                            if matches!(event, rawobjstr::event::StoreEvent::Flush { .. }) {
                                for s in &reload_stores {
                                    if let Err(e) = s.reload_index() {
                                        tracing::error!("read-only reload_index failed: {e}");
                                    }
                                }
                                rescan_signal.notify_one();
                            }
                        },
                    )
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!("ERROR: failed to subscribe to event socket: {e}");
                        std::process::exit(1);
                    });
                bg_handles.push(subscribe_handle);
                info!(socket = %sock_path, "subscribed to event socket (multi-shard read-only)");
            } else {
                // In multi-shard mode all shards are raw; shard_id == raw_stores index.
                let shard_pairs: Vec<(usize, Arc<RawObjectStore>)> = raw_stores.iter()
                    .enumerate()
                    .map(|(i, s)| (i, Arc::clone(s)))
                    .collect();
                let (_bus, server) = shardedobjstr::event::setup_event_socket(
                    &cluster,
                    &shard_pairs,
                    Path::new(sock_path),
                    es,
                    max_readers_global,
                    event_log_cb.clone(),
                ).unwrap_or_else(|e| {
                    eprintln!("ERROR: failed to start event socket: {e}");
                    std::process::exit(1);
                });
                info!(socket = %sock_path, "event socket started (multi-shard mode)");
                _event_server_keep_alive = Some(server);
            }
        }

        // Rebuild catalog from shards at startup so that
        // find_under_replicated() has accurate data from the first
        // poll cycle.
        if cluster.shard_count() > 1 {
            let before = cluster.catalog().len();
            match cluster.rebuild_catalog().await {
                Ok(n) => {
                    info!(entries = n, previous = before, "startup catalog rebuild complete");
                }
                Err(e) => {
                    warn!(error = %e, "startup catalog rebuild failed");
                }
            }
        }

        // Spawn background recovery task (health polling + auto-sync).
        let (recovery_handle, recovery_st) = objstrd::recovery::spawn_recovery_task(
            Arc::clone(&cluster),
            original_stores,
            device_paths,
            build_recovery_config(
                cli_recovery_enabled,
                cli_recovery_poll_secs,
                cli_recovery_probe_timeout,
                cli_recovery_failure_threshold,
                cli_recovery_re_replicate_batch,
                cli_repair_replication_interval,
                cli_repair_replication_batch,
                None,
            ),
            log_buffer.clone(),
            Some(Arc::clone(&registry)),
            Arc::clone(&admin_op_lock),
        );
        bg_handles.push(recovery_handle);
        recovery_status_handle = Some(recovery_st);

        // Spawn background repair-replication task if configured (multi-shard mode).
        {
            let rc = build_recovery_config(
                cli_recovery_enabled,
                cli_recovery_poll_secs,
                cli_recovery_probe_timeout,
                cli_recovery_failure_threshold,
                cli_recovery_re_replicate_batch,
                cli_repair_replication_interval,
                cli_repair_replication_batch,
                None,
            );
            if rc.repair_replication_interval_secs > 0 {
                let (reb_handle, reb_st) = objstrd::recovery::spawn_repair_replication_task(
                    Arc::clone(&cluster),
                    &rc,
                    log_buffer.clone(),
                    Some(Arc::clone(&registry)),
                    Arc::clone(&admin_op_lock),
                );
                bg_handles.push(reb_handle);
                repair_replication_status_handle = Some(reb_st);
                info!(interval_secs = rc.repair_replication_interval_secs, batch_size = rc.repair_replication_batch_size, "repair-replication task started (multi-shard)");
            }
        }

        // Spawn free-space tracker (multi-shard mode: all shards are raw).
        {
            let fs_cluster = Arc::clone(&cluster);
            let fs_raws = raw_stores.clone();
            bg_handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_secs(30),
                );
                loop {
                    interval.tick().await;
                    for (shard_id, raw) in fs_raws.iter().enumerate() {
                        let info = raw.device_info();
                        fs_cluster.set_shard_free_space(shard_id, info.free_space);
                    }
                }
            }));
        }

        let bucket_name = cfg.default_bucket.clone();
        let actual_bucket = if bucket != "testbucket" { bucket.clone() } else { bucket_name };

        viz_cluster = Some(Arc::clone(&cluster));
        adapter = Some(ObjectStoreS3Adapter::with_bucket_sharded(
            cluster, registry, &actual_bucket,
        ));

        // All shards are raw in multi-shard mode
        let rim: Vec<Option<usize>> = (0..shard_kinds.len()).map(|i| Some(i)).collect();

        viz_raw_stores = raw_stores;
        viz_obj_stores = viz_obj_stores_vec.into_iter().map(|o| o.unwrap_or_else(|| Arc::new(OfflineVizStore) as Arc<dyn ObjectStore>)).collect();
        viz_shard_names = shard_names;
        viz_shard_kinds = shard_kinds;
        viz_raw_index_map = rim;
        viz_shard_endpoints = vec![None; cfg.stores.len()];
        is_cluster_mode = true;
        viz_replication_factor = cfg.replication_factor;
        is_read_only = global_read_only;

        info!(
            shards = cfg.stores.len(),
            rf = cfg.replication_factor,
            read_only = global_read_only,
            "sharded store ready"
        );

    } else {
        // -- Single-store mode -----------------------------------------

        viz_replication_factor = 1;

        let store_obj: Option<Arc<RawObjectStore>> = match backend.as_str() {

        "raw" => {
            let image = cli_get("--image")
                .or_else(|| std::env::var("IMAGE").ok())
                .unwrap_or_else(|| "store.raw".to_string());
            let image_path = Path::new(&image);

            let size_mb: u64 = cli_get("--size-mb")
                .or_else(|| std::env::var("SIZE_MB").ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(256);

            let direct_io = cli_flag("--direct-io")
                || std::env::var("DIRECT_IO")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);

            let compression_str = cli_get("--compression")
                .or_else(|| std::env::var("COMPRESSION").ok())
                .unwrap_or_else(|| "none".to_string());
            let compression = Compression::from_str_name(&compression_str)
                .unwrap_or_else(|_| {
                    eprintln!("ERROR: unknown compression '{}'. Valid: none, zstd, snappy, gzip0..gzip9", compression_str);
                    std::process::exit(1);
                });

            let flush_interval_secs: u64 = cli_get("--flush-interval")
                .or_else(|| std::env::var("FLUSH_INTERVAL_SECS").ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(5);

            let event_socket_single = cli_get("--event-socket")
                .or_else(|| std::env::var("EVENT_SOCKET").ok());

            let store = if read_only {
                if !image_path.exists() {
                    eprintln!("ERROR: READ_ONLY=1 but image '{}' does not exist", image);
                    std::process::exit(1);
                }
                info!(image = %image, "opening existing image (read-only)");
                RawObjectStore::open_readonly(image_path)
                    .expect("failed to open image in read-only mode")
            } else if image_path.exists() {
                info!(image = %image, "opening existing image");
                RawObjectStore::open(image_path).expect("failed to open image")
            } else {
                info!(image = %image, size_mb = size_mb, direct_io = direct_io,
                      compression = %compression, "formatting new image");
                RawObjectStore::format_with_options(image_path, FormatOptions {
                    device_size: size_mb * 1024 * 1024,
                    direct_io,
                    index_slot_size: rawobjstr::INDEX_REGION_SIZE,
                    max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                    compression,
                }).expect("failed to format image")
            };

            let store_arc = Arc::new(store);

            // Event bus assignment for standalone mode (declared in outer scope).

            if read_only {
                #[cfg(unix)]
                if let Some(ref sock_path) = event_socket_single {
                    let es = event_secret.as_deref().unwrap_or_else(|| {
                        eprintln!("ERROR: EVENT_SOCKET set but EVENT_SECRET missing");
                        std::process::exit(1);
                    });
                    let reload_store = Arc::clone(&store_arc);
                    let rescan_signal = Arc::clone(&bucket_rescan_signal);
                    let subscribe_handle: tokio::task::JoinHandle<()> =
                        rawobjstr::event::unix::subscribe_events(
                        Path::new(sock_path),
                        es,
                        move |event| {
                            if matches!(event, rawobjstr::event::StoreEvent::Flush { .. }) {
                                if let Err(e) = reload_store.reload_index() {
                                    tracing::error!("read-only reload_index failed: {e}");
                                }
                                rescan_signal.notify_one();
                            }
                        },
                    )
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!("ERROR: failed to subscribe to event socket: {e}");
                        std::process::exit(1);
                    });
                    bg_handles.push(subscribe_handle);
                    info!(socket = %sock_path, "subscribed to event socket (read-only)");
                }

                info!("read-only mode: no flush loop, no bucket JSON");
            } else {
                #[cfg(unix)]
                if let Some(ref sock_path) = event_socket_single {
                    let es = event_secret.as_deref().unwrap_or_else(|| {
                        eprintln!("ERROR: EVENT_SOCKET set but EVENT_SECRET missing");
                        std::process::exit(1);
                    });
                    let bus = Arc::new(rawobjstr::event::EventBus::new(256));
                    // Register flush callback so FLUSH events are emitted.
                    let flush_bus = Arc::clone(&bus);
                    store_arc.add_flush_callback(Arc::new(move |txn_id| {
                        flush_bus.emit_flush(0, txn_id);
                    }));
                    let server = rawobjstr::event::unix::EventServer::start(
                        Path::new(sock_path),
                        es,
                        max_readers_global,
                        &bus,
                        event_log_cb.clone(),
                    ).unwrap_or_else(|e| {
                        eprintln!("ERROR: failed to start event socket: {e}");
                        std::process::exit(1);
                    });
                    info!(socket = %sock_path, max_readers = max_readers_global, "event socket started (single-store)");
                    event_bus_single = Some(bus);
                    _event_server_keep_alive = Some(server);
                }

                flush_handles.push(spawn_flush_task(
                    Arc::clone(&store_arc),
                    flush_interval_secs,
                    flush_cancel_rx.clone(),
                ));

                shutdown_raws.push(Arc::clone(&store_arc));
            }

            Some(store_arc)
        }

        "fs" => {
            let image = cli_get("--image")
                .or_else(|| std::env::var("IMAGE").ok())
                .unwrap_or_else(|| "fs_root".to_string());

            info!(root = %image, "opening filesystem store");

            let fs_store: Arc<dyn ObjectStore> = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&image)
                    .unwrap_or_else(|e| {
                        eprintln!("ERROR: failed to open fs store at '{}': {e}", image);
                        std::process::exit(1);
                    }),
            );

            let cluster = Arc::new(
                ShardedObjectStore::new_with_offline(vec![Some(fs_store.clone())], 1)
                    .with_read_only(global_read_only),
            );
            let registry = Arc::new(RawRefRegistry::new(
                vec![None],
                vec![ShardKind::Sidecar],
            ));

            viz_raw_stores = vec![];
            viz_obj_stores = vec![fs_store];
            viz_shard_names = vec![format!("fs:{}", image)];
            viz_shard_kinds = vec![ShardKind::Sidecar];
            viz_raw_index_map = vec![None];
            is_read_only = global_read_only;
            is_cluster_mode = true;
            viz_cluster = Some(Arc::clone(&cluster));

            adapter = Some(ObjectStoreS3Adapter::with_bucket_sharded(
                cluster, registry, &bucket,
            ));
            info!(bucket = %bucket, backend = "fs", "initial bucket registered");
            None
        }

        "mem" => {
            info!("opening in-memory store");

            let mem_store: Arc<dyn ObjectStore> = Arc::new(
                object_store::memory::InMemory::new(),
            );

            let cluster = Arc::new(
                ShardedObjectStore::new_with_offline(vec![Some(mem_store.clone())], 1)
                    .with_read_only(global_read_only),
            );
            let registry = Arc::new(RawRefRegistry::new(
                vec![None],
                vec![ShardKind::Sidecar],
            ));

            viz_raw_stores = vec![];
            viz_obj_stores = vec![mem_store];
            viz_shard_names = vec!["mem".to_string()];
            viz_shard_kinds = vec![ShardKind::Sidecar];
            viz_raw_index_map = vec![None];
            is_read_only = global_read_only;
            is_cluster_mode = true;
            viz_cluster = Some(Arc::clone(&cluster));

            adapter = Some(ObjectStoreS3Adapter::with_bucket_sharded(
                cluster, registry, &bucket,
            ));
            info!(bucket = %bucket, backend = "mem", "initial bucket registered");
            None
        }

        #[cfg(feature = "s3-backend")]
        "s3" => {
            let s3_endpoint = cli_get("--s3-endpoint")
                .or_else(|| std::env::var("S3_ENDPOINT").ok())
                .unwrap_or_else(|| {
                    eprintln!("ERROR: --s3-endpoint (or S3_ENDPOINT) is required for BACKEND=s3");
                    std::process::exit(1);
                });
            let s3_bucket = cli_get("--s3-bucket")
                .or_else(|| std::env::var("S3_BUCKET").ok())
                .unwrap_or_else(|| {
                    eprintln!("ERROR: --s3-bucket (or S3_BUCKET) is required for BACKEND=s3");
                    std::process::exit(1);
                });
            let s3_region = cli_get("--s3-region")
                .or_else(|| std::env::var("S3_REGION").ok())
                .unwrap_or_else(|| "us-east-1".to_string());
            let s3_access_key = cli_get("--s3-access-key")
                .or_else(|| std::env::var("S3_ACCESS_KEY").ok());
            let s3_secret_key = cli_get("--s3-secret-key")
                .or_else(|| std::env::var("S3_SECRET_KEY").ok());
            let s3_path_style = cli_flag("--s3-path-style")
                || std::env::var("S3_PATH_STYLE")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);

            info!(
                endpoint = %s3_endpoint,
                bucket = %s3_bucket,
                region = %s3_region,
                path_style = s3_path_style,
                "opening upstream S3 store",
            );

            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_endpoint(&s3_endpoint)
                .with_bucket_name(&s3_bucket)
                .with_region(&s3_region)
                .with_allow_http(true);
            if let Some(ref ak) = s3_access_key {
                builder = builder.with_access_key_id(ak);
            }
            if let Some(ref sk) = s3_secret_key {
                builder = builder.with_secret_access_key(sk);
            }
            if s3_access_key.is_none() && s3_secret_key.is_none() {
                builder = builder.with_skip_signature(true);
            }
            if s3_path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            let s3_store: Arc<dyn ObjectStore> = Arc::new(
                builder.build().unwrap_or_else(|e| {
                    eprintln!("ERROR: failed to build S3 store: {e}");
                    std::process::exit(1);
                }),
            );

            let cluster = Arc::new(
                ShardedObjectStore::new_with_offline(vec![Some(s3_store.clone())], 1)
                    .with_read_only(global_read_only),
            );
            let registry = Arc::new(RawRefRegistry::new(
                vec![None],
                vec![ShardKind::S3Like],
            ));

            viz_raw_stores = vec![];
            viz_obj_stores = vec![s3_store];
            viz_shard_names = vec![format!("s3:{}/{}", s3_endpoint, s3_bucket)];
            viz_shard_kinds = vec![ShardKind::S3Like];
            viz_raw_index_map = vec![None];
            is_read_only = global_read_only;
            is_cluster_mode = true;
            viz_cluster = Some(Arc::clone(&cluster));

            adapter = Some(ObjectStoreS3Adapter::with_bucket_sharded(
                cluster, registry, &bucket,
            ));
            info!(bucket = %bucket, backend = "s3", "initial bucket registered");
            None
        }

        #[cfg(not(feature = "s3-backend"))]
        "s3" => {
            eprintln!("ERROR: BACKEND=s3 requires the 's3-backend' feature to be enabled at compile time.");
            std::process::exit(1);
        }

        _other => {
            eprintln!("ERROR: unknown BACKEND value '{}'; expected raw, fs, mem, or s3", _other);
            std::process::exit(1);
        }
        }; // end match backend

        // -- Post-match setup for raw backend (needs store_obj) -------
        if let Some(ref store_obj) = store_obj {
        is_cluster_mode = false;
        viz_cluster = None;
        viz_raw_stores = vec![Arc::clone(&store_obj)];
        viz_obj_stores = vec![Arc::clone(&store_obj) as Arc<dyn ObjectStore>];
        viz_shard_names = vec!["primary".to_string()];
        viz_shard_kinds = vec![ShardKind::Raw];
        viz_raw_index_map = vec![Some(0)];
        is_read_only = read_only;

        let mut raw_adapter = ObjectStoreS3Adapter::with_bucket(store_obj.clone(), &bucket);
        // Wire event bus for PUT/DELETE events in standalone mode.
        #[cfg(unix)]
        if let Some(bus) = event_bus_single {
            raw_adapter.set_event_bus(bus);
        }
        info!(bucket = %bucket, "initial bucket registered");

        // In read-only mode, rebuild bucket list from the index instead of JSON.
        if read_only {
            let mut buckets = raw_adapter.rebuild_buckets_from_index().await;
            buckets.insert(bucket.clone());
            let count = buckets.len();
            raw_adapter.set_buckets(buckets).await;
            info!(count, "read-only: rebuilt bucket list from index");

            let bucket_reg = raw_adapter.bucket_registry();
            let bucket_store = store_obj.clone();
            let bucket_name = bucket.clone();
            let rescan_signal = Arc::clone(&bucket_rescan_signal);
            bg_handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                let mut last_count = count;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = rescan_signal.notified() => {}
                    }
                    let new_buckets = scan_bucket_names(bucket_store.as_ref(), &bucket_name).await;
                    if new_buckets.len() != last_count {
                        info!(
                            old = last_count,
                            new = new_buckets.len(),
                            "read-only: bucket list updated"
                        );
                        last_count = new_buckets.len();
                        *bucket_reg.write().await = new_buckets;
                    }
                }
            }));
        }
        adapter = Some(raw_adapter);
        } // end if raw backend
    } // end single-store else

    let mut adapter = adapter.expect("BUG: adapter not initialized by any backend path");

    // Capture refs we need for VizService before adapter is consumed
    let adapter_bucket_reg = adapter.bucket_registry();

    // When running unauthenticated, allow anonymous clients to call ListBuckets.
    if access_key.is_none() {
        adapter.set_allow_anon_list_buckets(true);
    }

    // Build S3 service
    let service = {
        let mut builder = S3ServiceBuilder::new(adapter);

        if let (Some(ak), Some(sk)) = (access_key.clone(), secret_key.clone()) {
            info!("SigV4 authentication enabled");
            let mut auth = SimpleAuth::from_single(ak, sk);

            for i in 1..10 {
                let ak_var = format!("ACCESS_KEY_{i}");
                let sk_var = format!("SECRET_KEY_{i}");
                if let (Ok(ak), Ok(sk)) = (std::env::var(&ak_var), std::env::var(&sk_var)) {
                    info!("registered additional credentials from {ak_var}");
                    auth.register(ak, sk.into());
                } else {
                    break;
                }
            }

            builder.set_auth(auth);
            builder.set_access(AllowAnonListBuckets);
        } else {
            info!("running without authentication (unauthenticated mode)");
            if bind != "127.0.0.1" && bind != "::1" && bind != "localhost" {
                tracing::warn!(
                    "WARNING: server is unauthenticated and bound to {bind} -- \
                     all S3 operations are open to the network. \
                     Set ACCESS_KEY/SECRET_KEY or BIND=127.0.0.1 for security."
                );
            }
        }

        builder.build()
    };

    // Wrap the S3 service with the visualization handler
    let admin_token = cli_get("--admin-token")
        .or_else(|| std::env::var("ADMIN_TOKEN").ok())
        .or(tree_admin_token.clone())
        .filter(|s| !s.is_empty());
    let cors_origin = cli_get("--cors-origin")
        .or_else(|| std::env::var("ADMIN_CORS_ORIGIN").ok())
        .or(tree_cors_origin.clone())
        .filter(|s| !s.is_empty());

    if admin_token.is_some() {
        info!("/_admin/* endpoints protected by ADMIN_TOKEN");
    } else {
        tracing::warn!("ADMIN_TOKEN not set -- /_admin/* admin endpoints are unprotected");
    }

    // Build CLI command string for display in config page
    let cli_command_str = cli_args.iter()
        .map(|a| if a.contains(' ') { format!("\"{}\"", a) } else { a.clone() })
        .collect::<Vec<_>>()
        .join(" ");

    // Read config file content (if --config was used)
    let config_file_content = config_path.as_ref().and_then(|p| {
        std::fs::read_to_string(p).ok()
    });

    // Capture event socket info for dashboard display.
    #[cfg(unix)]
    {
        if let Some(ref srv) = _event_server_keep_alive {
            viz_event_socket_path = Some(srv.socket_path().display().to_string());
            viz_event_socket_connections = Some(srv.subscriber_count_ref());
        }
    }

    let viz_event_source = event_source.clone();

    let service = VizService {
        s3: service,
        raw_stores: viz_raw_stores,
        obj_stores: viz_obj_stores,
        shard_names: viz_shard_names,
        shard_kinds: viz_shard_kinds,
        raw_index_map: viz_raw_index_map,
        cluster_mode: is_cluster_mode,
        replication_factor: viz_replication_factor,
        bucket_registry: adapter_bucket_reg,
        bucket: bucket.clone(),
        port,
        bind: bind.clone(),
        read_only: is_read_only,
        process_start_time,
        config_loaded_time: std::time::Instant::now(),
        admin_token,
        cors_origin,
        role: role.clone(),
        log_buffer: log_buffer.clone(),
        reload_signal: Arc::clone(&reload_signal),
        cluster: viz_cluster.clone(),
        recovery_status: recovery_status_handle,
        repair_replication_status: repair_replication_status_handle,
        admin_op_lock: Arc::clone(&admin_op_lock),
        cli_command: cli_command_str,
        config_content: config_file_content,
        stats_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        event_socket_path: viz_event_socket_path,
        event_socket_connections: viz_event_socket_connections,
        event_source: viz_event_source,
        node_name: node_name_opt.clone().unwrap_or_else(|| format!("{bind}:{port}")),
        original_stores: viz_original_stores,
        raw_device_paths: viz_device_paths,
        shard_endpoints: viz_shard_endpoints,
        op_log_tx: tokio::sync::broadcast::channel(1024).0,
    };

    // Accept loop
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("objstrd listening on http://{local_addr} role={role} backend={backend}");
    log_buffer.push(LogEntry::new(
        "info", "lifecycle", "internal",
        format!("listening on http://{local_addr} role={role} backend={backend}"),
    ));
    info!("system logs at http://{local_addr}/_admin/logs/ui");
    if !service.raw_stores.is_empty() {
        if service.cluster_mode {
            info!("cluster dashboard at http://{local_addr}/_admin/cluster");
            for (i, name) in service.shard_names.iter().enumerate() {
                info!("  shard {i} ({name}) at http://{local_addr}/_admin/shard/{i}/viz");
            }
        } else {
            info!("device visualizer at http://{local_addr}/_admin/");
        }
        info!("object manager   at http://{local_addr}/_admin/ui");
    }

    let reload_requested = loop {
        let (socket, remote_addr) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                info!("ctrl-c received, shutting down");
                break false;
            }
            _ = sighup.recv() => {
                info!("SIGHUP received -- reloading configuration");
                log_buffer.push(LogEntry::new(
                    "info", "lifecycle", "internal",
                    "SIGHUP received, reloading configuration".to_string(),
                ));
                break true;
            }
            _ = reload_signal.notified() => {
                info!("reload requested via HTTP -- reloading configuration");
                log_buffer.push(LogEntry::new(
                    "info", "lifecycle", "internal",
                    "reload requested via HTTP endpoint".to_string(),
                ));
                break true;
            }
        };

        tracing::debug!(remote = %remote_addr, "accepted connection");
        let conn = http_server.serve_connection(TokioIo::new(socket), service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    };

    // Graceful shutdown: drain in-flight connections
    tokio::select! {
        () = graceful.shutdown() => {
            info!("graceful shutdown complete");
        }
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            info!("shutdown timeout, aborting");
        }
    }

    // Cancel flush timer tasks and wait for them to exit
    let _ = flush_cancel_tx.send(true);
    for h in flush_handles {
        let _ = h.await;
    }

    // Abort background tasks (recovery, bucket rescan) to release Arc refs
    for h in &bg_handles {
        h.abort();
    }
    for h in bg_handles {
        let _ = h.await;
    }

    // Flush raw store indices to disk
    for (i, raw) in shutdown_raws.iter().enumerate() {
        if raw.needs_flush() {
            info!("flushing shard {i} index");
            if let Err(e) = raw.flush_index() {
                tracing::error!("shard {i} flush failed: {e}");
            }
        }
    }

    // Save catalog to disk if it has changed since last flush
    if let Some(ref cluster) = viz_cluster {
        match cluster.save_catalog_if_dirty() {
            Ok(true) => {
                info!(entries = cluster.catalog().len(), "catalog saved on shutdown");
            }
            Ok(false) => {} // not dirty, no save needed
            Err(e) => {
                tracing::error!("catalog save on shutdown failed: {e}");
            }
        }
    }

    if !reload_requested {
        break 'reload;
    }

    // Drop old state to release file locks and file descriptors
    info!("reload: dropping old stores and rebuilding");
    log_buffer.push(LogEntry::new(
        "info", "lifecycle", "internal",
        "reload: dropping old stores and rebuilding".to_string(),
    ));
    drop(shutdown_raws);

    } // end 'reload loop

    info!("objstrd stopped");
}
