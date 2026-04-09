//! Background shard health polling and auto-recovery.
//!
//! **7a -- Health polling:** A periodic task probes each shard every N seconds.
//! If a healthy shard becomes unreachable it is detached automatically.
//! If an offline shard becomes reachable again, syncing starts.
//!
//! **7b -- Mirror-mode sync (rf = shard_count):** Every shard holds every
//! object. Recovery lists a healthy source shard, diffs against the
//! recovering target, copies missing objects, deletes stale ones, then
//! re-attaches the shard.
//!
//! **7b' -- Partitioned-mode sync (rf < shard_count):** Uses
//! `find_under_replicated()` and `replicate_object()` to repair the
//! specific objects that belong on the recovering shard.
//!
//! The actual repair algorithms (probe, sync, sweep, trim) now live in
//! `shardedobjstr::repair`.  This module provides the daemon's
//! background loop, status tracking, and task spawning.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use object_store::ObjectStore;
use parking_lot::Mutex;
use shardedobjstr::{DetachReason, ShardHealth, ShardedObjectStore};
use shardedobjstr::metadata::RawRefRegistry;
use shardedobjstr::repair::{
    probe_store, sync_and_reattach, re_replication_sweep, over_replication_trim,
    repair_replication_sweep,
};
use tracing::{info, warn};

use crate::logging::{LogBuffer, LogEntry};

// -- Configuration ----------------------------------------------------

/// Tuning knobs for the recovery background task.
///
/// All timings are configurable via CLI flags, env vars, or tree config
/// directives.  Set `enabled = false` to disable all background recovery
/// (useful when intentionally taking a mirror offline for maintenance).
pub struct RecoveryConfig {
    /// Master switch: when false the recovery task exits immediately.
    pub enabled: bool,
    /// How often to poll shard health (default: 10 s).
    pub poll_interval_secs: u64,
    /// Timeout for a single health-check probe (default: 5 s).
    pub probe_timeout_secs: u64,
    /// Number of consecutive probe failures before detaching (default: 3).
    pub failure_threshold: u32,
    /// Maximum number of objects to re-replicate per sweep cycle
    /// (default: 100).  Prevents a single sweep from saturating IO.
    pub re_replicate_batch_size: usize,
    /// How often (in seconds) to run the background repair-replication
    /// task that discovers objects via `rebuild_catalog()` and copies
    /// under-replicated objects to restore the target RF.
    /// Set to 0 to disable (default: 0).
    pub repair_replication_interval_secs: u64,
    /// Maximum number of objects to replicate per repair-replication
    /// batch (default: 500).
    pub repair_replication_batch_size: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 10,
            probe_timeout_secs: 5,
            failure_threshold: 3,
            re_replicate_batch_size: 100,
            repair_replication_interval_secs: 0,
            repair_replication_batch_size: 500,
        }
    }
}

// -- Shared recovery status -------------------------------------------

/// Live recovery status published by the background loop and read by
/// the web UI via `/_admin/recovery`.
#[derive(Debug, Clone)]
pub struct RecoveryStatus {
    /// Current recovery config values.
    pub config: RecoveryConfigSnapshot,
    /// Total objects re-replicated since the recovery task started.
    pub total_re_replicated: u64,
    /// Objects re-replicated in the last sweep cycle.
    pub last_sweep_count: usize,
    /// Number of currently under-replicated objects (from last check).
    pub under_replicated_count: usize,
    /// Number of currently over-replicated objects (from last check).
    pub over_replicated_count: usize,
    /// Total excess replicas trimmed since the recovery task started.
    pub total_trimmed: u64,
    /// Excess replicas trimmed in the last sweep cycle.
    pub last_trim_count: usize,
    /// ISO 8601 timestamp of the last completed poll cycle.
    pub last_poll_at: Option<String>,
    /// ISO 8601 timestamp of the last re-replication sweep.
    pub last_sweep_at: Option<String>,
    /// Number of poll cycles completed since task start.
    pub poll_cycles: u64,
}

/// Snapshot of the active recovery configuration (for the web UI).
#[derive(Debug, Clone)]
pub struct RecoveryConfigSnapshot {
    pub enabled: bool,
    pub poll_interval_secs: u64,
    pub probe_timeout_secs: u64,
    pub failure_threshold: u32,
    pub re_replicate_batch_size: usize,
}

impl RecoveryStatus {
    fn new(config: &RecoveryConfig) -> Self {
        Self {
            config: RecoveryConfigSnapshot {
                enabled: config.enabled,
                poll_interval_secs: config.poll_interval_secs,
                probe_timeout_secs: config.probe_timeout_secs,
                failure_threshold: config.failure_threshold,
                re_replicate_batch_size: config.re_replicate_batch_size,
            },
            total_re_replicated: 0,
            last_sweep_count: 0,
            under_replicated_count: 0,
            over_replicated_count: 0,
            total_trimmed: 0,
            last_trim_count: 0,
            last_poll_at: None,
            last_sweep_at: None,
            poll_cycles: 0,
        }
    }
}

/// Thread-safe handle to the live recovery status.
pub type RecoveryStatusHandle = Arc<Mutex<RecoveryStatus>>;

// -- Public entry point -----------------------------------------------

/// Spawn the recovery background loop.
///
/// `original_stores` must contain the *real* `Arc<dyn ObjectStore>` for
/// every shard, in shard-index order.  These are kept so we can re-probe
/// a shard after it was detached (detaching replaces the live store with
/// an `OfflinePlaceholderStore`).
///
/// `device_paths` maps shard index to the on-disk path of the backing
/// device or file (if any).  When a path is `Some` and the file/device
/// disappears, the shard is detached immediately without waiting for
/// the normal failure-threshold count.
///
/// Returns `(JoinHandle, RecoveryStatusHandle)`.  The caller can
/// `.abort()` the handle on shutdown.  The status handle can be shared
/// with the HTTP server for the `/_admin/recovery` endpoint.
pub fn spawn_recovery_task(
    cluster: Arc<ShardedObjectStore>,
    original_stores: Vec<Option<Arc<dyn ObjectStore>>>,
    device_paths: Vec<Option<String>>,
    config: RecoveryConfig,
    log_buffer: LogBuffer,
    raw_refs: Option<Arc<RawRefRegistry>>,
    admin_op_lock: crate::viz::AdminOpLock,
) -> (tokio::task::JoinHandle<()>, RecoveryStatusHandle) {
    let status = Arc::new(Mutex::new(RecoveryStatus::new(&config)));
    let status_clone = Arc::clone(&status);
    let handle = tokio::spawn(async move {
        recovery_loop(cluster, original_stores, device_paths, config, status_clone, log_buffer, raw_refs, admin_op_lock).await;
    });
    (handle, status)
}

// -- Main loop --------------------------------------------------------

async fn recovery_loop(
    cluster: Arc<ShardedObjectStore>,
    original_stores: Vec<Option<Arc<dyn ObjectStore>>>,
    device_paths: Vec<Option<String>>,
    config: RecoveryConfig,
    status: RecoveryStatusHandle,
    log_buffer: LogBuffer,
    raw_refs: Option<Arc<RawRefRegistry>>,
    admin_op_lock: crate::viz::AdminOpLock,
) {
    if !config.enabled {
        info!("recovery task disabled by configuration");
        log_buffer.push(LogEntry::new("info", "lifecycle", "internal",
            "recovery task disabled by configuration".to_string()));
        return;
    }

    let mut interval =
        tokio::time::interval(Duration::from_secs(config.poll_interval_secs));
    let probe_timeout = Duration::from_secs(config.probe_timeout_secs);

    // Per-shard consecutive failure counter.
    let mut fail_counts: Vec<u32> = vec![0; original_stores.len()];

    info!(
        shard_count = original_stores.len(),
        paths = ?device_paths,
        poll_secs = config.poll_interval_secs,
        probe_timeout_secs = config.probe_timeout_secs,
        failure_threshold = config.failure_threshold,
        re_replicate_batch_size = config.re_replicate_batch_size,
        "recovery task started"
    );

    loop {
        interval.tick().await;

        let shard_count = cluster.shard_count();

        for shard_id in 0..shard_count {
            let health = match cluster.shard_health(shard_id) {
                Some(h) => h,
                None => continue,
            };

            // Fast path: if we know the device path and it no longer
            // exists on disk, detach immediately. This catches USB
            // pulls, deleted image files, and similar scenarios faster
            // than waiting for probe failures to accumulate.
            if matches!(health, ShardHealth::Healthy | ShardHealth::Degraded) {
                if let Some(Some(ref path)) = device_paths.get(shard_id) {
                    if !std::path::Path::new(path).exists() {
                        warn!(
                            shard_id,
                            path = %path,
                            "backing device/file disappeared, detaching shard"
                        );
                        log_buffer.push(LogEntry::new("warn", "health", "internal",
                            format!("shard {shard_id}: backing device/file disappeared ({path}), detaching")));
                        log_buffer.push(LogEntry::new("warn", "health", "internal",
                            format!("shard {shard_id}: Healthy -> Offline")));
                        cluster.detach_shard(shard_id);
                        cluster.set_detach_reason(shard_id, DetachReason::DeviceMissing);
                        fail_counts[shard_id] = 0;
                        continue;
                    }
                }
            }

            match health {
                // -- Healthy / Degraded: make sure the shard is still reachable --
                ShardHealth::Healthy | ShardHealth::Degraded => {
                    let store = match cluster.shard_store(shard_id) {
                        Some(s) => s,
                        None => continue,
                    };
                    if probe_store(&store, probe_timeout).await {
                        fail_counts[shard_id] = 0;
                    } else {
                        fail_counts[shard_id] += 1;
                        warn!(
                            shard_id,
                            failures = fail_counts[shard_id],
                            threshold = config.failure_threshold,
                            "shard probe failed"
                        );
                        log_buffer.push(LogEntry::new("warn", "health", "internal",
                            format!("shard {shard_id}: probe failed ({}/{} failures)",
                                fail_counts[shard_id], config.failure_threshold)));
                        if fail_counts[shard_id] >= config.failure_threshold {
                            warn!(shard_id, "detaching unreachable shard");
                            log_buffer.push(LogEntry::new("warn", "health", "internal",
                                format!("shard {shard_id}: detaching (threshold reached)")));
                            log_buffer.push(LogEntry::new("warn", "health", "internal",
                                format!("shard {shard_id}: Healthy -> Offline")));
                            cluster.detach_shard(shard_id);
                            cluster.set_detach_reason(shard_id, DetachReason::ProbeFailure);
                            fail_counts[shard_id] = 0;
                        }
                    }
                }

                // -- Offline: probe the *original* store to detect recovery --
                ShardHealth::Offline => {
                    // On Linux, moving a file does not invalidate open file
                    // descriptors.  The original RawObjectStore may still
                    // respond via its fd even after the backing file has been
                    // renamed or deleted.  Guard against this by skipping the
                    // probe entirely when the path no longer exists on disk.
                    if let Some(Some(ref path)) = device_paths.get(shard_id) {
                        if !std::path::Path::new(path).exists() {
                            // Path still gone -- stay offline this cycle.
                            continue;
                        }
                    }
                    if let Some(Some(original)) = original_stores.get(shard_id) {
                        if probe_store(original, probe_timeout).await {
                            info!(
                                shard_id,
                                "offline shard is reachable again, starting recovery"
                            );
                            log_buffer.push(LogEntry::new("info", "recovery", "internal",
                                format!("shard {shard_id}: offline shard reachable, starting sync")));
                            sync_and_reattach(
                                &cluster,
                                &original_stores,
                                shard_id,
                                raw_refs.as_deref(),
                            )
                            .await;
                            // Log the outcome of the sync.
                            let new_health = cluster.shard_health(shard_id);
                            match new_health {
                                Some(ShardHealth::Healthy) => {
                                    log_buffer.push(LogEntry::new("info", "health", "internal",
                                        format!("shard {shard_id}: Offline -> Healthy (sync complete)")));
                                }
                                Some(ShardHealth::Offline) => {
                                    log_buffer.push(LogEntry::new("warn", "health", "internal",
                                        format!("shard {shard_id}: sync failed, remains Offline")));
                                }
                                Some(h) => {
                                    log_buffer.push(LogEntry::new("info", "health", "internal",
                                        format!("shard {shard_id}: sync finished, health={h:?}")));
                                }
                                None => {}
                            }
                        }
                    }
                }

                // -- Detached: manually held offline, do NOT auto-reattach --
                ShardHealth::Detached => {}

                // -- Syncing: a recovery is already in progress --
                ShardHealth::Syncing => {}
            }
        }

        // -- Re-replication sweep ---------------------------------
        // Skip sweep/trim if a manual admin operation (drain, redistribute,
        // repair-replication) is running to avoid conflicting mutations.
        let admin_op_active = {
            let guard = admin_op_lock.lock().await;
            guard.is_some()
        };

        // If any shard is offline and not suppressed, proactively copy
        // under-replicated objects to other healthy shards so that RF is
        // restored even if the dead shard never comes back.
        let sweep_count = if !admin_op_active {
            re_replication_sweep(&cluster, config.re_replicate_batch_size, raw_refs.as_deref()).await
        } else {
            0
        };

        // -- Under-replication repair -----------------------------
        // Unconditionally check for under-replicated objects every
        // cycle, regardless of grace periods.  This catches cases
        // where all shards are healthy but data is missing (e.g. mem
        // shard reset on restart, manual deletion, partial writes).
        let under_repair_count = if !admin_op_active && sweep_count == 0 {
            let under = cluster.find_under_replicated();
            if !under.is_empty() {
                let batch: Vec<_> = under.into_iter()
                    .take(config.re_replicate_batch_size)
                    .collect();
                let mut repaired = 0usize;
                for (key, _count) in &batch {
                    let entry = match cluster.placement(key) {
                        Some(e) => e,
                        None => continue,
                    };
                    let source = match entry.shards.iter().copied().find(|&sid| {
                        cluster.shard_health(sid) == Some(ShardHealth::Healthy)
                    }) {
                        Some(s) => s,
                        None => continue,
                    };
                    let target = match cluster.find_replication_target(key) {
                        Some(t) => t,
                        None => continue,
                    };
                    if cluster.replicate_object(key, source, target, raw_refs.as_deref()).await.is_ok() {
                        repaired += 1;
                    }
                }
                repaired
            } else {
                0
            }
        } else {
            0
        };

        // -- Over-replication trim --------------------------------
        // Remove excess replicas from objects that have more copies
        // than the replication factor.  Shed from the fullest shard.
        let trim_count = if !admin_op_active {
            over_replication_trim(&cluster, config.re_replicate_batch_size).await
        } else {
            0
        };

        // -- Update shared status ---------------------------------
        {
            let now_str = Utc::now().to_rfc3339();
            let under_count = cluster.find_under_replicated().len();
            let over_count = cluster.find_over_replicated().len();

            if sweep_count > 0 {
                log_buffer.push(LogEntry::new("info", "replication", "internal",
                    format!("re-replication sweep: {sweep_count} objects copied")));
            }
            if under_repair_count > 0 {
                log_buffer.push(LogEntry::new("info", "replication", "internal",
                    format!("under-replication repair: {under_repair_count} objects copied")));
            }
            if trim_count > 0 {
                log_buffer.push(LogEntry::new("info", "replication", "internal",
                    format!("over-replication trim: {trim_count} excess replicas removed")));
            }

            let total_replicated = sweep_count + under_repair_count;
            let mut st = status.lock();
            st.poll_cycles += 1;
            st.last_poll_at = Some(now_str.clone());
            st.under_replicated_count = under_count;
            st.over_replicated_count = over_count;
            if total_replicated > 0 || trim_count > 0 {
                st.last_sweep_count = total_replicated;
                st.total_re_replicated += total_replicated as u64;
                st.last_trim_count = trim_count;
                st.total_trimmed += trim_count as u64;
                st.last_sweep_at = Some(now_str);
            }
        }
    }
}

// -- Repair-replication background task -------------------------------

/// Live status for the periodic repair-replication background task,
/// published to `/_admin/repair-replication-status`.
#[derive(Debug, Clone)]
pub struct RepairReplicationStatus {
    /// Whether the task is enabled.
    pub enabled: bool,
    /// Configured interval in seconds.
    pub interval_secs: u64,
    /// Configured batch size.
    pub batch_size: usize,
    /// Current phase: "idle", "scanning", or "replicating".
    pub phase: String,
    /// Number of full repair-replication cycles completed.
    pub cycles_completed: u64,
    /// Total objects replicated across all cycles.
    pub total_objects_replicated: u64,
    /// Total excess replicas trimmed across all cycles.
    pub total_objects_trimmed: u64,
    /// Object count from the last catalog rebuild.
    pub last_catalog_size: usize,
    /// Under-replicated objects remaining after the last sweep.
    pub remaining_under_replicated: usize,
    /// ISO 8601 timestamp of the last completed cycle.
    pub last_completed_at: Option<String>,
    /// ISO 8601 timestamp of the last cycle start.
    pub last_started_at: Option<String>,
}

/// Thread-safe handle to the live repair-replication status.
pub type RepairReplicationStatusHandle = Arc<Mutex<RepairReplicationStatus>>;

/// Spawn a periodic repair-replication background task.
///
/// Each cycle:
/// 1. `rebuild_catalog()` -- discovers all objects across all shards.
/// 2. `repair_replication_sweep()` in a loop -- copies under-replicated
///    objects and trims over-replicated ones.  Each
///    `replicate_object()` call updates the catalog incrementally, so
///    reads can hit the new shard as soon as each object is copied.
///
/// Returns `(JoinHandle, RepairReplicationStatusHandle)`.  The caller
/// can `.abort()` the handle on shutdown.
pub fn spawn_repair_replication_task(
    cluster: Arc<ShardedObjectStore>,
    config: &RecoveryConfig,
    log_buffer: LogBuffer,
    raw_refs: Option<Arc<RawRefRegistry>>,
    admin_op_lock: crate::viz::AdminOpLock,
) -> (tokio::task::JoinHandle<()>, RepairReplicationStatusHandle) {
    let status = Arc::new(Mutex::new(RepairReplicationStatus {
        enabled: true,
        interval_secs: config.repair_replication_interval_secs,
        batch_size: config.repair_replication_batch_size,
        phase: "idle".to_string(),
        cycles_completed: 0,
        total_objects_replicated: 0,
        total_objects_trimmed: 0,
        last_catalog_size: 0,
        remaining_under_replicated: 0,
        last_completed_at: None,
        last_started_at: None,
    }));
    let interval_secs = config.repair_replication_interval_secs;
    let batch_size = config.repair_replication_batch_size;
    let status_clone = Arc::clone(&status);
    let handle = tokio::spawn(async move {
        repair_replication_loop(cluster, interval_secs, batch_size, status_clone, log_buffer, raw_refs, admin_op_lock).await;
    });
    (handle, status)
}

async fn repair_replication_loop(
    cluster: Arc<ShardedObjectStore>,
    interval_secs: u64,
    batch_size: usize,
    status: RepairReplicationStatusHandle,
    log_buffer: LogBuffer,
    raw_refs: Option<Arc<RawRefRegistry>>,
    admin_op_lock: crate::viz::AdminOpLock,
) {
    info!(
        interval_secs,
        batch_size,
        "repair-replication task started"
    );
    log_buffer.push(LogEntry::new("info", "lifecycle", "internal",
        format!("repair-replication task started (interval={interval_secs}s, batch={batch_size})")));

    loop {
        // Skip this cycle if a manual admin operation is in progress.
        {
            let guard = admin_op_lock.lock().await;
            if let Some(ref op) = *guard {
                info!(operation = %op, "repair-replication: skipping cycle, admin operation in progress");
                drop(guard);
                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
                continue;
            }
        }

        // Update phase: scanning
        {
            let mut st = status.lock();
            st.phase = "scanning".to_string();
            st.last_started_at = Some(Utc::now().to_rfc3339());
        }

        // Phase 1: Discover all objects across all shards.
        let catalog_size = match cluster.rebuild_catalog().await {
            Ok(n) => {
                info!(objects = n, "repair-replication: catalog rebuilt");
                log_buffer.push(LogEntry::new("info", "replication", "internal",
                    format!("repair-replication: catalog rebuilt with {n} objects")));
                n
            }
            Err(e) => {
                warn!("repair-replication: catalog rebuild failed: {e}");
                log_buffer.push(LogEntry::new("warn", "replication", "internal",
                    format!("repair-replication: catalog rebuild failed: {e}")));
                0
            }
        };
        status.lock().last_catalog_size = catalog_size;

        // Phase 2: Replicate under-replicated objects in batches.
        {
            status.lock().phase = "replicating".to_string();
        }
        let mut cycle_replicated: u64 = 0;
        let mut cycle_trimmed: u64 = 0;
        loop {
            let result = repair_replication_sweep(&cluster, batch_size, raw_refs.as_deref(), None).await;
            cycle_replicated += result.re_replicated as u64;
            cycle_trimmed += result.trimmed as u64;
            if result.re_replicated > 0 || result.trimmed > 0 {
                info!(
                    re_replicated = result.re_replicated,
                    trimmed = result.trimmed,
                    remaining = result.under_remaining,
                    "repair-replication: sweep batch completed"
                );
            }
            if result.re_replicated == 0 || result.under_remaining == 0 {
                break;
            }
            // Brief yield between batches to avoid starving normal I/O.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Update status after cycle completes.
        let remaining = cluster.find_under_replicated().len();
        {
            let mut st = status.lock();
            st.phase = "idle".to_string();
            st.cycles_completed += 1;
            st.total_objects_replicated += cycle_replicated;
            st.total_objects_trimmed += cycle_trimmed;
            st.remaining_under_replicated = remaining;
            st.last_completed_at = Some(Utc::now().to_rfc3339());
        }

        if cycle_replicated > 0 || cycle_trimmed > 0 {
            log_buffer.push(LogEntry::new("info", "replication", "internal",
                format!("repair-replication cycle done: {cycle_replicated} replicated, {cycle_trimmed} trimmed, {remaining} remaining")));
        }

        // Sleep until next cycle.
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}
