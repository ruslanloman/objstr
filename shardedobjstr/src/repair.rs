//! Cluster repair-replication algorithms.
//!
//! These functions live in the library so they can be called by CLI
//! tools (`shardedobjstr repair-replication`) and Python bindings as
//! well as the daemon's background recovery loop.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore, PutOptions, PutPayload};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{ShardHealth, ShardId, ShardedObjectStore};

/// Optional channel for streaming per-object progress messages to a
/// caller (e.g. the daemon's SSE endpoint).
pub type ProgressSink = mpsc::UnboundedSender<String>;
use crate::metadata::{
    RawRefRegistry, ShardKind, get_metadata, meta_to_attributes, meta_sidecar_path,
    read_metadata_from_shard,
};
use crate::tlv::decode_metadata;

// -- Sync statistics --------------------------------------------------

/// Counters returned after a mirror-mode sync completes.
#[derive(Debug, Default)]
pub struct MirrorSyncReport {
    pub copied: usize,
    pub deleted: usize,
    pub bytes_copied: u64,
    /// Objects that existed on both source and target but had a newer
    /// version on the source (re-PUT while shard was offline).
    pub updated: usize,
}

/// Result of a `repair_replication_sweep` call.
#[derive(Debug, Default)]
pub struct RepairReplicationResult {
    /// Objects re-replicated to restore RF (under-replicated repair).
    pub re_replicated: usize,
    /// Excess replicas trimmed (over-replicated cleanup).
    pub trimmed: usize,
    /// Objects still under-replicated after the sweep.
    pub under_remaining: usize,
    /// Objects still over-replicated after the sweep.
    pub over_remaining: usize,
}

/// Result of a `redistribute_sweep` call.
#[derive(Debug, Default)]
pub struct RedistributeResult {
    /// Objects successfully moved from fuller to emptier shards.
    pub moved: usize,
    /// Objects skipped (no valid target or already balanced).
    pub skipped: usize,
    /// Errors encountered during move.
    pub errors: usize,
    /// Per-shard object counts after the sweep (shard_id, count).
    pub shard_counts: Vec<(ShardId, usize)>,
}

/// A planned move or trim that `repair_replication_sweep` would execute.
#[derive(Debug, Clone)]
pub struct PlannedAction {
    /// Object key.
    pub key: String,
    /// Current number of healthy replicas.
    pub current_count: usize,
    /// Target replication factor.
    pub target_rf: usize,
    /// What kind of action.
    pub action: PlannedActionKind,
}

/// The kind of repair-replication action.
#[derive(Debug, Clone)]
pub enum PlannedActionKind {
    /// Copy from `source_shard` to `target_shard`.
    Replicate {
        source_shard: ShardId,
        target_shard: ShardId,
    },
    /// Remove excess replica from `shard`.
    Trim {
        shard: ShardId,
    },
}

/// Dry-run plan returned by `plan_repair_replication()`.
#[derive(Debug, Default)]
pub struct RepairReplicationPlan {
    /// Planned replication actions (under-replicated repair).
    pub replications: Vec<PlannedAction>,
    /// Planned trim actions (over-replicated cleanup).
    pub trims: Vec<PlannedAction>,
    /// Objects still under-replicated but no source/target available.
    pub unrepairable: usize,
    /// Objects still over-replicated but no shard to trim.
    pub untrimmable: usize,
}

// -- Health probe -----------------------------------------------------

/// Try a small LIST against the store.  Returns `true` if the store
/// responds (even with zero results) within `timeout`.
pub async fn probe_store(store: &Arc<dyn ObjectStore>, timeout: Duration) -> bool {
    let result = tokio::time::timeout(timeout, store.list(None).try_next()).await;
    match result {
        Ok(Ok(_)) => true,  // responded (Some or None -- both fine)
        Ok(Err(_)) => false, // store-level error
        Err(_) => false,     // timed out
    }
}

// -- Metadata-aware copy helper ---------------------------------------

/// Write payload (and optional metadata) to a target shard, dispatching
/// by shard kind.  Used during mirror/partitioned sync to preserve S3
/// metadata (Content-Type, user headers, etc.) across replicas.
///
/// When `metadata` is empty or `raw_refs` does not cover `target_shard`,
/// falls back to a plain `target_store.put()`.
///
/// Also used by read-repair and replication in lib.rs -- keep pub(crate).
pub(crate) async fn write_with_meta_to_shard(
    raw_refs: &RawRefRegistry,
    target_shard: ShardId,
    target_store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    data: Bytes,
    metadata: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if metadata.is_empty() {
        target_store.put(location, PutPayload::from(data)).await?;
        return Ok(());
    }

    match raw_refs.kind(target_shard) {
        ShardKind::Raw => {
            if let Some(raw) = raw_refs.get(target_shard) {
                raw.put_with_meta(location, data, metadata)?;
            } else {
                // No raw ref available -- plain put as fallback.
                target_store.put(location, PutPayload::from(data)).await?;
            }
        }
        ShardKind::S3Like => {
            let meta_map = decode_metadata(metadata);
            let attrs = meta_to_attributes(&meta_map);
            let opts = PutOptions {
                attributes: attrs,
                ..Default::default()
            };
            target_store.put_opts(location, PutPayload::from(data), opts).await?;
        }
        ShardKind::Sidecar => {
            target_store.put(location, PutPayload::from(data.clone())).await?;
            let sidecar = meta_sidecar_path(location);
            target_store.put(&sidecar, PutPayload::from(metadata.to_vec())).await?;
        }
    }
    Ok(())
}

/// Read metadata for a source object.  Returns empty vec if metadata
/// is unavailable or the object has no metadata (meta_len == 0).
async fn read_source_metadata(
    cluster: &ShardedObjectStore,
    raw_refs: &RawRefRegistry,
    location: &object_store::path::Path,
) -> Vec<u8> {
    // Check catalog meta_len first -- skip the read if no metadata.
    let key = location.as_ref();
    if let Some(entry) = cluster.placement(key) {
        if entry.meta_len == 0 {
            return Vec::new();
        }
    }
    match get_metadata(cluster, raw_refs, location).await {
        Ok(bytes) => bytes.to_vec(),
        Err(_) => Vec::new(),
    }
}

// -- Sync + re-attach orchestration -----------------------------------

/// Sync a recovering shard from healthy replicas, then re-attach it.
///
/// `original_stores` maps shard index to the *real* `Arc<dyn ObjectStore>`
/// (before detach replaced it with the offline placeholder).
///
/// When `raw_refs` is provided, metadata (Content-Type, user headers)
/// is preserved during the copy.  Pass `None` if no `RawRefRegistry`
/// is available (backward compat).
pub async fn sync_and_reattach(
    cluster: &ShardedObjectStore,
    original_stores: &[Option<Arc<dyn ObjectStore>>],
    shard_id: ShardId,
    raw_refs: Option<&RawRefRegistry>,
) {
    cluster.set_shard_health(shard_id, ShardHealth::Syncing);

    let is_mirror =
        cluster.replication_factor() == cluster.shard_count();

    let sync_ok = if is_mirror {
        match mirror_sync(cluster, original_stores, shard_id, raw_refs).await {
            Ok(stats) => {
                info!(
                    shard_id,
                    copied = stats.copied,
                    deleted = stats.deleted,
                    bytes = stats.bytes_copied,
                    "mirror sync complete"
                );
                true
            }
            Err(e) => {
                error!(shard_id, error = %e, "mirror sync failed");
                false
            }
        }
    } else {
        match partitioned_sync(cluster, original_stores, shard_id, raw_refs).await {
            Ok(replicated) => {
                info!(shard_id, replicated, "partitioned sync complete");
                true
            }
            Err(e) => {
                error!(shard_id, error = %e, "partitioned sync failed");
                false
            }
        }
    };

    if !sync_ok {
        cluster.set_shard_health(shard_id, ShardHealth::Offline);
        return;
    }

    // Re-attach with the original store (force = true: just scan,
    // since we already synced the data above).
    let original = match original_stores.get(shard_id).and_then(|o| o.as_ref()) {
        Some(s) => Arc::clone(s),
        None => {
            error!(shard_id, "no original store, cannot re-attach");
            cluster.set_shard_health(shard_id, ShardHealth::Offline);
            return;
        }
    };

    match cluster.attach_shard(shard_id, original, true).await {
        Ok(count) => {
            info!(shard_id, objects = count, "shard re-attached");
        }
        Err(e) => {
            error!(shard_id, error = %e, "attach_shard failed");
            cluster.set_shard_health(shard_id, ShardHealth::Offline);
        }
    }
}

// -- Mirror-mode sync -------------------------------------------------

/// Full bi-directional diff-and-sync between a healthy source shard and
/// the recovering target shard.
///
/// 1. LIST source (via cluster -- uses the live store).
/// 2. LIST target (via `original_stores` -- the real backing store).
/// 3. Copy objects present on source but missing on target.
/// 4. Delete objects present on target but absent from source
///    (they were deleted while the shard was offline).
///
/// When `raw_refs` is provided, metadata is preserved during copies.
pub async fn mirror_sync(
    cluster: &ShardedObjectStore,
    original_stores: &[Option<Arc<dyn ObjectStore>>],
    target_shard: ShardId,
    raw_refs: Option<&RawRefRegistry>,
) -> Result<MirrorSyncReport, Box<dyn std::error::Error + Send + Sync>> {
    // Fall back to cluster's own raw_refs if caller didn't provide them.
    let cluster_refs = cluster.raw_refs();
    let effective_refs: Option<&RawRefRegistry> = raw_refs
        .or_else(|| cluster_refs.as_deref());
    // Pick a healthy source shard.
    let source_shard = (0..cluster.shard_count())
        .find(|&id| {
            id != target_shard
                && cluster.shard_health(id) == Some(ShardHealth::Healthy)
        })
        .ok_or("no healthy source shard available for mirror sync")?;

    let source_store = cluster
        .shard_store(source_shard)
        .ok_or("source shard store unavailable")?;
    let target_store = original_stores
        .get(target_shard)
        .and_then(|o| o.as_ref())
        .ok_or("target shard original store unavailable")?;

    // Collect inventories.
    let source_objects: Vec<ObjectMeta> =
        source_store.list(None).try_collect().await?;
    let target_objects: Vec<ObjectMeta> =
        target_store.list(None).try_collect().await?;

    let source_keys: HashSet<String> =
        source_objects.iter().map(|m| m.location.to_string()).collect();

    // Build maps for stale detection.
    let target_by_key: std::collections::HashMap<String, &ObjectMeta> =
        target_objects.iter().map(|m| (m.location.to_string(), m)).collect();

    let mut stats = MirrorSyncReport::default();

    // Copy missing objects (source -> target).
    // Also update stale objects (same key, source is newer or different size).
    for meta in &source_objects {
        let key = meta.location.to_string();
        match target_by_key.get(&key) {
            None => {
                // Missing on target -- copy (with metadata if available).
                let data = source_store.get(&meta.location).await?.bytes().await?;
                if let Some(refs) = effective_refs {
                    let md = read_source_metadata(cluster, refs, &meta.location).await;
                    write_with_meta_to_shard(
                        refs, target_shard, target_store, &meta.location, data, &md,
                    ).await?;
                } else {
                    target_store.put(&meta.location, data.into()).await?;
                }
                stats.copied += 1;
                stats.bytes_copied += meta.size as u64;
            }
            Some(target_meta) => {
                // Exists on both -- update if source is newer or size differs.
                if meta.last_modified > target_meta.last_modified
                    || meta.size != target_meta.size
                {
                    let data = source_store.get(&meta.location).await?.bytes().await?;
                    if let Some(refs) = effective_refs {
                        let md = read_source_metadata(cluster, refs, &meta.location).await;
                        write_with_meta_to_shard(
                            refs, target_shard, target_store, &meta.location, data, &md,
                        ).await?;
                    } else {
                        target_store.put(&meta.location, data.into()).await?;
                    }
                    stats.updated += 1;
                    stats.bytes_copied += meta.size as u64;
                }
            }
        }
    }

    // Delete stale objects (on target but no longer on source).
    for meta in &target_objects {
        let key = meta.location.to_string();
        if !source_keys.contains(&key) {
            target_store.delete(&meta.location).await?;
            stats.deleted += 1;
        }
    }

    Ok(stats)
}

// -- Partitioned-mode sync --------------------------------------------

/// Repair under-replicated objects by copying from a healthy replica to
/// the recovering shard using the ObjectStore API directly.
///
/// After copying, replays delete markers: any object on the target shard
/// whose `last_modified` is older than the corresponding delete marker
/// is removed (it was deleted while the shard was offline).
///
/// When `raw_refs` is provided, metadata is preserved during copies.
pub async fn partitioned_sync(
    cluster: &ShardedObjectStore,
    original_stores: &[Option<Arc<dyn ObjectStore>>],
    target_shard: ShardId,
    raw_refs: Option<&RawRefRegistry>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    // Fall back to cluster's own raw_refs if caller didn't provide them.
    let cluster_refs = cluster.raw_refs();
    let effective_refs: Option<&RawRefRegistry> = raw_refs
        .or_else(|| cluster_refs.as_deref());
    let target_store = original_stores
        .get(target_shard)
        .and_then(|o| o.as_ref())
        .ok_or("target shard original store unavailable")?;

    let under_replicated = cluster.find_under_replicated();
    let mut replicated: usize = 0;

    for (key, _healthy_count) in &under_replicated {
        // Find a healthy shard that holds this object.
        let entry = match cluster.placement(key) {
            Some(e) => e,
            None => continue,
        };

        let source_shard = entry
            .shards
            .iter()
            .copied()
            .find(|&sid| {
                sid != target_shard
                    && cluster.shard_health(sid) == Some(ShardHealth::Healthy)
            });

        let source_shard = match source_shard {
            Some(s) => s,
            None => {
                warn!(key, "no healthy source for under-replicated object");
                continue;
            }
        };

        let source_store = match cluster.shard_store(source_shard) {
            Some(s) => s,
            None => continue,
        };

        // Copy via ObjectStore API (read from source, write to target).
        // When raw_refs is available, preserve metadata.
        let path = object_store::path::Path::from(key.as_str());
        match source_store.get(&path).await {
            Ok(result) => match result.bytes().await {
                Ok(data) => {
                    let write_result = if let Some(refs) = effective_refs {
                        let md = read_source_metadata(cluster, refs, &path).await;
                        write_with_meta_to_shard(
                            refs, target_shard, target_store, &path, data, &md,
                        ).await
                    } else {
                        target_store.put(&path, PutPayload::from(data)).await
                            .map(|_| ())
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
                    };
                    if let Err(e) = write_result {
                        warn!(key, target_shard, error = %e, "failed to write replica");
                    } else {
                        replicated += 1;
                    }
                }
                Err(e) => {
                    warn!(key, source_shard, error = %e, "failed to read source object");
                }
            },
            Err(e) => {
                warn!(key, source_shard, error = %e, "failed to get source object");
            }
        }
    }

    // Replay delete markers: remove stale objects on the target shard
    // that should have been deleted while it was offline.
    let markers = cluster.list_delete_markers().await;
    for (key, deleted_at) in &markers {
        let path = object_store::path::Path::from(key.as_str());
        match target_store.head(&path).await {
            Ok(meta) => {
                if meta.last_modified <= *deleted_at {
                    // Object is older than the delete marker -- remove it.
                    if let Err(e) = target_store.delete(&path).await {
                        warn!(
                            key,
                            target_shard,
                            error = %e,
                            "failed to delete stale object during marker replay"
                        );
                    } else {
                        info!(key, target_shard, "deleted stale object via marker replay");
                    }
                }
                // If object is newer, it was re-PUT after deletion -- keep it.
            }
            Err(_) => {
                // Object does not exist on target -- nothing to do.
            }
        }
    }

    // Update stale objects: if the target shard holds an old version of
    // an object that was re-PUT while the shard was offline, replace it
    // with the current version from a healthy source.  If no source is
    // available, delete the stale copy so it does not get re-added to
    // the catalog during attach_shard (the replication sweeper will
    // restore RF later).
    let target_objects: Vec<ObjectMeta> =
        target_store.list(None).try_collect().await.unwrap_or_default();
    for meta in &target_objects {
        let key = meta.location.to_string();
        // Skip delete markers -- they are handled above.
        if key.starts_with(crate::DELETE_MARKER_PREFIX) {
            continue;
        }
        let entry = match cluster.placement(&key) {
            Some(e) => e,
            None => {
                // Object exists on shard but NOT in the catalog at all.
                // It was fully replaced or deleted while offline.  Remove
                // it so attach_shard does not resurrect it.
                let _ = target_store.delete(&meta.location).await;
                continue;
            }
        };
        // Compare the shard's last_modified against the catalog's updated
        // timestamp.  If the catalog entry is newer, the object was
        // re-PUT while this shard was offline.
        if meta.last_modified < entry.updated {
            // Find a healthy source to get the current version.
            let source_shard = entry.shards.iter().copied().find(|&sid| {
                sid != target_shard
                    && cluster.shard_health(sid) == Some(ShardHealth::Healthy)
            });
            match source_shard {
                Some(sid) => {
                    if let Some(source_store) = cluster.shard_store(sid) {
                        match source_store.get(&meta.location).await {
                            Ok(result) => {
                                if let Ok(data) = result.bytes().await {
                                    if let Some(refs) = raw_refs {
                                        let md = read_source_metadata(
                                            cluster, refs, &meta.location,
                                        ).await;
                                        let _ = write_with_meta_to_shard(
                                            refs, target_shard, target_store,
                                            &meta.location, data, &md,
                                        ).await;
                                    } else {
                                        let _ = target_store
                                            .put(&meta.location, PutPayload::from(data))
                                            .await;
                                    }
                                }
                            }
                            Err(_) => {
                                // Cannot read from source -- delete stale copy.
                                let _ = target_store.delete(&meta.location).await;
                            }
                        }
                    }
                }
                None => {
                    // No healthy source -- delete the stale copy.
                    let _ = target_store.delete(&meta.location).await;
                }
            }
        }
    }

    // Clean up stale delete markers on the target shard.  A marker is
    // stale when:
    //   (a) the cluster no longer has the marker (already vacuumed on
    //       healthy shards while this shard was offline), or
    //   (b) a live object exists whose last_modified > marker timestamp
    //       (the object was re-PUT after deletion).
    for meta in &target_objects {
        let key = meta.location.to_string();
        if !key.starts_with(crate::DELETE_MARKER_PREFIX) {
            continue;
        }
        let original_key = match key.strip_prefix(crate::DELETE_MARKER_PREFIX) {
            Some(k) => k,
            None => continue,
        };

        // Read the marker timestamp from the target shard.
        let deleted_at: DateTime<Utc> = match target_store.get(&meta.location).await {
            Ok(result) => match result.bytes().await {
                Ok(data) => match std::str::from_utf8(&data)
                    .ok()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                {
                    Some(ts) => ts,
                    None => continue,
                },
                Err(_) => continue,
            },
            Err(_) => continue,
        };

        // Check if the cluster still has a marker for this key.
        let cluster_marker = cluster.get_delete_marker(original_key).await;
        if cluster_marker.is_none() {
            // Cluster no longer has the marker (vacuumed) -- remove
            // from target so it does not linger.
            let _ = target_store.delete(&meta.location).await;
            continue;
        }

        // Cluster still has the marker.  Check if a live object
        // exists that is newer (re-PUT after deletion).
        let live_path = object_store::path::Path::from(original_key);
        if let Ok(live_meta) = cluster.head(&live_path).await {
            if live_meta.last_modified > deleted_at {
                // Live object is newer -- marker is stale.
                let _ = target_store.delete(&meta.location).await;
            }
        }
    }

    Ok(replicated)
}

// -- Proactive re-replication ----------------------------------------

/// Scan for under-replicated objects and copy them to alternate healthy
/// shards.  Only runs when at least one shard is offline and does not
/// have `suppress_replication` set.  This restores RF even if the dead
/// shard never comes back.
///
/// Returns the number of objects successfully re-replicated this sweep.
pub async fn re_replication_sweep(
    cluster: &ShardedObjectStore,
    batch_size: usize,
    raw_refs: Option<&RawRefRegistry>,
) -> usize {
    // Check whether any shard is offline/degraded and eligible for
    // re-replication.  Shards with suppress_replication=true (manually
    // detached with the flag) are skipped -- the operator intends to
    // bring them back soon.
    let mut any_eligible = false;
    for sid in 0..cluster.shard_count() {
        if cluster.shard_suppress_replication(sid) {
            continue;
        }
        let health = cluster.shard_health(sid);
        if matches!(health, Some(h) if h.is_unavailable() || h == ShardHealth::Degraded) {
            any_eligible = true;
            break;
        }
    }
    if !any_eligible {
        return 0;
    }

    let under = cluster.find_under_replicated();
    if under.is_empty() {
        return 0;
    }

    info!(
        under_replicated = under.len(),
        batch_size,
        "starting re-replication sweep"
    );

    let mut replicated: usize = 0;

    for (key, current_count) in under.iter().take(batch_size) {
        // Find a healthy source shard that holds this object.
        let entry = match cluster.placement(key) {
            Some(e) => e,
            None => continue,
        };

        let source_shard = match entry.shards.iter().copied().find(|&sid| {
            cluster.shard_health(sid) == Some(ShardHealth::Healthy)
        }) {
            Some(s) => s,
            None => {
                warn!(
                    key,
                    current_count,
                    "no healthy source for re-replication"
                );
                continue;
            }
        };

        // Find a healthy target shard that does NOT already hold it.
        let target_shard = match cluster.find_replication_target(key) {
            Some(t) => t,
            None => {
                warn!(
                    key,
                    current_count,
                    "no available target for re-replication"
                );
                continue;
            }
        };

        match cluster.replicate_object(key, source_shard, target_shard, raw_refs).await {
            Ok(size) => {
                debug!(
                    key,
                    from = source_shard,
                    to = target_shard,
                    size,
                    "re-replicated object"
                );
                replicated += 1;
            }
            Err(e) => {
                warn!(
                    key,
                    from = source_shard,
                    to = target_shard,
                    error = %e,
                    "re-replication failed"
                );
            }
        }
    }

    if replicated > 0 {
        info!(
            replicated,
            remaining = under.len().saturating_sub(replicated),
            "re-replication sweep complete"
        );
    }

    replicated
}

// -- Over-replication trimming ---------------------------------------

/// Scan for over-replicated objects and remove excess replicas from the
/// fullest healthy shards.  This restores the correct RF by shedding
/// copies from the most constrained shards first.
///
/// Returns the number of excess replicas successfully removed.
pub async fn over_replication_trim(
    cluster: &ShardedObjectStore,
    batch_size: usize,
) -> usize {
    let over = cluster.find_over_replicated();
    if over.is_empty() {
        return 0;
    }

    info!(
        over_replicated = over.len(),
        batch_size,
        "starting over-replication trim"
    );

    let mut trimmed: usize = 0;

    for (key, current_count) in over.iter().take(batch_size) {
        let excess_shard = match cluster.pick_excess_shard(key) {
            Some(s) => s,
            None => continue,
        };

        match cluster.remove_replica(key, excess_shard).await {
            Ok(()) => {
                debug!(
                    key,
                    from = excess_shard,
                    was = current_count,
                    "trimmed excess replica"
                );
                trimmed += 1;
            }
            Err(e) => {
                warn!(
                    key,
                    shard = excess_shard,
                    error = %e,
                    "over-replication trim failed"
                );
            }
        }
    }

    if trimmed > 0 {
        info!(
            trimmed,
            remaining = over.len().saturating_sub(trimmed),
            "over-replication trim complete"
        );
    }

    trimmed
}

// -- Drain shard -----------------------------------------------------

/// Result of draining a shard before removal.
#[derive(Debug, Default)]
pub struct DrainReport {
    /// Objects moved (or already present on survivors).
    pub moved: usize,
    /// Objects skipped because survivors already have a copy.
    pub skipped: usize,
    /// Objects that failed to drain.
    pub errors: usize,
    /// Objects deleted from the victim shard after drain.
    pub deleted: usize,
    /// Objects that failed to delete from the victim shard.
    pub delete_errors: usize,
    /// Objects re-replicated to restore replication factor.
    pub re_replicated: usize,
    /// Objects still under-replicated after the sweep.
    pub under_remaining: usize,
}

/// Drain all objects from a victim shard to a survivor cluster.
///
/// For each object on the victim shard:
/// - If the original cluster's catalog shows a replica already exists
///   on a non-victim shard, skip (already safe).
/// - Otherwise, read from the victim store and write to the survivor
///   cluster, which will place it on healthy surviving shards.
///
/// `cluster` is the original full cluster (including victim) with a
/// current catalog.  `survivor_cluster` is a cluster of just the
/// surviving shards.  `victim_shard_id` is the index into the original
/// cluster.  `victim_store` is the ObjectStore backing the victim.
///
/// When `raw_refs` is provided, metadata (TLV suffix on raw shards,
/// sidecar files, or S3 attributes) is preserved during the drain.
pub async fn drain_shard(
    cluster: &ShardedObjectStore,
    survivor_cluster: &ShardedObjectStore,
    victim_shard_id: ShardId,
    victim_store: &Arc<dyn ObjectStore>,
    raw_refs: Option<&RawRefRegistry>,
    progress: Option<&ProgressSink>,
) -> DrainReport {
    let victim_files: Vec<ObjectMeta> = victim_store
        .list(None)
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|m| !m.location.as_ref().ends_with(".__meta__"))
        .collect();

    let mut report = DrainReport::default();
    // Track which objects are safe to delete (moved or already replicated).
    let mut safe_to_delete: Vec<&ObjectMeta> = Vec::new();

    for meta in &victim_files {
        let key = meta.location.to_string();

        // Check if survivors already have a copy (from replication).
        if let Some(entry) = cluster.placement(&key) {
            let survivor_has_copy = entry
                .shards
                .iter()
                .any(|&s| s != victim_shard_id);
            if survivor_has_copy {
                report.skipped += 1;
                safe_to_delete.push(meta);
                continue;
            }
        }

        // Read from victim, capturing S3 attributes for metadata.
        match victim_store.get(&meta.location).await {
            Ok(get_result) => {
                let attributes = get_result.attributes.clone();
                match get_result.bytes().await {
                    Ok(data) => {
                        // Read metadata from the victim shard.
                        let meta_bytes: Vec<u8> = if let Some(refs) = raw_refs {
                            read_metadata_from_shard(
                                victim_store.as_ref(), victim_shard_id,
                                &meta.location, &attributes, refs,
                            ).await.unwrap_or_default()
                        } else {
                            Vec::new()
                        };

                        // Write to survivor cluster with metadata if available.
                        let write_result = if !meta_bytes.is_empty() {
                            if let Some(survivor_refs) = survivor_cluster.raw_refs() {
                                crate::metadata::put_with_meta(
                                    survivor_cluster, &survivor_refs,
                                    &meta.location, data, &meta_bytes,
                                ).await.map_err(|e| object_store::Error::Generic {
                                    store: "drain",
                                    source: Box::new(e),
                                })
                            } else {
                                survivor_cluster.put(
                                    &meta.location,
                                    object_store::PutPayload::from(data),
                                ).await.map(|_| ())
                            }
                        } else {
                            survivor_cluster.put(
                                &meta.location,
                                object_store::PutPayload::from(data),
                            ).await.map(|_| ())
                        };

                        match write_result {
                            Ok(()) => {
                                debug!(key, victim = victim_shard_id, "drain: moved object");
                                if let Some(tx) = progress {
                                    let _ = tx.send(format!("drain: moved {key} from shard {victim_shard_id}"));
                                }
                                report.moved += 1;
                                safe_to_delete.push(meta);
                            }
                            Err(e) => {
                                warn!(
                                    key,
                                    error = %e,
                                    "drain: failed to write to survivor cluster"
                                );
                                if let Some(tx) = progress {
                                    let _ = tx.send(format!("drain: FAILED to move {key}: {e}"));
                                }
                                report.errors += 1;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(key, error = %e, "drain: failed to read bytes");
                        report.errors += 1;
                    }
                }
            }
            Err(e) => {
                warn!(key, error = %e, "drain: failed to get from victim");
                report.errors += 1;
            }
        }
    }

    // Purge all catalog references to the victim shard.  Moved objects
    // were written to survivors (which updated the catalog to point at
    // the new shard), but the old victim entry remains.  Skipped objects
    // still have the victim in their shard list.  Clean up both.
    let purged = cluster.catalog().remove_all_for_shard(victim_shard_id);
    if purged > 0 {
        info!(victim = victim_shard_id, purged, "drain: purged catalog refs");
    }

    // Delete safe objects (and sidecar files) from the victim shard.
    // Only objects that were successfully moved or confirmed replicated
    // are deleted.  Objects that errored are left on the victim to
    // prevent data loss.
    for meta in &safe_to_delete {
        let key = meta.location.to_string();
        match victim_store.delete(&meta.location).await {
            Ok(()) => {
                debug!(key, victim = victim_shard_id, "drain: deleted from victim");
                if let Some(tx) = progress {
                    let _ = tx.send(format!("drain: deleted {key} from shard {victim_shard_id}"));
                }
                report.deleted += 1;
            }
            Err(e) => {
                warn!(key, error = %e, "drain: failed to delete from victim");
                report.delete_errors += 1;
            }
        }
        // Also delete the sidecar metadata file if it exists.
        crate::metadata::cleanup_sidecar_maybe(
            victim_store.as_ref(), &meta.location, raw_refs, victim_shard_id,
        ).await;
    }

    if report.moved > 0 || report.skipped > 0 || report.deleted > 0 {
        info!(
            moved = report.moved,
            skipped = report.skipped,
            errors = report.errors,
            deleted = report.deleted,
            delete_errors = report.delete_errors,
            victim = victim_shard_id,
            "shard drain complete"
        );
    }

    // Run a repair-replication sweep to restore RF on surviving shards.
    // After the drain, objects that were on the victim + other shards
    // now have one fewer replica.  The sweep copies them to other
    // healthy shards to bring them back up to the target RF.
    let sweep = repair_replication_sweep(
        cluster,
        usize::MAX,
        raw_refs,
        progress,
    ).await;
    report.re_replicated = sweep.re_replicated;
    report.under_remaining = sweep.under_remaining;
    if sweep.re_replicated > 0 || sweep.under_remaining > 0 {
        info!(
            re_replicated = sweep.re_replicated,
            under_remaining = sweep.under_remaining,
            "drain: repair-replication sweep complete"
        );
    }

    report
}

// -- High-level repair-replication -----------------------------------

/// Run one full repair-replication cycle: repair under-replicated
/// objects, then trim over-replicated ones.
///
/// Unlike the daemon's recovery loop this runs unconditionally (no
/// grace period).  The caller decides when to invoke it.
pub async fn repair_replication_sweep(
    cluster: &ShardedObjectStore,
    batch_size: usize,
    raw_refs: Option<&RawRefRegistry>,
    progress: Option<&ProgressSink>,
) -> RepairReplicationResult {
    let mut result = RepairReplicationResult::default();

    // Phase 1: fix under-replicated objects.
    let under = cluster.find_under_replicated();
    if !under.is_empty() {
        info!(
            under_replicated = under.len(),
            batch_size,
            "repair-replication: repairing under-replicated objects"
        );
        for (key, current_count) in under.iter().take(batch_size) {
            // Replicate until RF is met or no more targets are available.
            // Each iteration adds one replica; we may need several to
            // reach RF (e.g., object has 2 copies but RF is 4).
            loop {
                let entry = match cluster.placement(key) {
                    Some(e) => e,
                    None => break,
                };
                let healthy_count = entry.shards.iter()
                    .filter(|&&sid| cluster.shard_health(sid) == Some(ShardHealth::Healthy)
                                 || cluster.shard_health(sid) == Some(ShardHealth::Syncing))
                    .count();
                if healthy_count >= cluster.replication_factor() {
                    break;
                }
                let source_shard = match entry.shards.iter().copied().find(|&sid| {
                    cluster.shard_health(sid) == Some(ShardHealth::Healthy)
                }) {
                    Some(s) => s,
                    None => {
                        warn!(key, current_count, "no healthy source for repair-replication");
                        break;
                    }
                };
                let target_shard = match cluster.find_replication_target(key) {
                    Some(t) => t,
                    None => {
                        warn!(key, current_count, "no available target for repair-replication");
                        break;
                    }
                };
                match cluster.replicate_object(key, source_shard, target_shard, raw_refs).await {
                    Ok(size) => {
                        debug!(key, from = source_shard, to = target_shard, size, "repair-replication: replicated");
                        if let Some(tx) = progress {
                            let _ = tx.send(format!("repair: replicated {key} from shard {source_shard} to shard {target_shard} ({size} bytes)"));
                        }
                        result.re_replicated += 1;
                    }
                    Err(e) => {
                        warn!(key, from = source_shard, to = target_shard, error = %e, "repair-replication failed");
                        if let Some(tx) = progress {
                            let _ = tx.send(format!("repair: FAILED {key} from shard {source_shard} to shard {target_shard}: {e}"));
                        }
                        break;
                    }
                }
            }
        }
    }

    // Phase 2: trim over-replicated objects.
    let over = cluster.find_over_replicated();
    if !over.is_empty() {
        info!(
            over_replicated = over.len(),
            batch_size,
            "repair-replication: trimming over-replicated objects"
        );
        for (key, current_count) in over.iter().take(batch_size) {
            // Trim until at RF or no more excess shards.
            loop {
                let excess_shard = match cluster.pick_excess_shard(key) {
                    Some(s) => s,
                    None => break,
                };
                match cluster.remove_replica(key, excess_shard).await {
                    Ok(()) => {
                        debug!(key, from = excess_shard, was = current_count, "trimmed");
                        if let Some(tx) = progress {
                            let _ = tx.send(format!("repair: trimmed {key} from shard {excess_shard}"));
                        }
                        result.trimmed += 1;
                    }
                    Err(e) => {
                        warn!(key, shard = excess_shard, error = %e, "trim failed");
                        if let Some(tx) = progress {
                            let _ = tx.send(format!("repair: trim FAILED {key} shard {excess_shard}: {e}"));
                        }
                        break;
                    }
                }
            }
        }
    }

    // Report remaining counts (re-check after the sweep).
    result.under_remaining = cluster.find_under_replicated().len();
    result.over_remaining = cluster.find_over_replicated().len();

    if result.re_replicated > 0 || result.trimmed > 0 {
        info!(
            re_replicated = result.re_replicated,
            trimmed = result.trimmed,
            under_remaining = result.under_remaining,
            over_remaining = result.over_remaining,
            "repair-replication sweep complete"
        );
    }

    result
}

// -- Dry-run repair-replication plan ---------------------------------

/// Compute what a `repair_replication_sweep` would do without performing
/// any I/O.
///
/// Returns a `RepairReplicationPlan` listing every copy and trim
/// operation that would be executed, along with counts of objects that
/// cannot be repaired (no healthy source / no available target).
pub fn plan_repair_replication(
    cluster: &ShardedObjectStore,
    batch_size: usize,
) -> RepairReplicationPlan {
    let rf = cluster.replication_factor();
    let mut plan = RepairReplicationPlan::default();

    // Phase 1: under-replicated objects.
    let under = cluster.find_under_replicated();
    for (key, current_count) in under.iter().take(batch_size) {
        let entry = match cluster.placement(key) {
            Some(e) => e,
            None => continue,
        };
        let source_shard = match entry.shards.iter().copied().find(|&sid| {
            cluster.shard_health(sid) == Some(ShardHealth::Healthy)
        }) {
            Some(s) => s,
            None => {
                plan.unrepairable += 1;
                continue;
            }
        };
        let target_shard = match cluster.find_replication_target(key) {
            Some(t) => t,
            None => {
                plan.unrepairable += 1;
                continue;
            }
        };
        plan.replications.push(PlannedAction {
            key: key.clone(),
            current_count: *current_count,
            target_rf: rf,
            action: PlannedActionKind::Replicate {
                source_shard,
                target_shard,
            },
        });
    }

    // Phase 2: over-replicated objects.
    let over = cluster.find_over_replicated();
    for (key, current_count) in over.iter().take(batch_size) {
        let excess_shard = match cluster.pick_excess_shard(key) {
            Some(s) => s,
            None => {
                plan.untrimmable += 1;
                continue;
            }
        };
        plan.trims.push(PlannedAction {
            key: key.clone(),
            current_count: *current_count,
            target_rf: rf,
            action: PlannedActionKind::Trim {
                shard: excess_shard,
            },
        });
    }

    plan
}

// -- Redistribute (balance object counts across shards) ---------------

/// Move objects from the fullest shards to the least full to even out
/// object counts across healthy shards.
///
/// For each object moved, the function first replicates it to the
/// target shard (so the object temporarily has RF+1 copies), verifies
/// that the replication factor is still satisfied, and only then removes
/// the replica from the source shard.  This guarantees the object never
/// drops below RF during the move.
///
/// The sweep stops when either `batch_size` objects have been moved or
/// the imbalance `(max - min) / mean` drops to `tolerance_pct` or below.
///
/// **Pre-condition:** The caller should ensure no objects are
/// under-replicated before calling this function. If any exist the
/// function returns immediately with `moved = 0`.
pub async fn redistribute_sweep(
    cluster: &ShardedObjectStore,
    batch_size: usize,
    tolerance_pct: f64,
    raw_refs: Option<&RawRefRegistry>,
    progress: Option<&ProgressSink>,
) -> RedistributeResult {
    let mut result = RedistributeResult::default();
    let rf = cluster.replication_factor();

    // Refuse to run if RF is not met -- caller must fix replication first.
    let under = cluster.find_under_replicated();
    if !under.is_empty() {
        warn!(
            under_replicated = under.len(),
            "redistribute: aborting -- under-replicated objects exist; run repair-replication first"
        );
        result.shard_counts = cluster.shard_object_counts();
        return result;
    }

    // Build per-shard object counts for healthy shards only.
    let mut counts: Vec<(ShardId, usize)> = cluster.shard_object_counts();
    if counts.len() < 2 {
        info!("redistribute: fewer than 2 healthy shards, nothing to do");
        result.shard_counts = counts;
        return result;
    }

    let total: usize = counts.iter().map(|(_, c)| *c).sum();
    if total == 0 {
        result.shard_counts = counts;
        return result;
    }

    let mean = total as f64 / counts.len() as f64;

    // Check if already within tolerance.
    let max_c = counts.iter().map(|(_, c)| *c).max().unwrap_or(0);
    let min_c = counts.iter().map(|(_, c)| *c).min().unwrap_or(0);
    if mean > 0.0 && (max_c - min_c) as f64 / mean <= tolerance_pct {
        info!(
            max = max_c, min = min_c, mean = %format!("{:.1}", mean),
            tolerance_pct,
            "redistribute: already balanced within tolerance"
        );
        result.shard_counts = counts;
        return result;
    }

    info!(
        total, mean = %format!("{:.1}", mean), max = max_c, min = min_c,
        tolerance_pct, batch_size,
        "redistribute: starting sweep"
    );

    // Build a set of healthy shard IDs for quick lookup.
    let healthy_set: HashSet<ShardId> = counts.iter().map(|(sid, _)| *sid).collect();

    // Work loop: move objects from fullest to emptiest.
    let mut moved = 0usize;
    while moved < batch_size {
        // Re-sort: pick the fullest shard as source.
        counts.sort_by(|a, b| b.1.cmp(&a.1));
        let (src_shard, src_count) = counts[0];
        let (dst_shard, dst_count) = *counts.last().unwrap();

        // Check convergence: if imbalance is within tolerance, stop.
        let cur_max = src_count;
        let cur_min = dst_count;
        if mean > 0.0 && (cur_max - cur_min) as f64 / mean <= tolerance_pct {
            break;
        }
        // Also stop if the source has at most 1 more than the dest
        // (cannot improve further).
        if src_count <= dst_count + 1 {
            break;
        }

        // Pick an object on the source shard that does NOT already
        // exist on the destination shard.
        let candidates = cluster.catalog().entries_for_shard(src_shard);
        let mut found = false;
        for (key, entry) in &candidates {
            // Skip if the object is already on the destination.
            if entry.shards.contains(&dst_shard) {
                continue;
            }
            // After replicate to dst: healthy copies excluding src =
            // (current healthy copies - 1 for src) + 1 for dst.
            // We need that to be >= rf so removal of src is safe.
            let healthy_replicas: usize = entry.shards.iter()
                .filter(|&&s| s != src_shard && healthy_set.contains(&s))
                .count();
            if healthy_replicas + 1 < rf {
                continue;
            }

            // Step 1: Replicate to destination.
            match cluster.replicate_object(key, src_shard, dst_shard, raw_refs).await {
                Ok(size) => {
                    // Step 2: Verify RF is met after replication.
                    let ok = cluster.placement(key)
                        .map(|e| {
                            e.shards.iter()
                                .filter(|&&s| healthy_set.contains(&s))
                                .count() >= rf
                        })
                        .unwrap_or(false);
                    if !ok {
                        warn!(key, "redistribute: RF not met after replication, skipping removal");
                        result.skipped += 1;
                        found = true;
                        break;
                    }

                    // Step 3: Remove from source.
                    match cluster.remove_replica(key, src_shard).await {
                        Ok(()) => {
                            debug!(key, from = src_shard, to = dst_shard, size, "redistribute: moved");
                            if let Some(tx) = progress {
                                let _ = tx.send(format!("redistribute: moved {key} from shard {src_shard} to shard {dst_shard} ({size} bytes)"));
                            }
                            result.moved += 1;
                            moved += 1;
                            // Update local counts.
                            for (sid, cnt) in counts.iter_mut() {
                                if *sid == src_shard { *cnt = cnt.saturating_sub(1); }
                                if *sid == dst_shard { *cnt += 1; }
                            }
                            found = true;
                            break;
                        }
                        Err(e) => {
                            warn!(key, shard = src_shard, error = %e, "redistribute: remove_replica failed");
                            if let Some(tx) = progress {
                                let _ = tx.send(format!("redistribute: FAILED {key} remove from shard {src_shard}: {e}"));
                            }
                            result.errors += 1;
                            found = true;
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(key, from = src_shard, to = dst_shard, error = %e, "redistribute: replicate failed");
                    if let Some(tx) = progress {
                        let _ = tx.send(format!("redistribute: FAILED {key} replicate from shard {src_shard} to shard {dst_shard}: {e}"));
                    }
                    result.errors += 1;
                    found = true;
                    break;
                }
            }
        }

        // If no movable candidate was found on the fullest shard,
        // break to avoid an infinite loop.
        if !found {
            info!(src_shard, "redistribute: no movable object found on fullest shard");
            break;
        }
    }

    // Final counts.
    result.shard_counts = cluster.shard_object_counts();

    if result.moved > 0 || result.errors > 0 {
        info!(
            moved = result.moved,
            skipped = result.skipped,
            errors = result.errors,
            "redistribute sweep complete"
        );
    }

    result
}
