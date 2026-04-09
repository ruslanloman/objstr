//! End-to-end tests for repair, sync, repair-replication, and verification operations.
//!
//! Covers: repair_replication_sweep (execution), mirror_sync, partitioned_sync,
//! sync_and_reattach, probe_store, re_replication_sweep,
//! over_replication_trim, verify_object, verify_all, plan_repair_replication,
//! validate_shard_access, find_replication_target, and pick_excess_shard.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::metadata::{
    get_metadata, put_with_meta, RawRefRegistry, ShardKind,
};
use shardedobjstr::repair;
use shardedobjstr::repair::PlannedActionKind;
use shardedobjstr::ShardedObjectStore;
use shardedobjstr::ShardHealth;

use common::{build_cluster, flush_all, format_shard, seed_objects};

// -- Helpers ---------------------------------------------------------

fn setup_3shard(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    (cluster, raws)
}

fn setup_2shard_mirror(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2); // RF == shard_count => mirror mode
    (cluster, raws)
}

fn put_many_objects(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    count: usize,
) {
    rt.block_on(async {
        for i in 0..count {
            let key = format!("batch/obj_{:04}.bin", i);
            let data = vec![(i & 0xFF) as u8; 512];
            cluster
                .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
        }
    });
}

fn setup_3shard_with_refs(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, RawRefRegistry) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = RawRefRegistry::new(refs, kinds);
    (cluster, raws, registry)
}

// =====================================================================
// repair_replication_sweep tests
// =====================================================================

#[test]
fn repair_replication_sweep_restores_rf() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Detach shard 0 to create under-replication
    cluster.detach_shard(0);
    let under_before = cluster.find_under_replicated().len();
    assert!(under_before > 0, "some objects should be under-replicated");

    // Run repair-replication sweep
    let result = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));
    assert!(
        result.re_replicated > 0,
        "sweep should have re-replicated some objects"
    );
    assert_eq!(
        result.under_remaining, 0,
        "no under-replicated objects should remain"
    );

    // Idempotent: second sweep should do nothing
    let result2 = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));
    assert_eq!(result2.re_replicated, 0, "second sweep should be a no-op");
    assert_eq!(result2.under_remaining, 0);
}

#[test]
fn repair_replication_sweep_trims_over_replicated() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Manually create over-replication by replicating to the third shard
    rt.block_on(async {
        let under_rep = cluster.find_under_replicated();
        assert!(under_rep.is_empty(), "cluster should be balanced initially");

        // Pick an object and replicate it to a shard that doesn't hold it
        let key = "data/file-a.bin";
        let placement = cluster.placement(key).unwrap();
        assert_eq!(placement.shards.len(), 2, "RF=2 initially");

        // Find the shard that does NOT hold this object
        let target: usize = (0..3)
            .find(|s| !placement.shards.contains(s))
            .expect("should find a non-holding shard");

        cluster
            .replicate_object(key, placement.shards[0], target, None)
            .await
            .unwrap();
    });

    // Confirm over-replication
    let over = cluster.find_over_replicated();
    assert!(over.len() > 0, "should have at least one over-replicated object");

    // Sweep should trim
    let result = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));
    assert!(result.trimmed > 0, "sweep should have trimmed excess replicas");
    assert_eq!(result.over_remaining, 0, "no over-replicated should remain");

    // Verify all objects are exactly RF=2
    let all_entries = cluster.catalog().all_entries();
    for (key, entry) in &all_entries {
        assert_eq!(
            entry.shards.len(),
            2,
            "object {} should have exactly 2 replicas, got {}",
            key,
            entry.shards.len()
        );
    }
}

#[test]
fn repair_replication_sweep_batch_size_limits() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put 25 objects (more than batch_size of 5)
    put_many_objects(&rt, &cluster, 25);
    flush_all(&raws);

    // Detach shard to create under-replication
    cluster.detach_shard(0);
    let under_total = cluster.find_under_replicated().len();
    assert!(under_total > 5, "need more than 5 under-replicated for batch test");

    // Sweep with batch_size=5
    let result = rt.block_on(repair::repair_replication_sweep(&cluster, 5, None, None));
    assert!(
        result.re_replicated <= 5,
        "should not exceed batch_size; got {}",
        result.re_replicated
    );
    assert!(
        result.under_remaining > 0,
        "should still have remaining under-replicated objects"
    );
}

// =====================================================================
// mirror_sync tests (RF == shard_count)
// =====================================================================

#[test]
fn mirror_sync_copies_missing_and_deletes_stale() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_2shard_mirror(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed initial objects on both shards
    rt.block_on(async {
        for key in &["mirror/a.bin", "mirror/b.bin", "mirror/c.bin"] {
            cluster
                .put(
                    &Path::from(*key),
                    PutPayload::from(Bytes::from(vec![0x11u8; 1024])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Build original_stores for mirror_sync (needs direct store refs)
    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // Detach shard 1
    cluster.detach_shard(1);

    // Put new object while shard 1 offline (only lands on shard 0)
    rt.block_on(async {
        cluster
            .put(
                &Path::from("mirror/new_while_offline.bin"),
                PutPayload::from(Bytes::from(vec![0x22u8; 512])),
            )
            .await
            .unwrap();

        // Delete an object while shard 1 offline
        // (removed from shard 0 + catalog, stale copy remains on shard 1)
        cluster.delete(&Path::from("mirror/b.bin")).await.unwrap();
    });
    flush_all(&raws);

    // Run mirror_sync to recover shard 1
    let report = rt
        .block_on(repair::mirror_sync(&cluster, &original_stores, 1, None))
        .unwrap();

    assert!(
        report.copied >= 1,
        "should have copied at least the new object; got {}",
        report.copied
    );
    assert!(
        report.deleted >= 1,
        "should have deleted stale mirror/b.bin; got {}",
        report.deleted
    );
}

// =====================================================================
// partitioned_sync tests (RF < shard_count)
// =====================================================================

#[test]
fn partitioned_sync_repairs_under_replicated() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // Detach shard 1 to create under-replicated objects
    cluster.detach_shard(1);

    let under_before = cluster.find_under_replicated().len();
    assert!(under_before > 0);

    // Run partitioned_sync targeting shard 1
    let count = rt
        .block_on(repair::partitioned_sync(&cluster, &original_stores, 1, None))
        .unwrap();

    assert!(count > 0, "partitioned_sync should have replicated objects");
}

#[test]
fn partitioned_sync_restores_detached_shard() {
    // Verify that partitioned_sync re-replicates objects onto a shard that was
    // detached, using the original_stores handles from before detach.
    // Also documents that detach does NOT remove catalog entries: the catalog
    // still records shard 0 as a placement after detach, which is correct
    // -- the data is physically still there, and sync restores access to it.
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed objects.
    rt.block_on(async {
        for i in 0..5 {
            let key = format!("sync/obj_{i:03}.bin");
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![(i as u8) + 1; 512])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Capture shard 0's store handle before detaching.
    let original_store = cluster.shard_store(0).unwrap();
    cluster.detach_shard(0);

    let under = cluster.find_under_replicated();
    assert!(!under.is_empty(), "should have under-replicated objects after detach");

    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        vec![Some(original_store), None, None];

    let replicated = rt
        .block_on(repair::partitioned_sync(&cluster, &original_stores, 0, None))
        .unwrap();

    assert!(replicated > 0, "partitioned_sync should replicate objects");
}

// =====================================================================
// sync_and_reattach tests
// =====================================================================

#[test]
fn sync_and_reattach_state_transitions() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // Detach shard 1
    cluster.detach_shard(1);
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Offline));

    // sync_and_reattach should transition: Offline -> Syncing -> Healthy
    rt.block_on(repair::sync_and_reattach(
        &cluster,
        &original_stores,
        1,
        None,
    ));

    assert_eq!(
        cluster.shard_health(1),
        Some(ShardHealth::Healthy),
        "shard should be healthy after sync_and_reattach"
    );

    // Catalog should have entries for the reattached shard
    let entries = cluster.catalog().entries_for_shard(1);
    assert!(!entries.is_empty(), "catalog should contain shard 1 entries");
}

#[test]
fn sync_and_reattach_mirror_vs_partitioned() {
    // Mirror mode: 2 shards, RF=2 (rf == shard_count)
    let dir = tempfile::tempdir().unwrap();
    let (cluster_m, raws_m) = setup_2shard_mirror(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster_m
            .put(&Path::from("mp/obj.bin"), PutPayload::from(Bytes::from(vec![1u8; 512])))
            .await
            .unwrap();
    });
    flush_all(&raws_m);

    let orig_m: Vec<Option<Arc<dyn ObjectStore>>> =
        raws_m.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    cluster_m.detach_shard(1);
    rt.block_on(repair::sync_and_reattach(&cluster_m, &orig_m, 1, None));
    assert_eq!(cluster_m.shard_health(1), Some(ShardHealth::Healthy));

    // Partitioned mode: 3 shards, RF=2 (rf < shard_count)
    let dir2 = tempfile::tempdir().unwrap();
    let (cluster_p, raws_p) = setup_3shard(&dir2);

    rt.block_on(async {
        cluster_p
            .put(&Path::from("pp/obj.bin"), PutPayload::from(Bytes::from(vec![2u8; 512])))
            .await
            .unwrap();
    });
    flush_all(&raws_p);

    let orig_p: Vec<Option<Arc<dyn ObjectStore>>> =
        raws_p.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    cluster_p.detach_shard(1);
    rt.block_on(repair::sync_and_reattach(&cluster_p, &orig_p, 1, None));
    assert_eq!(cluster_p.shard_health(1), Some(ShardHealth::Healthy));
}

// =====================================================================
// probe_store tests
// =====================================================================

#[test]
fn probe_store_healthy_returns_true() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raw = format_shard(&dir.path().join("probe.raw"), sz);
    let store: Arc<dyn ObjectStore> = raw as Arc<dyn ObjectStore>;
    let rt = tokio::runtime::Runtime::new().unwrap();

    let result = rt.block_on(repair::probe_store(&store, Duration::from_secs(5)));
    assert!(result, "healthy store should be probe-able");
}

// =====================================================================
// re_replication_sweep tests
// =====================================================================

#[test]
fn re_replication_sweep_after_detach() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Detach shard 0
    cluster.detach_shard(0);

    // Sweep should replicate immediately
    let count = rt.block_on(repair::re_replication_sweep(
        &cluster,
        100,
        None,
    ));
    assert!(count > 0, "should re-replicate immediately after detach");
}

// =====================================================================
// over_replication_trim isolation
// =====================================================================

#[test]
fn over_replication_trim_removes_excess() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Manually over-replicate one object
    rt.block_on(async {
        let key = "data/file-a.bin";
        let placement = cluster.placement(key).unwrap();
        let target: usize = (0..3)
            .find(|s| !placement.shards.contains(s))
            .expect("find non-holding shard");
        cluster
            .replicate_object(key, placement.shards[0], target, None)
            .await
            .unwrap();
    });

    let over = cluster.find_over_replicated();
    assert!(!over.is_empty());

    let trimmed = rt.block_on(repair::over_replication_trim(&cluster, 100));
    assert!(trimmed > 0, "should have trimmed at least one excess replica");
    assert!(
        cluster.find_over_replicated().is_empty(),
        "no over-replicated objects should remain"
    );
}

// =====================================================================
// verify_object / verify_all tests
// =====================================================================

#[test]
fn verify_object_all_replicas_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);

    rt.block_on(async {
        let report = cluster.verify_object("batch/obj_0000.bin").await.unwrap();
        assert!(report.replicas_consistent);
        assert_eq!(report.replicas.len(), 2); // RF=2
        for r in &report.replicas {
            assert!(r.crc32c.is_some());
            assert!(r.error.is_none());
            assert_eq!(r.matches_catalog, Some(true));
        }
    });
}

#[test]
fn verify_object_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let result = cluster.verify_object("nonexistent").await;
        assert!(result.is_err());
    });
}

#[test]
fn verify_all_objects_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);

    rt.block_on(async {
        let report = cluster.verify_all(None).await;
        assert_eq!(report.objects_checked, 10);
        assert_eq!(report.objects_ok, 10);
        assert_eq!(report.objects_mismatched, 0);
        assert_eq!(report.objects_with_errors, 0);
        assert!(report.details.is_empty());
    });
}

#[test]
fn verify_all_with_prefix_filter() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(&Path::from("alpha/a.bin"), PutPayload::from(Bytes::from(vec![1u8; 100])))
            .await
            .unwrap();
        cluster
            .put(&Path::from("beta/b.bin"), PutPayload::from(Bytes::from(vec![2u8; 100])))
            .await
            .unwrap();
    });
    flush_all(&raws);

    rt.block_on(async {
        let report = cluster.verify_all(Some("alpha/")).await;
        assert_eq!(report.objects_checked, 1);
        assert_eq!(report.objects_ok, 1);
    });
}

#[test]
fn verify_object_with_offline_replica() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);

    // Detach shard 0 -- some objects will have one replica offline.
    cluster.detach_shard(0);

    rt.block_on(async {
        // Object that was on shard 0 should still verify (one replica readable).
        let report = cluster.verify_all(None).await;
        assert_eq!(report.objects_checked, 10);
        // All should be OK or have errors (offline placeholder errors), but no mismatches.
        assert_eq!(report.objects_mismatched, 0);
    });
}

// =====================================================================
// plan_repair_replication (dry-run) tests
// =====================================================================

#[test]
fn plan_repair_replication_empty_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_3shard(&dir);

    let plan = repair::plan_repair_replication(&cluster, 100);
    assert!(plan.replications.is_empty());
    assert!(plan.trims.is_empty());
    assert_eq!(plan.unrepairable, 0);
    assert_eq!(plan.untrimmable, 0);
}

#[test]
fn plan_repair_replication_detects_under_replicated() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);

    // Detach a shard to create under-replicated objects.
    cluster.detach_shard(0);

    let plan = repair::plan_repair_replication(&cluster, 100);
    // Some objects should be flagged for replication.
    let under_count = cluster.find_under_replicated().len();
    assert_eq!(plan.replications.len(), under_count);
    for action in &plan.replications {
        assert!(matches!(action.action, PlannedActionKind::Replicate { .. }));
    }
}

#[test]
fn plan_repair_replication_respects_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);

    cluster.detach_shard(0);

    let plan = repair::plan_repair_replication(&cluster, 2);
    // Should be capped at batch_size.
    assert!(plan.replications.len() <= 2);
}

// =====================================================================
// validate_shard_access tests
// =====================================================================

#[test]
fn validate_shard_access_all_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let results = rt.block_on(cluster.validate_shard_access());
    assert_eq!(results.len(), 3);
    for (_, accessible) in &results {
        assert!(accessible);
    }
}

#[test]
fn validate_shard_access_with_offline() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    cluster.detach_shard(1);

    let results = rt.block_on(cluster.validate_shard_access());
    assert_eq!(results.len(), 3);
    assert!(results[0].1);  // shard 0: accessible
    assert!(!results[1].1); // shard 1: offline
    assert!(results[2].1);  // shard 2: accessible
}

// =====================================================================
// find_replication_target / pick_excess_shard edge cases
// =====================================================================

#[test]
fn find_replication_target_excludes_holding_shards() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("target/obj.bin"),
                PutPayload::from(Bytes::from(vec![0xAAu8; 512])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    let placement = cluster.placement("target/obj.bin").unwrap();
    assert_eq!(placement.shards.len(), 2, "RF=2");

    // find_replication_target should return the one shard NOT holding the object
    let target = cluster.find_replication_target("target/obj.bin");
    assert!(target.is_some(), "should find a non-holding shard");
    let target_id = target.unwrap();
    assert!(
        !placement.shards.contains(&target_id),
        "target shard {} should not already hold object (held by {:?})",
        target_id,
        placement.shards
    );

    // Now replicate to the third shard so all 3 hold the object
    rt.block_on(async {
        cluster
            .replicate_object("target/obj.bin", placement.shards[0], target_id, None)
            .await
            .unwrap();
    });

    // With all shards holding the object, there should be no target
    let target_none = cluster.find_replication_target("target/obj.bin");
    assert!(
        target_none.is_none(),
        "no replication target when all shards hold the object"
    );
}

#[test]
fn pick_excess_shard_returns_valid_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("excess/obj.bin"),
                PutPayload::from(Bytes::from(vec![0xBBu8; 512])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Object is on 2 shards (RF=2). pick_excess_shard should return None
    // since it's at exactly RF.
    let excess = cluster.pick_excess_shard("excess/obj.bin");
    assert!(
        excess.is_none(),
        "at exactly RF, excess shard should be None"
    );

    // Over-replicate the object to all 3 shards
    let placement = cluster.placement("excess/obj.bin").unwrap();
    let extra: usize = (0..3)
        .find(|s| !placement.shards.contains(s))
        .unwrap();
    rt.block_on(async {
        cluster
            .replicate_object("excess/obj.bin", placement.shards[0], extra, None)
            .await
            .unwrap();
    });

    // Now pick_excess_shard should return one of the 3 shards
    let excess = cluster.pick_excess_shard("excess/obj.bin");
    assert!(
        excess.is_some(),
        "over-replicated object should have an excess shard"
    );
    let excess_id = excess.unwrap();
    // The excess shard should be one that holds the object
    let updated_placement = cluster.placement("excess/obj.bin").unwrap();
    assert!(
        updated_placement.shards.contains(&excess_id),
        "excess shard {} should be one holding the object",
        excess_id
    );
}

#[test]
fn pick_excess_shard_respects_free_space() {
    // When multiple shards all hold an object and one has the least free space,
    // pick_excess_shard should choose that shard as the one to trim.
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, _registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("excess/pick.bin"),
                PutPayload::from(Bytes::from(vec![0xAAu8; 512])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Over-replicate to all 3 shards.
    let placement = cluster.placement("excess/pick.bin").unwrap();
    let extra: usize = (0..3)
        .find(|s| !placement.shards.contains(s))
        .unwrap();
    rt.block_on(async {
        cluster
            .replicate_object("excess/pick.bin", placement.shards[0], extra, None)
            .await
            .unwrap();
    });

    // Give shard 1 the least free space so it is the preferred trim target.
    cluster.set_shard_free_space(0, 1_000_000);
    cluster.set_shard_free_space(1, 100);
    cluster.set_shard_free_space(2, 2_000_000);

    let excess = cluster.pick_excess_shard("excess/pick.bin");
    assert!(excess.is_some(), "should pick an excess shard");
    assert_eq!(
        excess.unwrap(),
        1,
        "should trim from shard 1 (least free space)"
    );
}

// =====================================================================
// Cascade failure: 3+ shards offline with RF=3, then recover
// =====================================================================

#[test]
fn cascade_failure_three_shards_offline_rf3() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..5)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed data
    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);

    // Offline shards 0, 1, 2 -- only shards 3 and 4 remain
    cluster.detach_shard(0);
    cluster.detach_shard(1);
    cluster.detach_shard(2);

    // With RF=3 on 5 shards, some objects may have all replicas on
    // offline shards. Check that we can still read objects that have
    // at least one replica on shards 3 or 4.
    let mut readable = 0usize;
    let mut unreadable = 0usize;
    rt.block_on(async {
        for i in 0..10 {
            let key = format!("batch/obj_{i:04}.bin");
            match cluster.get(&Path::from(key.as_str())).await {
                Ok(_) => readable += 1,
                Err(_) => unreadable += 1,
            }
        }
    });
    // At least some should be readable from surviving shards
    assert!(
        readable > 0,
        "at least some objects should be readable from surviving shards"
    );

    // Run repair-replication -- should re-replicate to shards 3 and 4 what it can
    let result = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));
    // Verify we have under-replicated objects since only 2 shards remain
    // but RF=3 requires 3 copies
    assert!(
        result.under_remaining > 0 || result.re_replicated > 0,
        "with only 2 healthy shards and RF=3, repair-replication should detect issues"
    );

    // Reattach all three shards and recover
    let orig: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    rt.block_on(async {
        repair::sync_and_reattach(&cluster, &orig, 0, None).await;
        repair::sync_and_reattach(&cluster, &orig, 1, None).await;
        repair::sync_and_reattach(&cluster, &orig, 2, None).await;
    });

    // All shards should be healthy again
    for i in 0..5 {
        assert_eq!(
            cluster.shard_health(i),
            Some(ShardHealth::Healthy),
            "shard {i} should be healthy after reattach"
        );
    }

    // Final repair-replication should fix everything
    let final_result = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));
    assert_eq!(
        final_result.under_remaining, 0,
        "no under-replicated objects should remain after full recovery"
    );
}

#[test]
fn cascade_failure_rf1_all_data_on_offline_shard_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 1); // RF=1, no redundancy
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_many_objects(&rt, &cluster, 6);
    flush_all(&raws);

    // Offline shards 0 and 1
    cluster.detach_shard(0);
    cluster.detach_shard(1);

    // Objects on shard 2 should still be readable; objects on 0 and 1 should not
    let mut readable = 0usize;
    rt.block_on(async {
        for i in 0..6 {
            let key = format!("batch/obj_{i:04}.bin");
            if cluster.get(&Path::from(key.as_str())).await.is_ok() {
                readable += 1;
            }
        }
    });

    // With RF=1 on 3 shards, ~1/3 should be on shard 2
    // (hash determines this -- some objects might all be on offline shards)
    assert!(
        readable < 6,
        "not all objects should be readable when 2/3 RF=1 shards are offline"
    );
}

// =====================================================================
// Drain shard with concurrent writes
// =====================================================================

#[test]
fn drain_shard_concurrent_with_writes() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed initial objects
    put_many_objects(&rt, &cluster, 10);
    flush_all(&raws);
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Build survivor cluster (shards 1, 2, 3)
    let survivor_stores: Vec<Arc<dyn ObjectStore>> =
        raws[1..].iter().map(|r| r.clone() as _).collect();
    let survivor = ShardedObjectStore::new(survivor_stores, 1);

    let cluster_arc = Arc::new(cluster);

    // Run drain concurrently with new writes
    let write_cluster = Arc::clone(&cluster_arc);
    rt.block_on(async {
        let drain_handle = tokio::spawn({
            let raws_0 = raws[0].clone() as Arc<dyn ObjectStore>;
            let cluster_ref = Arc::clone(&cluster_arc);
            async move {
                repair::drain_shard(&cluster_ref, &survivor, 0, &raws_0, None, None).await
            }
        });

        // Concurrent writes to keys that won't go to shard 0
        // (the drain is reading from shard 0)
        for i in 0..5 {
            let key = format!("concurrent/new_{i:03}.bin");
            let _ = write_cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![0xEE; 256])),
                )
                .await;
        }

        let report = drain_handle.await.unwrap();
        assert_eq!(report.errors, 0, "drain should complete without errors");
    });
}

// =====================================================================
// Issue: repair_replication_sweep drops metadata
//
// repair_replication_sweep calls replicate_object which uses plain get+put.
// On raw shards the TLV metadata suffix is stripped during the copy.
// =====================================================================

#[test]
fn repair_replication_sweep_drops_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = RawRefRegistry::new(refs, kinds);

    let payload = Bytes::from(vec![0xBBu8; 4096]);
    let metadata = b"repair-replication-meta";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/repair-replication.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify metadata is present.
    let placement = cluster.placement("meta/repair-replication.bin").unwrap();
    assert_eq!(placement.meta_len as usize, metadata.len());

    // Find the shard that does NOT hold the object.
    let missing_shard: usize = (0..3)
        .find(|s| !placement.shards.contains(s))
        .expect("should find a non-holding shard");

    // Detach one of the holding shards to create under-replication.
    let detach = placement.shards[0];
    cluster.detach_shard(detach);

    // Run repair_replication_sweep with registry so metadata is preserved.
    let result = rt.block_on(async {
        repair::repair_replication_sweep(&cluster, 100, Some(&registry), None).await
    });
    assert!(
        result.re_replicated > 0,
        "repair-replication should have re-replicated at least 1 object"
    );
    flush_all(&raws);

    // Check metadata on the new replica.
    let target_raw = &raws[missing_shard];
    let target_meta =
        target_raw.get_metadata(&Path::from("meta/repair-replication.bin"));

    // With the fix, replicate_object preserves metadata via
    // raw.put_with_meta().
    assert_eq!(
        &target_meta.unwrap()[..],
        metadata,
        "repair_replication_sweep should preserve metadata on the target shard"
    );
}

// =====================================================================
// replicate_object preserves TLV metadata
// =====================================================================

#[test]
fn replicate_object_preserves_metadata() {
    // replicate_object() uses raw.put_with_meta() when a RawRefRegistry is
    // provided, preserving the TLV metadata suffix on the target shard.
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xABu8; 4096]);
    let metadata = b"test-metadata-bytes";

    rt.block_on(async {
        shardedobjstr::metadata::put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/replicate.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let placement = cluster.placement("meta/replicate.bin").unwrap();
    assert_eq!(placement.shards.len(), 2);
    assert_eq!(placement.meta_len as usize, metadata.len());

    let target: usize = (0..3)
        .find(|s| !placement.shards.contains(s))
        .expect("should find a non-holding shard");
    let source = placement.shards[0];

    rt.block_on(async {
        cluster
            .replicate_object("meta/replicate.bin", source, target, Some(&registry))
            .await
            .unwrap();
    });
    flush_all(&raws);

    let target_meta = raws[target].get_metadata(&Path::from("meta/replicate.bin"));
    assert_eq!(
        &target_meta.unwrap()[..],
        metadata,
        "replicate_object should preserve metadata on the target shard"
    );
}

// =====================================================================
// partitioned_sync concurrent write race (documents known race)
// =====================================================================

#[test]
fn partitioned_sync_concurrent_write_race() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(&Path::from("sync/alpha"), PutPayload::from(Bytes::from(vec![0x11; 4096])))
            .await
            .unwrap();
        flush_all(&raws);

        // Detach shard 2 and write directly to it (simulates concurrent write).
        cluster.detach_shard(2);

        raws[2]
            .put(&Path::from("sync/alpha"), PutPayload::from(Bytes::from(vec![0x99; 4096])))
            .await
            .unwrap();
        raws[2].flush_index().unwrap();

        // Re-attach shard 2 and run partitioned_sync.
        cluster.attach_shard(2, raws[2].clone(), false).await.unwrap();

        let stores: Vec<Option<Arc<dyn ObjectStore>>> =
            raws.iter().map(|s| Some(s.clone() as Arc<dyn ObjectStore>)).collect();
        let synced = repair::partitioned_sync(&cluster, &stores, 2, None).await;
        assert!(synced.is_ok(), "partitioned_sync should complete: {:?}", synced);
    });
}

// =====================================================================
// re_replication_sweep actually restores replication factor
// =====================================================================

#[test]
fn re_replication_sweep_restores_rf() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Detach shard 0 -- some objects become under-replicated.
    cluster.detach_shard(0);

    let under_before = cluster.find_under_replicated();

    // With zero grace, sweep should re-replicate immediately.
    let count = rt.block_on(repair::re_replication_sweep(
        &cluster,
        100,
        None,
    ));

    // Verify that under-replicated objects were repaired.
    if !under_before.is_empty() {
        assert!(count > 0, "should re-replicate at least one object");
    }

    let under_after = cluster.find_under_replicated();
    assert!(
        under_after.len() < under_before.len(),
        "under-replicated count should decrease: before={}, after={}",
        under_before.len(),
        under_after.len()
    );
}

// =====================================================================
// drain_shard: verify metadata is preserved during drain
// =====================================================================

#[test]
fn drain_shard_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let sz: u64 = 64 * 1024 * 1024;
    // 3 shards, RF=1 so objects land on exactly one shard.
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("drain{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 1);
    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = RawRefRegistry::new(refs, kinds);
    let refs2: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds2 = vec![ShardKind::Raw; 3];
    let registry2 = RawRefRegistry::new(refs2, kinds2);
    cluster.set_raw_refs(Arc::new(registry2));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects with metadata on shard 0.
    let meta_bytes = b"drain-meta-test";
    let mut put_keys = Vec::new();
    for i in 0..10 {
        let key = format!("drain_meta/obj{i}.bin");
        let body = vec![0xAA + i as u8; 512];
        rt.block_on(async {
            put_with_meta(
                &cluster,
                &registry,
                &Path::from(key.as_str()),
                Bytes::from(body),
                meta_bytes,
            )
            .await
            .unwrap();
        });
        put_keys.push(key);
    }
    flush_all(&raws);
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Find which objects are on shard 0.
    let objs_on_0: Vec<String> = put_keys
        .iter()
        .filter(|key| {
            cluster
                .placement(key.as_str())
                .map_or(false, |e| e.shards.contains(&0))
        })
        .cloned()
        .collect();

    if objs_on_0.is_empty() {
        eprintln!("SKIP: no objects landed on shard 0 with RF=1");
        return;
    }

    // Build survivor cluster from shards 1 and 2.
    let survivor_raws = &raws[1..];
    let survivor_stores: Vec<Arc<dyn ObjectStore>> =
        survivor_raws.iter().map(|r| r.clone() as _).collect();
    let survivor = ShardedObjectStore::new(survivor_stores, 1);
    let survivor_refs: Vec<Option<Arc<RawObjectStore>>> =
        survivor_raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let survivor_kinds = vec![ShardKind::Raw; survivor_raws.len()];
    let survivor_registry = Arc::new(RawRefRegistry::new(survivor_refs, survivor_kinds));
    survivor.set_raw_refs(survivor_registry.clone());
    rt.block_on(survivor.rebuild_catalog()).unwrap();

    // Drain shard 0.
    let report = rt.block_on(repair::drain_shard(
        &cluster,
        &survivor,
        0,
        &(raws[0].clone() as Arc<dyn ObjectStore>),
        Some(&registry),
        None,
    ));
    assert!(report.moved > 0, "should have moved objects");
    assert_eq!(report.errors, 0, "should have no errors");

    // Verify metadata survived the drain.
    flush_all(survivor_raws);
    rt.block_on(survivor.rebuild_catalog()).unwrap();

    for key in &objs_on_0 {
        let got_meta = rt.block_on(async {
            get_metadata(&survivor, &survivor_registry, &Path::from(key.as_str()))
                .await
                .unwrap()
        });
        assert_eq!(
            &got_meta[..],
            meta_bytes,
            "metadata for '{}' should survive drain",
            key
        );
    }
}

// =====================================================================
// over_replication_trim: batch_size limits trimming
// =====================================================================

#[test]
fn over_replication_trim_respects_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("ot{i}.raw")), sz))
        .collect();
    // RF=3 (mirror mode) so every object goes to all 3 shards.
    let cluster = build_cluster(&raws, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write 20 objects.
    rt.block_on(async {
        for i in 0..20 {
            cluster
                .put(
                    &Path::from(format!("trim/obj{i:03}.bin")),
                    PutPayload::from(Bytes::from(vec![i as u8; 256])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Now add a 4th shard and drop RF to 2 so all objects are over-replicated.
    let extra = format_shard(&dir.path().join("ot3.raw"), sz);
    let _ = rt.block_on(cluster.attach_shard(
        3,
        extra.clone() as Arc<dyn ObjectStore>,
        true,
    ));

    // Manually build a new cluster with RF=2 from all 4 shards' data.
    let all_raws = vec![raws[0].clone(), raws[1].clone(), raws[2].clone(), extra.clone()];
    let stores4: Vec<Arc<dyn ObjectStore>> =
        all_raws.iter().map(|r| r.clone() as _).collect();
    let cluster2 = ShardedObjectStore::new(stores4, 2);
    rt.block_on(cluster2.rebuild_catalog()).unwrap();

    let over = cluster2.find_over_replicated();
    assert!(over.len() >= 10, "should have many over-replicated objects, got {}", over.len());

    // Trim with batch_size=5 -- should trim at most 5.
    let trimmed = rt.block_on(repair::over_replication_trim(&cluster2, 5));
    assert!(
        trimmed <= 5,
        "over_replication_trim should respect batch_size; got {}",
        trimmed
    );

    // Should still have remaining over-replicated objects.
    let remaining = cluster2.find_over_replicated();
    assert!(
        !remaining.is_empty(),
        "should still have over-replicated objects after partial trim"
    );
}

// =====================================================================
// HIGH: repair_replication_sweep actually executes (not just plan)
// =====================================================================

#[test]
fn repair_replication_sweep_executes_re_replication_and_trim() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Detach shard 0 to create under-replication.
    cluster.detach_shard(0);
    let under_before = cluster.find_under_replicated();
    assert!(!under_before.is_empty(), "some objects should be under-replicated");

    // Run actual repair_replication_sweep (not plan).
    let result = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));

    // At least some objects should have been re-replicated.
    assert!(
        result.re_replicated > 0 || under_before.is_empty(),
        "repair_replication_sweep should re-replicate: {:?}",
        result
    );

    // Under-replicated count should decrease.
    let under_after = cluster.find_under_replicated();
    assert!(
        under_after.len() < under_before.len(),
        "sweep should reduce under-replicated: before={}, after={}",
        under_before.len(),
        under_after.len()
    );
}

// =====================================================================
// HIGH: repair_replication_sweep trims over-replicated objects
// =====================================================================

#[test]
fn repair_replication_sweep_trims_over_replicated_standalone() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    // 3 shards RF=3 (mirror mode) -- every object on all 3.
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("sw{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        for i in 0..10 {
            cluster
                .put(
                    &Path::from(format!("sweep/obj{i:03}.bin")),
                    PutPayload::from(Bytes::from(vec![i as u8; 256])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Rebuild with RF=2 so everything is over-replicated.
    let stores: Vec<Arc<dyn ObjectStore>> =
        raws.iter().map(|r| r.clone() as _).collect();
    let cluster2 = ShardedObjectStore::new(stores, 2);
    rt.block_on(cluster2.rebuild_catalog()).unwrap();

    let over_before = cluster2.find_over_replicated();
    assert!(!over_before.is_empty(), "should have over-replicated objects");

    let result = rt.block_on(repair::repair_replication_sweep(&cluster2, 100, None, None));
    assert!(result.trimmed > 0, "sweep should trim excess replicas: {:?}", result);
}

// =====================================================================
// HIGH: mirror_sync fails when no healthy source shards exist
// =====================================================================

#[test]
fn mirror_sync_fails_when_no_healthy_sources() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Detach shards 0 and 1 -- only shard 2 is the target, no sources.
    cluster.detach_shard(0);
    cluster.detach_shard(1);

    let stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|s| Some(s.clone() as Arc<dyn ObjectStore>)).collect();

    // mirror_sync to shard 2 should fail or produce no copies
    // (no healthy source to read from).
    let result = rt.block_on(repair::mirror_sync(&cluster, &stores, 2, None));
    match result {
        Ok(report) => {
            // If it "succeeds" with nothing to do, that's acceptable.
            assert_eq!(
                report.copied, 0,
                "should not copy anything without healthy sources"
            );
        }
        Err(_) => {
            // Error is also acceptable -- no source available.
        }
    }
}

// =====================================================================
// MEDIUM: find_replication_target returns None when all shards hold key
// =====================================================================

#[test]
fn find_replication_target_returns_none_when_all_hold() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    // 3 shards, RF=3 -- every shard holds every object.
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("ft{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("full/obj.bin"),
                PutPayload::from(Bytes::from(vec![0xAA; 256])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // All shards hold the object -- no target for another replica.
    let target = cluster.find_replication_target("full/obj.bin");
    assert!(
        target.is_none(),
        "should return None when all shards already hold the object"
    );
}

// =====================================================================
// MEDIUM: pick_excess_shard returns None for RF=1
// =====================================================================

#[test]
fn pick_excess_shard_returns_none_at_rf() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("pe{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("exact/obj.bin"),
                PutPayload::from(Bytes::from(vec![0xBB; 256])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Object is on exactly RF shards -- nothing excess to pick.
    let placement = cluster.placement("exact/obj.bin").unwrap();
    assert_eq!(placement.shards.len(), 2, "should be on RF=2 shards");

    let excess = cluster.pick_excess_shard("exact/obj.bin");
    assert!(
        excess.is_none(),
        "should return None when replica count == RF"
    );
}

// =====================================================================
// BUG: re_replication_sweep only checks Offline, not Degraded
// =====================================================================
//
// `re_replication_sweep` now correctly handles Degraded shards.
// `find_under_replicated()` excludes Degraded replicas from the healthy
// count (only Healthy|Syncing count), and the sweep's eligibility gate
// includes Degraded alongside Offline/Detached.
//
// This test verifies the fix: objects on a Degraded shard are detected
// as under-replicated and re-replicated to a healthy target.

#[test]
fn re_replication_sweep_ignores_degraded_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    seed_objects(&cluster, &raws);

    // Mark shard 0 as Degraded (not Offline).
    cluster.set_shard_health(0, ShardHealth::Degraded);

    // Degraded replicas do not count as healthy, so objects with a
    // replica on shard 0 appear under-replicated.
    let under = cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "find_under_replicated should flag objects on Degraded shards"
    );

    // The sweep should detect and re-replicate from Degraded shards.
    let result = rt.block_on(repair::re_replication_sweep(
        &cluster,
        100,
        None,
    ));

    assert!(
        result > 0,
        "re_replication_sweep should re-replicate from Degraded shards"
    );
}

// =====================================================================
// Regression: repair_replication_sweep Phase 2 trim cleans sidecar files
// =====================================================================
//
// `repair_replication_sweep` Phase 2 calls `remove_replica()` which now
// also calls `cleanup_sidecar_maybe()` to delete the `__meta__/<key>`
// sidecar file on Sidecar-kind shards.
//
// This test verifies the fix: after trimming an over-replicated object,
// no orphaned sidecar file remains.

#[test]
fn repair_replication_sweep_trim_leaves_orphaned_sidecar() {
    use object_store::memory::InMemory;
    use shardedobjstr::metadata::{meta_sidecar_path, put_with_meta, ShardKind};

    let dir = tempfile::tempdir().unwrap();
    let raw0 = format_shard(&dir.path().join("trim0.raw"), 64 * 1024 * 1024);
    let mem1: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mem2: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // 3-shard cluster: Raw + 2x Sidecar, RF=2.
    let stores: Vec<Arc<dyn ObjectStore>> =
        vec![raw0.clone() as _, mem1.clone(), mem2.clone()];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let registry = shardedobjstr::metadata::RawRefRegistry::new(
        vec![Some(raw0.clone()), None, None],
        vec![ShardKind::Raw, ShardKind::Sidecar, ShardKind::Sidecar],
    );
    let reg_arc = Arc::new(shardedobjstr::metadata::RawRefRegistry::new(
        vec![Some(raw0.clone()), None, None],
        vec![ShardKind::Raw, ShardKind::Sidecar, ShardKind::Sidecar],
    ));
    cluster.set_raw_refs(reg_arc);

    // Write object with metadata.
    rt.block_on(async {
        put_with_meta(
            &cluster, &registry,
            &Path::from("trim/over.bin"),
            Bytes::from(vec![0xDD; 512]),
            b"trim-meta-test",
        ).await.unwrap();
    });
    flush_all(&[raw0.clone()]);

    // Manually add a third replica to make it over-replicated (RF=2, 3 replicas).
    let entry = cluster.placement("trim/over.bin").unwrap();
    let missing_shard = (0..3usize).find(|s| !entry.shards.contains(s)).unwrap();

    // Write data + sidecar to the missing shard.
    {
        let target_store = cluster.shard_store(missing_shard).unwrap();
        rt.block_on(async {
            target_store.put(
                &Path::from("trim/over.bin"),
                PutPayload::from(Bytes::from(vec![0xDD; 512])),
            ).await.unwrap();
            // For Sidecar shards, also write the sidecar file.
            if missing_shard > 0 {
                let sidecar = meta_sidecar_path(&Path::from("trim/over.bin"));
                target_store.put(
                    &sidecar,
                    PutPayload::from(Bytes::from(b"trim-meta-test".to_vec())),
                ).await.unwrap();
            }
        });
        cluster.catalog().add_replica("trim/over.bin", missing_shard, 512, 14);
        if missing_shard == 0 {
            flush_all(&[raw0.clone()]);
        }
    }

    // Confirm over-replicated.
    let over = cluster.find_over_replicated();
    assert!(!over.is_empty(), "object should be over-replicated");

    // Run repair_replication_sweep to trim.
    let result = rt.block_on(repair::repair_replication_sweep(&cluster, 100, None, None));
    assert!(result.trimmed > 0, "should have trimmed excess replicas");

    // Verify the sidecar file was also removed on the trimmed shard.
    // We need to find which shard was trimmed.
    let after = cluster.placement("trim/over.bin").unwrap();
    let trimmed_shard = (0..3usize).find(|s| !after.shards.contains(s));
    if let Some(sid) = trimmed_shard {
        if sid > 0 {
            let trimmed_store = if sid == 1 { &mem1 } else { &mem2 };
            let sidecar = meta_sidecar_path(&Path::from("trim/over.bin"));
            let orphan = rt.block_on(async { trimmed_store.head(&sidecar).await.is_ok() });
            assert!(
                !orphan,
                "sidecar file on shard {} should be deleted during trim",
                sid
            );
        }
    }
}

// =====================================================================
// plan_repair_replication dry-run reports correct actions
// =====================================================================

#[test]
fn plan_repair_replication_reports_actions() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir);

    seed_objects(&cluster, &raws);

    // Detach shard 0 to create under-replication.
    cluster.detach_shard(0);
    let under = cluster.find_under_replicated();
    assert!(!under.is_empty(), "some objects should be under-replicated");

    // Dry-run plan should show what repair-replication would do.
    let plan = repair::plan_repair_replication(&cluster, 100);
    assert!(
        !plan.replications.is_empty(),
        "plan should include replication actions for under-replicated objects"
    );

    // Verify the plan does not modify the catalog.
    let still_under = cluster.find_under_replicated();
    assert_eq!(
        under.len(),
        still_under.len(),
        "dry-run plan must not modify placement"
    );

    // Every planned action should have a valid source and target.
    for action in &plan.replications {
        match &action.action {
            PlannedActionKind::Replicate {
                source_shard,
                target_shard,
            } => {
                assert_ne!(
                    source_shard, target_shard,
                    "source and target must differ for {}", action.key
                );
                assert_eq!(
                    cluster.shard_health(*source_shard),
                    Some(ShardHealth::Healthy),
                    "source shard must be healthy"
                );
                assert_eq!(
                    cluster.shard_health(*target_shard),
                    Some(ShardHealth::Healthy),
                    "target shard must be healthy"
                );
            }
            _ => panic!("expected Replicate action"),
        }
    }
}
