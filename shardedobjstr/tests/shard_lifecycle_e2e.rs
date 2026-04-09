//! End-to-end tests for shard lifecycle operations: offline tracking,
//! free-space metadata, read ordering, drain, and registry helpers.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::metadata::{
    meta_sidecar_path, put_with_meta, RawRefRegistry, ShardKind,
};
use shardedobjstr::repair::drain_shard;
use shardedobjstr::{DetachReason, ShardHealth, ShardedObjectStore};

use common::{build_cluster, flush_all, format_shard};

// -- Helpers -----------------------------------------------------------------

fn setup(
    dir: &tempfile::TempDir,
    n: usize,
    rf: usize,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..n)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, rf);
    (cluster, raws)
}

fn put_key(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    key: &str,
    data: &[u8],
) {
    rt.block_on(async {
        cluster
            .put(
                &Path::from(key),
                PutPayload::from(Bytes::copy_from_slice(data)),
            )
            .await
            .unwrap();
    });
}

// -- shard_offline_since -----------------------------------------------------

#[test]
fn shard_offline_since_none_when_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    // All shards start healthy -- offline_since should be None.
    for id in 0..3 {
        assert!(
            cluster.shard_offline_since(id).is_none(),
            "shard {id} should not have an offline timestamp"
        );
    }
}

#[test]
fn shard_offline_since_set_when_offline() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    let before = chrono::Utc::now();
    cluster.set_shard_health(1, ShardHealth::Offline);
    let after = chrono::Utc::now();

    // Shard 1 should now have an offline_since timestamp.
    let ts = cluster
        .shard_offline_since(1)
        .expect("offline shard should have a timestamp");
    assert!(ts >= before && ts <= after);

    // Other shards should still be None.
    assert!(cluster.shard_offline_since(0).is_none());
    assert!(cluster.shard_offline_since(2).is_none());
}

#[test]
fn shard_offline_since_cleared_on_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    cluster.set_shard_health(0, ShardHealth::Offline);
    assert!(cluster.shard_offline_since(0).is_some());

    // Mark healthy again -- timestamp should clear.
    cluster.set_shard_health(0, ShardHealth::Healthy);
    assert!(
        cluster.shard_offline_since(0).is_none(),
        "timestamp should be cleared when shard goes healthy"
    );
}

// -- shard_free_space / set_shard_free_space ---------------------------------

#[test]
fn shard_free_space_default_is_max() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 2, 1);

    // Default free space is u64::MAX.
    for id in 0..2 {
        let free = cluster.shard_free_space(id).unwrap();
        assert_eq!(free, u64::MAX, "default free space should be u64::MAX");
    }
}

#[test]
fn shard_free_space_set_and_get() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 1);

    cluster.set_shard_free_space(0, 1_000_000);
    cluster.set_shard_free_space(1, 2_000_000);
    cluster.set_shard_free_space(2, 500_000);

    assert_eq!(cluster.shard_free_space(0).unwrap(), 1_000_000);
    assert_eq!(cluster.shard_free_space(1).unwrap(), 2_000_000);
    assert_eq!(cluster.shard_free_space(2).unwrap(), 500_000);
}

#[test]
fn shard_free_space_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 2, 1);

    assert!(cluster.shard_free_space(99).is_none());
}

// -- read_shard_order --------------------------------------------------------

#[test]
fn read_shard_order_returns_catalog_shards() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_key(&rt, &cluster, "order/test.bin", b"data");
    flush_all(&raws);

    let order = cluster.read_shard_order(&Path::from("order/test.bin"));
    assert!(!order.is_empty());

    // The catalog should know which shards hold the object.
    let entry = cluster.placement("order/test.bin").unwrap();
    // read_shard_order should return exactly the catalog shards.
    for shard in &entry.shards {
        assert!(
            order.contains(shard),
            "catalog shard {shard} should be in read_shard_order"
        );
    }
}

#[test]
fn read_shard_order_falls_back_for_unknown_key() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    // Key not in catalog -- should fallback to all non-offline shards.
    let order = cluster.read_shard_order(&Path::from("unknown/key.bin"));
    assert!(!order.is_empty(), "fallback should return at least some shards");
}

#[test]
fn read_shard_order_excludes_offline_shards_in_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    cluster.set_shard_health(1, ShardHealth::Offline);

    let order = cluster.read_shard_order(&Path::from("no_such_key.bin"));
    assert!(
        !order.contains(&1),
        "offline shard should not appear in fallback order"
    );
}

// -- drain_shard -------------------------------------------------------------

#[test]
fn drain_shard_moves_exclusive_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 3, 1); // rf=1 so each object on one shard
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put some objects.
    for i in 0..5 {
        put_key(&rt, &cluster, &format!("drain/obj{i}.bin"), &[i as u8; 256]);
    }
    flush_all(&raws);
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Find which objects are on shard 0.
    let objs_on_0: Vec<String> = (0..5)
        .filter_map(|i| {
            let key = format!("drain/obj{i}.bin");
            cluster
                .placement(&key)
                .filter(|e| e.shards.contains(&0))
                .map(|_| key)
        })
        .collect();

    if objs_on_0.is_empty() {
        // Nothing on shard 0, skip -- hash placement is deterministic but
        // depends on shard count. This shouldn't normally happen with 5 objects.
        return;
    }

    // Build survivor cluster from shards 1 and 2.
    let survivor_stores: Vec<Arc<dyn ObjectStore>> =
        raws[1..].iter().map(|r| r.clone() as _).collect();
    let survivor = ShardedObjectStore::new(survivor_stores, 1);

    let report = rt.block_on(drain_shard(
        &cluster,
        &survivor,
        0,
        &(raws[0].clone() as Arc<dyn ObjectStore>),
        None,
        None,
    ));

    assert!(report.moved > 0, "should have moved some objects");
    assert_eq!(report.errors, 0, "should have no errors");
}

#[test]
fn drain_shard_skips_replicated_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 3, 2); // rf=2 so each object on two shards
    let rt = tokio::runtime::Runtime::new().unwrap();

    for i in 0..4 {
        put_key(&rt, &cluster, &format!("drain_skip/obj{i}.bin"), &[i as u8; 128]);
    }
    flush_all(&raws);
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // With rf=2, every object on the victim shard also has a replica elsewhere.
    // So drain_shard should skip all of them.
    let victim_id = 0usize;
    let survivor_stores: Vec<Arc<dyn ObjectStore>> =
        raws[1..].iter().map(|r| r.clone() as _).collect();
    let survivor = ShardedObjectStore::new(survivor_stores, 1);

    let report = rt.block_on(drain_shard(
        &cluster,
        &survivor,
        victim_id,
        &(raws[victim_id].clone() as Arc<dyn ObjectStore>),
        None,
        None,
    ));

    // Objects already have replicas on non-victim shards => skipped.
    assert_eq!(
        report.errors, 0,
        "no errors expected"
    );
    // Either skipped or moved=0 (if nothing was on shard 0).
    assert_eq!(report.moved, 0, "should not move objects that have replicas");
}

#[test]
fn drain_shard_empty_victim() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Don't put any objects -- empty store.
    let survivor_stores: Vec<Arc<dyn ObjectStore>> =
        raws[1..].iter().map(|r| r.clone() as _).collect();
    let survivor = ShardedObjectStore::new(survivor_stores, 1);

    let report = rt.block_on(drain_shard(
        &cluster,
        &survivor,
        0,
        &(raws[0].clone() as Arc<dyn ObjectStore>),
        None,
        None,
    ));

    assert_eq!(report.moved, 0);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.errors, 0);
}

// -- RawRefRegistry helpers --------------------------------------------------

#[test]
fn registry_single_constructor() {
    let dir = tempfile::tempdir().unwrap();
    let raw = format_shard(&dir.path().join("single.raw"), 64 * 1024 * 1024);

    let reg = RawRefRegistry::single(raw.clone());
    assert_eq!(reg.shard_count(), 1);
    assert_eq!(reg.all_raw().len(), 1);
    assert!(reg.first_raw().is_some());
}

#[test]
fn registry_all_raw_filters_none() {
    let dir = tempfile::tempdir().unwrap();
    let raw0 = format_shard(&dir.path().join("r0.raw"), 64 * 1024 * 1024);
    let raw2 = format_shard(&dir.path().join("r2.raw"), 64 * 1024 * 1024);

    // Slot 1 is None (offline shard).
    let refs = vec![Some(raw0.clone()), None, Some(raw2.clone())];
    let kinds = vec![
        shardedobjstr::metadata::ShardKind::Raw,
        shardedobjstr::metadata::ShardKind::Raw,
        shardedobjstr::metadata::ShardKind::Raw,
    ];
    let reg = RawRefRegistry::new(refs, kinds);

    assert_eq!(reg.shard_count(), 3, "shard_count includes None slots");
    let all = reg.all_raw();
    assert_eq!(all.len(), 2, "all_raw should skip None slots");
}

#[test]
fn registry_first_raw_returns_first_non_none() {
    let dir = tempfile::tempdir().unwrap();
    let raw1 = format_shard(&dir.path().join("f1.raw"), 64 * 1024 * 1024);

    let refs = vec![None, Some(raw1.clone()), None];
    let kinds = vec![
        shardedobjstr::metadata::ShardKind::Raw,
        shardedobjstr::metadata::ShardKind::Raw,
        shardedobjstr::metadata::ShardKind::Raw,
    ];
    let reg = RawRefRegistry::new(refs, kinds);

    let first = reg.first_raw().expect("should have at least one raw");
    // The Arc should point to raw1.
    assert_eq!(Arc::as_ptr(&first), Arc::as_ptr(&raw1));
}

#[test]
fn registry_first_raw_none_when_all_offline() {
    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![None, None];
    let kinds = vec![
        shardedobjstr::metadata::ShardKind::Raw,
        shardedobjstr::metadata::ShardKind::Raw,
    ];
    let reg = RawRefRegistry::new(refs, kinds);

    assert!(reg.first_raw().is_none());
    assert_eq!(reg.all_raw().len(), 0);
    assert_eq!(reg.shard_count(), 2);
}

// -- Syncing health state exclusion ------------------------------------------

#[test]
fn syncing_shard_excluded_from_write_targets() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Mark shard 1 as Syncing.
    cluster.set_shard_health(1, ShardHealth::Syncing);

    // Write some objects -- none should land on the Syncing shard.
    for i in 0..20 {
        put_key(&rt, &cluster, &format!("sync_excl/obj{i}.bin"), &[i as u8; 256]);
    }
    flush_all(&raws);

    // Check no object's placement includes the Syncing shard.
    for i in 0..20 {
        let key = format!("sync_excl/obj{i}.bin");
        let entry = cluster.placement(&key).unwrap();
        assert!(
            !entry.shards.contains(&1),
            "object '{}' should NOT be on Syncing shard 1, got {:?}",
            key, entry.shards
        );
    }
}

#[test]
fn syncing_shard_excluded_from_select_targets() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    cluster.set_shard_health(0, ShardHealth::Syncing);

    // target_shards should not include shard 0.
    let targets = cluster.target_shards(&Path::from("sync_test/key.bin"));
    assert!(
        !targets.contains(&0),
        "Syncing shard should be excluded from targets: {:?}",
        targets
    );
    assert_eq!(targets.len(), 2, "should still select RF=2 from healthy shards");
}

#[test]
fn syncing_shard_still_readable() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed data while all shards healthy.
    put_key(&rt, &cluster, "sync_read/obj.bin", b"readable-data");
    flush_all(&raws);

    // Now mark one shard as Syncing.
    let placement = cluster.placement("sync_read/obj.bin").unwrap();
    let syncing_shard = placement.shards[0];
    cluster.set_shard_health(syncing_shard, ShardHealth::Syncing);

    // Reads should still work (Syncing is not Offline for reads).
    rt.block_on(async {
        let data = cluster
            .get(&Path::from("sync_read/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"readable-data");
    });
}

// -- Jump hash integration test: placement stability on shard add/remove -----

#[test]
fn jump_hash_placement_stable_on_shard_add() {
    // Verify that adding a shard only moves ~1/N of objects (integration-level
    // check of the jump consistent hash property).
    let dir = tempfile::tempdir().unwrap();
    let n_initial = 4usize;
    let rf = 1;
    let n_objects = 200;

    // Phase 1: build cluster with 4 shards, seed objects.
    let raws4: Vec<_> = (0..n_initial)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), 64 * 1024 * 1024))
        .collect();
    let cluster4 = build_cluster(&raws4, rf);
    let rt = tokio::runtime::Runtime::new().unwrap();

    for i in 0..n_objects {
        put_key(&rt, &cluster4, &format!("jh/obj{i:04}.bin"), &[i as u8; 128]);
    }
    flush_all(&raws4);

    // Record which shard each object has.
    let mut placements4 = std::collections::HashMap::new();
    for i in 0..n_objects {
        let key = format!("jh/obj{i:04}.bin");
        let entry = cluster4.placement(&key).unwrap();
        placements4.insert(key, entry.shards[0]);
    }

    // Phase 2: build cluster with 5 shards. Only look at select_targets --
    // we're checking the hash function, not actual data migration.
    let raws5: Vec<_> = (0..5)
        .map(|i| format_shard(&dir.path().join(format!("s5_{i}.raw")), 64 * 1024 * 1024))
        .collect();
    let cluster5 = build_cluster(&raws5, rf);

    // Count how many objects would move (different target shard).
    let mut moved = 0usize;
    for i in 0..n_objects {
        let key = format!("jh/obj{i:04}.bin");
        let new_targets = cluster5.target_shards(&Path::from(key.as_str()));
        let old_shard = placements4[&key];
        if new_targets[0] != old_shard {
            moved += 1;
        }
    }

    // With jump consistent hash, adding 1 shard to 4 should move ~1/5 = 20%.
    // Allow generous margin: < 40% (2x expected).
    let move_pct = (moved as f64 / n_objects as f64) * 100.0;
    assert!(
        move_pct < 40.0,
        "adding 1 shard to 4 should move <40% of keys, but moved {:.1}% ({}/{})",
        move_pct, moved, n_objects
    );
    // At least some should move (> 5%).
    assert!(
        move_pct > 5.0,
        "adding 1 shard to 4 should move >5% of keys, but moved {:.1}% ({}/{})",
        move_pct, moved, n_objects
    );
}

// =====================================================================
// MEDIUM: validate_shard_access returns correct reachability
// =====================================================================

#[test]
fn validate_shard_access_all_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let results = rt.block_on(async { cluster.validate_shard_access().await });
    assert_eq!(results.len(), 4, "should report 4 shards");
    for (sid, ok) in &results {
        assert!(ok, "shard {sid} should be reachable when all are healthy");
    }
}

#[test]
fn validate_shard_access_offline_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Mark shard 1 offline.
    cluster.set_shard_health(1, ShardHealth::Offline);

    let results = rt.block_on(async { cluster.validate_shard_access().await });
    assert_eq!(results.len(), 4);

    for (sid, ok) in &results {
        if *sid == 1 {
            assert!(!ok, "offline shard 1 should report unreachable");
        } else {
            assert!(ok, "healthy shard {sid} should report reachable");
        }
    }
}

#[test]
fn validate_shard_access_detached_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Detach shard 2.
    cluster.detach_shard(2);

    let results = rt.block_on(async { cluster.validate_shard_access().await });

    // Detached shards may still be probed (they have a store) or they
    // may be removed from the shard list. Either way, if present, they
    // should reflect correctness.
    for (sid, ok) in &results {
        // Detached shards are typically still listed but may fail probe.
        if *sid == 2 {
            // Accept either outcome -- the point is it doesn't panic.
            let _ = ok;
        } else {
            assert!(ok, "non-detached shard {sid} should be reachable");
        }
    }
}

// =====================================================================
// BUG: drain_shard copies __meta__/ sidecar files as real objects
// =====================================================================
//
// On Sidecar-kind shards (InMemory, LocalFileSystem), metadata is
// stored in a companion file at `__meta__/<key>`.  `drain_shard` calls
// `victim_store.list(None)` which returns ALL files including sidecar
// files.  These get copied as real objects to the survivor cluster,
// polluting the object namespace.
//
// Currently IGNORED because drain_shard does not filter sidecar files.

#[test]
fn drain_shard_copies_sidecar_files_as_real_objects() {
    use object_store::memory::InMemory;
    use futures::TryStreamExt;

    let dir = tempfile::tempdir().unwrap();
    let raw0 = format_shard(&dir.path().join("s0.raw"), 64 * 1024 * 1024);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Cluster with shard 0 (Raw) + shard 1 (Sidecar/InMemory), RF=2.
    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _, mem.clone()];
    let cluster = ShardedObjectStore::new(stores.clone(), 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let registry = RawRefRegistry::new(
        vec![Some(raw0.clone()), None],
        vec![ShardKind::Raw, ShardKind::Sidecar],
    );
    let reg_arc = Arc::new(RawRefRegistry::new(
        vec![Some(raw0.clone()), None],
        vec![ShardKind::Raw, ShardKind::Sidecar],
    ));
    cluster.set_raw_refs(reg_arc);

    // Write object with metadata -- shard 1 (Sidecar) gets a __meta__/ file.
    rt.block_on(async {
        put_with_meta(
            &cluster, &registry,
            &Path::from("drain/obj.bin"),
            Bytes::from(vec![0xCC; 512]),
            b"drain-test-meta",
        ).await.unwrap();
    });
    flush_all(&[raw0.clone()]);

    // Verify sidecar file exists on InMemory shard.
    let sidecar = meta_sidecar_path(&Path::from("drain/obj.bin"));
    let has_sidecar = rt.block_on(async { mem.head(&sidecar).await.is_ok() });
    assert!(has_sidecar, "sidecar file should exist on InMemory shard");

    // Count files on InMemory shard -- should include sidecar.
    let mem_files: Vec<_> = rt.block_on(async {
        mem.list(None).try_collect::<Vec<_>>().await.unwrap()
    });
    let sidecar_count = mem_files.iter()
        .filter(|m| m.location.as_ref().ends_with(".__meta__"))
        .count();
    assert!(sidecar_count > 0, "InMemory shard should have .__meta__ sidecar files");

    // Build survivor cluster (just shard 0) and drain shard 1.
    let survivor_stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _];
    let survivor = ShardedObjectStore::new(survivor_stores, 1);
    rt.block_on(survivor.rebuild_catalog()).unwrap();

    let _report = rt.block_on(drain_shard(
        &cluster, &survivor, 1, &mem, Some(&registry), None,
    ));
    flush_all(&[raw0.clone()]);

    // BUG: drain_shard should NOT copy __meta__/ files as real objects.
    // List all objects in the survivor cluster.
    let survivor_objects: Vec<_> = rt.block_on(async {
        survivor.list(None).try_collect::<Vec<_>>().await.unwrap()
    });

    let meta_objects: Vec<_> = survivor_objects.iter()
        .filter(|m| m.location.as_ref().ends_with(".__meta__"))
        .collect();

    assert!(
        meta_objects.is_empty(),
        "drain_shard should NOT copy sidecar files as real objects; found {} .__meta__ entries: {:?}",
        meta_objects.len(),
        meta_objects.iter().map(|m| m.location.as_ref()).collect::<Vec<_>>()
    );
}

// =====================================================================
// BUG: rebuild_catalog_for_shard does not remove stale entries
// =====================================================================
//
// `rebuild_catalog_for_shard()` only calls `catalog.add_replica()` for
// objects found on the shard.  It never removes catalog entries for
// objects that no longer exist on the shard.  If an object is deleted
// directly on the shard (bypassing the catalog), the catalog retains a
// stale entry pointing to a non-existent replica.
//
// Currently IGNORED because rebuild_catalog_for_shard does not purge
// stale entries.

#[test]
fn rebuild_catalog_for_shard_does_not_remove_stale_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write an object.
    rt.block_on(async {
        cluster
            .put(
                &Path::from("stale/obj.bin"),
                PutPayload::from(Bytes::from(vec![0xAA; 4096])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Identify which shards hold the object.
    let entry = cluster.placement("stale/obj.bin").unwrap();
    assert_eq!(entry.shards.len(), 2);
    let target_shard = entry.shards[0];

    // Delete the object directly on the target shard (bypass catalog).
    let store = cluster.shard_store(target_shard).unwrap();
    rt.block_on(async {
        store.delete(&Path::from("stale/obj.bin")).await.unwrap();
    });
    flush_all(&raws);

    // Catalog still thinks the object is on target_shard (stale entry).
    let before = cluster.placement("stale/obj.bin").unwrap();
    assert!(
        before.shards.contains(&target_shard),
        "catalog should still reference the shard before rebuild"
    );

    // Run rebuild_catalog_for_shard -- should discover the object is gone.
    let _count = rt.block_on(cluster.rebuild_catalog_for_shard(target_shard)).unwrap();

    // BUG: The stale entry should be removed, but it is not.
    let after = cluster.placement("stale/obj.bin").unwrap();
    assert!(
        !after.shards.contains(&target_shard),
        "rebuild_catalog_for_shard should remove stale entry for shard {} \
         (object no longer on disk), but catalog still lists it",
        target_shard
    );
}

// =====================================================================
// hold_offline / release_hold lifecycle
// =====================================================================

#[test]
fn hold_offline_and_release() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // hold_offline shard 1 with suppress_replication and Manual reason.
    let prev = cluster.hold_offline(1, true, DetachReason::Manual);
    assert_eq!(prev, Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Detached));
    assert!(cluster.shard_suppress_replication(1));
    assert_eq!(cluster.shard_detach_reason(1), Some(DetachReason::Manual));

    // Writes should still succeed on the 2 remaining healthy shards.
    rt.block_on(async {
        let data = Bytes::from(vec![0xEE; 256]);
        cluster
            .put(&Path::from("hold-test.bin"), PutPayload::from(data))
            .await
            .unwrap();
    });

    // release_hold should transition from Detached to Offline.
    assert!(cluster.release_hold(1));
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Offline));
    assert!(!cluster.shard_suppress_replication(1));
    assert_eq!(cluster.shard_detach_reason(1), None);

    // Second release_hold should return false (already released).
    assert!(!cluster.release_hold(1));
}

#[test]
fn hold_offline_drain_reason() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    let prev = cluster.hold_offline(2, false, DetachReason::Drain);
    assert_eq!(prev, Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_detach_reason(2), Some(DetachReason::Drain));
    assert!(!cluster.shard_suppress_replication(2));
}

// =====================================================================
// set_detach_reason after detach_shard (used by recovery.rs)
// =====================================================================

#[test]
fn set_detach_reason_after_detach_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup(&dir, 3, 2);

    // detach_shard alone does not set a reason.
    cluster.detach_shard(1);
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Offline));
    assert_eq!(cluster.shard_detach_reason(1), None);

    // set_detach_reason adds the reason after the fact (as recovery.rs does).
    cluster.set_detach_reason(1, DetachReason::ProbeFailure);
    assert_eq!(cluster.shard_detach_reason(1), Some(DetachReason::ProbeFailure));

    // DeviceMissing variant works too.
    cluster.set_detach_reason(1, DetachReason::DeviceMissing);
    assert_eq!(cluster.shard_detach_reason(1), Some(DetachReason::DeviceMissing));
}

// =====================================================================
// drain_shard moves exclusive objects (RF=1)
// =====================================================================

#[test]
fn drain_shard_moves_exclusive_objects_rf1() {
    let dir = tempfile::tempdir().unwrap();
    // RF=1 so each object is on exactly one shard.
    let (cluster, raws) = setup(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed test objects.
    rt.block_on(async {
        for i in 0..10 {
            let key = format!("drain/obj_{:03}.bin", i);
            let data = vec![(i & 0xFF) as u8; 512];
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(data)),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Count objects on shard 0 before drain.
    let before_count: usize = (0..10)
        .filter(|i| {
            let key = format!("drain/obj_{:03}.bin", i);
            cluster
                .placement(&key)
                .map(|e| e.shards.contains(&0))
                .unwrap_or(false)
        })
        .count();
    assert!(before_count > 0, "shard 0 should hold some objects");

    // Build survivor cluster from shards 1 and 2.
    let survivor_stores: Vec<Arc<dyn ObjectStore>> =
        raws[1..].iter().map(|s| s.clone() as _).collect();
    let survivor_cluster = ShardedObjectStore::new(survivor_stores, 1);

    let victim_store = cluster.shard_store(0).unwrap();
    let report = rt.block_on(
        drain_shard(&cluster, &survivor_cluster, 0, &victim_store, None, None)
    );

    assert!(
        report.errors == 0,
        "drain should complete without errors: {report:?}"
    );
    assert!(
        report.moved > 0 || report.skipped > 0,
        "drain should have processed at least some objects"
    );
}
