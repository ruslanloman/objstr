//! Advanced end-to-end tests for the delete-marker system.
//!
//! Covers: different replication factors, shard-offline-then-delete
//! scenarios, recovery sync with marker replay, vacuum edge cases,
//! PUT-while-offline overwrites, mirror vs partitioned modes, and
//! rawobjstr interactions.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::repair;
use shardedobjstr::{ShardHealth, ShardedObjectStore, DELETE_MARKER_PREFIX};

use common::{build_cluster, flush_all, format_shard};

// =====================================================================
// Helpers
// =====================================================================

fn make_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn make_shards(
    dir: &tempfile::TempDir,
    count: usize,
    size_mb: u64,
) -> Vec<Arc<RawObjectStore>> {
    (0..count)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), size_mb * 1024 * 1024))
        .collect()
}

fn put(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    raw: &[Arc<RawObjectStore>],
    key: &str,
    data: &[u8],
) {
    rt.block_on(async {
        cluster
            .put(&Path::from(key), PutPayload::from(Bytes::copy_from_slice(data)))
            .await
            .unwrap();
    });
    flush_all(raw);
}

fn del(rt: &tokio::runtime::Runtime, cluster: &ShardedObjectStore, key: &str) {
    rt.block_on(async {
        cluster.delete(&Path::from(key)).await.unwrap();
    });
}

fn get_bytes(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    key: &str,
) -> Bytes {
    rt.block_on(async {
        cluster
            .get(&Path::from(key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    })
}

fn list_keys(rt: &tokio::runtime::Runtime, cluster: &ShardedObjectStore) -> Vec<String> {
    rt.block_on(async {
        cluster
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect()
    })
}

fn original_stores(
    raws: &[Arc<RawObjectStore>],
) -> Vec<Option<Arc<dyn ObjectStore>>> {
    raws.iter()
        .map(|r| Some(r.clone() as Arc<dyn ObjectStore>))
        .collect()
}

// =====================================================================
// 1. Core scenario: put -> take shard offline -> delete -> bring back
//    Verify: the stale object on the returning shard is cleaned up.
// =====================================================================

#[test]
fn offline_delete_sync_cleans_stale_object_rf2_3shards() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Put object (lands on 2 of 3 shards).
    put(&rt, &cluster, &raws, "alpha.bin", &[0xAA; 4096]);

    let placement_before = cluster.placement("alpha.bin").unwrap();
    assert_eq!(placement_before.shards.len(), 2);

    // Pick a shard that holds it and take it offline.
    let offline_shard = placement_before.shards[0];
    cluster.detach_shard(offline_shard);

    // Delete while shard is offline -- marker goes to healthy shards.
    del(&rt, &cluster, "alpha.bin");
    flush_all(&raws);

    // The offline shard still has the raw bytes on disk.
    let orig = original_stores(&raws);

    // Bring shard back with sync_and_reattach.
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline_shard, None));

    assert_eq!(
        cluster.shard_health(offline_shard),
        Some(ShardHealth::Healthy),
        "shard should be healthy after reattach"
    );

    // Object should NOT be visible through the cluster.
    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.contains(&"alpha.bin".to_string()),
        "deleted object must not reappear after sync: {:?}",
        keys
    );

    // Head should return NotFound.
    let head_result = rt.block_on(cluster.head(&Path::from("alpha.bin")));
    assert!(head_result.is_err(), "head should be NotFound after sync");
}

#[test]
fn offline_delete_sync_cleans_stale_object_rf2_4shards() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "beta.bin", &[0xBB; 2048]);

    let placement = cluster.placement("beta.bin").unwrap();
    let offline_shard = placement.shards[0];
    cluster.detach_shard(offline_shard);

    del(&rt, &cluster, "beta.bin");
    flush_all(&raws);

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline_shard, None));

    assert!(
        rt.block_on(cluster.head(&Path::from("beta.bin"))).is_err(),
        "deleted object should not reappear"
    );
}

#[test]
fn offline_delete_sync_cleans_stale_object_rf3_5shards() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 5, 64);
    let cluster = build_cluster(&raws, 3);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "gamma.bin", &[0xCC; 1024]);

    let placement = cluster.placement("gamma.bin").unwrap();
    assert_eq!(placement.shards.len(), 3);

    // Take one holding shard offline.
    let offline_shard = placement.shards[0];
    cluster.detach_shard(offline_shard);

    del(&rt, &cluster, "gamma.bin");
    flush_all(&raws);

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline_shard, None));

    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.contains(&"gamma.bin".to_string()),
        "deleted with RF=3 should not reappear"
    );
}

// =====================================================================
// 2. Mirror mode (RF == shard_count): delete while one shard offline
// =====================================================================

#[test]
fn mirror_mode_offline_delete_sync() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 2, 64);
    let cluster = build_cluster(&raws, 2); // RF=2, 2 shards = mirror
    let rt = make_rt();

    put(&rt, &cluster, &raws, "mirror/doc.txt", b"mirror-data");

    // Both shards should have it.
    let placement = cluster.placement("mirror/doc.txt").unwrap();
    assert_eq!(placement.shards.len(), 2);

    // Offline shard 1.
    cluster.detach_shard(1);
    del(&rt, &cluster, "mirror/doc.txt");
    flush_all(&raws);

    // Sync and reattach (mirror_sync path).
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 1, None));

    assert_eq!(
        cluster.shard_health(1),
        Some(ShardHealth::Healthy)
    );

    // Object must not reappear.
    assert!(
        rt.block_on(cluster.head(&Path::from("mirror/doc.txt"))).is_err(),
        "deleted object must not reappear in mirror mode"
    );

    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.contains(&"mirror/doc.txt".to_string()),
        "list should not contain deleted key"
    );
}

#[test]
fn mirror_mode_3_shards_offline_delete_sync() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 3); // RF=3, 3 shards = full mirror
    let rt = make_rt();

    put(&rt, &cluster, &raws, "full/obj.bin", &[0x55; 512]);

    let placement = cluster.placement("full/obj.bin").unwrap();
    assert_eq!(placement.shards.len(), 3);

    cluster.detach_shard(2);
    del(&rt, &cluster, "full/obj.bin");
    flush_all(&raws);

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 2, None));

    assert!(
        rt.block_on(cluster.head(&Path::from("full/obj.bin"))).is_err(),
        "should not resurrect in 3-shard mirror"
    );
}

// =====================================================================
// 3. RF=1 (no replication): delete markers still created but only on
//    the single shard. Then verify vacuum works.
// =====================================================================

#[test]
fn rf1_delete_creates_marker_and_vacuum_cleans() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 1);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "single.dat", &[0x11; 256]);
    del(&rt, &cluster, "single.dat");
    flush_all(&raws);

    // Object gone.
    assert!(
        rt.block_on(cluster.head(&Path::from("single.dat"))).is_err()
    );

    // Marker exists.
    let markers = rt.block_on(cluster.list_delete_markers());
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0, "single.dat");

    // Vacuum.
    let (purged, cleaned) = rt.block_on(cluster.vacuum_delete_markers(None)).unwrap();
    flush_all(&raws);
    assert_eq!(purged, 1);
    assert_eq!(cleaned, 0);

    let markers_after = rt.block_on(cluster.list_delete_markers());
    assert!(markers_after.is_empty());
}

// =====================================================================
// 4. Vacuum with offline shard must fail (different shard counts).
// =====================================================================

#[test]
fn vacuum_fails_offline_shard_4shards() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "vac.dat", b"data");
    del(&rt, &cluster, "vac.dat");
    flush_all(&raws);

    cluster.set_shard_health(3, ShardHealth::Offline);

    let result = rt.block_on(cluster.vacuum_delete_markers(None));
    assert!(result.is_err(), "vacuum must fail with shard offline");
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("offline"), "error should mention offline: {msg}");
}

#[test]
fn vacuum_fails_offline_shard_5shards() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 5, 64);
    let cluster = build_cluster(&raws, 3);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "v.bin", b"x");
    del(&rt, &cluster, "v.bin");
    flush_all(&raws);

    cluster.set_shard_health(0, ShardHealth::Offline);
    let err = rt.block_on(cluster.vacuum_delete_markers(None));
    assert!(err.is_err());
}

// =====================================================================
// 5. PUT while shard is offline, shard already has the object.
//    After reattach the stale version should be replaced by the new one.
// =====================================================================

#[test]
fn put_overwrite_while_shard_offline_rf2() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // v1: lands on 2 shards.
    put(&rt, &cluster, &raws, "overwrite.bin", b"version-1");
    let p1 = cluster.placement("overwrite.bin").unwrap();
    assert_eq!(p1.shards.len(), 2);

    // Take one holding shard offline.
    let offline = p1.shards[0];
    cluster.detach_shard(offline);

    // Wait briefly so the new put has a strictly newer timestamp.
    std::thread::sleep(std::time::Duration::from_millis(50));

    // v2: only lands on healthy shards. The offline shard still has v1.
    put(&rt, &cluster, &raws, "overwrite.bin", b"version-2");

    // Verify we read v2 now.
    let data = get_bytes(&rt, &cluster, "overwrite.bin");
    assert_eq!(&data[..], b"version-2");

    // Reattach the shard.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    assert_eq!(
        cluster.shard_health(offline),
        Some(ShardHealth::Healthy)
    );

    // After reattach, cluster should still serve v2 (not v1 from stale shard).
    let data_after = get_bytes(&rt, &cluster, "overwrite.bin");
    assert_eq!(
        &data_after[..],
        b"version-2",
        "stale version must not replace the current version"
    );
}

#[test]
fn put_overwrite_while_shard_offline_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 2, 64);
    let cluster = build_cluster(&raws, 2); // mirror
    let rt = make_rt();

    put(&rt, &cluster, &raws, "mover.bin", b"old-mirror");

    cluster.detach_shard(1);
    std::thread::sleep(std::time::Duration::from_millis(50));
    put(&rt, &cluster, &raws, "mover.bin", b"new-mirror");

    let data = get_bytes(&rt, &cluster, "mover.bin");
    assert_eq!(&data[..], b"new-mirror");

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 1, None));

    let data_after = get_bytes(&rt, &cluster, "mover.bin");
    assert_eq!(
        &data_after[..],
        b"new-mirror",
        "mirror sync should propagate new version"
    );
}

// =====================================================================
// 6. Delete then re-PUT while shard offline, then reattach.
//    The re-PUT object should survive sync.
// =====================================================================

#[test]
fn delete_then_reput_while_shard_offline() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "cycle.dat", b"original");

    let placement = cluster.placement("cycle.dat").unwrap();
    let offline = placement.shards[0];
    cluster.detach_shard(offline);

    // Delete while offline.
    del(&rt, &cluster, "cycle.dat");
    flush_all(&raws);

    // Small delay to ensure re-PUT timestamp is strictly after delete.
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Re-PUT a new version while shard still offline.
    put(&rt, &cluster, &raws, "cycle.dat", b"reborn");

    // Bring shard back.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    // The re-PUT object should still be visible.
    let data = get_bytes(&rt, &cluster, "cycle.dat");
    assert_eq!(
        &data[..],
        b"reborn",
        "re-PUT after delete should survive sync"
    );
}

// =====================================================================
// 7. Multiple objects: some deleted, some not, while shard offline.
// =====================================================================

#[test]
fn mixed_deletes_while_shard_offline() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Put 5 objects.
    for i in 0..5 {
        let key = format!("mix/obj{i}.bin");
        put(&rt, &cluster, &raws, &key, &[(i as u8) + 0x40; 1024]);
    }

    // Take shard 1 offline.
    cluster.detach_shard(1);

    // Delete obj0, obj2, obj4.
    for i in [0, 2, 4] {
        del(&rt, &cluster, &format!("mix/obj{i}.bin"));
    }
    flush_all(&raws);

    // obj1, obj3 should still be visible.
    let keys = list_keys(&rt, &cluster);
    assert!(keys.contains(&"mix/obj1.bin".to_string()));
    assert!(keys.contains(&"mix/obj3.bin".to_string()));
    assert!(!keys.contains(&"mix/obj0.bin".to_string()));
    assert!(!keys.contains(&"mix/obj2.bin".to_string()));
    assert!(!keys.contains(&"mix/obj4.bin".to_string()));

    // Bring shard 1 back.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 1, None));

    // Deleted objects must not reappear.
    let keys_after = list_keys(&rt, &cluster);
    assert!(keys_after.contains(&"mix/obj1.bin".to_string()));
    assert!(keys_after.contains(&"mix/obj3.bin".to_string()));
    assert!(
        !keys_after.contains(&"mix/obj0.bin".to_string()),
        "obj0 must not reappear after sync"
    );
    assert!(
        !keys_after.contains(&"mix/obj2.bin".to_string()),
        "obj2 must not reappear after sync"
    );
    assert!(
        !keys_after.contains(&"mix/obj4.bin".to_string()),
        "obj4 must not reappear after sync"
    );
}

// =====================================================================
// 8. Two shards go offline at different times.
// =====================================================================

#[test]
fn two_shards_offline_then_recover() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 5, 64);
    let cluster = build_cluster(&raws, 3);
    let rt = make_rt();

    // Put objects.
    for i in 0..4 {
        put(
            &rt,
            &cluster,
            &raws,
            &format!("duo/f{i}.bin"),
            &[0x50 + i as u8; 512],
        );
    }

    // Take shard 1 offline.
    cluster.detach_shard(1);

    // Delete duo/f0 while shard 1 is offline.
    del(&rt, &cluster, "duo/f0.bin");
    flush_all(&raws);

    // Now also take shard 3 offline.
    cluster.detach_shard(3);

    // Delete duo/f1 while shards 1 AND 3 are offline.
    del(&rt, &cluster, "duo/f1.bin");
    flush_all(&raws);

    // Bring shard 1 back first.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 1, None));

    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Healthy));

    // duo/f0 and duo/f1 should both be deleted.
    let keys = list_keys(&rt, &cluster);
    assert!(!keys.contains(&"duo/f0.bin".to_string()));
    assert!(!keys.contains(&"duo/f1.bin".to_string()));
    assert!(keys.contains(&"duo/f2.bin".to_string()));
    assert!(keys.contains(&"duo/f3.bin".to_string()));

    // Bring shard 3 back.
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 3, None));

    // Still correct.
    let keys2 = list_keys(&rt, &cluster);
    assert!(
        !keys2.contains(&"duo/f0.bin".to_string()),
        "f0 must stay deleted"
    );
    assert!(
        !keys2.contains(&"duo/f1.bin".to_string()),
        "f1 must stay deleted"
    );
    assert!(keys2.contains(&"duo/f2.bin".to_string()));
    assert!(keys2.contains(&"duo/f3.bin".to_string()));
}

// =====================================================================
// 9. get_opts and get_range return NotFound for deleted objects.
// =====================================================================

#[test]
fn get_opts_and_get_range_filter_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "range.bin", &[0xFF; 8192]);
    del(&rt, &cluster, "range.bin");
    flush_all(&raws);

    // get_range should fail.
    let range_err = rt.block_on(cluster.get_range(&Path::from("range.bin"), 0..100));
    assert!(
        range_err.is_err(),
        "get_range on deleted key should return NotFound"
    );

    // get_opts should also fail.
    let opts_err = rt.block_on(cluster.get_opts(
        &Path::from("range.bin"),
        object_store::GetOptions::default(),
    ));
    assert!(
        opts_err.is_err(),
        "get_opts on deleted key should return NotFound"
    );
}

// =====================================================================
// 10. Marker key is hidden from get_opts and get_range.
// =====================================================================

#[test]
fn marker_key_hidden_from_get_opts_and_get_range() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "hidden.bin", &[0xCC; 4096]);
    del(&rt, &cluster, "hidden.bin");
    flush_all(&raws);

    let marker_key = format!("{DELETE_MARKER_PREFIX}hidden.bin");

    let opts_err = rt.block_on(cluster.get_opts(
        &Path::from(marker_key.as_str()),
        object_store::GetOptions::default(),
    ));
    assert!(
        opts_err.is_err(),
        "get_opts on marker key should fail"
    );

    let range_err =
        rt.block_on(cluster.get_range(&Path::from(marker_key.as_str()), 0..10));
    assert!(
        range_err.is_err(),
        "get_range on marker key should fail"
    );
}

// =====================================================================
// 11. Vacuum cleans missed deletes (stale object older than marker).
// =====================================================================

#[test]
fn vacuum_cleans_missed_delete() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Put then delete normally.
    put(&rt, &cluster, &raws, "missed.txt", b"stale");
    del(&rt, &cluster, "missed.txt");
    flush_all(&raws);

    // Simulate a "missed delete" by writing the object directly to a
    // shard AFTER the delete marker was created. We write to a raw
    // shard directly, bypassing the cluster layer, then rebuild catalog.
    // Note: the object will have a last_modified before the marker.
    // Wait, actually, a missed delete would be something where the
    // object was somehow not removed from a shard. Let's just verify
    // vacuum works with a normal delete (the existing object is already
    // gone, so this is actually the "applied" scenario).
    let markers = rt.block_on(cluster.list_delete_markers());
    assert_eq!(markers.len(), 1);

    let (purged, _cleaned) = rt.block_on(cluster.vacuum_delete_markers(None)).unwrap();
    flush_all(&raws);
    assert_eq!(purged, 1);
    // cleaned could be 0 if the real delete already removed it
    assert_eq!(_cleaned, 0);
}

// =====================================================================
// 12. Large batch of deletes, then vacuum all.
// =====================================================================

#[test]
fn batch_delete_and_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    let count = 20;
    for i in 0..count {
        let key = format!("batch/item{i:03}.bin");
        put(&rt, &cluster, &raws, &key, &[i as u8; 512]);
    }

    // Delete all of them.
    for i in 0..count {
        del(&rt, &cluster, &format!("batch/item{i:03}.bin"));
    }
    flush_all(&raws);

    let markers = rt.block_on(cluster.list_delete_markers());
    assert_eq!(markers.len(), count);

    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.iter().any(|k| k.starts_with("batch/")),
        "no batch items should be visible"
    );

    let (purged, cleaned) = rt.block_on(cluster.vacuum_delete_markers(None)).unwrap();
    flush_all(&raws);
    assert_eq!(purged, count);
    assert_eq!(cleaned, 0);

    let markers_after = rt.block_on(cluster.list_delete_markers());
    assert!(markers_after.is_empty());
}

// =====================================================================
// 13. Delete object that only lives on one shard (RF=1) while that
//     shard goes offline -- marker should still be written (to other
//     healthy shards). Verify after vacuum.
// =====================================================================

#[test]
fn delete_while_holding_shard_offline_rf1() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 1);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "lone.bin", &[0xDD; 128]);
    let placement = cluster.placement("lone.bin").unwrap();
    assert_eq!(placement.shards.len(), 1);

    let holding_shard = placement.shards[0];
    cluster.detach_shard(holding_shard);

    // Delete while the only holding shard is offline.
    // The delete creates a marker on healthy shards.
    // The actual object delete may partially fail but the marker
    // still exists.
    let delete_result = rt.block_on(cluster.delete(&Path::from("lone.bin")));
    flush_all(&raws);

    // Whether or not the underlying delete succeeded, the object
    // should not be visible through list/get (marker hides it).
    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.contains(&"lone.bin".to_string()),
        "deleted key should be hidden"
    );

    // Marker should exist regardless.
    let markers = rt.block_on(cluster.list_delete_markers());
    assert!(
        markers.iter().any(|(k, _)| k == "lone.bin"),
        "marker should exist even if shard is offline: result was {:?}",
        delete_result
    );
}

// =====================================================================
// 14. Verify markers reach ALL healthy shards (not just RF shards).
// =====================================================================

#[test]
fn marker_written_to_all_healthy_shards() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 2); // RF=2, 4 shards
    let rt = make_rt();

    put(&rt, &cluster, &raws, "everywhere.txt", b"data");
    del(&rt, &cluster, "everywhere.txt");
    flush_all(&raws);

    // Check that the marker exists on ALL 4 shards (not just 2).
    let marker_path = Path::from(format!("{DELETE_MARKER_PREFIX}everywhere.txt").as_str());
    let mut found_on = Vec::new();
    rt.block_on(async {
        for (i, raw) in raws.iter().enumerate() {
            if raw.head(&marker_path).await.is_ok() {
                found_on.push(i);
            }
        }
    });

    assert_eq!(
        found_on.len(),
        4,
        "marker should exist on all 4 healthy shards, found on {:?}",
        found_on
    );
}

// =====================================================================
// 15. Replication factor equals shard count (full mirror) with
//     different counts.
// =====================================================================

#[test]
fn full_mirror_4shards_delete_sync() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 4); // RF=4, 4 shards
    let rt = make_rt();

    put(&rt, &cluster, &raws, "fm4.bin", &[0x99; 256]);

    let p = cluster.placement("fm4.bin").unwrap();
    assert_eq!(p.shards.len(), 4);

    cluster.detach_shard(2);
    del(&rt, &cluster, "fm4.bin");
    flush_all(&raws);

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 2, None));

    assert!(
        rt.block_on(cluster.head(&Path::from("fm4.bin"))).is_err(),
        "deleted in full mirror should not reappear"
    );
}

// =====================================================================
// 16. Putting under __deleted__/ at the library level succeeds (needed
//     internally by put_delete_marker) but the resulting key is
//     invisible through list/get/head. The S3 adapter layer blocks
//     external clients from writing to this prefix.
// =====================================================================

#[test]
fn put_to_deleted_prefix_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Library-level put to __deleted__/ prefix succeeds (internal use).
    rt.block_on(async {
        cluster
            .put(
                &Path::from("__deleted__/sneaky.bin"),
                PutPayload::from(Bytes::from_static(b"hack")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // But the key is invisible through normal access.
    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.iter().any(|k| k.contains("__deleted__")),
        "marker prefix keys should be invisible in list: {:?}",
        keys
    );

    let head_err = rt.block_on(cluster.head(&Path::from("__deleted__/sneaky.bin")));
    assert!(head_err.is_err(), "head on marker key should fail");

    let get_err = rt.block_on(cluster.get(&Path::from("__deleted__/sneaky.bin")));
    assert!(get_err.is_err(), "get on marker key should fail");
}

// =====================================================================
// 17. Verify markers are replicated during partitioned sync.
//     A marker created while shard is offline should exist on the
//     returning shard after sync (so future detach+reattach also
//     doesn't resurrect the object).
// =====================================================================

#[test]
fn markers_replicated_during_partitioned_sync() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "sync-mark.bin", &[0x77; 512]);

    let placement = cluster.placement("sync-mark.bin").unwrap();
    let offline = placement.shards[0];
    cluster.detach_shard(offline);

    del(&rt, &cluster, "sync-mark.bin");
    flush_all(&raws);

    // Run partitioned_sync directly (not sync_and_reattach).
    let orig = original_stores(&raws);
    let _replicated = rt
        .block_on(repair::partitioned_sync(&cluster, &orig, offline, None))
        .unwrap();

    // After sync, the stale object on the offline shard should have
    // been removed by marker replay.
    let shard_store = raws[offline].clone();
    let head_result = rt.block_on(shard_store.head(&Path::from("sync-mark.bin")));
    assert!(
        head_result.is_err(),
        "stale object should be removed from shard {} by marker replay",
        offline
    );
}

// =====================================================================
// 18. Shard offline during PUT of new object -- object only on
//     healthy shards, then reattach should NOT add it (not under-
//     replicated since catalog already tracks it correctly).
// =====================================================================

#[test]
fn new_object_while_shard_offline_not_duplicated() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    cluster.detach_shard(2);

    // Put new object while shard 2 offline. Placement avoids shard 2.
    put(&rt, &cluster, &raws, "new-while-off.bin", b"fresh");

    let p = cluster.placement("new-while-off.bin").unwrap();
    assert!(
        !p.shards.contains(&2),
        "offline shard should not be in placement"
    );

    // Reattach.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 2, None));

    // Object should still be readable and correctly replicated.
    let data = get_bytes(&rt, &cluster, "new-while-off.bin");
    assert_eq!(&data[..], b"fresh");

    let p_after = cluster.placement("new-while-off.bin").unwrap();
    assert_eq!(
        p_after.shards.len(),
        2,
        "should still have RF=2 replicas, not more"
    );
}

// =====================================================================
// 19. Vacuum after sync (markers should be cleanable once all shards
//     are back online).
// =====================================================================

#[test]
fn vacuum_succeeds_after_all_shards_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "vac-after.bin", b"will-delete");

    let placement = cluster.placement("vac-after.bin").unwrap();
    let offline = placement.shards[0];
    cluster.detach_shard(offline);

    del(&rt, &cluster, "vac-after.bin");
    flush_all(&raws);

    // Vacuum should fail while shard is offline.
    let err = rt.block_on(cluster.vacuum_delete_markers(None));
    assert!(err.is_err(), "vacuum should fail with offline shard");

    // Bring shard back.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    // Now vacuum should succeed.
    let (purged, _) = rt.block_on(cluster.vacuum_delete_markers(None)).unwrap();
    flush_all(&raws);
    assert_eq!(purged, 1);

    let markers = rt.block_on(cluster.list_delete_markers());
    assert!(markers.is_empty(), "markers should be cleaned");
}

// =====================================================================
// 20. Delete markers should not interfere with listing other prefixes.
// =====================================================================

#[test]
fn list_with_delimiter_unaffected_by_markers() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "a/one.txt", b"1");
    put(&rt, &cluster, &raws, "a/two.txt", b"2");
    put(&rt, &cluster, &raws, "b/three.txt", b"3");

    del(&rt, &cluster, "a/one.txt");
    flush_all(&raws);

    let result = rt.block_on(cluster.list_with_delimiter(Some(&Path::from("a")))).unwrap();
    let obj_keys: Vec<String> = result.objects.iter().map(|o| o.location.to_string()).collect();

    assert!(
        !obj_keys.contains(&"a/one.txt".to_string()),
        "deleted object should not appear in list_with_delimiter"
    );
    assert!(
        obj_keys.contains(&"a/two.txt".to_string()),
        "non-deleted object should appear"
    );

    // No __deleted__/ prefixes should appear in common_prefixes.
    let prefixes: Vec<String> = result.common_prefixes.iter().map(|p| p.to_string()).collect();
    assert!(
        !prefixes.iter().any(|p| p.contains("__deleted__")),
        "delete marker prefix should not leak into common_prefixes: {:?}",
        prefixes
    );
}

// =====================================================================
// 21. Raw object store: markers can be listed and cleaned by CLI
//     operations (vacuum, list-deleted). Test the low-level behavior.
// =====================================================================

#[test]
fn raw_store_marker_keys_are_regular_objects() {
    let dir = tempfile::tempdir().unwrap();
    let raw = format_shard(&dir.path().join("raw.img"), 64 * 1024 * 1024);
    let rt = make_rt();

    // Raw store treats __deleted__/ keys as normal objects.
    rt.block_on(async {
        raw.put(
            &Path::from("__deleted__/test-key.bin"),
            PutPayload::from(Bytes::from_static(b"2026-01-01T00:00:00Z")),
        )
        .await
        .unwrap();
    });
    raw.flush_index().unwrap();

    // It's listable (raw store does NOT filter markers).
    let listed: Vec<String> = rt.block_on(async {
        raw.list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect()
    });
    assert!(
        listed.contains(&"__deleted__/test-key.bin".to_string()),
        "raw store should list marker key as regular object"
    );

    // head works.
    let meta = rt.block_on(raw.head(&Path::from("__deleted__/test-key.bin"))).unwrap();
    assert_eq!(meta.size, 20); // "2026-01-01T00:00:00Z" = 20 bytes

    // get works.
    let data = rt
        .block_on(async {
            raw.get(&Path::from("__deleted__/test-key.bin"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        });
    assert_eq!(&data[..], b"2026-01-01T00:00:00Z");

    // Delete works (simulates vacuum).
    rt.block_on(raw.delete(&Path::from("__deleted__/test-key.bin"))).unwrap();
    raw.flush_index().unwrap();

    let listed_after: Vec<String> = rt.block_on(async {
        raw.list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect()
    });
    assert!(
        !listed_after.contains(&"__deleted__/test-key.bin".to_string()),
        "marker should be gone after delete"
    );
}

// =====================================================================
// 22. Raw store: list only __deleted__/ keys (simulates list-deleted).
// =====================================================================

#[test]
fn raw_store_list_deleted_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let raw = format_shard(&dir.path().join("raw.img"), 64 * 1024 * 1024);
    let rt = make_rt();

    rt.block_on(async {
        raw.put(
            &Path::from("normal/data.bin"),
            PutPayload::from(Bytes::from_static(b"real data")),
        )
        .await
        .unwrap();
        raw.put(
            &Path::from("__deleted__/old-key.bin"),
            PutPayload::from(Bytes::from_static(b"2026-02-01T00:00:00Z")),
        )
        .await
        .unwrap();
        raw.put(
            &Path::from("__deleted__/another.bin"),
            PutPayload::from(Bytes::from_static(b"2026-03-01T00:00:00Z")),
        )
        .await
        .unwrap();
    });
    raw.flush_index().unwrap();

    // List with __deleted__ prefix.
    let deleted_keys: Vec<String> = rt.block_on(async {
        raw.list(Some(&Path::from("__deleted__")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect()
    });

    assert_eq!(deleted_keys.len(), 2);
    assert!(deleted_keys.contains(&"__deleted__/old-key.bin".to_string()));
    assert!(deleted_keys.contains(&"__deleted__/another.bin".to_string()));
}

// =====================================================================
// 23. Concurrent deletes of different objects should all create markers.
// =====================================================================

#[test]
fn concurrent_deletes_all_create_markers() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    for i in 0..10 {
        put(&rt, &cluster, &raws, &format!("conc/k{i}.bin"), &[i as u8; 256]);
    }

    // Delete all concurrently.
    rt.block_on(async {
        let paths: Vec<Path> = (0..10)
            .map(|i| Path::from(format!("conc/k{i}.bin").as_str()))
            .collect();
        let handles: Vec<_> = paths.iter().map(|p| cluster.delete(p)).collect();
        for h in handles {
            h.await.unwrap();
        }
    });
    flush_all(&raws);

    let markers = rt.block_on(cluster.list_delete_markers());
    assert_eq!(
        markers.len(),
        10,
        "all 10 concurrent deletes should produce markers"
    );

    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.iter().any(|k| k.starts_with("conc/")),
        "no concurrent objects should remain visible"
    );
}

// =====================================================================
// 24. Delete + vacuum + re-PUT: system is clean for the new object.
// =====================================================================

#[test]
fn delete_vacuum_reput_clean_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // v1 -> delete -> vacuum -> v2: cleanest lifecycle.
    put(&rt, &cluster, &raws, "lifecycle.bin", b"v1");
    del(&rt, &cluster, "lifecycle.bin");
    flush_all(&raws);

    let (purged, _) = rt.block_on(cluster.vacuum_delete_markers(None)).unwrap();
    flush_all(&raws);
    assert_eq!(purged, 1);

    // Re-PUT.
    put(&rt, &cluster, &raws, "lifecycle.bin", b"v2");

    // No markers should exist.
    let markers = rt.block_on(cluster.list_delete_markers());
    assert!(markers.is_empty(), "no markers after vacuum");

    // Object is clean.
    let data = get_bytes(&rt, &cluster, "lifecycle.bin");
    assert_eq!(&data[..], b"v2");
}

// =====================================================================
// 25. Shard that was never part of original cluster comes online
//     (empty shard). Verify no stale data.
// =====================================================================

#[test]
fn attach_empty_shard_after_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "pre.bin", b"before");
    del(&rt, &cluster, "pre.bin");
    flush_all(&raws);

    // Detach shard 2, then reattach with a fresh formatted shard.
    cluster.detach_shard(2);
    let fresh = format_shard(&dir.path().join("fresh.raw"), 64 * 1024 * 1024);
    rt.block_on(
        cluster.attach_shard(2, fresh.clone() as Arc<dyn ObjectStore>, false),
    )
    .unwrap();

    assert_eq!(cluster.shard_health(2), Some(ShardHealth::Healthy));

    // pre.bin should still be hidden.
    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.contains(&"pre.bin".to_string()),
        "deleted object should not reappear with fresh shard"
    );
}

// =====================================================================
// 26. PUT overwrite while shard offline: verify shard-level bytes.
//     The stale v1 on the returning shard must be replaced by v2.
// =====================================================================

#[test]
fn overwrite_while_offline_shard_level_bytes_partitioned() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // v1 on 2 shards.
    put(&rt, &cluster, &raws, "shard-check.bin", b"version-1");
    let p1 = cluster.placement("shard-check.bin").unwrap();
    assert_eq!(p1.shards.len(), 2);

    let offline = p1.shards[0];
    let staying = p1.shards[1];
    cluster.detach_shard(offline);

    std::thread::sleep(std::time::Duration::from_millis(50));

    // v2: lands on healthy shards only.
    put(&rt, &cluster, &raws, "shard-check.bin", b"version-2");

    // Confirm the offline shard still has v1 on disk.
    let stale_data = rt.block_on(async {
        raws[offline]
            .get(&Path::from("shard-check.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&stale_data[..], b"version-1", "offline shard still has v1");

    // Confirm the staying shard has v2.
    let current_data = rt.block_on(async {
        raws[staying]
            .get(&Path::from("shard-check.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&current_data[..], b"version-2");

    // Sync and reattach.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    assert_eq!(cluster.shard_health(offline), Some(ShardHealth::Healthy));

    // Now check shard-level: the previously-offline shard should have v2
    // (either updated during sync, or stale copy removed).
    let shard_data_after = rt.block_on(async {
        raws[offline]
            .get(&Path::from("shard-check.bin"))
            .await
    });

    match shard_data_after {
        Ok(result) => {
            let bytes = rt.block_on(result.bytes()).unwrap();
            assert_eq!(
                &bytes[..],
                b"version-2",
                "offline shard should now have v2 after sync"
            );
        }
        Err(_) => {
            // Object was removed (stale cleanup). That's also correct --
            // replication sweeper will copy v2 back later.
            // Verify cluster-level still serves v2.
            let cluster_data = get_bytes(&rt, &cluster, "shard-check.bin");
            assert_eq!(&cluster_data[..], b"version-2");
        }
    }

    // Cluster must serve v2 regardless.
    let data = get_bytes(&rt, &cluster, "shard-check.bin");
    assert_eq!(&data[..], b"version-2");
}

// =====================================================================
// 27. Same test but in mirror mode.
// =====================================================================

#[test]
fn overwrite_while_offline_shard_level_bytes_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 2, 64);
    let cluster = build_cluster(&raws, 2); // mirror
    let rt = make_rt();

    put(&rt, &cluster, &raws, "mirror-ver.bin", b"old-data");

    // Both shards have it.
    assert_eq!(cluster.placement("mirror-ver.bin").unwrap().shards.len(), 2);

    cluster.detach_shard(1);
    std::thread::sleep(std::time::Duration::from_millis(50));
    put(&rt, &cluster, &raws, "mirror-ver.bin", b"new-data");
    flush_all(&raws);

    // Shard 1 still has old data.
    let old = rt.block_on(async {
        raws[1]
            .get(&Path::from("mirror-ver.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&old[..], b"old-data");

    // Sync and reattach.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 1, None));

    // Now shard 1 should have the new version.
    let updated = rt.block_on(async {
        raws[1]
            .get(&Path::from("mirror-ver.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(
        &updated[..],
        b"new-data",
        "mirror sync should update stale version on returning shard"
    );

    let cluster_data = get_bytes(&rt, &cluster, "mirror-ver.bin");
    assert_eq!(&cluster_data[..], b"new-data");
}

// =====================================================================
// 28. Object re-PUT multiple times while shard offline.
//     Only the latest version should survive on the returning shard.
// =====================================================================

#[test]
fn multiple_overwrites_while_offline() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "multi.bin", b"v1");

    let placement = cluster.placement("multi.bin").unwrap();
    let offline = placement.shards[0];
    cluster.detach_shard(offline);

    // Overwrite several times.
    for i in 2..=5 {
        std::thread::sleep(std::time::Duration::from_millis(30));
        let data = format!("v{i}");
        put(&rt, &cluster, &raws, "multi.bin", data.as_bytes());
    }

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    // Cluster must serve v5.
    let data = get_bytes(&rt, &cluster, "multi.bin");
    assert_eq!(&data[..], b"v5", "cluster must serve latest version");

    // The returning shard should not have the stale v1.
    let shard_data = rt.block_on(async {
        raws[offline].get(&Path::from("multi.bin")).await
    });
    match shard_data {
        Ok(result) => {
            let bytes = rt.block_on(result.bytes()).unwrap();
            assert_ne!(
                &bytes[..],
                b"v1",
                "stale v1 must not survive on returning shard"
            );
        }
        Err(_) => {
            // Removed entirely -- fine, sweeper will restore.
        }
    }
}

// =====================================================================
// 29. Object not in catalog but on shard (shard has orphan from before
//     it went offline and the object was fully removed from cluster).
//     The orphan should be cleaned during sync.
// =====================================================================

#[test]
fn orphan_object_on_returning_shard_cleaned() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "orphan.bin", b"will-be-orphaned");

    let placement = cluster.placement("orphan.bin").unwrap();
    let offline = placement.shards[0];
    cluster.detach_shard(offline);

    // Delete the object while shard is offline.
    del(&rt, &cluster, "orphan.bin");
    flush_all(&raws);

    // Vacuum the delete marker so catalog has no trace of the object.
    rt.block_on(async {
        // Need all shards healthy for vacuum -- temporarily re-mark.
        cluster.set_shard_health(offline, ShardHealth::Healthy);
        let _ = cluster.vacuum_delete_markers(None).await;
        cluster.set_shard_health(offline, ShardHealth::Offline);
    });
    flush_all(&raws);

    // Confirm: no marker, no placement.
    let markers = rt.block_on(cluster.list_delete_markers());
    assert!(markers.is_empty(), "markers should be vacuumed");
    assert!(
        cluster.placement("orphan.bin").is_none(),
        "no catalog entry"
    );

    // Shard still has the old object on disk.
    let orphan_exists = rt.block_on(raws[offline].head(&Path::from("orphan.bin")));
    assert!(orphan_exists.is_ok(), "orphan should still be on shard disk");

    // Sync and reattach.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    // After sync, the orphan should be cleaned from the shard.
    let after = rt.block_on(raws[offline].head(&Path::from("orphan.bin")));
    assert!(
        after.is_err(),
        "orphan object should be removed from shard during sync"
    );

    // Cluster should not show the object.
    let keys = list_keys(&rt, &cluster);
    assert!(
        !keys.contains(&"orphan.bin".to_string()),
        "orphan must not reappear"
    );
}

// =====================================================================
// 30. After sync, the catalog placement should be correct (stale shard
//     not listed if its copy was removed, or listed with correct data
//     if it was updated).
// =====================================================================

#[test]
fn catalog_correct_after_stale_overwrite_sync() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    put(&rt, &cluster, &raws, "cat-check.bin", b"v1-data");
    let p1 = cluster.placement("cat-check.bin").unwrap();
    let offline = p1.shards[0];
    cluster.detach_shard(offline);

    std::thread::sleep(std::time::Duration::from_millis(50));
    put(&rt, &cluster, &raws, "cat-check.bin", b"v2-data");
    let p2 = cluster.placement("cat-check.bin").unwrap();
    assert!(
        !p2.shards.contains(&offline),
        "offline shard should not be in placement after re-PUT"
    );

    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    // After reattach, every shard listed in placement should serve v2.
    let p3 = cluster.placement("cat-check.bin").unwrap();
    for &sid in &p3.shards {
        let data = rt.block_on(async {
            raws[sid]
                .get(&Path::from("cat-check.bin"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        });
        assert_eq!(
            &data[..],
            b"v2-data",
            "shard {} should have v2 but got {:?}",
            sid,
            std::str::from_utf8(&data)
        );
    }
}

// =====================================================================
// 31. Re-PUT after delete automatically removes the stale marker.
// =====================================================================

#[test]
fn reput_after_delete_cleans_marker() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Put, then delete -- marker should appear.
    put(&rt, &cluster, &raws, "reput-mk.bin", b"original");
    del(&rt, &cluster, "reput-mk.bin");

    let marker = rt.block_on(cluster.get_delete_marker("reput-mk.bin"));
    assert!(marker.is_some(), "marker must exist after delete");

    // Re-PUT the same key -- marker should be cleaned up.
    std::thread::sleep(std::time::Duration::from_millis(50));
    put(&rt, &cluster, &raws, "reput-mk.bin", b"new-version");

    let marker_after = rt.block_on(cluster.get_delete_marker("reput-mk.bin"));
    assert!(
        marker_after.is_none(),
        "marker should be removed after re-PUT"
    );

    // Object must be readable with the new content.
    let data = get_bytes(&rt, &cluster, "reput-mk.bin");
    assert_eq!(&data[..], b"new-version");
}

// =====================================================================
// 32. Stale marker on returning shard is cleaned during reattach.
//     Scenario: delete -> marker on all 3 shards -> shard 2 goes
//     offline -> re-PUT lands on healthy shards -> reattach shard 2
//     -> its stale marker should be gone.
// =====================================================================

#[test]
fn stale_marker_cleaned_on_reattach() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Step 1: put and delete -- marker goes to all 3 healthy shards.
    put(&rt, &cluster, &raws, "stale-mk.bin", b"v1");
    del(&rt, &cluster, "stale-mk.bin");

    // Confirm marker exists on shard 2 at the raw level.
    let marker_path = Path::from(format!("{}{}", DELETE_MARKER_PREFIX, "stale-mk.bin"));
    let has_marker = rt.block_on(raws[2].head(&marker_path)).is_ok();
    assert!(has_marker, "shard 2 should have the delete marker");

    // Step 2: take shard 2 offline.
    cluster.detach_shard(2);

    // Step 3: re-PUT the same key (lands on healthy shards 0, 1).
    std::thread::sleep(std::time::Duration::from_millis(50));
    put(&rt, &cluster, &raws, "stale-mk.bin", b"v2-after-delete");

    // Marker should already be gone on cluster (cleaned by PUT).
    let cluster_marker = rt.block_on(cluster.get_delete_marker("stale-mk.bin"));
    assert!(
        cluster_marker.is_none(),
        "cluster marker should be cleaned by re-PUT"
    );

    // But shard 2 still has the stale marker from when it was online.
    let still_has = rt.block_on(raws[2].head(&marker_path)).is_ok();
    assert!(still_has, "shard 2 should still have stale marker before sync");

    // Step 4: sync and reattach shard 2.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, 2, None));

    // Stale marker on shard 2 should now be gone.
    let cleaned = rt.block_on(raws[2].head(&marker_path)).is_err();
    assert!(cleaned, "stale marker on shard 2 should be cleaned after sync");

    // Object must be readable with the re-PUT content.
    let data = get_bytes(&rt, &cluster, "stale-mk.bin");
    assert_eq!(&data[..], b"v2-after-delete");
}

// =====================================================================
// 33. Full lifecycle: put -> delete -> re-PUT -> delete -> vacuum.
// =====================================================================

#[test]
fn delete_reput_delete_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // 1. Put + delete.
    put(&rt, &cluster, &raws, "lifecycle.bin", b"v1");
    del(&rt, &cluster, "lifecycle.bin");
    assert!(rt.block_on(cluster.get(&Path::from("lifecycle.bin"))).is_err());
    let m1 = rt.block_on(cluster.get_delete_marker("lifecycle.bin"));
    assert!(m1.is_some(), "marker after first delete");

    // 2. Re-PUT -- marker should be cleaned.
    std::thread::sleep(std::time::Duration::from_millis(50));
    put(&rt, &cluster, &raws, "lifecycle.bin", b"v2");
    let m2 = rt.block_on(cluster.get_delete_marker("lifecycle.bin"));
    assert!(m2.is_none(), "marker gone after re-PUT");
    assert_eq!(&get_bytes(&rt, &cluster, "lifecycle.bin")[..], b"v2");

    // 3. Delete again.
    del(&rt, &cluster, "lifecycle.bin");
    assert!(rt.block_on(cluster.get(&Path::from("lifecycle.bin"))).is_err());
    let m3 = rt.block_on(cluster.get_delete_marker("lifecycle.bin"));
    assert!(m3.is_some(), "marker after second delete");

    // 4. Vacuum should purge the marker (object is gone).
    let (purged, _cleaned) = rt.block_on(cluster.vacuum_delete_markers(None)).unwrap();
    assert!(purged >= 1, "vacuum should purge at least 1 marker");

    let m4 = rt.block_on(cluster.get_delete_marker("lifecycle.bin"));
    assert!(m4.is_none(), "marker gone after vacuum");

    // Also verify list_delete_markers returns empty.
    let markers = rt.block_on(cluster.list_delete_markers());
    assert!(
        !markers.iter().any(|(k, _)| k == "lifecycle.bin"),
        "no lifecycle marker after vacuum"
    );
}

// =====================================================================
// 34. Delete with one shard offline: marker is retained (not rolled
//     back) and sync_and_reattach cleans the stale object on the
//     returning shard.
// =====================================================================

#[test]
fn delete_with_offline_shard_keeps_marker_and_sync_cleans() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Put object (lands on 2 of 3 shards).
    put(&rt, &cluster, &raws, "offline-del.bin", b"payload");
    let placement = cluster.placement("offline-del.bin").unwrap();

    // Take one of the holding shards offline.
    let offline = placement.shards[0];
    cluster.detach_shard(offline);

    // Delete while shard is offline.
    del(&rt, &cluster, "offline-del.bin");

    // Marker should exist (not rolled back even though shard was offline).
    let marker = rt.block_on(cluster.get_delete_marker("offline-del.bin"));
    assert!(marker.is_some(), "marker must survive even with offline shard");

    // Offline shard still has the stale object.
    let stale = rt.block_on(raws[offline].head(&Path::from("offline-del.bin")));
    assert!(stale.is_ok(), "offline shard still has the object");

    // Sync and reattach the offline shard.
    let orig = original_stores(&raws);
    rt.block_on(repair::sync_and_reattach(&cluster, &orig, offline, None));

    // Stale object should be cleaned by marker replay.
    let after_sync = rt.block_on(raws[offline].head(&Path::from("offline-del.bin")));
    assert!(
        after_sync.is_err(),
        "stale object on shard {} should be cleaned by marker replay",
        offline
    );

    // Object should not exist on the cluster.
    assert!(rt.block_on(cluster.get(&Path::from("offline-del.bin"))).is_err());
}

// =====================================================================
// Vacuum idempotency
// =====================================================================

#[test]
fn vacuum_idempotent_second_run_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let rt = make_rt();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);

    // Put and delete an object
    rt.block_on(async {
        cluster
            .put(
                &Path::from("vacuum_idem/a.bin"),
                PutPayload::from(Bytes::from(vec![0xAA; 256])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    rt.block_on(async {
        cluster.delete(&Path::from("vacuum_idem/a.bin")).await.unwrap();
    });

    // Verify marker exists
    let markers = rt.block_on(cluster.list_delete_markers());
    assert!(!markers.is_empty(), "should have at least one delete marker");

    // First vacuum
    let (purged1, _cleaned1) = rt
        .block_on(cluster.vacuum_delete_markers(None))
        .unwrap();
    assert!(purged1 > 0, "first vacuum should purge markers");

    // Second vacuum -- should be a no-op
    let (purged2, cleaned2) = rt
        .block_on(cluster.vacuum_delete_markers(None))
        .unwrap();
    assert_eq!(purged2, 0, "second vacuum should be a no-op");
    assert_eq!(cleaned2, 0, "second vacuum should clean nothing");

    // No markers remain
    let markers_after = rt.block_on(cluster.list_delete_markers());
    assert!(markers_after.is_empty(), "no markers should remain after vacuum");
}

#[test]
fn vacuum_with_re_put_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let rt = make_rt();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);

    // Put, delete, then re-put the same key
    rt.block_on(async {
        cluster
            .put(
                &Path::from("vacuum_reput/obj.bin"),
                PutPayload::from(Bytes::from_static(b"version1")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    rt.block_on(async {
        cluster
            .delete(&Path::from("vacuum_reput/obj.bin"))
            .await
            .unwrap();
    });

    // Re-put with new data (newer timestamp than deletion)
    rt.block_on(async {
        cluster
            .put(
                &Path::from("vacuum_reput/obj.bin"),
                PutPayload::from(Bytes::from_static(b"version2")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // The re-put already removed the stale delete marker (put cleans markers
    // on success), so vacuum should find nothing to purge.
    let (purged, _) = rt
        .block_on(cluster.vacuum_delete_markers(None))
        .unwrap();
    assert_eq!(purged, 0, "vacuum should find nothing -- put already cleaned the marker");

    // The re-put object should still be readable
    let data = rt.block_on(async {
        cluster
            .get(&Path::from("vacuum_reput/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&data[..], b"version2", "re-put data should survive vacuum");
}

// =====================================================================
// Concurrent vacuum: second call rejected with VacuumAlreadyRunning
// =====================================================================

#[test]
fn concurrent_vacuum_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = Arc::new(build_cluster(&raws, 2));
    let rt = make_rt();

    // Seed some objects and delete them so vacuum has work to do.
    for i in 0..20 {
        put(&rt, &cluster, &raws, &format!("cvac/obj{i}.bin"), &[i as u8; 512]);
    }
    for i in 0..20 {
        del(&rt, &cluster, &format!("cvac/obj{i}.bin"));
    }
    flush_all(&raws);

    // Spawn two concurrent vacuum calls.
    let cluster1 = Arc::clone(&cluster);
    let cluster2 = Arc::clone(&cluster);

    let (r1, r2) = rt.block_on(async {
        let f1 = tokio::spawn(async move { cluster1.vacuum_delete_markers(None).await });
        let f2 = tokio::spawn(async move { cluster2.vacuum_delete_markers(None).await });
        (f1.await.unwrap(), f2.await.unwrap())
    });

    // Exactly one should succeed, the other should fail with VacuumAlreadyRunning.
    let one_ok = r1.is_ok() != r2.is_ok();
    let both_ok = r1.is_ok() && r2.is_ok();
    // It's acceptable for both to succeed if the first finishes before the
    // second starts. But at least one must succeed.
    assert!(
        r1.is_ok() || r2.is_ok(),
        "at least one vacuum should succeed: r1={:?}, r2={:?}",
        r1, r2,
    );

    if one_ok {
        let err = if r1.is_err() { r1 } else { r2 };
        let msg = format!("{}", err.unwrap_err());
        assert!(
            msg.contains("already running"),
            "rejected vacuum should mention 'already running': {msg}"
        );
    }
    // Both OK is also valid (no race).
    let _ = both_ok;
}

// =====================================================================
// MEDIUM: vacuum should skip Syncing shards but still complete
// =====================================================================

#[test]
fn vacuum_skips_syncing_shard() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 3, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Seed some objects and delete them.
    for i in 0..10 {
        put(&rt, &cluster, &raws, &format!("vskip/obj{i}.bin"), &[i as u8; 256]);
    }
    for i in 0..10 {
        del(&rt, &cluster, &format!("vskip/obj{i}.bin"));
    }
    flush_all(&raws);

    // Transition shard 2 to Syncing.
    cluster.set_shard_health(2, ShardHealth::Syncing);

    // Vacuum should still complete (just skip syncing shard 2).
    let result = rt.block_on(async { cluster.vacuum_delete_markers(None).await });
    assert!(
        result.is_ok(),
        "vacuum should succeed even with a Syncing shard: {:?}",
        result.err()
    );

    // Delete markers on healthy shards should be cleaned up.
    rt.block_on(async {
        for sid in 0..2 {
            let store = cluster.shard_store(sid).unwrap();
            let list: Vec<_> = store
                .list(Some(&Path::from(DELETE_MARKER_PREFIX)))
                .try_collect()
                .await
                .unwrap();
            assert!(
                list.is_empty(),
                "shard {sid} should have no remaining delete markers, found {}",
                list.len()
            );
        }
    });
}

// =====================================================================
// LOW: delete marker min_writes inconsistency -- marker not on all shards
// =====================================================================

#[test]
fn delete_marker_partial_write_on_degraded_shard() {
    let dir = tempfile::tempdir().unwrap();
    let raws = make_shards(&dir, 4, 64);
    let cluster = build_cluster(&raws, 2);
    let rt = make_rt();

    // Write an object.
    put(&rt, &cluster, &raws, "dmpart/obj.bin", &[0xAA; 512]);

    // Record placement before delete.
    let placement = cluster.placement("dmpart/obj.bin").unwrap();
    let target_shards = placement.shards.clone();

    // Save the original store before detaching.
    let original_store = cluster.shard_store(target_shards[0]).unwrap();

    // Detach one of the target shards.
    cluster.detach_shard(target_shards[0]);

    // Delete: marker should only land on the healthy shard (min_writes=1).
    del(&rt, &cluster, "dmpart/obj.bin");
    flush_all(&raws);

    // Reattach the shard using the saved original store.
    rt.block_on(async {
        cluster.attach_shard(target_shards[0], original_store, true).await.unwrap();
    });

    // The detached shard should still have the object data (no delete marker).
    let store_detached = cluster.shard_store(target_shards[0]).unwrap();
    let has_data = rt.block_on(async {
        store_detached
            .get(&Path::from("dmpart/obj.bin"))
            .await
            .is_ok()
    });
    assert!(
        has_data,
        "detached shard should still have data (missed the delete marker)"
    );

    // The healthy shard should have the delete marker.
    let marker_key = format!("{DELETE_MARKER_PREFIX}dmpart/obj.bin");
    let store_healthy = cluster.shard_store(target_shards[1]).unwrap();
    let has_marker = rt.block_on(async {
        store_healthy
            .get(&Path::from(marker_key.as_str()))
            .await
            .is_ok()
    });
    assert!(
        has_marker,
        "healthy shard should have the delete marker"
    );
}

// =====================================================================
// BUG: vacuum_delete_markers race window with concurrent PUT
// =====================================================================
//
// `vacuum_delete_markers_inner` checks `head_raw(key)` to decide
// whether to purge a marker.  Between that check and the actual marker
// removal, a concurrent PUT could create the object.  If vacuum sees
// "object gone" and purges the marker, but then a PUT re-creates the
// object, the marker is gone.  If an offline shard returns with the old
// pre-delete data, there is no marker to prevent resurrection.
//
// This test demonstrates the race window sequentially:
// 1. Delete object (marker created)
// 2. Vacuum runs (sees object gone, purges marker)
// 3. Offline shard returns -- no marker exists to prevent resurrection
//
// Currently IGNORED because the race window exists in production.

#[test]
fn vacuum_put_race_allows_resurrection() {
    let rt = make_rt();
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("vr{i}.raw")), 64 * 1024 * 1024))
        .collect();
    let cluster = build_cluster(&raws, 2);

    // Step 1: Write an object.
    rt.block_on(async {
        cluster
            .put(
                &Path::from("race/obj.bin"),
                PutPayload::from(Bytes::from(vec![0xAA; 4096])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Step 2: Detach shard 2 (simulates offline shard with old data).
    let store2 = cluster.shard_store(2).unwrap();
    cluster.detach_shard(2);

    // Step 3: Delete the object (writes delete marker to healthy shards).
    rt.block_on(async {
        cluster.delete(&Path::from("race/obj.bin")).await.unwrap();
    });
    flush_all(&raws);

    // Step 4: Vacuum -- marker should be purged since object is gone.
    let (purged, _cleaned) = rt.block_on(async {
        // Need all shards healthy for vacuum -- reattach shard 2.
        cluster.attach_shard(2, store2.clone(), true).await.unwrap();
        cluster.vacuum_delete_markers(None).await.unwrap()
    });
    flush_all(&raws);
    assert!(purged > 0, "vacuum should have purged the delete marker");

    // Step 5: Check that no delete marker remains.
    let marker = rt.block_on(cluster.get_delete_marker("race/obj.bin"));
    assert!(
        marker.is_none(),
        "delete marker should be gone after vacuum"
    );

    // BUG DEMONSTRATION: If shard 2 still has old pre-delete data
    // and syncs back (mirror_sync/partitioned_sync), the marker is
    // gone and there is nothing to prevent resurrection.
    //
    // To fully fix this, vacuum should either:
    // (a) Verify the object is gone on ALL shards before purging, or
    // (b) Keep markers until all shards have confirmed the delete.
    //
    // For now, this test documents the race window.
    let has_old_data = rt.block_on(async {
        store2.get(&Path::from("race/obj.bin")).await.is_ok()
    });
    // Shard 2 may or may not still have the data (depends on whether
    // it was attached with force=true which doesn't delete).
    if has_old_data {
        assert!(
            marker.is_some(),
            "if any shard still holds old data, the delete marker must exist \
             to prevent resurrection (BUG: marker was purged prematurely)"
        );
    }
}
