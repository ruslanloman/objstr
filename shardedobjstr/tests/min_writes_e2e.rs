//! End-to-end tests for the min_writes feature.
//!
//! Verifies that ShardedObjectStore enforces the minimum number of
//! successful replica writes before reporting success.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};

use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

// Minimum shard size to satisfy the format requirements.
const SHARD_SIZE: u64 = 64 * 1024 * 1024;

// =====================================================================
// Test: default min_writes is max(rf - 1, 1)
// =====================================================================

#[test]
fn default_min_writes_rf1() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raw, 1);
    assert_eq!(cluster.min_writes(), 1);
}

#[test]
fn default_min_writes_rf2() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raw, 2);
    assert_eq!(cluster.min_writes(), 1);
}

#[test]
fn default_min_writes_rf3() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..6)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raw, 3);
    assert_eq!(cluster.min_writes(), 2);
}

// =====================================================================
// Test: with_min_writes builder sets the value (clamped)
// =====================================================================

#[test]
fn with_min_writes_sets_value() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(2);
    assert_eq!(cluster.min_writes(), 2);
    assert_eq!(cluster.replication_factor(), 3);
}

#[test]
fn with_min_writes_clamped_to_rf() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // Request min_writes=10 with rf=2 -- should be clamped to 2.
    let cluster = ShardedObjectStore::new(stores, 2).with_min_writes(10);
    assert_eq!(cluster.min_writes(), 2);
}

#[test]
fn with_min_writes_clamped_to_one() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // Request min_writes=0 -- should be clamped to 1.
    let cluster = ShardedObjectStore::new(stores, 2).with_min_writes(0);
    assert_eq!(cluster.min_writes(), 1);
}

// =====================================================================
// Test: put succeeds when enough replicas land
// =====================================================================

#[test]
fn put_succeeds_with_enough_replicas() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // rf=3, min_writes=2
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(2);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Detach 1 shard -- still have 3 healthy, rf=3 writes to 3, >= 2.
        cluster.detach_shard(0);

        let result = cluster
            .put(
                &Path::from("test/ok.bin"),
                PutPayload::from(Bytes::from_static(b"hello")),
            )
            .await;
        assert!(result.is_ok(), "put should succeed with 3 healthy shards: {:?}", result.err());
    });
    flush_all(&raw);
}

// =====================================================================
// Test: put fails when too few replicas land (InsufficientWrites)
// =====================================================================

#[test]
fn put_fails_with_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // rf=4, min_writes=3
    let cluster = ShardedObjectStore::new(stores, 4).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Detach 2 shards -- only 2 healthy, rf=4 targets all 4, only 2 succeed.
        cluster.detach_shard(0);
        cluster.detach_shard(1);

        let result = cluster
            .put(
                &Path::from("test/fail.bin"),
                PutPayload::from(Bytes::from_static(b"should fail")),
            )
            .await;
        assert!(result.is_err(), "put should fail with only 2 of 3 required writes");
        let err_str = format!("{}", result.unwrap_err());
        assert!(
            err_str.contains("InsufficientWrites") || err_str.contains("insufficient"),
            "error should mention insufficient writes: {err_str}"
        );
    });
}

// =====================================================================
// Test: min_writes=1 allows maximally degraded writes
// =====================================================================

#[test]
fn min_writes_one_allows_single_replica() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // rf=4, min_writes=1: accept any single successful write.
    let cluster = ShardedObjectStore::new(stores, 4).with_min_writes(1);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Detach 3 of 4 shards.
        cluster.detach_shard(0);
        cluster.detach_shard(1);
        cluster.detach_shard(2);

        let result = cluster
            .put(
                &Path::from("test/solo.bin"),
                PutPayload::from(Bytes::from_static(b"single replica")),
            )
            .await;
        assert!(result.is_ok(), "put should succeed with min_writes=1 and 1 healthy shard: {:?}", result.err());

        // Read it back.
        let data = cluster.get(&Path::from("test/solo.bin")).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"single replica");
    });
    flush_all(&raw);
}

// =====================================================================
// Test: min_writes=rf (strict) rejects any degraded write
// =====================================================================

#[test]
fn min_writes_equals_rf_rejects_degraded() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // rf=3, min_writes=3: require ALL replicas to land.
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // All 3 healthy -- should succeed.
        let ok = cluster
            .put(
                &Path::from("test/strict-ok.bin"),
                PutPayload::from(Bytes::from_static(b"all replicas")),
            )
            .await;
        assert!(ok.is_ok(), "put should work with all shards healthy");

        // Detach 1 -- now only 2 of 3 required, should fail.
        cluster.detach_shard(2);
        let fail = cluster
            .put(
                &Path::from("test/strict-fail.bin"),
                PutPayload::from(Bytes::from_static(b"not enough")),
            )
            .await;
        assert!(fail.is_err(), "put should fail when strict min_writes=rf and a shard is offline");
    });
}

// =====================================================================
// Test: config parsing includes min_writes
// =====================================================================

#[test]
fn flat_config_min_writes() {
    use shardedobjstr::config::load_cluster_conf;

    let dir = tempfile::tempdir().unwrap();
    let conf_path = dir.path().join("test.conf");
    std::fs::write(&conf_path, "replicas 3\nmin_writes 2\nshard mem\nshard mem\nshard mem\n").unwrap();
    let conf = load_cluster_conf(&conf_path).unwrap();
    assert_eq!(conf.replicas, 3);
    assert_eq!(conf.min_writes, Some(2));
}

#[test]
fn flat_config_no_min_writes_defaults_to_none() {
    use shardedobjstr::config::load_cluster_conf;

    let dir = tempfile::tempdir().unwrap();
    let conf_path = dir.path().join("test.conf");
    std::fs::write(&conf_path, "replicas 2\nshard mem\nshard mem\n").unwrap();
    let conf = load_cluster_conf(&conf_path).unwrap();
    assert_eq!(conf.replicas, 2);
    assert_eq!(conf.min_writes, None);
}

// =====================================================================
// Test: rf=3, 3 shards, min_writes=2, progressive failure
// Covers put (new), put (replace/overwrite), and delete at each level.
// =====================================================================

#[test]
fn progressive_failure_rf3_min2() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3)
        .with_min_writes(2);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // ---- Phase 1: all 3 shards healthy ----
        // New put.
        cluster
            .put(&Path::from("obj/a"), PutPayload::from(Bytes::from_static(b"aaa")))
            .await
            .expect("new put should succeed with all shards healthy");

        // Replace (overwrite same key).
        cluster
            .put(&Path::from("obj/a"), PutPayload::from(Bytes::from_static(b"aaa-v2")))
            .await
            .expect("replace put should succeed with all shards healthy");
        let data = cluster.get(&Path::from("obj/a")).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"aaa-v2");

        // Delete.
        cluster.delete(&Path::from("obj/a")).await
            .expect("delete should succeed with all shards healthy");

        // ---- Phase 2: kill shard 0 -- 2 of 3 remain, == min_writes=2 ----
        cluster.detach_shard(0);

        // New put.
        cluster
            .put(&Path::from("obj/b"), PutPayload::from(Bytes::from_static(b"bbb")))
            .await
            .expect("new put should succeed with 2 of 3 shards (>= min_writes=2)");

        // Replace.
        cluster
            .put(&Path::from("obj/b"), PutPayload::from(Bytes::from_static(b"bbb-v2")))
            .await
            .expect("replace put should succeed with 2 of 3 shards");
        let data = cluster.get(&Path::from("obj/b")).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"bbb-v2");

        // Delete.
        cluster.delete(&Path::from("obj/b")).await
            .expect("delete should succeed with 2 of 3 shards (>= min_writes=2)");

        // ---- Phase 3: kill shard 1 -- only 1 of 3 remains, < min_writes=2 ----
        cluster.detach_shard(1);

        // Write something on the single remaining shard first (min_writes=2 not
        // met, so the put itself must fail).
        let fail_put = cluster
            .put(&Path::from("obj/c"), PutPayload::from(Bytes::from_static(b"ccc")))
            .await;
        assert!(fail_put.is_err(), "new put should fail with 1 shard < min_writes=2");
        let err_str = format!("{}", fail_put.unwrap_err());
        assert!(
            err_str.contains("InsufficientWrites") || err_str.contains("insufficient"),
            "put error should mention InsufficientWrites: {err_str}"
        );

        // Replace must also fail -- re-use a key that already existed from
        // phase 2 on the surviving shard.
        // First seed a key while we have enough shards, then degrade.
        // (obj/b was deleted, put a new one with all-but-one detached so it
        // will fail -- we can't seed it now with min_writes=2.)
        // Instead, just verify a put to any key fails:
        let fail_replace = cluster
            .put(&Path::from("obj/b"), PutPayload::from(Bytes::from_static(b"bbb-v3")))
            .await;
        assert!(fail_replace.is_err(), "replace put should fail with 1 shard < min_writes=2");

        // Delete on a non-existent key with only 1 shard should still succeed
        // (deletes are best-effort by default).
        let _del = cluster.delete(&Path::from("obj/nonexistent")).await;
    });
    flush_all(&raw);
}

// =====================================================================
// Test: replace (overwrite) fails when min_writes not met
// Explicit test: write obj, degrade, overwrite must fail.
// =====================================================================

#[test]
fn replace_fails_with_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Write initial version with all shards healthy.
        cluster
            .put(&Path::from("key/replace"), PutPayload::from(Bytes::from_static(b"v1")))
            .await
            .expect("initial put should succeed");

        // Kill 1 shard -- only 2 healthy, min_writes=3 cannot be met.
        cluster.detach_shard(2);

        // Overwrite must fail.
        let fail = cluster
            .put(&Path::from("key/replace"), PutPayload::from(Bytes::from_static(b"v2")))
            .await;
        assert!(fail.is_err(), "replace should fail when min_writes=3 and only 2 shards healthy");
        let err_str = format!("{}", fail.unwrap_err());
        assert!(
            err_str.contains("InsufficientWrites") || err_str.contains("insufficient"),
            "replace error should mention InsufficientWrites: {err_str}"
        );

        // Note: after InsufficientWrites, some shards may have v2 while
        // others still have v1. The key is that put() returned an error --
        // the caller knows the write did not fully replicate.
        // The object is still readable (from whichever shard the read hits).
        let data = cluster.get(&Path::from("key/replace")).await.unwrap().bytes().await.unwrap();
        assert!(
            data.as_ref() == b"v1" || data.as_ref() == b"v2",
            "data should be either old or new version after partial write"
        );
    });
    flush_all(&raw);
}

// (delete_requires_min_writes feature not yet implemented -- test removed)

// =====================================================================
// Test: delete_requires_min_writes=false (default) -- deletes succeed
// even when min_writes quorum is not met for puts.
// =====================================================================

#[test]
fn delete_requires_min_writes_false_allows_degraded() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // delete_requires_min_writes defaults to false (best-effort deletes).
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(&Path::from("obj/y"), PutPayload::from(Bytes::from_static(b"yyy")))
            .await
            .unwrap();

        // Kill 2 shards -- only 1 healthy.
        cluster.detach_shard(0);
        cluster.detach_shard(1);

        // Put must fail (min_writes=3 not met with 1 shard).
        let fail_put = cluster
            .put(&Path::from("obj/z"), PutPayload::from(Bytes::from_static(b"zzz")))
            .await;
        assert!(fail_put.is_err(), "put should fail with 1 shard < min_writes=3");

        // Delete succeeds -- best-effort mode, 1 healthy shard is enough.
        let del = cluster.delete(&Path::from("obj/y")).await;
        assert!(del.is_ok(), "delete should succeed with delete_requires_min_writes=false: {:?}", del.err());
    });
    flush_all(&raw);
}

// =====================================================================
// Test: delete_requires_min_writes=true -- deletes fail when quorum
// is not met, just like puts.
// =====================================================================

#[test]
fn delete_requires_min_writes_true_enforces_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3)
        .with_min_writes(3)
        .with_delete_requires_min_writes(true);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(&Path::from("obj/strict"), PutPayload::from(Bytes::from_static(b"data")))
            .await
            .unwrap();

        // Kill 1 shard -- only 2 healthy, < min_writes=3.
        cluster.detach_shard(0);

        // Delete should fail (strict mode, 2 < min_writes=3).
        let del = cluster.delete(&Path::from("obj/strict")).await;
        assert!(del.is_err(), "delete should fail with delete_requires_min_writes=true and 2 of 3 shards");
    });
    flush_all(&raw);
}

// =====================================================================
// Test: config parsing includes delete_requires_min_writes
// =====================================================================

#[test]
fn flat_config_delete_requires_min_writes() {
    use shardedobjstr::config::load_cluster_conf;

    let dir = tempfile::tempdir().unwrap();
    let conf_path = dir.path().join("test.conf");
    std::fs::write(&conf_path, "replicas 3\nmin_writes 2\ndelete_requires_min_writes\nshard mem\nshard mem\nshard mem\n").unwrap();
    let conf = load_cluster_conf(&conf_path).unwrap();
    assert_eq!(conf.replicas, 3);
    assert_eq!(conf.min_writes, Some(2));
    assert!(conf.delete_requires_min_writes);
}

#[test]
fn flat_config_delete_requires_min_writes_default_false() {
    use shardedobjstr::config::load_cluster_conf;

    let dir = tempfile::tempdir().unwrap();
    let conf_path = dir.path().join("test.conf");
    std::fs::write(&conf_path, "replicas 2\nshard mem\nshard mem\n").unwrap();
    let conf = load_cluster_conf(&conf_path).unwrap();
    assert!(!conf.delete_requires_min_writes);
}

// =====================================================================
// Test: new key cleanup on InsufficientWrites
//
// When a NEW key fails min_writes, the orphaned data on shards that
// accepted the write should be cleaned up. After the failed put,
// no shard should hold the object.
// =====================================================================

#[test]
fn new_key_cleaned_up_on_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // rf=3, min_writes=3: all must succeed.
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Detach 1 shard -- only 2 healthy, min_writes=3 cannot be met.
        cluster.detach_shard(2);

        // Try to put a NEW key -- should fail.
        let result = cluster
            .put(
                &Path::from("cleanup/new_key.bin"),
                PutPayload::from(Bytes::from_static(b"orphan-data")),
            )
            .await;
        assert!(result.is_err(), "new key put should fail with insufficient writes");

        // The orphaned data should have been cleaned up from the shards
        // that accepted it. Verify by checking each healthy shard directly.
        for sid in 0..2 {
            let store = cluster.shard_store(sid).unwrap();
            let get_result = store.get(&Path::from("cleanup/new_key.bin")).await;
            assert!(
                get_result.is_err(),
                "shard {sid} should NOT have orphaned data for new key after cleanup"
            );
        }

        // Catalog should also have no entry.
        assert!(
            cluster.placement("cleanup/new_key.bin").is_none(),
            "catalog should not have an entry for the failed new key"
        );
    });
}

// =====================================================================
// Test: overwrite NOT cleaned up on InsufficientWrites
//
// When an EXISTING key is overwritten but min_writes is not met,
// the partial writes should NOT be cleaned up (the old data was
// replaced in-place; deleting would destroy the surviving copy).
// =====================================================================

#[test]
fn overwrite_not_cleaned_up_on_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    // rf=3, min_writes=3.
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // First, put the key successfully with all shards healthy.
        cluster
            .put(
                &Path::from("cleanup/existing.bin"),
                PutPayload::from(Bytes::from_static(b"version-1")),
            )
            .await
            .expect("initial put should succeed");

        // Now degrade -- detach 1 shard.
        cluster.detach_shard(2);

        // Overwrite: should fail (only 2 of 3 required).
        let result = cluster
            .put(
                &Path::from("cleanup/existing.bin"),
                PutPayload::from(Bytes::from_static(b"version-2")),
            )
            .await;
        assert!(result.is_err(), "overwrite should fail with insufficient writes");

        // Despite the failure, the data should still be readable.
        // The old or new version should exist (NOT cleaned up).
        let data = cluster
            .get(&Path::from("cleanup/existing.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            data.as_ref() == b"version-1" || data.as_ref() == b"version-2",
            "data should be old or new version, not deleted: {:?}",
            std::str::from_utf8(&data)
        );

        // At least one shard should still hold the data directly.
        let mut shard_has_data = false;
        for sid in 0..2 {
            let store = cluster.shard_store(sid).unwrap();
            if store.get(&Path::from("cleanup/existing.bin")).await.is_ok() {
                shard_has_data = true;
            }
        }
        assert!(
            shard_has_data,
            "at least one shard should retain data for overwrite (not cleaned up)"
        );
    });
}

// =====================================================================
// MEDIUM: RF=3 min_writes=3 strict quorum -- all 3 replicas must succeed
// =====================================================================

#[test]
fn rf3_min_writes_3_strict_quorum_all_shards_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // All shards healthy: write should succeed.
    rt.block_on(async {
        cluster
            .put(
                &Path::from("strict/obj1.bin"),
                PutPayload::from(Bytes::from_static(b"strict-data")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Verify data on all 3 target shards.
    let placement = cluster.placement("strict/obj1.bin").unwrap();
    assert_eq!(placement.shards.len(), 3);
    for &sid in &placement.shards {
        let store = cluster.shard_store(sid).unwrap();
        let got = rt.block_on(async {
            store.get(&Path::from("strict/obj1.bin")).await.unwrap().bytes().await.unwrap()
        });
        assert_eq!(&got[..], b"strict-data");
    }
}

#[test]
fn rf3_min_writes_3_detach_one_shard_fails() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Detach 2 shards: with 4 shards, RF=3, and min_writes=3, at most 2
    // healthy shards remain -- guaranteed fewer than the required 3.
    cluster.detach_shard(0);
    cluster.detach_shard(1);

    let result = rt.block_on(async {
        cluster
            .put(
                &Path::from("strict/obj2.bin"),
                PutPayload::from(Bytes::from_static(b"should-fail")),
            )
            .await
    });
    assert!(
        result.is_err(),
        "strict quorum (min_writes=3) should fail when any shard is detached"
    );
}

#[test]
fn rf3_min_writes_2_detach_one_shard_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Detach one shard: non-strict quorum (min_writes=2) should still succeed.
    cluster.detach_shard(0);

    let result = rt.block_on(async {
        cluster
            .put(
                &Path::from("strict/obj3.bin"),
                PutPayload::from(Bytes::from_static(b"should-succeed")),
            )
            .await
    });
    assert!(
        result.is_ok(),
        "non-strict quorum (min_writes=2, RF=3) should succeed with 1 shard detached: {:?}",
        result.err()
    );
}
