//! Catalog stress and scaling tests for ShardedObjectStore.
//!
//! Covers: large catalog (1000+ entries), catalog persistence roundtrip
//! at scale, rebuild_catalog with many objects, and validate_shard_access
//! under load.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::catalog::CatalogPersistence;
use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

const SHARD_SIZE: u64 = 64 * 1024 * 1024;

// =====================================================================
// Large catalog: put 1000 objects, verify catalog
// =====================================================================

#[test]
fn large_catalog_1000_objects() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 1000;
    rt.block_on(async {
        for i in 0..n {
            let key = format!("scale/obj_{i:05}.bin");
            let data = vec![(i & 0xFF) as u8; 256];
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

    // All objects should have catalog entries with correct RF
    for i in 0..n {
        let key = format!("scale/obj_{i:05}.bin");
        let entry = cluster.placement(&key);
        assert!(
            entry.is_some(),
            "object {key} should have catalog entry"
        );
        assert_eq!(
            entry.unwrap().shards.len(),
            2,
            "object {key} should be on 2 shards"
        );
    }

    // List should return all objects
    let listed = rt.block_on(async {
        let items: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        items
    });
    assert_eq!(listed.len(), n, "list should return all {n} objects");
}

// =====================================================================
// Large catalog persistence: JSON roundtrip
// =====================================================================

#[test]
fn large_catalog_persistence_json_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();

    let catalog_path = dir.path().join("catalog.json");
    let cluster = build_cluster(&raws, 2)
        .with_persistence(CatalogPersistence::json(catalog_path.clone()));
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 500;
    rt.block_on(async {
        for i in 0..n {
            let key = format!("persist/obj_{i:04}.bin");
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![0xAA; 128])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Save catalog
    cluster.save_catalog().unwrap();
    assert!(catalog_path.exists(), "catalog file should exist");

    // Build a new cluster and load the catalog
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster2 = ShardedObjectStore::new(stores, 2)
        .with_persistence(CatalogPersistence::json(catalog_path));
    cluster2.load_catalog().unwrap();

    // All entries should be restored
    for i in 0..n {
        let key = format!("persist/obj_{i:04}.bin");
        let entry = cluster2.placement(&key);
        assert!(
            entry.is_some(),
            "object {key} should be in loaded catalog"
        );
    }

    // Data should be readable
    let spot_check = rt.block_on(async {
        cluster2
            .get(&Path::from("persist/obj_0042.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(spot_check.len(), 128);
}

// =====================================================================
// Large catalog: bincode persistence roundtrip
// =====================================================================

#[test]
fn large_catalog_persistence_bincode_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();

    let catalog_path = dir.path().join("catalog.bin");
    let cluster = build_cluster(&raws, 2)
        .with_persistence(CatalogPersistence::bincode(catalog_path.clone()));
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 500;
    rt.block_on(async {
        for i in 0..n {
            let key = format!("binpersist/obj_{i:04}.bin");
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![0xBB; 128])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    cluster.save_catalog().unwrap();

    // Load into fresh cluster
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster2 = ShardedObjectStore::new(stores, 2)
        .with_persistence(CatalogPersistence::bincode(catalog_path));
    cluster2.load_catalog().unwrap();

    for i in 0..n {
        let key = format!("binpersist/obj_{i:04}.bin");
        assert!(cluster2.placement(&key).is_some(), "missing {key}");
    }
}

// =====================================================================
// Rebuild catalog from shards at scale
// =====================================================================

#[test]
fn rebuild_catalog_at_scale() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 200;
    rt.block_on(async {
        for i in 0..n {
            let key = format!("rebuild/obj_{i:04}.bin");
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![0xCC; 256])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Clear catalog by building a new cluster without rebuild
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster2 = ShardedObjectStore::new(stores, 2);

    // No entries yet
    assert!(cluster2.placement("rebuild/obj_0000.bin").is_none());

    // Rebuild from shards
    let count = rt.block_on(cluster2.rebuild_catalog()).unwrap();
    assert!(count >= n, "rebuild should discover at least {n} entries, got {count}");

    // All objects should be in the catalog
    for i in 0..n {
        let key = format!("rebuild/obj_{i:04}.bin");
        assert!(
            cluster2.placement(&key).is_some(),
            "rebuild should rediscover {key}"
        );
    }
}

// =====================================================================
// Verify_all at scale -- spot check correctness
// =====================================================================

#[test]
fn verify_all_at_scale() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 100;
    rt.block_on(async {
        for i in 0..n {
            let key = format!("verify/obj_{i:04}.bin");
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

    let report = rt.block_on(cluster.verify_all(Some("verify/")));
    assert_eq!(
        report.objects_checked, n,
        "should verify all {n} objects"
    );
    assert_eq!(
        report.objects_ok, n,
        "all objects should pass verification"
    );
    assert_eq!(
        report.objects_mismatched, 0,
        "no mismatches expected"
    );
    assert_eq!(
        report.objects_with_errors, 0,
        "no errors expected"
    );
}

// =====================================================================
// Validate shard access
// =====================================================================

#[test]
fn validate_shard_access_with_offline_shard() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // All should be accessible initially
    let all = rt.block_on(cluster.validate_shard_access());
    assert_eq!(all.len(), 3);
    for (_, ok) in &all {
        assert!(ok);
    }

    // Detach shard 1
    cluster.detach_shard(1);

    let after = rt.block_on(cluster.validate_shard_access());
    // Shard 1 should be not accessible (offline)
    for (id, ok) in &after {
        if *id == 1 {
            assert!(!ok, "detached shard should not be accessible");
        }
    }
}
