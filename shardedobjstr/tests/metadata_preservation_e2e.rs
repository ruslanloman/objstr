//! Metadata preservation tests across ALL write/copy/repair paths.
//!
//! Each test writes an object with metadata via put_with_meta(), then
//! performs an operation (copy, repair-replication, sync, etc.) and asserts the
//! metadata survives on the destination.
//!
//! Tests are written to assert the CORRECT behavior.  If a bug exists,
//! the test FAILS -- proving the bug.  When the bug is fixed the test
//! passes WITHOUT changes.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::metadata::{
    get_metadata, head_with_meta, meta_sidecar_path, put_with_meta, RawRefRegistry, ShardKind,
};
use shardedobjstr::repair;
use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

// -- Helpers ----------------------------------------------------------

const SHARD_SIZE: u64 = 64 * 1024 * 1024;

/// Build a 3-shard RF=2 cluster with RawRefRegistry wired up.
fn setup_3shard_with_refs(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, Arc<RawRefRegistry>) {
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = Arc::new(RawRefRegistry::new(refs, kinds));
    cluster.set_raw_refs(Arc::clone(&registry));

    (cluster, raws, registry)
}

/// Put an object with metadata, flush, and return the metadata bytes.
fn put_with_test_metadata(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    registry: &RawRefRegistry,
    raws: &[Arc<RawObjectStore>],
    key: &str,
) -> Vec<u8> {
    let metadata = format!("test-meta-for-{}", key).into_bytes();
    let payload = Bytes::from(vec![0xABu8; 4096]);
    rt.block_on(async {
        put_with_meta(
            cluster,
            registry,
            &Path::from(key),
            payload,
            &metadata,
        )
        .await
        .unwrap();
    });
    flush_all(raws);
    metadata
}

/// Assert that get_metadata returns the expected bytes for a key.
fn assert_metadata_matches(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    registry: &RawRefRegistry,
    key: &str,
    expected: &[u8],
    context: &str,
) {
    rt.block_on(async {
        let got = get_metadata(cluster, registry, &Path::from(key))
            .await
            .expect(&format!("{}: get_metadata failed", context));
        assert_eq!(
            &got[..],
            expected,
            "{}: metadata mismatch",
            context
        );

        let (obj_meta, meta_len) =
            head_with_meta(cluster, registry, &Path::from(key))
                .await
                .expect(&format!("{}: head_with_meta failed", context));
        assert_eq!(
            meta_len as usize,
            expected.len(),
            "{}: meta_len mismatch (head reports {})",
            context,
            meta_len
        );
        assert_eq!(
            obj_meta.size, 4096,
            "{}: body size wrong (should be body-only)",
            context
        );
    });
}

// =====================================================================
// 1. copy() preserves metadata
// =====================================================================

#[test]
fn copy_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, "cp/src.bin");

    rt.block_on(async {
        cluster
            .copy(&Path::from("cp/src.bin"), &Path::from("cp/dst.bin"))
            .await
            .unwrap();
    });
    flush_all(&raws);

    assert_metadata_matches(
        &rt, &cluster, &registry,
        "cp/dst.bin", &meta,
        "copy() destination",
    );
    // Source should still have metadata too.
    assert_metadata_matches(
        &rt, &cluster, &registry,
        "cp/src.bin", &meta,
        "copy() source unchanged",
    );
}

// =====================================================================
// 2. copy_if_not_exists() preserves metadata
// =====================================================================

#[test]
fn copy_if_not_exists_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, "cpne/src.bin");

    rt.block_on(async {
        cluster
            .copy_if_not_exists(
                &Path::from("cpne/src.bin"),
                &Path::from("cpne/dst.bin"),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    assert_metadata_matches(
        &rt, &cluster, &registry,
        "cpne/dst.bin", &meta,
        "copy_if_not_exists() destination",
    );
}

// =====================================================================
// 3. rename_if_not_exists() preserves metadata
// =====================================================================

#[test]
fn rename_if_not_exists_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, "ren/src.bin");

    rt.block_on(async {
        cluster
            .rename_if_not_exists(
                &Path::from("ren/src.bin"),
                &Path::from("ren/dst.bin"),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    assert_metadata_matches(
        &rt, &cluster, &registry,
        "ren/dst.bin", &meta,
        "rename_if_not_exists() destination",
    );
    // Source should be gone.
    assert!(
        cluster.placement("ren/src.bin").is_none(),
        "source should be deleted after rename"
    );
}

// =====================================================================
// 4. replicate_object() preserves metadata
// =====================================================================

#[test]
fn replicate_object_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, "rep/obj.bin");

    let entry = cluster.placement("rep/obj.bin").unwrap();
    assert_eq!(entry.shards.len(), 2, "RF=2 should place on 2 shards");

    // Find a shard that does NOT have the object.
    let target = (0..3usize)
        .find(|s| !entry.shards.contains(s))
        .expect("should find a shard without the object");
    let source = entry.shards[0];

    rt.block_on(async {
        cluster
            .replicate_object("rep/obj.bin", source, target, Some(&registry))
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Verify metadata on the NEW replica by reading directly from
    // the target shard's raw store.
    let target_raw = &raws[target];
    let got = target_raw.get_metadata(&Path::from("rep/obj.bin")).unwrap();
    assert_eq!(
        &got[..],
        &meta[..],
        "replicate_object: metadata missing on target shard {}",
        target
    );
}

// =====================================================================
// 5. repair_replication_sweep() preserves metadata
// =====================================================================

#[test]
fn repair_replication_sweep_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write 5 objects with metadata.
    let mut metas = Vec::new();
    for i in 0..5 {
        let key = format!("rebal/obj_{i:03}.bin");
        let m = put_with_test_metadata(&rt, &cluster, &registry, &raws, &key);
        metas.push((key, m));
    }

    // Detach shard 1 to create under-replication.
    cluster.detach_shard(1);
    let under = cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "should have under-replicated objects after detach"
    );

    // Run repair-replication to repair.
    let result = rt.block_on(async {
        repair::repair_replication_sweep(&cluster, 100, Some(&registry), None).await
    });
    assert!(result.re_replicated > 0, "should have re-replicated objects");

    flush_all(&raws);

    // Verify metadata survived on ALL remaining replicas.
    for (key, expected_meta) in &metas {
        let entry = cluster.placement(key).unwrap();
        for &sid in &entry.shards {
            if cluster.shard_health(sid) != Some(shardedobjstr::ShardHealth::Healthy) {
                continue;
            }
            let raw = &raws[sid];
            let got = raw.get_metadata(&Path::from(key.as_str()));
            assert!(
                got.is_ok(),
                "repair_replication_sweep: shard {} missing metadata for {}",
                sid, key
            );
            assert_eq!(
                &got.unwrap()[..],
                &expected_meta[..],
                "repair_replication_sweep: metadata mismatch on shard {} for {}",
                sid, key
            );
        }
    }
}

// =====================================================================
// 6. mirror_sync() preserves metadata
// =====================================================================

#[test]
fn mirror_sync_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    // 2-shard mirror (RF=2 == shard_count).
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 2];
    let registry = Arc::new(RawRefRegistry::new(refs, kinds));
    cluster.set_raw_refs(Arc::clone(&registry));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects with metadata.
    let mut metas = Vec::new();
    for i in 0..3 {
        let key = format!("msync/obj_{i}.bin");
        let m = put_with_test_metadata(&rt, &cluster, &registry, &raws, &key);
        metas.push((key, m));
    }

    // Detach shard 1, put a new object (only on shard 0).
    cluster.detach_shard(1);
    let new_key = "msync/new_while_offline.bin";
    let new_meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, new_key);
    metas.push((new_key.to_string(), new_meta));

    // Build original_stores for mirror_sync.
    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // Run mirror_sync to recover shard 1.
    let report = rt
        .block_on(repair::mirror_sync(&cluster, &original_stores, 1, Some(&registry)))
        .unwrap();
    assert!(report.copied >= 1, "should have copied objects");

    flush_all(&raws);

    // Verify metadata on shard 1 for ALL objects.
    let shard1_raw = &raws[1];
    for (key, expected_meta) in &metas {
        let got = shard1_raw.get_metadata(&Path::from(key.as_str()));
        assert!(
            got.is_ok(),
            "mirror_sync: shard 1 missing metadata for {}",
            key
        );
        assert_eq!(
            &got.unwrap()[..],
            &expected_meta[..],
            "mirror_sync: metadata mismatch on shard 1 for {}",
            key
        );
    }
}

// =====================================================================
// 7. partitioned_sync() preserves metadata
// =====================================================================

#[test]
fn partitioned_sync_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write objects with metadata.
    let mut metas = Vec::new();
    for i in 0..5 {
        let key = format!("psync/obj_{i:03}.bin");
        let m = put_with_test_metadata(&rt, &cluster, &registry, &raws, &key);
        metas.push((key, m));
    }

    // Detach shard 1.
    cluster.detach_shard(1);
    let under = cluster.find_under_replicated();
    assert!(!under.is_empty(), "should have under-replicated objects");

    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // Run partitioned_sync targeting shard 1.
    let count = rt
        .block_on(repair::partitioned_sync(
            &cluster, &original_stores, 1, Some(&registry),
        ))
        .unwrap();
    assert!(count > 0, "should have replicated objects");

    flush_all(&raws);

    // Check metadata on shard 1 for objects that were synced to it.
    let shard1_raw = &raws[1];
    let mut checked = 0usize;
    for (key, expected_meta) in &metas {
        // Only check objects that partitioned_sync placed on shard 1.
        let got = shard1_raw.get_metadata(&Path::from(key.as_str()));
        if let Ok(data) = got {
            assert_eq!(
                &data[..],
                &expected_meta[..],
                "partitioned_sync: metadata mismatch on shard 1 for {}",
                key
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "should have verified at least one synced object");
}

// =====================================================================
// 8. sync_and_reattach() preserves metadata
// =====================================================================

#[test]
fn sync_and_reattach_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write objects with metadata.
    let mut metas = Vec::new();
    for i in 0..5 {
        let key = format!("reatt/obj_{i:03}.bin");
        let m = put_with_test_metadata(&rt, &cluster, &registry, &raws, &key);
        metas.push((key, m));
    }

    // Detach shard 2.
    cluster.detach_shard(2);

    // Write a new object while shard 2 is offline.
    let new_key = "reatt/new_offline.bin";
    let new_meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, new_key);
    metas.push((new_key.to_string(), new_meta));

    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // sync_and_reattach should restore shard 2 to Healthy.
    rt.block_on(async {
        repair::sync_and_reattach(
            &cluster, &original_stores, 2, Some(&registry),
        ).await;
    });
    flush_all(&raws);

    assert_eq!(
        cluster.shard_health(2),
        Some(shardedobjstr::ShardHealth::Healthy),
        "shard 2 should be Healthy after sync_and_reattach"
    );

    // Verify metadata accessible through the cluster for all objects.
    for (key, expected_meta) in &metas {
        assert_metadata_matches(
            &rt, &cluster, &registry,
            key, expected_meta,
            &format!("sync_and_reattach: {}", key),
        );
    }
}

// =====================================================================
// 9. drain_shard() preserves metadata
// =====================================================================

#[test]
fn drain_shard_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    // Build 3 shards with RF=1 so drain must move every object.
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 1);

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = Arc::new(RawRefRegistry::new(refs, kinds));
    cluster.set_raw_refs(Arc::clone(&registry));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write objects with metadata.  RF=1, so each lands on exactly 1 shard.
    let mut metas = Vec::new();
    for i in 0..10 {
        let key = format!("drain/obj_{i:03}.bin");
        let metadata = format!("meta-for-drain-{i}").into_bytes();
        let payload = Bytes::from(vec![0xCCu8; 4096]);
        rt.block_on(async {
            put_with_meta(
                &cluster,
                &registry,
                &Path::from(key.as_str()),
                payload,
                &metadata,
            )
            .await
            .unwrap();
        });
        metas.push((key, metadata));
    }
    flush_all(&raws);

    // Find objects on shard 0.
    let on_shard0: Vec<_> = metas
        .iter()
        .filter(|(key, _)| {
            cluster
                .placement(key)
                .map_or(false, |e| e.shards.contains(&0))
        })
        .cloned()
        .collect();

    if on_shard0.is_empty() {
        // With RF=1 and 10 objects across 3 shards, at least some should be on
        // shard 0.  If none are, skip the test (unlikely).
        eprintln!("SKIP: no objects on shard 0 (unlikely with 10 objects)");
        return;
    }

    // Build a survivor cluster without shard 0.
    let survivor_raws: Vec<Arc<RawObjectStore>> = raws[1..].to_vec();
    let survivor_cluster = build_cluster(&survivor_raws, 1);

    // Set up raw refs on survivor cluster matching its own shard IDs.
    let survivor_refs: Vec<Option<Arc<RawObjectStore>>> =
        survivor_raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let survivor_kinds = vec![ShardKind::Raw; survivor_raws.len()];
    let survivor_registry = Arc::new(RawRefRegistry::new(survivor_refs, survivor_kinds));
    survivor_cluster.set_raw_refs(Arc::clone(&survivor_registry));

    let victim_store = raws[0].clone() as Arc<dyn ObjectStore>;

    let report = rt.block_on(async {
        repair::drain_shard(
            &cluster,
            &survivor_cluster,
            0,
            &victim_store,
            Some(&registry),
            None,
        ).await
    });
    flush_all(&raws);

    assert!(
        report.moved > 0,
        "should have drained objects from shard 0"
    );

    // Verify metadata on the survivor cluster for drained objects.
    for (key, expected_meta) in &on_shard0 {
        assert_metadata_matches(
            &rt, &survivor_cluster, &survivor_registry,
            key, expected_meta,
            &format!("drain_shard: {}", key),
        );
    }
}

// =====================================================================
// 10. multipart complete preserves metadata on all replicas
//
// objstrd handles S3 multipart completion by assembling parts into a
// temp file, appending TLV metadata, then calling put_with_meta_from_file.
// This test verifies that path writes metadata to all replica shards
// and records the correct meta_len in the catalog.
//
// Previously, multipart complete used plain store.put() for replicas
// and recorded meta_len=0 in the catalog.  The fix in lib.rs makes
// complete() read metadata from the primary and propagate it via
// write_with_meta_to_shard().
// =====================================================================

#[test]
fn multipart_complete_should_preserve_metadata() {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Simulate what objstrd's CompleteMultipartUpload does:
    // assemble body + TLV metadata into a temp file, then call
    // put_with_meta_from_file() which writes to all target shards.
    let body = vec![0xCCu8; 4096];
    let meta_bytes = b"content-type:application/octet-stream";
    let meta_len = meta_bytes.len() as u16;

    let mut tmp = tempfile::tempfile().unwrap();
    tmp.write_all(&body).unwrap();
    tmp.write_all(meta_bytes).unwrap();
    tmp.flush().unwrap();
    tmp.seek(SeekFrom::Start(0)).unwrap();

    rt.block_on(async {
        shardedobjstr::metadata::put_with_meta_from_file(
            &cluster,
            &registry,
            &Path::from("mp/meta.bin"),
            &mut tmp,
            meta_len,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Catalog should record meta_len > 0.
    let entry = cluster.placement("mp/meta.bin").unwrap();
    assert_eq!(
        entry.meta_len, meta_len,
        "catalog meta_len should match the metadata written"
    );
    assert_eq!(entry.shards.len(), 2, "RF=2 should place on 2 shards");

    // Metadata should be readable via get_metadata.
    assert_metadata_matches(
        &rt,
        &cluster,
        &registry,
        "mp/meta.bin",
        meta_bytes,
        "multipart complete via put_with_meta_from_file",
    );

    // Body should be correct (not include metadata suffix).
    let got_body = rt.block_on(async {
        cluster
            .get(&Path::from("mp/meta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(
        &got_body[..],
        &body[..],
        "body should be the multipart data without metadata suffix"
    );

    // Both replicas should have the metadata.
    for &sid in &entry.shards {
        let raw = raws[sid].clone();
        let meta = raw.get_metadata(&Path::from("mp/meta.bin")).unwrap();
        assert_eq!(
            &meta[..],
            &meta_bytes[..],
            "shard {sid} should have metadata after multipart complete"
        );
    }
}

// =====================================================================
// 11. multipart complete should replicate body correctly to all shards
//     (baseline sanity -- this should pass even with the metadata bug)
// =====================================================================

#[test]
fn multipart_complete_replicates_body_to_all_shards() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, _registry) = setup_3shard_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let mut upload = cluster
            .put_multipart(&Path::from("mpbody/data.bin"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xEEu8; 8192])))
            .await
            .unwrap();
        upload.complete().await.unwrap();
    });
    flush_all(&raws);

    let entry = cluster.placement("mpbody/data.bin").unwrap();
    assert_eq!(entry.shards.len(), 2, "RF=2 should place on 2 shards");

    // Both replicas should have the correct body.
    rt.block_on(async {
        for &sid in &entry.shards {
            let store = cluster.shard_store(sid).unwrap();
            let data = store
                .get(&Path::from("mpbody/data.bin"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 8192, "shard {} body size wrong", sid);
        }
    });
}

// =====================================================================
// NOTE: mirror_sync with raw_refs=None on Raw-kind shards
// =====================================================================
//
// When mirror_sync is called with raw_refs=None on Raw-kind shards,
// metadata survives because it is stored inline in the raw-format blob.
// The plain `put()` copies the entire blob including metadata bytes.
//
// This test verifies that behavior.  The bug only affects Sidecar/S3Like
// shards where metadata is stored separately (as a companion file or
// S3 attribute), and a plain `put()` without raw_refs would lose it.
//
// Currently IGNORED to keep it alongside the other bug-trigger tests.

#[test]
fn mirror_sync_without_raw_refs_loses_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 2];
    let registry = Arc::new(RawRefRegistry::new(refs, kinds));
    cluster.set_raw_refs(Arc::clone(&registry));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let meta = put_with_test_metadata(&rt, &cluster, &registry, &raws, "mloss/obj.bin");
    let _ = meta;

    // Detach shard 1 so mirror_sync has something to copy.
    cluster.detach_shard(1);

    let original_stores: Vec<Option<Arc<dyn ObjectStore>>> =
        raws.iter().map(|r| Some(r.clone() as Arc<dyn ObjectStore>)).collect();

    // Sync with raw_refs = None -- this is the bug trigger.
    rt.block_on(repair::mirror_sync(&cluster, &original_stores, 1, None))
        .unwrap();
    flush_all(&raws);

    // BUG: metadata is lost because mirror_sync does plain put() when
    // raw_refs is None.  This assertion will FAIL until the bug is fixed.
    let got = raws[1].get_metadata(&Path::from("mloss/obj.bin"));
    assert!(
        got.is_ok() && !got.unwrap().is_empty(),
        "mirror_sync(raw_refs=None) should preserve metadata"
    );
}

// =====================================================================
// BUG: remove_replica leaves orphaned sidecar files on Sidecar shards
// =====================================================================
//
// When `remove_replica()` deletes an object from a Sidecar-kind shard,
// it only deletes the data file (`store.delete(path)`) but NOT the
// companion `__meta__/<key>` sidecar file.  This leaves orphaned
// metadata files on disk.
//
// Currently IGNORED because remove_replica does not clean up sidecars.

#[test]
fn remove_replica_leaves_orphaned_sidecar_on_sidecar_shard() {
    use object_store::memory::InMemory;

    let dir = tempfile::tempdir().unwrap();
    let raw0 = format_shard(&dir.path().join("s0.raw"), SHARD_SIZE);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _, mem.clone()];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![Some(raw0.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(refs, kinds);
    let reg_arc = Arc::new(RawRefRegistry::new(
        vec![Some(raw0.clone()), None],
        vec![ShardKind::Raw, ShardKind::Sidecar],
    ));
    cluster.set_raw_refs(reg_arc);

    let registry_ref = &registry;

    // Write object with metadata (lands on both shards).
    rt.block_on(async {
        put_with_meta(
            &cluster, registry_ref,
            &Path::from("sidecar/trim.bin"),
            Bytes::from(vec![0xDD; 512]),
            b"sidecar-meta-test",
        ).await.unwrap();
    });
    flush_all(&[raw0.clone()]);

    // Verify sidecar file exists on InMemory shard (shard 1).
    let sidecar = meta_sidecar_path(&Path::from("sidecar/trim.bin"));
    let has_sidecar = rt.block_on(async { mem.head(&sidecar).await.is_ok() });
    assert!(has_sidecar, "sidecar file should exist before removal");

    // Find which shard is the InMemory one and remove that replica.
    let entry = cluster.placement("sidecar/trim.bin").unwrap();
    let mem_shard = entry.shards.iter().find(|&&sid| sid == 1);
    if let Some(&sid) = mem_shard {
        rt.block_on(cluster.remove_replica("sidecar/trim.bin", sid)).unwrap();
    } else {
        // Object wasn't placed on shard 1 -- skip test.
        return;
    }

    // BUG: The sidecar file is still present after remove_replica.
    // This assertion will FAIL until remove_replica also deletes sidecars.
    let sidecar_after = rt.block_on(async { mem.head(&sidecar).await.is_ok() });
    assert!(
        !sidecar_after,
        "sidecar file should be deleted when replica is removed (BUG: orphaned sidecar)"
    );
}
