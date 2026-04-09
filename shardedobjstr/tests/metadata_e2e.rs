//! End-to-end tests for metadata-aware operations in ShardedObjectStore.
//!
//! Covers: put_with_meta, head_with_meta, get_metadata, list_with_meta,
//! set_meta_len, delete_sidecar, and RawRefRegistry construction.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::{path::Path, PutPayload, ObjectStore};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::metadata::{
    delete_sidecar, get_metadata, head_with_meta, list_with_meta,
    meta_sidecar_path, put_with_meta, set_meta_len, RawRefRegistry, ShardKind,
};
use shardedobjstr::tlv::{decode_metadata, encode_metadata};
use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

// -- Helpers ---------------------------------------------------------

fn setup_cluster_with_refs(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, RawRefRegistry) {
    let shard_size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("shard0.raw"), shard_size);
    let s1 = format_shard(&dir.path().join("shard1.raw"), shard_size);
    let s2 = format_shard(&dir.path().join("shard2.raw"), shard_size);
    let raws = vec![s0, s1, s2];
    let cluster = build_cluster(&raws, 2);

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = RawRefRegistry::new(refs, kinds);

    (cluster, raws, registry)
}

// -- Tests -----------------------------------------------------------

#[test]
fn put_with_meta_and_get_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xABu8; 4096]);
    let metadata = b"content-type:application/json";

    rt.block_on(async {
        put_with_meta(&cluster, &registry, &Path::from("meta/doc.bin"), payload, metadata)
            .await
            .unwrap();
    });
    flush_all(&raws);

    rt.block_on(async {
        // get_metadata should return the exact metadata bytes
        let got = get_metadata(&cluster, &registry, &Path::from("meta/doc.bin"))
            .await
            .unwrap();
        assert_eq!(&got[..], metadata, "metadata roundtrip mismatch");

        // head_with_meta should report correct meta_len
        let (obj_meta, meta_len) =
            head_with_meta(&cluster, &registry, &Path::from("meta/doc.bin"))
                .await
                .unwrap();
        assert_eq!(meta_len as usize, metadata.len());
        // size is body-only (metadata suffix excluded)
        assert_eq!(obj_meta.size, 4096);

        // catalog should have a placement entry
        let placement = cluster.placement("meta/doc.bin");
        assert!(placement.is_some(), "catalog should contain the key");
    });
}

#[test]
fn head_with_meta_returns_correct_meta_len() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("meta/zero.bin", vec![]),
        ("meta/small.bin", vec![0x01; 10]),
        ("meta/large.bin", vec![0x02; 1000]),
    ];

    rt.block_on(async {
        for (key, meta) in &cases {
            let payload = Bytes::from(vec![0xFFu8; 512]);
            put_with_meta(
                &cluster,
                &registry,
                &Path::from(*key),
                payload,
                meta,
            )
            .await
            .unwrap();
        }
    });
    flush_all(&raws);

    rt.block_on(async {
        for (key, meta) in &cases {
            let (obj_meta, meta_len) =
                head_with_meta(&cluster, &registry, &Path::from(*key))
                    .await
                    .unwrap();
            assert_eq!(
                meta_len as usize,
                meta.len(),
                "meta_len mismatch for {}",
                key
            );
            assert_eq!(
                obj_meta.size,
                512,
                "size should be body-only for {}",
                key
            );
        }
    });
}

#[test]
fn list_with_meta_returns_per_object_meta_len() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put 5 objects with varying metadata sizes
    let objects: Vec<(&str, usize)> = vec![
        ("listing/a.bin", 0),
        ("listing/b.bin", 5),
        ("listing/c.bin", 50),
        ("listing/d.bin", 200),
        ("other/e.bin", 100),
    ];

    rt.block_on(async {
        for (key, meta_size) in &objects {
            let payload = Bytes::from(vec![0xAAu8; 256]);
            let meta = vec![0xBBu8; *meta_size];
            put_with_meta(
                &cluster,
                &registry,
                &Path::from(*key),
                payload,
                &meta,
            )
            .await
            .unwrap();
        }
    });
    flush_all(&raws);

    rt.block_on(async {
        // List all -- should return all 5
        let all = list_with_meta(&cluster, &registry, None).await;
        assert_eq!(all.len(), 5, "expected 5 objects in total");

        // List with prefix -- should only return "listing/" objects
        let filtered =
            list_with_meta(&cluster, &registry, Some(&Path::from("listing"))).await;
        assert_eq!(filtered.len(), 4, "expected 4 objects under listing/");

        // Verify each object's meta_len
        for (obj_meta, meta_len) in &all {
            let key = obj_meta.location.to_string();
            let expected = objects
                .iter()
                .find(|(k, _)| *k == key.as_str())
                .map(|(_, m)| *m)
                .unwrap_or_else(|| panic!("unexpected key: {}", key));
            assert_eq!(
                *meta_len as usize, expected,
                "meta_len mismatch for {}",
                key
            );
        }
    });
}

#[test]
fn set_meta_len_updates_index() {
    use shardedobjstr::ReadPreference;

    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    // Use ordered reads so set_meta_len and head_with_meta hit the same shard
    cluster.set_read_preference(ReadPreference::Ordered);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put object with no metadata
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("setmeta/obj.bin"),
            Bytes::from(vec![0xCCu8; 1024]),
            &[],
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify initial meta_len is 0
    rt.block_on(async {
        let (_, ml) = head_with_meta(&cluster, &registry, &Path::from("setmeta/obj.bin"))
            .await
            .unwrap();
        assert_eq!(ml, 0, "initial meta_len should be 0");

        // Update meta_len to 42
        set_meta_len(&cluster, &registry, &Path::from("setmeta/obj.bin"), 42)
            .await
            .unwrap();
    });
    flush_all(&raws);

    // After set_meta_len, head should report 42
    rt.block_on(async {
        let (_, ml) = head_with_meta(&cluster, &registry, &Path::from("setmeta/obj.bin"))
            .await
            .unwrap();
        assert_eq!(ml, 42, "meta_len should be updated to 42");
    });
}

#[test]
fn put_with_meta_replicates_to_all_shards() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xDDu8; 2048]);
    let metadata = b"test-metadata-for-replication";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("replicated/obj.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify placement: should be on 2 shards (RF=2)
    let placement = cluster.placement("replicated/obj.bin").unwrap();
    assert_eq!(placement.shards.len(), 2, "should be on 2 shards");

    // Verify both replicas are directly readable via per-shard stores
    rt.block_on(async {
        for &shard_id in &placement.shards {
            let store = cluster.shard_store(shard_id).unwrap();
            let result = store.get(&Path::from("replicated/obj.bin")).await;
            assert!(
                result.is_ok(),
                "shard {} should hold the replica",
                shard_id
            );
        }
    });

    // Detach one shard, verify get_metadata still works from survivor
    let detached = placement.shards[0];
    cluster.detach_shard(detached);

    rt.block_on(async {
        let got = get_metadata(&cluster, &registry, &Path::from("replicated/obj.bin"))
            .await
            .unwrap();
        assert_eq!(&got[..], metadata, "surviving replica should serve metadata");
    });
}

#[test]
fn put_with_meta_rejects_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("shard0.raw"), shard_size);
    let s1 = format_shard(&dir.path().join("shard1.raw"), shard_size);
    let raws = vec![s0, s1];
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();

    let cluster = ShardedObjectStore::new(stores, 2).with_read_only(true);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 2];
    let registry = RawRefRegistry::new(refs, kinds);

    rt.block_on(async {
        let result = put_with_meta(
            &cluster,
            &registry,
            &Path::from("should/fail.bin"),
            Bytes::from(vec![0u8; 100]),
            b"metadata",
        )
        .await;
        assert!(result.is_err(), "put_with_meta should fail in read-only mode");
    });
}

#[test]
fn delete_sidecar_noop_on_raw_shards() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put an object first
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sidecar/obj.bin"),
            Bytes::from(vec![0xEEu8; 512]),
            b"meta",
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // delete_sidecar on raw shards should be a no-op (no crash)
    rt.block_on(async {
        delete_sidecar(&cluster, &registry, &Path::from("sidecar/obj.bin")).await;
    });

    // Object should still be readable
    rt.block_on(async {
        let got = get_metadata(&cluster, &registry, &Path::from("sidecar/obj.bin"))
            .await
            .unwrap();
        assert_eq!(&got[..], b"meta");
    });
}

// =====================================================================
// Tests: put_with_meta enforces min_writes
// =====================================================================

/// Helper: build a 3-shard cluster with RF and min_writes, plus raw refs.
fn setup_cluster_with_refs_min_writes(
    dir: &tempfile::TempDir,
    rf: usize,
    min_writes: usize,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, RawRefRegistry) {
    setup_cluster_with_refs_full(dir, rf, min_writes, false)
}

/// Helper: build a 3-shard cluster with RF, min_writes, and delete_requires_min_writes.
fn setup_cluster_with_refs_full(
    dir: &tempfile::TempDir,
    rf: usize,
    min_writes: usize,
    delete_requires_min_writes: bool,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, RawRefRegistry) {
    let shard_size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("shard0.raw"), shard_size);
    let s1 = format_shard(&dir.path().join("shard1.raw"), shard_size);
    let s2 = format_shard(&dir.path().join("shard2.raw"), shard_size);
    let raws = vec![s0, s1, s2];
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, rf)
        .with_min_writes(min_writes)
        .with_delete_requires_min_writes(delete_requires_min_writes);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 3];
    let registry = RawRefRegistry::new(refs, kinds);

    (cluster, raws, registry)
}

#[test]
fn put_with_meta_succeeds_with_enough_replicas() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=2, 3 shards -- all healthy, should succeed
    let (cluster, raws, registry) = setup_cluster_with_refs_min_writes(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let result = put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/ok.bin"),
            Bytes::from(vec![0xAAu8; 1024]),
            b"content-type:text/plain",
        )
        .await;
        assert!(result.is_ok(), "put_with_meta should succeed: {:?}", result.err());
    });
    flush_all(&raws);

    // Verify data is readable
    rt.block_on(async {
        let meta = get_metadata(&cluster, &registry, &Path::from("meta/ok.bin"))
            .await
            .unwrap();
        assert_eq!(&meta[..], b"content-type:text/plain");
    });
}

#[test]
fn put_with_meta_succeeds_degraded_above_min_writes() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=2, detach 1 shard -- 2 writes land, equals min_writes
    let (cluster, raws, registry) = setup_cluster_with_refs_min_writes(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    cluster.detach_shard(0);

    rt.block_on(async {
        let result = put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/degraded-ok.bin"),
            Bytes::from(vec![0xBBu8; 1024]),
            b"content-type:application/json",
        )
        .await;
        assert!(result.is_ok(), "put_with_meta should succeed with 2 of 3: {:?}", result.err());
    });
    flush_all(&raws);

    // Verify metadata survived
    rt.block_on(async {
        let meta = get_metadata(&cluster, &registry, &Path::from("meta/degraded-ok.bin"))
            .await
            .unwrap();
        assert_eq!(&meta[..], b"content-type:application/json");
    });
}

#[test]
fn put_with_meta_fails_with_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=3, detach 1 shard -- only 2 writes land, < 3
    let (cluster, _raws, registry) = setup_cluster_with_refs_min_writes(&dir, 3, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    cluster.detach_shard(0);

    rt.block_on(async {
        let result = put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/fail.bin"),
            Bytes::from(vec![0xCCu8; 1024]),
            b"should-fail",
        )
        .await;
        assert!(result.is_err(), "put_with_meta should fail with only 2 of 3 required");
        let err_str = format!("{}", result.unwrap_err());
        assert!(
            err_str.contains("insufficient") || err_str.contains("Insufficient"),
            "error should mention insufficient writes: {err_str}"
        );
    });
}

#[test]
fn put_with_meta_fails_all_detached() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=2, detach all 3 shards -- total failure
    let (cluster, _raws, registry) = setup_cluster_with_refs_min_writes(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    cluster.detach_shard(0);
    cluster.detach_shard(1);
    cluster.detach_shard(2);

    rt.block_on(async {
        let result = put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/total-fail.bin"),
            Bytes::from(vec![0xDDu8; 512]),
            b"dead",
        )
        .await;
        assert!(result.is_err(), "put_with_meta should fail with all shards detached");
    });
}

// =====================================================================
// Tests: delete_requires_min_writes with metadata objects
// =====================================================================

#[test]
fn meta_delete_requires_min_writes_true_enforces() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=3, delete_requires_min_writes=true
    let (cluster, raws, registry) = setup_cluster_with_refs_full(&dir, 3, 3, true);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write a metadata object while all shards healthy.
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/del-strict.bin"),
            Bytes::from(vec![0xAAu8; 1024]),
            b"content-type:text/plain",
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Detach 1 shard -- delete should fail (only 2 of 3 required).
    cluster.detach_shard(0);

    rt.block_on(async {
        let del = cluster.delete(&Path::from("meta/del-strict.bin")).await;
        assert!(
            del.is_err(),
            "delete should fail with delete_requires_min_writes=true and 2 of 3 shards"
        );
    });

    // Object should still be readable on surviving shards.
    rt.block_on(async {
        let meta = get_metadata(&cluster, &registry, &Path::from("meta/del-strict.bin"))
            .await
            .unwrap();
        assert_eq!(&meta[..], b"content-type:text/plain");
    });
}

#[test]
fn meta_delete_requires_min_writes_false_allows_degraded() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=3, delete_requires_min_writes=false (default).
    // Puts require min_writes=3 but deletes are best-effort.
    let (cluster, raws, registry) = setup_cluster_with_refs_full(&dir, 3, 3, false);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write while healthy.
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/del-relaxed.bin"),
            Bytes::from(vec![0xBBu8; 1024]),
            b"content-type:application/json",
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Detach 2 shards -- puts should fail (1 < min_writes=3) but
    // deletes should succeed (best-effort, 1 shard is enough).
    cluster.detach_shard(0);
    cluster.detach_shard(1);

    rt.block_on(async {
        // put_with_meta should fail.
        let put = put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/should-fail.bin"),
            Bytes::from(vec![0xCCu8; 512]),
            b"nope",
        )
        .await;
        assert!(put.is_err(), "put_with_meta should fail with 1 of 3 required");

        // delete succeeds -- best-effort mode, 1 healthy shard is enough.
        let del = cluster.delete(&Path::from("meta/del-relaxed.bin")).await;
        assert!(
            del.is_ok(),
            "delete should succeed with delete_requires_min_writes=false: {:?}",
            del.err()
        );
    });
}

#[test]
fn meta_lifecycle_put_delete_with_strict_quorum() {
    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=2, delete_requires_min_writes=true
    // Full lifecycle: put_with_meta -> read metadata -> delete -> verify gone
    let (cluster, raws, registry) = setup_cluster_with_refs_full(&dir, 3, 2, true);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Phase 1: write with metadata, all healthy.
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/lifecycle.bin"),
            Bytes::from(vec![0xEEu8; 2048]),
            b"x-custom:lifecycle-test",
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify metadata is readable.
    rt.block_on(async {
        let meta = get_metadata(&cluster, &registry, &Path::from("meta/lifecycle.bin"))
            .await
            .unwrap();
        assert_eq!(&meta[..], b"x-custom:lifecycle-test");
    });

    // Phase 2: detach 1 shard -- still have 2 >= min_writes=2.
    // Both put_with_meta and delete should succeed.
    cluster.detach_shard(0);

    rt.block_on(async {
        // Overwrite with new metadata.
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/lifecycle.bin"),
            Bytes::from(vec![0xFFu8; 2048]),
            b"x-custom:updated",
        )
        .await
        .expect("put_with_meta should succeed with 2 of 3 shards");

        // Delete should also succeed.
        cluster
            .delete(&Path::from("meta/lifecycle.bin"))
            .await
            .expect("delete should succeed with 2 healthy shards");
    });
    flush_all(&raws);

    // Phase 3: detach another -- only 1 shard left, < min_writes=2.
    // Both put_with_meta and delete should fail.
    cluster.detach_shard(1);

    rt.block_on(async {
        let put = put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/lifecycle2.bin"),
            Bytes::from(vec![0x11u8; 512]),
            b"fail",
        )
        .await;
        assert!(put.is_err(), "put_with_meta should fail with 1 shard < min_writes=2");

        let del = cluster.delete(&Path::from("meta/lifecycle2.bin")).await;
        assert!(
            del.is_err(),
            "delete should fail with delete_requires_min_writes=true and 1 shard"
        );
    });
}

// =====================================================================
// Metadata on Sidecar shards (LocalFileSystem / InMemory)
// =====================================================================

fn setup_sidecar_cluster(
    _dir: &tempfile::TempDir,
) -> (ShardedObjectStore, RawRefRegistry) {
    // Use InMemory stores as Sidecar shards -- metadata stored
    // alongside the object as `{key}.__meta__` files.
    let mem0 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let mem1 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let mem2 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

    let stores = vec![mem0, mem1, mem2];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // No raw refs for sidecar shards -- all None
    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![None, None, None];
    let kinds = vec![ShardKind::Sidecar; 3];
    let registry = RawRefRegistry::new(refs, kinds);

    (cluster, registry)
}

#[test]
fn sidecar_put_with_meta_and_get_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xABu8; 4096]);
    let metadata = b"content-type:application/json";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sidecar/doc.bin"),
            payload.clone(),
            metadata,
        )
        .await
        .unwrap();

        // get_metadata should return the exact metadata bytes
        let got = get_metadata(&cluster, &registry, &Path::from("sidecar/doc.bin"))
            .await
            .unwrap();
        assert_eq!(&got[..], metadata, "sidecar metadata roundtrip mismatch");

        // head_with_meta reads meta_len from the catalog for non-raw shards.
        let (_obj_meta, meta_len) =
            head_with_meta(&cluster, &registry, &Path::from("sidecar/doc.bin"))
                .await
                .unwrap();
        assert_eq!(meta_len, metadata.len() as u16, "sidecar should report catalog meta_len in head");

        // Data should be intact (body only, no metadata suffix)
        let data = cluster
            .get(&Path::from("sidecar/doc.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    });
}

#[test]
fn sidecar_list_with_meta_hides_meta_files() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Put two objects with metadata
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sidecar/a.bin"),
            Bytes::from(vec![0x01; 128]),
            b"meta-a",
        )
        .await
        .unwrap();
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sidecar/b.bin"),
            Bytes::from(vec![0x02; 128]),
            b"meta-b",
        )
        .await
        .unwrap();

        // list_with_meta should return only the real objects, not __meta__ files
        let listed = list_with_meta(
            &cluster,
            &registry,
            Some(&Path::from("sidecar")),
        )
        .await;

        let keys: Vec<String> = listed.iter().map(|(m, _)| m.location.to_string()).collect();
        assert_eq!(keys.len(), 2, "should list 2 objects (not sidecar files)");
        assert!(
            !keys.iter().any(|k| k.contains("__meta__")),
            "list should not include __meta__ sidecar files"
        );
    });
}

#[test]
fn sidecar_delete_sidecar_cleans_meta_file() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sidecar/cleanup.bin"),
            Bytes::from(vec![0xCC; 64]),
            b"some-meta",
        )
        .await
        .unwrap();

        // Verify metadata exists
        let meta = get_metadata(&cluster, &registry, &Path::from("sidecar/cleanup.bin")).await;
        assert!(meta.is_ok(), "metadata should exist before delete_sidecar");

        // Delete sidecars
        delete_sidecar(&cluster, &registry, &Path::from("sidecar/cleanup.bin")).await;

        // Sidecar metadata should be gone (object itself remains)
        let meta_after =
            get_metadata(&cluster, &registry, &Path::from("sidecar/cleanup.bin")).await;
        assert!(
            meta_after.is_err(),
            "metadata should be gone after delete_sidecar"
        );

        // The data object should still exist
        let data = cluster.get(&Path::from("sidecar/cleanup.bin")).await;
        assert!(data.is_ok(), "data object should still exist");
    });
}

#[test]
fn sidecar_trait_delete_cleans_meta_file() {
    // delete() via the ObjectStore trait routes through delete_raw(), which must
    // also remove the companion {path}.__meta__ sidecar file on every shard.
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sidecar/orphan.bin"),
            Bytes::from(vec![0xCCu8; 256]),
            b"sidecar-meta",
        )
        .await
        .unwrap();

        let meta = get_metadata(&cluster, &registry, &Path::from("sidecar/orphan.bin")).await;
        assert!(meta.is_ok(), "metadata should exist before delete");

        cluster.delete(&Path::from("sidecar/orphan.bin")).await.unwrap();

        let data = cluster.get(&Path::from("sidecar/orphan.bin")).await;
        assert!(data.is_err(), "data should be deleted");

        // No shard should still hold the sidecar __meta__ file.
        let sidecar_path = meta_sidecar_path(&Path::from("sidecar/orphan.bin"));
        let mut sidecar_found = false;
        for sid in 0..3 {
            if let Some(store) = cluster.shard_store(sid) {
                if store.get(&sidecar_path).await.is_ok() {
                    sidecar_found = true;
                    break;
                }
            }
        }
        assert!(!sidecar_found, "sidecar __meta__ file should be cleaned up after delete");
    });
}

// =====================================================================
// Mixed raw + sidecar cluster
// =====================================================================

#[test]
fn mixed_raw_and_sidecar_shards_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;

    // Shard 0: raw, Shard 1: sidecar (InMemory), Shard 2: raw
    let raw0 = format_shard(&dir.path().join("shard0.raw"), shard_size);
    let mem1 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let raw2 = format_shard(&dir.path().join("shard2.raw"), shard_size);

    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raw0.clone() as _,
        mem1,
        raw2.clone() as _,
    ];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![
        Some(raw0.clone()),
        None, // sidecar
        Some(raw2.clone()),
    ];
    let kinds = vec![ShardKind::Raw, ShardKind::Sidecar, ShardKind::Raw];
    let registry = RawRefRegistry::new(refs, kinds);

    let payload = Bytes::from(vec![0xABu8; 2048]);
    let metadata = b"mixed-cluster-meta";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("mixed/test.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();

        // Metadata should be readable regardless of which shards were chosen
        let got = get_metadata(&cluster, &registry, &Path::from("mixed/test.bin"))
            .await
            .unwrap();
        assert_eq!(&got[..], metadata, "metadata should roundtrip on mixed cluster");

        // Data should be correct
        let data = cluster
            .get(&Path::from("mixed/test.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 2048);
    });
}

// =====================================================================
// get_metadata fallback across shards
// =====================================================================

#[test]
fn get_metadata_skips_detached_shard() {
    // get_metadata should fall back to the next shard when the first shard
    // in read order has no raw ref (detached or None).  The fixed code
    // warns and continues on non-NotFound errors rather than aborting.
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xFFu8; 4096]);
    let metadata = b"important-metadata";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("meta/abort.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify metadata is readable before detaching.
    rt.block_on(async {
        let got = get_metadata(&cluster, &registry, &Path::from("meta/abort.bin"))
            .await
            .unwrap();
        assert_eq!(&got[..], metadata);
    });

    let placement = cluster.placement("meta/abort.bin").unwrap();
    assert_eq!(placement.shards.len(), 2);

    // Detach the first shard in read order.
    let read_order = cluster.read_shard_order(&Path::from("meta/abort.bin"));
    let first_in_order = read_order[0];
    cluster.detach_shard(first_in_order);

    // Build a registry where the detached shard has no raw ref.
    let refs: Vec<Option<Arc<RawObjectStore>>> = (0..3)
        .map(|i| {
            if i == first_in_order {
                None
            } else {
                Some(Arc::clone(&raws[i]))
            }
        })
        .collect();
    let kinds = vec![ShardKind::Raw; 3];
    let new_registry = RawRefRegistry::new(refs, kinds);

    // get_metadata should succeed by falling back to the second shard.
    rt.block_on(async {
        let result = get_metadata(&cluster, &new_registry, &Path::from("meta/abort.bin"))
            .await;
        assert!(
            result.is_ok(),
            "get_metadata should succeed from second shard: {:?}",
            result.err()
        );
        assert_eq!(&result.unwrap()[..], metadata);
    });
}

// =====================================================================
// replicate_object loses metadata on Sidecar shards (known limitation)
// =====================================================================

#[test]
fn replicate_loses_metadata_on_sidecar_shard() {
    let dir = tempfile::tempdir().unwrap();
    let raw_path = dir.path().join("raw.raw");
    let fs_dir = dir.path().join("fs_shard");
    std::fs::create_dir_all(&fs_dir).unwrap();

    let raw = format_shard(&raw_path, 64 * 1024 * 1024);
    let fs_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&fs_dir).unwrap(),
    );

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw.clone() as _, fs_store];
    let cluster = ShardedObjectStore::new(stores, 1);

    let registry = RawRefRegistry::new(
        vec![Some(raw.clone()), None],
        vec![ShardKind::Raw, ShardKind::Sidecar],
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let meta = b"test-metadata-payload";
        put_with_meta(&cluster, &registry, &Path::from("obj/a"), Bytes::from(vec![1u8; 4096]), meta)
            .await
            .unwrap();
        raw.flush_index().unwrap();

        let got = get_metadata(&cluster, &registry, &Path::from("obj/a")).await.unwrap();
        assert!(!got.is_empty(), "metadata should exist on raw shard");

        let placement = cluster.placement("obj/a");
        assert!(placement.is_some());
        let entry = placement.unwrap();
        let from_shard = entry.shards[0];
        let to_shard = if from_shard == 0 { 1 } else { 0 };

        // replicate_object does a plain get+put, losing metadata.
        let result = cluster.replicate_object("obj/a", from_shard, to_shard, Some(&registry)).await;
        assert!(result.is_ok(), "replication should succeed for data: {:?}", result);

        if to_shard == 1 {
            let sidecar = meta_sidecar_path(&Path::from("obj/a"));
            let fs_check = object_store::local::LocalFileSystem::new_with_prefix(&fs_dir).unwrap();
            let sidecar_result = fs_check.get(&sidecar).await;
            assert!(
                sidecar_result.is_err(),
                "sidecar metadata should NOT be created by replicate_object (known limitation)"
            );
        }
    });
}

// =====================================================================
// get_metadata when all shards are offline returns error
// =====================================================================

#[test]
fn get_metadata_all_shards_offline_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let meta = b"some-metadata";
        put_with_meta(&cluster, &registry, &Path::from("meta/obj"), Bytes::from(vec![0xBB; 4096]), meta)
            .await
            .unwrap();
        flush_all(&raws);

        let got = get_metadata(&cluster, &registry, &Path::from("meta/obj")).await;
        assert!(got.is_ok(), "metadata should be readable: {:?}", got);

        // Take all shards offline.
        for i in 0..raws.len() {
            cluster.detach_shard(i);
        }

        let result = get_metadata(&cluster, &registry, &Path::from("meta/obj")).await;
        assert!(result.is_err(), "should fail when all shards offline");
    });
}

// =====================================================================
// get_metadata on object with empty metadata returns empty bytes
// =====================================================================

#[test]
fn get_metadata_object_without_metadata_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        put_with_meta(&cluster, &registry, &Path::from("no-meta/obj"), Bytes::from(vec![0xCC; 4096]), b"")
            .await
            .unwrap();
        flush_all(&raws);

        let result = get_metadata(&cluster, &registry, &Path::from("no-meta/obj")).await;
        match result {
            Ok(data) => assert!(data.is_empty(), "expected empty metadata, got {} bytes", data.len()),
            Err(_) => {
                // Also acceptable -- NotFound could mean "no metadata"
                // or "unreachable" (known conflation).
            }
        }
    });
}

// =====================================================================
// TLV decode_metadata: truncated input returns partial results
// =====================================================================

#[test]
fn tlv_decode_truncated_returns_partial_map() {
    use std::collections::HashMap;
    use shardedobjstr::tlv::{decode_metadata, encode_metadata};

    let mut meta = HashMap::new();
    meta.insert("x-amz-meta-alpha".to_string(), "value-a".to_string());
    meta.insert("x-amz-meta-beta".to_string(), "value-b".to_string());
    meta.insert("x-amz-meta-gamma".to_string(), "value-c".to_string());

    let encoded = encode_metadata(&meta).unwrap();
    assert!(!encoded.is_empty());

    let full = decode_metadata(&encoded);
    assert_eq!(full.len(), 3, "full decode should have all entries");
    assert_eq!(full.get("x-amz-meta-alpha").map(|s| s.as_str()), Some("value-a"));

    for cut in [1, 3, encoded.len() / 2, encoded.len() - 1] {
        let truncated = &encoded[..cut];
        let partial = decode_metadata(truncated);
        assert!(
            partial.len() <= full.len(),
            "truncated decode at {cut} should have <= {} entries, got {}",
            full.len(),
            partial.len()
        );
    }

    let empty = decode_metadata(&[]);
    assert!(empty.is_empty(), "empty input should produce empty map");
}

#[test]
fn tlv_decode_single_byte_truncation() {
    use std::collections::HashMap;
    use shardedobjstr::tlv::{decode_metadata, encode_metadata};

    let mut meta = HashMap::new();
    meta.insert("x-amz-meta-foo".to_string(), "bar".to_string());
    let encoded = encode_metadata(&meta).unwrap();

    if encoded.len() > 1 {
        let truncated = &encoded[..1];
        let result = decode_metadata(truncated);
        assert!(result.is_empty() || result.len() == 1, "single byte truncation handled");
    }
}

// =====================================================================
// put_with_meta_from_file roundtrip
// =====================================================================

#[test]
fn put_with_meta_from_file_roundtrip() {
    use shardedobjstr::metadata::put_with_meta_from_file;
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);

    let body = vec![0xAA; 8192];
    let metadata = b"file-meta-payload";
    let meta_len = metadata.len() as u16;

    // Build the file: body bytes followed by metadata bytes.
    let file_path = dir.path().join("upload.bin");
    {
        let mut f = std::fs::File::create(&file_path).unwrap();
        f.write_all(&body).unwrap();
        f.write_all(metadata).unwrap();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut f = std::fs::File::open(&file_path).unwrap();
        put_with_meta_from_file(&cluster, &registry, &Path::from("file/obj.bin"), &mut f, meta_len)
            .await
            .unwrap();
        flush_all(&raws);

        // Verify body is readable and correct size.
        let got = cluster.get(&Path::from("file/obj.bin")).await.unwrap();
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.len(), body.len(), "body size mismatch");
        assert_eq!(&bytes[..], &body[..]);

        // Verify metadata is readable.
        let got_meta = get_metadata(&cluster, &registry, &Path::from("file/obj.bin"))
            .await
            .unwrap();
        assert_eq!(&got_meta[..], metadata);

        // Verify catalog records correct meta_len.
        let entry = cluster.placement("file/obj.bin").unwrap();
        assert_eq!(entry.meta_len, meta_len);
    });
}

// =====================================================================
// Metadata on S3Like shards (InMemory supports attributes)
// =====================================================================

fn setup_s3like_cluster(
    _dir: &tempfile::TempDir,
) -> (ShardedObjectStore, RawRefRegistry) {
    // InMemory stores support put_opts with Attributes, so they work
    // as S3Like shards for metadata testing.
    let mem0 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let mem1 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let mem2 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

    let stores = vec![mem0, mem1, mem2];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // No raw refs for S3Like shards -- all None
    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![None, None, None];
    let kinds = vec![ShardKind::S3Like; 3];
    let registry = RawRefRegistry::new(refs, kinds);

    (cluster, registry)
}

#[test]
fn s3like_put_with_meta_and_get_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_s3like_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xABu8; 4096]);
    // Build TLV-encoded metadata with known fields
    let mut meta_map = std::collections::HashMap::new();
    meta_map.insert("content-type".to_string(), "application/json".to_string());
    meta_map.insert("x-amz-meta-color".to_string(), "blue".to_string());
    let metadata = encode_metadata(&meta_map).unwrap();

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("s3like/doc.bin"),
            payload.clone(),
            &metadata,
        )
        .await
        .unwrap();

        // get_metadata should return TLV-encoded bytes that decode to
        // the same fields we sent.
        let got = get_metadata(&cluster, &registry, &Path::from("s3like/doc.bin"))
            .await
            .unwrap();
        let decoded = decode_metadata(&got);
        assert_eq!(
            decoded.get("content-type").map(|s| s.as_str()),
            Some("application/json"),
            "content-type should roundtrip through S3Like"
        );
        assert_eq!(
            decoded.get("x-amz-meta-color").map(|s| s.as_str()),
            Some("blue"),
            "custom metadata should roundtrip through S3Like"
        );

        // head_with_meta reads meta_len from the catalog for non-raw shards.
        let (_obj_meta, meta_len) =
            head_with_meta(&cluster, &registry, &Path::from("s3like/doc.bin"))
                .await
                .unwrap();
        assert!(meta_len > 0, "S3Like shards should report catalog meta_len in head");

        // Body should be intact (no metadata suffix)
        let data = cluster
            .get(&Path::from("s3like/doc.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    });
}

#[test]
fn s3like_get_metadata_empty_when_no_attributes() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_s3like_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put without metadata (plain put through the cluster)
    rt.block_on(async {
        cluster
            .put(
                &Path::from("s3like/plain.bin"),
                object_store::PutPayload::from(Bytes::from(vec![0x01u8; 64])),
            )
            .await
            .unwrap();

        // get_metadata should return empty bytes (no attributes stored)
        let got = get_metadata(&cluster, &registry, &Path::from("s3like/plain.bin"))
            .await
            .unwrap();
        assert!(got.is_empty(), "plain put on S3Like should have empty metadata");
    });
}

#[test]
fn s3like_list_with_meta_returns_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_s3like_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta_map = std::collections::HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
    ]);
    let metadata = encode_metadata(&meta_map).unwrap();

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("s3like/list/a.txt"),
            Bytes::from(vec![0x01; 128]),
            &metadata,
        )
        .await
        .unwrap();
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("s3like/list/b.txt"),
            Bytes::from(vec![0x02; 128]),
            &metadata,
        )
        .await
        .unwrap();

        let listed = list_with_meta(
            &cluster,
            &registry,
            Some(&Path::from("s3like/list")),
        )
        .await;

        let keys: Vec<String> = listed.iter().map(|(m, _)| m.location.to_string()).collect();
        assert_eq!(keys.len(), 2, "should list 2 S3Like objects");
    });
}

#[test]
fn mixed_raw_and_s3like_cluster_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;

    // Shard 0: raw
    let raw0 = format_shard(&dir.path().join("shard0.raw"), shard_size);
    // Shard 1: S3Like (InMemory)
    let mem1 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _, mem1];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![Some(raw0.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::S3Like];
    let registry = RawRefRegistry::new(refs, kinds);

    let mut meta_map = std::collections::HashMap::new();
    meta_map.insert("content-type".to_string(), "image/png".to_string());
    let metadata = encode_metadata(&meta_map).unwrap();

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("mixed/obj.bin"),
            Bytes::from(vec![0xCC; 2048]),
            &metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&[raw0]);

    rt.block_on(async {
        // Metadata should be retrievable regardless of which shard serves it
        let got = get_metadata(&cluster, &registry, &Path::from("mixed/obj.bin"))
            .await
            .unwrap();
        let decoded = decode_metadata(&got);
        assert_eq!(
            decoded.get("content-type").map(|s| s.as_str()),
            Some("image/png"),
            "metadata should be available from mixed cluster"
        );

        // Body should be correct
        let data = cluster
            .get(&Path::from("mixed/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 2048);
    });
}

// =====================================================================
// Sidecar: head_with_meta returns meta_len=0
// =====================================================================

#[test]
fn sidecar_head_with_meta_returns_catalog_meta_len() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let metadata = b"sidecar-test-meta";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sc/head.bin"),
            Bytes::from(vec![0xBB; 256]),
            metadata,
        )
        .await
        .unwrap();

        // head_with_meta reads meta_len from the catalog for non-raw shards.
        let (_obj_meta, meta_len) =
            head_with_meta(&cluster, &registry, &Path::from("sc/head.bin"))
                .await
                .unwrap();
        assert_eq!(meta_len, metadata.len() as u16, "Sidecar should report catalog meta_len in head");

        // Body should be intact.
        let data = cluster
            .get(&Path::from("sc/head.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 256);
    });
}

// =====================================================================
// Sidecar: get_metadata roundtrip with raw bytes
// =====================================================================

#[test]
fn sidecar_get_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let metadata = b"sidecar-roundtrip-meta";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sc/get.bin"),
            Bytes::from(vec![0xCC; 512]),
            metadata,
        )
        .await
        .unwrap();

        let got = get_metadata(&cluster, &registry, &Path::from("sc/get.bin"))
            .await
            .unwrap();
        assert_eq!(
            &got[..], metadata,
            "metadata should roundtrip through Sidecar path"
        );
    });
}

// =====================================================================
// Sidecar: TLV-encoded metadata roundtrip
// =====================================================================

#[test]
fn sidecar_get_metadata_with_tlv_encoded() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut meta_map = std::collections::HashMap::new();
    meta_map.insert("content-type".to_string(), "text/html".to_string());
    meta_map.insert("x-amz-meta-tag".to_string(), "sidecar".to_string());
    let metadata = encode_metadata(&meta_map).unwrap();

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("sc/tlv.bin"),
            Bytes::from(vec![0xDD; 128]),
            &metadata,
        )
        .await
        .unwrap();

        let got = get_metadata(&cluster, &registry, &Path::from("sc/tlv.bin"))
            .await
            .unwrap();
        let decoded = decode_metadata(&got);
        assert_eq!(
            decoded.get("content-type").map(|s| s.as_str()),
            Some("text/html"),
            "content-type should roundtrip through Sidecar"
        );
        assert_eq!(
            decoded.get("x-amz-meta-tag").map(|s| s.as_str()),
            Some("sidecar"),
            "custom metadata should roundtrip through Sidecar"
        );
    });
}

// =====================================================================
// put_with_meta: retries on total initial target failure
// =====================================================================

#[test]
fn put_with_meta_retries_on_total_initial_failure() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    // 4 shards, RF=2.
    let raws: Vec<Arc<RawObjectStore>> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("pmr{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 4];
    let registry = RawRefRegistry::new(refs, kinds);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Determine initial targets for our key.
    let targets = cluster.target_shards(&Path::from("retry_meta/obj.bin"));
    assert_eq!(targets.len(), 2);

    // Detach both initial targets.
    for &sid in &targets {
        cluster.detach_shard(sid);
    }

    // put_with_meta should still succeed by retrying on fresh targets.
    let meta_bytes = b"retry-meta";
    rt.block_on(async {
        let result = put_with_meta(
            &cluster,
            &registry,
            &Path::from("retry_meta/obj.bin"),
            Bytes::from(vec![0x55; 256]),
            meta_bytes,
        )
        .await;
        assert!(
            result.is_ok(),
            "put_with_meta should succeed after retrying: {:?}",
            result.err()
        );
    });
    flush_all(&raws);

    // Verify the data arrived on the survivors.
    rt.block_on(async {
        let data = cluster
            .get(&Path::from("retry_meta/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 256);
    });
}

// =====================================================================
// HIGH: mixed shard kinds -- Raw + Sidecar (InMemory)
// =====================================================================

#[test]
fn mixed_shard_kinds_raw_and_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raw0 = format_shard(&dir.path().join("mix0.raw"), sz);

    // InMemory store acts as a Sidecar shard.
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _, mem.clone()];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Build RawRefRegistry with Raw + Sidecar kinds.
    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![Some(raw0.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(refs, kinds);

    let payload = Bytes::from(vec![0xCC; 512]);
    let metadata = b"mixed-meta-test";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("mixed/obj.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&[raw0.clone()]);

    // Verify metadata is retrievable.
    rt.block_on(async {
        let got = get_metadata(&cluster, &registry, &Path::from("mixed/obj.bin"))
            .await
            .unwrap();
        assert_eq!(
            &got[..],
            metadata,
            "metadata should roundtrip through mixed Raw+Sidecar cluster"
        );

        // head_with_meta should report correct meta_len.
        let (obj_meta, meta_len) =
            head_with_meta(&cluster, &registry, &Path::from("mixed/obj.bin"))
                .await
                .unwrap();
        assert_eq!(obj_meta.size, 512, "body size should be 512");
        // put_with_meta writes meta_len via catalog.put() which sets
        // it correctly. replicate_object and read_repair now also
        // propagate meta_len (previously hardcoded 0).
        assert!(meta_len > 0, "meta_len should be > 0 after put_with_meta");
    });

    // Verify sidecar file exists for the InMemory shard.
    let sidecar_path = meta_sidecar_path(&Path::from("mixed/obj.bin"));
    let sidecar_exists = rt.block_on(async { mem.head(&sidecar_path).await.is_ok() });
    assert!(
        sidecar_exists,
        "InMemory (Sidecar) shard should have a __meta__ sidecar file"
    );
}

// =====================================================================
// HIGH: list_with_meta on mixed shard cluster
// =====================================================================

#[test]
fn list_with_meta_mixed_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raw0 = format_shard(&dir.path().join("lm0.raw"), sz);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _, mem.clone()];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![Some(raw0.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(refs, kinds);

    // Put some objects with and without metadata.
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("lm/meta.bin"),
            Bytes::from(vec![0xAA; 256]),
            b"list-meta",
        )
        .await
        .unwrap();

        cluster
            .put(
                &Path::from("lm/plain.bin"),
                PutPayload::from(Bytes::from(vec![0xBB; 128])),
            )
            .await
            .unwrap();
    });
    flush_all(&[raw0.clone()]);

    rt.block_on(async {
        let items = list_with_meta(&cluster, &registry, Some(&Path::from("lm/")))
            .await;
        assert_eq!(items.len(), 2, "should list 2 objects");

        let meta_item = items.iter().find(|(m, _)| m.location.as_ref().contains("meta")).unwrap();
        assert!(meta_item.1 > 0, "meta.bin should have meta_len > 0");

        let plain_item = items.iter().find(|(m, _)| m.location.as_ref().contains("plain")).unwrap();
        assert_eq!(plain_item.1, 0, "plain.bin should have meta_len == 0");
    });
}

// =====================================================================
// HIGH: delete_sidecar cleans up __meta__ companion files
// =====================================================================

#[test]
fn delete_sidecar_cleans_companion_file() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raw0 = format_shard(&dir.path().join("ds0.raw"), sz);
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw0.clone() as _, mem.clone()];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let refs: Vec<Option<Arc<RawObjectStore>>> = vec![Some(raw0.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(refs, kinds);

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("dsc/obj.bin"),
            Bytes::from(vec![0xEE; 128]),
            b"will-be-deleted",
        )
        .await
        .unwrap();
    });
    flush_all(&[raw0.clone()]);

    // Verify sidecar exists.
    let sidecar = meta_sidecar_path(&Path::from("dsc/obj.bin"));
    let exists_before = rt.block_on(async { mem.head(&sidecar).await.is_ok() });
    assert!(exists_before, "sidecar should exist before delete");

    // Delete the sidecar.
    rt.block_on(async {
        delete_sidecar(&cluster, &registry, &Path::from("dsc/obj.bin"))
            .await;
    });

    // Verify sidecar is gone.
    let exists_after = rt.block_on(async { mem.head(&sidecar).await.is_ok() });
    assert!(!exists_after, "sidecar should be gone after delete_sidecar");
}

// =====================================================================
// Bug fix: put_with_meta_from_file marks shards Degraded and cleans
// up orphaned data on InsufficientWrites.
// =====================================================================

#[test]
fn put_with_meta_from_file_insufficient_writes_cleans_up() {
    use shardedobjstr::metadata::put_with_meta_from_file;
    use shardedobjstr::ShardHealth;
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    // rf=3, min_writes=3 (strict), 3 shards
    let (cluster, raws, registry) =
        setup_cluster_with_refs_full(&dir, 3, 3, false);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Take one shard offline so only 2 writes succeed < min_writes=3
    cluster.set_shard_health(2, ShardHealth::Offline);

    let body = vec![0xBB; 4096];
    let metadata = b"some-meta";
    let meta_len = metadata.len() as u16;

    let file_path = dir.path().join("upload_fail.bin");
    {
        let mut f = std::fs::File::create(&file_path).unwrap();
        f.write_all(&body).unwrap();
        f.write_all(metadata).unwrap();
    }

    let result = rt.block_on(async {
        let mut f = std::fs::File::open(&file_path).unwrap();
        put_with_meta_from_file(
            &cluster,
            &registry,
            &Path::from("file/fail.bin"),
            &mut f,
            meta_len,
        )
        .await
    });
    flush_all(&raws);
    assert!(result.is_err(), "should fail with InsufficientWrites");

    // The key should NOT be in the catalog (orphans must be cleaned up).
    assert!(
        cluster.placement("file/fail.bin").is_none(),
        "catalog should not contain the failed key"
    );
}

// =====================================================================
// Bug fix: fallback read recovers meta_len from Raw shards.
// =====================================================================

#[test]
fn fallback_read_recovers_meta_len_from_raw_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xCC; 2048]);
    let metadata = b"fallback-meta-test";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("fallback/obj.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify meta_len is recorded in catalog.
    let entry = cluster.placement("fallback/obj.bin").unwrap();
    assert_eq!(entry.meta_len, metadata.len() as u16);

    // Clear catalog so the next read is a fallback read.
    cluster.catalog().clear();
    assert!(cluster.placement("fallback/obj.bin").is_none());

    // Attach raw_refs so the fallback read can recover meta_len.
    let refs = Arc::new(registry);
    cluster.set_raw_refs(refs.clone());

    // A get() should succeed (fallback) and re-populate the catalog.
    rt.block_on(async {
        let got = cluster.get(&Path::from("fallback/obj.bin")).await.unwrap();
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.len(), 2048, "body should be body-only");
    });

    // The catalog entry should now have the correct meta_len recovered
    // from the Raw shard's index.
    let recovered = cluster.placement("fallback/obj.bin").unwrap();
    assert_eq!(
        recovered.meta_len,
        metadata.len() as u16,
        "fallback read should recover meta_len from raw shard"
    );
}

// =====================================================================
// cleanup_sidecar_maybe: only deletes sidecar on Sidecar-kind shards
// =====================================================================

#[test]
fn cleanup_sidecar_maybe_deletes_on_sidecar_shard() {
    use shardedobjstr::metadata::cleanup_sidecar_maybe;

    let dir = tempfile::tempdir().unwrap();
    let (cluster, registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put an object with metadata -> creates sidecar files.
    let payload = Bytes::from(vec![0xDD; 512]);
    let metadata = b"cleanup-test-meta";
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("cleanup/obj.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });

    // Verify sidecar exists on at least one shard.
    let sidecar = meta_sidecar_path(&Path::from("cleanup/obj.bin"));
    let placement = cluster.placement("cleanup/obj.bin").unwrap();
    let shard_id = placement.shards[0];
    let store = cluster.shard_store(shard_id).unwrap();
    let exists_before = rt.block_on(async { store.head(&sidecar).await.is_ok() });
    assert!(exists_before, "sidecar should exist before cleanup");

    // cleanup_sidecar_maybe with the registry -> should detect Sidecar kind and delete.
    rt.block_on(async {
        cleanup_sidecar_maybe(
            store.as_ref(),
            &Path::from("cleanup/obj.bin"),
            Some(&registry),
            shard_id,
        )
        .await;
    });

    let exists_after = rt.block_on(async { store.head(&sidecar).await.is_ok() });
    assert!(!exists_after, "sidecar should be deleted by cleanup_sidecar_maybe");
}

#[test]
fn cleanup_sidecar_maybe_noop_on_raw_shard() {
    use shardedobjstr::metadata::cleanup_sidecar_maybe;

    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup_cluster_with_refs(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put an object with metadata on raw shards.
    let payload = Bytes::from(vec![0xAA; 512]);
    let metadata = b"raw-cleanup-noop";
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("rawclean/obj.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // For Raw shards, cleanup_sidecar_maybe should be a no-op (no sidecar to delete).
    let placement = cluster.placement("rawclean/obj.bin").unwrap();
    let shard_id = placement.shards[0];
    let store = cluster.shard_store(shard_id).unwrap();

    // This should not error even though no sidecar exists.
    rt.block_on(async {
        cleanup_sidecar_maybe(
            store.as_ref(),
            &Path::from("rawclean/obj.bin"),
            Some(&registry),
            shard_id,
        )
        .await;
    });

    // Object should still be readable.
    rt.block_on(async {
        let data = cluster
            .get(&Path::from("rawclean/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 512, "original object should still be intact");
    });
}

#[test]
fn cleanup_sidecar_maybe_without_refs_assumes_sidecar() {
    use shardedobjstr::metadata::cleanup_sidecar_maybe;

    let dir = tempfile::tempdir().unwrap();
    let (cluster, _registry) = setup_sidecar_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = Bytes::from(vec![0xEE; 256]);
    let metadata = b"no-refs-cleanup";
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &_registry,
            &Path::from("norefs/obj.bin"),
            payload,
            metadata,
        )
        .await
        .unwrap();
    });

    let sidecar = meta_sidecar_path(&Path::from("norefs/obj.bin"));
    let placement = cluster.placement("norefs/obj.bin").unwrap();
    let shard_id = placement.shards[0];
    let store = cluster.shard_store(shard_id).unwrap();

    let exists_before = rt.block_on(async { store.head(&sidecar).await.is_ok() });
    assert!(exists_before, "sidecar should exist");

    // Pass refs=None -> assumes sidecar, should attempt delete.
    rt.block_on(async {
        cleanup_sidecar_maybe(
            store.as_ref(),
            &Path::from("norefs/obj.bin"),
            None,
            shard_id,
        )
        .await;
    });

    let exists_after = rt.block_on(async { store.head(&sidecar).await.is_ok() });
    assert!(
        !exists_after,
        "cleanup_sidecar_maybe with refs=None should assume Sidecar and delete"
    );
}
