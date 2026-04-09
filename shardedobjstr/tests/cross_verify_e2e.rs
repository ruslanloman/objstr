//! End-to-end tests for cross-shard MD5 verification.
//!
//! Covers: cross_verify_object, cross_verify_all, mixed raw+FS cluster,
//! corruption detection via behind-the-scenes file replacement.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use tempfile::TempDir;

use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard, build_mixed_cluster};

// -- Helpers ---------------------------------------------------------

fn setup_2shard_raw(
    dir: &TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    (cluster, raws)
}

fn put_test_objects(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    count: usize,
) -> Vec<String> {
    let keys: Vec<String> = (0..count)
        .map(|i| format!("test/obj_{:04}.bin", i))
        .collect();
    rt.block_on(async {
        for (i, key) in keys.iter().enumerate() {
            let data = vec![0x41 + (i as u8 % 26); 4096];
            cluster
                .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
        }
    });
    keys
}

// -- Tests -----------------------------------------------------------

/// Verify that a healthy 2-shard raw cluster passes cross-verification.
#[test]
fn cross_verify_consistent_raw_cluster() {
    let dir = TempDir::new().unwrap();
    let (cluster, raws) = setup_2shard_raw(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let _keys = put_test_objects(&rt, &cluster, 5);
    flush_all(&raws);

    let report = rt.block_on(cluster.cross_verify_all(None, None));

    assert_eq!(report.objects_checked, 5);
    assert_eq!(report.objects_ok, 5);
    assert_eq!(report.objects_mismatched, 0);
    assert_eq!(report.objects_with_errors, 0);
    assert_eq!(report.objects_skipped_single_replica, 0);
    assert!(report.details.is_empty());
}

/// Verify that a mixed raw+FS cluster with RF=2 passes when no corruption.
#[test]
fn cross_verify_consistent_mixed_cluster() {
    let dir = TempDir::new().unwrap();
    let raw_path = dir.path().join("s0.raw");
    let fs_dir = dir.path().join("fs_shard");
    std::fs::create_dir_all(&fs_dir).unwrap();

    let sz = 64 * 1024 * 1024u64;
    let (cluster, raw, _fs_path) = build_mixed_cluster(&raw_path, &fs_dir, sz, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let _keys = put_test_objects(&rt, &cluster, 5);
    raw.flush_index().unwrap();

    let report = rt.block_on(cluster.cross_verify_all(None, None));

    assert_eq!(report.objects_checked, 5);
    assert_eq!(report.objects_ok, 5);
    assert_eq!(report.objects_mismatched, 0);
    assert_eq!(report.objects_with_errors, 0);
}

/// Corrupt a file on the FS shard behind the scenes and verify the tool
/// detects the mismatch.
#[test]
fn cross_verify_detects_fs_corruption() {
    let dir = TempDir::new().unwrap();
    let raw_path = dir.path().join("s0.raw");
    let fs_dir = dir.path().join("fs_shard");
    std::fs::create_dir_all(&fs_dir).unwrap();

    let sz = 64 * 1024 * 1024u64;
    let (cluster, raw, fs_path) = build_mixed_cluster(&raw_path, &fs_dir, sz, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = put_test_objects(&rt, &cluster, 5);
    raw.flush_index().unwrap();

    // Sanity: everything is consistent before corruption.
    let report = rt.block_on(cluster.cross_verify_all(None, None));
    assert_eq!(report.objects_mismatched, 0, "pre-corruption should be clean");

    // Corrupt one file on the FS shard by overwriting it with different data.
    // The FS shard stores files at <fs_dir>/<key>, so the path is:
    let corrupt_key = &keys[2]; // "test/obj_0002.bin"
    let corrupt_fs_file = fs_path.join(corrupt_key);
    assert!(
        corrupt_fs_file.exists(),
        "FS shard file should exist at {:?}",
        corrupt_fs_file,
    );
    std::fs::write(&corrupt_fs_file, b"CORRUPTED DATA - this is not the original content").unwrap();

    // Now cross-verify should detect exactly 1 mismatch.
    let report = rt.block_on(cluster.cross_verify_all(None, None));

    assert_eq!(report.objects_checked, 5);
    assert_eq!(report.objects_mismatched, 1, "should detect the corrupted file");
    assert_eq!(report.objects_ok, 4);
    assert_eq!(report.details.len(), 1);

    let detail = &report.details[0];
    assert_eq!(detail.key, *corrupt_key);
    assert!(!detail.consistent);
    // Both shards should have reported an MD5, but they should differ.
    assert_eq!(detail.shards.len(), 2);
    assert_ne!(detail.shards[0].md5_hex, detail.shards[1].md5_hex);
}

/// Verify single-object cross-verification works.
#[test]
fn cross_verify_single_object() {
    let dir = TempDir::new().unwrap();
    let (cluster, raws) = setup_2shard_raw(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = put_test_objects(&rt, &cluster, 3);
    flush_all(&raws);

    let report = rt.block_on(cluster.cross_verify_object(&keys[0])).unwrap();
    assert!(report.consistent);
    assert_eq!(report.shards.len(), 2);
    assert_eq!(report.shards[0].md5_hex, report.shards[1].md5_hex);
    assert!(report.errors.is_empty());
}

/// Cross-verify on an object with only 1 replica should return an error.
#[test]
fn cross_verify_single_replica_returns_error() {
    let dir = TempDir::new().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    // RF=1: each object placed on only 1 shard.
    let cluster = build_cluster(&raws, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = put_test_objects(&rt, &cluster, 3);
    flush_all(&raws);

    // Single object should error (only 1 replica).
    let result = rt.block_on(cluster.cross_verify_object(&keys[0]));
    assert!(result.is_err(), "should error with only 1 replica");

    // cross_verify_all should skip all objects.
    let report = rt.block_on(cluster.cross_verify_all(None, None));
    assert_eq!(report.objects_checked, 0);
    assert_eq!(report.objects_skipped_single_replica, 3);
}

/// Verify prefix filtering works in cross_verify_all.
#[test]
fn cross_verify_with_prefix_filter() {
    let dir = TempDir::new().unwrap();
    let (cluster, raws) = setup_2shard_raw(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects with different prefixes.
    rt.block_on(async {
        for i in 0..3 {
            let key = format!("alpha/f{}.bin", i);
            let data = vec![0xAA; 1024];
            cluster.put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data))).await.unwrap();
        }
        for i in 0..2 {
            let key = format!("beta/f{}.bin", i);
            let data = vec![0xBB; 1024];
            cluster.put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data))).await.unwrap();
        }
    });
    flush_all(&raws);

    let report = rt.block_on(cluster.cross_verify_all(Some("alpha/"), None));
    assert_eq!(report.objects_checked, 3);
    assert_eq!(report.objects_ok, 3);
}
