//! End-to-end tests for the delete-marker system.
//!
//! Exercises: delete creates markers, markers are hidden from
//! list/head/get, list_delete_markers, vacuum_delete_markers,
//! re-PUT after delete, and vacuum with offline shard.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::{ShardHealth, ShardedObjectStore, DELETE_MARKER_PREFIX};

use common::{build_cluster, flush_all, format_shard};

/// Helper: put an object into the cluster and flush.
fn put_object(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    raw: &[Arc<RawObjectStore>],
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
    flush_all(raw);
}

/// Helper: create 3 shards and a cluster with replication factor 2.
fn setup_cluster(
    dir: &tempfile::TempDir,
) -> (Vec<Arc<RawObjectStore>>, ShardedObjectStore) {
    let size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("s0.raw"), size);
    let s1 = format_shard(&dir.path().join("s1.raw"), size);
    let s2 = format_shard(&dir.path().join("s2.raw"), size);
    let raw = vec![s0, s1, s2];
    let cluster = build_cluster(&raw, 2);
    (raw, cluster)
}

// ---- delete creates a marker and removes the real object ----

#[test]
fn delete_creates_marker_and_removes_object() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "data/foo.bin", &[0xAA; 4096]);

    // Object exists before delete.
    rt.block_on(async {
        cluster.head(&Path::from("data/foo.bin")).await.unwrap();
    });

    // Delete the object.
    rt.block_on(async {
        cluster.delete(&Path::from("data/foo.bin")).await.unwrap();
    });
    flush_all(&raw);

    // Real object is gone.
    rt.block_on(async {
        let err = cluster.head(&Path::from("data/foo.bin")).await;
        assert!(err.is_err(), "head should return NotFound after delete");
    });

    // Marker exists internally.
    rt.block_on(async {
        let ts = cluster.get_delete_marker("data/foo.bin").await;
        assert!(ts.is_some(), "delete marker should exist");
    });
}

// ---- markers are hidden from list ----

#[test]
fn list_hides_delete_markers() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "a.txt", b"aaa");
    put_object(&rt, &cluster, &raw, "b.txt", b"bbb");

    rt.block_on(async {
        cluster.delete(&Path::from("a.txt")).await.unwrap();
    });
    flush_all(&raw);

    let listed: Vec<String> = rt.block_on(async {
        cluster
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect()
    });

    assert!(
        !listed.iter().any(|k| k.starts_with(DELETE_MARKER_PREFIX)),
        "list must not contain marker keys: {:?}",
        listed
    );
    assert!(
        !listed.contains(&"a.txt".to_string()),
        "deleted object must not appear in list"
    );
    assert!(
        listed.contains(&"b.txt".to_string()),
        "non-deleted object should appear in list"
    );
}

// ---- head returns NotFound for __deleted__/* keys ----

#[test]
fn head_returns_not_found_for_marker_key() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "obj.dat", b"hello");

    rt.block_on(async {
        cluster.delete(&Path::from("obj.dat")).await.unwrap();
    });
    flush_all(&raw);

    // Direct head on the marker key should fail.
    rt.block_on(async {
        let marker_key = format!("{DELETE_MARKER_PREFIX}obj.dat");
        let err = cluster.head(&Path::from(marker_key.as_str())).await;
        assert!(err.is_err(), "head on marker key should return NotFound");
    });
}

// ---- get returns NotFound for __deleted__/* keys ----

#[test]
fn get_returns_not_found_for_marker_key() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "doc.txt", b"contents");

    rt.block_on(async {
        cluster.delete(&Path::from("doc.txt")).await.unwrap();
    });
    flush_all(&raw);

    rt.block_on(async {
        let marker_key = format!("{DELETE_MARKER_PREFIX}doc.txt");
        let err = cluster.get(&Path::from(marker_key.as_str())).await;
        assert!(err.is_err(), "get on marker key should return NotFound");
    });
}

// ---- list_delete_markers returns expected entries ----

#[test]
fn list_delete_markers_returns_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "x.bin", b"xxx");
    put_object(&rt, &cluster, &raw, "y.bin", b"yyy");
    put_object(&rt, &cluster, &raw, "z.bin", b"zzz");

    rt.block_on(async {
        cluster.delete(&Path::from("x.bin")).await.unwrap();
        cluster.delete(&Path::from("z.bin")).await.unwrap();
    });
    flush_all(&raw);

    let markers: Vec<(String, _)> = rt.block_on(async {
        cluster.list_delete_markers().await
    });

    let keys: Vec<&str> = markers.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"x.bin"), "x.bin should have a marker");
    assert!(keys.contains(&"z.bin"), "z.bin should have a marker");
    assert!(!keys.contains(&"y.bin"), "y.bin was not deleted");
    assert_eq!(markers.len(), 2);
}

// ---- vacuum purges applied markers (no live object) ----

#[test]
fn vacuum_purges_applied_markers() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "gone.txt", b"data");

    rt.block_on(async {
        cluster.delete(&Path::from("gone.txt")).await.unwrap();
    });
    flush_all(&raw);

    // Confirm marker exists before vacuum.
    let pre_markers = rt.block_on(async { cluster.list_delete_markers().await });
    assert_eq!(pre_markers.len(), 1, "one marker before vacuum");

    // Run vacuum.
    let (purged, cleaned) = rt.block_on(async {
        cluster.vacuum_delete_markers(None).await.unwrap()
    });
    flush_all(&raw);

    assert_eq!(purged, 1, "one marker should be purged");
    assert_eq!(cleaned, 0, "no stale objects to clean");

    // Marker is gone after vacuum.
    let post_markers = rt.block_on(async { cluster.list_delete_markers().await });
    assert_eq!(post_markers.len(), 0, "no markers after vacuum");
}

// ---- vacuum handles re-PUT: PUT clears the stale marker, vacuum is a no-op ----

#[test]
fn vacuum_handles_reput_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "reused.bin", b"original");

    rt.block_on(async {
        cluster.delete(&Path::from("reused.bin")).await.unwrap();
    });
    flush_all(&raw);

    // Re-PUT the same key with new content.
    // Small sleep to ensure timestamp is strictly later.
    std::thread::sleep(std::time::Duration::from_millis(50));
    put_object(&rt, &cluster, &raw, "reused.bin", b"new-version");

    // The PUT already cleaned the stale marker, so vacuum is a no-op.
    let (purged, cleaned) = rt.block_on(async {
        cluster.vacuum_delete_markers(None).await.unwrap()
    });
    flush_all(&raw);

    assert_eq!(purged, 0, "marker already cleaned by re-PUT");
    assert_eq!(cleaned, 0, "new object should not be cleaned");

    // Object still readable.
    rt.block_on(async {
        let result = cluster.get(&Path::from("reused.bin")).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(&data[..], b"new-version");
    });

    // No markers remain.
    let markers = rt.block_on(async { cluster.list_delete_markers().await });
    assert_eq!(markers.len(), 0);
}

// ---- vacuum fails if any shard is offline ----

#[test]
fn vacuum_fails_with_offline_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "file.dat", b"data");

    rt.block_on(async {
        cluster.delete(&Path::from("file.dat")).await.unwrap();
    });
    flush_all(&raw);

    // Mark shard 0 as offline.
    cluster.set_shard_health(0, ShardHealth::Offline);

    let result = rt.block_on(async {
        cluster.vacuum_delete_markers(None).await
    });

    assert!(result.is_err(), "vacuum should fail with offline shard");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("offline"),
        "error message should mention offline: {}",
        err_msg
    );
}

// ---- delete + re-PUT: new object is fully accessible ----

#[test]
fn delete_then_reput_works() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "cycle.bin", b"v1");

    rt.block_on(async {
        cluster.delete(&Path::from("cycle.bin")).await.unwrap();
    });
    flush_all(&raw);

    // Re-PUT.
    put_object(&rt, &cluster, &raw, "cycle.bin", b"v2");

    // Should be readable as the new version.
    rt.block_on(async {
        let result = cluster.get(&Path::from("cycle.bin")).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(&data[..], b"v2");
    });

    // Head should also work.
    rt.block_on(async {
        let meta = cluster.head(&Path::from("cycle.bin")).await.unwrap();
        assert_eq!(meta.size, 2); // "v2" is 2 bytes
    });

    // Should appear in listing.
    let listed: Vec<String> = rt.block_on(async {
        cluster
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect()
    });
    assert!(listed.contains(&"cycle.bin".to_string()));
}

// ---- is_delete_marker helper ----

#[test]
fn is_delete_marker_helper() {
    assert!(ShardedObjectStore::is_delete_marker("__deleted__/foo"));
    assert!(ShardedObjectStore::is_delete_marker("__deleted__/a/b/c"));
    assert!(!ShardedObjectStore::is_delete_marker("foo"));
    assert!(!ShardedObjectStore::is_delete_marker("deleted/foo"));
    assert!(!ShardedObjectStore::is_delete_marker(""));
}

// ---- delete of nonexistent key is idempotent ----

#[test]
fn delete_nonexistent_key() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Delete something that was never created.
    // The underlying stores treat NotFound as OK for delete, and we
    // still create a marker (which is fine -- vacuum will clean it up).
    rt.block_on(async {
        let result = cluster.delete(&Path::from("never-existed.bin")).await;
        // Should succeed -- object_store delete is typically idempotent.
        assert!(result.is_ok(), "delete of nonexistent key should succeed");
    });
    flush_all(&raw);
}

// ---- delete of nonexistent key with min_writes must not panic ----

#[test]
fn delete_nonexistent_with_min_writes_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);
    let cluster = cluster
        .with_min_writes(2)
        .with_delete_requires_min_writes(true);

    let rt = tokio::runtime::Runtime::new().unwrap();
    // Previously panicked via last_err.unwrap() when all shards
    // returned NotFound (treated as success), leaving last_err as None.
    let result = rt.block_on(cluster.delete(&Path::from("does-not-exist")));
    assert!(result.is_ok(), "delete of non-existent key should succeed (NotFound treated as success)");
}

// ---- vacuum cleans stale object on shard that missed delete ----

#[test]
fn vacuum_deletes_older_object() {
    let dir = tempfile::tempdir().unwrap();
    let (raw, cluster) = setup_cluster(&dir);

    let rt = tokio::runtime::Runtime::new().unwrap();

    put_object(&rt, &cluster, &raw, "vacuum/stale.bin", &[0xCC; 4096]);

    rt.block_on(async {
        // Detach shard 1 so the delete only reaches shard 0.
        cluster.detach_shard(1);

        // Small delay for timestamp ordering.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Delete -- marker lands on shard 0 but shard 1 still has stale data.
        cluster.delete(&Path::from("vacuum/stale.bin")).await.unwrap();

        // Re-attach shard 1 (has stale object, missed the delete).
        cluster.attach_shard(1, raw[1].clone(), false).await.unwrap();

        // Rebuild catalog picks up the stale object from shard 1.
        cluster.rebuild_catalog().await.unwrap();

        // Vacuum should clean up the stale object on shard 1.
        let (purged, cleaned) = cluster.vacuum_delete_markers(None).await.unwrap();
        assert!(purged >= 1 || cleaned >= 1, "vacuum should clean stale state");
    });
}
