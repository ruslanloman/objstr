//! End-to-end test for ShardedObjectStore.
//!
//! Exercises: format, put, get, list, head, copy, delete, verify,
//! add-shard, remove-shard, rebuild_catalog -- all via the Rust API
//! (same operations as shardedobjstr).

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::{path::Path, GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::ShardedObjectStore;
use shardedobjstr::ShardHealth;

use common::{
    build_cluster, build_cluster_with_events, count_delete_events, count_put_events,
    drain_events, flush_all, format_shard, open_shard, put_event_keys,
};

#[test]
fn cluster_e2e_all_features() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024; // 64 MB each
    let replicas = 2;

    // ── 1. Format 3 shards ──────────────────────────────────────────
    let s0_path = dir.path().join("shard0.raw");
    let s1_path = dir.path().join("shard1.raw");
    let s2_path = dir.path().join("shard2.raw");

    let raw0 = format_shard(&s0_path, shard_size);
    let raw1 = format_shard(&s1_path, shard_size);
    let raw2 = format_shard(&s2_path, shard_size);

    let raw_stores: Vec<Arc<RawObjectStore>> = vec![raw0, raw1, raw2];
    let (cluster, bus) = build_cluster_with_events(&raw_stores, replicas);
    let mut event_rx = bus.subscribe();

    assert_eq!(cluster.shard_count(), 3);
    assert_eq!(cluster.replication_factor(), 2);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // ── 2. Put several objects ──────────────────────────────────────
    let files: Vec<(&str, &[u8])> = vec![
        ("table/_versions/1.manifest", b"manifest-v1"),
        ("table/data/00000.db", &[0xAA; 4096]),
        ("table/data/00001.db", &[0xBB; 4096]),
        ("table/_transactions/0-uuid.txn", b"txn-data"),
    ];

    rt.block_on(async {
        for (key, data) in &files {
            cluster
                .put(&Path::from(*key), PutPayload::from(Bytes::copy_from_slice(data)))
                .await
                .unwrap();
        }
    });
    flush_all(&raw_stores);

    // Verify 4 PUT events from section 2
    let put_events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&put_events), 4, "section 2 should produce 4 PUT events");

    // Verify placement: each object should be on `replicas` shards
    for (key, _) in &files {
        let entry = cluster.placement(key).expect("missing placement");
        assert_eq!(
            entry.shards.len(),
            replicas,
            "object {} should be on {} shards",
            key,
            replicas
        );
    }

    // ── 3. List objects ─────────────────────────────────────────────
    let listed = rt.block_on(async {
        let items: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        items
    });
    assert_eq!(listed.len(), files.len(), "list should return all objects");

    // List with prefix
    let data_files = rt.block_on(async {
        let items: Vec<_> = cluster
            .list(Some(&Path::from("table/data")))
            .try_collect()
            .await
            .unwrap();
        items
    });
    assert_eq!(data_files.len(), 2, "prefix list for table/data");

    // ── 4. Get + verify content ─────────────────────────────────────
    rt.block_on(async {
        let result = cluster.get(&Path::from("table/data/00000.db")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.len(), 4096);
        assert!(bytes.iter().all(|&b| b == 0xAA), "content mismatch");
    });

    // ── 5. Head ─────────────────────────────────────────────────────
    rt.block_on(async {
        let meta = cluster.head(&Path::from("table/data/00001.db")).await.unwrap();
        assert_eq!(meta.size as usize, 4096);
    });

    // ── 6. Copy ─────────────────────────────────────────────────────
    rt.block_on(async {
        cluster
            .copy(
                &Path::from("table/data/00000.db"),
                &Path::from("table/data/00002.db"),
            )
            .await
            .unwrap();
    });
    flush_all(&raw_stores);

    // Copy should produce 1 PUT event
    let copy_events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&copy_events), 1, "copy should produce 1 PUT event");
    let copy_keys = put_event_keys(&copy_events);
    assert!(copy_keys.contains("table/data/00002.db"), "copy PUT key mismatch");

    let copy_entry = cluster.placement("table/data/00002.db").expect("copy missing");
    assert_eq!(copy_entry.shards.len(), replicas);

    // Verify copied content
    rt.block_on(async {
        let result = cluster.get(&Path::from("table/data/00002.db")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.len(), 4096);
        assert!(bytes.iter().all(|&b| b == 0xAA));
    });

    // ── 7. Delete ───────────────────────────────────────────────────
    rt.block_on(async {
        cluster
            .delete(&Path::from("table/_transactions/0-uuid.txn"))
            .await
            .unwrap();
    });
    flush_all(&raw_stores);

    // Delete should produce 1 DELETE event
    let del_events = drain_events(&mut event_rx);
    assert_eq!(count_delete_events(&del_events), 1, "delete should produce 1 DELETE event");

    assert!(
        cluster.placement("table/_transactions/0-uuid.txn").is_none(),
        "deleted object should be gone from catalog"
    );

    // List should now have 4 objects (3 original minus 1 deleted + 1 copy)
    let after_delete = rt.block_on(async {
        let items: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        items
    });
    assert_eq!(after_delete.len(), 4);

    // ── 8. Verify (per-shard integrity) ─────────────────────────────
    for (i, store) in raw_stores.iter().enumerate() {
        let report = store.verify_all();
        assert!(
            report.errors.is_empty(),
            "shard {} has verify errors: {:?}",
            i,
            report.errors.len()
        );
        assert!(report.free_list_consistent, "shard {} free list inconsistent", i);
        assert!(report.space_accounted, "shard {} space not accounted", i);
    }

    // ── 9. Rebuild catalog from scratch ─────────────────────────────
    // Drop the old cluster and raw stores to release flocks
    drop(cluster);
    drop(raw_stores);
    let raw_stores_reopened: Vec<Arc<RawObjectStore>> = vec![
        open_shard(&s0_path),
        open_shard(&s1_path),
        open_shard(&s2_path),
    ];
    let cluster2 = build_cluster(&raw_stores_reopened, replicas);

    let rebuilt_list = rt.block_on(async {
        let items: Vec<_> = cluster2.list(None).try_collect().await.unwrap();
        items
    });
    assert_eq!(
        rebuilt_list.len(),
        4,
        "rebuilt catalog should have 4 objects"
    );

    // ── 10. Add a 4th shard ─────────────────────────────────────────
    // Drop previous stores to release flocks before reopening
    drop(cluster2);
    drop(raw_stores_reopened);
    let s3_path = dir.path().join("shard3.raw");
    let raw3 = format_shard(&s3_path, shard_size);

    let raw_4shards: Vec<Arc<RawObjectStore>> = vec![
        open_shard(&s0_path),
        open_shard(&s1_path),
        open_shard(&s2_path),
        raw3,
    ];
    let cluster3 = build_cluster(&raw_4shards, replicas);
    assert_eq!(cluster3.shard_count(), 4);

    // Put a new object on the 4-shard cluster
    rt.block_on(async {
        cluster3
            .put(
                &Path::from("table/data/00003.db"),
                PutPayload::from(Bytes::from(vec![0xCC; 2048])),
            )
            .await
            .unwrap();
    });
    flush_all(&raw_4shards);

    let after_add = rt.block_on(async {
        let items: Vec<_> = cluster3.list(None).try_collect().await.unwrap();
        items
    });
    assert_eq!(after_add.len(), 5, "should have 5 objects after add-shard + put");

    // ── 11. Remove shard 2 (drain to survivors) ─────────────────────
    // First collect objects on shard 2
    let shard2_files = rt.block_on(async {
        let items: Vec<_> = raw_4shards[2].list(None).try_collect().await.unwrap();
        items
    });

    // Keep shard2 open for reading, drop others to release flocks
    let shard2_ref = raw_4shards[2].clone();
    drop(cluster3);
    drop(raw_4shards);

    // Build survivor cluster (shards 0, 1, 3)
    let surviving: Vec<Arc<RawObjectStore>> = vec![
        open_shard(&s0_path),
        open_shard(&s1_path),
        open_shard(&s3_path),
    ];
    let survivor_cluster = build_cluster(&surviving, replicas);

    // Drain: for each object on shard 2, check if survivors have it;
    // if not, copy it over.
    rt.block_on(async {
        for meta in &shard2_files {
            let key = meta.location.to_string();
            // Check if survivors already have it
            if survivor_cluster.placement(&key).is_some() {
                continue; // already replicated on survivors
            }
            // Read from victim shard and put to survivor cluster
            let result = shard2_ref.get(&meta.location).await.unwrap();
            let data = result.bytes().await.unwrap();
            survivor_cluster
                .put(&meta.location, PutPayload::from(data))
                .await
                .unwrap();
        }
    });
    flush_all(&surviving);

    // Verify all objects are still accessible on the 3-shard survivor cluster
    let after_remove = rt.block_on(async {
        let items: Vec<_> = survivor_cluster.list(None).try_collect().await.unwrap();
        items
    });
    assert_eq!(
        after_remove.len(),
        5,
        "all 5 objects should survive shard removal"
    );

    // Every object should be readable
    rt.block_on(async {
        for meta in &after_remove {
            let result = survivor_cluster.get(&meta.location).await;
            assert!(
                result.is_ok(),
                "failed to read {} after shard removal",
                meta.location
            );
        }
    });

    // ── 12. Final integrity check on survivors ──────────────────────
    for (i, store) in surviving.iter().enumerate() {
        let report = store.verify_all();
        assert!(
            report.errors.is_empty(),
            "survivor shard {} has errors",
            i
        );
    }
}

// ── PutMode tests ───────────────────────────────────────────────────

#[test]
fn put_mode_create_succeeds_on_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let cluster = build_cluster(&raw, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let opts = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    rt.block_on(async {
        cluster
            .put_opts(
                &Path::from("new.txt"),
                PutPayload::from(Bytes::from_static(b"hello")),
                opts,
            )
            .await
            .unwrap();

        let result = cluster.get(&Path::from("new.txt")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"hello");
    });
}

#[test]
fn put_mode_create_rejects_existing_key() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let cluster = build_cluster(&raw, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // First put succeeds
        cluster
            .put(
                &Path::from("exist.txt"),
                PutPayload::from(Bytes::from_static(b"original")),
            )
            .await
            .unwrap();

        // Second put with Create mode should fail
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let err = cluster
            .put_opts(
                &Path::from("exist.txt"),
                PutPayload::from(Bytes::from_static(b"duplicate")),
                opts,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, object_store::Error::AlreadyExists { .. }),
            "expected AlreadyExists, got: {err}"
        );

        // Original data should be untouched
        let result = cluster.get(&Path::from("exist.txt")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"original");
    });
}

#[test]
fn put_mode_overwrite_replaces_data() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let cluster = build_cluster(&raw, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let opts = PutOptions {
        mode: PutMode::Overwrite,
        ..PutOptions::default()
    };
    rt.block_on(async {
        cluster
            .put(
                &Path::from("ow.txt"),
                PutPayload::from(Bytes::from_static(b"v1")),
            )
            .await
            .unwrap();

        cluster
            .put_opts(
                &Path::from("ow.txt"),
                PutPayload::from(Bytes::from_static(b"v2")),
                opts,
            )
            .await
            .unwrap();

        let result = cluster.get(&Path::from("ow.txt")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"v2");
    });
}

#[test]
fn put_mode_update_without_preconditions_acts_as_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let cluster = build_cluster(&raw, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: None,
            version: None,
        }),
        ..PutOptions::default()
    };
    rt.block_on(async {
        cluster
            .put(
                &Path::from("upd.txt"),
                PutPayload::from(Bytes::from_static(b"old")),
            )
            .await
            .unwrap();

        cluster
            .put_opts(
                &Path::from("upd.txt"),
                PutPayload::from(Bytes::from_static(b"new")),
                opts,
            )
            .await
            .unwrap();

        let result = cluster.get(&Path::from("upd.txt")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"new");
    });
}

#[test]
fn put_mode_update_with_etag_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let cluster = build_cluster(&raw, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some("abc".into()),
            version: None,
        }),
        ..PutOptions::default()
    };
    rt.block_on(async {
        let err = cluster
            .put_opts(
                &Path::from("etag.txt"),
                PutPayload::from(Bytes::from_static(b"data")),
                opts,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, object_store::Error::Precondition { .. }),
            "expected Precondition, got: {err}"
        );
    });
}

#[test]
fn put_mode_update_with_version_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let cluster = build_cluster(&raw, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: None,
            version: Some("v1".into()),
        }),
        ..PutOptions::default()
    };
    rt.block_on(async {
        let err = cluster
            .put_opts(
                &Path::from("ver.txt"),
                PutPayload::from(Bytes::from_static(b"data")),
                opts,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, object_store::Error::Precondition { .. }),
            "expected Precondition, got: {err}"
        );
    });
}

// ── Multipart upload tests ─────────────────────────────────────────

#[test]
fn multipart_upload_single_part() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
    ];
    let (cluster, bus) = build_cluster_with_events(&raw, 2);
    let mut event_rx = bus.subscribe();
    let rt = tokio::runtime::Runtime::new().unwrap();

    let data = vec![0xAA_u8; 8192];
    rt.block_on(async {
        use object_store::MultipartUpload;
        let mut upload = cluster
            .put_multipart(&Path::from("mp/single.bin"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(data.clone())))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        // Verify data
        let result = cluster.get(&Path::from("mp/single.bin")).await.unwrap();
        let got = result.bytes().await.unwrap();
        assert_eq!(got.len(), 8192);
        assert!(got.iter().all(|&b| b == 0xAA));

        // Verify placement in catalog
        let entry = cluster.placement("mp/single.bin").unwrap();
        assert_eq!(entry.shards.len(), 2, "should be replicated to 2 shards");
        assert!(entry.crc32c.is_some(), "CRC should be recorded");
    });

    // Multipart complete should produce 1 PUT event
    let events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&events), 1, "multipart complete should produce 1 PUT event");

    flush_all(&raw);
}

#[test]
fn multipart_upload_multiple_parts() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let part1 = vec![0x11_u8; 4096];
    let part2 = vec![0x22_u8; 4096];
    let part3 = vec![0x33_u8; 2048];

    rt.block_on(async {
        use object_store::MultipartUpload;
        let mut upload = cluster
            .put_multipart(&Path::from("mp/multi.bin"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(part1.clone())))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(part2.clone())))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(part3.clone())))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        // Verify assembled content
        let result = cluster.get(&Path::from("mp/multi.bin")).await.unwrap();
        let got = result.bytes().await.unwrap();
        assert_eq!(got.len(), 4096 + 4096 + 2048);
        assert!(got[..4096].iter().all(|&b| b == 0x11));
        assert!(got[4096..8192].iter().all(|&b| b == 0x22));
        assert!(got[8192..].iter().all(|&b| b == 0x33));

        // Verify catalog entry
        let entry = cluster.placement("mp/multi.bin").unwrap();
        assert_eq!(entry.size, 4096 + 4096 + 2048);
    });
    flush_all(&raw);
}

#[test]
fn multipart_upload_abort() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let (cluster, bus) = build_cluster_with_events(&raw, 1);
    let mut event_rx = bus.subscribe();
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        use object_store::MultipartUpload;
        let mut upload = cluster
            .put_multipart(&Path::from("mp/aborted.bin"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xFF_u8; 1024])))
            .await
            .unwrap();
        upload.abort().await.unwrap();

        // Object should NOT exist in catalog
        assert!(cluster.placement("mp/aborted.bin").is_none());
    });

    // Aborted multipart should NOT produce any PUT event
    let events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&events), 0, "aborted multipart should produce 0 PUT events");
}

// ── set_shard_health tests ──────────────────────────────────────────

#[test]
fn set_shard_health_basic() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
    ];
    let stores: Vec<std::sync::Arc<dyn ObjectStore>> =
        raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 1);

    // Initially all healthy
    assert_eq!(cluster.shard_health(0), Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Healthy));

    // Set shard 0 to Degraded
    let prev = cluster.set_shard_health(0, ShardHealth::Degraded);
    assert_eq!(prev, Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_health(0), Some(ShardHealth::Degraded));

    // Set shard 1 to Offline
    let prev = cluster.set_shard_health(1, ShardHealth::Offline);
    assert_eq!(prev, Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Offline));

    // Out-of-range returns None
    assert_eq!(cluster.set_shard_health(99, ShardHealth::Degraded), None);
}

#[test]
fn set_shard_health_offline_excludes_from_reads() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let stores: Vec<std::sync::Arc<dyn ObjectStore>> =
        raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 2);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put an object (replicated to 2 shards)
    rt.block_on(async {
        cluster
            .put(
                &Path::from("health.txt"),
                PutPayload::from(Bytes::from_static(b"healthy-data")),
            )
            .await
            .unwrap();
    });
    flush_all(&raw);

    let entry = cluster.placement("health.txt").unwrap();
    let first_shard = entry.shards[0];

    // Mark the first shard offline
    cluster.set_shard_health(first_shard, ShardHealth::Offline);

    // Read should still succeed from the other replica
    rt.block_on(async {
        let result = cluster.get(&Path::from("health.txt")).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(&data[..], b"healthy-data");
    });
}

// ── Catalog bulk operations tests ───────────────────────────────────

#[test]
fn catalog_entries_for_shard() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put several objects
    rt.block_on(async {
        for i in 0..10 {
            cluster
                .put(
                    &Path::from(format!("obj/{i}.dat")),
                    PutPayload::from(Bytes::from(vec![i as u8; 100])),
                )
                .await
                .unwrap();
        }
    });

    // Check entries_for_shard returns correct results
    for shard_id in 0..3 {
        let entries = cluster.catalog().entries_for_shard(shard_id);
        for (key, entry) in &entries {
            assert!(
                entry.shards.contains(&shard_id),
                "entry for {} should reference shard {}",
                key,
                shard_id
            );
        }
    }
}

#[test]
fn catalog_remove_all_for_shard() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        for i in 0..10 {
            cluster
                .put(
                    &Path::from(format!("rm/{i}.dat")),
                    PutPayload::from(Bytes::from(vec![i as u8; 100])),
                )
                .await
                .unwrap();
        }
    });

    let before = cluster.catalog().len();
    let entries_on_0 = cluster.catalog().entries_for_shard(0);
    let affected = cluster.catalog().remove_all_for_shard(0);

    // Affected should equal number of entries that referenced shard 0
    assert_eq!(affected, entries_on_0.len());

    // No entries should reference shard 0 anymore
    let after_entries = cluster.catalog().entries_for_shard(0);
    assert!(after_entries.is_empty(), "shard 0 should have no entries after removal");

    // Objects that were ONLY on shard 0 should be gone from catalog
    let after = cluster.catalog().len();
    assert!(after <= before, "catalog should not have grown");
}

// ── invalidate_shard tests ──────────────────────────────────────────

#[test]
fn invalidate_shard_basic() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let stores: Vec<std::sync::Arc<dyn ObjectStore>> =
        raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 2);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects
    rt.block_on(async {
        for i in 0..20 {
            cluster
                .put(
                    &Path::from(format!("inv/{i}.dat")),
                    PutPayload::from(Bytes::from(vec![i as u8; 256])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raw);

    // Invalidate shard 0
    let report = rt.block_on(async {
        cluster.invalidate_shard(0).await.unwrap()
    });

    assert_eq!(report.shard_id, 0);
    assert!(report.scan_ok, "scan should succeed");
    assert!(report.entries_purged > 0, "should have purged some entries");
    assert!(report.entries_restored > 0, "should have restored entries from re-scan");
    assert!(
        report.missing_keys.is_empty(),
        "no keys should be missing since shard data is intact"
    );

    // Shard should be Healthy again after successful invalidation
    assert_eq!(cluster.shard_health(0), Some(ShardHealth::Healthy));

    // All objects should still be readable
    rt.block_on(async {
        for i in 0..20 {
            let result = cluster.get(&Path::from(format!("inv/{i}.dat"))).await;
            assert!(result.is_ok(), "object inv/{i}.dat should be readable");
        }
    });
}

#[test]
fn invalidate_shard_detects_missing_keys() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
    ];
    let stores: Vec<std::sync::Arc<dyn ObjectStore>> =
        raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 1); // RF=1, no redundancy

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects
    rt.block_on(async {
        for i in 0..10 {
            cluster
                .put(
                    &Path::from(format!("miss/{i}.dat")),
                    PutPayload::from(Bytes::from(vec![i as u8; 128])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raw);

    // Find objects on shard 0 and manually delete one from the underlying store
    let entries_on_0 = cluster.catalog().entries_for_shard(0);
    if let Some((key, _)) = entries_on_0.first() {
        let shard_store = cluster.shard_store(0).unwrap().clone();
        rt.block_on(async {
            shard_store.delete(&Path::from(key.as_str())).await.unwrap();
        });
        raw[0].flush_index().unwrap();

        // Invalidate shard 0
        let report = rt.block_on(async {
            cluster.invalidate_shard(0).await.unwrap()
        });

        assert!(report.scan_ok, "scan should succeed");
        assert!(
            report.missing_keys.contains(key),
            "the deleted key should appear in missing_keys"
        );
    }
}

#[test]
fn invalidate_shard_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![format_shard(&dir.path().join("s0.raw"), shard_size)];
    let stores: Vec<std::sync::Arc<dyn ObjectStore>> =
        raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, 1);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(async { cluster.invalidate_shard(99).await });
    assert!(result.is_err(), "should error for out-of-range shard ID");
}

// ── rebuild_catalog_for_shard test ──────────────────────────────────

#[test]
fn rebuild_catalog_for_single_shard() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
    ];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects
    rt.block_on(async {
        for i in 0..10 {
            cluster
                .put(
                    &Path::from(format!("rebshard/{i}.dat")),
                    PutPayload::from(Bytes::from(vec![i as u8; 128])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raw);

    // Clear catalog entries for shard 0, then rebuild just shard 0
    let before_count = cluster.catalog().entries_for_shard(0).len();
    cluster.catalog().remove_all_for_shard(0);
    assert!(
        cluster.catalog().entries_for_shard(0).is_empty(),
        "shard 0 entries should be gone"
    );

    let restored = rt.block_on(async {
        cluster.rebuild_catalog_for_shard(0).await.unwrap()
    });

    assert!(restored > 0, "should have restored some entries");
    let after_count = cluster.catalog().entries_for_shard(0).len();
    assert_eq!(
        after_count, before_count,
        "should restore the same number of entries"
    );
}

// ── Degraded startup tests ──────────────────────────────────────────
// Basic degraded startup, find_under_replicated, and detach/reattach
// lifecycle tests are in degraded_e2e.rs. The tests below cover
// attach+replicate and the laptop scenario specifically.

#[test]
fn attach_shard_force_and_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;

    let shard0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    // Start with shard 1 offline
    let stores: Vec<Option<Arc<dyn ObjectStore>>> = vec![
        Some(shard0.clone() as Arc<dyn ObjectStore>),
        None,
    ];

    let cluster = ShardedObjectStore::new_with_offline(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write objects to shard 0
    rt.block_on(async {
        for i in 0..3 {
            cluster
                .put(
                    &Path::from(format!("attach/{i}.dat")),
                    PutPayload::from(Bytes::from(vec![i; 128])),
                )
                .await
                .unwrap();
        }
    });
    shard0.flush_index().unwrap();

    // All under-replicated
    assert_eq!(cluster.find_under_replicated().len(), 3);

    // Now format and attach shard 1
    let shard1 = format_shard(&dir.path().join("s1.raw"), shard_size);
    let count = rt.block_on(async {
        cluster
            .attach_shard(1, shard1.clone() as Arc<dyn ObjectStore>, true)
            .await
            .unwrap()
    });
    assert_eq!(count, 0, "fresh shard has no objects yet");
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Healthy));

    // Replicate under-replicated objects
    let under = cluster.find_under_replicated();
    assert_eq!(under.len(), 3, "still 3 under-replicated before replication");

    for (key, _count) in &under {
        let placement = cluster.placement(key).unwrap();
        let from_shard = placement.shards[0];
        let to_shard = if from_shard == 0 { 1 } else { 0 };
        rt.block_on(async {
            cluster
                .replicate_object(key, from_shard, to_shard, None)
                .await
                .unwrap();
        });
    }
    shard1.flush_index().unwrap();

    // Now everything should be fully replicated
    let still_under = cluster.find_under_replicated();
    assert!(
        still_under.is_empty(),
        "all objects should now be fully replicated, but {} are under-replicated",
        still_under.len()
    );

    // Verify each object has 2 replicas in the catalog
    for (key, _) in &under {
        let placement = cluster.placement(key).unwrap();
        assert_eq!(
            placement.shards.len(),
            2,
            "object {} should be on 2 shards",
            key
        );
    }

    // Verify data integrity: read from shard 1 directly
    for i in 0..3u8 {
        let key = format!("attach/{i}.dat");
        let data = rt.block_on(async {
            let result = cluster
                .shard_store(1)
                .unwrap()
                .get(&Path::from(key.as_str()))
                .await
                .unwrap();
            result.bytes().await.unwrap()
        });
        assert_eq!(&data[..], &vec![i; 128]);
    }
}

#[test]
fn detach_reattach_laptop_scenario() {
    // Simulates: 2 shards, detach one, write to the other, reattach, replicate.
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;

    let s0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    let s1_orig = format_shard(&dir.path().join("s1.raw"), shard_size);
    let raw: Vec<Arc<RawObjectStore>> = vec![s0.clone(), s1_orig.clone()];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write initial objects (both shards online)
    rt.block_on(async {
        cluster
            .put(
                &Path::from("laptop/initial.txt"),
                PutPayload::from(Bytes::from_static(b"initial data")),
            )
            .await
            .unwrap();
    });
    flush_all(&raw);
    assert!(cluster.find_under_replicated().is_empty());

    // "Go offline" -- detach shard 1 (simulating S3 disconnect)
    cluster.detach_shard(1);

    // Write new objects while shard 1 is offline
    rt.block_on(async {
        cluster
            .put(
                &Path::from("laptop/offline_work.txt"),
                PutPayload::from(Bytes::from_static(b"wrote while offline")),
            )
            .await
            .unwrap();
    });
    s0.flush_index().unwrap();

    // New object only on shard 0
    let under = cluster.find_under_replicated();
    assert!(
        under.iter().any(|(k, _)| k == "laptop/offline_work.txt"),
        "new object should be under-replicated"
    );

    // Drop original references to shard 1 to release the flock
    drop(raw);
    drop(s1_orig);

    // "Come back online" -- reattach shard 1
    let s1 = open_shard(&dir.path().join("s1.raw"));
    rt.block_on(async {
        cluster
            .attach_shard(1, s1 as Arc<dyn ObjectStore>, true)
            .await
            .unwrap();
    });

    // Find what needs replication and replicate it
    let under = cluster.find_under_replicated();
    for (key, _count) in &under {
        let placement = cluster.placement(key).unwrap();
        let from_shard = placement.shards[0];
        let to_shard = if from_shard == 0 { 1 } else { 0 };
        rt.block_on(async {
            cluster
                .replicate_object(key, from_shard, to_shard, None)
                .await
                .unwrap();
        });
    }
    s0.flush_index().unwrap();

    // Everything should now be fully replicated
    assert!(
        cluster.find_under_replicated().is_empty(),
        "all objects should be fully replicated after laptop-mode resync"
    );

    // Verify both old and new objects are readable
    let d1 = rt.block_on(async {
        cluster.get(&Path::from("laptop/initial.txt")).await.unwrap().bytes().await.unwrap()
    });
    assert_eq!(&d1[..], b"initial data");

    let d2 = rt.block_on(async {
        cluster.get(&Path::from("laptop/offline_work.txt")).await.unwrap().bytes().await.unwrap()
    });
    assert_eq!(&d2[..], b"wrote while offline");
}

// =====================================================================
// PutMode::Update with no precondition
// =====================================================================

#[test]
fn put_opts_update_no_precondition_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    let raw1 = format_shard(&dir.path().join("s1.raw"), shard_size);
    let raw2 = format_shard(&dir.path().join("s2.raw"), shard_size);
    let raws = vec![raw0, raw1, raw2];
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put initial object
    rt.block_on(async {
        cluster
            .put(
                &Path::from("update/test.bin"),
                PutPayload::from(Bytes::from_static(b"original")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Update with no e_tag/version -- should succeed (delegates to put)
    rt.block_on(async {
        let opts = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: None,
                version: None,
            }),
            ..Default::default()
        };
        let result = cluster
            .put_opts(
                &Path::from("update/test.bin"),
                PutPayload::from(Bytes::from_static(b"updated")),
                opts,
            )
            .await;
        assert!(result.is_ok(), "Update with no precondition should succeed");
    });

    // Verify data is updated
    let data = rt.block_on(async {
        cluster
            .get(&Path::from("update/test.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&data[..], b"updated");
}

#[test]
fn put_opts_update_with_etag_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    let raw1 = format_shard(&dir.path().join("s1.raw"), shard_size);
    let raws = vec![raw0, raw1];
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let opts = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some("some-etag".to_string()),
                version: None,
            }),
            ..Default::default()
        };
        let result = cluster
            .put_opts(
                &Path::from("etag/test.bin"),
                PutPayload::from(Bytes::from_static(b"data")),
                opts,
            )
            .await;
        assert!(result.is_err(), "Update with e_tag precondition should be rejected");
    });
}

// =====================================================================
// Read-only mode
// =====================================================================

#[test]
fn read_only_rejects_writes() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    let raw1 = format_shard(&dir.path().join("s1.raw"), shard_size);
    let raws = vec![raw0, raw1];

    // First seed data in a writable cluster
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(
                &Path::from("ro/data.bin"),
                PutPayload::from(Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Rebuild as read-only
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let ro_cluster = ShardedObjectStore::new(stores, 2).with_read_only(true);
    rt.block_on(ro_cluster.rebuild_catalog()).unwrap();

    assert!(ro_cluster.is_read_only());

    // get should still work
    let data = rt.block_on(async {
        ro_cluster
            .get(&Path::from("ro/data.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&data[..], b"hello");

    // put should fail
    let put_err = rt.block_on(async {
        ro_cluster
            .put(
                &Path::from("ro/new.bin"),
                PutPayload::from(Bytes::from_static(b"fail")),
            )
            .await
    });
    assert!(put_err.is_err(), "put should fail in read-only mode");

    // delete should fail
    let del_err = rt.block_on(async { ro_cluster.delete(&Path::from("ro/data.bin")).await });
    assert!(del_err.is_err(), "delete should fail in read-only mode");

    // copy should fail
    let copy_err = rt.block_on(async {
        ro_cluster
            .copy(&Path::from("ro/data.bin"), &Path::from("ro/copy.bin"))
            .await
    });
    assert!(copy_err.is_err(), "copy should fail in read-only mode");

    // rename should fail
    let rename_err = rt.block_on(async {
        ro_cluster
            .rename(&Path::from("ro/data.bin"), &Path::from("ro/renamed.bin"))
            .await
    });
    assert!(rename_err.is_err(), "rename should fail in read-only mode");

    // put_multipart should fail
    let mp_err =
        rt.block_on(async { ro_cluster.put_multipart(&Path::from("ro/mp.bin")).await });
    assert!(mp_err.is_err(), "put_multipart should fail in read-only mode");
}

// =====================================================================
// Read preference
// =====================================================================

#[test]
fn read_preference_roundrobin_and_ordered() {
    use shardedobjstr::ReadPreference;

    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    let raw1 = format_shard(&dir.path().join("s1.raw"), shard_size);
    let raws = vec![raw0, raw1];
    let cluster = build_cluster(&raws, 2);

    // Default is RoundRobin
    assert!(matches!(cluster.read_preference(), ReadPreference::RoundRobin));

    // Switch to Ordered
    cluster.set_read_preference(ReadPreference::Ordered);
    assert!(matches!(cluster.read_preference(), ReadPreference::Ordered));

    // Switch back to RoundRobin
    cluster.set_read_preference(ReadPreference::RoundRobin);
    assert!(matches!(cluster.read_preference(), ReadPreference::RoundRobin));

    // Reads should work in both modes
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(
                &Path::from("pref/obj.bin"),
                PutPayload::from(Bytes::from_static(b"data")),
            )
            .await
            .unwrap();
    });

    // Read in Ordered mode
    cluster.set_read_preference(ReadPreference::Ordered);
    let d1 = rt.block_on(async {
        cluster
            .get(&Path::from("pref/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&d1[..], b"data");

    // Read in RoundRobin mode
    cluster.set_read_preference(ReadPreference::RoundRobin);
    let d2 = rt.block_on(async {
        cluster
            .get(&Path::from("pref/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&d2[..], b"data");
}

// ── Multipart upload tracking / expiry tests ───────────────────────

#[test]
fn multipart_upload_tracked_and_completed() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    assert_eq!(cluster.multipart_upload_count(), 0);

    rt.block_on(async {
        use object_store::MultipartUpload;
        let mut upload = cluster
            .put_multipart(&Path::from("mp/test.bin"))
            .await
            .unwrap();
        assert_eq!(cluster.multipart_upload_count(), 1);

        let uploads = cluster.list_multipart_uploads();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].1, "mp/test.bin");

        // Complete the upload.
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xCCu8; 4096])))
            .await
            .unwrap();
        upload.complete().await.unwrap();
    });

    // After completion, the upload should be deregistered.
    assert_eq!(cluster.multipart_upload_count(), 0);
    flush_all(&raw);
}

#[test]
fn multipart_upload_tracked_and_aborted() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raw = vec![
        format_shard(&dir.path().join("s0.raw"), shard_size),
        format_shard(&dir.path().join("s1.raw"), shard_size),
        format_shard(&dir.path().join("s2.raw"), shard_size),
    ];
    let cluster = build_cluster(&raw, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        use object_store::MultipartUpload;
        let mut upload = cluster
            .put_multipart(&Path::from("mp/abort.bin"))
            .await
            .unwrap();
        assert_eq!(cluster.multipart_upload_count(), 1);

        upload.abort().await.unwrap();
    });

    assert_eq!(cluster.multipart_upload_count(), 0);
}

#[test]
fn purge_stale_multiparts_respects_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("shard0.raw"), shard_size);
    let s1 = format_shard(&dir.path().join("shard1.raw"), shard_size);
    let raws = vec![s0, s1];
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();

    // Set a very short expiry (1 second).
    let cluster = ShardedObjectStore::new(stores, 1)
        .with_multipart_expiry(std::time::Duration::from_secs(1));

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let _upload = cluster
            .put_multipart(&Path::from("mp/stale.bin"))
            .await
            .unwrap();
        // Upload is "leaked" (not completed or aborted).
        assert_eq!(cluster.multipart_upload_count(), 1);

        // Purge immediately -- should not purge (not yet stale).
        let purged = cluster.purge_stale_multiparts().await;
        assert_eq!(purged, 0);
        assert_eq!(cluster.multipart_upload_count(), 1);

        // Wait for expiry.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Now purge -- should remove the stale upload.
        let purged = cluster.purge_stale_multiparts().await;
        assert_eq!(purged, 1);
        assert_eq!(cluster.multipart_upload_count(), 0);
    });
}

// =====================================================================
// rename_if_not_exists
// =====================================================================

#[test]
fn rename_if_not_exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("rename/src.bin"),
                PutPayload::from(Bytes::from(vec![0xAAu8; 4096])),
            )
            .await
            .unwrap();

        // Rename src -> dst
        cluster
            .rename_if_not_exists(&Path::from("rename/src.bin"), &Path::from("rename/dst.bin"))
            .await
            .unwrap();

        // dst should exist with same content
        let data = cluster
            .get(&Path::from("rename/dst.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0xAA));

        // src should be gone
        let src = cluster.get(&Path::from("rename/src.bin")).await;
        assert!(src.is_err(), "source should not exist after rename");
    });
}

#[test]
fn rename_if_not_exists_fails_when_dest_exists() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("rename/a.bin"),
                PutPayload::from(Bytes::from_static(b"aaa")),
            )
            .await
            .unwrap();
        cluster
            .put(
                &Path::from("rename/b.bin"),
                PutPayload::from(Bytes::from_static(b"bbb")),
            )
            .await
            .unwrap();

        // Rename a -> b should fail because b already exists
        let err = cluster
            .rename_if_not_exists(&Path::from("rename/a.bin"), &Path::from("rename/b.bin"))
            .await;
        assert!(err.is_err(), "rename should fail when destination exists");

        // Both originals should be intact
        let a = cluster
            .get(&Path::from("rename/a.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&a[..], b"aaa");
    });
}

#[test]
fn rename_if_not_exists_updates_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("rename/cat.bin"),
                PutPayload::from(Bytes::from(vec![0xCCu8; 512])),
            )
            .await
            .unwrap();

        cluster
            .rename_if_not_exists(
                &Path::from("rename/cat.bin"),
                &Path::from("rename/cat_moved.bin"),
            )
            .await
            .unwrap();

        // Source removed from catalog
        assert!(
            cluster.placement("rename/cat.bin").is_none(),
            "source key should be removed from catalog"
        );
        // Destination present in catalog with correct replica count
        let entry = cluster
            .placement("rename/cat_moved.bin")
            .expect("destination should be in catalog");
        assert_eq!(entry.shards.len(), 2, "destination should have RF=2 replicas");
    });
}

// =====================================================================
// Free-space placement preference
// =====================================================================

#[test]
fn free_space_affects_placement() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 1); // RF=1 so placement is deterministic
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Set shard 2 as the emptiest by far
    cluster.set_shard_free_space(0, 1_000);
    cluster.set_shard_free_space(1, 1_000);
    cluster.set_shard_free_space(2, 100_000_000);
    cluster.set_shard_free_space(3, 1_000);

    // Write many objects -- most should land on shard 2
    let n = 20;
    rt.block_on(async {
        for i in 0..n {
            let key = format!("freespace/obj_{i:03}.bin");
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![0xDD; 256])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    // Count placements per shard
    let mut counts = [0usize; 4];
    for i in 0..n {
        let key = format!("freespace/obj_{i:03}.bin");
        if let Some(entry) = cluster.placement(&key) {
            for &sid in &entry.shards {
                counts[sid] += 1;
            }
        }
    }

    // Shard 2 should have the most objects
    assert!(
        counts[2] > counts[0] && counts[2] > counts[1] && counts[2] > counts[3],
        "emptiest shard (2) should get most objects: counts={counts:?}"
    );
}

// =====================================================================
// PutMode::Create concurrent -- exactly one wins
// =====================================================================

#[test]
fn create_mode_concurrent_exactly_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = Arc::new(build_cluster(&raws, 2));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut handles = tokio::task::JoinSet::new();
        let key = Path::from("race/create-key");

        for i in 0..10u8 {
            let c = Arc::clone(&cluster);
            let k = key.clone();
            handles.spawn(async move {
                let data = vec![i; 4096];
                let opts = PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                };
                c.put_opts(&k, PutPayload::from(Bytes::from(data)), opts).await
            });
        }

        let mut successes = 0usize;
        let mut already_exists = 0usize;
        while let Some(result) = handles.join_next().await {
            match result.unwrap() {
                Ok(_) => successes += 1,
                Err(object_store::Error::AlreadyExists { .. }) => already_exists += 1,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(successes, 1, "exactly one Create should succeed");
        assert_eq!(already_exists, 9, "the rest should get AlreadyExists");

        let got = cluster.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.len(), 4096);
    });
}

// =====================================================================
// list_with_delimiter basic test
// =====================================================================

#[test]
fn list_with_delimiter_returns_prefixes_and_objects() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Create objects at multiple levels:
        //   data/a.bin, data/b.bin, data/sub/c.bin, root.bin
        for (key, fill) in [
            ("data/a.bin", 0xAAu8),
            ("data/b.bin", 0xBB),
            ("data/sub/c.bin", 0xCC),
            ("root.bin", 0xDD),
        ] {
            cluster
                .put(
                    &Path::from(key),
                    PutPayload::from(Bytes::from(vec![fill; 512])),
                )
                .await
                .unwrap();
        }
    });
    flush_all(&raws);

    rt.block_on(async {
        // List at "data" prefix -- should return objects a.bin, b.bin
        // and common prefix "data/sub".
        let result = cluster
            .list_with_delimiter(Some(&Path::from("data")))
            .await
            .unwrap();
        let names: Vec<_> = result.objects.iter().map(|o| o.location.as_ref().to_string()).collect();
        assert!(names.contains(&"data/a.bin".to_string()), "should contain data/a.bin");
        assert!(names.contains(&"data/b.bin".to_string()), "should contain data/b.bin");
        assert!(!names.contains(&"data/sub/c.bin".to_string()), "sub/c.bin should be under prefix, not listed as object");
        let prefixes: Vec<_> = result.common_prefixes.iter().map(|p| p.as_ref().to_string()).collect();
        assert!(prefixes.contains(&"data/sub".to_string()), "should have data/sub prefix");

        // List at root -- should return root.bin and "data" prefix.
        let root_result = cluster
            .list_with_delimiter(None)
            .await
            .unwrap();
        let root_names: Vec<_> = root_result.objects.iter().map(|o| o.location.as_ref().to_string()).collect();
        assert!(root_names.contains(&"root.bin".to_string()), "should contain root.bin");
        let root_prefixes: Vec<_> = root_result.common_prefixes.iter().map(|p| p.as_ref().to_string()).collect();
        assert!(root_prefixes.contains(&"data".to_string()), "should have data prefix");
    });
}

// =====================================================================
// get_opts: conditional headers (if_none_match, if_match, if_modified_since)
// =====================================================================

#[test]
fn get_opts_if_none_match_returns_not_modified() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("cond{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Put an object.
        cluster
            .put(
                &Path::from("cond/test.bin"),
                PutPayload::from(Bytes::from_static(b"conditional-data")),
            )
            .await
            .unwrap();

        // Head to get the etag.
        let meta = cluster.head(&Path::from("cond/test.bin")).await.unwrap();
        let etag = meta.e_tag.clone();

        // get_opts with matching etag in if_none_match should return 304.
        if let Some(ref tag) = etag {
            let opts = GetOptions {
                if_none_match: Some(tag.clone()),
                ..GetOptions::default()
            };
            let result = cluster
                .get_opts(&Path::from("cond/test.bin"), opts)
                .await;
            assert!(
                result.is_err(),
                "get_opts with matching if_none_match should fail (304)"
            );
        }

        // get_opts with non-matching if_none_match should succeed.
        let opts = GetOptions {
            if_none_match: Some("\"bogus-etag\"".to_string()),
            ..GetOptions::default()
        };
        let result = cluster
            .get_opts(&Path::from("cond/test.bin"), opts)
            .await;
        assert!(
            result.is_ok(),
            "get_opts with non-matching if_none_match should succeed: {:?}",
            result.err()
        );
    });
}

#[test]
fn get_opts_if_match_with_wrong_etag_fails() {
    // Use InMemory stores because RawObjectStore ignores conditional headers.
    let mem0 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let mem1 = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let cluster = ShardedObjectStore::new(vec![mem0, mem1], 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster.rebuild_catalog().await.unwrap();
        cluster
            .put(
                &Path::from("ifm/test.bin"),
                PutPayload::from(Bytes::from_static(b"match-data")),
            )
            .await
            .unwrap();

        // if_match with wrong etag should fail.
        let opts = GetOptions {
            if_match: Some("\"wrong-etag\"".to_string()),
            ..GetOptions::default()
        };
        let result = cluster
            .get_opts(&Path::from("ifm/test.bin"), opts)
            .await;
        assert!(
            result.is_err(),
            "get_opts with wrong if_match should fail"
        );
    });
}

#[test]
fn get_opts_if_match_with_correct_etag_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("ifc{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("ifc/test.bin"),
                PutPayload::from(Bytes::from_static(b"correct-match")),
            )
            .await
            .unwrap();

        let meta = cluster.head(&Path::from("ifc/test.bin")).await.unwrap();
        if let Some(ref tag) = meta.e_tag {
            let opts = GetOptions {
                if_match: Some(tag.clone()),
                ..GetOptions::default()
            };
            let result = cluster
                .get_opts(&Path::from("ifc/test.bin"), opts)
                .await;
            assert!(
                result.is_ok(),
                "get_opts with correct if_match should succeed: {:?}",
                result.err()
            );
            let bytes = result.unwrap().bytes().await.unwrap();
            assert_eq!(&bytes[..], b"correct-match");
        }
    });
}

#[test]
fn get_opts_if_modified_since_old_date_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<_> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("ims{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("ims/test.bin"),
                PutPayload::from(Bytes::from_static(b"date-test")),
            )
            .await
            .unwrap();

        // if_modified_since an old date should return the object.
        let old_date = chrono::Utc::now() - chrono::Duration::days(365);
        let opts = GetOptions {
            if_modified_since: Some(old_date.into()),
            ..GetOptions::default()
        };
        let result = cluster
            .get_opts(&Path::from("ims/test.bin"), opts)
            .await;
        assert!(
            result.is_ok(),
            "get_opts with old if_modified_since should succeed: {:?}",
            result.err()
        );
    });
}

// =====================================================================
// RF exceeds shard count -- should clamp
// =====================================================================

#[test]
fn rf_exceeds_shard_count() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    // RF=3 but only 2 shards -- should clamp internally.
    let cluster = build_cluster(&raws, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // RF is clamped to shard count: min(3, 2) = 2.
    assert_eq!(
        cluster.replication_factor(), 2,
        "replication_factor() should clamp to shard count"
    );

    // min_writes should be capped to available shards.
    assert!(
        cluster.min_writes() <= 2,
        "min_writes should not exceed shard count"
    );

    // Put should succeed with data on both shards.
    rt.block_on(async {
        let data = Bytes::from(vec![0xDD; 512]);
        cluster
            .put(&Path::from("rf-test.bin"), PutPayload::from(data))
            .await
            .unwrap();
    });
    flush_all(&raws);

    let entry = cluster.placement("rf-test.bin").unwrap();
    assert_eq!(
        entry.shards.len(), 2,
        "object should be placed on all 2 available shards"
    );
}
