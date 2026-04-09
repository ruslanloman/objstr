//! Crash / power-fail simulation tests:
//!   - Multi-checkpoint recovery (flush cycles, then crash mid-write)
//!   - Crash with deletes and overwrites
//!   - Crash during Lance-style table compaction
//!   - Periodic flush then crash (stress variant)

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

use common::{make_small, MEDIUM_DEVICE};

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: Multiple flush checkpoints, then crash mid-write
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn crash_multi_checkpoint_recovery() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Phase 1: create tables, flush after each batch
    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        // Batch 1: 20 files -> flush (checkpoint 1)
        for i in 0..20 {
            store
                .put(
                    &Path::from(format!("table/data/{i:04}.db")),
                    PutPayload::from(make_small(i, 16384)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();

        // Batch 2: 20 more files -> flush (checkpoint 2)
        for i in 20..40 {
            store
                .put(
                    &Path::from(format!("table/data/{i:04}.db")),
                    PutPayload::from(make_small(i, 16384)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();

        // Batch 3: 20 more files -> flush (checkpoint 3)
        for i in 40..60 {
            store
                .put(
                    &Path::from(format!("table/data/{i:04}.db")),
                    PutPayload::from(make_small(i, 16384)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();

        // Batch 4: write 40 more files but DO NOT flush -> simulate crash
        for i in 60..100 {
            store
                .put(
                    &Path::from(format!("table/data/{i:04}.db")),
                    PutPayload::from(make_small(i, 16384)),
                )
                .await
                .unwrap();
        }
        // store drops here -- simulating kill -9 / power loss
    }

    // Phase 2: reopen -- only checkpoint 3 data (60 files) should exist
    {
        let store = RawObjectStore::open(&path_buf).unwrap();
        let files: Vec<_> = store
            .list(Some(&Path::from("table/data")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            files.len(),
            60,
            "only 60 files from 3 checkpoints should survive (got {})",
            files.len()
        );

        // Verify data integrity of all surviving files
        for i in 0..60 {
            let data = store
                .get(&Path::from(format!("table/data/{i:04}.db")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 16384, "file {i} wrong size");
        }

        // Files 60-99 should NOT exist
        for i in 60..100 {
            assert!(
                store
                    .get(&Path::from(format!("table/data/{i:04}.db")))
                    .await
                    .is_err(),
                "file {i} should not survive crash"
            );
        }
    }

    // Phase 3: reopen should still work, and new writes should succeed
    {
        let store = RawObjectStore::open(&path_buf).unwrap();
        for i in 60..70 {
            store
                .put(
                    &Path::from(format!("table/data/{i:04}.db")),
                    PutPayload::from(make_small(i, 16384)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();
    }

    // Phase 4: verify the new writes persisted
    {
        let store = RawObjectStore::open(&path_buf).unwrap();
        let files: Vec<_> = store
            .list(Some(&Path::from("table/data")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(files.len(), 70, "60 recovered + 10 new should be 70");
    }

    println!("PASS crash_multi_checkpoint_recovery");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: Crash with deletes and overwrites
// ═══════════════════════════════════════════════════════════════════════

/// Crash while tables have been modified in complex ways (create, delete, overwrite).
#[tokio::test]
async fn crash_with_deletes_and_overwrites() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Phase 1: create 50 files, delete 20, overwrite 10, flush
    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        for i in 0..50 {
            store
                .put(
                    &Path::from(format!("data/{i:04}")),
                    PutPayload::from(make_small(i, 4096)),
                )
                .await
                .unwrap();
        }

        // Delete files 30-49
        for i in 30..50 {
            store.delete(&Path::from(format!("data/{i:04}"))).await.unwrap();
        }

        // Overwrite files 0-9 with new data
        for i in 0..10 {
            store
                .put(
                    &Path::from(format!("data/{i:04}")),
                    PutPayload::from(make_small(i + 1000, 8192)), // bigger + different tag
                )
                .await
                .unwrap();
        }

        store.flush_index().unwrap();

        // After flush: 30 files (0-29), files 0-9 are overwritten (8192 bytes)
        // Now add 20 more files and re-delete some WITHOUT flushing
        for i in 50..70 {
            store
                .put(
                    &Path::from(format!("data/{i:04}")),
                    PutPayload::from(make_small(i, 4096)),
                )
                .await
                .unwrap();
        }
        for i in 10..20 {
            store.delete(&Path::from(format!("data/{i:04}"))).await.unwrap();
        }
        // crash -- drop without flush
    }

    // Phase 2: recover -- should see the state at last flush
    {
        let store = RawObjectStore::open(&path_buf).unwrap();
        let files: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(files.len(), 30, "should recover 30 files from last checkpoint");

        // Files 0-9 should be the overwritten versions (8192 bytes)
        for i in 0..10 {
            let data = store
                .get(&Path::from(format!("data/{i:04}")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 8192, "file {i} should be overwritten size");
        }

        // Files 10-29 should be original (4096 bytes)
        for i in 10..30 {
            let data = store
                .get(&Path::from(format!("data/{i:04}")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 4096, "file {i} should be original size");
        }

        // Files 30-49 should be deleted, 50-69 should not exist
        for i in 30..70 {
            assert!(
                store.get(&Path::from(format!("data/{i:04}"))).await.is_err(),
                "file {i} should not exist"
            );
        }
    }

    println!("PASS crash_with_deletes_and_overwrites");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 3: Crash during Lance-style table compaction
// ═══════════════════════════════════════════════════════════════════════

/// Crash simulation with Lance-style table structure.
/// Create tables, add versions, compact, crash, verify recovery.
#[tokio::test]
async fn crash_lance_table_mid_compact() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Phase 1: create table with 10 data files, flush
    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        // Manifest
        store.put(
            &Path::from("metrics/_versions/1.manifest"),
            PutPayload::from(Bytes::from("manifest-v1")),
        ).await.unwrap();
        // Data files
        for i in 0..10usize {
            let path = format!("metrics/data/{i:05}.db");
            let data = make_small(1000 + i, 32768);
            store.put(&Path::from(path), PutPayload::from(data)).await.unwrap();
        }
        // Transaction log
        store.put(
            &Path::from("_transactions/1-metrics.txn"),
            PutPayload::from(Bytes::from("txn-metrics-v1")),
        ).await.unwrap();

        store.flush_index().unwrap();
    }

    // Phase 2: compact (merge 10 files into 1, delete old), then crash mid-way
    {
        let store = RawObjectStore::open(&path_buf).unwrap();

        // Read all old data
        let mut total_size = 0;
        for i in 0..10 {
            let data = store
                .get(&Path::from(format!("metrics/data/{i:05}.db")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            total_size += data.len();
        }

        // Write compacted file
        store
            .put(
                &Path::from("metrics/data/00010.db"),
                PutPayload::from(make_small(2000, total_size)),
            )
            .await
            .unwrap();

        // Delete first 5 old files (partial compaction)
        for i in 0..5 {
            store
                .delete(&Path::from(format!("metrics/data/{i:05}.db")))
                .await
                .unwrap();
        }

        // CRASH -- haven't deleted files 5-9 and haven't flushed
        // The on-disk state still has the pre-compaction snapshot
    }

    // Phase 3: recover -- should see original 10 files (pre-compaction state)
    {
        let store = RawObjectStore::open(&path_buf).unwrap();
        let data_files: Vec<_> = store
            .list(Some(&Path::from("metrics/data")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            data_files.len(),
            10,
            "crash during compaction should revert to pre-compaction state (got {})",
            data_files.len()
        );

        // Verify data integrity of all original files
        for i in 0..10 {
            let data = store
                .get(&Path::from(format!("metrics/data/{i:05}.db")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 32768);
        }

        // The compacted file should NOT exist
        assert!(
            store
                .get(&Path::from("metrics/data/00010.db"))
                .await
                .is_err(),
            "compacted file should not survive crash"
        );
    }

    println!("PASS crash_lance_table_mid_compact");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 4: Periodic flush checkpoints during heavy writes, then crash
// ═══════════════════════════════════════════════════════════════════════

/// Stress test: periodic flush checkpoints during heavy concurrent writes,
/// then crash. Verify recovery to last checkpoint.
#[tokio::test]
async fn stress_periodic_flush_then_crash() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = Arc::new(
            RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
        );

        // Write 100 files in 5 batches, flushing after each batch
        for batch in 0..5 {
            let mut puts = Vec::new();
            for i in 0..20 {
                let s = Arc::clone(&store);
                let fid = batch * 20 + i;
                puts.push(tokio::spawn(async move {
                    s.put(
                        &Path::from(format!("batch/f{fid:04}")),
                        PutPayload::from(make_small(fid, 4096)),
                    )
                    .await
                    .unwrap();
                }));
            }
            for p in puts {
                p.await.unwrap();
            }
            store.flush_index().unwrap();
        }
        // 100 files flushed across 5 checkpoints

        // Now write 50 more without flush (crash)
        for i in 100..150 {
            store
                .put(
                    &Path::from(format!("batch/f{i:04}")),
                    PutPayload::from(make_small(i, 4096)),
                )
                .await
                .unwrap();
        }
        // drop without flush -- explicit drop ensures flock is released
        // before the reopen below (Arc refcount should already be 1 here
        // because all spawned tasks were awaited above)
        drop(store);
    }

    // Recover: should have exactly 100 files
    let store = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(
        files.len(),
        100,
        "should recover 100 flushed files, lost 50 unflushed (got {})",
        files.len()
    );

    // Verify all recovered data is readable and content-correct
    for i in 0..100 {
        let data = store
            .get(&Path::from(format!("batch/f{i:04}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096, "file {i} wrong size");
        // Verify deterministic fill pattern from make_small(i, 4096)
        let fill = (i & 0xFF) as u8;
        if data.len() >= 8 {
            let tag = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
            assert_eq!(tag, i, "file {i} tag mismatch: got {tag}");
        }
        for (j, &b) in data[8..].iter().enumerate() {
            assert_eq!(b, fill, "file {i} content corrupt at byte {}", j + 8);
        }
    }

    println!("PASS stress_periodic_flush_then_crash");
}
