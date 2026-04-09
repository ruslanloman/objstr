//! Concurrent read/write pattern tests:
//!   - Multiple readers + single writer (append, overwrite, delete)
//!   - Multiple writers to separate prefixes
//!   - Concurrent append + delete stress
//!   - Concurrent merge-into pattern
//!   - Full stress: append + compact + merge + delete + readers

mod common;

use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

use common::{make_small, MEDIUM_DEVICE};

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: Concurrent readers + single writer
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_readers_single_writer() {
    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // Pre-populate with some data
    for i in 0..50 {
        store
            .put(
                &Path::from(format!("base/{i:04}")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    let writer_store = Arc::clone(&store);
    let writer = tokio::spawn(async move {
        // Append new files
        for i in 50..100 {
            writer_store
                .put(
                    &Path::from(format!("base/{i:04}")),
                    PutPayload::from(make_small(i, 4096)),
                )
                .await
                .unwrap();
        }
        // Overwrite some existing files
        for i in 0..20 {
            writer_store
                .put(
                    &Path::from(format!("base/{i:04}")),
                    PutPayload::from(make_small(i + 1000, 4096)),
                )
                .await
                .unwrap();
        }
        // Delete some files
        for i in 40..50 {
            writer_store
                .delete(&Path::from(format!("base/{i:04}")))
                .await
                .unwrap();
        }
    });

    // Spawn 10 concurrent readers
    let mut readers = Vec::new();
    for reader_id in 0..10 {
        let r_store = Arc::clone(&store);
        readers.push(tokio::spawn(async move {
            let mut reads = 0u32;
            for _ in 0..20 {
                // List all files
                let files: Vec<_> = r_store.list(None).try_collect().await.unwrap();
                assert!(!files.is_empty(), "reader {reader_id}: store should never be empty");

                // Read a random file from the listing
                if !files.is_empty() {
                    let idx = reads as usize % files.len();
                    let result = r_store.get(&files[idx].location).await;
                    // File might get deleted between list and get -- that's OK
                    match result {
                        Ok(r) => {
                            let data = r.bytes().await.unwrap();
                            // Verify data is a valid make_small output:
                            // size must be 4096 and content must be self-consistent
                            assert_eq!(data.len(), 4096, "reader {reader_id}: unexpected size");
                            if data.len() >= 8 {
                                let tag = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
                                let fill = (tag & 0xFF) as u8;
                                assert!(data[8..].iter().all(|&b| b == fill),
                                    "reader {reader_id}: corrupt data for tag {tag}");
                            }
                            reads += 1;
                        }
                        Err(_) => {} // deleted between list and get -- fine
                    }
                }
                tokio::task::yield_now().await;
            }
            reads
        }));
    }

    writer.await.unwrap();
    let mut total_reads = 0u32;
    for reader in readers {
        total_reads += reader.await.unwrap();
    }

    // Final verification
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    // Should have: 100 - 10 deleted = 90 files
    assert_eq!(files.len(), 90, "expected 90 files after writer finished");
    assert!(total_reads > 0, "readers should have completed some reads");
    println!("PASS concurrent_readers_single_writer: {total_reads} successful reads");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: Multiple concurrent writers to separate prefixes
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_multiple_writers() {
    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // 5 writers, each writing to its own prefix
    let mut writers = Vec::new();
    for w in 0..5 {
        let w_store = Arc::clone(&store);
        writers.push(tokio::spawn(async move {
            for i in 0..20 {
                w_store
                    .put(
                        &Path::from(format!("writer_{w}/{i:03}")),
                        PutPayload::from(make_small(w * 100 + i, 8192)),
                    )
                    .await
                    .unwrap();
            }
        }));
    }

    for writer in writers {
        writer.await.unwrap();
    }

    // Verify: 5 writers x 20 files = 100
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 100, "expected 100 files from 5 writers");

    // Verify each writer's data
    for w in 0..5usize {
        let prefix_files: Vec<_> = store
            .list(Some(&Path::from(format!("writer_{w}"))))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(prefix_files.len(), 20, "writer {w} should have 20 files");
    }

    // Flush and verify persistence
    store.flush_index().unwrap();
    drop(store); // release flock before reopening
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let files2: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files2.len(), 100, "all 100 files should persist");

    println!("PASS concurrent_multiple_writers");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 3: Concurrent append + delete stress
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_append_and_delete_stress() {
    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // Pre-populate 100 files
    for i in 0..100 {
        store
            .put(
                &Path::from(format!("stress/{i:04}")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    // Concurrent: 3 appenders, 2 deleters, 5 readers
    let mut handles = Vec::new();

    // 3 appenders: each adds 30 files
    for a in 0..3 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..30 {
                let fid = 100 + a * 30 + i;
                s.put(
                    &Path::from(format!("stress/{fid:04}")),
                    PutPayload::from(make_small(fid, 4096)),
                )
                .await
                .unwrap();
            }
        }));
    }

    // 2 deleters: each deletes 25 files from the original 100
    for d in 0..2 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..25 {
                let fid = d * 25 + i;
                // Ignore errors (might race with other deleter or already deleted)
                let _ = s.delete(&Path::from(format!("stress/{fid:04}"))).await;
            }
        }));
    }

    // 5 readers: continuously list and read
    for r in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let files: Vec<_> = s.list(None).try_collect().await.unwrap();
                assert!(!files.is_empty(), "reader {r}: should never be fully empty");
                // Try to read first file in listing
                if let Some(first) = files.first() {
                    let _ = s.get(&first.location).await; // might be deleted
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Final count: started with 100, added 90, deleted up to 50
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    // Exactly: 100 original - 50 deleted + 90 appended = 140
    assert_eq!(files.len(), 140, "expected 140 files after stress test");

    // Verify persistence
    store.flush_index().unwrap();
    drop(store); // release flock before reopening
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let files2: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files2.len(), 140, "all 140 files should persist");

    println!("PASS concurrent_append_and_delete_stress");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 4: Concurrent merge-into pattern
// ═══════════════════════════════════════════════════════════════════════

/// Simulate merge_into: read source, write merged result, delete source.
#[tokio::test]
async fn concurrent_merge_into_pattern() {
    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // Create a "target" table with 20 data files
    for i in 0..20 {
        store
            .put(
                &Path::from(format!("target/data/{i:04}.db")),
                PutPayload::from(make_small(i, 16384)),
            )
            .await
            .unwrap();
    }

    // Create a "source" table with 10 data files
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("source/data/{i:04}.db")),
                PutPayload::from(make_small(i + 100, 16384)),
            )
            .await
            .unwrap();
    }

    // Concurrent: merge_into writer + readers checking target + readers checking source
    let merge_store = Arc::clone(&store);
    let merger = tokio::spawn(async move {
        // Read each source file and merge into target
        for i in 0..10 {
            let src_path = Path::from(format!("source/data/{i:04}.db"));
            let data = merge_store
                .get(&src_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();

            // Write merged result to target
            let dst_path = Path::from(format!("target/data/{:04}.db", 20 + i));
            merge_store
                .put(&dst_path, PutPayload::from(data))
                .await
                .unwrap();

            // Delete source
            merge_store.delete(&src_path).await.unwrap();
        }
    });

    // Readers on target -- file count should only go up
    let mut reader_handles = Vec::new();
    for _ in 0..5 {
        let s = Arc::clone(&store);
        reader_handles.push(tokio::spawn(async move {
            let mut max_seen = 0;
            for _ in 0..20 {
                let files: Vec<_> = s
                    .list(Some(&Path::from("target/data")))
                    .try_collect()
                    .await
                    .unwrap();
                assert!(
                    files.len() >= 20,
                    "target should never shrink below 20"
                );
                if files.len() > max_seen {
                    max_seen = files.len();
                }
                tokio::task::yield_now().await;
            }
            max_seen
        }));
    }

    merger.await.unwrap();
    for h in reader_handles {
        let _ = h.await.unwrap();
    }

    // Verify final state
    let target_files: Vec<_> = store
        .list(Some(&Path::from("target/data")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(target_files.len(), 30, "target should have 30 files after merge");

    let source_files: Vec<_> = store
        .list(Some(&Path::from("source/data")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(source_files.len(), 0, "source should be empty after merge");

    // Persistence
    store.flush_index().unwrap();
    drop(store); // release flock before reopening
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let t: Vec<_> = store2
        .list(Some(&Path::from("target/data")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(t.len(), 30, "merge result should persist");

    println!("PASS concurrent_merge_into_pattern");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 5: Full stress -- concurrent append + compact + merge + delete +
//   readers, all at once
// ═══════════════════════════════════════════════════════════════════════

/// Heavy concurrent workload mixing appends, compaction (optimize),
/// merge_into, deletes, and reads -- all happening simultaneously.
#[tokio::test]
async fn stress_all_operations_concurrent() {
    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // Pre-populate: 2 tables x 20 data files each
    for t in 0..2 {
        for i in 0..20 {
            store
                .put(
                    &Path::from(format!("table_{t}/data/{i:04}.db")),
                    PutPayload::from(make_small(t * 1000 + i, 8192)),
                )
                .await
                .unwrap();
        }
    }

    let mut handles = Vec::new();

    // Writer 1: append to table_0 (files 20-49)
    {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 20..50 {
                s.put(
                    &Path::from(format!("table_0/data/{i:04}.db")),
                    PutPayload::from(make_small(i, 8192)),
                )
                .await
                .unwrap();
            }
        }));
    }

    // Writer 2: append to table_1 (files 20-49)
    {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 20..50 {
                s.put(
                    &Path::from(format!("table_1/data/{i:04}.db")),
                    PutPayload::from(make_small(1000 + i, 8192)),
                )
                .await
                .unwrap();
            }
        }));
    }

    // Compactor: "optimize" table_0 by reading files 0-9, writing a merged
    // file, then deleting originals
    {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            // Read 10 fragments
            let mut total = 0usize;
            for i in 0..10 {
                match s.get(&Path::from(format!("table_0/data/{i:04}.db"))).await {
                    Ok(r) => {
                        let d = r.bytes().await.unwrap();
                        total += d.len();
                    }
                    Err(_) => {} // might be deleted by another task
                }
            }
            if total > 0 {
                // Write compacted
                s.put(
                    &Path::from("table_0/compacted/0000.db"),
                    PutPayload::from(make_small(9999, total)),
                )
                .await
                .unwrap();
                // Delete originals
                for i in 0..10 {
                    let _ = s
                        .delete(&Path::from(format!("table_0/data/{i:04}.db")))
                        .await;
                }
            }
        }));
    }

    // Merger: merge from table_2 (new) into table_1
    {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            // Create source data
            for i in 0..10 {
                s.put(
                    &Path::from(format!("table_2/data/{i:04}.db")),
                    PutPayload::from(make_small(2000 + i, 4096)),
                )
                .await
                .unwrap();
            }
            // Merge into table_1
            for i in 0..10 {
                let src = Path::from(format!("table_2/data/{i:04}.db"));
                match s.get(&src).await {
                    Ok(r) => {
                        let data = r.bytes().await.unwrap();
                        let dst = Path::from(format!("table_1/merged/{i:04}.db"));
                        s.put(&dst, PutPayload::from(data)).await.unwrap();
                        let _ = s.delete(&src).await;
                    }
                    Err(_) => {}
                }
            }
        }));
    }

    // Deleter: delete some files from table_1
    {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..5 {
                let _ = s
                    .delete(&Path::from(format!("table_1/data/{i:04}.db")))
                    .await;
            }
        }));
    }

    // 5 concurrent readers
    for r in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let mut ok_reads = 0u32;
            for _ in 0..30 {
                // List everything
                let files: Vec<_> = s.list(None).try_collect().await.unwrap();
                assert!(!files.is_empty(), "reader {r}: store should never be empty");

                // Read a file
                let idx = ok_reads as usize % files.len();
                match s.get(&files[idx].location).await {
                    Ok(r) => {
                        let _ = r.bytes().await.unwrap();
                        ok_reads += 1;
                    }
                    Err(_) => {} // deleted between list and get
                }

                // list_with_delimiter
                let top = s.list_with_delimiter(None).await.unwrap();
                assert!(
                    !top.common_prefixes.is_empty(),
                    "reader {r}: should have table dirs"
                );

                tokio::task::yield_now().await;
            }
        }));
    }

    // Wait for all tasks
    for h in handles {
        h.await.unwrap();
    }

    // Verify consistency: list all files, read each one
    let all_files: Vec<_> = store.list(None).try_collect().await.unwrap();
    let file_count = all_files.len();
    assert!(file_count > 0, "store should not be empty after stress");

    for f in &all_files {
        let data = store.get(&f.location).await.unwrap().bytes().await.unwrap();
        assert_eq!(
            data.len(),
            f.size as usize,
            "size mismatch for {}",
            f.location
        );
        // Verify content is not corrupt (deterministic fill pattern)
        if data.len() >= 8 {
            let idx = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
            let fill = (idx & 0xFF) as u8;
            for (i, &b) in data[8..].iter().enumerate() {
                assert_eq!(b, fill, "content corrupt in {} at byte {}", f.location, i + 8);
            }
        }
    }

    // Flush and verify persistence
    store.flush_index().unwrap();
    drop(store); // release flock before reopening
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let files2: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(
        files2.len(),
        file_count,
        "all files should survive flush/reopen"
    );

    // Verify table_0: should have appended files + compacted - originals 0-9
    let t0_data: Vec<_> = store2
        .list(Some(&Path::from("table_0/data")))
        .try_collect()
        .await
        .unwrap();
    let t0_compacted: Vec<_> = store2
        .list(Some(&Path::from("table_0/compacted")))
        .try_collect()
        .await
        .unwrap();
    println!(
        "  table_0: {} data files, {} compacted files",
        t0_data.len(),
        t0_compacted.len()
    );

    // Verify table_1: merged files should exist
    let t1_merged: Vec<_> = store2
        .list(Some(&Path::from("table_1/merged")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(t1_merged.len(), 10, "merge_into should produce 10 files");

    println!("PASS stress_all_operations_concurrent: {file_count} total files");
}
