//! Concurrency, persistence, allocator, and stress tests for complete
//! code-path coverage: RwLock safety, flush cycling, free-list rebuild,
//! allocator exact-fit, fragmented reopen, many small files, etc.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

use common::{make_medium_store, make_small, make_store, verify_small, MEDIUM_DEVICE, SMALL_DEVICE};

// ═══════════════════════════════════════════════════════════════════════
// Persistence / reopen edge cases
// ═══════════════════════════════════════════════════════════════════════

/// Open a freshly formatted device that was never flushed after format.
/// This exercises the "compute free list from used intervals" path in open()
/// because the initial format writes an index with empty free_list.
#[tokio::test]
async fn reopen_after_format_no_extra_flush() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // format_with_size writes initial index and superblock, then returns.
    // Drop without any extra writes or flushes.
    {
        let _store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    }

    // Reopen: the stored index has empty free_list and empty files,
    // so the code falls through to computing free list from used intervals.
    let store = RawObjectStore::open(&path_buf).unwrap();

    // Should be able to write and read
    store
        .put(
            &Path::from("test.bin"),
            PutPayload::from(Bytes::from("hello")),
        )
        .await
        .unwrap();

    let data = store
        .get(&Path::from("test.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, Bytes::from("hello"));
}

/// Many flush cycles: flush 50 times, reopen, verify state.
#[tokio::test]
async fn many_flush_cycles() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        for cycle in 0..50 {
            store
                .put(
                    &Path::from(format!("cycle_{cycle:03}.bin")),
                    PutPayload::from(make_small(cycle, 1024)),
                )
                .await
                .unwrap();
            store.flush_index().unwrap();
        }
    }

    let store = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 50, "all 50 files should persist");

    // Verify each file
    for cycle in 0..50 {
        let data = store
            .get(&Path::from(format!("cycle_{cycle:03}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_small(&data, cycle, 1024);
    }
}

/// Verify that flush alternates between index regions A and B
/// by doing multiple flushes and reopening each time.
#[tokio::test]
async fn flush_alternates_index_regions() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    for i in 0..10 {
        {
            let store = if i == 0 {
                RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap()
            } else {
                RawObjectStore::open(&path_buf).unwrap()
            };

            store
                .put(
                    &Path::from(format!("file_{i:02}.bin")),
                    PutPayload::from(make_small(i, 512)),
                )
                .await
                .unwrap();
            store.flush_index().unwrap();
        }

        // Reopen and verify
        let store = RawObjectStore::open(&path_buf).unwrap();
        let files: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(files.len(), i + 1, "after flush {i}, expected {} files", i + 1);
    }
}

/// Fragmented allocator state survives reopen:
/// write many files, delete alternating ones (creating fragmentation),
/// flush, reopen, verify free space is usable.
#[tokio::test]
async fn fragmented_allocator_survives_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        // Write 100 files
        for i in 0..100 {
            store
                .put(
                    &Path::from(format!("frag/{i:04}.bin")),
                    PutPayload::from(make_small(i, 4096)),
                )
                .await
                .unwrap();
        }

        // Delete even-numbered files (creating 50 gaps)
        for i in (0..100).step_by(2) {
            store
                .delete(&Path::from(format!("frag/{i:04}.bin")))
                .await
                .unwrap();
        }

        store.flush_index().unwrap();
    }

    // Reopen
    let store = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 50, "50 odd files should survive");

    // The freed space from even files should be usable
    for i in 100..150 {
        store
            .put(
                &Path::from(format!("frag/{i:04}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 100, "should have 50 old + 50 new files");
}

/// Multiple open/write/flush/close cycles on the same device.
#[tokio::test]
async fn multiple_open_close_cycles() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Format initially
    {
        let _store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    }

    for cycle in 0..10 {
        let store = RawObjectStore::open(&path_buf).unwrap();
        store
            .put(
                &Path::from(format!("cycle/{cycle:02}.bin")),
                PutPayload::from(make_small(cycle, 2048)),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
        drop(store);
    }

    let store = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 10, "all 10 cycle files should persist");
}

// ═══════════════════════════════════════════════════════════════════════
// Allocator edge cases
// ═══════════════════════════════════════════════════════════════════════

/// Fill the device nearly full, then delete and refill to test
/// allocator coalescing under pressure.
#[tokio::test]
async fn allocator_fill_delete_refill() {
    let (store, _tmp) = make_store();

    // Fill with 4KB files until full
    let mut count = 0;
    loop {
        let result = store
            .put(
                &Path::from(format!("fill/{count:05}.bin")),
                PutPayload::from(make_small(count, 4096)),
            )
            .await;
        match result {
            Ok(_) => count += 1,
            Err(_) => break,
        }
    }
    assert!(count > 0, "should have written at least some files");

    // Delete all
    for i in 0..count {
        store
            .delete(&Path::from(format!("fill/{i:05}.bin")))
            .await
            .unwrap();
    }

    // Refill — should succeed with same count
    let mut count2 = 0;
    loop {
        let result = store
            .put(
                &Path::from(format!("refill/{count2:05}.bin")),
                PutPayload::from(make_small(count2, 4096)),
            )
            .await;
        match result {
            Ok(_) => count2 += 1,
            Err(_) => break,
        }
    }
    assert_eq!(count, count2, "refill should fit same number of files");
}

/// Device full error when writing to a full device.
#[tokio::test]
async fn device_full_error() {
    let tmp = NamedTempFile::new().unwrap();
    // Use minimum size device — very little data space
    let store =
        RawObjectStore::format_with_size(tmp.path(), rawobjstr::MIN_DEVICE_SIZE, false)
            .unwrap();

    // Try to write a large file that exceeds available space
    let err = store
        .put(
            &Path::from("big.bin"),
            PutPayload::from(Bytes::from(vec![0u8; 1024 * 1024])),
        )
        .await;
    assert!(err.is_err(), "should fail when device is full");
}

// ═══════════════════════════════════════════════════════════════════════
// Many small files
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn thousand_tiny_files() {
    let (store, tmp) = make_medium_store();
    let path_buf = tmp.path().to_path_buf();

    for i in 0..1000 {
        store
            .put(
                &Path::from(format!("tiny/{i:05}.bin")),
                PutPayload::from(make_small(i, 64)),
            )
            .await
            .unwrap();
    }

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 1000);

    // Verify a sampling
    for i in (0..1000).step_by(100) {
        let data = store
            .get(&Path::from(format!("tiny/{i:05}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_small(&data, i, 64);
    }

    // Flush and reopen
    store.flush_index().unwrap();
    drop(store); // release flock before reopening
    let store2 = RawObjectStore::open(&path_buf).unwrap();
    let files2: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files2.len(), 1000, "all 1000 files should persist");
}

#[tokio::test]
async fn many_files_with_various_prefixes() {
    let (store, _tmp) = make_medium_store();

    // 10 "tables" × 50 files each = 500 files
    for t in 0..10 {
        for f in 0..50 {
            store
                .put(
                    &Path::from(format!("table_{t:02}/data/{f:04}.db")),
                    PutPayload::from(make_small(t * 50 + f, 512)),
                )
                .await
                .unwrap();
        }
    }

    // Total count
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 500);

    // Per-table count
    for t in 0..10 {
        let table_files: Vec<_> = store
            .list(Some(&Path::from(format!("table_{t:02}/data"))))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(table_files.len(), 50, "table {t} should have 50 files");
    }

    // list_with_delimiter at root
    let root = store.list_with_delimiter(None).await.unwrap();
    assert_eq!(root.common_prefixes.len(), 10, "should see 10 table prefixes");
    assert_eq!(root.objects.len(), 0, "no root-level objects");
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrency: RwLock safety
// ═══════════════════════════════════════════════════════════════════════

/// Many concurrent readers — the RwLock should allow parallel reads.
#[tokio::test]
async fn concurrent_50_readers() {
    let (store, _tmp) = make_store();
    let store = Arc::new(store);

    // Pre-populate
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("read/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    // 50 concurrent readers, each reading all 10 files
    let mut handles = Vec::new();
    for _ in 0..50 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let data = s
                    .get(&Path::from(format!("read/{i:02}.bin")))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                verify_small(&data, i, 4096);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

/// Concurrent reads during ongoing writes.
#[tokio::test]
async fn concurrent_reads_during_writes() {
    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    // Pre-populate 10 files
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("base/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Writer: adds 50 new files
    let ws = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for i in 10..60 {
            ws.put(
                &Path::from(format!("base/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
        }
    }));

    // 10 readers: repeatedly read the original 10 files
    for _ in 0..10 {
        let rs = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _round in 0..20 {
                for i in 0..10 {
                    let data = rs
                        .get(&Path::from(format!("base/{i:02}.bin")))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                    verify_small(&data, i, 4096);
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 60);
}

/// Concurrent get + overwrite race: one thread reads, another overwrites.
#[tokio::test]
async fn concurrent_get_overwrite_race() {
    let (store, _tmp) = make_store();
    let store = Arc::new(store);
    let path = Path::from("race.bin");

    store
        .put(&path, PutPayload::from(make_small(0, 4096)))
        .await
        .unwrap();

    let mut handles = Vec::new();

    // Overwriter: 100 overwrites
    let ws = Arc::clone(&store);
    let wp = path.clone();
    handles.push(tokio::spawn(async move {
        for i in 1..101usize {
            ws.put(&wp, PutPayload::from(make_small(i, 4096)))
                .await
                .unwrap();
        }
    }));

    // Reader: 100 reads — each should return valid data (no partial/corrupt)
    let rs = Arc::clone(&store);
    let rp = path.clone();
    handles.push(tokio::spawn(async move {
        for _ in 0..100 {
            let data = rs.get(&rp).await.unwrap().bytes().await.unwrap();
            assert_eq!(data.len(), 4096, "read should get exactly 4096 bytes");
            // Data should be internally consistent (all fill bytes match tag)
            if data.len() >= 8 {
                let tag = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
                let fill = (tag & 0xFF) as u8;
                for &b in &data[8..] {
                    assert_eq!(b, fill, "data corrupt: tag={tag} but found byte {b}");
                }
            }
            tokio::task::yield_now().await;
        }
    }));

    for h in handles {
        h.await.unwrap();
    }
}

/// Concurrent delete + read race: deleter removes files, reader may get NotFound.
#[tokio::test]
async fn concurrent_delete_read_race() {
    let (store, _tmp) = make_store();
    let store = Arc::new(store);

    // Pre-populate 50 files
    for i in 0..50 {
        store
            .put(
                &Path::from(format!("dr/{i:03}.bin")),
                PutPayload::from(make_small(i, 1024)),
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Deleter: delete all 50
    let ds = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for i in 0..50 {
            let _ = ds.delete(&Path::from(format!("dr/{i:03}.bin"))).await;
        }
    }));

    // Reader: attempt reads — some will succeed, some will get NotFound, but no panics
    let rs = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        let mut found = 0u32;
        let mut not_found = 0u32;
        for i in 0..50 {
            match rs.get(&Path::from(format!("dr/{i:03}.bin"))).await {
                Ok(r) => {
                    let data = r.bytes().await.unwrap();
                    assert_eq!(data.len(), 1024);
                    found += 1;
                }
                Err(_) => not_found += 1,
            }
            tokio::task::yield_now().await;
        }
        assert!(
            found + not_found == 50,
            "all operations should complete: found={found} not_found={not_found}"
        );
    }));

    for h in handles {
        h.await.unwrap();
    }
}

/// Concurrent copy operations.
#[tokio::test]
async fn concurrent_copy_operations() {
    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    // Pre-populate 20 source files
    for i in 0..20 {
        store
            .put(
                &Path::from(format!("src/{i:03}.bin")),
                PutPayload::from(make_small(i, 2048)),
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // 5 concurrent copiers, each copying all 20 files to their own prefix
    for c in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..20 {
                s.copy(
                    &Path::from(format!("src/{i:03}.bin")),
                    &Path::from(format!("copy_{c}/{i:03}.bin")),
                )
                .await
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 20 source + 5 × 20 copies = 120 total
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 120);

    // Verify copies
    for c in 0..5 {
        for i in 0..20 {
            let data = store
                .get(&Path::from(format!("copy_{c}/{i:03}.bin")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            verify_small(&data, i, 2048);
        }
    }
}

/// Concurrent multipart uploads.
#[tokio::test]
async fn concurrent_multipart_uploads() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    let mut handles = Vec::new();

    for w in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let path = Path::from(format!("mp/{w:02}.bin"));
            let mut upload = s.put_multipart(&path).await.unwrap();
            for part in 0..10 {
                let data = vec![(w * 10 + part) as u8; 1024];
                upload
                    .put_part(PutPayload::from(Bytes::from(data)))
                    .await
                    .unwrap();
            }
            upload.complete().await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 5 files, each 10KB
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 5);
    for f in &files {
        assert_eq!(f.size, 10240);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Get result metadata verification
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn get_result_metadata_is_correct() {
    let (store, _tmp) = make_store();
    let path = Path::from("meta_check.bin");
    let payload = Bytes::from(vec![0xAB; 5000]);

    store
        .put(&path, PutPayload::from(payload))
        .await
        .unwrap();

    let result = store.get(&path).await.unwrap();
    assert_eq!(result.meta.size, 5000);
    assert_eq!(result.meta.location, path);
    assert!(result.meta.e_tag.is_none());
    assert!(result.meta.version.is_none());
    assert_eq!(result.range, 0..5000);

    let data = result.bytes().await.unwrap();
    assert_eq!(data.len(), 5000);
}

#[tokio::test]
async fn get_range_result_metadata() {
    use object_store::{GetOptions, GetRange};

    let (store, _tmp) = make_store();
    let path = Path::from("range_meta.bin");
    store
        .put(&path, PutPayload::from(Bytes::from(vec![0u8; 1000])))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Bounded(100..500)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    assert_eq!(result.range, 100..500);
    assert_eq!(result.meta.size, 1000, "meta.size should be total file size");
}

// ═══════════════════════════════════════════════════════════════════════
// Stress: interleaved flush + write
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn interleaved_write_flush_write() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        for batch in 0..20 {
            // Write 5 files
            for i in 0..5 {
                let fid = batch * 5 + i;
                store
                    .put(
                        &Path::from(format!("batch/{fid:04}.bin")),
                        PutPayload::from(make_small(fid, 2048)),
                    )
                    .await
                    .unwrap();
            }
            // Flush after every batch
            store.flush_index().unwrap();
        }
    }

    // Reopen and verify all 100 files
    let store = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 100);

    for fid in 0..100 {
        let data = store
            .get(&Path::from(format!("batch/{fid:04}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_small(&data, fid, 2048);
    }
}

/// Flush after delete: verify deleted files stay deleted after reopen.
#[tokio::test]
async fn flush_after_delete_persists() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

        for i in 0..20 {
            store
                .put(
                    &Path::from(format!("del/{i:03}.bin")),
                    PutPayload::from(make_small(i, 1024)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();

        // Delete half
        for i in 0..10 {
            store
                .delete(&Path::from(format!("del/{i:03}.bin")))
                .await
                .unwrap();
        }
        store.flush_index().unwrap();
    }

    let store = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 10, "only 10 should survive");

    // Deleted files should be gone
    for i in 0..10 {
        assert!(
            store
                .get(&Path::from(format!("del/{i:03}.bin")))
                .await
                .is_err()
        );
    }

    // Remaining files should be intact
    for i in 10..20 {
        let data = store
            .get(&Path::from(format!("del/{i:03}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_small(&data, i, 1024);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Stress: random operation mix
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn random_operation_mix_stress() {
    use rand::Rng;

    let (store, _tmp) = make_medium_store();
    let mut rng = rand::thread_rng();
    let mut existing_keys: Vec<String> = Vec::new();

    for _ in 0..500 {
        let op = rng.gen_range(0..5u32);

        match op {
            0 | 1 => {
                // PUT: write a new or existing key
                let key = format!("rng/{:05}.bin", rng.gen_range(0..200u32));
                let size = rng.gen_range(1..8192usize);
                store
                    .put(
                        &Path::from(key.as_str()),
                        PutPayload::from(Bytes::from(vec![0xAA; size])),
                    )
                    .await
                    .unwrap();
                if !existing_keys.contains(&key) {
                    existing_keys.push(key);
                }
            }
            2 => {
                // GET: read a known key
                if !existing_keys.is_empty() {
                    let idx = rng.gen_range(0..existing_keys.len());
                    let _result = store.get(&Path::from(existing_keys[idx].as_str())).await;
                }
            }
            3 => {
                // DELETE: remove a known key
                if !existing_keys.is_empty() {
                    let idx = rng.gen_range(0..existing_keys.len());
                    let key = existing_keys.remove(idx);
                    let _ = store.delete(&Path::from(key.as_str())).await;
                }
            }
            4 => {
                // LIST
                let _files: Vec<_> = store.list(None).try_collect().await.unwrap();
            }
            _ => unreachable!(),
        }
    }

    // Final consistency: list count should match our tracked keys
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(
        files.len(),
        existing_keys.len(),
        "tracked vs actual file count mismatch"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Error format strings (RawStoreError Display coverage)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn error_messages_are_descriptive() {
    // NotFound
    let (store, _tmp) = make_store();
    let err = store.get(&Path::from("nope")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("not found") || msg.contains("NotFound") || msg.contains("nope"),
        "NotFound error should be descriptive: {msg}"
    );

    // AlreadyExists
    store
        .put(
            &Path::from("dup.bin"),
            PutPayload::from(Bytes::from("x")),
        )
        .await
        .unwrap();
    let opts = object_store::PutOptions {
        mode: object_store::PutMode::Create,
        ..Default::default()
    };
    let err = store
        .put_opts(
            &Path::from("dup.bin"),
            PutPayload::from(Bytes::from("y")),
            opts,
        )
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("already exists") || msg.contains("AlreadyExists"),
        "AlreadyExists error should be descriptive: {msg}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent reads during active multipart upload
// ═══════════════════════════════════════════════════════════════════════

/// While one task does a multipart upload (10 parts), other tasks
/// continuously read pre-existing files.  The readers must never see
/// corrupt data or panic.  The multipart file should not be visible
/// until complete() is called.
#[tokio::test]
async fn concurrent_reads_during_multipart_upload() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    // Pre-populate 10 files for readers
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("pre/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Writer: multipart upload with 10 × 4KB parts
    let ws = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        let path = Path::from("mp/uploading.bin");
        let mut upload = ws.put_multipart(&path).await.unwrap();
        for part in 0..10 {
            let data = vec![(part & 0xFF) as u8; 4096];
            upload
                .put_part(PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
            // Yield between parts to interleave with readers
            tokio::task::yield_now().await;
        }
        upload.complete().await.unwrap();
    }));

    // 10 readers: repeatedly read pre-existing files during the upload
    for _ in 0..10 {
        let rs = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _round in 0..20 {
                for i in 0..10 {
                    let data = rs
                        .get(&Path::from(format!("pre/{i:02}.bin")))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                    verify_small(&data, i, 4096);
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // After completion, the multipart file should be readable
    let mp_data = store
        .get(&Path::from("mp/uploading.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(mp_data.len(), 10 * 4096);

    // Verify each part's fill byte
    for part in 0..10usize {
        let slice = &mp_data[part * 4096..(part + 1) * 4096];
        let expected = (part & 0xFF) as u8;
        for &b in slice {
            assert_eq!(b, expected, "multipart part {part} corrupt");
        }
    }
}

/// Two concurrent multipart uploads + readers that probe the in-progress keys.
/// GET on incomplete multipart should return NotFound (not partial data).
#[tokio::test]
async fn concurrent_multipart_and_probes() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    let mut handles = Vec::new();

    // Two multipart uploaders
    for w in 0..2 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let path = Path::from(format!("probe/mp_{w}.bin"));
            let mut upload = s.put_multipart(&path).await.unwrap();
            for part in 0..8 {
                let data = vec![(w * 100 + part) as u8; 2048];
                upload
                    .put_part(PutPayload::from(Bytes::from(data)))
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
            upload.complete().await.unwrap();
        }));
    }

    // Probers: try GET on the multipart paths during upload
    for w in 0..2 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _ in 0..30 {
                let path = Path::from(format!("probe/mp_{w}.bin"));
                match s.get(&path).await {
                    Ok(r) => {
                        // If visible, must be the completed file (all 8 × 2048 bytes)
                        let data = r.bytes().await.unwrap();
                        assert_eq!(data.len(), 8 * 2048, "partial multipart data visible");
                    }
                    Err(_) => {
                        // NotFound is expected while upload is in progress
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Both files should exist and be complete now
    for w in 0..2 {
        let data = store
            .get(&Path::from(format!("probe/mp_{w}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 8 * 2048);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent range reads during writes
// ═══════════════════════════════════════════════════════════════════════

/// Multiple tasks do bounded range reads on pre-existing files while
/// a writer task creates new files.  Range reads must return exactly
/// the expected byte slices — no corruption from concurrent writes.
#[tokio::test]
async fn concurrent_range_reads_during_writes() {
    use object_store::{GetOptions, GetRange};

    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    // Build a deterministic payload where byte[i] = (i % 251) as u8
    let payload_size = 100_000;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

    // Pre-populate 5 files with known data
    for f in 0..5 {
        store
            .put(
                &Path::from(format!("rng/{f}.bin")),
                PutPayload::from(Bytes::from(payload.clone())),
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Writer: creates 50 new files concurrently
    let ws = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for i in 0..50 {
            ws.put(
                &Path::from(format!("rng/new_{i:03}.bin")),
                PutPayload::from(make_small(i, 8192)),
            )
            .await
            .unwrap();
        }
    }));

    // 5 range-reader tasks, each doing 20 rounds of range reads on the 5 files
    let payload_ref = Bytes::from(payload);
    for _ in 0..5 {
        let rs = Arc::clone(&store);
        let expected = payload_ref.clone();
        handles.push(tokio::spawn(async move {
            for round in 0..20 {
                for f in 0..5 {
                    // Vary the range each round
                    let start = (round * 3000 + f * 1000) % (payload_size - 1000);
                    let end = start + 1000;
                    let opts = GetOptions {
                        range: Some(GetRange::Bounded(start as u64..end as u64)),
                        ..Default::default()
                    };
                    let result = rs
                        .get_opts(&Path::from(format!("rng/{f}.bin")), opts)
                        .await
                        .unwrap();
                    let data = result.bytes().await.unwrap();
                    assert_eq!(
                        data.as_ref(),
                        &expected[start..end],
                        "range read corrupt: file={f} round={round} range={start}..{end}"
                    );
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all 55 files exist
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 55);
}

/// Concurrent suffix and offset range reads racing with overwrites
/// on the same files. Each read must return consistent data from
/// either the old or new version — never a mix.
#[tokio::test]
async fn concurrent_range_reads_during_overwrites() {
    use object_store::{GetOptions, GetRange};

    let (store, _tmp) = make_medium_store();
    let store = Arc::new(store);

    // Two versions: v0 = all 0xAA, v1 = all 0xBB (each 50KB)
    let size = 50_000;
    let v0 = Bytes::from(vec![0xAAu8; size]);
    let v1 = Bytes::from(vec![0xBBu8; size]);

    // Write initial version
    for f in 0..5 {
        store
            .put(
                &Path::from(format!("ow/{f}.bin")),
                PutPayload::from(v0.clone()),
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Overwriter: rewrites all 5 files 10 times with v1
    let ws = Arc::clone(&store);
    let v1c = v1.clone();
    handles.push(tokio::spawn(async move {
        for _round in 0..10 {
            for f in 0..5 {
                ws.put(
                    &Path::from(format!("ow/{f}.bin")),
                    PutPayload::from(v1c.clone()),
                )
                .await
                .unwrap();
            }
            tokio::task::yield_now().await;
        }
    }));

    // Range readers: suffix reads of last 10KB — must be all-0xAA or all-0xBB
    for _ in 0..5 {
        let rs = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for _round in 0..30 {
                for f in 0..5 {
                    let opts = GetOptions {
                        range: Some(GetRange::Suffix(10_000)),
                        ..Default::default()
                    };
                    let result = rs
                        .get_opts(&Path::from(format!("ow/{f}.bin")), opts)
                        .await
                        .unwrap();
                    let data = result.bytes().await.unwrap();
                    assert_eq!(data.len(), 10_000);
                    let first = data[0];
                    assert!(
                        first == 0xAA || first == 0xBB,
                        "unexpected fill byte: {first:#x}"
                    );
                    // All bytes must match — no mixing of versions
                    for (i, &b) in data.iter().enumerate() {
                        assert_eq!(
                            b, first,
                            "version mix at byte {i}: expected {first:#x}, got {b:#x}"
                        );
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Flush / checkpoint race with concurrent operations
// ═══════════════════════════════════════════════════════════════════════

/// One task calls flush_index() repeatedly while other tasks are
/// actively writing and reading.  The store must not panic or corrupt,
/// and a reopen after the test must recover a consistent state.
#[tokio::test]
async fn flush_race_with_concurrent_ops() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();
    let store = Arc::new(store);

    let mut handles = Vec::new();

    // Writer: create 100 files
    let ws = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for i in 0..100 {
            ws.put(
                &Path::from(format!("fr/{i:04}.bin")),
                PutPayload::from(make_small(i, 2048)),
            )
            .await
            .unwrap();
            if i % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }));

    // Flusher: call flush_index 20 times during writes
    let fs = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for _ in 0..20 {
            fs.flush_index().unwrap();
            tokio::task::yield_now().await;
        }
    }));

    // Reader: continuously reads any files that exist
    let rs = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for _ in 0..200 {
            // List what's available and read a few
            let files: Vec<_> = rs.list(None).try_collect().await.unwrap();
            for meta in files.iter().take(5) {
                let data = rs.get(&meta.location).await.unwrap().bytes().await.unwrap();
                assert!(!data.is_empty(), "got empty read for {}", meta.location);
            }
            tokio::task::yield_now().await;
        }
    }));

    for h in handles {
        h.await.unwrap();
    }

    // Final flush
    store.flush_index().unwrap();
    drop(store); // release flock before reopening

    // Reopen and verify: all 100 files should be there
    let store2 = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 100, "all 100 files should survive flush race");

    for i in 0..100 {
        let data = store2
            .get(&Path::from(format!("fr/{i:04}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_small(&data, i, 2048);
    }
}

/// Flush interleaved with deletes: verify that deletions persist
/// correctly even when flush races with delete operations.
#[tokio::test]
async fn flush_race_with_deletes() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();
    let store = Arc::new(store);

    // Pre-populate 50 files
    for i in 0..50 {
        store
            .put(
                &Path::from(format!("fd/{i:03}.bin")),
                PutPayload::from(make_small(i, 1024)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    let mut handles = Vec::new();

    // Deleter: delete even-numbered files
    let ds = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for i in (0..50).step_by(2) {
            ds.delete(&Path::from(format!("fd/{i:03}.bin")))
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    }));

    // Flusher: flush 10 times during deletes
    let fs = Arc::clone(&store);
    handles.push(tokio::spawn(async move {
        for _ in 0..10 {
            fs.flush_index().unwrap();
            tokio::task::yield_now().await;
        }
    }));

    for h in handles {
        h.await.unwrap();
    }

    // Final flush
    store.flush_index().unwrap();
    drop(store); // release flock before reopening

    // Reopen and verify: only odd files survive
    let store2 = RawObjectStore::open(&path_buf).unwrap();
    let files: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 25, "only 25 odd files should survive");

    for i in (1..50).step_by(2) {
        let data = store2
            .get(&Path::from(format!("fd/{i:03}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_small(&data, i, 1024);
    }

    // Even files should be gone
    for i in (0..50).step_by(2) {
        assert!(
            store2
                .get(&Path::from(format!("fd/{i:03}.bin")))
                .await
                .is_err(),
            "even file {i} should be deleted"
        );
    }
}
