mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, PutMode, PutOptions, PutPayload};
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rawobjstr::store::RawObjectStore;
use rawobjstr::store::FormatOptions;
use rawobjstr::Compression;
use tempfile::NamedTempFile;

use common::{make_1gb_store, make_chunk, max_1mb_chunks, verify_chunk, CHUNK_SIZE, ONE_GB};

// ===========================================================================
// TEST 1: Fill 1 GB, verify, full-disk error, delete half, reuse, persist
// ===========================================================================

#[tokio::test]
async fn fill_1gb_delete_and_reuse() {
    let (store, tmp) = make_1gb_store();
    let max_chunks = max_1mb_chunks();
    println!("max 1 MB chunks: {max_chunks}");
    assert!(max_chunks > 900, "expected ~988 chunks, got {max_chunks}");

    // -- fill --
    for i in 0..max_chunks {
        let path = Path::from(format!("data/chunk_{:05}.bin", i));
        store
            .put(&path, PutPayload::from(make_chunk(i)))
            .await
            .unwrap_or_else(|e| panic!("put chunk {i}: {e}"));
        if i % 200 == 0 {
            println!("  wrote {i}/{max_chunks}");
        }
    }
    println!("filled {max_chunks} chunks");

    // -- verify all --
    for i in 0..max_chunks {
        let path = Path::from(format!("data/chunk_{:05}.bin", i));
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        verify_chunk(&data, i);
    }
    println!("all chunks verified");

    // -- full --
    let err = store.put(&Path::from("overflow"), PutPayload::from(make_chunk(99999))).await;
    assert!(err.is_err(), "should be full");
    println!("full-disk error: {}", err.unwrap_err());

    let tiny_err = store.put(&Path::from("another_mb"), PutPayload::from(make_chunk(99998))).await;
    assert!(tiny_err.is_err(), "another 1 MB chunk should fail too");

    // -- list count --
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), max_chunks);

    // -- delete every other --
    let deleted_count = (max_chunks + 1) / 2;
    for i in (0..max_chunks).step_by(2) {
        store.delete(&Path::from(format!("data/chunk_{:05}.bin", i))).await.unwrap();
    }
    println!("deleted {deleted_count} even chunks");

    // confirm gone
    for i in (0..max_chunks).step_by(2) {
        assert!(store.get(&Path::from(format!("data/chunk_{:05}.bin", i))).await.is_err());
    }
    // odd still ok
    for i in (1..max_chunks).step_by(2) {
        let data = store.get(&Path::from(format!("data/chunk_{:05}.bin", i))).await.unwrap().bytes().await.unwrap();
        verify_chunk(&data, i);
    }

    // -- refill freed space --
    let new_base = max_chunks + 1000;
    let mut new_count = 0usize;
    for i in 0..deleted_count {
        let path = Path::from(format!("new/chunk_{:05}.bin", i));
        match store.put(&path, PutPayload::from(make_chunk(new_base + i))).await {
            Ok(_) => new_count += 1,
            Err(_) => break,
        }
    }
    println!("reused {new_count}/{deleted_count} slots");
    assert!(
        new_count >= deleted_count.saturating_sub(2),
        "expected to reclaim nearly all deleted space: got {new_count}/{deleted_count}"
    );

    // verify new
    for i in 0..new_count {
        let data = store.get(&Path::from(format!("new/chunk_{:05}.bin", i))).await.unwrap().bytes().await.unwrap();
        verify_chunk(&data, new_base + i);
    }

    // -- flush & reopen --
    store.flush_index().unwrap();
    drop(store); // release flock before reopening
    let store2 = RawObjectStore::open(tmp.path()).unwrap();

    let data = store2.get(&Path::from("data/chunk_00001.bin")).await.unwrap().bytes().await.unwrap();
    verify_chunk(&data, 1);
    if new_count > 0 {
        let data = store2.get(&Path::from("new/chunk_00000.bin")).await.unwrap().bytes().await.unwrap();
        verify_chunk(&data, new_base);
    }
    assert!(store2.get(&Path::from("data/chunk_00000.bin")).await.is_err());

    let total: Vec<_> = store2.list(None).try_collect().await.unwrap();
    let expected = (max_chunks - deleted_count) + new_count;
    assert_eq!(total.len(), expected);
    println!("PASS fill_1gb_delete_and_reuse: {max_chunks} fill, {deleted_count} del, {new_count} reuse, {expected} total");
}

// ===========================================================================
// TEST 2: Concurrent readers + writers (Arc<RawObjectStore> across tasks)
// ===========================================================================

#[tokio::test]
async fn concurrent_read_write() {
    let (store, _tmp) = make_1gb_store();
    let store = Arc::new(store);

    // Seed some files
    for i in 0..50 {
        let path = Path::from(format!("conc/file_{:03}.bin", i));
        store.put(&path, PutPayload::from(make_chunk(i))).await.unwrap();
    }

    // Spawn 10 writers and 10 readers running simultaneously
    let mut handles = Vec::new();

    for w in 0..10 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..20 {
                let idx = 1000 + w * 20 + i;
                let path = Path::from(format!("conc/writer_{}/file_{:03}.bin", w, i));
                s.put(&path, PutPayload::from(make_chunk(idx))).await.unwrap();
            }
        }));
    }

    for r in 0..10 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                let path = Path::from(format!("conc/file_{:03}.bin", i));
                let data = s.get(&path).await.unwrap().bytes().await.unwrap();
                verify_chunk(&data, i);
            }
            println!("  reader {r} done");
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all writer data
    for w in 0..10usize {
        for i in 0..20usize {
            let idx = 1000 + w * 20 + i;
            let path = Path::from(format!("conc/writer_{}/file_{:03}.bin", w, i));
            let data = store.get(&path).await.unwrap().bytes().await.unwrap();
            verify_chunk(&data, idx);
        }
    }

    // T3 fix: add contention on shared keys — multiple writers overwriting the
    // same files concurrently, with readers that verify no partial/corrupt data.
    let mut contention_handles = Vec::new();
    // 5 writers all overwrite the same 10 keys
    for w in 0..5usize {
        let s = Arc::clone(&store);
        contention_handles.push(tokio::spawn(async move {
            for round in 0..4usize {
                for key in 0..10usize {
                    let idx = 5000 + w * 100 + round * 10 + key;
                    let path = Path::from(format!("contention/shared_{key:02}"));
                    s.put(&path, PutPayload::from(make_chunk(idx))).await.unwrap();
                }
            }
        }));
    }
    // 5 concurrent readers on those same keys
    for _r in 0..5usize {
        let s = Arc::clone(&store);
        contention_handles.push(tokio::spawn(async move {
            for _ in 0..20 {
                for key in 0..10usize {
                    let path = Path::from(format!("contention/shared_{key:02}"));
                    if let Ok(res) = s.get(&path).await {
                        let data = res.bytes().await.unwrap();
                        // Must be a valid complete chunk (any writer version) — never corrupt
                        assert_eq!(data.len(), CHUNK_SIZE, "contention read: wrong chunk size");
                        let fill = (data[..8].try_into().map(|b: [u8; 8]| u64::from_le_bytes(b)).unwrap() & 0xFF) as u8;
                        for (i, &b) in data[8..].iter().enumerate() {
                            assert_eq!(b, fill, "contention read: corrupt byte at {i}");
                        }
                    }
                    // NotFound is fine — writer may not have written yet
                }
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in contention_handles {
        h.await.unwrap();
    }

    println!("PASS concurrent_read_write: 10 writers x 20 + 10 readers x 50 + contention phase");
}

// ===========================================================================
// TEST 3: Drop store mid-write (no flush) -> reopen recovers last flushed state
// ===========================================================================

#[tokio::test]
async fn drop_without_flush_recovers_last_checkpoint() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Phase 1: write 10 files, flush
    {
        let store = RawObjectStore::format_with_size(tmp.path(), ONE_GB, false).unwrap();
        for i in 0..10 {
            store.put(&Path::from(format!("phase1/{i}.bin")), PutPayload::from(make_chunk(i))).await.unwrap();
        }
        store.flush_index().unwrap();
        println!("phase1: wrote and flushed 10 files");
    }

    // Phase 2: write 10 more files, DO NOT flush, drop
    {
        let store = RawObjectStore::open(&path_buf).unwrap();

        // Verify phase1 survived
        for i in 0..10 {
            let data = store.get(&Path::from(format!("phase1/{i}.bin"))).await.unwrap().bytes().await.unwrap();
            verify_chunk(&data, i);
        }

        // Write unflushed data
        for i in 10..20 {
            store.put(&Path::from(format!("phase2/{i}.bin")), PutPayload::from(make_chunk(i))).await.unwrap();
        }
        println!("phase2: wrote 10 more files WITHOUT flush, dropping store");
        // store drops here -- no flush
    }

    // Phase 3: reopen -- only phase1 data should be there
    {
        let store = RawObjectStore::open(&path_buf).unwrap();
        for i in 0..10 {
            let data = store.get(&Path::from(format!("phase1/{i}.bin"))).await.unwrap().bytes().await.unwrap();
            verify_chunk(&data, i);
        }
        // phase2 data should be lost
        for i in 10..20 {
            assert!(
                store.get(&Path::from(format!("phase2/{i}.bin"))).await.is_err(),
                "phase2 file {i} should not survive without flush"
            );
        }

        let all: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(all.len(), 10, "only phase1 files should exist");
        println!("PASS drop_without_flush: phase1 recovered, phase2 lost");
    }
}

// ===========================================================================
// TEST 4: Overwrite-in-place -- old space is reclaimed
// ===========================================================================

#[tokio::test]
async fn overwrite_reclaims_space() {
    let (store, _tmp) = make_1gb_store();
    let max_chunks = max_1mb_chunks();

    // Fill all but one slot (do_put allocates before freeing, so an
    // overwrite at 100% capacity would fail with NoSpace).
    for i in 0..max_chunks - 1 {
        store.put(&Path::from(format!("f/{i}")), PutPayload::from(make_chunk(i))).await.unwrap();
    }

    // Overwrite f/0: uses the one free slot, then frees old f/0 -> net 1 free
    store.put(&Path::from("f/0"), PutPayload::from(make_chunk(9999))).await.unwrap();
    let data = store.get(&Path::from("f/0")).await.unwrap().bytes().await.unwrap();
    verify_chunk(&data, 9999);

    // Other files still intact
    let data = store.get(&Path::from("f/1")).await.unwrap().bytes().await.unwrap();
    verify_chunk(&data, 1);

    // Overwrite again using the freed slot -> still net 1 free
    store.put(&Path::from("f/0"), PutPayload::from(make_chunk(8888))).await.unwrap();
    let data = store.get(&Path::from("f/0")).await.unwrap().bytes().await.unwrap();
    verify_chunk(&data, 8888);

    // Fill the last free slot
    store.put(&Path::from("f/last"), PutPayload::from(make_chunk(7777))).await.unwrap();

    // Now 100% full — overwrite should fail (alloc before free, no room)
    assert!(store.put(&Path::from("f/0"), PutPayload::from(make_chunk(0))).await.is_err());

    // Delete one to free a slot, then we can write a replacement
    store.delete(&Path::from("f/2")).await.unwrap();
    store.put(&Path::from("f/replaced"), PutPayload::from(make_chunk(5555))).await.unwrap();
    let data = store.get(&Path::from("f/replaced")).await.unwrap().bytes().await.unwrap();
    verify_chunk(&data, 5555);

    println!("PASS overwrite_reclaims_space");
}

// ===========================================================================
// TEST 5: Multipart upload end-to-end
// ===========================================================================

#[tokio::test]
async fn multipart_upload_e2e() {
    let (store, _tmp) = make_1gb_store();

    let mut upload = store
        .put_multipart(&Path::from("multi/big.bin"))
        .await
        .unwrap();

    // Upload 5 x 1 MB parts
    for i in 0..5 {
        upload.put_part(PutPayload::from(make_chunk(i))).await.unwrap();
    }
    upload.complete().await.unwrap();

    // Read back and verify all 5 MB
    let data = store.get(&Path::from("multi/big.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.len(), 5 * CHUNK_SIZE);
    for i in 0..5 {
        let slice = &data[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE];
        verify_chunk(slice, i);
    }
    println!("PASS multipart_upload_e2e: 5 parts verified");
}

// ===========================================================================
// TEST 6: Multipart abort -- nothing written
// ===========================================================================

#[tokio::test]
async fn multipart_abort_no_leak() {
    let (store, _tmp) = make_1gb_store();

    let mut upload = store
        .put_multipart(&Path::from("aborted/file.bin"))
        .await
        .unwrap();
    upload.put_part(PutPayload::from(make_chunk(0))).await.unwrap();
    upload.put_part(PutPayload::from(make_chunk(1))).await.unwrap();
    upload.abort().await.unwrap();

    // File should not exist
    assert!(store.get(&Path::from("aborted/file.bin")).await.is_err());

    // No files at all
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 0);
    println!("PASS multipart_abort_no_leak");
}

// ===========================================================================
// TEST 7: Various file sizes — block boundaries + Lance v2.2 blob thresholds
//   Lance v2.2: blobs ≤64 KB are stored inline, larger blobs up to ~4 MB
// ===========================================================================

#[tokio::test]
async fn various_file_sizes() {
    let (store, _tmp) = make_1gb_store();
    let sizes: Vec<(usize, &str)> = vec![
        // -- block-alignment boundaries --
        (1, "one_byte"),
        (4095, "just_under_block"),
        (4096, "exact_block"),
        (4097, "just_over_block"),
        // -- Lance v2.2 inline blob threshold (64 KB) --
        (32 * 1024, "32kb"),
        (64 * 1024 - 1, "64kb_minus_1_inline_max"),
        (64 * 1024, "64kb_exact_inline_boundary"),
        (64 * 1024 + 1, "64kb_plus_1_not_inline"),
        (128 * 1024, "128kb"),
        (256 * 1024, "256kb"),
        (512 * 1024, "512kb"),
        // -- MB range --
        (CHUNK_SIZE - 1, "1mb_minus_1"),
        (CHUNK_SIZE, "exactly_1mb"),
        (CHUNK_SIZE + 1, "1mb_plus_1"),
        (2 * CHUNK_SIZE, "2mb"),
        (3 * CHUNK_SIZE, "3mb"),
        // -- Lance v2.2 large blob threshold (4 MB) --
        (4 * CHUNK_SIZE - 1, "4mb_minus_1"),
        (4 * CHUNK_SIZE, "4mb_exact_large_blob"),
        (4 * CHUNK_SIZE + 1, "4mb_plus_1"),
        // -- bigger --
        (8 * CHUNK_SIZE, "8mb"),
        (10 * CHUNK_SIZE, "10mb"),
    ];

    for (size, name) in &sizes {
        let mut data = vec![0xABu8; *size];
        // Tag first 8 bytes if large enough
        if *size >= 8 {
            data[..8].copy_from_slice(&(*size as u64).to_le_bytes());
        }
        let payload = Bytes::from(data.clone());
        let path = Path::from(format!("sizes/{name}"));

        store.put(&path, PutPayload::from(payload)).await.unwrap();
        let read = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(read.len(), *size, "size mismatch for {name}");
        assert_eq!(&read[..], &data[..], "content mismatch for {name}");
        println!("  {name}: {size} bytes OK");
    }

    // head() should return the correct sizes
    for (size, name) in &sizes {
        let meta = store.head(&Path::from(format!("sizes/{name}"))).await.unwrap();
        assert_eq!(meta.size as usize, *size, "head size wrong for {name}");
    }
    println!("PASS various_file_sizes");
}

// ===========================================================================
// TEST 8: PutMode::Create rejects duplicate
// ===========================================================================

#[tokio::test]
async fn put_mode_create_rejects_duplicate() {
    let (store, _tmp) = make_1gb_store();
    let path = Path::from("unique.bin");

    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };

    store
        .put_opts(&path, PutPayload::from(Bytes::from("first")), opts.clone())
        .await
        .unwrap();

    let err = store
        .put_opts(&path, PutPayload::from(Bytes::from("second")), opts)
        .await;
    assert!(err.is_err(), "second Create should fail");
    match err.unwrap_err() {
        object_store::Error::AlreadyExists { path: p, .. } => {
            assert_eq!(p, "unique.bin");
        }
        other => panic!("expected AlreadyExists, got {other}"),
    }

    // Original data intact
    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("first"));
    println!("PASS put_mode_create_rejects_duplicate");
}

// ===========================================================================
// TEST 9: Repeated fill-delete cycles (fragmentation stress)
// ===========================================================================

#[tokio::test]
async fn fragmentation_stress() {
    let (store, _tmp) = make_1gb_store();
    let chunk_count = 200; // use 200 MB subset for speed

    for cycle in 0..5 {
        // Fill
        for i in 0..chunk_count {
            let path = Path::from(format!("frag/c{cycle}_{i:04}"));
            store.put(&path, PutPayload::from(make_chunk(cycle * 1000 + i))).await.unwrap();
        }

        // Delete all from this cycle
        for i in 0..chunk_count {
            store.delete(&Path::from(format!("frag/c{cycle}_{i:04}"))).await.unwrap();
        }

        let remaining: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(remaining.len(), 0, "cycle {cycle}: expected empty after delete");
        println!("  cycle {cycle}: wrote+deleted {chunk_count} chunks");
    }

    // After 5 cycles of delete-all, we should be able to fill the whole device again
    let max_chunks = max_1mb_chunks();
    for i in 0..max_chunks {
        store.put(&Path::from(format!("final/{i}")), PutPayload::from(make_chunk(i)))
            .await
            .unwrap_or_else(|e| panic!("final fill chunk {i}: {e}"));
    }
    // Another 1 MB chunk should fail (small left-over below 1 MB may remain)
    assert!(store.put(&Path::from("overflow"), PutPayload::from(make_chunk(99999))).await.is_err());

    // Verify every chunk's content after the final refill
    for i in 0..max_chunks {
        let data = store.get(&Path::from(format!("final/{i}"))).await.unwrap().bytes().await.unwrap();
        verify_chunk(&data, i);
    }

    println!("PASS fragmentation_stress: 5 cycles + full refill (all content verified)");
}

// ===========================================================================
// TEST 10: Random-size writes until full, then random deletes + refill
// ===========================================================================

#[tokio::test]
async fn random_sizes_fill_delete_refill() {
    let (store, _tmp) = make_1gb_store();
    let mut rng = StdRng::seed_from_u64(42);

    // Write random-sized objects (1 KB to 4 MB) until full
    let mut written: Vec<(String, Vec<u8>)> = Vec::new();
    let mut file_id = 0usize;
    loop {
        let size: usize = rng.gen_range(1024..4 * 1024 * 1024);
        let mut data = vec![0u8; size];
        data[..8.min(size)].copy_from_slice(&(file_id as u64).to_le_bytes()[..8.min(size)]);
        // Fill the rest with a deterministic pattern so we can verify later
        let fill = (file_id & 0xFF) as u8;
        for b in &mut data[8..] {
            *b = fill;
        }
        let path_str = format!("rand/{file_id:06}");
        let path = Path::from(path_str.clone());

        match store.put(&path, PutPayload::from(Bytes::from(data.clone()))).await {
            Ok(_) => {
                written.push((path_str, data));
                file_id += 1;
            }
            Err(_) => break,
        }
    }
    println!("wrote {file_id} random-size files until full");
    assert!(file_id > 100, "expected lots of files, got {file_id}");

    // Verify all — full byte-level content comparison
    for (path_str, expected_data) in &written {
        let data = store.get(&Path::from(path_str.as_str())).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len(), expected_data.len(), "size mismatch: {path_str}");
        assert_eq!(&data[..], &expected_data[..], "content mismatch: {path_str}");
    }

    // Delete random 50%
    let mut to_delete: Vec<usize> = (0..written.len()).collect();
    // Fisher-Yates shuffle first half
    for i in 0..to_delete.len() / 2 {
        let j = rng.gen_range(i..to_delete.len());
        to_delete.swap(i, j);
    }
    let delete_set: Vec<usize> = to_delete[..to_delete.len() / 2].to_vec();
    for &idx in &delete_set {
        store.delete(&Path::from(written[idx].0.as_str())).await.unwrap();
    }
    println!("deleted {} random files", delete_set.len());

    // Write more until full again
    let mut extra = 0usize;
    loop {
        let size: usize = rng.gen_range(1024..4 * 1024 * 1024);
        let data = vec![0xCDu8; size];
        let path = Path::from(format!("rand/extra_{extra:06}"));
        match store.put(&path, PutPayload::from(Bytes::from(data))).await {
            Ok(_) => extra += 1,
            Err(_) => break,
        }
    }
    println!("wrote {extra} more files into freed space");
    assert!(extra > 0, "should reclaim at least some space");

    println!("PASS random_sizes_fill_delete_refill: {file_id} initial, {} deleted, {extra} refilled", delete_set.len());
}

// ===========================================================================
// TEST 11: Copy operations at scale
// ===========================================================================

#[tokio::test]
async fn copy_at_scale() {
    let (store, _tmp) = make_1gb_store();

    // Write 100 files
    for i in 0..100 {
        store.put(&Path::from(format!("src/{i:03}")), PutPayload::from(make_chunk(i))).await.unwrap();
    }

    // Copy all to a new prefix
    for i in 0..100 {
        store.copy(&Path::from(format!("src/{i:03}")), &Path::from(format!("dst/{i:03}"))).await.unwrap();
    }

    // Verify copies
    for i in 0..100 {
        let orig = store.get(&Path::from(format!("src/{i:03}"))).await.unwrap().bytes().await.unwrap();
        let copy = store.get(&Path::from(format!("dst/{i:03}"))).await.unwrap().bytes().await.unwrap();
        verify_chunk(&orig, i);
        verify_chunk(&copy, i);
    }

    // copy_if_not_exists should fail since dst already exists
    let err = store.copy_if_not_exists(&Path::from("src/000"), &Path::from("dst/000")).await;
    assert!(err.is_err());

    // copy_if_not_exists to a new target should succeed
    store.copy_if_not_exists(&Path::from("src/000"), &Path::from("dst2/000")).await.unwrap();
    let data = store.get(&Path::from("dst2/000")).await.unwrap().bytes().await.unwrap();
    verify_chunk(&data, 0);

    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 201); // 100 src + 100 dst + 1 dst2
    println!("PASS copy_at_scale: 100 copies verified");
}

// ===========================================================================
// TEST 12: Multiple flushes and reopens (checkpoint chaining)
// ===========================================================================

#[tokio::test]
async fn multiple_flush_reopen_cycles() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    let store = RawObjectStore::format_with_size(tmp.path(), ONE_GB, false).unwrap();

    for cycle in 0..10 {
        // Write some files
        for i in 0..5 {
            let fid = cycle * 5 + i;
            store.put(
                &Path::from(format!("cycle/{fid:04}")),
                PutPayload::from(make_chunk(fid)),
            ).await.unwrap();
        }

        // Flush
        store.flush_index().unwrap();

        // Reopen a separate handle and verify (read-only; writer keeps the lock)
        let verify = RawObjectStore::open_readonly(&path_buf).unwrap();
        let all: Vec<_> = verify.list(None).try_collect().await.unwrap();
        assert_eq!(all.len(), (cycle + 1) * 5, "cycle {cycle} count mismatch after reopen");
    }

    // Final verification of all 50 files
    drop(store); // release flock before reopening
    let final_store = RawObjectStore::open(&path_buf).unwrap();
    for fid in 0..50 {
        let data = final_store.get(&Path::from(format!("cycle/{fid:04}"))).await.unwrap().bytes().await.unwrap();
        verify_chunk(&data, fid);
    }
    println!("PASS multiple_flush_reopen_cycles: 10 cycles, 50 files");
}

// ===========================================================================
// TEST 13: list_with_delimiter correctness at scale
// ===========================================================================

#[tokio::test]
async fn list_with_delimiter_at_scale() {
    let (store, _tmp) = make_1gb_store();

    // Create a directory-like structure
    // tables/t1/versions/1.manifest
    // tables/t1/data/00000.db
    // tables/t1/data/00001.db
    // tables/t2/data/00000.db
    // _transactions/0001.txn
    let paths = vec![
        "tables/t1/versions/1.manifest",
        "tables/t1/data/00000.db",
        "tables/t1/data/00001.db",
        "tables/t1/data/sub/deep.db",
        "tables/t2/data/00000.db",
        "tables/t2/data/00001.db",
        "_transactions/0001.txn",
        "_transactions/0002.txn",
        "top_level.bin",
    ];

    for (i, p) in paths.iter().enumerate() {
        store.put(&Path::from(*p), PutPayload::from(make_chunk(i))).await.unwrap();
    }

    // Root: top_level.bin is direct; tables, _transactions are dirs
    let root = store.list_with_delimiter(None).await.unwrap();
    assert_eq!(root.objects.len(), 1, "only top_level.bin at root");
    assert_eq!(root.objects[0].location, Path::from("top_level.bin"));
    let root_dirs: Vec<String> = root.common_prefixes.iter().map(|p| p.to_string()).collect();
    assert!(root_dirs.contains(&"tables".to_string()), "tables missing: {root_dirs:?}");
    assert!(root_dirs.contains(&"_transactions".to_string()), "_transactions missing: {root_dirs:?}");
    assert_eq!(root_dirs.len(), 2);

    // tables/: t1, t2 as directories
    let tables = store.list_with_delimiter(Some(&Path::from("tables"))).await.unwrap();
    assert_eq!(tables.objects.len(), 0);
    let table_dirs: Vec<String> = tables.common_prefixes.iter().map(|p| p.to_string()).collect();
    assert!(table_dirs.contains(&"tables/t1".to_string()));
    assert!(table_dirs.contains(&"tables/t2".to_string()));

    // tables/t1/: versions, data as dirs
    let t1 = store.list_with_delimiter(Some(&Path::from("tables/t1"))).await.unwrap();
    assert_eq!(t1.objects.len(), 0, "no direct files under t1");
    let t1_dirs: Vec<String> = t1.common_prefixes.iter().map(|p| p.to_string()).collect();
    assert!(t1_dirs.contains(&"tables/t1/data".to_string()));
    assert!(t1_dirs.contains(&"tables/t1/versions".to_string()));

    // tables/t1/data/: 00000, 00001 as objects; sub as directory
    let t1_data = store.list_with_delimiter(Some(&Path::from("tables/t1/data"))).await.unwrap();
    assert_eq!(t1_data.objects.len(), 2);
    assert_eq!(t1_data.common_prefixes.len(), 1);
    assert_eq!(t1_data.common_prefixes[0], Path::from("tables/t1/data/sub"));

    // Flat list under tables/t1 should return all 4 files
    let flat: Vec<_> = store.list(Some(&Path::from("tables/t1"))).try_collect().await.unwrap();
    assert_eq!(flat.len(), 4);

    println!("PASS list_with_delimiter_at_scale");
}

// ===========================================================================
// TEST 14: Concurrent writers racing on same key (last writer wins)
// ===========================================================================

#[tokio::test]
async fn concurrent_overwrite_same_key() {
    let (store, _tmp) = make_1gb_store();
    let store = Arc::new(store);
    let path = Path::from("race/target.bin");

    // Seed
    store.put(&path, PutPayload::from(make_chunk(0))).await.unwrap();

    // 20 tasks all overwriting the same file
    let mut handles = Vec::new();
    for w in 0..20usize {
        let s = Arc::clone(&store);
        let p = path.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let idx = w * 100 + i;
                s.put(&p, PutPayload::from(make_chunk(idx))).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // File should exist and be valid (some writer won)
    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.len(), CHUNK_SIZE);
    // Just verify the tag structure is intact
    let stored_idx = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    verify_chunk(&data, stored_idx);

    // Only one file should exist at that path
    let all: Vec<_> = store.list(Some(&Path::from("race"))).try_collect().await.unwrap();
    assert_eq!(all.len(), 1);

    println!("PASS concurrent_overwrite_same_key: final writer index={stored_idx}");
}

// ===========================================================================
// TEST 15: Delete non-existent files returns proper errors
// ===========================================================================

#[tokio::test]
async fn error_semantics() {
    let (store, _tmp) = make_1gb_store();

    // Get non-existent
    match store.get(&Path::from("nope")).await {
        Err(object_store::Error::NotFound { path, .. }) => assert_eq!(path, "nope"),
        other => panic!("expected NotFound, got {other:?}"),
    }

    // Delete non-existent (idempotent per ObjectStore trait contract)
    store.delete(&Path::from("nope")).await.unwrap();

    // Head non-existent
    match store.head(&Path::from("nope")).await {
        Err(object_store::Error::NotFound { .. }) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }

    // Copy from non-existent
    match store.copy(&Path::from("nope"), &Path::from("dst")).await {
        Err(object_store::Error::NotFound { .. }) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }

    println!("PASS error_semantics");
}

// ===========================================================================
// TEST 16: Lance v2.2 blob workload — inline (≤64 KB) + large (up to 4 MB)
//
// Simulates what LanceDB v2.2 actually writes: lots of small inline blobs
// (manifests, small indices) plus larger data files and blob columns.
// ===========================================================================

/// Build a deterministic blob of any size with a verifiable pattern.
fn make_blob(id: usize, size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    if size >= 16 {
        buf[..8].copy_from_slice(&(id as u64).to_le_bytes());
        buf[8..16].copy_from_slice(&(size as u64).to_le_bytes());
    } else if size >= 8 {
        buf[..8].copy_from_slice(&(id as u64).to_le_bytes());
    }
    let fill = ((id ^ size) & 0xFF) as u8;
    let start = 16.min(size);
    for b in &mut buf[start..] {
        *b = fill;
    }
    buf
}

fn verify_blob(data: &[u8], id: usize, expected_size: usize) {
    assert_eq!(data.len(), expected_size, "blob {id} size mismatch");
    if expected_size >= 16 {
        let stored_id = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        let stored_sz = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        assert_eq!(stored_id, id, "blob {id} id tag mismatch");
        assert_eq!(stored_sz, expected_size, "blob {id} size tag mismatch");
    }
    let fill = ((id ^ expected_size) & 0xFF) as u8;
    let start = 16.min(expected_size);
    for (i, &b) in data[start..].iter().enumerate() {
        assert_eq!(b, fill, "blob {id} byte {} mismatch", start + i);
    }
}

#[tokio::test]
async fn lance_v2_blob_workload() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();
    let store = RawObjectStore::format_with_size(tmp.path(), ONE_GB, false).unwrap();

    // -- Phase 1: Small inline blobs (≤64 KB) --
    // Lance stores manifests, small indices, and inline blob columns at these sizes
    let inline_sizes: Vec<usize> = vec![
        128, 256, 512, 1024, 2048, 4096,
        8 * 1024, 16 * 1024, 32 * 1024, 48 * 1024,
        63 * 1024, 64 * 1024 - 1, 64 * 1024, // at and just under inline limit
    ];
    let mut blobs: Vec<(String, usize)> = Vec::new();
    for (i, &size) in inline_sizes.iter().enumerate() {
        let path_str = format!("table1/_versions/{i:04}.manifest");
        store.put(
            &Path::from(path_str.as_str()),
            PutPayload::from(Bytes::from(make_blob(i, size))),
        ).await.unwrap();
        blobs.push((path_str, size));
    }
    println!("  wrote {} inline-range blobs (128 B to 64 KB)", inline_sizes.len());

    // -- Phase 2: Medium blobs (64 KB to 1 MB) --
    // Lance data fragment files, small lance files
    let medium_sizes: Vec<usize> = vec![
        64 * 1024 + 1, 96 * 1024, 128 * 1024, 256 * 1024, 512 * 1024,
        768 * 1024, CHUNK_SIZE - 1, CHUNK_SIZE,
    ];
    for (j, &size) in medium_sizes.iter().enumerate() {
        let i = inline_sizes.len() + j;
        let path_str = format!("table1/data/{i:06}.db");
        store.put(
            &Path::from(path_str.as_str()),
            PutPayload::from(Bytes::from(make_blob(i, size))),
        ).await.unwrap();
        blobs.push((path_str, size));
    }
    println!("  wrote {} medium blobs (64 KB to 1 MB)", medium_sizes.len());

    // -- Phase 3: Large blobs (1 MB to 4 MB) --
    // Lance large data files, blob columns with images/embeddings
    let large_sizes: Vec<usize> = vec![
        CHUNK_SIZE + 1,
        2 * CHUNK_SIZE,
        3 * CHUNK_SIZE,
        4 * CHUNK_SIZE - 1,
        4 * CHUNK_SIZE,         // exactly 4 MB
        4 * CHUNK_SIZE + 4096,  // just over 4 MB
    ];
    for (j, &size) in large_sizes.iter().enumerate() {
        let i = inline_sizes.len() + medium_sizes.len() + j;
        let path_str = format!("table1/data/{i:06}.db");
        store.put(
            &Path::from(path_str.as_str()),
            PutPayload::from(Bytes::from(make_blob(i, size))),
        ).await.unwrap();
        blobs.push((path_str, size));
    }
    println!("  wrote {} large blobs (1 MB to 4+ MB)", large_sizes.len());

    // -- Verify all blobs in-memory --
    for (idx, (path_str, expected_size)) in blobs.iter().enumerate() {
        let data = store.get(&Path::from(path_str.as_str())).await.unwrap().bytes().await.unwrap();
        verify_blob(&data, idx, *expected_size);
    }
    println!("  all {} blobs verified (in-memory)", blobs.len());

    // -- Flush, reopen, verify again --
    store.flush_index().unwrap();
    drop(store);
    let store2 = RawObjectStore::open(&path_buf).unwrap();

    for (idx, (path_str, expected_size)) in blobs.iter().enumerate() {
        let data = store2.get(&Path::from(path_str.as_str())).await.unwrap().bytes().await.unwrap();
        verify_blob(&data, idx, *expected_size);
    }

    let all: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), blobs.len());

    // -- Overwrite some blobs with different sizes (simulates compaction) --
    // Overwrite an inline blob with a large one
    store2.put(
        &Path::from("table1/_versions/0000.manifest"),
        PutPayload::from(Bytes::from(make_blob(9999, 2 * CHUNK_SIZE))),
    ).await.unwrap();
    let data = store2.get(&Path::from("table1/_versions/0000.manifest")).await.unwrap().bytes().await.unwrap();
    verify_blob(&data, 9999, 2 * CHUNK_SIZE);

    // Overwrite a large blob with a tiny one
    let last_large_path = &blobs[blobs.len() - 1].0;
    store2.put(
        &Path::from(last_large_path.as_str()),
        PutPayload::from(Bytes::from(make_blob(8888, 256))),
    ).await.unwrap();
    let data = store2.get(&Path::from(last_large_path.as_str())).await.unwrap().bytes().await.unwrap();
    verify_blob(&data, 8888, 256);

    // -- Flush, reopen a third time, verify overwrites persisted --
    store2.flush_index().unwrap();
    drop(store2);
    let store3 = RawObjectStore::open(&path_buf).unwrap();

    let data = store3.get(&Path::from("table1/_versions/0000.manifest")).await.unwrap().bytes().await.unwrap();
    verify_blob(&data, 9999, 2 * CHUNK_SIZE);

    let data = store3.get(&Path::from(last_large_path.as_str())).await.unwrap().bytes().await.unwrap();
    verify_blob(&data, 8888, 256);

    println!("PASS lance_v2_blob_workload: {} blobs (128 B to 4+ MB), flush/reopen/overwrite verified", blobs.len());
}

// ===========================================================================
// TEST 17: Concurrent mixed-size blob writes — inline + large simultaneously
// ===========================================================================

#[tokio::test]
async fn concurrent_mixed_blob_sizes() {
    let (store, _tmp) = make_1gb_store();
    let store = Arc::new(store);

    // 5 writers: each writes a mix of inline (≤64KB) and large (up to 4MB) blobs
    let mut handles = Vec::new();
    for w in 0..5usize {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let writer_sizes: Vec<usize> = vec![
                512, 4096, 32 * 1024, 64 * 1024,       // inline range
                128 * 1024, 512 * 1024,                  // medium
                CHUNK_SIZE, 2 * CHUNK_SIZE, 4 * CHUNK_SIZE, // large
            ];
            for (i, &size) in writer_sizes.iter().enumerate() {
                let blob_id = w * 1000 + i;
                let path = Path::from(format!("w{w}/blob_{i:03}"));
                s.put(
                    &path,
                    PutPayload::from(Bytes::from(make_blob(blob_id, size))),
                ).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all 45 blobs (5 writers × 9 sizes)
    let writer_sizes: Vec<usize> = vec![
        512, 4096, 32 * 1024, 64 * 1024,
        128 * 1024, 512 * 1024,
        CHUNK_SIZE, 2 * CHUNK_SIZE, 4 * CHUNK_SIZE,
    ];
    for w in 0..5usize {
        for (i, &size) in writer_sizes.iter().enumerate() {
            let blob_id = w * 1000 + i;
            let path = Path::from(format!("w{w}/blob_{i:03}"));
            let data = store.get(&path).await.unwrap().bytes().await.unwrap();
            verify_blob(&data, blob_id, size);
        }
    }

    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 45);
    println!("PASS concurrent_mixed_blob_sizes: 5 writers × 9 sizes (512 B to 4 MB)");
}

// ===========================================================================
// TEST 18: Full workload with 32 MB index slots
// ===========================================================================

/// Format a device with 32 MB index slots, write hundreds of files, flush,
/// reopen, and verify everything round-trips.
#[tokio::test]
async fn workload_with_32mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 32 * 1024 * 1024u64;
    let device_size = ONE_GB;

    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    }).unwrap();

    // Write 500 files of varying sizes (256 B – 64 KB)
    let mut rng = StdRng::seed_from_u64(0xDEAD_0032);
    for i in 0..500u32 {
        let size = rng.gen_range(256..=65536);
        let data = vec![(i & 0xFF) as u8; size];
        store.put(
            &Path::from(format!("data/part_{i:04}.parquet")),
            PutPayload::from(Bytes::from(data)),
        ).await.unwrap();
    }

    store.flush_index().unwrap();

    // Reopen and verify count + contents
    drop(store);
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let files: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 500);

    // Spot-check a few
    for &i in &[0u32, 42, 250, 499] {
        let data = store2.get(&Path::from(format!("data/part_{i:04}.parquet")))
            .await.unwrap().bytes().await.unwrap();
        assert_eq!(data[0], (i & 0xFF) as u8);
    }

    // Delete half, flush, reopen
    for i in 0..250u32 {
        store2.delete(&Path::from(format!("data/part_{i:04}.parquet"))).await.unwrap();
    }
    store2.flush_index().unwrap();
    drop(store2);

    let store3 = RawObjectStore::open(tmp.path()).unwrap();
    let files: Vec<_> = store3.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 250);

    println!("PASS workload_with_32mb_index_slots: 500 files → delete half → 250 remain");
}

// ===========================================================================
// TEST 19: Exhausted index — fill until flush fails
// ===========================================================================

/// Use the smallest possible index slot (16 MB) and write files until
/// the serialized index exceeds the slot capacity, verifying that
/// `flush_index()` returns a clear error.
#[tokio::test]
async fn exhausted_index_returns_no_space() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 16 * 1024 * 1024u64; // 16 MB — smallest allowed
    // Need a large enough device to hold all the tiny files + 32 MB index
    let device_size = ONE_GB;

    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    }).unwrap();

    // Each index entry is ~80-120 bytes. With 16 MB slot capacity we can
    // fit roughly 150k files.  Write 1-byte files with long-ish paths to
    // push the index over the limit faster.
    //
    // Use paths of ~80 chars so each entry is ~160 bytes →
    // 16 MB / 160 ≈ 100k entries needed.  That's a lot of puts, so we
    // batch in groups and try flushing periodically to detect the limit.
    let mut count = 0u64;
    let batch = 5000;

    loop {
        // Write a batch
        for i in 0..batch {
            let n = count + i;
            // ~80-char path: "idx_exhaust/aaaaaa…/file_NNNNNNN.bin"
            let dir = format!("idx_exhaust/{:a>40}", "");
            let path = Path::from(format!("{dir}/file_{n:07}.bin"));
            store.put(&path, PutPayload::from(Bytes::from_static(b"x")))
                .await.unwrap();
        }
        count += batch;

        // Try to flush — eventually the index will exceed slot capacity
        match store.flush_index() {
            Ok(()) => {
                // Still fits — keep going, but bail if we somehow reach 200k
                // without overflow (shouldn't happen with 16 MB slot & ~160 B/entry)
                if count >= 200_000 {
                    panic!("wrote {count} files without exhausting 16 MB index — check entry size estimates");
                }
            }
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("no space") || msg.contains("NoSpace")
                        || msg.contains("shard") && msg.contains("overflow"),
                    "expected NoSpace or ShardOverflow error, got: {msg}"
                );
                println!("Index exhausted after {count} files: {msg}");
                break;
            }
        }
    }

    // The store should still be usable for reads (in-memory state is fine)
    // — the last batch just wasn't persisted.
    // Verify we can read a file written before the last successful flush.
    let early = store.get(&Path::from("idx_exhaust/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/file_0000000.bin"))
        .await.unwrap().bytes().await.unwrap();
    assert_eq!(early, Bytes::from_static(b"x"));

    println!("PASS exhausted_index_returns_no_space: index full after {count} files");
}

// ===========================================================================
// TEST 20: Fuzz index_slot_size — valid multiples of 16 MB
// ===========================================================================

/// Format with every valid slot size from 16 MB to 128 MB, write files,
/// flush, reopen, and verify data survives the round-trip.
#[tokio::test]
async fn fuzz_valid_slot_sizes() {
    let valid_slots: Vec<u64> = vec![
        16 * 1024 * 1024,
        32 * 1024 * 1024,
        48 * 1024 * 1024,
        64 * 1024 * 1024,
        80 * 1024 * 1024,
        96 * 1024 * 1024,
        112 * 1024 * 1024,
        128 * 1024 * 1024,
    ];

    for slot_size in &valid_slots {
        let tmp = NamedTempFile::new().unwrap();
        let min_dev = rawobjstr::min_device_size(*slot_size);
        // Give enough room for data + the index regions
        let device_size = min_dev + 32 * 1024 * 1024;

        let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
            device_size,
            direct_io: false,
            index_slot_size: *slot_size,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        }).unwrap_or_else(|e| panic!("format failed for slot_size={}: {e}", slot_size));

        // Write 50 files of increasing size
        for i in 0..50u32 {
            let data = vec![(i & 0xFF) as u8; (i as usize + 1) * 100];
            store.put(
                &Path::from(format!("fuzz/{slot_size}/f_{i:03}.bin")),
                PutPayload::from(Bytes::from(data)),
            ).await.unwrap();
        }

        store.flush_index().unwrap_or_else(|e| panic!("flush failed for slot_size={}: {e}", slot_size));

        // Reopen and verify
        drop(store);
        let store2 = RawObjectStore::open(tmp.path())
            .unwrap_or_else(|e| panic!("reopen failed for slot_size={}: {e}", slot_size));

        let files: Vec<_> = store2.list(None).try_collect().await.unwrap();
        assert_eq!(files.len(), 50, "slot_size={slot_size}: expected 50 files, got {}", files.len());

        // Verify first and last
        let d0 = store2.get(&Path::from(format!("fuzz/{slot_size}/f_000.bin")))
            .await.unwrap().bytes().await.unwrap();
        assert_eq!(d0.len(), 100);
        assert_eq!(d0[0], 0);

        let d49 = store2.get(&Path::from(format!("fuzz/{slot_size}/f_049.bin")))
            .await.unwrap().bytes().await.unwrap();
        assert_eq!(d49.len(), 5000);
        assert_eq!(d49[0], 49);

        // Read superblock and verify stored capacity
        {
            let mut f = std::fs::File::open(tmp.path()).unwrap();
            let mut sb_bytes = vec![0u8; rawobjstr::SUPERBLOCK_SIZE as usize];
            use std::io::Read;
            f.read_exact(&mut sb_bytes).unwrap();
            let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();
            assert_eq!(sb.index_slot_capacity, *slot_size,
                "superblock should store slot_size={slot_size}");
            assert_eq!(sb.version, 4);
        }

        println!("  slot_size={} MB: OK", slot_size / (1024 * 1024));
    }

    println!("PASS fuzz_valid_slot_sizes: {} sizes tested", valid_slots.len());
}

// ===========================================================================
// TEST 21: Fuzz index_slot_size — invalid values rejected
// ===========================================================================

/// Throw a wide range of invalid slot sizes at format_with_options and
/// verify every one is rejected with the right error.
#[tokio::test]
async fn fuzz_invalid_slot_sizes() {
    let invalid_slots: Vec<u64> = vec![
        0,                          // zero
        1,                          // tiny
        4096,                       // 4 KB — way too small
        1024 * 1024,                // 1 MB
        8 * 1024 * 1024,            // 8 MB — below 16 MB minimum
        15 * 1024 * 1024,           // 15 MB — just under
        16 * 1024 * 1024 - 1,       // 16 MB minus 1 byte
        16 * 1024 * 1024 + 1,       // 16 MB plus 1 byte
        17 * 1024 * 1024,           // 17 MB — not a multiple
        20 * 1024 * 1024,           // 20 MB — not a multiple
        24 * 1024 * 1024,           // 24 MB — not a multiple
        30 * 1024 * 1024,           // 30 MB — not a multiple
        31 * 1024 * 1024,           // 31 MB — not a multiple
        33 * 1024 * 1024,           // 33 MB — not a multiple
        47 * 1024 * 1024,           // 47 MB — just under 48
        49 * 1024 * 1024,           // 49 MB — just over 48
        63 * 1024 * 1024,           // 63 MB — just under 64
        65 * 1024 * 1024,           // 65 MB — just over 64
    ];

    for slot_size in &invalid_slots {
        let tmp = NamedTempFile::new().unwrap();
        let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
            device_size: ONE_GB,
            direct_io: false,
            index_slot_size: *slot_size,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        });

        assert!(
            result.is_err(),
            "slot_size={} should be rejected but was accepted",
            slot_size
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("invalid index slot size"),
            "slot_size={}: expected 'invalid index slot size', got: {msg}",
            slot_size
        );
        println!("  slot_size={}: correctly rejected", slot_size);
    }

    println!("PASS fuzz_invalid_slot_sizes: {} bad values rejected", invalid_slots.len());
}

// ===========================================================================
// TEST 22: Fuzz — device too small for requested slot size
// ===========================================================================

/// For each valid slot size, try formatting with a device that's just barely
/// too small and verify it's rejected, then with exactly the minimum and
/// verify it succeeds.
#[tokio::test]
async fn fuzz_device_size_boundary_per_slot() {
    let slot_sizes: Vec<u64> = vec![
        16 * 1024 * 1024,
        32 * 1024 * 1024,
        48 * 1024 * 1024,
        64 * 1024 * 1024,
    ];

    for slot_size in &slot_sizes {
        let min_size = rawobjstr::min_device_size(*slot_size);

        // One byte too small — must fail
        {
            let tmp = NamedTempFile::new().unwrap();
            let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
                device_size: min_size - 1,
                direct_io: false,
                index_slot_size: *slot_size,
                max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                compression: Compression::None,
            });
            assert!(result.is_err(),
                "slot_size={}, device_size={}: should fail (too small)",
                slot_size, min_size - 1);
            let msg = format!("{}", result.unwrap_err());
            assert!(msg.contains("too small"), "slot_size={}: {msg}", slot_size);
        }

        // Exactly minimum — must succeed
        {
            let tmp = NamedTempFile::new().unwrap();
            let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
                device_size: min_size,
                direct_io: false,
                index_slot_size: *slot_size,
                max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                compression: Compression::None,
            }).unwrap_or_else(|e| panic!(
                "slot_size={}, device_size={} (exact min): should succeed: {e}",
                slot_size, min_size
            ));

            // Write one file to prove it works
            store.put(
                &Path::from("boundary_test.bin"),
                PutPayload::from(Bytes::from("ok")),
            ).await.unwrap();
            store.flush_index().unwrap();

            drop(store);
            let store2 = RawObjectStore::open(tmp.path()).unwrap();
            let data = store2.get(&Path::from("boundary_test.bin"))
                .await.unwrap().bytes().await.unwrap();
            assert_eq!(data, Bytes::from("ok"));
        }

        println!("  slot_size={} MB: boundary OK", slot_size / (1024 * 1024));
    }

    println!("PASS fuzz_device_size_boundary_per_slot");
}

// ===========================================================================
// TEST 23: Fuzz — random valid slot sizes with workload
// ===========================================================================

/// Pick random valid slot sizes (random multiples of 16 MB) and run a mini
/// workload on each: write, overwrite, delete, flush, reopen, verify.
#[tokio::test]
async fn fuzz_random_slot_sizes_workload() {
    let mut rng = StdRng::seed_from_u64(0xF022_5107);

    for trial in 0..10u32 {
        // Random multiple of 16 MB between 16 MB and 128 MB
        let multiplier = rng.gen_range(1..=8u64);
        let slot_size = multiplier * 16 * 1024 * 1024;
        let min_dev = rawobjstr::min_device_size(slot_size);
        let device_size = min_dev + 64 * 1024 * 1024; // extra data room

        let tmp = NamedTempFile::new().unwrap();
        let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
            device_size,
            direct_io: false,
            index_slot_size: slot_size,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        }).unwrap_or_else(|e| panic!("trial {trial}: slot={slot_size}: {e}"));

        // Write 30 files
        for i in 0..30u32 {
            let size = rng.gen_range(100..=10000);
            let data = vec![((trial * 100 + i) & 0xFF) as u8; size];
            store.put(
                &Path::from(format!("t{trial}/f_{i:02}.dat")),
                PutPayload::from(Bytes::from(data)),
            ).await.unwrap();
        }

        // Overwrite 5 of them
        for i in 0..5u32 {
            let size = rng.gen_range(50..=5000);
            let data = vec![0xBB; size];
            store.put(
                &Path::from(format!("t{trial}/f_{i:02}.dat")),
                PutPayload::from(Bytes::from(data)),
            ).await.unwrap();
        }

        // Delete 5 others
        for i in 25..30u32 {
            store.delete(&Path::from(format!("t{trial}/f_{i:02}.dat"))).await.unwrap();
        }

        store.flush_index().unwrap();

        // Reopen
        drop(store);
        let store2 = RawObjectStore::open(tmp.path()).unwrap();
        let files: Vec<_> = store2.list(None).try_collect().await.unwrap();
        assert_eq!(files.len(), 25,
            "trial {trial} slot={slot_size}: expected 25 files, got {}", files.len());

        // Verify an overwritten file has the new content
        let d = store2.get(&Path::from(format!("t{trial}/f_00.dat")))
            .await.unwrap().bytes().await.unwrap();
        assert_eq!(d[0], 0xBB, "trial {trial}: overwritten file should have 0xBB");

        println!("  trial {trial}: slot_size={} MB, 30 writes/5 overwrites/5 deletes → 25 files OK",
            slot_size / (1024 * 1024));
    }

    println!("PASS fuzz_random_slot_sizes_workload: 10 random trials");
}

// ===========================================================================
// TEST: Multipart orphan temp extents purged on reopen
// ===========================================================================

/// Start a multipart upload, write parts, flush (so parts are persisted in
/// the index), then drop the store WITHOUT calling complete() or abort().
/// On reopen, all __raw_multipart_tmp/ keys should be automatically purged
/// and their disk space returned to the free list.
#[tokio::test]
async fn multipart_orphan_purge_on_reopen() {
    let (store, tmp) = common::make_store();

    // Write a normal file first so we can verify it survives the purge.
    store
        .put(
            &Path::from("keeper.bin"),
            PutPayload::from(Bytes::from(vec![0xAA; 4096])),
        )
        .await
        .unwrap();

    let free_before = store.device_info().free_space;

    // Start multipart, add 3 parts, but do NOT complete or abort.
    let mut upload = store
        .put_multipart(&Path::from("orphan/file.bin"))
        .await
        .unwrap();
    upload
        .put_part(PutPayload::from(make_chunk(0)))
        .await
        .unwrap();
    upload
        .put_part(PutPayload::from(make_chunk(1)))
        .await
        .unwrap();
    upload
        .put_part(PutPayload::from(make_chunk(2)))
        .await
        .unwrap();

    // Flush so the __raw_multipart_tmp/ keys are persisted to disk.
    store.flush_index().unwrap();

    let free_after_parts = store.device_info().free_space;
    assert!(
        free_after_parts < free_before,
        "parts should consume space: before={free_before}, after={free_after_parts}"
    );

    // Drop the store WITHOUT completing or aborting (simulates crash).
    // RawMultipartUpload has no custom Drop, so dropping it does NOT call
    // abort() -- the temp keys remain in the flushed index.  We must drop
    // the upload before the store so the shared Arc<Inner> flock is released.
    drop(upload);
    drop(store);

    // Reopen -- open_impl should purge orphaned multipart temp extents.
    let store2 = RawObjectStore::open(tmp.path()).unwrap();

    // The orphan file should NOT exist.
    assert!(
        store2
            .get(&Path::from("orphan/file.bin"))
            .await
            .is_err(),
        "orphan file should not exist after purge"
    );

    // No __raw_multipart_tmp/ keys should be visible.
    let all: Vec<_> = store2.list(None).try_collect().await.unwrap();
    for meta in &all {
        assert!(
            !meta.location.as_ref().contains("__raw_multipart_tmp"),
            "multipart temp key should be purged: {}",
            meta.location
        );
    }

    // The normal file should still be intact.
    let data = store2
        .get(&Path::from("keeper.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.len(), 4096);
    assert!(data.iter().all(|&b| b == 0xAA));

    // Free space should be recovered (equal to pre-multipart state).
    let free_after_purge = store2.device_info().free_space;
    assert_eq!(
        free_after_purge, free_before,
        "disk space should be fully recovered after orphan purge"
    );

    println!("PASS multipart_orphan_purge_on_reopen");
}