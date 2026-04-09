//! Tests for partial/range reads across per-block CRC boundaries.
//!
//! Verifies that range reads spanning multiple 4KB blocks return exactly the
//! expected bytes, and that the per-block CRC verification works correctly
//! for all types of range queries (Bounded, Offset, Suffix).

mod common;

use bytes::Bytes;
use object_store::{path::Path, GetOptions, GetRange, ObjectStore, PutPayload};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use common::make_store;

/// Build a deterministic payload of `size` bytes where byte[i] = (i % 251) as u8.
/// Using a prime modulus so the pattern doesn't align with block boundaries.
fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Helper: do a bounded range read and return the bytes.
async fn get_bounded(store: &impl ObjectStore, path: &Path, range: std::ops::Range<usize>) -> Bytes {
    let opts = GetOptions {
        range: Some(GetRange::Bounded(range.start as u64..range.end as u64)),
        ..Default::default()
    };
    let result = store.get_opts(path, opts).await.unwrap();
    result.bytes().await.unwrap()
}

/// Helper: do a suffix range read and return the bytes.
async fn get_suffix(store: &impl ObjectStore, path: &Path, nbytes: usize) -> Bytes {
    let opts = GetOptions {
        range: Some(GetRange::Suffix(nbytes as u64)),
        ..Default::default()
    };
    let result = store.get_opts(path, opts).await.unwrap();
    result.bytes().await.unwrap()
}

/// Helper: do an offset range read and return the bytes.
async fn get_offset(store: &impl ObjectStore, path: &Path, offset: usize) -> Bytes {
    let opts = GetOptions {
        range: Some(GetRange::Offset(offset as u64)),
        ..Default::default()
    };
    let result = store.get_opts(path, opts).await.unwrap();
    result.bytes().await.unwrap()
}

// ---- Deterministic multi-block range tests ----

#[tokio::test]
async fn range_read_within_first_block() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Read bytes 10..100 -- fully within block 0 (header occupies first 32
    // content bytes, so payload byte 10 is at content offset 42, still in
    // block 0 whose data region holds 4092 bytes)
    let data = get_bounded(&store, &path, 10..100).await;
    assert_eq!(data.as_ref(), &payload[10..100]);
}

#[tokio::test]
async fn range_read_spanning_two_blocks() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Block 0 data region holds header(32) + payload[0..4059] = 4092 bytes.
    // Block 1 data region holds payload[4060..8151].
    // Read across that boundary: payload bytes 4000..4200
    let data = get_bounded(&store, &path, 4000..4200).await;
    assert_eq!(data.as_ref(), &payload[4000..4200]);
}

#[tokio::test]
async fn range_read_spanning_many_blocks() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Read 50KB spanning ~13 blocks
    let data = get_bounded(&store, &path, 100_000..151_200).await;
    assert_eq!(data.as_ref(), &payload[100_000..151_200]);
}

#[tokio::test]
async fn range_read_entire_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Full file via bounded range
    let data = get_bounded(&store, &path, 0..1024 * 1024).await;
    assert_eq!(data.as_ref(), &payload[..]);
}

#[tokio::test]
async fn range_read_last_block_boundary() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Read the last 100 bytes of the file
    let start = 1024 * 1024 - 100;
    let data = get_bounded(&store, &path, start..1024 * 1024).await;
    assert_eq!(data.as_ref(), &payload[start..]);
}

#[tokio::test]
async fn range_read_suffix_spanning_blocks() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Suffix read of last 10000 bytes (spans ~3 blocks)
    let data = get_suffix(&store, &path, 10000).await;
    let expected_start = 1024 * 1024 - 10000;
    assert_eq!(data.as_ref(), &payload[expected_start..]);
}

#[tokio::test]
async fn range_read_offset_mid_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Offset read from 500000 to end (~524KB, ~128 blocks)
    let data = get_offset(&store, &path, 500_000).await;
    assert_eq!(data.as_ref(), &payload[500_000..]);
}

#[tokio::test]
async fn range_read_exact_block_boundaries() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Block 0 holds payload bytes 0..4059 (4060 bytes, since header takes 32).
    // Block 1 holds payload bytes 4060..8151.
    // Read exactly block 1's payload range.
    let data = get_bounded(&store, &path, 4060..8152).await;
    assert_eq!(data.as_ref(), &payload[4060..8152]);
}

#[tokio::test]
async fn range_read_single_byte_each_block() {
    let (store, _tmp) = make_store();
    let path = Path::from("range/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    // Read a single byte from block 0, block 1, block 5, block 100
    // Block N starts at payload offset: N*4092 - 32 (for N>0), block 0 at 0
    // Simpler: just pick offsets we know are in different blocks
    for offset in [0, 4060, 8152, 100_000, 500_000, 1_048_575] {
        let data = get_bounded(&store, &path, offset..offset + 1).await;
        assert_eq!(data[0], payload[offset], "mismatch at offset {offset}");
    }
}

// ---- Various file sizes ----

#[tokio::test]
async fn range_reads_various_sizes() {
    let (store, _tmp) = make_store();

    // Test with sizes that hit interesting block boundaries:
    // 1 byte, 4060 (fills block 0 data after header), 4061 (needs 2 blocks),
    // 8152 (fills 2 blocks), 100_000, 1MB
    let sizes = [1, 100, 4060, 4061, 4092, 8152, 8153, 100_000, 1024 * 1024];

    for &size in &sizes {
        let key = format!("sized/{size}.bin");
        let path = Path::from(key.as_str());
        let payload = make_payload(size);
        store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

        // Full read
        let result = store.get(&path).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(data.as_ref(), &payload[..], "full read failed for size {size}");

        // Range read from middle (if large enough)
        if size > 10 {
            let mid = size / 2;
            let end = (mid + 50).min(size);
            let data = get_bounded(&store, &path, mid..end).await;
            assert_eq!(
                data.as_ref(),
                &payload[mid..end],
                "mid-range read failed for size {size}"
            );
        }

        // Suffix read
        if size > 5 {
            let tail = 5.min(size);
            let data = get_suffix(&store, &path, tail).await;
            assert_eq!(
                data.as_ref(),
                &payload[size - tail..],
                "suffix read failed for size {size}"
            );
        }
    }
}

// ---- Fuzz tests ----

#[tokio::test]
async fn fuzz_random_range_reads_1mb() {
    let (store, _tmp) = make_store();
    let path = Path::from("fuzz/1mb.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    let mut rng = StdRng::seed_from_u64(42);
    let file_size = payload.len();

    for _ in 0..200 {
        let a = rng.gen_range(0..file_size);
        let b = rng.gen_range(0..file_size);
        let (start, end) = if a <= b { (a, b) } else { (b, a) };
        // Ensure non-empty range
        let end = (end + 1).min(file_size);

        let data = get_bounded(&store, &path, start..end).await;
        assert_eq!(
            data.as_ref(),
            &payload[start..end],
            "fuzz bounded mismatch at {start}..{end}"
        );
    }
}

#[tokio::test]
async fn fuzz_random_suffix_reads_1mb() {
    let (store, _tmp) = make_store();
    let path = Path::from("fuzz/suffix.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    let mut rng = StdRng::seed_from_u64(99);
    let file_size = payload.len();

    for _ in 0..50 {
        let nbytes = rng.gen_range(1..file_size);
        let data = get_suffix(&store, &path, nbytes).await;
        let expected_start = file_size - nbytes;
        assert_eq!(
            data.as_ref(),
            &payload[expected_start..],
            "fuzz suffix mismatch for last {nbytes} bytes"
        );
    }
}

#[tokio::test]
async fn fuzz_random_offset_reads_1mb() {
    let (store, _tmp) = make_store();
    let path = Path::from("fuzz/offset.bin");
    let payload = make_payload(1024 * 1024);
    store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

    let mut rng = StdRng::seed_from_u64(77);
    let file_size = payload.len();

    for _ in 0..50 {
        let offset = rng.gen_range(0..file_size);
        let data = get_offset(&store, &path, offset).await;
        assert_eq!(
            data.as_ref(),
            &payload[offset..],
            "fuzz offset mismatch at offset {offset}"
        );
    }
}

#[tokio::test]
async fn fuzz_random_sizes_and_ranges() {
    let (store, _tmp) = make_store();
    let mut rng = StdRng::seed_from_u64(123);

    for i in 0..50 {
        // Random file size: 1 byte to 200KB
        let file_size = rng.gen_range(1..200_000);
        let key = format!("fuzz_sizes/{i}.bin");
        let path = Path::from(key.as_str());
        let payload = make_payload(file_size);
        store.put(&path, PutPayload::from(payload.clone())).await.unwrap();

        // Full read
        let result = store.get(&path).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(data.as_ref(), &payload[..], "full read failed for file {i} size {file_size}");

        // Random bounded range
        if file_size > 1 {
            let a = rng.gen_range(0..file_size);
            let b = rng.gen_range(0..file_size);
            let (start, end) = if a <= b { (a, b + 1) } else { (b, a + 1) };
            let end = end.min(file_size);
            let data = get_bounded(&store, &path, start..end).await;
            assert_eq!(
                data.as_ref(),
                &payload[start..end],
                "fuzz file {i} bounded {start}..{end} size {file_size}"
            );
        }

        // Random suffix
        let suffix_len = rng.gen_range(1..=file_size);
        let data = get_suffix(&store, &path, suffix_len).await;
        assert_eq!(
            data.as_ref(),
            &payload[file_size - suffix_len..],
            "fuzz file {i} suffix {suffix_len} size {file_size}"
        );
    }
}

#[tokio::test]
async fn range_reads_after_overwrite() {
    let (store, _tmp) = make_store();
    let path = Path::from("overwrite/data.bin");

    // Write a 50KB file, overwrite with 100KB, verify ranges on the new one
    let payload_v1 = make_payload(50_000);
    store.put(&path, PutPayload::from(payload_v1)).await.unwrap();

    let payload_v2: Vec<u8> = (0..100_000).map(|i| ((i * 7 + 3) % 251) as u8).collect();
    store.put(&path, PutPayload::from(payload_v2.clone())).await.unwrap();

    // Verify ranges on v2
    let data = get_bounded(&store, &path, 0..100).await;
    assert_eq!(data.as_ref(), &payload_v2[0..100]);

    let data = get_bounded(&store, &path, 49_000..51_000).await;
    assert_eq!(data.as_ref(), &payload_v2[49_000..51_000]);

    let data = get_bounded(&store, &path, 99_900..100_000).await;
    assert_eq!(data.as_ref(), &payload_v2[99_900..100_000]);

    let data = get_offset(&store, &path, 99_990).await;
    assert_eq!(data.as_ref(), &payload_v2[99_990..]);
}

#[tokio::test]
async fn range_reads_multiple_files_same_store() {
    let (store, _tmp) = make_store();
    let mut rng = StdRng::seed_from_u64(555);

    // Write 20 files of varying sizes, then do range reads on each
    let mut files: Vec<(Path, Vec<u8>)> = Vec::new();
    for i in 0..20 {
        let size = rng.gen_range(1000..500_000);
        let key = format!("multi/{i}.bin");
        let path = Path::from(key.as_str());
        let payload = make_payload(size);
        store.put(&path, PutPayload::from(payload.clone())).await.unwrap();
        files.push((path, payload));
    }

    // Random range reads on each file
    for (path, payload) in &files {
        let size = payload.len();
        let start = rng.gen_range(0..size);
        let len = rng.gen_range(1..=(size - start).min(50_000));
        let end = start + len;

        let data = get_bounded(&store, path, start..end).await;
        assert_eq!(
            data.as_ref(),
            &payload[start..end],
            "multi-file range mismatch for {} at {start}..{end}",
            path
        );
    }
}
