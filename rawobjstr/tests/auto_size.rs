//! Auto-size detection tests: verify that format without explicit --size
//! auto-detects the image size, and that fill-to-capacity works correctly.

mod common;

use std::fs::File;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::extent::padded_extent_size;
use rawobjstr::store::RawObjectStore;
use rawobjstr::{DATA_START, INDEX_TOTAL_SIZE, MIN_DEVICE_SIZE};
use tempfile::NamedTempFile;

use common::{make_chunk, make_small, CHUNK_SIZE};

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: Auto-size detection for various image sizes
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn auto_size_various_images() {
    let sizes: Vec<u64> = vec![
        MIN_DEVICE_SIZE + 8192,         // minimum + margin for one extent
        48 * 1024 * 1024,              // 48 MB
        64 * 1024 * 1024,              // 64 MB
        100 * 1024 * 1024,             // 100 MB (non-power-of-two)
        256 * 1024 * 1024,             // 256 MB
        512 * 1024 * 1024,             // 512 MB
    ];

    for &size in &sizes {
        let tmp = NamedTempFile::new().unwrap();
        let dev = tmp.path().to_str().unwrap();
        // Create the file at the right size first
        {
            let f = File::create(tmp.path()).unwrap();
            f.set_len(size).unwrap();
        }

        // format without --size should auto-detect the size
        common::run_cli(&["format", "--file", dev]);

        // Write a small payload that fits even on the smallest device
        let payload = make_small(0, 128);
        common::cli_put(dev, "test.bin", &payload);

        // Read it back
        let data = common::cli_get(dev, "test.bin");
        assert_eq!(data.len(), 128, "failed for size {size}");

        // Verify persistence (implicit -- next CLI call reopens)
        let data2 = common::cli_get(dev, "test.bin");
        assert_eq!(data2, &payload[..], "persistence failed for size {size}");

        println!("  auto_size OK: {} MB", size / (1024 * 1024));
    }
    println!("PASS auto_size_various_images: {} sizes tested", sizes.len());
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: Auto-size computes correct usable data area
// ═══════════════════════════════════════════════════════════════════════

/// Verify that auto-size computes the correct usable data area.
#[tokio::test]
async fn auto_size_fills_correctly() {
    let sizes: Vec<u64> = vec![
        64 * 1024 * 1024,
        128 * 1024 * 1024,
        256 * 1024 * 1024,
    ];

    for &size in &sizes {
        let tmp = NamedTempFile::new().unwrap();
        {
            let f = File::create(tmp.path()).unwrap();
            f.set_len(size).unwrap();
        }

        let store = RawObjectStore::format(tmp.path(), false).unwrap();
        let data_area = size - DATA_START - INDEX_TOTAL_SIZE;
        let extent_size = padded_extent_size(CHUNK_SIZE as u64).unwrap();
        let expected_chunks = (data_area / extent_size) as usize;

        // Fill to capacity
        for i in 0..expected_chunks {
            store
                .put(
                    &Path::from(format!("fill/{i:04}")),
                    PutPayload::from(make_chunk(i)),
                )
                .await
                .unwrap();
        }

        // Next write should fail with NoSpace
        let result = store
            .put(
                &Path::from("overflow"),
                PutPayload::from(make_chunk(9999)),
            )
            .await;
        assert!(result.is_err(), "should be full at {size} bytes");

        // Verify count
        let all: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(all.len(), expected_chunks, "wrong chunk count for {size}");

        println!(
            "  auto_size_fill OK: {} MB -> {expected_chunks} chunks",
            size / (1024 * 1024)
        );
    }
    println!("PASS auto_size_fills_correctly");
}
