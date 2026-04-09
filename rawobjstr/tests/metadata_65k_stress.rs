//! Stress test for per-object metadata size limits.
//!
//! The `meta_len` field is a u16, so the maximum metadata per object is
//! 65 535 bytes.  These tests verify:
//!   1. Metadata at exactly 65 535 bytes round-trips correctly.
//!   2. Metadata at 65 536 bytes (u16 overflow) is rejected, not silently
//!      truncated.
//!   3. Various large metadata sizes survive flush + reopen.

mod common;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use rawobjstr::store::RawObjectStore;

use common::make_store;

// ---------------------------------------------------------------------------
// Helper: deterministic metadata payload of a given size
// ---------------------------------------------------------------------------
fn make_meta(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

// ---------------------------------------------------------------------------
// 1. Max valid metadata (65 535 bytes) round-trips through put/get/reopen
// ---------------------------------------------------------------------------
#[tokio::test]
async fn metadata_max_u16_roundtrip() {
    let (store, tmp) = make_store();
    let path = tmp.path().to_path_buf();

    let body = b"hello world";
    let meta = make_meta(65_535); // u16::MAX

    store
        .put_with_meta(&Path::from("big_meta.bin"), Bytes::from_static(body), &meta)
        .unwrap();

    // Read back metadata before flush
    let got = store.get_metadata(&Path::from("big_meta.bin")).unwrap();
    assert_eq!(got.len(), 65_535, "metadata length before flush");
    assert_eq!(&got[..], &meta[..], "metadata content before flush");

    // head_with_meta should report the correct meta_len
    let (_obj_meta, meta_len) = store.head_with_meta(&Path::from("big_meta.bin")).unwrap();
    assert_eq!(meta_len, 65_535, "head_with_meta meta_len");

    // Body should still be intact (get() returns body-only, metadata stripped)
    let data = store
        .get(&Path::from("big_meta.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&data[..], body, "body content with max metadata");

    // Flush and reopen
    store.flush_index().expect("flush with 65535-byte metadata");
    drop(store);
    let store2 = RawObjectStore::open(&path).unwrap();

    let got2 = store2.get_metadata(&Path::from("big_meta.bin")).unwrap();
    assert_eq!(got2.len(), 65_535, "metadata length after reopen");
    assert_eq!(&got2[..], &meta[..], "metadata content after reopen");

    let data2 = store2
        .get(&Path::from("big_meta.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&data2[..], body, "body content after reopen");

    println!("PASS metadata_max_u16_roundtrip: 65535-byte metadata stored and recovered");
}

// ---------------------------------------------------------------------------
// 2. Metadata at 65 536 bytes should be REJECTED (u16 overflow)
// ---------------------------------------------------------------------------
#[test]
fn metadata_overflow_u16_rejected() {
    let (store, _tmp) = make_store();

    let body = b"payload";
    let meta = make_meta(65_536); // one byte over u16::MAX

    let result = store.put_with_meta(
        &Path::from("overflow.bin"),
        Bytes::from_static(body),
        &meta,
    );

    // The store should reject this rather than silently truncating meta_len.
    // If it does NOT reject it, meta_len wraps to 0 and get_metadata returns
    // empty -- a data-corruption bug.
    match result {
        Err(e) => {
            println!("Correctly rejected 65536-byte metadata: {e}");
        }
        Ok(()) => {
            // If put succeeded, check whether meta_len was silently truncated
            let got = store.get_metadata(&Path::from("overflow.bin")).unwrap();
            if got.len() != 65_536 {
                panic!(
                    "BUG: put_with_meta accepted 65536-byte metadata but \
                     get_metadata returned {} bytes (expected rejection or 65536). \
                     meta_len was silently truncated!",
                    got.len()
                );
            }
            // If somehow it stored all 65536 bytes correctly, that is fine too
            // (means the store upgraded to a wider field).
            println!("put_with_meta accepted 65536 bytes and stored them correctly");
        }
    }
    println!("PASS metadata_overflow_u16_rejected");
}

// ---------------------------------------------------------------------------
// 3. Multiple objects with various large metadata sizes
// ---------------------------------------------------------------------------
#[tokio::test]
async fn metadata_various_large_sizes() {
    let (store, tmp) = make_store();
    let path = tmp.path().to_path_buf();

    let sizes: Vec<usize> = vec![
        0,       // no metadata
        1,       // minimal
        4_096,   // one block
        32_768,  // 32 KB -- enough for a small thumbnail
        60_000,  // close to limit
        65_534,  // one below max
        65_535,  // exactly max
    ];

    // Write objects with different metadata sizes
    for (i, &sz) in sizes.iter().enumerate() {
        let key = format!("sized/{i:02}_meta_{sz}.bin");
        let body_content = format!("body-{i}");
        let meta = make_meta(sz);
        store
            .put_with_meta(
                &Path::from(key.as_str()),
                Bytes::from(body_content.clone()),
                &meta,
            )
            .unwrap_or_else(|e| panic!("put_with_meta size={sz}: {e}"));
    }

    // Flush + reopen
    store.flush_index().expect("flush with various meta sizes");
    drop(store);
    let store2 = RawObjectStore::open(&path).unwrap();

    // Verify each object
    for (i, &sz) in sizes.iter().enumerate() {
        let key = format!("sized/{i:02}_meta_{sz}.bin");
        let expected_body = format!("body-{i}");
        let expected_meta = make_meta(sz);

        // Check metadata
        let got_meta = store2.get_metadata(&Path::from(key.as_str())).unwrap();
        assert_eq!(
            got_meta.len(), sz,
            "object {key}: metadata length mismatch after reopen"
        );
        if sz > 0 {
            assert_eq!(
                &got_meta[..], &expected_meta[..],
                "object {key}: metadata content mismatch after reopen"
            );
        }

        // Check head_with_meta
        let (_obj, ml) = store2.head_with_meta(&Path::from(key.as_str())).unwrap();
        assert_eq!(ml as usize, sz, "object {key}: head meta_len mismatch");

        // Check body (get() returns body-only, metadata is stripped)
        let data = store2
            .get(&Path::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&data[..]).unwrap(),
            expected_body,
            "object {key}: body mismatch"
        );

        println!("  verified {key}: body OK, meta {sz} bytes OK");
    }

    println!("PASS metadata_various_large_sizes: all sizes verified after reopen");
}
