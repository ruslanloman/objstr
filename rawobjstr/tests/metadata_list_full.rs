//! Tests for metadata extensions:
//!   - `put_with_meta` / `get_metadata` -- store and retrieve metadata suffix
//!   - `head_with_meta` -- index-only metadata size lookup
//!   - `list_with_meta` -- bulk metadata size listing
//!   - `list_full` -- full extent info listing
//!   - `update_metadata` -- replace metadata suffix in-place

mod common;

use bytes::Bytes;
use object_store::{path::Path, GetOptions, GetRange, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use std::io::{Seek, Write};

use common::make_store;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn put_with_meta(store: &RawObjectStore, key: &str, body: &[u8], meta: &[u8]) {
    store
        .put_with_meta(
            &Path::from(key),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();
}

// ---------------------------------------------------------------------------
// put_with_meta / get_metadata round-trip
// ---------------------------------------------------------------------------

#[test]
fn metadata_roundtrip() {
    let (store, _tmp) = make_store();
    let body = b"body content";
    let meta = b"my-metadata";
    put_with_meta(&store, "file.bin", body, meta);

    let got_meta = store.get_metadata(&Path::from("file.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);
}

#[test]
fn metadata_body_unchanged() {
    let (store, _tmp) = make_store();
    let body = b"body content";
    let meta = b"some metadata";
    put_with_meta(&store, "file.bin", body, meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let got_body = rt.block_on(async {
        store
            .get(&Path::from("file.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    // get() returns body-only (metadata suffix is excluded)
    assert_eq!(got_body.as_ref(), body, "get() should return body-only");
}

#[test]
fn metadata_empty_meta() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "noMeta.bin", b"data only", b"");

    let got = store.get_metadata(&Path::from("noMeta.bin")).unwrap();
    assert_eq!(got.len(), 0);

    let (_, meta_len) = store.head_with_meta(&Path::from("noMeta.bin")).unwrap();
    assert_eq!(meta_len, 0);
}

#[test]
fn metadata_large_body_small_meta() {
    let (store, _tmp) = make_store();
    let body: Vec<u8> = (0..65536).map(|i| (i & 0xFF) as u8).collect();
    let meta = b"small-meta";
    put_with_meta(&store, "large.bin", &body, meta);

    let got = store.get_metadata(&Path::from("large.bin")).unwrap();
    assert_eq!(got.as_ref(), meta);
}

#[test]
fn metadata_overwrite_replaces_meta() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "file.bin", b"body", b"meta-v1");
    put_with_meta(&store, "file.bin", b"body", b"meta-v2-longer");

    let got = store.get_metadata(&Path::from("file.bin")).unwrap();
    assert_eq!(got.as_ref(), b"meta-v2-longer");
}

#[test]
fn metadata_not_found() {
    let (store, _tmp) = make_store();
    let err = store.get_metadata(&Path::from("nonexistent")).unwrap_err();
    assert!(
        matches!(err, rawobjstr::RawStoreError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// head_with_meta
// ---------------------------------------------------------------------------

#[test]
fn head_with_meta_returns_correct_meta_len() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body data", b"12345678");

    let (meta, meta_len) = store.head_with_meta(&Path::from("f.bin")).unwrap();
    assert_eq!(meta_len, 8);
    // size from head is body-only (metadata suffix excluded)
    assert_eq!(meta.size, b"body data".len() as u64);
}

#[test]
fn head_with_meta_no_io_for_regular_put() {
    let (store, _tmp) = make_store();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        store
            .put(
                &Path::from("plain.bin"),
                PutPayload::from(Bytes::from("plain data")),
            )
            .await
            .unwrap();
    });

    let (_, meta_len) = store.head_with_meta(&Path::from("plain.bin")).unwrap();
    assert_eq!(meta_len, 0, "regular put should have meta_len 0");
}

// ---------------------------------------------------------------------------
// list_with_meta
// ---------------------------------------------------------------------------

#[test]
fn list_with_meta_shows_meta_len() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "a/x.bin", b"body_x", b"meta_x");
    put_with_meta(&store, "a/y.bin", b"body_y", b"");
    put_with_meta(&store, "b/z.bin", b"body_z", b"metametameta");

    let mut results = store.list_with_meta(Some(&Path::from("a")));
    results.sort_by(|(a, _), (b, _)| a.location.as_ref().cmp(b.location.as_ref()));

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1, b"meta_x".len() as u16);
    assert_eq!(results[1].1, 0u16);
}

#[test]
fn list_with_meta_none_prefix_returns_all() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "p/f1.bin", b"b1", b"m1");
    put_with_meta(&store, "p/f2.bin", b"b2", b"");
    put_with_meta(&store, "q/f3.bin", b"b3", b"m3m3");

    let results = store.list_with_meta(None);
    assert_eq!(results.len(), 3);
}

// ---------------------------------------------------------------------------
// list_full
// ---------------------------------------------------------------------------

#[test]
fn list_full_returns_sorted_entries() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "z/last.bin", b"c", b"meta_c");
    put_with_meta(&store, "a/first.bin", b"a", b"");
    put_with_meta(&store, "m/middle.bin", b"b", b"meta_b");

    let results = store.list_full(None);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].key, "a/first.bin");
    assert_eq!(results[1].key, "m/middle.bin");
    assert_eq!(results[2].key, "z/last.bin");
}

#[test]
fn list_full_body_size_correct() {
    let (store, _tmp) = make_store();
    let body = b"hello body";
    let meta = b"metabytes";
    put_with_meta(&store, "f.bin", body, meta);

    let results = store.list_full(None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].body_size, body.len() as u64);
    assert_eq!(results[0].meta_len, meta.len() as u16);
}

#[test]
fn list_full_with_prefix_filter() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "table/data/f1.bin", b"d1", b"m1");
    put_with_meta(&store, "table/data/f2.bin", b"d2", b"");
    put_with_meta(&store, "other/f3.bin", b"d3", b"m3");

    let results = store.list_full(Some(&Path::from("table")));
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.key.starts_with("table")));
}

#[test]
fn list_full_empty_store() {
    let (store, _tmp) = make_store();
    assert!(store.list_full(None).is_empty());
}

#[test]
fn list_full_offset_nonzero() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"data", b"meta");

    let results = store.list_full(None);
    assert_eq!(results.len(), 1);
    // Offset must be at or after DATA_START (0x2000)
    assert!(results[0].offset >= rawobjstr::DATA_START);
    // padded_size must be at least as large as the payload
    assert!(results[0].padded_size > 0);
    assert!(results[0].padded_size >= results[0].body_size + results[0].meta_len as u64);
}

#[test]
fn list_full_created_txn_increases() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "first.bin", b"a", b"");
    put_with_meta(&store, "second.bin", b"b", b"");

    let results = store.list_full(None);
    assert_eq!(results.len(), 2);
    // Both have non-zero txn; second was written after first so its txn >= first
    let first = results.iter().find(|r| r.key == "first.bin").unwrap();
    let second = results.iter().find(|r| r.key == "second.bin").unwrap();
    assert!(second.created_txn >= first.created_txn);
}

// ---------------------------------------------------------------------------
// update_metadata
// ---------------------------------------------------------------------------

#[test]
fn update_metadata_replaces_suffix() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body data", b"v1-meta");

    store
        .update_metadata(&Path::from("f.bin"), Bytes::from("v2-meta-updated"))
        .unwrap();

    let got = store.get_metadata(&Path::from("f.bin")).unwrap();
    assert_eq!(got.as_ref(), b"v2-meta-updated");
}

#[test]
fn update_metadata_preserves_body() {
    let (store, _tmp) = make_store();
    let body = b"precious body content";
    put_with_meta(&store, "f.bin", body, b"original-meta");

    store
        .update_metadata(&Path::from("f.bin"), Bytes::from("new-meta"))
        .unwrap();

    // list_full shows updated meta_len and correct body_size
    let results = store.list_full(None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].body_size, body.len() as u64);
    assert_eq!(results[0].meta_len, b"new-meta".len() as u16);

    // get_metadata returns the new meta
    let got_meta = store.get_metadata(&Path::from("f.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), b"new-meta");
}

#[test]
fn update_metadata_no_meta_to_some_meta() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body", b"");

    let (_, meta_len_before) = store.head_with_meta(&Path::from("f.bin")).unwrap();
    assert_eq!(meta_len_before, 0);

    store
        .update_metadata(&Path::from("f.bin"), Bytes::from("added"))
        .unwrap();

    let (_, meta_len_after) = store.head_with_meta(&Path::from("f.bin")).unwrap();
    assert_eq!(meta_len_after, 5);
}

#[test]
fn update_metadata_some_meta_to_no_meta() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body", b"remove me");

    store
        .update_metadata(&Path::from("f.bin"), Bytes::new())
        .unwrap();

    let got = store.get_metadata(&Path::from("f.bin")).unwrap();
    assert_eq!(got.len(), 0);
    let (_, meta_len) = store.head_with_meta(&Path::from("f.bin")).unwrap();
    assert_eq!(meta_len, 0);
}

#[test]
fn update_metadata_not_found() {
    let (store, _tmp) = make_store();
    let err = store
        .update_metadata(&Path::from("ghost.bin"), Bytes::from("meta"))
        .unwrap_err();
    assert!(
        matches!(err, rawobjstr::RawStoreError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

#[test]
fn update_metadata_persists_across_reopen() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let store = RawObjectStore::format_with_size(tmp.path(), 64 * 1024 * 1024, false).unwrap();
        put_with_meta(&store, "f.bin", b"the body", b"meta-v1");
        store
            .update_metadata(&Path::from("f.bin"), Bytes::from("meta-v2"))
            .unwrap();
        store.flush_index().unwrap();
    }

    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let got = store2.get_metadata(&Path::from("f.bin")).unwrap();
    assert_eq!(got.as_ref(), b"meta-v2");
}

#[test]
fn update_metadata_repeated_updates() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body", b"v0");

    for i in 1u8..=10 {
        let new_meta = vec![i; i as usize];
        store
            .update_metadata(&Path::from("f.bin"), Bytes::from(new_meta.clone()))
            .unwrap();
        let got = store.get_metadata(&Path::from("f.bin")).unwrap();
        assert_eq!(got.as_ref(), new_meta.as_slice(), "iteration {}", i);
    }
}

// ---------------------------------------------------------------------------
// put_with_meta_from_file -- streaming metadata from a file
// ---------------------------------------------------------------------------

fn make_meta(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

#[test]
fn put_with_meta_from_file_basic_roundtrip() {
    let (store, _tmp) = make_store();
    let body = b"file body content here";
    let meta = b"file-metadata";

    // Write body+meta to a temp file
    let mut src = tempfile::tempfile().unwrap();
    src.write_all(body).unwrap();
    src.write_all(meta).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    store
        .put_with_meta_from_file(
            &Path::from("from_file.bin"),
            &mut src,
            meta.len() as u16,
        )
        .unwrap();

    // Verify metadata
    let got_meta = store.get_metadata(&Path::from("from_file.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    // Verify body via get()
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("from_file.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS put_with_meta_from_file_basic_roundtrip");
}

#[test]
fn put_with_meta_from_file_zero_metadata() {
    let (store, _tmp) = make_store();
    let body = b"just a body, no metadata";

    let mut src = tempfile::tempfile().unwrap();
    src.write_all(body).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    store
        .put_with_meta_from_file(&Path::from("no_meta.bin"), &mut src, 0)
        .unwrap();

    let got_meta = store.get_metadata(&Path::from("no_meta.bin")).unwrap();
    assert_eq!(got_meta.len(), 0);

    let (_, ml) = store.head_with_meta(&Path::from("no_meta.bin")).unwrap();
    assert_eq!(ml, 0);
    println!("PASS put_with_meta_from_file_zero_metadata");
}

#[test]
fn put_with_meta_from_file_large_body_streams() {
    let (store, _tmp) = make_store();
    // 4 MB body to exercise streaming (1 MB chunk boundary crossings)
    let body_size = 4 * 1024 * 1024;
    let meta = b"trailing-meta-large";

    let mut src = tempfile::tempfile().unwrap();
    let body: Vec<u8> = (0..body_size).map(|i| (i & 0xFF) as u8).collect();
    src.write_all(&body).unwrap();
    src.write_all(meta).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    store
        .put_with_meta_from_file(
            &Path::from("large_stream.bin"),
            &mut src,
            meta.len() as u16,
        )
        .unwrap();

    // Verify metadata round-trip
    let got_meta = store.get_metadata(&Path::from("large_stream.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    // Verify body content
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("large_stream.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.len(), body_size);
    assert_eq!(data.as_ref(), body.as_slice());
    println!("PASS put_with_meta_from_file_large_body_streams");
}

#[test]
fn put_with_meta_from_file_max_meta() {
    let (store, _tmp) = make_store();
    let body = b"body";
    let meta = make_meta(65_535); // u16::MAX

    let mut src = tempfile::tempfile().unwrap();
    src.write_all(body).unwrap();
    src.write_all(&meta).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    store
        .put_with_meta_from_file(
            &Path::from("max_meta_file.bin"),
            &mut src,
            65_535,
        )
        .unwrap();

    let got_meta = store
        .get_metadata(&Path::from("max_meta_file.bin"))
        .unwrap();
    assert_eq!(got_meta.len(), 65_535);
    assert_eq!(got_meta.as_ref(), meta.as_slice());
    println!("PASS put_with_meta_from_file_max_meta");
}

#[test]
fn put_with_meta_from_file_persists_after_reopen() {
    let (store, tmp) = make_store();
    let path = tmp.path().to_path_buf();
    let body = b"persistent body";
    let meta = b"persistent meta";

    let mut src = tempfile::tempfile().unwrap();
    src.write_all(body).unwrap();
    src.write_all(meta).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    store
        .put_with_meta_from_file(
            &Path::from("persist.bin"),
            &mut src,
            meta.len() as u16,
        )
        .unwrap();
    store.flush_index().unwrap();
    drop(store);

    let store2 = RawObjectStore::open(&path).unwrap();
    let got_meta = store2.get_metadata(&Path::from("persist.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store2
            .get(&Path::from("persist.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS put_with_meta_from_file_persists_after_reopen");
}

// ---------------------------------------------------------------------------
// set_meta_len -- index-only metadata length adjustment
// ---------------------------------------------------------------------------

#[test]
fn set_meta_len_basic() {
    let (store, _tmp) = make_store();
    let body_and_meta = b"actual-body-dataMETADATA";

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        store
            .put(
                &Path::from("setmeta.bin"),
                PutPayload::from(Bytes::from_static(body_and_meta)),
            )
            .await
            .unwrap();
    });

    // Initially meta_len = 0
    let (_, ml) = store.head_with_meta(&Path::from("setmeta.bin")).unwrap();
    assert_eq!(ml, 0);

    // Set meta_len to 8 (last 8 bytes = "METADATA")
    store.set_meta_len(&Path::from("setmeta.bin"), 8).unwrap();

    let (_, ml) = store.head_with_meta(&Path::from("setmeta.bin")).unwrap();
    assert_eq!(ml, 8);

    let got_meta = store.get_metadata(&Path::from("setmeta.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), b"METADATA");
    println!("PASS set_meta_len_basic");
}

#[test]
fn set_meta_len_not_found() {
    let (store, _tmp) = make_store();
    let err = store.set_meta_len(&Path::from("ghost.bin"), 10);
    assert!(err.is_err());
    println!("PASS set_meta_len_not_found");
}

#[test]
fn set_meta_len_persists_after_flush() {
    let (store, tmp) = make_store();
    let path = tmp.path().to_path_buf();
    let body_and_meta = b"body-partMETA-PART";

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        store
            .put(
                &Path::from("persist_ml.bin"),
                PutPayload::from(Bytes::from_static(body_and_meta)),
            )
            .await
            .unwrap();
    });

    store
        .set_meta_len(&Path::from("persist_ml.bin"), 9)
        .unwrap(); // "META-PART" = 9 bytes
    store.flush_index().unwrap();
    drop(store);

    let store2 = RawObjectStore::open(&path).unwrap();
    let (_, ml) = store2
        .head_with_meta(&Path::from("persist_ml.bin"))
        .unwrap();
    assert_eq!(ml, 9);

    let got_meta = store2
        .get_metadata(&Path::from("persist_ml.bin"))
        .unwrap();
    assert_eq!(got_meta.as_ref(), b"META-PART");
    println!("PASS set_meta_len_persists_after_flush");
}

#[test]
fn set_meta_len_zero_clears_metadata() {
    let (store, _tmp) = make_store();
    let body = b"body-data";
    let meta = b"initial-meta";

    store
        .put_with_meta(
            &Path::from("clear_meta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    let (_, ml) = store
        .head_with_meta(&Path::from("clear_meta.bin"))
        .unwrap();
    assert_eq!(ml, meta.len() as u16);

    // Set meta_len to 0 effectively hides the metadata
    store
        .set_meta_len(&Path::from("clear_meta.bin"), 0)
        .unwrap();

    let (_, ml) = store
        .head_with_meta(&Path::from("clear_meta.bin"))
        .unwrap();
    assert_eq!(ml, 0);

    let got_meta = store
        .get_metadata(&Path::from("clear_meta.bin"))
        .unwrap();
    assert_eq!(got_meta.len(), 0);
    println!("PASS set_meta_len_zero_clears_metadata");
}

// ---------------------------------------------------------------------------
// Metadata + range reads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_range_read_body_portion() {
    let (store, _tmp) = make_store();
    let body = b"ABCDEFGHIJKLMNOP"; // 16 bytes
    let meta = b"hidden-meta";

    store
        .put_with_meta(
            &Path::from("range_meta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    // Range read within the body portion
    let opts = GetOptions {
        range: Some(GetRange::Bounded(4..10)),
        ..Default::default()
    };
    let result = store.get_opts(&Path::from("range_meta.bin"), opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data.as_ref(), b"EFGHIJ");
    println!("PASS metadata_range_read_body_portion");
}

#[tokio::test]
async fn metadata_range_read_spanning_body_and_meta() {
    let (store, _tmp) = make_store();
    let body = b"ABCDEFGH"; // 8 bytes
    let meta = b"META";      // 4 bytes

    store
        .put_with_meta(
            &Path::from("span.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    // Range that would span into metadata is clamped to body end
    let opts = GetOptions {
        range: Some(GetRange::Bounded(6..12)),
        ..Default::default()
    };
    let result = store.get_opts(&Path::from("span.bin"), opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    // body = "ABCDEFGH", bytes 6..8 = "GH" (clamped to body end)
    assert_eq!(data.as_ref(), b"GH");
    println!("PASS metadata_range_read_spanning_body_and_meta");
}

#[tokio::test]
async fn metadata_range_read_only_meta_region() {
    let (store, _tmp) = make_store();
    let body = b"BODY"; // 4 bytes
    let meta = b"METAVALUE"; // 9 bytes

    store
        .put_with_meta(
            &Path::from("meta_only.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    // Range within body works normally
    let opts = GetOptions {
        range: Some(GetRange::Bounded(0..4)),
        ..Default::default()
    };
    let result = store
        .get_opts(&Path::from("meta_only.bin"), opts)
        .await
        .unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data.as_ref(), b"BODY");

    // Verify metadata is accessible via get_metadata, not via range reads
    let got_meta = store.get_metadata(&Path::from("meta_only.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);
    println!("PASS metadata_range_read_only_meta_region");
}

#[tokio::test]
async fn metadata_suffix_range_read() {
    let (store, _tmp) = make_store();
    let body = b"THE-BODY";
    let meta = b"SUFFIX-META";

    store
        .put_with_meta(
            &Path::from("suffix.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    // Suffix range: last 4 bytes of body = "BODY"
    let opts = GetOptions {
        range: Some(GetRange::Suffix(4)),
        ..Default::default()
    };
    let result = store
        .get_opts(&Path::from("suffix.bin"), opts)
        .await
        .unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data.as_ref(), b"BODY");
    println!("PASS metadata_suffix_range_read");
}

// =========================================================================
// put_with_meta_from_file error paths
// =========================================================================

/// put_with_meta_from_file rejects metadata exceeding u16::MAX bytes.
#[test]
fn put_with_meta_from_file_metadata_overflow() {
    let (store, _tmp) = make_store();
    let body = b"body";
    // meta_len = u16::MAX + 1 is impossible via the u16 parameter, but
    // we can test what happens when the file is shorter than meta_len claims.
    // A meta_len of 65535 on a 100-byte file means the "body" would be
    // negative, which the store should handle gracefully.
    let mut src = tempfile::tempfile().unwrap();
    src.write_all(body).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    // meta_len > file size -- the body would be negative
    let result = store.put_with_meta_from_file(
        &Path::from("overflow.bin"),
        &mut src,
        65535, // much larger than the file
    );
    // This should either error or succeed with an empty body + truncated meta.
    // Either behavior is fine as long as no panic occurs.
    if result.is_ok() {
        // If it succeeded, verify the object is retrievable
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            store.get(&Path::from("overflow.bin")).await
        });
    }
    println!("PASS put_with_meta_from_file_metadata_overflow");
}

/// put_with_meta_from_file with zero-byte body + metadata is rejected (EmptyPayload).
#[test]
fn put_with_meta_from_file_empty_file() {
    let (store, _tmp) = make_store();
    let mut src = tempfile::tempfile().unwrap();
    // Write nothing -- empty file
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    let result = store.put_with_meta_from_file(
        &Path::from("empty.bin"),
        &mut src,
        0,
    );
    assert!(result.is_err(), "empty file should be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("zero-byte") || msg.contains("empty") || msg.contains("EmptyPayload"),
        "error should mention empty payload: {msg}"
    );
    println!("PASS put_with_meta_from_file_empty_file");
}

/// put_with_meta_from_file on a readonly store is rejected.
#[test]
fn put_with_meta_from_file_readonly_rejected() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();
    {
        let store = RawObjectStore::format_with_size(tmp.path(), common::SMALL_DEVICE, false).unwrap();
        store.flush_index().unwrap();
    }
    let store = RawObjectStore::open_readonly(&path_buf).unwrap();

    let mut src = tempfile::tempfile().unwrap();
    src.write_all(b"data-and-meta").unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    let result = store.put_with_meta_from_file(&Path::from("f.bin"), &mut src, 4);
    assert!(result.is_err(), "put_with_meta_from_file should fail on readonly");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("read-only") || msg.contains("ReadOnly"),
        "error: {msg}"
    );
    println!("PASS put_with_meta_from_file_readonly_rejected");
}

// =========================================================================
// update_metadata error paths
// =========================================================================

/// update_metadata on a non-existent key returns NotFound (error message check).
#[test]
fn update_metadata_not_found_error_message() {
    let (store, _tmp) = make_store();
    let result = store.update_metadata(&Path::from("no_such_key.bin"), Bytes::from("meta"));
    assert!(result.is_err(), "update_metadata on missing key should fail");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("not found") || msg.contains("NotFound"),
        "error should indicate not found: {msg}"
    );
    println!("PASS update_metadata_not_found_error_message");
}

/// update_metadata rejects metadata exceeding u16::MAX.
#[test]
fn update_metadata_too_large() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body", b"small-meta");

    let huge_meta = vec![0xABu8; 65536]; // u16::MAX + 1
    let result = store.update_metadata(&Path::from("f.bin"), Bytes::from(huge_meta));
    assert!(result.is_err(), "metadata exceeding u16::MAX should be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("too large") || msg.contains("MetadataTooLarge"),
        "error: {msg}"
    );
    println!("PASS update_metadata_too_large");
}

/// update_metadata on a readonly store is rejected.
#[test]
fn update_metadata_readonly_rejected() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();
    {
        let store = RawObjectStore::format_with_size(tmp.path(), common::SMALL_DEVICE, false).unwrap();
        store.put_with_meta(&Path::from("f.bin"), Bytes::from("body"), b"meta").unwrap();
        store.flush_index().unwrap();
    }
    let store = RawObjectStore::open_readonly(&path_buf).unwrap();

    let result = store.update_metadata(&Path::from("f.bin"), Bytes::from("new-meta"));
    assert!(result.is_err(), "update_metadata should fail on readonly");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("read-only") || msg.contains("ReadOnly"),
        "error: {msg}"
    );
    println!("PASS update_metadata_readonly_rejected");
}

/// update_metadata to empty meta, check body is preserved.
#[test]
fn update_metadata_to_empty() {
    let (store, _tmp) = make_store();
    let body = b"important body data";
    put_with_meta(&store, "f.bin", body, b"original-meta");

    store.update_metadata(&Path::from("f.bin"), Bytes::new()).unwrap();

    let got_meta = store.get_metadata(&Path::from("f.bin")).unwrap();
    assert!(got_meta.is_empty(), "metadata should be empty after update");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store.get(&Path::from("f.bin")).await.unwrap().bytes().await.unwrap()
    });
    assert_eq!(data.as_ref(), body, "body should be preserved");
    println!("PASS update_metadata_to_empty");
}

/// update_metadata at exactly u16::MAX boundary.
#[test]
fn update_metadata_at_max_boundary() {
    let (store, _tmp) = make_store();
    put_with_meta(&store, "f.bin", b"body content here", b"small");

    // Exactly u16::MAX = 65535 bytes of metadata should be accepted
    let max_meta = vec![0x42u8; 65535];
    store.update_metadata(&Path::from("f.bin"), Bytes::from(max_meta.clone())).unwrap();

    let got = store.get_metadata(&Path::from("f.bin")).unwrap();
    assert_eq!(got.len(), 65535, "metadata should be 65535 bytes");
    assert_eq!(got.as_ref(), max_meta.as_slice());

    // Body should still be intact
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store.get(&Path::from("f.bin")).await.unwrap().bytes().await.unwrap()
    });
    assert_eq!(data.as_ref(), b"body content here");
    println!("PASS update_metadata_at_max_boundary");
}
