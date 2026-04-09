//! Regression tests for metadata body-only semantics.
//!
//! These tests verify that the ObjectStore trait methods (get, head, list,
//! get_range) expose body-only content and sizes, excluding any metadata
//! suffix appended by put_with_meta. This prevents regressions on the
//! fixes for catalog-size vs body-size confusion.

mod common;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, GetOptions, GetRange, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use common::make_store;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn put_with_meta(store: &RawObjectStore, key: &str, body: &[u8], meta: &[u8]) {
    store
        .put_with_meta(&Path::from(key), Bytes::copy_from_slice(body), meta)
        .unwrap();
}

// ---------------------------------------------------------------------------
// get() returns body-only
// ---------------------------------------------------------------------------

#[test]
fn get_returns_body_only_bytes() {
    let (store, _tmp) = make_store();
    let body = b"the actual body content";
    let meta = b"should-not-appear-in-get";
    put_with_meta(&store, "test.bin", body, meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let got = rt.block_on(async {
        store
            .get(&Path::from("test.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });

    assert_eq!(
        got.as_ref(),
        body,
        "get() must return body-only, not body+metadata"
    );
}

#[test]
fn get_returns_body_only_for_large_metadata() {
    let (store, _tmp) = make_store();
    let body = vec![0xAAu8; 8192];
    let meta = vec![0xBBu8; 4000];
    put_with_meta(&store, "large_meta.bin", &body, &meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let got = rt.block_on(async {
        store
            .get(&Path::from("large_meta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });

    assert_eq!(got.len(), body.len(), "get() length should be body-only");
    assert_eq!(got.as_ref(), body.as_slice());
}

// ---------------------------------------------------------------------------
// head() reports body-only size
// ---------------------------------------------------------------------------

#[test]
fn head_reports_body_only_size() {
    let (store, _tmp) = make_store();
    let body = b"twelve bytes";
    let meta = b"metadata-suffix";
    put_with_meta(&store, "sized.bin", body, meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let obj_meta = rt.block_on(async {
        store.head(&Path::from("sized.bin")).await.unwrap()
    });

    assert_eq!(
        obj_meta.size, body.len() as u64,
        "head() size must be body-only, not body+metadata"
    );
}

#[test]
fn head_matches_get_length() {
    let (store, _tmp) = make_store();
    let body = vec![0xCCu8; 5000];
    let meta = vec![0xDDu8; 500];
    put_with_meta(&store, "match.bin", &body, &meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (head_size, get_len) = rt.block_on(async {
        let head = store.head(&Path::from("match.bin")).await.unwrap();
        let get = store
            .get(&Path::from("match.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        (head.size as usize, get.len())
    });

    assert_eq!(head_size, get_len, "head().size must equal get().bytes().len()");
    assert_eq!(head_size, body.len());
}

// ---------------------------------------------------------------------------
// list() reports body-only size
// ---------------------------------------------------------------------------

#[test]
fn list_reports_body_only_size() {
    let (store, _tmp) = make_store();
    let body = b"list body";
    let meta = b"list-meta";
    put_with_meta(&store, "listed.bin", body, meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let items: Vec<object_store::ObjectMeta> = rt.block_on(async {
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    });

    let item = items
        .into_iter()
        .find(|m| m.location == Path::from("listed.bin"))
        .unwrap();

    assert_eq!(
        item.size, body.len() as u64,
        "list() size must be body-only"
    );
}

// ---------------------------------------------------------------------------
// Range reads clamped to body
// ---------------------------------------------------------------------------

#[test]
fn get_range_bounded_does_not_leak_metadata() {
    let (store, _tmp) = make_store();
    let body = b"0123456789";
    let meta = b"SECRET";
    put_with_meta(&store, "range.bin", body, meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    // Request range that would extend into metadata area if not clamped
    let got = rt.block_on(async {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(5..100)),
            ..Default::default()
        };
        store
            .get_opts(&Path::from("range.bin"), opts)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });

    // Should get bytes 5..10 of body, NOT any metadata bytes
    assert_eq!(got.as_ref(), &body[5..], "range read must clamp to body end");
}

#[test]
fn get_range_suffix_does_not_leak_metadata() {
    let (store, _tmp) = make_store();
    let body = b"abcdefghij";
    let meta = b"HIDDEN";
    put_with_meta(&store, "suffix.bin", body, meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    // Suffix(3) should return last 3 bytes of body, not metadata
    let got = rt.block_on(async {
        let opts = GetOptions {
            range: Some(GetRange::Suffix(3)),
            ..Default::default()
        };
        store
            .get_opts(&Path::from("suffix.bin"), opts)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });

    assert_eq!(got.as_ref(), b"hij", "suffix read must be from body end, not total end");
}

// ---------------------------------------------------------------------------
// get_metadata edge cases
// ---------------------------------------------------------------------------

#[test]
fn get_metadata_returns_empty_for_no_metadata() {
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

    let got = store.get_metadata(&Path::from("plain.bin")).unwrap();
    assert_eq!(got.len(), 0, "no metadata should return empty bytes without panic");
}

#[test]
fn get_metadata_roundtrip_exact() {
    let (store, _tmp) = make_store();
    let meta = b"exact-metadata-bytes";
    put_with_meta(&store, "exact.bin", b"body", meta);

    let got = store.get_metadata(&Path::from("exact.bin")).unwrap();
    assert_eq!(got.as_ref(), meta);
}

// ---------------------------------------------------------------------------
// Objects without metadata are unaffected
// ---------------------------------------------------------------------------

#[test]
fn no_metadata_objects_unchanged() {
    let (store, _tmp) = make_store();
    let body = b"no meta here";

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        store
            .put(
                &Path::from("plain.bin"),
                PutPayload::from(Bytes::copy_from_slice(body)),
            )
            .await
            .unwrap();
    });

    let (got_body, head_size) = rt.block_on(async {
        let data = store
            .get(&Path::from("plain.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let head = store.head(&Path::from("plain.bin")).await.unwrap();
        (data, head.size)
    });

    assert_eq!(got_body.as_ref(), body);
    assert_eq!(head_size, body.len() as u64);
}
