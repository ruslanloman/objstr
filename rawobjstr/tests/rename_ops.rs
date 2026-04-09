//! Tests for rename() and rename_if_not_exists() -- happy paths, error cases,
//! persistence across flush/reopen, metadata preservation, compression, and
//! chained renames.

mod common;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::{Compression, DEFAULT_MAX_KEY_LENGTH, INDEX_REGION_SIZE};
use tempfile::NamedTempFile;

use common::make_store;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_compressed_store(compression: Compression) -> (RawObjectStore, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_options(
        tmp.path(),
        FormatOptions {
            device_size: common::MEDIUM_DEVICE,
            direct_io: false,
            index_slot_size: INDEX_REGION_SIZE,
            max_key_length: DEFAULT_MAX_KEY_LENGTH,
            compression,
        },
    )
    .unwrap();
    (store, tmp)
}

// =========================================================================
// rename() -- happy paths and error cases
// =========================================================================

#[tokio::test]
async fn rename_basic() {
    let (store, _tmp) = make_store();
    let src = Path::from("original.bin");
    let dst = Path::from("renamed.bin");
    let payload = Bytes::from("rename me");

    store.put(&src, PutPayload::from(payload.clone())).await.unwrap();
    store.rename(&src, &dst).await.unwrap();

    // Source should be gone
    let err = store.get(&src).await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "source should not exist after rename"
    );

    // Destination has the data
    let data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, payload);
    println!("PASS rename_basic");
}

#[tokio::test]
async fn rename_overwrites_existing_destination() {
    let (store, _tmp) = make_store();
    let src = Path::from("src.bin");
    let dst = Path::from("dst.bin");

    store
        .put(&dst, PutPayload::from(Bytes::from("old dest data")))
        .await
        .unwrap();
    store
        .put(&src, PutPayload::from(Bytes::from("new from source")))
        .await
        .unwrap();

    store.rename(&src, &dst).await.unwrap();

    let data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("new from source"));

    let err = store.get(&src).await;
    assert!(matches!(err, Err(object_store::Error::NotFound { .. })));
    println!("PASS rename_overwrites_existing_destination");
}

#[tokio::test]
async fn rename_source_not_found() {
    let (store, _tmp) = make_store();
    let err = store
        .rename(&Path::from("nonexistent.bin"), &Path::from("dst.bin"))
        .await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "rename missing source should be NotFound, got {err:?}"
    );
    println!("PASS rename_source_not_found");
}

#[tokio::test]
async fn rename_preserves_data_across_flush_reopen() {
    let (store, tmp) = make_store();
    let path = tmp.path().to_path_buf();
    let payload = Bytes::from("persist after rename");

    store
        .put(&Path::from("before.bin"), PutPayload::from(payload.clone()))
        .await
        .unwrap();
    store
        .rename(&Path::from("before.bin"), &Path::from("after.bin"))
        .await
        .unwrap();
    store.flush_index().unwrap();
    drop(store);

    let store2 = RawObjectStore::open(&path).unwrap();
    let err = store2.get(&Path::from("before.bin")).await;
    assert!(matches!(err, Err(object_store::Error::NotFound { .. })));

    let data = store2
        .get(&Path::from("after.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, payload);
    println!("PASS rename_preserves_data_across_flush_reopen");
}

#[tokio::test]
async fn rename_to_self_is_noop() {
    let (store, _tmp) = make_store();
    let p = Path::from("self.bin");
    let payload = Bytes::from("self rename");

    store.put(&p, PutPayload::from(payload.clone())).await.unwrap();
    // Rename to the same key -- should succeed and data should remain
    store.rename(&p, &p).await.unwrap();

    let data = store.get(&p).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, payload);
    println!("PASS rename_to_self_is_noop");
}

#[tokio::test]
async fn rename_chain() {
    let (store, _tmp) = make_store();
    let payload = Bytes::from("chained rename data");

    store
        .put(&Path::from("step0.bin"), PutPayload::from(payload.clone()))
        .await
        .unwrap();

    for i in 0..5 {
        let src = Path::from(format!("step{i}.bin"));
        let dst = Path::from(format!("step{}.bin", i + 1));
        store.rename(&src, &dst).await.unwrap();
    }

    // Only step5.bin should exist
    let data = store
        .get(&Path::from("step5.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, payload);

    for i in 0..5 {
        let err = store.get(&Path::from(format!("step{i}.bin"))).await;
        assert!(
            matches!(err, Err(object_store::Error::NotFound { .. })),
            "step{i}.bin should not exist"
        );
    }
    println!("PASS rename_chain");
}

// =========================================================================
// rename_if_not_exists() -- happy paths and error cases
// =========================================================================

#[tokio::test]
async fn rename_if_not_exists_basic() {
    let (store, _tmp) = make_store();
    let src = Path::from("src_cond.bin");
    let dst = Path::from("dst_cond.bin");
    let payload = Bytes::from("conditional rename");

    store
        .put(&src, PutPayload::from(payload.clone()))
        .await
        .unwrap();
    store.rename_if_not_exists(&src, &dst).await.unwrap();

    let err = store.get(&src).await;
    assert!(matches!(err, Err(object_store::Error::NotFound { .. })));

    let data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, payload);
    println!("PASS rename_if_not_exists_basic");
}

#[tokio::test]
async fn rename_if_not_exists_destination_exists_rejected() {
    let (store, _tmp) = make_store();
    let src = Path::from("src_dup.bin");
    let dst = Path::from("dst_dup.bin");

    store
        .put(&src, PutPayload::from(Bytes::from("source")))
        .await
        .unwrap();
    store
        .put(&dst, PutPayload::from(Bytes::from("existing dest")))
        .await
        .unwrap();

    let err = store.rename_if_not_exists(&src, &dst).await;
    assert!(
        matches!(err, Err(object_store::Error::AlreadyExists { .. })),
        "expected AlreadyExists, got {err:?}"
    );

    // Both should still exist with original data
    let src_data = store.get(&src).await.unwrap().bytes().await.unwrap();
    assert_eq!(src_data, Bytes::from("source"));

    let dst_data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(dst_data, Bytes::from("existing dest"));
    println!("PASS rename_if_not_exists_destination_exists_rejected");
}

#[tokio::test]
async fn rename_if_not_exists_source_not_found() {
    let (store, _tmp) = make_store();
    let err = store
        .rename_if_not_exists(&Path::from("ghost.bin"), &Path::from("target.bin"))
        .await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "expected NotFound, got {err:?}"
    );
    println!("PASS rename_if_not_exists_source_not_found");
}

#[tokio::test]
async fn rename_if_not_exists_persists_after_flush() {
    let (store, tmp) = make_store();
    let path = tmp.path().to_path_buf();
    let payload = Bytes::from("persist conditional");

    store
        .put(
            &Path::from("cond_src.bin"),
            PutPayload::from(payload.clone()),
        )
        .await
        .unwrap();
    store
        .rename_if_not_exists(&Path::from("cond_src.bin"), &Path::from("cond_dst.bin"))
        .await
        .unwrap();
    store.flush_index().unwrap();
    drop(store);

    let store2 = RawObjectStore::open(&path).unwrap();
    let data = store2
        .get(&Path::from("cond_dst.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, payload);
    println!("PASS rename_if_not_exists_persists_after_flush");
}

// =========================================================================
// Rename with metadata preserved
// =========================================================================

#[tokio::test]
async fn rename_preserves_metadata() {
    let (store, _tmp) = make_store();
    let body = b"rename with meta";
    let meta = b"preserved-meta";

    store
        .put_with_meta(
            &Path::from("meta_src.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    store
        .rename(&Path::from("meta_src.bin"), &Path::from("meta_dst.bin"))
        .await
        .unwrap();

    let got_meta = store
        .get_metadata(&Path::from("meta_dst.bin"))
        .unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    let (_, ml) = store
        .head_with_meta(&Path::from("meta_dst.bin"))
        .unwrap();
    assert_eq!(ml, meta.len() as u16);
    println!("PASS rename_preserves_metadata");
}

// =========================================================================
// Rename with compression
// =========================================================================

#[tokio::test]
async fn rename_on_compressed_store() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let payload = Bytes::from("compressible data compressible data for rename");

    store
        .put(
            &Path::from("comp_src.bin"),
            PutPayload::from(payload.clone()),
        )
        .await
        .unwrap();

    store
        .rename(&Path::from("comp_src.bin"), &Path::from("comp_dst.bin"))
        .await
        .unwrap();

    let data = store
        .get(&Path::from("comp_dst.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, payload);
    println!("PASS rename_on_compressed_store");
}
