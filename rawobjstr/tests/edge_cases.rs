//! Edge-case tests: GetRange boundaries, zero-byte files, alignment edges,
//! PutMode::Update, copy_if_not_exists, multipart abort, error paths, etc.

mod common;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    path::Path, GetOptions, GetRange, MultipartUpload, ObjectStore, PutMode, PutOptions,
    PutPayload,
};
use rawobjstr::store::RawObjectStore;
use rawobjstr::MIN_DEVICE_SIZE;
use rawobjstr::store::FormatOptions;
use rawobjstr::Compression;
use tempfile::NamedTempFile;

use common::make_store;

// ═══════════════════════════════════════════════════════════════════════
// GetRange edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn get_range_bounded_start_beyond_eof() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    // Bounded range where start >= total_size should return empty
    let opts = GetOptions {
        range: Some(GetRange::Bounded(100..200)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert!(data.is_empty(), "start beyond EOF should return empty, got {} bytes", data.len());
}

#[tokio::test]
async fn get_range_bounded_end_exceeds_size() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    // end > total_size should be clamped
    let opts = GetOptions {
        range: Some(GetRange::Bounded(2..1000)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("llo"), "end should be clamped to total_size");
}

#[tokio::test]
async fn get_range_bounded_zero_length() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    // Range where start == end should return empty
    let opts = GetOptions {
        range: Some(GetRange::Bounded(3..3)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert!(data.is_empty(), "zero-length range should be empty");
}

#[tokio::test]
async fn get_range_offset_beyond_eof() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Offset(100)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert!(data.is_empty(), "offset beyond EOF should return empty");
}

#[tokio::test]
async fn get_range_offset_at_start() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Offset(0)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("hello"), "offset 0 should return entire file");
}

#[tokio::test]
async fn get_range_suffix_zero() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Suffix(0)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert!(data.is_empty(), "suffix(0) should return empty");
}

#[tokio::test]
async fn get_range_suffix_exceeds_file_size() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello")))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Suffix(1000)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("hello"), "suffix > size should return entire file");
}

#[tokio::test]
async fn get_range_suffix_partial() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello world")))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Suffix(5)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("world"), "suffix(5) should return last 5 bytes");
}

#[tokio::test]
async fn get_range_offset_mid_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("range_test.bin");
    store
        .put(&path, PutPayload::from(Bytes::from("hello world")))
        .await
        .unwrap();

    let opts = GetOptions {
        range: Some(GetRange::Offset(6)),
        ..Default::default()
    };
    let result = store.get_opts(&path, opts).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("world"), "offset(6) should return from byte 6 onwards");
}

// ═══════════════════════════════════════════════════════════════════════
// Zero-byte and alignment boundary files
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn zero_byte_file_put_get() {
    let (store, _tmp) = make_store();
    let path = Path::from("empty.bin");

    // Zero-byte objects are not supported by the store spec.
    let result = store
        .put(&path, PutPayload::from(Bytes::new()))
        .await;
    assert!(result.is_err(), "zero-byte put should be rejected");
}

#[tokio::test]
async fn exact_alignment_boundary_4064() {
    // 32-byte header + 4064-byte payload = 4096 exactly aligned
    let (store, _tmp) = make_store();
    let path = Path::from("exact_align.bin");
    let payload = Bytes::from(vec![0xAA; 4064]);

    store
        .put(&path, PutPayload::from(payload.clone()))
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, payload);
}

#[tokio::test]
async fn alignment_boundary_plus_one_4065() {
    // 32-byte header + 4065-byte payload = 4097 -> padded to 8192
    let (store, _tmp) = make_store();
    let path = Path::from("align_plus_one.bin");
    let payload = Bytes::from(vec![0xBB; 4065]);

    store
        .put(&path, PutPayload::from(payload.clone()))
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, payload);
}

#[tokio::test]
async fn one_byte_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("one.bin");
    let payload = Bytes::from(vec![0x42]);

    store
        .put(&path, PutPayload::from(payload.clone()))
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, payload);
}

#[tokio::test]
async fn various_sizes_systematic() {
    let (store, _tmp) = make_store();
    let sizes = [1, 100, 4064, 4065, 4096, 8160, 8161, 16384, 65536];

    for &size in &sizes {
        let path = Path::from(format!("size_{size}.bin"));
        let payload = Bytes::from(vec![(size & 0xFF) as u8; size]);

        store
            .put(&path, PutPayload::from(payload.clone()))
            .await
            .unwrap();

        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len(), size, "size {size} round-trip failed");
        assert_eq!(data, payload, "size {size} content mismatch");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// PutMode edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn put_mode_update_acts_as_overwrite() {
    let (store, _tmp) = make_store();
    let path = Path::from("update_test.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("original")))
        .await
        .unwrap();

    // PutMode::Update should be treated as overwrite (no e_tag support)
    let opts = PutOptions {
        mode: PutMode::Update(object_store::UpdateVersion {
            e_tag: None,
            version: None,
        }),
        ..Default::default()
    };
    store
        .put_opts(&path, PutPayload::from(Bytes::from("updated")), opts)
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("updated"));
}

#[tokio::test]
async fn put_mode_create_on_new_key() {
    let (store, _tmp) = make_store();
    let path = Path::from("create_new.bin");

    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    store
        .put_opts(&path, PutPayload::from(Bytes::from("new file")), opts)
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("new file"));
}

#[tokio::test]
async fn put_mode_create_rejects_existing() {
    let (store, _tmp) = make_store();
    let path = Path::from("create_dup.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("first")))
        .await
        .unwrap();

    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let err = store
        .put_opts(&path, PutPayload::from(Bytes::from("second")), opts)
        .await;
    assert!(
        matches!(err, Err(object_store::Error::AlreadyExists { .. })),
        "expected AlreadyExists, got {err:?}"
    );

    // Original data should be unchanged
    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("first"));
}

// ═══════════════════════════════════════════════════════════════════════
// copy and copy_if_not_exists edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn copy_source_not_found() {
    let (store, _tmp) = make_store();
    let err = store
        .copy(&Path::from("nonexistent"), &Path::from("dest"))
        .await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn copy_if_not_exists_happy_path() {
    let (store, _tmp) = make_store();
    let src = Path::from("src.bin");
    let dst = Path::from("dst.bin");

    store
        .put(&src, PutPayload::from(Bytes::from("copy me")))
        .await
        .unwrap();

    store.copy_if_not_exists(&src, &dst).await.unwrap();

    let data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("copy me"));
}

#[tokio::test]
async fn copy_if_not_exists_source_missing() {
    let (store, _tmp) = make_store();
    let err = store
        .copy_if_not_exists(&Path::from("nonexistent"), &Path::from("dest"))
        .await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn copy_if_not_exists_dest_exists() {
    let (store, _tmp) = make_store();
    let src = Path::from("src.bin");
    let dst = Path::from("dst.bin");

    store
        .put(&src, PutPayload::from(Bytes::from("source")))
        .await
        .unwrap();
    store
        .put(&dst, PutPayload::from(Bytes::from("existing")))
        .await
        .unwrap();

    let err = store.copy_if_not_exists(&src, &dst).await;
    assert!(
        matches!(err, Err(object_store::Error::AlreadyExists { .. })),
        "expected AlreadyExists, got {err:?}"
    );

    // Dest should be unchanged
    let data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("existing"));
}

#[tokio::test]
async fn copy_to_same_location() {
    let (store, _tmp) = make_store();
    let path = Path::from("self_copy.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("self")))
        .await
        .unwrap();

    // Copy to same location should overwrite with same data
    store.copy(&path, &path).await.unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("self"));
}

// ═══════════════════════════════════════════════════════════════════════
// Delete edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn delete_missing_is_idempotent() {
    let (store, _tmp) = make_store();
    // Per ObjectStore trait contract, delete of a missing key returns Ok
    store.delete(&Path::from("ghost.bin")).await.unwrap();
}

#[tokio::test]
async fn delete_then_put_same_key() {
    let (store, _tmp) = make_store();
    let path = Path::from("reuse.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("first")))
        .await
        .unwrap();
    store.delete(&path).await.unwrap();
    store
        .put(&path, PutPayload::from(Bytes::from("second")))
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("second"));
}

// ═══════════════════════════════════════════════════════════════════════
// List edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn list_empty_store() {
    let (store, _tmp) = make_store();
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert!(files.is_empty());
}

#[tokio::test]
async fn list_with_delimiter_empty_store() {
    let (store, _tmp) = make_store();
    let result = store.list_with_delimiter(None).await.unwrap();
    assert!(result.objects.is_empty());
    assert!(result.common_prefixes.is_empty());
}

#[tokio::test]
async fn list_prefix_no_match() {
    let (store, _tmp) = make_store();
    store
        .put(
            &Path::from("alpha/file.bin"),
            PutPayload::from(Bytes::from("a")),
        )
        .await
        .unwrap();

    let files: Vec<_> = store
        .list(Some(&Path::from("beta")))
        .try_collect()
        .await
        .unwrap();
    assert!(files.is_empty(), "prefix that matches nothing should return empty");
}

#[tokio::test]
async fn list_with_delimiter_nested_dirs() {
    let (store, _tmp) = make_store();

    store
        .put(
            &Path::from("a/b/c/d.bin"),
            PutPayload::from(Bytes::from("deep")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("a/b/e.bin"),
            PutPayload::from(Bytes::from("mid")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("a/f.bin"),
            PutPayload::from(Bytes::from("shallow")),
        )
        .await
        .unwrap();

    // Top-level: "a" is a common prefix
    let top = store.list_with_delimiter(None).await.unwrap();
    assert_eq!(top.objects.len(), 0, "no direct children at root");
    assert_eq!(top.common_prefixes.len(), 1, "only 'a' prefix at root");

    // Under "a": "b" is a common prefix, "f.bin" is an object
    let under_a = store
        .list_with_delimiter(Some(&Path::from("a")))
        .await
        .unwrap();
    assert_eq!(under_a.objects.len(), 1, "f.bin is direct child of a");
    assert_eq!(under_a.common_prefixes.len(), 1, "b is common prefix under a");

    // Under "a/b": "c" is a common prefix, "e.bin" is an object
    let under_ab = store
        .list_with_delimiter(Some(&Path::from("a/b")))
        .await
        .unwrap();
    assert_eq!(under_ab.objects.len(), 1, "e.bin is direct child of a/b");
    assert_eq!(under_ab.common_prefixes.len(), 1, "c is common prefix under a/b");
}

#[tokio::test]
async fn list_with_delimiter_with_prefix() {
    let (store, _tmp) = make_store();

    store
        .put(
            &Path::from("table/data/0001.db"),
            PutPayload::from(Bytes::from("d1")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("table/data/0002.db"),
            PutPayload::from(Bytes::from("d2")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("table/meta/manifest"),
            PutPayload::from(Bytes::from("m")),
        )
        .await
        .unwrap();

    let result = store
        .list_with_delimiter(Some(&Path::from("table")))
        .await
        .unwrap();
    assert_eq!(result.objects.len(), 0, "no direct children under 'table'");
    assert_eq!(
        result.common_prefixes.len(),
        2,
        "data and meta are common prefixes"
    );

    let result = store
        .list_with_delimiter(Some(&Path::from("table/data")))
        .await
        .unwrap();
    assert_eq!(result.objects.len(), 2, "two lance files");
    assert_eq!(result.common_prefixes.len(), 0, "no subdirs");
}

#[tokio::test]
async fn list_prefix_partial_name_no_match() {
    let (store, _tmp) = make_store();

    store
        .put(
            &Path::from("foobar/file.bin"),
            PutPayload::from(Bytes::from("x")),
        )
        .await
        .unwrap();

    // "foo" is a prefix of "foobar" but NOT a directory boundary
    // The list filter requires k starts_with(prefix) AND
    // (k.len==p.len || p.is_empty || k[p.len]=='/')
    // So "foo" should NOT match "foobar/file.bin"
    let files: Vec<_> = store
        .list(Some(&Path::from("foo")))
        .try_collect()
        .await
        .unwrap();
    assert!(
        files.is_empty(),
        "prefix 'foo' should not match 'foobar/file.bin' (no dir boundary)"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Head / metadata edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn head_missing_file() {
    let (store, _tmp) = make_store();
    let err = store.head(&Path::from("nope.bin")).await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn head_returns_correct_metadata() {
    let (store, _tmp) = make_store();
    let path = Path::from("meta_test.bin");
    let payload = Bytes::from(vec![0u8; 12345]);

    store
        .put(&path, PutPayload::from(payload))
        .await
        .unwrap();

    let meta = store.head(&path).await.unwrap();
    assert_eq!(meta.size, 12345);
    assert_eq!(meta.location, path);
    assert!(meta.e_tag.is_none());
    assert!(meta.version.is_none());
}

// ═══════════════════════════════════════════════════════════════════════
// Multipart edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn multipart_single_part() {
    let (store, _tmp) = make_store();
    let path = Path::from("mp_single.bin");

    let mut upload = store.put_multipart(&path).await.unwrap();
    upload.put_part(PutPayload::from(Bytes::from("single"))).await.unwrap();
    upload.complete().await.unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("single"));
}

#[tokio::test]
async fn multipart_zero_parts_empty_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("mp_empty.bin");

    let mut upload = store.put_multipart(&path).await.unwrap();
    let result = upload.complete().await;
    assert!(result.is_err(), "zero-part multipart should be rejected");
}

#[tokio::test]
async fn multipart_abort_then_new_upload() {
    let (store, _tmp) = make_store();
    let path = Path::from("mp_abort.bin");

    let mut upload = store.put_multipart(&path).await.unwrap();
    upload.put_part(PutPayload::from(Bytes::from("aborted data"))).await.unwrap();
    upload.abort().await.unwrap();

    // File should not exist after abort
    assert!(store.get(&path).await.is_err(), "aborted upload should not create file");

    // New upload to same path should succeed
    let mut upload2 = store.put_multipart(&path).await.unwrap();
    upload2.put_part(PutPayload::from(Bytes::from("real data"))).await.unwrap();
    upload2.complete().await.unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("real data"));
}

#[tokio::test]
async fn multipart_many_small_parts() {
    let (store, _tmp) = make_store();
    let path = Path::from("mp_many.bin");

    let mut upload = store.put_multipart(&path).await.unwrap();
    for i in 0..100u8 {
        upload.put_part(PutPayload::from(Bytes::from(vec![i; 10]))).await.unwrap();
    }
    upload.complete().await.unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.len(), 1000);
    for i in 0..100u8 {
        let chunk = &data[i as usize * 10..(i as usize + 1) * 10];
        assert!(chunk.iter().all(|&b| b == i), "part {i} content mismatch");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Format / open error paths
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn format_device_too_small() {
    let tmp = NamedTempFile::new().unwrap();
    let err = RawObjectStore::format_with_size(tmp.path(), 1024, false);
    assert!(err.is_err(), "1024-byte device should be too small");
    let msg = format!("{}", err.unwrap_err());
    assert!(msg.contains("too small"), "error should mention 'too small': {msg}");
}

#[tokio::test]
async fn format_minimum_size_device() {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_size(tmp.path(), MIN_DEVICE_SIZE, false).unwrap();

    // Should be able to write at least one small file
    store
        .put(
            &Path::from("tiny.bin"),
            PutPayload::from(Bytes::from("x")),
        )
        .await
        .unwrap();

    let data = store.get(&Path::from("tiny.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("x"));
}

// ═══════════════════════════════════════════════════════════════════════
// Overwrite size changes
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn overwrite_smaller_then_larger() {
    let (store, _tmp) = make_store();
    let path = Path::from("resize.bin");

    // Write large
    let large = Bytes::from(vec![0xAA; 16384]);
    store.put(&path, PutPayload::from(large)).await.unwrap();

    // Overwrite with small
    let small = Bytes::from(vec![0xBB; 100]);
    store
        .put(&path, PutPayload::from(small.clone()))
        .await
        .unwrap();
    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, small);

    // Overwrite with large again
    let large2 = Bytes::from(vec![0xCC; 32768]);
    store
        .put(&path, PutPayload::from(large2.clone()))
        .await
        .unwrap();
    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, large2);
}

#[tokio::test]
async fn overwrite_same_key_100_times() {
    let (store, _tmp) = make_store();
    let path = Path::from("hotkey.bin");

    for i in 0u32..100 {
        let payload = Bytes::from(i.to_le_bytes().to_vec());
        store
            .put(&path, PutPayload::from(payload))
            .await
            .unwrap();
    }

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    let val = u32::from_le_bytes(data[..4].try_into().unwrap());
    assert_eq!(val, 99, "should have the last written value");
}

// ═══════════════════════════════════════════════════════════════════════
// Path handling edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn deeply_nested_path() {
    let (store, _tmp) = make_store();
    let path = Path::from("a/b/c/d/e/f/g/h/i/j/k/l/m.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("deep")))
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("deep"));

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn paths_with_special_chars() {
    let (store, _tmp) = make_store();
    let test_paths = [
        "data-file.db",
        "data_file.db",
        "data.file.db",
        "DATA/FILE.DB",
        "123/456/789.bin",
    ];

    for (i, p) in test_paths.iter().enumerate() {
        let path = Path::from(*p);
        let payload = Bytes::from(vec![i as u8; 100]);
        store
            .put(&path, PutPayload::from(payload.clone()))
            .await
            .unwrap();

        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, payload, "path '{}' failed round-trip", p);
    }

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), test_paths.len());
}

#[tokio::test]
async fn case_sensitive_paths() {
    let (store, _tmp) = make_store();

    store
        .put(
            &Path::from("File.bin"),
            PutPayload::from(Bytes::from("upper")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("file.bin"),
            PutPayload::from(Bytes::from("lower")),
        )
        .await
        .unwrap();

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 2, "paths should be case-sensitive");

    let d1 = store
        .get(&Path::from("File.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let d2 = store
        .get(&Path::from("file.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(d1, Bytes::from("upper"));
    assert_eq!(d2, Bytes::from("lower"));
}

#[tokio::test]
async fn long_path_name() {
    let (store, _tmp) = make_store();
    // 200-char filename
    let name = "x".repeat(200);
    let path = Path::from(format!("dir/{name}.bin"));

    store
        .put(&path, PutPayload::from(Bytes::from("long")))
        .await
        .unwrap();

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("long"));
}

// ═══════════════════════════════════════════════════════════════════════
// Display / Debug impl coverage
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn display_and_debug() {
    let (store, _tmp) = make_store();
    let display = format!("{store}");
    assert!(display.contains("RawObjectStore"), "Display: {display}");

    let debug = format!("{store:?}");
    assert!(debug.contains("RawObjectStore"), "Debug: {debug}");
    assert!(debug.contains("device"), "Debug should show device field: {debug}");
}

// ═══════════════════════════════════════════════════════════════════════
// Configurable index slot size
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn format_with_32mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 32 * 1024 * 1024u64; // 32 MB per slot
    let device_size = rawobjstr::min_device_size(slot_size) + 64 * 1024 * 1024;

    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    }).unwrap();

    // Write and read back
    store.put(&Path::from("test.bin"), PutPayload::from(Bytes::from("hello 32mb"))).await.unwrap();
    store.flush_index().unwrap();

    let data = store.get(&Path::from("test.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("hello 32mb"));

    // Reopen and verify
    drop(store);
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let data2 = store2.get(&Path::from("test.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data2, Bytes::from("hello 32mb"));
}

#[tokio::test]
async fn format_with_64mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 64 * 1024 * 1024u64;
    let device_size = rawobjstr::min_device_size(slot_size) + 64 * 1024 * 1024;

    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    }).unwrap();

    // Write many files to exercise the larger index capacity
    for i in 0..100u32 {
        let path = Path::from(format!("files/file_{i:04}.bin"));
        store.put(&path, PutPayload::from(Bytes::from(vec![i as u8; 256]))).await.unwrap();
    }
    store.flush_index().unwrap();

    // Reopen and verify count
    drop(store);
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let files: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 100);
}

#[tokio::test]
async fn format_rejects_invalid_slot_sizes() {
    let tmp = NamedTempFile::new().unwrap();

    // Too small (< 16 MB)
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 8 * 1024 * 1024,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("invalid index slot size"), "error: {msg}");

    // Not a multiple of 16 MB
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 24 * 1024 * 1024, // 24 MB is not a multiple of 16 MB
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("invalid index slot size"), "error: {msg}");

    // Zero
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 0,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err());
}

#[tokio::test]
async fn format_rejects_device_too_small_for_slot_size() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 32 * 1024 * 1024u64;
    let min_size = rawobjstr::min_device_size(slot_size);

    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: min_size - 1,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("too small"), "error: {msg}");
}

// ═══════════════════════════════════════════════════════════════════════
// max_key_length format validation
// ═══════════════════════════════════════════════════════════════════════

/// format_with_options must reject max_key_length values above the hard ceiling.
#[tokio::test]
async fn format_rejects_max_key_over_64k() {
    let tmp = NamedTempFile::new().unwrap();
    let device_size = rawobjstr::MIN_DEVICE_SIZE + 64 * 1024 * 1024;

    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
        max_key_length: 65_537,
        compression: Compression::None,
    });
    assert!(result.is_err(), "should reject max_key_length=65537");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("65537") || msg.contains("max_key_length") || msg.contains("ceiling"),
        "error should mention the bad value: {msg}"
    );

    // Exactly 65536 must be accepted (it's the ceiling, not over it)
    let result2 = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
        max_key_length: 65_536,
        compression: Compression::None,
    });
    assert!(result2.is_ok(), "65536 (the hard ceiling) must be accepted: {:?}", result2);
}

/// max_key_length exactly at the hard ceiling is accepted but clamped to the
/// shard slot ceiling when stored (because the shard can't actually hold a full
/// 65536-byte key name alongside its bincode overhead).
#[tokio::test]
async fn format_clamps_max_key_to_shard_ceiling() {
    let tmp = NamedTempFile::new().unwrap();
    let device_size = rawobjstr::MIN_DEVICE_SIZE + 64 * 1024 * 1024;

    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
        max_key_length: 65_536, // at the hard ceiling,
        compression: Compression::None,
    }).unwrap();

    let info = store.device_info();
    // Clamped to shard_slot_size − overhead = 65536 − 98 = 65438
    assert!(
        info.max_key_length <= 65_536,
        "max_key_length must not exceed hard ceiling: got {}",
        info.max_key_length
    );
    assert!(
        info.max_key_length >= 65_000,
        "clamped value should still be near 64 KB: got {}",
        info.max_key_length
    );
}

/// Default format uses DEFAULT_MAX_KEY_LENGTH (1024).
#[tokio::test]
async fn max_key_length_default_is_1024() {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_size(tmp.path(), common::SMALL_DEVICE, false).unwrap();
    let info = store.device_info();
    assert_eq!(
        info.max_key_length,
        rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        "default max_key_length should be {}",
        rawobjstr::DEFAULT_MAX_KEY_LENGTH,
    );
}

/// Custom max_key_length survives a store close + reopen.
#[tokio::test]
async fn max_key_length_persists_across_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let custom_limit = 512usize;
    let device_size = rawobjstr::MIN_DEVICE_SIZE + 64 * 1024 * 1024;

    {
        let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
            device_size,
            direct_io: false,
            index_slot_size: rawobjstr::INDEX_REGION_SIZE,
            max_key_length: custom_limit,
            compression: Compression::None,
        }).unwrap();
        store.flush_index().unwrap();
    }

    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let info = store2.device_info();
    assert_eq!(
        info.max_key_length, custom_limit,
        "max_key_length should persist across reopen"
    );

    // A key at the limit should succeed
    let ok_key = "k".repeat(custom_limit);
    store2
        .put(&object_store::path::Path::from(ok_key.as_str()), object_store::PutPayload::from(bytes::Bytes::from("value")))
        .await
        .unwrap();

    // A key one byte over the limit should fail
    let over_key = "k".repeat(custom_limit + 1);
    let err = store2
        .put(&object_store::path::Path::from(over_key.as_str()), object_store::PutPayload::from(bytes::Bytes::from("value")))
        .await;
    assert!(err.is_err(), "key exceeding max_key_length must be rejected");
}

#[tokio::test]
async fn superblock_stores_slot_capacity() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 48 * 1024 * 1024u64; // 48 MB
    let device_size = rawobjstr::min_device_size(slot_size) + 64 * 1024 * 1024;

    let _store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    }).unwrap();
    drop(_store);

    // Read superblock directly and check the stored slot capacity
    let mut f = std::fs::File::open(tmp.path()).unwrap();
    let mut sb_bytes = vec![0u8; rawobjstr::SUPERBLOCK_SIZE as usize];
    use std::io::Read;
    f.read_exact(&mut sb_bytes).unwrap();
    let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();
    assert_eq!(sb.index_slot_capacity, slot_size);
    assert_eq!(sb.version, 4);
}

#[tokio::test]
async fn default_format_uses_16mb_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_size(tmp.path(), MIN_DEVICE_SIZE, false).unwrap();
    drop(store);

    let mut f = std::fs::File::open(tmp.path()).unwrap();
    let mut sb_bytes = vec![0u8; rawobjstr::SUPERBLOCK_SIZE as usize];
    use std::io::Read;
    f.read_exact(&mut sb_bytes).unwrap();
    let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();
    assert_eq!(sb.index_slot_capacity, 16 * 1024 * 1024);
}

// =====================================================================
// Tests for bug-fix / performance changes (code review round)
// =====================================================================

/// PutMode::Update with an e_tag precondition should return Precondition error
/// because we do not support conditional updates.
#[tokio::test]
async fn put_mode_update_with_etag_rejected() {
    let (store, _tmp) = make_store();
    let path = Path::from("etag_reject.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("original")))
        .await
        .unwrap();

    let opts = PutOptions {
        mode: PutMode::Update(object_store::UpdateVersion {
            e_tag: Some("some-etag".to_string()),
            version: None,
        }),
        ..Default::default()
    };
    let err = store
        .put_opts(&path, PutPayload::from(Bytes::from("conditional")), opts)
        .await;
    assert!(
        matches!(err, Err(object_store::Error::Precondition { .. })),
        "expected Precondition error, got {err:?}"
    );

    // Data should be unchanged
    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("original"));
}

/// PutMode::Update with a version precondition should also be rejected.
#[tokio::test]
async fn put_mode_update_with_version_rejected() {
    let (store, _tmp) = make_store();
    let path = Path::from("ver_reject.bin");

    store
        .put(&path, PutPayload::from(Bytes::from("v1")))
        .await
        .unwrap();

    let opts = PutOptions {
        mode: PutMode::Update(object_store::UpdateVersion {
            e_tag: None,
            version: Some("42".to_string()),
        }),
        ..Default::default()
    };
    let err = store
        .put_opts(&path, PutPayload::from(Bytes::from("v2")), opts)
        .await;
    assert!(
        matches!(err, Err(object_store::Error::Precondition { .. })),
        "expected Precondition error, got {err:?}"
    );

    let data = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("v1"));
}

/// list_with_delimiter must return objects in lexicographic order.
#[tokio::test]
async fn list_with_delimiter_objects_sorted() {
    let (store, _tmp) = make_store();

    // Insert files in non-alphabetical order
    for name in ["data/z.db", "data/a.db", "data/m.db", "data/f.db"] {
        store
            .put(&Path::from(name), PutPayload::from(Bytes::from("x")))
            .await
            .unwrap();
    }

    let result = store
        .list_with_delimiter(Some(&Path::from("data")))
        .await
        .unwrap();

    let keys: Vec<String> = result
        .objects
        .iter()
        .map(|o| o.location.to_string())
        .collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys, "objects must be in lexicographic order");
}

/// list_with_delimiter at root with multiple prefixes: both objects and
/// common_prefixes should be sorted.
#[tokio::test]
async fn list_with_delimiter_root_sorted() {
    let (store, _tmp) = make_store();

    // Insert files that produce multiple prefixes and root-level objects
    for name in [
        "zebra.bin", "alpha.bin", "mid.bin",
        "dir_z/file.bin", "dir_a/file.bin", "dir_m/file.bin",
    ] {
        store
            .put(&Path::from(name), PutPayload::from(Bytes::from("x")))
            .await
            .unwrap();
    }

    let result = store.list_with_delimiter(None).await.unwrap();

    // Objects
    let obj_keys: Vec<String> = result
        .objects
        .iter()
        .map(|o| o.location.to_string())
        .collect();
    let mut sorted_obj = obj_keys.clone();
    sorted_obj.sort();
    assert_eq!(obj_keys, sorted_obj, "root objects must be sorted");

    // Common prefixes
    let prefix_strs: Vec<String> = result
        .common_prefixes
        .iter()
        .map(|p| p.to_string())
        .collect();
    let mut sorted_pfx = prefix_strs.clone();
    sorted_pfx.sort();
    assert_eq!(prefix_strs, sorted_pfx, "common_prefixes must be sorted");
}

/// copy() with the raw-block-copy optimization must produce byte-identical
/// data for various file sizes spanning block boundaries.
#[tokio::test]
async fn copy_various_sizes_integrity() {
    let (store, _tmp) = make_store();

    let sizes = [1, 100, 4095, 4096, 4097, 8191, 8192, 8193, 65536, 1024 * 1024];
    for (i, &sz) in sizes.iter().enumerate() {
        let data = Bytes::from(vec![(i as u8).wrapping_add(0xAA); sz]);
        let src = Path::from(format!("src_{i}.bin"));
        let dst = Path::from(format!("dst_{i}.bin"));

        store
            .put(&src, PutPayload::from(data.clone()))
            .await
            .unwrap();
        store.copy(&src, &dst).await.unwrap();

        let read_src = store.get(&src).await.unwrap().bytes().await.unwrap();
        let read_dst = store.get(&dst).await.unwrap().bytes().await.unwrap();
        assert_eq!(read_src, data, "source changed after copy (size={})", sz);
        assert_eq!(read_dst, data, "destination mismatch after copy (size={})", sz);
    }
}

/// copy() should work correctly when overwriting an existing destination.
#[tokio::test]
async fn copy_overwrites_destination() {
    let (store, _tmp) = make_store();
    let src = Path::from("copy_src.bin");
    let dst = Path::from("copy_dst.bin");

    store
        .put(&src, PutPayload::from(Bytes::from("source data")))
        .await
        .unwrap();
    store
        .put(&dst, PutPayload::from(Bytes::from("old destination")))
        .await
        .unwrap();

    store.copy(&src, &dst).await.unwrap();

    let data = store.get(&dst).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("source data"));

    // Source should be unchanged
    let src_data = store.get(&src).await.unwrap().bytes().await.unwrap();
    assert_eq!(src_data, Bytes::from("source data"));
}

// ═══════════════════════════════════════════════════════════════════════
// Key length stress test -- find the practical limit
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn key_length_stress() {
    // Format with a high max_key_length so we can probe the physical shard limit
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_options(
        tmp.path(),
        FormatOptions {
            device_size: common::SMALL_DEVICE,
            direct_io: false,
            index_slot_size: rawobjstr::INDEX_REGION_SIZE,
            max_key_length: 65536,
            compression: Compression::None,
        },
    )
    .unwrap();
    let _tmp = tmp;
    let mut max_ok = 0usize;

    // Try keys of increasing length: 100, 200, ..., up to 10_000
    for len in (100..=10_000).step_by(100) {
        let key = "k/".to_string() + &"a".repeat(len);
        let path = Path::from(key.as_str());
        let data = Bytes::from(vec![0xBB; 8]);

        match store.put(&path, PutPayload::from(data)).await {
            Ok(_) => {
                // Verify round-trip
                let got = store.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(got.len(), 8, "key len={len}: data mismatch");
                max_ok = len;
            }
            Err(_) => {
                eprintln!("key_length_stress: first failure at len={len}, last ok={max_ok}");
                break;
            }
        }
    }
    // We must have stored at least the S3 standard (1024 bytes)
    assert!(
        max_ok >= rawobjstr::MAX_KEY_LENGTH,
        "should support at least MAX_KEY_LENGTH={} but max_ok={max_ok}",
        rawobjstr::MAX_KEY_LENGTH,
    );
    eprintln!("key_length_stress: max successful key length = {max_ok}");
}

// ═══════════════════════════════════════════════════════════════════════
// Object size stress test -- increasing sizes until we run out of space
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn object_size_stress() {
    // Use a 256 MB store so we can fit some big objects
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_size(
        tmp.path(),
        common::MEDIUM_DEVICE,
        false,
    )
    .unwrap();

    let sizes: Vec<usize> = vec![
        1,
        4095,
        4096,
        4097,
        65536,
        1024 * 1024,        // 1 MB
        4 * 1024 * 1024,    // 4 MB
        16 * 1024 * 1024,   // 16 MB
        64 * 1024 * 1024,   // 64 MB
        128 * 1024 * 1024,  // 128 MB
    ];

    let mut max_ok = 0usize;
    for &sz in &sizes {
        // Use unique key per size so we don't overwrite
        let key = format!("size_test/{sz}");
        let path = Path::from(key.as_str());
        let data = Bytes::from(vec![(sz % 251) as u8; sz]);

        match store.put(&path, PutPayload::from(data.clone())).await {
            Ok(_) => {
                let got = store.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(got.len(), sz, "size {sz}: read-back length mismatch");
                assert_eq!(got[..1.min(sz)], data[..1.min(sz)], "size {sz}: data mismatch");
                max_ok = sz;
                // Delete so we can try the next size
                store.delete(&path).await.unwrap();
            }
            Err(e) => {
                eprintln!("object_size_stress: first failure at size={sz}: {e}");
                break;
            }
        }
    }
    // Must handle at least 16 MB objects on a 256 MB store
    assert!(
        max_ok >= 16 * 1024 * 1024,
        "should handle at least 16 MB objects but max_ok={max_ok}"
    );
    eprintln!("object_size_stress: max successful object size = {max_ok}");
}

// ═══════════════════════════════════════════════════════════════════════
// Null / empty payload edge cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn put_empty_bytes_multiple_keys() {
    let (store, _tmp) = make_store();

    // Zero-byte objects are rejected by the store spec.
    for i in 0..5 {
        let key = format!("empty/{i:03}.bin");
        let result = store
            .put(&Path::from(key.as_str()), PutPayload::from(Bytes::new()))
            .await;
        assert!(result.is_err(), "zero-byte put should be rejected for {key}");
    }

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert!(files.is_empty(), "no objects should have been created");
}

#[tokio::test]
async fn overwrite_data_with_empty() {
    let (store, _tmp) = make_store();
    let path = Path::from("overwrite_empty.bin");

    // Write 1 MB
    store
        .put(&path, PutPayload::from(Bytes::from(vec![0xAA; 1024 * 1024])))
        .await
        .unwrap();
    assert_eq!(store.head(&path).await.unwrap().size, 1024 * 1024);

    // Overwrite with empty should be rejected
    let result = store
        .put(&path, PutPayload::from(Bytes::new()))
        .await;
    assert!(result.is_err(), "overwrite with zero-byte should be rejected");

    // Original data should still be intact
    assert_eq!(store.head(&path).await.unwrap().size, 1024 * 1024);
}

/// After copy, the copied file should survive flush + reopen.
#[tokio::test]
async fn copy_persists_after_flush() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();
    let payload = Bytes::from(vec![0x42u8; 12345]);

    {
        let store =
            RawObjectStore::format_with_size(tmp.path(), common::SMALL_DEVICE, false).unwrap();
        store
            .put(&Path::from("orig.bin"), PutPayload::from(payload.clone()))
            .await
            .unwrap();
        store
            .copy(&Path::from("orig.bin"), &Path::from("copied.bin"))
            .await
            .unwrap();
        store.flush_index().unwrap();
    }

    let store = RawObjectStore::open(&path_buf).unwrap();
    let data = store
        .get(&Path::from("copied.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, payload);
}

// =========================================================================
// FormatOptions boundary / invalid index size tests
// =========================================================================

/// 15 MB index slot size is rejected (below 16 MB minimum).
#[tokio::test]
async fn format_rejects_15mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 15 * 1024 * 1024,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err(), "15 MB slot should be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("invalid index slot size"), "error: {msg}");
}

/// 17 MB index slot size is rejected (not a multiple of 16 MB).
#[tokio::test]
async fn format_rejects_17mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 17 * 1024 * 1024,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err(), "17 MB (not a multiple of 16 MB) should be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("invalid index slot size"), "error: {msg}");
}

/// 1 MB index slot size is rejected (far below minimum).
#[tokio::test]
async fn format_rejects_1mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 1 * 1024 * 1024,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err(), "1 MB slot should be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("invalid index slot size"), "error: {msg}");
}

/// 48 MB index slot size is accepted (valid multiple of 16 MB).
#[tokio::test]
async fn format_accepts_48mb_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 48 * 1024 * 1024u64;
    let device_size = rawobjstr::min_device_size(slot_size) + 64 * 1024 * 1024;

    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(store.is_ok(), "48 MB (3x16) should be valid: {:?}", store.err());
}

/// 4096 bytes (one block) is rejected as index slot size.
#[tokio::test]
async fn format_rejects_4k_index_slots() {
    let tmp = NamedTempFile::new().unwrap();
    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: 256 * 1024 * 1024,
        direct_io: false,
        index_slot_size: 4096,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(result.is_err(), "4096-byte slot should be rejected");
}

/// max_key_length of 0 should be rejected or handled gracefully.
#[tokio::test]
async fn format_max_key_length_zero() {
    let tmp = NamedTempFile::new().unwrap();
    let device_size = rawobjstr::MIN_DEVICE_SIZE + 64 * 1024 * 1024;

    let result = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
        max_key_length: 0,
        compression: Compression::None,
    });
    // max_key_length=0 means no keys can be stored, should fail or produce
    // a store that immediately rejects any put
    if let Ok(store) = result {
        let err = store.put(
            &Path::from("a"),
            PutPayload::from(Bytes::from("data")),
        ).await;
        assert!(err.is_err(), "max_key_length=0 should reject all puts");
    }
}

/// Full option matrix: compression + custom index slots + custom key length.
#[tokio::test]
async fn format_full_options_matrix() {
    for compression in &[Compression::None, Compression::Zstd, Compression::Snappy] {
        let tmp = NamedTempFile::new().unwrap();
        let slot_size = 32 * 1024 * 1024u64;
        let device_size = rawobjstr::min_device_size(slot_size) + 64 * 1024 * 1024;

        let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
            device_size,
            direct_io: false,
            index_slot_size: slot_size,
            max_key_length: 2048,
            compression: *compression,
        }).unwrap();

        assert_eq!(store.compression(), *compression);
        assert_eq!(store.max_key_length(), 2048);

        // Write, flush, reopen, verify
        store.put(
            &Path::from("test.bin"),
            PutPayload::from(Bytes::from(vec![0x42u8; 8192])),
        ).await.unwrap();
        store.flush_index().unwrap();

        let path_buf = tmp.path().to_path_buf();
        drop(store);
        let store2 = RawObjectStore::open(&path_buf).unwrap();
        assert_eq!(store2.compression(), *compression);
        let data = store2.get(&Path::from("test.bin")).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len(), 8192);
    }
}

/// Device exactly at minimum size for given slots should work.
#[tokio::test]
async fn format_exact_minimum_device_size() {
    let tmp = NamedTempFile::new().unwrap();
    let slot_size = 16 * 1024 * 1024u64;
    let min_size = rawobjstr::min_device_size(slot_size);

    // Exactly at minimum should succeed
    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: min_size,
        direct_io: false,
        index_slot_size: slot_size,
        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
        compression: Compression::None,
    });
    assert!(store.is_ok(), "exact minimum should work: {:?}", store.err());
}

// =========================================================================
// import_from / export_to error path tests
// =========================================================================

/// import_from an empty source produces a report with 0 files.
#[tokio::test]
async fn import_from_empty_source() {
    let (store, _tmp) = common::make_store();
    let source_tmp = NamedTempFile::new().unwrap();
    let source = RawObjectStore::format_with_size(
        source_tmp.path(), common::SMALL_DEVICE, false
    ).unwrap();

    let report = store.import_from(&source, None).await.unwrap();
    assert_eq!(report.files_imported, 0);
    assert_eq!(report.bytes_imported, 0);
    assert!(report.errors.is_empty());
}

/// export_to an empty store produces a report with 0 files.
#[tokio::test]
async fn export_to_empty_store() {
    let (store, _tmp) = common::make_store();
    let target_tmp = NamedTempFile::new().unwrap();
    let target = RawObjectStore::format_with_size(
        target_tmp.path(), common::SMALL_DEVICE, false
    ).unwrap();

    let report = store.export_to(&target).await.unwrap();
    assert_eq!(report.files_exported, 0);
    assert_eq!(report.bytes_exported, 0);
    assert!(report.errors.is_empty());
}

/// import_from correctly reports file counts and byte counts.
#[tokio::test]
async fn import_from_report_counts() {
    let source_tmp = NamedTempFile::new().unwrap();
    let source = RawObjectStore::format_with_size(
        source_tmp.path(), common::SMALL_DEVICE, false
    ).unwrap();

    // Write files to source
    source.put(&Path::from("a.txt"), PutPayload::from(Bytes::from("hello"))).await.unwrap();
    source.put(&Path::from("b.txt"), PutPayload::from(Bytes::from("world!!"))).await.unwrap();
    source.flush_index().unwrap();

    let (dest, _tmp2) = common::make_store();
    let report = dest.import_from(&source, None).await.unwrap();
    assert_eq!(report.files_imported, 2);
    assert_eq!(report.bytes_imported, 12); // 5 + 7
    assert!(report.errors.is_empty());

    // Verify files were actually imported
    let data = dest.get(&Path::from("a.txt")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from("hello"));
}

/// export_to correctly reports file counts.
#[tokio::test]
async fn export_to_report_counts() {
    let (source, _tmp1) = common::make_store();
    source.put(&Path::from("x.bin"), PutPayload::from(Bytes::from(vec![0xAAu8; 1024]))).await.unwrap();
    source.put(&Path::from("y.bin"), PutPayload::from(Bytes::from(vec![0xBBu8; 2048]))).await.unwrap();
    source.flush_index().unwrap();

    let target_tmp = NamedTempFile::new().unwrap();
    let target = RawObjectStore::format_with_size(
        target_tmp.path(), common::SMALL_DEVICE, false
    ).unwrap();

    let report = source.export_to(&target).await.unwrap();
    assert_eq!(report.files_exported, 2);
    assert_eq!(report.bytes_exported, 3072); // 1024 + 2048
    assert!(report.errors.is_empty());

    // Verify exported files
    let data = target.get(&Path::from("x.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.len(), 1024);
}

/// import_from with a prefix filter only imports matching files.
#[tokio::test]
async fn import_from_with_prefix_filter() {
    let source_tmp = NamedTempFile::new().unwrap();
    let source = RawObjectStore::format_with_size(
        source_tmp.path(), common::SMALL_DEVICE, false
    ).unwrap();

    source.put(&Path::from("data/a.bin"), PutPayload::from(Bytes::from("aaa"))).await.unwrap();
    source.put(&Path::from("data/b.bin"), PutPayload::from(Bytes::from("bbb"))).await.unwrap();
    source.put(&Path::from("other/c.bin"), PutPayload::from(Bytes::from("ccc"))).await.unwrap();
    source.flush_index().unwrap();

    let (dest, _tmp2) = common::make_store();
    let report = dest.import_from(&source, Some(&Path::from("data"))).await.unwrap();
    assert_eq!(report.files_imported, 2);
    assert!(report.errors.is_empty());

    // "other/c.bin" should NOT have been imported
    let files: Vec<_> = dest.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 2);
}

/// import_from to a nearly-full device reports errors for files that won't fit.
#[tokio::test]
async fn import_from_no_space_reports_errors() {
    // Create a very small destination (minimum size)
    let dest_tmp = NamedTempFile::new().unwrap();
    let dest = RawObjectStore::format_with_size(
        dest_tmp.path(), rawobjstr::MIN_DEVICE_SIZE, false
    ).unwrap();

    // Fill it up so there's minimal free space
    let info = dest.device_info();
    let fill_size = (info.free_space as usize).saturating_sub(8192);
    if fill_size > 4096 {
        dest.put(
            &Path::from("filler.bin"),
            PutPayload::from(Bytes::from(vec![0u8; fill_size])),
        ).await.unwrap();
    }

    // Source has files that won't fit
    let source_tmp = NamedTempFile::new().unwrap();
    let source = RawObjectStore::format_with_size(
        source_tmp.path(), common::SMALL_DEVICE, false
    ).unwrap();
    source.put(&Path::from("big.bin"), PutPayload::from(Bytes::from(vec![0xFFu8; 65536]))).await.unwrap();
    source.flush_index().unwrap();

    let report = dest.import_from(&source, None).await.unwrap();
    // The import should have completed (not panicked) with errors for the file that didn't fit
    assert!(!report.errors.is_empty() || report.files_imported == 0,
        "should either have errors or imported 0 files when dest is full");
}

// =========================================================================
// Multipart edge case tests
// =========================================================================

/// Multipart upload with many parts (100 tiny parts).
#[tokio::test]
async fn multipart_many_tiny_parts() {
    let (store, _tmp) = common::make_store();
    let mut upload = store.put_multipart(&Path::from("many_parts.bin")).await.unwrap();

    let mut expected = Vec::new();
    for i in 0..100u8 {
        let part = Bytes::from(vec![i; 64]);
        expected.extend_from_slice(&part);
        upload.put_part(PutPayload::from(part)).await.unwrap();
    }
    upload.complete().await.unwrap();
    store.flush_index().unwrap();

    let data = store.get(&Path::from("many_parts.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.len(), 6400);
    assert_eq!(data.as_ref(), expected.as_slice());
}

/// Multipart upload abort cleans up temp extents.
#[tokio::test]
async fn multipart_abort_cleanup() {
    let (store, _tmp) = common::make_store();
    let free_before = store.device_info().free_space;

    let mut upload = store.put_multipart(&Path::from("aborted.bin")).await.unwrap();
    upload.put_part(PutPayload::from(Bytes::from(vec![0xAA; 4096]))).await.unwrap();
    upload.put_part(PutPayload::from(Bytes::from(vec![0xBB; 4096]))).await.unwrap();
    upload.abort().await.unwrap();

    // After abort, space should be reclaimed
    let free_after = store.device_info().free_space;
    assert!(
        free_after >= free_before - 4096,
        "space should be mostly reclaimed after abort: before={free_before}, after={free_after}"
    );

    // The key should not exist
    let result = store.get(&Path::from("aborted.bin")).await;
    assert!(result.is_err(), "aborted upload key should not exist");
}

// =========================================================================
// Event socket edge case tests
// =========================================================================

/// Event bus can be created and dropped without panicking.
#[test]
fn event_bus_create_drop() {
    let bus = rawobjstr::event::EventBus::new(256);
    bus.emit_put("test/key.bin");
    bus.emit_delete("test/key.bin");
    bus.emit_flush(0, 1);
    drop(bus);
    // No panic = pass
}

// =========================================================================
// layout_map tests
// =========================================================================

/// layout_map on empty store has no extents, one free region spanning the data area.
#[tokio::test]
async fn layout_map_empty_store() {
    let (store, _tmp) = common::make_store();
    let layout = store.layout_map();

    assert_eq!(layout.data_region_start, rawobjstr::DATA_START);
    assert!(layout.data_region_end > layout.data_region_start);
    assert!(layout.extents.is_empty());
    assert_eq!(layout.free_regions.len(), 1);

    let (off, sz) = layout.free_regions[0];
    assert_eq!(off, rawobjstr::DATA_START);
    assert_eq!(off + sz, layout.data_region_end);
    assert!(layout.txn_id >= 1);
}

/// layout_map correctly reflects written objects and free gaps.
#[tokio::test]
async fn layout_map_with_objects() {
    let (store, _tmp) = common::make_store();
    store.put(&Path::from("a.bin"), PutPayload::from(Bytes::from(vec![0xAA; 4096]))).await.unwrap();
    store.put(&Path::from("b.bin"), PutPayload::from(Bytes::from(vec![0xBB; 8192]))).await.unwrap();
    store.flush_index().unwrap();

    let layout = store.layout_map();

    // Two extents
    assert_eq!(layout.extents.len(), 2);

    // Extents should be sorted by offset
    assert!(layout.extents[0].offset < layout.extents[1].offset);

    // Each extent has correct key
    let keys: Vec<&str> = layout.extents.iter().map(|e| e.key.as_str()).collect();
    assert!(keys.contains(&"a.bin"));
    assert!(keys.contains(&"b.bin"));

    // Extent for a.bin should have size == 4096 on disk
    let a_ext = layout.extents.iter().find(|e| e.key == "a.bin").unwrap();
    assert_eq!(a_ext.size, 4096);
    assert!(a_ext.padded_size >= 4096);
    assert!(a_ext.offset >= rawobjstr::DATA_START);

    // Free region(s) should exist after the extents
    assert!(!layout.free_regions.is_empty());

    // Total: used extents + free should equal data region
    let total_used: u64 = layout.extents.iter().map(|e| e.padded_size).sum();
    let total_free: u64 = layout.free_regions.iter().map(|(_, s)| s).sum();
    assert_eq!(total_used + total_free, layout.data_region_end - layout.data_region_start);
}

/// layout_map after delete shows freed space in free regions.
#[tokio::test]
async fn layout_map_after_delete() {
    let (store, _tmp) = common::make_store();
    store.put(&Path::from("x.bin"), PutPayload::from(Bytes::from(vec![0xFF; 4096]))).await.unwrap();
    store.put(&Path::from("y.bin"), PutPayload::from(Bytes::from(vec![0xEE; 4096]))).await.unwrap();
    store.flush_index().unwrap();

    store.delete(&Path::from("x.bin")).await.unwrap();

    let layout = store.layout_map();
    assert_eq!(layout.extents.len(), 1);
    assert_eq!(layout.extents[0].key, "y.bin");

    // Should have at least 2 free regions (before y.bin and after y.bin)
    assert!(layout.free_regions.len() >= 2);
}

/// layout_map index regions are at the expected positions.
#[tokio::test]
async fn layout_map_index_regions() {
    let (store, _tmp) = common::make_store();
    let layout = store.layout_map();
    let info = store.device_info();

    assert_eq!(layout.index_region_a, info.index_region_a);
    assert_eq!(layout.index_region_b, info.index_region_b);
    assert_eq!(layout.active_index_region, info.active_index_region);
    assert_eq!(layout.device_size, info.device_size);
}
