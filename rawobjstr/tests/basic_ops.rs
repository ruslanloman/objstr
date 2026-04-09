mod common;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

use common::make_store;

#[tokio::test]
async fn put_get_round_trip() {
    let (store, _tmp) = make_store();
    let path = Path::from("data/00000.db");
    let payload = Bytes::from("hello world");

    store
        .put(&path, PutPayload::from(payload.clone()))
        .await
        .unwrap();

    let result = store.get(&path).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, payload);
}

#[tokio::test]
async fn put_overwrite() {
    let (store, _tmp) = make_store();
    let path = Path::from("test.txt");

    store
        .put(&path, PutPayload::from(Bytes::from("version1")))
        .await
        .unwrap();
    store
        .put(&path, PutPayload::from(Bytes::from("version2")))
        .await
        .unwrap();

    let result = store.get(&path).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("version2"));
}

#[tokio::test]
async fn delete_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("to_delete.db");

    store
        .put(&path, PutPayload::from(Bytes::from("data")))
        .await
        .unwrap();

    store.delete(&path).await.unwrap();

    let err = store.get(&path).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn get_not_found() {
    let (store, _tmp) = make_store();
    let err = store.get(&Path::from("nonexistent")).await;
    assert!(
        matches!(err, Err(object_store::Error::NotFound { .. })),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn list_files() {
    let (store, _tmp) = make_store();

    store
        .put(
            &Path::from("data/a.db"),
            PutPayload::from(Bytes::from("a")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("data/b.db"),
            PutPayload::from(Bytes::from("bb")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("meta/manifest"),
            PutPayload::from(Bytes::from("m")),
        )
        .await
        .unwrap();

    // List all
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 3);

    // List with prefix
    let data_files: Vec<_> = store
        .list(Some(&Path::from("data")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(data_files.len(), 2);
}

#[tokio::test]
async fn list_with_delimiter_test() {
    let (store, _tmp) = make_store();

    store
        .put(
            &Path::from("data/a.db"),
            PutPayload::from(Bytes::from("a")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("data/sub/b.db"),
            PutPayload::from(Bytes::from("b")),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("manifest"),
            PutPayload::from(Bytes::from("m")),
        )
        .await
        .unwrap();

    let result = store.list_with_delimiter(None).await.unwrap();
    // "manifest" is a direct child, "data" is a common prefix
    assert_eq!(result.objects.len(), 1);
    assert!(result.common_prefixes.len() >= 1);
}

#[tokio::test]
async fn copy_file() {
    let (store, _tmp) = make_store();
    let src = Path::from("src.db");
    let dst = Path::from("dst.db");

    store
        .put(&src, PutPayload::from(Bytes::from("copy me")))
        .await
        .unwrap();

    store.copy(&src, &dst).await.unwrap();

    let result = store.get(&dst).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("copy me"));

    // Source still exists
    let result = store.get(&src).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("copy me"));
}

#[tokio::test]
async fn head_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("sized.db");

    store
        .put(
            &path,
            PutPayload::from(Bytes::from("12345")),
        )
        .await
        .unwrap();

    let meta = store.head(&path).await.unwrap();
    assert_eq!(meta.size, 5);
    assert_eq!(meta.location, path);
}

#[tokio::test]
async fn flush_and_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), common::SMALL_DEVICE, false).unwrap();
        store
            .put(
                &Path::from("persist.db"),
                PutPayload::from(Bytes::from("survived")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
    }

    // Reopen
    let store = RawObjectStore::open(&path_buf).unwrap();
    let result = store.get(&Path::from("persist.db")).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data, Bytes::from("survived"));
}

#[tokio::test]
async fn large_file() {
    let (store, _tmp) = make_store();
    let path = Path::from("large.db");

    // 1 MB file
    let data = Bytes::from(vec![0xABu8; 1024 * 1024]);
    store
        .put(&path, PutPayload::from(data.clone()))
        .await
        .unwrap();

    let result = store.get(&path).await.unwrap();
    let read_data = result.bytes().await.unwrap();
    assert_eq!(read_data.len(), 1024 * 1024);
    assert_eq!(read_data, data);
}

#[tokio::test]
async fn get_range() {
    let (store, _tmp) = make_store();
    let path = Path::from("ranged.db");

    store
        .put(
            &path,
            PutPayload::from(Bytes::from("hello world")),
        )
        .await
        .unwrap();

    let data = store.get_range(&path, 0..5).await.unwrap();
    assert_eq!(data, Bytes::from("hello"));

    let data = store.get_range(&path, 6..11).await.unwrap();
    assert_eq!(data, Bytes::from("world"));
}
