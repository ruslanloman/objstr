//! Tests for flock writer exclusion, reader coexistence, and reload_index.
//!
//! These verify the advisory-lock mechanism that prevents multiple writer
//! processes on the same device and the reader reload path.

mod common;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use rawobjstr::RawStoreError;
use tempfile::NamedTempFile;

use common::{make_store, SMALL_DEVICE};

// ===========================================================================
// TEST 1: Double writer on same path returns DeviceLocked
// ===========================================================================

/// Open a RW store, then attempt a second RW open on the same path.
/// The second open must fail with DeviceLocked.
#[test]
fn flock_prevents_double_writer() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let _store1 = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();

    let err = RawObjectStore::open(path).unwrap_err();
    match &err {
        RawStoreError::DeviceLocked { path: p } => {
            assert!(
                p.contains(path.to_str().unwrap()),
                "DeviceLocked path should contain the device path, got: {p}"
            );
        }
        other => panic!("expected DeviceLocked, got: {other}"),
    }
}

// ===========================================================================
// TEST 2: Writer + read-only readers coexist
// ===========================================================================

/// A RW store and one or more RO stores can be open simultaneously.
#[tokio::test]
async fn flock_writer_plus_readers() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let store_rw = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();

    // Write some data so readers have something to verify
    store_rw
        .put(
            &Path::from("test/hello.txt"),
            PutPayload::from(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap();
    store_rw.flush_index().unwrap();

    // Open 3 read-only handles concurrently -- none should fail
    let ro1 = RawObjectStore::open_readonly(path).unwrap();
    let ro2 = RawObjectStore::open_readonly(path).unwrap();
    let ro3 = RawObjectStore::open_readonly(path).unwrap();

    // All readers can read the data
    for (i, ro) in [&ro1, &ro2, &ro3].iter().enumerate() {
        let data = ro
            .get(&Path::from("test/hello.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"hello", "reader {i} got wrong data");
    }

    // Writer can still write while readers are open
    store_rw
        .put(
            &Path::from("test/world.txt"),
            PutPayload::from(Bytes::from_static(b"world")),
        )
        .await
        .unwrap();
}

// ===========================================================================
// TEST 3: Multiple read-only opens coexist (no writer)
// ===========================================================================

/// Multiple RO handles on the same path all succeed.
#[tokio::test]
async fn flock_multiple_readers_no_writer() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    // Format, write, flush, drop (release flock)
    {
        let store = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
        store
            .put(
                &Path::from("data.bin"),
                PutPayload::from(Bytes::from(vec![42u8; 4096])),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
    }

    // Open 5 read-only handles
    let readers: Vec<_> = (0..5)
        .map(|_| RawObjectStore::open_readonly(path).unwrap())
        .collect();

    for (i, ro) in readers.iter().enumerate() {
        let data = ro
            .get(&Path::from("data.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096, "reader {i} wrong size");
        assert_eq!(data[0], 42, "reader {i} wrong content");
    }
}

// ===========================================================================
// TEST 4: Drop releases flock -- reopen succeeds
// ===========================================================================

/// After dropping a RW store the flock is released and a new RW open succeeds.
#[tokio::test]
async fn flock_release_on_drop() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
        store
            .put(
                &Path::from("a.txt"),
                PutPayload::from(Bytes::from_static(b"aaa")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
        // store dropped here
    }

    // Second RW open should succeed
    let store2 = RawObjectStore::open(&path_buf).unwrap();
    let data = store2
        .get(&Path::from("a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), b"aaa");
}

// ===========================================================================
// TEST 5: Explicit drop() releases flock mid-scope
// ===========================================================================

/// Calling drop(store) explicitly releases the flock so a new RW open succeeds
/// without needing a block scope.
#[tokio::test]
async fn flock_explicit_drop_releases() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    let store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    store
        .put(
            &Path::from("b.txt"),
            PutPayload::from(Bytes::from_static(b"bbb")),
        )
        .await
        .unwrap();
    store.flush_index().unwrap();
    drop(store); // explicit

    let store2 = RawObjectStore::open(&path_buf).unwrap();
    let data = store2
        .get(&Path::from("b.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), b"bbb");
}

// ===========================================================================
// TEST 6: Format while device is locked fails
// ===========================================================================

/// Attempting to format a path that has an active RW store returns DeviceLocked.
#[test]
fn flock_format_while_locked() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let _store = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();

    // format_with_size tries to create/open the file and acquire flock
    let err = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap_err();
    match &err {
        RawStoreError::DeviceLocked { .. } => {}
        other => panic!("expected DeviceLocked, got: {other}"),
    }
}

// ===========================================================================
// TEST 7: reload_index sees new data from writer
// ===========================================================================

/// Open a writer and a reader on the same image. Writer puts + flushes.
/// Reader calls reload_index() and can now see the new object.
#[tokio::test]
async fn reload_index_sees_new_data() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    let reader = RawObjectStore::open_readonly(path).unwrap();

    // Reader should see nothing initially
    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 0, "reader should see 0 files initially");

    // Writer puts + flushes
    writer
        .put(
            &Path::from("new/file.dat"),
            PutPayload::from(Bytes::from(vec![0xAB; 8192])),
        )
        .await
        .unwrap();
    writer.flush_index().unwrap();

    // Reader still sees 0 (stale index)
    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 0, "reader should still see 0 before reload");

    // reload_index should pick up the new data
    let changed = reader.reload_index().unwrap();
    assert!(changed, "reload_index should return true (new txn)");

    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 1, "reader should see 1 file after reload");

    let data = reader
        .get(&Path::from("new/file.dat"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.len(), 8192);
    assert!(data.iter().all(|&b| b == 0xAB));
}

// ===========================================================================
// TEST 8: reload_index returns false when nothing changed
// ===========================================================================

#[tokio::test]
async fn reload_index_no_change() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    writer
        .put(
            &Path::from("x.txt"),
            PutPayload::from(Bytes::from_static(b"x")),
        )
        .await
        .unwrap();
    writer.flush_index().unwrap();

    let reader = RawObjectStore::open_readonly(path).unwrap();

    // Reader already loaded the latest txn on open, so reload should be no-op.
    let changed = reader.reload_index().unwrap();
    assert!(!changed, "reload_index should return false (same txn)");

    // A second reload is also a no-op.
    let changed = reader.reload_index().unwrap();
    assert!(!changed, "second reload should also return false");
}

// ===========================================================================
// TEST 9: reload_index picks up deletes
// ===========================================================================

#[tokio::test]
async fn reload_index_sees_deletes() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    writer
        .put(
            &Path::from("del/a.txt"),
            PutPayload::from(Bytes::from_static(b"aaa")),
        )
        .await
        .unwrap();
    writer
        .put(
            &Path::from("del/b.txt"),
            PutPayload::from(Bytes::from_static(b"bbb")),
        )
        .await
        .unwrap();
    writer.flush_index().unwrap();

    let reader = RawObjectStore::open_readonly(path).unwrap();
    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 2);

    // Writer deletes one file and flushes
    writer.delete(&Path::from("del/a.txt")).await.unwrap();
    writer.flush_index().unwrap();

    // Reader reloads and should see only 1 file
    let changed = reader.reload_index().unwrap();
    assert!(changed);
    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 1, "reader should see 1 file after delete+reload");
    assert_eq!(files[0].location, Path::from("del/b.txt"));
}

// ===========================================================================
// TEST 10: reload_index picks up overwrites
// ===========================================================================

#[tokio::test]
async fn reload_index_sees_overwrites() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    writer
        .put(
            &Path::from("ow.txt"),
            PutPayload::from(Bytes::from_static(b"version1")),
        )
        .await
        .unwrap();
    writer.flush_index().unwrap();

    let reader = RawObjectStore::open_readonly(path).unwrap();
    let data = reader
        .get(&Path::from("ow.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), b"version1");

    // Writer overwrites and flushes
    writer
        .put(
            &Path::from("ow.txt"),
            PutPayload::from(Bytes::from_static(b"version2-longer")),
        )
        .await
        .unwrap();
    writer.flush_index().unwrap();

    // Reader reloads and sees the new content
    let changed = reader.reload_index().unwrap();
    assert!(changed);
    let data = reader
        .get(&Path::from("ow.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), b"version2-longer");
}

// ===========================================================================
// TEST 11: Multiple reload cycles
// ===========================================================================

/// Writer does multiple put+flush cycles. Reader reloads after each and
/// sees the cumulative state.
#[tokio::test]
async fn reload_index_multiple_cycles() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    let reader = RawObjectStore::open_readonly(path).unwrap();

    for i in 0..10 {
        writer
            .put(
                &Path::from(format!("cycle/{i:03}.dat")),
                PutPayload::from(Bytes::from(vec![(i & 0xFF) as u8; 1024])),
            )
            .await
            .unwrap();
        writer.flush_index().unwrap();

        let changed = reader.reload_index().unwrap();
        assert!(changed, "cycle {i} should detect a change");

        let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
        assert_eq!(files.len(), i + 1, "after cycle {i} reader should see {} files", i + 1);
    }
}

// ===========================================================================
// TEST 12: Reader reload with no flush between puts (no change)
// ===========================================================================

/// If the writer puts data but does NOT flush, reload_index returns false.
#[tokio::test]
async fn reload_index_unflushed_invisible() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    let reader = RawObjectStore::open_readonly(path).unwrap();

    // Writer puts but doesn't flush
    writer
        .put(
            &Path::from("invisible.txt"),
            PutPayload::from(Bytes::from_static(b"ghost")),
        )
        .await
        .unwrap();

    let changed = reader.reload_index().unwrap();
    assert!(!changed, "reload should see no change (no flush)");

    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 0, "unflushed data should be invisible to reader");
}

// ===========================================================================
// TEST 13: needs_flush reports dirty state
// ===========================================================================

#[tokio::test]
async fn needs_flush_tracks_dirty() {
    let (store, _tmp) = make_store();

    // Fresh store is not dirty
    assert!(!store.needs_flush(), "fresh store should not need flush");

    // After a put, it should be dirty
    store
        .put(
            &Path::from("dirty.txt"),
            PutPayload::from(Bytes::from_static(b"data")),
        )
        .await
        .unwrap();
    assert!(store.needs_flush(), "store should be dirty after put");

    // After flush, clean again
    store.flush_index().unwrap();
    assert!(!store.needs_flush(), "store should be clean after flush");

    // After delete, dirty again
    store.delete(&Path::from("dirty.txt")).await.unwrap();
    assert!(store.needs_flush(), "store should be dirty after delete");

    store.flush_index().unwrap();
    assert!(!store.needs_flush(), "clean after second flush");
}

// ===========================================================================
// TEST 14: Reader is_read_only, writer is not
// ===========================================================================

#[test]
fn is_read_only_flag() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    assert!(!writer.is_read_only(), "writer should not be read-only");

    let reader = RawObjectStore::open_readonly(path).unwrap();
    assert!(reader.is_read_only(), "reader should be read-only");
}

// ===========================================================================
// TEST 15: Reader cannot write
// ===========================================================================

/// All mutating operations on a read-only store return ReadOnly error.
#[tokio::test]
async fn reader_rejects_writes() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let _writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    let reader = RawObjectStore::open_readonly(path).unwrap();

    // put
    let err = reader
        .put(
            &Path::from("nope.txt"),
            PutPayload::from(Bytes::from_static(b"nope")),
        )
        .await;
    assert!(err.is_err(), "put on reader should fail");

    // delete
    let err = reader.delete(&Path::from("nope.txt")).await;
    assert!(err.is_err(), "delete on reader should fail");

    // copy
    let err = reader
        .copy(&Path::from("a"), &Path::from("b"))
        .await;
    assert!(err.is_err(), "copy on reader should fail");

    // rename
    let err = reader
        .rename(&Path::from("a"), &Path::from("b"))
        .await;
    assert!(err.is_err(), "rename on reader should fail");
}

// ===========================================================================
// TEST 16: Rapid open/close cycles don't leak flocks
// ===========================================================================

/// Open and close a store 20 times in a loop. Each open should succeed
/// because the previous handle is dropped (flock released).
#[tokio::test]
async fn flock_rapid_open_close() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Initial format
    {
        let store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
        store
            .put(
                &Path::from("persist.txt"),
                PutPayload::from(Bytes::from_static(b"here")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
    }

    for i in 0..20 {
        let store = RawObjectStore::open(&path_buf).unwrap();
        let data = store
            .get(&Path::from("persist.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"here", "cycle {i} data mismatch");
        // store dropped at end of loop iteration
    }
}

// ===========================================================================
// TEST 17: reload_index across many files
// ===========================================================================

/// Writer adds 100 files in batches, reader reloads and verifies counts.
#[tokio::test]
async fn reload_index_bulk() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path();

    let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
    let reader = RawObjectStore::open_readonly(path).unwrap();

    // Batch 1: 50 files
    for i in 0..50 {
        writer
            .put(
                &Path::from(format!("batch1/{i:03}")),
                PutPayload::from(Bytes::from(vec![i as u8; 512])),
            )
            .await
            .unwrap();
    }
    writer.flush_index().unwrap();

    let changed = reader.reload_index().unwrap();
    assert!(changed);
    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 50);

    // Batch 2: 50 more files
    for i in 50..100 {
        writer
            .put(
                &Path::from(format!("batch2/{i:03}")),
                PutPayload::from(Bytes::from(vec![i as u8; 512])),
            )
            .await
            .unwrap();
    }
    writer.flush_index().unwrap();

    let changed = reader.reload_index().unwrap();
    assert!(changed);
    let files: Vec<_> = reader.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 100);
}
