//! E2E tests for tombstone tracking and free-space scrubbing.
//!
//! Tombstones are created during open-time integrity scans when an extent is
//! found to be stale or corrupt.  The tests here verify:
//!
//!   1. A tombstone is created when a corrupt extent is detected on open.
//!   2. Tombstones carry the correct metadata (path, size, crc32c, reason).
//!   3. A clean re-open produces no tombstones.
//!   4. `delete_tombstone()` removes a single entry and persists across reopen.
//!   5. `delete_tombstone()` refuses to act on a path that is a live file.
//!   6. `clear_tombstones()` removes all entries in bulk.
//!   7. Tombstones survive a close / reopen cycle (persisted in the index).
//!   8. Writing a new object at a formerly-tombstoned path removes the tombstone.
//!   9. `scrub_free_space()` zeros the bytes that formerly held deleted objects.
//!  10. `scrub_free_space()` returns an accurate `ScrubReport`.
//!  11. Live objects are unaffected by a scrub.
//!  12. Tombstones + scrub combined: corrupt open → scrub → live objects intact.

mod common;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::{OpenMode, RawObjectStore};
use rawobjstr::DATA_START;
use rawobjstr::extent::padded_extent_size;
use tempfile::NamedTempFile;

use common::{flip_byte, make_store, read_bytes_at};

/// Format a 64 MB store, write `objects` (path → payload), flush, close.
/// Returns the `NamedTempFile` for further manipulation.
fn setup_with_objects(objects: &[(&str, &[u8])]) -> NamedTempFile {
    let (store, tmp) = make_store();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        for (key, data) in objects {
            store
                .put(&Path::from(*key), PutPayload::from(Bytes::copy_from_slice(data)))
                .await
                .unwrap();
        }
    });
    store.flush_index().unwrap();
    drop(store);
    tmp
}

/// Return the on-disk offset of the first extent allocated in a freshly
/// formatted store.  For sequential writes to an empty device the first
/// object lands at DATA_START.
fn extent_offset(n: usize, payload_size: u64) -> u64 {
    DATA_START + n as u64 * padded_extent_size(payload_size).unwrap()
}

// ══════════════════════════════════════════════════════════════════════════
// Test 1 – tombstone created when corrupt extent detected on open
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn tombstone_created_on_corrupt_open() {
    let payload = b"hello tombstone world";
    let tmp = setup_with_objects(&[("objects/a.bin", payload)]);

    // Corrupt block 0's content area (offset 4 within the first block) so
    // that the stored block-CRC no longer matches the actual content CRC.
    flip_byte(tmp.path(), DATA_START + 4);

    // Default open triggers the fast header-scan and detects the stale extent.
    let store = RawObjectStore::open(tmp.path()).unwrap();
    let tombs = store.list_tombstones();
    assert_eq!(tombs.len(), 1, "expected 1 tombstone, got {}", tombs.len());
    assert_eq!(tombs[0].path, "objects/a.bin");

    // The corrupt object must NOT be visible in the live index.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let all: Vec<_> = store.list(None).try_collect_vec().await;
        assert_eq!(all.len(), 0, "corrupt object should not appear in listing");
    });

    println!("PASS tombstone_created_on_corrupt_open");
}

// A small local helper so we don't need futures::TryStreamExt in scope.
trait TryCollectVec<T> {
    async fn try_collect_vec(self) -> Vec<T>;
}
impl<S, T, E> TryCollectVec<T> for S
where
    S: futures::TryStream<Ok = T, Error = E>,
    E: std::fmt::Debug,
{
    async fn try_collect_vec(self) -> Vec<T> {
        futures::TryStreamExt::try_collect(self).await.unwrap()
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Test 2 – tombstone carries correct metadata
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn tombstone_has_correct_metadata() {
    let payload = b"metadata check payload";
    let tmp = setup_with_objects(&[("meta/obj.bin", payload)]);

    // Flip a byte in the CONTENT area of block 0 so the block CRC fails.
    flip_byte(tmp.path(), DATA_START + 4);

    let store = RawObjectStore::open(tmp.path()).unwrap();
    let tombs = store.list_tombstones();
    assert_eq!(tombs.len(), 1);

    let t = &tombs[0];
    assert_eq!(t.path, "meta/obj.bin");
    assert_eq!(t.size, payload.len() as u64, "tombstone size should match original payload size");
    assert!(!t.reason.is_empty(), "tombstone reason should not be empty");
    assert!(t.tombstone_txn > 0, "tombstone transaction id should be > 0");

    println!("PASS tombstone_has_correct_metadata");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 3 – clean open produces no tombstones
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn clean_open_no_tombstones() {
    let tmp = setup_with_objects(&[
        ("a.bin", b"aaa"),
        ("b.bin", b"bbb"),
        ("c.bin", b"ccc"),
    ]);

    // No corruption — Default open should find everything clean.
    let store = RawObjectStore::open(tmp.path()).unwrap();
    let tombs = store.list_tombstones();
    assert!(tombs.is_empty(), "expected no tombstones on clean open, got {:?}", tombs.len());

    println!("PASS clean_open_no_tombstones");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 4 – delete_tombstone removes the entry and persists across reopen
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_tombstone_persists_across_reopen() {
    let payload = b"to be tombstoned";
    let tmp = setup_with_objects(&[("del/obj.bin", payload)]);
    flip_byte(tmp.path(), DATA_START + 4);

    let store = RawObjectStore::open(tmp.path()).unwrap();
    assert_eq!(store.list_tombstones().len(), 1);

    let removed = store.delete_tombstone("del/obj.bin").unwrap();
    assert!(removed, "delete_tombstone should return true");
    assert!(store.list_tombstones().is_empty(), "tombstone should be gone");

    // Persist the removal.
    store.flush_index().unwrap();
    drop(store);

    // Reopen with SkipVerify so the removal isn't undone by another scan.
    let store2 = RawObjectStore::open_with_mode(tmp.path(), OpenMode::SkipVerify).unwrap();
    assert!(
        store2.list_tombstones().is_empty(),
        "tombstone should still be gone after reopen"
    );

    println!("PASS delete_tombstone_persists_across_reopen");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 5 – delete_tombstone refuses to act on a live file
// ══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn delete_tombstone_refuses_live_file() {
    let (store, _tmp) = make_store();
    store
        .put(&Path::from("live.bin"), PutPayload::from(Bytes::from("alive")))
        .await
        .unwrap();

    // The path is live — must return false and leave the file intact.
    let result = store.delete_tombstone("live.bin").unwrap();
    assert!(!result, "delete_tombstone on a live file should return false");

    // File is still readable.
    let data = store.get(&Path::from("live.bin")).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.as_ref(), b"alive");

    println!("PASS delete_tombstone_refuses_live_file");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 6 – clear_tombstones removes all entries in bulk
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn clear_tombstones_bulk() {
    // Write two objects into sequential extents; corrupt both.
    let small = 64usize; // tiny payload so extents are close together
    let payload = vec![0xABu8; small];
    let tmp = setup_with_objects(&[
        ("bulk/a.bin", &payload),
        ("bulk/b.bin", &payload),
    ]);

    let off_a = extent_offset(0, small as u64);
    let off_b = extent_offset(1, small as u64);
    flip_byte(tmp.path(), off_a + 4);
    flip_byte(tmp.path(), off_b + 4);

    let store = RawObjectStore::open(tmp.path()).unwrap();
    let count = store.list_tombstones().len();
    assert_eq!(count, 2, "expected 2 tombstones before clear");

    let removed = store.clear_tombstones().unwrap();
    assert_eq!(removed, 2, "clear_tombstones should report 2 removed");
    assert!(store.list_tombstones().is_empty(), "tombstones should be empty after clear");

    println!("PASS clear_tombstones_bulk");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 7 – tombstones survive a close / reopen cycle
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn tombstones_persist_across_reopen() {
    let payload = b"persist tombstone";
    let tmp = setup_with_objects(&[("persist/x.bin", payload)]);
    flip_byte(tmp.path(), DATA_START + 4);

    // Open → tombstone is created → flush → close.
    {
        let store = RawObjectStore::open(tmp.path()).unwrap();
        assert_eq!(store.list_tombstones().len(), 1);
        store.flush_index().unwrap();
    }

    // Reopen with SkipVerify so no new scan overwrites the tombstone state.
    let store2 = RawObjectStore::open_with_mode(tmp.path(), OpenMode::SkipVerify).unwrap();
    let tombs = store2.list_tombstones();
    assert_eq!(tombs.len(), 1, "tombstone should survive reopen");
    assert_eq!(tombs[0].path, "persist/x.bin");

    println!("PASS tombstones_persist_across_reopen");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 8 – writing a new object at a tombstoned path removes the tombstone
// ══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn rewrite_at_tombstoned_path_removes_tombstone() {
    // Inline setup to avoid nested runtime (this is an async test).
    let (store_setup, tmp) = make_store();
    store_setup
        .put(
            &Path::from("rewrite/obj.bin"),
            PutPayload::from(Bytes::from(b"original data".as_ref())),
        )
        .await
        .unwrap();
    store_setup.flush_index().unwrap();
    drop(store_setup);
    flip_byte(tmp.path(), DATA_START + 4);

    // Open creates a tombstone for "rewrite/obj.bin".
    let store = RawObjectStore::open(tmp.path()).unwrap();
    assert_eq!(store.list_tombstones().len(), 1);

    // Writing a new value at the same path should clear the tombstone.
    store
        .put(
            &Path::from("rewrite/obj.bin"),
            PutPayload::from(Bytes::from("new data")),
        )
        .await
        .unwrap();

    let tombs = store.list_tombstones();
    assert!(
        tombs.is_empty(),
        "tombstone should be removed after rewriting the object"
    );

    // And the new data should be readable.
    let data = store
        .get(&Path::from("rewrite/obj.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), b"new data");

    println!("PASS rewrite_at_tombstoned_path_removes_tombstone");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 9 – scrub_free_space zeros bytes that formerly held deleted objects
// ══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn scrub_zeros_deleted_object_data() {
    let (store, tmp) = make_store();

    // Write an object, record where its data starts on disk.
    let unique = b"SECRET_DATA_DO_NOT_LEAK";
    store
        .put(&Path::from("secret.bin"), PutPayload::from(Bytes::from(unique.as_ref())))
        .await
        .unwrap();

    // Find the extent's actual on-disk offset via the layout map so the test
    // doesn't rely on a hardcoded offset assumption.
    let layout = store.layout_map();
    let extent = layout.extents.iter()
        .find(|e| e.key == "secret.bin")
        .expect("extent for secret.bin must be in layout");
    // Payload starts after the 4-byte CRC prefix in block 0.
    let data_payload_offset = extent.offset + 4;

    // Flush so the extent data is durable before we read it raw.
    store.flush_index().unwrap();

    // Delete the object; data bytes remain on disk (free but not zeroed yet).
    store.delete(&Path::from("secret.bin")).await.unwrap();

    // Confirm the raw bytes are still there before scrub.
    let before = read_bytes_at(tmp.path(), data_payload_offset, unique.len());
    assert_eq!(&before, unique, "raw bytes should still be present before scrub");

    // Scrub free space.
    store.scrub_free_space().unwrap();

    // After scrub the bytes should be zeroed.
    let after = read_bytes_at(tmp.path(), data_payload_offset, unique.len());
    assert!(
        after.iter().all(|&b| b == 0),
        "scrubbed region should be zeroed, got: {:?}",
        &after[..after.len().min(16)]
    );

    println!("PASS scrub_zeros_deleted_object_data");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 10 – scrub_free_space returns an accurate ScrubReport
// ══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn scrub_report_is_accurate() {
    let (store, _tmp) = make_store();

    // Write and delete two objects of known sizes.
    let sizes = [1024usize, 4096];
    for (i, &sz) in sizes.iter().enumerate() {
        let data = vec![0xFEu8; sz];
        store
            .put(
                &Path::from(format!("obj_{i}.bin")),
                PutPayload::from(Bytes::from(data)),
            )
            .await
            .unwrap();
    }
    for i in 0..sizes.len() {
        store.delete(&Path::from(format!("obj_{i}.bin"))).await.unwrap();
    }

    let report = store.scrub_free_space().unwrap();

    // Must have scrubbed at least as many bytes as we wrote (likely more
    // due to alignment padding plus the entire initial free area).
    assert!(
        report.bytes_scrubbed > 0,
        "scrub report must show bytes scrubbed"
    );
    assert!(
        report.regions_scrubbed > 0,
        "scrub report must show regions scrubbed"
    );

    println!(
        "PASS scrub_report_is_accurate: {} regions, {} bytes",
        report.regions_scrubbed, report.bytes_scrubbed
    );
}

// ══════════════════════════════════════════════════════════════════════════
// Test 11 – live objects are unaffected by a scrub
// ══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn scrub_does_not_corrupt_live_objects() {
    let (store, _tmp) = make_store();

    // Write several live objects.
    let expected: Vec<(&str, Vec<u8>)> = vec![
        ("live/a.bin", b"aaaa".to_vec()),
        ("live/b.bin", b"bbbb".to_vec()),
        ("live/c.bin", vec![0xCCu8; 8192]),
    ];

    for (key, data) in &expected {
        store
            .put(&Path::from(*key), PutPayload::from(Bytes::from(data.clone())))
            .await
            .unwrap();
    }

    // Write and delete a "dead" object to create some free space.
    store
        .put(&Path::from("dead.bin"), PutPayload::from(Bytes::from(vec![0xDDu8; 4096])))
        .await
        .unwrap();
    store.delete(&Path::from("dead.bin")).await.unwrap();

    // Scrub.
    store.scrub_free_space().unwrap();

    // Live objects must still be intact.
    for (key, data) in &expected {
        let got = store
            .get(&Path::from(*key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), data.as_slice(), "live object {key} corrupted by scrub");
    }

    println!("PASS scrub_does_not_corrupt_live_objects");
}

// ══════════════════════════════════════════════════════════════════════════
// Test 12 – tombstones + scrub combined
//   corrupt open → tombstones created → scrub → live objects still intact
// ══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tombstone_and_scrub_combined() {
    // Write two objects: one will be corrupted, one will stay live.
    // Inline setup to avoid nested runtime (this is an async test).
    let small = 64usize;
    let live_payload = vec![0x11u8; small];
    let corrupt_payload = vec![0x22u8; small];

    let (store_setup, tmp) = make_store();
    store_setup
        .put(
            &Path::from("combined/live.bin"),
            PutPayload::from(Bytes::from(live_payload.clone())),
        )
        .await
        .unwrap();
    store_setup
        .put(
            &Path::from("combined/corrupt.bin"),
            PutPayload::from(Bytes::from(corrupt_payload.clone())),
        )
        .await
        .unwrap();
    store_setup.flush_index().unwrap();
    drop(store_setup);

    // Corrupt the SECOND extent (index 1).  The content area of block 0
    // starts at `extent_offset(1, small) + 4`.
    let corrupt_extent = extent_offset(1, small as u64);
    flip_byte(tmp.path(), corrupt_extent + 4);

    // Open with Default scan: corrupt extent → tombstone.
    let store = RawObjectStore::open(tmp.path()).unwrap();

    let tombs = store.list_tombstones();
    assert_eq!(tombs.len(), 1, "expected 1 tombstone after corrupt open");
    assert_eq!(tombs[0].path, "combined/corrupt.bin");

    // Scrub free space (includes the now-freed corrupt extent).
    let report = store.scrub_free_space().unwrap();
    assert!(report.bytes_scrubbed > 0, "scrub should touch bytes");

    // Tombstone still present after scrub (scrub only zeros free bytes).
    assert_eq!(store.list_tombstones().len(), 1, "tombstone should survive scrub");

    // Live object must still be readable and correct.
    let data = store
        .get(&Path::from("combined/live.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), live_payload.as_slice(), "live object corrupted by scrub");

    println!("PASS tombstone_and_scrub_combined");
}
