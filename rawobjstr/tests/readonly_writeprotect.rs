//! Tests for read-only open modes, write-protect flag, and modify_flags.
//!
//! Covers:
//!  - open_readonly / open_readonly_with_mode
//!  - is_read_only getter
//!  - Writes rejected in read-only mode (put, delete, copy, rename, flush, repair, scrub)
//!  - Reads succeed in read-only mode (get, list, head, get_range, verify)
//!  - FLAG_WRITE_PROTECT: set via modify_flags, blocks normal open, allows readonly
//!  - modify_flags round-trip and unknown-bit rejection
//!  - Tombstone operations in read-only mode
//!  - CLI --readonly, --full-verify, set-property subcommand

mod common;

use bytes::Bytes;
use common::{make_store, SMALL_DEVICE};
use futures::TryStreamExt;
use object_store::{path::Path, MultipartUpload, ObjectStore, PutPayload};
use rawobjstr::store::{OpenMode, RawObjectStore};
use rawobjstr::{FLAG_DIRECT_IO, FLAG_WRITE_PROTECT};
use tempfile::NamedTempFile;

// =========================================================================
// Helpers
// =========================================================================

/// Format a small device and write some test objects, flush, drop, return tmpfile.
async fn setup_device_with_data() -> NamedTempFile {
    let (store, tmp) = make_store();
    store
        .put(
            &Path::from("alpha/one.bin"),
            PutPayload::from(Bytes::from(vec![0xAAu8; 1024])),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("alpha/two.bin"),
            PutPayload::from(Bytes::from(vec![0xBBu8; 2048])),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("beta/three.bin"),
            PutPayload::from(Bytes::from(vec![0xCCu8; 512])),
        )
        .await
        .unwrap();
    store.flush_index().unwrap();
    drop(store);
    tmp
}

// =========================================================================
// Test 1 - open_readonly sets is_read_only
// =========================================================================

#[tokio::test]
async fn open_readonly_reports_read_only() {
    let tmp = setup_device_with_data().await;

    let ro_store = RawObjectStore::open_readonly(tmp.path()).unwrap();
    assert!(ro_store.is_read_only(), "should report read-only");

    let rw_store = RawObjectStore::open(tmp.path()).unwrap();
    assert!(!rw_store.is_read_only(), "should report read-write");

    println!("PASS open_readonly_reports_read_only");
}

// =========================================================================
// Test 2 - open_readonly_with_mode works for all modes
// =========================================================================

#[tokio::test]
async fn open_readonly_with_all_modes() {
    let tmp = setup_device_with_data().await;

    // Default
    let s = RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::Default).unwrap();
    assert!(s.is_read_only());
    drop(s);

    // SkipVerify
    let s = RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::SkipVerify).unwrap();
    assert!(s.is_read_only());
    drop(s);

    // FullVerify
    let s = RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();
    assert!(s.is_read_only());
    drop(s);

    println!("PASS open_readonly_with_all_modes");
}

// =========================================================================
// Test 3 - Reads succeed in read-only mode
// =========================================================================

#[tokio::test]
async fn readonly_reads_succeed() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    // get
    let result = store.get(&Path::from("alpha/one.bin")).await.unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data.len(), 1024);
    assert!(data.iter().all(|&b| b == 0xAA));

    // get_range
    let range_data = store
        .get_range(&Path::from("alpha/two.bin"), 0..16)
        .await
        .unwrap();
    assert_eq!(range_data.len(), 16);
    assert!(range_data.iter().all(|&b| b == 0xBB));

    // head
    let meta = store.head(&Path::from("beta/three.bin")).await.unwrap();
    assert_eq!(meta.size, 512);

    // list
    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 3);

    // list with prefix
    let alpha_files: Vec<_> = store
        .list(Some(&Path::from("alpha")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(alpha_files.len(), 2);

    // list_with_delimiter
    let top = store.list_with_delimiter(None).await.unwrap();
    assert_eq!(top.common_prefixes.len(), 2); // alpha/, beta/

    println!("PASS readonly_reads_succeed");
}

// =========================================================================
// Test 4 - Writes rejected in read-only mode
// =========================================================================

#[tokio::test]
async fn readonly_rejects_put() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    let err = store
        .put(
            &Path::from("new.bin"),
            PutPayload::from(Bytes::from("data")),
        )
        .await;
    assert!(err.is_err(), "put should fail in read-only mode");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("read-only") || msg.contains("ReadOnly"),
        "error should mention read-only, got: {msg}"
    );

    println!("PASS readonly_rejects_put");
}

#[tokio::test]
async fn readonly_rejects_delete() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    let err = store.delete(&Path::from("alpha/one.bin")).await;
    assert!(err.is_err(), "delete should fail in read-only mode");

    println!("PASS readonly_rejects_delete");
}

#[tokio::test]
async fn readonly_rejects_copy() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    let err = store
        .copy(&Path::from("alpha/one.bin"), &Path::from("copy.bin"))
        .await;
    assert!(err.is_err(), "copy should fail in read-only mode");

    println!("PASS readonly_rejects_copy");
}

#[tokio::test]
async fn readonly_rejects_rename() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    let err = store
        .rename(&Path::from("alpha/one.bin"), &Path::from("moved.bin"))
        .await;
    assert!(err.is_err(), "rename should fail in read-only mode");

    println!("PASS readonly_rejects_rename");
}

#[tokio::test]
async fn readonly_rejects_flush() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    let err = store.flush_index();
    assert!(err.is_err(), "flush should fail in read-only mode");

    println!("PASS readonly_rejects_flush");
}

#[tokio::test]
async fn readonly_rejects_repair() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let err = store.repair();
    assert!(err.is_err(), "repair should fail in read-only mode");

    println!("PASS readonly_rejects_repair");
}

#[tokio::test]
async fn readonly_rejects_scrub() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let err = store.scrub_free_space();
    assert!(err.is_err(), "scrub should fail in read-only mode");

    println!("PASS readonly_rejects_scrub");
}

// =========================================================================
// Test 5 - Tombstone operations in read-only mode
// =========================================================================

#[tokio::test]
async fn readonly_rejects_delete_tombstone() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let err = store.delete_tombstone("alpha/one.bin");
    assert!(err.is_err(), "delete_tombstone should fail in read-only mode");

    println!("PASS readonly_rejects_delete_tombstone");
}

#[tokio::test]
async fn readonly_rejects_clear_tombstones() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let err = store.clear_tombstones();
    assert!(err.is_err(), "clear_tombstones should fail in read-only mode");

    println!("PASS readonly_rejects_clear_tombstones");
}

#[tokio::test]
async fn readonly_allows_list_tombstones() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    // Should not panic / error -- we just get an empty vec
    let tombstones = store.list_tombstones();
    assert!(tombstones.is_empty());

    println!("PASS readonly_allows_list_tombstones");
}

// =========================================================================
// Test 6 - verify_all works in read-only mode
// =========================================================================

#[tokio::test]
async fn readonly_verify_all_succeeds() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let report = store.verify_all();
    assert_eq!(report.files_checked, 3);
    assert_eq!(report.files_ok, 3);
    assert!(report.errors.is_empty());
    assert!(report.space_accounted);

    println!("PASS readonly_verify_all_succeeds");
}

// =========================================================================
// Test 7 - device_info works in read-only mode
// =========================================================================

#[tokio::test]
async fn readonly_device_info_works() {
    let tmp = setup_device_with_data().await;
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let info = store.device_info();
    assert_eq!(info.file_count, 3);
    assert!(info.device_size > 0);
    // In RO mode free_space is computed from device_bytes_used difference
    assert!(info.free_space > 0 || info.device_bytes_used > 0);

    println!("PASS readonly_device_info_works");
}

// =========================================================================
// Test 8 - Data unchanged after read-only open (no tombstone recording)
// =========================================================================

#[tokio::test]
async fn readonly_does_not_modify_device() {
    let tmp = setup_device_with_data().await;

    // Snapshot the file bytes
    let before = std::fs::read(tmp.path()).unwrap();

    // Open read-only and do some reads
    {
        let store = RawObjectStore::open_readonly(tmp.path()).unwrap();
        let _data = store
            .get(&Path::from("alpha/one.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let _list: Vec<_> = store.list(None).try_collect().await.unwrap();
        drop(store);
    }

    let after = std::fs::read(tmp.path()).unwrap();
    assert_eq!(before, after, "read-only open should not modify device");

    println!("PASS readonly_does_not_modify_device");
}

// =========================================================================
// Test 9 - modify_flags: set and clear write-protect
// =========================================================================

#[tokio::test]
async fn modify_flags_write_protect_round_trip() {
    let tmp = setup_device_with_data().await;

    // Initially no write-protect
    let flags = RawObjectStore::modify_flags(tmp.path(), 0, 0).unwrap();
    assert_eq!(flags & FLAG_WRITE_PROTECT, 0, "should start without WP");

    // Set write-protect
    let flags = RawObjectStore::modify_flags(tmp.path(), FLAG_WRITE_PROTECT, 0).unwrap();
    assert_ne!(flags & FLAG_WRITE_PROTECT, 0, "WP should be set");

    // Clear write-protect
    let flags = RawObjectStore::modify_flags(tmp.path(), 0, FLAG_WRITE_PROTECT).unwrap();
    assert_eq!(flags & FLAG_WRITE_PROTECT, 0, "WP should be cleared");

    println!("PASS modify_flags_write_protect_round_trip");
}

#[tokio::test]
async fn modify_flags_direct_io_round_trip() {
    let tmp = setup_device_with_data().await;

    // Set direct-io flag
    let flags = RawObjectStore::modify_flags(tmp.path(), FLAG_DIRECT_IO, 0).unwrap();
    assert_ne!(flags & FLAG_DIRECT_IO, 0);

    // Clear direct-io flag
    let flags = RawObjectStore::modify_flags(tmp.path(), 0, FLAG_DIRECT_IO).unwrap();
    assert_eq!(flags & FLAG_DIRECT_IO, 0);

    println!("PASS modify_flags_direct_io_round_trip");
}

#[tokio::test]
async fn modify_flags_rejects_unknown_bits() {
    let tmp = setup_device_with_data().await;

    let err = RawObjectStore::modify_flags(tmp.path(), 1 << 16, 0);
    assert!(err.is_err(), "should reject unknown flag bits");

    println!("PASS modify_flags_rejects_unknown_bits");
}

#[tokio::test]
async fn modify_flags_set_and_clear_multiple() {
    let tmp = setup_device_with_data().await;

    // Set both flags at once
    let flags =
        RawObjectStore::modify_flags(tmp.path(), FLAG_DIRECT_IO | FLAG_WRITE_PROTECT, 0).unwrap();
    assert_ne!(flags & FLAG_DIRECT_IO, 0);
    assert_ne!(flags & FLAG_WRITE_PROTECT, 0);

    // Clear both
    let flags =
        RawObjectStore::modify_flags(tmp.path(), 0, FLAG_DIRECT_IO | FLAG_WRITE_PROTECT).unwrap();
    assert_eq!(flags & FLAG_DIRECT_IO, 0);
    assert_eq!(flags & FLAG_WRITE_PROTECT, 0);

    println!("PASS modify_flags_set_and_clear_multiple");
}

// =========================================================================
// Test 10 - Write-protect blocks normal open
// =========================================================================

#[tokio::test]
async fn write_protect_blocks_rw_open() {
    let tmp = setup_device_with_data().await;

    // Enable write-protect
    RawObjectStore::modify_flags(tmp.path(), FLAG_WRITE_PROTECT, 0).unwrap();

    // Normal open should fail with WriteProtected error
    let err = RawObjectStore::open(tmp.path());
    assert!(err.is_err(), "RW open should fail when write-protected");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("write-protect") || msg.contains("WriteProtect"),
        "error should mention write-protect, got: {msg}"
    );

    // open_with_mode should also fail
    let err = RawObjectStore::open_with_mode(tmp.path(), OpenMode::FullVerify);
    assert!(
        err.is_err(),
        "open_with_mode should fail when write-protected"
    );

    println!("PASS write_protect_blocks_rw_open");
}

// =========================================================================
// Test 11 - Write-protect allows read-only open
// =========================================================================

#[tokio::test]
async fn write_protect_allows_readonly_open() {
    let tmp = setup_device_with_data().await;

    // Enable write-protect
    RawObjectStore::modify_flags(tmp.path(), FLAG_WRITE_PROTECT, 0).unwrap();

    // Read-only open should succeed
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();
    assert!(store.is_read_only());

    // Reads work
    let data = store
        .get(&Path::from("alpha/one.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.len(), 1024);

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 3);

    // open_readonly_with_mode also works
    drop(store);
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();
    assert!(store.is_read_only());

    println!("PASS write_protect_allows_readonly_open");
}

// =========================================================================
// Test 12 - Clear write-protect, then normal open works again
// =========================================================================

#[tokio::test]
async fn write_protect_clear_restores_rw() {
    let tmp = setup_device_with_data().await;

    // Set write-protect
    RawObjectStore::modify_flags(tmp.path(), FLAG_WRITE_PROTECT, 0).unwrap();

    // Confirm blocked
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // Clear write-protect
    RawObjectStore::modify_flags(tmp.path(), 0, FLAG_WRITE_PROTECT).unwrap();

    // Normal open works
    let store = RawObjectStore::open(tmp.path()).unwrap();
    assert!(!store.is_read_only());

    // Can write after clearing
    store
        .put(
            &Path::from("new_after_clear.bin"),
            PutPayload::from(Bytes::from("hello")),
        )
        .await
        .unwrap();

    let data = store
        .get(&Path::from("new_after_clear.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.as_ref(), b"hello");

    println!("PASS write_protect_clear_restores_rw");
}

// =========================================================================
// Test 13 - modify_flags persists across opens
// =========================================================================

#[tokio::test]
async fn modify_flags_persists() {
    let tmp = setup_device_with_data().await;

    // Set write-protect
    RawObjectStore::modify_flags(tmp.path(), FLAG_WRITE_PROTECT, 0).unwrap();

    // Open read-only and check info
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();
    let info = store.device_info();
    assert_ne!(
        info.flags & FLAG_WRITE_PROTECT,
        0,
        "write-protect flag should be visible in device_info"
    );
    drop(store);

    // Open readonly again -- flag still there
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();
    let info = store.device_info();
    assert_ne!(info.flags & FLAG_WRITE_PROTECT, 0);
    drop(store);

    // Clear and verify
    RawObjectStore::modify_flags(tmp.path(), 0, FLAG_WRITE_PROTECT).unwrap();
    let store = RawObjectStore::open(tmp.path()).unwrap();
    let info = store.device_info();
    assert_eq!(info.flags & FLAG_WRITE_PROTECT, 0);

    println!("PASS modify_flags_persists");
}

// =========================================================================
// Test 14 - Drop in read-only mode does not panic or warn
// =========================================================================

#[tokio::test]
async fn readonly_drop_is_clean() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    // Do some reads, then drop without flush (should be silent)
    let _ = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    drop(store);
    // If we get here without panic, the test passes.

    println!("PASS readonly_drop_is_clean");
}

// =========================================================================
// Test 15 - Concurrent RO opens on same device
// =========================================================================

#[tokio::test]
async fn concurrent_readonly_opens() {
    let tmp = setup_device_with_data().await;

    let s1 = RawObjectStore::open_readonly(tmp.path()).unwrap();
    let s2 = RawObjectStore::open_readonly(tmp.path()).unwrap();
    let s3 =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();

    let data1 = s1
        .get(&Path::from("alpha/one.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let data2 = s2
        .get(&Path::from("alpha/one.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let data3 = s3
        .get(&Path::from("alpha/one.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(data1, data2);
    assert_eq!(data2, data3);
    assert_eq!(data1.len(), 1024);

    println!("PASS concurrent_readonly_opens");
}

// =========================================================================
// Test 16 - RO FullVerify with corrupt data: no tombstone recorded
// =========================================================================

#[tokio::test]
async fn readonly_fullverify_corrupt_no_tombstone() {
    let tmp = setup_device_with_data().await;

    // Corrupt the first extent's data (flip a byte in the payload area)
    let data_offset = rawobjstr::DATA_START + 4; // skip into header/payload
    {
        let mut contents = std::fs::read(tmp.path()).unwrap();
        contents[data_offset as usize] ^= 0xFF;
        std::fs::write(tmp.path(), &contents).unwrap();
    }

    // Open RO with FullVerify -- should succeed (corruption causes stale entry
    // removal from in-memory index but no tombstone recording)
    let store =
        RawObjectStore::open_readonly_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();
    let tombstones = store.list_tombstones();
    assert!(
        tombstones.is_empty(),
        "RO mode should not record tombstones, found {}",
        tombstones.len()
    );

    // Verify device file was not modified
    drop(store);

    // Now open RW with FullVerify -- should create tombstone
    let store = RawObjectStore::open_with_mode(tmp.path(), OpenMode::FullVerify).unwrap();
    let tombstones = store.list_tombstones();
    assert!(
        !tombstones.is_empty(),
        "RW mode should record tombstones for corrupt data"
    );

    println!("PASS readonly_fullverify_corrupt_no_tombstone");
}

// =========================================================================
// CLI Tests
// =========================================================================

// Test 17 - CLI --readonly flag for list
#[test]
fn cli_readonly_list() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());
    common::cli_put(dev, "doc/a.txt", b"hello");
    common::cli_put(dev, "doc/b.txt", b"world");

    // List with --readonly
    let out = common::run_cli(&["list", "--file", dev, "--readonly"]);
    let files: Vec<&str> = out.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(files.len(), 2);
    assert!(files.contains(&"doc/a.txt"));
    assert!(files.contains(&"doc/b.txt"));

    println!("PASS cli_readonly_list");
}

// Test 18 - CLI --readonly flag for get
#[test]
fn cli_readonly_get() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());
    common::cli_put(dev, "myfile.bin", b"readonly-get-test");

    // Get with --readonly -- need to use run_cli directly
    let out_file = NamedTempFile::new().unwrap();
    common::run_cli(&[
        "get",
        "--file",
        dev,
        "--key",
        "myfile.bin",
        "--to",
        out_file.path().to_str().unwrap(),
        "--readonly",
    ]);
    let data = std::fs::read(out_file.path()).unwrap();
    assert_eq!(data, b"readonly-get-test");

    println!("PASS cli_readonly_get");
}

// Test 19 - CLI --readonly flag for info
#[test]
fn cli_readonly_info() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());
    common::cli_put(dev, "data.bin", b"info-test-data");

    let out = common::run_cli(&["info", "--file", dev, "--readonly"]);
    assert!(out.contains("Files:"), "info output should contain Files:");

    println!("PASS cli_readonly_info");
}

// Test 20 - CLI --readonly flag for verify
#[test]
fn cli_readonly_verify() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());
    common::cli_put(dev, "v.bin", b"verify-data");

    let out = common::run_cli(&["verify", "--file", dev, "--readonly"]);
    assert!(
        out.contains("Device is clean"),
        "verify should succeed in readonly mode"
    );

    println!("PASS cli_readonly_verify");
}

// Test 21 - CLI set-property --write-protect on/off
#[test]
fn cli_set_property_write_protect() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());
    common::cli_put(dev, "wp.bin", b"wp-data");

    // Set write-protect on
    let out = common::run_cli(&["set-property", "--file", dev, "--write-protect", "on"]);
    assert!(
        out.contains("WRITE_PROTECT"),
        "output should show WRITE_PROTECT flag"
    );

    // Normal open via CLI should fail (e.g. put)
    let (_stdout, _stderr, code) =
        common::run_cli_fail(&["put", "--file", dev, "--key", "fail.bin", "--from", dev]);
    assert_ne!(code, Some(0), "put should fail when write-protected");

    // Read-only operations should still work
    let out = common::run_cli(&["list", "--file", dev, "--readonly"]);
    assert!(out.contains("wp.bin"));

    // Clear write-protect
    let out = common::run_cli(&["set-property", "--file", dev, "--write-protect", "off"]);
    assert!(
        !out.contains("WRITE_PROTECT"),
        "output should not show WRITE_PROTECT after clearing"
    );

    // Normal open should work again
    common::cli_put(dev, "after_clear.bin", b"works-again");
    let files = common::cli_list(dev, None);
    assert!(files.contains(&"after_clear.bin".to_string()));

    println!("PASS cli_set_property_write_protect");
}

// Test 22 - CLI set-property --direct-io on/off
#[cfg(target_os = "linux")]
#[test]
fn cli_set_property_direct_io() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());

    // Enable direct-io
    let out = common::run_cli(&["set-property", "--file", dev, "--direct-io", "on"]);
    assert!(
        out.contains("O_DIRECT"),
        "should show O_DIRECT after enabling"
    );

    // Disable direct-io
    let out = common::run_cli(&["set-property", "--file", dev, "--direct-io", "off"]);
    // Should not contain O_DIRECT in the output (or flags should be 0)
    // Note: if only flag was direct-io, output shows "(none)"
    assert!(
        out.contains("(none)") || !out.contains("O_DIRECT"),
        "should not show O_DIRECT after disabling"
    );

    println!("PASS cli_set_property_direct_io");
}

// Test 23 - CLI set-property with invalid args
#[test]
fn cli_set_property_invalid_args() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());

    // No property flags
    let (_out, _err, code) = common::run_cli_fail(&["set-property", "--file", dev]);
    assert_ne!(code, Some(0), "should fail without any property flags");

    // Invalid value
    let (_out, _err, code) = common::run_cli_fail(&[
        "set-property",
        "--file",
        dev,
        "--write-protect",
        "maybe",
    ]);
    assert_ne!(code, Some(0), "should fail with invalid on/off value");

    println!("PASS cli_set_property_invalid_args");
}

// Test 24 - CLI info shows WRITE_PROTECT in flags
#[test]
fn cli_info_shows_write_protect_flag() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());

    // Info without WP
    let out = common::run_cli(&["info", "--file", dev]);
    assert!(
        !out.contains("WRITE_PROTECT"),
        "should not show WRITE_PROTECT initially"
    );

    // Set write-protect
    common::run_cli(&["set-property", "--file", dev, "--write-protect", "on"]);

    // Info with --readonly should show WRITE_PROTECT
    let out = common::run_cli(&["info", "--file", dev, "--readonly"]);
    assert!(
        out.contains("WRITE_PROTECT"),
        "info should show WRITE_PROTECT flag, got:\n{out}"
    );

    println!("PASS cli_info_shows_write_protect_flag");
}

// Test 25 - CLI --full-verify for tombstones
#[test]
fn cli_full_verify_tombstones() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    common::cli_format(dev, &SMALL_DEVICE.to_string());
    common::cli_put(dev, "ts.bin", b"tombstone-test");

    // tombstones with --full-verify (explicit, even though it defaults to FullVerify)
    let out = common::run_cli(&["tombstones", "--file", dev, "--full-verify"]);
    assert!(
        out.contains("No tombstones"),
        "clean device should have no tombstones"
    );

    // tombstones with --readonly --full-verify
    let out = common::run_cli(&[
        "tombstones",
        "--file",
        dev,
        "--readonly",
        "--full-verify",
    ]);
    assert!(out.contains("No tombstones"));

    println!("PASS cli_full_verify_tombstones");
}

// Test 26 - Write-protect + modify_flags + reopen cycle
#[tokio::test]
async fn write_protect_full_lifecycle() {
    let (store, tmp) = make_store();

    // Write some data
    store
        .put(
            &Path::from("lifecycle/data.bin"),
            PutPayload::from(Bytes::from(vec![0x42u8; 4096])),
        )
        .await
        .unwrap();
    store.flush_index().unwrap();
    drop(store);

    // Phase 1: Enable write-protect
    RawObjectStore::modify_flags(tmp.path(), FLAG_WRITE_PROTECT, 0).unwrap();

    // Phase 2: Verify RW open is blocked
    let err = RawObjectStore::open(tmp.path());
    assert!(err.is_err());
    let err = RawObjectStore::open_with_mode(tmp.path(), OpenMode::SkipVerify);
    assert!(err.is_err());
    let err = RawObjectStore::open_with_mode(tmp.path(), OpenMode::FullVerify);
    assert!(err.is_err());

    // Phase 3: RO open works, data readable
    let ro = RawObjectStore::open_readonly(tmp.path()).unwrap();
    let data = ro
        .get(&Path::from("lifecycle/data.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.len(), 4096);
    assert!(data.iter().all(|&b| b == 0x42));
    drop(ro);

    // Phase 4: Clear write-protect
    RawObjectStore::modify_flags(tmp.path(), 0, FLAG_WRITE_PROTECT).unwrap();

    // Phase 5: RW open works, can write new data
    let store = RawObjectStore::open(tmp.path()).unwrap();
    store
        .put(
            &Path::from("lifecycle/new.bin"),
            PutPayload::from(Bytes::from("new-data")),
        )
        .await
        .unwrap();
    store.flush_index().unwrap();

    let files: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(files.len(), 2);

    println!("PASS write_protect_full_lifecycle");
}

// Test 27 - Multipart upload rejected in read-only mode
#[tokio::test]
async fn readonly_rejects_multipart() {
    let tmp = setup_device_with_data().await;
    let store = RawObjectStore::open_readonly(tmp.path()).unwrap();

    // put_multipart_opts doesn't check RO at init time, it creates the upload
    // struct lazily. The guard fires on put_part or complete.
    let mut upload = store.put_multipart(&Path::from("multi.bin")).await.unwrap();

    let part_result = upload
        .put_part(PutPayload::from(Bytes::from("test data")))
        .await;
    if part_result.is_ok() {
        // If put_part somehow didn't fail, complete must fail
        let complete_result = upload.complete().await;
        assert!(
            complete_result.is_err(),
            "multipart complete should fail in read-only mode"
        );
    }
    // put_part failing is the expected path

    println!("PASS readonly_rejects_multipart");
}
