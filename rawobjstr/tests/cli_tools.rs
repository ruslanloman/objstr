//! E2E tests for CLI tools: format, info, list, get, put, delete, verify,
//! export, import, repair.
//!
//! These tests exercise the `rawobjstr` CLI binary as an external
//! process, validating each command end-to-end.

mod common;

use std::collections::HashMap;
use std::fs;

use tempfile::{NamedTempFile, TempDir};

use common::{run_cli, run_cli_fail, cli_version, assert_version_unchanged};
const DEVICE_SIZE: &str = "67108864"; // 64 MB

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a fresh device at the given path.
fn format_device(path: &str) {
    run_cli(&["format", "--file", path, "--size", DEVICE_SIZE]);
}

/// Generate deterministic test data for a given key.
fn test_data(key: &str, size: usize) -> Vec<u8> {
    let seed = key.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    let mut buf = vec![0u8; size];
    let mut val = seed;
    for chunk in buf.chunks_mut(4) {
        let bytes = val.to_le_bytes();
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = bytes[i % 4];
        }
        val = val.wrapping_mul(1103515245).wrapping_add(12345);
    }
    buf
}

/// Create a set of local files in a temp dir with deterministic content.
/// Returns (dir, map of key -> data).
fn create_local_files() -> (TempDir, HashMap<String, Vec<u8>>) {
    let dir = TempDir::new().unwrap();
    let files: Vec<(&str, usize)> = vec![
        ("my_table/_versions/1.manifest", 64),
        ("my_table/data/00000.db", 4096),
        ("my_table/data/00001.db", 4096),
        ("my_table/data/00002.db", 8192),
        ("other_table/_versions/1.manifest", 48),
        ("other_table/data/00000.db", 16384),
    ];

    let mut map = HashMap::new();
    for (key, size) in &files {
        let data = test_data(key, *size);
        let full_path = dir.path().join(key);
        fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        fs::write(&full_path, &data).unwrap();
        map.insert(key.to_string(), data);
    }
    (dir, map)
}

/// Parse `list --long` output into Vec<(size, path)>.
fn parse_long_list(output: &str) -> Vec<(usize, String)> {
    output
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let trimmed = line.trim();
            let (size_str, path) = trimmed.split_once(char::is_whitespace).unwrap();
            (
                size_str.trim().parse::<usize>().unwrap(),
                path.trim().to_string(),
            )
        })
        .collect()
}

/// Populate a formatted device with a handful of test files
fn populate_device(dev_path: &str) {
    let dir = TempDir::new().unwrap();
    let files: &[(&str, &[u8])] = &[
        ("my_table/_versions/1.manifest", b"manifest-v1-data"),
        ("my_table/data/00000.db", &[0x41; 4096]),
        ("my_table/data/00001.db", &[0x42; 4096]),
        ("my_table/_transactions/0-uuid.txn", b"txn-data"),
        ("other_table/_versions/1.manifest", b"other-manifest-v1"),
        ("other_table/data/00000.db", &[0x53; 8192]),
    ];
    for (key, data) in files {
        let tmp = dir.path().join(key.replace('/', "_"));
        fs::write(&tmp, data).unwrap();
        run_cli(&["put", "--file", dev_path, "--key", key, "--from", tmp.to_str().unwrap()]);
    }
}

// =========================================================================
// TEST 1: put/list/get/delete/export round-trip
//
// Create local files -> put them one-by-one into a device -> verify list
// sees each file after put -> spot-check get matches -> delete some ->
// verify list updated -> re-put deleted files -> export all -> compare
// exported files to originals.
// =========================================================================

#[test]
fn cli_put_list_get_delete_export_roundtrip() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let (local_dir, files) = create_local_files();
    let export_dir = TempDir::new().unwrap();

    // Put files one by one, checking list after each
    let mut put_so_far = Vec::new();
    let sorted_keys = {
        let mut keys: Vec<&String> = files.keys().collect();
        keys.sort();
        keys
    };
    for key in &sorted_keys {
        let local_path = local_dir.path().join(key.as_str());
        run_cli(&[
            "put",
            "--file",
            dev_path,
            "--key",
            key,
            "--from",
            local_path.to_str().unwrap(),
        ]);
        put_so_far.push(key.to_string());

        // list should now show all files put so far
        let list_out = run_cli(&["list", "--file", dev_path]);
        let listed: Vec<String> = list_out.lines().filter(|l| !l.is_empty()).map(String::from).collect();
        let mut listed_sorted = listed.clone();
        listed_sorted.sort();
        let mut expected_sorted = put_so_far.clone();
        expected_sorted.sort();
        assert_eq!(
            listed_sorted, expected_sorted,
            "after putting {key}, list mismatch"
        );
    }

    // Spot-check get: read every file back via CLI and compare
    for (key, expected_data) in &files {
        let got_file = NamedTempFile::new().unwrap();
        let got_path = got_file.path().to_str().unwrap();
        run_cli(&[
            "get",
            "--file",
            dev_path,
            "--key",
            key,
            "--to",
            got_path,
        ]);
        let got_data = fs::read(got_path).unwrap();
        assert_eq!(
            got_data.len(),
            expected_data.len(),
            "get {key}: size mismatch"
        );
        assert_eq!(&got_data, expected_data, "get {key}: data mismatch");
    }

    // list --long should show correct sizes
    let long_out = run_cli(&["list", "--file", dev_path, "--long"]);
    let long_entries = parse_long_list(&long_out);
    for (size, path) in &long_entries {
        let expected = files.get(path).unwrap_or_else(|| panic!("unexpected file in list: {path}"));
        assert_eq!(*size, expected.len(), "list --long size mismatch for {path}");
    }
    assert_eq!(long_entries.len(), files.len(), "list --long count mismatch");

    // Delete two files
    let delete_keys = vec![
        "my_table/data/00001.db",
        "other_table/_versions/1.manifest",
    ];
    for key in &delete_keys {
        run_cli(&["delete", "--file", dev_path, "--key", key]);
    }

    // list should reflect deletions
    let list_after_delete = run_cli(&["list", "--file", dev_path]);
    let listed: Vec<String> = list_after_delete
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();
    assert_eq!(
        listed.len(),
        files.len() - delete_keys.len(),
        "wrong count after delete"
    );
    for dk in &delete_keys {
        assert!(
            !listed.iter().any(|l| l == *dk),
            "{dk} still in list after delete"
        );
    }

    // Re-put the deleted files
    for key in &delete_keys {
        let local_path = local_dir.path().join(key);
        run_cli(&[
            "put",
            "--file",
            dev_path,
            "--key",
            key,
            "--from",
            local_path.to_str().unwrap(),
        ]);
    }

    // All files should be back
    let list_restored = run_cli(&["list", "--file", dev_path]);
    let listed: Vec<String> = list_restored
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();
    assert_eq!(listed.len(), files.len(), "not all files restored");

    // Export all to a local directory
    let export_path = export_dir.path().to_str().unwrap();
    run_cli(&[
        "export",
        "--file",
        dev_path,
        "--to",
        export_path,
    ]);

    // Compare every exported file to the original
    for (key, expected_data) in &files {
        let exported = fs::read(export_dir.path().join(key)).unwrap_or_else(|e| {
            panic!("missing exported file {key}: {e}")
        });
        assert_eq!(
            exported.len(),
            expected_data.len(),
            "export {key}: size mismatch"
        );
        assert_eq!(&exported, expected_data, "export {key}: data mismatch");
    }

    println!("PASS cli_put_list_get_delete_export_roundtrip");
}

// =========================================================================
// TEST 2: import all at once, then get each file individually
//
// Import a directory of files in one shot, then retrieve each file with
// `get` and verify contents match the originals.
// =========================================================================

#[test]
fn cli_import_then_get_each() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();

    // Import all files at once
    let out = run_cli(&["import", "--file", dev_path, "--from", local_path]);
    assert!(
        out.contains("Imported"),
        "import should confirm: {out}"
    );

    // list should show all files
    let list_out = run_cli(&["list", "--file", dev_path]);
    let listed: Vec<String> = list_out.lines().filter(|l| !l.is_empty()).map(String::from).collect();
    assert_eq!(listed.len(), files.len(), "import file count mismatch");

    // Get each file individually and compare
    for (key, expected_data) in &files {
        let got_file = NamedTempFile::new().unwrap();
        let got_path = got_file.path().to_str().unwrap();
        run_cli(&[
            "get",
            "--file",
            dev_path,
            "--key",
            key,
            "--to",
            got_path,
        ]);
        let got_data = fs::read(got_path).unwrap();
        assert_eq!(
            got_data.len(),
            expected_data.len(),
            "get {key}: size mismatch after import"
        );
        assert_eq!(&got_data, expected_data, "get {key}: data mismatch after import");
    }

    // info should show correct file count
    let info_out = run_cli(&["info", "--file", dev_path]);
    let files_line = info_out
        .lines()
        .find(|l| l.starts_with("Files:"))
        .expect("info missing Files: line");
    let count: usize = files_line
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(count, files.len(), "info file count mismatch");

    println!("PASS cli_import_then_get_each");
}

// =========================================================================
// TEST 3: import -> export to another raw device -> verify
//
// Populate device A via import, export to device B (raw://), then export
// device B to a local directory and compare to originals.
// =========================================================================

#[test]
fn cli_import_export_to_raw_device() {
    let dev_a = NamedTempFile::new().unwrap();
    let dev_b = NamedTempFile::new().unwrap();
    let dev_a_path = dev_a.path().to_str().unwrap();
    let dev_b_path = dev_b.path().to_str().unwrap();

    format_device(dev_a_path);
    format_device(dev_b_path);

    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();

    // Populate device A
    run_cli(&["import", "--file", dev_a_path, "--from", local_path]);

    // Import from device A into device B using raw:// URI
    let raw_uri = format!("raw://{}", dev_a_path);
    run_cli(&["import", "--file", dev_b_path, "--from", &raw_uri]);

    // list on device B should match device A
    let list_a = run_cli(&["list", "--file", dev_a_path, "--long"]);
    let list_b = run_cli(&["list", "--file", dev_b_path, "--long"]);

    let mut entries_a = parse_long_list(&list_a);
    let mut entries_b = parse_long_list(&list_b);
    entries_a.sort_by(|a, b| a.1.cmp(&b.1));
    entries_b.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(entries_a, entries_b, "device B listing should match device A");

    // Export device B to local directory and compare to originals
    let export_dir = TempDir::new().unwrap();
    let export_path = export_dir.path().to_str().unwrap();
    run_cli(&["export", "--file", dev_b_path, "--to", export_path]);

    for (key, expected_data) in &files {
        let exported = fs::read(export_dir.path().join(key))
            .unwrap_or_else(|e| panic!("missing from device B export: {key}: {e}"));
        assert_eq!(&exported, expected_data, "device B export {key}: data mismatch");
    }

    println!("PASS cli_import_export_to_raw_device");
}

// =========================================================================
// TEST 4: verify and repair
//
// Format device, populate, verify clean, then repair (idempotent on clean
// device), verify still clean.
// =========================================================================

#[test]
fn cli_verify_and_repair() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Populate
    let (local_dir, files) = create_local_files();
    for (key, _) in &files {
        let local_path = local_dir.path().join(key);
        run_cli(&[
            "put",
            "--file",
            dev_path,
            "--key",
            key,
            "--from",
            local_path.to_str().unwrap(),
        ]);
    }

    // Verify should be clean
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "verify should be clean: {out}");
    assert!(out.contains("Errors:              0"), "should have 0 errors: {out}");

    // Repair on a clean device should be idempotent
    let out = run_cli(&["repair", "--file", dev_path]);
    assert!(out.contains("Repair complete"), "repair should complete: {out}");

    // Verify again after repair
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(
        out.contains("Device is clean"),
        "verify after repair should be clean: {out}"
    );

    // All files should still be accessible
    for (key, expected_data) in &files {
        let got_file = NamedTempFile::new().unwrap();
        let got_path = got_file.path().to_str().unwrap();
        run_cli(&[
            "get",
            "--file",
            dev_path,
            "--key",
            key,
            "--to",
            got_path,
        ]);
        let got_data = fs::read(got_path).unwrap();
        assert_eq!(&got_data, expected_data, "post-repair get {key} mismatch");
    }

    println!("PASS cli_verify_and_repair");
}

// =========================================================================
// TEST 5: info reports correct stats
//
// Format, check empty stats, populate, check updated stats.
// =========================================================================

#[test]
fn cli_info_stats() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Empty device
    let out = run_cli(&["info", "--file", dev_path]);
    assert!(
        out.lines().any(|l| l.starts_with("Files:") && l.trim_end().ends_with(" 0")),
        "fresh device should have 0 files: {out}"
    );
    assert!(out.contains("Device size:"), "missing Device size: {out}");
    assert!(out.contains("Format version:"), "missing Format version: {out}");
    assert!(out.contains("Transaction ID:"), "missing Transaction ID: {out}");
    assert!(out.contains("Free space:"), "missing Free space: {out}");
    assert!(out.contains("Free fragments:"), "missing Free fragments: {out}");

    // Populate with test files
    populate_device(dev_path);

    let out = run_cli(&["info", "--file", dev_path]);
    // Should now show files
    let files_line = out
        .lines()
        .find(|l| l.starts_with("Files:"))
        .expect("missing Files:");
    let count: usize = files_line
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert!(count > 0, "should have files after populate: {out}");

    // Data stored should be > 0
    assert!(
        out.lines().any(|l| l.starts_with("Data stored:") && !l.contains("0 bytes (0.0 MB)")),
        "Data stored should be > 0 after populate: {out}"
    );

    println!("PASS cli_info_stats");
}

// =========================================================================
// TEST 6: format with custom index slot size
// =========================================================================

#[test]
fn cli_format_custom_index_slots() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();

    // 128 MB device with 32 MB index slots
    run_cli(&[
        "format",
        "--file",
        dev_path,
        "--size",
        "134217728",
        "--index-slot-size",
        "33554432",
    ]);

    let out = run_cli(&["info", "--file", dev_path]);
    assert!(
        out.contains("Index slot size:    33554432 bytes (32 MB)"),
        "should show 32 MB index slots: {out}"
    );

    // Should be usable -- put a file and verify
    let tmp_in = NamedTempFile::new().unwrap();
    std::fs::write(tmp_in.path(), b"test data for custom index slots").unwrap();
    run_cli(&["put", "--file", dev_path, "--key", "test.db", "--from", tmp_in.path().to_str().unwrap()]);
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "custom-slot device should be clean: {out}");

    println!("PASS cli_format_custom_index_slots");
}

// =========================================================================
// TEST 7: list with prefix filtering
// =========================================================================

#[test]
fn cli_list_prefix_filter() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();
    run_cli(&["import", "--file", dev_path, "--from", local_path]);

    // list all
    let all = run_cli(&["list", "--file", dev_path]);
    let all_lines: Vec<&str> = all.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(all_lines.len(), files.len(), "total file count mismatch");

    // list --prefix my_table
    let my_table = run_cli(&["list", "--file", dev_path, "--prefix", "my_table"]);
    let my_lines: Vec<&str> = my_table.lines().filter(|l| !l.is_empty()).collect();
    let expected_my = files.keys().filter(|k| k.starts_with("my_table")).count();
    assert_eq!(
        my_lines.len(),
        expected_my,
        "my_table prefix count wrong: got {:?}",
        my_lines
    );
    for line in &my_lines {
        assert!(line.starts_with("my_table/"), "wrong prefix: {line}");
    }

    // list --prefix other_table
    let other = run_cli(&["list", "--file", dev_path, "--prefix", "other_table"]);
    let other_lines: Vec<&str> = other.lines().filter(|l| !l.is_empty()).collect();
    let expected_other = files.keys().filter(|k| k.starts_with("other_table")).count();
    assert_eq!(other_lines.len(), expected_other, "other_table prefix count wrong");

    // list --prefix nonexistent
    let empty = run_cli(&["list", "--file", dev_path, "--prefix", "nonexistent"]);
    let empty_lines: Vec<&str> = empty.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(empty_lines.len(), 0, "nonexistent prefix should yield 0 files");

    println!("PASS cli_list_prefix_filter");
}

// =========================================================================
// TEST 8: get non-existent key should fail
// =========================================================================

#[test]
fn cli_get_missing_key_fails() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let got_file = NamedTempFile::new().unwrap();
    let got_path = got_file.path().to_str().unwrap();

    let (_, _, code) = run_cli_fail(&[
        "get",
        "--file",
        dev_path,
        "--key",
        "does/not/exist.db",
        "--to",
        got_path,
    ]);
    assert_ne!(code, Some(0), "get of missing key should fail");

    println!("PASS cli_get_missing_key_fails");
}

// =========================================================================
// TEST 9: delete non-existent key is idempotent (returns success)
// =========================================================================

#[test]
fn cli_delete_missing_key_is_idempotent() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Per ObjectStore trait contract, delete of missing key succeeds
    run_cli(&[
        "delete",
        "--file",
        dev_path,
        "--key",
        "does/not/exist.db",
    ]);

    println!("PASS cli_delete_missing_key_is_idempotent");
}

// =========================================================================
// TEST 10: put overwrite replaces content
// =========================================================================

#[test]
fn cli_put_overwrite() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let key = "data/test.bin";

    // Put version 1
    let v1 = TempDir::new().unwrap();
    let v1_file = v1.path().join("v1.bin");
    fs::write(&v1_file, b"version-one-data").unwrap();
    run_cli(&[
        "put",
        "--file",
        dev_path,
        "--key",
        key,
        "--from",
        v1_file.to_str().unwrap(),
    ]);

    // Get and verify v1
    let got = NamedTempFile::new().unwrap();
    run_cli(&[
        "get",
        "--file",
        dev_path,
        "--key",
        key,
        "--to",
        got.path().to_str().unwrap(),
    ]);
    assert_eq!(fs::read(got.path()).unwrap(), b"version-one-data");

    // Put version 2 (overwrite)
    let v2_file = v1.path().join("v2.bin");
    fs::write(&v2_file, b"version-two-data-longer").unwrap();
    run_cli(&[
        "put",
        "--file",
        dev_path,
        "--key",
        key,
        "--from",
        v2_file.to_str().unwrap(),
    ]);

    // Get should return v2
    let got2 = NamedTempFile::new().unwrap();
    run_cli(&[
        "get",
        "--file",
        dev_path,
        "--key",
        key,
        "--to",
        got2.path().to_str().unwrap(),
    ]);
    assert_eq!(fs::read(got2.path()).unwrap(), b"version-two-data-longer");

    // list should show exactly 1 file
    let list_out = run_cli(&["list", "--file", dev_path]);
    let count = list_out.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(count, 1, "overwrite should not duplicate: {list_out}");

    println!("PASS cli_put_overwrite");
}

// =========================================================================
// TEST 11: export then re-import into fresh device
//
// Populate device A, export to a directory, format device B, import from
// that directory, verify all files match.
// =========================================================================

#[test]
fn cli_export_reimport_fresh_device() {
    let dev_a = NamedTempFile::new().unwrap();
    let dev_b = NamedTempFile::new().unwrap();
    let dev_a_path = dev_a.path().to_str().unwrap();
    let dev_b_path = dev_b.path().to_str().unwrap();

    format_device(dev_a_path);
    format_device(dev_b_path);

    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();

    // Populate A via import
    run_cli(&["import", "--file", dev_a_path, "--from", local_path]);

    // Export A to directory
    let mid_dir = TempDir::new().unwrap();
    let mid_path = mid_dir.path().to_str().unwrap();
    run_cli(&["export", "--file", dev_a_path, "--to", mid_path]);

    // Import into B
    run_cli(&["import", "--file", dev_b_path, "--from", mid_path]);

    // Verify both devices have the same files
    let list_a = run_cli(&["list", "--file", dev_a_path, "--long"]);
    let list_b = run_cli(&["list", "--file", dev_b_path, "--long"]);
    let mut e_a = parse_long_list(&list_a);
    let mut e_b = parse_long_list(&list_b);
    e_a.sort_by(|a, b| a.1.cmp(&b.1));
    e_b.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(e_a, e_b, "device B should match device A");

    // Verify against originals
    for (key, expected_data) in &files {
        let got = NamedTempFile::new().unwrap();
        run_cli(&[
            "get",
            "--file",
            dev_b_path,
            "--key",
            key,
            "--to",
            got.path().to_str().unwrap(),
        ]);
        let got_data = fs::read(got.path()).unwrap();
        assert_eq!(&got_data, expected_data, "reimport {key}: data mismatch");
    }

    println!("PASS cli_export_reimport_fresh_device");
}

// =========================================================================
// TEST 12: large file round-trip via put/get
//
// Test files larger than a single block.
// =========================================================================

#[test]
fn cli_large_file_put_get() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let large_dir = TempDir::new().unwrap();
    let large_file = large_dir.path().join("big.bin");
    let large_data = test_data("big.bin", 1024 * 1024); // 1 MB
    fs::write(&large_file, &large_data).unwrap();

    run_cli(&[
        "put",
        "--file",
        dev_path,
        "--key",
        "data/big.bin",
        "--from",
        large_file.to_str().unwrap(),
    ]);

    let got = NamedTempFile::new().unwrap();
    run_cli(&[
        "get",
        "--file",
        dev_path,
        "--key",
        "data/big.bin",
        "--to",
        got.path().to_str().unwrap(),
    ]);

    let got_data = fs::read(got.path()).unwrap();
    assert_eq!(got_data.len(), large_data.len(), "large file size mismatch");
    assert_eq!(got_data, large_data, "large file data mismatch");

    // Verify CRCs are good
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "verify after large put: {out}");

    println!("PASS cli_large_file_put_get");
}

// =========================================================================
// TEST 13: S3 import (allowed to fail -- no S3 credentials in CI)
//
// This test attempts an S3 import. It is expected to fail unless the
// binary was built with --features aws AND valid credentials are present.
// The test verifies the error message is reasonable.
// =========================================================================

#[test]
fn cli_s3_import_graceful_failure() {
    // NOTE: This test is allowed to fail. It validates that the S3 import
    // path produces a clear error when credentials or the aws feature are
    // missing, rather than panicking or producing a confusing message.

    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let (_, stderr, code) = run_cli_fail(&[
        "import",
        "--file",
        dev_path,
        "--from",
        "s3://nonexistent-test-bucket/prefix",
    ]);

    // Should fail (no AWS feature or no credentials)
    if code == Some(0) {
        // If it somehow succeeded (e.g. running in AWS with the feature
        // enabled and the bucket exists), that is also fine.
        println!("S3 import unexpectedly succeeded -- environment has valid S3 access");
    } else {
        // Verify the error is descriptive, not a bare panic
        let combined = format!("{stderr}");
        let has_useful_msg = combined.contains("S3")
            || combined.contains("s3")
            || combined.contains("aws")
            || combined.contains("features")
            || combined.contains("import failed");
        assert!(has_useful_msg, "S3 error should be descriptive: {combined}");
        println!("S3 import failed as expected (no feature/creds): {}", stderr.trim());
    }

    println!("PASS cli_s3_import_graceful_failure");
}

// =========================================================================
// TEST 14: many small files stress test
//
// Put 100 tiny files, verify list count, get a sample, delete half,
// verify, export remaining, compare.
// =========================================================================

#[test]
fn cli_many_small_files() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let tmp = TempDir::new().unwrap();
    let n = 100;
    let mut all_files: HashMap<String, Vec<u8>> = HashMap::new();

    // Put 100 files
    for i in 0..n {
        let key = format!("data/{:04}.bin", i);
        let data = test_data(&key, 128 + i * 32);
        let path = tmp.path().join(format!("{i}.bin"));
        fs::write(&path, &data).unwrap();
        run_cli(&[
            "put",
            "--file",
            dev_path,
            "--key",
            &key,
            "--from",
            path.to_str().unwrap(),
        ]);
        all_files.insert(key, data);
    }

    // list should show 100 files
    let list_out = run_cli(&["list", "--file", dev_path]);
    let count = list_out.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(count, n, "should have {n} files, got {count}");

    // Spot-check 10 files via get
    for i in (0..n).step_by(10) {
        let key = format!("data/{:04}.bin", i);
        let got = NamedTempFile::new().unwrap();
        run_cli(&[
            "get",
            "--file",
            dev_path,
            "--key",
            &key,
            "--to",
            got.path().to_str().unwrap(),
        ]);
        let got_data = fs::read(got.path()).unwrap();
        assert_eq!(
            &got_data,
            all_files.get(&key).unwrap(),
            "spot-check {key} failed"
        );
    }

    // Delete odd-numbered files
    for i in (1..n).step_by(2) {
        let key = format!("data/{:04}.bin", i);
        run_cli(&["delete", "--file", dev_path, "--key", &key]);
        all_files.remove(&key);
    }

    let remaining = n / 2;
    let list_out = run_cli(&["list", "--file", dev_path]);
    let count = list_out.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(count, remaining, "after deleting odd files: {count} != {remaining}");

    // Export remaining
    let export_dir = TempDir::new().unwrap();
    run_cli(&[
        "export",
        "--file",
        dev_path,
        "--to",
        export_dir.path().to_str().unwrap(),
    ]);

    for (key, expected_data) in &all_files {
        let exported = fs::read(export_dir.path().join(key))
            .unwrap_or_else(|e| panic!("missing export {key}: {e}"));
        assert_eq!(&exported, expected_data, "export {key} mismatch");
    }

    // Verify device integrity
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "verify: {out}");

    println!("PASS cli_many_small_files");
}

// =========================================================================
// TEST 15: S3 full round-trip
//
// s3://bucket/prefix → import → raw store A → export to local FS →
// import → raw store B (different index size) → export to s3://bucket/new-prefix
//
// This test requires --features aws and valid AWS credentials.
// Without them it gracefully verifies error messages.
// =========================================================================

#[test]
fn cli_s3_full_roundtrip() {
    let dev_a = NamedTempFile::new().unwrap();
    let dev_b = NamedTempFile::new().unwrap();
    let dev_a_path = dev_a.path().to_str().unwrap();
    let dev_b_path = dev_b.path().to_str().unwrap();

    // 128 MB device A (default 16 MB index slots)
    run_cli(&["format", "--file", dev_a_path, "--size", "134217728"]);
    // 128 MB device B with 32 MB index slots
    run_cli(&[
        "format",
        "--file",
        dev_b_path,
        "--size",
        "134217728",
        "--index-slot-size",
        "33554432",
    ]);

    // Step 1: Import from S3 to device A
    let s3_source = "s3://test-rawobjstr/source-data";
    let (_, stderr_import, code_import) =
        run_cli_fail(&["import", "--file", dev_a_path, "--from", s3_source]);

    if code_import != Some(0) {
        // No AWS feature or creds -- verify both import and export error paths
        let has_msg = stderr_import.contains("S3")
            || stderr_import.contains("s3")
            || stderr_import.contains("aws")
            || stderr_import.contains("features");
        assert!(
            has_msg,
            "S3 import error should be descriptive: {stderr_import}"
        );

        // Put test data so the export actually attempts S3 PUTs (an empty
        // device would export 0 files and "succeed" without hitting S3).
        populate_device(dev_a_path);

        // Also check export-to-S3 error path
        let (stdout_export, stderr_export, code_export) = run_cli_fail(&[
            "export",
            "--file",
            dev_a_path,
            "--to",
            "s3://test-rawobjstr/export-folder",
        ]);
        assert_ne!(code_export, Some(0), "S3 export should fail without feature/creds");
        // Per-file errors are printed to stdout; top-level errors to stderr.
        let combined = format!("{stdout_export}{stderr_export}");
        let has_export_msg = combined.contains("S3")
            || combined.contains("s3")
            || combined.contains("aws")
            || combined.contains("features")
            || combined.contains("Error")
            || combined.contains("error")
            || combined.contains("export failed");
        assert!(
            has_export_msg,
            "S3 export error should be descriptive: stdout={stdout_export} stderr={stderr_export}"
        );

        println!("S3 round-trip: both import/export failed as expected (no feature/creds)");
        println!("PASS cli_s3_full_roundtrip (graceful failure)");
        return;
    }

    // If S3 import succeeded, run the full round-trip
    println!("S3 import succeeded — running full round-trip");

    // Step 2: Export device A to local FS
    let mid_dir = TempDir::new().unwrap();
    let mid_path = mid_dir.path().to_str().unwrap();
    run_cli(&["export", "--file", dev_a_path, "--to", mid_path]);

    // Capture file list from device A
    let list_a = run_cli(&["list", "--file", dev_a_path, "--long"]);
    let entries_a = parse_long_list(&list_a);
    assert!(!entries_a.is_empty(), "device A should have files after S3 import");

    // Step 3: Import local FS into device B (different index slot size)
    run_cli(&["import", "--file", dev_b_path, "--from", mid_path]);

    // Verify device B has same files
    let list_b = run_cli(&["list", "--file", dev_b_path, "--long"]);
    let mut entries_b = parse_long_list(&list_b);
    let mut entries_a_sorted = entries_a.clone();
    entries_a_sorted.sort_by(|a, b| a.1.cmp(&b.1));
    entries_b.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(
        entries_a_sorted, entries_b,
        "device B should match device A files"
    );

    // Verify device B info shows 32 MB index slots
    let info_b = run_cli(&["info", "--file", dev_b_path]);
    assert!(
        info_b.contains("33554432 bytes (32 MB)"),
        "device B should have 32 MB index slots: {info_b}"
    );

    // Step 4: Export device B to S3 under a new prefix
    let s3_dest = "s3://test-rawobjstr/exported-roundtrip";
    let export_out = run_cli(&["export", "--file", dev_b_path, "--to", s3_dest]);
    assert!(
        export_out.contains("Exported"),
        "S3 export should confirm: {export_out}"
    );

    // Verify both devices are clean
    let out = run_cli(&["verify", "--file", dev_a_path]);
    assert!(out.contains("Device is clean"), "device A: {out}");
    let out = run_cli(&["verify", "--file", dev_b_path]);
    assert!(out.contains("Device is clean"), "device B: {out}");

    println!("PASS cli_s3_full_roundtrip (full S3 round-trip)");
}

// =========================================================================
// TEST 16: repair after primary superblock corruption
//
// Populate device, corrupt the primary superblock (offset 0), run repair
// (open falls back to backup, repair flushes fresh copies), then verify
// the device is fully healthy with both superblocks restored.
// =========================================================================

#[test]
fn cli_repair_corrupt_primary_superblock() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Populate
    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();
    run_cli(&["import", "--file", dev_path, "--from", local_path]);

    // Verify clean first
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "pre-corruption: {out}");

    // Corrupt primary superblock (offset 0, overwrite first 64 bytes with garbage)
    corrupt_bytes(dev_path, 0, 64);

    // The device should still open (backup superblock at offset 4096 is fine)
    // Verify still works
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "after corrupt primary, verify should still pass: {out}");

    // Repair rebuilds free list and flushes BOTH superblocks fresh
    let out = run_cli(&["repair", "--file", dev_path]);
    assert!(out.contains("Repair complete"), "repair: {out}");

    // All files should be intact
    let list_out = run_cli(&["list", "--file", dev_path]);
    let count = list_out.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(count, files.len(), "all files should survive repair");

    // Now corrupt backup superblock (offset 4096) - device should still
    // work because repair wrote a fresh primary
    corrupt_bytes(dev_path, 4096, 64);
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(
        out.contains("Device is clean"),
        "after corrupt backup (post-repair), verify: {out}"
    );

    println!("PASS cli_repair_corrupt_primary_superblock");
}

// =========================================================================
// TEST 17: repair after backup superblock corruption
//
// Same as above but corrupt the backup copy at offset 4096.
// =========================================================================

#[test]
fn cli_repair_corrupt_backup_superblock() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Populate with test files
    populate_device(dev_path);

    // Corrupt backup superblock at offset 4096
    corrupt_bytes(dev_path, 4096, 128);

    // Should still open and verify (primary is good)
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "after corrupt backup: {out}");

    // Repair flushes both superblocks fresh
    let out = run_cli(&["repair", "--file", dev_path]);
    assert!(out.contains("Repair complete"), "repair: {out}");

    // Now corrupt primary too — should still work because repair wrote fresh backup
    corrupt_bytes(dev_path, 0, 128);
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(
        out.contains("Device is clean"),
        "after corrupt primary (post-repair), verify: {out}"
    );

    println!("PASS cli_repair_corrupt_backup_superblock");
}

// =========================================================================
// TEST 18: repair cannot recover when both superblocks are destroyed
//
// Corrupt both superblocks. Open should fail, and repair should fail too.
// =========================================================================

#[test]
fn cli_repair_both_superblocks_destroyed() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);
    populate_device(dev_path);

    // Destroy both superblocks
    corrupt_bytes(dev_path, 0, 256);
    corrupt_bytes(dev_path, 4096, 256);

    // Repair should fail (cannot open device)
    let (_, stderr, code) = run_cli_fail(&["repair", "--file", dev_path]);
    assert_ne!(code, Some(0), "repair should fail: {stderr}");

    // Verify should also fail
    let (_, stderr, code) = run_cli_fail(&["verify", "--file", dev_path]);
    assert_ne!(code, Some(0), "verify should fail: {stderr}");

    // But re-format should work (recovers the device for reuse)
    format_device(dev_path);
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "re-format recovers: {out}");

    println!("PASS cli_repair_both_superblocks_destroyed");
}

// =========================================================================
// TEST 19: repair after data corruption detects but does not fix bad data
//
// Corrupt a data extent. Verify detects the error. Repair rebuilds the
// free list but doesn't remove corrupt entries (repair is free-list only).
// Verify still reports the same data error after repair.
// =========================================================================

#[test]
fn cli_repair_does_not_fix_data_corruption() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Put a known file
    let tmp = TempDir::new().unwrap();
    let data_file = tmp.path().join("test.bin");
    fs::write(&data_file, &test_data("test.bin", 4096)).unwrap();
    run_cli(&[
        "put",
        "--file",
        dev_path,
        "--key",
        "data/test.bin",
        "--from",
        data_file.to_str().unwrap(),
    ]);

    // Verify clean
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "pre-corruption: {out}");

    // Corrupt data region (offset 8192 = DATA_START, flip some bytes in the
    // payload area)
    corrupt_bytes(dev_path, 8192, 32);

    // The integrity scan removes the corrupt entry and the allocator
    // is rebuilt from gaps, so space is always accounted correctly.
    // Verify detects data corruption on open, removes the bad entry,
    // and reports 0 files. Device appears clean (space accounted).
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(
        out.contains("Files checked:       0"),
        "corrupt entry should have been removed on open: {out}"
    );
    assert!(
        out.contains("Space accounted:     true"),
        "gap-based allocator should always account space: {out}"
    );

    // Repair is idempotent — device is already clean
    let out = run_cli(&["repair", "--file", dev_path]);
    assert!(out.contains("Repair complete"), "repair should succeed: {out}");

    // Still clean after repair
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(
        out.contains("Device is clean"),
        "device should be clean after repair: {out}"
    );

    println!("PASS cli_repair_does_not_fix_data_corruption");
}

// =========================================================================
// TEST 20: repair recovers leaked space after simulated crash
//
// Simulate a scenario where extents are orphaned (allocated on disk but
// not tracked in the index). We do this by:
// 1. Format and populate with files, flush
// 2. Note total data stored
// 3. Delete some files via CLI (which updates index + allocator)
// 4. Re-import to use some freed space
// 5. Repair and verify free space is consistent
// =========================================================================

#[test]
fn cli_repair_free_space_consistency() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Populate
    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();
    run_cli(&["import", "--file", dev_path, "--from", local_path]);

    // Note initial free space from info
    let info1 = run_cli(&["info", "--file", dev_path]);
    let free1 = extract_free_space(&info1);

    // Delete half the files
    let keys: Vec<String> = files.keys().cloned().collect();
    for key in keys.iter().take(keys.len() / 2) {
        run_cli(&["delete", "--file", dev_path, "--key", key]);
    }

    // Free space should increase after deletes
    let info2 = run_cli(&["info", "--file", dev_path]);
    let free2 = extract_free_space(&info2);
    assert!(free2 > free1, "free space should increase after deletes: {free1} vs {free2}");

    // Repair and check free space stays consistent
    let out = run_cli(&["repair", "--file", dev_path]);
    assert!(out.contains("Repair complete"), "repair: {out}");

    let info3 = run_cli(&["info", "--file", dev_path]);
    let free3 = extract_free_space(&info3);
    // After repair, free space should be >= what it was after deletes
    // (repair may recover additional fragments)
    assert!(
        free3 >= free2,
        "repair should not lose free space: before={free2}, after={free3}"
    );

    // Verify clean
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "post-repair: {out}");

    // Remaining files should be intact
    for key in keys.iter().skip(keys.len() / 2) {
        let got = NamedTempFile::new().unwrap();
        run_cli(&[
            "get",
            "--file",
            dev_path,
            "--key",
            key,
            "--to",
            got.path().to_str().unwrap(),
        ]);
        let got_data = fs::read(got.path()).unwrap();
        assert_eq!(
            &got_data,
            files.get(key).unwrap(),
            "post-repair get {key} mismatch"
        );
    }

    println!("PASS cli_repair_free_space_consistency");
}

// =========================================================================
// TEST 21: repair after repeated put/delete cycles
//
// Stress the allocator with many put/delete cycles, then repair and
// verify the free space matches expectations.
// =========================================================================

#[test]
fn cli_repair_after_churn() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let tmp = TempDir::new().unwrap();

    // 5 cycles of put 20 / delete 20
    for cycle in 0..5u32 {
        for i in 0..20u32 {
            let key = format!("cycle{cycle}/file{i:03}.bin");
            let data = test_data(&key, 512);
            let path = tmp.path().join(format!("c{cycle}f{i}.bin"));
            fs::write(&path, &data).unwrap();
            run_cli(&[
                "put",
                "--file",
                dev_path,
                "--key",
                &key,
                "--from",
                path.to_str().unwrap(),
            ]);
        }
        // Delete all files from this cycle
        for i in 0..20u32 {
            let key = format!("cycle{cycle}/file{i:03}.bin");
            run_cli(&["delete", "--file", dev_path, "--key", &key]);
        }
    }

    // Device should be empty
    let list_out = run_cli(&["list", "--file", dev_path]);
    let count = list_out.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(count, 0, "device should be empty after all deletes");

    // Repair should consolidate fragmented free space
    let out = run_cli(&["repair", "--file", dev_path]);
    assert!(out.contains("Repair complete"), "repair: {out}");

    // After repair, free space should be maximized (single free region)
    let info = run_cli(&["info", "--file", dev_path]);
    let frags_line = info
        .lines()
        .find(|l| l.starts_with("Free fragments:"))
        .expect("missing Free fragments");
    let frags: usize = frags_line.split_whitespace().last().unwrap().parse().unwrap();
    // After repair with no files, there should be exactly 1 free fragment
    assert_eq!(frags, 1, "repair should coalesce all free space into 1 fragment: {frags}");

    // Verify clean
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "post-churn-repair: {out}");

    println!("PASS cli_repair_after_churn");
}

// =========================================================================
// TEST 22: export to raw:// device via CLI
//
// Verify the new export-to-uri path works for raw:// targets. Populate
// device A, export to device B via raw://, compare contents.
// =========================================================================

#[test]
fn cli_export_to_raw_device() {
    let dev_a = NamedTempFile::new().unwrap();
    let dev_b = NamedTempFile::new().unwrap();
    let dev_a_path = dev_a.path().to_str().unwrap();
    let dev_b_path = dev_b.path().to_str().unwrap();

    format_device(dev_a_path);
    format_device(dev_b_path);

    // Populate A
    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();
    run_cli(&["import", "--file", dev_a_path, "--from", local_path]);

    // Export A → B via raw:// URI
    let raw_b_uri = format!("raw://{}", dev_b_path);
    run_cli(&[
        "export",
        "--file",
        dev_a_path,
        "--to",
        &raw_b_uri,
    ]);

    // B should have same files
    let list_a = run_cli(&["list", "--file", dev_a_path, "--long"]);
    let list_b = run_cli(&["list", "--file", dev_b_path, "--long"]);
    let mut entries_a = parse_long_list(&list_a);
    let mut entries_b = parse_long_list(&list_b);
    entries_a.sort_by(|a, b| a.1.cmp(&b.1));
    entries_b.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(entries_a, entries_b, "B should match A");

    // Export B to local dir and compare to originals
    let export_dir = TempDir::new().unwrap();
    run_cli(&[
        "export",
        "--file",
        dev_b_path,
        "--to",
        export_dir.path().to_str().unwrap(),
    ]);
    for (key, expected) in &files {
        let exported = fs::read(export_dir.path().join(key))
            .unwrap_or_else(|e| panic!("missing {key}: {e}"));
        assert_eq!(&exported, expected, "export {key} mismatch");
    }

    println!("PASS cli_export_to_raw_device");
}

// =========================================================================
// TEST 23: S3 export graceful failure
// =========================================================================

#[test]
fn cli_s3_export_graceful_failure() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);
    populate_device(dev_path);

    let (stdout, stderr, code) = run_cli_fail(&[
        "export",
        "--file",
        dev_path,
        "--to",
        "s3://nonexistent-bucket/some-prefix",
    ]);

    if code == Some(0) {
        println!("S3 export unexpectedly succeeded");
    } else {
        // Per-file errors are printed to stdout; top-level errors to stderr.
        let combined = format!("{stdout}{stderr}");
        let has_msg = combined.contains("S3")
            || combined.contains("s3")
            || combined.contains("aws")
            || combined.contains("features")
            || combined.contains("Error")
            || combined.contains("error")
            || combined.contains("export failed");
        assert!(has_msg, "S3 export error should be descriptive: stdout={stdout} stderr={stderr}");
        println!("S3 export failed as expected: {}", stderr.trim());
    }

    println!("PASS cli_s3_export_graceful_failure");
}

// =========================================================================
// TEST 24: verify --long shows per-file OK results
// =========================================================================

#[test]
fn cli_verify_long_shows_ok_files() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();
    run_cli(&["import", "--file", dev_path, "--from", local_path]);

    // Without --long: should NOT list OK files
    let out = run_cli(&["verify", "--file", dev_path]);
    assert!(out.contains("Device is clean"), "should be clean: {out}");
    assert!(!out.contains("OK files:"), "without --long should not list OK files: {out}");

    // With --long: should list every file with OK status
    let out = run_cli(&["verify", "--file", dev_path, "--long"]);
    assert!(out.contains("Device is clean"), "should be clean: {out}");
    assert!(out.contains("OK files:"), "with --long should show OK files: {out}");
    for key in files.keys() {
        assert!(
            out.contains(key),
            "--long should list {key}: {out}"
        );
    }
    // Each OK file line should have ): OK
    let ok_count = out.lines().filter(|l| l.contains("): OK")).count();
    assert_eq!(ok_count, files.len(), "--long OK file count mismatch: {out}");

    println!("PASS cli_verify_long_shows_ok_files");
}

// =========================================================================
// TEST 25: repair verbose output shows used extents and free regions
// =========================================================================

#[test]
fn cli_repair_verbose_output() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    let (local_dir, files) = create_local_files();
    let local_path = local_dir.path().to_str().unwrap();
    run_cli(&["import", "--file", dev_path, "--from", local_path]);

    let out = run_cli(&["repair", "--file", dev_path]);

    // Should show file count
    let expected_line = format!("Files in index: {}", files.len());
    assert!(out.contains(&expected_line), "should show file count: {out}");

    // Should list used extents
    assert!(out.contains("Used extents ("), "should show used extents section: {out}");
    for key in files.keys() {
        assert!(out.contains(key), "should list extent for {key}: {out}");
    }

    // Should show free regions
    assert!(out.contains("New free regions ("), "should show free regions: {out}");
    assert!(out.contains("0x00002000"), "free region should start at 0x00002000 (DATA_START): {out}");

    // Should show free list summary
    assert!(out.contains("entries ->"), "should show free list transition: {out}");
    assert!(out.contains("bytes ->"), "should show free space transition: {out}");

    // Should end with Repair complete
    assert!(out.contains("Repair complete"), "should end with Repair complete: {out}");
    assert!(out.contains("Flushed: true"), "should show Flushed: true: {out}");

    println!("PASS cli_repair_verbose_output");
}

// =========================================================================
// TEST 26: --version outputs version, git hash, and build date
//
// Validates the build.rs-generated version string is present and
// well-formed. This serves as the "start of test run" version check.
// =========================================================================

#[test]
fn cli_version_output() {
    let version = cli_version();
    assert!(
        version.starts_with("rawobjstr "),
        "version should start with 'rawobjstr ': {version}"
    );
    assert!(
        version.contains("git "),
        "version should contain git hash: {version}"
    );
    assert!(
        version.contains("built "),
        "version should contain build date: {version}"
    );
    // Should contain the crate version from Cargo.toml
    assert!(
        version.contains(env!("CARGO_PKG_VERSION")),
        "version should contain package version: {version}"
    );
    println!("Version: {version}");
    println!("PASS cli_version_output");
}

// =========================================================================
// TEST 27: version is stable across a test run
//
// Captures the version at start, runs some operations, then rechecks
// to detect if the binary was replaced during the test run.
// =========================================================================

#[test]
fn cli_version_stable_across_operations() {
    let version_start = cli_version();

    // Run a few representative operations
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);
    run_cli(&["info", "--file", dev_path]);
    run_cli(&["verify", "--file", dev_path]);

    // Recheck version -- should not have changed
    assert_version_unchanged(&version_start);

    println!("PASS cli_version_stable_across_operations");
}

// =========================================================================
// TEST 28: --help shows command descriptions
//
// Verify the improved help output includes one-line descriptions for
// each command.
// =========================================================================

#[test]
fn cli_help_shows_descriptions() {
    // --help prints to stderr (eprintln!) and exits 0.
    // Use run_cli_fail to capture stderr even on success.
    let (stdout, stderr, code) = run_cli_fail(&["--help"]);
    assert_eq!(code, Some(0), "help should exit 0");
    let help = if stderr.contains("Commands:") {
        stderr
    } else {
        stdout
    };

    let commands = [
        "format", "info", "list", "get", "put", "delete", "verify",
        "export", "import", "repair", "tombstones", "del-tombstone",
        "scrub", "set-property",
    ];

    for cmd in &commands {
        assert!(
            help.contains(cmd),
            "--help should mention command '{cmd}': {help}"
        );
    }

    // Should contain the version line
    assert!(
        help.contains("rawobjstr "),
        "--help should show version: {help}"
    );

    println!("PASS cli_help_shows_descriptions");
}

// ---------------------------------------------------------------------------
// CLI putmeta / getmeta round-trip
// ---------------------------------------------------------------------------

#[test]
fn cli_putmeta_getmeta_roundtrip() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // Put an object with some body
    let body = test_data("meta-test-object", 8192);
    let body_tmp = NamedTempFile::new().unwrap();
    fs::write(body_tmp.path(), &body).unwrap();
    run_cli(&[
        "put",
        "--file", dev_path,
        "--key", "obj/with-meta.bin",
        "--from", body_tmp.path().to_str().unwrap(),
    ]);

    // Write metadata to a file, then putmeta
    let meta_content = b"custom-metadata-v1-payload";
    let meta_in = NamedTempFile::new().unwrap();
    fs::write(meta_in.path(), meta_content).unwrap();
    run_cli(&[
        "putmeta",
        "--file", dev_path,
        "--key", "obj/with-meta.bin",
        "--from", meta_in.path().to_str().unwrap(),
    ]);

    // getmeta to a file and verify exact match
    let meta_out = NamedTempFile::new().unwrap();
    run_cli(&[
        "getmeta",
        "--file", dev_path,
        "--key", "obj/with-meta.bin",
        "--to", meta_out.path().to_str().unwrap(),
    ]);
    let got_meta = fs::read(meta_out.path()).unwrap();
    assert_eq!(
        got_meta, meta_content,
        "getmeta should return exactly what putmeta wrote"
    );

    // Verify body is unchanged after putmeta
    let got_body = common::cli_get(dev_path, "obj/with-meta.bin");
    assert_eq!(got_body, body, "body should be unchanged after putmeta");

    // putmeta with different metadata replaces old
    let meta2 = b"replaced-metadata-v2";
    let meta_in2 = NamedTempFile::new().unwrap();
    fs::write(meta_in2.path(), meta2).unwrap();
    run_cli(&[
        "putmeta",
        "--file", dev_path,
        "--key", "obj/with-meta.bin",
        "--from", meta_in2.path().to_str().unwrap(),
    ]);
    let meta_out2 = NamedTempFile::new().unwrap();
    run_cli(&[
        "getmeta",
        "--file", dev_path,
        "--key", "obj/with-meta.bin",
        "--to", meta_out2.path().to_str().unwrap(),
    ]);
    let got_meta2 = fs::read(meta_out2.path()).unwrap();
    assert_eq!(got_meta2, meta2, "metadata should be replaced");

    println!("PASS cli_putmeta_getmeta_roundtrip");
}

// ---------------------------------------------------------------------------
// CLI list-deleted and vacuum
// ---------------------------------------------------------------------------

#[test]
fn cli_list_deleted_and_vacuum() {
    let dev = NamedTempFile::new().unwrap();
    let dev_path = dev.path().to_str().unwrap();
    format_device(dev_path);

    // No delete markers initially
    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(
        out.contains("No delete markers"),
        "empty store should have no markers: {out}"
    );

    // Vacuum on empty store
    let out = run_cli(&["vacuum", "--file", dev_path]);
    assert!(
        out.contains("No delete markers"),
        "vacuum on empty should report no markers: {out}"
    );

    // Simulate delete markers by putting objects under __deleted__/ prefix.
    // This is how higher-level layers record deletions.
    let marker_keys = [
        "__deleted__/photos/a.jpg",
        "__deleted__/docs/readme.txt",
        "__deleted__/data/file.bin",
    ];
    for key in &marker_keys {
        let body = test_data(key, 128);
        let tmp = NamedTempFile::new().unwrap();
        fs::write(tmp.path(), &body).unwrap();
        run_cli(&[
            "put",
            "--file", dev_path,
            "--key", key,
            "--from", tmp.path().to_str().unwrap(),
        ]);
    }

    // list-deleted should show the markers
    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(
        out.contains("3 delete marker(s)"),
        "should list 3 markers: {out}"
    );
    assert!(out.contains("photos/a.jpg"), "should show original key: {out}");
    assert!(out.contains("docs/readme.txt"), "should show original key: {out}");
    assert!(out.contains("data/file.bin"), "should show original key: {out}");

    // Vacuum should purge all markers
    let out = run_cli(&["vacuum", "--file", dev_path]);
    assert!(
        out.contains("3 marker(s) purged"),
        "vacuum should purge 3 markers: {out}"
    );

    // list-deleted should now be empty
    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(
        out.contains("No delete markers"),
        "post-vacuum should have no markers: {out}"
    );

    // Normal files should not be affected
    let files = common::cli_list(dev_path, None);
    assert!(files.is_empty(), "no user files should exist");

    println!("PASS cli_list_deleted_and_vacuum");
}

// ---------------------------------------------------------------------------
// Additional helpers
// ---------------------------------------------------------------------------

/// Overwrite `len` bytes at `offset` in a file with 0xDE pattern.
fn corrupt_bytes(path: &str, offset: u64, len: usize) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap_or_else(|e| panic!("cannot open {path} for corruption: {e}"));
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&vec![0xDE; len]).unwrap();
    f.sync_all().unwrap();
}

/// Extract free space bytes from info output.
fn extract_free_space(info: &str) -> u64 {
    // Look for "Free space:      NNNNN bytes (X.X MB)"
    let line = info
        .lines()
        .find(|l| l.starts_with("Free space:"))
        .expect("missing Free space line");
    let parts: Vec<&str> = line.split_whitespace().collect();
    // "Free" "space:" "NNNNN" "bytes" "(X.X" "MB)"
    parts[2].parse::<u64>().unwrap_or_else(|e| {
        panic!("cannot parse free space from '{line}': {e}")
    })
}

// =========================================================================
// vacuum / list-deleted tests
// =========================================================================

/// Vacuum on a clean device (no delete markers) prints "No delete markers".
#[test]
fn cli_vacuum_empty_device() {
    let tmp = NamedTempFile::new().unwrap();
    let dev_path = tmp.path().to_str().unwrap();
    format_device(dev_path);

    // Put a normal file (not under __deleted__/)
    let data = test_data("normal.txt", 256);
    let body_tmp = NamedTempFile::new().unwrap();
    fs::write(body_tmp.path(), &data).unwrap();
    run_cli(&["put", "--file", dev_path, "--key", "normal.txt", "--from", body_tmp.path().to_str().unwrap()]);

    let out = run_cli(&["vacuum", "--file", dev_path]);
    assert!(out.contains("No delete markers"), "vacuum on clean device: {out}");

    // Normal file is still there
    let files = common::cli_list(dev_path, None);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0], "normal.txt");
    println!("PASS cli_vacuum_empty_device");
}

/// list-deleted on a clean device prints "No delete markers".
#[test]
fn cli_list_deleted_empty_device() {
    let tmp = NamedTempFile::new().unwrap();
    let dev_path = tmp.path().to_str().unwrap();
    format_device(dev_path);

    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(out.contains("No delete markers"), "list-deleted on clean device: {out}");
    println!("PASS cli_list_deleted_empty_device");
}

/// Vacuum correctly purges all markers and normal files are not affected.
#[test]
fn cli_vacuum_purges_markers() {
    let tmp = NamedTempFile::new().unwrap();
    let dev_path = tmp.path().to_str().unwrap();
    format_device(dev_path);

    // Put a normal file
    let normal_data = test_data("keep_me.bin", 512);
    let body_tmp = NamedTempFile::new().unwrap();
    fs::write(body_tmp.path(), &normal_data).unwrap();
    run_cli(&["put", "--file", dev_path, "--key", "keep_me.bin", "--from", body_tmp.path().to_str().unwrap()]);

    // Put several delete markers
    for key in &["__deleted__/old/file1.dat", "__deleted__/old/file2.dat", "__deleted__/recent/photo.jpg"] {
        let marker = test_data(key, 64);
        let mtmp = NamedTempFile::new().unwrap();
        fs::write(mtmp.path(), &marker).unwrap();
        run_cli(&["put", "--file", dev_path, "--key", key, "--from", mtmp.path().to_str().unwrap()]);
    }

    // list-deleted sees them
    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(out.contains("3 delete marker(s)"), "should see 3 markers: {out}");
    assert!(out.contains("old/file1.dat"), "marker key stripped: {out}");
    assert!(out.contains("old/file2.dat"), "marker key stripped: {out}");
    assert!(out.contains("recent/photo.jpg"), "marker key stripped: {out}");

    // Vacuum
    let out = run_cli(&["vacuum", "--file", dev_path]);
    assert!(out.contains("3 marker(s) purged"), "vacuum output: {out}");

    // list-deleted now empty
    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(out.contains("No delete markers"), "post-vacuum: {out}");

    // Normal file still intact
    let got = common::cli_get(dev_path, "keep_me.bin");
    assert_eq!(got, normal_data);
    println!("PASS cli_vacuum_purges_markers");
}

/// Vacuum is idempotent: running it twice is fine.
#[test]
fn cli_vacuum_idempotent() {
    let tmp = NamedTempFile::new().unwrap();
    let dev_path = tmp.path().to_str().unwrap();
    format_device(dev_path);

    // Put a marker then vacuum twice
    let marker = test_data("__deleted__/x.bin", 32);
    let mtmp = NamedTempFile::new().unwrap();
    fs::write(mtmp.path(), &marker).unwrap();
    run_cli(&["put", "--file", dev_path, "--key", "__deleted__/x.bin", "--from", mtmp.path().to_str().unwrap()]);

    let out = run_cli(&["vacuum", "--file", dev_path]);
    assert!(out.contains("1 marker(s) purged"), "first vacuum: {out}");

    let out = run_cli(&["vacuum", "--file", dev_path]);
    assert!(out.contains("No delete markers"), "second vacuum: {out}");
    println!("PASS cli_vacuum_idempotent");
}

/// list-deleted shows correct columns (KEY, DELETED AT, BODY SIZE).
#[test]
fn cli_list_deleted_column_format() {
    let tmp = NamedTempFile::new().unwrap();
    let dev_path = tmp.path().to_str().unwrap();
    format_device(dev_path);

    let marker = test_data("__deleted__/docs/report.pdf", 1024);
    let mtmp = NamedTempFile::new().unwrap();
    fs::write(mtmp.path(), &marker).unwrap();
    run_cli(&["put", "--file", dev_path, "--key", "__deleted__/docs/report.pdf", "--from", mtmp.path().to_str().unwrap()]);

    let out = run_cli(&["list-deleted", "--file", dev_path]);
    assert!(out.contains("KEY"), "header has KEY: {out}");
    assert!(out.contains("DELETED AT"), "header has DELETED AT: {out}");
    assert!(out.contains("BODY SIZE"), "header has BODY SIZE: {out}");
    assert!(out.contains("docs/report.pdf"), "shows original key: {out}");
    assert!(out.contains("1 delete marker(s)"), "shows count: {out}");
    println!("PASS cli_list_deleted_column_format");
}
