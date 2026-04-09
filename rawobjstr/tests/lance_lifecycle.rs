//! LanceDB-style table lifecycle test:
//!   create tables -> unmount -> remount -> verify -> add data ->
//!   optimize (compact) -> unmount -> remount -> verify

mod common;

use tempfile::NamedTempFile;

use common::{make_small, MEDIUM_DEVICE};

// ═══════════════════════════════════════════════════════════════════════
// Helpers: simulate Lance table structure
// ═══════════════════════════════════════════════════════════════════════

/// Simulate Lance table structure: versions, data files, transactions.
/// `file_start` controls where data file numbering starts so appends don't
/// overwrite existing files.
fn cli_create_table(
    dev: &str,
    table: &str,
    n_files: usize,
    file_start: usize,
    version: usize,
) {
    // Manifest
    let manifest = format!("{table}/_versions/{version}.manifest");
    common::cli_put(dev, &manifest, format!("manifest-v{version}").as_bytes());

    // Data files
    for i in 0..n_files {
        let fid = file_start + i;
        let path = format!("{table}/data/{fid:05}.db");
        let data = make_small(version * 1000 + i, 32768); // 32 KB per file
        common::cli_put(dev, &path, &data);
    }

    // Transaction log entry
    let txn = format!("_transactions/{version}-{table}.txn");
    common::cli_put(dev, &txn, format!("txn-{table}-v{version}").as_bytes());
}

/// Verify a table exists with the expected file count and manifest version.
fn cli_verify_table(dev: &str, table: &str, n_files: usize, version: usize) {
    // Check manifest
    let manifest_path = format!("{table}/_versions/{version}.manifest");
    let data = common::cli_get(dev, &manifest_path);
    let expected = format!("manifest-v{version}");
    assert_eq!(
        data,
        expected.as_bytes(),
        "manifest content mismatch for {table} v{version}"
    );

    // Check data files
    let data_prefix = format!("{table}/data");
    let data_files = common::cli_list(dev, Some(&data_prefix));
    assert_eq!(
        data_files.len(),
        n_files,
        "{table}: expected {n_files} data files, got {}",
        data_files.len()
    );
}

/// Simulate compaction: merge N data files into one, write new manifest.
fn cli_compact_table(dev: &str, table: &str, old_n_files: usize, new_version: usize) {
    // Read all old data
    let mut total_size = 0usize;
    for i in 0..old_n_files {
        let path = format!("{table}/data/{i:05}.db");
        let data = common::cli_get(dev, &path);
        total_size += data.len();
    }

    // Write compacted file
    let compacted = make_small(new_version * 1000, total_size);
    common::cli_put(
        dev,
        &format!("{table}/data/{:05}.db", old_n_files),
        &compacted,
    );

    // Delete old fragments
    for i in 0..old_n_files {
        let path = format!("{table}/data/{i:05}.db");
        common::cli_delete(dev, &path);
    }

    // New manifest
    common::cli_put(
        dev,
        &format!("{table}/_versions/{new_version}.manifest"),
        format!("manifest-v{new_version}").as_bytes(),
    );
}

// ═══════════════════════════════════════════════════════════════════════
// TEST: Full LanceDB-style lifecycle
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn lance_table_lifecycle() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();
    common::cli_format(dev, &MEDIUM_DEVICE.to_string());

    // Phase 1: create tables
    cli_create_table(dev, "users", 5, 0, 1);
    cli_create_table(dev, "events", 10, 0, 1);
    cli_create_table(dev, "other_table", 3, 0, 1);

    cli_verify_table(dev, "users", 5, 1);
    cli_verify_table(dev, "events", 10, 1);
    cli_verify_table(dev, "other_table", 3, 1);
    println!("phase1: created 3 tables");

    // Phase 2: verify all tables survive (implicit reopen via CLI)
    cli_verify_table(dev, "users", 5, 1);
    cli_verify_table(dev, "events", 10, 1);
    cli_verify_table(dev, "other_table", 3, 1);

    // Add more data to users (version 2: 3 new data files, starting at index 5)
    cli_create_table(dev, "users", 3, 5, 2);

    // Now users has v1 (5 files) + v2 (3 files) = 8 data files
    let user_data = common::cli_list(dev, Some("users/data"));
    assert_eq!(user_data.len(), 8, "users should have 8 data files after append");
    println!("phase2: verified, appended to users");

    // Phase 3: compact users table
    cli_compact_table(dev, "users", 8, 3);

    // Now users has 1 data file
    let user_data = common::cli_list(dev, Some("users/data"));
    assert_eq!(user_data.len(), 1, "users should have 1 compacted file");

    // Other tables untouched
    cli_verify_table(dev, "events", 10, 1);
    cli_verify_table(dev, "other_table", 3, 1);
    println!("phase3: compacted users, verified others untouched");

    // Phase 4: final verification -- everything persists
    let user_data = common::cli_list(dev, Some("users/data"));
    assert_eq!(user_data.len(), 1, "compacted users should persist");
    cli_verify_table(dev, "events", 10, 1);
    cli_verify_table(dev, "other_table", 3, 1);

    println!("phase4: final verification passed");
    println!("PASS lance_table_lifecycle");
}
