//! Shared test helpers used across multiple integration test files.
//!
//! Usage: add `mod common;` at the top of your test file, then call
//! `common::make_store()`, `common::make_chunk(i)`, etc.

#![allow(dead_code)]

use bytes::Bytes;
use rawobjstr::extent::padded_extent_size;
use rawobjstr::store::RawObjectStore;
use rawobjstr::{DATA_START, INDEX_TOTAL_SIZE};
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const SMALL_DEVICE: u64 = 64 * 1024 * 1024; // 64 MB
pub const MEDIUM_DEVICE: u64 = 256 * 1024 * 1024; // 256 MB
pub const ONE_GB: u64 = 1024 * 1024 * 1024;
pub const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB

// ---------------------------------------------------------------------------
// Store constructors
// ---------------------------------------------------------------------------

/// Format a 64 MB loopback device and return the store + temp file.
pub fn make_store() -> (RawObjectStore, NamedTempFile) {
    make_store_sized(SMALL_DEVICE)
}

/// Format a 256 MB loopback device and return the store + temp file.
pub fn make_medium_store() -> (RawObjectStore, NamedTempFile) {
    make_store_sized(MEDIUM_DEVICE)
}

/// Format a 1 GB loopback device and return the store + temp file.
pub fn make_1gb_store() -> (RawObjectStore, NamedTempFile) {
    make_store_sized(ONE_GB)
}

/// How many 1 MB chunks fit in a 1 GB device's data region.
pub fn max_1mb_chunks() -> usize {
    let data_area = ONE_GB - DATA_START - INDEX_TOTAL_SIZE;
    let extent_size = padded_extent_size(CHUNK_SIZE as u64).unwrap();
    (data_area / extent_size) as usize
}

/// Format a loopback device of arbitrary size.
pub fn make_store_sized(size: u64) -> (RawObjectStore, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_size(tmp.path(), size, false).unwrap();
    (store, tmp)
}

// ---------------------------------------------------------------------------
// Payload builders / verifiers
// ---------------------------------------------------------------------------

/// Build a deterministic 1 MB payload: first 8 bytes = index as LE u64,
/// rest filled with `(index & 0xFF)`.
pub fn make_chunk(index: usize) -> Bytes {
    let mut buf = vec![0u8; CHUNK_SIZE];
    buf[..8].copy_from_slice(&(index as u64).to_le_bytes());
    let fill = (index & 0xFF) as u8;
    for b in &mut buf[8..] {
        *b = fill;
    }
    Bytes::from(buf)
}

/// Verify a 1 MB chunk matches what `make_chunk()` would produce.
pub fn verify_chunk(data: &[u8], expected_index: usize) {
    assert_eq!(data.len(), CHUNK_SIZE, "chunk {} wrong size", expected_index);
    let stored = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    assert_eq!(stored, expected_index, "chunk tag mismatch");
    let fill = (expected_index & 0xFF) as u8;
    for (i, &b) in data[8..].iter().enumerate() {
        assert_eq!(
            b, fill,
            "chunk {} byte {} mismatch",
            expected_index,
            i + 8
        );
    }
}

/// Build a deterministic payload of arbitrary size: filled with
/// `(index & 0xFF)`, first 8 bytes = index as LE u64 (if size >= 8).
pub fn make_small(index: usize, size: usize) -> Bytes {
    let mut buf = vec![(index & 0xFF) as u8; size];
    if size >= 8 {
        buf[..8].copy_from_slice(&(index as u64).to_le_bytes());
    }
    Bytes::from(buf)
}

/// Verify a payload matches what `make_small()` would produce.
pub fn verify_small(data: &[u8], index: usize, expected_size: usize) {
    assert_eq!(data.len(), expected_size, "index {index}: wrong size");
    if data.len() >= 8 {
        let tag = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        assert_eq!(tag, index, "index {index}: tag mismatch (got {tag})");
        let fill = (index & 0xFF) as u8;
        for (j, &b) in data[8..].iter().enumerate() {
            assert_eq!(b, fill, "index {index}: corrupt at byte {}", j + 8);
        }
    }
}

// ---------------------------------------------------------------------------
// CLI binary helpers
// ---------------------------------------------------------------------------

/// Run the CLI binary with args, assert success, return stdout.
pub fn run_cli(args: &[&str]) -> String {
    let mut bin = std::env::current_exe().unwrap();
    bin.pop();
    if bin.ends_with("deps") {
        bin.pop();
    }
    bin.push("rawobjstr");

    let output = std::process::Command::new(&bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to run {}: {e}", bin.display()));

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success(),
        "CLI failed (exit {:?}) args={args:?}:\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    stdout
}

/// Run CLI expecting failure, return (stdout, stderr, exit code).
pub fn run_cli_fail(args: &[&str]) -> (String, String, Option<i32>) {
    let mut bin = std::env::current_exe().unwrap();
    bin.pop();
    if bin.ends_with("deps") {
        bin.pop();
    }
    bin.push("rawobjstr");

    let output = std::process::Command::new(&bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {}: {e}", bin.display()));

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr, output.status.code())
}

/// Format a device via CLI.
pub fn cli_format(path: &str, size: &str) {
    run_cli(&["format", "--file", path, "--size", size]);
}

/// Format a device with --direct-io via CLI.
pub fn cli_format_direct(path: &str, size: &str) {
    run_cli(&["format", "--file", path, "--size", size, "--direct-io"]);
}

/// Put data into the store via CLI. Writes data to a temp file, then calls put.
pub fn cli_put(dev: &str, key: &str, data: &[u8]) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), data).unwrap();
    run_cli(&[
        "put",
        "--file",
        dev,
        "--key",
        key,
        "--from",
        tmp.path().to_str().unwrap(),
    ]);
}

/// Get data from the store via CLI. Returns the file contents.
pub fn cli_get(dev: &str, key: &str) -> Vec<u8> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    run_cli(&[
        "get",
        "--file",
        dev,
        "--key",
        key,
        "--to",
        tmp.path().to_str().unwrap(),
    ]);
    std::fs::read(tmp.path()).unwrap()
}

/// Get data from the store via CLI, expecting failure.
pub fn cli_get_fail(dev: &str, key: &str) -> (String, String, Option<i32>) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    run_cli_fail(&[
        "get",
        "--file",
        dev,
        "--key",
        key,
        "--to",
        tmp.path().to_str().unwrap(),
    ])
}

/// List files in the store via CLI. Returns sorted file paths.
pub fn cli_list(dev: &str, prefix: Option<&str>) -> Vec<String> {
    let mut args = vec!["list", "--file", dev];
    if let Some(p) = prefix {
        args.push("--prefix");
        args.push(p);
    }
    let out = run_cli(&args);
    out.lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

/// Delete a file from the store via CLI.
pub fn cli_delete(dev: &str, key: &str) {
    run_cli(&["delete", "--file", dev, "--key", key]);
}

/// Get info output from the store via CLI.
pub fn cli_info(dev: &str) -> String {
    run_cli(&["info", "--file", dev])
}

/// Run verify via CLI, return output.
pub fn cli_verify(dev: &str) -> String {
    run_cli(&["verify", "--file", dev])
}
