//! O_DIRECT on/off workload tests and superblock flag persistence.

mod common;

use std::fs::File;
use tempfile::NamedTempFile;

use common::{make_small, SMALL_DEVICE};

// ═══════════════════════════════════════════════════════════════════════
// Helper: shared workload for both O_DIRECT on and off
// ═══════════════════════════════════════════════════════════════════════

fn cli_direct_io_workload(dev: &str, label: &str) {
    // Write a mix of sizes
    for i in 0..20usize {
        let size = 4096 * (i + 1);
        let data = make_small(i, size);
        common::cli_put(dev, &format!("{label}/file_{i:03}"), &data);
    }

    // Read them all back
    for i in 0..20usize {
        let data = common::cli_get(dev, &format!("{label}/file_{i:03}"));
        let expected_size = 4096 * (i + 1);
        assert_eq!(data.len(), expected_size, "size mismatch {label}/file_{i}");
    }

    // List
    let all = common::cli_list(dev, Some(label));
    assert_eq!(all.len(), 20, "{label}: expected 20 files");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: O_DIRECT off (buffered I/O) workload
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn direct_io_off_workload() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();
    common::cli_format(dev, &SMALL_DEVICE.to_string());

    cli_direct_io_workload(dev, "buffered");

    // Each CLI call reopens the store, so persistence is already tested.
    // Verify data still accessible (implicit reopen).
    let all = common::cli_list(dev, None);
    assert_eq!(all.len(), 20, "data didn't persist (buffered)");
    println!("PASS direct_io_off_workload");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: O_DIRECT on workload
// ═══════════════════════════════════════════════════════════════════════

#[cfg(target_os = "linux")]
#[test]
fn direct_io_on_workload() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();
    // O_DIRECT requires the file to already be the right size
    {
        let f = File::create(tmp.path()).unwrap();
        f.set_len(SMALL_DEVICE).unwrap();
    }
    common::cli_format_direct(dev, &SMALL_DEVICE.to_string());

    cli_direct_io_workload(dev, "direct");

    // Verify data persists (each CLI call reopens with O_DIRECT from superblock)
    let all = common::cli_list(dev, None);
    assert_eq!(all.len(), 20, "data didn't persist (O_DIRECT)");

    // Verify all data reads back correctly through O_DIRECT path
    for i in 0..20usize {
        let data = common::cli_get(dev, &format!("direct/file_{i:03}"));
        assert_eq!(data.len(), 4096 * (i + 1));
    }
    println!("PASS direct_io_on_workload");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 3: O_DIRECT flag persists in superblock across reopen
// ═══════════════════════════════════════════════════════════════════════

/// Format with O_DIRECT, reopen without specifying -- should auto-detect from superblock.
#[cfg(target_os = "linux")]
#[test]
fn direct_io_flag_persists_in_superblock() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();

    // O_DIRECT requires the file to already be the right size
    {
        let f = File::create(tmp.path()).unwrap();
        f.set_len(SMALL_DEVICE).unwrap();
    }
    common::cli_format_direct(dev, &SMALL_DEVICE.to_string());

    // Put a file
    common::cli_put(dev, "test.bin", b"hello direct");

    // Get it back
    let data = common::cli_get(dev, "test.bin");
    assert_eq!(data, b"hello direct");

    // Verify info shows O_DIRECT flag
    let info = common::cli_info(dev);
    assert!(info.contains("O_DIRECT"), "info should show O_DIRECT: {info}");
    println!("PASS direct_io_flag_persists_in_superblock");
}
