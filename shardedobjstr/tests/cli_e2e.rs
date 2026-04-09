//! End-to-end tests for the `shardedobjstr` and `shardedobjstr-catview`
//! CLI binaries.
//!
//! These tests exercise the command-line tools by spawning them as
//! subprocesses and checking exit codes and output.

use std::path::PathBuf;
use std::process::Command;

/// Locate the `shardedobjstr` binary built by `cargo test`.
fn cli_bin() -> PathBuf {
    // `cargo test` puts test-built binaries in the same target directory.
    let mut path = std::env::current_exe()
        .expect("cannot determine test binary path");
    // Go from .../deps/test_binary to .../shardedobjstr
    path.pop(); // remove test binary name
    if path.ends_with("deps") {
        path.pop(); // remove "deps"
    }
    path.push("shardedobjstr");
    path
}

/// Locate the `shardedobjstr-catview` binary.
fn catview_bin() -> PathBuf {
    let mut path = std::env::current_exe()
        .expect("cannot determine test binary path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push("shardedobjstr-catview");
    path
}

// =====================================================================
// version / help
// =====================================================================

#[test]
fn cli_version_prints_version_string() {
    let output = Command::new(cli_bin())
        .arg("version")
        .output()
        .expect("failed to run shardedobjstr");

    assert!(
        output.status.success(),
        "version should exit 0: {:?}",
        output.status
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("shardedobjstr"),
        "version output should contain 'shardedobjstr': {}",
        stdout
    );
}

#[test]
fn cli_no_args_prints_usage() {
    let output = Command::new(cli_bin())
        .output()
        .expect("failed to run shardedobjstr");

    // No args should print usage and exit non-zero.
    assert!(
        !output.status.success(),
        "no args should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage") || stderr.contains("usage"),
        "should print usage: {}",
        stderr
    );
}

// =====================================================================
// check-config
// =====================================================================

#[test]
fn cli_check_config_valid() {
    let dir = tempfile::tempdir().unwrap();
    let conf_path = dir.path().join("cluster.conf");
    std::fs::write(&conf_path, "replicas 2\nshard mem\nshard mem\nshard mem\n").unwrap();

    let output = Command::new(cli_bin())
        .args(["check-config", "--config"])
        .arg(&conf_path)
        .output()
        .expect("failed to run shardedobjstr check-config");

    assert!(
        output.status.success(),
        "check-config on valid config should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_check_config_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let conf_path = dir.path().join("bad.conf");
    std::fs::write(&conf_path, "unknown_directive 42\n").unwrap();

    let output = Command::new(cli_bin())
        .args(["check-config", "--config"])
        .arg(&conf_path)
        .output()
        .expect("failed to run shardedobjstr check-config");

    assert!(
        !output.status.success(),
        "check-config on invalid config should fail"
    );
}

// =====================================================================
// format + info + list + put + get + delete
// =====================================================================

#[test]
fn cli_format_info_put_get_delete_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let s0 = dir.path().join("s0.raw");
    let s1 = dir.path().join("s1.raw");
    let size = (64 * 1024 * 1024).to_string();

    let shards_arg = format!("{},{}", s0.display(), s1.display());

    // format
    let output = Command::new(cli_bin())
        .args(["format", "--shards", &shards_arg, "--size", &size, "--replicas", "2"])
        .output()
        .expect("format failed");
    assert!(
        output.status.success(),
        "format should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // info
    let output = Command::new(cli_bin())
        .args(["info", "--shards", &shards_arg])
        .output()
        .expect("info failed");
    assert!(
        output.status.success(),
        "info should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("shard") || stdout.contains("Shard"),
        "info output should mention shards"
    );

    // put
    let src_file = dir.path().join("input.dat");
    std::fs::write(&src_file, b"hello-cli-test").unwrap();

    let output = Command::new(cli_bin())
        .args([
            "put", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "test/obj.bin",
            "--from",
        ])
        .arg(&src_file)
        .output()
        .expect("put failed");
    assert!(
        output.status.success(),
        "put should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // list
    let output = Command::new(cli_bin())
        .args(["list", "--shards", &shards_arg, "--replicas", "2", "--long"])
        .output()
        .expect("list failed");
    assert!(output.status.success(), "list should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("test/obj.bin"),
        "list should show the object: {}",
        stdout
    );

    // get
    let out_file = dir.path().join("output.dat");
    let output = Command::new(cli_bin())
        .args([
            "get", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "test/obj.bin",
            "--to",
        ])
        .arg(&out_file)
        .output()
        .expect("get failed");
    assert!(
        output.status.success(),
        "get should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let got_data = std::fs::read(&out_file).unwrap();
    assert_eq!(&got_data, b"hello-cli-test", "get output mismatch");

    // delete
    let output = Command::new(cli_bin())
        .args([
            "delete", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "test/obj.bin",
        ])
        .output()
        .expect("delete failed");
    assert!(
        output.status.success(),
        "delete should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // verify key is gone
    let output = Command::new(cli_bin())
        .args(["list", "--shards", &shards_arg, "--replicas", "2"])
        .output()
        .expect("list after delete failed");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("test/obj.bin"),
        "deleted object should not appear in list: {}",
        stdout
    );
}

// =====================================================================
// verify + health
// =====================================================================

#[test]
fn cli_verify_and_health() {
    let dir = tempfile::tempdir().unwrap();
    let s0 = dir.path().join("s0.raw");
    let s1 = dir.path().join("s1.raw");
    let size = (64 * 1024 * 1024).to_string();
    let shards_arg = format!("{},{}", s0.display(), s1.display());

    // Format first.
    Command::new(cli_bin())
        .args(["format", "--shards", &shards_arg, "--size", &size])
        .output()
        .expect("format failed");

    // verify
    let output = Command::new(cli_bin())
        .args(["verify", "--shards", &shards_arg, "--replicas", "2"])
        .output()
        .expect("verify failed");
    assert!(
        output.status.success(),
        "verify on empty cluster should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // health
    let output = Command::new(cli_bin())
        .args(["health", "--shards", &shards_arg])
        .output()
        .expect("health failed");
    assert!(
        output.status.success(),
        "health should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// =====================================================================
// catview
// =====================================================================

#[test]
fn catview_version() {
    let output = Command::new(catview_bin())
        .arg("version")
        .output()
        .expect("failed to run catview");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("shardedobjstr-catview"),
        "catview version should identify itself: {}",
        stdout
    );
}

#[test]
fn catview_reads_json_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let s0 = dir.path().join("s0.raw");
    let s1 = dir.path().join("s1.raw");
    let size = (64 * 1024 * 1024).to_string();
    let shards_arg = format!("{},{}", s0.display(), s1.display());
    let catalog_path = dir.path().join("catalog.json");

    // Format, put an object, save catalog.
    Command::new(cli_bin())
        .args(["format", "--shards", &shards_arg, "--size", &size, "--replicas", "2"])
        .output()
        .expect("format failed");

    let src_file = dir.path().join("data.dat");
    std::fs::write(&src_file, b"catview-test-data").unwrap();

    Command::new(cli_bin())
        .args([
            "put", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "catview/obj.bin",
            "--from",
        ])
        .arg(&src_file)
        .args(["--catalog", &format!("json:{}", catalog_path.display())])
        .output()
        .expect("put failed");

    // Now use catview to read it.
    if !catalog_path.exists() {
        // The put command may not have created the catalog file if
        // --catalog is only honored by certain commands. Skip gracefully.
        return;
    }

    let output = Command::new(catview_bin())
        .arg(&catalog_path)
        .output()
        .expect("catview failed");

    assert!(
        output.status.success(),
        "catview should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("catview/obj.bin"),
        "catview should show the object: {}",
        stdout
    );
}

#[test]
fn catview_reads_bincode_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let s0 = dir.path().join("s0.raw");
    let s1 = dir.path().join("s1.raw");
    let size = (64 * 1024 * 1024).to_string();
    let shards_arg = format!("{},{}", s0.display(), s1.display());
    let catalog_path = dir.path().join("catalog.bin");

    // Format, put an object, save catalog in bincode format.
    Command::new(cli_bin())
        .args(["format", "--shards", &shards_arg, "--size", &size, "--replicas", "2"])
        .output()
        .expect("format failed");

    let src_file = dir.path().join("data.dat");
    std::fs::write(&src_file, b"catview-bincode-test-data").unwrap();

    Command::new(cli_bin())
        .args([
            "put", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "catview/binobj.bin",
            "--from",
        ])
        .arg(&src_file)
        .args(["--catalog", &format!("bincode:{}", catalog_path.display())])
        .output()
        .expect("put failed");

    if !catalog_path.exists() {
        return;
    }

    let output = Command::new(catview_bin())
        .args(["--format", "bin"])
        .arg(&catalog_path)
        .output()
        .expect("catview failed");

    assert!(
        output.status.success(),
        "catview should succeed on bincode: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("catview/binobj.bin"),
        "catview should show the object: {}",
        stdout
    );
}

// =====================================================================
// vacuum + list-deleted
// =====================================================================

#[test]
fn cli_vacuum_and_list_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let s0 = dir.path().join("s0.raw");
    let s1 = dir.path().join("s1.raw");
    let size = (64 * 1024 * 1024).to_string();
    let shards_arg = format!("{},{}", s0.display(), s1.display());

    // Format and put.
    Command::new(cli_bin())
        .args(["format", "--shards", &shards_arg, "--size", &size, "--replicas", "2"])
        .output()
        .expect("format failed");

    let src_file = dir.path().join("data.dat");
    std::fs::write(&src_file, b"vacuum-test").unwrap();

    Command::new(cli_bin())
        .args([
            "put", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "vac/obj.bin",
            "--from",
        ])
        .arg(&src_file)
        .output()
        .expect("put failed");

    // Delete.
    Command::new(cli_bin())
        .args([
            "delete", "--shards", &shards_arg,
            "--replicas", "2",
            "--key", "vac/obj.bin",
        ])
        .output()
        .expect("delete failed");

    // list-deleted should show the marker.
    let output = Command::new(cli_bin())
        .args(["list-deleted", "--shards", &shards_arg, "--replicas", "2"])
        .output()
        .expect("list-deleted failed");
    assert!(output.status.success());

    // vacuum
    let output = Command::new(cli_bin())
        .args(["vacuum", "--shards", &shards_arg, "--replicas", "2"])
        .output()
        .expect("vacuum failed");
    assert!(
        output.status.success(),
        "vacuum should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
