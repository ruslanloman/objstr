//! E2E tests for SIGHUP / POST /_admin/reload live config reload.
//!
//! These tests spawn the real `objstrd` binary as a subprocess with a tree
//! config file, send requests, trigger a reload, and verify the server comes
//! back and serves the same (or updated) data.
//!
//! NOTE: These tests require the `objstrd` binary to be built first.  When
//! running via `cargo test -p objstrd`, Cargo automatically builds the binary
//! and sets `CARGO_BIN_EXE_objstrd` for us.

mod subprocess_helpers;

use std::process::Command;
use std::time::Duration;
use subprocess_helpers::*;

/// Helper: get the build_git_hash from /_admin/info.
async fn get_build_hash(base: &str, client: &reqwest::Client) -> String {
    let r = client
        .get(&format!("{base}/_admin/info"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    body["build_git_hash"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

/// Helper: get the PID from /_admin/info.
async fn get_pid(base: &str, client: &reqwest::Client) -> u32 {
    let r = client
        .get(&format!("{base}/_admin/info"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    body["pid"].as_u64().unwrap() as u32
}

/// Helper: get the shard_count from /_admin/info.
async fn get_shard_count(base: &str, client: &reqwest::Client) -> usize {
    let r = client
        .get(&format!("{base}/_admin/info"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    body["shard_count"].as_u64().unwrap_or(1) as usize
}

/// Write a tree config file that points at one or more raw shards.
fn write_config(path: &std::path::Path, shard_paths: &[&str], port: u16) {
    write_multi_config(path, shard_paths, port, 1);
}

// =========================================================================

/// Test: PUT an object, trigger reload via SIGHUP, verify object persists.
#[tokio::test]
async fn reload_sighup_preserves_data() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_path = tmp.path().join("shard0.raw");
    format_raw_image(&shard_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18900);
    write_config(&config_path, &[shard_path.to_str().unwrap()], port);

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // PUT an object
    let resp = client
        .put(&format!("{}/testbucket/hello.txt", srv.base_url))
        .body("hello world")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Send SIGHUP
    let pid = get_pid(&srv.base_url, &client).await;
    let status = Command::new("kill")
        .args(["-HUP", &pid.to_string()])
        .status()
        .expect("failed to send SIGHUP");
    assert!(status.success(), "kill -HUP failed");

    // Wait for server to come back
    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(15)).await,
        "server did not come back after SIGHUP"
    );

    // Verify the object is still there
    let resp = client
        .get(&format!("{}/testbucket/hello.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "hello world");

    let _ = srv.child.kill();
}

/// Test: POST /_admin/reload triggers a reload and server comes back.
#[tokio::test]
async fn reload_http_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_path = tmp.path().join("shard0.raw");
    format_raw_image(&shard_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18901);
    write_config(&config_path, &[shard_path.to_str().unwrap()], port);

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // PUT an object before reload
    let resp = client
        .put(&format!("{}/testbucket/prereload.txt", srv.base_url))
        .body("before reload")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // POST /_admin/reload
    let resp = client
        .post(&format!("{}/_admin/reload", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    // Wait for server to come back
    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(15)).await,
        "server did not come back after POST /_admin/reload"
    );

    // Verify the object still exists
    let resp = client
        .get(&format!("{}/testbucket/prereload.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "before reload");

    let _ = srv.child.kill();
}

/// Test: Rename a shard file, update config, SIGHUP -- server reopens the
/// new path and still serves data.
#[tokio::test]
async fn reload_after_shard_rename() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_path = tmp.path().join("shard0.raw");
    format_raw_image(&shard_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18902);
    write_config(&config_path, &[shard_path.to_str().unwrap()], port);

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // PUT an object
    let resp = client
        .put(&format!("{}/testbucket/data.bin", srv.base_url))
        .body(vec![42u8; 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Rename the shard file (simulates `mv shard0.raw renamed.raw`)
    let renamed_path = tmp.path().join("renamed.raw");
    std::fs::rename(&shard_path, &renamed_path).unwrap();

    // Update the config to point at the renamed file
    write_config(&config_path, &[renamed_path.to_str().unwrap()], port);

    // Send SIGHUP
    let pid = get_pid(&srv.base_url, &client).await;
    let status = Command::new("kill")
        .args(["-HUP", &pid.to_string()])
        .status()
        .expect("failed to send SIGHUP");
    assert!(status.success());

    // Wait for server to come back
    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(15)).await,
        "server did not come back after shard rename + SIGHUP"
    );

    // Verify the object is still accessible via the renamed shard
    let resp = client
        .get(&format!("{}/testbucket/data.bin", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 1024);
    assert!(body.iter().all(|&b| b == 42));

    let _ = srv.child.kill();
}

/// Test: Add a second shard to the config, SIGHUP -- server picks up the
/// new shard and reports increased shard_count.
#[tokio::test]
async fn reload_adds_new_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0_path = tmp.path().join("shard0.raw");
    format_raw_image(&shard0_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18903);
    write_config(&config_path, &[shard0_path.to_str().unwrap()], port);

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // Verify single shard
    let count_before = get_shard_count(&srv.base_url, &client).await;
    assert_eq!(count_before, 1);

    // Format a second shard and update config
    let shard1_path = tmp.path().join("shard1.raw");
    format_raw_image(&shard1_path, 64);
    write_config(
        &config_path,
        &[shard0_path.to_str().unwrap(), shard1_path.to_str().unwrap()],
        port,
    );

    // Send SIGHUP
    let pid = get_pid(&srv.base_url, &client).await;
    let status = Command::new("kill")
        .args(["-HUP", &pid.to_string()])
        .status()
        .expect("failed to send SIGHUP");
    assert!(status.success());

    // Wait for server to come back
    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(15)).await,
        "server did not come back after adding shard + SIGHUP"
    );

    // Verify two shards now
    let count_after = get_shard_count(&srv.base_url, &client).await;
    assert_eq!(count_after, 2);

    let _ = srv.child.kill();
}

/// Test: Rapid double reload -- send two SIGHUPs in quick succession.
/// Server should survive without crashing.
#[tokio::test]
async fn reload_double_sighup() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_path = tmp.path().join("shard0.raw");
    format_raw_image(&shard_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18904);
    write_config(&config_path, &[shard_path.to_str().unwrap()], port);

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    let pid = get_pid(&srv.base_url, &client).await;

    // Send two SIGHUPs in rapid succession
    Command::new("kill").args(["-HUP", &pid.to_string()]).status().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    Command::new("kill").args(["-HUP", &pid.to_string()]).status().unwrap();

    // Wait for server to stabilize
    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(20)).await,
        "server did not come back after double SIGHUP"
    );

    // Verify server is functional
    let resp = client
        .put(&format!("{}/testbucket/after-double.txt", srv.base_url))
        .body("still works")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!("{}/testbucket/after-double.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "still works");

    let _ = srv.child.kill();
}

/// Test: Remove a shard via config reload -- server should come back with
/// fewer shards and still serve objects stored on the remaining shard.
#[tokio::test]
async fn reload_removes_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0_path = tmp.path().join("shard0.raw");
    let shard1_path = tmp.path().join("shard1.raw");
    format_raw_image(&shard0_path, 64);
    format_raw_image(&shard1_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18905);

    // Start with two shards (rf=1)
    write_config(
        &config_path,
        &[shard0_path.to_str().unwrap(), shard1_path.to_str().unwrap()],
        port,
    );

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // Verify two shards
    let count_before = get_shard_count(&srv.base_url, &client).await;
    assert_eq!(count_before, 2, "should start with 2 shards");

    // PUT objects (they will land on one or both shards)
    for i in 0..5 {
        let resp = client
            .put(&format!("{}/testbucket/obj-{}.txt", srv.base_url, i))
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Remove shard1 from config: now only shard0
    write_config(&config_path, &[shard0_path.to_str().unwrap()], port);

    // Send SIGHUP
    let pid = get_pid(&srv.base_url, &client).await;
    let status = Command::new("kill")
        .args(["-HUP", &pid.to_string()])
        .status()
        .expect("failed to send SIGHUP");
    assert!(status.success());

    // Wait for server to come back
    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(15)).await,
        "server did not come back after removing shard + SIGHUP"
    );

    // Verify only one shard now
    let count_after = get_shard_count(&srv.base_url, &client).await;
    assert_eq!(count_after, 1, "should have 1 shard after removing shard1");

    // Server should still be functional for new writes
    let resp = client
        .put(&format!("{}/testbucket/after-remove.txt", srv.base_url))
        .body("new data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!("{}/testbucket/after-remove.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "new data");

    let _ = srv.child.kill();
}

/// Test: Reload with an invalid (broken) config -- currently the server
/// exits on bad config during reload (it calls process::exit(1) in the
/// unwrap_or_else on load_tree_config).
///
/// TODO: The server should gracefully keep the old config on parse error
/// instead of exiting.  When that is fixed, update this test to assert
/// the server survives and data is still accessible.
#[tokio::test]
async fn reload_invalid_config_exits() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_path = tmp.path().join("shard0.raw");
    format_raw_image(&shard_path, 64);

    let config_path = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(18906);
    write_config(&config_path, &[shard_path.to_str().unwrap()], port);

    let mut srv = start_server(&config_path, "node1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // Write a broken config
    std::fs::write(&config_path, "this is not valid config data!!!").unwrap();

    // Send SIGHUP -- server currently exits on bad config
    let pid = get_pid(&srv.base_url, &client).await;
    let status = Command::new("kill")
        .args(["-HUP", &pid.to_string()])
        .status()
        .expect("failed to send SIGHUP");
    assert!(status.success());

    // Wait for the process to exit
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Server should no longer be reachable
    let reachable = wait_for_server(&srv.base_url, &client, Duration::from_secs(3)).await;
    assert!(
        !reachable,
        "server should have exited after bad config reload"
    );

    // Ensure child process has exited (non-zero)
    let exit = srv.child.try_wait().expect("failed to check child status");
    assert!(exit.is_some(), "child process should have exited");

    // Don't call kill on an already-dead process
}
