//! E2E tests for /_admin/* endpoints, admin-token auth,
//! --compression flag, --check-config validation, drain, log, and
//! all GET admin query endpoints (sysinfo, heatmap, region, nodeconfig,
//! shard info/objects, recovery status, etc.).
//!
//! These tests spawn the real `objstrd` binary as a subprocess.
//! NOTE: Requires the binary to be built first (cargo test handles this).

mod subprocess_helpers;

use std::io::Write;
use std::process::Command;
use std::time::Duration;
use subprocess_helpers::*;

// =========================================================================
// /_admin/flush
// =========================================================================

/// POST /_admin/flush should succeed on a raw-backed server.
#[tokio::test]
async fn test_admin_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19000);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT some data so there is something to flush
    let resp = client
        .put(&format!("{}/testbucket/flush-test.txt", srv.base_url))
        .body("data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // POST /_admin/flush
    let resp = client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true, "flush should return ok:true");

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/rebuild-index
// =========================================================================

/// POST /_admin/rebuild-index should reload the index and return bucket count.
#[tokio::test]
async fn test_admin_rebuild_index() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19001);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT an object into testbucket
    let resp = client
        .put(&format!("{}/testbucket/rebuild.txt", srv.base_url))
        .body("hi")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // POST /_admin/rebuild-index
    let resp = client
        .post(&format!("{}/_admin/rebuild-index", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true, "rebuild-index should return ok:true");
    assert!(
        body["buckets"].as_u64().unwrap_or(0) >= 1,
        "should find at least 1 bucket: {body}"
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/clear-bucket-cache
// =========================================================================

/// POST /_admin/clear-bucket-cache should delete __buckets__/ objects and
/// rebuild the registry.
#[tokio::test]
async fn test_admin_clear_bucket_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19002);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Create a bucket (which should create __buckets__ cache entries)
    let resp = client
        .put(&format!("{}/cachebucket", srv.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // PUT an object so bucket is discoverable from index
    let resp = client
        .put(&format!("{}/cachebucket/file.txt", srv.base_url))
        .body("data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // POST /_admin/clear-bucket-cache
    let resp = client
        .post(&format!("{}/_admin/clear-bucket-cache", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert!(
        body["buckets"].as_u64().unwrap_or(0) >= 1,
        "should still detect buckets from index: {body}"
    );

    // Verify object is still accessible
    let resp = client
        .get(&format!("{}/cachebucket/file.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/repair-replication
// =========================================================================

/// POST /_admin/repair-replication on a cluster-mode server should run a sweep.
#[tokio::test]
async fn test_admin_repair_replication() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19003);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        2,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // POST /_admin/repair-replication
    let resp = client
        .post(&format!("{}/_admin/repair-replication", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true, "repair-replication should return ok:true");

    let _ = srv.child.kill();
}

/// POST /_admin/repair-replication on a standalone (non-cluster) server should return 503.
#[tokio::test]
async fn test_admin_repair_replication_standalone_503() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("standalone.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19004);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
    ]);
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .post(&format!("{}/_admin/repair-replication", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "repair-replication on standalone should return 503"
    );

    drop(srv);
}

// =========================================================================
// /_admin/drain/{shard_id}
// =========================================================================

/// POST /_admin/drain/0 should detach the shard, repair-replication objects away,
/// and return the result.
#[tokio::test]
async fn test_admin_drain_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19005);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        2,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Write some objects first
    for i in 0..10 {
        let resp = client
            .put(&format!("{}/testbucket/drain-obj-{}.txt", srv.base_url, i))
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Drain shard 0
    let resp = client
        .post(&format!("{}/_admin/drain/0", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["shard_id"], 0);

    let _ = srv.child.kill();
}

/// POST /_admin/drain with an invalid shard ID should return 400.
#[tokio::test]
async fn test_admin_drain_invalid_shard_id() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19006);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        1,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Invalid (non-numeric) shard ID
    let resp = client
        .post(&format!("{}/_admin/drain/abc", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "non-numeric shard_id should return 400");

    // Valid number but shard does not exist
    let resp = client
        .post(&format!("{}/_admin/drain/999", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "nonexistent shard should return 404");

    let _ = srv.child.kill();
}

/// POST /_admin/drain on standalone (non-cluster) should return 503.
#[tokio::test]
async fn test_admin_drain_standalone_503() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("standalone.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19007);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
    ]);
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .post(&format!("{}/_admin/drain/0", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "drain on standalone should return 503");

    drop(srv);
}

// =========================================================================
// /_admin/redistribute
// =========================================================================

/// POST /_admin/redistribute on a multi-shard cluster should return ok.
#[tokio::test]
async fn test_admin_redistribute() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19050);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        2,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Write some objects first
    for i in 0..20 {
        let resp = client
            .put(&format!("{}/testbucket/redist-obj-{}.txt", srv.base_url, i))
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // POST /_admin/redistribute
    let resp = client
        .post(&format!("{}/_admin/redistribute", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true, "redistribute should return ok:true");
    assert!(body["moved"].is_number(), "should have moved count");
    assert!(body["skipped"].is_number(), "should have skipped count");
    assert!(body["errors"].is_number(), "should have errors count");

    let _ = srv.child.kill();
}

/// POST /_admin/redistribute on standalone (non-cluster) should return 503.
#[tokio::test]
async fn test_admin_redistribute_standalone_503() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("standalone.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19051);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
    ]);
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .post(&format!("{}/_admin/redistribute", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "redistribute on standalone should return 503");

    drop(srv);
}

/// GET /_admin/admin-op-status should report no running operation.
#[tokio::test]
async fn test_admin_op_status_idle() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19052);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        2,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/admin-op-status", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["running"], false, "no op should be running at idle");

    let _ = srv.child.kill();
}

// =========================================================================
// Admin token authentication
// =========================================================================

/// /_admin/* POST endpoints should be rejected without admin token when the
/// server is started with --admin-token.
#[tokio::test]
async fn test_admin_token_required() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("auth.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19008);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
        "--admin-token",
        "secret-test-token",
    ]);
    let client = build_client();

    assert!(wait_for_server_with_token(&srv.base_url, &client, Duration::from_secs(10), Some("secret-test-token")).await);

    // POST without token should be rejected
    let resp = client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "POST without token should be 403");

    // POST with wrong token should be rejected
    let resp = client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .header("authorization", "Bearer wrong-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "POST with wrong token should be 403");

    // POST with correct token should succeed
    let resp = client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .header("authorization", "Bearer secret-test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "POST with correct token should succeed");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    // GET /_admin/info without token should also be rejected (all /_admin/ paths
    // are protected when admin-token is set)
    let resp = client
        .get(&format!("{}/_admin/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "GET /_admin/info without token should be 403");

    // GET /_admin/info with correct token should succeed
    let resp = client
        .get(&format!("{}/_admin/info", srv.base_url))
        .header("authorization", "Bearer secret-test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /_admin/info with token should succeed");

    // S3 operations should NOT require admin token
    let resp = client
        .put(&format!("{}/testbucket/noauth.txt", srv.base_url))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "S3 PUT should work without admin token"
    );

    drop(srv);
}

// =========================================================================
// --check-config
// =========================================================================

/// --check-config with a valid config should exit 0.
#[tokio::test]
async fn test_check_config_valid() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("valid.conf");
    write_single_config(&config, shard.to_str().unwrap(), 9999);

    let output = Command::new(objstrd_bin())
        .args([
            "--check-config",
            "--config",
            config.to_str().unwrap(),
            "--node",
            "node1",
        ])
        .output()
        .expect("failed to run objstrd --check-config");

    assert!(
        output.status.success(),
        "check-config on valid config should exit 0: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Config OK"),
        "should print 'Config OK': {}",
        stderr
    );
}

/// --check-config with a broken config should exit 1.
#[tokio::test]
async fn test_check_config_broken() {
    let tmp = tempfile::tempdir().unwrap();
    let config = tmp.path().join("broken.conf");
    std::fs::write(&config, "this is not a valid config file!!!").unwrap();

    let output = Command::new(objstrd_bin())
        .args(["--check-config", "--config", config.to_str().unwrap()])
        .output()
        .expect("failed to run objstrd --check-config");

    assert!(
        !output.status.success(),
        "check-config on broken config should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ERROR") || stderr.contains("error") || stderr.contains("PARSE ERROR"),
        "should report an error: {}",
        stderr
    );
}

/// --check-config with duplicate node names should report an error.
#[tokio::test]
async fn test_check_config_duplicate_nodes() {
    let tmp = tempfile::tempdir().unwrap();
    let config = tmp.path().join("dup.conf");
    let shard0 = tmp.path().join("s0.raw");
    let shard1 = tmp.path().join("s1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    // Write a config with duplicate child node names under a parent.
    // The parser only reads one root node, so duplicates must be children.
    let mut f = std::fs::File::create(&config).unwrap();
    writeln!(f, "cluster  test-dup").unwrap();
    writeln!(f, "bucket   testbucket").unwrap();
    writeln!(f, "").unwrap();
    writeln!(
        f,
        "root  rf=1  listen=127.0.0.1:9000  endpoint=http://127.0.0.1:9000"
    )
    .unwrap();
    writeln!(
        f,
        "  child  rf=1  listen=127.0.0.1:9001  endpoint=http://127.0.0.1:9001"
    )
    .unwrap();
    writeln!(f, "    raw  {}", shard0.to_str().unwrap()).unwrap();
    writeln!(
        f,
        "  child  rf=1  listen=127.0.0.1:9002  endpoint=http://127.0.0.1:9002"
    )
    .unwrap();
    writeln!(f, "    raw  {}", shard1.to_str().unwrap()).unwrap();

    let output = Command::new(objstrd_bin())
        .args(["--check-config", "--config", config.to_str().unwrap()])
        .output()
        .expect("failed to run objstrd --check-config");

    assert!(
        !output.status.success(),
        "check-config with duplicate nodes should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("duplicate"),
        "should mention 'duplicate': {}",
        stderr
    );
}

/// --check-config with nonexistent node name should report an error.
#[tokio::test]
async fn test_check_config_missing_node() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("s.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("miss.conf");
    write_single_config(&config, shard.to_str().unwrap(), 9999);

    let output = Command::new(objstrd_bin())
        .args([
            "--check-config",
            "--config",
            config.to_str().unwrap(),
            "--node",
            "does-not-exist",
        ])
        .output()
        .expect("failed to run objstrd --check-config");

    assert!(
        !output.status.success(),
        "check-config with missing node should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "should mention 'not found': {}",
        stderr
    );
}

/// --check-config without --config should exit 1.
#[tokio::test]
async fn test_check_config_no_config_flag() {
    let output = Command::new(objstrd_bin())
        .args(["--check-config"])
        .output()
        .expect("failed to run objstrd --check-config");

    assert!(
        !output.status.success(),
        "check-config without --config should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("requires --config"),
        "should mention 'requires --config': {}",
        stderr
    );
}

// =========================================================================
// --compression flag
// =========================================================================

/// Server started with --compression zstd should store and retrieve data
/// correctly (compression is transparent to the S3 API).
#[tokio::test]
async fn test_compression_zstd_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("zstd.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19010);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
        "--compression",
        "zstd",
    ]);
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT a reasonably compressible object
    let data = "The quick brown fox jumps over the lazy dog. ".repeat(100);
    let resp = client
        .put(&format!("{}/testbucket/zstd-test.txt", srv.base_url))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET it back -- should be decompressed transparently
    let resp = client
        .get(&format!("{}/testbucket/zstd-test.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, data, "zstd round-trip should preserve content");

    // HEAD should report original (uncompressed) size
    let resp = client
        .head(&format!("{}/testbucket/zstd-test.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let cl = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    assert_eq!(cl, data.len(), "content-length should be original size");

    drop(srv);
}

/// Server started with --compression snappy should work end-to-end.
#[tokio::test]
async fn test_compression_snappy_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("snappy.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19011);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
        "--compression",
        "snappy",
    ]);
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let data = vec![42u8; 8192];
    let resp = client
        .put(&format!("{}/testbucket/snappy.bin", srv.base_url))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!("{}/testbucket/snappy.bin", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..], &data[..], "snappy round-trip should preserve content");

    drop(srv);
}

// =========================================================================
// /_admin/logs
// =========================================================================

/// GET /_admin/logs should return structured log entries.
#[tokio::test]
async fn test_logs_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19012);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate some activity so logs have entries
    for i in 0..5 {
        client
            .put(&format!("{}/testbucket/log-obj-{}.txt", srv.base_url, i))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    // GET /_admin/logs (no filters)
    let resp = client
        .get(&format!("{}/_admin/logs", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["total"].as_u64().unwrap_or(0) > 0,
        "should have some log entries: {body}"
    );
    assert!(
        body["entries"].is_array(),
        "entries should be an array: {body}"
    );

    // GET /_admin/logs with limit
    let resp = client
        .get(&format!("{}/_admin/logs?limit=2", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let returned = body["returned"].as_u64().unwrap_or(0);
    assert!(
        returned <= 2,
        "limit=2 should return at most 2 entries, got {returned}"
    );

    // GET /_admin/logs with level filter
    let resp = client
        .get(&format!("{}/_admin/logs?level=error", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    // May have 0 error entries, but the endpoint should still respond correctly
    assert!(body["entries"].is_array());

    let _ = srv.child.kill();
}

/// GET /_admin/logs with text search using q= parameter.
#[tokio::test]
async fn test_logs_text_search() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19013);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Search for something that probably does not appear in logs
    let resp = client
        .get(&format!(
            "{}/_admin/logs?q=xyzzy_nonexistent_string_12345",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["returned"].as_u64().unwrap_or(99),
        0,
        "text search for garbage should return 0 entries: {body}"
    );

    let _ = srv.child.kill();
}

/// --log-file should create a log file with entries.
#[tokio::test]
async fn test_log_file_output() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("logfile.raw");
    let log_path = tmp.path().join("server.log");
    let port = portpicker::pick_unused_port().unwrap_or(19014);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
        "--log-file",
        log_path.to_str().unwrap(),
    ]);
    let client = build_client();

    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate some activity
    client
        .put(&format!("{}/testbucket/logtest.txt", srv.base_url))
        .body("log this")
        .send()
        .await
        .unwrap();

    // Give a moment for async log writes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check log file exists and has content
    assert!(log_path.exists(), "log file should be created");
    let log_content = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        !log_content.is_empty(),
        "log file should contain entries"
    );

    drop(srv);
}

// =========================================================================
// /_admin/sysinfo
// =========================================================================

/// GET /_admin/sysinfo should return JSON with system stats.
#[tokio::test]
async fn test_admin_sysinfo() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("sysinfo.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19100);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
    ]);
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/sysinfo", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["process_uptime_secs"].is_number(),
        "should have process_uptime_secs: {body}"
    );
    assert!(
        body["load_avg_1"].is_number(),
        "should have load_avg_1: {body}"
    );
    assert!(
        body["mem_total_kb"].is_number(),
        "should have mem_total_kb: {body}"
    );
    assert!(
        body["mem_total_kb"].as_u64().unwrap_or(0) > 0,
        "mem_total_kb should be > 0"
    );

    drop(srv);
}

// =========================================================================
// /_admin/buckets
// =========================================================================

/// GET /_admin/buckets should return the list of known buckets.
#[tokio::test]
async fn test_admin_buckets() {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("buckets.raw");
    let port = portpicker::pick_unused_port().unwrap_or(19101);

    let srv = start_server_standalone(&[
        "--port",
        &port.to_string(),
        "--backend",
        "raw",
        "--image",
        image.to_str().unwrap(),
        "--size-mb",
        "64",
    ]);
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Create a bucket and put an object
    let resp = client
        .put(&format!("{}/mybucket", srv.base_url))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .put(&format!("{}/mybucket/file.txt", srv.base_url))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!("{}/_admin/buckets", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    let buckets = body["buckets"].as_array().expect("buckets should be array");
    let names: Vec<&str> = buckets
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        names.contains(&"testbucket"),
        "should contain default bucket 'testbucket': {:?}",
        names
    );
    assert!(
        names.contains(&"mybucket"),
        "should contain created bucket 'mybucket': {:?}",
        names
    );

    drop(srv);
}

// =========================================================================
// /_admin/heatmap
// =========================================================================

/// GET /_admin/heatmap should return chunk density data for a raw backend.
#[tokio::test]
async fn test_admin_heatmap() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19102);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT some data so the heatmap is not empty
    for i in 0..5 {
        let resp = client
            .put(&format!("{}/testbucket/heat-{}.txt", srv.base_url, i))
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let resp = client
        .get(&format!("{}/_admin/heatmap", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["device_size"].is_number(),
        "should have device_size: {body}"
    );
    assert!(
        body["chunk_size"].is_number(),
        "should have chunk_size: {body}"
    );
    assert!(
        body["file_counts"].is_array(),
        "should have file_counts array: {body}"
    );
    assert!(
        body["used_bytes"].is_array(),
        "should have used_bytes array: {body}"
    );
    assert!(
        body["free_bytes"].is_array(),
        "should have free_bytes array: {body}"
    );

    // Default chunks is 256
    let fc = body["file_counts"].as_array().unwrap();
    assert_eq!(fc.len(), 256, "default chunk count should be 256");

    let _ = srv.child.kill();
}

/// GET /_admin/heatmap?chunks=32 should return arrays with 32 elements.
#[tokio::test]
async fn test_admin_heatmap_custom_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19103);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/heatmap?chunks=32", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    let fc = body["file_counts"]
        .as_array()
        .expect("file_counts should be array");
    assert_eq!(fc.len(), 32, "chunks=32 should produce 32-element arrays");

    let ub = body["used_bytes"]
        .as_array()
        .expect("used_bytes should be array");
    assert_eq!(ub.len(), 32);

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/region
// =========================================================================

/// GET /_admin/region should return extents and free regions within a range.
#[tokio::test]
async fn test_admin_region() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19104);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT an object to create an extent
    let resp = client
        .put(&format!("{}/testbucket/region-test.txt", srv.base_url))
        .body("region data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Query region covering entire device (64 MB)
    let resp = client
        .get(&format!(
            "{}/_admin/region?from=0&to={}",
            srv.base_url,
            64 * 1024 * 1024
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(body["from"].is_number(), "should have 'from': {body}");
    assert!(body["to"].is_number(), "should have 'to': {body}");
    assert!(
        body["extents"].is_array(),
        "should have extents array: {body}"
    );
    assert!(
        body["free_regions"].is_array(),
        "should have free_regions array: {body}"
    );

    // We wrote one object, so there should be at least 1 extent
    let extents = body["extents"].as_array().unwrap();
    assert!(
        !extents.is_empty(),
        "should have at least 1 extent after writing an object"
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/extent_meta
// =========================================================================

/// GET /_admin/extent_meta?key=... should return decoded metadata for a key.
#[tokio::test]
async fn test_admin_extent_meta() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19105);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT an object with metadata
    let resp = client
        .put(&format!("{}/testbucket/meta-test.txt", srv.base_url))
        .header("content-type", "text/plain")
        .header("x-amz-meta-author", "testuser")
        .body("metadata object")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Query extent metadata
    let resp = client
        .get(&format!(
            "{}/_admin/extent_meta?key=testbucket/meta-test.txt",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "extent_meta should succeed: {}",
        resp.text().await.unwrap_or_default()
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/getraw
// =========================================================================

/// GET /_admin/getraw?key=... should return raw (uncompressed) bytes.
#[tokio::test]
async fn test_admin_getraw() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19106);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let payload = "raw bytes content";
    let resp = client
        .put(&format!("{}/testbucket/raw-test.txt", srv.base_url))
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!(
            "{}/_admin/getraw?key=testbucket/raw-test.txt",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "getraw should succeed");
    let body = resp.bytes().await.unwrap();
    // The raw bytes include the object body (and possibly metadata trailer).
    // At minimum, the body content should be present.
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains(payload),
        "getraw should contain the object body: got {} bytes",
        body.len()
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/nodeconfig
// =========================================================================

/// GET /_admin/nodeconfig should return the server configuration.
#[tokio::test]
async fn test_admin_nodeconfig() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19107);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/nodeconfig", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["shard_count"].is_number(),
        "should have shard_count: {body}"
    );
    assert_eq!(
        body["shard_count"].as_u64().unwrap(),
        1,
        "single-shard config should report shard_count=1"
    );
    assert!(
        body["shards"].is_array(),
        "should have shards array: {body}"
    );
    let shards = body["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1, "should have 1 shard entry");
    assert!(
        body["bucket"].is_string(),
        "should have bucket field: {body}"
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/shard/{id}/info
// =========================================================================

/// GET /_admin/shard/0/info should return stats for a raw shard.
#[tokio::test]
async fn test_admin_shard_info() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19108);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT some data
    let resp = client
        .put(&format!("{}/testbucket/shard-info.txt", srv.base_url))
        .body("shard info test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .get(&format!("{}/_admin/shard/0/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(
        body["shard_id"].as_u64().unwrap(),
        0,
        "should report shard_id=0"
    );
    assert!(
        body["device_size"].is_number(),
        "raw shard should have device_size: {body}"
    );
    assert!(
        body["device_size"].as_u64().unwrap() > 0,
        "device_size should be > 0"
    );
    assert!(
        body["file_count"].is_number(),
        "should have file_count: {body}"
    );
    assert!(
        body["file_count"].as_u64().unwrap() >= 1,
        "should have at least 1 file"
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/shard/{id}/objects
// =========================================================================

/// GET /_admin/shard/0/objects should return a paginated object listing.
#[tokio::test]
async fn test_admin_shard_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19109);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Write several objects
    for i in 0..5 {
        let resp = client
            .put(&format!("{}/testbucket/obj-{}.txt", srv.base_url, i))
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let resp = client
        .get(&format!("{}/_admin/shard/0/objects", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["objects"].is_array(),
        "should have objects array: {body}"
    );
    let objects = body["objects"].as_array().unwrap();
    assert!(
        objects.len() >= 5,
        "should have at least 5 objects, got {}",
        objects.len()
    );

    // Verify pagination fields
    assert!(
        body["total"].is_number(),
        "should have total: {body}"
    );
    assert!(
        body["offset"].is_number(),
        "should have offset: {body}"
    );
    assert!(
        body["limit"].is_number(),
        "should have limit: {body}"
    );

    // Test pagination: limit=2
    let resp = client
        .get(&format!(
            "{}/_admin/shard/0/objects?limit=2",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(
        objects.len(),
        2,
        "limit=2 should return exactly 2 objects"
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/shard/{invalid}/info -- error handling
// =========================================================================

/// GET /_admin/shard/999/info should return an error for a non-existent shard.
#[tokio::test]
async fn test_admin_shard_invalid_id() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19110);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Non-existent shard
    let resp = client
        .get(&format!("{}/_admin/shard/999/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 404 || resp.status() == 400,
        "non-existent shard should return 404 or 400, got {}",
        resp.status()
    );

    // Non-numeric shard ID
    let resp = client
        .get(&format!("{}/_admin/shard/abc/info", srv.base_url))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 404 || resp.status() == 400,
        "non-numeric shard ID should return 404 or 400, got {}",
        resp.status()
    );

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/recovery
// =========================================================================

/// GET /_admin/recovery should return recovery configuration and status.
#[tokio::test]
async fn test_admin_recovery_status() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19111);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        2,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Wait a moment for the recovery loop to run at least one cycle.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let resp = client
        .get(&format!("{}/_admin/recovery", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["enabled"].is_boolean(),
        "should have 'enabled' field: {body}"
    );
    assert_eq!(
        body["enabled"].as_bool().unwrap(),
        true,
        "recovery should be enabled by default in cluster mode"
    );
    assert!(
        body["poll_interval_secs"].is_number(),
        "should have poll_interval_secs: {body}"
    );
    assert!(
        body["poll_cycles"].is_number(),
        "should have poll_cycles: {body}"
    );
    assert!(
        body["total_re_replicated"].is_number(),
        "should have total_re_replicated: {body}"
    );
    assert!(
        body["under_replicated_count"].is_number(),
        "should have under_replicated_count: {body}"
    );

    let _ = srv.child.kill();
}
