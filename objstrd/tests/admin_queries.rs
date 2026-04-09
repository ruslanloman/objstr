//! E2E tests for /_admin/objects and /_admin/shards query endpoints.
//!
//! Covers object listing pagination, prefix filtering, since_txn,
//! and the aggregate shards endpoint.
//!
//! Uses subprocess pattern (spawns real objstrd binary).

mod subprocess_helpers;

use std::time::Duration;
use subprocess_helpers::*;

// =========================================================================
// /_admin/objects
// =========================================================================

/// Empty store should return zero objects.
#[tokio::test]
async fn test_objects_empty_store() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19300);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/objects", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["total"].as_u64().unwrap(), 0);
    assert!(json["objects"].as_array().unwrap().is_empty());

    let _ = srv.child.kill();
}

/// PUT some objects and verify they appear in /_admin/objects.
#[tokio::test]
async fn test_objects_lists_put_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19301);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Put 3 objects
    for name in &["alpha.txt", "beta.txt", "gamma.txt"] {
        let resp = client
            .put(&format!("{}/testbucket/{}", srv.base_url, name))
            .body("data")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
    }

    // Flush to ensure objects are indexed
    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(&format!("{}/_admin/objects", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["total"].as_u64().unwrap(), 3);
    let objects = json["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 3);

    // Each object should have key, size fields
    for obj in objects {
        assert!(obj["key"].is_string());
        assert!(obj["size"].is_number());
    }

    let _ = srv.child.kill();
}

/// Pagination with offset and limit.
#[tokio::test]
async fn test_objects_pagination() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19302);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Put 5 objects
    for i in 0..5 {
        let resp = client
            .put(&format!("{}/testbucket/obj-{:02}.txt", srv.base_url, i))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
    }

    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Page 1: limit=2, offset=0
    let resp = client
        .get(&format!(
            "{}/_admin/objects?limit=2&offset=0",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["total"].as_u64().unwrap(), 5);
    assert_eq!(json["objects"].as_array().unwrap().len(), 2);

    // Page 2: limit=2, offset=2
    let resp = client
        .get(&format!(
            "{}/_admin/objects?limit=2&offset=2",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["objects"].as_array().unwrap().len(), 2);

    // Page 3: limit=2, offset=4 -- only 1 remaining
    let resp = client
        .get(&format!(
            "{}/_admin/objects?limit=2&offset=4",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["objects"].as_array().unwrap().len(), 1);

    let _ = srv.child.kill();
}

/// Prefix filter should only return matching objects.
#[tokio::test]
async fn test_objects_prefix_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19303);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Put objects with different prefixes
    for name in &["images/a.png", "images/b.png", "docs/readme.md", "root.txt"] {
        client
            .put(&format!("{}/testbucket/{}", srv.base_url, name))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Filter by "images/"
    let resp = client
        .get(&format!(
            "{}/_admin/objects?prefix=images/",
            srv.base_url
        ))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    let objects = json["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 2);
    for obj in objects {
        let key = obj["key"].as_str().unwrap();
        assert!(key.contains("images/"), "unexpected key: {key}");
    }

    let _ = srv.child.kill();
}

/// since_txn filter should only return objects created after the given txn.
#[tokio::test]
async fn test_objects_since_txn() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19304);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Put first batch
    client
        .put(&format!("{}/testbucket/old.txt", srv.base_url))
        .body("old")
        .send()
        .await
        .unwrap();

    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Get current txn_id
    let info_resp = client
        .get(&format!("{}/_admin/info", srv.base_url))
        .send()
        .await
        .unwrap();
    let info: serde_json::Value = info_resp.json().await.unwrap();
    let txn_id = info["txn_id"].as_u64().unwrap();

    // Put second batch
    client
        .put(&format!("{}/testbucket/new.txt", srv.base_url))
        .body("new")
        .send()
        .await
        .unwrap();

    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Query since_txn -- should only return new.txt
    let resp = client
        .get(&format!(
            "{}/_admin/objects?since_txn={}",
            srv.base_url, txn_id
        ))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    let objects = json["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert!(objects[0]["key"].as_str().unwrap().contains("new.txt"));

    let _ = srv.child.kill();
}

// =========================================================================
// /_admin/shards
// =========================================================================

/// Single-shard server should return one shard in /_admin/shards.
#[tokio::test]
async fn test_shards_single_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19305);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/shards", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let shards = json["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1);

    let s = &shards[0];
    assert_eq!(s["id"].as_u64().unwrap(), 0);
    assert!(s["health"].is_string());

    let _ = srv.child.kill();
}

/// Multi-shard server should return all shards in /_admin/shards.
#[tokio::test]
async fn test_shards_multi_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19306);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        1,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    let resp = client
        .get(&format!("{}/_admin/shards", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let shards = json["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 2);

    // Both should have health and id fields
    for s in shards {
        assert!(s["id"].is_number());
        assert!(s["health"].is_string());
    }

    let _ = srv.child.kill();
}

/// /_admin/objects in multi-shard mode should aggregate across shards.
#[tokio::test]
async fn test_objects_multi_shard_aggregation() {
    let tmp = tempfile::tempdir().unwrap();
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19307);
    write_multi_config(
        &config,
        &[shard0.to_str().unwrap(), shard1.to_str().unwrap()],
        port,
        1,
    );

    let mut srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Put several objects (they will be distributed across shards by hash)
    for i in 0..6 {
        client
            .put(&format!("{}/testbucket/item-{:02}.txt", srv.base_url, i))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // /_admin/objects should return all 6
    let resp = client
        .get(&format!("{}/_admin/objects", srv.base_url))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["total"].as_u64().unwrap(), 6);

    let _ = srv.child.kill();
}
