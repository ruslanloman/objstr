//! Replication tests: verify that at RF=2 objects land on two shards
//! and remain readable when one shard is missing the object.

mod common;
use common::{
    collect_events_timeout, count_delete_events, count_put_events,
    DistributedTestServer,
};

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;

// ===========================================================================
// Replication = 2 mirror verification
// ===========================================================================

/// Put objects via S3 with RF=2, then inspect each raw store directly
/// to confirm every object is on exactly 2 shards.
#[tokio::test]
async fn replication_2_objects_mirrored() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    let keys: Vec<String> = (0..20).map(|i| format!("rep-{i:03}.bin")).collect();
    for key in &keys {
        let body = format!("body-of-{key}");
        let resp = srv
            .client
            .put(&srv.object_url(key))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "PUT {key} failed");
    }

    // For each key, count how many raw shards have it (search with full
    // internal path: "data/{key}").
    for key in &keys {
        let internal_path = format!("data/{key}");
        let path = Path::from(internal_path.as_str());
        let mut shard_hits = Vec::new();
        for (idx, raw) in srv.raw_stores.iter().enumerate() {
            match raw.head(&path).await {
                Ok(_) => shard_hits.push(idx),
                Err(_) => {}
            }
        }
        assert_eq!(
            shard_hits.len(),
            2,
            "key '{key}' should be on 2 shards, but found on {shard_hits:?}"
        );
    }

    // Verify exactly 20 PUT events -- one per object (not per replica).
    let events = collect_events_timeout(
        &mut event_rx, 20, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 20, "expected 20 PUT events for RF=2 writes");
}

/// With RF=2, verify that the two shards hold identical raw content
/// and that reading via S3 returns the correct body.
#[tokio::test]
async fn replication_2_content_identical() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    let key = "ident-check.bin";
    let body = b"identical on both shards";
    srv.client
        .put(&srv.object_url(key))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    // The raw store stores body + metadata trailer, so the two copies
    // must be identical to each other (exact byte match).
    let internal = Path::from(format!("data/{key}").as_str());
    let mut copies = Vec::new();
    for (idx, raw) in srv.raw_stores.iter().enumerate() {
        if let Ok(result) = raw.get(&internal).await {
            let bytes = result.bytes().await.unwrap();
            copies.push((idx, bytes));
        }
    }
    assert_eq!(copies.len(), 2, "expected 2 copies, got {}", copies.len());
    assert_eq!(
        copies[0].1, copies[1].1,
        "raw content on shard {} differs from shard {}",
        copies[0].0, copies[1].0
    );
    // Raw bytes start with the body
    assert!(
        copies[0].1.starts_with(body),
        "raw content does not start with expected body"
    );

    // S3 GET should strip metadata and return exact body
    let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), body);
}

/// With RF=2 and 3 shards, total object count across shards = 2x the
/// number of unique objects; S3 listing deduplicates to the true count.
#[tokio::test]
async fn replication_2_listing_deduplicates() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    let count = 10;
    for i in 0..count {
        srv.client
            .put(&srv.object_url(&format!("dup-{i}.bin")))
            .body(format!("v{i}"))
            .send()
            .await
            .unwrap();
    }

    // Raw total across all shards should be 2x (each stored twice)
    let mut raw_total = 0usize;
    for raw in &srv.raw_stores {
        let items: Vec<_> = raw.list(None).try_collect().await.unwrap();
        raw_total += items.len();
    }
    assert_eq!(
        raw_total,
        count * 2,
        "raw total should be {}, got {raw_total}",
        count * 2
    );

    // S3 listing should show deduplicated count
    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = common::extract_xml_tags(&body, "Key");
    assert_eq!(
        keys.len(),
        count,
        "S3 listing should deduplicate to {count} keys, got {}",
        keys.len()
    );
}

/// After PUT with RF=2, deleting via S3 removes from ALL shards.
#[tokio::test]
async fn replication_2_delete_removes_all_copies() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    let key = "del-repl.bin";
    srv.client
        .put(&srv.object_url(key))
        .body(b"replicated".to_vec())
        .send()
        .await
        .unwrap();

    // Confirm present on 2 shards
    let internal = Path::from(format!("data/{key}").as_str());
    let mut pre_count = 0;
    for raw in &srv.raw_stores {
        if raw.head(&internal).await.is_ok() {
            pre_count += 1;
        }
    }
    assert_eq!(pre_count, 2, "should be on 2 shards before delete");

    // DELETE via S3
    let resp = srv
        .client
        .delete(&srv.object_url(key))
        .send()
        .await
        .unwrap();
    assert!(resp.status() == 204 || resp.status() == 200);

    // Confirm gone from all shards
    for (i, raw) in srv.raw_stores.iter().enumerate() {
        assert!(
            raw.head(&internal).await.is_err(),
            "shard {i} still has the object after DELETE"
        );
    }

    // Verify 1 PUT + 1 DELETE event.
    let events = collect_events_timeout(
        &mut event_rx, 2, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event");
    assert_eq!(count_delete_events(&events), 1, "expected 1 DELETE event");
}

/// With RF=2, the catalog records exactly 2 shards per object.
#[tokio::test]
async fn replication_2_catalog_entries() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    for i in 0..10 {
        let key = format!("cat-{i}.bin");
        srv.client
            .put(&srv.object_url(&key))
            .body(format!("c{i}"))
            .send()
            .await
            .unwrap();
    }

    for i in 0..10 {
        let key = format!("data/cat-{i}.bin");
        let entry = srv
            .cluster
            .catalog()
            .get(&key)
            .unwrap_or_else(|| panic!("catalog missing entry for {key}"));
        assert_eq!(
            entry.shards.len(),
            2,
            "catalog entry for {key} has {} shards, expected 2",
            entry.shards.len()
        );
        // Shards should be different
        assert_ne!(
            entry.shards[0], entry.shards[1],
            "catalog has duplicate shard IDs for {key}"
        );
    }
}

/// With RF=2, overwrite an object and verify both shards get the new data.
#[tokio::test]
async fn replication_2_overwrite() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    let key = "ow-rep.bin";
    srv.client
        .put(&srv.object_url(key))
        .body(b"v1".to_vec())
        .send()
        .await
        .unwrap();

    srv.client
        .put(&srv.object_url(key))
        .body(b"v2".to_vec())
        .send()
        .await
        .unwrap();

    // Both shards should have updated raw content starting with "v2"
    let internal = Path::from(format!("data/{key}").as_str());
    for (idx, raw) in srv.raw_stores.iter().enumerate() {
        if let Ok(result) = raw.get(&internal).await {
            let bytes = result.bytes().await.unwrap();
            assert!(
                bytes.starts_with(b"v2"),
                "shard {idx} raw content does not start with v2 after overwrite"
            );
        }
    }

    // S3 GET should return exactly v2 (metadata stripped)
    let resp = srv
        .client
        .get(&srv.object_url(key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v2");

    // Verify 2 PUT events (original + overwrite).
    let events = collect_events_timeout(
        &mut event_rx, 2, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 2, "expected 2 PUT events for overwrite");
}

/// RF=3 on 3 shards means every object is on all shards.
#[tokio::test]
async fn replication_3_full_mirror() {
    let srv = DistributedTestServer::start(3, 3, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    let key = "full-mirror.bin";
    let body = b"on all shards";
    srv.client
        .put(&srv.object_url(key))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    // All 3 shards have the object and their raw content is identical
    let internal = Path::from(format!("data/{key}").as_str());
    let mut raw_copies = Vec::new();
    for (i, raw) in srv.raw_stores.iter().enumerate() {
        let result = raw
            .get(&internal)
            .await
            .unwrap_or_else(|e| panic!("shard {i} missing object: {e}"));
        let bytes = result.bytes().await.unwrap();
        assert!(
            bytes.starts_with(body),
            "shard {i} raw content does not start with expected body"
        );
        raw_copies.push(bytes);
    }
    // All copies identical
    assert_eq!(raw_copies[0], raw_copies[1], "shard 0 vs 1 mismatch");
    assert_eq!(raw_copies[1], raw_copies[2], "shard 1 vs 2 mismatch");

    // S3 GET returns exact body
    let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), body);

    // Verify 1 PUT event for RF=3 write (one event, not three).
    let events = collect_events_timeout(
        &mut event_rx, 1, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event for RF=3 write");
}

/// Metadata survives replication -- both shard copies have the same metadata.
#[tokio::test]
async fn replication_2_metadata_preserved() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    let key = "meta-rep.txt";
    srv.client
        .put(&srv.object_url(key))
        .header("content-type", "text/plain")
        .header("x-amz-meta-tag", "replicated")
        .body(b"meta content".to_vec())
        .send()
        .await
        .unwrap();

    // Read back via S3 HEAD -- should have the metadata
    let resp = srv
        .client
        .head(&srv.object_url(key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(ct, "text/plain");

    let tag = resp
        .headers()
        .get("x-amz-meta-tag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(tag, "replicated");
}
