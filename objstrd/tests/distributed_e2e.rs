//! E2E tests for the distributed store with 3 raw image backends
//! served through the S3 protocol layer.

mod common;
use common::{
    collect_events_timeout, complete_xml, count_delete_events, count_put_events,
    drain_events, extract_xml_tag, extract_xml_tags, put_event_keys, DistributedTestServer,
};

use futures::TryStreamExt;
use object_store::ObjectStore;

// ===========================================================================
// Basic CRUD through S3 on a 3-shard distributed store
// ===========================================================================

/// PUT then GET an object through the S3 layer backed by 3 raw shards.
#[tokio::test]
async fn distributed_put_get() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();
    let body = b"hello distributed world";
    let resp = srv
        .client
        .put(&srv.object_url("greeting.txt"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv
        .client
        .get(&srv.object_url("greeting.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.as_ref(), body);

    // Verify exactly 1 PUT event with the correct key.
    let events = collect_events_timeout(
        &mut event_rx, 1, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event");
    let keys = put_event_keys(&events);
    assert!(keys.contains("data/greeting.txt"), "PUT event key mismatch: {keys:?}");
}

/// PUT many objects across 3 shards and verify they all list correctly.
#[tokio::test]
async fn distributed_list_many() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    let count = 30;
    for i in 0..count {
        let key = format!("obj-{i:04}.bin");
        let body = format!("payload-{i}");
        let resp = srv
            .client
            .put(&srv.object_url(&key))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "PUT obj-{i:04}.bin failed");
    }

    // ListObjectsV2
    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(
        keys.len(),
        count,
        "expected {count} keys in listing, got {}",
        keys.len()
    );

    // Verify 30 PUT events were emitted.
    let events = collect_events_timeout(
        &mut event_rx, count, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(
        count_put_events(&events), count,
        "expected {count} PUT events, got {}",
        count_put_events(&events)
    );
}

/// Verify objects distribute across all 3 shards (not all on shard 0).
#[tokio::test]
async fn distributed_objects_spread_across_shards() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    for i in 0..30 {
        let key = format!("spread-{i}.bin");
        srv.client
            .put(&srv.object_url(&key))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Check each raw store directly to see how many objects it has
    let mut shard_counts = Vec::new();
    for raw in &srv.raw_stores {
        let items: Vec<_> = raw.list(None).try_collect().await.unwrap();
        shard_counts.push(items.len());
    }

    // Each shard should have at least 1 object (probabilistic, 30 objects / 3 shards)
    for (i, &c) in shard_counts.iter().enumerate() {
        assert!(c > 0, "shard {i} has 0 objects, distribution is broken");
    }
    // Total across all shards should equal 30
    let total: usize = shard_counts.iter().sum();
    assert_eq!(total, 30, "total objects across shards = {total}, expected 30");
}

/// DELETE through S3 on a distributed store.
#[tokio::test]
async fn distributed_delete() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    srv.client
        .put(&srv.object_url("del-me.txt"))
        .body(b"gone".to_vec())
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .delete(&srv.object_url("del-me.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "DELETE returned {}", resp.status());

    let resp = srv
        .client
        .get(&srv.object_url("del-me.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "object should be gone after DELETE");

    // Verify 1 PUT + 1 DELETE event.
    let events = collect_events_timeout(
        &mut event_rx, 2, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event");
    assert_eq!(count_delete_events(&events), 1, "expected 1 DELETE event");
}

/// HEAD on a distributed object returns correct content-length.
#[tokio::test]
async fn distributed_head() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let body = b"head check";
    srv.client
        .put(&srv.object_url("head.txt"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .head(&srv.object_url("head.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let len = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    assert_eq!(len, body.len(), "content-length mismatch");
}

/// HEAD bucket on a distributed store returns x-amz-bucket-region.
#[tokio::test]
async fn distributed_head_bucket_region() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let resp = srv
        .client
        .head(&srv.bucket_url())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let region = resp
        .headers()
        .get("x-amz-bucket-region")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(
        region.as_deref(),
        Some("us-east-1"),
        "HeadBucket should return x-amz-bucket-region header"
    );
}

/// ListBuckets on a distributed store returns BucketRegion.
#[tokio::test]
async fn distributed_list_buckets_region() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let resp = srv.client.get(&srv.base_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let region = extract_xml_tag(&body, "BucketRegion");
    assert_eq!(
        region,
        Some("us-east-1"),
        "ListBuckets should include BucketRegion: {}",
        body
    );
}

/// Objects with metadata survive the distributed path.
#[tokio::test]
async fn distributed_metadata_roundtrip() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    let resp = srv
        .client
        .put(&srv.object_url("meta.txt"))
        .header("content-type", "text/plain")
        .header("x-amz-meta-color", "blue")
        .body(b"metadata test".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv
        .client
        .head(&srv.object_url("meta.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(ct, "text/plain", "content-type mismatch: {ct}");

    let color = resp
        .headers()
        .get("x-amz-meta-color")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(color, "blue", "x-amz-meta-color mismatch: {color}");
}

/// Overwrite an object and verify the new content is returned.
#[tokio::test]
async fn distributed_overwrite() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    srv.client
        .put(&srv.object_url("ow.txt"))
        .body(b"version1".to_vec())
        .send()
        .await
        .unwrap();

    srv.client
        .put(&srv.object_url("ow.txt"))
        .body(b"version2".to_vec())
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .get(&srv.object_url("ow.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"version2");

    // Verify 2 PUT events (original + overwrite).
    let events = collect_events_timeout(
        &mut event_rx, 2, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 2, "expected 2 PUT events for overwrite");
}

/// Listing with a prefix only returns matching objects.
#[tokio::test]
async fn distributed_list_with_prefix() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    for key in ["alpha/one.txt", "alpha/two.txt", "beta/three.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body(b"x".to_vec())
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2&prefix=alpha/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 2, "expected 2 keys under alpha/, got {keys:?}");
    for k in &keys {
        assert!(k.starts_with("alpha/"), "unexpected key: {k}");
    }
}

/// GET on a non-existent key returns 404.
#[tokio::test]
async fn distributed_get_not_found() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let resp = srv
        .client
        .get(&srv.object_url("no-such-key.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// Catalog rebuild discovers objects from all shards.
#[tokio::test]
async fn distributed_catalog_rebuild() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    // Write 15 objects
    for i in 0..15 {
        srv.client
            .put(&srv.object_url(&format!("rb-{i}.bin")))
            .body(format!("d{i}"))
            .send()
            .await
            .unwrap();
    }

    // Catalog should track them
    assert_eq!(srv.cluster.catalog().len(), 15);

    // Clear and rebuild
    srv.cluster.catalog().clear();
    assert_eq!(srv.cluster.catalog().len(), 0);

    srv.cluster.rebuild_catalog().await.unwrap();
    assert_eq!(
        srv.cluster.catalog().len(),
        15,
        "catalog rebuild should rediscover all 15 objects"
    );
}

// ===========================================================================
// Range GET on distributed store
// ===========================================================================

/// Range GET returns 206 with the correct byte slice.
#[tokio::test]
async fn distributed_range_get() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let body = b"abcdefghijklmnopqrstuvwxyz";
    srv.client
        .put(&srv.object_url("range.bin"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    // Fetch bytes 5-9 (inclusive)
    let resp = srv
        .client
        .get(&srv.object_url("range.bin"))
        .header("range", "bytes=5-9")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "expected 206 Partial Content");
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"fghij", "range slice mismatch");
}

/// Suffix range GET: last N bytes.
#[tokio::test]
async fn distributed_range_get_suffix() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let body = b"0123456789";
    srv.client
        .put(&srv.object_url("suffix.bin"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .get(&srv.object_url("suffix.bin"))
        .header("range", "bytes=-3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"789");
}

// ===========================================================================
// CopyObject on distributed store
// ===========================================================================

/// Copy an object between (potentially different) shards.
#[tokio::test]
async fn distributed_copy_object() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();
    let body = b"copy me across shards";
    srv.client
        .put(&srv.object_url("src-copy.bin"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    // CopyObject
    let resp = srv
        .client
        .put(&srv.object_url("dst-copy.bin"))
        .header("x-amz-copy-source", "/data/src-copy.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();
    assert!(
        xml.contains("<CopyObjectResult"),
        "expected CopyObjectResult in response"
    );

    // Verify the copy is readable and has the correct body
    let resp = srv
        .client
        .get(&srv.object_url("dst-copy.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), body);

    // Verify 2 PUT events: one for the original, one for the copy.
    let events = collect_events_timeout(
        &mut event_rx, 2, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 2, "expected 2 PUT events (src + copy)");
    let keys = put_event_keys(&events);
    assert!(keys.contains("data/src-copy.bin"), "missing PUT event for src");
    assert!(keys.contains("data/dst-copy.bin"), "missing PUT event for copy dest");
}

/// Copy preserves metadata.
#[tokio::test]
async fn distributed_copy_preserves_metadata() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    srv.client
        .put(&srv.object_url("src-meta.txt"))
        .header("content-type", "text/csv")
        .header("x-amz-meta-origin", "test")
        .body(b"data".to_vec())
        .send()
        .await
        .unwrap();

    srv.client
        .put(&srv.object_url("dst-meta.txt"))
        .header("x-amz-copy-source", "/data/src-meta.txt")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .head(&srv.object_url("dst-meta.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(ct, "text/csv", "content-type not preserved in copy");
    let origin = resp
        .headers()
        .get("x-amz-meta-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(origin, "test", "x-amz-meta-origin not preserved in copy");
}

// ===========================================================================
// Multi-delete on distributed store
// ===========================================================================

/// DeleteObjects removes multiple objects in one request.
#[tokio::test]
async fn distributed_multi_delete() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    let keys: Vec<String> = (0..5).map(|i| format!("mdel-{i}.bin")).collect();
    for key in &keys {
        srv.client
            .put(&srv.object_url(key))
            .body(format!("body-{key}"))
            .send()
            .await
            .unwrap();
    }

    // Build DeleteObjects XML
    let mut xml = String::from("<Delete>");
    for key in &keys {
        xml.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    xml.push_str("</Delete>");

    let url = format!("{}?delete", srv.bucket_url());
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(xml)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<DeleteResult"), "expected DeleteResult XML");

    // Verify all objects are gone
    for key in &keys {
        let resp = srv
            .client
            .get(&srv.object_url(key))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "{key} should be gone after multi-delete"
        );
    }

    // Verify 5 PUT + 5 DELETE events.
    let events = collect_events_timeout(
        &mut event_rx, 10, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 5, "expected 5 PUT events");
    assert_eq!(count_delete_events(&events), 5, "expected 5 DELETE events");
}

/// Batch-deleting keys that do not exist still emits DELETE events in sharded
/// mode (the sharded backend returns Ok for missing keys).  In raw standalone
/// mode the backend returns NotFound and no event is emitted -- but that path
/// is not exercised here.
#[tokio::test]
async fn distributed_multi_delete_events_for_missing() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    // PUT 2 real objects
    for key in ["real-a.txt", "real-b.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    // Batch-delete: 2 real keys + 2 ghost keys
    let mut xml = String::from("<Delete>");
    for key in ["real-a.txt", "ghost-1.txt", "real-b.txt", "ghost-2.txt"] {
        xml.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    xml.push_str("</Delete>");

    let url = format!("{}?delete", srv.bucket_url());
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(xml)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    // S3 reports all 4 as successfully deleted
    assert!(body.contains("<DeleteResult"), "expected DeleteResult XML");

    // 2 PUT + 4 DELETE events (sharded backend emits for all keys including ghosts)
    let events = collect_events_timeout(
        &mut event_rx, 6, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 2, "expected 2 PUT events");
    assert_eq!(
        count_delete_events(&events), 4,
        "sharded backend should emit DELETE events for all keys including missing ones"
    );
}

// ===========================================================================
// ListObjectsV2 with delimiter on distributed store
// ===========================================================================

/// Delimiter listing returns CommonPrefixes for nested keys.
#[tokio::test]
async fn distributed_list_delimiter() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    // Create objects with directory-like structure
    for key in [
        "root.txt",
        "docs/readme.md",
        "docs/guide.md",
        "images/logo.png",
        "images/banner.png",
        "images/icons/small.png",
    ] {
        srv.client
            .put(&srv.object_url(key))
            .body(b"x".to_vec())
            .send()
            .await
            .unwrap();
    }

    // List with delimiter at root
    let url = format!("{}?list-type=2&delimiter=/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    // Should have 1 root-level object
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 1, "expected 1 root key, got {keys:?}");
    assert_eq!(keys[0], "root.txt");

    // Should have 2 common prefixes: docs/, images/
    let prefixes = extract_xml_tags(&body, "Prefix");
    assert!(
        prefixes.len() >= 2,
        "expected at least 2 common prefixes, got {prefixes:?}"
    );
    assert!(prefixes.contains(&"docs/"), "missing docs/ prefix");
    assert!(prefixes.contains(&"images/"), "missing images/ prefix");
}

/// Delimiter + prefix narrows to a subtree.
#[tokio::test]
async fn distributed_list_delimiter_with_prefix() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    for key in [
        "a/b/1.txt",
        "a/b/2.txt",
        "a/c/3.txt",
        "a/top.txt",
    ] {
        srv.client
            .put(&srv.object_url(key))
            .body(b"x".to_vec())
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2&delimiter=/&prefix=a/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 1, "expected 1 key under a/ at this level: {keys:?}");
    assert_eq!(keys[0], "a/top.txt");

    let prefixes = extract_xml_tags(&body, "Prefix");
    assert!(prefixes.contains(&"a/b/"), "missing a/b/ prefix in {prefixes:?}");
    assert!(prefixes.contains(&"a/c/"), "missing a/c/ prefix in {prefixes:?}");
}

// ===========================================================================
// Multipart upload on distributed store
// ===========================================================================

/// Basic multipart upload through the distributed S3 layer.
#[tokio::test]
async fn distributed_multipart_basic() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();
    let key = "multi/parts.bin";

    // Initiate
    let url = format!("{}?uploads", srv.object_url(key));
    let resp = srv.client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&xml, "UploadId")
        .expect("missing UploadId in InitiateMultipartUpload response");

    // Upload 3 parts (each > 0 bytes)
    let part_bodies: Vec<Vec<u8>> = vec![
        vec![0xAA; 1024],
        vec![0xBB; 2048],
        vec![0xCC; 512],
    ];
    let mut parts: Vec<(u32, String)> = Vec::new();
    for (i, body) in part_bodies.iter().enumerate() {
        let part_num = (i + 1) as u32;
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url(key),
            part_num,
            upload_id
        );
        let resp = srv.client.put(&url).body(body.clone()).send().await.unwrap();
        assert_eq!(resp.status(), 200, "UploadPart {part_num} failed");
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        parts.push((part_num, etag));
    }

    // Complete
    let xml_parts: Vec<(u32, &str)> = parts.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let complete_body = complete_xml(&xml_parts);
    let url = format!("{}?uploadId={}", srv.object_url(key), upload_id);
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "CompleteMultipartUpload failed");

    // Verify the full object
    let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let got = resp.bytes().await.unwrap();
    let expected_len: usize = part_bodies.iter().map(|p| p.len()).sum();
    assert_eq!(
        got.len(),
        expected_len,
        "multipart object size mismatch: got {} expected {}",
        got.len(),
        expected_len
    );
    // Verify content: first 1024 bytes should be 0xAA, next 2048 0xBB, last 512 0xCC
    assert!(got[..1024].iter().all(|&b| b == 0xAA), "part 1 content mismatch");
    assert!(got[1024..3072].iter().all(|&b| b == 0xBB), "part 2 content mismatch");
    assert!(got[3072..3584].iter().all(|&b| b == 0xCC), "part 3 content mismatch");

    // Verify 1 PUT event after CompleteMultipartUpload (not per-part).
    let events = collect_events_timeout(
        &mut event_rx, 1, std::time::Duration::from_secs(2),
    ).await;
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event after multipart complete");
    let keys = put_event_keys(&events);
    assert!(keys.contains("data/multi/parts.bin"), "PUT event key mismatch for multipart");
}

/// Multipart abort discards incomplete upload.
#[tokio::test]
async fn distributed_multipart_abort() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let mut event_rx = srv.event_bus.subscribe();
    let key = "multi/abort.bin";

    // Initiate
    let url = format!("{}?uploads", srv.object_url(key));
    let resp = srv.client.post(&url).send().await.unwrap();
    let xml = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&xml, "UploadId").unwrap();

    // Upload one part
    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url(key),
        upload_id
    );
    srv.client
        .put(&url)
        .body(vec![0xDD; 512])
        .send()
        .await
        .unwrap();

    // Abort
    let url = format!("{}?uploadId={}", srv.object_url(key), upload_id);
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204, "AbortMultipartUpload returned {}", resp.status());

    // Object should not exist
    let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
    assert_eq!(resp.status(), 404, "aborted multipart should not be accessible");

    // Verify NO PUT event was emitted (abort means the object was never completed).
    let events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&events), 0, "aborted multipart should not emit PUT event");
}

// ===========================================================================
// ListObjectsV2 pagination on distributed store
// ===========================================================================

/// max-keys limits the number of returned objects and sets IsTruncated.
#[tokio::test]
async fn distributed_list_pagination() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    for i in 0..10 {
        srv.client
            .put(&srv.object_url(&format!("pg-{i:02}.bin")))
            .body(format!("p{i}"))
            .send()
            .await
            .unwrap();
    }

    // First page: max-keys=4
    let url = format!("{}?list-type=2&max-keys=4", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 4, "first page should have 4 keys, got {}", keys.len());

    let truncated = extract_xml_tag(&body, "IsTruncated").unwrap_or("false");
    assert_eq!(truncated, "true", "should be truncated");

    let token = extract_xml_tag(&body, "NextContinuationToken")
        .expect("expected NextContinuationToken");

    // Second page
    let url = format!(
        "{}?list-type=2&max-keys=4&continuation-token={}",
        srv.bucket_url(),
        token
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys2 = extract_xml_tags(&body, "Key");
    assert_eq!(keys2.len(), 4, "second page should have 4 keys, got {}", keys2.len());

    // Third page: remaining 2
    if let Some(token2) = extract_xml_tag(&body, "NextContinuationToken") {
        let url = format!(
            "{}?list-type=2&max-keys=4&continuation-token={}",
            srv.bucket_url(),
            token2
        );
        let resp = srv.client.get(&url).send().await.unwrap();
        let body = resp.text().await.unwrap();
        let keys3 = extract_xml_tags(&body, "Key");
        assert_eq!(keys3.len(), 2, "third page should have 2 keys, got {}", keys3.len());
    }
}
