//! E2E tests for delete markers flowing through the S3 adapter backed by
//! a ShardedObjectStore.  Verifies that DELETE creates markers, markers
//! hide objects from LIST/GET/HEAD, re-PUT after delete clears stale
//! markers, and vacuum cleans up.

mod common;
use common::{
    collect_events_timeout, count_delete_events, count_put_events,
    extract_xml_tags, DistributedTestServer,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn delete_xml(keys: &[&str]) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>");
    for key in keys {
        xml.push_str(&format!("<Object><Key>{}</Key></Object>", key));
    }
    xml.push_str("</Delete>");
    xml
}

// ===========================================================================
// Delete markers via S3 protocol
// ===========================================================================

/// DELETE via S3 creates a delete marker in the ShardedObjectStore.
/// A subsequent GET returns 404 and the object is hidden from LIST.
#[tokio::test]
async fn s3_delete_creates_marker() {
    // RF=2 so markers go to multiple shards.
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT an object.
    let resp = srv
        .client
        .put(&srv.object_url("marker-test.txt"))
        .body("hello markers")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify it exists via GET.
    let resp = srv
        .client
        .get(&srv.object_url("marker-test.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello markers");

    // DELETE via S3.
    let resp = srv
        .client
        .delete(&srv.object_url("marker-test.txt"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 204 || resp.status() == 200,
        "expected 200 or 204, got {}",
        resp.status()
    );

    // GET should now return 404.
    let resp = srv
        .client
        .get(&srv.object_url("marker-test.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "deleted object should return 404 via S3"
    );

    // HEAD should return 404.
    let resp = srv
        .client
        .head(&srv.object_url("marker-test.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // LIST should not include the deleted key.
    let url = format!(
        "{}/data?list-type=2&prefix=marker-test",
        srv.base_url
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert!(
        !keys.contains(&"marker-test.txt"),
        "deleted key should not appear in listing"
    );

    // The delete marker should exist in the cluster.
    let markers = srv.cluster.list_delete_markers().await;
    assert!(
        markers.iter().any(|(k, _)| k == "data/marker-test.txt"),
        "expected delete marker for data/marker-test.txt, found: {:?}",
        markers
    );
}

/// Re-PUT after DELETE clears the stale delete marker.
#[tokio::test]
async fn s3_reput_after_delete_clears_marker() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT -> DELETE -> re-PUT cycle.
    let resp = srv
        .client
        .put(&srv.object_url("reput.txt"))
        .body("version1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv
        .client
        .delete(&srv.object_url("reput.txt"))
        .send()
        .await
        .unwrap();
    assert!(resp.status() == 200 || resp.status() == 204);

    // Marker should exist now.
    let markers = srv.cluster.list_delete_markers().await;
    assert!(
        markers.iter().any(|(k, _)| k == "data/reput.txt"),
        "marker should exist after delete"
    );

    // Re-PUT with new content.
    let resp = srv
        .client
        .put(&srv.object_url("reput.txt"))
        .body("version2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET should return the new content.
    let resp = srv
        .client
        .get(&srv.object_url("reput.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"version2");

    // The object should appear in listings.
    let url = format!(
        "{}/data?list-type=2&prefix=reput",
        srv.base_url
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert!(
        keys.contains(&"reput.txt"),
        "re-PUT key should appear in listing"
    );
}

/// Batch delete creates markers for all deleted keys.
#[tokio::test]
async fn s3_batch_delete_creates_markers() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    // Seed 5 objects.
    for i in 0..5 {
        let key = format!("batch-{i}.txt");
        let resp = srv
            .client
            .put(&srv.object_url(&key))
            .body(format!("payload-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
    let _ = collect_events_timeout(
        &mut event_rx,
        5,
        std::time::Duration::from_secs(2),
    )
    .await;

    // Batch delete 3 of them.
    let body = delete_xml(&["batch-0.txt", "batch-2.txt", "batch-4.txt"]);
    let url = format!("{}/data?delete", srv.base_url);
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify deleted keys return 404.
    for key in &["batch-0.txt", "batch-2.txt", "batch-4.txt"] {
        let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
        assert_eq!(
            resp.status(),
            404,
            "batch-deleted key {key} should return 404"
        );
    }

    // Verify surviving keys still return 200.
    for key in &["batch-1.txt", "batch-3.txt"] {
        let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
        assert_eq!(resp.status(), 200, "surviving key {key} should return 200");
    }

    // Listing should only contain survivors.
    let url = format!("{}/data?list-type=2&prefix=batch-", srv.base_url);
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 2, "expected 2 survivors, got: {:?}", keys);

    // DELETE events should have been emitted.
    let events = collect_events_timeout(
        &mut event_rx,
        3,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert!(
        count_delete_events(&events) >= 3,
        "expected at least 3 DELETE events, got {}",
        count_delete_events(&events)
    );
}

/// Vacuum removes stale delete markers.
#[tokio::test]
async fn s3_vacuum_cleans_markers() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT then DELETE to create a marker.
    srv.client
        .put(&srv.object_url("vac.txt"))
        .body("to be vacuumed")
        .send()
        .await
        .unwrap();
    srv.client
        .delete(&srv.object_url("vac.txt"))
        .send()
        .await
        .unwrap();

    // Marker exists.
    let markers = srv.cluster.list_delete_markers().await;
    assert!(
        markers.iter().any(|(k, _)| k == "data/vac.txt"),
        "marker should exist before vacuum"
    );

    // Run vacuum via the cluster API.
    let (markers_removed, _objects_cleaned) =
        srv.cluster.vacuum_delete_markers(None).await.unwrap();
    assert!(
        markers_removed >= 1,
        "vacuum should remove at least 1 marker, removed {markers_removed}"
    );

    // Marker should be gone now.
    let markers = srv.cluster.list_delete_markers().await;
    assert!(
        !markers.iter().any(|(k, _)| k == "data/vac.txt"),
        "marker should be gone after vacuum, still found: {:?}",
        markers
    );
}

/// DELETE on non-existent key returns 204 (S3 idempotent delete).
#[tokio::test]
async fn s3_delete_nonexistent_returns_204() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    let resp = srv
        .client
        .delete(&srv.object_url("does-not-exist.txt"))
        .send()
        .await
        .unwrap();
    // S3 returns 204 for deletes on non-existent keys.
    assert!(
        resp.status() == 200 || resp.status() == 204,
        "DELETE non-existent should return 200 or 204, got {}",
        resp.status()
    );
}

/// DELETE via S3 with RF=2 and events verifies the full lifecycle.
#[tokio::test]
async fn s3_delete_lifecycle_with_events() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    // PUT
    srv.client
        .put(&srv.object_url("lifecycle.txt"))
        .body("lifecycle data")
        .send()
        .await
        .unwrap();
    let put_events = collect_events_timeout(
        &mut event_rx,
        1,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert_eq!(count_put_events(&put_events), 1);

    // DELETE
    srv.client
        .delete(&srv.object_url("lifecycle.txt"))
        .send()
        .await
        .unwrap();
    let del_events = collect_events_timeout(
        &mut event_rx,
        1,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert!(
        count_delete_events(&del_events) >= 1,
        "expected DELETE event"
    );

    // re-PUT
    srv.client
        .put(&srv.object_url("lifecycle.txt"))
        .body("resurrected")
        .send()
        .await
        .unwrap();
    let put2_events = collect_events_timeout(
        &mut event_rx,
        1,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert_eq!(count_put_events(&put2_events), 1);

    // Verify content
    let resp = srv
        .client
        .get(&srv.object_url("lifecycle.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"resurrected");
}
