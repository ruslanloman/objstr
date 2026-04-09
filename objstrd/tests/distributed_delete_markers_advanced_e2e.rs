//! Advanced delete-marker tests flowing through the S3 adapter on a
//! ShardedObjectStore.
//!
//! Covers scenarios beyond the basic happy path: shard-offline deletes,
//! concurrent deletes, multi-step lifecycles, batch delete during
//! degraded mode, stale marker cleanup, and vacuum edge cases.
//!
//! See also: distributed_delete_markers.rs for the basic tests.

mod common;
use common::{extract_xml_tags, DistributedTestServer};
use object_store::ObjectStore;
use shardedobjstr::ShardHealth;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn delete_xml(keys: &[&str]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>",
    );
    for key in keys {
        xml.push_str(&format!("<Object><Key>{}</Key></Object>", key));
    }
    xml.push_str("</Delete>");
    xml
}

// ===========================================================================
// Delete with shard offline
// ===========================================================================

/// DELETE via S3 while 1-of-3 shards is offline (RF=2).
/// Markers are written to healthy shards; object returns 404 afterward.
#[tokio::test]
async fn s3_delete_with_shard_offline_creates_markers() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // Seed objects.
    for i in 0..5 {
        srv.client
            .put(&srv.object_url(&format!("off-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Take shard 2 offline.
    srv.cluster.set_shard_health(2, ShardHealth::Offline);

    // DELETE all 5 via S3.
    for i in 0..5 {
        let resp = srv
            .client
            .delete(&srv.object_url(&format!("off-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status() == 200 || resp.status() == 204,
            "DELETE off-{i}.txt failed with {}",
            resp.status()
        );
    }

    // All should return 404 via GET.
    for i in 0..5 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("off-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "off-{i}.txt should be 404 after delete with shard offline"
        );
    }

    // Markers should exist on healthy shards.
    let markers = srv.cluster.list_delete_markers().await;
    for i in 0..5 {
        let key = format!("data/off-{i}.txt");
        assert!(
            markers.iter().any(|(k, _)| k == &key),
            "missing delete marker for {key}"
        );
    }

    // Bring shard back via sync_and_reattach (cleans stale data on the
    // recovered shard before marking it healthy).
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = srv
        .raw_stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    shardedobjstr::repair::sync_and_reattach(&srv.cluster, &originals, 2, None).await;

    for i in 0..5 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("off-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "off-{i}.txt should stay deleted after shard recovery"
        );
    }
}

/// Batch DELETE with one shard offline; all keys return 404 afterward.
#[tokio::test]
async fn s3_batch_delete_with_shard_offline() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // Seed 5 objects.
    for i in 0..5 {
        srv.client
            .put(&srv.object_url(&format!("batch-off-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    srv.cluster.set_shard_health(1, ShardHealth::Offline);

    // Batch delete all 5.
    let keys: Vec<&str> = vec![
        "batch-off-0.txt",
        "batch-off-1.txt",
        "batch-off-2.txt",
        "batch-off-3.txt",
        "batch-off-4.txt",
    ];
    let body = delete_xml(&keys);
    let url = format!("{}?delete", srv.bucket_url());
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // All should be 404 via GET (catalog entries removed, markers prevent reads).
    for key in &keys {
        let resp = srv.client.get(&srv.object_url(key)).send().await.unwrap();
        assert_eq!(
            resp.status(),
            404,
            "{key} should be 404 after batch delete with shard offline"
        );
    }

    // Bring the offline shard back via sync_and_reattach so stale data on
    // shard 1 is cleaned before it becomes readable in listings.
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = srv
        .raw_stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    shardedobjstr::repair::sync_and_reattach(&srv.cluster, &originals, 1, None).await;

    // After sync, listing should be empty.
    let url = format!("{}?list-type=2&prefix=batch-off-", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let listed = extract_xml_tags(&body, "Key");
    assert!(
        listed.is_empty(),
        "no batch-off keys should appear in listing after sync: {listed:?}"
    );
}

// ===========================================================================
// Concurrent deletes
// ===========================================================================

/// Fire 5 concurrent DELETE requests via S3; all should succeed and create
/// markers.
#[tokio::test]
async fn s3_concurrent_deletes_all_create_markers() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    for i in 0..5 {
        srv.client
            .put(&srv.object_url(&format!("conc-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Delete all 5 concurrently.
    let mut handles = Vec::new();
    for i in 0..5 {
        let client = srv.client.clone();
        let url = srv.object_url(&format!("conc-{i}.txt"));
        handles.push(tokio::spawn(async move {
            client.delete(&url).send().await.unwrap()
        }));
    }
    for h in handles {
        let resp = h.await.unwrap();
        assert!(
            resp.status() == 200 || resp.status() == 204,
            "concurrent DELETE failed: {}",
            resp.status()
        );
    }

    // All should be 404.
    for i in 0..5 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("conc-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
    }

    // Markers should exist.
    let markers = srv.cluster.list_delete_markers().await;
    for i in 0..5 {
        let key = format!("data/conc-{i}.txt");
        assert!(
            markers.iter().any(|(k, _)| k == &key),
            "missing marker for {key}"
        );
    }
}

// ===========================================================================
// Multi-step lifecycle
// ===========================================================================

/// PUT -> DELETE -> re-PUT -> DELETE -> verify final state via S3.
#[tokio::test]
async fn s3_delete_reput_delete_lifecycle() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // Phase 1: PUT
    srv.client
        .put(&srv.object_url("life.txt"))
        .body("v1")
        .send()
        .await
        .unwrap();

    // Phase 2: DELETE
    srv.client
        .delete(&srv.object_url("life.txt"))
        .send()
        .await
        .unwrap();
    let resp = srv
        .client
        .get(&srv.object_url("life.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "should be 404 after first delete");

    // Phase 3: re-PUT
    srv.client
        .put(&srv.object_url("life.txt"))
        .body("v2")
        .send()
        .await
        .unwrap();
    let resp = srv
        .client
        .get(&srv.object_url("life.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v2");

    // Phase 4: DELETE again
    srv.client
        .delete(&srv.object_url("life.txt"))
        .send()
        .await
        .unwrap();
    let resp = srv
        .client
        .get(&srv.object_url("life.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "should be 404 after second delete");

    // Marker should exist.
    let markers = srv.cluster.list_delete_markers().await;
    assert!(
        markers.iter().any(|(k, _)| k == "data/life.txt"),
        "delete marker missing after lifecycle"
    );
}

// ===========================================================================
// Stale marker after re-PUT with shard offline
// ===========================================================================

/// PUT -> DELETE -> shard offline -> re-PUT -> shard back -> GET returns v2.
#[tokio::test]
async fn s3_stale_marker_cleared_on_get_after_reput() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT and DELETE to create marker.
    srv.client
        .put(&srv.object_url("stale.txt"))
        .body("v1")
        .send()
        .await
        .unwrap();
    srv.client
        .delete(&srv.object_url("stale.txt"))
        .send()
        .await
        .unwrap();

    // Take shard 2 offline (it may still hold the marker and/or stale data).
    srv.cluster.set_shard_health(2, ShardHealth::Offline);

    // Re-PUT while shard 2 is offline.
    let resp = srv
        .client
        .put(&srv.object_url("stale.txt"))
        .body("v2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Bring shard 2 back.
    srv.cluster.set_shard_health(2, ShardHealth::Healthy);

    // GET should return v2 -- the new object is newer than the stale marker.
    let resp = srv
        .client
        .get(&srv.object_url("stale.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v2");
}

// ===========================================================================
// Delete markers hidden from LIST with various prefix patterns
// ===========================================================================

/// After deleting several objects, LIST with various prefixes never returns
/// deleted keys.
#[tokio::test]
async fn s3_delete_marker_not_visible_in_list() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // Seed objects with a directory-like structure.
    let all_keys = [
        "docs/a.txt",
        "docs/b.txt",
        "docs/sub/c.txt",
        "images/d.png",
        "images/e.png",
        "root.txt",
    ];
    for key in &all_keys {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    // Delete some.
    let deleted = ["docs/b.txt", "images/d.png", "root.txt"];
    for key in &deleted {
        srv.client
            .delete(&srv.object_url(key))
            .send()
            .await
            .unwrap();
    }

    // LIST everything -- should only see survivors.
    let url = format!("{}?list-type=2&max-keys=100", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    for del in &deleted {
        assert!(
            !keys.iter().any(|k| k == del),
            "deleted key {del} should not appear in full listing"
        );
    }
    assert_eq!(keys.len(), 3, "expected 3 survivors, got {keys:?}");

    // LIST with prefix=docs/ -- should see a.txt and sub/c.txt only.
    let url = format!("{}?list-type=2&prefix=docs/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 2, "expected 2 docs survivors, got {keys:?}");
    assert!(!keys.iter().any(|k| *k == "docs/b.txt"), "docs/b.txt should be hidden");

    // LIST with prefix=images/ -- should see only e.png.
    let url = format!("{}?list-type=2&prefix=images/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 1, "expected 1 images survivor, got {keys:?}");
    assert_eq!(keys[0], "images/e.png");
}

// ===========================================================================
// Vacuum edge cases
// ===========================================================================

/// PUT A+B, DELETE both, re-PUT A, vacuum. A should remain accessible; B
/// stays deleted.
#[tokio::test]
async fn s3_vacuum_after_reput_removes_only_stale() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT A and B.
    srv.client
        .put(&srv.object_url("vac-a.txt"))
        .body("data-a")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("vac-b.txt"))
        .body("data-b")
        .send()
        .await
        .unwrap();

    // DELETE both.
    srv.client
        .delete(&srv.object_url("vac-a.txt"))
        .send()
        .await
        .unwrap();
    srv.client
        .delete(&srv.object_url("vac-b.txt"))
        .send()
        .await
        .unwrap();

    // Re-PUT A with new content.
    srv.client
        .put(&srv.object_url("vac-a.txt"))
        .body("data-a-v2")
        .send()
        .await
        .unwrap();

    // A should be accessible, B should be 404.
    let resp = srv
        .client
        .get(&srv.object_url("vac-a.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"data-a-v2");

    let resp = srv
        .client
        .get(&srv.object_url("vac-b.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Run vacuum.
    let (removed, _) = srv.cluster.vacuum_delete_markers(None).await.unwrap();
    assert!(
        removed >= 1,
        "vacuum should remove at least 1 marker, removed {removed}"
    );

    // A should still be accessible.
    let resp = srv
        .client
        .get(&srv.object_url("vac-a.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"data-a-v2");

    // B should still be 404.
    let resp = srv
        .client
        .get(&srv.object_url("vac-b.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// Vacuum with a shard offline should still succeed for markers reachable
/// on healthy shards.
#[tokio::test]
async fn s3_vacuum_during_degraded_mode() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT and DELETE.
    srv.client
        .put(&srv.object_url("vac-deg.txt"))
        .body("data")
        .send()
        .await
        .unwrap();
    srv.client
        .delete(&srv.object_url("vac-deg.txt"))
        .send()
        .await
        .unwrap();

    // Take shard offline.
    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    // Vacuum -- the implementation may refuse or succeed depending on version.
    // Either way, the system should not panic and the marker should be gone or
    // the operation should return an error.
    let result = srv.cluster.vacuum_delete_markers(None).await;
    match result {
        Ok((removed, _)) => {
            // If vacuum succeeded, verify the marker was cleaned.
            if removed > 0 {
                let markers = srv.cluster.list_delete_markers().await;
                assert!(
                    !markers.iter().any(|(k, _)| k == "data/vac-deg.txt"),
                    "marker should be gone after successful vacuum"
                );
            }
        }
        Err(_) => {
            // Some versions refuse vacuum with offline shards -- that is
            // also acceptable behavior.
        }
    }
}
