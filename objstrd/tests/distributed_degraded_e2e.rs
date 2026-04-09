//! E2E tests for S3 behavior during shard failures on a distributed backend.
//!
//! Covers: degraded reads/writes via replicas, cascade failures (multiple
//! shards offline), recovery after total outage, copy during degraded mode,
//! and event emission when shards are unavailable.
//!
//! All tests operate through the S3 HTTP layer (reqwest) backed by an
//! in-process ShardedObjectStore.  Shard failures are simulated via
//! `set_shard_health(id, Offline)`.

mod common;
use common::{
    collect_events_timeout, count_put_events, extract_xml_tags,
    DistributedTestServer,
};
use object_store::ObjectStore;
use shardedobjstr::ShardHealth;
use std::sync::Arc;

// ===========================================================================
// Degraded reads -- verify replicas serve requests when a shard is offline
// ===========================================================================

/// PUT 10 objects with RF=2 across 3 shards, take shard 0 offline, GET all.
#[tokio::test]
async fn s3_read_via_replica_when_shard_offline() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    for i in 0..10 {
        let resp = srv
            .client
            .put(&srv.object_url(&format!("obj-{i}.txt")))
            .body(format!("content-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Take shard 0 offline.  Each object has RF=2 replicas, so the
    // surviving shard still holds a copy.
    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    for i in 0..10 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("obj-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "obj-{i}.txt should be readable via replica"
        );
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.as_ref(), format!("content-{i}").as_bytes());
    }
}

/// HEAD returns correct content-length even when the home shard is offline.
#[tokio::test]
async fn s3_head_returns_correct_size_during_degraded() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let body = b"precise length check";

    srv.client
        .put(&srv.object_url("head-deg.txt"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    let resp = srv
        .client
        .head(&srv.object_url("head-deg.txt"))
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
    assert_eq!(len, body.len(), "content-length should be correct during degraded mode");
}

// ===========================================================================
// Degraded writes -- new PUTs land on surviving shards
// ===========================================================================

/// With 1-of-3 shards offline, PUT RF=2 succeeds (min_writes=1).
#[tokio::test]
async fn s3_put_succeeds_on_surviving_shards() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    for i in 0..5 {
        let resp = srv
            .client
            .put(&srv.object_url(&format!("new-{i}.txt")))
            .body(format!("degraded-write-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "PUT should succeed with 2 healthy shards (min_writes=1)"
        );
    }

    // Verify all written objects are readable.
    for i in 0..5 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("new-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            format!("degraded-write-{i}").as_bytes()
        );
    }
}

/// PUT events are emitted even when writes hit fewer shards.
#[tokio::test]
async fn s3_events_emitted_during_degraded_writes() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let mut event_rx = srv.event_bus.subscribe();

    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    for i in 0..5 {
        let resp = srv
            .client
            .put(&srv.object_url(&format!("ev-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    let events = collect_events_timeout(
        &mut event_rx,
        5,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        count_put_events(&events),
        5,
        "all 5 PUT events should fire even during degraded writes"
    );
}

// ===========================================================================
// LIST during shard failure
// ===========================================================================

/// LIST returns all objects even when a shard is offline (catalog knows all).
#[tokio::test]
async fn s3_list_during_shard_failure() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    let count = 15;
    for i in 0..count {
        srv.client
            .put(&srv.object_url(&format!("list-{i:03}.bin")))
            .body(format!("payload-{i}"))
            .send()
            .await
            .unwrap();
    }

    srv.cluster.set_shard_health(1, ShardHealth::Offline);

    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(
        keys.len(),
        count,
        "LIST should return all {count} objects despite shard 1 offline, got {}",
        keys.len()
    );
}

// ===========================================================================
// Cascade failure -- multiple shards offline
// ===========================================================================

/// RF=3 on 3 shards (mirror): take 2 offline, reads still work from survivor.
/// Writes fail because min_writes=2 but only 1 shard is healthy.
#[tokio::test]
async fn s3_cascade_failure_rf3_two_offline() {
    let srv = DistributedTestServer::start(3, 3, "data").await;

    // Write while all shards healthy.
    for i in 0..5 {
        let resp = srv
            .client
            .put(&srv.object_url(&format!("cascade-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Take 2 shards offline.
    srv.cluster.set_shard_health(1, ShardHealth::Offline);
    srv.cluster.set_shard_health(2, ShardHealth::Offline);

    // Reads should still work (shard 0 has every object in mirror mode).
    for i in 0..5 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("cascade-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "cascade-{i}.txt should be readable from surviving shard"
        );
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.as_ref(), format!("data-{i}").as_bytes());
    }

    // HEAD should also work.
    for i in 0..5 {
        let resp = srv
            .client
            .head(&srv.object_url(&format!("cascade-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // LIST should return all objects.
    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys = extract_xml_tags(&body, "Key");
    assert_eq!(keys.len(), 5, "LIST should return all 5 objects during cascade");

    // Writes should fail (min_writes=2, only 1 shard healthy).
    let resp = srv
        .client
        .put(&srv.object_url("new-during-cascade.txt"))
        .body("should fail")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().as_u16() >= 400,
        "PUT should fail with only 1 healthy shard; got {}",
        resp.status()
    );
}

/// All shards offline -> GET fails; bring them back -> GET succeeds.
#[tokio::test]
async fn s3_all_shards_offline_then_recover() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    for i in 0..5 {
        srv.client
            .put(&srv.object_url(&format!("recover-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Take all shards offline.
    for id in 0..3 {
        srv.cluster.set_shard_health(id, ShardHealth::Offline);
    }

    // GET should fail -- no healthy shards to serve the request.
    let resp = srv
        .client
        .get(&srv.object_url("recover-0.txt"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().as_u16() >= 400,
        "GET should fail with all shards offline; got {}",
        resp.status()
    );

    // Bring all shards back.
    for id in 0..3 {
        srv.cluster.set_shard_health(id, ShardHealth::Healthy);
    }

    // GET should succeed now.
    for i in 0..5 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("recover-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "recover-{i}.txt should be accessible after recovery"
        );
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.as_ref(), format!("data-{i}").as_bytes());
    }
}

// ===========================================================================
// CopyObject during degraded mode
// ===========================================================================

/// CopyObject (COPY mode) preserves content and metadata during degraded mode.
#[tokio::test]
async fn s3_copy_during_degraded_mode() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    // PUT with metadata.
    let resp = srv
        .client
        .put(&srv.object_url("src-deg.txt"))
        .header("content-type", "text/csv")
        .header("x-amz-meta-origin", "degraded-test")
        .body("copy me while degraded")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Take shard offline.
    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    // CopyObject.
    let resp = srv
        .client
        .put(&srv.object_url("dst-deg.txt"))
        .header("x-amz-copy-source", "/data/src-deg.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "CopyObject should succeed during degraded mode"
    );

    // Verify content.
    let resp = srv
        .client
        .get(&srv.object_url("dst-deg.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        b"copy me while degraded"
    );

    // Verify metadata preserved.
    let resp = srv
        .client
        .head(&srv.object_url("dst-deg.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(ct, "text/csv", "content-type not preserved during degraded copy");
    let origin = resp
        .headers()
        .get("x-amz-meta-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        origin, "degraded-test",
        "x-amz-meta-origin not preserved during degraded copy"
    );
}

// ===========================================================================
// DELETE during degraded mode, then verify after recovery
// ===========================================================================

/// DELETE while a shard is offline; bring shard back; object stays deleted.
#[tokio::test]
async fn s3_delete_during_degraded_then_recover() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    srv.client
        .put(&srv.object_url("del-recover.txt"))
        .body("will be deleted while degraded")
        .send()
        .await
        .unwrap();

    srv.cluster.set_shard_health(2, ShardHealth::Offline);

    let resp = srv
        .client
        .delete(&srv.object_url("del-recover.txt"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 200 || resp.status() == 204,
        "DELETE should succeed during degraded mode"
    );

    // Object should be gone.
    let resp = srv
        .client
        .get(&srv.object_url("del-recover.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Bring shard back via sync_and_reattach (cleans stale data before
    // marking the shard healthy).
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = srv
        .raw_stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    shardedobjstr::repair::sync_and_reattach(&srv.cluster, &originals, 2, None).await;

    let resp = srv
        .client
        .get(&srv.object_url("del-recover.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "object should remain deleted after shard recovery"
    );
}
