//! E2E tests verifying that recovery and repair-replication operations produce
//! correct results observable through the S3 protocol layer.
//!
//! Covers: repair-replication with metadata preservation, under-replication repair,
//! over-replication trimming, and catalog rebuild at scale.
//!
//! Tests exercise `shardedobjstr::repair` functions directly on the
//! `DistributedTestServer`'s cluster, then verify outcomes via S3 HTTP.

mod common;
use common::{extract_xml_tags, DistributedTestServer};
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::repair::{over_replication_trim, repair_replication_sweep};
use shardedobjstr::ShardHealth;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `RawRefRegistry` from the test server's raw stores.
fn build_registry(srv: &DistributedTestServer) -> RawRefRegistry {
    let raw_refs = srv
        .raw_stores
        .iter()
        .map(|s| Some(s.clone()))
        .collect::<Vec<_>>();
    let kinds = vec![ShardKind::Raw; raw_refs.len()];
    RawRefRegistry::new(raw_refs, kinds)
}

// ===========================================================================
// Repair-replication -- metadata preservation through S3
// ===========================================================================

/// PUT objects with metadata, take a shard offline, repair-replication, verify that
/// S3 HEAD still returns all user metadata (content-type + x-amz-meta-*).
#[tokio::test]
async fn s3_repair_replication_preserves_metadata() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    // PUT objects with rich metadata.
    for i in 0..10 {
        let resp = srv
            .client
            .put(&srv.object_url(&format!("meta-{i}.txt")))
            .header("content-type", "application/json")
            .header("x-amz-meta-author", "repair-replication-test")
            .header("x-amz-meta-seq", &i.to_string())
            .body(format!("{{\"id\": {i}}}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Take shard 0 offline -- some objects become under-replicated.
    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    let under = srv.cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "expected under-replicated objects after taking shard 0 offline"
    );

    // repair-replication with raw_refs so metadata is preserved during re-replication.
    let result = repair_replication_sweep(&srv.cluster, 100, Some(&registry), None).await;
    assert!(
        result.re_replicated > 0,
        "repair-replication should re-replicate at least one object"
    );

    // After repair-replication, nothing should be under-replicated (among healthy shards).
    let still_under = srv.cluster.find_under_replicated();
    assert!(
        still_under.is_empty(),
        "all objects should be fully replicated After repair-replication; still under: {still_under:?}"
    );

    // Bring shard 0 back so S3 can read from any shard.
    srv.cluster.set_shard_health(0, ShardHealth::Healthy);

    // Verify metadata survived the repair-replication via S3 HEAD.
    for i in 0..10 {
        let resp = srv
            .client
            .head(&srv.object_url(&format!("meta-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "HEAD meta-{i}.txt After repair-replication");
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(
            ct, "application/json",
            "content-type lost After repair-replication for meta-{i}.txt"
        );
        let author = resp
            .headers()
            .get("x-amz-meta-author")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(
            author, "repair-replication-test",
            "x-amz-meta-author lost After repair-replication for meta-{i}.txt"
        );
        let seq = resp
            .headers()
            .get("x-amz-meta-seq")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(
            seq,
            &i.to_string(),
            "x-amz-meta-seq lost After repair-replication for meta-{i}.txt"
        );
    }

    // Also verify GET returns the correct body.
    for i in 0..10 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("meta-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert_eq!(body, format!("{{\"id\": {i}}}"));
    }
}

// ===========================================================================
// Under-replication repair
// ===========================================================================

/// Take shard offline, write objects (under-replicated), repair-replication, verify
/// everything accessible via S3.
#[tokio::test]
async fn s3_repair_replication_under_replicated_restored() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    // Write with all shards healthy.
    for i in 0..15 {
        srv.client
            .put(&srv.object_url(&format!("ur-{i:03}.bin")))
            .body(format!("payload-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Take shard 1 offline.
    srv.cluster.set_shard_health(1, ShardHealth::Offline);

    let under = srv.cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "some objects should be under-replicated after shard 1 offline"
    );

    // repair-replication to fix.
    let result = repair_replication_sweep(&srv.cluster, 100, Some(&registry), None).await;
    assert!(result.re_replicated > 0, "expected re-replication");

    // All objects still accessible via S3 (even with shard 1 still offline).
    for i in 0..15 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("ur-{i:03}.bin")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "ur-{i:03}.bin should be accessible After repair-replication"
        );
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            format!("payload-{i}").as_bytes()
        );
    }

    // Bring shard 1 back.
    srv.cluster.set_shard_health(1, ShardHealth::Healthy);

    // Still accessible.
    for i in 0..15 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("ur-{i:03}.bin")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}

// ===========================================================================
// Over-replication trimming
// ===========================================================================

/// Shard offline -> repair-replication creates extra copies -> bring shard back ->
/// trim excess -> correct RF observable via S3.
#[tokio::test]
async fn s3_over_replication_trimmed_after_reattach() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    // Write objects on all 3 healthy shards (RF=2 -> 2 of 3 hold each).
    for i in 0..10 {
        srv.client
            .put(&srv.object_url(&format!("over-{i}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    // Take shard 2 offline -> some objects under-replicated.
    srv.cluster.set_shard_health(2, ShardHealth::Offline);

    // Repair-replication re-replicates to the 2 surviving shards.  Objects that
    // were on shards {0,2} now get a copy on shard 1, ending up on {0,1}.
    let _result = repair_replication_sweep(&srv.cluster, 100, Some(&registry), None).await;

    // Bring shard 2 back.  Objects that were on {0,2} are now on {0,1,2}
    // = over-replicated (3 copies for RF=2).
    srv.cluster.set_shard_health(2, ShardHealth::Healthy);

    let over = srv.cluster.find_over_replicated();
    // Only objects that gained an extra copy from repair-replication are over-replicated.
    // This depends on placement; may be 0 if all objects were on {0,1} already.
    if !over.is_empty() {
        let trimmed = over_replication_trim(&srv.cluster, 100).await;
        assert!(
            trimmed > 0,
            "expected at least one trim, got 0; over-replicated={:?}",
            over
        );

        // After trim, no over-replication.
        let still_over = srv.cluster.find_over_replicated();
        assert!(
            still_over.is_empty(),
            "over-replicated objects remain: {still_over:?}"
        );
    }

    // All objects still accessible and correct via S3.
    for i in 0..10 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("over-{i}.txt")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "over-{i}.txt should survive trim");
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            format!("data-{i}").as_bytes()
        );
    }
}

// ===========================================================================
// Catalog rebuild at scale
// ===========================================================================

/// Write 100 objects via S3, clear the catalog, rebuild from shards, verify
/// all 100 are accessible via S3.
#[tokio::test]
async fn s3_catalog_rebuild_at_scale() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    let count = 100;
    for i in 0..count {
        let resp = srv
            .client
            .put(&srv.object_url(&format!("scale-{i:04}.bin")))
            .body(format!("payload-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Verify listing before rebuild.
    let url = format!("{}?list-type=2&max-keys=200", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys_before = extract_xml_tags(&body, "Key");
    assert_eq!(
        keys_before.len(),
        count,
        "pre-rebuild listing should have {count} keys"
    );

    // Flush indexes so rebuild can scan them.
    for raw in &srv.raw_stores {
        raw.flush_index().unwrap();
    }

    // Clear the catalog.
    srv.cluster.catalog().clear();

    // Verify the catalog is actually empty.  (The S3 LIST endpoint uses
    // list_with_meta which reads raw stores directly, so it still returns
    // objects.  We verify the catalog itself is empty instead.)
    assert_eq!(
        srv.cluster.catalog().len(),
        0,
        "catalog should be empty after clear"
    );

    // Rebuild.  rebuild_catalog returns total (object, shard) entries, so
    // with RF=2 and 100 objects we expect ~200 entries.
    let rebuilt = srv.cluster.rebuild_catalog().await.unwrap();
    assert!(
        rebuilt >= count,
        "rebuild should recover at least {count} entries, got {rebuilt}"
    );

    // All objects accessible via S3 again.
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let keys_after = extract_xml_tags(&body, "Key");
    assert_eq!(
        keys_after.len(),
        count,
        "post-rebuild listing should have {count} keys"
    );

    // Spot-check some objects.
    for i in [0, 25, 50, 75, 99] {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("scale-{i:04}.bin")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            format!("payload-{i}").as_bytes()
        );
    }
}

// ===========================================================================
// find_under_replicated matches S3 observable state
// ===========================================================================

/// After taking a shard offline, find_under_replicated returns keys that
/// are still readable via S3 (degraded but not lost).
#[tokio::test]
async fn s3_under_replicated_objects_still_readable() {
    let srv = DistributedTestServer::start(3, 2, "data").await;

    for i in 0..20 {
        srv.client
            .put(&srv.object_url(&format!("chk-{i:03}.txt")))
            .body(format!("data-{i}"))
            .send()
            .await
            .unwrap();
    }

    srv.cluster.set_shard_health(0, ShardHealth::Offline);

    let under = srv.cluster.find_under_replicated();
    // Every under-replicated key should still be GET-able via its surviving replica.
    for (key, count) in &under {
        // Strip bucket prefix for the S3 URL.
        let short_key = key.strip_prefix("data/").unwrap_or(key);
        let resp = srv
            .client
            .get(&srv.object_url(short_key))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "under-replicated key {key} (count={count}) should still be readable"
        );
    }
}
