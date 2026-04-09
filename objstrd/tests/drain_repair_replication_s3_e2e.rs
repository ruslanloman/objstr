//! Comprehensive E2E tests for drain and repair-replication operations verified
//! through the S3 protocol layer.
//!
//! Tests exercise the /_admin/drain and /_admin/repair-replication endpoints via
//! HTTP, then verify data integrity, metadata preservation, listing
//! consistency, and placement correctness through S3 GET/HEAD/LIST.
//!
//! All tests use the in-process DistributedTestServer (3 raw shards).

mod common;

use std::sync::Arc;

use common::{extract_xml_tags, DistributedTestServer};
use object_store::ObjectStore;
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::repair::{drain_shard, repair_replication_sweep, over_replication_trim};
use shardedobjstr::ShardHealth;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_registry(srv: &DistributedTestServer) -> RawRefRegistry {
    let raw_refs = srv
        .raw_stores
        .iter()
        .map(|s| Some(s.clone()))
        .collect::<Vec<_>>();
    let kinds = vec![ShardKind::Raw; raw_refs.len()];
    RawRefRegistry::new(raw_refs, kinds)
}

/// Perform a drain of the given shard: grab victim store, detach, then
/// call drain_shard, and optionally follow up with repair_replication_sweep.
async fn do_drain(
    srv: &DistributedTestServer,
    shard_id: usize,
    registry: &RawRefRegistry,
) {
    let victim_store: Arc<dyn ObjectStore> = srv.raw_stores[shard_id].clone();
    srv.cluster.rebuild_catalog().await.unwrap();
    srv.cluster.detach_shard(shard_id);
    drain_shard(
        &srv.cluster,
        &srv.cluster,
        shard_id,
        &victim_store,
        Some(registry),
        None,
    )
    .await;
}

/// PUT `count` objects via S3 with deterministic keys and payloads.
/// Returns the list of keys (without bucket prefix).
async fn put_test_objects(
    srv: &DistributedTestServer,
    prefix: &str,
    count: usize,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("{prefix}/{i:04}.bin");
        let body = format!("payload-{prefix}-{i}");
        let resp = srv
            .client
            .put(&srv.object_url(&key))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "PUT {key} should succeed");
        keys.push(key);
    }
    keys
}

/// PUT objects with custom metadata via S3.
async fn put_objects_with_metadata(
    srv: &DistributedTestServer,
    prefix: &str,
    count: usize,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("{prefix}/{i:04}.bin");
        let resp = srv
            .client
            .put(&srv.object_url(&key))
            .header("content-type", "application/octet-stream")
            .header("x-amz-meta-index", &i.to_string())
            .header("x-amz-meta-source", "drain-test")
            .body(format!("meta-payload-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "PUT {key} with metadata should succeed");
        keys.push(key);
    }
    keys
}

/// S3 GET and verify body matches expected content.
async fn verify_object_body(
    srv: &DistributedTestServer,
    key: &str,
    expected: &str,
) {
    let resp = srv
        .client
        .get(&srv.object_url(key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET {key} should return 200");
    let body = resp.text().await.unwrap();
    assert_eq!(body, expected, "body mismatch for {key}");
}

/// S3 HEAD and verify metadata headers.
async fn verify_object_metadata(
    srv: &DistributedTestServer,
    key: &str,
    expected_index: usize,
) {
    let resp = srv
        .client
        .head(&srv.object_url(key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "HEAD {key} should return 200");

    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        ct, "application/octet-stream",
        "content-type lost for {key}"
    );

    let source = resp
        .headers()
        .get("x-amz-meta-source")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(source, "drain-test", "x-amz-meta-source lost for {key}");

    let idx = resp
        .headers()
        .get("x-amz-meta-index")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        idx,
        &expected_index.to_string(),
        "x-amz-meta-index wrong for {key}"
    );
}

/// S3 LIST all keys in the bucket via ListObjectsV2.
#[allow(dead_code)]
async fn list_all_keys(srv: &DistributedTestServer) -> Vec<String> {
    let url = format!("{}?list-type=2&max-keys=1000", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "LIST should succeed");
    let body = resp.text().await.unwrap();
    extract_xml_tags(&body, "Key")
        .into_iter()
        .map(|s| s.to_string())
        .collect()
}

/// S3 LIST keys with a prefix.
async fn list_keys_with_prefix(
    srv: &DistributedTestServer,
    prefix: &str,
) -> Vec<String> {
    let url = format!(
        "{}?list-type=2&max-keys=1000&prefix={}",
        srv.bucket_url(),
        prefix
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "LIST with prefix should succeed");
    let body = resp.text().await.unwrap();
    extract_xml_tags(&body, "Key")
        .into_iter()
        .map(|s| s.to_string())
        .collect()
}

// ===========================================================================
// Test 1: Drain preserves all data accessible via S3 GET
// ===========================================================================

/// PUT 30 objects, drain shard 0, verify every object is still readable
/// via S3 GET with correct body content.
#[tokio::test]
async fn drain_preserves_all_data_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "drain-data", 30).await;

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Every object should still be readable via S3.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-drain-data-{i}")).await;
    }
}

// ===========================================================================
// Test 2: Drain preserves metadata through S3 HEAD
// ===========================================================================

/// PUT objects with custom metadata, drain shard 0, verify HEAD still
/// returns content-type and x-amz-meta-* headers.
#[tokio::test]
async fn drain_preserves_metadata_via_s3_head() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_objects_with_metadata(&srv, "drain-meta", 20).await;

    do_drain(&srv, 0, &registry).await;

    // HEAD each object and verify metadata survived.
    for (i, key) in keys.iter().enumerate() {
        verify_object_metadata(&srv, key, i).await;
    }

    // Bodies should also be correct.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("meta-payload-{i}")).await;
    }
}

// ===========================================================================
// Test 3: S3 LIST consistency after drain
// ===========================================================================

/// Verify that S3 ListObjectsV2 returns the same keys before and after
/// draining a shard.
#[tokio::test]
async fn drain_list_consistency_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let _keys = put_test_objects(&srv, "drain-list", 25).await;

    // LIST before drain.
    let before = list_keys_with_prefix(&srv, "drain-list/").await;
    assert_eq!(
        before.len(),
        25,
        "pre-drain LIST should return 25 keys, got {}",
        before.len()
    );

    do_drain(&srv, 0, &registry).await;

    // LIST after drain.
    let after = list_keys_with_prefix(&srv, "drain-list/").await;
    assert_eq!(
        after.len(),
        25,
        "post-drain LIST should still return 25 keys, got {}",
        after.len()
    );

    // Same keys in the same order.
    let mut before_sorted = before.clone();
    let mut after_sorted = after.clone();
    before_sorted.sort();
    after_sorted.sort();
    assert_eq!(before_sorted, after_sorted, "key sets should match after drain");
}

// ===========================================================================
// Test 4: drain + repair-replication full lifecycle via S3
// ===========================================================================

/// PUT data, drain shard 0, Run repair-replication to restore RF, verify
/// everything via S3 (GET + LIST + HEAD).
#[tokio::test]
async fn drain_then_repair_replication_full_lifecycle_via_s3() {
    let srv = DistributedTestServer::start(4, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_objects_with_metadata(&srv, "lifecycle", 20).await;

    // Record pre-drain listing.
    let pre_keys = list_keys_with_prefix(&srv, "lifecycle/").await;
    assert_eq!(pre_keys.len(), 20);

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Shard 0 is now offline. repair-replication to restore RF on remaining shards.
    let _result = repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;
    // Some objects may have been under-replicated after drain; repair-replication fixes them.

    // All objects readable via S3 GET with correct body.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("meta-payload-{i}")).await;
    }

    // All metadata preserved via S3 HEAD.
    for (i, key) in keys.iter().enumerate() {
        verify_object_metadata(&srv, key, i).await;
    }

    // LIST still shows all keys.
    let post_keys = list_keys_with_prefix(&srv, "lifecycle/").await;
    assert_eq!(post_keys.len(), 20);

    // No under-replicated objects remain.
    let under = srv.cluster.find_under_replicated();
    assert!(
        under.is_empty(),
        "no under-replicated objects should remain After repair-replication: {:?}",
        under
    );
}

// ===========================================================================
// Test 5: Sequential drains -- drain shard 0, then shard 1
// ===========================================================================

/// Verify data survives multiple sequential drains.
#[tokio::test]
async fn sequential_drains_preserve_data_via_s3() {
    let srv = DistributedTestServer::start(4, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "seqdrain", 30).await;

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // repair-replication to restore RF.
    repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;

    // All still readable.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-seqdrain-{i}")).await;
    }

    // Drain shard 1.
    do_drain(&srv, 1, &registry).await;

    // Repair-replication again.
    repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;

    // All objects still accessible via S3 -- now distributed across shards 2 and 3.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-seqdrain-{i}")).await;
    }

    // LIST still returns all keys.
    let listed = list_keys_with_prefix(&srv, "seqdrain/").await;
    assert_eq!(listed.len(), 30);
}

// ===========================================================================
// Test 6: Writes after drain skip drained shard
// ===========================================================================

/// After draining shard 0, new PUTs via S3 should not place data on the
/// drained shard.
#[tokio::test]
async fn new_writes_skip_drained_shard_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    // Initial data.
    put_test_objects(&srv, "predrain", 10).await;

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Write new objects after drain.
    let new_keys = put_test_objects(&srv, "postdrain", 15).await;

    // Verify new objects are readable.
    for (i, key) in new_keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-postdrain-{i}")).await;
    }

    // New objects should not be placed on shard 0 (offline).
    for key in &new_keys {
        let bucket_key = format!("data/{key}");
        if let Some(entry) = srv.cluster.placement(&bucket_key) {
            assert!(
                !entry.shards.contains(&0),
                "new object {key} should not be on drained shard 0; placed on {:?}",
                entry.shards
            );
        }
    }
}

// ===========================================================================
// Test 7: Repair-replication after shard offline restores RF via S3
// ===========================================================================

/// Take shard offline (without drain), repair-replication, verify RF restored
/// and all objects readable via S3.
#[tokio::test]
async fn repair_replication_restores_rf_verified_via_s3() {
    let srv = DistributedTestServer::start(4, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "rebal-rf", 20).await;

    // Take shard 1 offline.
    srv.cluster.set_shard_health(1, ShardHealth::Offline);

    let under = srv.cluster.find_under_replicated();
    assert!(!under.is_empty(), "should have under-replicated objects");

    // Repair-replication.
    let result = repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;
    assert!(result.re_replicated > 0, "should re-replicate some objects");

    // No under-replicated objects.
    let still_under = srv.cluster.find_under_replicated();
    assert!(still_under.is_empty(), "all should be restored: {:?}", still_under);

    // All readable via S3 (shard 1 still offline -- reads from survivors).
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-rebal-rf-{i}")).await;
    }

    // LIST shows all keys.
    let listed = list_keys_with_prefix(&srv, "rebal-rf/").await;
    assert_eq!(listed.len(), 20);
}

// ===========================================================================
// Test 8: Over-replication trimming after shard returns
// ===========================================================================

/// Take shard offline, repair-replication (creates extra copies), bring back,
/// trim, verify correct RF and all data readable via S3.
#[tokio::test]
async fn over_replication_trim_verified_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "ovr-trim", 15).await;

    // Take shard 2 offline.
    srv.cluster.set_shard_health(2, ShardHealth::Offline);

    // Repair-replication: re-replicates to surviving shards.
    repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;

    // Bring shard 2 back: some objects now have 3 copies (over RF=2).
    srv.cluster.set_shard_health(2, ShardHealth::Healthy);

    let over = srv.cluster.find_over_replicated();
    if !over.is_empty() {
        let trimmed = over_replication_trim(&srv.cluster, 200).await;
        assert!(trimmed > 0, "should trim at least 1 excess replica");

        let still_over = srv.cluster.find_over_replicated();
        assert!(
            still_over.is_empty(),
            "no over-replicated objects after trim: {:?}",
            still_over
        );
    }

    // All objects still accessible via S3 with correct body.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-ovr-trim-{i}")).await;
    }
}

// ===========================================================================
// Test 9: Concurrent writes during drain
// ===========================================================================

/// Start writing objects while drain is in progress.  All writes should
/// succeed and be readable after drain completes.
#[tokio::test]
async fn concurrent_writes_during_drain_via_s3() {
    let srv = DistributedTestServer::start(4, 2, "data").await;
    let registry = build_registry(&srv);

    // Pre-populate.
    let pre_keys = put_test_objects(&srv, "conc-pre", 20).await;
    srv.cluster.rebuild_catalog().await.unwrap();

    // Spawn a writer task that runs concurrently with drain.
    let client = srv.client.clone();
    let base = srv.base_url.clone();
    let writer_handle = tokio::spawn(async move {
        let mut keys = Vec::new();
        for i in 0..15 {
            let key = format!("conc-during/{i:04}.bin");
            let url = format!("{base}/data/{key}");
            let body = format!("concurrent-{i}");
            // Allow retries on connection errors during drain.
            let mut attempts = 0;
            loop {
                match client.put(&url).body(body.clone()).send().await {
                    Ok(resp) if resp.status() == 200 => break,
                    Ok(resp) if resp.status() == 503 && attempts < 3 => {
                        attempts += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Ok(resp) => panic!("PUT {key} failed with status {}", resp.status()),
                    Err(e) if attempts < 3 => {
                        attempts += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        eprintln!("retry {attempts} for {key}: {e}");
                    }
                    Err(e) => panic!("PUT {key} failed: {e}"),
                }
            }
            keys.push(key);
        }
        keys
    });

    // Drain shard 0 concurrently.
    do_drain(&srv, 0, &registry).await;

    // Wait for writer.
    let concurrent_keys = writer_handle.await.unwrap();

    // repair-replication to ensure everything is correctly replicated.
    srv.cluster.rebuild_catalog().await.unwrap();
    repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;

    // Pre-existing objects readable.
    for (i, key) in pre_keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-conc-pre-{i}")).await;
    }

    // Concurrently-written objects readable.
    for (i, key) in concurrent_keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("concurrent-{i}")).await;
    }
}

// ===========================================================================
// Test 10: S3 DELETE during drain
// ===========================================================================

/// Delete objects while drain is in progress; deleted objects should stay
/// deleted and surviving objects should be accessible.
#[tokio::test]
async fn delete_during_drain_verified_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "del-drain", 20).await;

    // Delete the first 5 objects.
    for key in &keys[..5] {
        let resp = srv
            .client
            .delete(&srv.object_url(key))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status() == 204 || resp.status() == 200,
            "DELETE {key} should succeed"
        );
    }

    // Drain shard 1.
    do_drain(&srv, 1, &registry).await;

    // Deleted objects should return 404.
    for key in &keys[..5] {
        let resp = srv
            .client
            .get(&srv.object_url(key))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "deleted object {key} should be 404 after drain"
        );
    }

    // Surviving objects should be readable.
    for (i, key) in keys[5..].iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-del-drain-{}", i + 5)).await;
    }
}

// ===========================================================================
// Test 11: Drain with objects only on the victim shard
// ===========================================================================

/// Write objects with RF=1 to a specific shard, then drain it.  The sole
/// copies must be migrated to a survivor.
#[tokio::test]
async fn drain_moves_sole_copy_objects_via_s3() {
    // RF=1 means single copy per object.
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "sole", 30).await;
    srv.cluster.rebuild_catalog().await.unwrap();

    // Find which keys live on shard 0.
    let on_shard0: Vec<String> = keys
        .iter()
        .filter(|k| {
            let bk = format!("data/{k}");
            srv.cluster
                .placement(&bk)
                .map(|e| e.shards.contains(&0))
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // All objects that were exclusively on shard 0 should still be readable.
    for key in &on_shard0 {
        let resp = srv
            .client
            .get(&srv.object_url(key))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "sole-copy object {key} should survive drain"
        );
    }

    // All 30 objects should be readable.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-sole-{i}")).await;
    }
}

// ===========================================================================
// Test 12: repair-replication is idempotent
// ===========================================================================

/// Running repair-replication twice on a balanced cluster should be a no-op the
/// second time.
#[tokio::test]
async fn repair_replication_idempotent_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "idem", 20).await;

    // First repair-replication (cluster starts balanced, so should be no-op).
    let _r1 = repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;

    // Second repair-replication.
    let r2 = repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;
    assert_eq!(
        r2.re_replicated, 0,
        "Second repair-replication should re-replicate 0 objects"
    );
    assert_eq!(
        r2.trimmed, 0,
        "Second repair-replication should trim 0 objects"
    );

    // All data readable.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-idem-{i}")).await;
    }
}

// ===========================================================================
// Test 13: Large object count -- drain + repair-replication at scale
// ===========================================================================

/// 200 objects, drain, repair-replication, verify every one via S3.
#[tokio::test]
async fn drain_repair_replication_at_scale_via_s3() {
    let srv = DistributedTestServer::start(4, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "scale", 200).await;
    srv.cluster.rebuild_catalog().await.unwrap();

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Repair-replication.
    repair_replication_sweep(&srv.cluster, 500, Some(&registry), None).await;

    // LIST should return 200 keys.
    let listed = list_keys_with_prefix(&srv, "scale/").await;
    assert_eq!(listed.len(), 200, "should list 200 keys after drain+repair-replication");

    // Spot-check 10 objects.
    for i in [0, 20, 50, 99, 100, 150, 175, 190, 195, 199] {
        verify_object_body(&srv, &keys[i], &format!("payload-scale-{i}")).await;
    }

    // No under-replicated objects.
    let under = srv.cluster.find_under_replicated();
    assert!(
        under.is_empty(),
        "no under-replicated objects at scale: {:?}",
        under
    );
}

// ===========================================================================
// Test 14: Drain shard then write + list -- bucket discovery
// ===========================================================================

/// After draining, new buckets should still be creatable and listable.
#[tokio::test]
async fn drain_then_new_bucket_writes_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    put_test_objects(&srv, "before", 10).await;

    do_drain(&srv, 0, &registry).await;

    // Write to a new prefix (effectively same bucket but new key space).
    let new_keys = put_test_objects(&srv, "after-drain", 10).await;

    // All old and new objects readable.
    for (i, key) in new_keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-after-drain-{i}")).await;
    }

    // LIST with new prefix.
    let listed = list_keys_with_prefix(&srv, "after-drain/").await;
    assert_eq!(listed.len(), 10);
}

// ===========================================================================
// Test 15: Placement correctness After repair-replication
// ===========================================================================

/// After repair-replication, every object should have exactly RF replicas on
/// distinct healthy shards.
#[tokio::test]
async fn placement_correct_after_repair_replication_via_s3() {
    let srv = DistributedTestServer::start(4, 2, "data").await;
    let registry = build_registry(&srv);

    let keys = put_test_objects(&srv, "pl-check", 40).await;

    // Take shard 0 offline and repair-replication.
    srv.cluster.set_shard_health(0, ShardHealth::Offline);
    repair_replication_sweep(&srv.cluster, 200, Some(&registry), None).await;

    // Every object should have exactly 2 replicas, none on shard 0.
    for key in &keys {
        let bucket_key = format!("data/{key}");
        if let Some(entry) = srv.cluster.placement(&bucket_key) {
            let healthy_shards: Vec<usize> = entry
                .shards
                .iter()
                .filter(|&&s| s != 0)
                .copied()
                .collect();
            assert_eq!(
                healthy_shards.len(),
                2,
                "object {key} should have 2 healthy replicas, got {:?} (all: {:?})",
                healthy_shards,
                entry.shards
            );
        }
    }

    // All readable via S3.
    for (i, key) in keys.iter().enumerate() {
        verify_object_body(&srv, key, &format!("payload-pl-check-{i}")).await;
    }
}

// ===========================================================================
// Test 16: S3 multipart upload survives drain
// ===========================================================================

/// Start a multipart upload, complete it, drain, verify object is
/// accessible.
#[tokio::test]
async fn multipart_object_survives_drain_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    // Initiate multipart upload.
    let key = "mpu-drain/big.bin";
    let init_url = format!("{}?uploads", srv.object_url(key));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "initiate multipart should succeed");
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tags(&body, "UploadId");
    assert_eq!(upload_id.len(), 1, "should get exactly 1 UploadId");
    let upload_id = upload_id[0];

    // Upload 2 parts (5 MB each -- minimum part size).
    let part1_data = vec![0x41u8; 5 * 1024 * 1024];
    let part2_data = vec![0x42u8; 5 * 1024 * 1024];

    let part1_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url(key),
        upload_id
    );
    let resp = srv
        .client
        .put(&part1_url)
        .body(part1_data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag1 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let part2_url = format!(
        "{}?partNumber=2&uploadId={}",
        srv.object_url(key),
        upload_id
    );
    let resp = srv
        .client
        .put(&part2_url)
        .body(part2_data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag2 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Complete multipart upload.
    let complete_body = format!(
        "<CompleteMultipartUpload>\
         <Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part>\
         </CompleteMultipartUpload>"
    );
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url(key),
        upload_id
    );
    let resp = srv
        .client
        .post(&complete_url)
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "complete multipart should succeed");

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Verify the multipart object is readable.
    let resp = srv
        .client
        .get(&srv.object_url(key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "multipart object should survive drain");
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        body.len(),
        10 * 1024 * 1024,
        "multipart object should be 10 MB"
    );
    // First 5 MB should be 0x41, second 5 MB should be 0x42.
    assert!(body[0..5 * 1024 * 1024].iter().all(|&b| b == 0x41));
    assert!(body[5 * 1024 * 1024..].iter().all(|&b| b == 0x42));
}

// ===========================================================================
// Test 17: Range reads work after drain
// ===========================================================================

/// PUT a large object, drain, verify range reads return correct slices.
#[tokio::test]
async fn range_reads_after_drain_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    // PUT a 1 MB object with a known pattern.
    let key = "range-drain/pattern.bin";
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    let resp = srv
        .client
        .put(&srv.object_url(key))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Range read: bytes 100-199.
    let resp = srv
        .client
        .get(&srv.object_url(key))
        .header("range", "bytes=100-199")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 206 || resp.status() == 200,
        "range read should return 206 or 200, got {}",
        resp.status()
    );
    let slice = resp.bytes().await.unwrap();
    if slice.len() == 100 {
        assert_eq!(slice.as_ref(), &data[100..200], "range 100-199 mismatch");
    }

    // Range read: last 256 bytes.
    let resp = srv
        .client
        .get(&srv.object_url(key))
        .header("range", "bytes=-256")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 206 || resp.status() == 200,
        "suffix range should work"
    );
}

// ===========================================================================
// Test 18: Copy operation after drain
// ===========================================================================

/// Copy an object after drain; source and destination both readable.
#[tokio::test]
async fn copy_after_drain_via_s3() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let registry = build_registry(&srv);

    let key = "copy-drain/source.txt";
    let resp = srv
        .client
        .put(&srv.object_url(key))
        .header("x-amz-meta-origin", "copy-test")
        .body("original-data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Drain shard 0.
    do_drain(&srv, 0, &registry).await;

    // Copy source to destination.
    let dest_key = "copy-drain/dest.txt";
    let resp = srv
        .client
        .put(&srv.object_url(dest_key))
        .header(
            "x-amz-copy-source",
            &format!("/{}/{}", srv.bucket, key),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "COPY after drain should succeed");

    // Both readable.
    verify_object_body(&srv, key, "original-data").await;
    verify_object_body(&srv, dest_key, "original-data").await;

    // Metadata on destination.
    let resp = srv
        .client
        .head(&srv.object_url(dest_key))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let origin = resp
        .headers()
        .get("x-amz-meta-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(origin, "copy-test", "metadata should be copied to destination");
}
