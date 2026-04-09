//! E2E tests for the background repair-replication task.
//!
//! Tests verify that objects are replicated from populated shards to empty
//! ones when the cluster has under-replicated objects, and that the
//! `/_admin/repair-replication-status` admin endpoint reports correct progress.
//!
//! Test 10: Fresh raw image + FS-backed S3 endpoint, rf=2.
//! Test 11: Two raw images (stripe) + FS-backed S3 endpoint, rf=2.
//! Test 12: Two FS-backed S3 endpoints, rf=2 (S3-to-S3).
//! Test 13: Admin endpoint reports repair-replication status via subprocess.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::repair::repair_replication_sweep;
use shardedobjstr::ShardedObjectStore;

use objstrd::adapter::ObjectStoreS3Adapter;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spin up an in-process objstrd backed by a single LocalFileSystem,
/// wrapped in ShardedObjectStore(rf=1) exactly as `--backend fs` does.
/// Returns the base URL and a shutdown sender.
async fn start_fs_objstrd(
    fs_dir: &std::path::Path,
    bucket: &str,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let fs_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(fs_dir)
            .expect("failed to create LocalFileSystem for objstrd"),
    );

    let cluster = Arc::new(
        ShardedObjectStore::new_with_offline(vec![Some(fs_store)], 1),
    );
    let registry = Arc::new(RawRefRegistry::new(
        vec![None],
        vec![ShardKind::Sidecar],
    ));

    let mut adapter =
        ObjectStoreS3Adapter::with_bucket_sharded(cluster, registry, bucket);
    adapter.set_allow_anon_list_buckets(true);

    let service = {
        let builder = S3ServiceBuilder::new(adapter);
        builder.build()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let http_server = ConnBuilder::new(TokioExecutor::new());
        tokio::select! {
            _ = async {
                loop {
                    let (socket, _addr) = match listener.accept().await {
                        Ok(conn) => conn,
                        Err(_) => break,
                    };
                    let svc = service.clone();
                    let conn = http_server
                        .serve_connection(TokioIo::new(socket), svc)
                        .into_owned();
                    tokio::spawn(async move { let _ = conn.await; });
                }
            } => {}
            _ = shutdown_rx => {}
        }
    });

    (format!("http://127.0.0.1:{port}"), shutdown_tx)
}

/// Build an AmazonS3 ObjectStore client pointing at the given endpoint.
fn build_s3_client(endpoint: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_skip_signature(true)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("failed to build AmazonS3 client");
    Arc::new(store)
}

/// Upload `count` test objects directly to one shard (via its ObjectStore).
async fn put_objects_to_store(
    store: &dyn ObjectStore,
    count: usize,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("test/rebal_{:04}.bin", i);
        let data = vec![0x41 + (i as u8 % 26); 4096];
        store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(Bytes::from(data)),
            )
            .await
            .unwrap();
        keys.push(key);
    }
    keys
}

/// Verify that all keys can be read from the store.
async fn verify_keys_readable(store: &dyn ObjectStore, keys: &[String]) {
    for key in keys {
        store
            .get(&Path::from(key.as_str()))
            .await
            .unwrap_or_else(|e| panic!("expected key {key} to be readable: {e}"));
    }
}

/// Run repair-replication sweeps until convergence or a maximum number of iterations.
async fn run_repair_replication_to_completion(
    cluster: &ShardedObjectStore,
    batch_size: usize,
    registry: Option<&RawRefRegistry>,
    max_iters: usize,
) -> u64 {
    // First, rebuild the catalog so the cluster discovers all objects.
    cluster.rebuild_catalog().await.expect("catalog rebuild failed");

    let mut total_replicated = 0u64;
    for _ in 0..max_iters {
        let result = repair_replication_sweep(cluster, batch_size, registry, None).await;
        total_replicated += result.re_replicated as u64;
        if result.re_replicated == 0 || result.under_remaining == 0 {
            break;
        }
    }
    total_replicated
}

// =========================================================================
// Test 10: Fresh raw image + FS-backed S3 endpoint, rf=2
// =========================================================================
//
// Scenario: NVMe (raw) starts empty, S3 backend has objects. After
// repair-replication, all objects should be present on both shards.

#[tokio::test]
async fn test_repair_replication_raw_plus_s3_rf2() {
    let tmp = tempfile::tempdir().unwrap();
    let bucket = "rebaldata";

    // Shard 0: empty raw image (simulating fresh NVMe).
    let raw_path = tmp.path().join("shard0.raw");
    let raw_store = Arc::new(
        RawObjectStore::format_with_size(&raw_path, 64 * 1024 * 1024, false)
            .expect("failed to format raw image"),
    );

    // Shard 1: FS-backed S3 endpoint (simulating S3 with existing data).
    let fs_dir = tmp.path().join("s3data");
    std::fs::create_dir_all(&fs_dir).unwrap();
    let (s3_url, _shutdown) = start_fs_objstrd(&fs_dir, bucket).await;
    let s3_store = build_s3_client(&s3_url, bucket);

    // Populate S3 shard with 20 objects directly.
    let keys = put_objects_to_store(s3_store.as_ref(), 20).await;

    // Build the outer ShardedObjectStore with rf=2.
    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raw_store.clone() as Arc<dyn ObjectStore>,
        s3_store.clone(),
    ];
    let cluster = Arc::new(ShardedObjectStore::new(stores, 2));

    let raw_refs = vec![Some(raw_store.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(raw_refs, kinds);

    // Before repair-replication: catalog is empty, raw shard has nothing.
    assert_eq!(cluster.find_under_replicated().len(), 0, "catalog should start empty");

    // Run repair-replication (rebuilds catalog + sweeps).
    let replicated = run_repair_replication_to_completion(&cluster, 100, Some(&registry), 50).await;
    assert!(replicated >= 20, "should have replicated at least 20 objects, got {replicated}");

    // After repair-replication: verify objects exist on raw shard.
    for key in &keys {
        raw_store
            .get(&Path::from(key.as_str()))
            .await
            .unwrap_or_else(|e| panic!("key {key} not on raw shard After repair-replication: {e}"));
    }

    // Also verify through the cluster that nothing is under-replicated.
    let under = cluster.find_under_replicated();
    assert!(under.is_empty(), "still under-replicated After repair-replication: {under:?}");
}

// =========================================================================
// Test 11: Two raw images (stripe-like) + FS-backed S3, rf=2
// =========================================================================
//
// Scenario: Two empty raw shards plus one S3 shard with data. rf=2 means
// each object needs 2 replicas. After repair-replication, every S3 object should be
// on at least one raw shard as well.

#[tokio::test]
async fn test_repair_replication_stripe_plus_s3_rf2() {
    let tmp = tempfile::tempdir().unwrap();
    let bucket = "stripedata";

    // Shard 0+1: two empty raw images (like a local RAID stripe).
    let raw0_path = tmp.path().join("shard0.raw");
    let raw1_path = tmp.path().join("shard1.raw");
    let raw0 = Arc::new(
        RawObjectStore::format_with_size(&raw0_path, 64 * 1024 * 1024, false)
            .expect("failed to format raw0"),
    );
    let raw1 = Arc::new(
        RawObjectStore::format_with_size(&raw1_path, 64 * 1024 * 1024, false)
            .expect("failed to format raw1"),
    );

    // Shard 2: FS-backed S3 endpoint with data.
    let fs_dir = tmp.path().join("s3data");
    std::fs::create_dir_all(&fs_dir).unwrap();
    let (s3_url, _shutdown) = start_fs_objstrd(&fs_dir, bucket).await;
    let s3_store = build_s3_client(&s3_url, bucket);

    // Populate S3 shard with 15 objects.
    let keys = put_objects_to_store(s3_store.as_ref(), 15).await;

    // Build outer cluster: 3 shards, rf=2.
    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raw0.clone() as Arc<dyn ObjectStore>,
        raw1.clone() as Arc<dyn ObjectStore>,
        s3_store.clone(),
    ];
    let cluster = Arc::new(ShardedObjectStore::new(stores, 2));

    let raw_refs = vec![Some(raw0.clone()), Some(raw1.clone()), None];
    let kinds = vec![ShardKind::Raw, ShardKind::Raw, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(raw_refs, kinds);

    // Run repair-replication.
    let replicated = run_repair_replication_to_completion(&cluster, 100, Some(&registry), 50).await;
    assert!(replicated >= 15, "should have replicated at least 15 objects, got {replicated}");

    // Verify every key is on at least one raw shard.
    for key in &keys {
        let path = Path::from(key.as_str());
        let on_raw0 = raw0.get(&path).await.is_ok();
        let on_raw1 = raw1.get(&path).await.is_ok();
        assert!(
            on_raw0 || on_raw1,
            "key {key} not on any raw shard After repair-replication"
        );
    }

    // All should be fully replicated now.
    let under = cluster.find_under_replicated();
    assert!(under.is_empty(), "still under-replicated: {under:?}");
}

// =========================================================================
// Test 12: Two FS-backed S3 endpoints (S3-to-S3 repair-replication), rf=2
// =========================================================================
//
// Scenario: Two S3 endpoints backed by different filesystems. Data on one,
// repair-replication should copy to the other.

#[tokio::test]
async fn test_repair_replication_s3_to_s3_rf2() {
    let tmp = tempfile::tempdir().unwrap();
    let bucket = "s3s3data";

    // Shard 0: FS-backed S3 with data.
    let fs0_dir = tmp.path().join("s3_0");
    std::fs::create_dir_all(&fs0_dir).unwrap();
    let (s3_url_0, _shutdown_0) = start_fs_objstrd(&fs0_dir, bucket).await;
    let s3_store_0 = build_s3_client(&s3_url_0, bucket);

    // Shard 1: FS-backed S3 (empty).
    let fs1_dir = tmp.path().join("s3_1");
    std::fs::create_dir_all(&fs1_dir).unwrap();
    let (s3_url_1, _shutdown_1) = start_fs_objstrd(&fs1_dir, bucket).await;
    let s3_store_1 = build_s3_client(&s3_url_1, bucket);

    // Populate shard 0 with 10 objects.
    let keys = put_objects_to_store(s3_store_0.as_ref(), 10).await;

    // Build outer cluster: 2 S3 shards, rf=2.
    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        s3_store_0.clone(),
        s3_store_1.clone(),
    ];
    let cluster = Arc::new(ShardedObjectStore::new(stores, 2));

    // No raw refs for S3-only cluster.
    let raw_refs: Vec<Option<Arc<RawObjectStore>>> = vec![None, None];
    let kinds = vec![ShardKind::Sidecar, ShardKind::Sidecar];
    let registry = RawRefRegistry::new(raw_refs, kinds);

    // Run repair-replication.
    let replicated = run_repair_replication_to_completion(&cluster, 100, Some(&registry), 50).await;
    assert!(replicated >= 10, "should have replicated at least 10 objects, got {replicated}");

    // Verify objects now exist on shard 1.
    verify_keys_readable(s3_store_1.as_ref(), &keys).await;

    // Cluster should be fully balanced.
    let under = cluster.find_under_replicated();
    assert!(under.is_empty(), "still under-replicated: {under:?}");
}

// =========================================================================
// Test 13: /_admin/repair-replication-status endpoint via subprocess
// =========================================================================

mod subprocess_helpers;
use subprocess_helpers::*;
use std::io::Write;
use std::time::Duration;

#[tokio::test]
async fn test_admin_repair_replication_status_endpoint() {
    let tmp = tempfile::tempdir().unwrap();

    // Create two raw shards.
    let shard0 = tmp.path().join("shard0.raw");
    let shard1 = tmp.path().join("shard1.raw");
    format_raw_image(&shard0, 64);
    format_raw_image(&shard1, 64);

    let port = portpicker::pick_unused_port().unwrap_or(19200);

    // Write config with repair-replication enabled (interval=5s).
    let config = tmp.path().join("test.conf");
    {
        let mut f = std::fs::File::create(&config).unwrap();
        writeln!(f, "cluster  rebal-test").unwrap();
        writeln!(f, "bucket   testbucket").unwrap();
        writeln!(f, "repair_replication_interval  5").unwrap();
        writeln!(f, "repair_replication_batch_size  50").unwrap();
        writeln!(f, "").unwrap();
        writeln!(
            f,
            "node1  rf=2  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
            port, port
        ).unwrap();
        writeln!(f, "  raw  {}", shard0.display()).unwrap();
        writeln!(f, "  raw  {}", shard1.display()).unwrap();
    }

    let srv = start_server(&config, "node1");
    let client = build_client();

    assert!(
        wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // Poll the repair-replication status endpoint.
    let resp = client
        .get(&format!("{}/_admin/repair-replication-status", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["enabled"], true, "repair-replication should be enabled");
    assert_eq!(body["interval_secs"], 5, "interval should match config");
    assert_eq!(body["batch_size"], 50, "batch_size should match config");

    // Phase should be a string (idle, scanning, or replicating).
    let phase = body["phase"].as_str().expect("phase should be a string");
    assert!(
        phase == "idle" || phase == "scanning" || phase == "replicating",
        "unexpected phase: {phase}"
    );

    // Counters should be non-negative integers.
    assert!(body["cycles_completed"].is_number());
    assert!(body["total_objects_replicated"].is_number());
    assert!(body["total_objects_trimmed"].is_number());

    // When disabled, endpoint should still respond.
    drop(srv);

    // Also test that a server without repair-replication returns enabled=false.
    let config2 = tmp.path().join("test2.conf");
    {
        let mut f = std::fs::File::create(&config2).unwrap();
        writeln!(f, "cluster  rebal-test2").unwrap();
        writeln!(f, "bucket   testbucket").unwrap();
        writeln!(f, "").unwrap();
        let port2 = portpicker::pick_unused_port().unwrap_or(19201);
        writeln!(
            f,
            "node1  rf=2  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
            port2, port2
        ).unwrap();
        writeln!(f, "  raw  {}", shard0.display()).unwrap();
        writeln!(f, "  raw  {}", shard1.display()).unwrap();
    }

    let srv2 = start_server(&config2, "node1");
    let client2 = build_client();

    assert!(
        wait_for_server(&srv2.base_url, &client2, Duration::from_secs(10)).await,
        "server2 did not start"
    );

    let resp2 = client2
        .get(&format!("{}/_admin/repair-replication-status", srv2.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);

    let body2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(body2["enabled"], false, "repair-replication should be disabled without config");
}
