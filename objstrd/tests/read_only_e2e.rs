//! E2E tests: read-only mode exposed through the S3 protocol.
//!
//! Test 1 -- Single raw store opened read-only behind objstrd:
//!   Write data via writable server, restart on the same image with
//!   open_readonly(), verify GETs still work and PUTs / DELETEs are
//!   rejected.
//!
//! Test 2 -- ShardedObjectStore with read_only=true behind objstrd:
//!   Write data via a writable multi-shard cluster, then rebuild the
//!   cluster with with_read_only(true) (sub-stores stay writable) and
//!   verify the S3 layer rejects all mutations while reads succeed.

mod common;
use common::TestServer;

use std::sync::Arc;

use objstrd::adapter::ObjectStoreS3Adapter;
use rawobjstr::store::RawObjectStore;
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::ShardedObjectStore;

use s3s::service::S3ServiceBuilder;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

// ===================================================================
// Test 1 -- read-only single raw store over S3
// ===================================================================

#[tokio::test]
async fn test_read_only_single_store_s3() {
    // -- Phase 1: writable server, store some objects -----------------
    let mut srv = TestServer::start_with_bucket("data").await;

    let resp = srv
        .client
        .put(&srv.object_url("hello.txt"))
        .body("Hello read-only")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT on writable server should succeed");

    let resp = srv
        .client
        .put(&srv.object_url("dir/nested.bin"))
        .body("nested data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify data is readable
    let resp = srv
        .client
        .get(&srv.object_url("hello.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Hello read-only");

    // Persist bucket list & grab image path, then shut down.
    // Flush index so data survives re-open, then keep temp dir alive.
    srv.save_buckets_json().await;
    if let Some(ref store) = srv.raw_store {
        store.flush_index().expect("flush_index failed");
    }
    let image_path = srv.image_path.clone();
    let _keep_tmp = srv._tmp.take();
    drop(srv);

    // -- Phase 2: restart read-only ----------------------------------
    let ro = TestServer::start_on_image_readonly(&image_path, "data").await;

    // GETs must still work
    let resp = ro
        .client
        .get(&ro.object_url("hello.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET on read-only server should work");
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Hello read-only");

    let resp = ro
        .client
        .get(&ro.object_url("dir/nested.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"nested data");

    // PUT must be rejected
    let resp = ro
        .client
        .put(&ro.object_url("new.txt"))
        .body("should fail")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "PUT on read-only should fail, got {}",
        resp.status()
    );

    // DELETE must be rejected
    let resp = ro
        .client
        .delete(&ro.object_url("hello.txt"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "DELETE on read-only should fail, got {}",
        resp.status()
    );

    // Original data still intact after failed mutations
    let resp = ro
        .client
        .get(&ro.object_url("hello.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Hello read-only");
}

// ===================================================================
// Test 2 -- read-only ShardedObjectStore (cluster) over S3
// ===================================================================

/// Helper: spin up an in-process S3 server over a ShardedObjectStore cluster.
/// Returns (base_url, reqwest::Client, shutdown_tx).
async fn start_sharded_s3(
    cluster: Arc<ShardedObjectStore>,
    raw_refs: Arc<RawRefRegistry>,
    bucket: &str,
) -> (String, reqwest::Client, tokio::sync::oneshot::Sender<()>) {
    let mut adapter =
        ObjectStoreS3Adapter::with_bucket_sharded(cluster, raw_refs, bucket);
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

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    (format!("http://127.0.0.1:{port}"), client, shutdown_tx)
}

#[tokio::test]
async fn test_read_only_distributed_s3() {
    let shard_count = 3usize;
    let replication_factor = 2usize;
    let bucket = "data";

    // -- Phase 1: create images, writable cluster, PUT data ----------
    let tmp = tempfile::tempdir().unwrap();
    let mut raw_stores: Vec<Arc<RawObjectStore>> = Vec::new();
    let mut obj_stores: Vec<Arc<dyn object_store::ObjectStore>> = Vec::new();
    for i in 0..shard_count {
        let path = tmp.path().join(format!("shard-{i}.raw"));
        let store = RawObjectStore::format_with_size(&path, 64 * 1024 * 1024, false)
            .unwrap_or_else(|e| panic!("failed to format shard-{i}: {e}"));
        let arc = Arc::new(store);
        obj_stores.push(arc.clone() as Arc<dyn object_store::ObjectStore>);
        raw_stores.push(arc);
    }

    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, replication_factor));
    let raw_refs_vec: Vec<Option<Arc<RawObjectStore>>> =
        raw_stores.iter().map(|s| Some(s.clone())).collect();
    let kinds = vec![ShardKind::Raw; shard_count];
    let registry = Arc::new(RawRefRegistry::new(raw_refs_vec, kinds));

    let (base_url, client, shutdown_tx) =
        start_sharded_s3(cluster.clone(), registry, bucket).await;

    let obj_url = |key: &str| format!("{base_url}/{bucket}/{key}");

    // PUT several objects
    for i in 0..5 {
        let key = format!("obj-{i}.dat");
        let body = format!("payload-{i}");
        let resp = client
            .put(&obj_url(&key))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "PUT obj-{i} should succeed");
    }

    // Verify reads
    let resp = client.get(&obj_url("obj-0.dat")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"payload-0");

    // Shut down writable S3 server, flush all stores
    drop(shutdown_tx);
    for store in &raw_stores {
        store.flush_index().expect("flush_index failed");
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // -- Phase 2: build read-only cluster from same stores -----------
    // Sub-stores are still writable -- only the ShardedObjectStore blocks writes.
    let obj_stores2: Vec<Arc<dyn object_store::ObjectStore>> =
        raw_stores.iter().map(|s| s.clone() as Arc<dyn object_store::ObjectStore>).collect();
    let cluster_ro =
        Arc::new(ShardedObjectStore::new(obj_stores2, replication_factor).with_read_only(true));
    assert!(cluster_ro.is_read_only());

    let raw_refs_vec2: Vec<Option<Arc<RawObjectStore>>> =
        raw_stores.iter().map(|s| Some(s.clone())).collect();
    let kinds2 = vec![ShardKind::Raw; shard_count];
    let registry2 = Arc::new(RawRefRegistry::new(raw_refs_vec2, kinds2));

    let (base_url_ro, client_ro, _shutdown_tx_ro) =
        start_sharded_s3(cluster_ro, registry2, bucket).await;

    let obj_url_ro = |key: &str| format!("{base_url_ro}/{bucket}/{key}");

    // GETs must still work for all objects
    for i in 0..5 {
        let key = format!("obj-{i}.dat");
        let expected = format!("payload-{i}");
        let resp = client_ro.get(&obj_url_ro(&key)).send().await.unwrap();
        assert_eq!(resp.status(), 200, "GET {key} on read-only cluster should work");
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            expected.as_bytes(),
            "body mismatch for {key}"
        );
    }

    // PUT must be rejected
    let resp = client_ro
        .put(&obj_url_ro("new-obj.dat"))
        .body("should fail")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "PUT on read-only cluster should fail, got {}",
        resp.status()
    );

    // DELETE must be rejected
    let resp = client_ro
        .delete(&obj_url_ro("obj-0.dat"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "DELETE on read-only cluster should fail, got {}",
        resp.status()
    );

    // Data remains intact
    let resp = client_ro
        .get(&obj_url_ro("obj-0.dat"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"payload-0");
}
