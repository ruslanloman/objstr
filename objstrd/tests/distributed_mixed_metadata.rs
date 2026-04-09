//! E2E tests for S3 metadata flowing through a mixed-backend cluster.
//!
//! Topology (RF=3, every object on all 3 shards):
//!   Shard 0: RawObjectStore   (metadata in TLV extent suffix)
//!   Shard 1: LocalFileSystem  (metadata in .__meta__ sidecar)
//!   Shard 2: InMemory         (metadata in .__meta__ sidecar)
//!
//! Tests verify that content-type, user metadata (x-amz-meta-*),
//! and body sizes are correct through the S3 adapter when shards
//! use different metadata strategies.

mod common;

use std::sync::Arc;

use object_store::ObjectStore;
use rawobjstr::event::EventBus;
use rawobjstr::store::RawObjectStore;
#[allow(unused_imports)]
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::ShardedObjectStore;

use objstrd::adapter::ObjectStoreS3Adapter;

use common::extract_xml_tags;

use s3s::service::S3ServiceBuilder;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Helper: start an S3 server backed by a mixed Raw + FS + Mem cluster
// ---------------------------------------------------------------------------

struct MixedServer {
    base_url: String,
    client: reqwest::Client,
    bucket: String,
    cluster: Arc<ShardedObjectStore>,
    _tmp: tempfile::TempDir,
    _shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MixedServer {
    async fn start(bucket: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();

        // Shard 0: Raw block image (64 MB)
        let raw_path = tmp.path().join("shard-0.raw");
        let raw_store = RawObjectStore::format_with_size(&raw_path, 64 * 1024 * 1024, false)
            .expect("failed to format raw shard");
        let raw_arc = Arc::new(raw_store);

        // Shard 1: LocalFileSystem
        let fs_dir = tmp.path().join("shard-1-fs");
        std::fs::create_dir_all(&fs_dir).unwrap();
        let fs_store: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&fs_dir)
                .expect("failed to create LocalFileSystem"),
        );

        // Shard 2: InMemory
        let mem_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        let obj_stores: Vec<Arc<dyn ObjectStore>> = vec![
            raw_arc.clone() as Arc<dyn ObjectStore>,
            fs_store,
            mem_store,
        ];

        let cluster = Arc::new(ShardedObjectStore::new(obj_stores, 3));
        let bus = Arc::new(EventBus::new(512));
        cluster.set_event_bus(Arc::clone(&bus));

        let registry = Arc::new(RawRefRegistry::new(
            vec![Some(raw_arc), None, None],
            vec![ShardKind::Raw, ShardKind::Sidecar, ShardKind::Sidecar],
        ));

        let mut adapter =
            ObjectStoreS3Adapter::with_bucket_sharded(cluster.clone(), registry, bucket);
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

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: bucket.to_string(),
            cluster,
            _tmp: tmp,
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    #[allow(dead_code)]
    fn bucket_url(&self) -> String {
        format!("{}/{}", self.base_url, self.bucket)
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.base_url, self.bucket, key)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

/// PUT with content-type and user metadata, then HEAD and verify all headers
/// survive the mixed-backend round-trip.
#[tokio::test]
async fn mixed_metadata_roundtrip() {
    let srv = MixedServer::start("data").await;

    let body = b"mixed backend metadata test";
    let resp = srv
        .client
        .put(&srv.object_url("meta/round.txt"))
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-amz-meta-author", "test-suite")
        .header("x-amz-meta-version", "42")
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT should succeed");

    // HEAD should return correct content-type and user metadata.
    let resp = srv
        .client
        .head(&srv.object_url("meta/round.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        ct.contains("text/plain"),
        "content-type should be text/plain, got {ct}"
    );
    let cl: usize = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert_eq!(
        cl,
        body.len(),
        "content-length should be body-only ({} bytes), got {cl}",
        body.len()
    );
    let author = resp
        .headers()
        .get("x-amz-meta-author")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(author, "test-suite", "x-amz-meta-author mismatch");
    let version = resp
        .headers()
        .get("x-amz-meta-version")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(version, "42", "x-amz-meta-version mismatch");

    // GET should return the body without metadata trailer leak.
    let resp = srv
        .client
        .get(&srv.object_url("meta/round.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let got = resp.bytes().await.unwrap();
    assert_eq!(
        got.as_ref(),
        body,
        "GET body should match PUT body exactly (no trailer leak)"
    );
}

/// LIST should report body-only sizes for objects with metadata on
/// a mixed-backend cluster.
#[tokio::test]
async fn mixed_list_reports_body_only_size() {
    let srv = MixedServer::start("data").await;

    let body_a = b"short";
    let body_b = b"a slightly longer payload for size testing";

    srv.client
        .put(&srv.object_url("sizes/a.txt"))
        .header("content-type", "text/plain")
        .header("x-amz-meta-tag", "alpha")
        .body(body_a.to_vec())
        .send()
        .await
        .unwrap();

    srv.client
        .put(&srv.object_url("sizes/b.txt"))
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-tag", "beta")
        .body(body_b.to_vec())
        .send()
        .await
        .unwrap();

    let url = format!("{}/data?list-type=2&prefix=sizes/", srv.base_url);
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();

    let sizes: Vec<usize> = extract_xml_tags(&xml, "Size")
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    assert_eq!(sizes.len(), 2, "expected 2 objects in listing");
    assert!(
        sizes.contains(&body_a.len()),
        "listing should include body-a size ({}), got {:?}",
        body_a.len(),
        sizes
    );
    assert!(
        sizes.contains(&body_b.len()),
        "listing should include body-b size ({}), got {:?}",
        body_b.len(),
        sizes
    );
}

/// CopyObject in COPY mode preserves metadata across mixed backends.
#[tokio::test]
async fn mixed_copy_preserves_metadata() {
    let srv = MixedServer::start("data").await;

    // Seed source with metadata.
    srv.client
        .put(&srv.object_url("cp/src.txt"))
        .header("content-type", "text/csv")
        .header("x-amz-meta-origin", "upload")
        .body("source,data")
        .send()
        .await
        .unwrap();

    // Copy (COPY mode -- default, no x-amz-metadata-directive).
    let resp = srv
        .client
        .put(&srv.object_url("cp/dst.txt"))
        .header("x-amz-copy-source", "/data/cp/src.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "CopyObject should succeed");

    // HEAD the destination -- metadata should be preserved.
    let resp = srv
        .client
        .head(&srv.object_url("cp/dst.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        ct.contains("text/csv"),
        "copied content-type should be text/csv, got {ct}"
    );
    let origin = resp
        .headers()
        .get("x-amz-meta-origin")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(origin, "upload", "x-amz-meta-origin should survive copy");

    // GET the destination body.
    let resp = srv
        .client
        .get(&srv.object_url("cp/dst.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"source,data");
}

/// CopyObject in REPLACE mode uses caller-supplied metadata.
#[tokio::test]
async fn mixed_copy_replace_metadata() {
    let srv = MixedServer::start("data").await;

    // Seed source.
    srv.client
        .put(&srv.object_url("cprep/src.txt"))
        .header("content-type", "text/plain")
        .header("x-amz-meta-old", "original")
        .body("replace me")
        .send()
        .await
        .unwrap();

    // Copy with REPLACE directive and new metadata.
    let resp = srv
        .client
        .put(&srv.object_url("cprep/dst.txt"))
        .header("x-amz-copy-source", "/data/cprep/src.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "application/json")
        .header("x-amz-meta-new", "replaced")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // HEAD -- should have the NEW metadata, not the old.
    let resp = srv
        .client
        .head(&srv.object_url("cprep/dst.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        ct.contains("application/json"),
        "replaced content-type should be application/json, got {ct}"
    );
    let new_meta = resp
        .headers()
        .get("x-amz-meta-new")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(new_meta, "replaced");
    // Old metadata should be absent.
    assert!(
        resp.headers().get("x-amz-meta-old").is_none(),
        "old metadata should not survive REPLACE copy"
    );
}

/// Range reads on a mixed-backend cluster return correct slices
/// without metadata trailer contamination.
#[tokio::test]
async fn mixed_range_reads_exclude_metadata() {
    let srv = MixedServer::start("data").await;

    let body = b"0123456789abcdef";
    srv.client
        .put(&srv.object_url("range/data.bin"))
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-info", "range-test")
        .body(body.to_vec())
        .send()
        .await
        .unwrap();

    // bytes=0-3 should return "0123"
    let resp = srv
        .client
        .get(&srv.object_url("range/data.bin"))
        .header("range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"0123");

    // bytes=10-15 should return "abcdef"
    let resp = srv
        .client
        .get(&srv.object_url("range/data.bin"))
        .header("range", "bytes=10-15")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"abcdef");

    // bytes=-4 (suffix) should return "cdef"
    let resp = srv
        .client
        .get(&srv.object_url("range/data.bin"))
        .header("range", "bytes=-4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"cdef");
}

/// HEAD and GET report consistent body-only sizes for mixed backends.
#[tokio::test]
async fn mixed_head_get_size_consistency() {
    let srv = MixedServer::start("data").await;

    let body = vec![0xABu8; 500];
    srv.client
        .put(&srv.object_url("consist/obj.bin"))
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-purpose", "consistency check")
        .body(body.clone())
        .send()
        .await
        .unwrap();

    // HEAD
    let resp = srv
        .client
        .head(&srv.object_url("consist/obj.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let head_cl: usize = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert_eq!(head_cl, 500, "HEAD content-length should be 500");

    // GET
    let resp = srv
        .client
        .get(&srv.object_url("consist/obj.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.len(), 500, "GET body should be 500 bytes");
    assert_eq!(got.as_ref(), body.as_slice());
}
