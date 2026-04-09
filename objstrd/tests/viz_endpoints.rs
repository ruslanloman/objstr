//! Tests for the `/_admin/` visualization endpoints.
//!
//! These tests spin up an in-process server using the real `VizService`
//! from the library (not a stub), backed by a single raw shard.

use std::collections::HashSet;
use std::sync::Arc;

use objstrd::adapter::ObjectStoreS3Adapter;
use objstrd::logging::LogBuffer;
use objstrd::viz::VizService;
use rawobjstr::store::RawObjectStore;
use shardedobjstr::metadata::ShardKind;
use object_store::ObjectStore;
use s3s::service::S3ServiceBuilder;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// VizTestServer helper -- uses real VizService
// ---------------------------------------------------------------------------

struct VizTestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    pub _raw_store: Arc<RawObjectStore>,
    _tmp: tempfile::TempDir,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl VizTestServer {
    async fn start() -> Self {
        Self::start_with_options(None).await
    }

    async fn start_with_token(token: &str) -> Self {
        Self::start_with_options(Some(token.to_string())).await
    }

    async fn start_with_options(admin_token: Option<String>) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let store = RawObjectStore::format_with_size(&image_path, 64 * 1024 * 1024, false)
            .expect("failed to format test image");
        let store_arc = Arc::new(store);

        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), "data");
        adapter.set_allow_anon_list_buckets(true);

        let s3_service = S3ServiceBuilder::new(adapter).build();

        let obj_store: Arc<dyn ObjectStore> = Arc::clone(&store_arc) as Arc<dyn ObjectStore>;

        let service = VizService {
            s3: s3_service,
            raw_stores: vec![Arc::clone(&store_arc)],
            obj_stores: vec![obj_store],
            shard_names: vec!["shard0".to_string()],
            shard_kinds: vec![ShardKind::Raw],
            raw_index_map: vec![Some(0)],
            cluster_mode: false,
            replication_factor: 1,
            bucket_registry: Arc::new(tokio::sync::RwLock::new({
                let mut set = HashSet::new();
                set.insert("data".to_string());
                set
            })),
            bucket: "data".to_string(),
            port: 0,
            bind: "127.0.0.1".to_string(),
            read_only: false,
            process_start_time: std::time::Instant::now(),
            config_loaded_time: std::time::Instant::now(),
            admin_token,
            cors_origin: None,
            role: "standalone".to_string(),
            log_buffer: LogBuffer::new(100, None),
            reload_signal: Arc::new(tokio::sync::Notify::new()),
            cluster: None,
            recovery_status: None,
            repair_replication_status: None,
            cli_command: "test".to_string(),
            config_content: None,
            stats_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            event_socket_path: None,
            event_socket_connections: None,
            event_source: None,
            node_name: "test-node".to_string(),
            original_stores: vec![],
            raw_device_paths: vec![],
            shard_endpoints: vec![],
            admin_op_lock: Arc::new(tokio::sync::Mutex::new(None)),
            op_log_tx: tokio::sync::broadcast::channel(64).0,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let mut service = service;
        service.port = port;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let http_server = ConnBuilder::new(TokioExecutor::new());
            tokio::select! {
                _ = async {
                    loop {
                        let (socket, _) = match listener.accept().await {
                            Ok(c) => c,
                            Err(_) => break,
                        };
                        let svc = service.clone();
                        let conn = http_server.serve_connection(TokioIo::new(socket), svc).into_owned();
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
            _raw_store: store_arc,
            _tmp: tmp,
            _shutdown_tx: shutdown_tx,
        }
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/data/{key}", self.base_url)
    }
}

// ---------------------------------------------------------------------------
// Admin JSON API endpoint tests
// ---------------------------------------------------------------------------

/// `/_admin/info` should return server metadata as JSON.
#[tokio::test]
async fn test_admin_info_fields() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/info", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(body["pid"].is_number(), "should have pid: {body}");
    assert!(body["build_git_hash"].is_string(), "should have build_git_hash: {body}");
    assert!(body["build_date"].is_string(), "should have build_date: {body}");
    assert_eq!(body["role"].as_str().unwrap(), "standalone");
    assert_eq!(body["node_name"].as_str().unwrap(), "test-node");
    assert!(body["port"].is_number(), "should have port: {body}");
    assert!(body["backend"].is_string(), "should have backend: {body}");
    assert!(body["file_count"].is_number(), "should have file_count: {body}");
    assert!(body["device_size"].is_number(), "should have device_size: {body}");
    assert!(
        body["device_size"].as_u64().unwrap() >= 64 * 1024 * 1024,
        "device_size should be >= 64 MiB"
    );
}

/// `/_admin/shards` should return shard info with a shards array.
#[tokio::test]
async fn test_admin_shards_single() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/shards", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["shard_count"].as_u64().unwrap(), 1);
    assert_eq!(body["mode"].as_str().unwrap(), "single");
    assert_eq!(body["replication_factor"].as_u64().unwrap(), 1);

    let shards = body["shards"].as_array().expect("should have shards array");
    assert_eq!(shards.len(), 1, "should have 1 shard");

    let s = &shards[0];
    assert_eq!(s["id"].as_u64().unwrap(), 0);
    assert!(s["health"].is_string(), "should have health: {s}");
    assert!(s["type"].is_string(), "should have type: {s}");
    assert!(s["file_count"].is_number(), "should have file_count: {s}");
}

/// `/_admin/objects` on an empty store should return total=0.
#[tokio::test]
async fn test_admin_objects_empty() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/objects", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["total"].as_u64().unwrap(), 0);
    assert!(body["objects"].as_array().unwrap().is_empty());
}

/// After putting objects via S3, `/_admin/objects` should list them.
#[tokio::test]
async fn test_admin_objects_with_data() {
    let srv = VizTestServer::start().await;

    for key in &["alpha.txt", "beta.txt", "gamma.txt"] {
        let resp = srv.client.put(srv.object_url(key)).body("data").send().await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    let resp = srv.client.get(format!("{}/_admin/objects", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["total"].as_u64().unwrap(), 3);
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 3);
    for obj in objects {
        assert!(obj["key"].is_string(), "object should have key: {obj}");
        assert!(obj["size"].is_number(), "object should have size: {obj}");
    }
}

/// `/_admin/objects` pagination with limit and offset.
#[tokio::test]
async fn test_admin_objects_pagination() {
    let srv = VizTestServer::start().await;

    for i in 0..5 {
        srv.client.put(srv.object_url(&format!("pg-{i}.txt"))).body("x")
            .send().await.unwrap();
    }

    // limit=2
    let resp = srv.client.get(format!("{}/_admin/objects?limit=2", srv.base_url))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["objects"].as_array().unwrap().len(), 2);
    assert_eq!(body["total"].as_u64().unwrap(), 5);

    // offset=3, limit=10
    let resp = srv.client.get(format!("{}/_admin/objects?limit=10&offset=3", srv.base_url))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["objects"].as_array().unwrap().len(), 2);
}

/// `/_admin/sysinfo` should return system stats.
#[tokio::test]
async fn test_admin_sysinfo_fields() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/sysinfo", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(body["process_uptime_secs"].is_number(), "should have process_uptime_secs: {body}");
}

/// `/_admin/buckets` should include the default bucket.
#[tokio::test]
async fn test_admin_buckets_list() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/buckets", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    let buckets = body["buckets"].as_array().expect("buckets should be array");
    let names: Vec<&str> = buckets.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"data"), "should contain default bucket 'data': {:?}", names);
}

/// `/_admin/nodeconfig` should return server configuration fields.
#[tokio::test]
async fn test_admin_nodeconfig_fields() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/nodeconfig", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["shard_count"].as_u64().unwrap(), 1);
    assert!(body["bucket"].is_string(), "should have bucket: {body}");
    assert!(body["shards"].is_array(), "should have shards array: {body}");
    let shards = body["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1);
}

/// `/_admin/heatmap` should return chunk density data for a raw shard.
#[tokio::test]
async fn test_admin_heatmap_fields() {
    let srv = VizTestServer::start().await;

    for i in 0..3 {
        srv.client.put(srv.object_url(&format!("hm-{i}.txt"))).body(format!("data-{i}"))
            .send().await.unwrap();
    }

    let resp = srv.client.get(format!("{}/_admin/heatmap", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(body["device_size"].is_number(), "should have device_size: {body}");
    assert!(body["chunk_size"].is_number(), "should have chunk_size: {body}");
    assert!(body["file_counts"].is_array(), "should have file_counts: {body}");
    assert!(body["used_bytes"].is_array(), "should have used_bytes: {body}");
    assert!(body["free_bytes"].is_array(), "should have free_bytes: {body}");

    let fc = body["file_counts"].as_array().unwrap();
    assert_eq!(fc.len(), 256, "default chunk count should be 256");

    // Custom chunk count
    let resp = srv.client.get(format!("{}/_admin/heatmap?chunks=32", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["file_counts"].as_array().unwrap().len(), 32);
}

/// `/_admin/shard/0/info` should return device stats for a raw shard.
#[tokio::test]
async fn test_admin_shard_info() {
    let srv = VizTestServer::start().await;
    srv.client.put(srv.object_url("info-test.txt")).body("hello")
        .send().await.unwrap();

    let resp = srv.client.get(format!("{}/_admin/shard/0/info", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["shard_id"].as_u64().unwrap(), 0);
    assert!(body["device_size"].is_number(), "should have device_size: {body}");
    assert!(body["device_size"].as_u64().unwrap() > 0);
    assert!(body["file_count"].is_number(), "should have file_count: {body}");
    assert!(body["file_count"].as_u64().unwrap() >= 1);
}

/// `/_admin/shard/0/objects` should return a paginated object listing.
#[tokio::test]
async fn test_admin_shard_objects() {
    let srv = VizTestServer::start().await;

    for i in 0..4 {
        srv.client.put(srv.object_url(&format!("so-{i}.txt"))).body("x")
            .send().await.unwrap();
    }

    let resp = srv.client.get(format!("{}/_admin/shard/0/objects", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(body["objects"].is_array(), "should have objects: {body}");
    let objects = body["objects"].as_array().unwrap();
    assert!(objects.len() >= 4, "should have at least 4 objects, got {}", objects.len());
    assert!(body["total"].is_number(), "should have total: {body}");

    // Pagination: limit=2
    let resp = srv.client.get(format!("{}/_admin/shard/0/objects?limit=2", srv.base_url))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["objects"].as_array().unwrap().len(), 2);
}

/// `/_admin/shard/99/info` should return 404 for non-existent shard.
#[tokio::test]
async fn test_admin_shard_invalid_returns_404() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/shard/99/info", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

/// `/_admin/shard/abc/info` should return 404 for non-numeric shard ID.
#[tokio::test]
async fn test_admin_shard_nonnumeric_returns_404() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/shard/abc/info", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

/// `POST /_admin/flush` should succeed with a raw backend.
#[tokio::test]
async fn test_admin_flush_post() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.post(format!("{}/_admin/flush", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"].as_bool().unwrap(), true);
}

/// `POST /_admin/rebuild-index` should succeed with a raw backend.
#[tokio::test]
async fn test_admin_rebuild_index_post() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.post(format!("{}/_admin/rebuild-index", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"].as_bool().unwrap(), true);
}

/// `POST /_admin/repair-replication` should return 503 in standalone (non-cluster) mode.
#[tokio::test]
async fn test_admin_repair_replication_standalone_503() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.post(format!("{}/_admin/repair-replication", srv.base_url))
        .send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(), 503,
        "repair-replication should return 503 in standalone mode"
    );
}

/// `POST /_admin/drain/0` should return 503 in standalone (non-cluster) mode.
#[tokio::test]
async fn test_admin_drain_standalone_503() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.post(format!("{}/_admin/drain/0", srv.base_url))
        .send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(), 503,
        "drain should return 503 in standalone mode"
    );
}

/// `/_admin/region` should return extents and free regions.
#[tokio::test]
async fn test_admin_region() {
    let srv = VizTestServer::start().await;
    srv.client.put(srv.object_url("region-test.txt")).body("region data")
        .send().await.unwrap();

    let resp = srv.client.get(format!(
        "{}/_admin/region?from=0&to={}", srv.base_url, 64 * 1024 * 1024
    )).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(body["from"].is_number(), "should have from: {body}");
    assert!(body["to"].is_number(), "should have to: {body}");
    assert!(body["extents"].is_array(), "should have extents: {body}");
    assert!(body["free_regions"].is_array(), "should have free_regions: {body}");
    let extents = body["extents"].as_array().unwrap();
    assert!(!extents.is_empty(), "should have at least 1 extent");
}

/// `/_admin/extent_meta` should return decoded metadata for a key.
#[tokio::test]
async fn test_admin_extent_meta() {
    let srv = VizTestServer::start().await;
    srv.client.put(srv.object_url("meta-test.txt"))
        .header("content-type", "text/plain")
        .body("metadata obj")
        .send().await.unwrap();

    let resp = srv.client.get(format!(
        "{}/_admin/extent_meta?key=data/meta-test.txt", srv.base_url
    )).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

/// `/_admin/getraw` should return raw bytes for a key.
#[tokio::test]
async fn test_admin_getraw() {
    let srv = VizTestServer::start().await;
    let payload = "raw bytes content";
    srv.client.put(srv.object_url("raw-test.txt")).body(payload)
        .send().await.unwrap();

    let resp = srv.client.get(format!(
        "{}/_admin/getraw?key=data/raw-test.txt", srv.base_url
    )).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains(payload),
        "getraw should contain the object body: got {} bytes",
        body.len()
    );
}

/// `/_admin/logs` should return structured log entries.
#[tokio::test]
async fn test_admin_logs() {
    let srv = VizTestServer::start().await;

    for i in 0..3 {
        srv.client.put(srv.object_url(&format!("log-{i}.txt"))).body("x")
            .send().await.unwrap();
    }

    let resp = srv.client.get(format!("{}/_admin/logs", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["total"].is_number(), "should have total: {body}");
    assert!(body["entries"].is_array(), "should have entries: {body}");

    // With limit
    let resp = srv.client.get(format!("{}/_admin/logs?limit=1", srv.base_url))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let returned = body["returned"].as_u64().unwrap_or(0);
    assert!(returned <= 1, "limit=1 should return at most 1 entry, got {returned}");
}

/// `/_admin/recovery` should return recovery status (None in standalone).
#[tokio::test]
async fn test_admin_recovery_standalone() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/recovery", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["enabled"].is_boolean() || body["message"].is_string(),
        "Should have either enabled field or message: {body}");
}

/// Unknown `/_admin/*` path should return 404.
#[tokio::test]
async fn test_admin_unknown_path_404() {
    let srv = VizTestServer::start().await;
    let resp = srv.client.get(format!("{}/_admin/nonexistent", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

// ---------------------------------------------------------------------------
// Auth tests
// ---------------------------------------------------------------------------

/// With admin_token set, requests without token should get 403.
#[tokio::test]
async fn test_admin_token_auth_forbidden() {
    let srv = VizTestServer::start_with_token("secret-token-123").await;

    let resp = srv.client.get(format!("{}/_admin/info", srv.base_url))
        .send().await.unwrap();
    assert_eq!(resp.status(), 403, "no token should return 403");
}

/// With admin_token set, requests with correct token should succeed.
#[tokio::test]
async fn test_admin_token_auth_success() {
    let srv = VizTestServer::start_with_token("secret-token-123").await;

    let resp = srv.client.get(format!("{}/_admin/info", srv.base_url))
        .header("authorization", "Bearer secret-token-123")
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "correct token should return 200");
}

/// With admin_token set, S3 operations should still work without a token.
#[tokio::test]
async fn test_admin_token_does_not_affect_s3() {
    let srv = VizTestServer::start_with_token("secret-token-123").await;

    let resp = srv.client.put(srv.object_url("auth-test.txt")).body("works")
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "S3 PUT should work without admin token");

    let resp = srv.client.get(srv.object_url("auth-test.txt"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "works");
}

// ---------------------------------------------------------------------------
// S3 passthrough
// ---------------------------------------------------------------------------

/// S3 operations still work normally when VizService is wrapping S3Service.
#[tokio::test]
async fn test_s3_still_works_through_viz_wrapper() {
    let srv = VizTestServer::start().await;

    // PUT
    let resp = srv.client.put(srv.object_url("hello.txt")).body("world").send().await.unwrap();
    assert_eq!(resp.status(), 200, "PUT should succeed");

    // GET
    let resp = srv.client.get(srv.object_url("hello.txt")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "world");

    // DELETE
    let resp = srv.client.delete(srv.object_url("hello.txt")).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // GET after delete should be 404
    let resp = srv.client.get(srv.object_url("hello.txt")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

// ---------------------------------------------------------------------------
// HTML page endpoints
// ---------------------------------------------------------------------------

/// HTML page endpoints should return 200 with text/html content type.
#[tokio::test]
async fn test_html_pages_return_200() {
    let srv = VizTestServer::start().await;

    let html_paths = [
        "/_admin/",
        "/_admin/viz",
        "/_admin/ui",
        "/_admin/server",
        "/_admin/cluster",
        "/_admin/config",
        "/_admin/logs/ui",
    ];

    for path in &html_paths {
        let resp = srv.client.get(format!("{}{}", srv.base_url, path))
            .send().await.unwrap();
        assert_eq!(resp.status(), 200, "expected 200 for {path}");
        let ct = resp.headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap_or(""))
            .unwrap_or("");
        assert!(ct.contains("text/html"), "expected text/html for {path}, got {ct}");
    }
}

/// Per-shard HTML pages should return 200 for shard 0.
#[tokio::test]
async fn test_per_shard_html_pages() {
    let srv = VizTestServer::start().await;

    for path in &["/_admin/shard/0/viz", "/_admin/shard/0/ui", "/_admin/shard/0/server"] {
        let resp = srv.client.get(format!("{}{}", srv.base_url, path))
            .send().await.unwrap();
        assert_eq!(resp.status(), 200, "expected 200 for {path}");
    }
}
