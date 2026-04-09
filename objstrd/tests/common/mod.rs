//! Test helpers -- in-process s3s server backed by RawObjectStore.
//!
//! Each integration test file compiles this module independently, so not
//! every symbol is used in every test binary.  Suppress the resulting
//! dead-code warnings at the module level.
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use rawobjstr::event::{EventBus, StoreEvent};
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::Compression;
use objstrd::adapter::ObjectStoreS3Adapter;
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::ShardedObjectStore;

use s3s::service::S3ServiceBuilder;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

/// Bind a random port, spawn an HTTP accept loop, and return (port, shutdown_tx).
async fn spawn_http_server(
    service: s3s::service::S3Service,
) -> (u16, tokio::sync::oneshot::Sender<()>) {
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

    (port, shutdown_tx)
}

/// In-process S3 test server backed by a RawObjectStore temp image.
pub struct TestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    pub bucket: String,
    pub image_path: PathBuf,
    pub buckets_json_path: PathBuf,
    pub bucket_registry: Arc<RwLock<HashSet<String>>>,
    pub raw_store: Option<Arc<RawObjectStore>>,
    pub _tmp: Option<tempfile::TempDir>,
    /// Sender dropped to signal the accept loop to stop.
    _shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TestServer {
    /// Start a test server with the default bucket name "data".
    pub async fn start() -> Self {
        Self::start_with_bucket("data").await
    }

    /// Start a test server with a custom bucket name.
    pub async fn start_with_bucket(bucket: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let store = RawObjectStore::format_with_size(&image_path, 64 * 1024 * 1024, false)
            .expect("failed to format test image");
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), bucket);
        adapter.set_allow_anon_list_buckets(true);
        let bucket_registry = adapter.bucket_registry();
        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: bucket.to_string(),
            image_path,
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: Some(tmp),
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a test server backed by a freshly-formatted store of `size_gb` gigabytes.
    ///
    /// The image is created under a temporary directory that is owned by the
    /// returned `TestServer`; it is deleted when the server is dropped.
    /// Intended for large-object / memory-pressure tests that need a store
    /// big enough to hold multi-GB objects.
    pub async fn start_with_size_gb(size_gb: u64, bucket: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let size_bytes = size_gb * 1024 * 1024 * 1024;
        let store = RawObjectStore::format_with_size(&image_path, size_bytes, false)
            .expect("failed to format large test image");
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), bucket);
        adapter.set_allow_anon_list_buckets(true);
        let bucket_registry = adapter.bucket_registry();
        let buckets_json_path =
            PathBuf::from(format!("{}.buckets.json", image_path.display()));

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        // No timeout: large uploads can take minutes.
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(3600))
            .build()
            .unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: bucket.to_string(),
            image_path,
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: Some(tmp),
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a test server with a custom upload expiry duration.
    /// Useful for testing the lazy multipart-upload reaper.
    pub async fn start_with_upload_expiry(expiry: std::time::Duration) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let store = RawObjectStore::format_with_size(&image_path, 64 * 1024 * 1024, false)
            .expect("failed to format test image");
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), "data");
        adapter.set_allow_anon_list_buckets(true);
        adapter.set_upload_expiry(expiry);
        let bucket_registry = adapter.bucket_registry();
        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: "data".to_string(),
            image_path,
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: Some(tmp),
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a test server reusing an existing image file (simulates restart).
    /// The temp dir is NOT owned -- caller must keep it alive.
    ///
    /// Retries opening the store for up to ~1 second so that a previous server
    /// instance (dropped just before this call) has time to release its file
    /// lock after its tokio task is scheduled out.
    pub async fn start_on_image(image_path: &PathBuf, bucket: &str) -> Self {
        let store = {
            let mut last_err = None;
            let mut opened = None;
            for _ in 0..20 {
                match RawObjectStore::open(image_path) {
                    Ok(s) => { opened = Some(s); break; }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
            opened.unwrap_or_else(|| panic!("failed to open existing test image: {:?}", last_err))
        };
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), bucket);
        adapter.set_allow_anon_list_buckets(true);

        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        // If buckets JSON exists, load it and merge with the initial bucket
        if let Ok(data) = std::fs::read(&buckets_json_path) {
            if let Ok(list) = serde_json::from_slice::<Vec<String>>(&data) {
                let mut set: HashSet<String> = list.into_iter().collect();
                set.insert(bucket.to_string());
                adapter.set_buckets(set).await;
            }
        } else {
            // Rebuild from index
            let mut discovered = adapter.rebuild_buckets_from_index().await;
            discovered.insert(bucket.to_string());
            adapter.set_buckets(discovered).await;
        }

        let bucket_registry = adapter.bucket_registry();

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: bucket.to_string(),
            image_path: image_path.clone(),
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: None,
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a test server reusing an existing image opened **read-only**.
    /// The underlying `RawObjectStore` is opened with `open_readonly`, so all
    /// write attempts through the store will fail.  The temp dir is NOT owned.
    pub async fn start_on_image_readonly(image_path: &PathBuf, bucket: &str) -> Self {
        let store = {
            let mut last_err = None;
            let mut opened = None;
            for _ in 0..20 {
                match RawObjectStore::open_readonly(image_path) {
                    Ok(s) => { opened = Some(s); break; }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
            opened.unwrap_or_else(|| panic!("failed to open_readonly test image: {:?}", last_err))
        };
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), bucket);
        adapter.set_allow_anon_list_buckets(true);

        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        if let Ok(data) = std::fs::read(&buckets_json_path) {
            if let Ok(list) = serde_json::from_slice::<Vec<String>>(&data) {
                let mut set: HashSet<String> = list.into_iter().collect();
                set.insert(bucket.to_string());
                adapter.set_buckets(set).await;
            }
        } else {
            let mut discovered = adapter.rebuild_buckets_from_index().await;
            discovered.insert(bucket.to_string());
            adapter.set_buckets(discovered).await;
        }

        let bucket_registry = adapter.bucket_registry();

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: bucket.to_string(),
            image_path: image_path.clone(),
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: None,
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Save the current bucket registry to the buckets JSON file.
    pub async fn save_buckets_json(&self) {
        let buckets = self.bucket_registry.read().await;
        let mut sorted: Vec<&String> = buckets.iter().collect();
        sorted.sort();
        let json = serde_json::to_string_pretty(&sorted).unwrap();
        let tmp = self.buckets_json_path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes()).unwrap();
        std::fs::rename(&tmp, &self.buckets_json_path).unwrap();
    }

    /// URL for the bucket root, e.g. `http://127.0.0.1:PORT/data`.
    pub fn bucket_url(&self) -> String {
        format!("{}/{}", self.base_url, self.bucket)
    }

    /// URL for an object key, e.g. `http://127.0.0.1:PORT/data/foo.txt`.
    pub fn object_url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.base_url, self.bucket, key)
    }

    /// Start a test server with an event bus attached.
    /// Returns `(TestServer, broadcast::Receiver<StoreEvent>)`.
    pub async fn start_with_event_bus() -> (Self, tokio::sync::broadcast::Receiver<StoreEvent>) {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let store = RawObjectStore::format_with_size(&image_path, 64 * 1024 * 1024, false)
            .expect("failed to format test image");
        let store_arc = Arc::new(store);
        let bus = Arc::new(EventBus::new(512));
        let rx = bus.subscribe();
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), "data");
        adapter.set_allow_anon_list_buckets(true);
        adapter.set_event_bus(Arc::clone(&bus));
        let bucket_registry = adapter.bucket_registry();
        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let srv = Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: "data".to_string(),
            image_path,
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: Some(tmp),
            _shutdown_tx: Some(shutdown_tx),
        };
        (srv, rx)
    }

    /// Start a test server backed by a store formatted with compression.
    pub async fn start_with_compression(compression: Compression) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let store = RawObjectStore::format_with_options(
            &image_path,
            FormatOptions {
                device_size: 64 * 1024 * 1024,
                direct_io: false,
                index_slot_size: rawobjstr::INDEX_REGION_SIZE,
                max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                compression,
            },
        )
        .expect("failed to format compressed test image");
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), "data");
        adapter.set_allow_anon_list_buckets(true);
        let bucket_registry = adapter.bucket_registry();
        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: "data".to_string(),
            image_path,
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: Some(tmp),
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a test server backed by a very small store (for capacity tests).
    pub async fn start_with_size_mb(size_mb: u64) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let image_path = tmp.path().join("test.raw");
        let size_bytes = size_mb * 1024 * 1024;
        let store = RawObjectStore::format_with_size(&image_path, size_bytes, false)
            .expect("failed to format small test image");
        let store_arc = Arc::new(store);
        let mut adapter = ObjectStoreS3Adapter::with_bucket(Arc::clone(&store_arc), "data");
        adapter.set_allow_anon_list_buckets(true);
        let bucket_registry = adapter.bucket_registry();
        let buckets_json_path = PathBuf::from(format!("{}.buckets.json", image_path.display()));

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: "data".to_string(),
            image_path,
            buckets_json_path,
            bucket_registry,
            raw_store: Some(store_arc),
            _tmp: Some(tmp),
            _shutdown_tx: Some(shutdown_tx),
        }
    }
}

/// Extract the text content of a simple XML tag, e.g. `<Tag>value</Tag>`.
/// Returns `None` if the tag is not found.
pub fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

/// Build the CompleteMultipartUpload XML body from (part_number, etag) pairs.
pub fn complete_xml(parts: &[(u32, &str)]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (num, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            num, etag
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

/// Extract ALL occurrences of a simple XML tag.
pub fn extract_xml_tags<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut results = Vec::new();
    let mut search_from = 0;
    while let Some(start_pos) = xml[search_from..].find(&open) {
        let abs_start = search_from + start_pos + open.len();
        if let Some(end_pos) = xml[abs_start..].find(&close) {
            results.push(&xml[abs_start..abs_start + end_pos]);
            search_from = abs_start + end_pos + close.len();
        } else {
            break;
        }
    }
    results
}

// ===========================================================================
//  DistributedTestServer -- S3 over multiple raw stores (ShardedObjectStore)
// ===========================================================================

/// In-process S3 test server backed by a `ShardedObjectStore` wrapping
/// multiple `RawObjectStore` images.
pub struct DistributedTestServer {
    pub base_url: String,
    pub client: reqwest::Client,
    pub bucket: String,
    pub cluster: Arc<ShardedObjectStore>,
    pub raw_stores: Vec<Arc<RawObjectStore>>,
    pub event_bus: Arc<EventBus>,
    pub _tmp: tempfile::TempDir,
    _shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DistributedTestServer {
    /// Create `shard_count` raw image files (64 MB each) inside a temp dir,
    /// build a `ShardedObjectStore` with the given `replication_factor`, and
    /// start an S3 server on a random port.
    pub async fn start(
        shard_count: usize,
        replication_factor: usize,
        bucket: &str,
    ) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut raw_stores = Vec::with_capacity(shard_count);
        let mut raw_refs = Vec::with_capacity(shard_count);
        let mut obj_stores: Vec<Arc<dyn object_store::ObjectStore>> = Vec::with_capacity(shard_count);
        for i in 0..shard_count {
            let path = tmp.path().join(format!("shard-{i}.raw"));
            let store = RawObjectStore::format_with_size(&path, 64 * 1024 * 1024, false)
                .unwrap_or_else(|e| panic!("failed to format shard-{i}: {e}"));
            let arc = Arc::new(store);
            obj_stores.push(arc.clone() as Arc<dyn object_store::ObjectStore>);
            raw_refs.push(Some(arc.clone()));
            raw_stores.push(arc);
        }
        let cluster = Arc::new(ShardedObjectStore::new(obj_stores, replication_factor));
        let bus = Arc::new(EventBus::new(512));
        cluster.set_event_bus(Arc::clone(&bus));
        let kinds = vec![ShardKind::Raw; raw_refs.len()];
        let registry = Arc::new(RawRefRegistry::new(raw_refs, kinds));

        let mut adapter =
            ObjectStoreS3Adapter::with_bucket_sharded(cluster.clone(), registry, bucket);
        adapter.set_allow_anon_list_buckets(true);

        let service = {
            let builder = S3ServiceBuilder::new(adapter);
            builder.build()
        };

        let (port, shutdown_tx) = spawn_http_server(service).await;

        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            client,
            bucket: bucket.to_string(),
            cluster,
            raw_stores,
            event_bus: bus,
            _tmp: tmp,
            _shutdown_tx: Some(shutdown_tx),
        }
    }

    /// URL for the bucket root.
    pub fn bucket_url(&self) -> String {
        format!("{}/{}", self.base_url, self.bucket)
    }

    /// URL for an object key.
    pub fn object_url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.base_url, self.bucket, key)
    }
}

// ===========================================================================
//  Event socket test helpers
// ===========================================================================

/// Drain all pending events from a broadcast receiver (non-blocking).
pub fn drain_events(
    rx: &mut tokio::sync::broadcast::Receiver<StoreEvent>,
) -> Vec<StoreEvent> {
    let mut events = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(ev) => events.push(ev),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                eprintln!("WARNING: event receiver lagged by {n} events");
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
        }
    }
    events
}

/// Async: wait until at least `expected` events have been collected, or
/// `timeout` elapses.  Returns all events received (may exceed `expected`).
pub async fn collect_events_timeout(
    rx: &mut tokio::sync::broadcast::Receiver<StoreEvent>,
    expected: usize,
    timeout: std::time::Duration,
) -> Vec<StoreEvent> {
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if events.len() >= expected {
            // Also drain any extra events that arrived in the same batch.
            events.extend(drain_events(rx));
            break;
        }
        let remaining = deadline - tokio::time::Instant::now();
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(ev)) => events.push(ev),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                eprintln!("WARNING: event receiver lagged by {n} events");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
            Err(_) => break, // timeout
        }
    }
    events
}

/// Filter PUT events and return the set of keys.
pub fn put_event_keys(events: &[StoreEvent]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Put { key } => Some(key.clone()),
            _ => None,
        })
        .collect()
}

/// Filter DELETE events and return the set of keys.
pub fn delete_event_keys(events: &[StoreEvent]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Delete { key } => Some(key.clone()),
            _ => None,
        })
        .collect()
}

/// Assert that exactly the expected PUT keys were observed.
pub fn assert_put_events(events: &[StoreEvent], expected_keys: &[&str]) {
    let got = put_event_keys(events);
    let expected: HashSet<String> = expected_keys.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        got, expected,
        "PUT event key mismatch.\n  expected: {expected:?}\n  got:      {got:?}"
    );
}

/// Assert that exactly the expected DELETE keys were observed.
pub fn assert_delete_events(events: &[StoreEvent], expected_keys: &[&str]) {
    let got = delete_event_keys(events);
    let expected: HashSet<String> = expected_keys.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        got, expected,
        "DELETE event key mismatch.\n  expected: {expected:?}\n  got:      {got:?}"
    );
}

/// Count PUT events in a slice.
pub fn count_put_events(events: &[StoreEvent]) -> usize {
    events.iter().filter(|e| matches!(e, StoreEvent::Put { .. })).count()
}

/// Count DELETE events in a slice.
pub fn count_delete_events(events: &[StoreEvent]) -> usize {
    events.iter().filter(|e| matches!(e, StoreEvent::Delete { .. })).count()
}
