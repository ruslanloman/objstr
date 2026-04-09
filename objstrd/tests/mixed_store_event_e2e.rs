//! E2E test: 4-store heterogeneous cluster with event socket monitoring.
//!
//! Stores:
//!   - shard 0: simulated Raw
//!   - shard 1: simulated Filesystem (Sidecar)
//!   - shard 2: simulated InMemory (Sidecar)
//!   - shard 3: simulated S3 (S3Like)
//!
//! Replication factor = 4 (every object on every shard).
//!
//! Phases:
//!   1. PUT 10 objects, verify GETs, check event socket sees 10 PUT events
//!   2. DELETE 3 objects, verify gone, check DELETE events
//!   3. Fail shard 0 (raw) and shard 3 (s3), wait for auto-detach
//!   4. PUT 5 more objects while shards are down (land on shards 1+2 only)
//!   5. Bring shard 0 and shard 3 back, wait for sync + re-attach
//!   6. Verify all surviving objects readable, synced to recovered shards

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use rawobjstr::event::{EventBus, StoreEvent};
use shardedobjstr::{ShardHealth, ShardedObjectStore};

// ---- ControllableStore (same pattern as recovery_e2e.rs) ----

#[derive(Debug)]
struct ControllableStore {
    inner: InMemory,
    online: AtomicBool,
}

impl ControllableStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            online: AtomicBool::new(true),
        }
    }

    fn set_online(&self, value: bool) {
        self.online.store(value, Ordering::SeqCst);
    }

    fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    fn err(&self) -> object_store::Error {
        object_store::Error::Generic {
            store: "ControllableStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "store is offline (simulated)",
            )),
        }
    }
}

impl std::fmt::Display for ControllableStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ControllableStore(online={})", self.is_online())
    }
}

#[async_trait]
impl ObjectStore for ControllableStore {
    async fn put(&self, path: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.put(path, payload).await
    }

    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.put_opts(path, payload, opts).await
    }

    async fn put_multipart(
        &self,
        path: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.put_multipart(path).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.put_multipart_opts(path, opts).await
    }

    async fn get(&self, path: &Path) -> object_store::Result<GetResult> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.get(path).await
    }

    async fn get_opts(
        &self,
        path: &Path,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.get_opts(path, opts).await
    }

    async fn get_range(
        &self,
        path: &Path,
        range: std::ops::Range<u64>,
    ) -> object_store::Result<Bytes> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.get_range(path, range).await
    }

    async fn head(&self, path: &Path) -> object_store::Result<ObjectMeta> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.head(path).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if !self.is_online() {
            return Box::pin(futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "ControllableStore",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "store is offline (simulated)",
                    )),
                })
            }));
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        if !self.is_online() { return Err(self.err()); }
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ---- Helpers ----

fn fast_recovery_config() -> objstrd::recovery::RecoveryConfig {
    objstrd::recovery::RecoveryConfig {
        enabled: true,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 2,
        re_replicate_batch_size: 100,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    }
}

async fn wait_for_health(
    cluster: &ShardedObjectStore,
    shard_id: usize,
    expected: ShardHealth,
    timeout_secs: u64,
) -> bool {
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        if cluster.shard_health(shard_id) == Some(expected) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Drain all pending events from a broadcast receiver (non-blocking).
fn drain_events(
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

// ---- Main Test ----

/// Full lifecycle test with 4 heterogeneous store types, event monitoring,
/// shard failures, writes-during-failure, and recovery sync.
#[tokio::test]
async fn mixed_4store_event_socket_lifecycle() {
    // -- Setup: 4 controllable stores, rf=4 --
    let s_raw = Arc::new(ControllableStore::new());  // shard 0: simulated raw
    let s_fs  = Arc::new(ControllableStore::new());  // shard 1: simulated filesystem
    let s_mem = Arc::new(ControllableStore::new());  // shard 2: simulated in-memory
    let s_s3  = Arc::new(ControllableStore::new());  // shard 3: simulated S3

    let stores: Vec<Arc<ControllableStore>> =
        vec![s_raw.clone(), s_fs.clone(), s_mem.clone(), s_s3.clone()];

    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();

    // rf=4: every object goes to all 4 shards.
    // min_writes=1: allow degraded writes when shards go offline.
    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, 4).with_min_writes(1));

    // -- Attach event bus and subscribe --
    let bus = Arc::new(EventBus::new(512));
    cluster.set_event_bus(Arc::clone(&bus));
    let mut event_rx = bus.subscribe();

    // ========================================================
    // Phase 1: PUT 10 objects, verify GETs, check PUT events
    // ========================================================
    for i in 0..10 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = format!("payload-{i}");
        cluster
            .put(&path, PutPayload::from(Bytes::from(data)))
            .await
            .unwrap();
    }

    // Verify all 10 objects are readable and correct.
    for i in 0..10 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("payload-{i}")));
    }

    // Verify replication: each object should be on all 4 shards.
    for i in 0..10 {
        let internal = Path::from(format!("obj/{i}.bin"));
        let mut shard_count = 0;
        for store in &stores {
            if store.head(&internal).await.is_ok() {
                shard_count += 1;
            }
        }
        assert_eq!(
            shard_count, 4,
            "obj/{i}.bin should be on all 4 shards, found on {shard_count}"
        );
    }

    // Check event bus received 10 PUT events.
    let events = drain_events(&mut event_rx);
    let put_events: Vec<&StoreEvent> = events
        .iter()
        .filter(|e| matches!(e, StoreEvent::Put { .. }))
        .collect();
    assert_eq!(
        put_events.len(), 10,
        "expected 10 PUT events, got {}: {:?}",
        put_events.len(), put_events
    );

    // Verify the PUT keys match what we wrote.
    let put_keys: HashSet<String> = events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Put { key } => Some(key.clone()),
            _ => None,
        })
        .collect();
    for i in 0..10 {
        assert!(
            put_keys.contains(&format!("obj/{i}.bin")),
            "missing PUT event for obj/{i}.bin"
        );
    }

    // ========================================================
    // Phase 2: DELETE 3 objects, verify gone, check DELETE events
    // ========================================================
    for i in [2, 5, 8] {
        let path = Path::from(format!("obj/{i}.bin"));
        cluster.delete(&path).await.unwrap();
    }

    // Verify deleted objects are gone.
    for i in [2, 5, 8] {
        let path = Path::from(format!("obj/{i}.bin"));
        let result = cluster.get(&path).await;
        assert!(result.is_err(), "obj/{i}.bin should be deleted");
    }

    // Verify surviving objects still readable.
    for i in [0, 1, 3, 4, 6, 7, 9] {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("payload-{i}")));
    }

    // Check DELETE events.
    let events2 = drain_events(&mut event_rx);
    let del_events: Vec<&StoreEvent> = events2
        .iter()
        .filter(|e| matches!(e, StoreEvent::Delete { .. }))
        .collect();
    assert_eq!(
        del_events.len(), 3,
        "expected 3 DELETE events, got {}: {:?}",
        del_events.len(), del_events
    );

    let del_keys: HashSet<String> = events2
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Delete { key } => Some(key.clone()),
            _ => None,
        })
        .collect();
    for i in [2, 5, 8] {
        assert!(
            del_keys.contains(&format!("obj/{i}.bin")),
            "missing DELETE event for obj/{i}.bin"
        );
    }

    // ========================================================
    // Phase 3: Fail shard 0 (raw) and shard 3 (s3)
    // ========================================================
    let log_buffer = objstrd::logging::LogBuffer::new(100, None);
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let (recovery_handle, _recovery_status) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        fast_recovery_config(),
        log_buffer,
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Take shard 0 (raw) and shard 3 (s3) offline.
    s_raw.set_online(false);
    s_s3.set_online(false);

    // Wait for auto-detach of both.
    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Offline, 10).await,
        "shard 0 (raw) should be detached"
    );
    assert!(
        wait_for_health(&cluster, 3, ShardHealth::Offline, 10).await,
        "shard 3 (s3) should be detached"
    );

    // Shards 1 (fs) and 2 (mem) should still be healthy.
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_health(2), Some(ShardHealth::Healthy));

    // Reads still work via remaining replicas (shards 1+2).
    for i in [0, 1, 3, 4, 6, 7, 9] {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(
            data,
            Bytes::from(format!("payload-{i}")),
            "obj/{i}.bin should still be readable from healthy shards"
        );
    }

    // ========================================================
    // Phase 4: PUT 5 more objects while shards 0+3 are down
    // ========================================================
    for i in 10..15 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = format!("payload-{i}");
        cluster
            .put(&path, PutPayload::from(Bytes::from(data)))
            .await
            .unwrap();
    }

    // Verify the new objects are readable.
    for i in 10..15 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("payload-{i}")));
    }

    // New objects should only be on shards 1 and 2 (the healthy ones).
    for i in 10..15 {
        let internal = Path::from(format!("obj/{i}.bin"));
        assert!(s_fs.head(&internal).await.is_ok(), "obj/{i}.bin should be on shard 1 (fs)");
        assert!(s_mem.head(&internal).await.is_ok(), "obj/{i}.bin should be on shard 2 (mem)");
        // Offline stores should NOT have these since they were down during put.
    }

    // Check event bus received 5 more PUT events.
    let events3 = drain_events(&mut event_rx);
    let new_puts: Vec<&StoreEvent> = events3
        .iter()
        .filter(|e| matches!(e, StoreEvent::Put { .. }))
        .collect();
    assert_eq!(
        new_puts.len(), 5,
        "expected 5 PUT events while shards were down, got {}",
        new_puts.len()
    );

    // ========================================================
    // Phase 5: Bring shard 0 (raw) and shard 3 (s3) back
    // ========================================================
    s_raw.set_online(true);
    s_s3.set_online(true);

    // Wait for recovery to sync + re-attach both shards.
    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Healthy, 20).await,
        "shard 0 (raw) should be re-attached after sync"
    );
    assert!(
        wait_for_health(&cluster, 3, ShardHealth::Healthy, 20).await,
        "shard 3 (s3) should be re-attached after sync"
    );

    // ========================================================
    // Phase 6: Verify everything after recovery
    // ========================================================

    // All 12 surviving objects (7 original + 5 new) should be readable.
    let surviving: Vec<usize> = vec![0, 1, 3, 4, 6, 7, 9, 10, 11, 12, 13, 14];
    for i in &surviving {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(
            data,
            Bytes::from(format!("payload-{i}")),
            "obj/{i}.bin should be readable after recovery"
        );
    }

    // Deleted objects should still be gone.
    for i in [2, 5, 8] {
        let path = Path::from(format!("obj/{i}.bin"));
        let result = cluster.get(&path).await;
        assert!(result.is_err(), "obj/{i}.bin should still be deleted after recovery");
    }

    // Give a brief moment for mirror sync to propagate.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify that new objects (written while shards 0+3 were down) got
    // synced to the recovered shards.
    for i in 10..15 {
        let internal = Path::from(format!("obj/{i}.bin"));
        let on_raw = s_raw.head(&internal).await.is_ok();
        let on_s3 = s_s3.head(&internal).await.is_ok();
        assert!(
            on_raw,
            "obj/{i}.bin should be synced to shard 0 (raw) after recovery"
        );
        assert!(
            on_s3,
            "obj/{i}.bin should be synced to shard 3 (s3) after recovery"
        );
    }

    // Verify that the original surviving objects are still on the
    // recovered shards (they had them before the failure).
    for i in [0, 1, 3, 4, 6, 7, 9] {
        let internal = Path::from(format!("obj/{i}.bin"));
        let on_raw = s_raw.head(&internal).await.is_ok();
        let on_s3 = s_s3.head(&internal).await.is_ok();
        assert!(
            on_raw,
            "obj/{i}.bin should still be on shard 0 (raw) after recovery"
        );
        assert!(
            on_s3,
            "obj/{i}.bin should still be on shard 3 (s3) after recovery"
        );
    }

    // Deleted objects should not be on recovered shards.
    for i in [2, 5, 8] {
        let internal = Path::from(format!("obj/{i}.bin"));
        assert!(
            s_raw.head(&internal).await.is_err(),
            "obj/{i}.bin should be deleted from shard 0 (raw) after sync"
        );
        assert!(
            s_s3.head(&internal).await.is_err(),
            "obj/{i}.bin should be deleted from shard 3 (s3) after sync"
        );
    }

    // No under-replicated objects should remain.
    let under = cluster.find_under_replicated();
    assert!(
        under.is_empty(),
        "no objects should be under-replicated after full recovery, got: {under:?}"
    );

    recovery_handle.abort();
}
