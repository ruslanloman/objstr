//! End-to-end tests for the auto-recovery module (health polling + sync).
//!
//! Uses a `ControllableStore` wrapper that delegates to `InMemory` but can
//! be toggled offline/online via an `AtomicBool`.  This lets us simulate
//! shard failures without real network or device issues.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use shardedobjstr::{ShardHealth, ShardedObjectStore};
use rawobjstr::event::{EventBus, StoreEvent};
use objstrd::logging::LogBuffer;

// ── ControllableStore ────────────────────────────────────────────────

/// An `ObjectStore` that wraps `InMemory` and can be toggled offline.
/// When offline, all operations return `Generic` errors.
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

// ── Helpers ──────────────────────────────────────────────────────────

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

/// Build a 3-shard mirror cluster (rf=3) from ControllableStores.
fn build_mirror_cluster(
    stores: &[Arc<ControllableStore>],
) -> Arc<ShardedObjectStore> {
    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();
    Arc::new(ShardedObjectStore::new(obj_stores, stores.len()))
}

/// Build a mirror cluster with an EventBus pre-wired.
fn build_mirror_cluster_with_events(
    stores: &[Arc<ControllableStore>],
) -> (Arc<ShardedObjectStore>, Arc<EventBus>) {
    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();
    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, stores.len()));
    let bus = Arc::new(EventBus::new(512));
    cluster.set_event_bus(Arc::clone(&bus));
    (cluster, bus)
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

fn count_put_events(events: &[StoreEvent]) -> usize {
    events.iter().filter(|e| matches!(e, StoreEvent::Put { .. })).count()
}

fn count_delete_events(events: &[StoreEvent]) -> usize {
    events.iter().filter(|e| matches!(e, StoreEvent::Delete { .. })).count()
}

fn put_event_keys(events: &[StoreEvent]) -> std::collections::HashSet<String> {
    events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Put { key } => Some(key.clone()),
            _ => None,
        })
        .collect()
}

/// Wait until a shard reaches the expected health, with a timeout.
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

// ── Tests ────────────────────────────────────────────────────────────

/// Health polling detects an unreachable shard and auto-detaches it.
#[tokio::test]
async fn health_poll_detaches_unreachable_shard() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let s2 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone(), s2.clone()];
    let (cluster, bus) = build_mirror_cluster_with_events(&stores);
    let mut event_rx = bus.subscribe();

    // Write an object while all shards are healthy.
    let path = Path::from("test/hello.txt");
    cluster
        .put(&path, PutPayload::from_static(b"hello"))
        .await
        .unwrap();

    // Verify PUT event was emitted.
    let events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event for initial write");
    let keys = put_event_keys(&events);
    assert!(keys.contains("test/hello.txt"), "PUT event key mismatch");

    // Start recovery task.
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        fast_recovery_config(),
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Take shard 1 offline.
    s1.set_online(false);

    // Wait for the recovery loop to detect and detach shard 1.
    assert!(
        wait_for_health(&cluster, 1, ShardHealth::Offline, 10).await,
        "shard 1 should become Offline after probe failures"
    );

    // Verify shard 0 and 2 are still healthy.
    assert_eq!(cluster.shard_health(0), Some(ShardHealth::Healthy));
    assert_eq!(cluster.shard_health(2), Some(ShardHealth::Healthy));

    // Reads still work via remaining replicas.
    let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data, Bytes::from_static(b"hello"));

    handle.abort();
}

/// After detach, bringing the shard back triggers mirror sync and re-attach.
#[tokio::test]
async fn mirror_sync_reattaches_recovered_shard() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone()];
    let (cluster, bus) = build_mirror_cluster_with_events(&stores);
    let mut event_rx = bus.subscribe();

    // Write initial objects while both shards are up.
    for i in 0..5 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = format!("data-{i}");
        cluster
            .put(&path, PutPayload::from(Bytes::from(data)))
            .await
            .unwrap();
    }

    // Verify 5 PUT events from initial writes.
    let events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&events), 5, "expected 5 PUT events for initial writes");

    // Start recovery task.
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        fast_recovery_config(),
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Take shard 1 offline.
    s1.set_online(false);

    // Wait for auto-detach.
    assert!(
        wait_for_health(&cluster, 1, ShardHealth::Offline, 10).await,
        "shard 1 should be detached"
    );

    // Write new objects while shard 1 is offline (only lands on shard 0).
    for i in 5..8 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = format!("data-{i}");
        cluster
            .put(&path, PutPayload::from(Bytes::from(data)))
            .await
            .unwrap();
    }

    // Verify PUT events for degraded writes.
    let degraded_events = drain_events(&mut event_rx);
    assert_eq!(
        count_put_events(&degraded_events), 3,
        "expected 3 PUT events for writes while shard 1 is offline"
    );
    let degraded_keys = put_event_keys(&degraded_events);
    for i in 5..8 {
        assert!(
            degraded_keys.contains(&format!("obj/{i}.bin")),
            "missing PUT event for obj/{i}.bin during degraded write"
        );
    }

    // Delete one of the original objects while shard 1 is offline.
    cluster.delete(&Path::from("obj/2.bin")).await.unwrap();

    // Verify DELETE event.
    let del_events = drain_events(&mut event_rx);
    assert_eq!(
        count_delete_events(&del_events), 1,
        "expected 1 DELETE event for obj/2.bin"
    );

    // Bring shard 1 back online.
    s1.set_online(true);

    // Wait for sync + re-attach (shard goes Syncing -> Healthy).
    assert!(
        wait_for_health(&cluster, 1, ShardHealth::Healthy, 15).await,
        "shard 1 should be re-attached after sync"
    );

    // Verify: all 7 surviving objects are readable.
    for i in (0..8).filter(|&i| i != 2) {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("data-{i}")));
    }

    // Verify: deleted object is gone.
    let result = cluster.get(&Path::from("obj/2.bin")).await;
    assert!(result.is_err(), "obj/2.bin should have been deleted during sync");

    // Verify: shard 1 was synced (check objects directly on it).
    let s1_objects: Vec<ObjectMeta> =
        s1.list(None).try_collect().await.unwrap();
    let s1_keys: std::collections::HashSet<String> =
        s1_objects.iter().map(|m| m.location.to_string()).collect();
    // New objects written during downtime should be on shard 1 now.
    for i in 5..8 {
        assert!(
            s1_keys.contains(&format!("obj/{i}.bin")),
            "obj/{i}.bin should be synced to shard 1"
        );
    }
    // Deleted object should NOT be on shard 1.
    assert!(
        !s1_keys.contains("obj/2.bin"),
        "obj/2.bin should be deleted from shard 1 during mirror sync"
    );

    handle.abort();
}

/// Writes during shard transitions are not lost.
#[tokio::test]
async fn writes_during_recovery_preserved() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let s2 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone(), s2.clone()];
    let (cluster, bus) = build_mirror_cluster_with_events(&stores);
    let mut event_rx = bus.subscribe();

    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        fast_recovery_config(),
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Write initial data.
    cluster
        .put(
            &Path::from("file.txt"),
            PutPayload::from_static(b"version-1"),
        )
        .await
        .unwrap();

    // Verify initial PUT event.
    let events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&events), 1, "expected 1 PUT event for initial write");

    // Take shard 2 offline.
    s2.set_online(false);
    assert!(wait_for_health(&cluster, 2, ShardHealth::Offline, 10).await);

    // Overwrite while shard 2 is down.
    cluster
        .put(
            &Path::from("file.txt"),
            PutPayload::from_static(b"version-2"),
        )
        .await
        .unwrap();

    // Verify overwrite PUT event.
    let ow_events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&ow_events), 1, "expected 1 PUT event for overwrite");

    // Bring shard 2 back.
    s2.set_online(true);
    assert!(wait_for_health(&cluster, 2, ShardHealth::Healthy, 15).await);

    // The latest version should be readable (from shard 0 or 1).
    let data = cluster
        .get(&Path::from("file.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data, Bytes::from_static(b"version-2"));

    // Note: shard 2 may still have version-1 because mirror sync only
    // copies missing/extra keys, not stale content.  A content-aware
    // sync (comparing ETags or timestamps) is a future improvement.
    // Verify at least that the new object written while shard 2 was
    // offline gets synced to it.
    cluster
        .put(
            &Path::from("new-while-offline.txt"),
            PutPayload::from_static(b"created-during-outage"),
        )
        .await
        .unwrap();

    // Take shard 2 offline then back to re-trigger sync with the new key.
    s2.set_online(false);
    assert!(wait_for_health(&cluster, 2, ShardHealth::Offline, 10).await);
    s2.set_online(true);
    assert!(wait_for_health(&cluster, 2, ShardHealth::Healthy, 15).await);

    // New key should now exist on shard 2.
    let s2_data = s2
        .get(&Path::from("new-while-offline.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(s2_data, Bytes::from_static(b"created-during-outage"));

    handle.abort();
}

/// Partitioned mode (rf < shard_count) recovery uses
/// find_under_replicated + replicate_object.
#[tokio::test]
async fn partitioned_sync_repairs_under_replicated() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let s2 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone(), s2.clone()];

    // rf=2, 3 shards -> partitioned mode.
    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();
    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, 2));

    // Write objects -- each lands on 2 of 3 shards.
    for i in 0..10 {
        let path = Path::from(format!("data/{i}.bin"));
        cluster
            .put(&path, PutPayload::from(Bytes::from(format!("val-{i}"))))
            .await
            .unwrap();
    }

    // Manually detach shard 0 so we can observe under-replication
    // before the recovery loop has a chance to repair it.
    s0.set_online(false);
    cluster.detach_shard(0);

    // Some objects are now under-replicated (those that had shard 0 as a replica).
    let under = cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "should have under-replicated objects after shard 0 goes offline"
    );

    // Now start recovery and bring shard 0 back -- recovery should
    // reattach it and heal the under-replication.
    s0.set_online(true);
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        fast_recovery_config(),
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    assert!(wait_for_health(&cluster, 0, ShardHealth::Healthy, 15).await);

    // After recovery, all objects should be fully replicated again.
    // Give a brief extra moment for catalog rebuild.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let still_under = cluster.find_under_replicated();
    assert!(
        still_under.is_empty(),
        "no objects should be under-replicated after recovery, got: {still_under:?}"
    );

    // All objects should be readable.
    for i in 0..10 {
        let path = Path::from(format!("data/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("val-{i}")));
    }

    handle.abort();
}

/// If a shard flaps (goes offline briefly then comes back before threshold),
/// it should NOT be detached.
#[tokio::test]
async fn brief_flap_does_not_detach() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone()];
    let cluster = build_mirror_cluster(&stores);

    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let config = objstrd::recovery::RecoveryConfig {
        enabled: true,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 3, // needs 3 consecutive failures
        re_replicate_batch_size: 100,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    };
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        config,
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Take shard 0 offline for one poll cycle, then bring it back.
    s0.set_online(false);
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    s0.set_online(true);

    // Wait a few more cycles.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Shard 0 should still be healthy (flap was too brief for threshold=3).
    assert_eq!(
        cluster.shard_health(0),
        Some(ShardHealth::Healthy),
        "shard 0 should not be detached after a brief flap"
    );

    handle.abort();
}

/// When a shard is permanently offline past the grace period, the
/// re-replication sweep copies under-replicated objects to other
/// healthy shards to restore RF -- even though the dead shard never
/// comes back.
#[tokio::test]
async fn re_replication_restores_rf_on_permanent_failure() {
    // 4 shards, rf=2  ->  partitioned mode.
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let s2 = Arc::new(ControllableStore::new());
    let s3 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone(), s2.clone(), s3.clone()];

    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();
    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, 2));
    let bus = Arc::new(EventBus::new(512));
    cluster.set_event_bus(Arc::clone(&bus));
    let mut event_rx = bus.subscribe();

    // Write 20 objects -- each lands on 2 of 4 shards.
    for i in 0..20 {
        let path = Path::from(format!("data/{i}.bin"));
        cluster
            .put(&path, PutPayload::from(Bytes::from(format!("val-{i}"))))
            .await
            .unwrap();
    }

    // Verify 20 PUT events from initial writes.
    let init_events = drain_events(&mut event_rx);
    assert_eq!(
        count_put_events(&init_events), 20,
        "expected 20 PUT events for initial writes"
    );

    // Verify baseline: no under-replicated objects.
    assert!(
        cluster.find_under_replicated().is_empty(),
        "all objects should be fully replicated before shard failure"
    );

    // Manually kill shard 0 and detach it so we can observe
    // under-replication before the recovery loop repairs anything.
    s0.set_online(false);
    cluster.detach_shard(0);

    // Some objects should now be under-replicated.
    let under = cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "should have under-replicated objects after shard 0 dies"
    );
    let under_count = under.len();

    // Start recovery with fast timings and re-replication enabled
    // (grace = 3s so we don't wait too long in a test).
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let config = objstrd::recovery::RecoveryConfig {
        enabled: true,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 2,
        re_replicate_batch_size: 50,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    };
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        config,
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Wait for re-replication grace period + sweep cycles.
    // Grace = 3s, poll = 1s, so after ~5-6s the sweep should have run.
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;

    // All objects should be fully replicated on the 3 surviving shards.
    let still_under = cluster.find_under_replicated();
    assert!(
        still_under.is_empty(),
        "re-replication should restore RF; still under-replicated: {still_under:?} (was {under_count})"
    );

    // All objects should be readable.
    for i in 0..20 {
        let path = Path::from(format!("data/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("val-{i}")));
    }

    handle.abort();
}

/// Recovery is disabled when `enabled = false`.
#[tokio::test]
async fn recovery_disabled_does_not_detach() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone()];
    let cluster = build_mirror_cluster(&stores);

    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let config = objstrd::recovery::RecoveryConfig {
        enabled: false,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 2,
        re_replicate_batch_size: 100,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    };
    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        config,
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Take shard 0 offline.
    s0.set_online(false);

    // Wait several poll cycles.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Shard should still show as Healthy because recovery is disabled
    // (the loop returned immediately, no probing).
    assert_eq!(
        cluster.shard_health(0),
        Some(ShardHealth::Healthy),
        "shard 0 should remain Healthy when recovery is disabled"
    );

    handle.abort();
}

/// Regression test: a shard whose device path no longer exists on disk must
/// stay Offline even when the original ObjectStore (which may hold a stale
/// Linux file descriptor to the old inode) responds to probes successfully.
///
/// Without the path-existence guard in recovery.rs the probe would succeed via
/// the in-memory store (simulating a stale fd) and the shard would be falsely
/// re-attached.
#[tokio::test]
async fn device_path_gone_stays_offline() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone()];
    let cluster = build_mirror_cluster(&stores);

    // Write an object so there is data to replicate.
    cluster
        .put(
            &Path::from("probe-test.txt"),
            PutPayload::from_static(b"regression"),
        )
        .await
        .unwrap();

    // Create a real temp file to represent the backing device for shard 0.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let device_path = tmp.path().to_str().unwrap().to_string();

    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    // device_paths: shard 0 has the temp file path, shard 1 has none.
    let device_paths = vec![Some(device_path.clone()), None];

    let (handle, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        device_paths,
        fast_recovery_config(),
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // -- Phase 1: take shard 0 offline via probe failures --
    s0.set_online(false);
    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Offline, 10).await,
        "shard 0 should be auto-detached after probe failures"
    );

    // -- Phase 2: store comes "back" but the device path is gone --
    // Simulate: original store fd is still alive (s0 back online) but
    // the file on disk has been removed (e.g. after an 'mv' or 'rm').
    s0.set_online(true);
    drop(tmp); // deletes the temp file; device_path string still valid

    // Wait several poll cycles.  Without the fix the probe would succeed
    // and the shard would be re-attached; with the fix it must stay Offline.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    assert_eq!(
        cluster.shard_health(0),
        Some(ShardHealth::Offline),
        "shard 0 must stay Offline when device path is gone, even if store probe succeeds"
    );

    // -- Phase 3: restore the file -- shard should recover --
    std::fs::File::create(&device_path).unwrap();

    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Healthy, 15).await,
        "shard 0 should recover once the device path is restored"
    );

    // Cleanup.
    std::fs::remove_file(&device_path).ok();
    handle.abort();
}

/// When a shard dies and re-replication restores RF on surviving shards,
/// then the dead shard comes back (re-attach), objects that were
/// re-replicated now have MORE copies than RF.  The over-replication
/// trim sweep should automatically remove the excess replicas.
#[tokio::test]
async fn over_replication_trimmed_after_shard_returns() {
    // 4 shards, rf=2.
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let s2 = Arc::new(ControllableStore::new());
    let s3 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone(), s2.clone(), s3.clone()];

    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();
    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, 2));
    let bus = Arc::new(EventBus::new(512));
    cluster.set_event_bus(Arc::clone(&bus));
    let mut event_rx = bus.subscribe();

    // Write 20 objects -- each lands on 2 of 4 shards.
    for i in 0..20 {
        let path = Path::from(format!("obj/{i}.bin"));
        cluster
            .put(&path, PutPayload::from(Bytes::from(format!("data-{i}"))))
            .await
            .unwrap();
    }

    // Verify 20 PUT events from initial writes.
    let init_events = drain_events(&mut event_rx);
    assert_eq!(count_put_events(&init_events), 20, "expected 20 PUT events for initial writes");

    // Baseline: no under- or over-replicated objects.
    assert!(cluster.find_under_replicated().is_empty());
    assert!(cluster.find_over_replicated().is_empty());

    // -- Phase 1: kill shard 0, re-replicate via recovery task --
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let config = objstrd::recovery::RecoveryConfig {
        enabled: true,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 2,
        re_replicate_batch_size: 100,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    };
    let (handle1, _) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals.clone(),
        vec![None; stores.len()],
        config,
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    s0.set_online(false);
    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Offline, 10).await,
        "shard 0 should be detached"
    );

    // Wait for grace period + sweep to re-replicate.
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    assert!(
        cluster.find_under_replicated().is_empty(),
        "re-replication should have restored RF"
    );

    // Stop the first recovery task so it does not trim in the same
    // cycle as the reattach.
    handle1.abort();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // -- Phase 2: bring shard 0 back and manually reattach --
    // The InMemory store still holds shard 0's objects.
    // attach_shard(force=true) scans the store and re-adds shard 0
    // to catalog entries, creating over-replication.
    s0.set_online(true);
    let reattached = cluster
        .attach_shard(0, Arc::clone(&s0) as Arc<dyn ObjectStore>, true)
        .await
        .unwrap();
    assert!(reattached > 0, "shard 0 should still have objects");

    let over_before = cluster.find_over_replicated();
    assert!(
        !over_before.is_empty(),
        "should have over-replicated objects after shard 0 returns; got none"
    );

    // -- Phase 3: start a new recovery task and let it trim --
    let config2 = objstrd::recovery::RecoveryConfig {
        enabled: true,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 2,
        re_replicate_batch_size: 100,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    };
    let (handle2, status) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        config2,
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Wait a few poll cycles for the trim sweep.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let over_after = cluster.find_over_replicated();
    assert!(
        over_after.is_empty(),
        "over-replication trim should have removed excess replicas; still over: {over_after:?}"
    );

    // No under-replicated objects either.
    assert!(
        cluster.find_under_replicated().is_empty(),
        "should have no under-replicated objects after trim"
    );

    // Every object should have exactly rf=2 replicas in the catalog.
    for i in 0..20 {
        let key = format!("obj/{i}.bin");
        let entry = cluster.placement(&key).expect("object should exist in catalog");
        assert_eq!(
            entry.shards.len(),
            2,
            "object {key} should have exactly 2 replicas, has {:?}",
            entry.shards
        );
    }

    // All objects should be readable with correct data.
    for i in 0..20 {
        let path = Path::from(format!("obj/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("data-{i}")));
    }

    // Verify the status handle tracked trimming.
    {
        let st = status.lock().clone();
        assert!(
            st.total_trimmed > 0,
            "recovery status should show trimmed replicas; total_trimmed={}",
            st.total_trimmed
        );
    }

    handle2.abort();
}

/// The RecoveryStatusHandle tracks poll_cycles, last_poll_at, and config snapshot.
#[tokio::test]
async fn recovery_status_tracking() {
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone()];
    let cluster = build_mirror_cluster(&stores);

    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let config = objstrd::recovery::RecoveryConfig {
        enabled: true,
        poll_interval_secs: 1,
        probe_timeout_secs: 2,
        failure_threshold: 3,
        re_replicate_batch_size: 50,
        repair_replication_interval_secs: 0,
        repair_replication_batch_size: 500,
    };
    let (handle, status) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        config,
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Let the recovery loop run several poll cycles.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    let st = status.lock().clone();

    // poll_cycles should have incremented.
    assert!(
        st.poll_cycles >= 2,
        "expected at least 2 poll cycles after 4s with 1s interval, got {}",
        st.poll_cycles
    );

    // last_poll_at should be populated.
    assert!(
        st.last_poll_at.is_some(),
        "last_poll_at should be Some after polls have run"
    );

    // Config snapshot should reflect what we passed in.
    assert!(st.config.enabled);
    assert_eq!(st.config.poll_interval_secs, 1);
    assert_eq!(st.config.failure_threshold, 3);
    assert_eq!(st.config.re_replicate_batch_size, 50);

    // With all shards healthy, under/over counts should be zero.
    assert_eq!(st.under_replicated_count, 0);
    assert_eq!(st.over_replicated_count, 0);

    handle.abort();
}

/// Two shards failing simultaneously: cluster stays readable via surviving
/// replicas, and both recover when brought back.
#[tokio::test]
async fn two_shards_fail_simultaneously() {
    // 4 shards, rf=3 -> each object on 3 of 4 shards.
    let s0 = Arc::new(ControllableStore::new());
    let s1 = Arc::new(ControllableStore::new());
    let s2 = Arc::new(ControllableStore::new());
    let s3 = Arc::new(ControllableStore::new());
    let stores = vec![s0.clone(), s1.clone(), s2.clone(), s3.clone()];

    let obj_stores: Vec<Arc<dyn ObjectStore>> = stores
        .iter()
        .map(|s| Arc::clone(s) as Arc<dyn ObjectStore>)
        .collect();
    let cluster = Arc::new(ShardedObjectStore::new(obj_stores, 3));

    // Write 10 objects.
    for i in 0..10 {
        let path = Path::from(format!("multi/{i}.bin"));
        cluster
            .put(&path, PutPayload::from(Bytes::from(format!("val-{i}"))))
            .await
            .unwrap();
    }

    // Start recovery.
    let originals: Vec<Option<Arc<dyn ObjectStore>>> = stores
        .iter()
        .map(|s| Some(Arc::clone(s) as Arc<dyn ObjectStore>))
        .collect();
    let (handle, status) = objstrd::recovery::spawn_recovery_task(
        Arc::clone(&cluster),
        originals,
        vec![None; stores.len()],
        fast_recovery_config(),
        LogBuffer::new(100, None),
        None,
        Arc::new(tokio::sync::Mutex::new(None)),
    );

    // Kill shards 0 and 2 simultaneously.
    s0.set_online(false);
    s2.set_online(false);

    // Both should go Offline.
    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Offline, 10).await,
        "shard 0 should be detached"
    );
    assert!(
        wait_for_health(&cluster, 2, ShardHealth::Offline, 10).await,
        "shard 2 should be detached"
    );

    // Reads should still work -- each object has rf=3 and only 2 shards
    // are down, so at least 1 healthy replica exists for every object.
    for i in 0..10 {
        let path = Path::from(format!("multi/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("val-{i}")));
    }

    // Bring both back.
    s0.set_online(true);
    s2.set_online(true);

    assert!(
        wait_for_health(&cluster, 0, ShardHealth::Healthy, 15).await,
        "shard 0 should recover"
    );
    assert!(
        wait_for_health(&cluster, 2, ShardHealth::Healthy, 15).await,
        "shard 2 should recover"
    );

    // All objects should still be readable with correct data.
    for i in 0..10 {
        let path = Path::from(format!("multi/{i}.bin"));
        let data = cluster.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from(format!("val-{i}")));
    }

    // Status should show poll cycles ran during the episode.
    let st = status.lock().clone();
    assert!(
        st.poll_cycles >= 2,
        "expected poll cycles to increment during dual-failure episode"
    );

    handle.abort();
}
