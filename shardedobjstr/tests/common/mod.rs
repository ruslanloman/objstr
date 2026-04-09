//! Shared helpers for shardedobjstr integration tests.
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::Path as StdPath;
use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::event::{EventBus, StoreEvent};
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::Compression;

use shardedobjstr::ShardedObjectStore;

/// Format a shard image at `path` with the given size.
pub fn format_shard(path: &StdPath, size: u64) -> Arc<RawObjectStore> {
    let store = RawObjectStore::format_with_options(
        path,
        FormatOptions {
            device_size: size,
            direct_io: false,
            index_slot_size: 16 * 1024 * 1024,
            max_key_length: 1024,
            compression: Compression::None,
        },
    )
    .unwrap();
    store.flush_index().unwrap();
    Arc::new(store)
}

/// Open an existing shard image.
pub fn open_shard(path: &StdPath) -> Arc<RawObjectStore> {
    Arc::new(RawObjectStore::open(path).unwrap())
}

/// Build a ShardedObjectStore from raw stores, rebuilding the catalog.
pub fn build_cluster(
    raw: &[Arc<RawObjectStore>],
    replicas: usize,
) -> ShardedObjectStore {
    let stores: Vec<Arc<dyn ObjectStore>> = raw.iter().map(|s| s.clone() as _).collect();
    let cluster = ShardedObjectStore::new(stores, replicas);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();
    cluster
}

/// Flush all shard indexes to disk.
pub fn flush_all(raw: &[Arc<RawObjectStore>]) {
    for s in raw {
        s.flush_index().unwrap();
    }
}

/// Seed a cluster with test objects and flush. Returns the keys.
pub fn seed_objects(cluster: &ShardedObjectStore, raw: &[Arc<RawObjectStore>]) -> Vec<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let keys = vec![
        "data/file-a.bin",
        "data/file-b.bin",
        "data/file-c.bin",
        "other/doc.txt",
        "root.dat",
    ];
    rt.block_on(async {
        for (i, key) in keys.iter().enumerate() {
            let fill = (0x10 + i as u8).wrapping_mul(0x11);
            let data = vec![fill; 4096];
            cluster
                .put(&Path::from(*key), PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
        }
    });
    flush_all(raw);
    keys.into_iter().map(String::from).collect()
}

/// Build a cluster with an attached EventBus.
/// Returns the cluster and the bus so callers can subscribe.
pub fn build_cluster_with_events(
    raw: &[Arc<RawObjectStore>],
    replicas: usize,
) -> (ShardedObjectStore, Arc<EventBus>) {
    let cluster = build_cluster(raw, replicas);
    let bus = Arc::new(EventBus::new(512));
    cluster.set_event_bus(Arc::clone(&bus));
    (cluster, bus)
}

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

/// Count PUT events in a slice.
pub fn count_put_events(events: &[StoreEvent]) -> usize {
    events.iter().filter(|e| matches!(e, StoreEvent::Put { .. })).count()
}

/// Count DELETE events in a slice.
pub fn count_delete_events(events: &[StoreEvent]) -> usize {
    events.iter().filter(|e| matches!(e, StoreEvent::Delete { .. })).count()
}

/// Build a mixed cluster with one raw shard and one filesystem shard.
///
/// Returns the cluster, the raw store (for flushing), and the filesystem
/// root directory path (so callers can tamper with files for testing).
pub fn build_mixed_cluster(
    raw_path: &StdPath,
    fs_dir: &StdPath,
    shard_size: u64,
    replicas: usize,
) -> (ShardedObjectStore, Arc<RawObjectStore>, std::path::PathBuf) {
    let raw = format_shard(raw_path, shard_size);
    let fs_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(fs_dir)
            .expect("failed to create LocalFileSystem"),
    );

    let stores: Vec<Arc<dyn ObjectStore>> = vec![raw.clone() as _, fs_store];
    let cluster = ShardedObjectStore::new(stores, replicas);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    (cluster, raw, fs_dir.to_path_buf())
}
