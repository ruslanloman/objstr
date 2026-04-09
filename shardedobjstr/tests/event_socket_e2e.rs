//! End-to-end tests for the event socket infrastructure.
//!
//! Exercises: setup_event_socket, subscribe_store_events, PUT/DELETE/FLUSH
//! event delivery over a Unix domain socket, and authentication rejection.
//!
//! These tests require Unix (event socket uses Unix domain sockets).

#![cfg(unix)]

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::event::StoreEvent;
use rawobjstr::store::RawObjectStore;

use shardedobjstr::event::{setup_event_socket, subscribe_store_events};

use common::{format_shard, flush_all};

/// Helper: build a 3-shard cluster with event socket on a temp Unix socket.
/// Returns (cluster, raw_stores, socket_path, _server_guard, runtime).
///
/// The runtime is returned because EventServer must live inside a Tokio
/// context (it spawns an accept task).
fn setup_cluster_with_socket(
    dir: &std::path::Path,
) -> (
    shardedobjstr::ShardedObjectStore,
    Vec<Arc<RawObjectStore>>,
    std::path::PathBuf,
    rawobjstr::event::unix::EventServer,
    tokio::runtime::Runtime,
) {
    let shard_size: u64 = 64 * 1024 * 1024;
    let raws = vec![
        format_shard(&dir.join("s0.raw"), shard_size),
        format_shard(&dir.join("s1.raw"), shard_size),
        format_shard(&dir.join("s2.raw"), shard_size),
    ];

    let stores: Vec<Arc<dyn ObjectStore>> =
        raws.iter().map(|s| Arc::clone(s) as Arc<dyn ObjectStore>).collect();
    let cluster = shardedobjstr::ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let raw_pairs: Vec<(usize, Arc<RawObjectStore>)> = raws
        .iter()
        .enumerate()
        .map(|(i, s)| (i, Arc::clone(s)))
        .collect();

    let socket_path = dir.join("events.sock");
    let secret = "test-secret-key";

    // EventServer::start calls tokio::spawn, so needs a runtime context.
    let _guard = rt.enter();
    let (_bus, server) = setup_event_socket(
        &cluster,
        &raw_pairs,
        &socket_path,
        secret,
        16,
        None,
    )
    .unwrap();

    (cluster, raws, socket_path, server, rt)
}

// =====================================================================
// PUT / DELETE events arrive over the socket
// =====================================================================

#[test]
fn event_socket_put_delete_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, socket_path, _server, rt) =
        setup_cluster_with_socket(dir.path());
    let secret = "test-secret-key";

    // Collect events in a shared vec.
    let events: Arc<std::sync::Mutex<Vec<StoreEvent>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);

    let handle = rt.block_on(async {
        subscribe_store_events(&socket_path, secret, move |ev| {
            events_clone.lock().unwrap().push(ev);
        })
        .await
        .unwrap()
    });

    // Give the subscriber a moment to connect.
    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    // PUT 5 objects.
    rt.block_on(async {
        for i in 0..5 {
            let key = format!("evt/obj_{}.bin", i);
            let data = vec![i as u8; 512];
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(data)),
                )
                .await
                .unwrap();
        }
    });

    // DELETE 2 objects.
    rt.block_on(async {
        cluster
            .delete(&Path::from("evt/obj_0.bin"))
            .await
            .unwrap();
        cluster
            .delete(&Path::from("evt/obj_1.bin"))
            .await
            .unwrap();
    });

    // Allow events to propagate.
    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    let collected = events.lock().unwrap();
    let put_count = collected
        .iter()
        .filter(|e| matches!(e, StoreEvent::Put { .. }))
        .count();
    let delete_count = collected
        .iter()
        .filter(|e| matches!(e, StoreEvent::Delete { .. }))
        .count();
    let put_keys: std::collections::HashSet<String> = collected
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Put { key } => Some(key.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(put_count, 5, "expected 5 PUT events");
    assert_eq!(delete_count, 2, "expected 2 DELETE events");
    for i in 0..5 {
        assert!(
            put_keys.contains(&format!("evt/obj_{}.bin", i)),
            "missing PUT event for obj_{}",
            i
        );
    }

    handle.abort();
    flush_all(&raws);
}

// =====================================================================
// FLUSH events arrive when raw shards flush
// =====================================================================

#[test]
fn event_socket_flush_events() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, socket_path, _server, rt) =
        setup_cluster_with_socket(dir.path());
    let secret = "test-secret-key";

    let events: Arc<std::sync::Mutex<Vec<StoreEvent>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);

    let handle = rt.block_on(async {
        subscribe_store_events(&socket_path, secret, move |ev| {
            events_clone.lock().unwrap().push(ev);
        })
        .await
        .unwrap()
    });

    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    // PUT an object so there is data to flush.
    rt.block_on(async {
        cluster
            .put(
                &Path::from("flush/test.bin"),
                PutPayload::from(Bytes::from(vec![0xAA; 4096])),
            )
            .await
            .unwrap();
    });

    // Flush all raw shards -- should produce FLUSH events.
    flush_all(&raws);

    // Allow events to propagate.
    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    let collected = events.lock().unwrap();
    let flush_events: Vec<&StoreEvent> = collected
        .iter()
        .filter(|e| matches!(e, StoreEvent::Flush { .. }))
        .collect();

    assert!(
        !flush_events.is_empty(),
        "expected at least one FLUSH event after flushing raw shards"
    );

    // Each FLUSH should have a valid shard_id (0, 1, or 2).
    for ev in &flush_events {
        if let StoreEvent::Flush { shard_id, .. } = ev {
            assert!(
                *shard_id < 3,
                "FLUSH shard_id {} out of range",
                shard_id
            );
        }
    }

    handle.abort();
}

// =====================================================================
// Authentication rejection with wrong secret
// =====================================================================

#[test]
fn event_socket_rejects_bad_secret() {
    let dir = tempfile::tempdir().unwrap();
    let (_cluster, _raws, socket_path, _server, rt) =
        setup_cluster_with_socket(dir.path());

    let result = rt.block_on(async {
        subscribe_store_events(&socket_path, "wrong-secret!", |_ev| {})
            .await
    });

    assert!(
        result.is_err(),
        "subscribing with wrong secret should fail"
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::PermissionDenied,
        "error should be PermissionDenied"
    );
}

// =====================================================================
// Multiple concurrent subscribers
// =====================================================================

#[test]
fn event_socket_multiple_subscribers() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, socket_path, _server, rt) =
        setup_cluster_with_socket(dir.path());
    let secret = "test-secret-key";

    // Create 3 independent subscribers.
    let mut handles = Vec::new();
    let mut event_vecs = Vec::new();

    for _ in 0..3 {
        let evts: Arc<std::sync::Mutex<Vec<StoreEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let evts_clone = Arc::clone(&evts);
        let h = rt.block_on(async {
            subscribe_store_events(&socket_path, secret, move |ev| {
                evts_clone.lock().unwrap().push(ev);
            })
            .await
            .unwrap()
        });
        handles.push(h);
        event_vecs.push(evts);
    }

    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    // PUT 3 objects.
    rt.block_on(async {
        for i in 0..3 {
            let key = format!("multi/obj_{}.bin", i);
            cluster
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![i as u8; 256])),
                )
                .await
                .unwrap();
        }
    });

    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    // Each subscriber should have received all 3 PUT events.
    for (i, evts) in event_vecs.iter().enumerate() {
        let collected = evts.lock().unwrap();
        let put_count = collected
            .iter()
            .filter(|e| matches!(e, StoreEvent::Put { .. }))
            .count();
        assert_eq!(
            put_count, 3,
            "subscriber {} should have received 3 PUT events, got {}",
            i, put_count
        );
    }

    for h in handles {
        h.abort();
    }
    flush_all(&raws);
}

// =====================================================================
// subscribe_streaming_replica -- keeps catalog in sync via events
// =====================================================================

/// Build a 2-shard mirror-mode cluster (rf=2 == shard_count) with event socket.
fn setup_mirror_cluster_with_socket(
    dir: &std::path::Path,
) -> (
    shardedobjstr::ShardedObjectStore,
    Vec<Arc<RawObjectStore>>,
    std::path::PathBuf,
    rawobjstr::event::unix::EventServer,
    tokio::runtime::Runtime,
) {
    let shard_size: u64 = 64 * 1024 * 1024;
    let raws = vec![
        format_shard(&dir.join("m0.raw"), shard_size),
        format_shard(&dir.join("m1.raw"), shard_size),
    ];
    let stores: Vec<Arc<dyn ObjectStore>> =
        raws.iter().map(|s| Arc::clone(s) as Arc<dyn ObjectStore>).collect();
    // Mirror mode: rf == shard_count
    let cluster = shardedobjstr::ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    let raw_pairs: Vec<(usize, Arc<RawObjectStore>)> = raws
        .iter()
        .enumerate()
        .map(|(i, s)| (i, Arc::clone(s)))
        .collect();

    let socket_path = dir.join("mirror_events.sock");
    let secret = "mirror-secret";
    let _guard = rt.enter();
    let (_bus, server) = setup_event_socket(
        &cluster,
        &raw_pairs,
        &socket_path,
        secret,
        16,
        None,
    )
    .unwrap();

    (cluster, raws, socket_path, server, rt)
}

#[test]
fn streaming_replica_catalog_sync() {
    use shardedobjstr::event::subscribe_streaming_replica;

    let dir = tempfile::tempdir().unwrap();
    let (writer, raws, socket_path, _server, rt) =
        setup_mirror_cluster_with_socket(dir.path());
    let secret = "mirror-secret";

    // Build a read-only replica cluster pointing at the same underlying shards.
    let stores: Vec<Arc<dyn ObjectStore>> =
        raws.iter().map(|s| Arc::clone(s) as Arc<dyn ObjectStore>).collect();
    let replica = Arc::new(
        shardedobjstr::ShardedObjectStore::new(stores, 2)
            .with_read_only(true),
    );
    // Replica catalog starts empty.
    assert_eq!(replica.catalog().len(), 0);

    let replica_clone = Arc::clone(&replica);
    let sock_str = socket_path.to_str().unwrap().to_string();

    let sub_handle = rt.block_on(async {
        subscribe_streaming_replica(&sock_str, secret, replica_clone)
            .await
            .unwrap()
    });

    // Wait for the subscriber to connect.
    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    });

    // PUT 3 objects on the writer.
    rt.block_on(async {
        for i in 0..3 {
            let key = format!("sr/obj_{}.bin", i);
            writer
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(Bytes::from(vec![i as u8; 1024])),
                )
                .await
                .unwrap();
        }
    });

    // Allow events to propagate and HEAD calls to complete.
    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    });

    // Replica catalog should have all 3 objects.
    assert_eq!(
        replica.catalog().len(),
        3,
        "replica catalog should have 3 entries after PUT events"
    );
    for i in 0..3 {
        let key = format!("sr/obj_{}.bin", i);
        let entry = replica.catalog().get(&key);
        assert!(entry.is_some(), "replica should have entry for {}", key);
        assert_eq!(entry.unwrap().size, 1024, "size should match");
    }

    // DELETE 1 object on the writer.
    rt.block_on(async {
        writer
            .delete(&Path::from("sr/obj_0.bin"))
            .await
            .unwrap();
    });

    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    });

    // Replica should have removed the deleted key from catalog.
    assert!(
        replica.catalog().get("sr/obj_0.bin").is_none(),
        "replica should not have entry for deleted object"
    );
    assert_eq!(
        replica.catalog().len(),
        2,
        "replica catalog should have 2 entries after DELETE"
    );

    sub_handle.abort();
    flush_all(&raws);
}

#[test]
fn streaming_replica_rejects_non_mirror_cluster() {
    use shardedobjstr::event::subscribe_streaming_replica;

    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let raws = vec![
        format_shard(&dir.path().join("r0.raw"), shard_size),
        format_shard(&dir.path().join("r1.raw"), shard_size),
        format_shard(&dir.path().join("r2.raw"), shard_size),
    ];
    let stores: Vec<Arc<dyn ObjectStore>> =
        raws.iter().map(|s| Arc::clone(s) as Arc<dyn ObjectStore>).collect();
    // rf=2 with 3 shards: NOT mirror mode -> should reject
    let cluster = Arc::new(shardedobjstr::ShardedObjectStore::new(stores, 2));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(async {
        subscribe_streaming_replica("/tmp/nonexistent.sock", "secret", cluster).await
    });

    assert!(result.is_err(), "should reject non-mirror cluster");
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        err.to_string().contains("mirror mode"),
        "error should mention mirror mode: {}",
        err
    );

    flush_all(&raws);
}
