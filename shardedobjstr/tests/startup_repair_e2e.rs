//! E2E tests for startup under-replication repair.
//!
//! Scenario: cluster has a mem shard. After restart the mem shard is empty,
//! so objects that had a replica on it are now under-replicated. The
//! rebuild_catalog + find_under_replicated + replicate_object pipeline
//! should detect and repair them.
//!
//! To simulate a restart we construct a NEW cluster using the same raw
//! store files but a fresh (empty) InMemory shard. This mirrors what
//! really happens when objstrd restarts: raw shards persist on disk and
//! a brand-new InMemory store replaces the old one.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::repair;
use shardedobjstr::ShardedObjectStore;

use common::{flush_all, format_shard, open_shard};

// -- Helpers ---------------------------------------------------------

/// Build a cluster with 2 raw shards + 1 InMemory shard, RF=2.
fn build_raw_plus_mem(
    raws: &[Arc<RawObjectStore>],
) -> ShardedObjectStore {
    let mem = Arc::new(InMemory::new());
    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raws[0].clone() as _,
        raws[1].clone() as _,
        mem as _,
    ];
    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();
    cluster
}

/// Simulate a server restart: re-open raw shards from disk, create a
/// fresh empty InMemory shard, strip mem entries, rebuild catalog.
/// The caller must drop the previous cluster first to release file locks.
fn simulate_restart(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    // Re-open raw stores from disk (they kept their data).
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| open_shard(&dir.path().join(format!("s{i}.raw"))))
        .collect();
    let fresh_mem = Arc::new(InMemory::new());
    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raws[0].clone() as _,
        raws[1].clone() as _,
        fresh_mem as _,
    ];
    let cluster = ShardedObjectStore::new(stores, 2);

    // Simulate what server.rs does on startup:
    // 1. Strip catalog entries for the mem shard (shard 2).
    cluster.catalog().remove_all_for_shard(2);
    // 2. Rebuild catalog from all healthy shards.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    (cluster, raws)
}

/// Put test objects into the cluster.
fn put_objects(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    count: usize,
) {
    rt.block_on(async {
        for i in 0..count {
            let key = format!("startup/obj_{:04}.bin", i);
            let data = vec![(i & 0xFF) as u8; 512];
            cluster
                .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
        }
    });
}

// =====================================================================
// Test: mem shard data loss detected as under-replication after rebuild
// =====================================================================

#[test]
fn mem_shard_loss_detected_after_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_raw_plus_mem(&raws);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write 20 objects at RF=2. Some will land on the mem shard.
    put_objects(&rt, &cluster, 20);
    flush_all(&raws);

    // Confirm everything is fully replicated right now.
    let under_before = cluster.find_under_replicated();
    assert!(
        under_before.is_empty(),
        "no under-replicated objects before mem shard loss"
    );

    // Count how many objects have a replica on shard 2 (mem).
    let on_mem = cluster.catalog().entries_for_shard(2).len();
    // With 3 shards and RF=2, shard 2 should hold a fair share.
    assert!(on_mem > 0, "mem shard should hold some objects");

    // Drop original cluster to release raw file locks.
    drop(cluster);
    drop(raws);

    // --- Simulate restart: new cluster with fresh empty InMemory ---
    let (cluster2, _raws2) = simulate_restart(&dir);

    // Now find_under_replicated should report objects that lost their
    // mem replica and only have 1 healthy copy left.
    let under_after = cluster2.find_under_replicated();
    assert!(
        !under_after.is_empty(),
        "should detect under-replicated objects after mem shard loss"
    );

    println!(
        "objects on mem shard: {}, under-replicated after rebuild: {}",
        on_mem,
        under_after.len()
    );
}

// =====================================================================
// Test: replicate_object restores RF after mem shard loss
// =====================================================================

#[test]
fn replicate_restores_rf_after_mem_loss() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_raw_plus_mem(&raws);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, 30);
    flush_all(&raws);

    // Drop original cluster to release raw file locks.
    drop(cluster);
    drop(raws);

    // Simulate restart: new cluster with fresh empty InMemory.
    let (cluster2, raws2) = simulate_restart(&dir);

    let under = cluster2.find_under_replicated();
    assert!(!under.is_empty(), "should have under-replicated objects");

    // Run repair-replication sweep to fix under-replicated objects.
    let result = rt.block_on(repair::repair_replication_sweep(
        &cluster2, 200, None, None,
    ));
    flush_all(&raws2);

    println!(
        "repaired {} objects (under_remaining={})",
        result.re_replicated, result.under_remaining
    );
    assert!(result.re_replicated > 0, "should have repaired at least one object");

    // After repair, everything should be fully replicated.
    let still_under = cluster2.find_under_replicated();
    assert!(
        still_under.is_empty(),
        "all objects should be fully replicated after repair, still under: {:?}",
        still_under
    );

    // Verify data integrity -- all objects still readable with correct content.
    rt.block_on(async {
        for i in 0..30 {
            let key = format!("startup/obj_{:04}.bin", i);
            let result = cluster2.get(&Path::from(key.as_str())).await.unwrap();
            let body = result.bytes().await.unwrap();
            let expected = vec![(i & 0xFF) as u8; 512];
            assert_eq!(body.as_ref(), expected.as_slice(), "data mismatch for {}", key);
        }
    });
}

// =====================================================================
// Test: detach + rebuild also triggers under-replication detection
// =====================================================================

#[test]
fn detach_shard_triggers_under_replication() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = common::build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, 15);
    flush_all(&raws);

    // Detach shard 1 (simulates it going offline).
    cluster.detach_shard(1);

    let under = cluster.find_under_replicated();
    let on_shard1 = cluster.catalog().entries_for_shard(1).len();

    // All objects that had a copy on shard 1 should be under-replicated.
    println!(
        "entries on shard 1: {}, under-replicated: {}",
        on_shard1,
        under.len()
    );
    assert!(!under.is_empty(), "detaching a shard should cause under-replication");

    // Repair
    let result = rt.block_on(repair::repair_replication_sweep(
        &cluster, 200, None, None,
    ));
    flush_all(&raws);
    println!("repaired {} objects", result.re_replicated);

    let still_under = cluster.find_under_replicated();
    assert!(
        still_under.is_empty(),
        "all objects should be fully replicated after repair"
    );
}
