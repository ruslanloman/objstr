//! Library-level tests for `redistribute_sweep`.
//!
//! This function had ZERO direct library tests -- only the HTTP endpoint
//! was covered by external shell scripts.  These tests exercise the
//! function directly to verify balancing logic, batch limits, tolerance
//! thresholds, pre-condition checks, and data integrity.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::repair::{self, RedistributeResult};
use shardedobjstr::{ShardHealth, ShardedObjectStore};

use common::{build_cluster, flush_all, format_shard};

// -- Helpers ----------------------------------------------------------

fn setup_cluster(
    dir: &tempfile::TempDir,
    shard_count: usize,
    rf: usize,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..shard_count)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, rf);
    (cluster, raws)
}

/// Put objects whose keys hash to a specific shard to create imbalance.
/// With rf=1, each object lands on exactly one shard.  We brute-force
/// keys until shard 0 has a lot more objects than the others.
fn create_imbalanced_cluster(
    cluster: &ShardedObjectStore,
    raws: &[Arc<RawObjectStore>],
    target_shard: usize,
    target_count: usize,
    other_max: usize,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut on_target = 0usize;
    let mut on_other = 0usize;
    let mut i = 0u64;

    rt.block_on(async {
        // Put many objects; keep going until shard 0 has target_count
        // and other shards have at most other_max each.
        // With 3 shards and rf=1, ~1/3 of keys land on each shard.
        // We just put enough objects so all shards get some.
        while on_target < target_count {
            let key = Path::from(format!("imb/obj_{:06}", i));
            let targets = cluster.target_shards(&key);
            if targets.contains(&target_shard) {
                cluster
                    .put(&key, PutPayload::from(Bytes::from(vec![0xAA; 256])))
                    .await
                    .unwrap();
                on_target += 1;
            } else if on_other < other_max * (targets.len()) {
                // Put a few objects on other shards too, but fewer.
                cluster
                    .put(&key, PutPayload::from(Bytes::from(vec![0xBB; 256])))
                    .await
                    .unwrap();
                on_other += 1;
            }
            i += 1;
            if i > 100_000 {
                break; // safety valve
            }
        }
    });

    flush_all(raws);
}

/// Put N objects spread across all shards (rf=1).
fn put_objects(cluster: &ShardedObjectStore, raws: &[Arc<RawObjectStore>], count: usize) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for i in 0..count {
            let key = Path::from(format!("obj/item_{:04}", i));
            let data = Bytes::from(format!("data-for-{}", i).into_bytes());
            cluster.put(&key, PutPayload::from(data)).await.unwrap();
        }
    });
    flush_all(raws);
}

// =====================================================================
// Test 1: redistribute_sweep balances uneven shard counts
// =====================================================================

#[test]
fn redistribute_balances_uneven_shards() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create imbalance: shard 0 gets ~30 objects, others get ~5 each.
    create_imbalanced_cluster(&cluster, &raws, 0, 30, 5);

    let counts_before = cluster.shard_object_counts();
    let max_before = counts_before.iter().map(|(_, c)| *c).max().unwrap();
    let min_before = counts_before.iter().map(|(_, c)| *c).min().unwrap();
    assert!(
        max_before - min_before > 5,
        "pre-condition: shards should be imbalanced (max={max_before} min={min_before})"
    );

    let result: RedistributeResult = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.1, None, None,
    ));

    assert_eq!(result.errors, 0, "redistribute should have no errors");
    assert!(result.moved > 0, "redistribute should move at least 1 object");

    // After redistribute, max-min should be within tolerance.
    let total: usize = result.shard_counts.iter().map(|(_, c)| *c).sum();
    let mean = total as f64 / result.shard_counts.len() as f64;
    let max_after = result.shard_counts.iter().map(|(_, c)| *c).max().unwrap();
    let min_after = result.shard_counts.iter().map(|(_, c)| *c).min().unwrap();
    let imbalance = if mean > 0.0 {
        (max_after - min_after) as f64 / mean
    } else {
        0.0
    };
    assert!(
        imbalance <= 0.15,
        "shards should be balanced: max={max_after} min={min_after} imbalance={imbalance:.2}"
    );
}

// =====================================================================
// Test 2: redistribute is a no-op when already balanced
// =====================================================================

#[test]
fn redistribute_noop_when_balanced() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects that naturally spread across shards via jump hash.
    put_objects(&cluster, &raws, 60);

    let result = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.5, None, None,
    ));

    // With 60 objects across 3 shards, jump hash distributes ~20 each.
    // Tolerance 0.5 (50%) is very generous so no moves needed.
    assert_eq!(
        result.moved, 0,
        "no moves needed when already balanced (tolerance=50%)"
    );
    assert_eq!(result.errors, 0);
}

// =====================================================================
// Test 3: redistribute aborts when under-replicated objects exist
// =====================================================================

#[test]
fn redistribute_aborts_with_under_replicated() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects with rf=2.
    put_objects(&cluster, &raws, 20);

    // Detach shard 0 to create under-replication.
    cluster.detach_shard(0);

    let under = cluster.find_under_replicated();
    assert!(!under.is_empty(), "should have under-replicated objects");

    let result = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.1, None, None,
    ));

    assert_eq!(
        result.moved, 0,
        "redistribute must abort when under-replicated objects exist"
    );
    assert_eq!(result.errors, 0, "aborting is not an error");
}

// =====================================================================
// Test 4: redistribute respects batch_size limit
// =====================================================================

#[test]
fn redistribute_respects_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create heavy imbalance.
    create_imbalanced_cluster(&cluster, &raws, 0, 30, 3);

    // batch_size=5 limits how many moves happen.
    let result = rt.block_on(repair::redistribute_sweep(&cluster, 5, 0.01, None, None,
    ));

    assert_eq!(result.errors, 0);
    assert!(
        result.moved <= 5,
        "moved={} should respect batch_size=5",
        result.moved,
    );
}

// =====================================================================
// Test 5: redistribute preserves data integrity
// =====================================================================

#[test]
fn redistribute_preserves_data_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create imbalance with known data.
    create_imbalanced_cluster(&cluster, &raws, 0, 25, 5);

    // Record all objects and their data before redistribute.
    let all_keys: Vec<String> = {
        let entries = cluster.catalog().all_entries();
        entries.into_iter().map(|(k, _)| k).collect()
    };

    let data_before: Vec<(String, Bytes)> = rt.block_on(async {
        let mut out = Vec::new();
        for key in &all_keys {
            let result = cluster.get(&Path::from(key.as_str())).await.unwrap();
            let body = result.bytes().await.unwrap();
            out.push((key.clone(), body));
        }
        out
    });

    let result = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.1, None, None,
    ));
    assert_eq!(result.errors, 0);

    // Verify all objects are still readable with correct data.
    rt.block_on(async {
        for (key, expected) in &data_before {
            let result = cluster.get(&Path::from(key.as_str())).await;
            assert!(
                result.is_ok(),
                "object {} should still be readable after redistribute",
                key,
            );
            let body = result.unwrap().bytes().await.unwrap();
            assert_eq!(
                &body, expected,
                "data mismatch for {} after redistribute",
                key,
            );
        }
    });
}

// =====================================================================
// Test 6: redistribute with fewer than 2 healthy shards is a no-op
// =====================================================================

#[test]
fn redistribute_noop_with_one_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&cluster, &raws, 10);

    // Detach 2 of 3 shards, leaving only 1 healthy.
    cluster.detach_shard(1);
    cluster.detach_shard(2);

    let result = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.1, None, None,
    ));

    assert_eq!(result.moved, 0, "cannot redistribute with <2 healthy shards");
    assert_eq!(result.errors, 0);
}

// =====================================================================
// Test 7: redistribute with zero objects is a no-op
// =====================================================================

#[test]
fn redistribute_noop_empty_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_cluster(&dir, 3, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let result = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.1, None, None,
    ));

    assert_eq!(result.moved, 0, "nothing to move in empty cluster");
    assert_eq!(result.errors, 0);
}

// =====================================================================
// Test 8: redistribute with rf=2 moves objects safely
// =====================================================================

#[test]
fn redistribute_with_replication_preserves_rf() {
    let dir = tempfile::tempdir().unwrap();
    // 4 shards, rf=2
    let (cluster, raws) = setup_cluster(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put objects; with rf=2 each object is on 2 shards.
    put_objects(&cluster, &raws, 40);

    let result = rt.block_on(repair::redistribute_sweep(&cluster, 200, 0.05, None, None,
    ));

    assert_eq!(result.errors, 0);

    // After redistribute, every object should still have rf=2.
    let entries = cluster.catalog().all_entries();
    for (key, entry) in &entries {
        let healthy_copies: usize = entry
            .shards
            .iter()
            .filter(|&&s| cluster.shard_health(s) == Some(ShardHealth::Healthy))
            .count();
        assert!(
            healthy_copies >= 2,
            "object {} has only {} healthy copies after redistribute (expected >=2)",
            key,
            healthy_copies,
        );
    }
}
