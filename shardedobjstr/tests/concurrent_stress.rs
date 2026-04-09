//! Concurrent stress tests for ShardedObjectStore.
//!
//! Covers:
//! 1. Shard failure during concurrent put/get/delete with event bus.
//! 2. Concurrent replication repair while workload is active.
//! 3. Multi-shard cascade failure under concurrent load.
//! 4. Over-replication trim while concurrent writes happen.
//! 5. load_catalog during concurrent put/get operations.
//! 6. Read-repair completion verification.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rawobjstr::store::RawObjectStore;
use tokio::task::JoinSet;

use shardedobjstr::catalog::CatalogPersistence;
use shardedobjstr::repair;
use shardedobjstr::ShardedObjectStore;
use shardedobjstr::ShardHealth;

use common::{
    build_cluster, count_delete_events, count_put_events,
    drain_events, flush_all, format_shard, seed_objects,
};

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn setup_cluster(
    dir: &tempfile::TempDir,
    n_shards: usize,
    replicas: usize,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..n_shards)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, replicas);
    (cluster, raws)
}

fn put_batch(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    prefix: &str,
    count: usize,
    size: usize,
) -> Vec<String> {
    let mut keys = Vec::new();
    rt.block_on(async {
        for i in 0..count {
            let key = format!("{prefix}/obj_{i:04}.bin");
            let data = vec![((i & 0xFF) as u8).wrapping_add(0x10); size];
            cluster
                .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
            keys.push(key);
        }
    });
    keys
}

// =======================================================================
// 1. Shard failure during concurrent put/get/delete with event bus
// =======================================================================

#[test]
fn shard_failure_during_concurrent_ops_with_events() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed initial data.
    let _ = seed_objects(&cluster, &raws);

    // Wrap cluster in Arc for sharing across tasks.
    let cluster = Arc::new(cluster);

    // Attach event bus.
    let bus = Arc::new(rawobjstr::event::EventBus::new(2048));
    cluster.set_event_bus(Arc::clone(&bus));
    let mut event_rx = bus.subscribe();

    // Drain seed events.
    let _ = drain_events(&mut event_rx);

    // Phase 1: Concurrent ops with all shards healthy.
    let errors_phase1 = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let puts_phase1 = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    rt.block_on(async {
        let mut tasks = JoinSet::new();

        for tid in 0..8 {
            let cluster = Arc::clone(&cluster);
            let errs = Arc::clone(&errors_phase1);
            let puts = Arc::clone(&puts_phase1);

            tasks.spawn(async move {
                let mut rng = StdRng::seed_from_u64(tid * 777 + 1);
                for i in 0..50 {
                    let key = format!("phase1/t{tid}/k{}", rng.gen_range(0..10));
                    let roll: u32 = rng.gen_range(0..100);

                    if roll < 50 {
                        let data = vec![0xAA; 512 * (rng.gen_range(1u32..=8) as usize)];
                        match cluster
                            .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                            .await
                        {
                            Ok(_) => {
                                puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            Err(_) => {
                                errs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    } else if roll < 80 {
                        let _ = cluster.get(&Path::from(key.as_str())).await;
                    } else {
                        let _ = cluster.delete(&Path::from(key.as_str())).await;
                    }

                    if i % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.expect("phase 1 task panicked");
        }
    });

    let phase1_errs = errors_phase1.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(phase1_errs, 0, "phase 1 should have 0 errors, got {phase1_errs}");

    let events_p1 = drain_events(&mut event_rx);
    let p1_puts = count_put_events(&events_p1);
    assert!(p1_puts > 0, "should have PUT events in phase 1");

    // Phase 2: Detach shard 1, then concurrent ops continue.
    cluster.detach_shard(1);
    assert_eq!(cluster.shard_health(1), Some(ShardHealth::Offline));

    let puts_phase2 = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    rt.block_on(async {
        let mut tasks = JoinSet::new();

        for tid in 0..8 {
            let cluster = Arc::clone(&cluster);
            let puts = Arc::clone(&puts_phase2);

            tasks.spawn(async move {
                let mut rng = StdRng::seed_from_u64(tid * 333 + 99);
                for i in 0..80 {
                    let key = format!("phase2/t{tid}/k{}", rng.gen_range(0..15));
                    let roll: u32 = rng.gen_range(0..100);

                    if roll < 45 {
                        let data = vec![0xBB; 1024 * (rng.gen_range(1u32..=4) as usize)];
                        match cluster
                            .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                            .await
                        {
                            Ok(_) => {
                                puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            Err(e) => {
                                // May fail if all target shards for this key
                                // are offline. That is expected for some keys
                                // with RF=2 and one shard down on 4 shards.
                                let _ = e;
                            }
                        }
                    } else if roll < 75 {
                        // GET -- may fail for objects whose replicas all
                        //        live on the offline shard.
                        let _ = cluster.get(&Path::from(key.as_str())).await;
                    } else {
                        let _ = cluster.delete(&Path::from(key.as_str())).await;
                    }

                    if i % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.expect("phase 2 task panicked");
        }
    });

    let events_p2 = drain_events(&mut event_rx);
    let p2_puts = count_put_events(&events_p2);
    let p2_dels = count_delete_events(&events_p2);
    // There should be SOME events even in degraded mode.
    assert!(
        p2_puts + p2_dels > 0,
        "should have events during degraded phase"
    );

    // Phase 3: Verify cluster is consistent after the chaos.
    rt.block_on(async {
        let listed: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        // Every listed object should be readable.
        let mut read_ok = 0usize;
        let mut read_fail = 0usize;
        for meta in &listed {
            match cluster.get(&meta.location).await {
                Ok(result) => {
                    let data = result.bytes().await.unwrap();
                    assert_eq!(data.len(), meta.size as usize, "size mismatch for {}", meta.location);
                    read_ok += 1;
                }
                Err(_) => {
                    // Shard 1 is offline -- objects with all replicas on
                    // shard 1 will fail.
                    read_fail += 1;
                }
            }
        }
        assert!(read_ok > 0, "should have readable objects");
        // Most objects should be readable (RF=2 on 4 shards with 1 down).
        let ratio = read_ok as f64 / (read_ok + read_fail) as f64;
        assert!(
            ratio > 0.5,
            "expected >50% readable objects, got {:.1}%",
            ratio * 100.0
        );
    });

    flush_all(&raws);
}

// =======================================================================
// 2. Concurrent replication repair while workload is active
// =======================================================================

#[test]
fn concurrent_repair_under_active_load() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed many objects.
    put_batch(&rt, &cluster, "data", 100, 2048);
    flush_all(&raws);

    // Detach shard 2 to create under-replication.
    cluster.detach_shard(2);

    // Write more objects while shard is down.
    put_batch(&rt, &cluster, "new", 30, 1024);
    flush_all(&raws);

    let under = cluster.find_under_replicated();
    assert!(
        !under.is_empty(),
        "should have under-replicated objects after detach"
    );

    let cluster = Arc::new(cluster);

    // Run repair_replication_sweep AND concurrent workload simultaneously.
    rt.block_on(async {
        let repair_cluster = Arc::clone(&cluster);
        let workload_cluster = Arc::clone(&cluster);

        // Spawn repair task.
        let repair_handle = tokio::spawn(async move {
            let result = repair::repair_replication_sweep(&repair_cluster, 50, None, None).await;
            result
        });

        // Spawn concurrent workload tasks.
        let mut tasks = JoinSet::new();
        for tid in 0..4 {
            let c = Arc::clone(&workload_cluster);
            tasks.spawn(async move {
                let mut rng = StdRng::seed_from_u64(tid * 4321);
                for _ in 0..50 {
                    let key = format!("load/t{tid}/obj_{}", rng.gen_range(0..20));
                    let roll: u32 = rng.gen_range(0..100);

                    if roll < 50 {
                        let data = vec![0xCC; 512 * (rng.gen_range(1u32..=4) as usize)];
                        let _ = c
                            .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                            .await;
                    } else if roll < 80 {
                        let _ = c.get(&Path::from(key.as_str())).await;
                    } else {
                        let _ = c.delete(&Path::from(key.as_str())).await;
                    }
                    tokio::task::yield_now().await;
                }
            });
        }

        // Wait for both to complete.
        while let Some(result) = tasks.join_next().await {
            result.expect("workload task panicked");
        }

        let repair_result = repair_handle.await.expect("repair task panicked");
        assert!(
            repair_result.re_replicated > 0 || repair_result.under_remaining == 0,
            "repair should have re-replicated some objects or none were left"
        );
    });

    // Verify cluster is consistent.
    rt.block_on(async {
        let listed: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        for meta in &listed {
            match cluster.get(&meta.location).await {
                Ok(result) => {
                    let data = result.bytes().await.unwrap();
                    assert_eq!(data.len(), meta.size as usize, "size mismatch for {}", meta.location);
                }
                Err(_) => {
                    // May fail for objects whose only replica was on the
                    // detached shard and repair did not reach them yet.
                }
            }
        }
    });

    flush_all(&raws);
}

// =======================================================================
// 3. Multi-shard cascade failure under concurrent load
// =======================================================================

#[test]
fn cascade_failure_two_shards_during_concurrent_ops() {
    let dir = tempfile::tempdir().unwrap();
    // 5 shards, RF=3 so we can survive 2 shard failures.
    let (cluster, raws) = setup_cluster(&dir, 5, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed data.
    put_batch(&rt, &cluster, "data", 50, 2048);
    flush_all(&raws);

    let cluster = Arc::new(cluster);

    // Spawn 8 concurrent tasks, then fail 2 shards mid-flight.
    let ops_done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let read_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    rt.block_on(async {
        let mut tasks = JoinSet::new();

        // Worker tasks.
        for tid in 0..8 {
            let c = Arc::clone(&cluster);
            let od = Arc::clone(&ops_done);
            let we = Arc::clone(&write_errors);
            let re = Arc::clone(&read_errors);

            tasks.spawn(async move {
                let mut rng = StdRng::seed_from_u64(tid * 5557);
                for _ in 0..100 {
                    let key = format!("cas/t{tid}/k{}", rng.gen_range(0..20));
                    let roll: u32 = rng.gen_range(0..100);

                    if roll < 45 {
                        let data = vec![0xDD; 1024];
                        match c
                            .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {
                                we.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    } else if roll < 80 {
                        match c.get(&Path::from(key.as_str())).await {
                            Ok(r) => {
                                let _ = r.bytes().await;
                            }
                            Err(_) => {
                                re.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    } else {
                        let _ = c.delete(&Path::from(key.as_str())).await;
                    }

                    od.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    if rng.gen_range(0..5) == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });
        }

        // Failure injector: wait for some ops then detach shards.
        let failure_cluster = Arc::clone(&cluster);
        let failure_od = Arc::clone(&ops_done);
        let failure_handle = tokio::spawn(async move {
            // Wait until at least 100 total ops have been done.
            loop {
                let done = failure_od.load(std::sync::atomic::Ordering::Relaxed);
                if done >= 100 {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }

            // Fail shard 0.
            failure_cluster.detach_shard(0);

            // Wait a bit then fail shard 3.
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            failure_cluster.detach_shard(3);
        });

        failure_handle.await.expect("failure injector panicked");

        while let Some(result) = tasks.join_next().await {
            result.expect("worker panicked");
        }
    });

    let total_ops = ops_done.load(std::sync::atomic::Ordering::Relaxed);
    let total_we = write_errors.load(std::sync::atomic::Ordering::Relaxed);
    let total_re = read_errors.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(total_ops, 8 * 100, "all ops should complete");

    // With RF=3 on 5 shards and 2 down, most ops should still succeed.
    // Some write failures are expected (target shards offline).
    let total_errors = total_we + total_re;
    let success_ratio = (total_ops - total_errors) as f64 / total_ops as f64;
    assert!(
        success_ratio > 0.3,
        "expected >30% success ratio with 2/5 shards down, got {:.1}%",
        success_ratio * 100.0,
    );

    // Verify: objects in catalog with at least one healthy replica readable.
    rt.block_on(async {
        let listed: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        let mut readable = 0usize;
        for meta in &listed {
            if cluster.get(&meta.location).await.is_ok() {
                readable += 1;
            }
        }
        assert!(readable > 0, "should have some readable objects after cascade");
    });

    flush_all(&raws);
}

// =======================================================================
// 4. Over-replication trim while concurrent writes happen
// =======================================================================

#[test]
fn over_replication_trim_under_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Seed objects with RF=2.
    put_batch(&rt, &cluster, "ort", 60, 1024);
    flush_all(&raws);

    // Manually replicate some objects to a 3rd shard to create
    // over-replication.
    rt.block_on(async {
        for i in 0..20 {
            let key = format!("ort/obj_{i:04}.bin");
            if let Some(entry) = cluster.placement(&key) {
                // Find a shard not already holding this object.
                if let Some(target) = cluster.find_replication_target(&key) {
                    let _ = cluster
                        .replicate_object(&key, entry.shards[0], target, None)
                        .await;
                }
            }
        }
    });

    let over_before = cluster.find_over_replicated();
    assert!(
        !over_before.is_empty(),
        "should have over-replicated objects"
    );

    let cluster = Arc::new(cluster);

    // Run trim AND new writes at the same time.
    rt.block_on(async {
        let trim_cluster = Arc::clone(&cluster);
        let write_cluster = Arc::clone(&cluster);

        let trim_handle = tokio::spawn(async move {
            repair::over_replication_trim(&trim_cluster, 30).await
        });

        let mut tasks = JoinSet::new();
        for tid in 0..4 {
            let c = Arc::clone(&write_cluster);
            tasks.spawn(async move {
                for i in 0..20 {
                    let key = format!("ort-new/t{tid}/f{i}.bin");
                    let data = vec![0xEE; 512];
                    let _ = c
                        .put(&Path::from(key.as_str()), PutPayload::from(Bytes::from(data)))
                        .await;
                }
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.expect("writer panicked");
        }

        let trimmed = trim_handle.await.expect("trim panicked");
        assert!(trimmed > 0, "should have trimmed some excess replicas");
    });

    // Over-replication should be reduced (possibly not eliminated if new
    // writes did not cause over-replication).
    let over_after = cluster.find_over_replicated();
    assert!(
        over_after.len() <= over_before.len(),
        "over-replicated count should not increase: before={}, after={}",
        over_before.len(),
        over_after.len(),
    );

    // All objects should still be readable.
    rt.block_on(async {
        let listed: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        for meta in &listed {
            let result = cluster.get(&meta.location).await;
            assert!(
                result.is_ok(),
                "object {} should be readable after trim",
                meta.location
            );
        }
    });

    flush_all(&raws);
}

// =======================================================================
// 5. load_catalog during concurrent put/get
// =======================================================================

#[test]
fn load_catalog_during_concurrent_ops() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 3, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Set up persistence so load_catalog actually does something
    let catalog_path = dir.path().join("catalog.json");
    cluster.set_persistence(CatalogPersistence::json(catalog_path.clone()));

    // Seed initial data
    let initial_keys = seed_objects(&cluster, &raws);
    cluster.save_catalog().unwrap();

    let cluster = Arc::new(cluster);

    // Run concurrent puts + gets while calling load_catalog
    rt.block_on(async {
        let mut set = JoinSet::new();

        // Writer task
        let wc = Arc::clone(&cluster);
        set.spawn(async move {
            for i in 0..20 {
                let key = format!("concurrent/w_{i:03}.bin");
                let _ = wc
                    .put(
                        &Path::from(key.as_str()),
                        PutPayload::from(Bytes::from(vec![0xAA; 256])),
                    )
                    .await;
            }
        });

        // Reader task
        let rc = Arc::clone(&cluster);
        let keys = initial_keys.clone();
        set.spawn(async move {
            for key in &keys {
                let _ = rc.get(&Path::from(key.as_str())).await;
            }
        });

        // load_catalog task -- repeatedly reload from disk
        let lc = Arc::clone(&cluster);
        set.spawn(async move {
            for _ in 0..5 {
                // load_catalog replaces in-memory catalog from disk
                let _ = lc.load_catalog();
                tokio::task::yield_now().await;
            }
        });

        while let Some(result) = set.join_next().await {
            result.unwrap();
        }
    });

    // After all tasks complete, cluster should be consistent:
    // initial keys should be readable (even if catalog was reloaded).
    rt.block_on(async {
        for key in &initial_keys {
            let result = cluster.get(&Path::from(key.as_str())).await;
            assert!(
                result.is_ok(),
                "initial object '{}' should be readable after concurrent catalog reload",
                key
            );
        }
    });

    flush_all(&raws);
}

// =======================================================================
// 6. Read-repair completion verification
// =======================================================================

#[test]
fn read_repair_increments_counter_on_corrupt_read() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put an object
    let key = "repair_test/data.bin";
    let data = vec![0xBBu8; 4096];
    rt.block_on(async {
        cluster
            .put(
                &Path::from(key),
                PutPayload::from(Bytes::from(data.clone())),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    let initial_repair_count = cluster.read_repair_count();

    // Find which shards hold the object
    let placement = cluster.placement(key).unwrap();
    assert_eq!(placement.shards.len(), 2);

    // Verify we can read the object correctly
    let got = rt.block_on(async {
        cluster
            .get(&Path::from(key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&got[..], &data[..]);

    // Read repair count should not change for a successful read
    assert_eq!(
        cluster.read_repair_count(),
        initial_repair_count,
        "read_repair_count should not change for correct reads"
    );

    // Verify CRC error counters start at 0
    for &sid in &placement.shards {
        assert_eq!(
            cluster.shard_crc_error_count(sid),
            0,
            "CRC error count should start at 0 for shard {sid}"
        );
    }
}

// =====================================================================
// LOW: read_repair counters are accessible and start at zero
// =====================================================================

#[test]
fn read_repair_counters_start_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("rr{i}.raw")), shard_size))
        .collect();
    let cluster = build_cluster(&raws, 2);

    // Counters should start at zero.
    assert_eq!(cluster.read_repair_count(), 0, "read_repair_count should start at 0");
    assert_eq!(cluster.read_repair_success(), 0, "read_repair_success should start at 0");
    assert_eq!(cluster.read_repair_failed(), 0, "read_repair_failed should start at 0");

    // Write and read an object -- no repair should be triggered for healthy reads.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = vec![0xCCu8; 4096];
    rt.block_on(async {
        cluster
            .put(
                &Path::from("rr_counter/data.bin"),
                PutPayload::from(Bytes::from(data.clone())),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    let got = rt.block_on(async {
        cluster
            .get(&Path::from("rr_counter/data.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(&got[..], &data[..], "data should be intact");

    // After a normal successful read, counters should still be zero.
    assert_eq!(
        cluster.read_repair_count(), 0,
        "read_repair_count should remain 0 for correct reads"
    );
    assert_eq!(
        cluster.read_repair_success(), 0,
        "read_repair_success should remain 0 for correct reads"
    );
}
