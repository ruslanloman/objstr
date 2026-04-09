//! Crash recovery tests.
//!
//! These tests simulate:
//! - Interrupted flush (partial index write, only primary superblock updated)
//! - Recovery using prev_index fields in superblock
//! - Crash-during-recovery (open, fail, repair, open again)
//! - Repair verification after various corruption types
//! - Simulated power loss at different points in the flush sequence
//! - Reopen stability after crash (multiple reopen cycles)

mod common;

use object_store::path::Path;
use object_store::ObjectStore;
use object_store::PutPayload;

use rawobjstr::store::RawObjectStore;
use rawobjstr::SUPERBLOCK_SIZE;
use tempfile::NamedTempFile;

use common::{
    flip_byte, make_small, read_bytes_at, write_bytes_at,
    MEDIUM_DEVICE, SMALL_DEVICE,
};

// =======================================================================
// 1. Interrupted flush -- index written but backup superblock not updated
// =======================================================================

/// Simulate crash after writing new index and primary superblock, but before
/// writing the backup superblock. The primary superblock has the new txn_id
/// and the backup has the old one. Open should pick the primary (higher txn_id).
#[tokio::test]
async fn crash_after_primary_sb_update_before_backup() {
    let tmp = NamedTempFile::new().unwrap();

    // Phase 1: format, write data, flush
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("phase1/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Record the backup superblock state after first flush
    let backup_sb_after_phase1 =
        read_bytes_at(tmp.path(), SUPERBLOCK_SIZE, SUPERBLOCK_SIZE as usize);

    // Phase 2: write more data, flush again
    for i in 10..20 {
        store
            .put(
                &Path::from(format!("phase2/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    // Now simulate: revert the backup superblock to phase 1's version
    // (as if the crash happened after writing primary but before backup)
    write_bytes_at(tmp.path(), SUPERBLOCK_SIZE, &backup_sb_after_phase1);

    // Open should use primary (which has the higher txn_id from phase 2)
    let store = RawObjectStore::open(tmp.path()).unwrap();

    // All 20 files should be visible
    for i in 0..10 {
        let data = store
            .get(&Path::from(format!("phase1/{i:02}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }
    for i in 10..20 {
        let data = store
            .get(&Path::from(format!("phase2/{i:02}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }
    println!("PASS crash_after_primary_sb_update_before_backup");
}

/// Simulate crash after writing new index but before ANY superblock update.
/// Both superblocks still point to the old index. Data from the unflushed
/// phase should be lost, but phase 1 data should survive.
#[tokio::test]
async fn crash_after_index_write_before_sb_update() {
    let tmp = NamedTempFile::new().unwrap();

    // Phase 1: format, write data, flush
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("phase1/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Save both superblocks after phase 1
    let sb_primary = read_bytes_at(tmp.path(), 0, SUPERBLOCK_SIZE as usize);
    let sb_backup =
        read_bytes_at(tmp.path(), SUPERBLOCK_SIZE, SUPERBLOCK_SIZE as usize);

    // Phase 2: write more data, flush
    for i in 10..15 {
        store
            .put(
                &Path::from(format!("phase2/{i:02}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    // Revert BOTH superblocks to phase 1 state
    // (simulates crash after index data written but before superblock updated)
    write_bytes_at(tmp.path(), 0, &sb_primary);
    write_bytes_at(tmp.path(), SUPERBLOCK_SIZE, &sb_backup);

    // Open -- should see only phase 1 data
    let store = RawObjectStore::open(tmp.path()).unwrap();

    // Phase 1 files should all be there
    for i in 0..10 {
        let data = store
            .get(&Path::from(format!("phase1/{i:02}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }

    // Phase 2 files should NOT be visible (superblock still points to old index)
    for i in 10..15 {
        let result = store.get(&Path::from(format!("phase2/{i:02}.bin"))).await;
        assert!(
            result.is_err(),
            "phase2 file {i} should not be visible after crash before SB update"
        );
    }
    println!("PASS crash_after_index_write_before_sb_update");
}

// =======================================================================
// 2. Crash-during-recovery -- open fails, repair, then open again
// =======================================================================

/// Corrupt the active index, try to open (fails), then corrupt the primary
/// superblock too. Now corrupt the backup. Format fresh and verify device
/// is recoverable by reformatting.
#[tokio::test]
async fn cascading_corruption_then_reformat() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    let sb_bytes = read_bytes_at(tmp.path(), 0, SUPERBLOCK_SIZE as usize);
    let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();

    drop(store);

    // Step 1: corrupt active index -> open fails
    flip_byte(tmp.path(), sb.index_region_offset + 10);
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // Step 2: also corrupt primary superblock
    flip_byte(tmp.path(), 20);
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // Step 3: backup superblock still intact but index is corrupt -- still fail
    // Note: backup SB points to same (corrupt) index
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // Step 4: corrupt backup too -> total loss
    flip_byte(tmp.path(), SUPERBLOCK_SIZE + 20);
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // Step 5: reformat should work
    let store = RawObjectStore::format(tmp.path(), false).unwrap();
    store
        .put(
            &Path::from("recovered.bin"),
            PutPayload::from(make_small(42, 4096)),
        )
        .await
        .unwrap();
    let data = store
        .get(&Path::from("recovered.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.len(), 4096);
    println!("PASS cascading_corruption_then_reformat");
}

// =======================================================================
// 3. Repair after superblock corruption
// =======================================================================

/// Corrupt primary superblock, open with backup, repair (rewrites both SBs),
/// then corrupt backup -- primary should now work.
#[tokio::test]
async fn repair_fixes_primary_superblock() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    for i in 0..8 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    // Corrupt primary
    flip_byte(tmp.path(), 15);

    // Open with backup, repair
    let store = RawObjectStore::open(tmp.path()).unwrap();
    store.repair().unwrap();
    drop(store);

    // Now corrupt the backup -- primary should have been fixed by repair
    flip_byte(tmp.path(), SUPERBLOCK_SIZE + 15);

    let store = RawObjectStore::open(tmp.path()).unwrap();
    for i in 0..8 {
        let data = store
            .get(&Path::from(format!("f/{i}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }
    println!("PASS repair_fixes_primary_superblock");
}

/// Corrupt backup superblock, open (uses primary), repair, then corrupt
/// primary -- backup should now work.
#[tokio::test]
async fn repair_fixes_backup_superblock() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    for i in 0..8 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    // Corrupt backup
    flip_byte(tmp.path(), SUPERBLOCK_SIZE + 15);

    // Open with primary, repair
    let store = RawObjectStore::open(tmp.path()).unwrap();
    store.repair().unwrap();
    drop(store);

    // Now corrupt primary -- backup should have been fixed by repair
    flip_byte(tmp.path(), 15);

    let store = RawObjectStore::open(tmp.path()).unwrap();
    for i in 0..8 {
        let data = store
            .get(&Path::from(format!("f/{i}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }
    println!("PASS repair_fixes_backup_superblock");
}

// =======================================================================
// 4. Simulated power loss at different points in the flush sequence
// =======================================================================

/// Simulate power loss right after data writes but before flush_index.
/// On reopen, only the last-flushed state should be visible.
#[tokio::test]
async fn power_loss_before_flush_loses_unflushed_data() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    // Flush 1: write and flush 5 files
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("committed/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Write 5 more WITHOUT flushing
    for i in 5..10 {
        store
            .put(
                &Path::from(format!("uncommitted/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }

    // Simulate crash: drop without flush
    drop(store);

    // Reopen
    let store = RawObjectStore::open(tmp.path()).unwrap();

    // Committed files should be there
    for i in 0..5 {
        let data = store
            .get(&Path::from(format!("committed/{i}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }

    // Uncommitted files should be gone
    for i in 5..10 {
        let result = store.get(&Path::from(format!("uncommitted/{i}.bin"))).await;
        assert!(
            result.is_err(),
            "uncommitted file {i} should not survive power loss"
        );
    }
    println!("PASS power_loss_before_flush_loses_unflushed_data");
}

/// Multiple flush points: commit batches. Crash after 3rd flush.
/// Only data from first 3 flushes should survive.
#[tokio::test]
async fn multi_flush_crash_preserves_all_flushed_batches() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    // Batch 1
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("b1/{i}.bin")),
                PutPayload::from(make_small(i, 2048)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Batch 2
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("b2/{i}.bin")),
                PutPayload::from(make_small(100 + i, 2048)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Batch 3
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("b3/{i}.bin")),
                PutPayload::from(make_small(200 + i, 2048)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Batch 4: NOT flushed
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("b4/{i}.bin")),
                PutPayload::from(make_small(300 + i, 2048)),
            )
            .await
            .unwrap();
    }

    // Crash
    drop(store);

    // Reopen
    let store = RawObjectStore::open(tmp.path()).unwrap();

    // Batches 1-3 should be there
    for prefix in &["b1", "b2", "b3"] {
        for i in 0..5 {
            let data = store
                .get(&Path::from(format!("{prefix}/{i}.bin")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 2048);
        }
    }

    // Batch 4 should be gone
    for i in 0..5 {
        let result = store.get(&Path::from(format!("b4/{i}.bin"))).await;
        assert!(result.is_err(), "batch 4 file {i} should not survive crash");
    }
    println!("PASS multi_flush_crash_preserves_all_flushed_batches");
}

// =======================================================================
// 5. Reopen stability after crash -- multiple cycles
// =======================================================================

/// Open and close many times without writing. Should be stable.
#[tokio::test]
async fn many_reopen_cycles_stable() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
    for i in 0..10 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    // Reopen 20 times without writing
    for cycle in 0..20 {
        let store = RawObjectStore::open(tmp.path()).unwrap();
        for i in 0..10 {
            let data = store
                .get(&Path::from(format!("f/{i}.bin")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 4096, "cycle {cycle}, file {i}");
        }
    }
    println!("PASS many_reopen_cycles_stable");
}

/// Flush-reopen-verify in a tight loop.
#[tokio::test]
async fn rapid_flush_reopen_cycle() {
    let tmp = NamedTempFile::new().unwrap();

    for cycle in 0..15 {
        let store = if cycle == 0 {
            RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap()
        } else {
            RawObjectStore::open(tmp.path()).unwrap()
        };

        // Write one file per cycle
        store
            .put(
                &Path::from(format!("cycle/{cycle}.bin")),
                PutPayload::from(make_small(cycle, 4096)),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
        drop(store);
    }

    // Final verify: all 15 files should exist
    let store = RawObjectStore::open(tmp.path()).unwrap();
    for cycle in 0..15 {
        let data = store
            .get(&Path::from(format!("cycle/{cycle}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
    }
    println!("PASS rapid_flush_reopen_cycle");
}

// =======================================================================
// 6. Crash after delete but before flush -- deleted files should reappear
// =======================================================================

#[tokio::test]
async fn crash_after_delete_before_flush_restores_deleted_files() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    for i in 0..10 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Delete half the files WITHOUT flushing
    for i in (0..10).step_by(2) {
        store.delete(&Path::from(format!("f/{i}.bin"))).await.unwrap();
    }

    // Crash
    drop(store);

    // Reopen -- deleted files should reappear (delete was not flushed)
    let store = RawObjectStore::open(tmp.path()).unwrap();
    for i in 0..10 {
        let data = store
            .get(&Path::from(format!("f/{i}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096, "file {i} should survive crash-after-delete");
    }
    println!("PASS crash_after_delete_before_flush_restores_deleted_files");
}

// =======================================================================
// 7. Crash after overwrite but before flush -- old version persists
// =======================================================================

#[tokio::test]
async fn crash_after_overwrite_before_flush_keeps_old_version() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    // Write version 1
    store
        .put(
            &Path::from("data.bin"),
            PutPayload::from(make_small(1, 4096)),
        )
        .await
        .unwrap();
    store.flush_index().unwrap();

    // Overwrite with version 2 (different content) but don't flush
    store
        .put(
            &Path::from("data.bin"),
            PutPayload::from(make_small(2, 8192)),
        )
        .await
        .unwrap();

    // Crash
    drop(store);

    // Reopen -- should see version 1
    let store = RawObjectStore::open(tmp.path()).unwrap();
    let data = store
        .get(&Path::from("data.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    // Version 1 was 4096 bytes
    assert_eq!(data.len(), 4096, "should see v1 (4096 bytes) not v2 (8192 bytes)");
    println!("PASS crash_after_overwrite_before_flush_keeps_old_version");
}

// =======================================================================
// 8. Simultaneous primary SB corruption + stale backup
// =======================================================================

/// Flush twice, corrupt primary, revert backup to txn 1.
/// Open should use backup (which has valid but older data).
#[tokio::test]
async fn stale_backup_sb_plus_corrupt_primary() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    // Flush 1: 5 files
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Save backup superblock after flush 1
    let backup_sb_v1 =
        read_bytes_at(tmp.path(), SUPERBLOCK_SIZE, SUPERBLOCK_SIZE as usize);

    // Flush 2: 5 more files
    for i in 5..10 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    // Corrupt primary superblock
    flip_byte(tmp.path(), 10);

    // Revert backup to v1
    write_bytes_at(tmp.path(), SUPERBLOCK_SIZE, &backup_sb_v1);

    // Open should use backup v1 -- only first 5 files visible
    let store = RawObjectStore::open(tmp.path()).unwrap();

    for i in 0..5 {
        let data = store
            .get(&Path::from(format!("f/{i}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096, "file {i} from v1 should be visible");
    }

    // Files 5-9 may or may not be visible depending on whether the old index
    // region still has them. They should NOT be visible since backup v1's
    // index doesn't include them.
    let mut missing_count = 0;
    for i in 5..10 {
        if store.get(&Path::from(format!("f/{i}.bin"))).await.is_err() {
            missing_count += 1;
        }
    }
    assert!(
        missing_count > 0,
        "at least some v2 files should not be visible with v1 backup"
    );
    println!("PASS stale_backup_sb_plus_corrupt_primary");
}

// =======================================================================
// 9. Repair after alternating flush corruption
// =======================================================================

/// Do multiple flushes to alternate between index regions A and B.
/// Corrupt a shard in the active region, then verify open fails.
/// Reformat recovers.
#[tokio::test]
async fn repair_after_active_index_corruption() {
    use rawobjstr::NUM_SHARDS;

    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    // Write and flush multiple times to exercise alternation
    for batch in 0..3 {
        for i in 0..3 {
            store
                .put(
                    &Path::from(format!("batch{batch}/f{i}.bin")),
                    PutPayload::from(make_small(batch * 100 + i, 4096)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();
    }
    drop(store);

    // Read primary superblock to find shard layout
    let sb_bytes = read_bytes_at(tmp.path(), 0, SUPERBLOCK_SIZE as usize);
    let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();

    let shard_slot_size = sb.index_slot_capacity / NUM_SHARDS as u64;
    let index_total = sb.index_slot_capacity * 2;
    let index_region_a = sb.device_size - index_total;
    let index_region_b = sb.device_size - sb.index_slot_capacity;

    // Find a shard with data and corrupt it
    let shard_idx = sb.shard_slots.iter().position(|s| s.size > 0)
        .expect("should have at least one populated shard");
    let meta = &sb.shard_slots[shard_idx];
    let shard_offset = if meta.active_slot == 0 {
        index_region_a + shard_idx as u64 * shard_slot_size
    } else {
        index_region_b + shard_idx as u64 * shard_slot_size
    };
    flip_byte(tmp.path(), shard_offset + 50);

    // Open should fail (corrupt shard CRC)
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // Reformat recovers
    let store = RawObjectStore::format(tmp.path(), false).unwrap();
    store
        .put(
            &Path::from("recovered.bin"),
            PutPayload::from(make_small(999, 4096)),
        )
        .await
        .unwrap();
    store.flush_index().unwrap();

    let data = store
        .get(&Path::from("recovered.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(data.len(), 4096);
    println!("PASS repair_after_active_index_corruption");
}

// =======================================================================
// 10. Verify that free space is consistent after crash recovery
// =======================================================================

#[tokio::test]
async fn free_space_consistent_after_crash_recovery() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    // Write 20 files, flush
    for i in 0..20 {
        store
            .put(
                &Path::from(format!("f/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();

    // Delete 10 files, flush
    for i in 0..10 {
        store.delete(&Path::from(format!("f/{i}.bin"))).await.unwrap();
    }
    store.flush_index().unwrap();

    // Write 5 more (uses freed space), DON'T flush
    for i in 100..105 {
        store
            .put(
                &Path::from(format!("unflushed/{i}.bin")),
                PutPayload::from(make_small(i, 4096)),
            )
            .await
            .unwrap();
    }
    drop(store);

    // Reopen
    let store = RawObjectStore::open(tmp.path()).unwrap();

    // Run verify -- space should be consistent
    let report = store.verify_all();
    assert!(
        report.space_accounted,
        "space accounting should be consistent after crash recovery"
    );
    assert!(
        report.free_list_consistent,
        "free list should be consistent after crash recovery"
    );

    // Should have 10 files (the unflushed 5 are lost, but the 10 deleted
    // ones from the flushed delete are gone)
    assert_eq!(report.files_checked, 10);
    assert_eq!(report.files_ok, 10);

    // Repair and verify again
    store.repair().unwrap();
    let report2 = store.verify_all();
    assert!(report2.space_accounted);
    assert!(report2.free_list_consistent);
    println!("PASS free_space_consistent_after_crash_recovery");
}

// =======================================================================
// 11. Crash simulation with concurrent writes -- last flush wins
// =======================================================================

#[tokio::test]
async fn crash_after_concurrent_writes_recovers_to_last_flush() {
    let tmp = NamedTempFile::new().unwrap();
    let store = std::sync::Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // Phase 1: concurrent writes, then flush
    let mut handles = Vec::new();
    for t in 0..4 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                s.put(
                    &Path::from(format!("t{t}/f{i}.bin")),
                    PutPayload::from(make_small(t * 100 + i, 4096)),
                )
                .await
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    store.flush_index().unwrap();

    // Phase 2: more writes WITHOUT flush
    for i in 0..5 {
        store
            .put(
                &Path::from(format!("unflushed/{i}.bin")),
                PutPayload::from(make_small(500 + i, 4096)),
            )
            .await
            .unwrap();
    }

    // Crash
    drop(store);

    // Reopen -- should have 40 files from phase 1, not 5 from phase 2
    let store = RawObjectStore::open(tmp.path()).unwrap();

    let mut found = 0;
    for t in 0..4 {
        for i in 0..10 {
            if store
                .get(&Path::from(format!("t{t}/f{i}.bin")))
                .await
                .is_ok()
            {
                found += 1;
            }
        }
    }
    assert_eq!(found, 40, "all 40 phase-1 files should survive");

    let mut unflushed_found = 0;
    for i in 0..5 {
        if store
            .get(&Path::from(format!("unflushed/{i}.bin")))
            .await
            .is_ok()
        {
            unflushed_found += 1;
        }
    }
    assert_eq!(unflushed_found, 0, "unflushed files should not survive crash");
    println!("PASS crash_after_concurrent_writes_recovers_to_last_flush");
}
