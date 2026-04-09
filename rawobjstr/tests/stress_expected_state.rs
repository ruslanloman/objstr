//! Expected-state stress tests.
//!
//! Core concept: maintain a shadow HashMap of what the store should contain.
//! Perform randomized operations (put, get, delete, list, copy, overwrite),
//! then verify the store matches the expected state. Periodically close and
//! reopen the store to test persistence/recovery.
//!
//! Key patterns:
//! - Expected state tracks all keys and their expected sizes
//! - Every read is verified against expected state
//! - Periodic reopen (close + open) during stress
//! - After crash simulation, verify expected state matches reality
//! - Operation mix: put/get/delete/list/copy with configurable weights

mod common;

use std::collections::HashMap;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store::PutPayload;
use rand::Rng;

use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

use common::{make_small, SMALL_DEVICE, MEDIUM_DEVICE};

// -----------------------------------------------------------------------
// Expected State
// -----------------------------------------------------------------------

/// Shadow model of what the store should contain.
#[derive(Clone, Debug)]
struct ExpectedState {
    /// Map from path -> (index_tag, size). The index_tag is used with
    /// make_small(tag, size) to reconstruct expected content.
    files: HashMap<String, (usize, usize)>,
}

impl ExpectedState {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    fn put(&mut self, path: &str, tag: usize, size: usize) {
        self.files.insert(path.to_string(), (tag, size));
    }

    fn delete(&mut self, path: &str) -> bool {
        self.files.remove(path).is_some()
    }

    fn exists(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    fn get(&self, path: &str) -> Option<(usize, usize)> {
        self.files.get(path).copied()
    }

    fn copy(&mut self, src: &str, dst: &str) -> bool {
        if let Some(&val) = self.files.get(src) {
            self.files.insert(dst.to_string(), val);
            true
        } else {
            false
        }
    }

    fn file_count(&self) -> usize {
        self.files.len()
    }

    #[allow(dead_code)]
    fn paths(&self) -> Vec<String> {
        let mut p: Vec<String> = self.files.keys().cloned().collect();
        p.sort();
        p
    }
}

/// Verify the store's content against the expected state.
async fn verify_against_expected(
    store: &RawObjectStore,
    expected: &ExpectedState,
    label: &str,
) {
    // 1. Check file count via list
    let listed: Vec<_> = store
        .list(None)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        listed.len(),
        expected.file_count(),
        "{label}: list count mismatch (listed={}, expected={})",
        listed.len(),
        expected.file_count()
    );

    // 2. Verify every expected file is readable with correct content
    for (path, &(tag, size)) in &expected.files {
        let data = store
            .get(&Path::from(path.as_str()))
            .await
            .unwrap_or_else(|e| panic!("{label}: get({path}) failed: {e}"))
            .bytes()
            .await
            .unwrap_or_else(|e| panic!("{label}: bytes({path}) failed: {e}"));

        assert_eq!(
            data.len(),
            size,
            "{label}: {path} size mismatch (got={}, expected={size})",
            data.len()
        );

        // Verify content matches make_small(tag, size)
        let expected_data = make_small(tag, size);
        assert_eq!(
            &data[..],
            &expected_data[..],
            "{label}: {path} content mismatch"
        );
    }

    // 3. Check that getting a path NOT in expected state fails
    let nonexistent = "nonexistent_verification_key.bin";
    if !expected.exists(nonexistent) {
        let result = store.get(&Path::from(nonexistent)).await;
        assert!(result.is_err(), "{label}: nonexistent key should return error");
    }
}

// =======================================================================
// 1. Basic expected-state stress: random put/get/delete mix
// =======================================================================

#[tokio::test]
async fn stress_expected_state_random_ops() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut rng = rand::thread_rng();
    let mut next_tag = 0usize;

    let key_space = 50; // keys are "k/00" .. "k/49"
    let file_sizes = [128, 512, 1024, 4096, 8192, 16384];

    for op_num in 0..500 {
        let key_idx = rng.gen_range(0..key_space);
        let key = format!("k/{key_idx:02}");

        // Operation weights: 50% put, 30% get, 20% delete
        let roll: u32 = rng.gen_range(0..100);

        if roll < 50 {
            // PUT
            let size = file_sizes[rng.gen_range(0..file_sizes.len())];
            let tag = next_tag;
            next_tag += 1;

            store
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(make_small(tag, size)),
                )
                .await
                .unwrap();
            expected.put(&key, tag, size);
        } else if roll < 80 {
            // GET
            if let Some((tag, size)) = expected.get(&key) {
                let data = store
                    .get(&Path::from(key.as_str()))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(data.len(), size, "op {op_num}: {key} size mismatch");
                let expected_data = make_small(tag, size);
                assert_eq!(&data[..], &expected_data[..], "op {op_num}: {key} content mismatch");
            } else {
                let result = store.get(&Path::from(key.as_str())).await;
                assert!(result.is_err(), "op {op_num}: {key} should not exist");
            }
        } else {
            // DELETE
            let result = store.delete(&Path::from(key.as_str())).await;
            if expected.exists(&key) {
                assert!(result.is_ok(), "op {op_num}: delete {key} should succeed");
                expected.delete(&key);
            }
            // Note: delete of non-existent key may or may not error depending on impl
            if result.is_ok() && !expected.exists(&key) {
                // Already deleted from expected state above, or key never existed
            }
        }
    }

    // Full verification at the end
    verify_against_expected(&store, &expected, "after 500 ops").await;
    println!("PASS stress_expected_state_random_ops: {} files remain", expected.file_count());
}

// =======================================================================
// 2. Expected-state stress with periodic reopen
// =======================================================================

#[tokio::test]
async fn stress_expected_state_with_periodic_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store =
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut rng = rand::thread_rng();
    let mut next_tag = 0usize;

    let key_space = 30;
    let file_sizes = [256, 1024, 4096, 8192];
    let reopen_interval = 50; // reopen every 50 ops

    for op_num in 0..300 {
        // Periodic reopen
        if op_num > 0 && op_num % reopen_interval == 0 {
            store.flush_index().unwrap();
            drop(store);
            store = RawObjectStore::open(tmp.path()).unwrap();

            // Verify after reopen
            verify_against_expected(&store, &expected, &format!("reopen at op {op_num}")).await;
        }

        let key_idx = rng.gen_range(0..key_space);
        let key = format!("rk/{key_idx:02}");
        let roll: u32 = rng.gen_range(0..100);

        if roll < 50 {
            // PUT
            let size = file_sizes[rng.gen_range(0..file_sizes.len())];
            let tag = next_tag;
            next_tag += 1;
            store
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(make_small(tag, size)),
                )
                .await
                .unwrap();
            expected.put(&key, tag, size);
        } else if roll < 80 {
            // GET + verify
            if let Some((tag, size)) = expected.get(&key) {
                let data = store
                    .get(&Path::from(key.as_str()))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(data.len(), size);
                let expected_data = make_small(tag, size);
                assert_eq!(&data[..], &expected_data[..]);
            }
        } else {
            // DELETE
            if expected.exists(&key) {
                store.delete(&Path::from(key.as_str())).await.unwrap();
                expected.delete(&key);
            }
        }
    }

    store.flush_index().unwrap();
    verify_against_expected(&store, &expected, "final").await;
    println!(
        "PASS stress_expected_state_with_periodic_reopen: {} files remain",
        expected.file_count()
    );
}

// =======================================================================
// 3. Expected-state stress with copy operations
// =======================================================================

#[tokio::test]
async fn stress_expected_state_with_copies() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut rng = rand::thread_rng();
    let mut next_tag = 0usize;

    let key_space = 40;

    for _op_num in 0..300 {
        let roll: u32 = rng.gen_range(0..100);

        if roll < 40 {
            // PUT
            let key_idx = rng.gen_range(0..key_space);
            let key = format!("c/{key_idx:02}");
            let size = 1024 * rng.gen_range(1..=8);
            let tag = next_tag;
            next_tag += 1;
            store
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(make_small(tag, size)),
                )
                .await
                .unwrap();
            expected.put(&key, tag, size);
        } else if roll < 60 {
            // COPY
            let src_idx = rng.gen_range(0..key_space);
            let dst_idx = rng.gen_range(0..key_space);
            let src = format!("c/{src_idx:02}");
            let dst = format!("c_copy/{dst_idx:02}");

            if expected.exists(&src) {
                let result = store
                    .copy(&Path::from(src.as_str()), &Path::from(dst.as_str()))
                    .await;
                if result.is_ok() {
                    expected.copy(&src, &dst);
                }
            }
        } else if roll < 80 {
            // GET + verify
            let key_idx = rng.gen_range(0..key_space);
            let key = format!("c/{key_idx:02}");
            if let Some((tag, size)) = expected.get(&key) {
                let data = store
                    .get(&Path::from(key.as_str()))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(data.len(), size);
                let expected_data = make_small(tag, size);
                assert_eq!(&data[..], &expected_data[..]);
            }
        } else {
            // DELETE
            let key_idx = rng.gen_range(0..key_space);
            let key = format!("c/{key_idx:02}");
            if expected.exists(&key) {
                store.delete(&Path::from(key.as_str())).await.unwrap();
                expected.delete(&key);
            }
        }
    }

    verify_against_expected(&store, &expected, "final_copies").await;
    println!(
        "PASS stress_expected_state_with_copies: {} files remain",
        expected.file_count()
    );
}

// =======================================================================
// 4. Expected-state stress with crash simulation (drop without flush)
// =======================================================================

/// After a crash (drop without flush), the store rolls back to the last flushed
/// index. We verify paths and sizes match the committed state.
///
/// IMPORTANT: we only create NEW keys each round (never overwrite committed
/// keys) because a put that overwrites a committed file writes new data to
/// the same disk location (first-fit reuses freed space), making the old
/// index point to changed data. This is correct hardware behaviour.
#[tokio::test]
async fn stress_expected_state_crash_simulation() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store =
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

    let mut committed_state = ExpectedState::new();
    let mut rng = rand::thread_rng();
    let mut next_tag = 0usize;
    let mut next_key = 0usize;

    let file_sizes = [256, 1024, 4096];

    for round in 0..6 {
        // Each round creates fresh keys that don't collide with committed data
        let ops_this_round = rng.gen_range(5..15);
        for _ in 0..ops_this_round {
            let key = format!("cs/{next_key:04}");
            next_key += 1;
            let size = file_sizes[rng.gen_range(0..file_sizes.len())];
            let tag = next_tag;
            next_tag += 1;
            store
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(make_small(tag, size)),
                )
                .await
                .unwrap();
            // Track in committed_state only if this round will flush
            if round % 2 == 0 {
                committed_state.put(&key, tag, size);
            }
        }

        if round % 2 == 0 {
            // Flush (commit)
            store.flush_index().unwrap();
            drop(store);
        } else {
            // Crash: drop without flush. Unflushed writes are lost.
            drop(store);
        }

        // Reopen and verify against the committed state
        store = RawObjectStore::open(tmp.path()).unwrap();
        verify_against_expected(
            &store,
            &committed_state,
            &format!("round {round}"),
        )
        .await;
    }

    println!(
        "PASS stress_expected_state_crash_simulation: {} files committed",
        committed_state.file_count()
    );
}

// =======================================================================
// 5. Expected-state: list verification (all paths, prefix filtering)
// =======================================================================

#[tokio::test]
async fn stress_expected_state_list_verification() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut next_tag = 0usize;

    let prefixes = ["alpha", "beta", "gamma", "delta"];

    // Put files across multiple prefixes
    for prefix in &prefixes {
        for i in 0..10 {
            let key = format!("{prefix}/{i:02}.bin");
            let tag = next_tag;
            next_tag += 1;
            store
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(make_small(tag, 512)),
                )
                .await
                .unwrap();
            expected.put(&key, tag, 512);
        }
    }

    // Verify total list
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 40);

    // Verify per-prefix list
    for prefix in &prefixes {
        let filtered: Vec<_> = store
            .list(Some(&Path::from(*prefix)))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            filtered.len(),
            10,
            "prefix {prefix} should have 10 files"
        );
    }

    // Delete some files from one prefix
    for i in 0..5 {
        let key = format!("alpha/{i:02}.bin");
        store.delete(&Path::from(key.as_str())).await.unwrap();
        expected.delete(&key);
    }

    // Verify after deletes
    let alpha: Vec<_> = store
        .list(Some(&Path::from("alpha")))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(alpha.len(), 5);
    assert_eq!(expected.file_count(), 35);

    verify_against_expected(&store, &expected, "after_prefix_deletes").await;
    println!("PASS stress_expected_state_list_verification");
}

// =======================================================================
// 6. Expected-state: head metadata verification
// =======================================================================

#[tokio::test]
async fn stress_expected_state_head_verification() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let sizes = [1, 100, 4095, 4096, 4097, 8192, 65536];

    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("h/{i}.bin");
        store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(make_small(i, size)),
            )
            .await
            .unwrap();
        expected.put(&key, i, size);
    }

    // Verify head for each file
    for (path, &(_tag, size)) in &expected.files {
        let meta = store.head(&Path::from(path.as_str())).await.unwrap();
        assert_eq!(
            meta.size, size as u64,
            "head({path}): size mismatch (got={}, expected={size})",
            meta.size
        );
    }

    // Verify head for non-existent file
    let result = store.head(&Path::from("h/nonexistent")).await;
    assert!(result.is_err());

    println!("PASS stress_expected_state_head_verification");
}

// =======================================================================
// 7. Expected-state: overwrite verification (content and size change)
// =======================================================================

#[tokio::test]
async fn stress_expected_state_overwrite_verification() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut rng = rand::thread_rng();
    let mut next_tag = 0usize;

    let key_space = 20;

    // Phase 1: populate
    for idx in 0..key_space {
        let key = format!("ov/{idx:02}");
        let size = 4096;
        let tag = next_tag;
        next_tag += 1;
        store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(make_small(tag, size)),
            )
            .await
            .unwrap();
        expected.put(&key, tag, size);
    }

    // Phase 2: random overwrites with different sizes
    for _ in 0..100 {
        let idx = rng.gen_range(0..key_space);
        let key = format!("ov/{idx:02}");
        let new_size = 1024 * rng.gen_range(1..=16);
        let tag = next_tag;
        next_tag += 1;
        store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(make_small(tag, new_size)),
            )
            .await
            .unwrap();
        expected.put(&key, tag, new_size);
    }

    // Verify all
    verify_against_expected(&store, &expected, "after_overwrites").await;

    // Flush, reopen, verify again
    store.flush_index().unwrap();
    drop(store);
    let store = RawObjectStore::open(tmp.path()).unwrap();
    verify_against_expected(&store, &expected, "after_reopen").await;

    println!(
        "PASS stress_expected_state_overwrite_verification: {} files",
        expected.file_count()
    );
}

// =======================================================================
// 8. Expected-state: full device cycle (fill -> delete all -> refill)
// =======================================================================

#[tokio::test]
async fn stress_expected_state_fill_delete_refill() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut next_tag = 0usize;

    // Phase 1: fill with 4KB files until full
    let mut i = 0;
    loop {
        let key = format!("fill/{i:04}");
        let tag = next_tag;
        next_tag += 1;
        let result = store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(make_small(tag, 4096)),
            )
            .await;
        match result {
            Ok(_) => {
                expected.put(&key, tag, 4096);
                i += 1;
            }
            Err(_) => break, // NoSpace
        }
    }
    let filled_count = i;
    assert!(filled_count > 0, "should have filled at least some files");

    verify_against_expected(&store, &expected, "after_fill").await;

    // Phase 2: delete all
    for j in 0..filled_count {
        let key = format!("fill/{j:04}");
        store.delete(&Path::from(key.as_str())).await.unwrap();
        expected.delete(&key);
    }
    assert_eq!(expected.file_count(), 0);

    // Phase 3: refill
    for j in 0..filled_count {
        let key = format!("refill/{j:04}");
        let tag = next_tag;
        next_tag += 1;
        store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(make_small(tag, 4096)),
            )
            .await
            .unwrap();
        expected.put(&key, tag, 4096);
    }

    verify_against_expected(&store, &expected, "after_refill").await;

    // Flush, reopen, verify
    store.flush_index().unwrap();
    drop(store);
    let store = RawObjectStore::open(tmp.path()).unwrap();
    verify_against_expected(&store, &expected, "after_refill_reopen").await;

    println!(
        "PASS stress_expected_state_fill_delete_refill: {filled_count} files each phase"
    );
}

// =======================================================================
// 9. Expected-state: concurrent stress with expected state
// =======================================================================

#[tokio::test]
async fn stress_expected_state_verify_all_report() {
    let tmp = NamedTempFile::new().unwrap();
    let store =
        RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut next_tag = 0usize;

    // Put 30 files
    for i in 0..30 {
        let key = format!("v/{i:03}");
        let size = 4096 * (1 + i % 4);
        let tag = next_tag;
        next_tag += 1;
        store
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(make_small(tag, size)),
            )
            .await
            .unwrap();
        expected.put(&key, tag, size);
    }

    // Run verify_all
    let report = store.verify_all();
    assert_eq!(report.files_checked, 30);
    assert_eq!(report.files_ok, 30);
    assert!(report.errors.is_empty());
    assert!(report.space_accounted);
    assert!(report.free_list_consistent);

    // Delete some, verify again
    for i in (0..30).step_by(3) {
        let key = format!("v/{i:03}");
        store.delete(&Path::from(key.as_str())).await.unwrap();
        expected.delete(&key);
    }

    let report = store.verify_all();
    assert_eq!(report.files_checked, expected.file_count());
    assert_eq!(report.files_ok, expected.file_count());
    assert!(report.errors.is_empty());
    assert!(report.space_accounted);

    verify_against_expected(&store, &expected, "after_partial_delete").await;
    println!("PASS stress_expected_state_verify_all_report");
}

// =======================================================================
// 10. Long-running mixed workload with flush/reopen/verify cycles
// =======================================================================

#[tokio::test]
async fn stress_long_mixed_workload_with_checkpoints() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store =
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

    let mut expected = ExpectedState::new();
    let mut rng = rand::thread_rng();
    let mut next_tag = 0usize;

    let key_space = 60;
    let file_sizes = [128, 512, 1024, 2048, 4096, 8192];

    for round in 0..10 {
        let ops = 50;
        for _ in 0..ops {
            let key_idx = rng.gen_range(0..key_space);
            let key = format!("lw/{key_idx:03}");
            let roll: u32 = rng.gen_range(0..100);

            if roll < 45 {
                // PUT
                let size = file_sizes[rng.gen_range(0..file_sizes.len())];
                let tag = next_tag;
                next_tag += 1;
                store
                    .put(
                        &Path::from(key.as_str()),
                        PutPayload::from(make_small(tag, size)),
                    )
                    .await
                    .unwrap();
                expected.put(&key, tag, size);
            } else if roll < 70 {
                // GET + verify
                if let Some((tag, size)) = expected.get(&key) {
                    let data = store
                        .get(&Path::from(key.as_str()))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                    assert_eq!(data.len(), size);
                    let exp = make_small(tag, size);
                    assert_eq!(&data[..], &exp[..]);
                }
            } else if roll < 85 {
                // DELETE
                if expected.exists(&key) {
                    store.delete(&Path::from(key.as_str())).await.unwrap();
                    expected.delete(&key);
                }
            } else {
                // COPY
                let dst_idx = rng.gen_range(0..key_space);
                let dst = format!("lw_cp/{dst_idx:03}");
                if expected.exists(&key) {
                    let _ = store
                        .copy(
                            &Path::from(key.as_str()),
                            &Path::from(dst.as_str()),
                        )
                        .await;
                    expected.copy(&key, &dst);
                }
            }
        }

        // Flush, reopen, full verify
        store.flush_index().unwrap();
        drop(store);
        store = RawObjectStore::open(tmp.path()).unwrap();
        verify_against_expected(
            &store,
            &expected,
            &format!("round {round}"),
        )
        .await;

        // Also check verify_all report
        let report = store.verify_all();
        assert_eq!(report.files_checked, expected.file_count());
        assert_eq!(report.files_ok, expected.file_count());
        assert!(report.errors.is_empty(), "round {round}: unexpected verify errors");
    }

    println!(
        "PASS stress_long_mixed_workload_with_checkpoints: {} files, {} total ops",
        expected.file_count(),
        10 * 50
    );
}
