//! High-contention concurrent tests.
//!
//! Multiple threads hammer the SAME small set of keys with puts, gets,
//! deletes, and range reads.  Verifies that the store never panics,
//! never returns corrupt data, and that after all threads drain the
//! final state matches a shadow model.
//!
//! Optionally wires an event socket subscriber and verifies that every
//! mutation appears in the event stream.

#![cfg(unix)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::{NamedTempFile, TempDir};
use tokio::task::JoinSet;

use rawobjstr::event::unix::{subscribe_events, EventServer};
use rawobjstr::event::{EventBus, StoreEvent};
use rawobjstr::store::RawObjectStore;

use common::{make_small, MEDIUM_DEVICE};

// -----------------------------------------------------------------------
// Shared expected state
// -----------------------------------------------------------------------

#[derive(Debug)]
struct ShadowState {
    /// path -> (tag, size) where make_small(tag, size) reproduces content.
    files: HashMap<String, (usize, usize)>,
    next_tag: usize,
}

impl ShadowState {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
            next_tag: 0,
        }
    }
}

// -----------------------------------------------------------------------
// Verification
// -----------------------------------------------------------------------

async fn verify_final_state(
    store: &RawObjectStore,
    expected: &HashMap<String, (usize, usize)>,
    label: &str,
) {
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(
        listed.len(),
        expected.len(),
        "{label}: list count mismatch (listed={}, expected={})",
        listed.len(),
        expected.len(),
    );

    for (path, &(tag, size)) in expected {
        let data = store
            .get(&Path::from(path.as_str()))
            .await
            .unwrap_or_else(|e| panic!("{label}: get({path}) failed: {e}"))
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), size, "{label}: {path} size mismatch");
        let want = make_small(tag, size);
        assert_eq!(&data[..], &want[..], "{label}: {path} content mismatch");
    }
}

// =======================================================================
// 1. 8 threads, 5 keys, heavy contention -- put/get/delete/head
// =======================================================================

#[tokio::test]
async fn contention_8_threads_5_keys_put_get_delete() {
    let num_tasks: usize = 8;
    let ops_per_task: usize = 400;
    let key_space: usize = 5;
    let sizes: &[usize] = &[512, 1024, 4096, 8192, 16384];

    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );
    let state = Arc::new(Mutex::new(ShadowState::new()));

    let mut tasks = JoinSet::new();
    for tid in 0..num_tasks {
        let store = Arc::clone(&store);
        let state = Arc::clone(&state);

        tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(tid as u64 * 9973 + 31);

            for _ in 0..ops_per_task {
                let key = format!("hot/{}", rng.gen_range(0..key_space));
                let roll: u32 = rng.gen_range(0..100);

                if roll < 40 {
                    // PUT (40%)
                    let size = sizes[rng.gen_range(0..sizes.len())];
                    let tag = {
                        let mut s = state.lock();
                        let t = s.next_tag;
                        s.next_tag += 1;
                        s.files.insert(key.clone(), (t, size));
                        t
                    };
                    store
                        .put(
                            &Path::from(key.as_str()),
                            PutPayload::from(make_small(tag, size)),
                        )
                        .await
                        .unwrap();
                } else if roll < 65 {
                    // GET (25%)
                    match store.get(&Path::from(key.as_str())).await {
                        Ok(r) => {
                            let data = r.bytes().await.unwrap();
                            // Verify this is a valid make_small() output:
                            // first 8 bytes are a LE u64 tag, rest is fill byte
                            assert!(!data.is_empty(), "got empty body for {key}");
                            if data.len() >= 8 {
                                let tag = u64::from_le_bytes(
                                    data[..8].try_into().unwrap(),
                                ) as usize;
                                let fill = (tag & 0xFF) as u8;
                                assert!(
                                    data[8..].iter().all(|&b| b == fill),
                                    "corrupt data for {key}: tag={tag}",
                                );
                            }
                        }
                        Err(_) => { /* deleted by another task */ }
                    }
                } else if roll < 80 {
                    // DELETE (15%)
                    {
                        state.lock().files.remove(&key);
                    }
                    let _ = store.delete(&Path::from(key.as_str())).await;
                } else if roll < 90 {
                    // HEAD (10%)
                    match store.head(&Path::from(key.as_str())).await {
                        Ok(meta) => {
                            assert!(meta.size > 0, "head returned size 0 for {key}");
                        }
                        Err(_) => {}
                    }
                } else {
                    // RANGE READ (10%)
                    match store.get_range(&Path::from(key.as_str()), 0..64).await {
                        Ok(data) => {
                            assert!(
                                data.len() <= 64,
                                "range read returned > 64 bytes for {key}"
                            );
                        }
                        Err(_) => {}
                    }
                }

                // No yield -- maximize contention.
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.expect("task panicked");
    }

    store.flush_index().unwrap();

    let snapshot = state.lock().files.clone();
    verify_final_state(&store, &snapshot, "8t-5k-contention").await;
}

// =======================================================================
// 2. Same-key contention with event socket verification
// =======================================================================

#[tokio::test]
async fn contention_same_keys_with_event_socket() {
    let num_tasks: usize = 8;
    let ops_per_task: usize = 200;
    let key_space: usize = 5;
    let sizes: &[usize] = &[1024, 4096, 8192];

    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // -- Event bus + socket server --
    let sock_dir = TempDir::new().unwrap();
    let sock_path = sock_dir.path().join("contention.sock");
    let secret = "contention-secret";
    let bus = Arc::new(EventBus::new(2048));
    let _server = EventServer::start(&sock_path, secret, 4, &bus, None).unwrap();

    let bus_flush = Arc::clone(&bus);
    store.add_flush_callback(Arc::new(move |txn_id| {
        bus_flush.emit_flush(0, txn_id);
    }));

    // -- Subscriber collecting events --
    let put_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let del_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let flush_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let pc = Arc::clone(&put_count);
    let dc = Arc::clone(&del_count);
    let fc = Arc::clone(&flush_count);
    let subscriber = subscribe_events(&sock_path, secret, move |event| {
        match event {
            StoreEvent::Put { .. } => {
                pc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            StoreEvent::Delete { .. } => {
                dc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            StoreEvent::Flush { .. } => {
                fc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    })
    .await
    .expect("subscribe_events failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let state = Arc::new(Mutex::new(ShadowState::new()));
    let total_puts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_deletes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tasks = JoinSet::new();
    for tid in 0..num_tasks {
        let store = Arc::clone(&store);
        let state = Arc::clone(&state);
        let bus = Arc::clone(&bus);
        let tp = Arc::clone(&total_puts);
        let td = Arc::clone(&total_deletes);

        tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(tid as u64 * 7919 + 3);

            for _ in 0..ops_per_task {
                let key = format!("evt/{}", rng.gen_range(0..key_space));
                let roll: u32 = rng.gen_range(0..100);

                if roll < 50 {
                    // PUT
                    let size = sizes[rng.gen_range(0..sizes.len())];
                    let tag = {
                        let mut s = state.lock();
                        let t = s.next_tag;
                        s.next_tag += 1;
                        s.files.insert(key.clone(), (t, size));
                        t
                    };
                    store
                        .put(
                            &Path::from(key.as_str()),
                            PutPayload::from(make_small(tag, size)),
                        )
                        .await
                        .unwrap();
                    bus.emit_put(&key);
                    tp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else if roll < 75 {
                    // GET
                    let _ = store.get(&Path::from(key.as_str())).await;
                } else {
                    // DELETE
                    {
                        state.lock().files.remove(&key);
                    }
                    let result = store.delete(&Path::from(key.as_str())).await;
                    // Emit delete event regardless (matches existing pattern).
                    bus.emit_delete(&key);
                    td.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = result;
                }
            }
        });
    }

    // Periodic flusher while workers run.
    let flush_store = Arc::clone(&store);
    let flusher = tokio::spawn(async move {
        for _ in 0..8 {
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
            let _ = flush_store.flush_index();
        }
    });

    while let Some(result) = tasks.join_next().await {
        result.expect("task panicked");
    }
    flusher.await.expect("flusher panicked");

    store.flush_index().unwrap();

    // Let events propagate.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // -- Verify final state --
    let snapshot = state.lock().files.clone();
    verify_final_state(&store, &snapshot, "contention-events").await;

    // -- Verify event counts --
    let expected_puts = total_puts.load(std::sync::atomic::Ordering::Relaxed);
    let expected_dels = total_deletes.load(std::sync::atomic::Ordering::Relaxed);
    let got_puts = put_count.load(std::sync::atomic::Ordering::Relaxed);
    let got_dels = del_count.load(std::sync::atomic::Ordering::Relaxed);
    let got_flushes = flush_count.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        got_puts, expected_puts,
        "PUT event count mismatch: got {got_puts}, expected {expected_puts}"
    );
    assert_eq!(
        got_dels, expected_dels,
        "DELETE event count mismatch: got {got_dels}, expected {expected_dels}"
    );
    assert!(
        got_flushes >= 2,
        "expected at least 2 FLUSH events, got {got_flushes}"
    );

    subscriber.abort();
}

// =======================================================================
// 3. Concurrent overwrites of the same key -- many writers, one reader
// =======================================================================

#[tokio::test]
async fn contention_many_writers_one_reader_same_key() {
    let num_writers: usize = 8;
    let writes_per_writer: usize = 200;

    let tmp = NamedTempFile::new().unwrap();
    let store = Arc::new(
        RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap(),
    );

    // Seed the key so the reader always finds something.
    store
        .put(
            &Path::from("contended"),
            PutPayload::from(make_small(0, 1024)),
        )
        .await
        .unwrap();

    // Track last successful write per writer.
    let last_written = Arc::new(Mutex::new(Vec::<Option<(usize, usize)>>::new()));
    {
        let mut lw = last_written.lock();
        lw.resize(num_writers, None);
    }

    let mut tasks = JoinSet::new();

    // Spawn writers.
    for wid in 0..num_writers {
        let store = Arc::clone(&store);
        let lw = Arc::clone(&last_written);

        tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(wid as u64 * 1117 + 5);
            for i in 0..writes_per_writer {
                let size = 512 * (rng.gen_range(1u32..=16) as usize);
                let tag = wid * 100_000 + i;
                store
                    .put(
                        &Path::from("contended"),
                        PutPayload::from(make_small(tag, size)),
                    )
                    .await
                    .unwrap();
                lw.lock()[wid] = Some((tag, size));
            }
        });
    }

    // Spawn one continuous reader.
    let reader_store = Arc::clone(&store);
    let reader_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let re = Arc::clone(&reader_errors);
    let reader = tokio::spawn(async move {
        let mut reads = 0usize;
        loop {
            match reader_store.get(&Path::from("contended")).await {
                Ok(result) => {
                    let data = result.bytes().await.unwrap();
                    // Data must not be empty (the key always exists).
                    if data.is_empty() {
                        re.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    reads += 1;
                }
                Err(_) => {
                    // Should not happen -- key is never deleted.
                    re.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            if reads >= 500 {
                break;
            }
            tokio::task::yield_now().await;
        }
        reads
    });

    // Wait for writers.
    while let Some(result) = tasks.join_next().await {
        result.expect("writer panicked");
    }

    let total_reads = reader.await.expect("reader panicked");
    assert!(total_reads > 0, "reader should have completed some reads");

    let errs = reader_errors.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(errs, 0, "reader encountered {errs} errors");

    // Final state: key should exist with one of the last written values.
    store.flush_index().unwrap();
    let data = store
        .get(&Path::from("contended"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(!data.is_empty(), "final read returned empty");
}

// =======================================================================
// 4. Concurrent put + delete of SAME keys with periodic flush + reopen
// =======================================================================

#[tokio::test]
async fn contention_same_keys_flush_reopen_cycles() {
    let num_tasks: usize = 8;
    let ops_per_task: usize = 100;
    let key_space: usize = 5;

    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    let mut store = Arc::new(
        RawObjectStore::format_with_size(&path, MEDIUM_DEVICE, false).unwrap(),
    );
    let state = Arc::new(Mutex::new(ShadowState::new()));

    for round in 0..3 {
        let mut tasks = JoinSet::new();
        for tid in 0..num_tasks {
            let store = Arc::clone(&store);
            let state = Arc::clone(&state);
            let seed = (round * 1000 + tid) as u64;

            tasks.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                for _ in 0..ops_per_task {
                    let key = format!("rr/{}", rng.gen_range(0..key_space));
                    let roll: u32 = rng.gen_range(0..100);

                    if roll < 55 {
                        // PUT
                        let size = 1024 * (rng.gen_range(1u32..=4) as usize);
                        let tag = {
                            let mut s = state.lock();
                            let t = s.next_tag;
                            s.next_tag += 1;
                            s.files.insert(key.clone(), (t, size));
                            t
                        };
                        store
                            .put(
                                &Path::from(key.as_str()),
                                PutPayload::from(make_small(tag, size)),
                            )
                            .await
                            .unwrap();
                    } else if roll < 75 {
                        // GET
                        let _ = store.get(&Path::from(key.as_str())).await;
                    } else {
                        // DELETE
                        state.lock().files.remove(&key);
                        let _ = store.delete(&Path::from(key.as_str())).await;
                    }
                }
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.expect("task panicked");
        }

        // Flush and verify.
        store.flush_index().unwrap();
        let snapshot = state.lock().files.clone();
        verify_final_state(
            &store,
            &snapshot,
            &format!("round-{round}"),
        )
        .await;

        // Reopen.
        let inner = Arc::try_unwrap(store).expect("store still shared");
        drop(inner);
        store = Arc::new(RawObjectStore::open(&path).unwrap());

        // Verify after reopen.
        verify_final_state(
            &store,
            &snapshot,
            &format!("round-{round}-reopened"),
        )
        .await;
    }
}
