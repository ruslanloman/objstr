/// Performance benchmark suite for RawObjectStore.
///
/// Measures throughput for writes, reads, deletes, overwrites, and mixed
/// workloads across a range of object sizes. Run with:
///
///   cargo test --release --test perf_bench -- --nocapture
///
/// The results are printed as a table for easy before/after comparison.
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const DEVICE_SIZE: u64 = 512 * 1024 * 1024; // 512 MB loopback

fn make_store(direct_io: bool) -> (Arc<RawObjectStore>, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_size(tmp.path(), DEVICE_SIZE, direct_io).unwrap();
    (Arc::new(store), tmp)
}

fn random_payload(rng: &mut StdRng, size: usize) -> Bytes {
    let mut buf = vec![0u8; size];
    rng.fill(&mut buf[..]);
    Bytes::from(buf)
}

struct BenchResult {
    name: String,
    ops: usize,
    total_bytes: u64,
    elapsed: Duration,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
    fn mb_per_sec(&self) -> f64 {
        (self.total_bytes as f64 / (1024.0 * 1024.0)) / self.elapsed.as_secs_f64()
    }
}

fn print_results(results: &[BenchResult]) {
    println!();
    println!(
        "{:<40} {:>8} {:>12} {:>12} {:>12}",
        "Benchmark", "Ops", "Elapsed(ms)", "Ops/sec", "MB/sec"
    );
    println!("{}", "-".repeat(88));
    for r in results {
        println!(
            "{:<40} {:>8} {:>12.1} {:>12.1} {:>12.2}",
            r.name,
            r.ops,
            r.elapsed.as_secs_f64() * 1000.0,
            r.ops_per_sec(),
            r.mb_per_sec(),
        );
    }
    println!("{}", "-".repeat(88));
    println!();
}

// ---------------------------------------------------------------------------
// individual benchmarks
// ---------------------------------------------------------------------------

/// Sequential writes: create N objects of the given size.
fn bench_seq_write(
    store: &RawObjectStore,
    rt: &tokio::runtime::Runtime,
    count: usize,
    payload_size: usize,
    label: &str,
) -> BenchResult {
    let mut rng = StdRng::seed_from_u64(42);
    // Pre-generate payloads so allocation time is excluded
    let payloads: Vec<Bytes> = (0..count).map(|_| random_payload(&mut rng, payload_size)).collect();

    let start = Instant::now();
    for (i, payload) in payloads.into_iter().enumerate() {
        let path = Path::from(format!("bench/write/{}/{}", label, i));
        rt.block_on(store.put(&path, PutPayload::from(payload)))
            .unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("seq_write {} x {}", count, label),
        ops: count,
        total_bytes: count as u64 * payload_size as u64,
        elapsed,
    }
}

/// Sequential reads: read N existing objects.
fn bench_seq_read(
    store: &RawObjectStore,
    rt: &tokio::runtime::Runtime,
    count: usize,
    payload_size: usize,
    label: &str,
) -> BenchResult {
    let start = Instant::now();
    for i in 0..count {
        let path = Path::from(format!("bench/write/{}/{}", label, i));
        let result = rt.block_on(store.get(&path)).unwrap();
        let data = rt.block_on(result.bytes()).unwrap();
        assert_eq!(data.len(), payload_size);
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("seq_read {} x {}", count, label),
        ops: count,
        total_bytes: count as u64 * payload_size as u64,
        elapsed,
    }
}

/// Overwrite existing objects (simulates append/update pattern).
fn bench_overwrite(
    store: &RawObjectStore,
    rt: &tokio::runtime::Runtime,
    count: usize,
    payload_size: usize,
    label: &str,
) -> BenchResult {
    let mut rng = StdRng::seed_from_u64(99);
    let payloads: Vec<Bytes> = (0..count).map(|_| random_payload(&mut rng, payload_size)).collect();

    let start = Instant::now();
    for (i, payload) in payloads.into_iter().enumerate() {
        let path = Path::from(format!("bench/write/{}/{}", label, i));
        rt.block_on(store.put(&path, PutPayload::from(payload)))
            .unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("overwrite {} x {}", count, label),
        ops: count,
        total_bytes: count as u64 * payload_size as u64,
        elapsed,
    }
}

/// Delete all objects created by bench_seq_write.
fn bench_delete(
    store: &RawObjectStore,
    rt: &tokio::runtime::Runtime,
    count: usize,
    payload_size: usize,
    label: &str,
) -> BenchResult {
    let start = Instant::now();
    for i in 0..count {
        let path = Path::from(format!("bench/write/{}/{}", label, i));
        rt.block_on(store.delete(&path)).unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("delete {} x {}", count, label),
        ops: count,
        total_bytes: count as u64 * payload_size as u64,
        elapsed,
    }
}

/// Flush index to disk and measure time.
fn bench_flush(
    store: &RawObjectStore,
    file_count: usize,
) -> BenchResult {
    let start = Instant::now();
    store.flush_index().unwrap();
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("flush_index ({} files)", file_count),
        ops: 1,
        total_bytes: 0,
        elapsed,
    }
}

/// Mixed workload: interleave writes, reads, overwrites, and deletes.
fn bench_mixed(
    store: &RawObjectStore,
    rt: &tokio::runtime::Runtime,
    iterations: usize,
    payload_size: usize,
) -> BenchResult {
    let mut rng = StdRng::seed_from_u64(77);
    let mut existing: Vec<String> = Vec::new();
    let mut ops = 0usize;
    let mut total_bytes = 0u64;

    let start = Instant::now();
    for i in 0..iterations {
        let op = rng.gen_range(0..100);
        if op < 40 || existing.is_empty() {
            // 40%: write new object
            let key = format!("bench/mixed/{}", i);
            let payload = random_payload(&mut rng, payload_size);
            let path = Path::from(key.as_str());
            rt.block_on(store.put(&path, PutPayload::from(payload)))
                .unwrap();
            existing.push(key);
            total_bytes += payload_size as u64;
        } else if op < 70 {
            // 30%: read random existing object
            let idx = rng.gen_range(0..existing.len());
            let path = Path::from(existing[idx].as_str());
            let result = rt.block_on(store.get(&path)).unwrap();
            let data = rt.block_on(result.bytes()).unwrap();
            total_bytes += data.len() as u64;
        } else if op < 85 {
            // 15%: overwrite random existing object
            let idx = rng.gen_range(0..existing.len());
            let path = Path::from(existing[idx].as_str());
            let payload = random_payload(&mut rng, payload_size);
            rt.block_on(store.put(&path, PutPayload::from(payload)))
                .unwrap();
            total_bytes += payload_size as u64;
        } else {
            // 15%: delete random existing object
            let idx = rng.gen_range(0..existing.len());
            let path = Path::from(existing[idx].as_str());
            rt.block_on(store.delete(&path)).unwrap();
            existing.swap_remove(idx);
        }
        ops += 1;
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("mixed {}ops x {}B", iterations, payload_size),
        ops,
        total_bytes,
        elapsed,
    }
}

/// Concurrent reads: N tokio tasks reading simultaneously.
fn bench_concurrent_read(
    store: Arc<RawObjectStore>,
    rt: &tokio::runtime::Runtime,
    count: usize,
    payload_size: usize,
    concurrency: usize,
) -> BenchResult {
    let start = Instant::now();
    rt.block_on(async {
        let mut handles = Vec::new();
        for t in 0..concurrency {
            let s = Arc::clone(&store);
            let per_task = count / concurrency;
            let offset = t * per_task;
            handles.push(tokio::spawn(async move {
                for i in offset..offset + per_task {
                    let path = Path::from(format!("bench/write/4KB/{}", i));
                    let result = s.get(&path).await.unwrap();
                    let data = result.bytes().await.unwrap();
                    assert_eq!(data.len(), payload_size);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("concurrent_read {}x{}t x {}B", count, concurrency, payload_size),
        ops: count,
        total_bytes: count as u64 * payload_size as u64,
        elapsed,
    }
}

// ---------------------------------------------------------------------------
// main test
// ---------------------------------------------------------------------------

#[test]
fn perf_benchmark_suite() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    println!("\n=== RawObjectStore Performance Benchmark ===\n");
    println!("Device: {} MB loopback (buffered I/O)", DEVICE_SIZE / (1024 * 1024));

    let mut results = Vec::new();

    // -----------------------------------------------------------------------
    // Phase 1: Sequential writes at various sizes
    // -----------------------------------------------------------------------
    {
        let (store, _tmp) = make_store(false);

        // Small objects: 4 KB x 2000
        results.push(bench_seq_write(&store, &rt, 2000, 4096, "4KB"));
        // Medium objects: 64 KB x 500
        results.push(bench_seq_write(&store, &rt, 500, 64 * 1024, "64KB"));
        // Large objects: 1 MB x 100
        results.push(bench_seq_write(&store, &rt, 100, 1024 * 1024, "1MB"));

        // -----------------------------------------------------------------------
        // Phase 2: Sequential reads at various sizes
        // -----------------------------------------------------------------------
        results.push(bench_seq_read(&store, &rt, 2000, 4096, "4KB"));
        results.push(bench_seq_read(&store, &rt, 500, 64 * 1024, "64KB"));
        results.push(bench_seq_read(&store, &rt, 100, 1024 * 1024, "1MB"));

        // -----------------------------------------------------------------------
        // Phase 3: Overwrites (append/update simulation)
        // -----------------------------------------------------------------------
        results.push(bench_overwrite(&store, &rt, 2000, 4096, "4KB"));
        results.push(bench_overwrite(&store, &rt, 500, 64 * 1024, "64KB"));

        // -----------------------------------------------------------------------
        // Phase 4: Flush index with various file counts
        // -----------------------------------------------------------------------
        results.push(bench_flush(&store, 2600));

        // -----------------------------------------------------------------------
        // Phase 5: Concurrent reads (4 threads)
        // -----------------------------------------------------------------------
        store.flush_index().unwrap(); // ensure data is on disk for the readonly handle
        let store_arc = Arc::new(
            RawObjectStore::open_readonly(
                _tmp.path(),
            ).unwrap()
        );
        // Re-populate for concurrent read (open() loads fresh from disk)
        // The 2000 x 4KB objects written in phase 1+3 are still there
        results.push(bench_concurrent_read(Arc::clone(&store_arc), &rt, 2000, 4096, 4));

        // -----------------------------------------------------------------------
        // Phase 6: Deletes
        // -----------------------------------------------------------------------
        results.push(bench_delete(&store, &rt, 2000, 4096, "4KB"));
        results.push(bench_delete(&store, &rt, 500, 64 * 1024, "64KB"));
        results.push(bench_delete(&store, &rt, 100, 1024 * 1024, "1MB"));
    }

    // -----------------------------------------------------------------------
    // Phase 7: Mixed workload (fresh store)
    // -----------------------------------------------------------------------
    {
        let (store, _tmp) = make_store(false);
        results.push(bench_mixed(&store, &rt, 5000, 4096));
        results.push(bench_mixed(&store, &rt, 1000, 64 * 1024));
    }

    // -----------------------------------------------------------------------
    // Phase 8: Open + load index timing
    // -----------------------------------------------------------------------
    {
        let (store, tmp) = make_store(false);
        // Write 1000 objects so the index has some entries
        let mut rng = StdRng::seed_from_u64(123);
        for i in 0..1000 {
            let path = Path::from(format!("bench/open/{}", i));
            let payload = random_payload(&mut rng, 4096);
            rt.block_on(store.put(&path, PutPayload::from(payload)))
                .unwrap();
        }
        store.flush_index().unwrap();
        drop(store);

        let start = Instant::now();
        let _store2 = RawObjectStore::open(tmp.path()).unwrap();
        let elapsed = start.elapsed();
        results.push(BenchResult {
            name: "open + load index (1000 files)".to_string(),
            ops: 1,
            total_bytes: 0,
            elapsed,
        });
    }

    print_results(&results);
}
