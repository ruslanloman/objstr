/// Performance benchmark for ShardedObjectStore.
///
/// Measures write and read throughput across 3 shards with replication,
/// using small (<=64 KB), medium (128 KB -- 1 MB), and large (1 -- 6 MB)
/// objects totalling ~5 GB.
///
/// Run with:
///   cargo test --release --test perf_bench -- --nocapture
///
/// Results are printed as a table.
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rawobjstr::store::RawObjectStore;
use tempfile::NamedTempFile;

use shardedobjstr::ShardedObjectStore;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const SHARD_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4 GB per shard (3 shards = 12 GB raw, ~6 GB usable with RF=2)
const SEED: u64 = 42;
const REPLICATION_FACTOR: usize = 2;

// Small objects: <= 64 KB — target ~1.5 GB
const N_SMALL: usize = 40_000;
const SMALL_MIN: usize = 512;
const SMALL_MAX: usize = 65_536;

// Medium objects: 128 KB -- 1 MB — target ~1.5 GB
const N_MEDIUM: usize = 2_500;
const MEDIUM_MIN: usize = 131_072;
const MEDIUM_MAX: usize = 1_048_576;

// Large objects: 1 MB -- 6 MB — target ~2 GB
const N_LARGE: usize = 600;
const LARGE_MIN: usize = 1_048_576;
const LARGE_MAX: usize = 6_291_456;

const N_RANDOM_READS: usize = 2_000;

// Flush after every N writes per category
const FLUSH_SMALL: usize = 1_000;
const FLUSH_MEDIUM: usize = 50;
const FLUSH_LARGE: usize = 10;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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
        "{:<45} {:>8} {:>12} {:>12} {:>12}",
        "Benchmark", "Ops", "Elapsed(ms)", "Ops/sec", "MB/sec"
    );
    println!("{}", "-".repeat(93));
    for r in results {
        println!(
            "{:<45} {:>8} {:>12.1} {:>12.1} {:>12.2}",
            r.name,
            r.ops,
            r.elapsed.as_secs_f64() * 1000.0,
            r.ops_per_sec(),
            r.mb_per_sec(),
        );
    }
    println!("{}", "-".repeat(93));
    println!();
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

struct Workload {
    small_sizes: Vec<usize>,
    medium_sizes: Vec<usize>,
    large_sizes: Vec<usize>,
    pattern: Vec<u8>,
}

impl Workload {
    fn generate() -> Self {
        let mut rng = StdRng::seed_from_u64(SEED);
        let small_sizes: Vec<usize> =
            (0..N_SMALL).map(|_| rng.gen_range(SMALL_MIN..=SMALL_MAX)).collect();
        let medium_sizes: Vec<usize> =
            (0..N_MEDIUM).map(|_| rng.gen_range(MEDIUM_MIN..=MEDIUM_MAX)).collect();
        let large_sizes: Vec<usize> =
            (0..N_LARGE).map(|_| rng.gen_range(LARGE_MIN..=LARGE_MAX)).collect();

        // Deterministic pattern buffer (6 MB + 1)
        let mut pattern = vec![0u8; LARGE_MAX + 1];
        for (i, b) in pattern.iter_mut().enumerate() {
            *b = (i.wrapping_mul(7).wrapping_add(13) % 251) as u8;
        }

        Self { small_sizes, medium_sizes, large_sizes, pattern }
    }

    fn payload(&self, size: usize) -> PutPayload {
        PutPayload::from(Bytes::copy_from_slice(&self.pattern[..size]))
    }

    fn print_summary(&self) {
        let s: u64 = self.small_sizes.iter().map(|s| *s as u64).sum();
        let m: u64 = self.medium_sizes.iter().map(|s| *s as u64).sum();
        let l: u64 = self.large_sizes.iter().map(|s| *s as u64).sum();
        let total = s + m + l;
        let total_files = N_SMALL + N_MEDIUM + N_LARGE;
        println!("Workload: {} files, {:.1} GB total", total_files, total as f64 / (1024.0 * 1024.0 * 1024.0));
        println!(
            "  {} small  ({} B -- {} KB) = {:.1} MB",
            N_SMALL, SMALL_MIN, SMALL_MAX / 1024, s as f64 / 1_048_576.0
        );
        println!(
            "  {} medium ({} KB -- {} MB) = {:.1} MB",
            N_MEDIUM, MEDIUM_MIN / 1024, MEDIUM_MAX / 1_048_576, m as f64 / 1_048_576.0
        );
        println!(
            "  {} large  ({} MB -- {} MB) = {:.1} MB",
            N_LARGE, LARGE_MIN / 1_048_576, LARGE_MAX / 1_048_576, l as f64 / 1_048_576.0
        );
        println!("  Replication factor: {} (each object on {} shards)", REPLICATION_FACTOR, REPLICATION_FACTOR);
        println!();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_cluster() -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, Vec<NamedTempFile>) {
    let mut stores: Vec<Arc<dyn ObjectStore>> = Vec::new();
    let mut raws = Vec::new();
    let mut tmps = Vec::new();

    for i in 0..3 {
        let tmp = NamedTempFile::new().unwrap();
        let raw = Arc::new(
            RawObjectStore::format_with_size(tmp.path(), SHARD_SIZE, false)
                .unwrap_or_else(|e| panic!("failed to format shard {}: {}", i, e)),
        );
        stores.push(raw.clone() as Arc<dyn ObjectStore>);
        raws.push(raw);
        tmps.push(tmp);
    }

    let cluster = ShardedObjectStore::new(stores, REPLICATION_FACTOR);
    (cluster, raws, tmps)
}

fn flush_all(raws: &[Arc<RawObjectStore>]) {
    for r in raws {
        r.flush_index().expect("flush_index failed");
    }
}

fn path_for(category: &str, index: usize) -> Path {
    Path::from(format!("bench/{}/{:06}.dat", category, index))
}

// ---------------------------------------------------------------------------
// Benchmark phases
// ---------------------------------------------------------------------------

async fn bench_write(
    cluster: &ShardedObjectStore,
    raws: &[Arc<RawObjectStore>],
    wl: &Workload,
    category: &str,
    sizes: &[usize],
    flush_interval: usize,
) -> (BenchResult, Vec<String>) {
    let mut paths = Vec::with_capacity(sizes.len());
    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    for (i, &size) in sizes.iter().enumerate() {
        let p = path_for(category, i);
        cluster.put(&p, wl.payload(size)).await.expect("put failed");
        paths.push(p.to_string());
        total_bytes += size as u64;

        if (i + 1) % flush_interval == 0 {
            flush_all(raws);
        }
    }
    flush_all(raws);
    let elapsed = start.elapsed();

    (
        BenchResult {
            name: format!("write_{}", category),
            ops: sizes.len(),
            total_bytes,
            elapsed,
        },
        paths,
    )
}

async fn bench_seq_read(
    cluster: &ShardedObjectStore,
    category: &str,
    sizes: &[usize],
) -> BenchResult {
    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    for (i, &size) in sizes.iter().enumerate() {
        let p = path_for(category, i);
        let result = cluster.get(&p).await.expect("get failed");
        let data = result.bytes().await.expect("bytes failed");
        assert_eq!(data.len(), size, "size mismatch at {}", p);
        total_bytes += size as u64;
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("seq_read_{}", category),
        ops: sizes.len(),
        total_bytes,
        elapsed,
    }
}

async fn bench_random_read(
    cluster: &ShardedObjectStore,
    all_paths: &[(String, usize)],
    count: usize,
) -> BenchResult {
    let mut rng = StdRng::seed_from_u64(99);
    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    for _ in 0..count {
        let idx = rng.gen_range(0..all_paths.len());
        let (ref key, expected_size) = all_paths[idx];
        let p = Path::from(key.as_str());
        let result = cluster.get(&p).await.expect("random get failed");
        let data = result.bytes().await.expect("bytes failed");
        assert_eq!(data.len(), expected_size);
        total_bytes += expected_size as u64;
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("random_read_{}", count),
        ops: count,
        total_bytes,
        elapsed,
    }
}

async fn bench_rebuild_catalog(
    cluster: &ShardedObjectStore,
) -> BenchResult {
    cluster.catalog().clear();

    let start = Instant::now();
    let count = cluster.rebuild_catalog().await.expect("rebuild_catalog failed");
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("rebuild_catalog ({} objects)", count),
        ops: count,
        total_bytes: 0,
        elapsed,
    }
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

#[test]
fn perf_benchmark_suite() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    println!("\n=== ShardedObjectStore Performance Benchmark ===\n");
    println!(
        "Cluster: 3 shards x {} GB each, replication factor {}",
        SHARD_SIZE / (1024 * 1024 * 1024),
        REPLICATION_FACTOR,
    );

    let (cluster, raws, _tmps) = make_cluster();
    let wl = Workload::generate();
    wl.print_summary();

    let mut results = Vec::new();

    // -----------------------------------------------------------------------
    // Phase 1: Sequential writes
    // -----------------------------------------------------------------------
    println!("Phase 1: Writing small objects...");
    let (r, small_paths) = rt.block_on(bench_write(
        &cluster, &raws, &wl, "small", &wl.small_sizes, FLUSH_SMALL,
    ));
    println!(
        "  {} files, {:.1} MB in {:.2}s ({:.1} MB/s)",
        r.ops, r.total_bytes as f64 / 1_048_576.0, r.elapsed.as_secs_f64(), r.mb_per_sec()
    );
    results.push(r);

    println!("Phase 1: Writing medium objects...");
    let (r, medium_paths) = rt.block_on(bench_write(
        &cluster, &raws, &wl, "medium", &wl.medium_sizes, FLUSH_MEDIUM,
    ));
    println!(
        "  {} files, {:.1} MB in {:.2}s ({:.1} MB/s)",
        r.ops, r.total_bytes as f64 / 1_048_576.0, r.elapsed.as_secs_f64(), r.mb_per_sec()
    );
    results.push(r);

    println!("Phase 1: Writing large objects...");
    let (r, large_paths) = rt.block_on(bench_write(
        &cluster, &raws, &wl, "large", &wl.large_sizes, FLUSH_LARGE,
    ));
    println!(
        "  {} files, {:.1} MB in {:.2}s ({:.1} MB/s)",
        r.ops, r.total_bytes as f64 / 1_048_576.0, r.elapsed.as_secs_f64(), r.mb_per_sec()
    );
    results.push(r);

    // -----------------------------------------------------------------------
    // Phase 2: Sequential reads
    // -----------------------------------------------------------------------
    println!("\nPhase 2: Sequential reads...");

    let r = rt.block_on(bench_seq_read(&cluster, "small", &wl.small_sizes));
    println!(
        "  small:  {} files, {:.1} MB/s",
        r.ops, r.mb_per_sec()
    );
    results.push(r);

    let r = rt.block_on(bench_seq_read(&cluster, "medium", &wl.medium_sizes));
    println!(
        "  medium: {} files, {:.1} MB/s",
        r.ops, r.mb_per_sec()
    );
    results.push(r);

    let r = rt.block_on(bench_seq_read(&cluster, "large", &wl.large_sizes));
    println!(
        "  large:  {} files, {:.1} MB/s",
        r.ops, r.mb_per_sec()
    );
    results.push(r);

    // -----------------------------------------------------------------------
    // Phase 3: Random reads across all sizes
    // -----------------------------------------------------------------------
    println!("\nPhase 3: Random reads ({} ops across all sizes)...", N_RANDOM_READS);

    // Build combined path list with expected sizes
    let mut all_paths: Vec<(String, usize)> = Vec::new();
    for (i, &sz) in wl.small_sizes.iter().enumerate() {
        all_paths.push((small_paths[i].clone(), sz));
    }
    for (i, &sz) in wl.medium_sizes.iter().enumerate() {
        all_paths.push((medium_paths[i].clone(), sz));
    }
    for (i, &sz) in wl.large_sizes.iter().enumerate() {
        all_paths.push((large_paths[i].clone(), sz));
    }
    // Shuffle for randomness
    let mut rng = StdRng::seed_from_u64(77);
    all_paths.shuffle(&mut rng);

    let r = rt.block_on(bench_random_read(&cluster, &all_paths, N_RANDOM_READS));
    println!(
        "  {} ops, {:.1} MB in {:.2}s ({:.1} MB/s, {:.0} ops/s)",
        r.ops, r.total_bytes as f64 / 1_048_576.0, r.elapsed.as_secs_f64(),
        r.mb_per_sec(), r.ops_per_sec()
    );
    results.push(r);

    // -----------------------------------------------------------------------
    // Phase 4: Catalog rebuild (recovery scenario)
    // -----------------------------------------------------------------------
    println!("\nPhase 4: Catalog rebuild from shards...");
    let r = rt.block_on(bench_rebuild_catalog(&cluster));
    println!(
        "  {} objects recovered in {:.2}s ({:.0} ops/s)",
        r.ops, r.elapsed.as_secs_f64(), r.ops_per_sec()
    );
    results.push(r);

    // -----------------------------------------------------------------------
    // Phase 5: Per-shard stats
    // -----------------------------------------------------------------------
    println!("\nPer-shard statistics:");
    for (i, raw) in raws.iter().enumerate() {
        let info = raw.device_info();
        println!(
            "  Shard {}: {} files, {:.1} MB used, {:.1} MB free",
            i,
            info.file_count,
            info.data_bytes_stored as f64 / 1_048_576.0,
            info.free_space as f64 / 1_048_576.0,
        );
    }

    // -----------------------------------------------------------------------
    // Summary table
    // -----------------------------------------------------------------------
    print_results(&results);
}
