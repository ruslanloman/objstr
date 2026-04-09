//! bench_storage: Compare ext4 vs RawObjectStore (img) vs RawObjectStore (/dev/sdb)
//!
//! Usage:  bench_storage [ext4|img|raw|all]
//!
//! Writes ~15 GB of data across ~63,512 files with varying sizes:
//!   - 50,000 small files  (512 B -- 64 KB)  = ~1.6 GB
//!   - 1,500 medium files  (128 KB -- 2 MB)  = ~1.6 GB
//!   - 2,000 large files   (1 MB -- 8 MB)    = ~9.0 GB
//!   - 10,000 tiny appends (256 B -- 4 KB)   = ~21 MB
//!   - 12 huge files       (1 MB -- 512 MB)  = ~3 GB
//!
//! The dataset (~15 GB) is sized to exceed typical VM RAM (3-8 GB)
//! so that cold-cache reads hit actual storage, not the kernel page cache.
//!
//! Measures: write throughput, sequential read, random read, list,
//!           optimize (consolidate small files), and post-optimize reads.

use std::path::Path as StdPath;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore, PutPayload};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use rawobjstr::io::DeviceIo;
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::Compression;

// -- Configuration --------------------------------------------------------

const IMG_SIZE: u64 = 20 * 1024 * 1024 * 1024; // 20 GB loopback image
const INDEX_SLOT: u64 = 128 * 1024 * 1024; // 128 MB index slots
const SEED: u64 = 42;

const N_SMALL: usize = 50_000;
const SMALL_MIN: usize = 512;
const SMALL_MAX: usize = 65_536; // 64 KB

const N_MEDIUM: usize = 1_500;
const MEDIUM_MIN: usize = 131_072; // 128 KB
const MEDIUM_MAX: usize = 2_097_152; // 2 MB

const N_LARGE: usize = 2_000;
const LARGE_MIN: usize = 1_048_576; // 1 MB
const LARGE_MAX: usize = 8_388_608; // 8 MB

const N_TINY: usize = 10_000;
const TINY_MIN: usize = 256;
const TINY_MAX: usize = 4096; // 4 KB

const N_HUGE: usize = 12;
const HUGE_MIN: usize = 1_048_576; // 1 MB
const HUGE_MAX: usize = 536_870_912; // 512 MB

const N_RANDOM_READS: usize = 5_000;
const CONSOLIDATE_TARGET: usize = 4 * 1024 * 1024; // 4 MB

// Flush intervals (RawObjectStore only)
const FLUSH_SMALL: usize = 2_000;
const FLUSH_MEDIUM: usize = 100;
const FLUSH_LARGE: usize = 20;
const FLUSH_TINY: usize = 5_000;
const FLUSH_HUGE: usize = 1;

// -- Types ----------------------------------------------------------------

struct BenchStore {
    store: Arc<dyn ObjectStore>,
    raw: Option<Arc<RawObjectStore>>,
    /// If true, call sync(2) at flush intervals (for ext4+sync fairness test)
    do_sync: bool,
    name: String,
}

impl BenchStore {
    fn flush(&self) {
        if let Some(r) = &self.raw {
            r.flush_index().expect("flush_index failed");
        }
        if self.do_sync {
            // Triple sync: matches the write-barrier pattern in flush_index
            // (index write + sync, primary SB + sync, backup SB + sync)
            std::process::Command::new("sync").status().ok();
            std::process::Command::new("sync").status().ok();
            std::process::Command::new("sync").status().ok();
        }
    }
    fn needs_flush(&self) -> bool {
        self.raw.is_some() || self.do_sync
    }
}

#[derive(Clone)]
struct BenchResult {
    phase: String,
    files: usize,
    bytes: u64,
    elapsed_ms: u128,
}

impl BenchResult {
    fn mbs(&self) -> f64 {
        if self.elapsed_ms == 0 {
            return 0.0;
        }
        self.bytes as f64 / 1_048_576.0 / (self.elapsed_ms as f64 / 1000.0)
    }
    fn ops(&self) -> f64 {
        if self.elapsed_ms == 0 {
            return 0.0;
        }
        self.files as f64 / (self.elapsed_ms as f64 / 1000.0)
    }
    fn secs(&self) -> f64 {
        self.elapsed_ms as f64 / 1000.0
    }
    fn mb(&self) -> f64 {
        self.bytes as f64 / 1_048_576.0
    }
}

// -- Workload generation --------------------------------------------------

struct Workload {
    small_sizes: Vec<usize>,
    medium_sizes: Vec<usize>,
    large_sizes: Vec<usize>,
    tiny_sizes: Vec<usize>,
    huge_sizes: Vec<usize>,
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
        let tiny_sizes: Vec<usize> =
            (0..N_TINY).map(|_| rng.gen_range(TINY_MIN..=TINY_MAX)).collect();
        let huge_sizes: Vec<usize> =
            (0..N_HUGE).map(|_| rng.gen_range(HUGE_MIN..=HUGE_MAX)).collect();

        // Deterministic pattern buffer (8 MB + 1) -- reused via repetition
        // for huge files that exceed this size.
        let mut pattern = vec![0u8; LARGE_MAX + 1];
        for (i, b) in pattern.iter_mut().enumerate() {
            *b = (i.wrapping_mul(7).wrapping_add(13) % 251) as u8;
        }

        Self { small_sizes, medium_sizes, large_sizes, tiny_sizes, huge_sizes, pattern }
    }

    fn payload(&self, size: usize) -> PutPayload {
        if size <= self.pattern.len() {
            PutPayload::from(Bytes::copy_from_slice(&self.pattern[..size]))
        } else {
            // For huge files: tile the pattern buffer to fill the target size
            let mut buf = Vec::with_capacity(size);
            while buf.len() < size {
                let chunk = (size - buf.len()).min(self.pattern.len());
                buf.extend_from_slice(&self.pattern[..chunk]);
            }
            PutPayload::from(Bytes::from(buf))
        }
    }

    fn print_summary(&self) {
        let s: u64 = self.small_sizes.iter().map(|s| *s as u64).sum();
        let m: u64 = self.medium_sizes.iter().map(|s| *s as u64).sum();
        let l: u64 = self.large_sizes.iter().map(|s| *s as u64).sum();
        let t: u64 = self.tiny_sizes.iter().map(|s| *s as u64).sum();
        let h: u64 = self.huge_sizes.iter().map(|s| *s as u64).sum();
        let total = s + m + l + t + h;
        let total_files = N_SMALL + N_MEDIUM + N_LARGE + N_TINY + N_HUGE;
        println!("Workload: {} files, {:.1} MB total", total_files, total as f64 / 1_048_576.0);
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
        println!(
            "  {} tiny   ({} B -- {} KB) = {:.1} MB",
            N_TINY, TINY_MIN, TINY_MAX / 1024, t as f64 / 1_048_576.0
        );
        println!(
            "  {} huge   ({} MB -- {} MB) = {:.1} MB",
            N_HUGE, HUGE_MIN / 1_048_576, HUGE_MAX / 1_048_576, h as f64 / 1_048_576.0
        );
        println!();
    }
}

// -- Helpers --------------------------------------------------------------

fn drop_caches() {
    std::process::Command::new("sync").status().ok();
    // Use sudo -S with password piped via stdin
    let mut child = std::process::Command::new("sudo")
        .args(["-S", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok();
    if let Some(ref mut c) = child {
        use std::io::Write;
        if let Some(ref mut stdin) = c.stdin {
            let _ = stdin.write_all(b"vmpassword\n");
        }
        let _ = c.wait();
    }
}

fn path_for(category: &str, index: usize) -> Path {
    Path::from(format!("bench/{}/{:06}.dat", category, index))
}

fn print_result_line(r: &BenchResult) {
    println!(
        " done. {:.1} MB in {:.2}s  ({:.1} MB/s, {:.0} ops/s)",
        r.mb(),
        r.secs(),
        r.mbs(),
        r.ops()
    );
}

// -- Benchmark phases -----------------------------------------------------

async fn write_batch(
    bs: &BenchStore,
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
        bs.store.put(&p, wl.payload(size)).await.expect("put failed");
        paths.push(p.to_string());
        total_bytes += size as u64;

        if bs.needs_flush() && (i + 1) % flush_interval == 0 {
            bs.flush();
        }
    }
    if bs.needs_flush() {
        bs.flush();
    }
    let elapsed = start.elapsed().as_millis();

    (
        BenchResult {
            phase: format!("write_{}", category),
            files: sizes.len(),
            bytes: total_bytes,
            elapsed_ms: elapsed,
        },
        paths,
    )
}

async fn read_sequential(
    bs: &BenchStore,
    paths: &[String],
    phase_name: &str,
) -> BenchResult {
    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    for p in paths {
        let result = bs.store.get(&Path::from(p.as_str())).await.expect("get failed");
        let data = result.bytes().await.expect("read bytes failed");
        total_bytes += data.len() as u64;
    }
    let elapsed = start.elapsed().as_millis();

    BenchResult {
        phase: phase_name.to_string(),
        files: paths.len(),
        bytes: total_bytes,
        elapsed_ms: elapsed,
    }
}

async fn read_random(
    bs: &BenchStore,
    paths: &[String],
    n: usize,
    phase_name: &str,
) -> BenchResult {
    let n = n.min(paths.len());
    let mut rng = StdRng::seed_from_u64(SEED + 1000);
    let mut indices: Vec<usize> = (0..paths.len()).collect();
    indices.shuffle(&mut rng);
    indices.truncate(n);

    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    for &idx in &indices {
        let result = bs.store
            .get(&Path::from(paths[idx].as_str()))
            .await
            .expect("get failed");
        let data = result.bytes().await.expect("read bytes failed");
        total_bytes += data.len() as u64;
    }
    let elapsed = start.elapsed().as_millis();

    BenchResult {
        phase: phase_name.to_string(),
        files: n,
        bytes: total_bytes,
        elapsed_ms: elapsed,
    }
}

async fn list_all(bs: &BenchStore, phase_name: &str) -> BenchResult {
    let start = Instant::now();
    let items: Vec<object_store::ObjectMeta> = bs
        .store
        .list(Some(&Path::from("bench")))
        .try_collect()
        .await
        .expect("list failed");
    let elapsed = start.elapsed().as_millis();

    let total_bytes: u64 = items.iter().map(|m| m.size as u64).sum();

    BenchResult {
        phase: phase_name.to_string(),
        files: items.len(),
        bytes: total_bytes,
        elapsed_ms: elapsed,
    }
}

async fn optimize(
    bs: &BenchStore,
    small_paths: &[String],
    tiny_paths: &[String],
) -> (BenchResult, Vec<String>) {
    let all_to_compact: Vec<&String> = small_paths.iter().chain(tiny_paths.iter()).collect();
    let source_count = all_to_compact.len();
    let mut consolidated_paths = Vec::new();
    let mut total_io: u64 = 0;
    let mut buffer = Vec::new();
    let mut cons_idx = 0usize;

    let start = Instant::now();

    // Read all small + tiny files, accumulate into consolidated files
    for p in &all_to_compact {
        let result = bs.store.get(&Path::from(p.as_str())).await.expect("get failed");
        let data = result.bytes().await.expect("read bytes failed");
        total_io += data.len() as u64;
        buffer.extend_from_slice(&data);

        if buffer.len() >= CONSOLIDATE_TARGET {
            let cons_path = path_for("optimized", cons_idx);
            let payload = PutPayload::from(Bytes::from(std::mem::take(&mut buffer)));
            total_io += payload.content_length() as u64;
            bs.store.put(&cons_path, payload).await.expect("put consolidated");
            consolidated_paths.push(cons_path.to_string());
            cons_idx += 1;
        }
    }
    // Write remainder
    if !buffer.is_empty() {
        let cons_path = path_for("optimized", cons_idx);
        total_io += buffer.len() as u64;
        let payload = PutPayload::from(Bytes::from(buffer));
        bs.store.put(&cons_path, payload).await.expect("put consolidated");
        consolidated_paths.push(cons_path.to_string());
    }

    // Delete all originals
    for p in &all_to_compact {
        bs.store
            .delete(&Path::from(p.as_str()))
            .await
            .expect("delete failed");
    }

    if bs.needs_flush() {
        bs.flush();
    }

    let elapsed = start.elapsed().as_millis();

    let result = BenchResult {
        phase: format!("optimize ({}->{})", source_count, consolidated_paths.len()),
        files: source_count,
        bytes: total_io,
        elapsed_ms: elapsed,
    };
    (result, consolidated_paths)
}

// -- Main benchmark runner ------------------------------------------------

async fn run_benchmark(bs: &BenchStore, wl: &Workload) -> Vec<BenchResult> {
    let mut results = Vec::new();

    println!();
    println!("============================================================");
    println!("  BENCHMARK: {}", bs.name);
    println!("============================================================");
    println!();

    // -- Writes --
    print!("  Writing {} small files...", N_SMALL);
    let (r, small_paths) =
        write_batch(bs, wl, "small", &wl.small_sizes, FLUSH_SMALL).await;
    print_result_line(&r);
    results.push(r);

    print!("  Writing {} medium files...", N_MEDIUM);
    let (r, medium_paths) =
        write_batch(bs, wl, "medium", &wl.medium_sizes, FLUSH_MEDIUM).await;
    print_result_line(&r);
    results.push(r);

    print!("  Writing {} large files...", N_LARGE);
    let (r, large_paths) =
        write_batch(bs, wl, "large", &wl.large_sizes, FLUSH_LARGE).await;
    print_result_line(&r);
    results.push(r);

    print!("  Writing {} tiny appends...", N_TINY);
    let (r, tiny_paths) =
        write_batch(bs, wl, "tiny", &wl.tiny_sizes, FLUSH_TINY).await;
    print_result_line(&r);
    results.push(r);

    print!("  Writing {} huge files...", N_HUGE);
    let (r, huge_paths) =
        write_batch(bs, wl, "huge", &wl.huge_sizes, FLUSH_HUGE).await;
    print_result_line(&r);
    results.push(r);

    // Total write stats
    let tw_bytes: u64 = results.iter().map(|r| r.bytes).sum();
    let tw_ms: u128 = results.iter().map(|r| r.elapsed_ms).sum();
    let tw_files: usize = results.iter().map(|r| r.files).sum();
    let tw = BenchResult {
        phase: "TOTAL_WRITE".into(),
        files: tw_files,
        bytes: tw_bytes,
        elapsed_ms: tw_ms,
    };
    println!(
        "  --- Total write: {:.1} MB in {:.2}s ({:.1} MB/s, {:.0} ops/s) ---",
        tw.mb(), tw.secs(), tw.mbs(), tw.ops()
    );
    results.push(tw);

    // All paths in write order
    let mut all_paths: Vec<String> = Vec::new();
    all_paths.extend(small_paths.iter().cloned());
    all_paths.extend(medium_paths.iter().cloned());
    all_paths.extend(large_paths.iter().cloned());
    all_paths.extend(tiny_paths.iter().cloned());
    all_paths.extend(huge_paths.iter().cloned());

    // Verify for raw backends
    if let Some(raw) = &bs.raw {
        let report = raw.verify_all();
        println!(
            "  Verify: {} files OK, {} errors",
            report.files_ok,
            report.errors.len()
        );
    }

    // -- Drop caches --
    println!("  Syncing and dropping caches...");
    drop_caches();

    // -- Pre-optimize reads --
    print!("  Sequential read (all {} files)...", all_paths.len());
    let r = read_sequential(bs, &all_paths, "read_seq").await;
    print_result_line(&r);
    results.push(r);

    print!("  Random read ({} of {} files)...", N_RANDOM_READS, all_paths.len());
    let r = read_random(bs, &all_paths, N_RANDOM_READS, "read_random").await;
    print_result_line(&r);
    results.push(r);

    print!("  List all files...");
    let r = list_all(bs, "list").await;
    print_result_line(&r);
    results.push(r);

    // -- Optimize --
    print!("  Optimizing (consolidating small+tiny files)...");
    let (r, consolidated_paths) = optimize(bs, &small_paths, &tiny_paths).await;
    print_result_line(&r);
    results.push(r);

    // Post-optimize path list
    let mut post_paths: Vec<String> = Vec::new();
    post_paths.extend(consolidated_paths.iter().cloned());
    post_paths.extend(medium_paths.iter().cloned());
    post_paths.extend(large_paths.iter().cloned());
    post_paths.extend(huge_paths.iter().cloned());

    println!("  Post-optimize: {} files remaining", post_paths.len());

    // -- Drop caches again --
    println!("  Syncing and dropping caches...");
    drop_caches();

    // -- Post-optimize reads --
    print!("  Sequential read (post-opt, {} files)...", post_paths.len());
    let r = read_sequential(bs, &post_paths, "read_seq_post").await;
    print_result_line(&r);
    results.push(r);

    print!(
        "  Random read (post-opt, {} of {} files)...",
        N_RANDOM_READS.min(post_paths.len()),
        post_paths.len()
    );
    let r = read_random(bs, &post_paths, N_RANDOM_READS, "read_random_post").await;
    print_result_line(&r);
    results.push(r);

    print!("  List (post-opt)...");
    let r = list_all(bs, "list_post").await;
    print_result_line(&r);
    results.push(r);

    results
}

// -- Results table --------------------------------------------------------

fn print_table(name: &str, results: &[BenchResult]) {
    println!();
    println!(
        "+{:-<26}+{:-<8}+{:-<10}+{:-<9}+{:-<10}+{:-<10}+",
        "", "", "", "", "", ""
    );
    println!(
        "| {:<24} | {:>6} | {:>8} | {:>7} | {:>8} | {:>8} |",
        name, "Files", "Size MB", "Time s", "MB/s", "ops/s"
    );
    println!(
        "+{:-<26}+{:-<8}+{:-<10}+{:-<9}+{:-<10}+{:-<10}+",
        "", "", "", "", "", ""
    );
    for r in results {
        println!(
            "| {:<24} | {:>6} | {:>8.1} | {:>7.2} | {:>8.1} | {:>8.0} |",
            r.phase,
            r.files,
            r.mb(),
            r.secs(),
            r.mbs(),
            r.ops()
        );
    }
    println!(
        "+{:-<26}+{:-<8}+{:-<10}+{:-<9}+{:-<10}+{:-<10}+",
        "", "", "", "", "", ""
    );
}

// -- Backend setup --------------------------------------------------------

fn setup_ext4() -> BenchStore {
    let dir = StdPath::new("/tmp/bench_ext4");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("create /tmp/bench_ext4");

    let store = LocalFileSystem::new_with_prefix(dir).expect("LocalFileSystem::new");
    println!("  Setup: ext4 at /tmp/bench_ext4");

    BenchStore {
        store: Arc::new(store),
        raw: None,
        do_sync: false,
        name: "ext4".to_string(),
    }
}

fn cleanup_ext4() {
    let _ = std::fs::remove_dir_all("/tmp/bench_ext4");
}

fn setup_ext4_sync() -> BenchStore {
    let dir = StdPath::new("/tmp/bench_ext4sync");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("create /tmp/bench_ext4sync");

    let store = LocalFileSystem::new_with_prefix(dir).expect("LocalFileSystem::new");
    println!("  Setup: ext4+sync at /tmp/bench_ext4sync");

    BenchStore {
        store: Arc::new(store),
        raw: None,
        do_sync: true,
        name: "ext4+sync".to_string(),
    }
}

fn cleanup_ext4_sync() {
    let _ = std::fs::remove_dir_all("/tmp/bench_ext4sync");
}

fn setup_img() -> BenchStore {
    let path = StdPath::new("/tmp/bench_raw.img");
    let _ = std::fs::remove_file(path);

    println!("  Setup: formatting {:.0} GB image at {} (direct-io, {} MB index slots)...",
        IMG_SIZE as f64 / 1_073_741_824.0, path.display(), INDEX_SLOT / 1_048_576);

    let store = RawObjectStore::format_with_options(
        path,
        FormatOptions {
            device_size: IMG_SIZE,
            direct_io: true,
            index_slot_size: INDEX_SLOT,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        },
    )
    .expect("format img");
    let arc = Arc::new(store);

    BenchStore {
        store: arc.clone(),
        raw: Some(arc),
        do_sync: false,
        name: "img+directio".to_string(),
    }
}

fn cleanup_img() {
    let _ = std::fs::remove_file("/tmp/bench_raw.img");
}

fn setup_img_buf() -> BenchStore {
    let path = StdPath::new("/tmp/bench_raw_buf.img");
    let _ = std::fs::remove_file(path);

    println!("  Setup: formatting {:.0} GB image at {} (buffered, {} MB index slots)...",
        IMG_SIZE as f64 / 1_073_741_824.0, path.display(), INDEX_SLOT / 1_048_576);

    let store = RawObjectStore::format_with_options(
        path,
        FormatOptions {
            device_size: IMG_SIZE,
            direct_io: false,
            index_slot_size: INDEX_SLOT,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        },
    )
    .expect("format img buffered");
    let arc = Arc::new(store);

    BenchStore {
        store: arc.clone(),
        raw: Some(arc),
        do_sync: false,
        name: "img+buffered".to_string(),
    }
}

fn cleanup_img_buf() {
    let _ = std::fs::remove_file("/tmp/bench_raw_buf.img");
}

fn setup_raw() -> BenchStore {
    let path = StdPath::new("/dev/sdb");

    // Probe actual device size
    let probe = DeviceIo::open(path, false).expect("open /dev/sdb (check permissions)");
    let dev_size = probe.size().expect("get device size");
    drop(probe);

    println!(
        "  Setup: formatting /dev/sdb ({:.1} GB, direct-io, {} MB index slots)...",
        dev_size as f64 / 1_073_741_824.0,
        INDEX_SLOT / 1_048_576
    );

    let store = RawObjectStore::format_with_options(
        path,
        FormatOptions {
            device_size: dev_size,
            direct_io: true,
            index_slot_size: INDEX_SLOT,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        },
    )
    .expect("format /dev/sdb");
    let arc = Arc::new(store);

    BenchStore {
        store: arc.clone(),
        raw: Some(arc),
        do_sync: false,
        name: "raw+directio".to_string(),
    }
}

fn setup_raw_buf() -> BenchStore {
    let path = StdPath::new("/dev/sdb");

    let probe = DeviceIo::open(path, false).expect("open /dev/sdb (check permissions)");
    let dev_size = probe.size().expect("get device size");
    drop(probe);

    println!(
        "  Setup: formatting /dev/sdb ({:.1} GB, buffered, {} MB index slots)...",
        dev_size as f64 / 1_073_741_824.0,
        INDEX_SLOT / 1_048_576
    );

    let store = RawObjectStore::format_with_options(
        path,
        FormatOptions {
            device_size: dev_size,
            direct_io: false,
            index_slot_size: INDEX_SLOT,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        },
    )
    .expect("format /dev/sdb buffered");
    let arc = Arc::new(store);

    BenchStore {
        store: arc.clone(),
        raw: Some(arc),
        do_sync: false,
        name: "raw+buffered".to_string(),
    }
}

// -- Main -----------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    let wl = Workload::generate();
    wl.print_summary();

    let rt = tokio::runtime::Runtime::new().unwrap();

    match mode {
        "ext4" => {
            let bs = setup_ext4();
            let results = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &results);
            drop(bs);
            cleanup_ext4();
        }
        "img" => {
            let bs = setup_img();
            let results = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &results);
            drop(bs);
            cleanup_img();
        }
        "raw" => {
            let bs = setup_raw();
            let results = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &results);
        }
        "ext4sync" => {
            let bs = setup_ext4_sync();
            let results = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &results);
            drop(bs);
            cleanup_ext4_sync();
        }
        "img_buf" => {
            let bs = setup_img_buf();
            let results = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &results);
            drop(bs);
            cleanup_img_buf();
        }
        "raw_buf" => {
            let bs = setup_raw_buf();
            let results = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &results);
        }
        "all" => {
            let mut all_results: Vec<(String, Vec<BenchResult>)> = Vec::new();

            // ext4
            let bs = setup_ext4();
            let r = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &r);
            all_results.push((bs.name.clone(), r));
            drop(bs);
            cleanup_ext4();

            // ext4 + sync (fair comparison -- forces writes to disk)
            let bs = setup_ext4_sync();
            let r = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &r);
            all_results.push((bs.name.clone(), r));
            drop(bs);
            cleanup_ext4_sync();

            // img + direct IO
            let bs = setup_img();
            let r = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &r);
            all_results.push((bs.name.clone(), r));
            drop(bs);
            cleanup_img();

            // img + buffered IO (no O_DIRECT)
            let bs = setup_img_buf();
            let r = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &r);
            all_results.push((bs.name.clone(), r));
            drop(bs);
            cleanup_img_buf();

            // raw block device + direct IO
            let bs = setup_raw();
            let r = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &r);
            all_results.push((bs.name.clone(), r));

            // raw block device + buffered IO
            let bs = setup_raw_buf();
            let r = rt.block_on(run_benchmark(&bs, &wl));
            print_table(&bs.name, &r);
            all_results.push((bs.name.clone(), r));

            // Side-by-side comparison
            print_comparison(&all_results);
        }
        _ => {
            eprintln!("Usage: bench_storage [ext4|img|raw|ext4sync|img_buf|raw_buf|all]");
            std::process::exit(1);
        }
    }
}

fn print_comparison(all: &[(String, Vec<BenchResult>)]) {
    println!();
    println!("============================================================");
    println!("  COMPARISON (MB/s)");
    println!("============================================================");
    println!();

    // Collect all phase names preserving order from first backend
    let phase_names: Vec<String> = all[0].1.iter().map(|r| r.phase.clone()).collect();

    // Header
    print!("{:<26}", "Phase");
    for (name, _) in all {
        print!(" | {:>14}", name);
    }
    println!();
    print!("{:-<26}", "");
    for _ in all {
        print!("-+-{:-<14}", "");
    }
    println!();

    // Each phase
    for phase in &phase_names {
        print!("{:<26}", phase);
        for (_, results) in all {
            if let Some(r) = results.iter().find(|r| r.phase == *phase) {
                print!(" | {:>10.1} MB/s", r.mbs());
            } else {
                print!(" | {:>14}", "N/A");
            }
        }
        println!();
    }
    println!();
}

// -- Performance notes --------------------------------------------------------
//
// Fairness: ext4 writes go to the Linux page cache and return immediately.
// The data is NOT on disk until a later writeback (typically 5-30 s).
// To compare fairly, the "ext4+sync" variant calls sync() three times at the
// same flush intervals where RawObjectStore calls flush_index().  This forces
// dirty pages to disk, making the write numbers comparable.  We call sync()
// three times (sync;sync;sync) because the first call may only start I/O and
// the subsequent calls ensure all data and metadata are fully persisted.
//
// O_DIRECT: The "img+directio" and "raw+directio" variants open the backing
// file/device with O_DIRECT, bypassing the kernel page cache entirely.  Each
// write goes straight to the storage device.  This is the most honest write
// benchmark because the data is durable as soon as write() returns.
//
// Buffered IO: The "img+buffered" and "raw+buffered" variants use normal
// buffered I/O through the page cache but still call flush_index() which
// writes the index header synchronously.  Reads benefit from hot caches
// (we drop caches before read phases, but subsequent reads in the same phase
// may still benefit from readahead).
//
// List performance: RawObjectStore keeps a HashMap of file paths in memory,
// so list() is essentially free (O(n) memory scan).  ext4 list uses kernel
// readdir which must traverse directory entries on disk.
//
// Optimize: RawObjectStore consolidates many small extents into contiguous
// 4 MB chunks, dramatically improving sequential and random read throughput.
// ext4 optimize is a file-level copy+rename so it benefits from page cache.
//
// See PERFORMANCE.md in the project root for full results and analysis.
