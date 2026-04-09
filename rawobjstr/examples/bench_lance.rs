//! bench_lance: Lance dataset write + scan across 6 storage backends.
//!
//! Mirrors the Python bench_write_scan.py but uses LanceDB's native Rust API
//! with RawObjectStore directly (no Python FFI overhead).
//!
//! Dataset: 20M rows (20 x 1M-row chunks), ~5.7 GB Arrow data.
//! Schema: id(i64), name(utf8), city(utf8), amount(f64), category(i32),
//!         payload(binary, 256 random bytes/row -- incompressible)
//!
//! Backends:
//!   ext4         -- local filesystem via LanceDB built-in
//!   ext4+sync    -- local filesystem + sync after each chunk
//!   img+directio -- RawObjectStore 16 GB image, O_DIRECT
//!   img+buffered -- RawObjectStore 16 GB image, buffered I/O
//!   raw+directio -- RawObjectStore /dev/sdb, O_DIRECT
//!   raw+buffered -- RawObjectStore /dev/sdb, buffered I/O
//!
//! Usage:
//!   bench_lance                           # all 6 backends
//!   bench_lance --backends ext4,raw+directio
//!   bench_lance --chunks 5               # quick test

use std::fmt;
use std::fs;
use std::path::{Path as StdPath, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams,
    providers::{ObjectStoreProvider, ObjectStoreRegistry},
};
use lancedb::query::{ExecutableQuery, QueryBase};
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use url::Url;

use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::Compression;

// -- Config ---------------------------------------------------------------

const CHUNK_ROWS: usize = 1_000_000;
const N_CHUNKS: usize = 20;
const IMG_SIZE: u64 = 16 * 1024 * 1024 * 1024; // 16 GB
const PAYLOAD_SIZE: usize = 256;

const CITIES: &[&str] = &[
    "London", "Paris", "Berlin", "Madrid", "Rome", "Vienna", "Prague",
    "Warsaw", "Dublin", "Lisbon", "Oslo", "Helsinki", "Athens", "Zurich",
    "Brussels", "Amsterdam", "Stockholm", "Copenhagen", "Budapest", "Bucharest",
];

const LETTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// -- Provider: plugs RawObjectStore into LanceDB -------------------------

struct RawStoreProvider {
    store: Arc<RawObjectStore>,
}

impl fmt::Debug for RawStoreProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawStoreProvider").finish()
    }
}

#[async_trait]
impl ObjectStoreProvider for RawStoreProvider {
    async fn new_store(
        &self,
        base_path: Url,
        _params: &ObjectStoreParams,
    ) -> lance_core::error::Result<LanceObjectStore> {
        let inner: Arc<dyn ObjectStore> = self.store.clone();
        Ok(LanceObjectStore::new(
            inner,
            base_path,
            None,   // block_size
            None,   // wrapper
            false,  // use_constant_size_upload_parts
            false,  // list_is_lexically_ordered (RawObjectStore does not sort)
            4,      // io_parallelism
            3,      // download_retry_count
            None,   // storage_options
        ))
    }
}

// -- Data generation -----------------------------------------------------

fn make_chunk(chunk_idx: usize, n_rows: usize) -> RecordBatch {
    let offset = (chunk_idx * n_rows) as i64;
    let mut rng = StdRng::seed_from_u64(42 + chunk_idx as u64);

    // ids
    let ids: Vec<i64> = (offset..offset + n_rows as i64).collect();

    // amounts
    let amounts: Vec<f64> = (0..n_rows)
        .map(|_| rng.gen::<f64>() * 2000.0 + 4000.0)
        .collect();

    // categories: 0..99
    let categories: Vec<i32> = (0..n_rows).map(|_| rng.gen_range(0..100i32)).collect();

    // names: "Xuser_NNNNNN"
    let names: Vec<String> = (0..n_rows)
        .map(|_| {
            let letter = LETTERS[rng.gen_range(0..26)] as char;
            let suffix: u32 = rng.gen_range(100_000..999_999);
            format!("{}user_{}", letter, suffix)
        })
        .collect();

    // cities
    let cities: Vec<&str> = (0..n_rows)
        .map(|_| CITIES[rng.gen_range(0..CITIES.len())])
        .collect();

    // payload: 256 random bytes per row (incompressible)
    let mut payload_buf = vec![0u8; n_rows * PAYLOAD_SIZE];
    rng.fill_bytes(&mut payload_buf);
    let payloads: Vec<&[u8]> = (0..n_rows)
        .map(|i| &payload_buf[i * PAYLOAD_SIZE..(i + 1) * PAYLOAD_SIZE])
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Int32, false),
        Field::new("payload", DataType::Binary, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(cities)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(Int32Array::from(categories)),
            Arc::new(BinaryArray::from(payloads)),
        ],
    )
    .expect("failed to build RecordBatch")
}

fn batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum()
}

// -- Formatting helpers --------------------------------------------------

fn fmt_size(bytes: u64) -> String {
    let gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    if gb >= 1.0 {
        format!("{:.2} GB", gb)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn fmt_rate(bytes: u64, secs: f64) -> String {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    format!("{:.1} MB/s", mb / secs)
}

fn fmt_time(secs: f64) -> String {
    if secs < 1.0 {
        format!("{:.1} ms", secs * 1000.0)
    } else {
        format!("{:.2} s", secs)
    }
}

fn drop_caches() {
    let _ = fs::write("/proc/sys/vm/drop_caches", "3\n");
}

fn do_sync() {
    for _ in 0..3 {
        let _ = Command::new("sync").status();
    }
}

/// Write a 4 GB junk file to evict all page-cache contents, then drop caches
/// and remove it. This ensures no residual cached data from writes or
/// previous scans affects the next read benchmark.
fn evict_page_cache() {
    do_sync();
    drop_caches();

    // Write 4 GB via dd (faster than Rust write loop)
    let cache_buster = "/tmp/cache_buster.bin";
    let _ = Command::new("dd")
        .args(&[
            "if=/dev/zero",
            &format!("of={}", cache_buster),
            "bs=1M",
            "count=4096",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    do_sync();
    drop_caches();
    let _ = fs::remove_file(cache_buster);
}

// -- Backend trait -------------------------------------------------------

trait Backend: Send + Sync {
    fn name(&self) -> &str;

    /// Return (lance_uri, Option<session>)
    fn connect(&mut self, rt: &tokio::runtime::Runtime) -> (String, Option<Arc<lancedb::Session>>);

    /// Whether to call sync after each chunk write
    fn sync_writes(&self) -> bool {
        false
    }

    /// Cleanup after benchmark
    fn cleanup(&mut self);
}

// -- Ext4 backend --------------------------------------------------------

struct Ext4Backend {
    label: String,
    dir: PathBuf,
    sync: bool,
}

impl Ext4Backend {
    fn new(base: &str, sync: bool) -> Self {
        let label = if sync {
            "ext4+sync".to_string()
        } else {
            "ext4".to_string()
        };
        let name = if sync { "lance_ext4sync_bench" } else { "lance_ext4_bench" };
        Self {
            label,
            dir: PathBuf::from(base).join(name),
            sync,
        }
    }
}

impl Backend for Ext4Backend {
    fn name(&self) -> &str {
        &self.label
    }

    fn connect(&mut self, rt: &tokio::runtime::Runtime) -> (String, Option<Arc<lancedb::Session>>) {
        if self.dir.exists() {
            fs::remove_dir_all(&self.dir).ok();
        }
        let uri = self.dir.to_string_lossy().to_string();
        (uri, None) // use built-in local fs
    }

    fn sync_writes(&self) -> bool {
        self.sync
    }

    fn cleanup(&mut self) {
        if self.dir.exists() {
            fs::remove_dir_all(&self.dir).ok();
        }
    }
}

// -- RawObjectStore backend ----------------------------------------------

struct RawBackend {
    label: String,
    path: String,
    size: Option<u64>,
    direct_io: bool,
    store: Option<Arc<RawObjectStore>>,
}

impl RawBackend {
    fn new(path: &str, size: Option<u64>, direct_io: bool, label: &str) -> Self {
        Self {
            label: label.to_string(),
            path: path.to_string(),
            size,
            direct_io,
            store: None,
        }
    }
}

impl Backend for RawBackend {
    fn name(&self) -> &str {
        &self.label
    }

    fn connect(&mut self, _rt: &tokio::runtime::Runtime) -> (String, Option<Arc<lancedb::Session>>) {
        let store = if let Some(sz) = self.size {
            Arc::new(
                RawObjectStore::format_with_options(
                    StdPath::new(&self.path),
                    FormatOptions {
                        device_size: sz,
                        direct_io: self.direct_io,
                        index_slot_size: 128 * 1024 * 1024,
                        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                        compression: Compression::None,
                    },
                )
                .expect("format failed"),
            )
        } else {
            // Raw block device -- detect size via blockdev, then format
            let output = Command::new("blockdev")
                .arg("--getsize64")
                .arg(&self.path)
                .output()
                .expect("blockdev failed");
            let dev_size: u64 = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .expect("bad blockdev output");
            Arc::new(
                RawObjectStore::format_with_options(
                    StdPath::new(&self.path),
                    FormatOptions {
                        device_size: dev_size,
                        direct_io: self.direct_io,
                        index_slot_size: 128 * 1024 * 1024,
                        max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
                        compression: Compression::None,
                    },
                )
                .expect("format failed"),
            )
        };
        store.flush_index().unwrap();
        self.store = Some(Arc::clone(&store));

        let provider = Arc::new(RawStoreProvider { store });
        let registry = ObjectStoreRegistry::default();
        registry.insert("raw", provider);
        let session = Arc::new(lancedb::Session::new(128, 128, Arc::new(registry)));

        ("raw:///".to_string(), Some(session))
    }

    fn cleanup(&mut self) {
        if let Some(ref store) = self.store {
            store.flush_index().unwrap();
        }
        self.store = None;
        if let Some(_sz) = self.size {
            // Image file -- remove it
            fs::remove_file(&self.path).ok();
        }
    }
}

// -- Benchmark runner ----------------------------------------------------

struct BenchResult {
    write_secs: f64,
    write_bytes: u64,
    count_rows_secs: f64,
    count_rows_n: u64,
    stream_scan_secs: f64,
    stream_scan_bytes: u64,
    stream_scan_rows: u64,
    filter_like_secs: f64,
    filter_like_rows: u64,
    col_project_secs: f64,
    col_project_bytes: u64,
    col_project_rows: u64,
    point_filter_secs: f64,
    point_filter_rows: u64,
}

fn run_benchmark(
    backend: &mut dyn Backend,
    rt: &tokio::runtime::Runtime,
    n_chunks: usize,
    chunk_rows: usize,
) -> Result<BenchResult, String> {
    let total_rows = n_chunks * chunk_rows;

    let (uri, session) = backend.connect(rt);

    let db = rt.block_on(async {
        let mut builder = lancedb::connect(&uri);
        if let Some(s) = session.as_ref() {
            builder = builder.session(Arc::clone(s));
        }
        builder.execute().await
    }).map_err(|e| format!("connect: {}", e))?;

    // -- Chunked write ---------------------------------------------------
    println!("  writing {} x {:>10} rows...", n_chunks, format_commas(chunk_rows));
    let mut total_bytes: u64 = 0;
    let write_start = Instant::now();
    let table_name = "bench";

    for i in 0..n_chunks {
        let chunk = make_chunk(i, chunk_rows);
        let chunk_bytes = batch_bytes(&chunk) as u64;
        total_bytes += chunk_bytes;

        if i == 0 {
            rt.block_on(async {
                db.create_table(table_name, chunk)
                    .execute()
                    .await
            }).map_err(|e| format!("create_table: {}", e))?;
        } else {
            let tbl = rt.block_on(async {
                db.open_table(table_name).execute().await
            }).map_err(|e| format!("open_table: {}", e))?;
            rt.block_on(async {
                tbl.add(chunk).execute().await
            }).map_err(|e| format!("add: {}", e))?;
        }

        if backend.sync_writes() {
            do_sync();
        }

        let elapsed = write_start.elapsed().as_secs_f64();
        let rate = total_bytes as f64 / elapsed / (1024.0 * 1024.0);
        println!(
            "    chunk {:>2}/{}: {:>8} written, {:>7.1} MB/s cumulative",
            i + 1,
            n_chunks,
            fmt_size(total_bytes),
            rate,
        );
    }

    let write_elapsed = write_start.elapsed().as_secs_f64();
    println!(
        "  WRITE TOTAL:    {:>10}  ({}, {})",
        fmt_time(write_elapsed),
        fmt_rate(total_bytes, write_elapsed),
        fmt_size(total_bytes),
    );

    // Evict page cache so scans start cold
    print!("  evicting page cache...");
    evict_page_cache();
    println!(" done");

    // -- count_rows (metadata) -------------------------------------------
    evict_page_cache();
    print!("  count_rows...");
    let t0 = Instant::now();
    let tbl = rt.block_on(async {
        db.open_table(table_name).execute().await
    }).map_err(|e| format!("open: {}", e))?;
    let row_count = rt.block_on(async {
        tbl.count_rows(None).await
    }).map_err(|e| format!("count: {}", e))?;
    let count_elapsed = t0.elapsed().as_secs_f64();
    println!(" {} rows in {}", format_commas(row_count as usize), fmt_time(count_elapsed));

    // -- Full stream scan (all columns) ----------------------------------
    evict_page_cache();
    print!("  full stream scan...");
    let t0 = Instant::now();
    let tbl = rt.block_on(async {
        db.open_table(table_name).execute().await
    }).map_err(|e| format!("open: {}", e))?;
    let (scan_rows, scan_bytes) = rt.block_on(async {
        let stream = tbl.query().execute().await.map_err(|e| format!("query: {}", e))?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| format!("collect: {}", e))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let bytes: usize = batches.iter().map(|b| batch_bytes(b)).sum();
        Ok::<(usize, usize), String>((rows, bytes))
    })?;
    let scan_elapsed = t0.elapsed().as_secs_f64();
    println!(
        " {} rows ({}) in {} ({})",
        format_commas(scan_rows),
        fmt_size(scan_bytes as u64),
        fmt_time(scan_elapsed),
        fmt_rate(scan_bytes as u64, scan_elapsed),
    );

    // -- Filtered scan: name LIKE 'A%' -----------------------------------
    evict_page_cache();
    print!("  filter: name LIKE 'A%'...");
    let t0 = Instant::now();
    let tbl = rt.block_on(async {
        db.open_table(table_name).execute().await
    }).map_err(|e| format!("open: {}", e))?;
    let filt_rows = rt.block_on(async {
        let stream = tbl
            .query()
            .only_if("starts_with(name, 'A')")
            .execute()
            .await
            .map_err(|e| format!("query: {}", e))?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| format!("collect: {}", e))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok::<usize, String>(rows)
    })?;
    let filt_elapsed = t0.elapsed().as_secs_f64();
    println!(
        " {} matching rows in {}",
        format_commas(filt_rows),
        fmt_time(filt_elapsed),
    );

    // -- Column projection: id + amount ----------------------------------
    evict_page_cache();
    print!("  column projection (id, amount)...");
    let t0 = Instant::now();
    let tbl = rt.block_on(async {
        db.open_table(table_name).execute().await
    }).map_err(|e| format!("open: {}", e))?;
    let (proj_rows, proj_bytes) = rt.block_on(async {
        let stream = tbl
            .query()
            .select(lancedb::query::Select::Columns(vec![
                "id".to_string(),
                "amount".to_string(),
            ]))
            .execute()
            .await
            .map_err(|e| format!("query: {}", e))?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| format!("collect: {}", e))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let bytes: usize = batches.iter().map(|b| batch_bytes(b)).sum();
        Ok::<(usize, usize), String>((rows, bytes))
    })?;
    let proj_elapsed = t0.elapsed().as_secs_f64();
    println!(
        " {} rows ({}) in {} ({})",
        format_commas(proj_rows),
        fmt_size(proj_bytes as u64),
        fmt_time(proj_elapsed),
        fmt_rate(proj_bytes as u64, proj_elapsed),
    );

    // -- Point filter: category = 42 -------------------------------------
    evict_page_cache();
    print!("  filter: category = 42...");
    let t0 = Instant::now();
    let tbl = rt.block_on(async {
        db.open_table(table_name).execute().await
    }).map_err(|e| format!("open: {}", e))?;
    let point_rows = rt.block_on(async {
        let stream = tbl
            .query()
            .only_if("category = 42")
            .execute()
            .await
            .map_err(|e| format!("query: {}", e))?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| format!("collect: {}", e))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok::<usize, String>(rows)
    })?;
    let point_elapsed = t0.elapsed().as_secs_f64();
    println!(
        " {} matching rows in {}",
        format_commas(point_rows),
        fmt_time(point_elapsed),
    );

    // Explicitly drop the database connection and session so all Arc<RawObjectStore>
    // references through the Session/Registry/Provider chain are released before
    // cleanup() tries to format the same device for the next backend.
    drop(db);
    drop(session);

    Ok(BenchResult {
        write_secs: write_elapsed,
        write_bytes: total_bytes,
        count_rows_secs: count_elapsed,
        count_rows_n: row_count as u64,
        stream_scan_secs: scan_elapsed,
        stream_scan_bytes: scan_bytes as u64,
        stream_scan_rows: scan_rows as u64,
        filter_like_secs: filt_elapsed,
        filter_like_rows: filt_rows as u64,
        col_project_secs: proj_elapsed,
        col_project_bytes: proj_bytes as u64,
        col_project_rows: proj_rows as u64,
        point_filter_secs: point_elapsed,
        point_filter_rows: point_rows as u64,
    })
}

fn format_commas(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

// -- Main ----------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut n_chunks = N_CHUNKS;
    let mut chunk_rows = CHUNK_ROWS;
    let mut img_size = IMG_SIZE;
    let mut device = "/dev/sdb".to_string();
    let mut ext4_dir = "/tmp".to_string();
    let mut selected_backends: Option<Vec<String>> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--chunks" => {
                i += 1;
                n_chunks = args[i].parse().expect("bad --chunks");
            }
            "--chunk-rows" => {
                i += 1;
                chunk_rows = args[i].parse().expect("bad --chunk-rows");
            }
            "--img-size" => {
                i += 1;
                img_size = args[i].parse().expect("bad --img-size");
            }
            "--device" => {
                i += 1;
                device = args[i].clone();
            }
            "--ext4-dir" => {
                i += 1;
                ext4_dir = args[i].clone();
            }
            "--backends" => {
                i += 1;
                if args[i] != "all" {
                    selected_backends =
                        Some(args[i].split(',').map(|s| s.trim().to_string()).collect());
                }
            }
            other => {
                eprintln!("unknown arg: {}", other);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let total_rows = n_chunks * chunk_rows;
    let est_bytes = total_rows as u64 * (8 + 12 + 8 + 8 + 4 + PAYLOAD_SIZE as u64);
    println!("=== Lance Large Scan Benchmark (Rust) ===");
    println!(
        "chunks={}  chunk_rows={}  total_rows={}",
        n_chunks,
        format_commas(chunk_rows),
        format_commas(total_rows),
    );
    println!(
        "estimated dataset: ~{}  payload={} bytes/row",
        fmt_size(est_bytes),
        PAYLOAD_SIZE,
    );
    println!();

    let backend_order = [
        "ext4",
        "ext4+sync",
        "img+directio",
        "img+buffered",
        "raw+directio",
        "raw+buffered",
    ];

    let selected: Vec<&str> = if let Some(ref sel) = selected_backends {
        sel.iter().map(|s| s.as_str()).collect()
    } else {
        backend_order.to_vec()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let mut all_results: Vec<(&str, Option<BenchResult>)> = Vec::new();

    for &name in &selected {
        println!("{}", "=".repeat(60));
        println!("  BACKEND: {}", name);
        println!("{}", "=".repeat(60));

        let mut backend: Box<dyn Backend> = match name {
            "ext4" => Box::new(Ext4Backend::new(&ext4_dir, false)),
            "ext4+sync" => Box::new(Ext4Backend::new(&ext4_dir, true)),
            "img+directio" => Box::new(RawBackend::new(
                "/tmp/lance_scan_dio.img",
                Some(img_size),
                true,
                "img+directio",
            )),
            "img+buffered" => Box::new(RawBackend::new(
                "/tmp/lance_scan_buf.img",
                Some(img_size),
                false,
                "img+buffered",
            )),
            "raw+directio" => Box::new(RawBackend::new(
                &device,
                None,
                true,
                "raw+directio",
            )),
            "raw+buffered" => Box::new(RawBackend::new(
                &device,
                None,
                false,
                "raw+buffered",
            )),
            _ => {
                eprintln!("  unknown backend: {}", name);
                all_results.push((name, None));
                continue;
            }
        };

        match run_benchmark(backend.as_mut(), &rt, n_chunks, chunk_rows) {
            Ok(result) => {
                all_results.push((name, Some(result)));
            }
            Err(e) => {
                eprintln!("  ERROR: {}", e);
                all_results.push((name, None));
            }
        }
        backend.cleanup();
        // Let async runtime drain any lingering tasks that may hold
        // Arc<RawObjectStore> references (e.g. lancedb background tasks).
        // Without this, the flock on /dev/sdb may still be held when the
        // next backend tries to format the same device.
        std::thread::sleep(std::time::Duration::from_secs(1));
        println!();
    }

    // -- Summary tables --------------------------------------------------
    println!("=== Summary: Write ===");
    println!(
        "{:<16}{:>10}{:>10}{:>10}",
        "backend", "time", "MB/s", "data"
    );
    println!("{}", "-".repeat(46));
    for (name, result) in &all_results {
        if let Some(r) = result {
            let rate = r.write_bytes as f64 / r.write_secs / (1024.0 * 1024.0);
            println!(
                "{:<16}{:>10}{:>9.1}{:>10}",
                name,
                fmt_time(r.write_secs),
                rate,
                fmt_size(r.write_bytes),
            );
        } else {
            println!("{:<16}  ERROR", name);
        }
    }
    println!();

    println!("=== Summary: Scan Times ===");
    println!(
        "{:<16}{:>12}{:>12}{:>12}{:>12}{:>12}",
        "backend", "count", "stream", "LIKE 'A%'", "id+amount", "cat=42"
    );
    println!("{}", "-".repeat(76));
    for (name, result) in &all_results {
        if let Some(r) = result {
            println!(
                "{:<16}{:>11}{:>11}{:>11}{:>11}{:>11}",
                name,
                fmt_time(r.count_rows_secs),
                fmt_time(r.stream_scan_secs),
                fmt_time(r.filter_like_secs),
                fmt_time(r.col_project_secs),
                fmt_time(r.point_filter_secs),
            );
        } else {
            println!("{:<16}  ERROR", name);
        }
    }
    println!();

    println!("=== Summary: Stream Scan Throughput ===");
    println!("{:<16}{:>10}{:>10}", "backend", "MB/s", "data");
    println!("{}", "-".repeat(36));
    for (name, result) in &all_results {
        if let Some(r) = result {
            let rate = r.stream_scan_bytes as f64 / r.stream_scan_secs / (1024.0 * 1024.0);
            println!(
                "{:<16}{:>9.1}{:>10}",
                name,
                rate,
                fmt_size(r.stream_scan_bytes),
            );
        }
    }
    println!();
}
