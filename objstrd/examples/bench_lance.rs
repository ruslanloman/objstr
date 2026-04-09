//! bench_lance: Lance dataset write + scan via objstrd S3 endpoint.
//!
//! Measures the overhead of the S3 HTTP protocol layer (s3s, SigV4, hyper,
//! TCP loopback) by starting objstrd as a child process for each backend
//! and connecting LanceDB through its S3 endpoint.
//!
//! Dataset: 20M rows (20 x 1M-row chunks), ~5.7 GB Arrow data.
//! Schema: id(i64), name(utf8), city(utf8), amount(f64), category(i32),
//!         payload(binary, 256 random bytes/row -- incompressible)
//!
//! Backends:
//!   fs           -- objstrd --backend fs (LocalFileSystem via S3)
//!   mem          -- objstrd --backend mem (in-memory, pure HTTP overhead)
//!   img+directio -- objstrd image file, O_DIRECT via S3
//!   img+buffered -- objstrd image file, buffered via S3
//!   raw+directio -- objstrd /dev/sdb, O_DIRECT via S3
//!   raw+buffered -- objstrd /dev/sdb, buffered via S3
//!
//! Usage:
//!   bench_lance --objstrd-bin /path/to/objstrd    # all 6 backends
//!   bench_lance --backends fs,mem --chunks 5      # quick test
//!   bench_lance --backends raw+directio --device /dev/sdb

use std::collections::HashMap;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

// -- Config ---------------------------------------------------------------

const CHUNK_ROWS: usize = 1_000_000;
const N_CHUNKS: usize = 20;
const IMG_SIZE_MB: u64 = 16384; // 16 GB
const PAYLOAD_SIZE: usize = 256;
const DEFAULT_PORT: u16 = 8900;
const BUCKET: &str = "testbucket";

const CITIES: &[&str] = &[
    "London", "Paris", "Berlin", "Madrid", "Rome", "Vienna", "Prague",
    "Warsaw", "Dublin", "Lisbon", "Oslo", "Helsinki", "Athens", "Zurich",
    "Brussels", "Amsterdam", "Stockholm", "Copenhagen", "Budapest", "Bucharest",
];

const LETTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// -- Data generation (identical to rawobjstr/shardedobjstr bench) ---------

fn make_chunk(chunk_idx: usize, n_rows: usize) -> RecordBatch {
    let offset = (chunk_idx * n_rows) as i64;
    let mut rng = StdRng::seed_from_u64(42 + chunk_idx as u64);

    let ids: Vec<i64> = (offset..offset + n_rows as i64).collect();

    let amounts: Vec<f64> = (0..n_rows)
        .map(|_| rng.gen::<f64>() * 2000.0 + 4000.0)
        .collect();

    let categories: Vec<i32> = (0..n_rows).map(|_| rng.gen_range(0..100i32)).collect();

    let names: Vec<String> = (0..n_rows)
        .map(|_| {
            let letter = LETTERS[rng.gen_range(0..26)] as char;
            let suffix: u32 = rng.gen_range(100_000..999_999);
            format!("{}user_{}", letter, suffix)
        })
        .collect();

    let cities: Vec<&str> = (0..n_rows)
        .map(|_| CITIES[rng.gen_range(0..CITIES.len())])
        .collect();

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

// -- Formatting helpers ---------------------------------------------------

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

fn drop_caches() {
    let _ = fs::write("/proc/sys/vm/drop_caches", "3\n");
}

fn do_sync() {
    for _ in 0..3 {
        let _ = Command::new("sync").status();
    }
}

fn evict_page_cache() {
    do_sync();
    drop_caches();

    let cache_buster = "/tmp/cache_buster.bin";
    let _ = Command::new("dd")
        .args(&[
            "if=/dev/zero",
            &format!("of={}", cache_buster),
            "bs=1M",
            "count=4096",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    do_sync();
    drop_caches();
    let _ = fs::remove_file(cache_buster);
}

// -- objstrd process management -------------------------------------------

fn start_objstrd(objstrd_bin: &str, args: &[&str]) -> Child {
    let stderr_file = std::fs::File::create("/tmp/objstrd_bench_stderr.log")
        .expect("failed to create stderr log");
    Command::new(objstrd_bin)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .unwrap_or_else(|e| panic!("failed to start objstrd: {}", e))
}

fn wait_for_ready(port: u16, timeout_secs: u64) -> bool {
    let url = format!("http://localhost:{}/_admin/info", port);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if let Ok(output) = Command::new("curl")
            .args(&["-s", "-o", "/dev/null", "-w", "%{http_code}", &url])
            .output()
        {
            let code = String::from_utf8_lossy(&output.stdout);
            if code.trim() == "200" {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

fn stop_objstrd(child: &mut Child) {
    // Send SIGTERM via kill
    let pid = child.id();
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status();
    match child.wait() {
        Ok(_) => {}
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn storage_options(port: u16) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert("aws_endpoint".to_string(), format!("http://localhost:{}", port));
    opts.insert("region".to_string(), "us-east-1".to_string());
    opts.insert("allow_http".to_string(), "true".to_string());
    opts.insert("aws_s3_allow_unsafe_rename".to_string(), "true".to_string());
    // Skip SigV4 auth -- objstrd runs without --access-key/--secret-key
    opts.insert("skip_signature".to_string(), "true".to_string());
    opts.insert("unsigned_payload".to_string(), "true".to_string());
    // Connection pool: keep 32 idle connections for concurrent range reads
    opts.insert("pool_max_idle_per_host".to_string(), "32".to_string());
    opts
}

// -- Backend definitions --------------------------------------------------

struct S3Backend {
    name: String,
    objstrd_args: Vec<String>,
    objstrd_bin: String,
    port: u16,
    cleanup_paths: Vec<String>,
    pre_create_dirs: Vec<String>,
    wipe_device: Option<String>,
    child: Option<Child>,
}

impl S3Backend {
    fn new(
        name: &str,
        args: &[&str],
        port: u16,
        objstrd_bin: &str,
        cleanup_paths: Vec<String>,
        pre_create_dirs: Vec<String>,
        wipe_device: Option<String>,
    ) -> Self {
        Self {
            name: name.to_string(),
            objstrd_args: args.iter().map(|s| s.to_string()).collect(),
            objstrd_bin: objstrd_bin.to_string(),
            port,
            cleanup_paths,
            pre_create_dirs,
            wipe_device,
            child: None,
        }
    }

    fn setup(&mut self) -> Result<(), String> {
        for path in &self.pre_create_dirs {
            let _ = fs::create_dir_all(path);
        }
        if let Some(ref dev) = self.wipe_device {
            println!("  formatting device: {}", dev);
            // Use rawobjstr CLI to format the device fresh.
            // Try common build locations.
            let rawobjstr_candidates = [
                "~/build-rawobjstr/release/rawobjstr",
                "rawobjstr",
            ];
            let mut formatted = false;
            for bin in &rawobjstr_candidates {
                let status = Command::new(bin)
                    .args(&["format", "--file", dev, "--size",
                            &format!("{}", 20u64 * 1024 * 1024 * 1024)])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
                if let Ok(s) = status {
                    if s.success() {
                        formatted = true;
                        break;
                    }
                }
            }
            if !formatted {
                return Err(format!("failed to format device {}", dev));
            }
            do_sync();
        }
        let args_ref: Vec<&str> = self.objstrd_args.iter().map(|s| s.as_str()).collect();
        self.child = Some(start_objstrd(&self.objstrd_bin, &args_ref));
        // Give it a moment to start or crash
        std::thread::sleep(Duration::from_secs(1));
        // Check if already exited
        if let Some(ref mut child) = self.child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let stderr = std::fs::read_to_string("/tmp/objstrd_bench_stderr.log")
                        .unwrap_or_default();
                    self.child = None;
                    return Err(format!(
                        "objstrd exited immediately with {} (backend: {})\nstderr: {}",
                        status, self.name, stderr.trim()
                    ));
                }
                _ => {}
            }
        }
        if !wait_for_ready(self.port, 30) {
            self.cleanup();
            return Err(format!(
                "objstrd did not become ready on port {} (backend: {})",
                self.port, self.name
            ));
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        if let Some(ref mut child) = self.child {
            stop_objstrd(child);
        }
        self.child = None;
        for path in &self.cleanup_paths {
            let p = std::path::Path::new(path);
            if p.is_dir() {
                let _ = fs::remove_dir_all(path);
            } else if p.is_file() {
                let _ = fs::remove_file(path);
            }
        }
    }
}

// -- Benchmark result -----------------------------------------------------

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

// -- Benchmark runner -----------------------------------------------------

fn run_benchmark(
    port: u16,
    rt: &tokio::runtime::Runtime,
    n_chunks: usize,
    chunk_rows: usize,
) -> Result<BenchResult, String> {
    let uri = format!("s3://{}/lance-bench", BUCKET);
    let opts = storage_options(port);

    let db = rt.block_on(async {
        let mut builder = lancedb::connect(&uri);
        for (k, v) in &opts {
            builder = builder.storage_option(k, v);
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

    drop(db);

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

// -- Main ----------------------------------------------------------------

fn main() {
    // Lance I/O thread pool: 32 threads for concurrent range reads
    std::env::set_var("LANCE_IO_THREADS", "32");

    let args: Vec<String> = std::env::args().collect();

    let mut n_chunks = N_CHUNKS;
    let mut chunk_rows = CHUNK_ROWS;
    let mut img_size_mb = IMG_SIZE_MB;
    let mut device = "/dev/sdb".to_string();
    let mut port = DEFAULT_PORT;
    let mut objstrd_bin = "objstrd".to_string();
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
            "--img-size-mb" => {
                i += 1;
                img_size_mb = args[i].parse().expect("bad --img-size-mb");
            }
            "--device" => {
                i += 1;
                device = args[i].clone();
            }
            "--port" => {
                i += 1;
                port = args[i].parse().expect("bad --port");
            }
            "--objstrd-bin" => {
                i += 1;
                objstrd_bin = args[i].clone();
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
    let port_s = port.to_string();
    let img_size_s = img_size_mb.to_string();

    println!("=== Lance S3 HTTP Layer Benchmark (Rust) ===");
    println!("objstrd S3 endpoint on port {}", port);
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

    let fs_root = "/tmp/lance_s3_fs";

    let backend_order = [
        "fs",
        "mem",
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

        let mut backend = match name {
            "fs" => S3Backend::new(
                "fs",
                &["--backend", "fs", "--image", fs_root,
                  "--port", &port_s, "--bucket", BUCKET],
                port, &objstrd_bin,
                vec![fs_root.to_string()],
                vec![fs_root.to_string()],
                None,
            ),
            "mem" => S3Backend::new(
                "mem",
                &["--backend", "mem",
                  "--port", &port_s, "--bucket", BUCKET],
                port, &objstrd_bin,
                vec![],
                vec![],
                None,
            ),
            "img+directio" => S3Backend::new(
                "img+directio",
                &["--image", "/tmp/lance_s3_dio.img",
                  "--size-mb", &img_size_s, "--direct-io",
                  "--port", &port_s, "--bucket", BUCKET],
                port, &objstrd_bin,
                vec!["/tmp/lance_s3_dio.img".to_string()],
                vec![],
                None,
            ),
            "img+buffered" => S3Backend::new(
                "img+buffered",
                &["--image", "/tmp/lance_s3_buf.img",
                  "--size-mb", &img_size_s,
                  "--port", &port_s, "--bucket", BUCKET],
                port, &objstrd_bin,
                vec!["/tmp/lance_s3_buf.img".to_string()],
                vec![],
                None,
            ),
            "raw+directio" => S3Backend::new(
                "raw+directio",
                &["--image", &device, "--direct-io",
                  "--port", &port_s, "--bucket", BUCKET],
                port, &objstrd_bin,
                vec![],
                vec![],
                None,
            ),
            "raw+buffered" => S3Backend::new(
                "raw+buffered",
                &["--image", &device,
                  "--port", &port_s, "--bucket", BUCKET],
                port, &objstrd_bin,
                vec![],
                vec![],
                None,
            ),
            _ => {
                eprintln!("  unknown backend: {}", name);
                all_results.push((name, None));
                continue;
            }
        };

        match backend.setup() {
            Ok(()) => {
                match run_benchmark(port, &rt, n_chunks, chunk_rows) {
                    Ok(result) => {
                        all_results.push((name, Some(result)));
                    }
                    Err(e) => {
                        eprintln!("  ERROR: {}", e);
                        all_results.push((name, None));
                    }
                }
            }
            Err(e) => {
                eprintln!("  ERROR: {}", e);
                all_results.push((name, None));
            }
        }
        backend.cleanup();
        // Brief pause to let the port be released
        std::thread::sleep(Duration::from_secs(1));
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
                "{:<16}{:>12}{:>12}{:>12}{:>12}{:>12}",
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

    println!("=== Summary: Scan Throughput (MB/s) ===");
    println!(
        "{:<16}{:>12}{:>12}",
        "backend", "stream", "id+amount"
    );
    println!("{}", "-".repeat(40));
    for (name, result) in &all_results {
        if let Some(r) = result {
            let stream_rate =
                r.stream_scan_bytes as f64 / r.stream_scan_secs / (1024.0 * 1024.0);
            let proj_rate =
                r.col_project_bytes as f64 / r.col_project_secs / (1024.0 * 1024.0);
            println!(
                "{:<16}{:>11.1}{:>12.1}",
                name, stream_rate, proj_rate,
            );
        } else {
            println!("{:<16}  ERROR", name);
        }
    }
}
