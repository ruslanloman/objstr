/// Index write amplification benchmarks for RawObjectStore.
///
/// Measures the cost of flush_index() under various workload patterns to
/// quantify index write amplification. Each workload tracks:
///   - Total wall-clock time
///   - Time spent only in flush_index()
///   - Number of flushes
///   - Final serialized index size
///   - Estimated total index bytes written to disk
///
/// Run with:
///   cargo test --release --test index_write_amplification -- --nocapture --ignored
///
/// For raw device + O_DIRECT (bypasses Linux page cache):
///   RAW_DEVICE=/dev/sdb cargo test --release --test index_write_amplification \
///     -- --nocapture --ignored bench_all_workloads
///
/// All tests are #[ignore] so they don't run in normal CI.
mod common;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rawobjstr::store::RawObjectStore;
use std::path::Path as StdPath;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const DEVICE_512MB: u64 = 512 * 1024 * 1024;
const DEVICE_1GB: u64 = 1024 * 1024 * 1024;

/// When RAW_DEVICE is set, format on the raw block device with O_DIRECT.
/// Otherwise, use a temp file with buffered I/O (original behavior).
/// Returns (store, Option<NamedTempFile>) — keep the NamedTempFile alive.
fn make_bench_store(size: u64) -> (RawObjectStore, Option<NamedTempFile>) {
    if let Ok(dev) = std::env::var("RAW_DEVICE") {
        let store = RawObjectStore::format(StdPath::new(&dev), true).unwrap();
        (store, None)
    } else {
        let (store, tmp) = common::make_store_sized(size);
        (store, Some(tmp))
    }
}

fn bench_mode_label() -> &'static str {
    if std::env::var("RAW_DEVICE").is_ok() {
        "raw-device+O_DIRECT"
    } else {
        "loopback+buffered"
    }
}

// 200-char key prefix to simulate realistic path lengths
fn long_key(prefix: &str, index: usize) -> String {
    // "prefix/aaa...aaa/00001" -- total ~200 chars
    let num = format!("{:05}", index);
    let prefix_part = format!("{}/", prefix);
    let suffix = format!("/{}", num);
    let pad_len = 200usize
        .saturating_sub(prefix_part.len())
        .saturating_sub(suffix.len());
    let pad: String = std::iter::repeat('a').take(pad_len).collect();
    format!("{}{}{}", prefix_part, pad, suffix)
}

fn make_payload(index: usize, size: usize) -> Bytes {
    common::make_small(index, size)
}

// ---------------------------------------------------------------------------
// Result tracking
// ---------------------------------------------------------------------------

struct WorkloadResult {
    name: String,
    object_count: usize,
    total_ops: usize,
    total_elapsed: Duration,
    flush_time: Duration,
    flush_count: usize,
    index_bytes_final: u64,
    data_bytes_written: u64,
    index_bytes_written_est: u64,
}

impl WorkloadResult {
    fn ops_per_sec(&self) -> f64 {
        if self.total_elapsed.as_secs_f64() > 0.0 {
            self.total_ops as f64 / self.total_elapsed.as_secs_f64()
        } else {
            0.0
        }
    }
    fn avg_flush_ms(&self) -> f64 {
        if self.flush_count > 0 {
            (self.flush_time.as_secs_f64() * 1000.0) / self.flush_count as f64
        } else {
            0.0
        }
    }
    fn write_amplification(&self) -> f64 {
        if self.data_bytes_written > 0 {
            self.index_bytes_written_est as f64 / self.data_bytes_written as f64
        } else {
            0.0
        }
    }
}

fn print_result(r: &WorkloadResult) {
    println!();
    println!("=== {} ===", r.name);
    println!(
        "  Objects: {:>8}  |  Ops: {:>8}  |  Flushes: {:>8}",
        r.object_count, r.total_ops, r.flush_count
    );
    println!(
        "  Total time: {:>10.1} ms  |  Flush time: {:>10.1} ms  |  Avg flush: {:>8.2} ms",
        r.total_elapsed.as_secs_f64() * 1000.0,
        r.flush_time.as_secs_f64() * 1000.0,
        r.avg_flush_ms(),
    );
    println!(
        "  Index final: {:>10} B ({:.1} KB)  |  Est index I/O: {:>12} B ({:.1} MB)",
        r.index_bytes_final,
        r.index_bytes_final as f64 / 1024.0,
        r.index_bytes_written_est,
        r.index_bytes_written_est as f64 / (1024.0 * 1024.0),
    );
    println!(
        "  Data written: {:>10} B ({:.1} MB)  |  Write amp: {:>8.1}x  |  Ops/sec: {:>10.1}",
        r.data_bytes_written,
        r.data_bytes_written as f64 / (1024.0 * 1024.0),
        r.write_amplification(),
        r.ops_per_sec(),
    );
}

fn print_summary_table(results: &[WorkloadResult]) {
    println!();
    println!(
        "{:<50} {:>8} {:>8} {:>10} {:>10} {:>12} {:>12} {:>8}",
        "Workload", "Objects", "Flushes", "Total(ms)", "Flush(ms)", "IdxFinal(KB)", "IdxIO(MB)", "WrAmp"
    );
    println!("{}", "-".repeat(130));
    for r in results {
        println!(
            "{:<50} {:>8} {:>8} {:>10.1} {:>10.1} {:>12.1} {:>12.1} {:>8.1}x",
            r.name,
            r.object_count,
            r.flush_count,
            r.total_elapsed.as_secs_f64() * 1000.0,
            r.flush_time.as_secs_f64() * 1000.0,
            r.index_bytes_final as f64 / 1024.0,
            r.index_bytes_written_est as f64 / (1024.0 * 1024.0),
            r.write_amplification(),
        );
    }
    println!("{}", "-".repeat(130));
}

// ---------------------------------------------------------------------------
// W1: Sequential Append (flush per PUT)
// ---------------------------------------------------------------------------

fn w1_sequential_append(count: usize, device_size: u64) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(device_size);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;
    let mut flush_time = Duration::ZERO;
    let mut flush_count = 0usize;
    let mut index_bytes_written_est = 0u64;

    let start = Instant::now();
    for i in 0..count {
        let key = long_key("w1", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();

        let t = Instant::now();
        store.flush_index().unwrap();
        flush_time += t.elapsed();
        flush_count += 1;

        // After flush, last_flush_bytes reflects bytes written in this flush
        let info = store.device_info();
        index_bytes_written_est += info.last_flush_bytes;
    }
    let total_elapsed = start.elapsed();

    let info = store.device_info();
    WorkloadResult {
        name: format!("W1: seq_append({}) flush_per_put", count),
        object_count: info.file_count,
        total_ops: count,
        total_elapsed,
        flush_time,
        flush_count,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written: count as u64 * payload_size as u64,
        index_bytes_written_est,
    }
}

// ---------------------------------------------------------------------------
// W2: Batch Append (varying flush interval)
// ---------------------------------------------------------------------------

fn w2_batch_append(total: usize, flush_every: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_1GB);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;
    let mut flush_time = Duration::ZERO;
    let mut flush_count = 0usize;
    let mut index_bytes_written_est = 0u64;

    let start = Instant::now();
    for i in 0..total {
        let key = long_key("w2", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();

        if (i + 1) % flush_every == 0 || i == total - 1 {
            let t = Instant::now();
            store.flush_index().unwrap();
            flush_time += t.elapsed();
            flush_count += 1;
            let info = store.device_info();
            index_bytes_written_est += info.last_flush_bytes;
        }
    }
    let total_elapsed = start.elapsed();

    let info = store.device_info();
    WorkloadResult {
        name: format!("W2: batch_append({}x flush_every_{})", total, flush_every),
        object_count: info.file_count,
        total_ops: total,
        total_elapsed,
        flush_time,
        flush_count,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written: total as u64 * payload_size as u64,
        index_bytes_written_est,
    }
}

// ---------------------------------------------------------------------------
// W3: Mixed Sizes Append (flush per PUT, same key count)
// ---------------------------------------------------------------------------

fn w3_mixed_sizes(count_per_size: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_1GB);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sizes = [512usize, 4096, 65536, 1024 * 1024];
    let mut flush_time = Duration::ZERO;
    let mut flush_count = 0usize;
    let mut index_bytes_written_est = 0u64;
    let mut data_bytes_written = 0u64;
    let mut total_ops = 0usize;

    let start = Instant::now();
    for (si, &sz) in sizes.iter().enumerate() {
        for i in 0..count_per_size {
            let key = long_key(&format!("w3/{}", si), i);
            let payload = make_payload(i, sz);
            data_bytes_written += sz as u64;
            rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
                .unwrap();
            total_ops += 1;

            let t = Instant::now();
            store.flush_index().unwrap();
            flush_time += t.elapsed();
            flush_count += 1;
            let info = store.device_info();
            index_bytes_written_est += info.last_flush_bytes;
        }
    }
    let total_elapsed = start.elapsed();

    let info = store.device_info();
    WorkloadResult {
        name: format!("W3: mixed_sizes({}x4_sizes)", count_per_size),
        object_count: info.file_count,
        total_ops,
        total_elapsed,
        flush_time,
        flush_count,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written,
        index_bytes_written_est,
    }
}

// ---------------------------------------------------------------------------
// W4: Overwrite-Heavy (constant index size)
// ---------------------------------------------------------------------------

fn w4_overwrite(base_count: usize, overwrite_ops: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_512MB);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;

    // Pre-populate
    for i in 0..base_count {
        let key = long_key("w4", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();
    }
    store.flush_index().unwrap();

    let mut rng = StdRng::seed_from_u64(42);
    let mut flush_time = Duration::ZERO;
    let mut flush_count = 0usize;
    let mut index_bytes_written_est = 0u64;

    let start = Instant::now();
    for _ in 0..overwrite_ops {
        let idx = rng.gen_range(0..base_count);
        let key = long_key("w4", idx);
        let payload = make_payload(idx + 100000, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();

        let t = Instant::now();
        store.flush_index().unwrap();
        flush_time += t.elapsed();
        flush_count += 1;
        let info = store.device_info();
        index_bytes_written_est += info.last_flush_bytes;
    }
    let total_elapsed = start.elapsed();

    let info = store.device_info();
    WorkloadResult {
        name: format!("W4: overwrite(base={} ops={})", base_count, overwrite_ops),
        object_count: info.file_count,
        total_ops: overwrite_ops,
        total_elapsed,
        flush_time,
        flush_count,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written: overwrite_ops as u64 * payload_size as u64,
        index_bytes_written_est,
    }
}

// ---------------------------------------------------------------------------
// W5: Delete-Heavy (shrinking index)
// ---------------------------------------------------------------------------

fn w5_delete(initial_count: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_512MB);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;

    // Pre-populate
    for i in 0..initial_count {
        let key = long_key("w5", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();
    }
    store.flush_index().unwrap();

    let mut flush_time = Duration::ZERO;
    let mut flush_count = 0usize;
    let mut index_bytes_written_est = 0u64;

    let start = Instant::now();
    for i in 0..initial_count {
        let key = long_key("w5", i);
        rt.block_on(store.delete(&Path::from(key))).unwrap();

        let t = Instant::now();
        store.flush_index().unwrap();
        flush_time += t.elapsed();
        flush_count += 1;
        let info = store.device_info();
        index_bytes_written_est += info.last_flush_bytes;
    }
    let total_elapsed = start.elapsed();

    let info = store.device_info();
    WorkloadResult {
        name: format!("W5: delete({}->0)", initial_count),
        object_count: info.file_count,
        total_ops: initial_count,
        total_elapsed,
        flush_time,
        flush_count,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written: 0,
        index_bytes_written_est,
    }
}

// ---------------------------------------------------------------------------
// W6: Mixed CRUD
// ---------------------------------------------------------------------------

fn w6_mixed_crud(base_count: usize, total_ops: usize, flush_every: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_1GB);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;

    // Pre-populate base
    for i in 0..base_count {
        let key = long_key("w6", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();
    }
    store.flush_index().unwrap();

    let mut rng = StdRng::seed_from_u64(42);
    let mut next_id = base_count;
    let mut live_keys: Vec<String> = (0..base_count).map(|i| long_key("w6", i)).collect();
    let mut flush_time = Duration::ZERO;
    let mut flush_count = 0usize;
    let mut index_bytes_written_est = 0u64;
    let mut data_bytes = 0u64;
    let mut ops_done = 0usize;

    let start = Instant::now();
    for op_i in 0..total_ops {
        let r: f64 = rng.gen();
        if r < 0.50 {
            // PUT new
            let key = long_key("w6", next_id);
            next_id += 1;
            let payload = make_payload(next_id, payload_size);
            data_bytes += payload_size as u64;
            rt.block_on(store.put(&Path::from(key.clone()), PutPayload::from(payload)))
                .unwrap();
            live_keys.push(key);
        } else if r < 0.70 && !live_keys.is_empty() {
            // Overwrite existing
            let idx = rng.gen_range(0..live_keys.len());
            let key = live_keys[idx].clone();
            let payload = make_payload(next_id + 100000, payload_size);
            data_bytes += payload_size as u64;
            rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
                .unwrap();
        } else if r < 0.90 && !live_keys.is_empty() {
            // GET (read-only, still counts as an op but no data written)
            let idx = rng.gen_range(0..live_keys.len());
            let key = live_keys[idx].clone();
            let _ = rt.block_on(store.get(&Path::from(key)));
        } else if !live_keys.is_empty() {
            // DELETE
            let idx = rng.gen_range(0..live_keys.len());
            let key = live_keys.swap_remove(idx);
            let _ = rt.block_on(store.delete(&Path::from(key)));
        }
        ops_done += 1;

        if (op_i + 1) % flush_every == 0 {
            let t = Instant::now();
            store.flush_index().unwrap();
            flush_time += t.elapsed();
            flush_count += 1;
            let info = store.device_info();
            index_bytes_written_est += info.last_flush_bytes;
        }
    }
    // Final flush
    let t = Instant::now();
    store.flush_index().unwrap();
    flush_time += t.elapsed();
    flush_count += 1;
    let info = store.device_info();
    index_bytes_written_est += info.last_flush_bytes;

    let total_elapsed = start.elapsed();
    WorkloadResult {
        name: format!(
            "W6: mixed_crud(base={} ops={} flush_every={})",
            base_count, total_ops, flush_every
        ),
        object_count: info.file_count,
        total_ops: ops_done,
        total_elapsed,
        flush_time,
        flush_count,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written: data_bytes,
        index_bytes_written_est,
    }
}

// ---------------------------------------------------------------------------
// W7: Reopen / Recovery Time
// ---------------------------------------------------------------------------

fn w7_reopen(count: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_1GB);
    let reopen_path = if let Ok(dev) = std::env::var("RAW_DEVICE") {
        std::path::PathBuf::from(dev)
    } else {
        _tmp.as_ref().unwrap().path().to_path_buf()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;
    for i in 0..count {
        let key = long_key("w7", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();
    }
    store.flush_index().unwrap();
    let info = store.device_info();
    let index_bytes_final = info.index_serialized_bytes;
    drop(store);

    // Measure open time
    let start = Instant::now();
    let store2 = RawObjectStore::open(&reopen_path).unwrap();
    let open_elapsed = start.elapsed();

    let info2 = store2.device_info();
    assert_eq!(info2.file_count, count);

    WorkloadResult {
        name: format!("W7: reopen({} objects)", count),
        object_count: count,
        total_ops: 1,
        total_elapsed: open_elapsed,
        flush_time: Duration::ZERO,
        flush_count: 0,
        index_bytes_final,
        data_bytes_written: 0,
        index_bytes_written_est: 0,
    }
}

// ---------------------------------------------------------------------------
// W8: Index Size Tracking
// ---------------------------------------------------------------------------

fn w8_index_size(count: usize) -> WorkloadResult {
    let (store, _tmp) = make_bench_store(DEVICE_1GB);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let payload_size = 4096usize;
    for i in 0..count {
        let key = long_key("w8", i);
        let payload = make_payload(i, payload_size);
        rt.block_on(store.put(&Path::from(key), PutPayload::from(payload)))
            .unwrap();
    }
    store.flush_index().unwrap();

    let info = store.device_info();
    WorkloadResult {
        name: format!("W8: index_size({} objects)", count),
        object_count: info.file_count,
        total_ops: count,
        total_elapsed: Duration::ZERO,
        flush_time: Duration::ZERO,
        flush_count: 1,
        index_bytes_final: info.index_serialized_bytes,
        data_bytes_written: count as u64 * payload_size as u64,
        index_bytes_written_est: info.index_serialized_bytes,
    }
}

// ===========================================================================
// Test entry points
// ===========================================================================

#[test]
#[ignore]
fn bench_w1_sequential_append() {
    println!("\n########## W1: Sequential Append (flush per PUT) ##########");
    let counts = [100, 500, 1000, 5000];
    let mut results = Vec::new();
    for &c in &counts {
        let device = if c > 2000 { DEVICE_1GB } else { DEVICE_512MB };
        let r = w1_sequential_append(c, device);
        print_result(&r);
        results.push(r);
    }
    print_summary_table(&results);
}

#[test]
#[ignore]
fn bench_w2_batch_append() {
    println!("\n########## W2: Batch Append (varying flush interval) ##########");
    let intervals = [1, 10, 50, 100, 500];
    let mut results = Vec::new();
    for &n in &intervals {
        let r = w2_batch_append(10000, n);
        print_result(&r);
        results.push(r);
    }
    print_summary_table(&results);
}

#[test]
#[ignore]
fn bench_w3_mixed_sizes() {
    println!("\n########## W3: Mixed Sizes (flush per PUT) ##########");
    // 500 objects per size category = 2000 total
    let r = w3_mixed_sizes(500);
    print_result(&r);
}

#[test]
#[ignore]
fn bench_w4_overwrite() {
    println!("\n########## W4: Overwrite-Heavy ##########");
    let r = w4_overwrite(1000, 2000);
    print_result(&r);
}

#[test]
#[ignore]
fn bench_w5_delete() {
    println!("\n########## W5: Delete-Heavy ##########");
    let r = w5_delete(2000);
    print_result(&r);
}

#[test]
#[ignore]
fn bench_w6_mixed_crud() {
    println!("\n########## W6: Mixed CRUD ##########");
    let r = w6_mixed_crud(1000, 10000, 10);
    print_result(&r);
}

#[test]
#[ignore]
fn bench_w7_reopen() {
    println!("\n########## W7: Reopen / Recovery Time ##########");
    let counts = [100, 1000, 5000, 10000];
    let mut results = Vec::new();
    for &c in &counts {
        let r = w7_reopen(c);
        print_result(&r);
        results.push(r);
    }
    print_summary_table(&results);
}

#[test]
#[ignore]
fn bench_w8_index_size() {
    println!("\n########## W8: Index Size Tracking ##########");
    let counts = [100, 500, 1000, 5000, 10000];
    let mut results = Vec::new();
    for &c in &counts {
        let r = w8_index_size(c);
        println!(
            "  {} objects  |  index = {} B ({:.1} KB)  |  bytes/entry = {:.0}",
            c,
            r.index_bytes_final,
            r.index_bytes_final as f64 / 1024.0,
            r.index_bytes_final as f64 / c as f64,
        );
        results.push(r);
    }
}

#[test]
#[ignore]
fn bench_all_workloads() {
    println!("\n########## Running All Index Write Amplification Benchmarks ##########");
    println!("Mode: {}\n", bench_mode_label());

    let mut all_results: Vec<WorkloadResult> = Vec::new();

    // W1
    println!("--- W1: Sequential Append ---");
    for &c in &[100, 500, 1000, 5000] {
        let device = if c > 2000 { DEVICE_1GB } else { DEVICE_512MB };
        let r = w1_sequential_append(c, device);
        print_result(&r);
        all_results.push(r);
    }

    // W2
    println!("\n--- W2: Batch Append ---");
    for &n in &[1, 10, 50, 100, 500] {
        let r = w2_batch_append(10000, n);
        print_result(&r);
        all_results.push(r);
    }

    // W3
    println!("\n--- W3: Mixed Sizes ---");
    {
        let r = w3_mixed_sizes(500);
        print_result(&r);
        all_results.push(r);
    }

    // W4
    println!("\n--- W4: Overwrite-Heavy ---");
    {
        let r = w4_overwrite(1000, 2000);
        print_result(&r);
        all_results.push(r);
    }

    // W5
    println!("\n--- W5: Delete-Heavy ---");
    {
        let r = w5_delete(2000);
        print_result(&r);
        all_results.push(r);
    }

    // W6
    println!("\n--- W6: Mixed CRUD ---");
    {
        let r = w6_mixed_crud(1000, 10000, 10);
        print_result(&r);
        all_results.push(r);
    }

    // W7
    println!("\n--- W7: Reopen ---");
    for &c in &[100, 1000, 5000, 10000] {
        let r = w7_reopen(c);
        print_result(&r);
        all_results.push(r);
    }

    // W8
    println!("\n--- W8: Index Size ---");
    for &c in &[100, 500, 1000, 5000, 10000] {
        let r = w8_index_size(c);
        println!(
            "  {} objects  |  index = {} B ({:.1} KB)  |  bytes/entry = {:.0}",
            c,
            r.index_bytes_final,
            r.index_bytes_final as f64 / 1024.0,
            r.index_bytes_final as f64 / c as f64,
        );
        all_results.push(r);
    }

    // Summary
    println!("\n\n########## SUMMARY ##########");
    print_summary_table(&all_results);
}
