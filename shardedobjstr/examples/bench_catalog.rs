//! Benchmark catalog persistence: rebuild, load, and save times.
//!
//! Opens a raw object store image, wraps it as a single-shard cluster,
//! and times catalog operations (rebuild from index, JSON load/save,
//! bincode load/save) across multiple iterations.
//!
//! Usage:
//!   bench_catalog <IMAGE_PATH> [ITERATIONS]
//!
//! Example:
//!   bench_catalog /tmp/bench_seed.raw 3

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use rawobjstr::store::RawObjectStore;
use shardedobjstr::catalog::CatalogPersistence;
use shardedobjstr::ShardedObjectStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: bench_catalog <IMAGE_PATH> [ITERATIONS]");
        eprintln!("  IMAGE_PATH  Path to a raw object store image");
        eprintln!("  ITERATIONS  Number of timing iterations (default: 3)");
        std::process::exit(1);
    }
    let image_path = &args[1];
    let iterations: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);

    if !Path::new(image_path).exists() {
        eprintln!("FATAL: image not found: {}", image_path);
        std::process::exit(1);
    }

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let json_path = tmp_dir.path().join("catalog.json");
    let bin_path = tmp_dir.path().join("catalog.bin");

    // Open the raw store once to count objects.
    let raw = Arc::new(
        RawObjectStore::open_readonly(Path::new(image_path))
            .expect("failed to open raw store"),
    );
    let cluster = ShardedObjectStore::new(vec![raw.clone() as Arc<dyn object_store::ObjectStore>], 1);
    let obj_count = rt.block_on(cluster.rebuild_catalog()).expect("rebuild");

    println!("========================================");
    println!("  Catalog Benchmark");
    println!("  Image:      {}", image_path);
    println!("  Objects:    {}", obj_count);
    println!("  Iterations: {}", iterations);
    println!("========================================");
    println!();

    // ------------------------------------------------------------------
    // Phase 1: Rebuild from index (no persistence)
    // ------------------------------------------------------------------
    println!("--- Phase 1: Rebuild from index (no persistence) ---");
    let rebuild_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        let t = Instant::now();
        let _n = rt.block_on(cluster.rebuild_catalog()).expect("rebuild");
        t.elapsed().as_millis() as u64
    });
    println!("  => median: {} ms", rebuild_ms);
    println!();

    // ------------------------------------------------------------------
    // Phase 2: JSON - create initial file
    // ------------------------------------------------------------------
    println!("--- Phase 2: JSON - create catalog file ---");
    {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        let _n = rt.block_on(cluster.rebuild_catalog()).expect("rebuild");
        cluster.set_persistence(CatalogPersistence::json(&json_path));
        let t = Instant::now();
        cluster.save_catalog().expect("save json");
        let save_ms = t.elapsed().as_millis();
        let file_size = std::fs::metadata(&json_path).map(|m| m.len()).unwrap_or(0);
        println!("  Initial save: {} ms", save_ms);
        println!("  File size: {} bytes ({:.1} MB)", file_size, file_size as f64 / 1_048_576.0);
    }
    println!();

    // ------------------------------------------------------------------
    // Phase 3: JSON - load from file (clean)
    // ------------------------------------------------------------------
    println!("--- Phase 3: JSON - load from file (clean) ---");
    let json_load_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        cluster.set_persistence(CatalogPersistence::json(&json_path));
        let t = Instant::now();
        cluster.load_catalog().expect("load json");
        t.elapsed().as_millis() as u64
    });
    println!("  => load median: {} ms", json_load_ms);
    println!();

    // ------------------------------------------------------------------
    // Phase 4: JSON - clean shutdown (save_if_dirty, not dirty)
    // ------------------------------------------------------------------
    println!("--- Phase 4: JSON - clean shutdown (no save needed) ---");
    let json_clean_close_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        cluster.set_persistence(CatalogPersistence::json(&json_path));
        cluster.load_catalog().expect("load json");
        let t = Instant::now();
        let _saved = cluster.save_catalog_if_dirty().expect("save_if_dirty");
        t.elapsed().as_millis() as u64
    });
    println!("  => close median: {} ms", json_clean_close_ms);
    println!();

    // ------------------------------------------------------------------
    // Phase 5: JSON - dirty shutdown (modify catalog, then save)
    // ------------------------------------------------------------------
    println!("--- Phase 5: JSON - dirty shutdown (save after mutation) ---");
    let json_dirty_close_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        cluster.set_persistence(CatalogPersistence::json(&json_path));
        cluster.load_catalog().expect("load json");
        // Mutate the catalog so it becomes dirty.
        cluster.catalog().put(
            "bench-dirty/marker".to_string(),
            vec![0],
            4096,
            None,
            0,
        );
        let t = Instant::now();
        let _saved = cluster.save_catalog_if_dirty().expect("save_if_dirty");
        t.elapsed().as_millis() as u64
    });
    println!("  => dirty close median: {} ms", json_dirty_close_ms);
    println!();

    // ------------------------------------------------------------------
    // Phase 6: Bincode - create initial file
    // ------------------------------------------------------------------
    println!("--- Phase 6: Bincode - create catalog file ---");
    {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        let _n = rt.block_on(cluster.rebuild_catalog()).expect("rebuild");
        cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
        let t = Instant::now();
        cluster.save_catalog().expect("save bincode");
        let save_ms = t.elapsed().as_millis();
        let file_size = std::fs::metadata(&bin_path).map(|m| m.len()).unwrap_or(0);
        println!("  Initial save: {} ms", save_ms);
        println!("  File size: {} bytes ({:.1} MB)", file_size, file_size as f64 / 1_048_576.0);
    }
    println!();

    // ------------------------------------------------------------------
    // Phase 7: Bincode - load from file (clean)
    // ------------------------------------------------------------------
    println!("--- Phase 7: Bincode - load from file (clean) ---");
    let bin_load_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
        let t = Instant::now();
        cluster.load_catalog().expect("load bincode");
        t.elapsed().as_millis() as u64
    });
    println!("  => load median: {} ms", bin_load_ms);
    println!();

    // ------------------------------------------------------------------
    // Phase 8: Bincode - clean shutdown
    // ------------------------------------------------------------------
    println!("--- Phase 8: Bincode - clean shutdown (no save needed) ---");
    let bin_clean_close_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
        cluster.load_catalog().expect("load bincode");
        let t = Instant::now();
        let _saved = cluster.save_catalog_if_dirty().expect("save_if_dirty");
        t.elapsed().as_millis() as u64
    });
    println!("  => close median: {} ms", bin_clean_close_ms);
    println!();

    // ------------------------------------------------------------------
    // Phase 9: Bincode - dirty shutdown
    // ------------------------------------------------------------------
    println!("--- Phase 9: Bincode - dirty shutdown (save after mutation) ---");
    let bin_dirty_close_ms = run_iterations(iterations, || {
        let raw = Arc::new(
            RawObjectStore::open_readonly(Path::new(image_path))
                .expect("open"),
        );
        let cluster = ShardedObjectStore::new(vec![raw as Arc<dyn object_store::ObjectStore>], 1);
        cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
        cluster.load_catalog().expect("load bincode");
        // Mutate the catalog so it becomes dirty.
        cluster.catalog().put(
            "bench-dirty/marker".to_string(),
            vec![0],
            4096,
            None,
            0,
        );
        let t = Instant::now();
        let _saved = cluster.save_catalog_if_dirty().expect("save_if_dirty");
        t.elapsed().as_millis() as u64
    });
    println!("  => dirty close median: {} ms", bin_dirty_close_ms);
    println!();

    // ------------------------------------------------------------------
    // Results
    // ------------------------------------------------------------------
    let json_size = std::fs::metadata(&json_path).map(|m| m.len()).unwrap_or(0);
    let bin_size = std::fs::metadata(&bin_path).map(|m| m.len()).unwrap_or(0);

    println!("========================================");
    println!("  Results -- {} objects", obj_count);
    println!("========================================");
    println!("  {:20} {:>10} {:>12} {:>14}", "Mode", "Load (ms)", "Close (ms)", "Dirty Close (ms)");
    println!("  {:20} {:>10} {:>12} {:>14}", "----", "---------", "---------", "---------------");
    println!("  {:20} {:>10} {:>12} {:>14}", "rebuild (index)",   rebuild_ms,       "-",                  "-");
    println!("  {:20} {:>10} {:>12} {:>14}", "json",              json_load_ms,     json_clean_close_ms,  json_dirty_close_ms);
    println!("  {:20} {:>10} {:>12} {:>14}", "bincode",           bin_load_ms,      bin_clean_close_ms,   bin_dirty_close_ms);
    println!("========================================");
    println!();
    println!("  JSON file:    {:.1} MB ({} bytes)", json_size as f64 / 1_048_576.0, json_size);
    println!("  Bincode file: {:.1} MB ({} bytes)", bin_size as f64 / 1_048_576.0, bin_size);
    println!();
    println!("  Times are median of {} iterations.", iterations);
    println!("  Load = catalog load from file (or rebuild from raw index).");
    println!("  Close = save_catalog_if_dirty (0 ms when clean).");
    println!("  Dirty Close = save_catalog_if_dirty after 1 mutation.");
}

/// Run a closure `n` times, print per-run times, return the median.
fn run_iterations<F: FnMut() -> u64>(n: usize, mut f: F) -> u64 {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let ms = f();
        println!("    run {}: {} ms", i + 1, ms);
        times.push(ms);
    }
    times.sort();
    times[n / 2]
}
