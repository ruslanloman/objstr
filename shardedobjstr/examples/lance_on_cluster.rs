//! Demo: LanceDB running on a sharded object store.
//!
//! Creates 3 shard images, builds a ShardedObjectStore, registers it
//! with LanceDB under the "cluster://" scheme, then creates a table,
//! inserts data, queries, and shows the object placement across shards.
//!
//! ```bash
//! # Quick run (creates temp images in /tmp):
//! lance_on_cluster
//!
//! # With custom paths and size:
//! lance_on_cluster --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
//!     --size 268435456 --replicas 2
//! ```

use std::fmt;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;

use arrow_array::{Float32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::query::ExecutableQuery;
use async_trait::async_trait;
use futures::TryStreamExt;
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams,
    providers::{ObjectStoreProvider, ObjectStoreRegistry},
};
use object_store::ObjectStore;
use url::Url;

use shardedobjstr::ShardedObjectStore;
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::Compression;

// -- Provider: bridges ShardedObjectStore into LanceDB -------------

struct ShardedObjectStoreProvider {
    store: Arc<ShardedObjectStore>,
}

impl ShardedObjectStoreProvider {
    fn new(store: Arc<ShardedObjectStore>) -> Self {
        Self { store }
    }
}

impl fmt::Debug for ShardedObjectStoreProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedObjectStoreProvider").finish()
    }
}

#[async_trait]
impl ObjectStoreProvider for ShardedObjectStoreProvider {
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
            false,  // list_is_lexically_ordered
            4,      // io_parallelism
            3,      // download_retry_count
            None,   // storage_options
        ))
    }
}

// -- Helpers ---------------------------------------------------------

fn format_shard(path: &StdPath, size: u64) -> Arc<RawObjectStore> {
    let store = RawObjectStore::format_with_options(
        path,
        FormatOptions {
            device_size: size,
            direct_io: false,
            index_slot_size: 16 * 1024 * 1024,
            max_key_length: rawobjstr::DEFAULT_MAX_KEY_LENGTH,
            compression: Compression::None,
        },
    )
    .unwrap_or_else(|e| panic!("format {} failed: {}", path.display(), e));
    store.flush_index().unwrap();
    Arc::new(store)
}

fn open_shard(path: &StdPath) -> Arc<RawObjectStore> {
    Arc::new(
        RawObjectStore::open(path)
            .unwrap_or_else(|e| panic!("open {} failed: {}", path.display(), e)),
    )
}

fn build_cluster(
    raw_stores: &[Arc<RawObjectStore>],
    replicas: usize,
    catalog_path: Option<PathBuf>,
) -> ShardedObjectStore {
    let stores: Vec<Arc<dyn ObjectStore>> = raw_stores.iter().map(|s| s.clone() as _).collect();
    let mut cluster = ShardedObjectStore::new(stores, replicas);

    if let Some(cp) = catalog_path {
        cluster.set_persistence(shardedobjstr::catalog::CatalogPersistence::Json { path: cp });
    }

    // Try loading from file; if that fails, rebuild from shards.
    if cluster.load_catalog().is_err() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(cluster.rebuild_catalog()).expect("rebuild_catalog failed");
    }

    let n = cluster.catalog().len();
    if n > 0 {
        println!("  Catalog: {} objects", n);
    } else {
        println!("  Catalog empty (no prior data)");
    }
    cluster
}

fn flush_all(raw_stores: &[Arc<RawObjectStore>]) {
    for s in raw_stores {
        s.flush_index().unwrap();
    }
}

// -- Arg parsing -----------------------------------------------------

struct Args {
    shard_paths: Vec<String>,
    replicas: usize,
    size: u64,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut shard_paths = Vec::new();
    let mut replicas = 2usize;
    let mut size = 256u64 * 1024 * 1024; // 256 MB default

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--shards" => {
                i += 1;
                shard_paths = args[i].split(',').map(|s| s.trim().to_string()).collect();
            }
            "--replicas" => {
                i += 1;
                replicas = args[i].parse().expect("invalid --replicas");
            }
            "--size" => {
                i += 1;
                size = args[i].parse().expect("invalid --size");
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Default: create temp shard paths
    if shard_paths.is_empty() {
        shard_paths = vec![
            "/tmp/lance_cluster_s0.raw".to_string(),
            "/tmp/lance_cluster_s1.raw".to_string(),
            "/tmp/lance_cluster_s2.raw".to_string(),
        ];
    }

    Args {
        shard_paths,
        replicas,
        size,
    }
}

// -- Main ------------------------------------------------------------

fn main() {
    let args = parse_args();
    let replicas = args.replicas;
    let catalog_file = PathBuf::from(format!("{}.catalog.json", args.shard_paths[0]));

    // -- 1. Format shard images --------------------------------------
    println!("=== 1. Formatting {} shards ({} MB each) ===", args.shard_paths.len(), args.size / (1024*1024));
    let raw_stores: Vec<Arc<RawObjectStore>> = args
        .shard_paths
        .iter()
        .enumerate()
        .map(|(i, p)| {
            print!("  shard {}: {} ... ", i, p);
            let s = format_shard(StdPath::new(p), args.size);
            println!("OK");
            s
        })
        .collect();

    // -- 2. Build sharded store with catalog persistence -----------
    println!("\n=== 2. Building cluster (replicas={}) ===", replicas);
    println!("  Catalog file: {}", catalog_file.display());
    let cluster = build_cluster(&raw_stores, replicas, Some(catalog_file.clone()));
    let cluster = Arc::new(cluster);

    // -- 3. Register with LanceDB ------------------------------------
    println!("\n=== 3. Connecting LanceDB via cluster:// scheme ===");
    let provider = Arc::new(ShardedObjectStoreProvider::new(Arc::clone(&cluster)));
    let registry = ObjectStoreRegistry::default();
    registry.insert("cluster", provider);
    let session = Arc::new(lancedb::Session::new(128, 128, Arc::new(registry)));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(async {
        lancedb::connect("cluster:///")
            .session(session)
            .execute()
            .await
            .expect("failed to connect LanceDB to cluster")
    });
    println!("  Connected.");

    // -- 4. Create a table -------------------------------------------
    println!("\n=== 4. Creating 'test' table ===");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float32, false),
    ]));

    let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec!["temp", "humidity", "pressure", "wind", "light"]);
    let values = Float32Array::from(vec![22.5, 65.0, 1013.25, 12.3, 850.0]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(ids),
        Arc::new(names),
        Arc::new(values),
    ])
    .unwrap();

    let table = rt.block_on(async {
        db.create_table("test", batch)
            .execute()
            .await
            .expect("create table failed")
    });
    flush_all(&raw_stores);
    println!("  Table 'test' created with 5 rows.");

    // -- 5. Query the table ------------------------------------------
    println!("\n=== 5. Querying test table ===");
    rt.block_on(async {
        let batches: Vec<RecordBatch> = table
            .query()
            .execute()
            .await
            .expect("query failed")
            .try_collect()
            .await
            .expect("collect failed");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("  Query returned {} batches, {} total rows", batches.len(), total_rows);

        for batch in &batches {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let values = batch.column(2).as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..batch.num_rows() {
                println!("    id={}, name={}, value={:.2}", ids.value(i), names.value(i), values.value(i));
            }
        }
    });

    // -- 6. Add more rows --------------------------------------------
    println!("\n=== 6. Adding 3 more rows ===");
    let ids2 = Int64Array::from(vec![6, 7, 8]);
    let names2 = StringArray::from(vec!["co2", "noise", "uv"]);
    let values2 = Float32Array::from(vec![415.0, 42.0, 7.5]);
    let batch2 = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(ids2),
        Arc::new(names2),
        Arc::new(values2),
    ])
    .unwrap();

    rt.block_on(async {
        table
            .add(batch2)
            .execute()
            .await
            .expect("add rows failed");
    });
    flush_all(&raw_stores);
    println!("  Added. Table now has 8 rows.");

    // -- 7. Show object placement across shards ----------------------
    println!("\n=== 7. Object placement across shards ===");
    rt.block_on(async {
        let files: Vec<_> = cluster.list(None).try_collect().await.unwrap();
        let mut sorted: Vec<_> = files.iter().collect();
        sorted.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

        for f in &sorted {
            let placement = cluster.placement(f.location.as_ref())
                .map(|e| e.shards.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(","))
                .unwrap_or_else(|| "?".to_string());
            println!("  {:>10}  [shards: {}]  {}", f.size, placement, f.location);
        }
        println!("  {} objects total", sorted.len());
    });

    // -- 8. Save catalog to disk -------------------------------------
    println!("\n=== 8. Saving catalog to {} ===", catalog_file.display());
    cluster.save_catalog().expect("save catalog failed");
    let cat_size = std::fs::metadata(&catalog_file).map(|m| m.len()).unwrap_or(0);
    println!("  Saved ({} bytes)", cat_size);

    // -- 9. Verify: reopen shards, load catalog, check data ---------
    println!("\n=== 9. Reopen shards + load persisted catalog ===");
    drop(table);
    drop(db);
    drop(cluster);
    drop(raw_stores);

    let raw_reopened: Vec<Arc<RawObjectStore>> = args
        .shard_paths
        .iter()
        .map(|p| open_shard(StdPath::new(p)))
        .collect();

    let cluster2 = build_cluster(&raw_reopened, replicas, Some(catalog_file.clone()));
    println!("  Catalog has {} objects after reload", cluster2.catalog().len());

    let cluster2 = Arc::new(cluster2);
    let provider2 = Arc::new(ShardedObjectStoreProvider::new(Arc::clone(&cluster2)));
    let registry2 = ObjectStoreRegistry::default();
    registry2.insert("cluster", provider2);
    let session2 = Arc::new(lancedb::Session::new(128, 128, Arc::new(registry2)));

    let db2 = rt.block_on(async {
        lancedb::connect("cluster:///")
            .session(session2)
            .execute()
            .await
            .expect("reconnect failed")
    });

    rt.block_on(async {
        let table2 = db2.open_table("test").execute().await.expect("open table failed");
        let count = table2.count_rows(None).await.expect("count failed");
        println!("  Reopened 'test' table: {} rows", count);
        assert!(count >= 5, "expected at least 5 rows, got {}", count);
    });

    // -- 10. Per-shard stats -----------------------------------------
    println!("\n=== 10. Per-shard storage stats ===");
    for (i, store) in raw_reopened.iter().enumerate() {
        let info = store.device_info();
        println!(
            "  Shard {}: {} files, {:.1} KB data, {:.1} MB free",
            i,
            info.file_count,
            info.data_bytes_stored as f64 / 1024.0,
            info.free_space as f64 / 1_048_576.0,
        );
    }

    // Cleanup catalog file
    let _ = std::fs::remove_file(&catalog_file);

    println!("\nDone. LanceDB on sharded object store works.");
}
