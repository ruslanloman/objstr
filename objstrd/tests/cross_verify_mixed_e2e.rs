//! End-to-end cross-verify test with 3 heterogeneous store types:
//!
//!   Shard 0: RawObjectStore (block image)
//!   Shard 1: LocalFileSystem wrapped in ShardedObjectStore(rf=1)
//!   Shard 2: AmazonS3 client -> in-process objstrd --backend fs
//!
//! RF=3 so every object lands on all three shards. We then corrupt files
//! on the FS-backed shards and verify cross_verify_all detects them.
//!
//! Shard 1 uses the same wrapping that objstrd uses internally for
//! --backend fs: ShardedObjectStore::new_with_offline(vec![Some(fs)], 1).
//! Shard 2 is a full S3 endpoint backed by FS via the daemon path.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;
use shardedobjstr::metadata::{RawRefRegistry, ShardKind};
use shardedobjstr::ShardedObjectStore;

use objstrd::adapter::ObjectStoreS3Adapter;

use s3s::service::S3ServiceBuilder;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

/// Spin up an in-process objstrd backed by a single LocalFileSystem,
/// wrapped in ShardedObjectStore(rf=1) exactly as `--backend fs` does.
/// Returns the base URL and a shutdown sender.
async fn start_fs_objstrd(
    fs_dir: &std::path::Path,
    bucket: &str,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let fs_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(fs_dir)
            .expect("failed to create LocalFileSystem for objstrd"),
    );

    let cluster = Arc::new(
        ShardedObjectStore::new_with_offline(vec![Some(fs_store)], 1),
    );
    let registry = Arc::new(RawRefRegistry::new(
        vec![None],
        vec![ShardKind::Sidecar],
    ));

    let mut adapter =
        ObjectStoreS3Adapter::with_bucket_sharded(cluster, registry, bucket);
    adapter.set_allow_anon_list_buckets(true);

    let service = {
        let builder = S3ServiceBuilder::new(adapter);
        builder.build()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let http_server = ConnBuilder::new(TokioExecutor::new());
        tokio::select! {
            _ = async {
                loop {
                    let (socket, _addr) = match listener.accept().await {
                        Ok(conn) => conn,
                        Err(_) => break,
                    };
                    let svc = service.clone();
                    let conn = http_server
                        .serve_connection(TokioIo::new(socket), svc)
                        .into_owned();
                    tokio::spawn(async move { let _ = conn.await; });
                }
            } => {}
            _ = shutdown_rx => {}
        }
    });

    (format!("http://127.0.0.1:{port}"), shutdown_tx)
}

/// Build an AmazonS3 ObjectStore client pointing at the given endpoint.
fn build_s3_client(endpoint: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_skip_signature(true)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("failed to build AmazonS3 client");
    Arc::new(store)
}

/// Upload `count` test objects to the cluster. Returns the list of keys.
async fn upload_test_objects(
    cluster: &ShardedObjectStore,
    count: usize,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("test/obj_{:04}.bin", i);
        let data = vec![0x30 + (i as u8 % 26); 4096];
        cluster
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(Bytes::from(data)),
            )
            .await
            .unwrap();
        keys.push(key);
    }
    keys
}

/// Cross-verify with 3 store types: raw + FS-via-shard-wrapper + S3-backed-by-FS.
///
/// Setup:
///   - Outer ShardedObjectStore RF=3 (every object on all 3 shards)
///   - Shard 0: RawObjectStore (image file)
///   - Shard 1: ShardedObjectStore(rf=1) wrapping LocalFileSystem at fs2_dir
///   - Shard 2: AmazonS3 -> objstrd --backend fs -> LocalFileSystem at fs3_dir
///
/// After uploading 10 objects:
///   - Corrupt obj_0002 on shard 1 (FS file at fs2_dir)
///   - Corrupt obj_0005 on shard 2 (FS file at fs3_dir, behind the S3 daemon)
///   - Corrupt obj_0008 on shard 0 (raw) AND shard 1 (FS) with different data
///
/// Expected: 7 ok, 3 mismatched, 0 errors.
#[tokio::test]
async fn cross_verify_three_store_types() {
    let tmp = tempfile::tempdir().unwrap();

    // -- Shard 0: RawObjectStore ----------------------------------------
    let raw_path = tmp.path().join("shard0.raw");
    let raw_store = {
        let store = RawObjectStore::format_with_size(&raw_path, 64 * 1024 * 1024, false)
            .expect("failed to format raw image");
        Arc::new(store)
    };

    // -- Shard 1: LocalFileSystem wrapped in ShardedObjectStore(rf=1) ---
    // This mirrors what objstrd --backend fs does internally.
    let fs2_dir = tmp.path().join("fs2");
    std::fs::create_dir_all(&fs2_dir).unwrap();
    let fs2_inner: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&fs2_dir).unwrap(),
    );
    let fs2_store: Arc<dyn ObjectStore> = Arc::new(
        ShardedObjectStore::new_with_offline(vec![Some(fs2_inner)], 1),
    );

    // -- Shard 2: AmazonS3 -> objstrd -> LocalFileSystem ----------------
    let fs3_dir = tmp.path().join("fs3");
    std::fs::create_dir_all(&fs3_dir).unwrap();
    let (s3_url, _shutdown_tx) = start_fs_objstrd(&fs3_dir, "testbucket").await;
    let s3_store = build_s3_client(&s3_url, "testbucket");

    // -- Outer ShardedObjectStore RF=3 ----------------------------------
    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raw_store.clone() as Arc<dyn ObjectStore>,
        fs2_store,
        s3_store,
    ];
    let cluster = ShardedObjectStore::new(stores, 3);
    cluster.rebuild_catalog().await.unwrap();

    // -- Upload 10 objects ----------------------------------------------
    let keys = upload_test_objects(&cluster, 10).await;
    raw_store.flush_index().unwrap();

    // -- Sanity check: everything consistent ----------------------------
    let report = cluster.cross_verify_all(None, None).await;
    assert_eq!(
        report.objects_mismatched, 0,
        "pre-corruption: all should be consistent, but got {} mismatches: {:#?}",
        report.objects_mismatched, report.details,
    );
    assert_eq!(report.objects_checked, 10);
    assert_eq!(report.objects_ok, 10);

    // -- Corrupt obj_0002 on shard 1 (FS behind shard wrapper) ----------
    // The shard wrapper stores at the same path as the key, so the file
    // lives at fs2_dir / <key>.
    let corrupt_path_2 = fs2_dir.join(&keys[2]);
    assert!(corrupt_path_2.exists(), "FS shard 1 file missing: {:?}", corrupt_path_2);
    std::fs::write(&corrupt_path_2, b"BAD-DATA-SHARD1-obj0002").unwrap();

    // -- Corrupt obj_0005 on shard 2 (via S3 PUT to objstrd) -----------
    // Direct file corruption behind S3 does not update the sidecar ETag,
    // so write corrupt data through the S3 API which updates both the
    // data file and the ETag/metadata atomically.
    let direct_s3 = build_s3_client(&s3_url, "testbucket");
    direct_s3
        .put(
            &Path::from(keys[5].as_str()),
            PutPayload::from(Bytes::from(b"BAD-DATA-SHARD2-obj0005".to_vec())),
        )
        .await
        .unwrap();

    // -- Corrupt obj_0008 on shard 0 (raw) AND shard 1 (FS) ------------
    // This leaves only shard 2 (S3) with the original good copy.
    raw_store
        .put(
            &Path::from(keys[8].as_str()),
            PutPayload::from(Bytes::from(b"CORRUPT-RAW-obj0008".to_vec())),
        )
        .await
        .unwrap();
    raw_store.flush_index().unwrap();

    let corrupt_path_8 = fs2_dir.join(&keys[8]);
    assert!(corrupt_path_8.exists(), "FS shard 1 file missing: {:?}", corrupt_path_8);
    std::fs::write(&corrupt_path_8, b"CORRUPT-FS-obj0008-different").unwrap();

    // -- Verify ---------------------------------------------------------
    let report = cluster.cross_verify_all(None, None).await;

    eprintln!("=== Cross-verify report ===");
    eprintln!("  checked: {}", report.objects_checked);
    eprintln!("  ok: {}", report.objects_ok);
    eprintln!("  mismatched: {}", report.objects_mismatched);
    eprintln!("  errors: {}", report.objects_with_errors);
    eprintln!("  skipped: {}", report.objects_skipped_single_replica);
    for d in &report.details {
        eprintln!("  DETAIL key={} consistent={} errors={:?}", d.key, d.consistent, d.errors);
        for s in &d.shards {
            eprintln!("    shard {} md5={} size={}", s.shard_id, s.md5_hex, s.size);
        }
    }

    assert_eq!(report.objects_checked, 10);
    assert_eq!(report.objects_ok, 7, "7 objects should be clean");
    assert_eq!(report.objects_mismatched, 3, "3 objects should have mismatches");
    assert_eq!(report.objects_with_errors, 0, "no errors expected");

    // Check that the right keys were flagged.
    let flagged: HashSet<String> = report
        .details
        .iter()
        .map(|d| d.key.clone())
        .collect();
    assert!(flagged.contains(&keys[2]), "obj_0002 should be flagged");
    assert!(flagged.contains(&keys[5]), "obj_0005 should be flagged");
    assert!(flagged.contains(&keys[8]), "obj_0008 should be flagged");

    // obj_0008: all three shards should report different MD5s
    let detail_8 = report
        .details
        .iter()
        .find(|d| d.key == keys[8])
        .expect("obj_0008 detail missing");
    assert_eq!(detail_8.shards.len(), 3);
    let md5s: HashSet<&str> = detail_8
        .shards
        .iter()
        .map(|s| s.md5_hex.as_str())
        .collect();
    assert_eq!(md5s.len(), 3, "obj_0008 should have 3 distinct MD5s (raw, fs2, s3)");
}
