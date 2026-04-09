//! Reliability tests for multipart uploads in ShardedObjectStore.
//!
//! These are regression tests for bugs that have been fixed:
//!
//! 1. **Tracking leak on InsufficientWrites** (FIXED): complete() now
//!    deregisters the tracking entry before replication, so a later
//!    purge_stale_multiparts() cannot delete committed data.
//!
//! 2. **Orphan cleanup on InsufficientWrites** (FIXED): complete() now
//!    deletes data from all shards that succeeded when min_writes is
//!    not met, mirroring write_to_shards_inner().
//!
//! 3. **Metadata after multipart overwrite**: multipart uploads have no
//!    mechanism to attach metadata (put_multipart() accepts no metadata
//!    argument). After a multipart overwrite, meta_len=0 is correct
//!    because the assembled blob has no metadata. This is expected
//!    behavior, not a bug.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::metadata::{
    put_with_meta, RawRefRegistry, ShardKind,
};
use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

const SHARD_SIZE: u64 = 64 * 1024 * 1024;

// =====================================================================
// FailableStore: wraps any ObjectStore and can be toggled to fail puts.
// =====================================================================

#[derive(Debug)]
struct FailableStore {
    inner: Arc<dyn ObjectStore>,
    fail_puts: AtomicBool,
}

impl FailableStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fail_puts: AtomicBool::new(false),
        }
    }

    fn set_fail_puts(&self, fail: bool) {
        self.fail_puts.store(fail, Ordering::SeqCst);
    }

    fn check_put(&self) -> object_store::Result<()> {
        if self.fail_puts.load(Ordering::SeqCst) {
            Err(object_store::Error::Generic {
                store: "FailableStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "simulated failure",
                )),
            })
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Display for FailableStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailableStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FailableStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        self.check_put()?;
        self.inner.put(location, payload).await
    }

    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.check_put()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, opts).await
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<u64>) -> object_store::Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// =====================================================================
// Regression: Tracking leak on InsufficientWrites (FIXED).
//
// Previously, when complete() hit InsufficientWrites after the primary
// succeeded, the tracking entry was NOT removed. A subsequent purge
// treated it as an abandoned upload and DELETEd committed data.
//
// Fix: complete() now calls multipart_registry.remove() immediately
// after the primary commit, before replication begins.
//
// We use FailableStore wrappers on replicas. The primary is a real
// raw store. After put_multipart captures the stores, we toggle the
// replicas to fail. Then complete() succeeds on the primary but
// fails to replicate, returning InsufficientWrites.
// =====================================================================

#[test]
fn bug1_tracking_leak_on_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    // Create 3 raw stores as the underlying backends.
    let raw0 = format_shard(&dir.path().join("s0.raw"), SHARD_SIZE);
    let raw1 = format_shard(&dir.path().join("s1.raw"), SHARD_SIZE);
    let raw2 = format_shard(&dir.path().join("s2.raw"), SHARD_SIZE);

    // Wrap replicas in FailableStore.
    let fail1 = Arc::new(FailableStore::new(raw1.clone() as Arc<dyn ObjectStore>));
    let fail2 = Arc::new(FailableStore::new(raw2.clone() as Arc<dyn ObjectStore>));

    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raw0.clone() as Arc<dyn ObjectStore>,
        fail1.clone() as Arc<dyn ObjectStore>,
        fail2.clone() as Arc<dyn ObjectStore>,
    ];
    // RF=3, min_writes=3 so all shards must succeed.
    let cluster = ShardedObjectStore::new(stores, 3)
        .with_min_writes(3)
        .with_multipart_expiry(std::time::Duration::from_millis(1));

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("bug1/leak.bin");

        // Create multipart while replicas are healthy.
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        assert_eq!(cluster.multipart_upload_count(), 1);

        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xBB; 4096])))
            .await
            .unwrap();

        // NOW toggle replicas to fail puts. The primary's multipart
        // handle is already open and parts are spooled. On complete(),
        // the primary assembles successfully, but replication to
        // fail1/fail2 will fail.
        fail1.set_fail_puts(true);
        fail2.set_fail_puts(true);

        // complete() should fail with InsufficientWrites (1 placed, 3 needed).
        let result = upload.complete().await;
        assert!(
            result.is_err(),
            "complete() should fail: only primary succeeded, need 3"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("InsufficientWrites") || err_msg.contains("insufficient"),
            "error should be InsufficientWrites, got: {err_msg}"
        );

        // Regression: tracking entry must be removed before replication,
        // so it is already gone even when replication fails.
        assert_eq!(
            cluster.multipart_upload_count(),
            0,
            "tracking entry should be deregistered before replication"
        );

        // Verify purge finds nothing stale (tracking was cleaned up).
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let purged = cluster.purge_stale_multiparts().await;
        assert_eq!(
            purged, 0,
            "purge must not find stale entries -- data loss risk!"
        );
    });

    flush_all(&[raw0, raw1, raw2]);
}

// =====================================================================
// Regression: Orphan cleanup after InsufficientWrites (FIXED).
//
// Previously, after InsufficientWrites the primary shard had committed
// data but no catalog entry -- an unreachable orphan.
//
// Fix: complete() now deletes orphaned data from all shards that
// succeeded (including the primary) when min_writes is not met,
// mirroring write_to_shards_inner().
// =====================================================================

#[test]
fn bug2_orphan_data_after_insufficient_writes() {
    let dir = tempfile::tempdir().unwrap();
    let raw0 = format_shard(&dir.path().join("s0.raw"), SHARD_SIZE);
    let raw1 = format_shard(&dir.path().join("s1.raw"), SHARD_SIZE);
    let raw2 = format_shard(&dir.path().join("s2.raw"), SHARD_SIZE);

    let fail1 = Arc::new(FailableStore::new(raw1.clone() as Arc<dyn ObjectStore>));
    let fail2 = Arc::new(FailableStore::new(raw2.clone() as Arc<dyn ObjectStore>));

    let stores: Vec<Arc<dyn ObjectStore>> = vec![
        raw0.clone() as Arc<dyn ObjectStore>,
        fail1.clone() as Arc<dyn ObjectStore>,
        fail2.clone() as Arc<dyn ObjectStore>,
    ];
    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(3);

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("bug2/orphan.bin");

        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xDD; 4096])))
            .await
            .unwrap();

        // Fail replicas so complete returns InsufficientWrites.
        fail1.set_fail_puts(true);
        fail2.set_fail_puts(true);

        let result = upload.complete().await;
        assert!(result.is_err(), "should fail with InsufficientWrites");

        // No catalog entry should exist.
        let entry = cluster.placement("bug2/orphan.bin");
        assert!(entry.is_none(), "no catalog entry after failed complete");

        // Regression: orphaned data on the primary must be cleaned up.
        let orphan = raw0.get(&key).await;
        assert!(
            orphan.is_err(),
            "primary shard data should be deleted after InsufficientWrites"
        );
    });

    flush_all(&[raw0, raw1, raw2]);
}

// =====================================================================
// Expected behavior: multipart overwrite clears metadata.
//
// The ObjectStore trait's put_multipart() has no mechanism to attach
// metadata. The assembled blob on the primary shard is a fresh write
// with no metadata. Therefore after a multipart overwrite, meta_len=0
// in the catalog is correct -- the old metadata from a prior
// put_with_meta is gone because the key was overwritten with new data
// that has no metadata.
//
// This is NOT a bug. It is expected behavior.
// =====================================================================

#[test]
fn bug3_metadata_not_preserved_on_replicas() {
    let dir = tempfile::tempdir().unwrap();
    let shard_size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("s0.raw"), shard_size);
    let s1 = format_shard(&dir.path().join("s1.raw"), shard_size);
    let s2 = format_shard(&dir.path().join("s2.raw"), shard_size);
    let raws = vec![s0, s1, s2];
    let cluster = build_cluster(&raws, 2);

    let refs1: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let registry = RawRefRegistry::new(refs1, vec![ShardKind::Raw; 3]);

    let refs2: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    cluster.set_raw_refs(Arc::new(RawRefRegistry::new(refs2, vec![ShardKind::Raw; 3])));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Step 1: Write with metadata via metadata-aware path.
    rt.block_on(async {
        let key = Path::from("bug3/meta.bin");
        let payload = Bytes::from(vec![0xAAu8; 4096]);
        let meta = b"content-type:text/plain";

        put_with_meta(&cluster, &registry, &key, payload, meta)
            .await
            .unwrap();
    });
    flush_all(&raws);

    // Verify metadata exists on all replicas.
    rt.block_on(async {
        let key = Path::from("bug3/meta.bin");
        let entry = cluster.placement("bug3/meta.bin").unwrap();
        assert!(entry.shards.len() >= 2);
        for &sid in &entry.shards {
            let meta = raws[sid].get_metadata(&key);
            assert!(
                meta.is_ok(),
                "shard {sid} missing metadata after put_with_meta"
            );
        }
        // meta_len should be non-zero in catalog.
        assert!(
            entry.meta_len > 0,
            "catalog meta_len should be >0 after put_with_meta"
        );
    });

    // Step 2: Overwrite via multipart upload.
    rt.block_on(async {
        let key = Path::from("bug3/meta.bin");
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xCC; 4096])))
            .await
            .unwrap();
        upload.complete().await.unwrap();
    });
    flush_all(&raws);

    // Step 3: After multipart overwrite, catalog meta_len is 0.
    // This is correct: put_multipart() has no metadata argument, so
    // the assembled object has no metadata attached.
    rt.block_on(async {
        let entry = cluster.placement("bug3/meta.bin").unwrap();
        // The catalog reflects the new multipart data which has no metadata.
        assert_eq!(
            entry.meta_len, 0,
            "catalog meta_len should be 0 after multipart overwrite (no metadata mechanism)"
        );

        // Body should be correct on all replicas.
        let key = Path::from("bug3/meta.bin");
        for &sid in &entry.shards {
            let store = cluster.shard_store(sid).unwrap();
            let data = store.get(&key).await.unwrap().bytes().await.unwrap();
            assert!(
                data.iter().all(|&b| b == 0xCC),
                "shard {sid} body should be new multipart data"
            );
        }
    });

    flush_all(&raws);
}

// =====================================================================
// Baseline happy-path tests (should always pass).
// =====================================================================

#[test]
fn happy_path_multipart_tracking_cleared() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2)
        .with_multipart_expiry(std::time::Duration::from_millis(1));
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("happy/mp.bin");
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xAA; 4096])))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        assert_eq!(cluster.multipart_upload_count(), 0);
        let entry = cluster.placement("happy/mp.bin").unwrap();
        assert!(entry.crc32c.is_some());
        assert_eq!(entry.size, 4096);
        assert_eq!(entry.shards.len(), 2);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(cluster.purge_stale_multiparts().await, 0);

        let data = cluster.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len(), 4096);
    });

    flush_all(&raws);
}

#[test]
fn happy_path_single_shard_multipart() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..1)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("single/mp.bin");
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xBB; 4096])))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        assert_eq!(cluster.multipart_upload_count(), 0);
        let entry = cluster.placement("single/mp.bin").unwrap();
        assert!(entry.crc32c.is_some());
        assert_eq!(entry.size, 4096);

        let data = cluster.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len(), 4096);
    });

    flush_all(&raws);
}
