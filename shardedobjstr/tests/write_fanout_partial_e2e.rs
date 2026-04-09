//! Tests for write fan-out partial failure behavior.
//!
//! These tests verify that when SOME (not all) target shards fail during
//! a PUT, the write still succeeds if quorum is met, data is accessible,
//! and failing shards are marked Degraded.  Also tests the
//! InsufficientWrites error path when quorum is not met.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::{ShardHealth, ShardedObjectStore};

use common::{build_cluster, flush_all, format_shard};

// -- FailingPutStore: wraps a real store but always fails on put ------

/// A store that fails all put operations but delegates reads to a real
/// backend.  Used to simulate a shard that becomes write-impaired.
#[derive(Debug)]
struct FailingPutStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for FailingPutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingPutStore({})", self.inner)
    }
}

impl FailingPutStore {
    fn put_err() -> object_store::Error {
        object_store::Error::Generic {
            store: "FailingPutStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "simulated write failure",
            )),
        }
    }
}

#[async_trait]
impl ObjectStore for FailingPutStore {
    async fn put(&self, _: &Path, _: PutPayload) -> object_store::Result<PutResult> {
        Err(Self::put_err())
    }
    async fn put_opts(
        &self, _: &Path, _: PutPayload, _: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(Self::put_err())
    }
    async fn put_multipart(
        &self, _: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(Self::put_err())
    }
    async fn put_multipart_opts(
        &self, _: &Path, _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(Self::put_err())
    }
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }
    async fn get_opts(
        &self, location: &Path, options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn get_range(
        &self, location: &Path, range: std::ops::Range<u64>,
    ) -> object_store::Result<Bytes> {
        self.inner.get_range(location, range).await
    }
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }
    fn list(
        &self, prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self, prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self, from: &Path, to: &Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// -- Helpers ----------------------------------------------------------

/// Build a cluster where shard `failing_id` uses FailingPutStore.
fn build_cluster_with_failing_shard(
    dir: &tempfile::TempDir,
    shard_count: usize,
    rf: usize,
    failing_id: usize,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..shard_count)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();

    let stores: Vec<Arc<dyn ObjectStore>> = raws
        .iter()
        .enumerate()
        .map(|(i, raw)| {
            if i == failing_id {
                Arc::new(FailingPutStore {
                    inner: raw.clone() as Arc<dyn ObjectStore>,
                }) as Arc<dyn ObjectStore>
            } else {
                raw.clone() as Arc<dyn ObjectStore>
            }
        })
        .collect();

    let cluster = ShardedObjectStore::new(stores, rf);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    (cluster, raws)
}

// =====================================================================
// Test 1: Partial failure still succeeds when quorum met (rf=2, mw=1)
// =====================================================================

#[test]
fn partial_write_succeeds_when_quorum_met() {
    let dir = tempfile::tempdir().unwrap();
    // 3 shards, rf=2, min_writes = max(1, 2-1) = 1
    // Shard 0 fails puts. Any key targeting shards [0,X] will have
    // 1 success + 1 failure, which meets min_writes=1.
    let (cluster, raws) = build_cluster_with_failing_shard(&dir, 3, 2, 0);
    let rt = tokio::runtime::Runtime::new().unwrap();

    assert_eq!(cluster.min_writes(), 1);

    // Find a key that targets shard 0 (the failing shard) as one of its targets.
    let mut key_on_failing = None;
    for i in 0..1000 {
        let k = Path::from(format!("partial/obj_{:04}", i));
        let targets = cluster.target_shards(&k);
        if targets.contains(&0) {
            key_on_failing = Some((k, targets));
            break;
        }
    }
    let (key, targets) = key_on_failing.expect("should find a key targeting shard 0");
    assert!(targets.contains(&0), "key should target failing shard");

    // Put should succeed because at least 1 replica (on a healthy shard) lands.
    rt.block_on(async {
        let data = Bytes::from(vec![0xAA; 1024]);
        let result = cluster.put(&key, PutPayload::from(data)).await;
        assert!(
            result.is_ok(),
            "put should succeed with partial failure when quorum met: {:?}",
            result.err(),
        );
    });

    // The failing shard should be marked Degraded.
    assert_eq!(
        cluster.shard_health(0),
        Some(ShardHealth::Degraded),
        "failing shard should be marked Degraded after partial write failure",
    );

    // The successful shard should still be Healthy.
    let healthy_target = targets.iter().find(|&&t| t != 0).unwrap();
    assert_eq!(
        cluster.shard_health(*healthy_target),
        Some(ShardHealth::Healthy),
        "successful shard should remain Healthy",
    );

    // Object should be readable from the healthy shard.
    flush_all(&raws);
    rt.block_on(async {
        let result = cluster.get(&key).await;
        assert!(result.is_ok(), "object should be readable after partial write");
        let body = result.unwrap().bytes().await.unwrap();
        assert_eq!(body.len(), 1024);
    });
}

// =====================================================================
// Test 2: Partial failure data is accessible after write
// =====================================================================

#[test]
fn partial_write_data_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = build_cluster_with_failing_shard(&dir, 3, 2, 0);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write multiple objects; some will target shard 0, some will not.
    let mut keys_written = Vec::new();
    rt.block_on(async {
        for i in 0..20 {
            let key = Path::from(format!("integrity/obj_{:04}", i));
            let data = Bytes::from(format!("data-for-object-{}", i).into_bytes());
            let result = cluster.put(&key, PutPayload::from(data)).await;
            assert!(result.is_ok(), "all puts should succeed (quorum=1)");
            keys_written.push(format!("integrity/obj_{:04}", i));
        }
    });

    flush_all(&raws);

    // Verify every object is readable with correct content.
    rt.block_on(async {
        for (i, key_str) in keys_written.iter().enumerate() {
            let key = Path::from(key_str.as_str());
            let result = cluster.get(&key).await;
            assert!(result.is_ok(), "object {} should be readable", key_str);
            let body = result.unwrap().bytes().await.unwrap();
            let expected = format!("data-for-object-{}", i);
            assert_eq!(
                body.as_ref(),
                expected.as_bytes(),
                "data mismatch for {}",
                key_str,
            );
        }
    });
}

// =====================================================================
// Test 3: All targets fail triggers retry on fresh targets
// =====================================================================

#[test]
fn all_targets_fail_triggers_retry() {
    let dir = tempfile::tempdir().unwrap();
    // 4 shards, rf=2, shards 0 and 1 are failing.
    // A key targeting [0,1] will have all initial targets fail,
    // triggering retry on shards 2 and 3.
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();

    let stores: Vec<Arc<dyn ObjectStore>> = raws
        .iter()
        .enumerate()
        .map(|(i, raw)| {
            if i < 2 {
                // Shards 0 and 1 fail puts.
                Arc::new(FailingPutStore {
                    inner: raw.clone() as Arc<dyn ObjectStore>,
                }) as Arc<dyn ObjectStore>
            } else {
                raw.clone() as Arc<dyn ObjectStore>
            }
        })
        .collect();

    let cluster = ShardedObjectStore::new(stores, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // Find a key whose initial targets are both on failing shards [0,1].
    let mut key_both_failing = None;
    for i in 0..10000 {
        let k = Path::from(format!("retry/obj_{:05}", i));
        let targets = cluster.target_shards(&k);
        if targets.len() == 2 && targets.contains(&0) && targets.contains(&1) {
            key_both_failing = Some(k);
            break;
        }
    }
    let key = key_both_failing.expect("should find a key targeting shards 0 and 1");

    // After all-fail retry, shards 0 and 1 get marked Offline, and the
    // retry uses fresh targets (2 and/or 3).
    rt.block_on(async {
        let data = Bytes::from(vec![0xCC; 512]);
        let result = cluster.put(&key, PutPayload::from(data)).await;
        assert!(
            result.is_ok(),
            "put should succeed after retry on fresh targets: {:?}",
            result.err(),
        );
    });

    // Failing shards should now be Offline (all-fail marks Offline).
    assert_eq!(
        cluster.shard_health(0),
        Some(ShardHealth::Offline),
        "shard 0 should be Offline after all-targets-fail",
    );
    assert_eq!(
        cluster.shard_health(1),
        Some(ShardHealth::Offline),
        "shard 1 should be Offline after all-targets-fail",
    );

    // Data should be readable from surviving shards.
    flush_all(&raws);
    rt.block_on(async {
        let result = cluster.get(&key).await;
        assert!(result.is_ok(), "object should be readable after retry");
    });
}

// =====================================================================
// Test 4: InsufficientWrites when quorum not met
// =====================================================================

#[test]
fn insufficient_writes_when_quorum_not_met() {
    let dir = tempfile::tempdir().unwrap();
    // 3 shards, rf=3, min_writes = max(1, 3-1) = 2.
    // Shards 0 and 1 fail puts. Shard 2 works.
    // Any key gives targets [0,1,2]: only 1 success, need 2. Should fail.
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();

    let stores: Vec<Arc<dyn ObjectStore>> = raws
        .iter()
        .enumerate()
        .map(|(i, raw)| {
            if i < 2 {
                Arc::new(FailingPutStore {
                    inner: raw.clone() as Arc<dyn ObjectStore>,
                }) as Arc<dyn ObjectStore>
            } else {
                raw.clone() as Arc<dyn ObjectStore>
            }
        })
        .collect();

    let cluster = ShardedObjectStore::new(stores, 3);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // With rf=3 and 3 shards, min_writes = 2.
    assert_eq!(cluster.min_writes(), 2, "min_writes should be 2 for rf=3, 3 shards");

    // All keys target all 3 shards. Only shard 2 succeeds (1 < min_writes=2).
    rt.block_on(async {
        let key = Path::from("quorum/fail.bin");
        let data = Bytes::from(vec![0xDD; 256]);
        let result = cluster.put(&key, PutPayload::from(data)).await;
        assert!(
            result.is_err(),
            "put should fail when only 1/3 replicas succeed (min_writes=2)",
        );
        let err_str = format!("{}", result.unwrap_err());
        assert!(
            err_str.contains("InsufficientWrites") || err_str.contains("insufficient"),
            "error should be InsufficientWrites, got: {}",
            err_str,
        );
    });
}

// =====================================================================
// Test 5: with_min_writes(1) allows single replica success
// =====================================================================

#[test]
fn min_writes_1_allows_single_success() {
    let dir = tempfile::tempdir().unwrap();
    // 3 shards, rf=3, but override min_writes to 1.
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();

    let stores: Vec<Arc<dyn ObjectStore>> = raws
        .iter()
        .enumerate()
        .map(|(i, raw)| {
            if i < 2 {
                Arc::new(FailingPutStore {
                    inner: raw.clone() as Arc<dyn ObjectStore>,
                }) as Arc<dyn ObjectStore>
            } else {
                raw.clone() as Arc<dyn ObjectStore>
            }
        })
        .collect();

    let cluster = ShardedObjectStore::new(stores, 3).with_min_writes(1);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    assert_eq!(cluster.min_writes(), 1);

    // With min_writes=1, even 1/3 success is enough.
    rt.block_on(async {
        let key = Path::from("mw1/obj.bin");
        let data = Bytes::from(vec![0xEE; 512]);
        let result = cluster.put(&key, PutPayload::from(data)).await;
        assert!(
            result.is_ok(),
            "put should succeed with min_writes=1 even if 2/3 fail: {:?}",
            result.err(),
        );
    });

    flush_all(&raws);
    rt.block_on(async {
        let result = cluster.get(&Path::from("mw1/obj.bin")).await;
        assert!(result.is_ok(), "object should be readable");
    });
}

// =====================================================================
// Test 6: Degraded marking only affects failing shards
// =====================================================================

#[test]
fn degraded_marking_is_selective() {
    let dir = tempfile::tempdir().unwrap();
    // 4 shards, rf=2, shard 2 is the failing one.
    let (cluster, raws) = build_cluster_with_failing_shard(&dir, 4, 2, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // All shards start Healthy.
    for i in 0..4 {
        assert_eq!(cluster.shard_health(i), Some(ShardHealth::Healthy));
    }

    // Write objects; some will target shard 2, causing partial failures.
    rt.block_on(async {
        for i in 0..30 {
            let key = Path::from(format!("selective/obj_{:04}", i));
            let data = Bytes::from(vec![0xFF; 128]);
            let _ = cluster.put(&key, PutPayload::from(data)).await;
        }
    });

    flush_all(&raws);

    // Shard 2 should be Degraded (or Offline if all-fail retry kicked in).
    let h2 = cluster.shard_health(2).unwrap();
    assert!(
        h2 == ShardHealth::Degraded || h2 == ShardHealth::Offline,
        "failing shard 2 should be Degraded or Offline, got {:?}",
        h2,
    );

    // At least some non-failing shards should still be Healthy.
    let healthy_count = (0..4)
        .filter(|&i| i != 2)
        .filter(|&i| cluster.shard_health(i) == Some(ShardHealth::Healthy))
        .count();
    assert!(
        healthy_count >= 2,
        "at least 2 non-failing shards should remain Healthy (got {})",
        healthy_count,
    );
}

// =====================================================================
// Write-path retry when all initial targets are offline
// =====================================================================

fn setup_cluster(
    dir: &tempfile::TempDir,
    shard_count: usize,
    rf: usize,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..shard_count)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, rf);
    (cluster, raws)
}

#[test]
fn write_path_retries_on_all_targets_offline() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_cluster(&dir, 4, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Determine initial targets for our test key.
    let key = Path::from("retry-test/obj.bin");
    let targets = cluster.target_shards(&key);
    assert_eq!(targets.len(), 2, "RF=2 should give 2 targets");

    // Take those two targets offline.
    for &t in &targets {
        cluster.set_shard_health(t, ShardHealth::Offline);
    }

    // Normal put should succeed after retry on the remaining 2 shards.
    rt.block_on(async {
        let data = Bytes::from(vec![0xAA; 1024]);
        let result = cluster.put(&key, PutPayload::from(data)).await;
        assert!(
            result.is_ok(),
            "put should retry on fresh targets when initial targets fail: {result:?}"
        );
    });

    // Verify the object landed on the surviving shards (not the offline ones).
    flush_all(&raws);
    let entry = cluster.placement("retry-test/obj.bin");
    assert!(entry.is_some(), "object must be in catalog after retry-put");
    let placed_shards = entry.unwrap().shards;
    for &s in &placed_shards {
        assert!(
            !targets.contains(&s),
            "object should NOT be on originally-offline shard {s}"
        );
    }
}

#[test]
fn write_path_fails_when_no_targets_available() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_cluster(&dir, 2, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Take ALL shards offline -- no targets left even after retry.
    cluster.set_shard_health(0, ShardHealth::Offline);
    cluster.set_shard_health(1, ShardHealth::Offline);

    rt.block_on(async {
        let key = Path::from("fail-test/obj.bin");
        let data = Bytes::from(vec![0xBB; 512]);
        let result = cluster.put(&key, PutPayload::from(data)).await;
        assert!(result.is_err(), "put must fail when all shards are offline");
    });
}
