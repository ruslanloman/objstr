//! Regression tests for metadata body-only semantics at the sharded layer.
//!
//! Verifies that ShardedObjectStore (via ObjectStore trait) and the
//! metadata module correctly expose body-only sizes after put_with_meta,
//! and that catalog entries record the correct meta_len.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::metadata::{
    get_metadata, head_with_meta, put_with_meta, RawRefRegistry, ShardKind,
};
use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

// -- Helpers ---------------------------------------------------------

fn setup(
    dir: &tempfile::TempDir,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>, RawRefRegistry) {
    let size: u64 = 64 * 1024 * 1024;
    let s0 = format_shard(&dir.path().join("s0.raw"), size);
    let s1 = format_shard(&dir.path().join("s1.raw"), size);
    let raws = vec![s0, s1];
    let cluster = build_cluster(&raws, 2);

    let refs: Vec<Option<Arc<RawObjectStore>>> =
        raws.iter().map(|r| Some(Arc::clone(r))).collect();
    let kinds = vec![ShardKind::Raw; 2];
    let registry = RawRefRegistry::new(refs, kinds);

    (cluster, raws, registry)
}

// -- catalog meta_len ------------------------------------------------

#[test]
fn catalog_records_meta_len_after_put_with_meta() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = b"catalog-meta-test";
    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("cattest/obj.bin"),
            Bytes::from(vec![0xAAu8; 2048]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let entry = cluster.placement("cattest/obj.bin").unwrap();
    assert_eq!(
        entry.meta_len,
        meta.len() as u16,
        "catalog entry must record the metadata length"
    );
}

#[test]
fn catalog_meta_len_zero_for_plain_put() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, _registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        cluster
            .put(
                &Path::from("plain/obj.bin"),
                object_store::PutPayload::from(Bytes::from(vec![0xBBu8; 512])),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);

    let entry = cluster.placement("plain/obj.bin").unwrap();
    assert_eq!(entry.meta_len, 0, "plain put should have meta_len 0");
}

// -- get() returns body-only through sharded layer --------------------

#[test]
fn sharded_get_returns_body_only() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let body = vec![0xCCu8; 4096];
    let meta = b"should-not-appear-in-get";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("gettest/obj.bin"),
            Bytes::from(body.clone()),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let got = rt.block_on(async {
        cluster
            .get(&Path::from("gettest/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });

    assert_eq!(
        got.len(),
        body.len(),
        "get() through sharded layer must return body-only"
    );
    assert_eq!(got.as_ref(), body.as_slice());
}

// -- head() reports body-only through sharded layer -------------------

#[test]
fn sharded_head_reports_body_only_size() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let body_len = 3000usize;
    let meta = b"head-meta-test-data";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("headtest/obj.bin"),
            Bytes::from(vec![0xDDu8; body_len]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let obj_meta = rt.block_on(async {
        cluster.head(&Path::from("headtest/obj.bin")).await.unwrap()
    });

    assert_eq!(
        obj_meta.size, body_len as u64,
        "head() size must be body-only through sharded layer"
    );
}

// -- head_with_meta reports body-only size + correct meta_len ---------

#[test]
fn sharded_head_with_meta_body_only_size() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let body_len = 2048usize;
    let meta = b"hwm-regression";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("hwm/obj.bin"),
            Bytes::from(vec![0xEEu8; body_len]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let (obj_meta, meta_len) = rt.block_on(async {
        head_with_meta(&cluster, &registry, &Path::from("hwm/obj.bin"))
            .await
            .unwrap()
    });

    assert_eq!(
        obj_meta.size, body_len as u64,
        "head_with_meta size must be body-only"
    );
    assert_eq!(meta_len as usize, meta.len());
}

// -- list() reports body-only size through sharded layer --------------

#[test]
fn sharded_list_reports_body_only_size() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let body_len = 1500usize;
    let meta = b"list-meta";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("listtest/obj.bin"),
            Bytes::from(vec![0xFFu8; body_len]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let items = rt.block_on(async {
        cluster
            .list(Some(&Path::from("listtest")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    });

    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].size, body_len as u64,
        "list() size must be body-only through sharded layer"
    );
}

// -- get_metadata still returns correct metadata ----------------------

#[test]
fn sharded_get_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = b"metadata-roundtrip-test";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("gmrt/obj.bin"),
            Bytes::from(vec![0x11u8; 1024]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let got = rt.block_on(async {
        get_metadata(&cluster, &registry, &Path::from("gmrt/obj.bin"))
            .await
            .unwrap()
    });

    assert_eq!(got.as_ref(), meta, "get_metadata must return exact metadata bytes");
}

// -- head/get consistency: sizes match --------------------------------

#[test]
fn sharded_head_and_get_sizes_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let body_len = 7777usize;
    let meta = b"consistency-test";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("cons/obj.bin"),
            Bytes::from(vec![0x22u8; body_len]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let (head_size, get_len) = rt.block_on(async {
        let head = cluster.head(&Path::from("cons/obj.bin")).await.unwrap();
        let get = cluster
            .get(&Path::from("cons/obj.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        (head.size as usize, get.len())
    });

    assert_eq!(head_size, body_len);
    assert_eq!(get_len, body_len);
    assert_eq!(head_size, get_len, "head size and get length must match");
}

// -- rebuild_catalog preserves meta_len when raw_refs are set ---------

#[test]
fn rebuild_catalog_preserves_meta_len() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = b"rebuild-meta-test-data";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("rebuild/obj.bin"),
            Bytes::from(vec![0xAA; 2048]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    // Verify catalog has meta_len before rebuild.
    let entry_before = cluster.placement("rebuild/obj.bin").unwrap();
    assert_eq!(
        entry_before.meta_len,
        meta.len() as u16,
        "meta_len should be set before rebuild"
    );

    // Set raw_refs on the cluster so rebuild can discover meta_len.
    cluster.set_raw_refs(std::sync::Arc::new(registry));

    // Rebuild catalog from scratch.
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    // After rebuild, meta_len should be preserved.
    let entry_after = cluster.placement("rebuild/obj.bin").unwrap();
    assert_eq!(
        entry_after.meta_len,
        meta.len() as u16,
        "meta_len should be preserved after rebuild_catalog with raw_refs"
    );

    // Body size should still be body-only.
    assert_eq!(entry_after.size, 2048, "size should be body-only after rebuild");
}

#[test]
fn rebuild_catalog_for_shard_preserves_meta_len() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws, registry) = setup(&dir);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let meta = b"per-shard-rebuild";

    rt.block_on(async {
        put_with_meta(
            &cluster,
            &registry,
            &Path::from("rshard/obj.bin"),
            Bytes::from(vec![0xBB; 1024]),
            meta,
        )
        .await
        .unwrap();
    });
    flush_all(&raws);

    let entry_before = cluster.placement("rshard/obj.bin").unwrap();
    let target_shard = entry_before.shards[0];

    cluster.set_raw_refs(std::sync::Arc::new(registry));

    // Rebuild just one shard.
    rt.block_on(cluster.rebuild_catalog_for_shard(target_shard)).unwrap();

    let entry_after = cluster.placement("rshard/obj.bin").unwrap();
    assert_eq!(
        entry_after.meta_len,
        meta.len() as u16,
        "meta_len should be preserved after rebuild_catalog_for_shard"
    );
}
