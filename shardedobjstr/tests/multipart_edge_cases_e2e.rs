//! Edge-case end-to-end tests for multipart uploads in ShardedObjectStore.
//!
//! Covers: abort-then-reupload same key, multipart with shard going
//! offline mid-upload, and multipart tracking consistency.

mod common;

use bytes::Bytes;
use object_store::{path::Path, MultipartUpload, ObjectStore, PutMultipartOptions, PutPayload};

use common::{build_cluster, flush_all, format_shard};

const SHARD_SIZE: u64 = 64 * 1024 * 1024;

// =====================================================================
// Abort then re-upload same key
// =====================================================================

#[test]
fn multipart_abort_then_reupload_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("mp_abort/file.bin");

        // Start a multipart upload, write a part, then abort
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xAA; 4096])))
            .await
            .unwrap();
        upload.abort().await.unwrap();

        // Tracking should be cleared
        assert_eq!(
            cluster.multipart_upload_count(),
            0,
            "upload count should be 0 after abort"
        );

        // Object should not exist
        let get_result = cluster.get(&key).await;
        assert!(
            get_result.is_err(),
            "aborted upload should not create an object"
        );

        // Re-upload same key -- should succeed
        let mut upload2 = cluster.put_multipart(&key).await.unwrap();
        upload2
            .put_part(PutPayload::from(Bytes::from(vec![0xBB; 4096])))
            .await
            .unwrap();
        upload2.complete().await.unwrap();

        // Object should now exist with new content
        let data = cluster
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
        assert!(
            data.iter().all(|&b| b == 0xBB),
            "re-uploaded data should be 0xBB, not 0xAA from aborted upload"
        );
    });

    flush_all(&raws);
}

// =====================================================================
// Multiple abort cycles
// =====================================================================

#[test]
fn multipart_multiple_abort_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("mp_cycles/file.bin");

        // Abort three times in a row
        for round in 0..3u8 {
            let mut upload = cluster.put_multipart(&key).await.unwrap();
            upload
                .put_part(PutPayload::from(Bytes::from(vec![round + 1; 4096])))
                .await
                .unwrap();
            upload.abort().await.unwrap();
            assert_eq!(cluster.multipart_upload_count(), 0);
        }

        // Object should not exist after all aborts
        assert!(cluster.get(&key).await.is_err());

        // Final successful upload
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xFF; 4096])))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        let data = cluster
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(data.iter().all(|&b| b == 0xFF));
    });

    flush_all(&raws);
}

// =====================================================================
// Multipart with shard going offline before complete
// =====================================================================

#[test]
fn multipart_shard_offline_before_complete() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..4)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("mp_offline/file.bin");

        // Start multipart
        let mut upload = cluster.put_multipart(&key).await.unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xCC; 4096])))
            .await
            .unwrap();

        // Detach shard 0 before complete -- simulates a disk failure
        cluster.detach_shard(0);

        // Complete should still succeed (replication to remaining shards)
        let complete_result = upload.complete().await;
        assert!(
            complete_result.is_ok(),
            "multipart complete should succeed with 3/4 healthy shards"
        );

        // Object should be readable
        let data = cluster
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0xCC));
    });

    flush_all(&raws);
}

// =====================================================================
// Multipart multi-part assembly
// =====================================================================

#[test]
fn multipart_multiple_parts_assembled_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let key = Path::from("mp_multi/assembled.bin");

        let mut upload = cluster.put_multipart(&key).await.unwrap();

        // Write 3 distinct parts
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0x11; 4096])))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0x22; 4096])))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0x33; 4096])))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        // Read and verify all parts are present in order
        let data = cluster
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 3 * 4096, "should have all 3 parts");
        assert!(data[..4096].iter().all(|&b| b == 0x11), "part 1");
        assert!(data[4096..8192].iter().all(|&b| b == 0x22), "part 2");
        assert!(data[8192..].iter().all(|&b| b == 0x33), "part 3");

        // Should have correct replicas
        let entry = cluster.placement("mp_multi/assembled.bin").unwrap();
        assert_eq!(entry.shards.len(), 2, "should be on RF=2 shards");
    });

    flush_all(&raws);
}

// =====================================================================
// Multipart tracking: concurrent uploads tracked independently
// =====================================================================

#[test]
fn multipart_concurrent_uploads_tracked() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Start two concurrent uploads
        let mut up1 = cluster
            .put_multipart(&Path::from("mp_track/a.bin"))
            .await
            .unwrap();
        assert_eq!(cluster.multipart_upload_count(), 1);

        let mut up2 = cluster
            .put_multipart(&Path::from("mp_track/b.bin"))
            .await
            .unwrap();
        assert_eq!(cluster.multipart_upload_count(), 2);

        // Complete first, abort second
        up1.put_part(PutPayload::from(Bytes::from(vec![0xAA; 4096])))
            .await
            .unwrap();
        up1.complete().await.unwrap();
        assert_eq!(cluster.multipart_upload_count(), 1);

        up2.put_part(PutPayload::from(Bytes::from(vec![0xBB; 4096])))
            .await
            .unwrap();
        up2.abort().await.unwrap();
        assert_eq!(cluster.multipart_upload_count(), 0);

        // Only first should exist
        assert!(cluster.get(&Path::from("mp_track/a.bin")).await.is_ok());
        assert!(cluster.get(&Path::from("mp_track/b.bin")).await.is_err());
    });

    flush_all(&raws);
}

// =====================================================================
// put_multipart_opts with explicit PutMultipartOptions
// =====================================================================

#[test]
fn put_multipart_opts_completes_successfully() {
    let dir = tempfile::tempdir().unwrap();
    let raws: Vec<_> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("mp{i}.raw")), SHARD_SIZE))
        .collect();
    let cluster = build_cluster(&raws, 2);
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let opts = PutMultipartOptions::default();
        let mut upload = cluster
            .put_multipart_opts(&Path::from("mpo/test.bin"), opts)
            .await
            .unwrap();

        assert_eq!(cluster.multipart_upload_count(), 1);

        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xEE; 4096])))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from(Bytes::from(vec![0xFF; 2048])))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        assert_eq!(cluster.multipart_upload_count(), 0);

        // Verify data.
        let data = cluster
            .get(&Path::from("mpo/test.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096 + 2048);
    });
    flush_all(&raws);
}
