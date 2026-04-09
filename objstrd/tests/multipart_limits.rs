//! Multipart upload resource-exhaustion and limit tests.
//!
//! These tests verify that the server enforces limits on multipart uploads
//! to prevent denial-of-service via memory or disk exhaustion.

mod common;
use common::{complete_xml, extract_xml_tag, TestServer};

/// The server should reject new uploads once the concurrent upload cap
/// (MAX_CONCURRENT_UPLOADS = 1000) is reached.
#[tokio::test]
async fn test_concurrent_upload_cap() {
    let srv = TestServer::start().await;

    // Open uploads until we hit the cap.
    let mut upload_ids = Vec::new();
    for i in 0..1001 {
        let url = format!("{}?uploads", srv.object_url(&format!("oom/cap-{i}.bin")));
        let resp = srv.client.post(&url).send().await.unwrap();
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap();

        if status == 200 {
            let uid = extract_xml_tag(&body, "UploadId").unwrap().to_string();
            upload_ids.push((i, uid));
        } else {
            // Should be rejected at upload 1001 (index 1000) with 503 SlowDown.
            assert!(
                status == 503 || status == 429,
                "Expected 503 SlowDown at upload {i}, got {status}: {body}"
            );
            assert!(
                i >= 1000,
                "Rejected too early at upload {i} (expected at 1000)"
            );
            // Clean up: abort all uploads so the server can release resources.
            for (idx, uid) in &upload_ids {
                let url = format!(
                    "{}?uploadId={}",
                    srv.object_url(&format!("oom/cap-{idx}.bin")),
                    uid
                );
                let _ = srv.client.delete(&url).send().await;
            }
            return;
        }
    }

    // Clean up even if we somehow got all 1001 (should not happen).
    for (idx, uid) in &upload_ids {
        let url = format!(
            "{}?uploadId={}",
            srv.object_url(&format!("oom/cap-{idx}.bin")),
            uid
        );
        let _ = srv.client.delete(&url).send().await;
    }
    panic!("Server accepted all 1001 uploads without enforcing the cap");
}

/// Part numbers must be in 1..=10000. The server should reject out-of-range
/// part numbers rather than silently accepting unbounded part registrations.
#[tokio::test]
async fn test_part_number_bounds() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("oom/pnum.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Part 0 should be rejected.
    let url = format!(
        "{}?partNumber=0&uploadId={}",
        srv.object_url("oom/pnum.bin"),
        upload_id
    );
    let resp = srv.client.put(&url).body("data").send().await.unwrap();
    assert!(
        resp.status().is_client_error(),
        "Part 0 should be rejected, got {}",
        resp.status()
    );

    // Part 10001 should be rejected.
    let url = format!(
        "{}?partNumber=10001&uploadId={}",
        srv.object_url("oom/pnum.bin"),
        upload_id
    );
    let resp = srv.client.put(&url).body("data").send().await.unwrap();
    assert!(
        resp.status().is_client_error(),
        "Part 10001 should be rejected, got {}",
        resp.status()
    );

    // Part 10000 should be accepted.
    let url = format!(
        "{}?partNumber=10000&uploadId={}",
        srv.object_url("oom/pnum.bin"),
        upload_id
    );
    let resp = srv.client.put(&url).body("ok").send().await.unwrap();
    assert_eq!(resp.status(), 200, "Part 10000 should be accepted");

    // Clean up.
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("oom/pnum.bin"),
        upload_id
    );
    let _ = srv.client.delete(&url).send().await;
}

/// upload_part_copy streams the source object to a temp file on disk (same
/// pattern as upload_part). This test verifies the copy path works end-to-end
/// with a modest 1 MB object.
#[tokio::test]
async fn test_upload_part_copy_buffers_in_memory() {
    let srv = TestServer::start().await;

    // Create a 1 MB source object.
    let src_data: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    let resp = srv
        .client
        .put(&srv.object_url("oom/copy-src.bin"))
        .body(src_data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Initiate multipart upload.
    let url = format!("{}?uploads", srv.object_url("oom/copy-dst.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Copy the source as part 1 via x-amz-copy-source.
    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("oom/copy-dst.bin"),
        upload_id
    );
    let copy_source = format!("/{}/oom/copy-src.bin", srv.bucket);
    let resp = srv
        .client
        .put(&url)
        .header("x-amz-copy-source", &copy_source)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "upload_part_copy should succeed: {}",
        body
    );

    // Extract ETag from the XML response (CopyPartResult).
    let etag = extract_xml_tag(&body, "ETag")
        .expect("CopyPartResult should contain ETag")
        .to_string();

    // Complete the upload.
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("oom/copy-dst.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify content matches source.
    let resp = srv
        .client
        .get(&srv.object_url("oom/copy-dst.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result = resp.bytes().await.unwrap();
    assert_eq!(result.len(), src_data.len());
    assert_eq!(&result[..], &src_data[..]);
}

/// Many parts on a single upload: the HashMap<i32, PartMeta> grows per-part.
/// This test registers a moderate number of parts (100) to verify the path,
/// but documents that 10,000 parts x 1000 uploads = ~10M PartMeta entries
/// could use significant memory.
#[tokio::test]
async fn test_many_parts_metadata_pressure() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("oom/manyparts.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    let mut etags = Vec::new();
    for i in 1..=100 {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url("oom/manyparts.bin"),
            i,
            upload_id
        );
        let resp = srv.client.put(&url).body("x").send().await.unwrap();
        assert_eq!(resp.status(), 200, "Part {i} should succeed");
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        etags.push((i as u32, etag));
    }

    // Complete with all 100 parts.
    let parts: Vec<(u32, &str)> = etags.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("oom/manyparts.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&parts))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify assembled object: 100 x "x" = 100 bytes.
    let resp = srv
        .client
        .get(&srv.object_url("oom/manyparts.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 100);
    assert!(body.iter().all(|&b| b == b'x'));
}

/// Aborting an upload should free all temp files and metadata.
/// This is a regression check that cleanup works under load.
#[tokio::test]
async fn test_abort_frees_resources() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("oom/abort-free.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload 10 parts of 4 KB each.
    let part_data = vec![0xABu8; 4096];
    for i in 1..=10 {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url("oom/abort-free.bin"),
            i,
            upload_id
        );
        let resp = srv
            .client
            .put(&url)
            .body(part_data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Abort should succeed.
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("oom/abort-free.bin"),
        upload_id
    );
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // After abort, a new upload should succeed (slot freed).
    let url = format!("{}?uploads", srv.object_url("oom/abort-free2.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

/// Upload to a nonexistent upload ID should fail cleanly.
#[tokio::test]
async fn test_upload_part_bad_upload_id() {
    let srv = TestServer::start().await;

    let url = format!(
        "{}?partNumber=1&uploadId=does-not-exist",
        srv.object_url("oom/ghost.bin")
    );
    let resp = srv
        .client
        .put(&url)
        .body("dangling")
        .send()
        .await
        .unwrap();
    // Should fail with NoSuchUpload (404).
    assert_eq!(
        resp.status(),
        404,
        "Upload part to nonexistent upload should return 404"
    );
}

// ===========================================================================
// Multipart upload staleness / lazy reaper
// ===========================================================================

/// Stale multipart uploads should be purged by the lazy reaper when a new
/// upload is created.  We set a very short expiry (1 second) so we can
/// verify the reaper fires without waiting 24 hours.
#[tokio::test]
async fn test_stale_upload_purged() {
    // Start a server whose upload expiry is just 1 second.
    let srv = TestServer::start_with_upload_expiry(std::time::Duration::from_secs(1)).await;

    // Create a multipart upload and note its ID.
    let init_url = format!("{}?uploads", srv.object_url("exp/stale.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let stale_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload a part so we know it was real.
    let part_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("exp/stale.bin"),
        stale_id
    );
    let resp = srv.client.put(&part_url).body("data").send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Wait for the upload to become stale (>1 second).
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Create another upload -- this should trigger the lazy reaper.
    let init_url2 = format!("{}?uploads", srv.object_url("exp/fresh.bin"));
    let resp = srv.client.post(&init_url2).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Listing multipart uploads should NOT include the stale upload.
    let list_url = format!("{}?uploads", srv.bucket_url());
    let resp = srv.client.get(&list_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains(&stale_id),
        "Stale upload {} should have been purged by the lazy reaper. Body: {}",
        stale_id,
        body
    );

    // Trying to upload a part to the stale upload should fail with NoSuchUpload.
    let part_url2 = format!(
        "{}?partNumber=2&uploadId={}",
        srv.object_url("exp/stale.bin"),
        stale_id
    );
    let resp = srv.client.put(&part_url2).body("more").send().await.unwrap();
    assert_eq!(
        resp.status(),
        404,
        "Uploading to a purged upload should return 404 NoSuchUpload"
    );
}
