//! M3 Tests -- Multipart upload lifecycle.

mod common;
use common::{complete_xml, extract_xml_tag, TestServer};

// ===========================================================================
// Multipart upload
// ===========================================================================

/// Initiate a multipart upload and verify the response contains an UploadId.
#[tokio::test]
async fn test_create_multipart_upload() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("multi/init.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId");
    assert!(
        upload_id.is_some() && !upload_id.unwrap().is_empty(),
        "Should return a non-empty UploadId: {}",
        body
    );
}

/// Full multipart flow: initiate, upload 2 parts, complete, verify.
#[tokio::test]
async fn test_multipart_basic() {
    let srv = TestServer::start().await;

    // Initiate
    let url = format!("{}?uploads", srv.object_url("multi/basic.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload part 1
    let url1 = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("multi/basic.bin"),
        upload_id
    );
    let resp = srv.client.put(&url1).body("AAAA").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let etag1 = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    // Upload part 2
    let url2 = format!(
        "{}?partNumber=2&uploadId={}",
        srv.object_url("multi/basic.bin"),
        upload_id
    );
    let resp = srv.client.put(&url2).body("BBBB").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let etag2 = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    // Complete
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/basic.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag1), (2, &etag2)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify assembled object
    let resp = srv
        .client
        .get(&srv.object_url("multi/basic.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"AAAABBBB");
}

/// Multipart with small parts (1 byte each).
#[tokio::test]
async fn test_multipart_small_parts() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("multi/small.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    let mut etags = Vec::new();
    for i in 1u8..=3 {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url("multi/small.bin"),
            i,
            upload_id
        );
        let resp = srv.client.put(&url).body(vec![b'A' + i - 1]).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();
        etags.push((i as u32, etag));
    }

    let parts: Vec<(u32, &str)> = etags.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/small.bin"),
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

    let resp = srv
        .client
        .get(&srv.object_url("multi/small.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"ABC");
}

/// Multipart with a single empty part.
#[tokio::test]
async fn test_multipart_single_empty_part() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("multi/empty.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("multi/empty.bin"),
        upload_id
    );
    let resp = srv.client.put(&url).body("").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/empty.bin"),
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

    let resp = srv
        .client
        .get(&srv.object_url("multi/empty.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().len(), 0);
}

/// Abort a multipart upload, then GET should 404.
#[tokio::test]
async fn test_multipart_abort() {
    let srv = TestServer::start().await;

    // Initiate
    let url = format!("{}?uploads", srv.object_url("multi/abort.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload a part
    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("multi/abort.bin"),
        upload_id
    );
    srv.client.put(&url).body("data").send().await.unwrap();

    // Abort
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/abort.bin"),
        upload_id
    );
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // Object should not exist
    let resp = srv
        .client
        .get(&srv.object_url("multi/abort.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// Abort with an unknown upload ID. S3 semantics vary -- some implementations
/// return 404, others return 204 (idempotent).
#[tokio::test]
async fn test_multipart_abort_unknown_upload_id() {
    let srv = TestServer::start().await;

    let url = format!(
        "{}?uploadId=nonexistent-id",
        srv.object_url("multi/ghost.bin")
    );
    let resp = srv.client.delete(&url).send().await.unwrap();
    let status = resp.status().as_u16();
    assert!(
        status == 204 || status == 404,
        "Abort unknown uploadId should return 204 or 404, got {}",
        status
    );
}

/// ListParts should return uploaded parts.
#[tokio::test]
async fn test_multipart_list_parts() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("multi/lp.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload 2 parts
    for part in 1..=2 {
        let url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url("multi/lp.bin"),
            part,
            upload_id
        );
        srv.client
            .put(&url)
            .body(format!("part{}", part))
            .send()
            .await
            .unwrap();
    }

    // ListParts
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/lp.bin"),
        upload_id
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<PartNumber>1</PartNumber>"),
        "Should list part 1: {}",
        body
    );
    assert!(
        body.contains("<PartNumber>2</PartNumber>"),
        "Should list part 2: {}",
        body
    );
}

/// Multipart overwrite: completing a multipart upload on an existing key replaces it.
#[tokio::test]
async fn test_multipart_overwrite_existing() {
    let srv = TestServer::start().await;

    // Seed with PUT
    srv.client
        .put(&srv.object_url("multi/ow.bin"))
        .body("original")
        .send()
        .await
        .unwrap();

    // Multipart upload to same key
    let url = format!("{}?uploads", srv.object_url("multi/ow.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("multi/ow.bin"),
        upload_id
    );
    let resp = srv.client.put(&url).body("replaced").send().await.unwrap();
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/ow.bin"),
        upload_id
    );
    srv.client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag)]))
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .get(&srv.object_url("multi/ow.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        b"replaced",
        "Multipart should overwrite existing object"
    );
}

/// Multipart with varying part sizes.
#[tokio::test]
async fn test_multipart_varying_part_sizes() {
    let srv = TestServer::start().await;

    let url = format!("{}?uploads", srv.object_url("multi/varied.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Parts of different sizes: 10 bytes, 10000 bytes, 100 bytes
    let sizes = [10usize, 10_000, 100];
    let mut etags = Vec::new();
    let mut expected = Vec::new();

    for (i, &size) in sizes.iter().enumerate() {
        let part_num = (i + 1) as u32;
        let data: Vec<u8> = (0..size).map(|j| ((j + i) % 256) as u8).collect();
        expected.extend_from_slice(&data);

        let url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url("multi/varied.bin"),
            part_num,
            upload_id
        );
        let resp = srv.client.put(&url).body(data).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        etags.push((part_num, etag));
    }

    // Complete
    let parts: Vec<(u32, &str)> = etags.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("multi/varied.bin"),
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

    // Verify
    let resp = srv
        .client
        .get(&srv.object_url("multi/varied.bin"))
        .send()
        .await
        .unwrap();
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), expected.len(), "Total size should match");
    assert_eq!(
        body.as_ref(),
        expected.as_slice(),
        "Content should match concatenated parts"
    );
}

/// Complete with an invalid (unknown) uploadId should fail.
#[tokio::test]
async fn test_multipart_complete_unknown_upload_id() {
    let srv = TestServer::start().await;

    let url = format!(
        "{}?uploadId=bogus-id",
        srv.object_url("multi/ghost.bin")
    );
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, "\"fake-etag\"")]))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        404,
        "Complete with unknown uploadId should return 404"
    );
}

// ===========================================================================
// ListMultipartUploads
// ===========================================================================

/// ListMultipartUploads should return in-progress uploads.
#[tokio::test]
async fn test_list_multipart_uploads() {
    let srv = TestServer::start().await;

    // Initiate two uploads
    let url1 = format!("{}?uploads", srv.object_url("multi/lmu1.bin"));
    let resp1 = srv.client.post(&url1).send().await.unwrap();
    assert_eq!(resp1.status(), 200);
    let body1 = resp1.text().await.unwrap();
    let upload_id1 = extract_xml_tag(&body1, "UploadId").unwrap().to_string();

    let url2 = format!("{}?uploads", srv.object_url("multi/lmu2.bin"));
    let resp2 = srv.client.post(&url2).send().await.unwrap();
    assert_eq!(resp2.status(), 200);
    let body2 = resp2.text().await.unwrap();
    let upload_id2 = extract_xml_tag(&body2, "UploadId").unwrap().to_string();

    // ListMultipartUploads
    let url = format!("{}?uploads", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    assert!(
        body.contains(&upload_id1),
        "Should list upload 1 ({}): {}",
        upload_id1,
        body
    );
    assert!(
        body.contains(&upload_id2),
        "Should list upload 2 ({}): {}",
        upload_id2,
        body
    );

    // Abort both to clean up
    let del1 = format!("{}?uploadId={}", srv.object_url("multi/lmu1.bin"), upload_id1);
    srv.client.delete(&del1).send().await.unwrap();
    let del2 = format!("{}?uploadId={}", srv.object_url("multi/lmu2.bin"), upload_id2);
    srv.client.delete(&del2).send().await.unwrap();
}

/// After aborting all uploads, ListMultipartUploads should be empty.
#[tokio::test]
async fn test_list_multipart_uploads_empty_after_abort() {
    let srv = TestServer::start().await;

    let init_url = format!("{}?uploads", srv.object_url("multi/lmu-abort.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Abort
    let del = format!("{}?uploadId={}", srv.object_url("multi/lmu-abort.bin"), upload_id);
    srv.client.delete(&del).send().await.unwrap();

    // ListMultipartUploads
    let url = format!("{}?uploads", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        !body.contains(&upload_id),
        "Aborted upload should not appear in list: {}",
        body
    );
}

// ===========================================================================
// UploadPartCopy
// ===========================================================================

/// UploadPartCopy: use copy-source to assemble a multipart from existing objects.
#[tokio::test]
async fn test_upload_part_copy() {
    let srv = TestServer::start().await;

    // Seed source objects
    srv.client
        .put(&srv.object_url("upc/src1.txt"))
        .body("AAAA")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("upc/src2.txt"))
        .body("BBBB")
        .send()
        .await
        .unwrap();

    // Initiate multipart
    let init_url = format!("{}?uploads", srv.object_url("upc/assembled.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // UploadPartCopy: part 1 from src1
    let url1 = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("upc/assembled.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url1)
        .header("x-amz-copy-source", "/data/upc/src1.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "UploadPartCopy part 1 should succeed");
    let body1 = resp.text().await.unwrap();
    let etag1 = extract_xml_tag(&body1, "ETag")
        .expect("UploadPartCopy should return ETag")
        .to_string();

    // UploadPartCopy: part 2 from src2
    let url2 = format!(
        "{}?partNumber=2&uploadId={}",
        srv.object_url("upc/assembled.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url2)
        .header("x-amz-copy-source", "/data/upc/src2.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "UploadPartCopy part 2 should succeed");
    let body2 = resp.text().await.unwrap();
    let etag2 = extract_xml_tag(&body2, "ETag")
        .expect("UploadPartCopy should return ETag")
        .to_string();

    // Complete
    let url = format!(
        "{}?uploadId={}",
        srv.object_url("upc/assembled.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag1), (2, &etag2)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify assembled object
    let resp = srv
        .client
        .get(&srv.object_url("upc/assembled.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"AAAABBBB");
}

/// UploadPartCopy with x-amz-copy-source-range should copy only the
/// requested byte range from the source object into the part.
#[tokio::test]
async fn test_upload_part_copy_with_range() {
    let srv = TestServer::start().await;

    // Seed a source object with known content: "AABBCCDD" (8 bytes)
    srv.client
        .put(&srv.object_url("upcr/source.bin"))
        .body("AABBCCDD")
        .send()
        .await
        .unwrap();

    // Initiate multipart upload
    let init_url = format!("{}?uploads", srv.object_url("upcr/assembled.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Part 1: copy bytes 0-3 ("AABB")
    let url1 = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("upcr/assembled.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url1)
        .header("x-amz-copy-source", "/data/upcr/source.bin")
        .header("x-amz-copy-source-range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "UploadPartCopy with range part 1 should succeed");
    let body1 = resp.text().await.unwrap();
    let etag1 = extract_xml_tag(&body1, "ETag")
        .expect("UploadPartCopy should return ETag")
        .to_string();

    // Part 2: copy bytes 4-7 ("CCDD")
    let url2 = format!(
        "{}?partNumber=2&uploadId={}",
        srv.object_url("upcr/assembled.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url2)
        .header("x-amz-copy-source", "/data/upcr/source.bin")
        .header("x-amz-copy-source-range", "bytes=4-7")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "UploadPartCopy with range part 2 should succeed");
    let body2 = resp.text().await.unwrap();
    let etag2 = extract_xml_tag(&body2, "ETag")
        .expect("UploadPartCopy should return ETag")
        .to_string();

    // Complete
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("upcr/assembled.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&complete_url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag1), (2, &etag2)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify assembled object has the expected content
    let resp = srv
        .client
        .get(&srv.object_url("upcr/assembled.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let assembled = resp.text().await.unwrap();
    assert_eq!(assembled, "AABBCCDD", "assembled object should be the two ranges concatenated");
}

/// UploadPartCopy with out-of-bounds range should return error.
#[tokio::test]
async fn test_upload_part_copy_range_out_of_bounds() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("upcr/small.bin"))
        .body("tiny")
        .send()
        .await
        .unwrap();

    let init_url = format!("{}?uploads", srv.object_url("upcr/oob.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Request range beyond the source size (source is 4 bytes)
    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("upcr/oob.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url)
        .header("x-amz-copy-source", "/data/upcr/small.bin")
        .header("x-amz-copy-source-range", "bytes=0-100")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "Out-of-bounds range should fail, got {}",
        resp.status()
    );
}

/// UploadPartCopy with inverted range (start > end) should return error.
#[tokio::test]
async fn test_upload_part_copy_range_inverted() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("upcr/inv.bin"))
        .body("some data here")
        .send()
        .await
        .unwrap();

    let init_url = format!("{}?uploads", srv.object_url("upcr/inv-out.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("upcr/inv-out.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url)
        .header("x-amz-copy-source", "/data/upcr/inv.bin")
        .header("x-amz-copy-source-range", "bytes=5-2")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "Inverted range (start > end) should fail, got {}",
        resp.status()
    );
}

// ===========================================================================
// Multipart part gaps
// ===========================================================================

/// CompleteMultipartUpload with non-contiguous part numbers (gap) should
/// still succeed -- S3 only requires the parts listed in the Complete
/// request to exist (not contiguous numbers).
#[tokio::test]
async fn test_multipart_non_contiguous_parts() {
    let srv = TestServer::start().await;

    // CreateMultipartUpload
    let url = format!("{}?uploads", srv.object_url("gapped.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap();

    // Upload parts 1, 3, 5 (skip 2, 4)
    let mut etags = Vec::new();
    for part_num in [1, 3, 5] {
        let part_url = format!(
            "{}?partNumber={}&uploadId={}",
            srv.object_url("gapped.bin"),
            part_num,
            upload_id
        );
        let data = format!("part-{}", part_num);
        let resp = srv.client.put(&part_url).body(data).send().await.unwrap();
        assert_eq!(resp.status(), 200, "UploadPart {} should succeed", part_num);
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        etags.push((part_num as u32, etag));
    }

    // Complete with parts 1, 3, 5
    let parts: Vec<(u32, &str)> = etags.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let xml = complete_xml(&parts);
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("gapped.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&complete_url)
        .body(xml)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Complete with non-contiguous parts should succeed");

    // Verify assembled body = part-1 + part-3 + part-5
    let resp = srv
        .client
        .get(&srv.object_url("gapped.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "part-1part-3part-5");
}

/// CompleteMultipartUpload referencing a part that was never uploaded
/// should fail.
#[tokio::test]
async fn test_multipart_missing_part_fails_complete() {
    let srv = TestServer::start().await;

    // Create
    let url = format!("{}?uploads", srv.object_url("missing.bin"));
    let resp = srv.client.post(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap();

    // Upload only part 1
    let part_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("missing.bin"),
        upload_id
    );
    let resp = srv.client.put(&part_url).body("only-one").send().await.unwrap();
    let etag1 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Try to complete with part 1 AND part 2 (never uploaded)
    let xml = format!(
        "<CompleteMultipartUpload>\
         <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>\"fake\"</ETag></Part>\
         </CompleteMultipartUpload>",
        etag1
    );
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("missing.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&complete_url)
        .body(xml)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "Complete with missing part should fail, got {}",
        resp.status()
    );
}
