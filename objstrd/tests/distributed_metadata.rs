//! E2E tests: verify body-only size semantics survive the full S3 round-trip
//! through the distributed (ShardedObjectStore) path.
//!
//! These tests complement `metadata.rs` (which uses the single-node TestServer)
//! by exercising the same invariants through the sharded backend.  If a future
//! change breaks the adapter/sharded-store interaction, these tests will catch
//! it.

mod common;
use common::{complete_xml, extract_xml_tag, DistributedTestServer};

// ===========================================================================
// HEAD content-length must equal body size (not body+trailer)
// ===========================================================================

#[tokio::test]
async fn distributed_head_content_length_excludes_trailer() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let body = "exactly twenty bytes";
    assert_eq!(body.len(), 20);
    let url = srv.object_url("dmeta/size-check.bin");

    srv.client
        .put(&url)
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-tag", "size-test")
        .body(body)
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let cl: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(cl, 20, "HEAD content-length should be body size (20), got {cl}");
}

// ===========================================================================
// GET must return exactly the body bytes (no trailer leak)
// ===========================================================================

#[tokio::test]
async fn distributed_get_body_no_trailer_leak() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let data = vec![0xCDu8; 500];
    let url = srv.object_url("dmeta/noleak.bin");

    srv.client
        .put(&url)
        .header("x-amz-meta-stuff", "lots-of-metadata")
        .body(data.clone())
        .send()
        .await
        .unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.len(), 500, "GET body length should be 500, got {}", got.len());
    assert_eq!(got.as_ref(), data.as_slice(), "GET body should match PUT body exactly");
}

// ===========================================================================
// LIST size must equal body size
// ===========================================================================

#[tokio::test]
async fn distributed_list_size_excludes_trailer() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let body = "hello list";
    let url = srv.object_url("dmeta/listed.bin");

    srv.client
        .put(&url)
        .header("x-amz-meta-listed", "yes")
        .body(body)
        .send()
        .await
        .unwrap();

    let list_url = format!("{}?list-type=2&prefix=dmeta/listed", srv.bucket_url());
    let resp = srv.client.get(&list_url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let xml = resp.text().await.unwrap();
    let size_str = extract_xml_tag(&xml, "Size").expect("Size tag missing in LIST response");
    let size: u64 = size_str.parse().expect("Size not a number");
    assert_eq!(
        size,
        body.len() as u64,
        "LIST size should be body size ({}), got {size}",
        body.len()
    );
}

// ===========================================================================
// HEAD, GET, and LIST all report the same body-only size
// ===========================================================================

#[tokio::test]
async fn distributed_head_get_list_size_consistency() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let body = "consistency check payload";
    let url = srv.object_url("dmeta/consistent.bin");

    srv.client
        .put(&url)
        .header("content-type", "text/plain")
        .header("x-amz-meta-purpose", "consistency")
        .body(body)
        .send()
        .await
        .unwrap();

    // HEAD
    let resp = srv.client.head(&url).send().await.unwrap();
    let head_size: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // GET
    let resp = srv.client.get(&url).send().await.unwrap();
    let get_size = resp.bytes().await.unwrap().len() as u64;

    // LIST
    let list_url = format!("{}?list-type=2&prefix=dmeta/consistent", srv.bucket_url());
    let resp = srv.client.get(&list_url).send().await.unwrap();
    let xml = resp.text().await.unwrap();
    let list_size: u64 = extract_xml_tag(&xml, "Size")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let expected = body.len() as u64;
    assert_eq!(head_size, expected, "HEAD size mismatch");
    assert_eq!(get_size, expected, "GET size mismatch");
    assert_eq!(list_size, expected, "LIST size mismatch");
}

// ===========================================================================
// Range reads must target body-only range (not metadata)
// ===========================================================================

#[tokio::test]
async fn distributed_range_get_with_metadata() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let url = srv.object_url("dmeta/range.txt");

    srv.client
        .put(&url)
        .header("x-amz-meta-range-test", "yes")
        .body("0123456789ABCDEF")
        .send()
        .await
        .unwrap();

    // bytes=0-3
    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"0123");

    // bytes=10-15 (the ABCDEF part)
    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=10-15")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"ABCDEF");

    // Suffix range: last 4 bytes
    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=-4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"CDEF");
}

// ===========================================================================
// Copy COPY mode -- metadata preserved, sizes correct
// ===========================================================================

#[tokio::test]
async fn distributed_copy_preserves_metadata_and_size() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    srv.client
        .put(&srv.object_url("dmeta/src-copy.txt"))
        .header("content-type", "image/png")
        .header("cache-control", "no-cache")
        .header("x-amz-meta-origin", "test")
        .body("png data here")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("dmeta/dst-copy.txt"))
        .header("x-amz-copy-source", "/data/dmeta/src-copy.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // HEAD the copy -- metadata preserved and size is body-only
    let resp = srv
        .client
        .head(&srv.object_url("dmeta/dst-copy.txt"))
        .send()
        .await
        .unwrap();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("image/png"), "copy should preserve content-type: {ct}");
    let cc = resp
        .headers()
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(cc, "no-cache", "copy should preserve cache-control");
    let origin = resp
        .headers()
        .get("x-amz-meta-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(origin, "test", "copy should preserve x-amz-meta-origin");

    let cl: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(cl, 13, "copy content-length should be body size (13), got {cl}");

    // GET body should match
    let resp = srv
        .client
        .get(&srv.object_url("dmeta/dst-copy.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"png data here");
}

// ===========================================================================
// Copy REPLACE mode -- new metadata, body preserved, sizes correct
// ===========================================================================

#[tokio::test]
async fn distributed_copy_replace_metadata_and_size() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    srv.client
        .put(&srv.object_url("dmeta/src-replace.txt"))
        .header("content-type", "text/plain")
        .header("x-amz-meta-old", "should-be-gone")
        .body("data")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("dmeta/dst-replace.txt"))
        .header("x-amz-copy-source", "/data/dmeta/src-replace.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "application/xml")
        .header("x-amz-meta-new", "fresh")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    let resp = srv
        .client
        .head(&srv.object_url("dmeta/dst-replace.txt"))
        .send()
        .await
        .unwrap();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("application/xml"), "REPLACE should set new content-type: {ct}");
    let new_meta = resp
        .headers()
        .get("x-amz-meta-new")
        .and_then(|v| v.to_str().ok());
    assert_eq!(new_meta, Some("fresh"), "REPLACE should set new x-amz-meta");
    let old_meta = resp.headers().get("x-amz-meta-old");
    assert!(old_meta.is_none(), "REPLACE should drop old x-amz-meta-old");

    let cl: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(cl, 4, "REPLACE content-length should be body size (4), got {cl}");

    // Body preserved
    let resp = srv
        .client
        .get(&srv.object_url("dmeta/dst-replace.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"data");
}

// ===========================================================================
// Multipart upload metadata + size correctness through distributed path
// ===========================================================================

#[tokio::test]
async fn distributed_multipart_metadata_and_size() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    // Initiate with metadata
    let init_url = format!("{}?uploads", srv.object_url("dmeta/multi.bin"));
    let resp = srv
        .client
        .post(&init_url)
        .header("content-type", "video/mp4")
        .header("x-amz-meta-encoder", "ffmpeg")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let init_body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&init_body, "UploadId")
        .unwrap()
        .to_string();

    // Upload two parts
    let part1_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("dmeta/multi.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&part1_url)
        .body("AAAA")
        .send()
        .await
        .unwrap();
    let etag1 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let part2_url = format!(
        "{}?partNumber=2&uploadId={}",
        srv.object_url("dmeta/multi.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&part2_url)
        .body("BBBBBB")
        .send()
        .await
        .unwrap();
    let etag2 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Complete
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("dmeta/multi.bin"),
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
    assert_eq!(resp.status().as_u16(), 200);

    // HEAD: content-length = 4 + 6 = 10 (body only)
    let resp = srv
        .client
        .head(&srv.object_url("dmeta/multi.bin"))
        .send()
        .await
        .unwrap();
    let cl: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(cl, 10, "multipart HEAD content-length should be 10 (body), got {cl}");

    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("video/mp4"), "multipart should preserve content-type: {ct}");

    let encoder = resp
        .headers()
        .get("x-amz-meta-encoder")
        .and_then(|v| v.to_str().ok());
    assert_eq!(encoder, Some("ffmpeg"), "multipart should preserve x-amz-meta");

    // GET body = AAAABBBBBB
    let resp = srv
        .client
        .get(&srv.object_url("dmeta/multi.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"AAAABBBBBB");

    // LIST size = 10
    let list_url = format!("{}?list-type=2&prefix=dmeta/multi", srv.bucket_url());
    let resp = srv.client.get(&list_url).send().await.unwrap();
    let xml = resp.text().await.unwrap();
    let size: u64 = extract_xml_tag(&xml, "Size")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert_eq!(size, 10, "multipart LIST size should be 10 (body), got {size}");
}

// ===========================================================================
// UploadPartCopy from source with metadata -- sizes not inflated
// ===========================================================================

#[tokio::test]
async fn distributed_upload_part_copy_with_metadata() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    // Seed source with metadata
    srv.client
        .put(&srv.object_url("dmeta/upc-src.txt"))
        .header("x-amz-meta-source", "yes")
        .body("SOURCEBODY")
        .send()
        .await
        .unwrap();

    // Initiate multipart
    let init_url = format!("{}?uploads", srv.object_url("dmeta/upc-dst.bin"));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let init_body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&init_body, "UploadId")
        .unwrap()
        .to_string();

    // UploadPartCopy from source
    let url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("dmeta/upc-dst.bin"),
        upload_id
    );
    let resp = srv
        .client
        .put(&url)
        .header("x-amz-copy-source", "/data/dmeta/upc-src.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    let etag = extract_xml_tag(&body, "ETag").unwrap().to_string();

    // Complete
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("dmeta/upc-dst.bin"),
        upload_id
    );
    let resp = srv
        .client
        .post(&complete_url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Body should be exactly the source body (10 bytes), no metadata leaking
    let resp = srv
        .client
        .get(&srv.object_url("dmeta/upc-dst.bin"))
        .send()
        .await
        .unwrap();
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"SOURCEBODY", "UploadPartCopy should copy body only");
    assert_eq!(got.len(), 10, "assembled object size should be 10, got {}", got.len());
}

// ===========================================================================
// HEAD bucket stats reflect body-only sizes in distributed mode
// ===========================================================================

#[tokio::test]
async fn distributed_head_bucket_bytes_used_logical() {
    let srv = DistributedTestServer::start(3, 1, "data").await;

    srv.client
        .put(&srv.object_url("dmeta/stat1.txt"))
        .header("x-amz-meta-a", "1")
        .body("aaaa")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("dmeta/stat2.txt"))
        .header("x-amz-meta-b", "2")
        .body("bbbbbb")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&srv.bucket_url()).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    let bytes_used: u64 = resp
        .headers()
        .get("x-rgw-bytes-used")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(999999);
    assert_eq!(
        bytes_used, 10,
        "x-rgw-bytes-used should be 10 (body only), got {bytes_used}"
    );

    let obj_count: u64 = resp
        .headers()
        .get("x-rgw-object-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(obj_count, 2, "x-rgw-object-count should be 2, got {obj_count}");
}

// ===========================================================================
// Empty body object with metadata in distributed mode
// ===========================================================================

#[tokio::test]
async fn distributed_empty_body_metadata() {
    let srv = DistributedTestServer::start(3, 1, "data").await;
    let url = srv.object_url("dmeta/empty.txt");

    srv.client
        .put(&url)
        .header("content-type", "text/plain")
        .header("x-amz-meta-empty", "true")
        .body("")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    let cl: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(999);
    assert_eq!(cl, 0, "empty body should have content-length 0, got {cl}");

    let empty_val = resp
        .headers()
        .get("x-amz-meta-empty")
        .and_then(|v| v.to_str().ok());
    assert_eq!(empty_val, Some("true"));

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().len(), 0);
}
