//! M7 Tests -- Inline trailer metadata round-trip.
//!
//! Verifies that all S3 metadata headers are persisted in the 256-byte inline
//! trailer and returned correctly on HEAD, GET, LIST, COPY, and multipart.

mod common;
use common::{complete_xml, extract_xml_tag, TestServer};

// ===========================================================================
// Basic metadata round-trip
// ===========================================================================

/// PUT with x-amz-meta-* headers, then HEAD should return them.
#[tokio::test]
async fn test_custom_metadata_roundtrip_head() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/custom.txt");

    srv.client
        .put(&url)
        .header("content-type", "text/plain")
        .header("x-amz-meta-author", "alice")
        .header("x-amz-meta-version", "42")
        .body("hello metadata")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let author = resp.headers().get("x-amz-meta-author")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(author.as_deref(), Some("alice"), "x-amz-meta-author mismatch");

    let version = resp.headers().get("x-amz-meta-version")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(version.as_deref(), Some("42"), "x-amz-meta-version mismatch");

    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("text/plain"), "content-type mismatch: {ct}");
}

/// PUT with x-amz-meta-* headers, then GET should return them.
#[tokio::test]
async fn test_custom_metadata_roundtrip_get() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/get-meta.txt");

    srv.client
        .put(&url)
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-tag", "important")
        .body("body bytes")
        .send()
        .await
        .unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let tag = resp.headers().get("x-amz-meta-tag")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(tag.as_deref(), Some("important"), "x-amz-meta-tag mismatch on GET");

    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("application/octet-stream"), "content-type mismatch on GET: {ct}");

    // Body must be intact (no trailer bytes leaking)
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"body bytes");
}

/// PUT with cache-control and content-disposition headers.
#[tokio::test]
async fn test_cache_control_and_content_disposition() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/cache.txt");

    srv.client
        .put(&url)
        .header("content-type", "text/css")
        .header("cache-control", "max-age=3600, public")
        .header("content-disposition", "attachment; filename=\"style.css\"")
        .body("body { color: red; }")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let cc = resp.headers().get("cache-control")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(cc, "max-age=3600, public", "cache-control mismatch");

    let cd = resp.headers().get("content-disposition")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(cd.contains("attachment"), "content-disposition mismatch: {cd}");
}

/// ETag should be an MD5 hex digest and consistent between PUT response and HEAD.
#[tokio::test]
async fn test_etag_consistency() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/etag-check.txt");

    let resp = srv.client
        .put(&url)
        .body("etag test data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let put_etag = resp.headers().get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("PUT should return ETag");

    let resp = srv.client.head(&url).send().await.unwrap();
    let head_etag = resp.headers().get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("HEAD should return ETag");
    assert_eq!(put_etag, head_etag, "PUT and HEAD ETags should match");

    let resp = srv.client.get(&url).send().await.unwrap();
    let get_etag = resp.headers().get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("GET should return ETag");
    assert_eq!(put_etag, get_etag, "PUT and GET ETags should match");
}

/// HEAD should report correct content-length (body size, not body+trailer).
#[tokio::test]
async fn test_head_content_length_excludes_trailer() {
    let srv = TestServer::start().await;
    let body = "exactly 20 bytes!!!";
    assert_eq!(body.len(), 19); // sanity
    let url = srv.object_url("meta/size-check.bin");

    srv.client.put(&url).body(body).send().await.unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    let cl: u64 = resp.headers().get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(cl, 19, "content-length should be body size, not body+trailer");
}

/// GET should return exactly the body bytes (no trailer leaking).
#[tokio::test]
async fn test_get_body_no_trailer_leak() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/noleak.bin");
    let data = vec![0xABu8; 1000];

    srv.client
        .put(&url)
        .header("x-amz-meta-stuff", "lots")
        .body(data.clone())
        .send()
        .await
        .unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.len(), 1000, "GET body length should match PUT body");
    assert_eq!(got.as_ref(), data.as_slice(), "GET body should match PUT body exactly");
}

/// Empty body object should still have correct metadata.
#[tokio::test]
async fn test_empty_body_metadata() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/empty.txt");

    srv.client
        .put(&url)
        .header("content-type", "text/plain")
        .header("x-amz-meta-empty", "true")
        .body("")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let cl: u64 = resp.headers().get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(999);
    assert_eq!(cl, 0, "empty body should have content-length 0");

    let empty_val = resp.headers().get("x-amz-meta-empty")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(empty_val.as_deref(), Some("true"));

    // GET should return empty body
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().len(), 0);
}

/// Overwriting an object should update metadata.
#[tokio::test]
async fn test_overwrite_replaces_metadata() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/overwrite.txt");

    // First write
    srv.client
        .put(&url)
        .header("content-type", "text/plain")
        .header("x-amz-meta-ver", "1")
        .body("version 1")
        .send()
        .await
        .unwrap();

    // Overwrite with different metadata
    srv.client
        .put(&url)
        .header("content-type", "application/json")
        .header("x-amz-meta-ver", "2")
        .header("x-amz-meta-new-key", "added")
        .body("{\"v\":2}")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("application/json"), "content-type should be updated: {ct}");

    let ver = resp.headers().get("x-amz-meta-ver")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(ver.as_deref(), Some("2"), "x-amz-meta-ver should be updated");

    let new_key = resp.headers().get("x-amz-meta-new-key")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(new_key.as_deref(), Some("added"), "new metadata key should exist");

    // Body should be the new content
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"{\"v\":2}");
}

// ===========================================================================
// Listing sizes
// ===========================================================================

/// ListObjectsV2 sizes should reflect body size, not body+trailer.
#[tokio::test]
async fn test_list_size_excludes_trailer() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/listed.bin");
    let body = "hello list";
    srv.client.put(&url).body(body).send().await.unwrap();

    let list_url = format!("{}?list-type=2&prefix=meta/listed", srv.bucket_url());
    let resp = srv.client.get(&list_url).send().await.unwrap();
    let xml = resp.text().await.unwrap();
    let size_str = extract_xml_tag(&xml, "Size").expect("Size tag missing");
    let size: u64 = size_str.parse().expect("Size not a number");
    assert_eq!(size, body.len() as u64, "listed size should be body size, got {size}");
}

// ===========================================================================
// Copy metadata
// ===========================================================================

/// COPY directive should preserve source metadata.
#[tokio::test]
async fn test_copy_preserves_metadata() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("meta/src-copy.txt"))
        .header("content-type", "image/png")
        .header("cache-control", "no-cache")
        .header("x-amz-meta-origin", "test")
        .body("png data")
        .send()
        .await
        .unwrap();

    let resp = srv.client
        .put(&srv.object_url("meta/dst-copy.txt"))
        .header("x-amz-copy-source", "/data/meta/src-copy.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // HEAD the copy -- should have the source's metadata
    let resp = srv.client.head(&srv.object_url("meta/dst-copy.txt")).send().await.unwrap();
    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("image/png"), "copy should preserve content-type: {ct}");

    let cc = resp.headers().get("cache-control")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(cc, "no-cache", "copy should preserve cache-control");

    let origin = resp.headers().get("x-amz-meta-origin")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(origin.as_deref(), Some("test"), "copy should preserve x-amz-meta-origin");
}

/// REPLACE directive should use new metadata, dropping old.
#[tokio::test]
async fn test_copy_replace_metadata() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("meta/src-replace.txt"))
        .header("content-type", "text/plain")
        .header("x-amz-meta-old", "should-be-gone")
        .body("data")
        .send()
        .await
        .unwrap();

    let resp = srv.client
        .put(&srv.object_url("meta/dst-replace.txt"))
        .header("x-amz-copy-source", "/data/meta/src-replace.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "application/xml")
        .header("x-amz-meta-new", "fresh")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv.client.head(&srv.object_url("meta/dst-replace.txt")).send().await.unwrap();

    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("application/xml"), "REPLACE should set new content-type: {ct}");

    let new_meta = resp.headers().get("x-amz-meta-new")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(new_meta.as_deref(), Some("fresh"), "REPLACE should set new x-amz-meta");

    let old_meta = resp.headers().get("x-amz-meta-old");
    assert!(old_meta.is_none(), "REPLACE should drop old x-amz-meta-old");

    // Body should be preserved
    let resp = srv.client.get(&srv.object_url("meta/dst-replace.txt")).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"data");
}

/// COPY + REPLACE should preserve the ETag (data is identical).
#[tokio::test]
async fn test_copy_replace_preserves_etag() {
    let srv = TestServer::start().await;

    let put_resp = srv.client
        .put(&srv.object_url("meta/etag-src.txt"))
        .body("etag test")
        .send()
        .await
        .unwrap();
    let src_etag = put_resp.headers().get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap();

    srv.client
        .put(&srv.object_url("meta/etag-dst.txt"))
        .header("x-amz-copy-source", "/data/meta/etag-src.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("x-amz-meta-changed", "yes")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&srv.object_url("meta/etag-dst.txt")).send().await.unwrap();
    let dst_etag = resp.headers().get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap();
    assert_eq!(src_etag, dst_etag, "REPLACE copy should preserve ETag since data is identical");
}

// ===========================================================================
// Multipart metadata
// ===========================================================================

/// Multipart upload with metadata at init time should be present on final object.
#[tokio::test]
async fn test_multipart_metadata_roundtrip() {
    let srv = TestServer::start().await;

    // Initiate with metadata
    let init_url = format!("{}?uploads", srv.object_url("meta/multi.bin"));
    let resp = srv.client
        .post(&init_url)
        .header("content-type", "video/mp4")
        .header("x-amz-meta-encoder", "ffmpeg")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload one part
    let part_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("meta/multi.bin"),
        upload_id
    );
    let resp = srv.client.put(&part_url).body("video data").send().await.unwrap();
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    // Complete
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("meta/multi.bin"),
        upload_id
    );
    let resp = srv.client
        .post(&complete_url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // HEAD should show metadata from init
    let resp = srv.client.head(&srv.object_url("meta/multi.bin")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("video/mp4"), "multipart should preserve content-type: {ct}");

    let encoder = resp.headers().get("x-amz-meta-encoder")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(encoder.as_deref(), Some("ffmpeg"), "multipart should preserve x-amz-meta");

    let cl: u64 = resp.headers().get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(cl, 10, "content-length should be body size");

    // GET body should be intact
    let resp = srv.client.get(&srv.object_url("meta/multi.bin")).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"video data");
}

// ===========================================================================
// Multiple metadata keys
// ===========================================================================

/// PUT with many x-amz-meta-* headers, verify all round-trip.
#[tokio::test]
async fn test_multiple_custom_metadata_keys() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/many-keys.txt");

    srv.client
        .put(&url)
        .header("x-amz-meta-key1", "val1")
        .header("x-amz-meta-key2", "val2")
        .header("x-amz-meta-key3", "val3")
        .header("x-amz-meta-key4", "val4")
        .header("x-amz-meta-key5", "val5")
        .body("multi-meta")
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    for i in 1..=5 {
        let key = format!("x-amz-meta-key{i}");
        let expected = format!("val{i}");
        let val = resp.headers().get(&key)
            .map(|v| v.to_str().unwrap().to_string());
        assert_eq!(val.as_deref(), Some(expected.as_str()), "{key} mismatch");
    }
}

// ===========================================================================
// No sidecar objects
// ===========================================================================

/// After PUT, there should be no __meta__ sidecar objects in the store.
#[tokio::test]
async fn test_no_meta_sidecar_extents() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("meta/nosidecar.txt"))
        .header("x-amz-meta-check", "inline")
        .body("data")
        .send()
        .await
        .unwrap();

    // List all extents via the raw store
    let store = srv.raw_store.as_ref().expect("raw_store should be available");
    let layout = store.layout_map();
    let meta_keys: Vec<_> = layout.extents.iter()
        .filter(|e| e.key.contains("__meta__"))
        .collect();
    assert!(meta_keys.is_empty(), "no __meta__ sidecar extents should exist: {:?}",
        meta_keys.iter().map(|e| &e.key).collect::<Vec<_>>());
}

// ===========================================================================
// Range read with metadata
// ===========================================================================

/// Range GET should return the right slice of body, not the trailer.
#[tokio::test]
async fn test_range_get_with_metadata() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/range.txt");

    srv.client
        .put(&url)
        .header("x-amz-meta-range-test", "yes")
        .body("0123456789ABCDEF")
        .send()
        .await
        .unwrap();

    // Range: bytes=0-3
    let resp = srv.client
        .get(&url)
        .header("range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"0123");

    // Range: bytes=10-15 (the ABCDEF part)
    let resp = srv.client
        .get(&url)
        .header("range", "bytes=10-15")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"ABCDEF");

    // Suffix range: last 4 bytes
    let resp = srv.client
        .get(&url)
        .header("range", "bytes=-4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"CDEF");
}

// ===========================================================================
// Delete cleans up metadata
// ===========================================================================

/// After DELETE, HEAD should 404 (no stale metadata).
#[tokio::test]
async fn test_delete_removes_metadata() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/del-meta.txt");

    srv.client
        .put(&url)
        .header("content-type", "text/plain")
        .header("x-amz-meta-temp", "yes")
        .body("temporary")
        .send()
        .await
        .unwrap();

    // Verify it exists
    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Delete
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // HEAD should 404
    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

// ===========================================================================
// HEAD bucket stats reflect logical sizes
// ===========================================================================

/// HEAD bucket x-rgw-bytes-used should report logical sizes (body only).
#[tokio::test]
async fn test_head_bucket_bytes_used_logical() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("meta/stat1.txt"))
        .body("aaaa")       // 4 bytes
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("meta/stat2.txt"))
        .body("bbbbbb")     // 6 bytes
        .send()
        .await
        .unwrap();

    let resp = srv.client.head(&srv.bucket_url()).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let bytes_used: u64 = resp.headers().get("x-rgw-bytes-used")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(999999);
    // 4 + 6 = 10 logical bytes
    assert_eq!(bytes_used, 10, "x-rgw-bytes-used should be sum of body sizes (10), got {bytes_used}");

    let obj_count: u64 = resp.headers().get("x-rgw-object-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(obj_count, 2, "x-rgw-object-count should be 2, got {obj_count}");
}

// ===========================================================================
// Latin-1 metadata encoding boundary
// ===========================================================================

/// Metadata values with characters outside Latin-1 range (> U+00FF) should
/// be silently dropped -- they must not appear in HEAD response headers.
#[tokio::test]
async fn test_latin1_metadata_non_latin1_dropped() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/nonlatin1.txt");

    // PUT with a valid header and one containing non-Latin-1 chars.
    // U+2713 (checkmark) has code point > 0xFF, so add_user_meta_headers()
    // should skip it.  We use the UTF-8 string directly because Rust strings
    // are UTF-8 and reqwest encodes header values from str.
    let resp = srv
        .client
        .put(&url)
        .header("x-amz-meta-good", "ascii-value")
        .header("x-amz-meta-emoji", "smiley \u{2713}")  // U+2713 > U+00FF
        .body("test body")
        .send()
        .await
        .unwrap();
    // PUT may succeed (metadata is stored) or the framework may reject it.
    // Either way, the non-Latin-1 value should not survive a round-trip.
    assert!(
        resp.status() == 200 || resp.status() == 400,
        "PUT should either succeed or reject non-Latin-1 header, got {}",
        resp.status()
    );

    if resp.status() == 200 {
        // HEAD should return the good header but not the non-Latin-1 one.
        let resp = srv.client.head(&url).send().await.unwrap();
        assert_eq!(resp.status(), 200);

        let good = resp
            .headers()
            .get("x-amz-meta-good")
            .map(|v| v.to_str().unwrap().to_string());
        assert_eq!(
            good.as_deref(),
            Some("ascii-value"),
            "Valid metadata should be preserved"
        );

        // The non-Latin-1 header should be absent (silently dropped).
        let emoji = resp.headers().get("x-amz-meta-emoji");
        assert!(
            emoji.is_none(),
            "Non-Latin-1 metadata should be silently dropped, but got: {:?}",
            emoji
        );
    }
}

/// Metadata values within Latin-1 range (U+0000..U+00FF) should round-trip.
#[tokio::test]
async fn test_latin1_metadata_roundtrip() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/latin1.txt");

    // Use printable ASCII as a baseline and Latin-1 supplement.
    // HTTP header values must be printable, so we use visible ASCII.
    let resp = srv
        .client
        .put(&url)
        .header("x-amz-meta-plain", "hello world")
        .header("x-amz-meta-nums", "12345")
        .header("x-amz-meta-special", "a/b+c=d")
        .body("latin1 test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT should succeed");

    // HEAD should return all headers.
    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let plain = resp
        .headers()
        .get("x-amz-meta-plain")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(plain.as_deref(), Some("hello world"));

    let nums = resp
        .headers()
        .get("x-amz-meta-nums")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(nums.as_deref(), Some("12345"));

    let special = resp
        .headers()
        .get("x-amz-meta-special")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(special.as_deref(), Some("a/b+c=d"));
}

// ===========================================================================
// Regression: copy REPLACE must preserve all standard HTTP headers
// (Bug 2.5: content-disposition, content-encoding, content-language, expires
//  were silently dropped before the fix.)
// ===========================================================================

/// COPY with REPLACE directive must carry over content-disposition,
/// content-encoding, and content-language to the destination object.
#[tokio::test]
async fn test_copy_replace_preserves_all_standard_headers() {
    let srv = TestServer::start().await;

    // Source object with basic metadata.
    srv.client
        .put(&srv.object_url("meta/copy-src-std.bin"))
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-old", "dropped")
        .body("binary payload")
        .send()
        .await
        .unwrap();

    // REPLACE copy with all standard headers.
    let resp = srv.client
        .put(&srv.object_url("meta/copy-dst-std.bin"))
        .header("x-amz-copy-source", "/data/meta/copy-src-std.bin")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "application/pdf")
        .header("content-disposition", "attachment; filename=\"report.pdf\"")
        .header("content-encoding", "gzip")
        .header("content-language", "en-US")
        .header("expires", "Thu, 01 Jan 2099 00:00:00 GMT")
        .header("cache-control", "no-store")
        .header("x-amz-meta-new", "present")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "copy REPLACE should succeed");

    let resp = srv.client.head(&srv.object_url("meta/copy-dst-std.bin")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // content-type
    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("application/pdf"), "content-type: {ct}");

    // content-disposition (was missing before bug fix)
    let cd = resp.headers().get("content-disposition")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        cd.contains("attachment") && cd.contains("report.pdf"),
        "content-disposition should survive REPLACE copy: {cd}",
    );

    // content-encoding (was missing before bug fix)
    let ce = resp.headers().get("content-encoding")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(ce, "gzip", "content-encoding should survive REPLACE copy");

    // content-language (was missing before bug fix)
    let cl = resp.headers().get("content-language")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(cl, "en-US", "content-language should survive REPLACE copy");

    // expires (was missing before bug fix)
    let ex = resp.headers().get("expires")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(!ex.is_empty(), "expires should survive REPLACE copy");
    assert!(ex.contains("2099"), "expires should contain year 2099: {ex}");

    // cache-control
    let cc = resp.headers().get("cache-control")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(cc, "no-store", "cache-control should survive REPLACE copy");

    // New user metadata present, old dropped
    let new_m = resp.headers().get("x-amz-meta-new")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(new_m.as_deref(), Some("present"));
    assert!(resp.headers().get("x-amz-meta-old").is_none(), "old metadata should be dropped");

    // Body unchanged
    let resp = srv.client.get(&srv.object_url("meta/copy-dst-std.bin")).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"binary payload");
}

// ===========================================================================
// Regression: multipart init must preserve all standard HTTP headers
// (Bug 2.5: content-disposition, content-encoding, content-language, expires
//  were silently dropped on create-multipart-upload before the fix.)
// ===========================================================================

/// Multipart upload initiated with content-disposition, content-encoding,
/// content-language, and expires must surface those on the final object.
#[tokio::test]
async fn test_multipart_preserves_all_standard_headers() {
    let srv = TestServer::start().await;

    // Initiate with all standard headers.
    let init_url = format!("{}?uploads", srv.object_url("meta/mp-std.bin"));
    let resp = srv.client
        .post(&init_url)
        .header("content-type", "video/webm")
        .header("content-disposition", "inline; filename=\"clip.webm\"")
        .header("content-encoding", "identity")
        .header("content-language", "fr")
        .header("expires", "Fri, 31 Dec 2100 23:59:59 GMT")
        .header("cache-control", "max-age=86400")
        .header("x-amz-meta-studio", "paris")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").unwrap().to_string();

    // Upload one part
    let part_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url("meta/mp-std.bin"),
        upload_id,
    );
    let resp = srv.client.put(&part_url).body("video frames").send().await.unwrap();
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    // Complete
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url("meta/mp-std.bin"),
        upload_id,
    );
    let resp = srv.client
        .post(&complete_url)
        .header("content-type", "application/xml")
        .body(complete_xml(&[(1, &etag)]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // HEAD the final object
    let resp = srv.client.head(&srv.object_url("meta/mp-std.bin")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // content-type
    let ct = resp.headers().get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(ct.contains("video/webm"), "content-type: {ct}");

    // content-disposition (was missing before bug fix)
    let cd = resp.headers().get("content-disposition")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        cd.contains("inline") && cd.contains("clip.webm"),
        "content-disposition should survive multipart: {cd}",
    );

    // content-encoding (was missing before bug fix)
    let ce = resp.headers().get("content-encoding")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(ce, "identity", "content-encoding should survive multipart");

    // content-language (was missing before bug fix)
    let cl_hdr = resp.headers().get("content-language")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(cl_hdr, "fr", "content-language should survive multipart");

    // expires (was missing before bug fix)
    let ex = resp.headers().get("expires")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(!ex.is_empty(), "expires should survive multipart");
    assert!(ex.contains("2100"), "expires should contain year 2100: {ex}");

    // cache-control
    let cc = resp.headers().get("cache-control")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(cc, "max-age=86400", "cache-control should survive multipart");

    // user metadata
    let studio = resp.headers().get("x-amz-meta-studio")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(studio.as_deref(), Some("paris"));

    // Body intact
    let resp = srv.client.get(&srv.object_url("meta/mp-std.bin")).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"video frames");
}
