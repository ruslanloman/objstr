//! M1 Tests -- Object CRUD via S3 protocol.
//!
//! Covers: bucket listing/HEAD, PUT/GET/HEAD/DELETE, ETags, content-type,
//! region headers, key validation (path traversal, reserved components,
//! trailing-slash keys), and spool-threshold large-object integrity.

mod common;
use common::{extract_xml_tag, TestServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ===========================================================================
// Bucket operations
// ===========================================================================

/// GET / should list buckets including the pre-registered one.
#[tokio::test]
async fn test_list_buckets() {
    let srv = TestServer::start().await;
    let resp = srv.client.get(&srv.base_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Bucket>") || body.contains("<Name>data</Name>"),
        "ListBuckets should contain at least the default bucket: {}",
        body
    );
}

/// HEAD /{bucket} for an existing bucket should return 200.
#[tokio::test]
async fn test_head_bucket() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .head(&srv.bucket_url())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "HeadBucket for existing bucket should 200");
}

/// HEAD /{bucket} for a non-existent bucket should return 404.
#[tokio::test]
async fn test_head_bucket_not_found() {
    let srv = TestServer::start().await;
    let url = format!("{}/no-such-bucket", srv.base_url);
    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404, "HeadBucket for missing bucket should 404");
}

/// PUT /{bucket} for an already-existing bucket.
#[tokio::test]
async fn test_create_bucket_already_exists() {
    let srv = TestServer::start().await;
    let resp = srv.client.put(&srv.bucket_url()).send().await.unwrap();
    // S3 returns 409 BucketAlreadyOwnedByYou or 200 depending on implementation
    let status = resp.status().as_u16();
    assert!(
        status == 200 || status == 409,
        "CreateBucket on existing bucket should return 200 or 409, got {}",
        status
    );
}

/// GET /{bucket}?location should return a LocationConstraint.
#[tokio::test]
async fn test_get_bucket_location() {
    let srv = TestServer::start().await;
    let url = format!("{}?location", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("LocationConstraint"),
        "Response should contain LocationConstraint: {}",
        body
    );
}

// ===========================================================================
// Object CRUD
// ===========================================================================

/// PUT then GET an object.
#[tokio::test]
async fn test_put_then_get() {
    let srv = TestServer::start().await;
    let url = srv.object_url("hello.txt");

    let resp = srv.client.put(&url).body("Hello, S3!").send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Hello, S3!");
}

/// PUT with a nested key (directory-like prefix).
#[tokio::test]
async fn test_put_nested_key() {
    let srv = TestServer::start().await;
    let url = srv.object_url("dir/sub/file.txt");

    srv.client.put(&url).body("nested").send().await.unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"nested");
}

/// PUT overwrite should replace the object content.
#[tokio::test]
async fn test_put_overwrite() {
    let srv = TestServer::start().await;
    let url = srv.object_url("overwrite.txt");

    srv.client.put(&url).body("v1").send().await.unwrap();
    srv.client.put(&url).body("v2").send().await.unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v2");
}

/// PUT to a non-existent bucket should fail.
#[tokio::test]
async fn test_put_no_such_bucket() {
    let srv = TestServer::start().await;
    let url = format!("{}/no-such-bucket/key.txt", srv.base_url);
    let resp = srv.client.put(&url).body("x").send().await.unwrap();
    assert_eq!(resp.status(), 404, "PUT to non-existent bucket should 404");
}

/// PUT zero-byte object.
#[tokio::test]
async fn test_put_zero_bytes() {
    let srv = TestServer::start().await;
    let url = srv.object_url("empty.bin");

    let resp = srv.client.put(&url).body("").send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap().len(), 0);
}

/// GET a key that does not exist should 404.
#[tokio::test]
async fn test_get_not_found() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .get(&srv.object_url("no-such-key.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// HEAD existing object returns 200 with content-length.
#[tokio::test]
async fn test_head_object() {
    let srv = TestServer::start().await;
    let url = srv.object_url("head-me.txt");
    srv.client.put(&url).body("12345").send().await.unwrap();

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let cl = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    assert_eq!(cl, Some(5), "Content-Length should be 5");
}

/// HEAD non-existent object returns 404.
#[tokio::test]
async fn test_head_object_not_found() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .head(&srv.object_url("ghost.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// DELETE then GET should 404.
#[tokio::test]
async fn test_delete_object() {
    let srv = TestServer::start().await;
    let url = srv.object_url("del-me.txt");
    srv.client.put(&url).body("bye").send().await.unwrap();

    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

/// DELETE a non-existent key should return 204 (S3 semantics).
#[tokio::test]
async fn test_delete_object_not_found_204() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .delete(&srv.object_url("ghost.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "DELETE non-existent key should 204");
}

/// PUT should return an ETag header.
#[tokio::test]
async fn test_put_returns_etag() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .put(&srv.object_url("etag.txt"))
        .body("etag content")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp.headers().get("etag");
    assert!(etag.is_some(), "PutObject should return ETag header");
}

/// PUT with content-type should be persisted and returned on GET.
#[tokio::test]
async fn test_put_content_type() {
    let srv = TestServer::start().await;
    let url = srv.object_url("typed.json");

    srv.client
        .put(&url)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        ct.contains("application/json"),
        "Content-Type should be application/json, got: {}",
        ct
    );
}

// ===========================================================================
// Bucket region
// ===========================================================================

/// HEAD bucket should include x-amz-bucket-region header.
#[tokio::test]
async fn test_head_bucket_region_header() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .head(&srv.bucket_url())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let region = resp
        .headers()
        .get("x-amz-bucket-region")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(
        region.as_deref(),
        Some("us-east-1"),
        "HeadBucket should return x-amz-bucket-region: us-east-1"
    );
}

/// ListBuckets should include BucketRegion for each bucket.
#[tokio::test]
async fn test_list_buckets_region() {
    let srv = TestServer::start().await;
    let resp = srv.client.get(&srv.base_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    let region = extract_xml_tag(&body, "BucketRegion");
    assert_eq!(
        region,
        Some("us-east-1"),
        "ListBuckets should include BucketRegion: {}",
        body
    );
}

// ===========================================================================
// Key validation -- path traversal prevention
// ===========================================================================

/// Helper: send a raw HTTP PUT and return the status code.
///
/// We bypass `reqwest` entirely because it (via the `url` crate) normalises
/// paths containing `..`, which means the server would never see the literal
/// `..` component. Using a raw TCP socket lets us send any path verbatim.
async fn raw_put(addr: &str, path: &str, body: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "PUT {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        path,
        body.len(),
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    // Parse status code from "HTTP/1.1 NNN ..."
    resp.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

/// PUT with keys containing ".." path components should be rejected.
///
/// Uses raw TCP to bypass URL normalisation in HTTP client libraries so that
/// the server actually receives the literal `..` in the path.
#[tokio::test]
async fn test_path_traversal_rejected() {
    let srv = TestServer::start().await;

    // Extract "127.0.0.1:PORT" from "http://127.0.0.1:PORT"
    let addr = srv.base_url.strip_prefix("http://").unwrap();

    let bad_paths = [
        format!("/{}/foo/../bar", srv.bucket),
        format!("/{}/../etc/passwd", srv.bucket),
        format!("/{}/a/b/../../c", srv.bucket),
        format!("/{}/dir/../../../secret", srv.bucket),
        format!("/{}/normal/../traversal", srv.bucket),
    ];

    for path in &bad_paths {
        let status = raw_put(addr, path, b"payload").await;
        assert!(
            status >= 400 && status < 500,
            "PUT {} should be rejected, got {}",
            path,
            status,
        );
    }

    // A key with ".." as a substring but not a path component should be fine.
    let ok_url = srv.object_url("file..name.txt");
    let resp = srv
        .client
        .put(&ok_url)
        .body("ok")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "Key with '..' as substring (not component) should succeed"
    );
}

// ===========================================================================
// Key validation -- reserved __DIRMARK__ component
// ===========================================================================

/// PUT with keys containing __DIRMARK__ as a path component should be rejected.
#[tokio::test]
async fn test_dirmark_reserved_rejected() {
    let srv = TestServer::start().await;

    let bad_keys = [
        "__DIRMARK__",
        "foo/__DIRMARK__/bar",
        "dir/__DIRMARK__",
    ];

    for key in &bad_keys {
        let url = srv.object_url(key);
        let resp = srv.client.put(&url).body("payload").send().await.unwrap();
        assert!(
            resp.status().is_client_error(),
            "PUT with key '{}' containing __DIRMARK__ component should be rejected, got {}",
            key,
            resp.status()
        );
    }

    // __DIRMARK__ as a substring (not a full component) should be fine.
    let ok_url = srv.object_url("file__DIRMARK__suffix.txt");
    let resp = srv
        .client
        .put(&ok_url)
        .body("ok")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "Key with __DIRMARK__ as substring (not component) should succeed"
    );
}

// ===========================================================================
// Key validation -- path traversal on GET/HEAD/DELETE
// ===========================================================================

/// Helper: send a raw HTTP request with an arbitrary method and return the
/// status code.  Same bypass approach as raw_put -- avoids URL normalisation.
async fn raw_request(addr: &str, method: &str, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        method, path,
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    resp.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

/// GET with ".." path components should be rejected.
#[tokio::test]
async fn test_path_traversal_rejected_on_get() {
    let srv = TestServer::start().await;
    let addr = srv.base_url.strip_prefix("http://").unwrap();

    let bad_paths = [
        format!("/{}/foo/../bar", srv.bucket),
        format!("/{}/../etc/passwd", srv.bucket),
    ];
    for path in &bad_paths {
        let status = raw_request(addr, "GET", path).await;
        assert!(
            status >= 400 && status < 500,
            "GET {} should be rejected, got {}",
            path,
            status,
        );
    }
}

/// HEAD with ".." path components should be rejected.
#[tokio::test]
async fn test_path_traversal_rejected_on_head() {
    let srv = TestServer::start().await;
    let addr = srv.base_url.strip_prefix("http://").unwrap();

    let status = raw_request(addr, "HEAD", &format!("/{}/foo/../bar", srv.bucket)).await;
    assert!(
        status >= 400 && status < 500,
        "HEAD with .. should be rejected, got {}",
        status,
    );
}

/// DELETE with ".." path components should be rejected.
#[tokio::test]
async fn test_path_traversal_rejected_on_delete() {
    let srv = TestServer::start().await;
    let addr = srv.base_url.strip_prefix("http://").unwrap();

    let status = raw_request(addr, "DELETE", &format!("/{}/foo/../bar", srv.bucket)).await;
    assert!(
        status >= 400 && status < 500,
        "DELETE with .. should be rejected, got {}",
        status,
    );
}

/// GET with __DIRMARK__ as a path component should be rejected.
#[tokio::test]
async fn test_dirmark_rejected_on_get() {
    let srv = TestServer::start().await;
    let addr = srv.base_url.strip_prefix("http://").unwrap();

    let bad_paths = [
        format!("/{}/__DIRMARK__", srv.bucket),
        format!("/{}/foo/__DIRMARK__/bar", srv.bucket),
    ];
    for path in &bad_paths {
        let status = raw_request(addr, "GET", path).await;
        assert!(
            status >= 400 && status < 500,
            "GET {} should be rejected, got {}",
            path,
            status,
        );
    }
}

/// DELETE with __DIRMARK__ as a path component should be rejected.
#[tokio::test]
async fn test_dirmark_rejected_on_delete() {
    let srv = TestServer::start().await;
    let addr = srv.base_url.strip_prefix("http://").unwrap();

    let bad_paths = [
        format!("/{}/__DIRMARK__", srv.bucket),
        format!("/{}/dir/__DIRMARK__", srv.bucket),
    ];
    for path in &bad_paths {
        let status = raw_request(addr, "DELETE", path).await;
        assert!(
            status >= 400 && status < 500,
            "DELETE {} should be rejected, got {}",
            path,
            status,
        );
    }
}

// ===========================================================================
// Trailing-slash key round-trip
// ===========================================================================

/// Keys ending with "/" should be stored and retrievable via PUT/GET/HEAD/DELETE.
#[tokio::test]
async fn test_trailing_slash_key_roundtrip() {
    let srv = TestServer::start().await;

    // PUT a key ending with "/"
    let url = srv.object_url("dirs/mydir/");
    let resp = srv
        .client
        .put(&url)
        .body("directory marker")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "PUT with trailing-slash key should succeed"
    );

    // GET the same key
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "GET trailing-slash key should succeed");
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        b"directory marker",
        "Body should match"
    );

    // HEAD the same key
    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "HEAD trailing-slash key should succeed");
    let cl = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    assert_eq!(cl, 16, "content-length should be 16");

    // LIST should include the key (with trailing slash)
    let list_url = format!(
        "{}/{}?list-type=2&prefix=dirs/",
        srv.base_url, srv.bucket
    );
    let resp = srv.client.get(&list_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("dirs/mydir/"),
        "Listing should contain the trailing-slash key: {}",
        body
    );

    // DELETE the key
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "DELETE trailing-slash key should succeed"
    );

    // GET after delete should be 404
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        404,
        "GET after DELETE should return 404"
    );
}

// ===========================================================================
// Spool threshold -- objects above 8 MB use temp-file spooling
// ===========================================================================

/// PUT a 10 MB object (above the 8 MB SPOOL_THRESHOLD), GET it back, and
/// verify the content is byte-for-byte identical.  This exercises the
/// temp-file spooling path in put_object().
#[tokio::test]
async fn test_spool_threshold_large_object() {
    let srv = TestServer::start().await;

    let size = 10 * 1024 * 1024; // 10 MB
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let url = srv.object_url("spool/large.bin");

    // PUT 10 MB
    let resp = srv
        .client
        .put(&url)
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "PUT 10 MB object should succeed (temp-file spool path)"
    );

    // HEAD should report correct content-length
    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let cl = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    assert_eq!(
        cl, size,
        "content-length should be {} for 10 MB object",
        size
    );

    // GET and verify content integrity
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), size, "GET body length should match PUT");
    assert_eq!(&body[..], &data[..], "GET body should match PUT data");
}

/// PUT objects just below and just above the spool threshold to exercise
/// both the in-memory fast path and the temp-file path.
#[tokio::test]
async fn test_spool_threshold_boundary() {
    let srv = TestServer::start().await;

    // Just below threshold: 8 MB - 1 byte (in-memory path)
    let below_size = 8 * 1024 * 1024 - 1;
    let below_data: Vec<u8> = (0..below_size).map(|i| (i % 199) as u8).collect();
    let url_below = srv.object_url("spool/below.bin");

    let resp = srv
        .client
        .put(&url_below)
        .body(below_data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT just-below-threshold should succeed");

    let resp = srv.client.get(&url_below).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), below_size);
    assert_eq!(&body[..], &below_data[..], "Below-threshold content mismatch");

    // Just above threshold: 8 MB + 1 byte (temp-file path)
    let above_size = 8 * 1024 * 1024 + 1;
    let above_data: Vec<u8> = (0..above_size).map(|i| (i % 211) as u8).collect();
    let url_above = srv.object_url("spool/above.bin");

    let resp = srv
        .client
        .put(&url_above)
        .body(above_data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT just-above-threshold should succeed");

    let resp = srv.client.get(&url_above).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), above_size);
    assert_eq!(&body[..], &above_data[..], "Above-threshold content mismatch");
}

// ===========================================================================
// HEAD bucket stats headers
// ===========================================================================

/// HEAD bucket should return x-rgw-object-count and x-rgw-bytes-used headers
/// that reflect the objects stored in that bucket.
#[tokio::test]
async fn test_head_bucket_stats_headers() {
    let srv = TestServer::start().await;

    // Verify the bucket exists (it is created at startup as the default).
    let bucket_url = srv.bucket_url();
    let resp = srv.client.head(&bucket_url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "HEAD bucket should succeed");

    // Initially the bucket may have 0 objects.
    let initial_count = resp
        .headers()
        .get("x-rgw-object-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // PUT 3 objects
    for i in 0..3 {
        let url = srv.object_url(&format!("stats/obj-{}.txt", i));
        let resp = srv
            .client
            .put(&url)
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // HEAD bucket again -- wait a moment for any cache to expire.
    // BUCKET_STATS_TTL is 5 seconds, so this sleep ensures fresh stats.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    let resp = srv.client.head(&bucket_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let count = resp
        .headers()
        .get("x-rgw-object-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    assert!(
        count.is_some(),
        "HEAD bucket should return x-rgw-object-count header"
    );
    assert!(
        count.unwrap() >= initial_count + 3,
        "object count should reflect at least 3 new objects, got {:?} (initial was {})",
        count,
        initial_count
    );

    let bytes_used = resp
        .headers()
        .get("x-rgw-bytes-used")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    assert!(
        bytes_used.is_some(),
        "HEAD bucket should return x-rgw-bytes-used header"
    );
    assert!(
        bytes_used.unwrap() > 0,
        "bytes_used should be > 0 after putting objects"
    );
}

/// HEAD bucket called twice within the cache TTL should still return
/// consistent stats (verifies the cache does not corrupt values).
#[tokio::test]
async fn test_head_bucket_stats_cache_consistency() {
    let srv = TestServer::start().await;

    // PUT an object so stats are non-zero.
    let url = srv.object_url("cache/test.txt");
    let resp = srv.client.put(&url).body("cached").send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Wait for cache to expire to get fresh stats.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // First HEAD
    let bucket_url = srv.bucket_url();
    let resp1 = srv.client.head(&bucket_url).send().await.unwrap();
    assert_eq!(resp1.status(), 200);
    let count1 = resp1
        .headers()
        .get("x-rgw-object-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // Second HEAD immediately (should hit cache, same values).
    let resp2 = srv.client.head(&bucket_url).send().await.unwrap();
    assert_eq!(resp2.status(), 200);
    let count2 = resp2
        .headers()
        .get("x-rgw-object-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    assert_eq!(
        count1, count2,
        "Two rapid HEAD-bucket calls should return identical object counts"
    );
}

// ===========================================================================
// Bucket creation / deletion edge cases
// ===========================================================================

/// CreateBucket with an invalid DNS name should return 400.
#[tokio::test]
async fn test_create_bucket_invalid_dns_name() {
    let srv = TestServer::start().await;
    let url = format!("{}/INVALID_DNS", srv.base_url);
    let resp = srv.client.put(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        400,
        "Invalid bucket name should return 400: {}",
        resp.status()
    );
}

/// DeleteBucket on a non-empty bucket should return 409 Conflict.
#[tokio::test]
async fn test_delete_bucket_not_empty() {
    let srv = TestServer::start().await;

    // Put an object so the default bucket is non-empty
    srv.client
        .put(&srv.object_url("del-ne-obj.txt"))
        .body("data")
        .send()
        .await
        .unwrap();

    let resp = srv.client.delete(&srv.bucket_url()).send().await.unwrap();
    assert_eq!(
        resp.status(),
        409,
        "Deleting non-empty bucket should be 409 Conflict"
    );
}

/// DeleteBucket on a non-existent bucket should return 404.
#[tokio::test]
async fn test_delete_bucket_not_found() {
    let srv = TestServer::start().await;
    let url = format!("{}/no-such-bucket-xyz", srv.base_url);
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        404,
        "Deleting non-existent bucket should return 404"
    );
}

/// Create a bucket, confirm it exists, then delete it successfully.
#[tokio::test]
async fn test_create_then_delete_empty_bucket() {
    let srv = TestServer::start().await;
    let url = format!("{}/ephemeral-bucket", srv.base_url);

    let resp = srv.client.put(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "Create bucket should succeed");

    let resp = srv.client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200, "Bucket should exist");

    let resp = srv.client.delete(&url).send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "Delete empty bucket should succeed: {}",
        resp.status()
    );
}

// ===========================================================================
// Metadata / header round-trips
// ===========================================================================

/// Content-Encoding and Content-Language should round-trip through PUT/GET.
#[tokio::test]
async fn test_content_encoding_and_language() {
    let srv = TestServer::start().await;
    let url = srv.object_url("meta/encoding.bin");

    srv.client
        .put(&url)
        .header("content-encoding", "gzip")
        .header("content-language", "en-US")
        .body("compressed-ish")
        .send()
        .await
        .unwrap();

    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let enc = resp
        .headers()
        .get("content-encoding")
        .map(|v| v.to_str().unwrap().to_string());
    let lang = resp
        .headers()
        .get("content-language")
        .map(|v| v.to_str().unwrap().to_string());

    assert_eq!(enc.as_deref(), Some("gzip"), "Content-Encoding should round-trip");
    assert_eq!(lang.as_deref(), Some("en-US"), "Content-Language should round-trip");
}

// ===========================================================================
// Error path mapping (NoSpace -> S3 status)
// ===========================================================================

/// Writing an object larger than the available space should return an error
/// (not panic or hang).
#[tokio::test]
async fn test_no_space_returns_error() {
    // 34 MB store -- index takes ~32 MB, leaving ~2 MB for data.
    let srv = TestServer::start_with_size_mb(34).await;

    // First write: fill most of the space with a ~1.5 MB object
    let big = vec![b'A'; 1_500_000];
    let resp = srv
        .client
        .put(&srv.object_url("fill.bin"))
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "First object should fit");

    // Second write: another 1.5 MB should fail (no space)
    let big2 = vec![b'B'; 1_500_000];
    let resp = srv
        .client
        .put(&srv.object_url("overflow.bin"))
        .body(big2)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_server_error() || resp.status().as_u16() == 500,
        "NoSpace should map to server error, got {}",
        resp.status()
    );
}
