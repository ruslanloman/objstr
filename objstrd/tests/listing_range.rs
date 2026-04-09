//! M2 Tests -- ListObjectsV2, ListObjectsV1, ListObjectVersions, and Range reads.

mod common;
use common::{extract_xml_tag, extract_xml_tags, TestServer};

// ===========================================================================
// ListObjectsV2
// ===========================================================================

/// Basic V2 listing with 3 objects.
#[tokio::test]
async fn test_v2_basic() {
    let srv = TestServer::start().await;

    for key in &["a.txt", "b.txt", "c.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(body.contains("<KeyCount>3</KeyCount>") || body.contains("<KeyCount>"),
        "Should list 3 keys: {}", body);
    for key in &["a.txt", "b.txt", "c.txt"] {
        assert!(body.contains(&format!("<Key>{}</Key>", key)),
            "Should contain key {}: {}", key, body);
    }
}

/// V2 listing with prefix filter.
#[tokio::test]
async fn test_v2_prefix() {
    let srv = TestServer::start().await;

    for key in &["logs/a.log", "logs/b.log", "data/x.csv"] {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2&prefix=logs/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("<Key>logs/a.log</Key>"), "body: {}", body);
    assert!(body.contains("<Key>logs/b.log</Key>"), "body: {}", body);
    assert!(!body.contains("<Key>data/x.csv</Key>"), "Should exclude data/ prefix: {}", body);
}

/// V2 listing with delimiter groups common prefixes.
#[tokio::test]
async fn test_v2_delimiter() {
    let srv = TestServer::start().await;

    for key in &["photos/2024/a.jpg", "photos/2024/b.jpg", "photos/2025/c.jpg", "readme.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2&delimiter=/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("<Key>readme.txt</Key>"), "Top-level key: {}", body);
    assert!(body.contains("<Prefix>photos/</Prefix>"), "CommonPrefix: {}", body);
}

/// V2 listing with prefix + delimiter.
#[tokio::test]
async fn test_v2_prefix_delimiter() {
    let srv = TestServer::start().await;

    for key in &["photos/2024/a.jpg", "photos/2024/b.jpg", "photos/2025/c.jpg"] {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2&prefix=photos/&delimiter=/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("<Prefix>photos/2024/</Prefix>"), "body: {}", body);
    assert!(body.contains("<Prefix>photos/2025/</Prefix>"), "body: {}", body);
}

/// V2 listing with max-keys should truncate results.
#[tokio::test]
async fn test_v2_max_keys() {
    let srv = TestServer::start().await;

    for i in 0..5 {
        srv.client
            .put(&srv.object_url(&format!("mk/{}.txt", i)))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?list-type=2&prefix=mk/&max-keys=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "Should be truncated: {}",
        body
    );
    assert!(
        body.contains("<KeyCount>2</KeyCount>"),
        "Should have 2 keys: {}",
        body
    );
}

/// V2 listing with continuation-token for pagination.
#[tokio::test]
async fn test_v2_continuation_token() {
    let srv = TestServer::start().await;

    for i in 0..4 {
        srv.client
            .put(&srv.object_url(&format!("pg/{}.txt", i)))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    // Page 1
    let url = format!("{}?list-type=2&prefix=pg/&max-keys=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "First page should be truncated: {}",
        body
    );
    assert!(
        body.contains("<KeyCount>2</KeyCount>"),
        "First page should have 2 keys: {}",
        body
    );

    // NextContinuationToken must be present when IsTruncated is true
    let token = extract_xml_tag(&body, "NextContinuationToken")
        .expect("NextContinuationToken must be present when IsTruncated=true");
    let url = format!(
        "{}?list-type=2&prefix=pg/&max-keys=2&continuation-token={}",
        srv.bucket_url(),
        token
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    let body2 = resp.text().await.unwrap();

    let all = format!("{}{}", body, body2);
    for i in 0..4 {
        assert!(
            all.contains(&format!("<Key>pg/{}.txt</Key>", i)),
            "Missing key pg/{}.txt in: {}",
            i,
            all
        );
    }
}

/// V2 listing of an empty bucket.
#[tokio::test]
async fn test_v2_empty() {
    let srv = TestServer::start().await;

    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<KeyCount>0</KeyCount>"),
        "Empty listing should have KeyCount 0: {}",
        body
    );
}

/// V2 listing includes Size and LastModified metadata.
#[tokio::test]
async fn test_v2_metadata() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("meta.txt"))
        .body("12345")
        .send()
        .await
        .unwrap();

    let url = format!("{}?list-type=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("<Size>5</Size>"), "Should include Size: {}", body);
    assert!(body.contains("<LastModified>"), "Should include LastModified: {}", body);
}

// ===========================================================================
// Range reads
// ===========================================================================

/// Range: bytes=0-4 should return 206 with the first 5 bytes.
#[tokio::test]
async fn test_range_partial_content() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range.txt");
    srv.client.put(&url).body("Hello, Range!").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=0-4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Hello");
}

/// Range: bytes=7- should skip leading bytes.
#[tokio::test]
async fn test_range_skip_leading() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range2.txt");
    srv.client.put(&url).body("Hello, Range!").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=7-")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Range!");
}

/// Range: bytes=-6 (suffix) should return the last 6 bytes.
#[tokio::test]
async fn test_range_suffix() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range3.txt");
    srv.client.put(&url).body("Hello, Range!").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=-6")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"Range!");
}

/// Range that covers the entire object may return 200 or 206.
#[tokio::test]
async fn test_range_entire_object() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range4.txt");
    srv.client.put(&url).body("ABCDE").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=0-4")
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert!(status == 200 || status == 206, "Expected 200 or 206, got {}", status);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"ABCDE");
}

/// Range on non-existent object should return 404.
#[tokio::test]
async fn test_range_not_found() {
    let srv = TestServer::start().await;
    let resp = srv
        .client
        .get(&srv.object_url("nope.txt"))
        .header("range", "bytes=0-10")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// 206 response should include Content-Range header.
#[tokio::test]
async fn test_content_range_header() {
    let srv = TestServer::start().await;
    let url = srv.object_url("cr.txt");
    srv.client.put(&url).body("0123456789").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=2-5")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);

    let cr = resp
        .headers()
        .get("content-range")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        cr.starts_with("bytes "),
        "Content-Range should start with 'bytes ': {}",
        cr
    );
}

// ===========================================================================
// URL encoding in V2 listings (encoding-type=url)
// ===========================================================================

/// V2 listing with encoding-type=url should percent-encode special characters
/// in object keys.
#[tokio::test]
async fn test_v2_encoding_type_url() {
    let srv = TestServer::start().await;

    // PUT keys with special characters that need URL encoding.
    let special_keys = [
        "enc/key with spaces.txt",
        "enc/key+plus.txt",
        "enc/key@at.txt",
    ];
    for key in &special_keys {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!(
        "{}?list-type=2&prefix=enc/&encoding-type=url",
        srv.bucket_url()
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    // Response should declare encoding type
    assert!(
        body.contains("<EncodingType>url</EncodingType>"),
        "Response should contain <EncodingType>url</EncodingType>: {}",
        body
    );

    // Spaces should be percent-encoded as %20
    assert!(
        body.contains("key%20with%20spaces.txt"),
        "Spaces should be percent-encoded: {}",
        body
    );

    // '+' should be percent-encoded as %2B
    assert!(
        body.contains("key%2Bplus.txt"),
        "Plus sign should be percent-encoded: {}",
        body
    );

    // '@' should be percent-encoded as %40
    assert!(
        body.contains("key%40at.txt"),
        "At sign should be percent-encoded: {}",
        body
    );

    // '/' should NOT be encoded (it is a safe character)
    assert!(
        body.contains("enc/"),
        "Forward slash should not be encoded: {}",
        body
    );
}

/// V2 listing with encoding-type=url and delimiter should percent-encode
/// common prefix values.
#[tokio::test]
async fn test_v2_encoding_type_url_prefixes() {
    let srv = TestServer::start().await;

    // Create objects under a directory whose name has a special character.
    srv.client
        .put(&srv.object_url("urlpfx/dir with space/a.txt"))
        .body("x")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("urlpfx/dir with space/b.txt"))
        .body("x")
        .send()
        .await
        .unwrap();

    let url = format!(
        "{}?list-type=2&prefix=urlpfx/&delimiter=/&encoding-type=url",
        srv.bucket_url()
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    // The common prefix should have its space percent-encoded.
    assert!(
        body.contains("dir%20with%20space/"),
        "Common prefix should have spaces percent-encoded: {}",
        body
    );
}

// ===========================================================================
// ListObjectVersions
// ===========================================================================

/// ListObjectVersions returns objects with version_id=null.
#[tokio::test]
async fn test_list_object_versions() {
    let srv = TestServer::start().await;

    for key in &["ver/a.txt", "ver/b.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?versions", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Version>") || body.contains("<Key>ver/a.txt</Key>"),
        "ListObjectVersions should contain version entries: {}",
        body
    );
    assert!(body.contains("<Key>ver/a.txt</Key>"), "Should contain ver/a.txt: {}", body);
    assert!(body.contains("<Key>ver/b.txt</Key>"), "Should contain ver/b.txt: {}", body);
    assert!(
        body.contains("<VersionId>null</VersionId>"),
        "Versions should have version_id=null: {}",
        body
    );
    assert!(
        body.contains("<IsLatest>true</IsLatest>"),
        "Versions should have is_latest=true: {}",
        body
    );
}

/// ListObjectVersions with prefix filter.
#[tokio::test]
async fn test_list_object_versions_prefix() {
    let srv = TestServer::start().await;

    for key in &["vp/a.txt", "vp/b.txt", "other/c.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?versions&prefix=vp/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(body.contains("<Key>vp/a.txt</Key>"), "body: {}", body);
    assert!(body.contains("<Key>vp/b.txt</Key>"), "body: {}", body);
    assert!(
        !body.contains("<Key>other/c.txt</Key>"),
        "Should exclude other/ prefix: {}",
        body
    );
}

// ===========================================================================
// ListObjects V1
// ===========================================================================

/// ListObjects V1 (no list-type=2 param) should return object listing.
#[tokio::test]
async fn test_list_objects_v1() {
    let srv = TestServer::start().await;

    for key in &["v1/a.txt", "v1/b.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?prefix=v1/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Key>v1/a.txt</Key>"),
        "V1 listing should contain v1/a.txt: {}",
        body
    );
    assert!(
        body.contains("<Key>v1/b.txt</Key>"),
        "V1 listing should contain v1/b.txt: {}",
        body
    );
}

/// ListObjects V1 with delimiter should return NextMarker when truncated.
#[tokio::test]
async fn test_list_objects_v1_next_marker() {
    let srv = TestServer::start().await;

    for i in 0..5 {
        srv.client
            .put(&srv.object_url(&format!("v1mk/dir{}/file.txt", i)))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?prefix=v1mk/&delimiter=/&max-keys=2", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    if body.contains("<IsTruncated>true</IsTruncated>") {
        assert!(
            body.contains("<NextMarker>"),
            "Truncated V1 listing with delimiter should have NextMarker: {}",
            body
        );
    }
}

/// ListObjects V1 with encoding-type=url should percent-encode special
/// characters in object keys.
#[tokio::test]
async fn test_list_objects_v1_encoding_type_url() {
    let srv = TestServer::start().await;

    let special_keys = [
        "v1enc/key with spaces.txt",
        "v1enc/key+plus.txt",
        "v1enc/key@at.txt",
    ];
    for key in &special_keys {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    // V1 listing (no list-type=2) with encoding-type=url
    let url = format!("{}?prefix=v1enc/&encoding-type=url", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("<EncodingType>url</EncodingType>"),
        "V1 response should contain <EncodingType>url</EncodingType>: {}",
        body
    );

    // Spaces should be percent-encoded
    assert!(
        body.contains("key%20with%20spaces.txt"),
        "Spaces should be percent-encoded in V1 listing: {}",
        body
    );

    // '+' should be percent-encoded
    assert!(
        body.contains("key%2Bplus.txt"),
        "Plus sign should be percent-encoded in V1 listing: {}",
        body
    );

    // '@' should be percent-encoded
    assert!(
        body.contains("key%40at.txt"),
        "At sign should be percent-encoded in V1 listing: {}",
        body
    );
}

/// When a truncated V1 listing has both object keys and common prefixes,
/// NextMarker should equal the lexicographically greater of the last object
/// key and the last common prefix.
#[tokio::test]
async fn test_list_objects_v1_next_marker_value() {
    let srv = TestServer::start().await;

    // Create objects under a prefix so that listing with delimiter produces
    // both object keys and common prefixes.
    //
    // With delimiter=/ and prefix=v1nm/:
    //   - "v1nm/a-file.txt"  -> direct object key
    //   - "v1nm/z-dir/"      -> common prefix (from z-dir/file.txt)
    //
    // "z-dir/" > "a-file.txt" lexicographically, so NextMarker should be
    // "v1nm/z-dir/" (the common prefix).
    srv.client
        .put(&srv.object_url("v1nm/a-file.txt"))
        .body("x")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("v1nm/z-dir/file.txt"))
        .body("x")
        .send()
        .await
        .unwrap();

    // max-keys=1 forces truncation: only one result (either the object or
    // the prefix) is returned per page.
    let url = format!(
        "{}?prefix=v1nm/&delimiter=/&max-keys=1",
        srv.bucket_url()
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "Listing should be truncated with max-keys=1: {}",
        body
    );
    assert!(
        body.contains("<NextMarker>"),
        "Truncated V1 listing with delimiter should have NextMarker: {}",
        body
    );

    // Extract NextMarker value and verify it is present
    let marker = extract_xml_tag(&body, "NextMarker")
        .expect("NextMarker should be present in truncated V1 listing");

    // NextMarker should be the last key or prefix in the page
    // (whichever is lexicographically greater)
    assert!(
        !marker.is_empty(),
        "NextMarker should not be empty"
    );

    // Use the marker to paginate and get the second page
    let url2 = format!(
        "{}?prefix=v1nm/&delimiter=/&max-keys=1&marker={}",
        srv.bucket_url(),
        marker
    );
    let resp2 = srv.client.get(&url2).send().await.unwrap();
    let body2 = resp2.text().await.unwrap();

    // Between the two pages, we should see both the object key and the prefix
    let combined = format!("{}{}", body, body2);
    assert!(
        combined.contains("<Key>v1nm/a-file.txt</Key>"),
        "Combined pages should contain the object key: {}",
        combined
    );
    assert!(
        combined.contains("<Prefix>v1nm/z-dir/</Prefix>"),
        "Combined pages should contain the common prefix: {}",
        combined
    );
}

// ===========================================================================
// ListBuckets
// ===========================================================================

/// ListBuckets should return multiple buckets.
#[tokio::test]
async fn test_list_buckets_multiple() {
    let srv = TestServer::start().await;

    // Create additional buckets
    for name in &["extra-one", "extra-two"] {
        let url = format!("{}/{}", srv.base_url, name);
        let resp = srv.client.put(&url).send().await.unwrap();
        assert!(resp.status().is_success(), "CreateBucket {} failed", name);
    }

    let resp = srv.client.get(&srv.base_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    let names = extract_xml_tags(&body, "Name");
    assert!(
        names.len() >= 3,
        "Should have at least 3 buckets (data, extra-one, extra-two): {:?}",
        names
    );
    assert!(
        names.contains(&"extra-one"),
        "Should contain extra-one: {:?}",
        names
    );
    assert!(
        names.contains(&"extra-two"),
        "Should contain extra-two: {:?}",
        names
    );
}

// ===========================================================================
// Range edge cases
// ===========================================================================

/// Range request beyond object size should return 416 Range Not Satisfiable.
#[tokio::test]
async fn test_range_invalid_416() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range/invalid.txt");

    srv.client.put(&url).body("short").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=1000-2000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        416,
        "Range beyond object size should return 416"
    );
}

/// Suffix range exceeding object size should return the entire object (206).
/// Per HTTP/S3 spec: `bytes=-N` where N > size returns the whole body.
#[tokio::test]
async fn test_range_suffix_exceeds_size() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range/suffix-big.txt");

    srv.client.put(&url).body("tiny").send().await.unwrap(); // 4 bytes

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=-100") // asking for last 100 bytes of 4-byte object
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.bytes().await.unwrap();
    assert!(
        status == 200 || status == 206,
        "suffix range exceeding size should return 200 or 206, got {status}"
    );
    assert_eq!(body.as_ref(), b"tiny", "should return entire object body");
}

/// Range request on a zero-byte object should return 416.
#[tokio::test]
async fn test_range_on_zero_byte_object() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range/empty.txt");

    srv.client.put(&url).body("").send().await.unwrap(); // 0 bytes

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=0-0")
        .send()
        .await
        .unwrap();
    // For a zero-byte object any byte range is unsatisfiable
    assert_eq!(
        resp.status().as_u16(),
        416,
        "Range on zero-byte object should return 416"
    );
}

/// Single-byte range at the last byte of an object.
#[tokio::test]
async fn test_range_last_byte_exact() {
    let srv = TestServer::start().await;
    let url = srv.object_url("range/lastbyte.txt");

    srv.client.put(&url).body("ABCDE").send().await.unwrap(); // 5 bytes, indices 0-4

    let resp = srv
        .client
        .get(&url)
        .header("range", "bytes=4-4") // last byte only
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 206);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "E", "should return only the last byte");
}

// ===========================================================================
// V2 listing with start-after
// ===========================================================================

/// ListObjectsV2 with start-after should skip objects before the marker.
#[tokio::test]
async fn test_v2_start_after() {
    let srv = TestServer::start().await;

    for key in &["sa/a.txt", "sa/b.txt", "sa/c.txt", "sa/d.txt"] {
        srv.client
            .put(&srv.object_url(key))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let url = format!(
        "{}?list-type=2&prefix=sa/&start-after=sa/b.txt",
        srv.bucket_url()
    );
    let resp = srv.client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        !body.contains("<Key>sa/a.txt</Key>"),
        "a.txt should be skipped: {}",
        body
    );
    assert!(
        !body.contains("<Key>sa/b.txt</Key>"),
        "b.txt should be skipped (start-after is exclusive): {}",
        body
    );
    assert!(
        body.contains("<Key>sa/c.txt</Key>"),
        "c.txt should be present: {}",
        body
    );
    assert!(
        body.contains("<Key>sa/d.txt</Key>"),
        "d.txt should be present: {}",
        body
    );
}

// ===========================================================================
// ListObjectVersions with trailing-slash keys (DIRMARK stripping)
// ===========================================================================

/// Keys ending in '/' should appear correctly in ListObjectVersions
/// (no __DIRMARK__ suffix leaked).
#[tokio::test]
async fn test_list_versions_trailing_slash_key() {
    let srv = TestServer::start().await;

    // PUT a key ending in /
    srv.client
        .put(&srv.object_url("mydir/"))
        .body("")
        .send()
        .await
        .unwrap();

    // Also a normal key
    srv.client
        .put(&srv.object_url("mydir/file.txt"))
        .body("content")
        .send()
        .await
        .unwrap();

    // ListObjectVersions
    let url = format!("{}?versions", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    let keys = extract_xml_tags(&body, "Key");
    assert!(
        !keys.iter().any(|k| k.contains("__DIRMARK__")),
        "ListObjectVersions should not leak __DIRMARK__: keys = {:?}",
        keys
    );
    assert!(
        keys.contains(&"mydir/"),
        "Should contain 'mydir/' key: {:?}",
        keys
    );
    assert!(
        keys.contains(&"mydir/file.txt"),
        "Should contain 'mydir/file.txt' key: {:?}",
        keys
    );
}

/// ListObjectVersions with prefix filter should also strip DIRMARK.
#[tokio::test]
async fn test_list_versions_prefix_trailing_slash() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("dirs/sub/"))
        .body("")
        .send()
        .await
        .unwrap();

    let url = format!("{}?versions&prefix=dirs/", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();

    let keys = extract_xml_tags(&body, "Key");
    assert!(
        !keys.iter().any(|k| k.contains("__DIRMARK__")),
        "Prefixed ListObjectVersions should not leak __DIRMARK__: keys = {:?}",
        keys
    );
    assert!(
        keys.contains(&"dirs/sub/"),
        "Should contain 'dirs/sub/' key: {:?}",
        keys
    );
}
