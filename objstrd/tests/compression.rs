//! Compression end-to-end tests: Zstd PUT/GET/HEAD round-trip through S3,
//! range reads on compressed objects, small-object passthrough, and metadata
//! survival across compression.

mod common;
use common::TestServer;
use rawobjstr::Compression;

// ===========================================================================
// Compression end-to-end
// ===========================================================================

/// PUT a compressible object through S3 on a Zstd-compressed store,
/// GET it back, verify body matches.
#[tokio::test]
async fn test_compression_zstd_put_get_roundtrip() {
    let srv = TestServer::start_with_compression(Compression::Zstd).await;

    // Create a compressible body (> 4096 bytes so compression kicks in).
    let body = "abcdefghij".repeat(1000); // 10 KB of repetitive data

    // PUT
    let resp = srv
        .client
        .put(&srv.object_url("compressed.txt"))
        .header("content-type", "text/plain")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT should succeed");
    let put_etag = resp
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string());
    assert!(put_etag.is_some(), "PUT should return an etag");

    // GET
    let resp = srv
        .client
        .get(&srv.object_url("compressed.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET should succeed");
    let content_length: usize = resp
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(content_length, body.len(), "Content-Length should be uncompressed size");
    let got = resp.text().await.unwrap();
    assert_eq!(got, body, "Body should round-trip through compression");

    // HEAD
    let resp = srv
        .client
        .head(&srv.object_url("compressed.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let head_len: usize = resp
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(head_len, body.len(), "HEAD Content-Length should be uncompressed size");
}

/// Range read on a compressed object should work for objects under the
/// compressed-range-read limit.
#[tokio::test]
async fn test_compression_range_read() {
    let srv = TestServer::start_with_compression(Compression::Zstd).await;

    // 10 KB compressible body
    let body = "0123456789".repeat(1000);

    srv.client
        .put(&srv.object_url("ranged.txt"))
        .body(body.clone())
        .send()
        .await
        .unwrap();

    // Range: bytes=0-9
    let resp = srv
        .client
        .get(&srv.object_url("ranged.txt"))
        .header("range", "bytes=0-9")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "Range read should return 206");
    let got = resp.text().await.unwrap();
    assert_eq!(got, &body[..10], "Range bytes should match");
}

/// Small objects (< 4096 bytes) are stored uncompressed even on a
/// compressed store. Verify round-trip.
#[tokio::test]
async fn test_compression_small_object_passthrough() {
    let srv = TestServer::start_with_compression(Compression::Zstd).await;

    let body = "tiny";
    srv.client
        .put(&srv.object_url("small.txt"))
        .body(body)
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .get(&srv.object_url("small.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), body);
}

/// Compressed store with metadata: metadata should survive compression.
#[tokio::test]
async fn test_compression_with_metadata() {
    let srv = TestServer::start_with_compression(Compression::Zstd).await;

    let body = "metadata test data ".repeat(500); // > 4 KB

    srv.client
        .put(&srv.object_url("meta-compressed.txt"))
        .header("content-type", "application/octet-stream")
        .header("x-amz-meta-color", "blue")
        .body(body.clone())
        .send()
        .await
        .unwrap();

    // HEAD should return metadata
    let resp = srv
        .client
        .head(&srv.object_url("meta-compressed.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("x-amz-meta-color").unwrap().to_str().unwrap(),
        "blue"
    );
    let head_len: usize = resp
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(head_len, body.len(), "HEAD size should be body-only");

    // GET should return body without metadata trailer
    let resp = srv
        .client
        .get(&srv.object_url("meta-compressed.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("x-amz-meta-color").unwrap().to_str().unwrap(),
        "blue"
    );
    let got = resp.text().await.unwrap();
    assert_eq!(got, body);
}
