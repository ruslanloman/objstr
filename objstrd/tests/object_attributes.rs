//! E2E tests for GetObjectAttributes (S3 API).
//!
//! Verifies that the adapter returns ETag, ObjectSize, StorageClass, and
//! LastModified for simple objects and multipart uploads.
//!
//! NOTE: ObjectParts (per-part manifest) is not yet persisted during
//! CompleteMultipartUpload, so requesting ObjectParts returns None.

mod common;
use common::{complete_xml, extract_xml_tag, TestServer};

// ---------------------------------------------------------------------------
// Helper: send GetObjectAttributes request
// ---------------------------------------------------------------------------

/// Issue `GET /<bucket>/<key>?attributes` with the given list of attribute
/// names (e.g. ["ETag", "ObjectSize", "StorageClass"]).
async fn get_object_attributes(
    srv: &TestServer,
    key: &str,
    attrs: &[&str],
) -> reqwest::Response {
    let url = format!("{}?attributes", srv.object_url(key));
    let mut req = srv.client.get(&url);
    for attr in attrs {
        req = req.header("x-amz-object-attributes", *attr);
    }
    req.send().await.unwrap()
}

// ===========================================================================
// Basic: simple PutObject then GetObjectAttributes
// ===========================================================================

#[tokio::test]
async fn test_get_object_attributes_basic() {
    let srv = TestServer::start().await;
    let key = "attrs/basic.txt";
    let body = "hello attributes";

    // PUT object
    let resp = srv
        .client
        .put(&srv.object_url(key))
        .header("content-type", "text/plain")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GetObjectAttributes -- request all four supported fields
    let resp = get_object_attributes(&srv, key, &["ETag", "ObjectSize", "StorageClass"]).await;
    assert_eq!(resp.status(), 200, "GetObjectAttributes should succeed");

    let xml = resp.text().await.unwrap();

    // ObjectSize must match body length
    let size = extract_xml_tag(&xml, "ObjectSize")
        .expect("ObjectSize missing from response");
    assert_eq!(
        size,
        body.len().to_string(),
        "ObjectSize mismatch"
    );

    // ETag should be present and non-empty
    let etag = extract_xml_tag(&xml, "ETag");
    assert!(etag.is_some(), "ETag missing from response");
    assert!(!etag.unwrap().is_empty(), "ETag should not be empty");

    // StorageClass should default to STANDARD
    let sc = extract_xml_tag(&xml, "StorageClass")
        .expect("StorageClass missing from response");
    assert_eq!(sc, "STANDARD");
}

// ===========================================================================
// Request only ObjectSize
// ===========================================================================

#[tokio::test]
async fn test_get_object_attributes_size_only() {
    let srv = TestServer::start().await;
    let key = "attrs/size-only.bin";
    let body = vec![0xABu8; 1024];

    srv.client
        .put(&srv.object_url(key))
        .body(body.clone())
        .send()
        .await
        .unwrap();

    let resp = get_object_attributes(&srv, key, &["ObjectSize"]).await;
    assert_eq!(resp.status(), 200);

    let xml = resp.text().await.unwrap();
    let size = extract_xml_tag(&xml, "ObjectSize")
        .expect("ObjectSize missing");
    assert_eq!(size, "1024");

    // ETag should NOT be present since we did not request it
    assert!(
        extract_xml_tag(&xml, "ETag").is_none(),
        "ETag should be absent when not requested"
    );
}

// ===========================================================================
// Non-existent object returns NoSuchKey
// ===========================================================================

#[tokio::test]
async fn test_get_object_attributes_not_found() {
    let srv = TestServer::start().await;

    let resp = get_object_attributes(&srv, "does-not-exist.txt", &["ETag", "ObjectSize"]).await;
    assert_eq!(resp.status(), 404, "should return 404 for missing key");

    let xml = resp.text().await.unwrap();
    assert!(
        xml.contains("NoSuchKey"),
        "error body should contain NoSuchKey: {xml}"
    );
}

// ===========================================================================
// Multipart upload -- ObjectSize and ETag work, ObjectParts is None
// ===========================================================================

#[tokio::test]
async fn test_get_object_attributes_multipart() {
    let srv = TestServer::start().await;
    let key = "attrs/multipart.bin";
    let part_body = vec![0x42u8; 5 * 1024 * 1024]; // 5 MB

    // Initiate multipart upload
    let init_url = format!("{}?uploads", srv.object_url(key));
    let resp = srv.client.post(&init_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&xml, "UploadId").expect("no UploadId");

    // Upload single part
    let part_url = format!(
        "{}?partNumber=1&uploadId={}",
        srv.object_url(key),
        upload_id
    );
    let resp = srv
        .client
        .put(&part_url)
        .body(part_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag1 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Complete multipart upload
    let complete_url = format!(
        "{}?uploadId={}",
        srv.object_url(key),
        upload_id
    );
    let complete_body = complete_xml(&[(1, &etag1)]);
    let resp = srv
        .client
        .post(&complete_url)
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GetObjectAttributes
    let resp = get_object_attributes(&srv, key, &["ETag", "ObjectSize", "ObjectParts"]).await;
    assert_eq!(resp.status(), 200);

    let xml = resp.text().await.unwrap();

    // ObjectSize must equal part size
    let size = extract_xml_tag(&xml, "ObjectSize")
        .expect("ObjectSize missing for multipart object");
    assert_eq!(size, part_body.len().to_string());

    // ETag should be present
    assert!(
        extract_xml_tag(&xml, "ETag").is_some(),
        "ETag missing for multipart object"
    );
}

// ===========================================================================
// LastModified is always present
// ===========================================================================

#[tokio::test]
async fn test_get_object_attributes_last_modified() {
    let srv = TestServer::start().await;
    let key = "attrs/lastmod.txt";

    srv.client
        .put(&srv.object_url(key))
        .body("data")
        .send()
        .await
        .unwrap();

    let resp = get_object_attributes(&srv, key, &["ObjectSize"]).await;
    assert_eq!(resp.status(), 200);

    // LastModified is returned as a response header per AWS spec
    let lm = resp.headers().get("last-modified")
        .map(|v| v.to_str().unwrap().to_string());
    assert!(lm.is_some(), "Last-Modified header should always be present");
    assert!(!lm.unwrap().is_empty());
}

// ===========================================================================
// ETag only request
// ===========================================================================

#[tokio::test]
async fn test_get_object_attributes_etag_only() {
    let srv = TestServer::start().await;
    let key = "attrs/etag-only.txt";

    srv.client
        .put(&srv.object_url(key))
        .body("etag test body")
        .send()
        .await
        .unwrap();

    let resp = get_object_attributes(&srv, key, &["ETag"]).await;
    assert_eq!(resp.status(), 200);

    let xml = resp.text().await.unwrap();
    let etag = extract_xml_tag(&xml, "ETag");
    assert!(etag.is_some(), "ETag missing");
    assert!(!etag.unwrap().is_empty());

    // ObjectSize should be absent
    assert!(
        extract_xml_tag(&xml, "ObjectSize").is_none(),
        "ObjectSize should be absent when not requested"
    );
}
