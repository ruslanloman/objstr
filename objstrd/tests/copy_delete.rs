//! M4 Tests -- CopyObject and batch DeleteObjects.

mod common;
use common::{extract_xml_tag, TestServer};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Count occurrences of a substring.
fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

/// Build a DeleteObjects XML body from a list of keys.
fn delete_xml(keys: &[&str]) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>");
    for key in keys {
        xml.push_str(&format!("<Object><Key>{}</Key></Object>", key));
    }
    xml.push_str("</Delete>");
    xml
}

// ===========================================================================
// CopyObject
// ===========================================================================

/// PUT with x-amz-copy-source copies an existing object to a new key.
#[tokio::test]
async fn test_copy_object_basic() {
    let srv = TestServer::start().await;

    // Seed source
    srv.client
        .put(&srv.object_url("src.txt"))
        .body("copy me")
        .send()
        .await
        .unwrap();

    // Copy
    let resp = srv
        .client
        .put(&srv.object_url("dst.txt"))
        .header("x-amz-copy-source", "/data/src.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "CopyObject should return 200");

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<CopyObjectResult"),
        "Response should contain CopyObjectResult: {}",
        body
    );

    // GET copy
    let resp = srv
        .client
        .get(&srv.object_url("dst.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        b"copy me",
        "Copied object content should match source"
    );
}

/// Copy should preserve original content; source should remain readable.
#[tokio::test]
async fn test_copy_preserves_content() {
    let srv = TestServer::start().await;
    let content = vec![0xCA; 4096];

    srv.client
        .put(&srv.object_url("original.bin"))
        .body(content.clone())
        .send()
        .await
        .unwrap();

    // Copy
    srv.client
        .put(&srv.object_url("clone.bin"))
        .header("x-amz-copy-source", "/data/original.bin")
        .send()
        .await
        .unwrap();

    // Both should be readable with identical content
    let resp = srv
        .client
        .get(&srv.object_url("original.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content.as_slice());

    let resp = srv
        .client
        .get(&srv.object_url("clone.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content.as_slice());
}

/// CopyObject response should include ETag and LastModified.
#[tokio::test]
async fn test_copy_verify_etag() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("etag-src.txt"))
        .body("etag test")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("etag-dst.txt"))
        .header("x-amz-copy-source", "/data/etag-src.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<ETag>"),
        "CopyObjectResult should include ETag: {}",
        body
    );
    assert!(
        body.contains("<LastModified>"),
        "CopyObjectResult should include LastModified: {}",
        body
    );
}

/// Copy from a key that does not exist should return 404.
#[tokio::test]
async fn test_copy_source_not_found() {
    let srv = TestServer::start().await;

    let resp = srv
        .client
        .put(&srv.object_url("copy-ghost.txt"))
        .header("x-amz-copy-source", "/data/no-such-key.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "Copy from non-existent source should 404"
    );
}

/// Copy an object to the same key without metadata change is illegal per S3 spec.
/// The server must return 400 (InvalidRequest).
#[tokio::test]
async fn test_copy_object_same_key() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("self.txt"))
        .body("self copy")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("self.txt"))
        .header("x-amz-copy-source", "/data/self.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "Self-copy without metadata replace should return 400 InvalidRequest per S3 spec"
    );
}

/// Self-copy with metadata-directive: REPLACE is valid and should succeed.
#[tokio::test]
async fn test_copy_object_same_key_replace() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("self-replace.txt"))
        .body("replace me")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("self-replace.txt"))
        .header("x-amz-copy-source", "/data/self-replace.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Self-copy with REPLACE should succeed");

    // Verify content is intact
    let resp = srv
        .client
        .get(&srv.object_url("self-replace.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"replace me");
}

/// Copy to a nested key path should work.
#[tokio::test]
async fn test_copy_to_nested_key() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("flat.txt"))
        .body("flat content")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("deep/nested/copy.txt"))
        .header("x-amz-copy-source", "/data/flat.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv
        .client
        .get(&srv.object_url("deep/nested/copy.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"flat content");
}

/// Copy should overwrite the destination if it already exists.
#[tokio::test]
async fn test_copy_overwrites_existing() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("src-ow.txt"))
        .body("new content")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("dst-ow.txt"))
        .body("old content")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("dst-ow.txt"))
        .header("x-amz-copy-source", "/data/src-ow.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv
        .client
        .get(&srv.object_url("dst-ow.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        b"new content",
        "Copy should overwrite destination"
    );
}

// ===========================================================================
// DeleteObjects (batch)
// ===========================================================================

/// POST /{bucket}?delete with 3 keys should delete all and return results.
#[tokio::test]
async fn test_delete_objects_basic() {
    let srv = TestServer::start().await;

    // Seed 3 objects
    for i in 0..3 {
        srv.client
            .put(&srv.object_url(&format!("batch/{}.txt", i)))
            .body(format!("content-{}", i))
            .send()
            .await
            .unwrap();
    }

    // Batch delete
    let url = format!("{}?delete", srv.bucket_url());
    let body = delete_xml(&["batch/0.txt", "batch/1.txt", "batch/2.txt"]);
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "DeleteObjects should return 200");

    let xml = resp.text().await.unwrap();
    assert!(
        xml.contains("<DeleteResult"),
        "Response should contain DeleteResult: {}",
        xml
    );
    assert_eq!(
        count_occurrences(&xml, "<Deleted>"),
        3,
        "Should have 3 Deleted entries: {}",
        xml
    );

    // All should be gone
    for i in 0..3 {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("batch/{}.txt", i)))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404, "Deleted object {} should 404", i);
    }
}

/// Batch delete with some keys that don't exist should still succeed.
#[tokio::test]
async fn test_delete_objects_partial_missing() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("exists.txt"))
        .body("here")
        .send()
        .await
        .unwrap();

    let url = format!("{}?delete", srv.bucket_url());
    let body = delete_xml(&["exists.txt", "ghost1.txt", "ghost2.txt"]);
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let xml = resp.text().await.unwrap();
    assert_eq!(
        count_occurrences(&xml, "<Deleted>"),
        3,
        "All keys (including missing) should be Deleted: {}",
        xml
    );

    let resp = srv
        .client
        .get(&srv.object_url("exists.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// Batch delete with an empty list. s3s validates the XML and returns 400
/// for empty Delete requests (MalformedXML), which is valid S3 behavior.
#[tokio::test]
async fn test_delete_objects_empty_list() {
    let srv = TestServer::start().await;

    let url = format!("{}?delete", srv.bucket_url());
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete></Delete>";
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert!(
        status == 200 || status == 400,
        "Empty delete should return 200 or 400, got {}",
        status
    );
}

/// Batch delete should not affect other objects.
#[tokio::test]
async fn test_delete_objects_leaves_others() {
    let srv = TestServer::start().await;

    for i in 0..4 {
        srv.client
            .put(&srv.object_url(&format!("keep/{}.txt", i)))
            .body(format!("data-{}", i))
            .send()
            .await
            .unwrap();
    }

    let url = format!("{}?delete", srv.bucket_url());
    let body = delete_xml(&["keep/1.txt", "keep/3.txt"]);
    srv.client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();

    // 0 and 2 should still exist
    for i in [0, 2] {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("keep/{}.txt", i)))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "Object {} should survive", i);
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            format!("data-{}", i).as_bytes()
        );
    }

    // 1 and 3 should be gone
    for i in [1, 3] {
        let resp = srv
            .client
            .get(&srv.object_url(&format!("keep/{}.txt", i)))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404, "Object {} should be deleted", i);
    }
}

/// DeleteResult should echo each deleted key.
#[tokio::test]
async fn test_delete_objects_result_keys() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("echo/a.txt"))
        .body("a")
        .send()
        .await
        .unwrap();
    srv.client
        .put(&srv.object_url("echo/b.txt"))
        .body("b")
        .send()
        .await
        .unwrap();

    let url = format!("{}?delete", srv.bucket_url());
    let body = delete_xml(&["echo/a.txt", "echo/b.txt"]);
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();

    let xml = resp.text().await.unwrap();
    assert!(
        xml.contains("<Key>echo/a.txt</Key>"),
        "Should echo key a: {}",
        xml
    );
    assert!(
        xml.contains("<Key>echo/b.txt</Key>"),
        "Should echo key b: {}",
        xml
    );
}

// ===========================================================================
// CopyObject etag persistence when source has no metadata
// ===========================================================================

/// PUT an object with NO custom metadata, then COPY it. The copy's
/// HEAD should return the same etag as the CopyObject response.
#[tokio::test]
async fn test_copy_no_metadata_preserves_etag() {
    let srv = TestServer::start().await;

    // PUT without any custom headers (minimal request)
    srv.client
        .put(&srv.object_url("plain.txt"))
        .body("no metadata here")
        .send()
        .await
        .unwrap();

    // COPY (default mode, no metadata-directive)
    let resp = srv
        .client
        .put(&srv.object_url("plain-copy.txt"))
        .header("x-amz-copy-source", "/data/plain.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let copy_body = resp.text().await.unwrap();
    let copy_etag = extract_xml_tag(&copy_body, "ETag").expect("CopyObject should return ETag");

    // HEAD the copy -- etag should match
    let resp = srv
        .client
        .head(&srv.object_url("plain-copy.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let head_etag = resp
        .headers()
        .get("etag")
        .expect("HEAD should return etag")
        .to_str()
        .unwrap();
    // Strip surrounding quotes for comparison
    let head_etag_clean = head_etag.trim_matches('"');
    let copy_etag_clean = copy_etag.trim_matches('"');
    assert_eq!(
        head_etag_clean, copy_etag_clean,
        "HEAD etag should match CopyObject etag"
    );
}

/// COPY REPLACE on an object with no prior metadata should also have
/// a valid etag on the destination HEAD.
#[tokio::test]
async fn test_copy_replace_no_source_metadata() {
    let srv = TestServer::start().await;

    srv.client
        .put(&srv.object_url("bare.txt"))
        .body("bare bones")
        .send()
        .await
        .unwrap();

    let resp = srv
        .client
        .put(&srv.object_url("bare-copy.txt"))
        .header("x-amz-copy-source", "/data/bare.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("x-amz-meta-added", "yes")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = srv
        .client
        .head(&srv.object_url("bare-copy.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("etag").is_some(),
        "HEAD on REPLACE copy should have etag"
    );
    assert_eq!(
        resp.headers()
            .get("x-amz-meta-added")
            .unwrap()
            .to_str()
            .unwrap(),
        "yes"
    );
}
