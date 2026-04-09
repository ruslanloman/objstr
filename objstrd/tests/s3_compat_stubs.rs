//! Tests for S3 compatibility stub endpoints.
//!
//! Covers: ACL stubs (GetObjectAcl, GetBucketAcl), versioning stubs
//! (GetBucketVersioning, PutBucketVersioning). These endpoints return
//! hardcoded or no-op responses for compatibility with S3 clients.

mod common;
use common::TestServer;

// ===========================================================================
// ACL stubs
// ===========================================================================

/// GetObjectAcl should return 200 with a hardcoded owner/grant.
#[tokio::test]
async fn test_get_object_acl() {
    let srv = TestServer::start().await;
    let url = srv.object_url("acl/file.txt");

    srv.client.put(&url).body("data").send().await.unwrap();

    let acl_url = format!("{}?acl", url);
    let resp = srv.client.get(&acl_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Owner>") || body.contains("<AccessControlPolicy"),
        "GetObjectAcl should return ACL XML: {}",
        body
    );
    assert!(
        body.contains("FULL_CONTROL"),
        "Should contain FULL_CONTROL grant: {}",
        body
    );
}

/// GetObjectAcl on non-existent key should return 404.
#[tokio::test]
async fn test_get_object_acl_not_found() {
    let srv = TestServer::start().await;

    let url = format!("{}?acl", srv.object_url("acl/ghost.txt"));
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404, "GetObjectAcl on missing object should 404");
}

/// GetBucketAcl should return 200 with a hardcoded owner/grant.
#[tokio::test]
async fn test_get_bucket_acl() {
    let srv = TestServer::start().await;

    let url = format!("{}?acl", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("FULL_CONTROL"),
        "GetBucketAcl should contain FULL_CONTROL: {}",
        body
    );
}

// ===========================================================================
// Versioning stubs
// ===========================================================================

/// GetBucketVersioning should return 200 (empty/disabled).
#[tokio::test]
async fn test_get_bucket_versioning() {
    let srv = TestServer::start().await;

    let url = format!("{}?versioning", srv.bucket_url());
    let resp = srv.client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

/// PutBucketVersioning should be accepted (but ignored).
#[tokio::test]
async fn test_put_bucket_versioning_accepted() {
    let srv = TestServer::start().await;

    let url = format!("{}?versioning", srv.bucket_url());
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
</VersioningConfiguration>"#;
    let resp = srv
        .client
        .put(&url)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "PutBucketVersioning should be accepted"
    );
}
