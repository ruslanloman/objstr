//! E2E tests for conditional S3 requests on a distributed (sharded) backend.
//!
//! The existing conditional_requests.rs tests run against a single raw store.
//! This file verifies the same semantics hold when the S3 adapter sits on
//! top of a 3-shard ShardedObjectStore with RF=2.

mod common;
use common::DistributedTestServer;

// ===========================================================================
// If-None-Match on GET (ETag-based caching)
// ===========================================================================

/// PUT then GET with matching If-None-Match returns 304 through the
/// distributed backend.
#[tokio::test]
async fn distributed_if_none_match_304() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/inm.txt");

    let resp = srv.client.put(&url).body("data").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("PUT should return ETag");

    let resp = srv
        .client
        .get(&url)
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        304,
        "GET with matching If-None-Match should return 304 on sharded backend"
    );
}

/// GET with non-matching If-None-Match returns 200 through the
/// distributed backend.
#[tokio::test]
async fn distributed_if_none_match_200() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/inm200.txt");

    srv.client.put(&url).body("data").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("if-none-match", "\"wrong-etag\"")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"data");
}

// ===========================================================================
// If-Match on GET (ETag validation)
// ===========================================================================

/// GET with matching If-Match returns 200 through the distributed backend.
#[tokio::test]
async fn distributed_if_match_200() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/im.txt");

    let resp = srv.client.put(&url).body("match me").send().await.unwrap();
    let etag = resp
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("PUT should return ETag");

    let resp = srv
        .client
        .get(&url)
        .header("if-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"match me");
}

/// GET with non-matching If-Match returns 412 through the distributed backend.
#[tokio::test]
async fn distributed_if_match_412() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/im412.txt");

    srv.client.put(&url).body("data").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("if-match", "\"wrong-etag\"")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        412,
        "GET with non-matching If-Match should return 412 on sharded backend"
    );
}

// ===========================================================================
// If-Modified-Since on GET
// ===========================================================================

/// GET with If-Modified-Since matching object mtime returns 304.
#[tokio::test]
async fn distributed_if_modified_since_304() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/ims.txt");

    srv.client.put(&url).body("hello").send().await.unwrap();

    // HEAD to get Last-Modified
    let resp = srv.client.head(&url).send().await.unwrap();
    let lm = resp
        .headers()
        .get("last-modified")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("HEAD should return Last-Modified");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let resp = srv
        .client
        .get(&url)
        .header("if-modified-since", &lm)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        304,
        "GET with If-Modified-Since >= object mtime should return 304"
    );
}

/// GET with If-Modified-Since in the past returns 200.
#[tokio::test]
async fn distributed_if_modified_since_200() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/ims200.txt");

    srv.client.put(&url).body("hello").send().await.unwrap();

    let resp = srv
        .client
        .get(&url)
        .header("if-modified-since", "Thu, 01 Jan 2000 00:00:00 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");
}

// ===========================================================================
// Overwrite and ETag change detection
// ===========================================================================

/// Overwriting an object changes its ETag; If-None-Match with old ETag
/// returns 200 (confirming the new version is different).
#[tokio::test]
async fn distributed_overwrite_changes_etag() {
    let srv = DistributedTestServer::start(3, 2, "data").await;
    let url = srv.object_url("cond/overwrite.txt");

    let resp = srv.client.put(&url).body("v1").send().await.unwrap();
    let etag_v1 = resp
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("PUT v1 should return ETag");

    let resp = srv.client.put(&url).body("v2").send().await.unwrap();
    let etag_v2 = resp
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("PUT v2 should return ETag");

    assert_ne!(etag_v1, etag_v2, "different content should produce different ETags");

    // GET with old ETag should return 200 (not 304).
    let resp = srv
        .client
        .get(&url)
        .header("if-none-match", &etag_v1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "stale ETag should not match after overwrite"
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"v2");
}
