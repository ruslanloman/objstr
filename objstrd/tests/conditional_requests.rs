//! Tests for S3 conditional request headers.
//!
//! Covers: If-Modified-Since, If-Unmodified-Since, If-Match, If-None-Match.

mod common;
use common::TestServer;

// ===========================================================================
// If-Modified-Since
// ===========================================================================

/// If-Modified-Since: should return 304 when object was not modified after the date.
#[tokio::test]
async fn test_if_modified_since_304() {
    let srv = TestServer::start().await;
    let url = srv.object_url("cond/ims.txt");

    srv.client.put(&url).body("hello").send().await.unwrap();

    // HEAD to get Last-Modified
    let resp = srv.client.head(&url).send().await.unwrap();
    let lm = resp
        .headers()
        .get("last-modified")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("HEAD should return Last-Modified");

    // Wait a moment to ensure the date is in the past
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // GET with If-Modified-Since set to the object's last-modified (or later)
    // should return 304
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

/// If-Modified-Since: should return 200 when object was modified after the date.
#[tokio::test]
async fn test_if_modified_since_200() {
    let srv = TestServer::start().await;
    let url = srv.object_url("cond/ims200.txt");

    srv.client.put(&url).body("hello").send().await.unwrap();

    // Use a date far in the past
    let resp = srv
        .client
        .get(&url)
        .header("if-modified-since", "Thu, 01 Jan 2000 00:00:00 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "GET with If-Modified-Since in the past should return 200"
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");
}

// ===========================================================================
// If-Unmodified-Since
// ===========================================================================

/// If-Unmodified-Since: should return 412 when object was modified after the date.
#[tokio::test]
async fn test_if_unmodified_since_412() {
    let srv = TestServer::start().await;
    let url = srv.object_url("cond/ius.txt");

    srv.client.put(&url).body("hello").send().await.unwrap();

    // Use a date far in the past -- object was modified after this
    let resp = srv
        .client
        .get(&url)
        .header("if-unmodified-since", "Thu, 01 Jan 2000 00:00:00 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        412,
        "GET with If-Unmodified-Since in the past should return 412"
    );
}

/// If-Unmodified-Since: should return 200 when object was not modified after the date.
#[tokio::test]
async fn test_if_unmodified_since_200() {
    let srv = TestServer::start().await;
    let url = srv.object_url("cond/ius200.txt");

    srv.client.put(&url).body("hello").send().await.unwrap();

    // Use a date far in the future
    let resp = srv
        .client
        .get(&url)
        .header("if-unmodified-since", "Thu, 01 Jan 2099 00:00:00 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");
}

// ===========================================================================
// If-Match
// ===========================================================================

/// If-Match: should return 200 when ETag matches.
#[tokio::test]
async fn test_if_match_200() {
    let srv = TestServer::start().await;
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

/// If-Match: should return 412 when ETag does not match.
#[tokio::test]
async fn test_if_match_412() {
    let srv = TestServer::start().await;
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
        "GET with non-matching If-Match should return 412"
    );
}

// ===========================================================================
// If-None-Match
// ===========================================================================

/// If-None-Match: should return 304 when ETag matches.
#[tokio::test]
async fn test_if_none_match_304() {
    let srv = TestServer::start().await;
    let url = srv.object_url("cond/inm.txt");

    let resp = srv.client.put(&url).body("data").send().await.unwrap();
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
        "GET with matching If-None-Match should return 304"
    );
}

/// If-None-Match: should return 200 when ETag does not match.
#[tokio::test]
async fn test_if_none_match_200() {
    let srv = TestServer::start().await;
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
