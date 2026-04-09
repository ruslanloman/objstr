//! M5 Tests -- Bucket persistence via JSON file.
//!
//! Verifies that bucket names survive server restarts by being saved to a
//! JSON sidecar file and reloaded (or rebuilt from the index) on startup.

mod common;
use common::{extract_xml_tags, TestServer};

// ---------------------------------------------------------------------------
//  Helpers
// ---------------------------------------------------------------------------

/// Parse the buckets JSON file and return the sorted list.
fn read_buckets_json(path: &std::path::Path) -> Vec<String> {
    let data = std::fs::read(path).expect("buckets JSON should exist");
    let mut list: Vec<String> = serde_json::from_slice(&data).expect("valid JSON array");
    list.sort();
    list
}

/// Query ListBuckets and return sorted bucket names from the XML response.
async fn list_bucket_names(srv: &TestServer) -> Vec<String> {
    let resp = srv.client.get(&srv.base_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let mut names = extract_xml_tags(&body, "Name");
    let mut owned: Vec<String> = names.drain(..).map(|s| s.to_string()).collect();
    owned.sort();
    owned
}

// ===========================================================================
//  Tests
// ===========================================================================

/// Basic flow: start server, create buckets, save JSON, verify contents.
#[tokio::test]
async fn test_bucket_json_written_on_save() {
    let srv = TestServer::start_with_bucket("alpha").await;

    // Create a second bucket via S3 API
    let url = format!("{}/bravo", srv.base_url);
    let resp = srv.client.put(&url).send().await.unwrap();
    assert!(resp.status().is_success(), "CreateBucket bravo failed: {}", resp.status());

    // Put an object so the bucket is not empty
    let obj_url = format!("{}/bravo/hello.txt", srv.base_url);
    let resp = srv.client.put(&obj_url).body("world").send().await.unwrap();
    assert!(resp.status().is_success());

    // JSON should not exist yet (no flush triggered)
    assert!(!srv.buckets_json_path.exists(), "JSON should not exist before save");

    // Trigger save
    srv.save_buckets_json().await;

    // Verify JSON contents
    let buckets = read_buckets_json(&srv.buckets_json_path);
    assert_eq!(buckets, vec!["alpha", "bravo"]);

    // Verify ListBuckets S3 API matches
    let api_buckets = list_bucket_names(&srv).await;
    assert_eq!(api_buckets, vec!["alpha", "bravo"]);
}

/// Delete a bucket, save, verify it is removed from JSON.
#[tokio::test]
async fn test_bucket_json_reflects_delete() {
    let srv = TestServer::start_with_bucket("keep").await;

    // Create then delete a bucket
    let url = format!("{}/gone", srv.base_url);
    let resp = srv.client.put(&url).send().await.unwrap();
    assert!(resp.status().is_success());
    srv.save_buckets_json().await;
    let before = read_buckets_json(&srv.buckets_json_path);
    assert!(before.contains(&"gone".to_string()));

    // Delete bucket "gone" (it is empty so this should succeed)
    let resp = srv.client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), 204, "DeleteBucket should 204");

    // Save again
    srv.save_buckets_json().await;
    let after = read_buckets_json(&srv.buckets_json_path);
    assert!(!after.contains(&"gone".to_string()), "deleted bucket should be absent");
    assert!(after.contains(&"keep".to_string()));
}

/// Simulate restart: save JSON, start new server on same image,
/// verify all buckets are recovered from JSON.
#[tokio::test]
async fn test_restart_loads_from_json() {
    let srv1 = TestServer::start_with_bucket("first").await;

    // Create another bucket with data
    let url = format!("{}/second", srv1.base_url);
    let resp = srv1.client.put(&url).send().await.unwrap();
    assert!(resp.status().is_success());
    let obj_url = format!("{}/second/file.txt", srv1.base_url);
    let resp = srv1.client.put(&obj_url).body("data").send().await.unwrap();
    assert!(resp.status().is_success());

    // Flush index and save buckets JSON
    srv1.raw_store.as_ref().unwrap().flush_index().unwrap();
    srv1.save_buckets_json().await;

    let image_path = srv1.image_path.clone();
    // Take ownership of the temp dir so it outlives srv1's drop.
    let mut srv1 = srv1;
    let _keep_dir = srv1._tmp.take();
    drop(srv1);

    // "Restart" -- new server on same image, same default bucket
    let srv2 = TestServer::start_on_image(&image_path, "first").await;

    let api_buckets = list_bucket_names(&srv2).await;
    assert_eq!(api_buckets, vec!["first", "second"]);

    // Verify the object in "second" is still accessible
    let obj_url2 = format!("{}/second/file.txt", srv2.base_url);
    let resp = srv2.client.get(&obj_url2).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "data");
}

/// Delete the JSON file, restart, verify buckets are rebuilt from index.
#[tokio::test]
async fn test_rebuild_from_index_when_json_missing() {
    let srv1 = TestServer::start_with_bucket("alpha").await;

    // Create "bravo" with an object
    let url = format!("{}/bravo", srv1.base_url);
    let resp = srv1.client.put(&url).send().await.unwrap();
    assert!(resp.status().is_success());
    let obj_url = format!("{}/bravo/test.dat", srv1.base_url);
    let resp = srv1.client.put(&obj_url).body("123").send().await.unwrap();
    assert!(resp.status().is_success());

    // Flush and save JSON
    srv1.raw_store.as_ref().unwrap().flush_index().unwrap();
    srv1.save_buckets_json().await;

    let image_path = srv1.image_path.clone();
    let json_path = srv1.buckets_json_path.clone();

    // Remove the JSON file
    std::fs::remove_file(&json_path).unwrap();
    assert!(!json_path.exists());

    // Take ownership of the temp dir so it outlives srv1's drop.
    let mut srv1 = srv1;
    let _keep_dir = srv1._tmp.take();
    drop(srv1);

    // Restart -- should rebuild from index
    let srv2 = TestServer::start_on_image(&image_path, "alpha").await;

    let api_buckets = list_bucket_names(&srv2).await;
    assert!(api_buckets.contains(&"alpha".to_string()), "alpha missing: {:?}", api_buckets);
    assert!(api_buckets.contains(&"bravo".to_string()), "bravo missing: {:?}", api_buckets);
}

/// Move (rename) the JSON file away, restart, save again, compare contents.
#[tokio::test]
async fn test_move_json_rebuild_and_compare() {
    let srv1 = TestServer::start_with_bucket("one").await;

    // Create "two" and "three" with objects
    for name in &["two", "three"] {
        let url = format!("{}/{name}", srv1.base_url);
        let resp = srv1.client.put(&url).send().await.unwrap();
        assert!(resp.status().is_success());
        let obj_url = format!("{}/{name}/obj.bin", srv1.base_url);
        let resp = srv1.client.put(&obj_url).body("x").send().await.unwrap();
        assert!(resp.status().is_success());
    }

    srv1.raw_store.as_ref().unwrap().flush_index().unwrap();
    srv1.save_buckets_json().await;

    let image_path = srv1.image_path.clone();
    let json_path = srv1.buckets_json_path.clone();

    let original = read_buckets_json(&json_path);

    // Move JSON away
    let backup = json_path.with_extension("json.bak");
    std::fs::rename(&json_path, &backup).unwrap();
    assert!(!json_path.exists());

    // Take ownership of the temp dir so it outlives srv1's drop.
    let mut srv1 = srv1;
    let _keep_dir = srv1._tmp.take();
    drop(srv1);

    // Restart (rebuilds from index), then save
    let srv2 = TestServer::start_on_image(&image_path, "one").await;
    srv2.save_buckets_json().await;

    let rebuilt = read_buckets_json(&json_path);

    // The rebuilt list should match the original
    assert_eq!(rebuilt, original, "rebuilt list should match original");
}

/// Verify JSON is valid JSON with sorted unique bucket names.
#[tokio::test]
async fn test_json_format() {
    let srv = TestServer::start_with_bucket("zz-last").await;

    // Create buckets in reverse-alpha order
    for name in &["mm-mid", "aa-first"] {
        let url = format!("{}/{name}", srv.base_url);
        let resp = srv.client.put(&url).send().await.unwrap();
        assert!(resp.status().is_success());
    }

    srv.save_buckets_json().await;

    // Read raw JSON and verify it is sorted
    let raw = std::fs::read_to_string(&srv.buckets_json_path).unwrap();
    let parsed: Vec<String> = serde_json::from_str(&raw).unwrap();
    assert_eq!(parsed, vec!["aa-first", "mm-mid", "zz-last"]);

    // Verify pretty-printed (contains newlines)
    assert!(raw.contains('\n'), "JSON should be pretty-printed");
}

/// Create bucket, put+delete objects to dirty the index, flush, verify
/// bucket still in JSON after objects are gone (empty bucket persists).
#[tokio::test]
async fn test_empty_bucket_persists() {
    let srv = TestServer::start_with_bucket("persistent").await;

    // Create another bucket, put an object, then delete the object
    let bkt_url = format!("{}/ephemeral-data", srv.base_url);
    let resp = srv.client.put(&bkt_url).send().await.unwrap();
    assert!(resp.status().is_success());

    let obj_url = format!("{}/ephemeral-data/temp.txt", srv.base_url);
    let resp = srv.client.put(&obj_url).body("temp").send().await.unwrap();
    assert!(resp.status().is_success());

    let resp = srv.client.delete(&obj_url).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // Flush and save
    srv.raw_store.as_ref().unwrap().flush_index().unwrap();
    srv.save_buckets_json().await;

    let buckets = read_buckets_json(&srv.buckets_json_path);
    assert!(
        buckets.contains(&"ephemeral-data".to_string()),
        "empty bucket should still be in JSON: {:?}",
        buckets
    );
}
