//! E2E tests for structured logging via /_admin/logs and --log-file.
//!
//! Verifies:
//! - S3 requests produce log entries at the correct level (info, warn, error)
//! - Admin operations produce log entries
//! - Level filtering works: ?level=warn returns only warn+error
//! - Category filtering works: ?category=requests vs ?category=admin
//! - Text search works: ?q=PUT
//! - Log entry structure has all required fields
//! - --log-file writes tab-delimited entries that match /_admin/logs
//! - Log file contains correct levels for each operation type
//!
//! Uses subprocess pattern (spawns real objstrd binary).

mod subprocess_helpers;

use std::time::Duration;
use subprocess_helpers::*;

/// Helper: fetch logs from /_admin/logs with optional query params.
async fn fetch_logs(
    client: &reqwest::Client,
    base_url: &str,
    query: &str,
) -> serde_json::Value {
    let url = if query.is_empty() {
        format!("{base_url}/_admin/logs")
    } else {
        format!("{base_url}/_admin/logs?{query}")
    };
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200, "/_admin/logs should return 200");
    resp.json().await.unwrap()
}

/// Helper: count entries at a specific level in a logs response.
fn count_level(body: &serde_json::Value, level: &str) -> usize {
    body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["level"].as_str().unwrap().eq_ignore_ascii_case(level))
        .count()
}

/// Helper: verify every entry in the response has the required structure.
fn assert_entry_structure(body: &serde_json::Value) {
    for entry in body["entries"].as_array().unwrap() {
        assert!(entry["time"].is_string(), "entry missing 'time': {entry}");
        assert!(entry["level"].is_string(), "entry missing 'level': {entry}");
        assert!(
            entry["category"].is_string(),
            "entry missing 'category': {entry}"
        );
        assert!(
            entry["service"].is_string(),
            "entry missing 'service': {entry}"
        );
        assert!(
            entry["message"].is_string(),
            "entry missing 'message': {entry}"
        );
    }
}

// =========================================================================
// Tests
// =========================================================================

/// Successful PUT requests should produce info-level log entries.
#[tokio::test]
async fn logs_put_produces_info() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19400);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT 3 objects
    for i in 0..3 {
        let url = format!("{}/testbucket/log-test-{i}.txt", srv.base_url);
        let resp = client.put(&url).body("hello").send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
    }

    // Fetch all logs
    let body = fetch_logs(&client, &srv.base_url, "").await;
    let total = body["total"].as_u64().unwrap();
    assert!(total >= 3, "should have at least 3 log entries, got {total}");

    // Verify structure of every entry
    assert_entry_structure(&body);

    // All PUT entries should be info-level
    let entries = body["entries"].as_array().unwrap();
    let put_entries: Vec<_> = entries
        .iter()
        .filter(|e| {
            e["message"]
                .as_str()
                .unwrap_or("")
                .contains("PUT")
        })
        .collect();
    assert!(
        put_entries.len() >= 3,
        "should have at least 3 PUT entries, got {}",
        put_entries.len()
    );
    for pe in &put_entries {
        assert_eq!(
            pe["level"].as_str().unwrap(),
            "info",
            "PUT 200 should be info level: {pe}"
        );
        assert_eq!(
            pe["category"].as_str().unwrap(),
            "requests",
            "PUT should be in 'requests' category: {pe}"
        );
        assert_eq!(
            pe["service"].as_str().unwrap(),
            "s3",
            "PUT should be in 's3' service: {pe}"
        );
    }
}

/// GET on a non-existent key returns 404, which should be warn-level.
#[tokio::test]
async fn logs_get_404_produces_warn() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19401);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // GET a non-existent key
    let resp = client
        .get(&format!("{}/testbucket/does-not-exist.bin", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    // Fetch only warn-level logs
    let body = fetch_logs(&client, &srv.base_url, "level=warn").await;

    let entries = body["entries"].as_array().unwrap();
    let not_found: Vec<_> = entries
        .iter()
        .filter(|e| {
            e["message"]
                .as_str()
                .unwrap_or("")
                .contains("does-not-exist")
        })
        .collect();
    assert!(
        !not_found.is_empty(),
        "should have warn entry for 404: entries={entries:?}"
    );
    for nf in &not_found {
        assert_eq!(
            nf["level"].as_str().unwrap(),
            "warn",
            "404 should be warn level: {nf}"
        );
    }
}

/// Level filtering: ?level=warn should exclude info entries.
#[tokio::test]
async fn logs_level_filter_excludes_lower() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19402);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate info-level entries (PUTs) and warn-level entries (404s)
    for i in 0..3 {
        let url = format!("{}/testbucket/obj-{i}.txt", srv.base_url);
        client.put(&url).body("data").send().await.unwrap();
    }
    client
        .get(&format!("{}/testbucket/nonexistent", srv.base_url))
        .send()
        .await
        .unwrap();

    // All logs should have both info and warn
    let all = fetch_logs(&client, &srv.base_url, "").await;
    let info_count = count_level(&all, "info");
    let warn_count = count_level(&all, "warn");
    assert!(info_count > 0, "should have info entries");
    assert!(warn_count > 0, "should have warn entries");

    // Filtering by level=warn should exclude info
    let warned = fetch_logs(&client, &srv.base_url, "level=warn").await;
    let info_in_warned = count_level(&warned, "info");
    assert_eq!(
        info_in_warned, 0,
        "level=warn filter should exclude info entries"
    );
    let warn_in_warned = count_level(&warned, "warn");
    assert!(
        warn_in_warned > 0,
        "level=warn filter should include warn entries"
    );

    // Filtering by level=error should exclude info and warn
    let errors = fetch_logs(&client, &srv.base_url, "level=error").await;
    let info_in_errors = count_level(&errors, "info");
    let warn_in_errors = count_level(&errors, "warn");
    assert_eq!(
        info_in_errors, 0,
        "level=error filter should exclude info entries"
    );
    assert_eq!(
        warn_in_errors, 0,
        "level=error filter should exclude warn entries"
    );
}

/// Category filtering: ?category=requests vs ?category=admin.
#[tokio::test]
async fn logs_category_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19403);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate request-category entries (PUTs)
    client
        .put(&format!("{}/testbucket/cat-test.txt", srv.base_url))
        .body("cat-data")
        .send()
        .await
        .unwrap();

    // Generate admin-category entries (flush)
    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Filter by category=requests
    let req_logs = fetch_logs(&client, &srv.base_url, "category=requests").await;
    let req_entries = req_logs["entries"].as_array().unwrap();
    for e in req_entries {
        assert_eq!(
            e["category"].as_str().unwrap(),
            "requests",
            "category=requests filter should only return requests: {e}"
        );
    }

    // Filter by category=admin
    let admin_logs = fetch_logs(&client, &srv.base_url, "category=admin").await;
    let admin_entries = admin_logs["entries"].as_array().unwrap();
    for e in admin_entries {
        assert_eq!(
            e["category"].as_str().unwrap(),
            "admin",
            "category=admin filter should only return admin: {e}"
        );
    }
    assert!(
        !admin_entries.is_empty(),
        "flush should generate an admin log entry"
    );
}

/// Text search: ?q=PUT should return only entries mentioning PUT.
#[tokio::test]
async fn logs_text_search() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19404);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT + GET + flush to create diverse entries
    client
        .put(&format!("{}/testbucket/search-obj.txt", srv.base_url))
        .body("search-data")
        .send()
        .await
        .unwrap();
    client
        .get(&format!("{}/testbucket/search-obj.txt", srv.base_url))
        .send()
        .await
        .unwrap();
    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Search for PUT
    let put_logs = fetch_logs(&client, &srv.base_url, "q=PUT").await;
    let put_entries = put_logs["entries"].as_array().unwrap();
    assert!(!put_entries.is_empty(), "q=PUT should find PUT entries");
    for e in put_entries {
        let msg = e["message"].as_str().unwrap().to_uppercase();
        assert!(msg.contains("PUT"), "q=PUT should only return PUT: {e}");
    }

    // Search for GET
    let get_logs = fetch_logs(&client, &srv.base_url, "q=GET").await;
    let get_entries = get_logs["entries"].as_array().unwrap();
    assert!(!get_entries.is_empty(), "q=GET should find GET entries");
    for e in get_entries {
        let msg = e["message"].as_str().unwrap().to_uppercase();
        assert!(msg.contains("GET"), "q=GET should only return GET: {e}");
    }
}

/// The limit parameter should cap the number of returned entries.
#[tokio::test]
async fn logs_limit_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19405);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate 5 entries
    for i in 0..5 {
        let url = format!("{}/testbucket/lim-{i}.txt", srv.base_url);
        client.put(&url).body("x").send().await.unwrap();
    }

    // limit=2 should return at most 2
    let body = fetch_logs(&client, &srv.base_url, "limit=2").await;
    let returned = body["returned"].as_u64().unwrap();
    assert!(returned <= 2, "limit=2 should return at most 2, got {returned}");
    let total = body["total"].as_u64().unwrap();
    assert!(total >= 5, "should have at least 5 total, got {total}");
}

/// Admin flush produces an admin-category info entry.
#[tokio::test]
async fn logs_flush_produces_admin_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19406);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Trigger flush
    let resp = client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Check admin logs for flush entry
    let body = fetch_logs(&client, &srv.base_url, "category=admin").await;
    let entries = body["entries"].as_array().unwrap();
    let flush_entries: Vec<_> = entries
        .iter()
        .filter(|e| {
            e["message"]
                .as_str()
                .unwrap_or("")
                .contains("flush")
        })
        .collect();
    assert!(
        !flush_entries.is_empty(),
        "should have admin entry for flush: entries={entries:?}"
    );
    for fe in &flush_entries {
        assert_eq!(fe["level"].as_str().unwrap(), "info");
        assert_eq!(fe["category"].as_str().unwrap(), "admin");
    }
}

/// Combined filters: level + category together.
#[tokio::test]
async fn logs_combined_level_and_category() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19407);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate info-level requests
    client
        .put(&format!("{}/testbucket/combo.txt", srv.base_url))
        .body("x")
        .send()
        .await
        .unwrap();

    // Generate warn-level requests (404)
    client
        .get(&format!("{}/testbucket/no-such-key", srv.base_url))
        .send()
        .await
        .unwrap();

    // Generate admin entries (flush)
    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // level=warn + category=requests: only 404s, no PUTs, no admin
    let body = fetch_logs(
        &client,
        &srv.base_url,
        "level=warn&category=requests",
    )
    .await;
    let entries = body["entries"].as_array().unwrap();
    for e in entries {
        let lvl = e["level"].as_str().unwrap();
        assert!(
            lvl == "warn" || lvl == "error",
            "should only have warn/error, got {lvl}"
        );
        assert_eq!(e["category"].as_str().unwrap(), "requests");
    }
}

/// Log entries are returned newest-first.
#[tokio::test]
async fn logs_newest_first() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19408);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server(&config, "node1");
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // PUT two objects with distinct names
    client
        .put(&format!("{}/testbucket/first.txt", srv.base_url))
        .body("1")
        .send()
        .await
        .unwrap();
    // Small delay to ensure distinct timestamps
    tokio::time::sleep(Duration::from_millis(50)).await;
    client
        .put(&format!("{}/testbucket/second.txt", srv.base_url))
        .body("2")
        .send()
        .await
        .unwrap();

    let body = fetch_logs(&client, &srv.base_url, "category=requests").await;
    let entries = body["entries"].as_array().unwrap();

    // Find indices of first and second
    let first_idx = entries
        .iter()
        .position(|e| {
            e["message"]
                .as_str()
                .unwrap_or("")
                .contains("first.txt")
        });
    let second_idx = entries
        .iter()
        .position(|e| {
            e["message"]
                .as_str()
                .unwrap_or("")
                .contains("second.txt")
        });
    if let (Some(fi), Some(si)) = (first_idx, second_idx) {
        assert!(
            si < fi,
            "second.txt (idx {si}) should appear before first.txt (idx {fi}) in newest-first order"
        );
    }
}

// =========================================================================
// Log file tests (--log-file)
// =========================================================================

/// Helper: parse a tab-delimited log line into (timestamp, level, service, category, message).
fn parse_log_line(line: &str) -> Option<(&str, &str, &str, &str, &str)> {
    let parts: Vec<&str> = line.splitn(5, '\t').collect();
    if parts.len() == 5 {
        Some((parts[0], parts[1], parts[2], parts[3], parts[4]))
    } else {
        None
    }
}

/// --log-file should create a file with tab-delimited log entries.
#[tokio::test]
async fn logfile_entries_written() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let log_path = tmp.path().join("test.log");
    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19410);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server_with_args(
        &config,
        "node1",
        &["--log-file", log_path.to_str().unwrap()],
    );
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate some entries
    client
        .put(&format!("{}/testbucket/logfile-obj.txt", srv.base_url))
        .body("file-test")
        .send()
        .await
        .unwrap();
    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();

    // Give a moment for file writes to flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read log file
    let content = std::fs::read_to_string(&log_path)
        .expect("log file should exist");
    assert!(!content.is_empty(), "log file should not be empty");

    let lines: Vec<&str> = content.lines().collect();
    assert!(
        lines.len() >= 2,
        "log file should have at least 2 lines (startup + request), got {}",
        lines.len()
    );

    // Every line should be valid tab-delimited format
    for line in &lines {
        let parsed = parse_log_line(line);
        assert!(
            parsed.is_some(),
            "log line should have 5 tab-delimited fields: {line}"
        );
        let (ts, level, _service, _category, _msg) = parsed.unwrap();
        // Timestamp should look like ISO 8601
        assert!(
            ts.contains('T') && ts.contains('Z'),
            "timestamp should be ISO 8601: {ts}"
        );
        // Level should be a known value
        assert!(
            level == "info" || level == "warn" || level == "error" || level == "debug",
            "unknown log level: {level}"
        );
    }
}

/// --log-file should contain the same entries as /_admin/logs.
#[tokio::test]
async fn logfile_matches_admin_logs() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let log_path = tmp.path().join("match.log");
    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19411);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server_with_args(
        &config,
        "node1",
        &["--log-file", log_path.to_str().unwrap()],
    );
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate entries
    for i in 0..3 {
        let url = format!("{}/testbucket/match-{i}.txt", srv.base_url);
        client.put(&url).body("data").send().await.unwrap();
    }
    client
        .get(&format!("{}/testbucket/no-exist", srv.base_url))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get counts from log file
    let content = std::fs::read_to_string(&log_path).unwrap();
    let file_lines: Vec<&str> = content.lines().collect();

    // Get counts from /_admin/logs (fetch all with high limit)
    let body = fetch_logs(&client, &srv.base_url, "limit=10000").await;
    let api_total = body["total"].as_u64().unwrap() as usize;

    // The file should have the same number of entries as the ring buffer
    assert_eq!(
        file_lines.len(),
        api_total,
        "log file lines ({}) should match /_admin/logs total ({api_total})",
        file_lines.len()
    );

    // The PUT entries in the file should be info level
    let file_puts: Vec<_> = file_lines
        .iter()
        .filter_map(|l| parse_log_line(l))
        .filter(|(_, _, _, _, msg)| msg.contains("PUT"))
        .collect();
    for (_, level, _, _, msg) in &file_puts {
        assert_eq!(
            *level, "info",
            "PUT 200 should be info in log file: {msg}"
        );
    }

    // The 404 entry in the file should be warn level
    let file_404s: Vec<_> = file_lines
        .iter()
        .filter_map(|l| parse_log_line(l))
        .filter(|(_, _, _, _, msg)| msg.contains("no-exist"))
        .collect();
    assert!(!file_404s.is_empty(), "should have 404 entry in log file");
    for (_, level, _, _, msg) in &file_404s {
        assert_eq!(
            *level, "warn",
            "GET 404 should be warn in log file: {msg}"
        );
    }
}

/// --log-file should contain the lifecycle startup entry.
#[tokio::test]
async fn logfile_contains_startup_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let log_path = tmp.path().join("startup.log");
    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19412);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server_with_args(
        &config,
        "node1",
        &["--log-file", log_path.to_str().unwrap()],
    );
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let content = std::fs::read_to_string(&log_path).unwrap();
    let startup_lines: Vec<_> = content
        .lines()
        .filter_map(|l| parse_log_line(l))
        .filter(|(_, _, _, cat, msg)| *cat == "lifecycle" && msg.contains("starting"))
        .collect();

    assert!(
        !startup_lines.is_empty(),
        "log file should contain a lifecycle startup entry"
    );
    let (_, level, service, category, _) = startup_lines[0];
    assert_eq!(level, "info", "startup should be info level");
    assert_eq!(service, "internal", "startup should be internal service");
    assert_eq!(category, "lifecycle", "startup should be lifecycle category");
}

/// Log file should record admin operation entries with correct categories.
#[tokio::test]
async fn logfile_admin_operations() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let log_path = tmp.path().join("admin.log");
    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19413);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server_with_args(
        &config,
        "node1",
        &["--log-file", log_path.to_str().unwrap()],
    );
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Trigger flush and rebuild-index
    client
        .post(&format!("{}/_admin/flush", srv.base_url))
        .send()
        .await
        .unwrap();
    client
        .post(&format!("{}/_admin/rebuild-index", srv.base_url))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let content = std::fs::read_to_string(&log_path).unwrap();
    let admin_lines: Vec<_> = content
        .lines()
        .filter_map(|l| parse_log_line(l))
        .filter(|(_, _, _, cat, _)| *cat == "admin")
        .collect();

    assert!(
        admin_lines.len() >= 2,
        "should have at least 2 admin entries (flush + rebuild), got {}",
        admin_lines.len()
    );

    // Check flush entry exists
    let has_flush = admin_lines
        .iter()
        .any(|(_, _, _, _, msg)| msg.contains("flush"));
    assert!(has_flush, "should have flush admin entry in log file");

    // Check rebuild entry exists
    let has_rebuild = admin_lines
        .iter()
        .any(|(_, _, _, _, msg)| msg.contains("rebuild"));
    assert!(has_rebuild, "should have rebuild admin entry in log file");
}

/// Log file entries should be in chronological order (oldest first).
#[tokio::test]
async fn logfile_chronological_order() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = tmp.path().join("shard0.raw");
    format_raw_image(&shard, 64);

    let log_path = tmp.path().join("order.log");
    let config = tmp.path().join("test.conf");
    let port = portpicker::pick_unused_port().unwrap_or(19414);
    write_single_config(&config, shard.to_str().unwrap(), port);

    let srv = start_server_with_args(
        &config,
        "node1",
        &["--log-file", log_path.to_str().unwrap()],
    );
    let client = build_client();
    assert!(wait_for_server(&srv.base_url, &client, Duration::from_secs(10)).await);

    // Generate several entries
    for i in 0..5 {
        let url = format!("{}/testbucket/order-{i}.txt", srv.base_url);
        client.put(&url).body("x").send().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let content = std::fs::read_to_string(&log_path).unwrap();
    let timestamps: Vec<&str> = content
        .lines()
        .filter_map(|l| parse_log_line(l))
        .map(|(ts, _, _, _, _)| ts)
        .collect();

    // File should be chronological (oldest first = ascending order)
    for window in timestamps.windows(2) {
        assert!(
            window[0] <= window[1],
            "log file should be chronological: {} should come before {}",
            window[0],
            window[1]
        );
    }
}
