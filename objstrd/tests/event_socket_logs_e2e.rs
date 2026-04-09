//! E2E test: event socket log entries.
//!
//! Starts an objstrd writer with an event socket and verifies that:
//!
//!   1. A subscriber connecting shows up in /_admin/logs.
//!   2. A bad-password attempt shows up in /_admin/logs.
//!   3. A subscriber disconnecting shows up in /_admin/logs.
//!   4. The subscriber count in /_admin/info reflects connections.

mod subprocess_helpers;

use std::io::Write;
use std::process::Command;
use std::time::Duration;
use subprocess_helpers::*;

/// Fetch log entries from /_admin/logs and return the messages.
async fn fetch_log_messages(base: &str, client: &reqwest::Client) -> Vec<String> {
    let resp = client
        .get(&format!("{base}/_admin/logs"))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    json["entries"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|e| e["message"].as_str().map(|s| s.to_string()))
        .collect()
}

/// Connect to event socket, send secret, read response line.
/// Returns the stream (kept alive to hold the connection open).
async fn connect_event_socket(
    sock_path: &str,
    secret: &str,
) -> std::io::Result<tokio::net::UnixStream> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut stream = tokio::net::UnixStream::connect(sock_path).await?;

    // Send secret
    stream
        .write_all(format!("SECRET {secret}\n").as_bytes())
        .await?;

    // Read response (need to read the OK line)
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    // Reassemble
    let stream = reader.into_inner().reunite(write_half).unwrap();
    Ok(stream)
}

/// Connect, send bad secret, read ERR response.
async fn connect_bad_secret(sock_path: &str) -> std::io::Result<String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let stream = tokio::net::UnixStream::connect(sock_path).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half
        .write_all(b"SECRET wrong-password-here\n")
        .await?;

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(line)
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_socket_log_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let event_sock = tmp.path().join("events.sock");
    let port = portpicker::pick_unused_port().unwrap_or(18960);
    let secret = "test-log-secret-12345";

    // -- Config -------------------------------------------------------
    let conf_path = tmp.path().join("writer.conf");
    {
        let mut f = std::fs::File::create(&conf_path).unwrap();
        writeln!(f, "cluster  log-test").unwrap();
        writeln!(f, "bucket   testbucket").unwrap();
        writeln!(f, "").unwrap();
        writeln!(f, "event_socket   {}", event_sock.to_str().unwrap()).unwrap();
        writeln!(f, "event_secret   {}", secret).unwrap();
        writeln!(f, "").unwrap();
        writeln!(
            f,
            "writer  rf=1  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
            port, port
        )
        .unwrap();
        writeln!(f, "  fs  {}", shard_dir.to_str().unwrap()).unwrap();
    }

    // -- Start server -------------------------------------------------
    let child = Command::new(objstrd_bin())
        .args(["--config", conf_path.to_str().unwrap(), "--node", "writer"])
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn objstrd");

    let base = format!("http://127.0.0.1:{port}");
    let _server = ServerProcess { child, base_url: base.clone() };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&base, &client, Duration::from_secs(10)).await,
        "server did not start"
    );

    // Small delay to let event socket bind
    tokio::time::sleep(Duration::from_millis(300)).await;

    // -- Test 1: bad password attempt ---------------------------------
    let response = connect_bad_secret(event_sock.to_str().unwrap()).await.unwrap();
    assert!(
        response.contains("ERR"),
        "expected ERR response for bad secret, got: {}",
        response.trim()
    );

    // Give server time to log the event
    tokio::time::sleep(Duration::from_millis(300)).await;

    let logs = fetch_log_messages(&base, &client).await;
    assert!(
        logs.iter().any(|m| m.contains("authentication failed")),
        "expected auth failure log entry, logs: {:?}",
        logs
    );

    // -- Test 2: successful subscriber connect ------------------------
    let sock_path = event_sock.to_str().unwrap().to_string();
    let subscriber = connect_event_socket(&sock_path, secret).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let logs = fetch_log_messages(&base, &client).await;
    assert!(
        logs.iter().any(|m| m.contains("subscriber connected")),
        "expected subscriber connected log entry, logs: {:?}",
        logs
    );

    // Verify subscriber count in /_admin/info
    let info: serde_json::Value = client
        .get(&format!("{base}/_admin/info"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        info["event_socket_subscribers"].as_u64().unwrap_or(0),
        1,
        "expected 1 subscriber in /_admin/info"
    );

    // -- Test 3: subscriber disconnect --------------------------------
    // Drop the subscriber stream, then trigger a PUT on the server so
    // the event relay loop detects the broken pipe and logs disconnect.
    drop(subscriber);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger an event so the server tries to write to the disconnected subscriber.
    let resp = client
        .put(&format!("{base}/testbucket/trigger-disconnect.txt"))
        .body("ping")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "PUT should succeed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let logs = fetch_log_messages(&base, &client).await;
    assert!(
        logs.iter().any(|m| m.contains("subscriber disconnected")),
        "expected subscriber disconnected log entry, logs: {:?}",
        logs
    );

    // Verify subscriber count dropped back to 0
    let info: serde_json::Value = client
        .get(&format!("{base}/_admin/info"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        info["event_socket_subscribers"].as_u64().unwrap_or(99),
        0,
        "expected 0 subscribers after disconnect"
    );
}
