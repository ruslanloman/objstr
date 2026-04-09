//! E2E test: streaming replica mode.
//!
//! Starts two objstrd instances from tree configs with filesystem shards:
//!
//!   1. **Writer** -- normal read-write node with an event socket.
//!   2. **Reader** -- read-only node with `event_source` pointing at the
//!      writer's socket.  Watches PUT/DELETE events and keeps its
//!      in-memory catalog up to date via HEAD calls.
//!
//! Verifies:
//!   - Objects PUT via the writer are readable via the reader.
//!   - Objects DELETEd via the writer become 404 on the reader.
//!   - The reader rejects PUT requests (read-only).

mod subprocess_helpers;

use std::io::Write;
use std::time::Duration;
use subprocess_helpers::*;

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn streaming_replica_put_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let event_sock = tmp.path().join("events.sock");
    let writer_port = portpicker::pick_unused_port().unwrap_or(18950);
    let reader_port = portpicker::pick_unused_port().unwrap_or(18951);

    // -- Writer config ------------------------------------------------
    let writer_conf = tmp.path().join("writer.conf");
    {
        let mut f = std::fs::File::create(&writer_conf).unwrap();
        writeln!(f, "cluster  test-replica").unwrap();
        writeln!(f, "bucket   testbucket").unwrap();
        writeln!(f, "").unwrap();
        writeln!(
            f,
            "event_socket   {}",
            event_sock.to_str().unwrap()
        )
        .unwrap();
        writeln!(f, "event_secret   test-secret").unwrap();
        writeln!(f, "").unwrap();
        writeln!(
            f,
            "writer  rf=1  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
            writer_port, writer_port
        )
        .unwrap();
        writeln!(f, "  fs  {}", shard_dir.to_str().unwrap()).unwrap();
    }

    // -- Reader config ------------------------------------------------
    let reader_conf = tmp.path().join("reader.conf");
    {
        let mut f = std::fs::File::create(&reader_conf).unwrap();
        writeln!(f, "cluster  test-replica").unwrap();
        writeln!(f, "bucket   testbucket").unwrap();
        writeln!(f, "").unwrap();
        writeln!(
            f,
            "event_source   {}",
            event_sock.to_str().unwrap()
        )
        .unwrap();
        writeln!(f, "event_secret   test-secret").unwrap();
        writeln!(f, "").unwrap();
        writeln!(
            f,
            "reader  rf=1  listen=127.0.0.1:{}  endpoint=http://127.0.0.1:{}",
            reader_port, reader_port
        )
        .unwrap();
        writeln!(f, "  fs  {}", shard_dir.to_str().unwrap()).unwrap();
    }

    // -- Start writer -------------------------------------------------
    let _writer = start_server(&writer_conf, "writer");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    assert!(
        wait_for_server(&_writer.base_url, &client, Duration::from_secs(10)).await,
        "writer did not start"
    );

    // -- Start reader (read-only + event_source) ----------------------
    let _reader = start_server_with_args(&reader_conf, "reader", &["--read-only"]);

    assert!(
        wait_for_server(&_reader.base_url, &client, Duration::from_secs(10)).await,
        "reader did not start"
    );

    // -- PUT via writer -----------------------------------------------
    let resp = client
        .put(&format!("{}/testbucket/hello.txt", _writer.base_url))
        .body("streaming replica works")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT on writer should succeed");

    let resp = client
        .put(&format!("{}/testbucket/dir/nested.bin", _writer.base_url))
        .body("nested data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "PUT nested on writer should succeed");

    // Give event propagation + HEAD time
    tokio::time::sleep(Duration::from_millis(500)).await;

    // -- GET via reader -----------------------------------------------
    let resp = client
        .get(&format!("{}/testbucket/hello.txt", _reader.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET on reader should succeed");
    let body = resp.text().await.unwrap();
    assert_eq!(body, "streaming replica works");

    let resp = client
        .get(&format!("{}/testbucket/dir/nested.bin", _reader.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET nested on reader should succeed");
    let body = resp.text().await.unwrap();
    assert_eq!(body, "nested data");

    // -- HEAD via reader ----------------------------------------------
    let resp = client
        .head(&format!("{}/testbucket/hello.txt", _reader.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "HEAD on reader should succeed");
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "23"
    );

    // -- Reader rejects writes ----------------------------------------
    let resp = client
        .put(&format!("{}/testbucket/nope.txt", _reader.base_url))
        .body("should fail")
        .send()
        .await
        .unwrap();
    // Read-only nodes currently return 500 for writes; ideally this would be
    // 403 Forbidden or 405 Method Not Allowed.
    assert!(
        resp.status() == 403 || resp.status() == 405 || resp.status() == 500,
        "PUT on read-only reader should be rejected (403/405/500), got {}",
        resp.status()
    );

    // -- DELETE via writer --------------------------------------------
    let resp = client
        .delete(&format!("{}/testbucket/hello.txt", _writer.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "DELETE on writer should return 204, got {}",
        resp.status()
    );

    // Give event propagation time
    tokio::time::sleep(Duration::from_millis(500)).await;

    // -- Verify deleted on reader -------------------------------------
    let resp = client
        .get(&format!("{}/testbucket/hello.txt", _reader.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "GET deleted object on reader should 404, got {}",
        resp.status()
    );

    // Other object still there
    let resp = client
        .get(&format!("{}/testbucket/dir/nested.bin", _reader.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "non-deleted object should still be readable");
}
