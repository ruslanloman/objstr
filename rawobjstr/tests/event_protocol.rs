//! Tests for the event notification protocol (event.rs).
//!
//! These verify the EventServer/Client Unix-socket protocol:
//! authentication with a shared secret, event message delivery,
//! max-readers enforcement, and integration with flush_index().
//!
//! All tests are `#[cfg(unix)]` because the event system uses
//! Unix domain sockets.

#![cfg(unix)]

mod common;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::event::unix::{
    subscribe_events, EventServer, MAX_READERS_CEILING, MIN_SECRET_LENGTH,
};
use rawobjstr::event::{EventBus, StoreEvent, parse_event};
use rawobjstr::store::RawObjectStore;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::{timeout, Duration};

use common::{make_store, SMALL_DEVICE};

    /// Helper: create a temp socket path inside a TempDir.
    fn sock_path(dir: &TempDir, name: &str) -> std::path::PathBuf {
        dir.path().join(name)
    }

    /// Helper: create an EventBus + EventServer pair.
    fn start_server(
        path: &std::path::Path,
        secret: &str,
        max_readers: usize,
    ) -> (Arc<EventBus>, EventServer) {
        let bus = Arc::new(EventBus::new(256));
        let server = EventServer::start(path, secret, max_readers, &bus, None).unwrap();
        (bus, server)
    }

    // ===========================================================================
    // TEST 1: Server starts and cleans up socket on drop
    // ===========================================================================

    #[tokio::test]
    async fn server_start_and_drop_cleanup() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test1.sock");

        {
            let (_bus, _server) = start_server(&path, "secretXY", 4);
            assert!(path.exists(), "socket file should exist while server is alive");
        }
        // EventServer::drop removes the socket file.
        assert!(!path.exists(), "socket file should be removed after drop");
    }

    // ===========================================================================
    // TEST 2: Server rejects secret shorter than MIN_SECRET_LENGTH
    // ===========================================================================

    #[tokio::test]
    async fn server_rejects_short_secret() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test2.sock");
        let short = &"x".repeat(MIN_SECRET_LENGTH - 1);
        let bus = Arc::new(EventBus::new(256));
        let err = EventServer::start(&path, short, 4, &bus, None);
        assert!(err.is_err(), "short secret should be rejected");
    }

    // ===========================================================================
    // TEST 3: Client authenticates and receives OK
    // ===========================================================================

    #[tokio::test]
    async fn client_authenticates_ok() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test3.sock");
        let secret = "test-secret-123";

        let (_bus, _server) = start_server(&path, secret, 4);

        // Raw client connect and authenticate.
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        write
            .write_all(format!("SECRET {secret}\n").as_bytes())
            .await
            .unwrap();

        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "OK");
    }

    // ===========================================================================
    // TEST 4: Client with wrong secret is rejected
    // ===========================================================================

    #[tokio::test]
    async fn client_wrong_secret_rejected() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test4.sock");
        let secret = "correct-secret";

        let (_bus, _server) = start_server(&path, secret, 4);

        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        write.write_all(b"SECRET wrong-secret\n").await.unwrap();

        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert!(
            line.starts_with("ERR"),
            "wrong secret should get ERR response, got: {line}"
        );
    }

    // ===========================================================================
    // TEST 5: Client receives FLUSH event
    // ===========================================================================

    #[tokio::test]
    async fn client_receives_flush_event() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test5.sock");
        let secret = "event-test-secret";

        let (bus, _server) = start_server(&path, secret, 4);

        // Connect and authenticate.
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        write
            .write_all(format!("SECRET {secret}\n").as_bytes())
            .await
            .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "OK");

        // Emit a flush event.
        let sent = bus.emit_flush(0, 42);
        assert_eq!(sent, 1, "should have notified 1 reader");

        // Client should receive "FLUSH 0 42".
        line.clear();
        let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
        assert!(result.is_ok(), "should receive event within timeout");
        assert_eq!(line.trim(), "FLUSH 0 42");
    }

    // ===========================================================================
    // TEST 6: Client receives PUT and DELETE events
    // ===========================================================================

    #[tokio::test]
    async fn client_receives_put_and_delete_events() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test6a.sock");
        let secret = "put-del-test-12";

        let (bus, _server) = start_server(&path, secret, 4);

        // Connect and authenticate.
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        write
            .write_all(format!("SECRET {secret}\n").as_bytes())
            .await
            .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "OK");

        // Emit PUT.
        bus.emit_put("images/cat.jpg");
        line.clear();
        let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
        assert!(result.is_ok());
        assert_eq!(line.trim(), "PUT images/cat.jpg");

        // Emit DELETE.
        bus.emit_delete("old/junk.bin");
        line.clear();
        let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
        assert!(result.is_ok());
        assert_eq!(line.trim(), "DELETE old/junk.bin");
    }

    // ===========================================================================
    // TEST 7: Multiple clients all receive the same event
    // ===========================================================================

    #[tokio::test]
    async fn multiple_clients_receive_event() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test7.sock");
        let secret = "multi-client-test";

        let (bus, _server) = start_server(&path, secret, 8);

        // Connect 3 clients.
        let mut readers = Vec::new();
        for _ in 0..3 {
            let stream = UnixStream::connect(&path).await.unwrap();
            let (read, mut write) = stream.into_split();
            write
                .write_all(format!("SECRET {secret}\n").as_bytes())
                .await
                .unwrap();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line.trim(), "OK");
            readers.push(reader);
        }

        // Give the server a moment to register all clients.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let sent = bus.emit_flush(1, 99);
        assert_eq!(sent, 3, "should have notified 3 readers");

        // Each client should receive the message.
        for (i, reader) in readers.iter_mut().enumerate() {
            let mut line = String::new();
            let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
            assert!(result.is_ok(), "client {i} should receive event");
            assert_eq!(line.trim(), "FLUSH 1 99", "client {i} wrong message");
        }
    }

    // ===========================================================================
    // TEST 8: Max readers limit enforced
    // ===========================================================================

    #[tokio::test]
    async fn max_readers_limit_enforced() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test8.sock");
        let secret = "max-readers-test";
        let max = 2;

        let (_bus, _server) = start_server(&path, secret, max);

        // Fill up to max_readers.
        let mut handles = Vec::new();
        for _ in 0..max {
            let stream = UnixStream::connect(&path).await.unwrap();
            let (read, mut write) = stream.into_split();
            write
                .write_all(format!("SECRET {secret}\n").as_bytes())
                .await
                .unwrap();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line.trim(), "OK");
            handles.push((reader, write));
        }

        // Give the server time to update its reader count.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The next connection should be rejected with "ERR max readers".
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut line = String::new();

        let _ = write
            .write_all(format!("SECRET {secret}\n").as_bytes())
            .await;

        let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
        match result {
            Ok(Ok(n)) if n > 0 => {
                assert!(
                    line.starts_with("ERR"),
                    "over-limit connection should get ERR, got: {line}"
                );
            }
            _ => {
                // Connection was closed immediately -- also acceptable.
            }
        }
    }

    // ===========================================================================
    // TEST 9: Max readers capped at MAX_READERS_CEILING
    // ===========================================================================

    #[tokio::test]
    async fn max_readers_capped_at_ceiling() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test9.sock");
        let bus = Arc::new(EventBus::new(256));
        // Request more than the ceiling -- server should silently cap.
        let _server =
            EventServer::start(&path, "ceiling-test", MAX_READERS_CEILING + 100, &bus, None).unwrap();
    }

    // ===========================================================================
    // TEST 10: Multiple events arrive in order
    // ===========================================================================

    #[tokio::test]
    async fn events_arrive_in_order() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test10.sock");
        let secret = "order-test-1234";

        let (bus, _server) = start_server(&path, secret, 4);

        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        write
            .write_all(format!("SECRET {secret}\n").as_bytes())
            .await
            .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "OK");

        // Send 10 flush events.
        for txn_id in 1..=10u64 {
            bus.emit_flush(0, txn_id);
        }

        // Read all 10 and verify order.
        for expected_txn in 1..=10u64 {
            line.clear();
            let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
            assert!(result.is_ok(), "should receive txn {expected_txn}");
            assert_eq!(
                line.trim(),
                format!("FLUSH 0 {expected_txn}"),
                "wrong order at txn {expected_txn}"
            );
        }
    }

    // ===========================================================================
    // TEST 11: subscribe_events helper works end-to-end
    // ===========================================================================

    #[tokio::test]
    async fn subscribe_events_end_to_end() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test11.sock");
        let secret = "subscribe-test1";

        let (bus, _server) = start_server(&path, secret, 4);

        let last_txn = Arc::new(AtomicU64::new(0));
        let last_txn_clone = Arc::clone(&last_txn);

        let handle = subscribe_events(&path, secret, move |event| {
            if let StoreEvent::Flush { txn_id, .. } = event {
                last_txn_clone.store(txn_id, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();

        // Give the subscription task time to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        bus.emit_flush(0, 7);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(last_txn.load(Ordering::SeqCst), 7);

        bus.emit_flush(0, 8);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(last_txn.load(Ordering::SeqCst), 8);

        handle.abort();
    }

    // ===========================================================================
    // TEST 12: subscribe_events rejects wrong secret
    // ===========================================================================

    #[tokio::test]
    async fn subscribe_events_wrong_secret() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test12.sock");
        let secret = "real-secret-123";

        let (_bus, _server) = start_server(&path, secret, 4);

        let result = subscribe_events(&path, "wrong-secret", |_| {}).await;
        assert!(
            result.is_err(),
            "subscribe with wrong secret should fail"
        );
    }

    // ===========================================================================
    // TEST 13: Integration -- flush_callback + EventBus delivers to client
    // ===========================================================================

    #[tokio::test]
    async fn store_flush_delivers_event() {
        let dir = TempDir::new().unwrap();
        let sock = sock_path(&dir, "test13.sock");
        let secret = "store-flush-int";

        let (store, _tmp) = make_store();

        // Create bus + server and register flush callback on the store.
        let (bus, _server) = start_server(&sock, secret, 4);
        let bus_clone = Arc::clone(&bus);
        store.add_flush_callback(Arc::new(move |txn_id| {
            bus_clone.emit_flush(0, txn_id);
        }));

        // Subscribe a client.
        let last_txn = Arc::new(AtomicU64::new(0));
        let last_txn_clone = Arc::clone(&last_txn);
        let handle = subscribe_events(&sock, secret, move |event| {
            if let StoreEvent::Flush { txn_id, .. } = event {
                last_txn_clone.store(txn_id, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Put data and flush -- this should trigger a FLUSH event.
        store
            .put(
                &Path::from("event/test.txt"),
                PutPayload::from(Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();

        // Wait for the event to arrive.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let txn = last_txn.load(Ordering::SeqCst);
        assert!(txn > 0, "should have received a flush event (txn={txn})");

        // A second flush should deliver a higher txn_id.
        let first_txn = txn;
        store
            .put(
                &Path::from("event/test2.txt"),
                PutPayload::from(Bytes::from_static(b"world")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        let second_txn = last_txn.load(Ordering::SeqCst);
        assert!(
            second_txn > first_txn,
            "second flush txn ({second_txn}) should be > first ({first_txn})"
        );

        handle.abort();
    }

    // ===========================================================================
    // TEST 14: Integration -- reader reload_index via event subscription
    // ===========================================================================

    #[tokio::test]
    async fn event_triggers_reload_index() {
        let dir = TempDir::new().unwrap();
        let sock = sock_path(&dir, "test14.sock");
        let secret = "reload-event-12";

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let writer = RawObjectStore::format_with_size(path, SMALL_DEVICE, false).unwrap();
        let (bus, _server) = start_server(&sock, secret, 4);
        let bus_clone = Arc::clone(&bus);
        writer.add_flush_callback(Arc::new(move |txn_id| {
            bus_clone.emit_flush(0, txn_id);
        }));

        let reader = Arc::new(RawObjectStore::open_readonly(path).unwrap());

        // Subscribe and reload on each FLUSH event.
        let reader_clone = Arc::clone(&reader);
        let reload_count = Arc::new(AtomicU64::new(0));
        let reload_count_clone = Arc::clone(&reload_count);
        let handle = subscribe_events(&sock, secret, move |event| {
            if matches!(event, StoreEvent::Flush { .. }) {
                let _ = reader_clone.reload_index();
                reload_count_clone.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Writer puts + flushes.
        writer
            .put(
                &Path::from("live/data.bin"),
                PutPayload::from(Bytes::from(vec![0xCC; 4096])),
            )
            .await
            .unwrap();
        writer.flush_index().unwrap();

        // Wait for event + reload.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Reader should now see the new file.
        let data = reader
            .get(&Path::from("live/data.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0xCC));

        assert!(
            reload_count.load(Ordering::SeqCst) >= 1,
            "reload should have been called at least once"
        );

        handle.abort();
    }

    // ===========================================================================
    // TEST 15: No crash when no callbacks registered
    // ===========================================================================

    #[tokio::test]
    async fn flush_without_callbacks_works() {
        let (store, _tmp) = make_store();

        // No flush callback registered -- flush should still work fine.
        store
            .put(
                &Path::from("no-event.txt"),
                PutPayload::from(Bytes::from_static(b"data")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();

        let data = store
            .get(&Path::from("no-event.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"data");
    }

    // ===========================================================================
    // TEST 16: Client timeout on no SECRET sent
    // ===========================================================================

    #[tokio::test]
    async fn client_timeout_no_secret() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test16.sock");
        let secret = "timeout-test-12";

        let (_bus, _server) = start_server(&path, secret, 4);

        // Connect but don't send SECRET -- server should timeout after 10s.
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, _write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut line = String::new();

        // Server timeout is 10s, so we wait 12s to be sure.
        let result = timeout(Duration::from_secs(12), reader.read_line(&mut line)).await;
        match result {
            Ok(Ok(0)) => {
                // Connection closed -- acceptable.
            }
            Ok(Ok(_)) => {
                assert!(
                    line.starts_with("ERR"),
                    "should get ERR on timeout, got: {line}"
                );
            }
            Ok(Err(_)) => {
                // I/O error on closed connection -- acceptable.
            }
            Err(_) => {
                panic!("test timed out waiting for server timeout response");
            }
        }
    }

    // ===========================================================================
    // TEST 17: Stale socket file from previous crash is cleaned up
    // ===========================================================================

    #[tokio::test]
    async fn stale_socket_cleaned_up() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test17.sock");

        // Create a stale socket file manually.
        std::fs::write(&path, b"stale").unwrap();
        assert!(path.exists());

        // Starting a new server should succeed (old file removed).
        let (_bus, _server) = start_server(&path, "stale-test-1", 4);

        // Verify we can connect.
        let stream = UnixStream::connect(&path).await;
        assert!(stream.is_ok(), "should connect to new server");
    }

    // ===========================================================================
    // TEST 18: Max concurrent readers stress test
    // ===========================================================================

    #[tokio::test]
    async fn max_readers_stress() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test18.sock");
        let secret = "stress-test-123";
        let max = 10;

        let (bus, _server) = start_server(&path, secret, max);

        // Connect max clients.
        let mut clients = Vec::new();
        for _ in 0..max {
            let stream = UnixStream::connect(&path).await.unwrap();
            let (read, mut write) = stream.into_split();
            write
                .write_all(format!("SECRET {secret}\n").as_bytes())
                .await
                .unwrap();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line.trim(), "OK", "all {max} clients should authenticate");
            clients.push((reader, write));
        }

        // Give server time to update reader count.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a PUT event -- all clients should receive it.
        let sent = bus.emit_put("stress/object.bin");
        assert_eq!(sent, max, "all {max} clients should be notified");

        for (i, (reader, _write)) in clients.iter_mut().enumerate() {
            let mut line = String::new();
            let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
            assert!(result.is_ok(), "client {i} should receive event");
            assert_eq!(line.trim(), "PUT stress/object.bin");
        }
    }

    // ===========================================================================
    // TEST 19: parse_event round-trips all event types
    // ===========================================================================

    #[tokio::test]
    async fn parse_event_roundtrip() {
        // PUT
        let e = parse_event("PUT images/cat.jpg\n").unwrap();
        assert!(matches!(e, StoreEvent::Put { ref key } if key == "images/cat.jpg"));

        // DELETE
        let e = parse_event("DELETE old/junk.bin\n").unwrap();
        assert!(matches!(e, StoreEvent::Delete { ref key } if key == "old/junk.bin"));

        // FLUSH
        let e = parse_event("FLUSH 3 42\n").unwrap();
        assert!(matches!(e, StoreEvent::Flush { shard_id: 3, txn_id: 42 }));

        // Unknown
        assert!(parse_event("UNKNOWN foo\n").is_none());

        // Malformed FLUSH
        assert!(parse_event("FLUSH abc\n").is_none());
    }

    // ===========================================================================
    // TEST 20: Mixed event types arrive in order
    // ===========================================================================

    #[tokio::test]
    async fn mixed_events_in_order() {
        let dir = TempDir::new().unwrap();
        let path = sock_path(&dir, "test20.sock");
        let secret = "mixed-test-1234";

        let (bus, _server) = start_server(&path, secret, 4);

        let stream = UnixStream::connect(&path).await.unwrap();
        let (read, mut write) = stream.into_split();
        write
            .write_all(format!("SECRET {secret}\n").as_bytes())
            .await
            .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), "OK");

        // Send mixed events.
        bus.emit_put("a/b.txt");
        bus.emit_delete("c/d.txt");
        bus.emit_flush(0, 1);
        bus.emit_put("e/f.txt");
        bus.emit_flush(1, 2);

        // Read them back and verify order.
        let expected = [
            "PUT a/b.txt",
            "DELETE c/d.txt",
            "FLUSH 0 1",
            "PUT e/f.txt",
            "FLUSH 1 2",
        ];
        for exp in &expected {
            line.clear();
            let result = timeout(Duration::from_secs(5), reader.read_line(&mut line)).await;
            assert!(result.is_ok(), "should receive event: {exp}");
            assert_eq!(line.trim(), *exp);
        }
    }
