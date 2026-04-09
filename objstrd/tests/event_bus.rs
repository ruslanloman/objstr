//! Event bus emission tests: verify PUT and DELETE S3 operations emit the
//! expected events on the in-process event bus.

mod common;
use common::{collect_events_timeout, TestServer};
use rawobjstr::event::StoreEvent;

// ===========================================================================
// Event bus emission
// ===========================================================================

/// PUT through S3 should emit a PUT event on the event bus.
#[tokio::test]
async fn test_event_bus_put_emitted() {
    let (srv, mut rx) = TestServer::start_with_event_bus().await;

    srv.client
        .put(&srv.object_url("hello.txt"))
        .body("world")
        .send()
        .await
        .unwrap();

    let events = collect_events_timeout(&mut rx, 1, std::time::Duration::from_secs(2)).await;
    let put_keys: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Put { key } => Some(key.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        put_keys.contains(&"hello.txt"),
        "Should see PUT event for hello.txt, got: {:?}",
        events
    );
}

/// DELETE through S3 should emit a DELETE event on the event bus.
#[tokio::test]
async fn test_event_bus_delete_emitted() {
    let (srv, mut rx) = TestServer::start_with_event_bus().await;

    // Seed object
    srv.client
        .put(&srv.object_url("to-delete.txt"))
        .body("bye")
        .send()
        .await
        .unwrap();

    // Drain the PUT event
    let _ = collect_events_timeout(&mut rx, 1, std::time::Duration::from_secs(2)).await;

    // DELETE
    srv.client
        .delete(&srv.object_url("to-delete.txt"))
        .send()
        .await
        .unwrap();

    let events = collect_events_timeout(&mut rx, 1, std::time::Duration::from_secs(2)).await;
    let delete_keys: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Delete { key } => Some(key.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        delete_keys.contains(&"to-delete.txt"),
        "Should see DELETE event, got: {:?}",
        events
    );
}

/// DELETE on non-existent key should still emit a DELETE event.
#[tokio::test]
async fn test_event_bus_delete_nonexistent_emits() {
    let (srv, mut rx) = TestServer::start_with_event_bus().await;

    srv.client
        .delete(&srv.object_url("phantom.txt"))
        .send()
        .await
        .unwrap();

    let events = collect_events_timeout(&mut rx, 1, std::time::Duration::from_secs(2)).await;
    let delete_keys: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            StoreEvent::Delete { key } => Some(key.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        delete_keys.contains(&"phantom.txt"),
        "Should see DELETE event even for non-existent key, got: {:?}",
        events
    );
}
