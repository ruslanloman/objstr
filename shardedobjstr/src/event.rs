//! Event socket setup for sharded clusters.
//!
//! Provides helpers to wire an [`EventBus`] + [`EventServer`] into a
//! [`ShardedObjectStore`] with flush callbacks on each raw shard.
//!
//! ## Usage
//!
//! ```ignore
//! let (bus, _server) = setup_event_socket(
//!     &cluster,
//!     &raw_stores,     // Vec<(shard_id, Arc<RawObjectStore>)>
//!     Path::new("/run/objstrd/events.sock"),
//!     "my-secret-key",
//!     16,
//! ).unwrap();
//! ```
//!
//! The returned `EventServer` must be kept alive for the lifetime of
//! the daemon.  Drop it to stop accepting new clients and clean up the
//! socket file.
//!
//! ## Streaming replica
//!
//! For read-only replicas that share the same backing shards (same S3
//! bucket, same filesystem path, or same raw image opened read-only),
//! use [`subscribe_streaming_replica`].  It watches the writer's event
//! stream and updates the replica's in-memory catalog on each PUT and
//! DELETE -- no FLUSH or `reload_index()` required.
//!
//! **Constraint:** only works when every object is on every shard
//! (rf=1 with a single shard, or rf=shard_count in mirror mode).

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use rawobjstr::event::EventBus;
use rawobjstr::store::RawObjectStore;

use crate::ShardedObjectStore;

/// Wire up a complete event socket for a sharded cluster.
///
/// 1. Creates an [`EventBus`] and attaches it to `cluster` via
///    `set_event_bus()` (so PUT/DELETE events are emitted).
/// 2. Registers a flush callback on each raw store so FLUSH events
///    include the correct `shard_id`.
/// 3. Starts an [`EventServer`] on `socket_path`.
///
/// Returns `(bus, server)`.  The server must be kept alive.
///
/// `raw_stores` is a list of `(shard_id, store)` pairs for every shard
/// backed by a `RawObjectStore`.  Non-raw shards (S3, fs, mem) do not
/// emit FLUSH events and should not be included.
///
/// **Caveat:** FLUSH events are only emitted for `RawObjectStore`
/// shards that have a flush callback registered here.  In a mixed
/// cluster (raw + S3/fs/mem shards), writes that land on non-raw
/// shards will still produce PUT/DELETE events but no corresponding
/// FLUSH.  A read-only subscriber relying on FLUSH to trigger
/// `reload_index()` will therefore only see updates for raw shards.
/// Unless the cluster is in a mirror/full-replication setup where
/// every object also lands on at least one raw shard, some updates
/// may not trigger a reload on read-only nodes.
#[cfg(unix)]
pub fn setup_event_socket(
    cluster: &ShardedObjectStore,
    raw_stores: &[(usize, Arc<RawObjectStore>)],
    socket_path: &Path,
    secret: &str,
    max_readers: usize,
    on_log: Option<rawobjstr::event::unix::EventLogFn>,
) -> rawobjstr::Result<(Arc<EventBus>, rawobjstr::event::unix::EventServer)> {
    let bus = Arc::new(EventBus::new(256));

    // Attach the bus to the cluster so put/delete emit events.
    cluster.set_event_bus(Arc::clone(&bus));

    // Register flush callbacks on each raw store.
    for &(shard_id, ref store) in raw_stores {
        let bus_clone = Arc::clone(&bus);
        store.add_flush_callback(Arc::new(move |txn_id| {
            bus_clone.emit_flush(shard_id, txn_id);
        }));
    }

    // Start the socket server.
    let server =
        rawobjstr::event::unix::EventServer::start(socket_path, secret, max_readers, &bus, on_log)?;

    tracing::info!(
        socket = %socket_path.display(),
        raw_shards = raw_stores.len(),
        "event socket ready"
    );

    Ok((bus, server))
}

/// Subscribe to events from an event socket.
///
/// Convenience wrapper around
/// [`rawobjstr::event::unix::subscribe_events`] that handles
/// connection and authentication.
///
/// Returns a `JoinHandle` for the background reader task.
#[cfg(unix)]
pub async fn subscribe_store_events<F>(
    socket_path: &Path,
    secret: &str,
    event_fn: F,
) -> std::io::Result<tokio::task::JoinHandle<()>>
where
    F: Fn(rawobjstr::event::StoreEvent) + Send + 'static,
{
    rawobjstr::event::unix::subscribe_events(socket_path, secret, event_fn).await
}

/// Start a streaming replica subscriber.
///
/// Connects to the writer's event source (Unix socket or TCP via
/// `tcp:host:port`), watches the PUT/DELETE event stream, and keeps
/// `cluster`'s in-memory catalog up to date.
///
/// On **PUT**: HEAD shard 0 to get object size, then insert into the
/// catalog with all shard IDs (mirror mode placement).
///
/// On **DELETE**: remove from the catalog.
///
/// **FLUSH** events are ignored -- the catalog is maintained purely
/// from the event stream plus HEAD calls.
///
/// # Constraints
///
/// Only correct when every object is on every shard:
/// - `rf == 1` with a single shard, or
/// - `rf == shard_count` (mirror mode).
///
/// The function validates this and returns an error otherwise.
pub async fn subscribe_streaming_replica(
    event_source: &str,
    secret: &str,
    cluster: Arc<ShardedObjectStore>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let shard_count = cluster.shard_count();
    let rf = cluster.replication_factor();

    if shard_count > 1 && rf != shard_count {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "streaming replica requires rf=1 with 1 shard or rf=shard_count \
                 (mirror mode), got rf={rf} with {shard_count} shards"
            ),
        ));
    }

    let all_shards: Vec<usize> = (0..shard_count).collect();

    // Channel bridges the sync event callback into an async processing loop.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let _sub_handle = rawobjstr::event::subscribe_events_auto(
        event_source,
        secret,
        move |event| {
            let _ = tx.send(event);
        },
    )
    .await?;

    tracing::info!(
        event_source = %event_source,
        shard_count,
        rf,
        "streaming replica subscriber started"
    );

    let handle = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                rawobjstr::event::StoreEvent::Put { key } => {
                    let path = object_store::path::Path::from(key.as_str());
                    let store = match cluster.shard_store(0) {
                        Some(s) => s,
                        None => {
                            tracing::error!("streaming replica: no shard 0");
                            continue;
                        }
                    };
                    match store.head(&path).await {
                        Ok(meta) => {
                            cluster.catalog().put(
                                key,
                                all_shards.clone(),
                                meta.size as u64,
                                None,
                                0,
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                key = %key,
                                err = %e,
                                "streaming replica: HEAD failed after PUT, skipping"
                            );
                        }
                    }
                }
                rawobjstr::event::StoreEvent::Delete { key } => {
                    cluster.catalog().remove(&key);
                }
                rawobjstr::event::StoreEvent::Flush { .. } => {
                    // Ignored in streaming replica mode.
                }
            }
        }
        tracing::warn!("streaming replica: event stream ended");
    });

    Ok(handle)
}
