//! Event notification system for object store operations.
//!
//! Broadcasts typed events (PUT, DELETE, FLUSH) over a Unix domain socket.
//! Replaces the old flush-only `notify` module with a richer event stream
//! that covers all mutations.
//!
//! ## Wire protocol (text-based, one line per message)
//!
//! 1. Client connects to the Unix socket.
//! 2. Client sends: `SECRET <shared_secret>\n`
//! 3. Server validates the secret.
//!    - On success: sends `OK\n`
//!    - On failure: sends `ERR bad secret\n` and closes the connection.
//! 4. Server sends event lines as they occur:
//!    - `PUT <key>\n`       -- object written (after all replicas confirm)
//!    - `DELETE <key>\n`    -- object deleted
//!    - `FLUSH <shard_id> <txn_id>\n` -- raw shard flushed to disk
//!
//! The text-based format is intentionally simple so events can be
//! observed with `socat` or `nc -U`.

use std::fmt;

/// A store event that can be broadcast to listeners.
#[derive(Clone, Debug)]
pub enum StoreEvent {
    /// An object was written (key only, after all replicas confirmed).
    Put { key: String },
    /// An object was deleted.
    Delete { key: String },
    /// A raw shard flushed its index to disk.
    Flush { shard_id: usize, txn_id: u64 },
}

impl fmt::Display for StoreEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreEvent::Put { key } => write!(f, "PUT {key}"),
            StoreEvent::Delete { key } => write!(f, "DELETE {key}"),
            StoreEvent::Flush { shard_id, txn_id } => {
                write!(f, "FLUSH {shard_id} {txn_id}")
            }
        }
    }
}

/// Parse a wire-protocol line back into a `StoreEvent`.
///
/// Returns `None` if the line does not match any known event format.
pub fn parse_event(line: &str) -> Option<StoreEvent> {
    let line = line.trim_end();
    if let Some(key) = line.strip_prefix("PUT ") {
        Some(StoreEvent::Put { key: key.to_owned() })
    } else if let Some(key) = line.strip_prefix("DELETE ") {
        Some(StoreEvent::Delete { key: key.to_owned() })
    } else if let Some(rest) = line.strip_prefix("FLUSH ") {
        let mut parts = rest.splitn(2, ' ');
        let shard_id = parts.next()?.parse::<usize>().ok()?;
        let txn_id = parts.next()?.parse::<u64>().ok()?;
        Some(StoreEvent::Flush { shard_id, txn_id })
    } else {
        None
    }
}

/// Broadcast channel for store events.
///
/// One `EventBus` serves the entire store (including all shards).
/// Producers call `emit()` to broadcast; consumers subscribe via
/// `subscribe()`.
pub struct EventBus {
    tx: tokio::sync::broadcast::Sender<StoreEvent>,
}

impl EventBus {
    /// Create a new event bus with the given channel capacity.
    ///
    /// 256 is a good default -- slow consumers will lag rather than
    /// block producers.
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(capacity);
        Self { tx }
    }

    /// Broadcast an event to all subscribers.
    ///
    /// Returns the number of receivers that got the message.
    pub fn emit(&self, event: StoreEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Convenience: emit a PUT event.
    pub fn emit_put(&self, key: &str) -> usize {
        self.emit(StoreEvent::Put { key: key.to_owned() })
    }

    /// Convenience: emit a DELETE event.
    pub fn emit_delete(&self, key: &str) -> usize {
        self.emit(StoreEvent::Delete { key: key.to_owned() })
    }

    /// Convenience: emit a FLUSH event.
    pub fn emit_flush(&self, shard_id: usize, txn_id: u64) -> usize {
        self.emit(StoreEvent::Flush { shard_id, txn_id })
    }

    /// Get a new receiver for subscribing to events.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<StoreEvent> {
        self.tx.subscribe()
    }
}

// -------------------------------------------------------------------
//  Unix domain socket server and client
// -------------------------------------------------------------------
#[cfg(unix)]
pub mod unix {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::broadcast;
    use tokio::task::JoinHandle;
    use tracing::{debug, info, warn};

    /// Hard ceiling for max_readers.
    pub const MAX_READERS_CEILING: usize = 64;

    /// Minimum length for the shared secret string.
    pub const MIN_SECRET_LENGTH: usize = 8;

    /// Callback type for event socket log messages.
    ///
    /// Arguments: `(level, message)` where level is "info", "warn", etc.
    pub type EventLogFn = Arc<dyn Fn(&str, &str) + Send + Sync>;

    /// Server that broadcasts [`StoreEvent`]s to authenticated Unix
    /// socket clients.
    pub struct EventServer {
        _accept_task: JoinHandle<()>,
        socket_path: PathBuf,
        connection_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl EventServer {
        /// Start the event server.
        ///
        /// Events are read from `bus` and relayed to all authenticated
        /// clients connected to `socket_path`.
        ///
        /// - `socket_path`: path for the Unix domain socket.
        /// - `secret`: shared secret (min 8 chars).
        /// - `max_readers`: max concurrent authenticated readers
        ///   (capped at [`MAX_READERS_CEILING`]).
        /// - `bus`: the [`EventBus`] whose events are relayed.
        /// - `on_log`: optional callback for structured log events
        ///   (connect, disconnect, auth failures).
        pub fn start(
            socket_path: &Path,
            secret: &str,
            max_readers: usize,
            bus: &Arc<EventBus>,
            on_log: Option<EventLogFn>,
        ) -> crate::Result<Self> {
            if secret.len() < MIN_SECRET_LENGTH {
                return Err(crate::RawStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "EVENT_SECRET must be at least {} characters (got {})",
                        MIN_SECRET_LENGTH,
                        secret.len()
                    ),
                )));
            }
            let effective_max = max_readers.min(MAX_READERS_CEILING);

            // Remove stale socket file from a previous crash.
            let _ = std::fs::remove_file(socket_path);

            let listener = UnixListener::bind(socket_path).map_err(|e| {
                crate::RawStoreError::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to bind event socket {}: {}",
                        socket_path.display(),
                        e
                    ),
                ))
            })?;

            let bus = Arc::clone(bus);
            let secret_owned = secret.to_owned();
            let sock_path = socket_path.to_path_buf();
            let reader_count =
                Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let reader_count_outer = Arc::clone(&reader_count);

            let accept_task = tokio::spawn(async move {

                loop {
                    let (stream, _addr) = match listener.accept().await {
                        Ok(conn) => conn,
                        Err(e) => {
                            warn!(
                                path = %sock_path.display(),
                                "accept error: {e}"
                            );
                            continue;
                        }
                    };

                    let current = reader_count
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if current >= effective_max {
                        warn!(
                            current = current,
                            max = effective_max,
                            "max readers reached, rejecting connection"
                        );
                        if let Some(ref cb) = on_log {
                            cb("warn", &format!(
                                "event socket: max readers reached ({}/{}), rejecting connection",
                                current, effective_max
                            ));
                        }
                        let mut s = stream;
                        let _ =
                            s.write_all(b"ERR max readers\n").await;
                        let _ = s.shutdown().await;
                        continue;
                    }

                    let secret_ref = secret_owned.clone();
                    let rx = bus.subscribe();
                    let count = Arc::clone(&reader_count);
                    let log_fn = on_log.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_client(
                            stream,
                            &secret_ref,
                            rx,
                            &count,
                            log_fn.as_ref(),
                        )
                        .await
                        {
                            debug!("event client handler finished: {e}");
                        }
                    });
                }
            });

            info!(
                path = %socket_path.display(),
                max_readers = effective_max,
                "event server started"
            );

            Ok(Self {
                _accept_task: accept_task,
                socket_path: socket_path.to_path_buf(),
                connection_count: reader_count_outer,
            })
        }

        /// Number of currently authenticated subscribers.
        pub fn subscriber_count(&self) -> usize {
            self.connection_count
                .load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Shared counter of active subscribers (can be cloned for dashboards).
        pub fn subscriber_count_ref(&self) -> Arc<std::sync::atomic::AtomicUsize> {
            Arc::clone(&self.connection_count)
        }

        /// Path of the Unix domain socket.
        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }
    }

    impl Drop for EventServer {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.socket_path);
            self._accept_task.abort();
        }
    }

    /// Per-client handler: authenticate, then relay events.
    async fn handle_client(
        stream: UnixStream,
        expected_secret: &str,
        mut rx: broadcast::Receiver<StoreEvent>,
        reader_count: &std::sync::atomic::AtomicUsize,
        on_log: Option<&EventLogFn>,
    ) -> std::io::Result<()> {
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Step 1: read SECRET line (timeout after 10 seconds).
        let mut line = String::new();
        let read_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            reader.read_line(&mut line),
        )
        .await;

        match read_result {
            Ok(Ok(0)) | Err(_) => {
                let _ = write_half.write_all(b"ERR timeout\n").await;
                let _ = write_half.shutdown().await;
                return Ok(());
            }
            Ok(Err(e)) => return Err(e),
            Ok(Ok(_)) => {}
        }

        let line = line.trim_end();
        let provided_secret =
            line.strip_prefix("SECRET ").unwrap_or("");

        // Constant-time comparison to prevent timing side-channels.
        let secret_ok = {
            let a = provided_secret.as_bytes();
            let b = expected_secret.as_bytes();
            let mut diff: u8 = if a.len() != b.len() { 1 } else { 0 };
            let n = a.len().max(b.len());
            for i in 0..n {
                let x = if i < a.len() { a[i] } else { 0 };
                let y = if i < b.len() { b[i] } else { 0 };
                diff |= x ^ y;
            }
            diff == 0
        };
        if !secret_ok {
            warn!("client sent wrong secret, rejecting");
            if let Some(cb) = on_log {
                cb("warn", "event socket: client authentication failed (wrong secret)");
            }
            let _ =
                write_half.write_all(b"ERR bad secret\n").await;
            let _ = write_half.shutdown().await;
            return Ok(());
        }

        // Increment reader count.
        reader_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Send OK.  If this fails, decrement reader_count before returning
        // so the counter does not permanently drift upward.
        if let Err(e) = write_half.write_all(b"OK\n").await {
            reader_count
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err(e);
        }

        debug!("event client authenticated, relaying events");
        if let Some(cb) = on_log {
            let n = reader_count.load(std::sync::atomic::Ordering::Relaxed);
            cb("info", &format!("event socket: subscriber connected ({} active)", n));
        }

        // Step 2: relay events until client disconnects or channel closes.
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let msg = format!("{event}\n");
                    if write_half
                        .write_all(msg.as_bytes())
                        .await
                        .is_err()
                    {
                        break; // client disconnected
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!(
                        "event client lagged by {n} messages, continuing"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break; // server shutting down
                }
            }
        }

        reader_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(cb) = on_log {
            let n = reader_count.load(std::sync::atomic::Ordering::Relaxed);
            cb("info", &format!("event socket: subscriber disconnected ({} active)", n));
        }
        Ok(())
    }

    // -------------------------------------------------------------------
    //  Client side
    // -------------------------------------------------------------------

    /// Connect to an [`EventServer`] and call `event_fn` on each event.
    ///
    /// Returns a `JoinHandle` for the background task.  Drop or abort it
    /// to disconnect.
    pub async fn subscribe_events<F>(
        socket_path: &Path,
        secret: &str,
        event_fn: F,
    ) -> std::io::Result<JoinHandle<()>>
    where
        F: Fn(StoreEvent) + Send + 'static,
    {
        let stream = UnixStream::connect(socket_path).await?;
        let (read_half, mut write_half) = stream.into_split();

        // Authenticate.
        let auth_line = format!("SECRET {secret}\n");
        write_half.write_all(auth_line.as_bytes()).await?;

        let mut reader = BufReader::new(read_half);
        let mut response = String::new();
        reader.read_line(&mut response).await?;
        let response = response.trim_end();

        if response != "OK" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "event server rejected authentication: {response}"
                ),
            ));
        }

        let handle = tokio::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // server closed connection
                    Ok(_) => {
                        if let Some(event) = parse_event(&line) {
                            event_fn(event);
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(handle)
    }
}

// -------------------------------------------------------------------
//  TCP client -- for subscribing across machines via socat or direct
// -------------------------------------------------------------------

/// Connect to an event stream over TCP and call `event_fn` on each event.
///
/// Uses the same wire protocol as the Unix domain socket server:
/// sends `SECRET <secret>\n`, expects `OK\n`, then reads event lines.
///
/// `addr` is a standard `host:port` string (e.g. `"10.0.1.5:9999"`).
///
/// Returns a `JoinHandle` for the background task.
pub async fn subscribe_events_tcp<F>(
    addr: &str,
    secret: &str,
    event_fn: F,
) -> std::io::Result<tokio::task::JoinHandle<()>>
where
    F: Fn(StoreEvent) + Send + 'static,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    let stream = TcpStream::connect(addr).await?;
    let (read_half, mut write_half) = stream.into_split();

    // Authenticate.
    let auth_line = format!("SECRET {secret}\n");
    write_half.write_all(auth_line.as_bytes()).await?;

    let mut reader = BufReader::new(read_half);
    let mut response = String::new();
    reader.read_line(&mut response).await?;
    let response = response.trim_end().to_owned();

    if response != "OK" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("event server rejected authentication: {response}"),
        ));
    }

    let handle = tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if let Some(event) = parse_event(&line) {
                        event_fn(event);
                    }
                }
                Err(_) => break,
            }
        }
    });

    Ok(handle)
}

/// Connect to an event source (Unix socket path or `tcp:host:port`).
///
/// If `addr` starts with `tcp:` the remainder is treated as a TCP
/// `host:port` address.  Otherwise it is treated as a Unix domain
/// socket path (requires the `unix` feature / Unix platform).
///
/// Returns a `JoinHandle` for the background reader task.
pub async fn subscribe_events_auto<F>(
    addr: &str,
    secret: &str,
    event_fn: F,
) -> std::io::Result<tokio::task::JoinHandle<()>>
where
    F: Fn(StoreEvent) + Send + 'static,
{
    if let Some(tcp_addr) = addr.strip_prefix("tcp:") {
        subscribe_events_tcp(tcp_addr, secret, event_fn).await
    } else {
        #[cfg(unix)]
        {
            unix::subscribe_events(
                std::path::Path::new(addr),
                secret,
                event_fn,
            )
            .await
        }
        #[cfg(not(unix))]
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix domain sockets not available on this platform; use tcp: prefix",
            ))
        }
    }
}
