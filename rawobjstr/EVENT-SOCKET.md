# Event Socket (Writer-to-Reader Push)

> **This is a rawobjstr + shardedobjstr feature.** A single Unix
> domain socket broadcasts `PUT <key>`, `DELETE <key>`, and `FLUSH <shard_id>
> <txn_id>` events to all connected subscribers. In a sharded cluster, the
> `shardedobjstr::event` module sets up one event socket that covers all
> shards.
> Non-raw backends (filesystem, S3, in-memory) do not generate FLUSH events
> because they have no superblock or transaction ID, but PUT and DELETE events
> are still emitted. In a sharded cluster without mirror replication, writes
> that land only on non-raw shards will never fire a FLUSH, so read-only
> subscribers relying on FLUSH to `reload_index()` will miss those updates.

When one process holds the device open for writing (e.g. `objstrd`) and other
processes open it read-only, the readers need a way to know when the writer has
flushed new data. Without events, readers must poll `reload_index()` on
a timer. The event socket replaces polling with a push model over
a Unix domain socket. It also notifies subscribers about PUT and DELETE
operations in real time.

## Architecture

```
Writer process                     Reader process(es)
+--------------+                   +------------------+
| ShardedStore |                   | ShardedStore(RO) |
|   .put()     |---PUT key-------->|  (app logic)     |
|   .delete()  |---DELETE key----->|                  |
|   .flush()   |---FLUSH sid tid-->|  .reload_index() |
|              |   (Unix socket)   |                  |
+--------------+                   +------------------+
```

## Wire Protocol

Text-based, one line per message:

1. Client connects to the Unix domain socket.
2. Client sends: `SECRET <shared_secret>\n`
3. Server validates the secret (constant-time comparison).
   - On success: sends `OK\n`
   - On failure: sends `ERR bad secret\n` and closes.
4. After each operation, server sends one of:
   - `PUT <key>\n`
   - `DELETE <key>\n`
   - `FLUSH <shard_id> <txn_id>\n`

## Rust API

```rust
use rawobjstr::event::{StoreEvent, EventBus};
use rawobjstr::event::unix::{EventServer, subscribe_events};
use std::sync::Arc;

// Create an event bus and server
let bus = Arc::new(EventBus::new(256));
let server = EventServer::start(
    std::path::Path::new("/tmp/rawobj.sock"),
    "my-shared-secret",  // min 8 chars
    16,                   // max concurrent readers (ceiling: 64)
    &bus,
    None,                 // on_log: Option<EventLogFn> for diagnostics
).unwrap();

// Emit events (e.g. from flush callbacks)
bus.emit_flush(0, 42);
bus.emit_put("my/object.txt");
bus.emit_delete("old/object.txt");

// Reader side: subscribe and react to events
let reader = Arc::new(
    rawobjstr::store::RawObjectStore::open_readonly(
        std::path::Path::new("/dev/sdb")
    ).unwrap()
);
let reader_clone = Arc::clone(&reader);
let handle = subscribe_events(
    std::path::Path::new("/tmp/rawobj.sock"),
    "my-shared-secret",
    move |event| {
        if matches!(event, StoreEvent::Flush { .. }) {
            let _ = reader_clone.reload_index();
        }
    },
).await.unwrap();

// handle.abort() to disconnect
```

## Python Example

The wire protocol is simple enough to use with Python's `socket` module
directly -- no compiled bindings needed:

```python
#!/usr/bin/env python3
"""Watch an objstrd event socket and print events as they arrive."""

import socket
import sys

SOCK_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/objstrd.sock"
SECRET = sys.argv[2] if len(sys.argv) > 2 else "my-shared-secret"

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(SOCK_PATH)
sock.sendall(f"SECRET {SECRET}\n".encode())

# Read the auth response
buf = b""
while not buf.endswith(b"\n"):
    buf += sock.recv(1)
resp = buf.decode().strip()
if resp != "OK":
    print(f"Auth failed: {resp}", file=sys.stderr)
    sys.exit(1)

print(f"Connected to {SOCK_PATH}")

# Read events forever
remainder = b""
while True:
    data = sock.recv(4096)
    if not data:
        print("Server closed connection")
        break
    remainder += data
    while b"\n" in remainder:
        line, remainder = remainder.split(b"\n", 1)
        msg = line.decode().strip()
        if not msg:
            continue
        parts = msg.split(" ", 1)
        event_type = parts[0]
        payload = parts[1] if len(parts) > 1 else ""
        print(f"[{event_type}] {payload}")
```

Run with:

```bash
python3 watch_events.py /tmp/objstrd.sock my-shared-secret
```

Output:

```
Connected to /tmp/objstrd.sock
[PUT] data/2024/sales.parquet
[PUT] data/2024/orders.parquet
[DELETE] data/2023/old.parquet
[FLUSH] 0 42
[PUT] docs/readme.md
[FLUSH] 0 43
```

## Shell Script: Collect PUTs Between Flushes

This script connects to the event socket, accumulates PUT keys, and prints
the batch each time a FLUSH arrives. Useful for triggering downstream work
(e.g. cache invalidation, replication) on each flush boundary:

```bash
#!/usr/bin/env bash
# watch_flush_batch.sh -- collect PUT keys between FLUSHes
# Usage: ./watch_flush_batch.sh /tmp/objstrd.sock my-shared-secret

SOCK_PATH="${1:-/tmp/objstrd.sock}"
SECRET="${2:-my-shared-secret}"

cleanup() { rm -f "$FIFO"; }
trap cleanup EXIT

FIFO=$(mktemp -u /tmp/event_fifo.XXXXXX)
mkfifo "$FIFO"

# Use socat to talk to the Unix socket:
#   1. Send the SECRET line
#   2. Pipe all received lines into the FIFO
(echo "SECRET $SECRET"; cat) | socat - UNIX-CONNECT:"$SOCK_PATH" > "$FIFO" &
SOCAT_PID=$!

# Read the auth response
read -r RESP < "$FIFO"
if [ "$RESP" != "OK" ]; then
    echo "Auth failed: $RESP" >&2
    kill "$SOCAT_PID" 2>/dev/null
    exit 1
fi

echo "Connected to $SOCK_PATH -- collecting PUTs between FLUSHes"

BATCH=""
COUNT=0

while read -r TYPE PAYLOAD; do
    case "$TYPE" in
        PUT)
            BATCH="${BATCH}${BATCH:+$'\n'}  $PAYLOAD"
            COUNT=$((COUNT + 1))
            ;;
        FLUSH)
            if [ "$COUNT" -gt 0 ]; then
                echo "--- FLUSH (shard=$PAYLOAD) -- $COUNT objects ---"
                echo "$BATCH"
                echo ""
            fi
            BATCH=""
            COUNT=0
            ;;
        DELETE)
            # Optionally log deletes; not collected in the PUT batch
            ;;
    esac
done < "$FIFO"

kill "$SOCAT_PID" 2>/dev/null
```

Run with:

```bash
./watch_flush_batch.sh /tmp/objstrd.sock my-shared-secret
```

Output:

```
Connected to /tmp/objstrd.sock -- collecting PUTs between FLUSHes
--- FLUSH (shard=0 42) -- 3 objects ---
  data/2024/sales.parquet
  data/2024/orders.parquet
  data/2024/returns.parquet

--- FLUSH (shard=0 43) -- 1 objects ---
  docs/readme.md
```

Requires `socat` (`apt install socat`).

## Forwarding Events to a Remote Machine with socat

If you need to consume events on a different machine (e.g. a monitoring host or
a replica that should react to writes), you can use `socat` to bridge the local
Unix socket to a TCP port, then connect from the remote side.

**On the server (machine with the event socket):**

```bash
# Expose the Unix socket as a TCP listener on port 9800.
# Each remote client gets its own connection to the Unix socket.
socat TCP-LISTEN:9800,reuseaddr,fork UNIX-CONNECT:/tmp/objstrd.sock
```

**On the remote machine:**

```bash
# Connect and authenticate, then stream events to stdout:
(echo "SECRET my-shared-secret"; cat) \
  | socat - TCP:192.168.1.10:9800
```

You can also pipe the output into a processing script:

```bash
(echo "SECRET my-shared-secret"; sleep infinity) \
  | socat - TCP:192.168.1.10:9800 \
  | while read -r TYPE PAYLOAD; do
      echo "[$(date +%T)] $TYPE $PAYLOAD"
    done
```

Output on the remote machine:

```
OK
[14:02:31] PUT data/2024/orders.parquet
[14:02:31] FLUSH 0 42
[14:02:35] PUT data/2024/returns.parquet
[14:02:35] DELETE data/2023/old.parquet
[14:02:35] FLUSH 0 43
```

**Notes:**

- The TCP bridge has no encryption. For untrusted networks, wrap it in an SSH
  tunnel instead: `ssh -L 9800:/tmp/objstrd.sock server-host` on the remote
  side, then connect to `localhost:9800`.
- The `fork` option on `TCP-LISTEN` allows multiple remote clients.
- The shared secret still provides application-level auth -- every remote
  client must send `SECRET <secret>\n` after connecting.

## Key Details

- The secret must be at least 8 characters. Comparison uses constant-time XOR to prevent timing side-channels.
- `max_readers` is capped at 64 (`MAX_READERS_CEILING`). Connections beyond the limit receive `ERR max readers\n`.
- Clients that don't send `SECRET` within 10 seconds are disconnected.
- The socket file is removed on `EventServer` drop and cleaned up if stale from a previous crash.
- The CLI (`rawobjstr`) does not use event sockets -- it's designed for long-lived server processes like `objstrd`.
- The `subscribe_events` helper spawns a tokio task that calls your callback on each event. It handles auth but does **not** auto-reconnect -- if the connection breaks, the task exits.
- `subscribe_events_tcp(addr, secret, callback)` is the TCP equivalent -- connects to a `host:port` instead of a Unix socket.
- `subscribe_events_auto(addr, secret, callback)` dispatches automatically: if `addr` starts with `tcp:` it uses TCP, otherwise Unix socket.
- `parse_event(line) -> Option<StoreEvent>` parses a single wire-protocol line back into a `StoreEvent`. Useful when consuming the protocol from a raw socket.
- `EventLogFn` (`Arc<dyn Fn(&str, &str) + Send + Sync>`) is an optional logging callback passed to `EventServer::start()` for diagnostic messages.

See [objstrd/examples/streaming-replica.sh](../objstrd/examples/streaming-replica.sh) for a more complete example.