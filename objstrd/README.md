# objstrd

S3-compatible distributed object store daemon. A single binary that runs in
one of three roles:

| Role | Description |
|------|-------------|
| **standalone** | Single-node S3 server (default, current behavior) |
| **node** | Data node in a cluster: serves S3, heartbeats to coordinator (planned) |
| **coordinator** | Cluster orchestrator: global index, replication, repair-replication (planned) |

> **Note:** The tree config (`--config`) for multi-shard and multi-node
> local clusters is fully implemented today. The planned `node` and
> `coordinator` roles refer to a future globally-distributed coordination
> protocol.

Exposes any `ObjectStore` backend (raw block device, loopback image, local filesystem, in-memory, or real S3) as an S3-compatible HTTP endpoint.

---

## Quick Start (standalone)


Clone and build the daemon with a single command:

```bash
git clone https://github.com/sysadminmike/objstr.git
cd objstr/objstrd
chmod +x ./build.sh
./build.sh --location ~/build-objstrd
```


```bash
# Create a 512 MB image and serve it on port 8000
objstrd --image /tmp/store.raw --size-mb 512 --port 8000

# With zstd compression
objstrd --image /tmp/store.raw --compression zstd

# Environment-variable style 
IMAGE=/tmp/store.raw SIZE_MB=512 PORT=8000 objstrd
```


### Alternative backends

```bash
# Local filesystem backend -- serves /tmp/mydata as an S3 bucket
objstrd --backend fs --fs-root /tmp/mydata --port 8000

# In-memory backend -- fast throwaway store, data lost on exit
objstrd --backend mem --port 8000

# S3 proxy -- re-expose an existing S3 bucket as a local endpoint
objstrd --backend s3 --s3-endpoint http://upstream:9000 \
  --s3-bucket mybucket --s3-access-key AKID --s3-secret-key SECRET \
  --s3-path-style --port 8000
```

Each alternative backend is wrapped internally in a single-shard
`ShardedObjectStore` (rf=1), so all S3 operations (multipart, copy,
batch delete, listing, etc.) work exactly as they do with the raw backend.

---
## CLI Options & Environment Variables

See [CLI.md](CLI.md) for the complete CLI flag and environment variable
reference, including all backend types, recovery tuning, multi-store
configuration, and cluster roles.

---

## Architecture


```
  S3/R2 HTTP Clients (boto3, aws-sdk, rclone, mc, curl)
              |
           objstrd 
              |
        StoreBackend enum
        /                \
  Raw (standalone)    Sharded (cluster)
       |                    |
  RawObjectStore    ShardedObjectStore
                    Catalog + Replication
                    RawRefRegistry
                          |
                 +--------+--------+--------+
                 |        |        |        |
               Raw     LocalFS   InMem   S3/objstrd
```

```
+------------------------------------------------------------------+
|                         objstrd binary                            |
|                                                                   |
|  ROLE=standalone    ROLE=node           ROLE=coordinator          |
|  +--------------+   +---------------+   +--------------------+   |
|  | S3 server    |   | S3 server     |   | Coordinator API    |   |
|  | VizService   |   | VizService    |   | Global catalog     |   |
|  | single store |   | heartbeat tx  |   | Node registry      |   |
|  |              |   | registration  |   | Replication engine |   |
|  +--------------+   +---------------+   | Repair-replication |   |
|                                         +--------------------+   |
|                                                                   |
|  Shared modules:                                                  |
|    adapter.rs      - S3 protocol translation (s3s::S3 trait)      |
|    config.rs       - Config parsing (JSON file / env vars)        |
|    registry.rs     - Multi-store backend registry                 |
+------------------------------------------------------------------+
```


### S3 Protocol Stack

```
HTTP Client (aws-cli, boto3, LanceDB, rclone)
    |
    v
ObjectStoreS3Adapter  (adapter.rs)
    |
    v
StoreBackend enum
    |
    +-- Raw(Arc<RawObjectStore>)                    single device
    +-- Sharded(Arc<ShardedObjectStore>,             multi-shard CRC32c placement
               Arc<RawRefRegistry>)                  + raw metadata routing
```

The `StoreBackend` enum branches into two modes:

- **`Raw`** - standalone mode with a single `RawObjectStore` on a block
  device. No sharding, no replication.
- **`Sharded`** - cluster mode with a `ShardedObjectStore` wrapping one or
  more shards. Each shard is any `Arc<dyn ObjectStore>` (raw, filesystem,
  in-memory, or S3). The `RawRefRegistry` tracks which shards are raw for
  metadata-aware operations.
---

## Modules

Everything below is implemented, tested, and working today.

### Core Modules

| Module | Description |
|--------|-------------|
| `adapter.rs` | Full S3 trait impl: PUT/GET/DELETE/HEAD/LIST/COPY, multipart, batch delete |
| `config.rs` | Config parsing: JSON file, `STORE_N_*` env vars, legacy single-store env |

> **Batch delete note:** In sharded mode, DELETE events are emitted for
> every key in the batch, including keys that do not exist. This differs
> from single-store mode (which only emits for existing keys) and is
> intentional - it keeps streaming replicas in sync when the sender and
> receiver have different catalog states.
| `logging.rs` | Structured logging: LogBuffer ring, log queries, category filtering |
| `recovery.rs` | Background health polling, auto-sync, proactive re-replication, over-replication trim, periodic repair-replication |
| `registry.rs` | StoreRegistry: tracks N backend stores by name, kind, shard index |
| `viz.rs` | Admin endpoints, VizService, cluster dashboard, per-shard routing |
| `server.rs` | HTTP server: s3s 0.13, SigV4 auth, hyper 1, multi-shard, cluster dashboard |
| `build.rs` | Version baking: `/_admin/info` returns `build_git_hash`, `build_date` |

### S3 Operations

All standard S3 operations are supported:

CreateBucket, DeleteBucket, HeadBucket, ListBuckets, GetBucketLocation,
PutObject, GetObject (including Range), HeadObject, GetObjectAttributes,
DeleteObject, DeleteObjects (batch), CopyObject, ListObjectsV1,
ListObjectsV2 (prefix, delimiter, pagination), CreateMultipartUpload,
UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts,
ListMultipartUploads.

GetObjectAttributes returns ETag, ObjectSize, StorageClass, and
LastModified.  ObjectParts (multipart part manifest) is not yet
persisted during CompleteMultipartUpload and will return empty.

### Features

| Feature | Notes |
|---------|-------|
| StoreBackend dispatch | Raw vs Sharded via enum in `adapter.rs` |
| Admin API | Endpoints for health, objects, shards, recovery, logging, and HTML dashboards. See [ADMIN-API.md](ADMIN-API.md). |
| Replication (local shards) | Put to N shards via ShardedObjectStore, read fallback across replicas |
| CRC error tracking | Per-shard atomic CRC error counter; corrupt replicas auto-removed from catalog |
| Bucket persistence | Bucket names persisted to `{IMAGE}.buckets.json` sidecar file. Atomic save (write-to-temp + rename). On startup, loads from JSON if it exists; if missing or corrupt, rebuilds by scanning the index for top-level path components. |
| Writer/reader events | Unix event socket broadcasts PUT, DELETE, and FLUSH events to subscribers |
| Graceful shutdown | CTRL-C handler, index flush for all shards before exit |
| Live config reload | SIGHUP or `POST /_admin/reload` to re-read config, close and reopen all shards |

### Tests

See [TESTS.md](TESTS.md) for the full list of integration test files
and instructions for running them. External S3 compatibility suites
(Ceph s3-tests, MinIO Mint, rclone) are in
[external-tests/](../external-tests/).

---

Admin endpoint reference is in [ADMIN-API.md](ADMIN-API.md).

---

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| [lance](benchmarks/lance/) | LanceDB S3 HTTP layer - measures write and scan throughput across six backends, isolating the cost of the S3/HTTP stack |
| [listing](benchmarks/listing/) | S3 ListObjectsV2 speed - compares paginated listing performance across raw, filesystem, and in-memory backends |
| [catalog](benchmarks/catalog/) | Catalog persistence - measures rebuild, JSON/bincode load and save times for stores with 300k+ objects |
| [rclone](benchmarks/rclone/) | rclone throughput - upload/download speed across 8 backends using `rclone test speed` (7 phases, single PUT and multipart) |

---

## Background Tasks

The daemon spawns several background tasks at startup. All tasks are
cancelled cleanly on shutdown (Ctrl-C) and on config reload (SIGHUP /
`POST /_admin/reload`).

| Task | Interval | Config | Notes |
|------|----------|--------|-------|
| **Periodic index flush** | 5 s | `--flush-interval` / `FLUSH_INTERVAL_SECS` | Writes dirty index pages to disk. Per raw shard. Skipped in read-only mode. |
| **Recovery polling** | 10 s | `--recovery-poll-secs` / `RECOVERY_POLL_SECS` | Probes each shard for health. Detaches after N consecutive failures (`--recovery-failure-threshold`, default 3). Repairs under-replicated objects. Trims over-replicated objects. Status at `/_admin/recovery`. |
| **Background repair-replication** | configurable | `--repair-replication-interval-secs` / `REPAIR_REPLICATION_INTERVAL_SECS` | Periodically rebuilds the catalog and runs repair-replication sweeps to populate empty shards (e.g. fresh NVMe) from existing replicas. Objects become queryable incrementally. Disabled by default (interval=0). Status at `/_admin/repair-replication-status`. |
| **Free-space tracker** | 30 s | (not configurable) | Updates cached free-space map for capacity-aware replica placement. Multi-shard and cluster modes only. |
| **Bucket rescan** | 5 s | (not configurable) | Scans the index for new bucket prefixes. Single-store read-only mode only. Wakes immediately on FLUSH events. |
| **Event socket server** | event-driven | `--event-socket` / `--event-secret` / `--max-readers` | Broadcasts PUT, DELETE, FLUSH events to Unix socket subscribers. Read-write mode. |
| **Event socket subscriber** | event-driven | `--event-socket` / `--event-secret` | Subscribes to a writer's event socket and reloads the index on FLUSH. Read-only mode. |
| **Streaming replica** | event-driven | `--event-source` / `--event-secret` | Applies PUT/DELETE events from a writer to an in-memory catalog via HEAD calls. Replaces recovery polling. Cluster (tree-config) mode only. |
| **Signal handlers** | on-demand | - | SIGHUP triggers config reload; Ctrl-C triggers graceful shutdown with a 10-second drain timeout. |

### Recovery task flags

All recovery flags can be set via CLI or environment variable. When using
a tree config file (`--config`), values in the config override CLI flags.

| Flag | Env var | Default |
|------|---------|---------|
| `--recovery-enabled` | `RECOVERY_ENABLED` | `true` |
| `--recovery-poll-secs` | `RECOVERY_POLL_SECS` | `10` |
| `--recovery-probe-timeout` | `RECOVERY_PROBE_TIMEOUT_SECS` | `5` |
| `--recovery-failure-threshold` | `RECOVERY_FAILURE_THRESHOLD` | `3` |
| `--recovery-re-replicate-batch` | `RECOVERY_RE_REPLICATE_BATCH_SIZE` | `100` |

### Repair-replication task flags

The background repair-replication task is disabled by default. When enabled
(interval > 0), it runs in a separate tokio task and does not interfere
with the recovery polling loop.

| Flag | Env var | Default |
|------|---------|---------|
| `--repair-replication-interval-secs` | `REPAIR_REPLICATION_INTERVAL_SECS` | `0` (disabled) |
| `--repair-replication-batch-size` | `REPAIR_REPLICATION_BATCH_SIZE` | `500` |

---

## Admin API

See [ADMIN-API.md](ADMIN-API.md) for the complete admin endpoint reference,
including all `/_admin/*` routes, query parameters, HTML pages, and examples.

---

## Live Config Reload (SIGHUP)

objstrd supports live config reload without restarting the process. When
triggered, the daemon:

1. **Stops accepting** new connections (existing in-flight requests drain
   with a 10-second timeout).
2. **Cancels flush timers** - the periodic background tasks that write
   dirty index pages to disk are stopped cleanly.
3. **Flushes all raw stores** - every shard with pending index changes is
   flushed to disk so no data is lost.
4. **Drops all old state** - the `RawObjectStore` arcs, the
   `ShardedObjectStore`, the S3 adapter, the `VizService`, and the recovery
   task are all dropped. This releases file locks (`flock`) and file
   descriptors, so renamed/moved shard files are fully detached.
5. **Re-parses the config file** - the tree config (or env-var config) is
   read again from disk, picking up any changes to shard paths, new shards,
   removed shards, or changed replication factor.
6. **Rebuilds all stores** - new `RawObjectStore` instances are opened
   (acquiring fresh file locks and file descriptors on the current paths),
   a new `ShardedObjectStore` is constructed, a new S3 adapter and
   `VizService` are built, and new flush timers and recovery tasks are
   spawned.
7. **Resumes accepting** connections on the same TCP listener (the socket
   stays open across reloads so the port remains bound and clients queue in
   the TCP backlog during the brief pause).

The whole cycle typically completes in under a second.

### Triggering a reload

**Via SIGHUP (Unix signal):**

```bash
kill -HUP $(pidof objstrd)
```

**Via HTTP endpoint (any platform):**

```bash
curl -X POST http://localhost:8000/_admin/reload \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"message":"reload initiated"}
```

The HTTP endpoint is protected by the admin token (same as other
`/_admin/*` endpoints). It can also be triggered from the Server Info web
page using the Reload button.

### Use cases

- **Renamed/moved shard files:** If you `mv shard-a.raw new-shard-a.raw`
  and update the config, the old file descriptor still points at the old
  inode. A reload closes it and opens the new path.
- **Adding or removing shards:** Edit the config to add or remove shard
  entries, then reload. The daemon picks up the new topology.
- **Changing replication factor:** Update `rf=` in the config and reload.
- **Rotating credentials:** If access keys are stored in env vars, update
  them and send SIGHUP (env vars are re-read on each reload iteration).

**Warning - in-memory shards:** Any shard declared as `mem` stores data
only in RAM. A config reload drops and recreates all stores, so
**all data in memory shards is lost on reload**. Raw and filesystem
shards are unaffected because their data is flushed to disk first.

### Device disappearance detection

If a backing device or file is removed while the daemon is running (e.g. a
USB drive is unplugged, or someone deletes the image file), the recovery
task detects the missing path within one poll cycle (default 10 seconds)
and immediately detaches the shard. The shard is marked `Offline` and all
requests routed to it return errors.

On Linux, deleting a file while the fd is open does not immediately break
I/O (the inode stays alive). However, once the server reloads or restarts,
the file is gone and the shard cannot be reopened. The path-existence check
catches this case proactively so administrators see a warning before the
next reload fails.

### Cluster behavior

When a node in a cluster tree reloads:

- **Parent nodes** see a brief period (under 1 second) where the child is
  unreachable. The parent's recovery task probes every 10 seconds and
  requires 3 consecutive failures before detaching, so a fast reload is
  invisible to the parent.
- **Child nodes** are unaffected - they keep running independently. The
  reloading node creates fresh S3 clients to its children on rebuild.
- **If rf > 1**, in-flight reads from other clients fall back to other
  replicas automatically.

---

## Event Socket

```bash
# Writer: start event socket
objstrd --config cluster.conf --node primary \
  --event-socket /run/objstrd/events.sock \
  --event-secret "my-shared-secret"

# Broadcasts PUT, DELETE, and FLUSH events over a single socket
```

**Limitations:**

- Unix-only (`#[cfg(unix)]`). Not available on Windows.
- FLUSH events only fire for raw-backed shards. Filesystem, S3, and in-memory
  shards have no transaction ID.
- PUT and DELETE events are emitted for all shard types.
- Batch delete (`DeleteObjects`): In sharded (multi-shard) mode, DELETE
  events are emitted for all keys including keys that did not exist
  (because the sharded backend returns success for missing keys). In raw
  standalone mode, DELETE events are only emitted for keys that actually
  existed; missing keys are silently skipped.
- In a cluster that is **not** using mirror-style replication (i.e. every
  object stored on every shard), a write may land on a non-raw shard and
  therefore never trigger a FLUSH event. Read-only subscribers that rely on
  FLUSH to call `reload_index()` will miss those updates. If you need
  FLUSH-driven reload for all writes, either use mirror replication so every
  object reaches at least one raw shard, or use only raw-backed shards.

---

## Example Use Cases

### Change Feed: Watching for New Objects

Use `/_admin/objects?since_txn=N` to poll for new objects, or connect to
the event socket for real-time `PUT`/`DELETE`/`FLUSH` notifications.
See [`examples/change-feed.md`](examples/change-feed.md) for a complete
shell script and parameter reference.

### Writer + Read-Only Replica

Run a writer serving S3 on port 8000 and a read-only replica on port 8001
that automatically picks up new objects via event socket.

> **Note:** In tree-config or multi-shard mode, a read-only node subscribes to
> the writer's event socket and calls `reload_index()` on FLUSH. However, FLUSH
> events only fire for raw-backed shards. If the cluster uses non-mirror
> replication (objects are **not** written to every shard), a write that lands
> on a non-raw shard will not produce a FLUSH and the read-only replica will
> not see it until the next periodic reload. Use mirror replication or an
> all-raw-shard topology to guarantee FLUSH-driven updates for every write.

```bash
# Terminal 1: writer
objstrd --image /tmp/store.raw --size-mb 512 --port 8000 \
  --event-socket /tmp/objstrd.sock --event-secret "my-shared-secret"

# Terminal 2: read-only replica
objstrd --image /tmp/store.raw --port 8001 --read-only \
  --event-socket /tmp/objstrd.sock --event-secret "my-shared-secret"

# Terminal 3: write via S3, read from reader
aws s3 cp myfile.txt s3://testbucket/myfile.txt --endpoint-url http://localhost:8000
aws s3 cp s3://testbucket/myfile.txt - --endpoint-url http://localhost:8001
```

### Streaming Replica (Event-Driven)

The reader subscribes to the writer's event stream and applies PUT/DELETE
changes to its in-memory catalog in real time. Works with any shard backend
(fs, S3, mem, raw) and does not require periodic index scans.

See [`examples/streaming-replica.sh`](examples/streaming-replica.sh) for a
complete working example with 1 writer + 2 readers (one via Unix socket,
one via socat TCP bridge).

### S3/R2 Pass-Through Proxy

Expose an upstream AWS S3 or Cloudflare R2 bucket as a local S3 endpoint.
Useful for local auth, provider abstraction, and development/testing.
See [`examples/s3-proxy.md`](examples/s3-proxy.md) for configs and details.

### Auto-Syncing Local Mirror

Run a local objstrd as your dev endpoint, auto-synced to AWS S3, R2, or
another objstrd via `aws s3 sync`. Either side can go offline and
reconciles when it comes back. See
[`examples/sync-mirror.md`](examples/sync-mirror.md) for setup and configs.

### Heterogeneous Auto-Recovery Cluster

Mix local filesystem, R2, and AWS S3 shards under a single `rf=3` endpoint.
If any shard goes offline, reads/writes continue on the remaining replicas;
when it returns, auto-recovery restores full replication. See
[`examples/hybrid-mirror.md`](examples/hybrid-mirror.md) for config and
recovery flow details.

### Data-Local Work Queue

Run objstrd on each worker machine. A dispatcher PUTs work items to nodes
(RF=1, routed by free space). A simple bash script on each node watches
the event socket, reads locally, runs a command, and deletes the object.
Workers need zero cluster awareness. See
[`examples/work-queue.md`](examples/work-queue.md) for dispatcher and
worker scripts, including a Ray cluster variant.

---

## File Structure

```
objstrd/
+-- Cargo.toml
+-- README.md                           # This file
+-- AGENTS.md                           # AI agent hints
+-- ADMIN-API.md                        # Admin endpoint reference
+-- BUILD.md                            # Build instructions
+-- CLI.md                              # CLI flags and env var reference
+-- METADATA-STORAGE-FORMAT.md          # On-disk metadata TLV format
+-- TESTS.md                            # Running the test suite
+-- build.rs                            # Version baking
+-- build.sh                            # Standalone build script
+-- src/
|   +-- lib.rs                          # Module exports
|   +-- adapter.rs                      # S3 protocol adapter (s3s::S3 trait impl)
|   +-- config.rs                       # Multi-store config parsing (JSON / env vars)
|   +-- logging.rs                      # Structured logging, LogBuffer, log queries
|   +-- recovery.rs                     # Background health polling, auto-sync, re-replication
|   +-- registry.rs                     # Multi-store backend registry
|   +-- viz.rs                          # Admin endpoints, VizService, cluster dashboard
|   +-- bin/
|       +-- server.rs                   # Main binary, VizService, cluster support
+-- static/                             # Admin dashboard HTML pages
+-- examples/                           # Use case examples (change feed, streaming replica, etc.)
+-- benchmarks/                         # Performance benchmarks (catalog, lance, listing, rclone)
+-- tests/                              # Integration tests
```
