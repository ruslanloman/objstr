# objstrd -- Admin API Reference

All `/_admin/*` endpoints are optionally protected by `--admin-token`.
When set, requests must include `Authorization: Bearer <token>`.
S3 operations are not affected by the admin token.

CORS preflight (`OPTIONS`) and response headers are enabled when
`--cors-origin` is set.

---

## Endpoint Reference

### Info & Health

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/info` | GET | JSON | Server stats: pid, build_git_hash, build_date, version/hash fields for rawobjstr and shardedobjstr, role, node_name, port, bind, bucket, backend, has_raw, cluster_mode, shard_count, replication_factor, read_only, device info (raw only), cluster totals, event_socket/event_source (if configured) |
| `/_admin/sysinfo` | GET | JSON | System stats: process_uptime_secs, config_uptime_secs, system_uptime_secs, load_avg_1/5/15, mem_total_kb, mem_free_kb, mem_available_kb, process_rss_kb |
| `/_admin/buckets` | GET | JSON | List of registered bucket names |
| `/_admin/nodeconfig` | GET | JSON | Node configuration: shard_count, shards array, bucket, recovery config |

### Index & Cache Operations

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/flush` | POST | JSON | Force immediate index checkpoint (normally on 5s tick) |
| `/_admin/rebuild-index` | POST | JSON | Reload index from disk, rebuild bucket registry. Returns bucket count. |
| `/_admin/clear-bucket-cache` | POST | JSON | Delete `__buckets__/` objects, rescan prefixes, rebuild registry |

### Object Listing & Metadata

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/objects` | GET | JSON | Paginated object listing. Per-object: key, size, padded_size, offset, created_txn, last_modified. Excludes `__buckets__` keys. In cluster mode, deduplicates across shards. |
| `/_admin/extent_meta?key=K` | GET | JSON | Decoded TLV metadata for a specific key |
| `/_admin/getraw?key=K` | GET | bytes | Raw (uncompressed) extent bytes for a key (debug). Response headers: `x-raw-uncompressed-size`, `x-raw-compression`. |

**`/_admin/objects` query parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `offset` | 0 | Pagination offset |
| `limit` | 100 | Max results per page (capped at 1000) |
| `prefix` | (none) | Filter keys containing this substring |
| `since_txn` | 0 | Only return objects with `created_txn > since_txn` |

### Device Visualization (Raw Backend)

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/heatmap` | GET | JSON | Aggregated chunk density. Arrays: file_counts, used_bytes, free_bytes, dead_bytes. |
| `/_admin/region?from=X&to=Y` | GET | JSON | Extents and free regions within a byte range |

**`/_admin/heatmap` query parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `chunks` | 256 | Number of chunks to aggregate into (min 16, max 4096) |

Returns HTTP 503 for heatmap/region when backend is not `raw`.

### Replication & Recovery (Cluster Mode)

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/shards` | GET | JSON | Per-shard health, type, object/byte counts, crc_error_count. Corrupt replicas detected via CRC mismatch are auto-removed from the catalog. |
| `/_admin/recovery` | GET | JSON | Live recovery status: enabled, poll_interval_secs, poll_cycles, total_re_replicated, total_trimmed, under/over_replicated_count, last_poll_at |
| `/_admin/repair-replication` | POST | JSON | Run one repair-replication sweep (under-replicated repair + over-replicated trim). Returns re_replicated, trimmed, remaining counts. |
| `/_admin/repair-replication-status` | GET | JSON | Live background repair-replication task status: enabled, phase, interval_secs, batch_size, cycles_completed, total_objects_replicated, total_objects_trimmed, last_catalog_size, remaining_under_replicated, last_started_at, last_completed_at. Returns `{"enabled":false}` when the task is not active. |
| `/_admin/drain/{id}` | POST | JSON | Drain all objects off a shard. Detaches shard (Detached), copies objects to healthy shards. Returns ok, shard_id, moved, skipped, errors, deleted, delete_errors, re_replicated, under_remaining. |
| `/_admin/redistribute` | POST | JSON | Redistribute objects to balance shard object counts. Moves objects from the fullest to the emptiest shard (replicate then delete) until within 10% tolerance. Returns moved, skipped, errors counts. Requires no under-replicated objects. |
| `/_admin/admin-op-status` | GET | JSON | Returns `{running: bool, operation?: string}`. Reports whether a long-running admin operation (drain, repair-replication, redistribute) is in progress. Only one can run at a time. |
| `/_admin/take-offline/{id}` | POST | JSON | Take a shard offline manually. Shard enters Detached state; recovery loop will not auto-reattach it. Optional query param `?suppress_replication=true` prevents re-replication of objects on this shard. Returns previous_health. |
| `/_admin/attach/{id}` | POST | JSON | Reattach a Detached or Offline shard. Runs `sync_and_reattach`: replays delete markers from healthy shards, cleans stale copies, then rescans and rebuilds catalog. Returns objects_found count. Returns 409 if shard is already Healthy. |
| `/_admin/vacuum` | POST | JSON | Purge fully-applied delete markers from all shards. Returns `{ok, purged, cleaned}`. Returns 409 if any shard is offline or detached. |
| `/_admin/cross-verify` | POST | JSON | Run cross-shard MD5 verification. Compares object content across all replicas and reports mismatches. Streams progress to `/_admin/op-log/stream`. Returns `{ok, checked, matched, mismatched, errors, skipped_single_replica}`. |

Returns HTTP 503 for repair-replication/drain/redistribute/cross-verify on standalone (non-cluster) servers.

### Admin Operation Progress

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/op-log/stream` | GET | SSE | Server-Sent Events stream of admin operation progress messages. Sends `data: {"message":"..."}` frames during drain, repair-replication, redistribute, or cross-verify. Sends `event: done` when the operation completes. Connect with EventSource or `curl -N`. Accepts `?token=<value>` query param as auth fallback for EventSource clients that cannot set headers. |

### Config Reload

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/reload` | POST | JSON | Trigger graceful config reload (same as SIGHUP) |

### Logging

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/logs` | GET | JSON | Structured log entries with filtering. Returns total, returned, entries array. |
| `/_admin/logs/ui` | GET | HTML | Log viewer web page |

**`/_admin/logs` query parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `limit` | 100 | Max entries to return (capped at 10,000) |
| `level` | (all) | Minimum level filter: `debug`, `info`, `warn`, `error` (hierarchical) |
| `category` | (all) | Filter by category (comma-separated, see table below) |
| `service` | (all) | Filter by service: `s3`, `coord`, `admin`, `internal` |
| `q` | (none) | Text search in log messages (case-insensitive, percent-decoded) |
| `since` | (none) | ISO 8601 minimum timestamp |
| `until` | (none) | ISO 8601 maximum timestamp |

#### Log Categories

Each log entry gets a `category` tag so logs can be filtered:

| Category | What gets logged | Level |
|----------|-----------------|-------|
| `requests` | Every S3 client request: method, path, status code, latency_ms, bytes_in, bytes_out, client_ip | `info` |
| `index` | Master/coordinator index queries: `/_coord/placement`, `/_coord/locate`, `/_coord/notify` -- key, requester node, result, latency_ms | `info` |
| `replication` | Shard write fan-out: key, target shards, per-shard success/fail, total latency_ms | `info` |
| `health` | Shard health transitions (Healthy->Degraded->Offline->Detached->Syncing), health poll results, detach/attach events | `info` |
| `recovery` | Auto-recovery events: sync started/completed, objects replicated, delete log replay, quiesce windows | `info` |
| `admin` | Admin endpoint calls: `/_admin/*`, `/_coord/*` management operations, who called, result | `info` |
| `lifecycle` | Startup, shutdown, config loaded, flush cycles, shard open/close | `info` |
| `errors` | All warn/error events from any category, with original category preserved as sub-field | `warn`/`error` |

#### Log Entry Format

Each log entry is a struct:

```rust
struct LogEntry {
    time: DateTime<Utc>,       // ISO 8601 timestamp
    level: String,             // "debug", "info", "warn", "error"
    category: String,          // "requests", "health", "recovery", etc.
    service: String,           // "s3", "coord", "admin", "internal"
    message: String,
}
```

**JSON response fields:** time, level, category, service, message.

#### In-Memory Ring Buffer

A fixed-size ring buffer (default 10,000 entries, configurable via
`--log-buffer-size` / `LOG_BUFFER_SIZE`) holds recent log entries in memory.
This is separate from stdout logging -- stdout continues as before for
external log aggregation. The ring buffer feeds the REST API and web UI.

Implementation: `Arc<Mutex<VecDeque<LogEntry>>>` with a custom tracing
`Layer` that captures formatted spans/events and pushes them into the
buffer, dropping the oldest when full.

#### Persistent File Log

In addition to the ring buffer, all log entries are appended to a text
file on disk when `--log-file <PATH>` / `LOG_FILE` is set. Format is
tab-separated: `timestamp\tlevel\tservice\tcategory\tmessage`. This
file survives restarts and can be ingested by Loki, Promtail, Filebeat,
or any other log shipper for centralized aggregation across nodes.

- File is opened in append mode at startup
- No rotation built in -- use external logrotate or similar
- Ring buffer is always active (for the web UI); file is optional
- Future: JSON-lines format option for structured log shippers

#### CLI Flags

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--log-file <PATH>` | `LOG_FILE` | string | (disabled) | Append text logs to file (tab-separated) |
| `--log-buffer-size <N>` | `LOG_BUFFER_SIZE` | usize | 10000 | In-memory log ring buffer size (100-1,000,000) |

### Per-Shard Endpoints

In multi-shard mode, each shard has its own set of admin endpoints:

| Endpoint | Method | Response | Description |
|----------|--------|----------|-------------|
| `/_admin/shard/{id}/info` | GET | JSON | Per-shard stats. Raw shards: shard_id, shard_name, type, device_path, device_size, format_version, direct_io, txn_id, file_count, data_bytes_stored, device_bytes_used, free_space, free_fragments, largest_free_extent, index_serialized_bytes, index_slot_capacity, shard_sizes, shard_slot_size, compression. Non-raw shards: shard_id, shard_name, type, file_count, data_bytes_stored. |
| `/_admin/shard/{id}/objects` | GET | JSON | Per-shard object listing with pagination (same params as `/_admin/objects`) |
| `/_admin/shard/{id}/viz` | GET | HTML | Per-shard device heat map page |
| `/_admin/shard/{id}/ui` | GET | HTML | Per-shard object manager page |
| `/_admin/shard/{id}/server` | GET | HTML | Per-shard server info page |

Invalid shard IDs return 404. Non-numeric IDs return 400.

### HTML Pages

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/_admin/` | GET | Cluster dashboard (same as `/_admin/cluster`): shard cards, usage bars, Repair Replication button, Vacuum Delete Markers button, per-shard Drain, CRC error display |
| `/_admin/viz` | GET | Device heat map visualizer (pure SVG, no D3.js) |
| `/_admin/ui` | GET | Object manager page (paginated table with live search) |
| `/_admin/server` | GET | Server info page (with Reload button) |
| `/_admin/cluster` | GET | Cluster dashboard (same as `/_admin/`) |
| `/_admin/config` | GET | Node config viewer page |
| `/_admin/logs/ui` | GET | Structured log viewer page |

---

## Examples

### Force Index Flush

```bash
curl -X POST http://localhost:8000/_admin/flush \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true}
```

### Repair Replication

```bash
curl -X POST http://localhost:8000/_admin/repair-replication \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"re_replicated":3,"trimmed":1,"under_remaining":0,"over_remaining":0}
```

### Drain a Shard

```bash
curl -X POST http://localhost:8000/_admin/drain/2 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"shard_id":2,"re_replicated":12,"trimmed":0,"under_remaining":0,"over_remaining":0}
# Shard is now Detached (will not auto-reattach)
```

### Redistribute Objects

```bash
curl -X POST http://localhost:8000/_admin/redistribute \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"moved":15,"skipped":2,"errors":0,"shard_counts":[[0,50],[1,48]]}
# Objects are moved from fuller to emptier shards until within 10% tolerance.
# Requires no under-replicated objects. Mutually exclusive with drain and
# repair-replication.
```

### Cross-Verify Replicas

```bash
curl -X POST http://localhost:8000/_admin/cross-verify \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"checked":150,"matched":148,"mismatched":2,"errors":0,"skipped_single_replica":5}
# Compares MD5 of object content across all replicas.
# Streams progress to /_admin/op-log/stream during execution.
```

### Watch Admin Operation Progress (SSE)

```bash
# In one terminal, subscribe to the progress stream:
curl -N http://localhost:8000/_admin/op-log/stream \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# data: {"message":"cross-verify: checked 50/150 objects"}
# data: {"message":"cross-verify: checked 100/150 objects"}
# event: done
# data: done

# In another terminal, start the operation:
curl -X POST http://localhost:8000/_admin/cross-verify \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Admin Operation Status

```bash
curl http://localhost:8000/_admin/admin-op-status
# When idle: {"running":false}
# During op: {"running":true,"operation":"redistribute"}
```

### Take a Shard Offline

```bash
# Take offline (re-replication allowed after grace period)
curl -X POST http://localhost:8000/_admin/take-offline/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"shard_id":1,"previous_health":"Healthy","suppress_replication":false}

# Take offline and suppress re-replication
curl -X POST 'http://localhost:8000/_admin/take-offline/1?suppress_replication=true' \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"shard_id":1,"previous_health":"Healthy","suppress_replication":true}
```

### Reattach a Shard

```bash
curl -X POST http://localhost:8000/_admin/attach/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"shard_id":1,"objects_found":4523}
# Runs sync_and_reattach: replays delete markers, cleans stale objects,
# rescans index, rebuilds catalog for that shard.
# Returns 409 if shard is already Healthy.
```

### Vacuum Delete Markers

```bash
curl -X POST http://localhost:8000/_admin/vacuum \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"purged":12,"cleaned":0}
# Removes fully-applied __deleted__/* marker objects from all shards.
# Returns 409 if any shard is offline/detached.
```

### Query Shard Health

```bash
curl http://localhost:8000/_admin/shards
# => {"mode":"sharded","replication_factor":2,"shard_count":3,"shards":[...]}
# Per-shard fields: id, name, type, health, crc_error_count,
#   detach_reason, suppress_replication,
#   device_path, device_size, file_count, data_bytes_stored, etc.
# Health values: Healthy, Degraded, Offline, Detached, Syncing
```

### Watch for New Objects (Change Feed)

```bash
# Get the current txn_id
TXN=$(curl -s http://localhost:8000/_admin/info | jq '.txn_id')

# Later: fetch only objects written since that txn
curl -s "http://localhost:8000/_admin/objects?since_txn=$TXN" | jq '.objects[].key'

# Filter by prefix
curl -s "http://localhost:8000/_admin/objects?since_txn=$TXN&prefix=images/" | jq '.objects[].key'
```

### Query Logs

```bash
# Recent error logs
curl "http://localhost:8000/_admin/logs?level=error&limit=50"

# Search for a string in log messages
curl "http://localhost:8000/_admin/logs?q=connection+refused"

# Logs from a specific time range
curl "http://localhost:8000/_admin/logs?since=2026-04-01T00:00:00Z&limit=100"
```

### Trigger Config Reload

```bash
# Via HTTP
curl -X POST http://localhost:8000/_admin/reload \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# => {"ok":true,"message":"reload initiated"}

# Via Unix signal
kill -HUP $(pidof objstrd)
```

---

## HTML Pages

### viz.html (Device Heat Map)

Pure SVG heat map: 512 chunks, 64 columns, density-colored cells.
Click a cell to view `/_admin/region` detail. Shift-click to select a byte
range. Index shard fill heat map below the device map. Stats bar: device size,
file count, used %, free, fragment count, txn_id. Polls `/_admin/heatmap`
every 5s; polls `/_admin/info` every 10s.

### ui.html (Object Manager)

Paginated object table: TXN, Key, Size, On-disk, Offset (hex), Last Modified,
Actions. Server-side pagination via `/_admin/objects`; 300ms debounced prefix
filter; 5s polling.

### cluster.html (Cluster Dashboard)

Shard cards showing health status, usage bars, per-shard links. Includes
Repair Replication button, per-shard Drain button, CRC error display.

### server.html (Server Info)

Server build info, uptime, configuration summary. Includes Reload button.

### config.html (Config Viewer)

Node configuration display from `/_admin/nodeconfig`.

### logs.html (Log Viewer)

Structured log viewer with filtering by level, category, service, and text
search. Auto-refreshing.

---

## Error Responses

| Status | Condition |
|--------|-----------|
| 403 | Missing or incorrect `Authorization: Bearer` when `--admin-token` is set |
| 404 | Unknown `/_admin/*` path, or invalid shard ID |
| 400 | Non-numeric shard ID in `/_admin/drain/{id}` or `/_admin/shard/{id}/*` |
| 503 | Raw-only endpoint (heatmap, region) when backend is not `raw`; cluster-only endpoint (repair-replication, drain, redistribute) on standalone server |
