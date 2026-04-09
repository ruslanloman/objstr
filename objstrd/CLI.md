# CLI Reference

All options can be set via CLI flags or environment variables. CLI flags take
precedence over environment variables, which take precedence over tree config
values, which take precedence over defaults.

See [BUILD.md](BUILD.md) for build instructions.

---

## Quick Start

```bash
# Create a 512 MB image and serve it on port 8000
objstrd --image /tmp/store.raw --size-mb 512 --port 8000

# With aws s3 authentication
objstrd --image /tmp/store.raw --access-key mykey --secret-key mysecret

# With zstd compression
objstrd --image /tmp/store.raw --compression zstd

# Legacy environment-variable style still works
IMAGE=/tmp/store.raw SIZE_MB=512 PORT=8000 objstrd
```

### Alternative Backends

```bash
# Local filesystem backend -- serves /tmp/mydata as an S3 bucket
objstrd --backend fs --image /tmp/mydata --port 8000

# In-memory backend -- fast throwaway store, data lost on exit
objstrd --backend mem --port 8000

# S3 proxy -- re-expose an existing S3 bucket as a local endpoint
objstrd --backend s3 \
  --s3-endpoint http://upstream:9000 \
  --s3-bucket mybucket \
  --s3-access-key AKID \
  --s3-secret-key SECRET \
  --s3-path-style --port 8000
```

### Tree Config (Multi-Shard / Cluster)

```bash
# Start from a tree config file as a specific node
objstrd --config cluster.conf --node primary

# Validate config without starting the server (useful for CI)
objstrd --check-config --config cluster.conf --node primary
```

See [CONFIG.md](../CONFIG.md) for tree config file format.

---

## Global Options

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--port <PORT>` | `PORT` | u16 | 8000 | HTTP listen port |
| `--bind <ADDR>` | `BIND` | string | 0.0.0.0 | Bind address |
| `--bucket <NAME>` | `BUCKET` | string | testbucket | Default bucket name |
| `--backend <TYPE>` | `BACKEND` | enum | raw | Backend type: `raw`, `fs`, `mem`, `s3` |
| `--role <ROLE>` | `ROLE` | enum | standalone | Daemon role: `standalone`, `node`, `coordinator` |
| `--help`, `-h` | -- | flag | -- | Show help and exit |
| `--version`, `-V` | -- | flag | -- | Show build version and exit |

---

## Raw Backend (`--backend raw`)

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--image <PATH>` | `IMAGE` | string | store.raw | Path to raw image file or block device |
| `--size-mb <MB>` | `SIZE_MB` | u64 | 256 | Image size in MB when creating a new image |
| `--direct-io` | `DIRECT_IO` | bool | 0 | Enable O_DIRECT (values: `1`, `true`) |
| `--compression <ALG>` | `COMPRESSION` | enum | none | Compression: `none`, `zstd`, `snappy`, `gzip0`..`gzip9` |
| `--read-only` | `READ_ONLY` | bool | 0 | Open in read-only mode (values: `1`, `true`) |
| `--flush-interval <SECS>` | `FLUSH_INTERVAL_SECS` | u64 | 5 | Periodic index flush interval in seconds |

---

## Filesystem Backend (`--backend fs`)

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--image <PATH>` | `IMAGE` | string | (required) | Root directory to serve as S3 bucket |

---

## S3 Backend (`--backend s3`)

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--s3-endpoint <URL>` | `S3_ENDPOINT` | string | (required) | Upstream S3 endpoint URL |
| `--s3-bucket <NAME>` | `S3_BUCKET` | string | (required) | Upstream S3 bucket name |
| `--s3-region <REGION>` | `S3_REGION` | string | us-east-1 | S3 region |
| `--s3-access-key <KEY>` | `S3_ACCESS_KEY` | string | (none) | Upstream S3 access key |
| `--s3-secret-key <KEY>` | `S3_SECRET_KEY` | string | (none) | Upstream S3 secret key |
| `--s3-path-style` | `S3_PATH_STYLE` | bool | false | Use path-style requests (values: `1`, `true`) |

---

## Authentication

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--access-key <KEY>` | `ACCESS_KEY` | string | (none) | SigV4 access key (enables auth for all S3 ops) |
| `--secret-key <KEY>` | `SECRET_KEY` | string | (none) | SigV4 secret key |
| `--admin-token <TOKEN>` | `ADMIN_TOKEN` | string | (none) | Bearer token for `/_admin/*` endpoints |
| `--cors-origin <ORIGIN>` | `ADMIN_CORS_ORIGIN` | string | (none) | CORS origin for `/_admin/*` responses |

Multiple credentials can be configured via numbered environment variables
(`ACCESS_KEY_1`/`SECRET_KEY_1` through `ACCESS_KEY_9`/`SECRET_KEY_9`).
The daemon registers each pair found in sequence and stops at the first
missing pair.

---

## Event Socket

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--event-socket <PATH>` | `EVENT_SOCKET` | string | (disabled) | Unix socket path for event notifications |
| `--event-secret <SECRET>` | `EVENT_SECRET` | string | (none) | Shared secret for event socket auth |
| `--max-readers <N>` | `MAX_READERS` | usize | 16 | Max concurrent event socket readers |
| `--event-source <URL>` | `EVENT_SOURCE` | string | (none) | Event source for streaming replica mode (forces read-only). Accepts a Unix socket path (e.g. `/tmp/objstrd.sock`) or a TCP address with `tcp:` prefix (e.g. `tcp:127.0.0.1:9000`). TCP is required on Windows. |

---

## Logging

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--log-file <PATH>` | `LOG_FILE` | string | (disabled) | Append text logs to file (tab-separated) |
| `--log-buffer-size <N>` | `LOG_BUFFER_SIZE` | usize | 10000 | In-memory log ring buffer size (100-1,000,000) |

---

## Cluster / Tree Config

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--config <PATH>` | `CONFIG` | string | (none) | Tree config file path (requires `--node`) |
| `--node <NAME>` | `NODE` | string | (none) | This node's name in the tree config |
| `--check-config` | -- | flag | -- | Validate config and exit (requires `--config`) |
| `--read-prefer <MODE>` | `READ_PREFER` | enum | (none) | Read preference: `ordered` or `round-robin` |

---

## Catalog Persistence

Persist the in-memory catalog to disk so restarts do not require a full
shard rescan. The catalog file includes an embedded CRC32c checksum for
integrity validation on load. Only dirty catalogs (those with unsaved
mutations) are written during periodic flushes and shutdown.

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--catalog-path <PATH>` | `CATALOG_PATH` | string | (none) | Path to catalog file. Omit to disable persistence |
| `--catalog-format <FMT>` | `CATALOG_FORMAT` | enum | json | Catalog format: `json` or `bincode` |
| `--catalog-flush-interval <SECS>` | `CATALOG_FLUSH_INTERVAL_SECS` | u64 | 0 | Periodic catalog flush interval in seconds. 0 = disabled (save on shutdown only) |

When `--catalog-path` is set, the daemon:

1. **Startup** -- loads the catalog file, validates its CRC32c checksum.
   If the file is missing, starts with an empty catalog.
   After loading, entries referencing offline or ephemeral (mem) shards
   are stripped so that `find_under_replicated()` reports accurate
   replica counts from the first poll cycle.
2. **Periodic flush** -- if `--catalog-flush-interval` > 0, a background
   task saves the catalog every N seconds, but only if dirty.
3. **Shutdown** -- saves the catalog if dirty before exiting.

Regardless of whether a catalog file is configured, the daemon rebuilds
the catalog from all healthy shards at startup (before serving traffic).
This ensures the catalog reflects the actual shard contents -- especially
important for ephemeral shards (mem) that lose data on restart.

Tree config equivalents: `catalog_path`, `catalog_format`, `catalog_flush_interval`.

---

## Recovery (Multi-Shard Mode)

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--recovery-enabled <BOOL>` | `RECOVERY_ENABLED` | bool | true | Master switch for the recovery loop |
| `--recovery-poll-secs <N>` | `RECOVERY_POLL_SECS` | u64 | 10 | Health poll interval in seconds (min 1) |
| `--recovery-probe-timeout <N>` | `RECOVERY_PROBE_TIMEOUT_SECS` | u64 | 5 | Probe timeout in seconds (min 1) |
| `--recovery-failure-threshold <N>` | `RECOVERY_FAILURE_THRESHOLD` | u32 | 3 | Consecutive failures before detach (min 1) |
| `--recovery-re-replicate-batch <N>` | `RECOVERY_RE_REPLICATE_BATCH_SIZE` | usize | 100 | Max objects per re-replication sweep (1-10,000) |

The recovery loop performs two replication-related passes every poll cycle:

1. **Under-replication repair** -- checks for objects below the replication
   factor regardless of shard health. This catches cases where shards are
   offline, data is missing (e.g. a mem shard lost data on restart, manual
   deletion, or partial writes), or any other scenario that leaves objects
   under-replicated.
2. **Over-replication trim** -- removes excess replicas from objects above the
   replication factor, shedding from the fullest shard first.

---

## Background Repair-Replication (Multi-Shard Mode)

Periodic background task that rebuilds the catalog and runs repair-replication
sweeps to populate empty shards from existing replicas. Disabled by
default (interval=0). Objects become queryable on the target shard
incrementally as each one is copied.

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--repair-replication-interval-secs <N>` | `REPAIR_REPLICATION_INTERVAL_SECS` | u64 | 0 | Seconds between repair-replication cycles. 0 = disabled. |
| `--repair-replication-batch-size <N>` | `REPAIR_REPLICATION_BATCH_SIZE` | usize | 500 | Max objects per repair-replication sweep (1-10,000) |

Status is available at `/_admin/repair-replication-status` and on the cluster
dashboard.

---

## Environment Variables (Multi-Store)

For multi-shard setups without tree config, use numbered environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_FILE` | (none) | JSON config file for multi-store setup |
| `STORE_N_TYPE` | (none) | Store type for shard N (0-indexed): `raw`, `fs`, `mem`, `s3` |
| `STORE_N_IMAGE` | (none) | Image path for raw shard N |
| `STORE_N_SIZE_MB` | (none) | Image size for raw shard N |
| `REPLICATION_FACTOR` | 1 | Replication factor across local shards |
| `MIN_WRITES` | max(rf-1, 1) | Minimum successful writes for a PUT to succeed |
| `DELETE_REQUIRES_MIN_WRITES` | false | When `true`, deletes also require `MIN_WRITES` replicas |

### Node Role (`ROLE=node`) -- not yet implemented

| Variable | Default | Description |
|----------|---------|-------------|
| `COORDINATOR` | (required) | Coordinator address (e.g. `http://10.0.0.1:9000`) |
| `NODE_ID` | (auto) | Unique node identifier |
| `FAILURE_GROUP` | default | Failure domain (rack, site, zone) |
| `HEARTBEAT_INTERVAL_SECS` | 5 | Heartbeat interval |

### Coordinator Role (`ROLE=coordinator`) -- not yet implemented

| Variable | Default | Description |
|----------|---------|-------------|
| `COORD_PORT` | 9000 | Coordinator API listen port |
| `REPLICATION_FACTOR` | 2 | Target replication factor |
| `MIN_WRITES` | max(rf-1, 1) | Minimum successful writes for a PUT to succeed |
| `DELETE_REQUIRES_MIN_WRITES` | false | When `true`, deletes also require `MIN_WRITES` replicas |

---

## Notes

- **Boolean env vars** accept `1` or `true` (case-insensitive).

