# Config File Format

Both `shardedobjstr` (CLI) and `objstrd` (daemon) read the same `.conf`
file. Directives that only apply to one tool are tagged below:

- **(daemon-only)** - used by `objstrd`, silently ignored by the CLI.
- **(CLI-only)** - used by `shardedobjstr`, not applicable to the daemon.
- Untagged directives are shared by both.

The config supports two formats: **flat** and **tree**.

- **Flat config** **(CLI-only)** - a simple list of shards with global
  settings. Suited for single-node clusters or simple shard lists.
- **Tree config** - indent-based nested topology with named nodes.
  Suited for multi-site, multi-tier, or distributed clusters.

Both formats share the same file extension (`.conf`) and directive syntax.
The parser auto-detects the format: if any indent-level-0 line contains
`rf=` and is not a shard declaration, the file is parsed as a tree config.

---

## Validating a Config File

Both tools can validate a config without running anything:

```bash
# CLI - validates flat configs (and tree configs with --node)
shardedobjstr check-config --config cluster.conf

# Daemon - validates tree configs, optionally checking a specific node
objstrd --check-config --config cluster.conf
objstrd --check-config --config cluster.conf --node top
```

Example output:

```
Checking config: cluster.conf
  INFO   node 'top' shard 0: raw '/tmp/shard0.raw' does not exist (will be auto-created)
  INFO   cluster 'test': 2 node(s), 4 shard(s), bucket 'testbucket'

Config OK.
```

Checks performed (tree config):
- Parse errors (missing fields, invalid values)
- Node name uniqueness
- Requested `--node` exists in the config
- Compression algorithm validity (global and per-shard)
- Raw shard paths and parent directories exist
- FS shard root directories exist and are directories
- S3 endpoint URLs start with `http(s)://`
- Replication factor vs shard count
- Child node references resolve
- **Duplicate S3 backends** (planned, not yet implemented) - two S3
  shards anywhere in the tree with the same endpoint + bucket should be
  flagged as an error (replication to the same backend is pointless).
  Same bucket with different endpoints (e.g. R2 vs AWS) is fine since
  they are different storage backends.
- **Duplicate raw/fs on the same machine** (planned, not yet implemented)
  - nodes whose endpoints resolve to the same hostname are assumed to be
  on the same machine. If two shards across those nodes reference the same
  raw device path or fs mount root, it should be flagged as an error.
  Nodes on different hosts can freely reuse the same paths (e.g.
  `/dev/nvme0n1`) since they are different physical devices.

---

## Flat Config (CLI-only)

A flat config declares a single cluster with one replication factor
and a list of shards.

```
# cluster.conf
replicas     2
min_writes   1
delete_requires_min_writes  true
catalog      json:/tmp/catalog.json
read_prefer  round-robin
direct_io    true
compression  zstd
size_mb      1024

shard  raw  /dev/nvme0n1
shard  raw  /dev/nvme1n1
shard  raw  /tmp/extra.raw  size_mb=512  compression=none
```

### Flat Config Directives

| Directive | Default | Description |
|-----------|---------|-------------|
| `replicas` | `1` | Replication factor |
| `min_writes` | `max(replicas - 1, 1)` | Minimum successful replica writes for a put to succeed |
| `delete_requires_min_writes` | `false` | When `true`, deletes also require `min_writes` replicas |
| `catalog` | (none) | Catalog persistence: `none`, `json:<path>`, `bincode:<path>`, or bare path (JSON) |
| `read_prefer` | `round-robin` | Read preference: `round-robin` or `ordered` |
| `direct_io` | `false` | Default O_DIRECT for all shards |
| `compression` | (none) | Default compression: `none`, `zstd`, `snappy`, `gzip0`..`gzip9` |
| `size_mb` | (none) | Default image size in MB for formatting |
| `read_only` | `false` | Open all shards read-only |

### Flat Config Shard Lines

Each shard line starts with `shard` and specifies a backend type and path.
Per-shard options override global defaults.

```
shard  raw  <path>  [readonly] [direct_io] [compression=<alg>] [size_mb=<N>]
shard  fs   <path>  [readonly]
shard  s3   endpoint=<url>  bucket=<name>  [region=<r>] [access_key=<k>] [secret_key=<s>] [path_style]
shard  mem
shard  <path>                   # bare path - implicit raw type
```

Comments (`#`) and blank lines are allowed anywhere.

### Mixed-Backend Example

```
# mixed.conf - local disk + S3 mirror
replicas  2

shard  raw  /dev/nvme0n1  direct_io
shard  s3   endpoint=https://s3.amazonaws.com  bucket=my-backup  region=us-east-1  access_key=AKIA...  secret_key=wJal...
```

### Flat Config CLI Usage

```bash
shardedobjstr info --config cluster.conf
shardedobjstr list --config cluster.conf --long
shardedobjstr verify --config cluster.conf

# CLI flags override config values
shardedobjstr list --config cluster.conf --replicas 3
shardedobjstr put --config cluster.conf --min-writes 2 mykey data.bin
```

---

## Tree Config

A tree config defines a nested topology where each node has a name,
a replication factor, and its own list of shards (which can include
references to child nodes).

Indentation defines the tree structure. Child shards and child nodes
are indented under their parent node.

### Full Example

```
# cluster.conf
cluster  hetero-test
bucket   testbucket                              # (daemon-only)

# Global shard defaults
compression  zstd
direct_io    true
size_mb      4096

# Daemon options (daemon-only - silently ignored by the CLI)
flush_interval   5
admin_token      my-secret-token
access_key       AKIAIOSFODNN7EXAMPLE
secret_key       wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
log_file         /var/log/objstrd.log
catalog_path     /var/lib/objstrd/catalog.json
catalog_format   json
catalog_flush_interval  30

# Recovery (daemon-only)
recovery_enabled          true

top  rf=3  listen=0.0.0.0:8000  endpoint=http://10.0.1.10:8000
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1  compression=snappy
  inner-left  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.20:8000
    raw  /data/shard0.raw  size_mb=2048
    inner-sub  rf=1  listen=0.0.0.0:8001  endpoint=http://10.0.1.20:8001
      raw  /dev/sda
      fs   /mnt/nfs-share  readonly
  inner-right  rf=1  listen=0.0.0.0:8000  endpoint=http://10.0.1.30:8000
    raw  /data/shard3.raw
    raw  /data/shard4.raw  readonly
  s3   endpoint=https://s3.amazonaws.com  bucket=my-archive  region=us-east-1
  s3   endpoint=https://abc123.r2.cloudflarestorage.com  bucket=hot-cache
```

### How It Works

1. The **root node** is the first line with `rf=N` at indent level 0.
   Its name (e.g. `top`) is used as the default `--node` target.

2. **Child nodes** are lines with `rf=N` indented under a parent node.
   They appear as `Node` shards in the parent's shard list.

3. **Shard lines** (`raw`, `fs`, `s3`, `mem`) indented under a node
   belong to that node.

4. **Global directives** (`cluster`, `compression`, `size_mb`,
   `direct_io`, `read_only`, `read_prefer`, `catalog`) can appear at
   indent level 0.

5. **Daemon-only directives** (`bucket`, `flush_interval`,
   `admin_token`, `access_key`, `secret_key`, `cors_origin`,
   `log_file`, `log_buffer_size`, `event_socket`, `event_secret`,
   `max_readers`, `event_source`, `catalog_path`, `catalog_format`,
   `catalog_flush_interval`, and all `recovery_*` /
   `repair_replication_*` directives) are silently ignored by the CLI.
   Per-node fields (`listen`, `endpoint`, `delete_requires_min_writes`)
   are also daemon-only and silently skipped when the CLI parses node
   lines. This means the same config file can be used by both tools.

### How to Read the Topology

The topology is a tree. Each node's shards are the lines indented directly
below it. Child nodes also become shards (remote S3 endpoints) for the parent.

Shard order is line order. This matters for consistent hashing placement.

**top** has 6 shards:

| Shard | Type | Target |
|-------|------|--------|
| 0 | raw | /dev/nvme0n1 (local, machine A) |
| 1 | raw | /dev/nvme1n1 (local, machine A) |
| 2 | node | inner-left (remote S3, machine B) |
| 3 | node | inner-right (remote S3, machine C) |
| 4 | s3 | AWS S3 bucket my-archive (direct) |
| 5 | s3 | Cloudflare R2 bucket hot-cache (direct) |

**inner-left** has 2 shards:

| Shard | Type | Target |
|-------|------|--------|
| 0 | raw | /data/shard0.raw (local, machine B) |
| 1 | node | inner-sub (remote S3, machine B port 8001) |

**inner-sub** has 2 shards:

| Shard | Type | Target |
|-------|------|--------|
| 0 | raw | /dev/sda (local, machine B) |
| 1 | fs | /mnt/nfs-share (local, machine B) |

**inner-right** has 2 shards:

| Shard | Type | Target |
|-------|------|--------|
| 0 | raw | /data/shard3.raw (local, machine C) |
| 1 | raw | /data/shard4.raw (local, machine C) |

---

## Node Fields

| Field | Required | Description |
|-------|----------|-------------|
| name | yes | First word on the line. Unique within the cluster. |
| `rf` | yes | Replication factor for this node's ShardedObjectStore. |
| `min_writes` | no | Minimum successful writes for a put (default: `max(rf - 1, 1)`). Syntax: `min_writes=<N>`. |
| `delete_requires_min_writes` | daemon-only | When `true`, deletes also require `min_writes` replicas (default: `false`). Parsed by `objstrd` only; silently ignored by the CLI. |
| `listen` | daemon-only | Bind address for this node, e.g. `0.0.0.0:8000`. Required by `objstrd`. |
| `endpoint` | daemon-only | How other nodes reach this one, e.g. `http://10.0.1.10:8000`. Required by `objstrd`. |

Node line syntax:

```
<name>  rf=<N>  [min_writes=<M>]  [delete_requires_min_writes=<bool>]  [listen=<addr>]  [endpoint=<url>]
```

Only `<name>` and `rf=<N>` are required by the CLI. The `listen` and
`endpoint` pairs are required by `objstrd` and ignored by the CLI.

---

## Raw Shard Fields

| Field | Required | Description |
|-------|----------|-------------|
| path | yes | Second word on the line. Path to image file or block device. |
| `readonly` | no | Open in read-only mode. Flag (no `=`). |
| `compression` | no | Compression for new images: `none`, `zstd`, `snappy`, `gzip0`..`gzip9`. Overrides global. |
| `direct_io` | no | Enable O_DIRECT. Flag or `direct_io=true`. Overrides global. |
| `size_mb` | no | Image size in MB when creating new images. Overrides global. |

Per-shard options only apply when formatting a **new** image file. Existing
images are opened as-is (compression and O_DIRECT are read from the superblock).

## FS Shard Fields

| Field | Required | Description |
|-------|----------|-------------|
| path | yes | Second word on the line. Root directory for LocalFileSystem. |
| `readonly` | no | Open in read-only mode. Flag (no `=`). |

## S3 Shard Fields

| Field | Required | Description |
|-------|----------|-------------|
| `endpoint` | yes | S3-compatible endpoint URL. |
| `bucket` | yes | Bucket name on the remote endpoint. |
| `region` | no | AWS region (default: us-east-1). |
| `access_key` | no | Access key (or use env `AWS_ACCESS_KEY_ID`). |
| `secret_key` | no | Secret key (or use env `AWS_SECRET_ACCESS_KEY`). |
| `path_style` | no | Use path-style requests (needed for some S3-compat services). |

## Shard Line Syntax

```
raw  <path>  [readonly]  [compression=<alg>]  [direct_io]  [size_mb=<N>]
fs   <path>  [readonly]
s3   endpoint=<url>  bucket=<name>  [region=<r>]  [access_key=<k>]  [secret_key=<s>]  [path_style]
mem
```

---

## Global Shard Defaults

These top-level directives set defaults for raw shards. Per-shard values
on the `raw` line override them.

| Directive | Default | Description |
|-----------|---------|-------------|
| `compression` | `none` | Default compression for new raw images (`none`, `zstd`, `snappy`, `gzip0`..`gzip9`). |
| `direct_io` | `false` | Default O_DIRECT setting for new raw images. |
| `size_mb` | (none) | Default image size in MB when creating new raw images. The daemon CLI defaults to 256 when unset. |

---

## Design Rules

- **Topology only.** The config never creates, formats, or deletes anything.
  Raw devices must already be formatted. FS directories must exist. S3 buckets
  must already exist. Nodes report their own size at runtime.
- **Read-only.** Both tools read the config at startup and do not modify it.
- **Same file everywhere.** Every node in the cluster gets the same config file.
  Each `objstrd` instance identifies itself with `--node <name>`.
- **No sizes.** Nodes discover storage capacity at runtime from the underlying
  backend. The config does not specify sizes.
- **Config files are limited to 10 MB** in `objstrd`; files exceeding this size are rejected.

---

## Recovery Directives (daemon-only)

Recovery settings are top-level directives in the tree config. They apply
to every `objstrd` node that reads the config. The CLI silently ignores them.

**Priority:** CLI flag > environment variable > tree config > default.

```
recovery_enabled          true
recovery_poll_secs        10
recovery_probe_timeout_secs 5
recovery_failure_threshold 3
recovery_re_replicate_batch_size 50

repair_replication_interval   300
repair_replication_batch_size 500
```

| Directive | Default | Description |
|-----------|---------|-------------|
| `recovery_enabled` | `true` | Master switch. Set `false` to disable health polling entirely (planned maintenance). |
| `recovery_poll_secs` | `10` | How often each shard is probed. Clamped to minimum 1. |
| `recovery_probe_timeout_secs` | `5` | Per-probe timeout in seconds. Clamped to minimum 1. |
| `recovery_failure_threshold` | `3` | Consecutive failures before `detach_shard()`. Clamped to minimum 1. |
| `recovery_re_replicate_batch_size` | `100` | Max objects per sweep cycle. Clamped to 1-10,000. |

### Repair-Replication Directives (daemon-only)

| Directive | Default | Description |
|-----------|---------|-------------|
| `repair_replication_interval` | `0` | Seconds between background repair-replication cycles. `0` disables the task. |
| `repair_replication_batch_size` | `500` | Max objects per repair-replication sweep. Clamped to 1-10,000. |

The background repair-replication task periodically rebuilds the catalog (discovers
all objects across all shards) then runs repair-replication sweeps to copy
under-replicated objects and trim over-replicated ones. Objects become
queryable on target shards incrementally. Status at `/_admin/repair-replication-status`.

### Reattach Behavior (daemon-only)

When a shard comes back online after being offline (and its
objects have been re-replicated to other healthy shards), the attach process
(`POST /_admin/attach/{id}`) cleans it up before the shard becomes
readable again. The shard transitions through `Syncing` before reaching
`Healthy`.

During sync, `sync_and_reattach` performs these steps:

1. **Delete marker replay** - objects that were deleted (via S3 DELETE)
   while the shard was offline still exist on its storage. The sync
   replays delete markers from healthy shards and removes any object
   whose `last_modified` is older than the delete timestamp.

2. **Stale object removal** - in mirror mode, any object present on the
   returning shard but absent from healthy shards is deleted. In
   partitioned mode, objects no longer in the catalog are removed.

3. **Stale version replacement** - if an object was re-PUT while the
   shard was offline, the old version on the returning shard is
   replaced with the current version from healthy shards.

4. **Catalog rebuild** - the shard's contents are rescanned and merged
   into the in-memory catalog so new reads can be served.

5. **Over-replication trimming** - after the shard becomes Healthy, the
   recovery loop's next sweep detects that some objects now have more
   replicas than the replication factor (the original on the returned
   shard plus the copy made during re-replication). The trimmer removes
   excess copies from the fullest shards first to rebalance storage.

The net result: a shard that returns after a long outage is cleaned of
stale/deleted data before serving reads, and duplicate copies created by
re-replication are trimmed automatically. No manual intervention is needed.

---

## Daemon Options (daemon-only)

These top-level directives configure `objstrd`. They are silently ignored
by `shardedobjstr` when it parses the same config file.

**Priority:** CLI flag > environment variable > tree config directive > default.

| Directive | Env Var | Default | Description |
|-----------|---------|---------|-------------|
| `bucket` | -- | `testbucket` | Default S3 bucket name. CLI `--bucket` can override. |
| `flush_interval` | `FLUSH_INTERVAL_SECS` | `5` | Periodic index flush interval in seconds for raw shards. |
| `admin_token` | `ADMIN_TOKEN` | (none) | Bearer token for `/_admin/*` admin endpoints. |
| `access_key` | `ACCESS_KEY` | (none) | S3 access key for SigV4 auth and child-node connections. |
| `secret_key` | `SECRET_KEY` | (none) | S3 secret key. |
| `cors_origin` | `ADMIN_CORS_ORIGIN` | (none) | CORS origin for admin endpoints. |
| `log_file` | `LOG_FILE` | (none) | Path to structured log file. |
| `log_buffer_size` | `LOG_BUFFER_SIZE` | `10000` | In-memory log ring buffer size. Clamped to 100-1,000,000. |
| `event_socket` | `EVENT_SOCKET` | (none) | Unix domain socket path for event broadcasting. |
| `event_secret` | `EVENT_SECRET` | (none) | Authentication secret for event socket connections. |
| `max_readers` | `MAX_READERS` | `16` | Maximum concurrent event socket readers. |
| `event_source` | `EVENT_SOURCE` | (none) | Event source address for streaming replica mode (Unix path or `tcp:host:port`). Read-only nodes only. |
| `catalog_path` | `CATALOG_PATH` | (none) | Path to catalog persistence file. Omit to disable. |
| `catalog_format` | `CATALOG_FORMAT` | `json` | Catalog file format: `json` or `bincode`. |
| `catalog_flush_interval` | `CATALOG_FLUSH_INTERVAL_SECS` | `0` | Periodic catalog flush interval in seconds. `0` = save on shutdown only. |

---

## Streaming Replica (daemon-only)

A read-only `objstrd` instance can keep its in-memory catalog up to date by
subscribing to the writer's event stream. Instead of periodically rescanning
shards, the reader watches PUT and DELETE events in real time and updates
its catalog with a HEAD call per object.

**Constraints:**

- Only works when every object lives on every shard: single-shard (rf=1)
  or mirror mode (rf = shard_count).
- Both writer and reader must point at the same backing storage (same
  filesystem path, same S3 bucket, or same raw image opened read-only).
- The reader must be started with `--read-only`.

### Writer config (writer.conf)

```
cluster   myapp
bucket    data

event_socket   /run/objstrd/events.sock
event_secret   s3cr3t

writer  rf=1  listen=0.0.0.0:8801  endpoint=http://10.0.1.10:8801
  fs  /mnt/storage
```

### Reader config (reader.conf)

```
cluster   myapp
bucket    data

event_source   /run/objstrd/events.sock
event_secret   s3cr3t

reader  rf=1  listen=0.0.0.0:8802  endpoint=http://10.0.1.10:8802
  fs  /mnt/storage
```

Start the reader with `--read-only`:

```bash
objstrd --config reader.conf --node reader --read-only
```

### Cross-machine setup with socat

If the reader runs on a different machine, bridge the Unix socket over TCP
with socat:

```bash
# On the writer machine - expose the Unix socket on TCP port 9090
socat TCP-LISTEN:9090,reuseaddr,fork UNIX-CONNECT:/run/objstrd/events.sock
```

Then change the reader config to use TCP:

```
event_source   tcp:writer-host:9090
event_secret   s3cr3t
```

---

## Startup Order (daemon-only)

Start leaf nodes first, then work up to the root:

```bash
# Machine B (leaves)
objstrd --config cluster.conf --node inner-sub
objstrd --config cluster.conf --node inner-right

# Machine B (depends on inner-sub)
objstrd --config cluster.conf --node inner-left

# Machine A (root, depends on inner-left and inner-right)
objstrd --config cluster.conf --node top
```

S3/R2 shards need no startup - they are already running.

## Interaction with Other Config Modes (daemon-only)

The tree config (`--config`/`--node`) is an alternative to the existing JSON
config file (`CONFIG_FILE` env var) and multi-store env vars (`STORE_N_*`).
If `--config` is provided it takes priority. All three modes produce the same
internal `ClusterConfig` used by the server.

When using `--config`, the `--port` and `--bind` flags are ignored because the
node's listen address comes from the config file. The `--bucket` flag can
override the config's `bucket` directive.

---

## Tree Config with the CLI (CLI-only)

### Selecting a Node

By default, the CLI operates on the **root node** of the tree.
Use `--node <name>` to target a specific child node:

```bash
# Operate on the root (default)
shardedobjstr list --config cluster.conf

# Operate on a specific child node
shardedobjstr list --config cluster.conf --node site-west
shardedobjstr health --config cluster.conf --node site-east
shardedobjstr info --config cluster.conf --node nvme-pool
```

`--node` works with most commands: `list`, `get`, `put`, `delete`,
`info`, `verify`, `health`, `report`, `repair-replication`, `vacuum`,
and `list-deleted`.

### How Node Resolution Works

When the CLI encounters a tree config:

1. It parses the full tree structure.
2. It finds the target node (root by default, or the `--node` value).
3. It extracts the target node's shard list and replication factor.
4. Child node references (e.g. `site-west`) become `Node` shards -
   the CLI recursively opens them as nested `ShardedObjectStore` instances.
5. The resulting nested cluster behaves like a single `ObjectStore`.

### NVMe Striped + S3 Mirror Example

A tree config expressing the striped NVMe + S3 mirror pattern:

```
cluster      nvme-s3-mirror
read_prefer  ordered

root  rf=2
  nvme-pool  rf=1
    raw  /dev/nvme0n1
    raw  /dev/nvme1n1
    raw  /dev/nvme2n1
  s3   endpoint=https://s3.us-west-2.amazonaws.com  bucket=nvme-mirror  region=us-west-2  access_key=AKIA...  secret_key=wJal...
```

- **root** (rf=2): mirrors between the NVMe pool and S3.
- **nvme-pool** (rf=1): stripes across 3 NVMe drives (RAID-0 style).
- **read_prefer ordered**: reads always try shards in config order -
  NVMe pool first, S3 only as fallback. Without this the default
  `round-robin` policy would route roughly half of all reads to S3.

Every object written to root lands on both the NVMe pool and S3.
The NVMe pool spreads objects across 3 drives for capacity.

### Multi-Site Example

```
cluster    prod-cluster
catalog    json:/var/lib/objstr/catalog.json
compression  zstd

root  rf=3
  raw  /dev/nvme0n1
  raw  /dev/nvme1n1
  site-west  rf=2
    raw  /dev/sda
    raw  /dev/sdb
    raw  /dev/sdc
  site-east  rf=1
    raw  /data/shard0.raw  size_mb=2048
    fs   /mnt/nfs-archive  readonly
  s3   endpoint=https://abc123.r2.cloudflarestorage.com  bucket=cold-tier
```

This gives you:

- **root** (rf=3): 5 shards (2 local NVMe + 2 child nodes + 1 R2 bucket).
  Every object written to root is stored on 3 of those 5 shards.
- **site-west** (rf=2): 3 raw devices, each object stored on 2.
- **site-east** (rf=1): no replication, single copy across its 2 shards.
- **R2 bucket**: a leaf shard visible to root, not a separate node.

---

## Auto-Detection (CLI-only)

The `is_tree_config()` function detects the format by scanning for
indent-level-0 lines that contain `rf=` and do not start with a shard
keyword (`raw`, `fs`, `s3`, `mem`, `shard`). If any such line is found,
the file is a tree config.

The `load_auto_conf()` function uses this detection to parse either
format and returns a `TreeConf` in both cases - flat configs are
wrapped in a single-node tree.

---

## Rust API (CLI-only)

```rust
use shardedobjstr::config::{
    // Flat config
    parse_cluster_conf, load_cluster_conf, validate_cluster_conf,
    ClusterConf, ShardConf, ClusterDiag,

    // Tree config
    parse_tree_conf, load_tree_conf, is_tree_config, load_auto_conf,
    TreeConf, TreeNode,

    // Diagnostics
    ClusterDiagLevel,
};

// Parse flat config
let conf: ClusterConf = load_cluster_conf(Path::new("cluster.conf"))?;
let diags = validate_cluster_conf(&conf);

// Parse tree config
let tree: TreeConf = load_tree_conf(Path::new("tree.conf"))?;
let root = &tree.root;
println!("Root: {}, rf={}", root.name, root.replication_factor);

// Find a specific node
if let Some(node) = tree.find_node("site-west") {
    println!("{}: {} shards, rf={}", node.name, node.shards.len(), node.replication_factor);
}

// Auto-detect format
let tree = load_auto_conf(Path::new("any.conf"))?;
```

### Key Types

**`ShardConf`** - enum with variants:

| Variant | Fields | Description |
|---------|--------|-------------|
| `Raw` | `path`, `read_only`, `compression`, `direct_io`, `size_mb` | Block device or loopback image |
| `Fs` | `root`, `read_only` | Local directory |
| `S3` | `endpoint`, `bucket`, `region`, `access_key`, `secret_key`, `path_style` | S3-compatible endpoint |
| `Mem` | (none) | In-memory store (testing) |
| `Node` | `String` (child name) | Reference to a child node in a tree config |

**`TreeNode`** - a node in the tree:

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | Node name |
| `replication_factor` | `usize` | Replication factor for this node |
| `min_writes` | `Option<usize>` | Minimum successful writes (None = default) |
| `shards` | `Vec<ShardConf>` | Shards (including `Node` references to children) |
| `children` | `Vec<TreeNode>` | Child node definitions |

**`TreeConf`** - the full tree:

| Field | Type | Description |
|-------|------|-------------|
| `cluster_name` | `String` | Cluster name from `cluster` directive (defaults to root node name) |
| `root` | `TreeNode` | Root node |
| `catalog` | `Option<String>` | Catalog persistence |
| `read_prefer` | `Option<String>` | Read preference |
| `compression` | `Option<String>` | Default compression |
| `direct_io` | `bool` | Default O_DIRECT |
| `size_mb` | `Option<u64>` | Default size in MB |
| `read_only` | `bool` | Read-only mode |

Note: In `TreeNode`, `min_writes` is `Option<usize>` (None
means use `max(rf - 1, 1)`).

**`ClusterConf`** - flat config:

| Field | Type | Description |
|-------|------|-------------|
| `replicas` | `usize` | Replication factor |
| `min_writes` | `Option<usize>` | Minimum successful writes (None = default) |
| `delete_requires_min_writes` | `bool` | Deletes require min_writes replicas |
| `catalog` | `Option<String>` | Catalog file path |
| `read_prefer` | `Option<String>` | Read preference |
| `direct_io` | `bool` | Default O_DIRECT |
| `compression` | `Option<String>` | Default compression |
| `size_mb` | `Option<u64>` | Default size in MB |
| `read_only` | `bool` | Read-only mode |
| `shards` | `Vec<ShardConf>` | List of shard configs |

---

## Python API (CLI-only)

```python
import shardedobjstr

# Load and inspect config (flat or tree - auto-detected)
conf = shardedobjstr.load_config("cluster.conf")
print(conf["replicas"], len(conf["shards"]))

# Validate
diags = shardedobjstr.check_config("cluster.conf")
for d in diags:
    print(d["level"], d["message"])

# Open cluster directly from config (raw shards only)
store = shardedobjstr.open_cluster_from_config("cluster.conf")
store.put("key", b"value")

# Override options at open time
store = shardedobjstr.open_cluster_from_config(
    "cluster.conf", direct_io=False, read_only=True,
)
```

**Limitations:**

- `open_cluster_from_config()` currently only supports **raw shards**.
  Configs containing `fs`, `s3`, `mem`, or `node` shards will raise
  `ValueError`. Use `open_cluster()` or `open_fs_cluster()` directly
  for other shard types.
- `load_config()` auto-detects flat vs tree configs. Tree configs are
  flattened to the root node's perspective (child nodes become `node`
  shard entries in the returned dict).
- An async wrapper is available: `shardedobjstr.aio.async_open_cluster_from_config()`.
