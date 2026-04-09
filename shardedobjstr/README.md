# ShardedObjectStore

Lets you compose multiple storage backends: local disks, S3, or remote nodes into a single, logical ObjectStore. It handles sharding and replication internally, allowing you to build complex storage topologies that remain invisible to your application logic.

Why it's useful:

Nested Topologies: Unlike a flat RAID, you can nest ShardedObjectStore instances. A root node can replicate across local NVMe and a child node, which then manages its own S3/disk shards.

Drop-in Replacement: Implements the Apache ObjectStore trait. Works instantly with LanceDB, DataFusion, or any Arc<dyn ObjectStore> consumer.


## Supported Backends

| Type | Description |
|------|-------------|
| `raw` | Block device or loopback image via `RawObjectStore` |
| `fs` | Local directory via `LocalFileSystem` |
| `s3` | Any S3-compatible endpoint (AWS, R2, MinIO, another `objstrd`) |
| `mem` | In-memory store (testing only) |

The crate adds:

- A **placement catalog** that tracks which shard(s) hold each object
- **Replication** so objects are copied to multiple shards for redundancy
- **Catalog persistence** via JSON or bincode file (optional; without it,  the catalog is rebuilt from shard indexes on startup)

## Architecture

A flat (single-node) cluster looks like this:

```
+----------------------------------------------------------+
|  Your Application (LanceDB, DataFusion, custom code)     |
|                                                          |
|  +----------------------------------------------------+  |
|  |  ShardedObjectStore  (rf=2, impl ObjectStore)      |  |
|  |                                                    |  |
|  |  +----------+ +----------+ +----------+           |  |
|  |  | Shard 0  | | Shard 1  | | Shard 2  |           |  |
|  |  | /dev/sdb | | /dev/sdc | | S3 (R2)  |           |  |
|  |  | RawObj   | | RawObj   | | S3 client |           |  |
|  |  +----------+ +----------+ +----------+           |  |
|  +----------------------------------------------------+  |
+----------------------------------------------------------+
```

A tree cluster nests nodes, each with its own replication factor:

```
                      root  rf=2
                     /    |     \
             +------+ +------+ +------+
             |NVMe 0| |NVMe 1| |leaf-a|  (S3 to child objstrd)
             | Raw  | | Raw  | |  S3  |
             +------+ +------+ +------+
                                   |
                             leaf-a  rf=1
                             /        \
                       +--------+ +--------+
                       | /dev/sd | | fs dir |
                       |  Raw   | | LocalFS|
                       +--------+ +--------+
```

The root replicates each object to 2 of its 3 shards (2 local NVMe + 1
child node). The leaf has rf=1 so it stores a single copy across its own
shards. Each level is independent - a write at the root fans out to the
child via S3, and the child stores it using its own policy.


## Fault Tolerance 

A shard failure does **not** invalidate the remaining shards. Each shard
is an independent object store that can be accessed on its own. When a
shard goes offline:

- All objects stored on **surviving** shards are still fully accessible.
- Only objects that had **no replicas** on surviving shards become
  unavailable (i.e. objects whose only copy was on the failed shard).
- Any surviving shard can be mounted and read as a standalone
  `RawObjectStore` or S3 endpoint - no cluster metadata is needed to
  recover its contents.

This is different from RAID-5/6, where a failed drive makes
the entire array degraded and a second failure can destroy all data. Here,
each shard is self-contained. If you lose one drive out of five, the other
four still serve every object they hold, and the cluster continues
operating in degraded mode with no risk of cascading data loss.


## Quick Start

### Building

Clone and build the CLI tools with a single command:

```bash
git clone https://github.com/sysadminmike/objstr.git
cd objstr/shardedobjstr
chmod +x build.sh
./build.sh --location ~/build-sharded
```

This checks for Rust/Cargo and a C linker, builds two binaries, and prints
their paths when done. Use `--debug` instead of the default `--release` for
faster compile times during development.

- `shardedobjstr` -- CLI for managing sharded stores (format, put, get, list, verify, repair-replication, etc.)
- `shardedobjstr-catview` -- dumps catalog files as text

To remove everything: `rm -rf ~/build-sharded`

### CLI

```bash
# Format 3 shards (256 MB each, replication factor 2)
shardedobjstr format \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --size 268435456

# List objects across the cluster
shardedobjstr list \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --long

# Check integrity
shardedobjstr verify \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2
```

See [CLI.md](CLI.md) for the full CLI command reference and
[CONFIG.md](../CONFIG.md) for config file format details.

## Features

- **Full ObjectStore trait** - `put`, `get`, `delete`, `list`, `head`, `copy`, `get_range`, `get_opts`, `put_opts`, multipart upload
- **Placement catalog** - in-memory `HashMap` with pluggable persistence (None, JSON, Bincode); atomic writes for crash safety
- **Jump consistent hash placement** - Lamping & Veach algorithm; adding a shard moves only ~1/(N+1) of objects instead of nearly all
- **Free-space-aware write placement** - sorts candidate shards by free space descending; jump hash as tiebreaker
- **Configurable replication** - synchronous N-way writes; configurable write quorum (`min_writes`) with automatic default; optional `delete_requires_min_writes` for strict delete quorum; ordered and round-robin reads with automatic failover
- **CRC32C integrity** - per-shard atomic CRC error counter; corrupt replicas auto-removed from catalog on read; repair-replication restores RF
- **Degraded startup** - `new_with_offline()` accepts `None` for unavailable shards; reads/writes work on healthy shards
- **Detach / reattach** - `detach_shard()` replaces shard with offline placeholder; `attach_shard()` swaps in a live store
- **Take offline / hold** - `hold_offline()` puts a shard in `Detached` state so the recovery loop will not auto-reattach it; optional `suppress_replication` prevents re-replication sweep from copying objects elsewhere; `release_hold()` clears the hold
- **Shard health management** - `set_shard_health()`, `invalidate_shard()` with full re-scan and `InvalidateReport`; five health states: Healthy, Degraded, Offline, Syncing, Detached
- **Repair module** - `probe_store`, `sync_and_reattach`, `mirror_sync`, `partitioned_sync`, `re_replication_sweep`, `over_replication_trim`, `repair_replication_sweep`, `drain_shard`, `plan_repair_replication`, `redistribute_sweep`
- **TLV metadata** - encode/decode for shard metadata headers; metadata-aware I/O routing via `RawRefRegistry`- **Delete markers** - `vacuum_delete_markers`, `list_delete_markers`, `is_delete_marker`; soft-delete with marker keys, vacuum to purge- **CLI (`shardedobjstr`)** - commands: version, check-config, format, info, list, get, put, delete, verify, cross-verify, health, add-shard, remove-shard, repair-replication, report, vacuum, list-deleted
- **Catalog viewer (`shardedobjstr-catview`)** - dumps JSON or bincode catalogs as text
- **LanceDB integration** - `ObjectStoreProvider` trait; see `lance_on_cluster` example
- **PutMode support** - Create (AlreadyExists), Overwrite, Update (rejects e_tag/version)
- **Multipart upload** - delegates to primary shard's native multipart (disk-backed, not RAM); replicates on complete

### Metadata and copy() limitation

`copy()` does **not** preserve TLV metadata because it uses `get` + `put`
internally (the generic `ObjectStore` trait has no metadata parameter on
`copy`).  The `objstrd` S3 adapter works around this by reading body via
`get()`, metadata via `get_metadata()`, and writing both via
`put_with_meta()`.  If you call `copy()` directly from Rust, pass a
`RawRefRegistry` so the store can route metadata correctly, or use the
metadata module functions to do an explicit get-metadata-then-put-with-meta
cycle.

### Metadata strategy per backend type

| Backend | Storage | Read API |
|---------|---------|----------|
| **Raw** (block device) | TLV suffix appended to extent body | `get_metadata()` via `RawObjectStore` |
| **S3 / Node** (S3-compatible) | Native S3 attributes (`put_opts`) | `GetResult.attributes` |
| **Fs / Mem** (filesystem, in-memory) | Sidecar file at `{key}.__meta__` | Separate GET on sidecar path |

When a cluster mixes backend types, the metadata module handles routing
automatically via `RawRefRegistry` and `ShardKind`.


## Example: Local + S3 Mirror

A common pattern is keeping a local copy of everything that also lives in S3,
like `aws s3 sync` but automatic - every put lands on both stores, every
get reads from whichever is faster, and the two stay in sync without cron
jobs or scripts.

```
# mirror.conf - local filesystem + S3, rf=2
# Every object is written to BOTH shards automatically.

replicas  2

shard  fs   /mnt/data/mirror
shard  s3   endpoint=https://s3.amazonaws.com  bucket=my-backup  region=us-east-1  access_key=AKIA...  secret_key=wJal...
```

With 2 shards and `replicas 2`, every object is written to both shards on
every put. Deletes remove from both. Gets read from either and fail over to
the other if one is down. The local filesystem gives you fast reads and the
S3 bucket gives you off-site durability - no sync lag, no missed files, no
eventual consistency surprises.

For developers it allows accessing an object store while offline and when
returning online it just carries on working without any intervention.

## Example: Striped NVMe Mirrored to S3

Ideal for data analysis of huge datasets on spot instances. Spin up a spot
instance with local NVMe storage, point the mirror at your S3 bucket, and
objects synchronise automatically. You can start working immediately -
reads that hit a missing local object fall through to S3, so there is no
need to wait for a full sync before running queries. If your spot instance is
reclaimed, nothing is lost - S3 still has every object. Launch a new
instance and the NVMe drives repopulate on demand as objects are accessed.
Within the same AWS region, data transfer between EC2 and S3 is free - you
only pay the GET request fee ($0.0004 per 1,000 requests), so repopulating
the local cache is essentially free.

This uses nested `ShardedObjectStore` instances: an inner cluster stripes
across 3 NVMe drives (rf=1, no replication - just spread objects for
capacity), and an outer cluster mirrors between that inner pool and an S3
bucket (rf=2, every object on both).

```
Inner: NVMe pool (rf=1, striped)         Outer: mirror (rf=2)
+-----------------------------------+    +-------------------------+
| ShardedObjectStore  rf=1          |    | ShardedObjectStore rf=2 |
|                                   |    |                         |
|  +--------+ +--------+ +--------+|    |  +- inner (NVMe pool)   |
|  | NVMe 0 | | NVMe 1 | | NVMe 2 ||<---|  +- S3 bucket           |
|  |/dev/    | |/dev/    | |/dev/   ||    |                         |
|  |nvme0n1  | |nvme1n1  | |nvme2n1 ||    +-------------------------+
|  +--------+ +--------+ +--------+|
+-----------------------------------+
```

With 3x 2TB NVMe drives you get ~6TB of usable capacity (no replication
overhead on the NVMe side), all mirrored to S3. Reads come from NVMe at
full local speed; S3 is the failover and backup.

As a config file:

```
# nvme-s3-mirror.conf - 3 NVMe striped, mirrored to S3
cluster      nvme-s3-mirror
read_prefer  ordered

root  rf=2
  nvme-pool  rf=1
    raw  /dev/nvme0n1
    raw  /dev/nvme1n1
    raw  /dev/nvme2n1
  s3   endpoint=https://s3.us-west-2.amazonaws.com  bucket=nvme-mirror  region=us-west-2  access_key=AKIA...  secret_key=wJal...
```

`read_prefer ordered` tells the outer cluster to always try shards in
config order - NVMe pool first, S3 only as fallback. Without this the
default `round-robin` policy would route roughly half of all reads to S3.

```bash
# Write - lands on all 3 NVMe drives (striped) AND S3
shardedobjstr put --config nvme-s3-mirror.conf \
  --key dataset/chunk_001.parquet --from chunk_001.parquet

# List -- shows objects across the nested topology
shardedobjstr list --config nvme-s3-mirror.conf --long

# Inspect just the NVMe pool
shardedobjstr health --config nvme-s3-mirror.conf --node nvme-pool
```

See [CONFIG.md](../CONFIG.md) for the full config file reference and
[API.md](API.md) for the Rust API to build nested topologies
programmatically (including a complete code example for this topology).

## S3 Frontend with objstrd

`objstrd` wraps any `ShardedObjectStore` cluster in a full S3-compatible
HTTP server. Any S3 client (aws-cli, boto3, rclone, LanceDB) can talk to
the cluster without knowing anything about the underlying shards or
replication - it just looks like a normal S3 bucket.

```
S3 client (aws-cli, boto3, rclone, LanceDB)
    |
    v
objstrd  (S3 HTTP server)
    |
    v
ShardedObjectStore  (rf=2)
    |
    +-- NVMe pool (rf=1, 3 drives)
    +-- S3 bucket (off-site backup)
```

Start the server by pointing `objstrd` at the same config file the CLI uses:

```bash
# Serve the NVMe+S3 cluster as an S3 bucket on port 8000
objstrd --config nvme-s3-mirror.conf --node root \
  --access-key mykey --secret-key mysecret

# Now any S3 client can use it
aws --endpoint-url http://localhost:8000 s3 ls s3://data/
aws --endpoint-url http://localhost:8000 s3 cp local.parquet s3://data/dataset/
```

For the simple mirror config, `objstrd` can also be configured with
environment variables instead of a config file:

```bash
# Serve the mirror with env vars
STORE_0_TYPE=fs STORE_0_FS_ROOT=/mnt/data/mirror \
STORE_1_TYPE=s3 STORE_1_S3_ENDPOINT=https://s3.amazonaws.com \
  STORE_1_S3_BUCKET=my-backup STORE_1_S3_REGION=us-east-1 \
  STORE_1_S3_ACCESS_KEY=AKIA... STORE_1_S3_SECRET_KEY=wJal... \
REPLICATION_FACTOR=2 \
objstrd --port 8000 --access-key mykey --secret-key mysecret
```

`objstrd` adds a background recovery loop that automatically detects shard
failures, re-replicates under-replicated objects, and trims
over-replicated copies. It also provides admin endpoints
(`/_admin/cluster`, `/_admin/repair-replication`, `/_admin/drain/{id}`) and a browser
dashboard for monitoring the cluster. See the
[objstrd README](../objstrd/README.md) for the full reference.

## API

Implements the full [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html)
trait (`put`, `get`, `delete`, `list`, `copy`, `head`, `get_range`,
`put_multipart`, etc.) plus cluster management methods for degraded startup,
shard attachment/detachment, repair, repair-replication, verification, delete markers,
and multipart upload management.

See [API.md](API.md) for reference (all methods, types, report
structs, error types, Python bindings, and quick-start examples).

See [Python README](python/README.md) for Python library reference 


## Limits & Object Size

Object size limits are inherited from the underlying shard stores. When
`RawObjectStore` is used as the shard implementation, the per-object maximum
is the largest contiguous free extent on that shard (see
[RawObjectStore - Object Size Limits](../rawobjstr/README.md#object-size-limits)
for the full formula and constraints).

| Constraint | Source |
|------------|--------|
| Max object size | Largest contiguous free extent on the target shard |
| RAM usage | Entire object payload buffered in memory for `put` and `get`; multipart delegates to shard (disk-backed) |
| CRC verification | Full payload read required - no partial/streaming reads |
| Index capacity | Serialized index must fit the shard's index slot |
| Fragmentation | Per-shard first-fit allocator; no built-in defrag |

Because the cluster distributes objects across shards, each shard only holds a
fraction of the total data. Free space on any individual shard still fragments
over time, limiting the maximum object size that shard can accept. However,
the cluster layer has unique advantages for dealing with fragmentation - see
[Cluster-Assisted Defragmentation](#cluster-assisted-defragmentation) below.

## Cluster-Assisted Defragmentation

A single `RawObjectStore` shard can only defragment by reformatting and
re-importing all its objects (an offline, destructive operation). The cluster
layer makes this substantially easier because it has multiple shards and
replication:

1. **Evacuate-and-repack** - To defragment shard N, migrate all its objects
   to other shards (the cluster already knows how to `put` to any shard),
   reformat shard N, then migrate objects back. With replication factor >= 2,
   reads continue from replicas during the process - zero downtime.

2. **Opportunistic redistribution** - When a shard becomes heavily fragmented
   (large gaps between extents), the cluster can move objects off that shard
   to shards with more contiguous free space, then compact the vacated shard.
   This avoids reformatting altogether for moderate fragmentation.

3. **Rolling compaction** - Process one shard at a time: drain -> reformat ->
   refill. The cluster maintains read availability through replicas throughout.
   This is analogous to how distributed databases do rolling compaction on
   individual nodes.

4. **Free-space-aware placement** - Write placement sorts candidate shards by
   free space descending so emptier shards fill first, delaying fragmentation.
   When free space is equal the jump consistent hash tiebreaker keeps
   placement stable across shard count changes.

Because the cluster already tracks which shards hold each object (the catalog)
and supports `put`/`delete` on individual shards, the building blocks for all
of these strategies are already in place - the main work is orchestration and
rate limiting.

## Object Hashing and Placement

Objects are assigned to shards using **jump consistent hash** (Lamping & Veach,
Google 2014).  The key property: when the shard count changes from N to N+1,
only ~1/(N+1) of objects change their home shard.  The naive `hash % N`
approach remaps ~(N-1)/N of all objects - nearly everything.

**Why it matters:** adding or removing a shard triggers a repair-replication sweep.
With naive modulo, almost every object must migrate.  With jump hash, only
the minimum set of objects moves, reducing repair-replication time and I/O
proportionally.

**How it works:**

1. Each object key is hashed with CRC32C to produce a 64-bit seed.
2. `jump_consistent_hash(seed, shard_count)` returns the "home" shard in O(ln N)
   time with zero memory overhead.
3. For replication factor > 1, additional replicas are placed on successive
   shards walking the ring from the home position.
4. Write placement further sorts candidates by free space descending - the
   jump hash only serves as a tiebreaker when free space is equal.
5. The catalog records the actual placement, so reads always go to the right
   shard regardless of hash changes.

The algorithm (from the paper):

```
fn jump_consistent_hash(key: u64, num_buckets: u32) -> u32:
    b, j = -1, 0
    while j < num_buckets:
        b = j
        key = key * 2862933555777941757 + 1  (wrapping)
        j = (b + 1) * (2^31 / ((key >> 33) + 1))
    return b
```

The function is exposed as `shardedobjstr::replication::jump_consistent_hash`
for use by external tools and tests.

## Event Socket

A Unix domain socket broadcasts `PUT`, `DELETE`, and `FLUSH` events to all
connected subscribers. See
[rawobjstr -- Event Socket](../rawobjstr/README.md#event-socket-writer-to-reader-push)
for the full wire protocol, Rust API, and usage examples.

The `shardedobjstr::event` module provides helpers to wire the event
socket into a sharded cluster:

| Function | Description |
|----------|-------------|
| `setup_event_socket(cluster, raw_stores, path, secret, max, on_log)` | Create bus, register flush callbacks on each raw store, start EventServer |
| `subscribe_store_events(path, secret, cb)` | Subscribe to the event socket, receive all events |
| `subscribe_streaming_replica(source, secret, cluster)` | Subscribe and apply PUT/DELETE events to a read-only replica's catalog |

See [objstrd README](../objstrd/README.md#event-socket) for the `objstrd`
command-line example and platform/shard-type limitations.


## LanceDB Integration

Any code that uses the `object_store` trait - can use
`ShardedObjectStore` as its storage backend. See the `lance_on_cluster`
example and [API.md](API.md) for details.


## File Structure

```
shardedobjstr/
+-- Cargo.toml
+-- README.md
+-- API.md                          # Full API reference (Rust + Python)
+-- CLI.md                          # CLI command reference
+-- TESTS.md                        # Test suite inventory
+-- src/
|   +-- lib.rs                      # ShardedObjectStore + ObjectStore trait impl
|   +-- catalog.rs                  # Placement catalog (HashMap + JSON/Bincode persistence)
|   +-- config.rs                   # Config parsing (flat + tree), auto-detection
|   +-- event.rs                    # Event socket helpers (setup, subscribe)
|   +-- replication.rs              # Shard selection, replication policy
|   +-- repair.rs                   # Repair & repair-replication algorithms (sync, re-replication, trim)
|   +-- tlv.rs                      # TLV encode/decode for shard metadata
|   +-- metadata.rs                 # RawRefRegistry, ShardKind, metadata routing
|   +-- bin/
|       +-- shardedobjstr.rs        # shardedobjstr CLI commands
|       +-- shardedobjstr_catview.rs # shardedobjstr-catview (dump catalog files)
+-- examples/
|   +-- bench_catalog.rs            # Catalog operations benchmark
|   +-- bench_lance.rs              # LanceDB integration benchmark
|   +-- lance_on_cluster.rs         # LanceDB integration demo
+-- tests/                          # See TESTS.md for full inventory
+-- python/                         # Python bindings (pip: shardedobjstr)
```


