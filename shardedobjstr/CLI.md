# CLI Tools

`shardedobjstr` and `shardedobjstr-catview` are standalone binaries for managing
sharded raw object stores. See [BUILD.md](BUILD.md) for build instructions.

```bash
shardedobjstr <command> [options]
shardedobjstr-catview <catalog-file> [options]
```

---

## shardedobjstr

### Global Options

| Flag | Description |
|------|-------------|
| `--config <path>` | Path to a cluster `.conf` file (alternative to `--shards`). See [CONFIG.md](../CONFIG.md). |
| `--shards <path,path,...>` | Comma-separated shard image/device paths |
| `--replicas <N>` | Replication factor (default: 1) |
| `--min-writes <N>` | Minimum successful writes for a put (default: `max(rf - 1, 1)`) |
| `--delete-requires-min-writes` | Deletes also require `min_writes` replicas |
| `--direct-io` | Use O_DIRECT on Linux for shard I/O |
| `--catalog <spec>` | Catalog persistence: `none`, `json:<path>`, or `bin:<path>` (alias: `bincode:<path>`) |
| `--long` / `-l` | Show sizes and shard placement in list output |
| `--node <name>` | Target a specific node in a tree config (default: root node) |

Either `--shards` or `--config` is required (except for `check-config` and `version`).
When `--config` is used, shard paths, replicas, catalog, and direct_io
are read from the file. CLI flags override config file values.

Both flat and tree config files are supported. Tree configs are auto-detected.
See [CONFIG.md](../CONFIG.md) for the full config file reference.

### Commands

| Command | Description |
|---------|-------------|
| `version` | Print version, git hash, and build date |
| `check-config` | Validate a config file and exit |
| `format` | Format new shard images (destroys all data) |
| `info` | Show cluster and per-shard statistics |
| `list` | List all objects across the cluster (alias: `ls`) |
| `get` | Read a single object |
| `put` | Write a single object (auto-replicated) |
| `delete` | Delete a single object from all shards (alias: `del`) |
| `verify` | Check integrity and cross-shard consistency |
| `cross-verify` | MD5-based cross-shard verification of object data |
| `health` | Probe each shard and report status |
| `add-shard` | Add a new shard to the cluster |
| `remove-shard` | Drain and remove a shard |
| `repair-replication` | Repair under-replicated and trim over-replicated objects |
| `report` | Generate a cluster health report with object placement details |
| `vacuum` | Purge stale delete markers |
| `list-deleted` | List all current delete markers |

---

### version

Print the binary name, version, git hash, and build date.

```bash
shardedobjstr version
shardedobjstr --version
shardedobjstr -V
```

Example output:

```
shardedobjstr 0.1.0 (git a1b2c3d, built 2026-04-04 10:00:00 UTC)
```

---

### check-config

Validate a cluster config file and exit. Does not open any shards.

```bash
shardedobjstr check-config --config cluster.conf
```

Example output:

```
Checking config: cluster.conf
  INFO   shard 0: '/tmp/shard0.raw' does not exist (will be formatted)
  INFO   shard 1: '/tmp/shard1.raw' does not exist (will be formatted)
  INFO   2 shard(s), replicas=2, catalog=json:/tmp/catalog.json

Config OK.
```

Exits 0 if valid, 1 if errors found.

---

### format

Format shard images. Each shard gets its own `RawObjectStore` format.

```bash
shardedobjstr format \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --size 268435456
```

| Option | Required | Description |
|--------|----------|-------------|
| `--size <bytes>` | Yes | Size for each shard in bytes |

---

### info

Display cluster statistics: total capacity, file count, data usage, and per-shard breakdown.

```bash
shardedobjstr info --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw
```

Example output:

```
Cluster: 3 shards, replication factor 1
Total capacity:  768.0 MB
Total files:     12
Total data:      48.5 KB

Shard 0: /tmp/s0.raw
  Files: 4, Data: 16.2 KB, Free: 224.0 MB

Shard 1: /tmp/s1.raw
  Files: 4, Data: 16.2 KB, Free: 224.0 MB

Shard 2: /tmp/s2.raw
  Files: 4, Data: 16.1 KB, Free: 224.0 MB
```

---

### list

List all objects in the cluster. Uses the catalog for a deduplicated view.

```bash
# Simple listing
shardedobjstr list --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --replicas 2

# With sizes and shard placement
shardedobjstr list --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --replicas 2 --long

# Filter by prefix
shardedobjstr list --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --replicas 2 \
  --prefix test
```

| Option | Required | Description |
|--------|----------|-------------|
| `--prefix <prefix>` | No | Only list objects under this prefix |
| `--long` / `-l` | No | Show sizes and shard placement |

Default output:

```
test/myfile.txt
test/mypic.jpg
config/settings.json
```

Long output (`--long`):

```
      512  [0,2]  test/myfile.txt
     4096  [0,2]  test/mypic.jpg
       64  [1,2]  config/settings.json
```

Aliases: `ls`

---

### get

Read an object from the cluster. Reads are round-robin balanced across all
replicas, with automatic failover if a shard is unavailable.

```bash
# To stdout
shardedobjstr get --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --key test/myfile.txt > out.txt

# To a file
shardedobjstr get --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --key test/myfile.txt --to out.txt
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path of the object |
| `--to <local-file>` | No | Write to file instead of stdout |

---

### put

Write an object to the cluster. Automatically replicated to N shards.

```bash
shardedobjstr put --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --key test/myfile.txt --from myfile.txt
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path to store as |
| `--from <local-file>` | No | Local file to read (reads from stdin if omitted) |

Output shows which shards received the object:

```
Put test/myfile.txt -> shards [0, 2] (4096 bytes)
```

---

### delete

Delete an object from all shards that hold it.

```bash
shardedobjstr delete --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --key test/myfile.txt
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path to delete |

Aliases: `del`

---

### verify

Check integrity of every shard and cross-shard consistency of replicated objects.

```bash
shardedobjstr verify --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --replicas 2
```

Checks performed:
- Per-shard: extent CRC32c, header consistency, free list, space accounting
- Cross-shard: compare CRC32c of the same object on different replicas

Example clean output:

```
Shard 0: 4 files, 0 errors
Shard 1: 4 files, 0 errors
Shard 2: 4 files, 0 errors

Cross-shard CRC check: 6 objects verified, 0 mismatches
Cluster is clean.
```

---

### cross-verify

MD5-based cross-shard verification. Reads the full body of every replica
from each shard, computes MD5, and compares. Unlike `verify` (which uses
the CRC32c stored in the catalog/shard index), `cross-verify` re-reads
the actual bytes so it catches silent corruption on any backend type
(raw, filesystem, S3).

Requires `--replicas >= 2`.

```bash
# Verify all objects across the cluster
shardedobjstr cross-verify \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2

# Verify a single object
shardedobjstr cross-verify \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --key test/myfile.txt

# Verify only objects under a prefix
shardedobjstr cross-verify \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 --prefix data/
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <key>` | No | Verify a single object instead of all |
| `--prefix <prefix>` | No | Only verify objects under this prefix |

Example clean output:

```
Cross-shard MD5 verification
============================
  Objects checked:                6
  Objects OK:                     6
  Objects with MD5 mismatch:      0
  Objects with read errors:        0
  Objects skipped (single replica): 0

RESULT: ALL REPLICAS CONSISTENT
```

Example with corruption detected:

```
Cross-shard MD5 verification
============================
  Objects checked:                6
  Objects OK:                     5
  Objects with MD5 mismatch:      1
  Objects with read errors:        0
  Objects skipped (single replica): 0

Details:

  test/obj_0002.bin [MISMATCH]
    catalog CRC32c: 0x1a2b3c4d
    shard  0:  md5=3475cb96e0308cb84502be1c1531b588  size=      4096  modified=2026-04-05T10:00:00+00:00
    shard  1:  md5=53ed66e9559a6ca274de180d085e7fb9  size=        23  modified=2026-04-05T10:01:00+00:00
    shard  2:  md5=3475cb96e0308cb84502be1c1531b588  size=      4096  modified=2026-04-05T10:00:00+00:00

RESULT: MISMATCHES FOUND
```

Exit code: 0 if all replicas consistent, 1 if any mismatch or error.

---

### health

Probe each shard for liveness and report status. Each shard is opened
and tested with a timeout; unreachable or corrupt shards are flagged.

```bash
shardedobjstr health --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw
```

Example output:

```
Probing 3 shards...

Shard 0: HEALTHY  /tmp/s0.raw  (4 files, 2.1% used, 223.5 MB free)
Shard 1: HEALTHY  /tmp/s1.raw  (4 files, 2.1% used, 223.5 MB free)
Shard 2: HEALTHY  /tmp/s2.raw  (4 files, 2.0% used, 223.5 MB free)

All 3 shards healthy.
```

Exit code 0 if all shards are healthy, 1 if any are unreachable or failed.

---

### add-shard

Add a new shard to the cluster. The new shard is formatted and added but
existing objects are not moved (repair-replication is manual).

```bash
shardedobjstr add-shard \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --new-shard /tmp/s3.raw \
  --size 268435456 \
  --replicas 2
```

| Option | Required | Description |
|--------|----------|-------------|
| `--new-shard <path>` | Yes | Path for the new shard image |
| `--size <bytes>` | Yes | Size of the new shard |

---

### remove-shard

Drain all objects from a shard and remove it from the cluster.

```bash
shardedobjstr remove-shard \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw,/tmp/s3.raw \
  --remove /tmp/s3.raw \
  --replicas 2
```

| Option | Required | Description |
|--------|----------|-------------|
| `--remove <path>` | Yes | Path of the shard to remove |

**Guards:**
- Minimum 3 shards must remain
- Remaining shards must be >= replication factor

The command reads all objects from the victim shard and writes them to
surviving shards before removing it.

---

### repair-replication

Scan the cluster for replication imbalances and fix them in one pass.
Phase 1 re-replicates objects below the replication factor (under-replicated).
Phase 2 trims excess copies from objects above the replication factor
(over-replicated), preferring to remove from the fullest shard.

```bash
# Basic repair-replication (default batch size 100)
shardedobjstr repair-replication \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2

# With catalog persistence and larger batch
shardedobjstr repair-replication \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 \
  --catalog json:/tmp/catalog.json \
  --batch-size 500
```

| Option | Required | Description |
|--------|----------|-------------|
| `--batch-size <N>` | No | Max objects to process per phase (default: 100) |

Example output:

```
Repair-replication: re_replicated=3, trimmed=2, under_remaining=0, over_remaining=0
```

**When to use:**
- After a shard returns from being offline (re-replication may have created
  extra copies; trimming restores the correct RF)
- After adding or removing a shard to redistribute data
- Periodically as a consistency check

**Automatic repair-replication:** The `objstrd` daemon can run repair-replication sweeps
automatically in the background via `--repair-replication-interval-secs` (or the
`repair_replication_interval` tree config directive). See
[objstrd/CLI.md](../objstrd/CLI.md) for details.

---

### report

Generate a cluster health report showing all objects with their size, replica
count, CRC32c, and shard placement. Objects below the target replication factor
are marked with `*`.

```bash
# Full report (all objects)
shardedobjstr report \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2

# Only under-replicated objects
shardedobjstr report \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 \
  --under-replicated

# With catalog persistence
shardedobjstr report \
  --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw \
  --replicas 2 \
  --catalog json:/tmp/catalog.json
```

| Option | Required | Description |
|--------|----------|-------------|
| `--under-replicated` | No | Show only objects below the target replication factor |

Full report output:

```
Cluster report: 3 shards, RF = 2, 6 objects

KEY                                                            SIZE  REPLICAS     CRC32C  SHARDS
----------------------------------------------------------------------------------------------------
config/settings.json                                             64         2   1a2b3c4d  [1,2]
db/_versions/1.manifest                                    512         2   a3b1c4d2  [0,2]
db/data/00000.lance                                       4096         2   e7f2a1b3  [0,1]
db/data/00001.lance                                       4096         1   f1g2h3i4  [0] *
db/data/00002.lance                                       8192         2   b5c6d7e8  [1,2]
db/data/00003.lance                                       4096         2   c9d0e1f2  [0,2]

6 objects total
1 under-replicated (marked with *)
```

Under-replicated filter output (`--under-replicated`):

```
1 under-replicated objects (RF = 2):

KEY                                                          REPLICAS
----------------------------------------------------------------------
db/data/00001.lance                                            1

Total: 1 under-replicated
```

If all objects meet the replication target:

```
All objects meet replication factor 2
```

---

### vacuum

Purge stale delete markers from all shards. Delete markers prevent object
resurrection during recovery; once all shards are synced they can be cleaned up.

```bash
shardedobjstr vacuum --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --replicas 2
```

Example output:

```
Vacuum complete: 5 markers purged, 2 stale objects cleaned
```

---

### list-deleted

List all current delete markers with their deletion timestamps.

```bash
shardedobjstr list-deleted --shards /tmp/s0.raw,/tmp/s1.raw,/tmp/s2.raw --replicas 2
```

Example output:

```
KEY                                                          DELETED AT
------------------------------------------------------------ ------------------------------
old/data/file1.text                                         2026-03-28T10:15:03+00:00
old/data/file2.text                                         2026-03-28T10:16:44+00:00

2 delete marker(s) total.
```

If no markers exist:

```
No delete markers found.
```



## shardedobjstr-catview

Standalone tool to dump a JSON or bincode catalog file as human-readable text.

```bash
shardedobjstr-catview <catalog-file> [options]
```

### Options

| Flag | Description |
|------|-------------|
| `--format json\|bin` | Force format (default: auto-detect by extension) |

Auto-detection rules: `.json` extension uses JSON, `.bin` extension uses
bincode. If the extension is unrecognized, both formats are tried (JSON first,
then bincode). Output is always sorted by path.

### Example output

```
PATH                                                          SIZE       CRC32C        SHARDS  UPDATED
--------------------------------------------------------------------------------------------------------------
ex1/_versions/1.manifest                                  512 B    0xa3b1c4d2         0,2  2026-03-28 10:15:03
ex1/data/00000.lance                                      4.0 KB   0xe7f2a1b3         0,1  2026-03-28 10:15:03
config/settings.json                                          64 B     0x1a2b3c4d         1,2  2026-03-28 10:15:04
--------------------------------------------------------------------------------------------------------------
3 objects, 4.6 KB total
```

---

## Programmatic API

Both CLI tools are thin wrappers around `ShardedObjectStore` methods:

| CLI Command | Rust Method | Notes |
|-------------|-------------|-------|
| `info` | `shard_store(id)` + `device_info()` | Per-shard stats |
| `list` | `list(prefix)` | Deduplicated via catalog |
| `get` | `get(&path)` | Failover across replicas |
| `put` | `put(&path, payload)` | Auto-replicated |
| `delete` | `delete(&path)` | Fan-out to all shards |
| `verify` | Per-shard `verify_all()` + CRC cross-check | |
| `repair-replication` | `repair::repair_replication_sweep()` | Under-replication repair + over-replication trim |
| `report` | `catalog().all_entries()` + `find_under_replicated()` | Placement and health report |
| -- | `find_under_replicated()` | Objects below RF |
| -- | `find_over_replicated()` | Objects above RF |
| -- | `find_replication_target(key)` | Least-full healthy shard not holding the object |
| -- | `pick_excess_shard(key)` | Fullest healthy shard to shed |
| -- | `replicate_object(key, from, to, raw_refs)` | Copy object between shards, preserving metadata when `raw_refs` is `Some` |
| -- | `remove_replica(key, shard_id)` | Delete a single replica |
| -- | `shard_crc_error_count(id)` | Per-shard CRC error counter (atomic, survives SIGHUP) |
| -- | `shard_store(id)` | Direct access to a shard's underlying `ObjectStore` |
| -- | `shard_health(id)` | Query the current health of a shard |
| -- | `shard_offline_since(id)` | When the shard went offline (`DateTime<Utc>`) |
| -- | `shard_free_space(id)` | Free space on a shard |
| -- | `set_shard_free_space(id, free)` | Update free space tracking |
| -- | `shard_count()` | Number of shards |
| -- | `replication_factor()` | Configured replication factor |
| -- | `target_shards(path)` | Select target shards for a new write |
| -- | `read_shard_order(path)` | Select read order for an existing object |
| -- | `read_preference()` / `set_read_preference(pref)` | Get/set read preference (RoundRobin, Ordered) |
| -- | `detach_shard(id)` | Replace shard with offline placeholder |
| -- | `attach_shard(id, store, force)` | Swap in a live store, rebuild catalog |
| -- | `rebuild_catalog_for_shard(id)` | Rebuild catalog entries for one shard |
| -- | `set_shard_health(id, health)` | Set shard health (Healthy/Degraded/Offline) |
| -- | `invalidate_shard(shard_id)` | Purge + re-scan shard; returns `InvalidateReport` |
| -- | `set_persistence(p)` / `persistence()` | Get/set catalog persistence (None, JSON, Bincode) |
| -- | `save_catalog()` / `load_catalog()` | Persist/load catalog to/from configured backend. On load, the daemon strips entries for offline and ephemeral (mem) shards so replica counts are accurate. |
| -- | `is_read_only()` / `with_read_only(bool)` | Builder/query for read-only mode |
| -- | `set_event_bus(bus)` | Attach an event bus for change notifications |
| -- | `catalog.entries_for_shard(id)` | Query catalog entries on a shard |
| -- | `catalog.remove_all_for_shard(id)` | Bulk purge entries for a shard. Called at startup for offline and mem shards to keep the catalog accurate after a restart. |
| -- | `catalog.all_entries()` | All catalog entries (used by `report`) |

```rust
use shardedobjstr::ShardedObjectStore;
use object_store::{ObjectStore, path::Path, PutPayload};

let cluster = ShardedObjectStore::new(stores, 2);

// Put (replicated)
cluster.put(&Path::from("key"), PutPayload::from(b"data".to_vec())).await?;

// Get (with failover)
let result = cluster.get(&Path::from("key")).await?;

// Check placement
if let Some(entry) = cluster.placement("key") {
    println!("On shards: {:?}, size: {}", entry.shards, entry.size);
}

// Rebuild catalog from shards (recovery)
let count = cluster.rebuild_catalog().await?;

// Multipart upload (parts spooled to disk on primary shard, not RAM)
let upload = cluster.put_multipart(&Path::from("big/file")).await?;
// ... upload.put_part(chunk).await? ...
// upload.complete().await? -- replicates assembled object to other shards

// Shard health management
cluster.set_shard_health(0, ShardHealth::Degraded);
let report = cluster.invalidate_shard(1).await?;
println!("purged={}, restored={}, missing={}", report.entries_purged, report.entries_restored, report.missing_keys.len());
```

---

## Config File Format

Both flat and tree config files are supported. The CLI auto-detects the format.
See **[CONFIG.md](../CONFIG.md)** for the full reference, including:

- Flat config directives and shard line syntax
- Tree config with nested nodes and per-node replication factors
- Auto-detection rules
- `--node` targeting for tree configs
- Rust and Python API functions

### Quick Example (flat)

```
# cluster.conf
replicas     2
catalog      json:/tmp/catalog.json

shard  raw  /dev/nvme0n1
shard  raw  /dev/nvme1n1
```

```bash
shardedobjstr info --config cluster.conf
shardedobjstr list --config cluster.conf --long
```

### Quick Example (tree)

```
cluster  my-cluster

root  rf=2
  raw  /dev/nvme0n1
  child  rf=1
    fs   /mnt/data
    raw  /dev/sda
```

```bash
shardedobjstr list --config cluster.conf
shardedobjstr health --config cluster.conf --node child
```

---

## lance_on_cluster

Demonstrates LanceDB using the sharded store as its backend. Creates a LanceDB
database, writes tables, queries data, and shows object placement across shards.

```bash
cargo run --example lance_on_cluster -- [options]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--shards <paths>` | `/tmp/lance_cluster_s0.raw,...s2.raw` | Shard image paths (3 paths) |
| `--size <bytes>` | `268435456` (256 MB) | Size per shard |
| `--replicas <N>` | `2` | Replication factor |

### What it does

1. **Format shards** - Creates 3 shard images
2. **Build cluster** - Initializes `ShardedObjectStore` with catalog persistence
3. **Connect LanceDB** - Registers `ShardedObjectStoreProvider` under `cluster://` scheme
4. **Create table** - Creates a `test` table with 5 rows (id, value, label)
5. **Query** - Reads all rows back via `table.query().execute()`
6. **Add rows** - Inserts 3 more rows with `table.add(batch).execute()`
7. **Show placement** - Prints which shards hold each Lance file
8. **Save catalog** - Persists the catalog to a JSON file
9. **Reopen & verify** - Reopens shards, loads the persisted catalog, verifies data intact
10. **Per-shard stats** - Shows file count and data usage per shard

### Example output

```
=== Step 1: Formatting 3 shards ===
  Formatted /tmp/lance_cluster_s0.raw (256 MB)
  Formatted /tmp/lance_cluster_s1.raw (256 MB)
  Formatted /tmp/lance_cluster_s2.raw (256 MB)

=== Step 2: Building cluster (3 shards, replication=2) ===
  Rebuilt catalog: 0 objects

=== Step 3: Connecting LanceDB via cluster:// ===
  Connected to cluster://data

=== Step 4: Creating 'test' table (5 rows) ===
  Table created: 5 rows

=== Step 5: Querying table ===
  Read 5 rows

=== Step 7: Object placement ===
  test/_versions/1.manifest -> shards [1, 2] (512 bytes)
  test/data/00000.sb     -> shards [0, 1] (4096 bytes)

=== Step 8: Saving catalog ===
  Saved to /tmp/lance_cluster_catalog.json

=== Step 9: Reopen + verify ===
  Loaded catalog: 4 objects
  Table still has 8 rows [x]
```

---