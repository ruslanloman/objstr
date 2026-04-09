# shardedobjstr - Python bindings for shardedobjstr

Distribute objects across multiple raw block device shards with
configurable replication and automatic failover.

## Quick start

```bash
# Build the native extension (Linux only)
pip install maturin
cd shardedobjstr/python
maturin develop --release
```

```python
import shardedobjstr

# Format two 1 GB shard images and open as a cluster (replication=2)
store = shardedobjstr.format_and_open_cluster(
    [("/tmp/shard0.raw", 1_073_741_824), ("/tmp/shard1.raw", 1_073_741_824)],
    replication_factor=2,
)

# Same put/get/delete/list API as rawobjstr.Store
store.put("hello.txt", b"Hello, cluster!")
data = store.get("hello.txt")

# Inspect placement
info = store.placement("hello.txt")
print(info.shards)   # e.g. [0, 1]

# Persist indexes and close
store.flush_all()
```

## Write Quorum (min_writes)

By default, a put requires at least `max(replication_factor - 1, 1)` replicas
to land. Override with `min_writes`:

```python
# Require all replicas to succeed (strict):
store = shardedobjstr.open_cluster(paths, replication_factor=3, min_writes=3)

# Allow maximally degraded writes (1 replica is enough):
store = shardedobjstr.open_cluster(paths, replication_factor=3, min_writes=1)

print(store.min_writes())  # 1
```

## Delete Quorum (delete_requires_min_writes)

By default, deletes succeed if at least one shard accepts the delete marker.
Set `delete_requires_min_writes=True` to make deletes also require `min_writes`
replicas:

```python
store = shardedobjstr.open_cluster(
    paths,
    replication_factor=3,
    min_writes=2,
    delete_requires_min_writes=True,
)

print(store.delete_requires_min_writes())  # True
# delete() now fails if fewer than 2 shards accept the delete marker
```

## Formatting shards

```python
import shardedobjstr

# Format individual shards (size required for image files; auto-detected for block devices)
shardedobjstr.format_shard("/tmp/shard0.raw", size=1_073_741_824)

# Format with full options
shardedobjstr.format_shard_with_options(
    "/tmp/shard0.raw",
    size=1_073_741_824,
    index_slot_size=16 * 1024 * 1024,
    max_key_length=512,
    compression="zstd",  # "snappy", "zstd", or None
)

# Format + open in one step, with compression
store = shardedobjstr.format_and_open_cluster(
    [("/tmp/s0.raw", 1_073_741_824), ("/tmp/s1.raw", 1_073_741_824)],
    replication_factor=2,
    compression="snappy",
)
```

## Opening an existing cluster

```python
import shardedobjstr

store = shardedobjstr.open_cluster(
    ["/tmp/shard0.raw", "/tmp/shard1.raw"],
    replication_factor=2,
)

# Rebuild catalog by scanning all shards
count = store.rebuild_catalog()
print(f"Found {count} objects across shards")
```

## Catalog persistence

```python
import shardedobjstr

# Open with a catalog file - loads automatically if it exists
store = shardedobjstr.open_cluster(
    ["/tmp/shard0.raw", "/tmp/shard1.raw"],
    replication_factor=2,
    catalog_path="/tmp/catalog.json",
)

# Save catalog before shutdown
store.save_catalog("/tmp/catalog.json")
```

## Config file

A `.conf` file can describe the entire cluster (shards, replication, catalog,
and per-shard options) so you do not have to pass them all in code.
Both flat and tree config formats are auto-detected.
See [CONFIG.md](../../CONFIG.md) for the full file format reference.

**Note:** `open_cluster_from_config()` currently only supports **raw shards**.
Configs containing `fs`, `s3`, `mem`, or `node` shards will raise `ValueError`.
Use `open_cluster()` or `open_fs_cluster()` directly for other shard types.

```ini
# cluster.conf
replicas   2
catalog    /tmp/catalog.json
direct_io  true

shard raw /tmp/shard0.raw
shard raw /tmp/shard1.raw compression=zstd
shard raw /tmp/shard2.raw readonly
```

```python
import shardedobjstr

# Open a cluster from a config file (raw shards only)
store = shardedobjstr.open_cluster_from_config("cluster.conf")

# Override options at open time
store = shardedobjstr.open_cluster_from_config(
    "cluster.conf", direct_io=False, read_only=True,
)

# Load config as a dict (without opening - works with all shard types)
conf = shardedobjstr.load_config("cluster.conf")
print(conf["replicas"], conf["shards"])

# Validate a config file
diags = shardedobjstr.check_config("cluster.conf")
for d in diags:
    print(d["level"], d["message"])
```

## Read, list, copy, delete

```python
# Byte-range read
chunk = store.get("data.bin", range=(0, 1024))

# Metadata without reading data
meta = store.head("data.bin")
print(meta.location, meta.size, meta.last_modified)

# List all objects (or filter by prefix)
for obj in store.list("table/data"):
    print(obj.location, obj.size)

# Directory-style listing
result = store.list_with_delimiter("table")
print(result.objects, result.common_prefixes)

# Copy, rename, delete
store.copy("src.bin", "dst.bin")
store.rename("old.txt", "new.txt")
store.delete("old.txt")
```

## Delete markers

When an object is deleted, a delete marker is written to all healthy shards so
that the deletion is visible cluster-wide even before recovery sync runs. The
marker is a lightweight sentinel - it hides the key from `get()`, `head()`, and
`list()` calls.

Key behaviors:

- **PUT cleans stale markers:** Re-writing a key that was previously deleted
  automatically removes the old delete marker (best-effort).
- **Markers are not rolled back:** If `delete()` succeeds on some shards but
  fails on others, the marker is kept for safety rather than rolled back.
- **Vacuum requires all shards online:** `vacuum_delete_markers()` refuses to
  run if any shard is offline, because the marker may still be needed for
  recovery.
- **Reattach cleans stale markers:** When a shard comes back online, the sync
  process removes stale markers that are no longer relevant (e.g., the object
  was re-PUT while the shard was away).

```python
# Delete an object - marker written to all healthy shards
store.delete("report.csv")

# List current markers and their timestamps
markers = store.list_delete_markers()
for key, deleted_at in markers:
    print(f"{key} deleted at {deleted_at}")

# Vacuum: purge fully-applied markers (all shards must be healthy)
purged, stale_cleaned = store.vacuum_delete_markers()
print(f"Purged {purged} markers, cleaned {stale_cleaned} stale objects")

# Re-PUT after delete: marker is cleaned automatically
store.put("report.csv", new_data)
assert len(store.list_delete_markers()) == 0  # marker gone
```

## Cluster management

```python
# Number of shards and replication factor
print(store.shard_count(), store.replication_factor())

# Shard health
for i in range(store.shard_count()):
    print(i, store.shard_health(i))

# Object placement
info = store.placement("data.bin")
print(info.shards, info.size, info.crc32c)

# Catalog size
print(store.catalog_len(), "objects tracked")
```

## Degraded startup and shard management

```python
import shardedobjstr

# Open a cluster where shard 1 is offline
store = shardedobjstr.open_cluster_degraded(
    ["/tmp/shard0.raw", None, "/tmp/shard2.raw"],
    replication_factor=2,
)

# Reads and writes work on healthy shards
store.put("key", b"value")
data = store.get("key")

# Find under-replicated objects
for key, count in store.find_under_replicated():
    print(f"{key}: {count} replicas (need {store.replication_factor()})")

# Bring shard 1 back online
shardedobjstr.format_shard("/tmp/shard1.raw", size=1_073_741_824)
count = store.attach_shard(1, "/tmp/shard1.raw", force=True)
print(f"Found {count} objects on reattached shard")

# Replicate under-replicated objects
for key, _count in store.find_under_replicated():
    info = store.placement(key)
    store.replicate_object(key, info.shards[0], 1)

# Detach a shard (goes offline, catalog entries preserved)
prev_health = store.detach_shard(0)
print(f"Shard 0 was {prev_health}, now offline")

# Take a shard offline with hold (recovery loop will not auto-reattach)
prev = store.hold_offline(0)
print(f"Shard 0 was {prev}, now Detached")

# Take offline and suppress re-replication (objects stay as-is)
prev = store.hold_offline(0, suppress_replication=True)

# Release the hold (shard transitions to Offline, eligible for manual attach)
was_held = store.release_hold(0)

# Rebuild catalog for a single shard
count = store.rebuild_catalog_for_shard(2)
print(f"Restored {count} entries for shard 2")
```

## Repair-replication and repair

```python
import shardedobjstr

store = shardedobjstr.open_cluster(
    ["/tmp/shard0.raw", "/tmp/shard1.raw", "/tmp/shard2.raw"],
    replication_factor=2,
)

# One-shot repair-replication: repair under-replicated, trim over-replicated
result = store.repair_replication(batch_size=200)
print(result)
# RepairReplicationResult(re_replicated=3, trimmed=1, under_remaining=0, over_remaining=0)

# Re-replication sweep
count = store.re_replication_sweep(batch_size=100)
print(f"Re-replicated {count} objects")

# Over-replication trim (remove excess copies beyond RF)
trimmed = store.over_replication_trim(batch_size=200)
print(f"Trimmed {trimmed} excess replicas")

# Drain a shard (replicate single-copy objects, then detach + repair-replication)
result = store.drain_shard(2, batch_size=500)
print(result)
# RepairReplicationResult(re_replicated=15, trimmed=0, under_remaining=0, over_remaining=0)

# Check CRC error count per shard
for i in range(store.shard_count()):
    errors = store.crc_error_count(i)
    if errors > 0:
        print(f"Shard {i}: {errors} CRC errors detected")

# Monitor read-repair activity
print(f"Read repairs triggered: {store.read_repair_count()}")
print(f"  succeeded: {store.read_repair_success()}")
print(f"  failed:    {store.read_repair_failed()}")

# Check shard health details
for i in range(store.shard_count()):
    offline_ts = store.shard_offline_since(i)
    free = store.shard_free_space(i)
    if offline_ts is not None:
        print(f"Shard {i}: offline since {offline_ts}")
    if free is not None:
        print(f"Shard {i}: {free} bytes free")

# Query multipart expiry
print(f"Multipart expiry: {store.multipart_expiry()} seconds")

# Manual over-replication detection and trimming
for key, count in store.find_over_replicated():
    excess = store.pick_excess_shard(key)
    if excess is not None:
        store.remove_replica(key, excess)
        print(f"Trimmed {key} from shard {excess}")
store.flush_all()

# Manual replication target selection
target = store.find_replication_target("data/file.bin")
if target is not None:
    store.replicate_object("data/file.bin", source_shard, target)
```

## Shard health and invalidation

```python
import shardedobjstr

# ShardHealth values: Healthy, Degraded, Offline, Syncing, Detached

# Mark a shard as degraded
prev = store.set_shard_health(0, shardedobjstr.ShardHealth.Degraded)
print(f"Shard 0 was {prev}, now Degraded")

# Invalidate a shard (purge catalog entries, re-scan, report)
report = store.invalidate_shard(1)
print(report)  # InvalidateReport(shard=1, purged=5, restored=4, missing=1, ok=True)
print(report.missing_keys)  # keys lost on this shard

# Query catalog entries on a specific shard
entries = store.entries_for_shard(0)
for key, info in entries:
    print(key, info.shards, info.size)

# Bulk purge all entries for a shard
removed = store.remove_all_for_shard(2)
print(f"Removed {removed} catalog entries for shard 2")
```

## Multipart upload

```python
# Single-shot convenience (one part + complete)
store.put_multipart("large/file.bin", data)
```

## Metadata-aware I/O

Objects can carry an arbitrary metadata suffix (raw bytes) alongside the body.
This is useful for storing schema info, checksums, or provenance tags without
a second round-trip.

```python
# Write an object with metadata attached
store.put_with_meta("events.parquet", parquet_bytes, b'{"schema_v": 2}')

# Read just the metadata (no body I/O)
meta_bytes = store.get_metadata("events.parquet")
print(meta_bytes)  # b'{"schema_v": 2}'

# head_with_meta returns (ObjectMeta, meta_len)
obj_meta, meta_len = store.head_with_meta("events.parquet")
print(f"size={obj_meta.size}, meta_len={meta_len}")

# List with metadata lengths
for obj_meta, meta_len in store.list_with_meta("events"):
    print(f"{obj_meta.location}: meta_len={meta_len}")

# Update metadata length in-place (without re-uploading the body)
store.set_meta_len("events.parquet", 16)
```

## Cross-verify (MD5-based)

Cross-verify compares MD5 digests across replicas rather than relying on
catalog CRC32c values. This detects corruption even when replicas were
written through different backends (raw block devices, filesystem shards,
S3-backed shards) that may not share the same CRC metadata.

```python
# Verify a single object across replicas
report = store.cross_verify_object("data/file.bin")
print(report.consistent)  # True if all replicas match
for d in report.shards:
    print(f"  shard {d.shard_id}: md5={d.md5_hex}, size={d.size}")
for shard_id, err in report.errors:
    print(f"  shard {shard_id}: ERROR {err}")

# Verify all objects (only objects with 2+ replicas)
report = store.cross_verify_all()
print(f"checked={report.objects_checked}, ok={report.objects_ok}, "
      f"mismatched={report.objects_mismatched}, "
      f"skipped={report.objects_skipped_single_replica}")

# Filter by prefix
report = store.cross_verify_all(prefix="table/data/")
for detail in report.details:
    print(f"MISMATCH: {detail.key}")

# Compare with CRC32c-based verify
crc_report = store.verify_all()
md5_report = store.cross_verify_all()
print(f"CRC32c verify: {crc_report.objects_ok}/{crc_report.objects_checked}")
print(f"MD5 cross-verify: {md5_report.objects_ok}/{md5_report.objects_checked}")
```

## Context manager

```python
with shardedobjstr.format_and_open_cluster(shards, replication_factor=2) as store:
    store.put("key", b"value")
    # flush_all() called automatically on exit
```

## API reference

### Module functions

| Function | Description |
|----------|-------------|
| `format_shard(path, *, size, direct_io)` | Format a single shard |
| `format_shard_with_options(path, *, size, direct_io, index_slot_size, max_key_length, compression)` | Format with full options |
| `open_cluster(paths, *, replication_factor, min_writes, delete_requires_min_writes, direct_io, read_only, catalog_path, catalog_format)` | Open existing shards as a cluster |
| `open_cluster_degraded(shard_paths, *, replication_factor, min_writes, delete_requires_min_writes, direct_io, read_only, catalog_path, catalog_format)` | Open with some shards offline (degraded startup) |
| `format_and_open_cluster(shards, *, replication_factor, min_writes, delete_requires_min_writes, direct_io, compression)` | Format and open in one step |
| `open_fs_cluster(roots, *, replication_factor, min_writes, delete_requires_min_writes)` | Open a cluster backed by filesystem directories |
| `open_cluster_from_config(path, *, min_writes, direct_io, read_only)` | Open a cluster from a `.conf` file (raw shards only) |
| `load_config(path)` | Parse a config file and return as a dict (all shard types) |
| `check_config(path)` | Validate a config file; returns list of diagnostics |

### ClusterStore methods

| Method | Description |
|--------|-------------|
| `put(key, data)` | Write an object (replicated to N shards) |
| `put_if_not_exists(key, data)` | Write only if absent; raises `FileExistsError` if key exists |
| `get(key, *, range)` | Read an object or byte range |
| `head(key)` | Get metadata without reading data |
| `delete(key)` | Delete from all replica shards |
| `list_delete_markers()` | List current delete markers as `(key, deleted_at_rfc3339)` tuples |
| `vacuum_delete_markers()` | Purge applied markers; returns `(purged, stale_cleaned)` |
| `list(prefix)` | List objects (optional prefix filter) |
| `list_with_delimiter(prefix)` | Directory-style listing |
| `copy(src, dst)` | Copy an object. **Does not preserve metadata** (uses get+put internally) |
| `copy_if_not_exists(src, dst)` | Copy only if destination is absent. Same metadata limitation as `copy()` |
| `rename(src, dst)` | Rename (copy + delete). Same metadata limitation as `copy()` |
| `rename_if_not_exists(src, dst)` | Rename only if destination is absent. Same metadata limitation |
| `shard_count()` | Number of shards |
| `replication_factor()` | Configured replication factor |
| `min_writes()` | Minimum successful writes required for puts |
| `delete_requires_min_writes()` | Whether deletes also enforce `min_writes` |
| `shard_health(id)` | Health of a shard |
| `set_shard_health(id, health)` | Set shard health; returns previous `ShardHealth` |
| `invalidate_shard(shard_id)` | Purge + re-scan shard; returns `InvalidateReport` |
| `entries_for_shard(shard_id)` | List catalog entries on a shard |
| `remove_all_for_shard(shard_id)` | Purge all catalog entries for a shard; returns count |
| `put_multipart(key, data)` | Single-shot multipart upload (delegates to primary shard) |
| `put_with_meta(key, data, metadata)` | Write an object with raw metadata bytes attached |
| `head_with_meta(key)` | Returns `(ObjectMeta, meta_len)` without reading body |
| `get_metadata(key)` | Read raw metadata bytes for an object |
| `list_with_meta(prefix)` | List objects with metadata lengths as `(ObjectMeta, meta_len)` tuples |
| `set_meta_len(key, meta_len)` | Update metadata length in-place without re-uploading |
| `placement(key)` | Which shards hold an object |
| `catalog_len()` | Number of tracked objects |
| `save_catalog(path, *, format)` | Save catalog (`"json"` or `"bincode"`) |
| `rebuild_catalog()` | Rebuild catalog from shard scans |
| `rebuild_catalog_for_shard(shard_id)` | Rebuild catalog entries for a single shard |
| `find_under_replicated()` | Find objects below target replication factor |
| `detach_shard(shard_id)` | Detach shard (goes offline, catalog preserved) |
| `hold_offline(shard_id, suppress_replication=False)` | Detach and mark Detached (recovery will not auto-reattach); suppress_replication prevents re-replication sweep |
| `release_hold(shard_id)` | Clear the Detached hold (transitions to Offline) |
| `attach_shard(shard_id, path, *, force)` | Attach a store to an offline shard slot |
| `replicate_object(key, from_shard, to_shard)` | Copy an object between shards |
| `find_over_replicated()` | Find objects with more replicas than RF |
| `pick_excess_shard(key)` | Pick the fullest healthy shard to shed a replica from |
| `remove_replica(key, shard_id)` | Remove a single replica from a shard |
| `find_replication_target(key)` | Find the least-full healthy shard not holding the object |
| `repair_replication(*, batch_size)` | Run full repair-replication sweep; returns `RepairReplicationResult` |
| `re_replication_sweep(*, batch_size)` | Copy under-replicated objects to healthy shards |
| `over_replication_trim(*, batch_size)` | Remove excess replicas beyond RF |
| `drain_shard(shard_id, *, batch_size)` | Replicate single-copy objects, then detach and repair-replication |
| `crc_error_count(shard_id)` | Atomic counter of CRC integrity failures detected on reads |
| `shard_offline_since(shard_id)` | Unix timestamp when shard went offline, or `None` if healthy |
| `shard_free_space(shard_id)` | Cached free space in bytes, or `None` if not set |
| `set_shard_free_space(shard_id, free)` | Update the cached free space for a shard |
| `read_repair_count()` | Number of background read-repair tasks triggered |
| `read_repair_success()` | Number of read-repair tasks that succeeded |
| `read_repair_failed()` | Number of read-repair tasks that failed |
| `multipart_expiry()` | Multipart upload expiry duration in seconds |
| `verify_object(key)` | CRC32c-based verify of a single object across replicas |
| `verify_all(*, prefix)` | CRC32c-based verify of all objects (optional prefix filter) |
| `cross_verify_object(key)` | MD5-based cross-verify of a single object across replicas |
| `cross_verify_all(*, prefix)` | MD5-based cross-verify of all objects (optional prefix filter) |
| `plan_repair_replication(*, batch_size)` | Dry-run: preview what `repair_replication()` would do; returns `RepairReplicationPlan` |
| `validate_shard_access()` | Check each shard's accessibility; returns `[(shard_id, bool)]` |
| `multipart_upload_count()` | Number of in-progress multipart uploads |
| `list_multipart_uploads()` | List in-progress multipart uploads |
| `purge_stale_multiparts()` | Purge expired multipart uploads; returns count |
| `read_only` | Property: whether the cluster is in read-only mode |
| `read_preference()` | Get read preference (`"round-robin"` or `"ordered"`) |
| `set_read_preference(pref)` | Set read preference |
| `put_with_meta_from_file(key, file_path, meta_len)` | Stream a file with metadata directly (avoids loading into memory) |
| `redistribute(*, batch_size, tolerance_pct)` | Balance object counts across shards; returns `RedistributeResult` |
| `shard_suppress_replication(shard_id)` | Check if re-replication is suppressed for a shard |
| `shard_detach_reason(shard_id)` | Return why a shard was detached, or `None` |
| `set_detach_reason(shard_id, reason)` | Annotate a detached shard with a reason (`"Manual"`, `"ProbeFailure"`, `"DeviceMissing"`, `"Drain"`) |
| `flush_all()` | Flush every shard's index to disk |

### Data classes

| Class | Fields |
|-------|--------|
| `ObjectMeta` | `location`, `size`, `last_modified`, `e_tag` |
| `ListResult` | `objects`, `common_prefixes` |
| `PlacementInfo` | `shards`, `size`, `crc32c`, `updated` |
| `ShardHealth` | `Healthy`, `Degraded`, `Offline`, `Syncing`, `Detached` |
| `InvalidateReport` | `shard_id`, `entries_purged`, `entries_restored`, `missing_keys`, `scan_ok` |
| `RepairReplicationResult` | `re_replicated`, `trimmed`, `under_remaining`, `over_remaining` |
| `VerifyReplicaResult` | `shard_id`, `crc32c`, `size`, `matches_catalog`, `error` |
| `VerifyObjectReport` | `key`, `catalog_crc`, `replicas`, `replicas_consistent` |
| `VerifyReport` | `objects_checked`, `objects_ok`, `objects_mismatched`, `objects_with_errors`, `details` |
| `PlannedAction` | `key`, `current_count`, `target_rf`, `action_type`, `source_shard`, `target_shard`, `trim_shard` |
| `RepairReplicationPlan` | `replications`, `trims`, `unrepairable`, `untrimmable` |
| `CrossVerifyShardDigest` | `shard_id`, `md5_hex`, `size`, `last_modified` |
| `CrossVerifyObjectReport` | `key`, `catalog_crc`, `shards`, `consistent`, `errors` |
| `CrossVerifyReport` | `objects_checked`, `objects_ok`, `objects_mismatched`, `objects_with_errors`, `objects_skipped_single_replica`, `details` |
| `RedistributeResult` | `moved`, `skipped`, `errors`, `shard_counts` |

### Module attributes

| Attribute | Description |
|-----------|-------------|
| `__version__` | Package version string |
| `__build_info__` | Version, git hash, and build date |

## fsspec integration

The `shardedobjst://` protocol is registered with [fsspec](https://filesystem-spec.readthedocs.io/),
so any fsspec-aware library (pandas, PyArrow, Dask, Polars, xarray, etc.) can
read and write directly from a sharded cluster.

```bash
pip install fsspec
```

### Basic usage

```python
import fsspec

# Open a filesystem backed by a 2-shard cluster with replication
fs = fsspec.filesystem("shardedobjst",
                       shard_paths=["/dev/nvme0n1", "/dev/nvme1n1"],
                       replication_factor=2)

# Write and read - objects are replicated across shards automatically
fs.pipe("hello.txt", b"Hello from sharded fsspec!")
data = fs.cat("hello.txt")

# List, copy, delete work as expected
for name in fs.ls("data/", detail=False):
    print(name)

fs.copy("hello.txt", "backup.txt")
fs.rm("hello.txt")
```

### pandas

```python
import pandas as pd

df = pd.read_parquet("shardedobjst:///data.parquet",
                     storage_options={
                         "shard_paths": ["/dev/nvme0n1", "/dev/nvme1n1"],
                         "replication_factor": 2,
                     })
```

### Pre-opened store

```python
import shardedobjstr
from shardedobjstr.fsspec_impl import ShardedFileSystem

store = shardedobjstr.open_cluster(
    ["/dev/nvme0n1", "/dev/nvme1n1"],
    replication_factor=2,
)
fs = ShardedFileSystem(store=store)
fs.pipe("key", b"value")
```

## Async API

`shardedobjstr.aio.AsyncClusterStore` wraps the synchronous `ClusterStore`
and offloads all blocking calls to the thread-pool executor via
`loop.run_in_executor()`. No Rust changes needed - pure Python async.

```python
import asyncio
import shardedobjstr
from shardedobjstr.aio import AsyncClusterStore

async def main():
    store = shardedobjstr.open_cluster(
        ["/tmp/shard0.raw", "/tmp/shard1.raw"],
        replication_factor=2,
    )
    astore = AsyncClusterStore(store)

    await astore.put("key", b"value")
    data = await astore.get("key")
    meta = await astore.head("key")
    await astore.delete("key")

    async for entry in astore.list():
        print(entry.location, entry.size)

asyncio.run(main())
```

All synchronous `ClusterStore` methods have async equivalents on
`AsyncClusterStore`. Tests: `tests/test_aio.py`.
