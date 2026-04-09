# API Reference

## Quick Start -- Rust API

### Basic ObjectStore Usage

Shards can be any `Arc<dyn ObjectStore>` -- RawObjectStore, LocalFileSystem,
an S3 client, or another ShardedObjectStore.

```rust
use std::sync::Arc;
use rawobjstr::store::RawObjectStore;
use shardedobjstr::ShardedObjectStore;
use object_store::{ObjectStore, path::Path, PutPayload};

// Shards can be any ObjectStore impl -- raw devices, local FS, S3, etc.
let shard0 = Arc::new(RawObjectStore::open("/dev/sdx".as_ref())?);
let shard1 = Arc::new(RawObjectStore::open("/dev/sdy".as_ref())?);
let shard2 = Arc::new(RawObjectStore::open("/dev/sdz".as_ref())?);

// Create a cluster with replication factor 2
let cluster = ShardedObjectStore::new(
    vec![shard0, shard1, shard2],
    2, // each object stored on 2 shards
);
// Default min_writes = max(rf - 1, 1) = 1
// Override: cluster.with_min_writes(2) to require both copies

// Use it like any ObjectStore
let path = Path::from("data/file.txt");
cluster.put(&path, PutPayload::from(b"hello".to_vec())).await?;
let result = cluster.get(&path).await?;
let bytes = result.bytes().await?;
assert_eq!(&bytes[..], b"hello");
```

### With Catalog Persistence

Three persistence strategies are available. Switch between them freely --
if the configured file does not exist (e.g. you changed from JSON to bincode),
`load_catalog()` auto-rebuilds from the shards and saves in the new format.

Both formats embed a CRC32c checksum inside the file for integrity
validation on load. JSON uses an envelope (`{"checksum": <u32>, "data": {...}}`),
bincode prefixes the payload with a 4-byte little-endian CRC. If the checksum
does not match, `load_catalog()` returns an error.

The catalog tracks a dirty flag that is set automatically on any mutation
(put, remove, clear, replace, add_replica). Use `save_catalog_if_dirty()` to
persist only when changes have been made.

```rust
use shardedobjstr::catalog::CatalogPersistence;

// Option 1: No persistence (default) -- always rebuild from shard indexes
let cluster = ShardedObjectStore::new(stores.clone(), 2);
// Call rebuild_catalog() manually when needed

// Option 2: JSON -- human-readable, good for debugging
let cluster = ShardedObjectStore::new(stores.clone(), 2)
    .with_persistence(CatalogPersistence::json("/var/lib/catalog.json"));
cluster.load_catalog()?;        // loads file, validates CRC; not async
// ... do work ...
cluster.save_catalog_if_dirty()?;  // persist only if mutations occurred

// Option 3: Bincode -- compact binary, fast for millions of objects
let cluster = ShardedObjectStore::new(stores.clone(), 2)
    .with_persistence(CatalogPersistence::bincode("/var/lib/catalog.bin"));
cluster.load_catalog()?;
// ... do work ...
cluster.save_catalog_if_dirty()?;

// Switching formats: just change the persistence and restart.
// If catalog.bin doesn't exist yet, load_catalog() returns an empty catalog.
// Call rebuild_catalog() to scan shards if needed.

// Dirty tracking:
cluster.catalog_is_dirty();       // true if any mutation since last save/load
cluster.save_catalog()?;           // always saves, clears dirty flag
cluster.save_catalog_if_dirty()?;  // saves only if dirty; returns Ok(true) if saved
```

### Using with LanceDB

Register the cluster as a custom object store provider so LanceDB reads and
writes its tables across the shards:

```rust
use std::sync::Arc;
use lancedb::connect;
use lance_io::object_store::{ObjectStoreRegistry, ObjectStoreParams};
use lance_io::object_store::providers::ObjectStoreProvider;

// Wrap the cluster in an ObjectStoreProvider
struct ClusterProvider { store: Arc<ShardedObjectStore> }

#[async_trait::async_trait]
impl ObjectStoreProvider for ClusterProvider {
    async fn new_store(
        &self, base_path: url::Url, _params: &ObjectStoreParams,
    ) -> lance_core::error::Result<lance_io::object_store::LanceObjectStore> {
        Ok(lance_io::object_store::LanceObjectStore::new(
            self.store.clone(), base_path.into(),
            None, None, false, false, 4, 3, None,
        ))
    }
}

// Register under "cluster://" scheme and connect
let registry = ObjectStoreRegistry::default();
registry.register("cluster", Arc::new(ClusterProvider { store: cluster.clone() }));
let db = connect("cluster://data")
    .object_store_registry(Arc::new(registry))
    .execute().await?;

// Now use LanceDB normally -- all I/O goes through the cluster
let table = db.create_table("test", initial_batch).execute().await?;
let rows = table.count_rows(None).await?;
```

See [examples/lance_on_cluster.rs](examples/lance_on_cluster.rs) for the
complete working example.

---

## ShardedObjectStore

The main type. Distributes objects across multiple shards (raw block devices or any `ObjectStore` backend) with configurable replication. Implements the [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) trait from `object_store` 0.12.

### Construction

| Method | Description |
|--------|-------------|
| `ShardedObjectStore::new(stores, replication_factor)` | Create a cluster with all shards marked Healthy |
| `ShardedObjectStore::new_with_offline(stores, replication_factor)` | Create a cluster where some shards may be `None` (degraded mode) |

### Builder Methods

| Method | Description |
|--------|-------------|
| `.with_persistence(p)` | Set catalog persistence (JSON, Bincode, or None) |
| `.with_read_only(read_only)` | Enable or disable read-only mode |
| `.with_multipart_expiry(expiry)` | Set the expiry duration for stale multipart uploads |
| `.with_min_writes(n)` | Set minimum successful shard writes before put returns Ok. Clamped to `[1, rf]` |
| `.with_delete_requires_min_writes(enabled)` | Require `min_writes` quorum for deletes (default: false) |

### Accessors

| Method | Description |
|--------|-------------|
| `shard_count()` | Number of shards in the cluster |
| `replication_factor()` | Configured replication factor |
| `shard_store(id)` | Get the underlying `ObjectStore` for a shard |
| `is_read_only()` | Whether the cluster is in read-only mode |
| `multipart_expiry()` | Current multipart upload expiry duration |
| `multipart_upload_count()` | Number of in-flight multipart uploads |
| `catalog()` | Reference to the in-memory catalog |
| `persistence()` | Current catalog persistence configuration (clone) |
| `min_writes()` | Minimum successful shard writes required for a put to succeed |
| `delete_requires_min_writes()` | Whether deletes require `min_writes` quorum |
| `read_repair_count()` | Number of background read-repairs triggered so far |
| `read_repair_success()` | Number of read-repair tasks that succeeded |
| `read_repair_failed()` | Number of read-repair tasks that failed |
| `shard_crc_error_count(id)` | Cumulative CRC error count for a shard |

### Catalog and Placement

| Method | Description |
|--------|-------------|
| `placement(path)` | Look up the `PlacementEntry` for an object |
| `target_shards(path)` | Compute which shards an object should be placed on |
| `read_shard_order(path)` | Return shard IDs in the order they will be tried for reads |
| `rebuild_catalog()` | Scan all shards and rebuild the entire catalog. Returns entry count |
| `rebuild_catalog_for_shard(shard_id)` | Rebuild catalog entries for a single shard. Returns entry count |
| `save_catalog()` | Persist the catalog to disk using the configured persistence. Clears the dirty flag |
| `save_catalog_if_dirty()` | Save only if the catalog has been mutated since the last save/load. Returns `Ok(true)` if saved, `Ok(false)` if clean |
| `load_catalog()` | Load the catalog from disk (validates embedded CRC checksum) |
| `catalog_is_dirty()` | Whether the catalog has unsaved mutations |

### Shard Health Management

| Method | Description |
|--------|-------------|
| `shard_health(id)` | Get the health status of a shard |
| `set_shard_health(id, health)` | Set the health status. Returns the previous health |
| `shard_offline_since(id)` | When a shard went offline (`None` if healthy) |
| `shard_free_space(id)` | Free space reported by a shard |
| `set_shard_free_space(id, free)` | Update the cached free space for a shard |
| `shard_crc_error_count(id)` | Number of CRC errors detected on reads from this shard |
| `validate_shard_access()` | Probe all shards and return `(shard_id, reachable)` pairs |
| `invalidate_shard(shard_id)` | Purge catalog entries for a shard, rescan, and report |

### Shard Attachment / Detachment

| Method | Description |
|--------|-------------|
| `attach_shard(shard_id, store, force)` | Attach (or reattach) a shard. `force=true` skips invalidation. Returns entry count |
| `detach_shard(shard_id)` | Detach a shard, marking it Offline. Returns the previous health |
| `hold_offline(shard_id, suppress_replication, reason)` | Detach and mark Detached (recovery loop will not auto-reattach). Optional suppress_replication prevents re-replication sweep from copying objects off this shard. Returns previous health |
| `release_hold(shard_id)` | Clear the Detached hold, transitioning to Offline (eligible for manual attach). Returns true if shard was held |
| `shard_detach_reason(id)` | Returns the `DetachReason` for a detached/held shard, or `None` |
| `set_detach_reason(id, reason)` | Override the detach reason for a shard |
| `shard_suppress_replication(id)` | Returns `true` if re-replication sweeps should skip this shard |

### Read Preference

| Method | Description |
|--------|-------------|
| `read_preference()` | Current read preference |
| `set_read_preference(pref)` | Set the read preference (`RoundRobin` or `Ordered`) |

### Event Bus and Raw References

| Method | Description |
|--------|-------------|
| `set_event_bus(bus)` | Wire an `EventBus` for store-level notifications |
| `set_raw_refs(registry)` | Attach a `RawRefRegistry` for metadata-aware operations (read-repair, copy, replication, rebuild-catalog) |
| `raw_refs()` | Get a clone of the attached `RawRefRegistry`, if any |

### Replication and Placement Analysis

| Method | Description |
|--------|-------------|
| `shard_object_counts()` | Returns `Vec<(ShardId, usize)>` with catalog entry count per shard |
| `find_under_replicated()` | Return `(key, replica_count)` for objects with fewer replicas than the replication factor |
| `find_over_replicated()` | Return `(key, replica_count)` for objects with more replicas than the replication factor |
| `find_replication_target(key)` | Pick a healthy shard that should hold a new replica of this object |
| `pick_excess_shard(key)` | Pick a shard that holds a redundant replica suitable for removal |
| `replicate_object(key, from_shard, to_shard, raw_refs)` | Copy an object between shards, preserving metadata when `raw_refs` is `Some`. Returns bytes copied |
| `remove_replica(key, shard_id)` | Delete a single replica from a specific shard |

### Verification

| Method | Description |
|--------|-------------|
| `verify_object(key)` | CRC-verify all replicas of a single object across shards |
| `verify_all(prefix)` | CRC-verify all objects (optionally filtered by prefix) |
| `cross_verify_object(key)` | MD5-verify all replicas of a single object. Returns `CrossVerifyReport` |
| `cross_verify_all(prefix, progress)` | MD5-verify all objects (optionally filtered by prefix). Returns `CrossVerifyAllReport` |

### Delete Markers

| Method | Description |
|--------|-------------|
| `list_delete_markers()` | Return `(key, deleted_at)` for all delete markers |
| `vacuum_delete_markers(progress)` | Remove stale delete markers. Returns `(purged, stale_objects_cleaned)` |
| `get_delete_marker(key)` | Get the deletion timestamp of a specific key |
| `is_delete_marker(key)` | Check if a key string is a delete marker path (static) |

### Multipart Upload Management

| Method | Description |
|--------|-------------|
| `list_multipart_uploads()` | List in-flight uploads: `(upload_id, key, created_epoch, shard_id)` |
| `purge_stale_multiparts()` | Abort uploads older than `multipart_expiry()`. Returns count purged |

### ObjectStore Trait Methods

All standard `ObjectStore` methods are implemented:

| Method | Description |
|--------|-------------|
| `put(location, payload)` | Write an object, replicating to target shards |
| `put_opts(location, payload, opts)` | Write with options (`Create`, `Overwrite`, `Update`) |
| `put_multipart(location)` | Start a multipart upload |
| `put_multipart_opts(location, opts)` | Start a multipart upload with options |
| `get(location)` | Read an object (tries replicas in read-preference order) |
| `get_opts(location, options)` | Read with byte-range support |
| `get_range(location, range)` | Read a byte range |
| `head(location)` | Get object metadata from the catalog |
| `delete(location)` | Delete an object from all shards (writes a delete marker first) |
| `list(prefix)` | List objects matching prefix (merged and deduplicated across shards) |
| `list_with_delimiter(prefix)` | List with directory-like grouping |
| `copy(from, to)` | Copy an object within the cluster. Preserves metadata when `raw_refs` is configured AND the source object has non-empty metadata (reads via `get_metadata()`, writes via `put_with_meta()`). Falls back to plain get+put (no metadata) otherwise |
| `copy_if_not_exists(from, to)` | Atomic copy-if-absent. Same metadata behavior as `copy()` |
| `rename_if_not_exists(from, to)` | Rename by copy-then-delete (not atomic; see doc comment) |

---

## Repair and Repair-Replication

Functions in the `repair` module for cluster maintenance.

### Health Probing

| Function | Description |
|----------|-------------|
| `probe_store(store, timeout)` | Check if a shard is reachable within a timeout. Returns `true` if healthy |

### Sync and Reattach

| Function | Description |
|----------|-------------|
| `sync_and_reattach(cluster, original_stores, shard_id, raw_refs)` | Probe, sync, and reattach a previously-offline shard. `raw_refs` enables metadata-aware copy |
| `mirror_sync(cluster, original_stores, target_shard, raw_refs)` | Full sync: copy missing objects to the target shard and delete stale ones. Returns `MirrorSyncReport` |
| `partitioned_sync(cluster, original_stores, target_shard, raw_refs)` | Sync only objects that belong to the target shard per the hash ring. Returns count |

### Repair-Replication Operations

| Function | Description |
|----------|-------------|
| `re_replication_sweep(cluster, batch_size, raw_refs)` | Copy under-replicated objects to new shards. Shards with `suppress_replication=true` are skipped. Returns count |
| `over_replication_trim(cluster, batch_size)` | Remove excess replicas from over-replicated objects. Returns count |
| `repair_replication_sweep(cluster, batch_size, raw_refs, progress)` | Combined re-replication + trim in one pass. Returns `RepairReplicationResult` |
| `plan_repair_replication(cluster, batch_size)` | Dry-run: compute what `repair_replication_sweep` would do without any I/O (synchronous). Returns `RepairReplicationPlan` |
| `drain_shard(cluster, survivor_cluster, victim_shard_id, victim_store, raw_refs, progress)` | Move all objects off a shard before removal. When `raw_refs` is `Some`, metadata is preserved. Returns `DrainReport` |
| `redistribute_sweep(cluster, batch_size, tolerance_pct, raw_refs, progress)` | Move objects from fuller shards to emptier ones to balance object counts. Stops when imbalance drops to `tolerance_pct` or `batch_size` moves are done. Returns `RedistributeResult` |

---

## Catalog

In-memory placement map that tracks which shards hold each object.

### PlacementEntry

| Field | Type | Description |
|-------|------|-------------|
| `shards` | `Vec<ShardId>` | Shard IDs holding replicas |
| `size` | `u64` | Object size in bytes |
| `crc32c` | `Option<u32>` | CRC32c of the payload (if known) |
| `updated` | `DateTime<Utc>` | Last modification timestamp |
| `meta_len` | `u16` | Length of TLV metadata suffix (0 = no metadata) |

### Catalog Methods

| Method | Description |
|--------|-------------|
| `new()` | Create an empty catalog |
| `put(key, shards, size, crc32c, meta_len)` | Insert or replace an entry. `meta_len` is the TLV metadata suffix length (0 = none) |
| `add_replica(key, shard_id, size, meta_len)` | Add a shard to an existing entry. Pass `meta_len=0` to preserve existing value |
| `get(key)` | Look up a placement entry |
| `try_insert(key, shards, size)` | Insert only if the key does not already exist. Returns `true` on insert |
| `remove(key)` | Remove and return an entry |
| `clear()` | Remove all entries |
| `replace(new_map)` | Replace the entire catalog |
| `remove_shard(key, shard_id)` | Remove a shard from one entry |
| `remove_all_for_shard(shard_id)` | Remove a shard from every entry. Returns count of entries modified |
| `entries_for_shard(shard_id)` | Return all entries on a given shard |
| `len()` | Number of entries |
| `is_empty()` | Whether the catalog is empty |
| `all_entries()` | Return all `(key, PlacementEntry)` pairs |
| `with_entries(f)` | Call `f` with a read-locked reference to the inner map (avoids cloning) |
| `load_into(other)` | Replace the catalog contents with entries from `other`. Clears dirty flag |
| `is_dirty()` | Whether the catalog has been mutated since last save/load |
| `clear_dirty()` | Manually clear the dirty flag |
| `list(prefix)` | Return `ObjectMeta` for all objects matching a prefix |
| `to_json()` / `from_json(data)` | Serialize/deserialize as JSON |
| `to_bincode()` / `from_bincode(data)` | Serialize/deserialize as bincode |
| `save_to_file(path)` / `load_from_file(path)` | Persist/load from a file path |

### CatalogPersistence

| Variant | Description |
|---------|-------------|
| `None` | In-memory only (no disk persistence) |
| `Json { path }` | Human-readable JSON file. CRC32c checksum embedded in a JSON envelope |
| `Bincode { path }` | Compact binary format. 4-byte LE CRC32c prefix before the bincode payload |

| Method | Description |
|--------|-------------|
| `json(path)` | Create a JSON persistence config |
| `bincode(path)` | Create a bincode persistence config |
| `path()` | Return the file path (if any) |
| `is_none()` | Whether persistence is disabled |
| `save(catalog)` | Persist the catalog |
| `load()` | Load the catalog from disk |

---

## Replication

Hash-based object placement using the Jump Consistent Hash algorithm (Lamping & Veach).

| Function / Method | Description |
|-------------------|-------------|
| `jump_consistent_hash(key, num_buckets)` | Map a 64-bit key to a bucket. Minimal key migration when `num_buckets` changes |
| `ReplicationPolicy::new(replication_factor, shard_count)` | Create a placement policy |
| `policy.factor()` | Return the replication factor |

---

## Config

Config file parsing for cluster configuration. Both flat and tree formats
are supported. See [CONFIG.md](../CONFIG.md) for the full file format reference.

### ShardConf (enum)

| Variant | Fields | Description |
|---------|--------|-------------|
| `Raw` | `path`, `read_only`, `compression`, `direct_io`, `size_mb` | Block device or loopback image |
| `Fs` | `root`, `read_only` | Local directory |
| `S3` | `endpoint`, `bucket`, `region`, `access_key`, `secret_key`, `path_style` | S3-compatible endpoint |
| `Mem` | (none) | In-memory store (testing only) |
| `Node` | `String` (child name) | Reference to a child node in a tree config |

### ClusterConf (flat config)

| Field | Type | Description |
|-------|------|-------------|
| `replicas` | `usize` | Replication factor |
| `catalog` | `Option<String>` | Catalog file path |
| `read_prefer` | `Option<String>` | Read preference (`"round-robin"` or `"ordered"`) |
| `direct_io` | `bool` | Default O_DIRECT for all shards |
| `compression` | `Option<String>` | Default compression for all shards |
| `size_mb` | `Option<u64>` | Default shard size in MB |
| `read_only` | `bool` | Whether the cluster is read-only |
| `delete_requires_min_writes` | `bool` | When true, deletes require `min_writes` quorum (default: false) |
| `min_writes` | `Option<usize>` | Minimum successful writes for puts. `None` = default `max(replicas - 1, 1)` |
| `shards` | `Vec<ShardConf>` | List of shard configurations |

### TreeNode

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | Node name |
| `replication_factor` | `usize` | Replication factor for this node |
| `min_writes` | `Option<usize>` | Minimum successful writes for puts. `None` = default `max(rf - 1, 1)` |
| `shards` | `Vec<ShardConf>` | Shards (including `Node` references to children) |
| `children` | `Vec<TreeNode>` | Child node definitions |

| Method | Description |
|--------|-------------|
| `find_node(name)` | Recursively find a child node by name |

### TreeConf (tree config)

| Field | Type | Description |
|-------|------|-------------|
| `cluster_name` | `String` | Cluster name from `cluster` directive |
| `root` | `TreeNode` | Root node of the tree |
| `catalog` | `Option<String>` | Catalog persistence |
| `read_prefer` | `Option<String>` | Read preference |
| `compression` | `Option<String>` | Default compression |
| `direct_io` | `bool` | Default O_DIRECT |
| `size_mb` | `Option<u64>` | Default size in MB |
| `read_only` | `bool` | Read-only mode |

| Method | Description |
|--------|-------------|
| `find_node(name)` | Find a node anywhere in the tree by name |

### Functions

| Function | Description |
|----------|-------------|
| `parse_cluster_conf(text)` | Parse flat config text into `ClusterConf` |
| `load_cluster_conf(path)` | Read file and parse as flat config |
| `validate_cluster_conf(conf)` | Return a list of `ClusterDiag` warnings/errors |
| `parse_tree_conf(text)` | Parse tree config text into `TreeConf` |
| `load_tree_conf(path)` | Read file and parse as tree config |
| `is_tree_config(text)` | Returns `true` if the text is a tree config |
| `load_auto_conf(path)` | Auto-detect format and return `TreeConf` (flat configs wrapped in single-node tree) |

### ClusterDiag

| Field | Type | Description |
|-------|------|-------------|
| `level` | `ClusterDiagLevel` | `Error`, `Warning`, or `Info` |
| `message` | `String` | Diagnostic message |

---

## Metadata

Raw metadata routing for shards with different storage backends.

### ShardKind

| Variant | Description |
|---------|-------------|
| `Raw` | Block device shard (metadata stored in extent header) |
| `S3Like` | S3/R2/child node (native attributes) |
| `Sidecar` | Local filesystem (sidecar file) |

### RawRefRegistry

| Method | Description |
|--------|-------------|
| `new(refs, kinds)` | Create a registry from per-shard raw references and kinds |
| `single(raw)` | Create a registry for a single raw store |
| `get(shard_id)` | Get the raw store reference for a shard |
| `kind(shard_id)` | Get the shard kind |
| `all_raw()` | Return all raw store references |
| `first_raw()` | Return the first raw store reference |
| `shard_count()` | Number of shards |

### Metadata-Aware I/O

| Function | Description |
|----------|-------------|
| `put_with_meta(cluster, raw_refs, location, payload, metadata)` | Write body + opaque metadata to all target shards. On `InsufficientWrites` for new keys, cleans up partial writes including sidecar `.__meta__` files |
| `head_with_meta(cluster, raw_refs, location)` | Return `(ObjectMeta, meta_len)` for an object |
| `get_metadata(cluster, raw_refs, location)` | Read only the metadata suffix |
| `list_with_meta(cluster, raw_refs, prefix)` | List objects with their `meta_len` values |
| `set_meta_len(cluster, raw_refs, location, meta_len)` | Update the `meta_len` in the index for an existing object |
| `delete_sidecar(cluster, raw_refs, location)` | Delete the sidecar metadata companion file for an object |
| `put_with_meta_from_file(cluster, raw_refs, location, file, meta_len)` | Write body + metadata from a file |

### Attribute Conversion

| Function | Description |
|----------|-------------|
| `meta_to_attributes(meta)` | Convert a `HashMap<String, String>` to `object_store::Attributes`. Logs `tracing::debug!` for unrecognized keys that cannot be mapped to S3 attributes |
| `attributes_to_meta(attrs)` | Convert `object_store::Attributes` to a `HashMap<String, String>` |

---

## TLV Metadata Encoding

Variable-length Tag-Length-Value encoding for compact metadata storage.

| Function | Description |
|----------|-------------|
| `field_name_to_tag(name)` | Map a metadata field name to its 1-byte tag |
| `tag_to_field_name(tag)` | Map a tag byte back to the field name |
| `value_lookup(tag, value)` | Look up a compact 1-byte encoding for a known value |
| `value_from_lookup(tag, idx)` | Reverse a compact value encoding |
| `encode_metadata(meta)` | Encode a `HashMap<String, String>` to TLV bytes |
| `decode_metadata(data)` | Decode TLV bytes back to a `HashMap<String, String>` |

### MetadataTooLarge

| Field | Type | Description |
|-------|------|-------------|
| `field` | `String` | Field name that exceeded the limit |
| `len` | `usize` | Actual length |
| `max` | `usize` | Maximum allowed (65535) |

---

## Event Module

Unix domain socket event delivery for real-time store notifications.

| Function | Description |
|----------|-------------|
| `setup_event_socket(cluster, raw_stores, socket_path, secret, max_readers)` | Create an event bus and server. Returns `(EventBus, EventServer)` |
| `subscribe_store_events(socket_path, secret, event_fn)` | Subscribe to store events from a running server. Returns a `JoinHandle` |
| `subscribe_streaming_replica(event_source, secret, cluster)` | Start a streaming replica subscriber that keeps the cluster catalog in sync via PUT/DELETE events. Requires mirror mode (`rf == shard_count`) or single-shard (`rf == 1`). Returns a `JoinHandle` |

`setup_event_socket` and `subscribe_store_events` are `#[cfg(unix)]` only.
`subscribe_streaming_replica` works on all platforms (accepts Unix socket
paths or `tcp:host:port` via `subscribe_events_auto`).

### ShardInfo

Internal per-shard metadata tracked by `ShardedObjectStore`.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `ShardId` | Shard index (0-based) |
| `store` | `Arc<dyn ObjectStore>` | Backing store (uses `OfflinePlaceholderStore` when offline) |
| `health` | `ShardHealth` | Current health status |
| `free_space` | `u64` | Cached free space estimate in bytes |
| `offline_since` | `Option<DateTime<Utc>>` | When the shard went offline (None if healthy) |
| `crc_error_count` | `AtomicU64` | Cumulative CRC errors detected on reads |
| `detach_reason` | `Option<DetachReason>` | Why the shard was detached (Manual, ProbeFailure, DeviceMissing, Drain) |
| `suppress_replication` | `bool` | When true, re-replication sweeps skip this shard |

---

## Error Types

### ShardError

```rust
pub enum ShardError {
    NoShards,                                    // Cluster has no shards
    NotFound(String),                            // Object not found
    AllReplicasFailed { path, errors },           // All replicas failed
    InsufficientWrites { path, required, actual, errors }, // Fewer than min_writes succeeded
    ObjectStore(object_store::Error),            // Underlying ObjectStore error
    Io(std::io::Error),                          // I/O error
    ReadOnly,                                    // Cluster is in read-only mode
    VacuumOfflineShard { shard_id: usize },      // Cannot vacuum with offline shards
    VacuumAlreadyRunning,                        // Concurrent vacuum rejected
}
```

---

## Report Types

### InvalidateReport

Returned by `invalidate_shard()`.

| Field | Type | Description |
|-------|------|-------------|
| `shard_id` | `ShardId` | The shard that was invalidated |
| `entries_purged` | `usize` | Catalog entries removed |
| `entries_restored` | `usize` | Entries restored by rescanning the shard |
| `missing_keys` | `Vec<String>` | Keys that were in the catalog but not found on disk |
| `scan_ok` | `bool` | Whether the rescan completed without errors |

### VerifyObjectReport

Returned by `verify_object()`.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | Object key |
| `catalog_crc` | `Option<u32>` | CRC32c from the catalog |
| `replicas` | `Vec<VerifyReplicaResult>` | Per-replica verification results |
| `replicas_consistent` | `bool` | Whether all replicas agree |

### VerifyReplicaResult

| Field | Type | Description |
|-------|------|-------------|
| `shard_id` | `ShardId` | Shard holding this replica |
| `crc32c` | `Option<u32>` | Computed CRC32c |
| `size` | `Option<u64>` | Replica size in bytes |
| `matches_catalog` | `Option<bool>` | Whether the CRC matches the catalog |
| `error` | `Option<String>` | Error message if the replica could not be read |

### VerifyReport

Returned by `verify_all()`.

| Field | Type | Description |
|-------|------|-------------|
| `objects_checked` | `usize` | Total objects verified |
| `objects_ok` | `usize` | Objects that passed |
| `objects_mismatched` | `usize` | Objects with CRC mismatches |
| `objects_with_errors` | `usize` | Objects with read errors |
| `details` | `Vec<VerifyObjectReport>` | Details for mismatched/errored objects only |

### MirrorSyncReport

Returned by `mirror_sync()`.

| Field | Type | Description |
|-------|------|-------------|
| `copied` | `usize` | Objects copied to the target shard |
| `deleted` | `usize` | Stale objects removed from the target shard |
| `bytes_copied` | `u64` | Total bytes copied |
| `updated` | `usize` | Catalog entries updated |

### RepairReplicationResult

Returned by `repair_replication_sweep()`.

| Field | Type | Description |
|-------|------|-------------|
| `re_replicated` | `usize` | Objects that gained a new replica |
| `trimmed` | `usize` | Excess replicas removed |
| `under_remaining` | `usize` | Still-under-replicated objects |
| `over_remaining` | `usize` | Still-over-replicated objects |

### RepairReplicationPlan

Returned by `plan_repair_replication()` (dry-run).

| Field | Type | Description |
|-------|------|-------------|
| `replications` | `Vec<PlannedAction>` | Planned copy operations |
| `trims` | `Vec<PlannedAction>` | Planned delete operations |
| `unrepairable` | `usize` | Objects that cannot be repaired (no healthy source) |
| `untrimmable` | `usize` | Objects that cannot be trimmed |

### DrainReport

Returned by `drain_shard()`.

| Field | Type | Description |
|-------|------|-------------|
| `moved` | `usize` | Objects successfully moved off the shard |
| `skipped` | `usize` | Objects skipped (already replicated elsewhere) |
| `errors` | `usize` | Objects that failed to move |
| `deleted` | `usize` | Objects deleted from the victim shard after drain |
| `delete_errors` | `usize` | Objects that failed to delete from the victim shard |
| `re_replicated` | `usize` | Objects re-replicated to restore replication factor |
| `under_remaining` | `usize` | Objects still under-replicated after the sweep |

### RedistributeResult

Returned by `redistribute_sweep()`.

| Field | Type | Description |
|-------|------|-------------|
| `moved` | `usize` | Objects successfully moved from fuller to emptier shards |
| `skipped` | `usize` | Objects skipped (no valid target or already balanced) |
| `errors` | `usize` | Errors encountered during move |
| `shard_counts` | `Vec<(ShardId, usize)>` | Per-shard object counts after the sweep |

### ProgressSink

```rust
pub type ProgressSink = mpsc::UnboundedSender<String>;
```

Optional channel for streaming per-object progress messages to a caller (e.g. the daemon's SSE endpoint). Accepted by `redistribute_sweep`, `drain_shard`, and other long-running repair functions.

---

## Enums

### ShardHealth

```rust
pub enum ShardHealth {
    Healthy,   // Shard is available for reads and writes
    Degraded,  // Shard is reachable but reporting errors
    Offline,   // Shard is not reachable (recovery loop will probe and auto-reattach)
    Syncing,   // Shard is being re-synced after coming back online
    Detached,  // Manually taken offline; recovery loop will NOT auto-reattach
}

impl ShardHealth {
    /// Returns true for Healthy or Degraded.
    pub fn is_writable(&self) -> bool;
    /// Returns true for Offline or Detached.
    pub fn is_unavailable(&self) -> bool;
}
```

### DetachReason

```rust
pub enum DetachReason {
    Manual,        // Operator used take-offline endpoint or hold_offline()
    ProbeFailure,  // Recovery loop detected repeated probe failures
    DeviceMissing, // Device path not found at startup
    Drain,         // Shard was drained via /_admin/drain
}
```

### ReadPreference

```rust
pub enum ReadPreference {
    RoundRobin,  // Rotate across replicas for load-balancing (default)
    Ordered,     // Try shards in config order; first healthy wins
}
```

---

## Constants

| Constant | Description |
|----------|-------------|
| `BUILD_GIT_HASH` | Git commit hash baked at compile time |
| `BUILD_DATE` | UTC build timestamp |
| `VERSION` | Crate version from Cargo.toml |
| `DELETE_MARKER_PREFIX` | `"__deleted__/"` -- reserved prefix for internal delete markers |

---

## CLI Binaries

### shardedobjstr

Command-line tool for managing a sharded cluster. Supports both flat and
tree config files (auto-detected). Use `--node <name>` with tree configs
to target a specific node. See [CLI.md](CLI.md) for the full command
reference and [CONFIG.md](../CONFIG.md) for config file format.

### shardedobjstr-catview

Dump a persisted catalog file in human-readable form.

```
shardedobjstr-catview [--format json|bin] <catalog-file>
```

---

## Python Bindings (`shardedobjstr`)

Install: `pip install shardedobjstr` (or build with `maturin develop --release`).

See [python/README.md](python/README.md) for the full Python API reference,
including:

- Module-level constructors (`format_shard`, `open_cluster`, `open_cluster_degraded`,
  `format_and_open_cluster`, `load_config`, `check_config`, `open_cluster_from_config`)
- `ClusterStore` sync API (object I/O, cluster management, verification, metadata-aware
  operations, delete markers, multipart uploads, catalog persistence)
- `AsyncClusterStore` async API (`shardedobjstr.aio`)
- `ShardedFileSystem` fsspec integration (`shardedobjst://` protocol)
- All data classes (`ObjectMeta`, `ListResult`, `PlacementInfo`, `ShardHealth`,
  `InvalidateReport`, `RepairReplicationResult`, `VerifyReport`, `RepairReplicationPlan`,
  `CrossVerifyReport`, etc.)

---

## Behavioral Notes

### Object Placement

Objects are placed using Jump Consistent Hash (Lamping & Veach). This provides minimal key migration when shards are added or removed (~1/N keys migrate instead of ~(N-1)/N). The replication factor determines how many shards each object is written to.

### Health-Aware Writes

When writing, the cluster skips `Offline` and `Syncing` shards and backfills from the next positions on the hash ring. This means writes succeed as long as at least one shard is healthy.

### Read Fallback

Reads try replicas in the configured order (`RoundRobin` or `Ordered`). If a replica fails, the next one is tried. A `ShardError::AllReplicasFailed` is returned only when every replica is unreachable or corrupt.

When a read succeeds on a shard that the catalog did not know about (fallback mode), the catalog is updated with the discovered replica. For Raw shards with `RawRefRegistry` attached, the synchronous `head_with_meta` index lookup recovers the correct `meta_len` so that subsequent metadata operations work without requiring a full `rebuild_catalog`.

### CRC Auto-Repair

When a read detects a CRC mismatch on a replica, the corrupt replica is removed from the catalog and the `crc_error_count` for that shard is incremented. The next `repair_replication_sweep` will restore the missing replica from a healthy source.

## Delete Markers

In a replicated cluster a simple `delete()` on one shard can be undone by a
recovery sync that copies the object back from another shard. Delete markers
prevent this resurrection.

### How it works

1. `delete("foo")` first attempts to write a marker object at `__deleted__/foo`
   to all healthy shards (best-effort -- succeeds if at least one shard accepts
   the marker). The body is the deletion timestamp (RFC 3339).
2. The real object is then removed from all shards.
3. If the real delete fails, the marker is retained (not rolled back) so that
   recovery sync can still prevent resurrection. Stale markers are cleaned by
   vacuum or by a subsequent PUT.

Markers are invisible to callers -- `list()`, `head()`, and `get()` filter
them out. The S3 adapter in `objstrd` rejects client PUTs to the `__deleted__/`
prefix with AccessDenied.

**Re-PUT after delete:** When an object is re-PUT after deletion, the stale
delete marker is automatically removed. No vacuum needed for this case.

### Recovery sync

When `partitioned_sync()` reattaches an offline shard it replays markers:
if a stale copy of the object exists on the recovering shard with
`last_modified <= marker timestamp`, it is deleted. Objects re-PUT after the
delete (newer timestamp) are kept. Stale markers on the returning shard
(e.g. from a delete that was later followed by a re-PUT while the shard
was offline) are also cleaned up during sync.

### Vacuum

`vacuum_delete_markers()` purges stale markers once **all** shards are
healthy. For each marker:

| Condition | Action |
|-----------|--------|
| No live object exists | Marker fully applied -- purge it |
| Live object is newer than marker | Object was re-PUT after delete -- purge the stale marker |
| Live object is older than marker | Missed delete -- delete the stale object, then purge the marker |

Vacuum refuses to run if any shard is offline (the marker may still be needed
when that shard comes back).


### Catalog Persistence

The catalog can be persisted as JSON (human-readable) or bincode (compact).
Writes are atomic (write to temp file, then rename). Both formats embed a
CRC32c checksum inside the file:

- **JSON**: `{"checksum": <crc32c_of_data_json>, "data": {<entries>}}`
- **Bincode**: `[4-byte LE CRC32c][bincode payload]`

On load, the checksum is validated. Corrupted or tampered files produce an
error. Legacy JSON files (plain map without envelope) are still accepted
for backward compatibility (no checksum validation).

The catalog tracks a dirty flag set by any mutation (put, remove, clear,
replace, add_replica). Use `save_catalog_if_dirty()` in periodic flush
tasks or shutdown hooks to avoid unnecessary I/O. The `objstrd` daemon
wires this up automatically: load on startup, periodic flush if configured,
and save-if-dirty on shutdown.

### Multipart Uploads

Multipart uploads are assigned to a single shard. On `complete()`, the assembled object is replicated to all target shards. Stale uploads (older than `multipart_expiry()`) are cleaned up by `purge_stale_multiparts()`.

#### Known Multipart Limitations

1. **Tracking leak on InsufficientWrites**: If multipart `complete()` triggers
   replication but fewer than `min_writes` shards accept the data, the tracking
   entry is removed even though partial data may remain on the primary shard.
   A subsequent `purge_stale_multiparts()` will not find it. The orphaned data
   persists until the next `rebuild_catalog` + `vacuum`.

2. **Orphaned data on primary after incomplete upload**: If a multipart upload
   is never completed or aborted, the parts remain on the primary shard with
   no catalog entry. `purge_stale_multiparts()` cleans up tracked uploads only;
   it cannot find untracked part data.

3. **Metadata not preserved on replicas during multipart complete**: The
   `complete()` path assembles parts on the primary shard, then replicates
   via plain `put()`. S3 attributes or sidecar metadata from the original
   `put_multipart_opts()` call are not forwarded to replicas.

---

## Nested Topology Example -- Striped NVMe Mirrored to S3

Build a nested cluster in Rust: 3 NVMe drives striped for capacity,
mirrored to S3 for durability. The inner cluster spreads objects across
drives (rf=1), the outer cluster writes to both the NVMe pool and S3 (rf=2).

```rust
use std::sync::Arc;
use object_store::ObjectStore;
use rawobjstr::store::RawObjectStore;
use shardedobjstr::{ShardedObjectStore, ReadPreference};

// Inner: stripe across 3 NVMe drives (rf=1 -- capacity, not redundancy)
let nvme0 = Arc::new(RawObjectStore::open("/dev/nvme0n1")?);
let nvme1 = Arc::new(RawObjectStore::open("/dev/nvme1n1")?);
let nvme2 = Arc::new(RawObjectStore::open("/dev/nvme2n1")?);
let nvme_pool = Arc::new(ShardedObjectStore::new(
    vec![nvme0, nvme1, nvme2], 1,  // rf=1: spread objects, no duplication
));

// S3 backup
let s3 = Arc::new(AmazonS3Builder::new()
    .with_endpoint("https://s3.us-west-2.amazonaws.com")
    .with_bucket_name("nvme-mirror")
    .with_region("us-west-2")
    .build()?);

// Outer: mirror between the NVMe pool and S3 (rf=2: both get every object)
let cluster = ShardedObjectStore::new(vec![nvme_pool, s3], 2);
// Prefer NVMe pool (shard 0) for reads; fall back to S3 only if NVMe is unavailable
cluster.set_read_preference(ReadPreference::Ordered);

// Use as a single ObjectStore -- 6TB local capacity, fully backed up to S3
cluster.put(&Path::from("dataset/chunk_001.parquet"), data.into()).await?;
```

Because `ShardedObjectStore` implements the `ObjectStore` trait, the inner
NVMe pool is just another shard to the outer mirror -- the nesting is
invisible to callers. See [CONFIG.md](../CONFIG.md) for the tree config file
format that expresses this topology declaratively.
