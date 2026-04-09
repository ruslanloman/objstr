# DISTRIBUTED-OPERATIONS.md

Consolidated reference for all distributed and cluster behavior: crash
recovery, replication, index invalidation, buddy lists, lights-out
operation, self-healing, and manual operations. This document covers
both the current local multi-shard mode and the planned coordinator
cluster mode.

For the implementation roadmap and phase numbering, see `OVERALL-PLAN.md`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Object Placement and Replication](#2-object-placement-and-replication)
3. [Index and Catalog Management](#3-index-and-catalog-management)
4. [Delete Markers](#4-delete-markers)
5. [Crash Recovery](#5-crash-recovery)
6. [Corruption Detection and Handling](#6-corruption-detection-and-handling)
7. [Shard Index Invalidation Protocol](#7-shard-index-invalidation-protocol)
8. [Repair and Sync Operations](#8-repair-and-sync-operations)
9. [Buddy Lists and Lights-Out Operation](#9-buddy-lists-and-lights-out-operation)
10. [Self-Healing Pipeline](#10-self-healing-pipeline)
11. [Manual Operations](#11-manual-operations)
11b. [Degraded Startup, Detach, and Reattach](#11b-degraded-startup-detach-and-reattach)
11c. [Object Redistribution](#11c-object-redistribution)
12. [Monitoring and Observability](#12-monitoring-and-observability)

---

## 1. Architecture Overview

The system supports three operational modes:

| Mode | Description | Status |
|------|-------------|--------|
| Standalone | Single objstrd process, single raw device or other backend | Implemented |
| Local multi-shard | Single objstrd process, multiple raw devices as shards with replication | Implemented (in-process) |
| Coordinator cluster | Multiple objstrd nodes, central coordinator for placement and orchestration | Planned (Phases B-N) |

### Components

| Component | Crate | Role |
|-----------|-------|------|
| `RawObjectStore` | `rawobjstr` | Writes directly to raw block device. ACID-like crash safety (WAL, double-buffered superblock). CRC32c on every extent. |
| `ShardedObjectStore` | `shardedobjstr` | ObjectStore trait implementation that routes objects across N shards with replication factor RF. Manages catalog, health, failover. |
| `Catalog` | `shardedobjstr` | In-memory `HashMap<String, PlacementEntry>` tracking which shards hold each object. Optional persistence (JSON or Bincode). |
| `RawRefRegistry` | `shardedobjstr` (`metadata.rs`) | Maps `ShardId -> Arc<RawObjectStore>` + `ShardKind` per shard. Metadata methods branch on kind: `Raw` uses inline TLV trailers, `S3Like` uses native S3 attributes, `Sidecar` uses `__meta__` files. |
| `StoreBackend` | `objstrd` | Enum routing S3 requests: `Raw(Arc<RawObjectStore>)` for standalone, `Sharded(Arc<ShardedObjectStore>, Arc<RawRefRegistry>)` for multi-shard. |
| Coordinator | `objstrd` (planned) | Cluster orchestrator: node registry, global placement catalog, replication orchestration, health monitoring. |

### Data Flow (Local Multi-Shard)

```
S3 Client
    |
    v
objstrd (S3 adapter)
    |
    v
StoreBackend::Sharded
    |
    +---> ShardedObjectStore (ObjectStore trait: put/get/delete/list)
    |         |
    |         +---> select_targets(path) -> [shard_0, shard_1]  (jump hash + clockwise walk)
    |         +---> Concurrent writes/reads to selected shards
    |         +---> Catalog tracks placement
    |
    +---> RawRefRegistry (metadata methods: put_with_meta, head_with_meta, ...)
              |
              +---> ShardKind::Raw     -> RawObjectStore[shard_id] inline TLV trailer
              +---> ShardKind::S3Like  -> ObjectStore put_opts() / GetResult.attributes
              +---> ShardKind::Sidecar -> ObjectStore {path}.__meta__ sidecar file
```

### Data Flow (Coordinator Cluster -- Planned)

```
S3 Client
    |
    v
objstrd (node, ROLE=node)
    |
    +---> Local RawObjectStore(s) for data storage
    +---> Heartbeat loop -> POST /_coord/heartbeat to coordinator
    +---> Write/delete notifications -> POST /_coord/notify to coordinator
    +---> Index invalidation -> POST /_coord/invalidate to coordinator

Coordinator (ROLE=coordinator)
    |
    +---> Node registry: HashMap<NodeId, NodeInfo>
    +---> Global catalog: which nodes hold each object
    +---> /_coord/info, /_coord/nodes, /_coord/placement, /_coord/invalidate
    +---> objstrproxyd queries /_cluster/locate for read routing
```

---

## 2. Object Placement and Replication

### Shard Selection (Jump Consistent Hash)

Shard placement uses Google's jump consistent hash (Lamping & Veach,
2014). When the shard count changes by 1, only ~1/N of keys are
remapped (vs ~(N-1)/N with naive modulus). O(ln N) time, zero memory.

```rust
/// Jump consistent hash (Lamping & Veach, Google 2014).
pub fn jump_consistent_hash(mut key: u64, num_buckets: u32) -> u32 {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < num_buckets as i64 {
        b = j;
        key = key.wrapping_mul(2862933555777941757).wrapping_add(1);
        j = ((b + 1) as f64 * ((1i64 << 31) as f64 / ((key >> 33) + 1) as f64)) as i64;
    }
    b as u32
}

pub struct ReplicationPolicy {
    replication_factor: usize,   // clamped to min(rf, shard_count)
    // shard_count is only used in the constructor, not stored
}

// crc32c(path) -> jump_consistent_hash(hash, N) -> primary shard
// Then pick RF shards walking clockwise from the primary
// E.g., RF=2 on 3 shards: jump_hash(crc32c("data/file.bin"), 3) = 0 -> [shard_0, shard_1]
```

### Health-Aware Target Selection (`select_targets`)

`select_targets()` uses the jump consistent hash to pick a "home"
shard for each key, then walks clockwise from home collecting the
first RF writable shards:

1. Mark each shard as writable: exclude `Offline`, `Detached`, and `Syncing`.
2. If fewer writable shards than RF, return them all (the write will
   be under-replicated but not rejected).
3. Compute the home shard via `jump_consistent_hash(crc32c(path), N)`.
4. Walk clockwise from home, collecting the first RF writable shards.

```rust
fn select_targets(&self, path: &Path) -> Vec<ShardId> {
    let rf = self.replication.factor();
    let shards = self.shards.read();
    let n = shards.len();

    let writable: Vec<bool> = (0..n)
        .map(|id| {
            !shards[id].health.is_unavailable()   // Offline | Detached
                && shards[id].health != ShardHealth::Syncing
        })
        .collect();

    let writable_count = writable.iter().filter(|&&w| w).count();
    if writable_count <= rf {
        return (0..n).filter(|&id| writable[id]).collect();
    }

    let hash = crc32c::crc32c(path.as_ref().as_bytes()) as u64;
    let home = replication::jump_consistent_hash(hash, n as u32) as usize;

    let mut targets = Vec::with_capacity(rf);
    for offset in 0..n {
        let shard_id = (home + offset) % n;
        if writable[shard_id] {
            targets.push(shard_id);
            if targets.len() == rf {
                break;
            }
        }
    }
    targets
}
```

### Synchronous N-Way Writes

All RF replicas are written concurrently via `write_to_shards()`.
At least `min_writes` replicas must succeed; if fewer land, orphan
data on successful shards is cleaned up (for new keys only -- overwrites
are not rolled back because that would destroy the only surviving copy)
and an `InsufficientWrites` error is returned. If all targets fail,
the failed shards are marked Offline, a retry is attempted on fresh
targets, and if the retry also fails, an `AllReplicasFailed` error
is returned.

```
PUT "data/file.bin" with RF=2, min_writes=1, 3 shards:
  1. select_targets("data/file.bin") -> [shard_0, shard_1]   (clockwise from jump hash home)
  2. write_to_shards(path, data, [shard_0, shard_1])          (concurrent writes)
  3. If placed.len() < min_writes -> cleanup orphans + InsufficientWrites
  4. catalog.put("data/file.bin", shards=[0,2], size=N, crc32c=Some(X))
  5. remove_delete_marker("data/file.bin")                     (clear any stale marker)
```

### Write Quorum (min_writes)

`min_writes` controls how many shard replicas must accept a write
before it is considered successful. If fewer than `min_writes` shards
succeed, the write is rolled back and returns `InsufficientWrites`.

| Setting | Default | Description |
|---------|---------|-------------|
| `min_writes` | `max(RF - 1, 1)` | Minimum successful replicas per write |
| `delete_requires_min_writes` | `false` | When true, deletes also enforce `min_writes` |

Examples:
- RF=1: `min_writes` = 1 (must land on the only shard)
- RF=2: `min_writes` = 1 (one of two replicas suffices)
- RF=3: `min_writes` = 2 (two of three replicas required)

The value is clamped to `[1, RF]` and can be overridden via
`with_min_writes(n)` or the `min_writes` config directive.

Both write paths enforce `min_writes` identically:
- Normal puts: `write_to_shards_inner()` in `lib.rs`
- Metadata-aware puts: `put_with_meta()` / `put_with_meta_from_file()` in `metadata.rs`

When `min_writes` is not met, orphaned data on shards that did
accept the write is cleaned up (best-effort delete, including
sidecar `.__meta__` files on Sidecar-kind shards).

### Catalog Tracking

Each object has a `PlacementEntry`:

```rust
pub struct PlacementEntry {
    pub shards: Vec<ShardId>,   // which replicas hold this object
    pub size: u64,              // object body size in bytes (excludes metadata)
    pub crc32c: Option<u32>,    // optional CRC for cross-replica verification
    pub updated: DateTime<Utc>, // last modification time
    pub meta_len: u16,          // metadata suffix length (0 when no metadata)
}
```

### Shard Health States

```rust
pub enum ShardHealth {
    Healthy,    // Normal operation
    Degraded,   // Suspect (CRC errors, intermittent failures)
    Offline,    // Unreachable, writes routed around it (recovery loop auto-reattaches)
    Syncing,    // Coming back online, recovery sync in progress
    Detached,   // Manually taken offline; recovery loop will NOT probe or reattach
}
```

Offline, Syncing, and Detached shards are excluded from write target selection.
Degraded shards are de-prioritized in read order but still targeted
for writes.

The key difference between Offline and Detached: the recovery loop will
probe Offline shards and auto-reattach them when they become reachable,
but it completely skips Detached shards. A shard enters Detached state
via `hold_offline()`, `/_admin/take-offline`, or `/_admin/drain`.

### Shard Info

Each shard tracks runtime state:

```rust
pub struct ShardInfo {
    pub id: ShardId,
    pub store: Arc<dyn ObjectStore>,
    pub health: ShardHealth,
    pub free_space: u64,                       // updated by health probes
    pub offline_since: Option<DateTime<Utc>>,  // when the shard went offline
    pub crc_error_count: AtomicU64,            // cumulative CRC errors
    pub detach_reason: Option<DetachReason>,    // why shard was detached (Manual, Drain, ProbeFailure, DeviceMissing)
    pub suppress_replication: bool,             // when true, re_replication_sweep skips this shard
}
```

### Read Failover (Round-Robin + Fallback)

Reads rotate across replicas for load distribution. If a replica fails,
the next one is tried transparently:

```
GET "data/file.bin":
  1. catalog.get("data/file.bin") -> PlacementEntry { shards: [0, 2], ... }
  2. start = read_counter.fetch_add(1) % 2  (round-robin)
  3. Try shard at index `start`; on error, try next
  4. Return data from first successful replica
```

### Delete Flow (with Markers)

Deletes write a delete marker first, then remove the real object from
all shards. Markers prevent object resurrection when offline shards
return with stale data. See section 4 for full details.

```
DELETE "data/file.bin":
  1. Write delete marker "__deleted__/data/file.bin" to ALL healthy shards
  2. Remove real object from all shards that hold it
  3. Remove catalog entry
```

### Multipart Uploads

Multipart uploads route to the primary shard (hash-selected) and
complete with replication. In-progress uploads are tracked with
creation timestamps for stale-upload cleanup.

```rust
pub fn multipart_upload_count(&self) -> usize
pub fn list_multipart_uploads(&self) -> Vec<(u64, String, u64, ShardId)>
pub async fn purge_stale_multiparts(&self) -> usize
// Default expiry: 24 hours (DEFAULT_MULTIPART_EXPIRY)
```

---

## 3. Index and Catalog Management

### Catalog Persistence Modes

```rust
pub enum CatalogPersistence {
    None,                    // Rebuild from shard scans every startup
    Json { path: PathBuf },  // Human-readable JSON file (atomic rename)
    Bincode { path: PathBuf }, // Compact binary (atomic rename)
}
```

### Catalog Rebuild

`rebuild_catalog()` scans every shard's `list()` output and populates
the catalog from scratch. This is O(total_objects) I/O.

```rust
pub async fn rebuild_catalog(&self) -> Result<usize>
// Returns number of objects found
```

### Startup Loading

1. Try `load_catalog()` from persistence file
2. If file missing or corrupt, fall back to `rebuild_catalog()`
3. Startup is ready once catalog is populated

### Shard Index Invalidation and Reload (NEW)

A shard or node can signal that its catalog entries are stale and must
be rebuilt. This is the key new feature -- see section 7 for the full
protocol.

**When to invalidate:**

| Trigger | Scope | Example |
|---------|-------|---------|
| Crash recovery | Full shard | Node restarts after unclean shutdown; last flush may not have persisted all writes |
| CRC mismatch on read | Single object | A read from one replica returns bad CRC; that object's entry on the bad shard is suspect |
| Manual operator command | Full shard | Admin wants to force re-index after maintenance |

**Why this matters:**

After a crash, the shard's on-disk index may be behind the catalog.
Objects that the catalog says are on this shard may actually be missing
(the write was ACKed and cataloged but the shard's index flush had not
completed). Without invalidation, the master thinks the replication
factor is correct but it is not -- reads to that replica will fail.

---

## 4. Delete Markers

Delete markers are internal sentinel objects that prevent object
resurrection during recovery. When a shard goes offline and comes
back with stale data, delete markers ensure that objects deleted
during the outage are not silently restored.

### Marker Format

| Property | Value |
|----------|-------|
| Key | `__deleted__/<original_key>` |
| Body | Deletion timestamp in RFC 3339 format |
| Placement | Written to ALL healthy shards (not just RF targets) |

Constant: `DELETE_MARKER_PREFIX = "__deleted__/"`.

### Delete Flow

```
DELETE "data/file.bin":
  1. Write marker "__deleted__/data/file.bin" to all healthy shards
     (body = "2026-04-01T12:00:00+00:00")
  2. Remove real object from all shards
  3. Remove catalog entry
  4. If real delete fails, marker is retained (not rolled back)
```

Markers are written to ALL healthy shards (not just RF targets) so that
every shard knows about the deletion. This is critical because any
shard could be offline during the delete and return later with stale
data.

### Visibility

Delete markers are completely hidden from external callers:

| Operation | Behavior |
|-----------|----------|
| `GET __deleted__/*` | Returns NotFound |
| `HEAD __deleted__/*` | Returns NotFound |
| `LIST` | Filters out `__deleted__/` prefix entries |
| `PUT __deleted__/*` (via S3) | Returns AccessDenied |
| `head_with_meta` / `get_metadata` | Returns NotFound |
| `list_with_meta` | Skips markers |

### Re-PUT Clears Markers

When an object is written (PUT), any existing delete marker for that
key is automatically removed. This handles the case where an object
is deleted and then recreated -- the stale marker is cleaned up.

### Marker Replay During Recovery

When a shard comes back online after being offline, delete markers
are replayed during sync (`partitioned_sync()`):

```
For each delete marker on the cluster:
  1. Check if the object exists on the recovering shard
  2. If object exists AND is older than the marker timestamp:
     -> Delete the stale object (it should have been deleted)
  3. If object exists AND is newer than the marker:
     -> Keep the object (it was re-PUT after the delete)
  4. If object does not exist:
     -> Nothing to do
```

### Vacuum

Vacuum purges stale delete markers. It only runs when ALL shards are
healthy (refuses if any shard is Offline).

```rust
pub async fn vacuum_delete_markers(&self) -> Result<(usize, usize)>
// Returns (markers_purged, stale_objects_cleaned)
```

For each marker:

| Condition | Action |
|-----------|--------|
| No live object exists | Marker fully applied -- purge marker |
| Live object newer than marker | Re-PUT after delete -- purge stale marker |
| Live object older than marker | Missed delete -- delete object, then purge marker |

### Discovery and Management APIs

```rust
pub async fn list_delete_markers(&self) -> Vec<(String, DateTime<Utc>)>
pub async fn get_delete_marker(&self, key: &str) -> Option<DateTime<Utc>>
pub async fn vacuum_delete_markers(&self) -> Result<(usize, usize)>
```

CLI commands:

```bash
# List all delete markers with timestamps
shardedobjstr list-deleted --shards /tmp/s0.raw,/tmp/s1.raw --replicas 2

# Purge stale markers (requires all shards healthy)
shardedobjstr vacuum --shards /tmp/s0.raw,/tmp/s1.raw --replicas 2
```

---

## 5. Crash Recovery

### Storage Level (RawObjectStore)

The raw store is crash-safe at the device level:

| Mechanism | Description |
|-----------|-------------|
| Double-buffered superblock | Two superblocks with `txn_id`; on recovery, the higher valid `txn_id` wins |
| Write-ahead index | All metadata changes logged before applying |
| Atomic index flush | `flush_index()` makes the entire index atomically durable |
| CRC32c per extent | Every block has a CRC; corruption is detected on read |
| Tombstone recording | Objects that fail integrity on open are tombstoned, not silently dropped |

**Open modes for integrity checking:**

| Mode | Behavior | Use Case |
|------|----------|----------|
| `Default` | Fast block-0 scan (1 pread per object, 4KB each) | Normal startup |
| `FullVerify` | Reads every block, verifies all CRCs | After suspected corruption |
| `SkipVerify` | No integrity checks | Embedded use (e.g., LanceDB) |

### Node Level (Per-Shard Operations)

Existing methods on `RawObjectStore` for recovery:

| Method | Description |
|--------|-------------|
| `reload_index()` | Re-read superblock and index from disk; returns true if index changed |
| `verify_all()` | Full integrity scan; returns `VerifyReport` with list of errors, overlapping extents, free-list consistency |
| `repair()` | Rebuilds free list from clean index entries, re-flushes superblocks; returns `RepairReport` |
| `list_tombstones()` | Returns objects that were corrupted on open (path, size, CRC, reason) |
| `device_info()` | Returns device stats (size, free space, file count, txn_id) -- used for liveness checks |
| `flush_index()` | Force immediate index persistence |

### Shard Level (Catalog Invalidation)

After a shard crashes and restarts:

1. The shard's `RawObjectStore` recovers using its double-buffered
   superblock (storage-level crash safety)
2. Some objects may be missing from the shard's index if the last
   `flush_index()` did not complete before the crash
3. The catalog still lists those objects as present on this shard
4. **Invalidation** corrects this: drop catalog entries for the shard,
   re-scan the shard, and re-replicate anything that is missing

See section 7 for the full invalidation protocol.

### Cluster Level (Planned -- Phase H)

When the coordinator comes back online after a crash:

1. Scan all nodes' object indexes (S3 listing per node)
2. Rebuild global catalog from scratch
3. Detect missing or orphaned replicas
4. Trigger background re-replication to restore correct RF

---

## 6. Corruption Detection and Handling

### CRC32c Verification

Every extent (block) in `RawObjectStore` has a CRC32c checksum. On every
read, the CRC is verified. A mismatch means the data on disk has been
silently corrupted (bit-rot, bad sector, partial write, etc.).

### Current Behavior on CRC Mismatch

1. The read returns an error to the caller
2. If detected during open (Default/FullVerify mode), the object is
   tombstoned -- it appears in `list_tombstones()` but not in the live
   index

### Enhanced Behavior (Planned -- Transparent Read Repair)

When `ShardedObjectStore::get()` reads from a replica and gets a CRC
mismatch:

1. **Fallback**: Try the next replica in the read order
2. **Return healthy data**: If another replica succeeds, return that
   data to the caller (transparent to the application)
3. **Alert**: Log a warning with shard_id, object path, and expected
   vs actual CRC -- this is a **drive health signal** (could indicate
   a dying drive)
4. **Schedule repair**: Queue a background task to overwrite the corrupt
   copy on the bad shard with the good copy from the healthy replica
5. **Escalation**: Track CRC failure count per shard. If failures exceed
   a configurable threshold (default: 10 failures in 1 hour), escalate
   the shard to `ShardHealth::Degraded`

### Tombstones

Objects that fail integrity checks become tombstones:

```rust
pub struct TombstoneEntry {
    pub path: String,
    pub size: u64,
    pub crc32c: u32,
    pub last_modified: DateTime<Utc>,
    pub reason: String,       // e.g., "crc_mismatch", "block_corrupt"
    pub tombstone_txn: u64,
}
```

Tombstones can be listed (`list_tombstones()`), individually deleted
(`delete_tombstone()`), or bulk-cleared (`clear_tombstones()`). They
represent objects that were on the device but could not be verified.

### Full Device Verification

`verify_all()` scans every object on the device and returns a
`VerifyReport`:

- `files_checked` / `files_ok` -- total vs passing objects
- `errors` -- list of per-extent verification failures
- `overlapping_extents` -- pairs of objects sharing the same blocks
- `free_list_consistent` -- whether the free list accounts for all space
- `space_accounted` -- whether used + free = total

If `verify_all()` reports errors, `repair()` can rebuild the free list
and re-flush superblocks.

---

## 7. Shard Index Invalidation Protocol

This section describes how a shard or node tells the master (local
`ShardedObjectStore` or remote coordinator) that its index entries are
stale and must be refreshed. Designed for local multi-shard now, extends
to coordinator cluster later.

### Triggers

| Trigger | Type | Description |
|---------|------|-------------|
| Crash recovery | Full shard | Shard restarts after unclean shutdown. Last `flush_index()` may not have completed -- some objects the catalog thinks are here may be missing. |
| CRC mismatch on read | Single object | A read from this shard returned bad CRC. The object may be corrupt or partially written. |
| Manual operator command | Full shard | Admin forces re-index via API endpoint or CLI. |

### Local Multi-Shard Mode

In local multi-shard mode, the `ShardedObjectStore` is the master. The
invalidation happens in-process:

**Step 1: Detect issue**

- Crash recovery: on shard reopen, `RawObjectStore` may report
  tombstones (`list_tombstones()`) or `reload_index()` returns true
  (index changed from what was expected)
- CRC mismatch: `ShardedObjectStore::get()` catches read error with
  CRC details
- Manual: operator calls API endpoint

**Step 2: Mark shard Degraded**

```rust
// New method
pub fn set_shard_health(&mut self, shard_id: ShardId, health: ShardHealth)
```

Marks the shard as `Degraded`. Reads can still be attempted but the
shard is de-prioritized in read order. Writes continue to target this
shard (it is still up, just untrusted). During recovery sync, the
shard transitions to `Syncing` (excluded from write targets).

**Step 3: Snapshot and purge catalog entries for shard**

```rust
// New method on Catalog
pub fn remove_all_for_shard(&self, shard_id: ShardId) -> usize
// Returns number of entries modified (shard removed from their shards list)
```

For every `PlacementEntry` that lists `shard_id`, remove `shard_id`
from the `shards` vec. If the entry's `shards` becomes empty, the
object is effectively lost (all replicas were on this shard -- this
should not happen with RF >= 2 and proper failure domains).

Before purging, snapshot the current entries so we can compare:

```rust
// New method on Catalog
pub fn entries_for_shard(&self, shard_id: ShardId) -> Vec<(String, PlacementEntry)>
```

**Step 4: Re-scan the shard**

```rust
// New method on ShardedObjectStore
pub async fn rebuild_catalog_for_shard(&self, shard_id: ShardId) -> Result<usize>
// Calls list() on just this one shard and adds entries back to catalog
// Returns number of objects found on the shard
```

This is like `rebuild_catalog()` but only for one shard, so it is
O(objects_on_shard) instead of O(total_objects).

**Step 5: Compare and detect missing objects**

```
pre_invalidation = entries_for_shard(shard_id)   // Step 3 snapshot
post_rebuild = entries_for_shard(shard_id)        // after Step 4

missing = pre_invalidation.keys() - post_rebuild.keys()
// These objects were in the catalog for this shard but are NOT on disk
```

For each missing object:
- Log WARNING: "object {key} was cataloged on shard {shard_id} but not found after re-scan"
- Check if other replicas still have it (look at the entry's remaining shards)
- If RF is now below target: queue background re-replication from a
  healthy replica to a new target shard

**Step 6: Mark shard Healthy**

Once re-scan is complete and re-replication is queued, mark the shard
`Healthy` again.

**Step 7: Return report**

```rust
pub struct InvalidateReport {
    pub shard_id: ShardId,
    pub entries_before: usize,      // catalog entries that referenced this shard
    pub entries_after: usize,       // entries found on re-scan
    pub objects_missing: usize,     // were in catalog but not on disk
    pub objects_under_replicated: usize, // missing + no other replica has them at target RF
    pub re_replication_queued: usize,    // how many objects queued for re-replication
}
```

**Full flow:**

```rust
// New method on ShardedObjectStore
pub async fn invalidate_shard(
    &mut self,
    shard_id: ShardId,
) -> Result<InvalidateReport>
```

### Coordinator Cluster Mode (Future -- Phase D+)

In coordinator cluster mode, the node communicates with the coordinator
over HTTP:

**Node side:**

1. Node detects issue (crash recovery, CRC mismatch, manual)
2. Node sends `POST /_coord/invalidate` to coordinator:
   ```json
   {
     "node_id": "node-3",
     "reason": "crash_recovery",
     "affected_keys": null
   }
   ```
   (`affected_keys` is null for full invalidation, or a list of keys
   for targeted invalidation after CRC mismatch)
3. Node continues serving reads from its current valid objects

**Coordinator side:**

1. Receive `POST /_coord/invalidate`
2. Mark node as `Degraded` in the node registry
3. Purge global catalog entries for that node (or specific keys)
4. Re-scan node: `GET /<node>/listing` or accept node's pushed listing
5. Compare pre/post, detect missing objects
6. For under-replicated objects: select healthy nodes as re-replication
   targets and dispatch background copy tasks
7. Mark node `Healthy` when re-scan and re-replication dispatch are done
8. Return `InvalidateReport` to node

**CRC mismatch alerting in coordinator mode:**

When a node reports CRC mismatch via `/_coord/invalidate` with a
specific key, the coordinator:

1. Invalidates that key's entry on the reporting node
2. Logs a drive-health warning for that node
3. Tracks CRC failure count per node
4. If threshold exceeded (configurable, default 10/hour): marks node
   `Degraded` and alerts operator
5. Attempts to read the object from another replica and re-replicate
   to a healthy node to maintain RF

### API Summary (New Methods)

**ShardedObjectStore:**

```rust
pub async fn invalidate_shard(&mut self, shard_id: ShardId) -> Result<InvalidateReport>
pub async fn rebuild_catalog_for_shard(&self, shard_id: ShardId) -> Result<usize>
pub fn set_shard_health(&mut self, shard_id: ShardId, health: ShardHealth)
```

**Catalog:**

```rust
pub fn remove_all_for_shard(&self, shard_id: ShardId) -> usize
pub fn entries_for_shard(&self, shard_id: ShardId) -> Vec<(String, PlacementEntry)>
```

**Hookpoints into existing RawObjectStore methods:**

| Method | Used For |
|--------|----------|
| `reload_index()` | Re-read superblock and index after crash |
| `verify_all()` | Full integrity scan of a shard |
| `list_tombstones()` | Detect objects corrupted during crash |
| `device_info()` | Check shard liveness (free space, txn_id) |

---

## 8. Repair and Sync Operations

The `repair` module (`shardedobjstr/src/repair.rs`) contains all
algorithms for recovering shards and maintaining replication health.
These functions live in the library crate so CLI tools and Python
bindings can use them directly.

### Health Probing

```rust
pub async fn probe_store(store: &Arc<dyn ObjectStore>, timeout: Duration) -> bool
```

Attempts a small LIST with a timeout. Returns true if the store
responds, false if it is unreachable or times out. Used by the
recovery polling loop to detect when offline shards come back.

### Mirror Sync (RF == shard_count)

```rust
pub async fn mirror_sync(
    cluster: &ShardedObjectStore,
    original_stores: &[Option<Arc<dyn ObjectStore>>],
    target_shard: ShardId,
    raw_refs: Option<&RawRefRegistry>,
) -> Result<MirrorSyncReport>
```

Full bi-directional diff-and-sync for setups where every object
should exist on every shard (RF equals shard count). Copies missing
objects from healthy shards to the target, and vice versa.

Returns `MirrorSyncReport { copied, deleted, bytes_copied, updated }`.

### Partitioned Sync (RF < shard_count)

```rust
pub async fn partitioned_sync(
    cluster: &ShardedObjectStore,
    original_stores: &[Option<Arc<dyn ObjectStore>>],
    target_shard: ShardId,
    raw_refs: Option<&RawRefRegistry>,
) -> Result<usize>
```

Repairs under-replicated objects and replays delete markers for
partitioned (RF < shard_count) setups. Only copies objects that
should live on the target shard according to the hash ring.

During sync, delete markers are replayed: stale objects on the
target shard (older than the marker timestamp) are removed.

### Sync and Reattach Orchestration

```rust
pub async fn sync_and_reattach(
    cluster: &ShardedObjectStore,
    original_stores: &[Option<Arc<dyn ObjectStore>>],
    shard_id: ShardId,
    raw_refs: Option<&RawRefRegistry>,
)
```

Orchestrates the full recovery lifecycle:
1. Sets shard health to `Syncing`
2. Runs `mirror_sync()` or `partitioned_sync()` depending on RF
3. Calls `attach_shard(shard_id, store, force=true)`
4. Sets shard health to `Healthy`

### Repair-Replication Sweep

```rust
pub async fn repair_replication_sweep(
    cluster: &ShardedObjectStore,
    batch_size: usize,
    raw_refs: Option<&RawRefRegistry>,
    progress: Option<&ProgressSink>,
) -> RepairReplicationResult
```

Full repair-replication cycle: repairs under-replicated objects and trims
over-replicated objects (removes excess copies from shards that
should not hold them according to the hash ring).

Returns `RepairReplicationResult { re_replicated, trimmed, under_remaining, over_remaining }`.

### Repair-Replication Planning (Dry Run)

```rust
pub fn plan_repair_replication(
    cluster: &ShardedObjectStore,
    batch_size: usize,
) -> RepairReplicationPlan
```

Dry-run repair-replication that performs no I/O. Returns a plan showing
what would be replicated, trimmed, or left unresolved.

Returns `RepairReplicationPlan { replications, trims, unrepairable, untrimmable }`.

### Shard Draining

```rust
pub async fn drain_shard(
    cluster: &ShardedObjectStore,
    survivor_cluster: &ShardedObjectStore,
    victim_shard_id: ShardId,
    victim_store: &Arc<dyn ObjectStore>,
    raw_refs: Option<&RawRefRegistry>,
    progress: Option<&ProgressSink>,
) -> DrainReport
```

Moves all objects off a shard to other healthy shards (`survivor_cluster`
is a cluster built from just the surviving shards). Used before
decommissioning a device. The shard is marked Detached after draining
(recovery loop will not auto-reattach it). Use `/_admin/attach` to
manually bring it back online.

---

## 9. Buddy Lists and Lights-Out Operation

**Status: Planned (Phase G, currently parked)**

Buddy lists enable the cluster to continue operating when the
coordinator is down -- "lights-out" operation.

### Concept

Each node maintains a **buddy list**: the set of peer nodes that are
likely to hold replicas of the same objects. This is derived from the
replication policy and consistent hash ring.

```
Node A's buddy list:
  - Node B (holds replicas for hash range 0x0000-0x5555)
  - Node C (holds replicas for hash range 0x5555-0xAAAA)
  - Node D (holds replicas for hash range 0xAAAA-0xFFFF)
```

### Coordinator-Down Behavior

When the coordinator is unreachable (detected via missed heartbeat
responses):

1. **Reads continue**: Nodes use their local catalog + buddy knowledge
   to serve reads. If a local read fails, the node contacts the buddy
   that should have the replica.
2. **Writes continue**: Nodes write locally and replicate to buddies
   based on the hash ring. The write is ACKed once RF replicas confirm.
3. **No catalog updates**: The global catalog on the coordinator is
   stale. Writes and deletes are tracked in a local journal.
4. **Coordinator recovery**: When the coordinator comes back, nodes
   replay their journals to reconcile the global catalog (Phase H).

### Interaction with Index Invalidation

When a node invalidates its index (section 7), the buddy list is
critical:

- The buddies are the source of truth for re-replication
- If the invalidating node lost objects, the coordinator (or the node
  itself in lights-out mode) reads them from buddies and writes them
  back to the invalidating node
- If the coordinator is down during invalidation, the node contacts
  its buddies directly to verify what objects it should have and
  re-replicate from them

### Buddy List Maintenance

- Built on startup from the hash ring and replication policy
- Updated when nodes join or leave the cluster
- Persisted locally so it survives node restarts
- Coordinator broadcasts buddy list updates when topology changes

---

## 10. Self-Healing Pipeline

End-to-end flow from detection to recovery. This pipeline operates
automatically -- no operator intervention unless escalation thresholds
are hit.

### Pipeline Stages

```
  DETECTION
      |
      v
  INVALIDATION
      |
      v
  ASSESSMENT
      |
      v
  RE-REPLICATION
      |
      v
  ALERTING
      |
      v
  VERIFICATION
```

### Stage 1: Detection

| Source | Detects | Scope |
|--------|---------|-------|
| Crash recovery (reopen) | Missing objects (flush not persisted) | Full shard |
| CRC mismatch on read | Corrupt block | Single object |
| `verify_all()` | All integrity issues | Full shard |
| Heartbeat timeout | Node unreachable | Full node |

### Stage 2: Invalidation

Drop stale catalog entries for the affected shard or node. See section 7.

- Full shard: `invalidate_shard(shard_id)`
- Single object: `catalog.remove_shard(key, shard_id)`

### Stage 3: Assessment

Compare pre-invalidation catalog entries with post-re-scan results:

- **Objects present**: Shard has them, catalog updated -- no action
- **Objects missing**: Were in catalog, not on disk -- need re-replication
- **Objects extra**: On disk but not in catalog (possible orphan from
  interrupted delete) -- add to catalog or quarantine

### Stage 4: Re-Replication

For each under-replicated object:

1. Find a healthy replica (from remaining shards in `PlacementEntry`,
   or via buddy list)
2. Read the object from the healthy replica
3. Write it to a target shard to restore RF
4. Update catalog with the new placement
5. Rate-limit to avoid saturating network/disk I/O

### Stage 5: Alerting

| Condition | Alert Level | Action |
|-----------|-------------|--------|
| Single CRC mismatch | INFO | Log warning, continue |
| Repeated CRC failures on same shard (>= threshold) | WARNING | Mark shard Degraded, operator notification |
| Object unrecoverable (no healthy replica) | CRITICAL | Operator must intervene -- data may be lost |
| Node unreachable past grace period | WARNING | Mark node Offline, begin re-replication |

**Drive health signal:** Repeated CRC failures on the same shard likely
indicate a failing drive. The alert should include the shard/device path
and failure count so the operator can schedule drive replacement.

**Configurable thresholds:**
- `crc_failure_threshold`: number of CRC failures before Degraded (default: 10)
- `crc_failure_window_secs`: time window for counting failures (default: 3600)
- `heartbeat_grace_secs`: time before marking node Offline (default: 90)

### Stage 6: Verification

After re-replication completes:

1. Cross-shard CRC32c comparison: read from each replica, compare CRCs
2. If CRCs match: replication is healthy
3. If CRCs mismatch: quarantine the bad copy, alert, re-replicate again

---

## 11. Manual Operations

### Current (Implemented)

| Operation | Method | Description |
|-----------|--------|-------------|
| Force index flush | `POST /_admin/flush` | Triggers immediate `flush_index()` on the raw store |
| Storage repair | `rawobjstr repair --file <device>` | Rebuilds free list, re-flushes superblocks |
| Verify device | `rawobjstr verify --file <device>` | Full CRC scan, reports all errors |
| List tombstones | `rawobjstr tombstones --file <device>` | Shows objects that failed integrity |
| Scrub free space | `rawobjstr scrub --file <device>` | Zeros free blocks, reclaims fragmented space |

### Planned (With Invalidation Feature)

| Operation | Method | Description |
|-----------|--------|-------------|
| Force shard re-index | `POST /_admin/invalidate?shard=N` | Drops and rebuilds catalog entries for shard N (local multi-shard) |
| Force node re-index | `POST /_coord/invalidate` | Node requests coordinator to re-index its entries (coordinator mode) |
| Repair-replication sweep | `POST /_admin/repair-replication` | Run one repair-replication cycle (repair under-replicated + trim over-replicated) |
| Drain shard | `POST /_admin/drain/<shard_id>` | Move all objects off a shard before decommissioning |
| Redistribute objects | `POST /_admin/redistribute` | Balance shard object counts (fullest to emptiest, 10% tolerance) |
| Take shard offline | `POST /_admin/take-offline/<shard_id>` | Manual detach with optional suppress_replication |
| Reattach shard | `POST /_admin/attach/<shard_id>` | Reopen store, rescan index, return to Healthy |
| Rebuild index | `POST /_admin/rebuild-index` | Reload index from device + rebuild registry |
| Clear bucket cache | `POST /_admin/clear-bucket-cache` | Delete `__buckets__/` objects, rebuild registry |
| Config reload | `POST /_admin/reload` | Trigger config reload (like SIGHUP) |
| List delete markers | `shardedobjstr list-deleted` | Show all delete markers with timestamps |
| Vacuum markers | `shardedobjstr vacuum` | Purge stale delete markers (requires all shards healthy) |
| Check replication health | `GET /_admin/replication` (future) | Report under-replicated objects and current RF per object |

---

## 11b. Degraded Startup, Detach, and Reattach

This section covers the workflow for operating a sharded store when
one or more backing stores are intermittently unavailable. The primary
use case is a "RAID mirror" setup (e.g., raw device + S3, RF=2) where
either store may be offline at any time.

### Degraded Startup

The `ShardedObjectStore::new_with_offline()` constructor accepts
`Vec<Option<Arc<dyn ObjectStore>>>`. Slots that are `None` get an
`OfflinePlaceholderStore` (errors on every operation) and are marked
`ShardHealth::Offline`. The cluster starts with only the available
shards active.

```rust
// Example: S3 is available, raw device is not attached
let stores: Vec<Option<Arc<dyn ObjectStore>>> = vec![
    Some(s3_store),   // shard 0: S3 -- available
    None,             // shard 1: raw device -- not attached
];
let cluster = ShardedObjectStore::new_with_offline(stores, 2);
// cluster starts in degraded mode: writes go to S3 only
// reads work for objects on S3, fail for objects only on raw device
```

The shard count is fixed by the length of the `stores` vector, so the
hash ring stays stable. When the raw device is attached later, it
occupies the same slot position.

### Detach

To take a shard offline at runtime (e.g., to grow the device, do
maintenance, or because the network dropped):

```rust
cluster.detach_shard(shard_id);
// Replaces the store with OfflinePlaceholderStore
// Marks shard Offline
// Catalog entries are PRESERVED (not purged)
```

Catalog entries are intentionally kept. This means:

- `find_under_replicated()` will list objects that reference this
  shard because it counts only non-Offline shard copies
- The system knows which objects *should* be on this shard when it
  comes back
- No data is lost from the catalog perspective

### Reattach (Force Mode)

When a shard comes back online and its data is trusted (no crash,
no corruption -- just a reconnect):

```rust
let count = cluster.attach_shard(shard_id, store, true).await?;
// force=true: trust existing data, scan and add to catalog
// No purge, no invalidation
// Returns number of objects found on the shard
```

This is the right mode for:

- Reconnecting S3 after a network outage
- Reattaching a raw device after growing it
- Plugging in a USB drive that was disconnected
- The "laptop scenario" (see below)

### Reattach (Invalidate Mode)

When a shard comes back and its data may be stale (e.g., after a
crash or suspected corruption):

```rust
let count = cluster.attach_shard(shard_id, store, false).await?;
// force=false: full invalidation (purge + re-scan + detect missing)
```

### The Laptop Scenario

A user has a sharded store with S3 + raw device, RF=2.

1. **At home (both online):** All objects replicated to both stores
2. **On the go (S3 offline):** User detaches S3 (or it is simply
   unreachable). New objects go only to the raw device. The catalog
   tracks them as having 1 replica (under-replicated).
3. **Back home (S3 online again):** User reattaches S3 with
   `attach_shard(s3_id, s3_store, force=true)`. The catalog is
   rebuilt for S3. Then `find_under_replicated()` identifies objects
   that need replication. The user (or a background task) calls
   `replicate_object()` for each.

The reverse also works: raw device offline, work via S3, reattach
raw device later.

Key design decisions for this scenario:

- **No timeout-based rejection:** Unlike the coordinator cluster mode
  (which uses heartbeat grace periods), the library-level detach/
  reattach has no concept of "away too long". A shard can be offline
  for seconds or months -- reattach always works.
- **Force reattach trusts data:** No objects are invalidated or
  purged. The shard's existing objects are added to the catalog
  as-is. This avoids the lights-out problem where a store returning
  after a long absence is rejected.
- **Under-replication is detected, not auto-fixed:** The library
  provides `find_under_replicated()` and `replicate_object()` but
  does not run background replication automatically. The caller
  (CLI tool, daemon, or Python script) decides when to replicate.

### Replication Check Workflow

The replication count is not stored persistently -- it is computed
from the catalog each time. The workflow:

1. `rebuild_catalog()` or `attach_shard(force=true)` -- scans each
   available shard's index, merges into catalog. Each object's
   `PlacementEntry.shards` lists which shards hold it.
2. `find_under_replicated()` -- iterates all catalog entries,
   counts non-Offline replicas, returns entries below target RF.
3. For each under-replicated object: pick a healthy source shard
   and a target shard, call `replicate_object(key, from, to)`.
4. The catalog is updated incrementally as each replication completes.

This is O(catalog_size) for the check and O(object_size) per
replication. No extra persistent state is needed.

### Interaction with Invalidation

| Scenario | Use `force=true` | Use `force=false` |
|----------|-------------------|-------------------|
| Network reconnect (S3 back online) | Yes | No |
| USB drive reconnected (clean disconnect) | Yes | No |
| Device grown and reattached | Yes | No |
| Shard crashed and restarted | No | Yes |
| Suspected corruption | No | Yes |
| Routine maintenance (no data change) | Yes | No |

### API Summary

```rust
// Degraded startup
pub fn new_with_offline(stores: Vec<Option<Arc<dyn ObjectStore>>>, rf: usize) -> Self

// Runtime detach/attach
pub fn detach_shard(&mut self, shard_id: ShardId) -> Option<ShardHealth>
pub async fn attach_shard(&mut self, id: ShardId, store: Arc<dyn ObjectStore>, force: bool) -> Result<usize>

// Under-replication detection and repair
pub fn find_under_replicated(&self) -> Vec<(String, usize)>
pub async fn replicate_object(&self, key: &str, from: ShardId, to: ShardId) -> Result<u64>
```

### Write Path with Offline Shards

When shards are offline, the write path automatically routes around them
and retries on failure. This applies to both `ShardedObjectStore` methods
and `objstrd`'s metadata adapter (`put_with_meta`).

**Shard selection (`target_shards` / `select_targets`):**

1. Compute hash ring position from the object path (CRC32C).
2. Walk the ring starting from the hashed position.
3. Skip any shard with health Offline, Detached, or Syncing.
4. Collect up to `replication_factor` healthy shards.
5. If fewer healthy shards than RF, return what is available (writes
   succeed with fewer replicas, objects are under-replicated).

**Retry on write failure:**

All write methods (`put`, `put_opts`, `put_multipart_opts`, and
`put_with_meta`) follow this pattern:

1. Select target shards (health-aware).
2. Attempt the write concurrently to all targets.
3. If at least `min_writes` targets succeed: return success. The
   catalog records only the shards that received the object.
   Failed shards are marked `Degraded`.
4. If ALL targets fail:
   a. Mark each failed shard as `ShardHealth::Offline` via
      `set_shard_health()`.
   b. Re-select targets (the newly-offline shards are now excluded).
   c. Retry the write on the new targets (skipping any already tried).
   d. If the retry also fails entirely: return the error.

This means a shard can fail mid-request (e.g., disk I/O error, device
removed between health checks) and the write still succeeds on another
shard. The failed shard is immediately marked Offline so subsequent
writes avoid it without waiting for the next recovery poll cycle.

**Catalog behavior:**

- Catalog entries for objects on the offline shard are NOT purged.
- `resolve_shards()` filters out Offline shard IDs at read time.
- Objects that exist only on the offline shard return NotFound until
  the shard comes back.
- Objects with replicas on healthy shards remain fully accessible.
- When the shard recovers, `sync_and_reattach()` runs the mirror
  sync and `attach_shard(force=true)` rebuilds catalog entries.

**Recovery task interaction (`recovery.rs`):**

The recovery task runs a polling loop (every 10 seconds) that:

1. Checks each offline shard via `probe_store()` (small LIST with
   timeout -- fast health check).
2. When a previously-offline shard becomes reachable:
   a. Sets shard health to `Syncing`.
   b. Runs `mirror_sync()` or `partitioned_sync()` to copy missing
      objects from healthy shards to the returning shard.
   c. During `partitioned_sync()`, replays delete markers to remove
      stale objects on the returning shard.
   d. Calls `attach_shard(shard_id, store, force=true)` to scan the
      shard's contents and merge into the catalog.
   e. Marks the shard `ShardHealth::Healthy`.
3. Objects that were NotFound (only on the offline shard) become
   accessible again after reattach.

---

## 11c. Object Redistribution

Redistribution balances object counts across shards by moving objects
from the fullest shard to the emptiest shard. This is useful after
adding a new shard, after a drain, or whenever object placement has
become uneven.

### Algorithm

`redistribute_sweep()` in `shardedobjstr/src/repair.rs`:

```rust
pub async fn redistribute_sweep(
    cluster: &ShardedObjectStore,
    batch_size: usize,
    tolerance_pct: f64,
    raw_refs: Option<&RawRefRegistry>,
    progress: Option<&ProgressSink>,
) -> RedistributeResult
```

1. **Precondition check:** Refuses to run if any under-replicated
   objects exist (`find_under_replicated()` must return empty). The
   caller must run `repair_replication_sweep()` first.
2. **Early exit:** Returns immediately if fewer than 2 healthy shards
   or 0 total objects.
3. **Per-iteration:** Count objects per shard, pick the fullest as
   source and the emptiest as destination. Stop when
   `(max_count - min_count) / mean <= tolerance_pct` or when
   `src_count <= dst_count + 1`.
4. **Per-object move:** Find an object on the source shard that is
   NOT already on the destination. Verify that the object's healthy
   replica count (excluding source) plus one is >= RF. Then:
   - `replicate_object(key, src, dst)` -- copy to destination
   - Verify RF is met after replication
   - `remove_replica(key, src)` -- remove from source
5. **Metadata preservation:** When `raw_refs` is provided, metadata
   (custom headers, content-type) is preserved during the move via
   the metadata-aware replication path.
6. Re-sorts shard counts after each move to always pick the current
   fullest/emptiest pair.

### Result

```rust
pub struct RedistributeResult {
    pub moved: usize,                          // objects successfully moved
    pub skipped: usize,                        // objects skipped (no valid target)
    pub errors: usize,                         // errors during move
    pub shard_counts: Vec<(ShardId, usize)>,   // per-shard counts after sweep
}
```

### Admin Endpoint

```
POST /_admin/redistribute
Authorization: Bearer <ADMIN_TOKEN>

Response: { "moved": 42, "skipped": 0, "errors": 0,
            "shard_counts": [[0, 150], [1, 148], [2, 152]] }
```

The operation runs asynchronously -- only one long-running admin
operation (drain, repair-replication, redistribute) can run at a time.
Poll `GET /_admin/admin-op-status` to check progress:

```
{ "running": true, "operation": "redistribute" }
```

### Python Binding

```python
result = cluster.redistribute(batch_size=500, tolerance_pct=0.10)
print(result.moved, result.skipped, result.errors)
print(result.shard_counts)  # [(0, 150), (1, 148), (2, 152)]
```

### Relationship to Other Operations

| Operation | Purpose |
|-----------|---------|
| `redistribute_sweep` | Balance object counts across shards (even distribution) |
| `drain_shard` | Evacuate ALL objects off a single shard (pre-decommission) |
| `repair_replication_sweep` | Fix under-replicated and over-replicated objects (correct RF) |

Redistribution does not change the replication factor -- it moves
existing replicas so that each shard holds roughly the same number
of objects. It is distinct from the planned hash-ring-aware
rebalancing (Phase E+) which would reassign objects to match their
consistent-hash home shards after topology changes.

---

## 12. Monitoring and Observability

### Current Endpoints (Per-Shard, RawObjectStore)

| Endpoint | Response | Notes |
|----------|----------|-------|
| `GET /_admin/info` | JSON: device stats (size, free space, file count, txn_id, build hash, PID) | Also used for binary-swap detection in test suites |
| `GET /_admin/heatmap?chunks=N` | JSON: aggregated chunk density array | For device heatmap visualization |
| `GET /_admin/objects?offset=N&limit=N&prefix=X` | JSON: paginated object listing with sizes | Object browser |
| `GET /_admin/` or `GET /_admin/viz` | SVG/HTML: device heatmap | Visual representation of block usage |
| `GET /_admin/recovery` | JSON: live recovery status and config | Shows offline shards, sync progress |
| `GET /_admin/shards` | JSON: per-shard health, free space, stats | Shard dashboard data |
| `GET /_admin/sysinfo` | JSON: system info (hostname, OS, CPU, memory) | Server diagnostics |
| `GET /_admin/nodeconfig` | JSON: raw node config | For config viewer UI |
| `GET /_admin/buckets` | JSON: list of buckets | Bucket management |
| `GET /_admin/shard/{id}/{sub}` | Per-shard visualization | Shard-specific heatmap/objects |
| `POST /_admin/flush` | Forces immediate index flush | Returns 200 on success |
| `POST /_admin/rebuild-index` | Reload index from device + rebuild registry | Admin action, requires token |
| `POST /_admin/clear-bucket-cache` | Delete `__buckets__/` objects | Admin action, requires token |
| `POST /_admin/reload` | Config reload (like SIGHUP) | Admin action, requires token |
| `POST /_admin/repair-replication` | Run one repair-replication sweep | Under+over replication repair |
| `POST /_admin/drain/<shard_id>` | Drain all objects off a shard | Pre-decommission, shard goes Detached |
| `POST /_admin/take-offline/<shard_id>` | Take a shard offline manually | Shard goes Detached, recovery skips it |
| `POST /_admin/attach/<shard_id>` | Reattach a Detached/Offline shard | Reopens store, rescans index |

**UI Pages:**

| Page | URL | Description |
|------|-----|-------------|
| Cluster dashboard | `/_admin/cluster` | Shard health, object counts, free space |
| Config viewer | `/_admin/config` | Current configuration |
| Object manager | `/_admin/ui` | Browse and manage objects |
| Server info | `/_admin/server` | Build info, uptime, PID |
| System logs | `/_admin/logs/ui` | Searchable system log viewer |

**Auth:** Admin endpoints (`POST /_admin/*`) require a bearer token
(env: `ADMIN_TOKEN`). CORS origin configurable via `ADMIN_CORS_ORIGIN`.

Raw-device-specific endpoints (heatmap, region) return HTTP 503 when
the backend is not a raw device. Cluster-only endpoints (repair-replication,
drain, redistribute) return 503 on a standalone server. Most admin
endpoints (`/_admin/info`, `/_admin/shards`, etc.) work with any backend.

### Planned Endpoints (Coordinator Cluster)

| Endpoint | Response | Notes |
|----------|----------|-------|
| `GET /_coord/info` | Coordinator status, uptime, node count | Phase D |
| `GET /_coord/nodes` | List of all nodes with health, capacity, last_seen | Phase C |
| `GET /_coord/placement?key=X` | Which nodes hold an object | Phase D |
| `POST /_coord/invalidate` | Trigger node index invalidation | Phase D (section 7) |
| `GET /_cluster/locate?bucket=X&key=Y` | Object location for edge proxies (objstrproxyd) | Phase D |

### Health Checks

| Check | Frequency | Action on Failure |
|-------|-----------|-------------------|
| Recovery task probe | Every 10s (configurable) | `probe_store()` checks offline shards; runs `sync_and_reattach()` when reachable |
| Heartbeat (node to coordinator) | Every 30s (configurable) | Grace period, then mark Offline |
| `device_info()` poll | Every 60s (configurable) | Mark shard Degraded if unreachable |
| CRC failure counter | Per-read (cumulative per shard) | Escalate to Degraded at threshold |
| Catalog vs reality drift | On invalidation or periodic scan | Re-scan and re-replicate |
| Stale multipart cleanup | On demand (`purge_stale_multiparts()`) | Purge uploads older than 24h |

---


