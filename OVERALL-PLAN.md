# OVERALL-PLAN.md

## shardedobjstr Roadmap

### Near-term

These are library-level APIs that objstrd (and other consumers) call.
Orchestration (timers, HTTP endpoints, crash-recovery triggers) lives in
the objstrd roadmap.

| # | Item | Notes |
|---|------|-------|
| 18 | `over_replication_trim` shard selection | `pick_excess_shard()` removes from the shard with the least free space. This is non-optimal: it should prefer removing from shards that are most over-represented in the cluster, or consider IO load, not just free space. A better heuristic would factor in shard replica count, health, and proximity to repair-replication targets. |
| 19 | `cross_verify_object` double download | For raw/filesystem shards without an ETag, `get_shard_md5()` first calls `head()` to check for an ETag, then falls back to `get()` and reads the full body. This means the full object is downloaded once for MD5. However, the caller (`cross_verify_all`) also downloads objects via `verify_all` if both are run in sequence. Consider caching digests or combining the two verification sweeps. |
| 20 | `list_with_meta` sequential iteration | `list_with_meta()` on `ShardedObjectStore` (in `metadata.rs`) iterates shards sequentially. For clusters with many shards, this should use concurrent shard scans (like `rebuild_catalog` does) to improve listing performance. |
| 21 | Optional MD5 verification on shard reattach | When `sync_and_reattach` runs the sync phase (mirror or partitioned), it currently compares keys and `last_modified` timestamps to detect stale/missing objects. It does not verify content integrity. Add an optional `verify_on_reattach` flag (config directive or query param on `POST /_admin/attach/{id}?verify=true`) that computes MD5 of every object on the returning shard and compares it against the healthy replica's MD5. Objects with mismatched digests are re-copied from the healthy source. This catches bit-rot or partial writes that occurred while the shard was offline but are invisible to timestamp comparison. Off by default because it requires reading the full body of every object on the shard, which is expensive for large shards. Ties into item 19 (cross_verify digest caching) - reuse `get_shard_md5()` infrastructure. |

### Known Issues (mitigated)

| # | Issue | Status | Notes |
|---|-------|--------|-------|
| C1 | Multipart tracking leak | Mitigated | `ShardedMultipartUpload` registers in `multipart_uploads` on creation but only deregisters on `complete()`. If a client abandons an upload (never calls `complete` or `abort`), the tracking entry leaks. Mitigated by `purge_stale_multiparts()` which runs periodically in `objstrd` and cleans uploads older than the configured expiry. |
| C3 | Vacuum timestamp comparison | Mitigated | `vacuum_delete_markers_inner` compares `meta.last_modified` (from `head_raw`) against the marker's `deleted_at` timestamp. On raw shards, `last_modified` is set at block-allocation time, not at write-completion time, so a slow write could appear older than the marker. Mitigated by requiring all shards to be healthy before vacuum runs (refuses if any shard is offline), so the window for stale comparison is limited to slow-but-healthy writes. |

### Medium-term

| # | Item | Notes |
|---|------|-------|
| 13 | Catalog lock contention | `Catalog` uses a single `RwLock<HashMap>` which serializes all writes under a global lock. Under high-throughput parallel writes this becomes a bottleneck. Replace with `dashmap` or a manually sharded concurrent map so independent keys do not contend. Also consider lock-free reads for the hot `get()` path. |
| 14 | TLV format versioning | Add a version byte prefix to the TLV metadata encoding in `tlv.rs`. Currently no version field exists, so format changes would silently corrupt old metadata. A single-byte prefix (v0 = current format) enables forward-compatible upgrades. |
| 15 | `PutMode::Create` TOCTOU on raw shards | `put_if_not_exists` does a catalog check then writes - a classic time-of-check-time-of-use race. Between the check and the write another client can write the same key, causing a silent overwrite. S3-like shards support conditional puts natively but raw block-device shards do not, so the guarantee is inconsistent across shard types. A correct fix needs either a distributed lock/lease, a two-phase conflict-detection protocol, or accepting weaker semantics on raw shards and documenting it. |
| 17 | `verify_on_read` mode | Optional flag on `ShardedObjectStore` that checks the catalog CRC32c after each GET. The `rawobjstr` layer already verifies block-level CRCs on every read, and `handle_read_error()` + `read_with_fallback!` catch and repair propagated CRC errors. `verify_object()` and `cross_verify_object()` exist for on-demand and sweep checks. This adds a proactive inline check: after a successful `get()`, compute CRC32c of the returned bytes and compare against the catalog entry. On mismatch, schedule read repair (same as `handle_read_error` path). Off by default to avoid the hashing overhead on every read. |

### Smart Write Path - load/capacity-aware placement (Phase 2+)

Phase 1 (health-aware shard selection) is done. See shardedobjstr README.

This is a cluster-wide concern because in a distributed deployment the
"shards" are remote nodes, and the coordinator/index server should make
placement decisions considering:

| Signal | Source | Use |
|--------|--------|-----|
| Free space / fullness | `device_info().free_space` or node heartbeat | Avoid nearly-full shards; weighted placement toward emptier nodes |
| Node load | Heartbeat metrics (CPU, IO queue depth, inflight requests) | Spread writes across less-loaded nodes |
| Network distance | Client IP / node topology | Prefer local rack/region for replica |
| Write latency histogram | Per-shard moving average | Deprioritize consistently slow shards |
| Pending replication queue | Per-shard queue depth | Avoid shards with large replication backlogs |

**Planned approach (Phase 2+):**

1. `select_targets()` gains optional capacity/load signals so it can
   reorder hash ring candidates by weight rather than just skipping
   offline shards.
2. In distributed mode, the coordinator's placement engine provides
   aggregated heartbeat data for weighted selection.

This overlaps with items 9 (consistent hashing upgrade), 10 (rebalancing),
and 11 (free-space-aware placement) above. All should be designed together.

### Smart Read Path (index server routing)

Currently reads use round-robin across replicas (`read_counter % replica_count`)
with sequential fallback on error. This is simple but suboptimal in a
distributed cluster where replicas live on different nodes at different
network distances from the client.

The coordinator/index server should provide a **ranked replica list**
for each GET, considering:

| Signal | Source | Use |
|--------|--------|-----|
| Client location | Client IP, X-Forwarded-For, or edge proxy ID | Prefer replicas on the nearest node/rack/region |
| Node load | Heartbeat metrics (active readers, IO utilization) | Prefer less-loaded replicas |
| Shard health | ShardHealth + error rate counters | Avoid degraded shards, skip offline |
| Cache hotness | Per-node LRU/LFU stats (if available) | Prefer nodes that likely have the data in page cache |
| Network bandwidth | Per-link throughput estimates | Route around congested links |
| Object size | Catalog entry size | For large objects, prefer nodes with more available bandwidth |

**Planned approach:**

1. `GET /_coord/locate?key=<path>` endpoint on the coordinator returns
   an ordered list of `(node_id, address, score)` tuples. The client
   (or edge proxy / objstrproxyd) tries replicas in the given order.
2. Score function: `score = w_dist * distance_penalty + w_load * load_score + w_health * health_penalty`.
   Weights are configurable. Default: strongly prefer proximity, then load.
3. For the local sharded case (single-node multi-shard), the read path
   skips offline shards (already fixed) and uses shard-local load
   metrics if available.
4. objstrproxyd (edge proxy) uses the ranked list to do parallel striped
   range reads from the top-N replicas for maximum throughput on large
   objects.
5. Stale routing: if the coordinator's ranking is stale (node went down
   between ranking and client request), the client falls back to the
   next entry in the list. No hard dependency on coordinator availability
   for reads - the list is a hint, not a requirement.

This connects to Phase D (global placement index), Phase E (client-to-node
data path), and objstrproxyd (edge proxy). Design should be coordinated.

### Preferred Read Path (mirror-aware replica selection)

In a mirror topology (rf = shard_count) every replica holds the full
dataset, so the read path is free to prefer one replica over the others
for every request. This goes beyond the scored ranking above - it lets
the operator declare a **read preference policy** per cluster or per
client that controls which replica is tried first.

**Use cases:**

| Scenario | Policy | Effect |
|----------|--------|--------|
| SSD local + slow HDD mirror + S3 mirror | `prefer: local-ssd` | Always read from the NVMe shard first; only fall back to HDD/S3 on failure |
| Edge node with local cache + remote S3 | `prefer: local` | Read from the co-located shard; avoid cross-network reads unless local is down |
| Multi-region mirrors (us-east, eu-west) | `prefer: nearest` | Client IP / topology tag determines which region's replica is tried first |
| Cost-sensitive (cheap egress vs expensive) | `prefer: cheapest` | Prefer the shard with lowest egress cost (cloud tag) |
| Mixed backends with explicit priority | `prefer: ordered` | Read shards in config order: try raw-nvme first, then filesystem, then R2, then S3. Skip unavailable shards and fall through to the next. Simplest policy for heterogeneous mirrors. |
| Load balancing across identical mirrors | `prefer: round-robin` | Current default behavior - rotate evenly |
| Read throughput maximization | `prefer: least-loaded` | Pick replica with lowest inflight reader count from heartbeat data |
| Latency-optimized | `prefer: lowest-latency` | Pick replica with smallest moving-average read latency |
| Large-object striped reads (3 NVMe, rf=2) | `prefer: striped` | Split range reads across replicas in parallel - each replica serves different byte ranges. Maximizes aggregate read bandwidth for large objects by using all available drives. |

**Read preference resolution order:**

1. Per-request hint (optional `X-Read-Prefer` header or query param) -
   allows the client or edge proxy to override policy per request.
2. Per-bucket or per-prefix policy (tree config) - lets operators set
   different preferences for different data paths.
3. Cluster-wide default policy (tree config top level or env var).
4. Fallback: round-robin (current behavior).

**Tree config syntax (proposed):**

```
cluster  hybrid-mirror
bucket   project-data
read_prefer  local        # cluster-wide default

master  rf=3  listen=0.0.0.0:8000  endpoint=http://localhost:8000
  fs   /data/nvme-store    tags=local,ssd
  s3   endpoint=https://...r2.cloudflarestorage.com  bucket=...  tags=remote,cheap-egress
  s3   endpoint=https://s3.us-east-1.amazonaws.com   bucket=...  tags=remote,expensive-egress
```

Tags on shards allow the preference engine to match policies like
`prefer: tag=ssd` or `prefer: tag=cheap-egress` without hard-coding
shard indices.

**Config-defined read order (`prefer: ordered`):**

The simplest preference for heterogeneous mirrors: read shards in the
order they appear in the config file. The operator lists backends from
fastest/cheapest to slowest/most-expensive. The read path tries each
shard in that order, skipping any that are offline or unhealthy, and
returns the first successful response.

```
cluster  hybrid-mirror
bucket   project-data
read_prefer  ordered       # use config order as read priority

master  rf=4  listen=0.0.0.0:8000  endpoint=http://localhost:8000
  raw  /dev/nvme0n1                                          # 1st choice: local raw NVMe
  fs   /mnt/data-store                                       # 2nd choice: local filesystem
  s3   endpoint=https://ACCT.r2.cloudflarestorage.com  ...   # 3rd choice: R2 (cheap egress)
  s3   endpoint=https://s3.us-east-1.amazonaws.com     ...   # 4th choice: S3 (expensive egress)
```

With `read_prefer ordered`, a GET for any key tries raw NVMe first. If
that shard is offline, it tries the filesystem shard. If that is also
down, it tries R2, then S3. Writes always go to all 4 shards (rf=4).
This gives the operator full control over read priority with zero
runtime heuristics - just list the backends in the order you want them
read.

**Striped range reads across replicas (`prefer: striped`):**

For large objects, a single replica cannot saturate the aggregate
bandwidth of multiple drives. When the cluster has N shards and rf < N
(e.g. 3 NVMe drives with rf=2), each object lives on 2 of the 3 drives.
A naive read picks one replica and reads the full object from that
single drive. A striped read splits the byte range into chunks and
reads different chunks from different replicas in parallel, using all
drives that hold the object simultaneously.

Example: 3 NVMe shards, rf=2. Object X lives on shards [0, 2].

```
Client requests: GET object X (100 MB)

Non-striped (current):  shard 0 serves all 100 MB  -> ~500 MB/s
Striped (rf=2):         shard 0 serves bytes 0-49 MB
                        shard 2 serves bytes 50-99 MB
                        -> ~1000 MB/s aggregate
```

This applies to both range reads (`get_range()`) and full-object reads.
The S3 `Range` header maps naturally: split the requested range into
sub-ranges, issue parallel `GET` requests with `Range` headers to each
replica, reassemble the responses in order.

When combined with the preference engine, `striped` mode uses all
healthy replicas for parallel reads. If a replica fails mid-read,
the remaining replicas absorb its chunks (fallback to fewer stripes).
For mirror mode (rf = shard_count), striping can use all N shards,
giving maximum bandwidth.

This is the same pattern that objstrproxyd implements at the edge proxy
level, but here it runs inside the sharded object store itself for
clients that connect directly to objstrd.

**Implementation sketch:**

1. `ReadPreference` enum: `RoundRobin` (**DONE**), `Ordered` (**DONE**), plus future variants:
   `Striped`, `Local`, `Tagged(String)`, `Nearest`,
   `LeastLoaded`, `LowestLatency`, `Explicit(ShardId)`.
   - `Ordered` (**DONE**): use the shard indices as listed in the config (0, 1, 2, ...).
     No scoring, no runtime signals - pure static priority. The first
     healthy shard in config order wins. This is the recommended default
     for heterogeneous mirror setups where the operator knows upfront
     which backend is preferred.
   - `Striped` (future): split range reads across all healthy replicas in parallel.
     Each replica serves a different byte range of the same object.
     Chunk size is configurable (default 4 MB). For small objects (below
     chunk size), falls back to single-replica read using the preference
     chain. For large objects, issues parallel `get_range()` calls to
     each replica and reassembles. Maximizes aggregate read throughput
     when multiple fast drives hold the same object.
   - **Grouped (future)**: mix ordered and round-robin in one policy by
     grouping shards with parentheses. Shards inside a group are
     round-robined; groups are tried in order (first group exhausted
     before falling through to the next).

     ```
     read_prefer  ordered
       (raw /dev/nvme0n1, raw /dev/nvme1n1), s3 endpoint=https://...
     ```

     Here `raw /dev/nvme0n1` and `raw /dev/nvme1n1` form a
     round-robin group - reads alternate between them for load
     balancing. If both raw shards are down, the read falls through
     to the S3 shard. This lets operators get round-robin across
     identical fast backends while still maintaining ordered fallback
     to slower/remote backends. Groups can contain any number of
     shards; ungrouped shards are treated as single-element groups.
2. `ShardedObjectStore::get()` resolves the preference through the
   resolution chain above, sorts the replica list accordingly, then
   falls back through the list on error (same as today but with a
   smarter initial ordering).
3. In distributed/coordinator mode, the `/_coord/locate` response
   includes the preference-adjusted ordering so the requesting node
   or edge proxy does not need to know shard tags - the coordinator
   resolves them.
4. For the local multi-shard case, preference is resolved in-process
   using the shard tags from the tree config.
5. Node state signals (load, latency, health) are fed from heartbeat
   data (coordinator mode) or local metrics (single-node mode). Stale
   signals are acceptable - the fallback chain ensures correctness.

**Interaction with objstrproxyd:** objstrproxyd does parallel striped reads
for large objects at the edge proxy level. The preference engine provides
a ranked replica list; objstrproxyd uses the top-N from that list for its
parallel stripes while still respecting the preference ordering for
tie-breaking. When `prefer: striped` is set on the cluster itself,
objstrd performs striping internally - objstrproxyd then sees a single
fast source and can layer its own cross-node striping on top for
multi-node clusters. The two levels compose: intra-node striping
(across local drives) and inter-node striping (across remote nodes).

### Caching Tiers - Mem/NVMe Shards as Cache in Front of Large S3 Buckets

When dealing with huge S3 buckets (TBs+) where full replication to local
storage is impractical, the sharded store can use mem or NVMe shards as
caching tiers rather than full replicas. The S3 shard holds the
authoritative data; local shards act as bounded caches with eviction.

**Read cache (mem or NVMe shard with LRU eviction):**

A mem shard (or a small NVMe shard) acts as a transparent read cache.
On GET, check the cache shard first; on miss, fetch from S3, write a
copy to the cache shard, and return. When the cache shard fills up,
evict least-recently-used objects to make room. The cache shard is not
a full replica -- it holds a hot subset of the bucket.

Eviction policies to support:

| Policy | Description |
|--------|-------------|
| LRU | Evict least-recently-read objects first. Best general-purpose policy. |
| LFU | Evict least-frequently-read objects. Better for skewed access patterns. |
| Creation-time | Keep only objects newer than a threshold (e.g. last 24h). Good for time-series or append-heavy workloads where recent data is hot. |
| Prefix-pinned | Pin certain prefixes in cache (e.g. `_versions/`, `_indices/`), evict others via LRU. Useful for LanceDB where manifests/indices are tiny but read on every query. |
| Size-bounded LRU | Standard LRU but with a max total cache size. Evict oldest entries when the cache exceeds the bound. |

The cache shard participates in the normal read preference chain --
`read_prefer ordered` with the cache shard listed first gives
cache-first reads with S3 fallback, zero config beyond shard ordering.

Cache metadata (access timestamps, frequency counters) tracked in a
lightweight index alongside or within the catalog. Eviction runs as a
background task or inline on cache-full during a write.

**Write cache (volatile mem shard for batching small writes):**

A mem shard absorbs bursts of small PUTs without immediately writing to
S3. Objects accumulate in memory, then a background flush task streams
them to the S3 shard in batches. This amortizes per-object S3 overhead
(HTTP round trips, multipart initiation) and is useful for workloads
that produce thousands of small objects quickly (e.g. LanceDB writes,
log ingestion, ETL intermediate files).

Trade-offs:
- **Volatile**: mem shard contents are lost on restart. Only suitable
  for workloads where jobs can be rerun if the write cache is lost
  (batch ETL, ML training checkpoints, reproducible pipelines).
- **Flush strategies**: time-based (every N seconds), count-based
  (every N objects), size-based (every N MB accumulated), or
  on-demand (`POST /_admin/flush-write-cache`).
- **Consistency**: objects in the write cache are readable immediately
  from the mem shard. They appear in listings. But they are not
  durable until flushed to S3. A `X-Cache-State: pending-flush`
  header or metadata tag could indicate unflushed objects.
- **NVMe write cache**: same concept but with an NVMe shard instead of
  mem. Survives restarts (not volatile) but still evicts to S3 once
  the local shard fills. Hybrid of durability and speed.

**Deployment patterns for large S3 buckets:**

| Pattern | Shards | RF | read_prefer | Use case |
|---------|--------|----|-------------|----------|
| Read-through cache | mem + S3 | 1 (S3 authoritative) | ordered (mem first) | Hot-subset caching for huge read-heavy buckets. Mem shard has LRU eviction. |
| NVMe read cache | NVMe + S3 | 1 (S3 authoritative) | ordered (NVMe first) | Same but cache survives restarts. Good for multi-TB buckets with ~100GB hot set. |
| Write-absorb + S3 | mem + S3 | 1 (flush to S3) | ordered | Batch small writes in mem, stream to S3 in background. Jobs must be rerunnable. |
| NVMe staging + S3 | NVMe + S3 | 1 (flush to S3) | ordered | Durable local staging, async replication to S3. Safer than mem write cache. |
| Tiered read + write | mem + NVMe + S3 | varies | ordered | Mem for hot reads, NVMe for warm, S3 for cold. Writes go to NVMe + S3; mem populated on read. |
| Time-window cache | NVMe + S3 | 1 | ordered | Only cache objects created in the last N hours. Evict older objects. Good for time-series. |
| Prefix-selective cache | NVMe + S3 | 1 | ordered | Cache metadata prefixes fully (`_versions/`, `_indices/`), LRU for data prefixes. |

**Key design questions:**

- Cache coherence: when the upstream S3 object is overwritten by an
  external writer, the local cache becomes stale. Options: TTL-based
  expiry, S3 event notifications, or periodic `head()` checks
  against S3 to detect changes. Ties into the S3 Metadata Tables
  polling item (objstrd standalone roadmap).
- Eviction granularity: per-object (simple, flexible) vs per-prefix
  (evict entire prefix trees at once, useful for LanceDB dataset
  versions).
- Cache warm-up: on startup, optionally pre-populate the cache from
  S3 based on a manifest, a prefix filter, or a previous access log.
- Catalog interaction: cache-only objects need to be distinguishable
  from fully-replicated objects in the catalog. Add a
  `CacheState { Cached, PendingFlush, Authoritative }` field to
  `PlacementEntry`.

**Connections to existing items:**
- "Tiered storage" (Parked) -- read-side tiering via `read_prefer ordered`
  already supports the read path; this adds eviction and write caching.
- "Read cache for hot objects" (Production Hardening) -- that item is a
  simple in-process LRU for small objects; this is a shard-level cache
  backed by a full mem or NVMe ObjectStore.
- "S3 write-through proxy with local cache" (objstrd Standalone) -- that
  is a single-node daemon feature with synchronous write-through; this
  covers async write-absorb and broader shard-level caching patterns.

### Put-with-Replacement (overwrite in distributed mode)

When a node receives a PUT for a key that already exists in the cluster,
the write must replace the object at all locations where it currently
lives -- not just write to the hash-selected shards for the new content.
This is critical for correctness: if the old object lives on shards
[0, 2] but the new hash selects [1, 2], a naive put would leave a
stale copy on shard 0 and miss shard 1.

**Two cases depending on how the PUT arrives:**

**Case 1: PUT arrives at the master/coordinator node**

The master owns the global catalog. Flow:

```
1. Client sends PUT key=X to master
2. Master checks catalog: does key X exist?
   a. NO  -> normal put path: select_shards(X), write to all, update catalog
   b. YES -> catalog returns PlacementEntry { shards: [old_locations], ... }
3. Compute new placement: select_shards(X) -> [new_locations]
4. Merge: write_targets = union(old_locations, new_locations)
   - Shards in new_locations get the new object written
   - Shards in old_locations that are NOT in new_locations get a DELETE
     for the old object (cleanup stale replicas)
5. Update catalog: PlacementEntry { shards: new_locations, ... }
```

For mirror mode (rf = shard_count), old_locations == new_locations ==
all shards, so the merge step is trivially "overwrite everywhere."

**Case 2: PUT arrives at a non-master node directly**

The receiving node does not own the global catalog: it cannot know
whether the object exists elsewhere or where its replicas live. Flow:

```
1. Client sends PUT key=X to node N (not the master)
2. Node N asks the master/coordinator: GET /_coord/placement?key=X
   Response: { "exists": bool, "locations": [shard_ids], "placement": [new_shard_ids] }
3. If exists == false:
   a. Normal put: node N writes to its local shard(s) per placement
   b. Notifies master: POST /_coord/notify { op: "put", key: X, shards: [...] }
4. If exists == true:
   a. Node N receives old locations AND new placement from master
   b. Node N writes new object to all shards in new_locations
      (for remote shards: PUT to their S3 endpoint; for local: direct write)
   c. Node N deletes stale copies: for each shard in old_locations
      that is NOT in new_locations, send DELETE to that shard
   d. Node N notifies master: POST /_coord/notify { op: "replace", key: X,
      old_shards: [...], new_shards: [...] }
   e. Master updates catalog
```

**Race condition handling:**

Concurrent PUTs for the same key are a risk. Two nodes could both query
the master, both get the same old_locations, and both try to write/delete.
Mitigation options (pick one or combine):

| Strategy | Tradeoff |
|----------|----------|
| Master-side lock per key | Simple, serializes concurrent overwrites for the same key. Small lock held only during placement query + write confirmation. Coordinator returns a write_token that must be passed back in the notify call; stale tokens are rejected. |
| Fencing via catalog version | Each PlacementEntry has a monotonic version. The notify includes expected_version; coordinator rejects if version has advanced. Losing writer retries. |
| Last-writer-wins (no lock) | Simplest. Concurrent writes may leave brief inconsistency (extra replicas). Background consistency scan cleans up. Acceptable for many workloads. |

Recommended default: **master-side lock per key** for correctness, with
a configurable timeout (default 30s). If the lock holder crashes, the
lock expires and the next writer proceeds. The lock is lightweight (just
a HashSet of locked keys on the coordinator).

**Coordinator endpoint (proposed):**

```
GET  /_coord/placement?key=<path>
Response:
{
  "exists": true,
  "locations": [0, 2],       // current shard IDs holding the object
  "new_placement": [1, 2],   // where the new object should go per hash
  "write_token": "abc123",   // lock token, must be returned in notify
  "token_expires": "2026-04-01T12:00:30Z"
}

POST /_coord/notify
Body:
{
  "op": "replace",
  "key": "path/to/object",
  "write_token": "abc123",
  "old_shards": [0, 2],
  "new_shards": [1, 2]
}
```

**Implementation order:**

1. Add `/_coord/placement` endpoint to coordinator (Phase D dependency)
2. Add write_token lock table to coordinator (in-memory HashMap with expiry)
3. Modify node PUT path: before writing, query coordinator for placement
4. Implement merge logic (write new + delete stale) in node's put handler
5. Add `/_coord/notify` with op=replace support
6. Tests: concurrent overwrites, lock expiry, stale replica cleanup

This is a Phase E concern (client-to-node data path) but the coordinator
side (placement query + lock table) belongs in Phase D.

### Parked
- **Cluster-assisted defragmentation** -- Evacuate-and-repack or rolling
  compaction of individual shards (see above).
- **Failure domains** -- Ensure replicas land on different disks/hosts.
- **Metrics / observability** -- Per-shard latency, replication lag, free space (partial: free space per shard tracked and exposed via `/_admin/info`/`/_admin/shards`; missing: latency histograms, replication lag, Prometheus/OTel export).
- **Tiered storage** -- Read-side tiering is done via `read_prefer ordered` (try fast shards first, fall back to slow). Write-side tiering (route hot data to NVMe, cold to HDD based on access patterns or storage class) is not implemented. Also planned: **size-based placement** -- small objects get fully replicated across shards for durability, while large objects are stored on a single shard (no replication) to save space. The write path would use a configurable size threshold to decide rf per object, and the operator could designate which shards receive small vs large objects (e.g. fast NVMe for small hot objects, large HDD/S3 for bulk data).

> **Structured logging** (categories, ring buffer, file log, REST API) is
> documented in [objstrd/ADMIN-API.md](objstrd/ADMIN-API.md) under "Logging".
> CLI flags are in [objstrd/CLI.md](objstrd/CLI.md) under "Logging".



---

## objstrd Standalone -- Remaining Work

| Item | Priority | Status |
|------|----------|--------|
| Crash-recovery invalidation trigger -- on shard reopen after crash, call `invalidate_shard()` to re-sync catalog with reality | Medium | Not started |
| CRC failure counter + escalation -- track per-shard CRC failures, mark Degraded at threshold (configurable, default 10/hour) | Medium | **Partial.** `crc_error_count` (AtomicU64) per shard is tracked and incremented on CRC read errors. Missing: automatic escalation to Degraded status when the count exceeds a threshold. |
| `POST /_admin/invalidate?shard=N` -- HTTP endpoint to manually trigger `invalidate_shard()` for a local shard | Medium | Not started |
| Cross-shard CRC verification task -- periodic task calls `verify_replicas()` across objects, quarantines mismatches | Low | **Partial.** `cross_verify_all()` and `verify_all()` exist for manual/CLI invocation. No periodic background task. |
| SSE-S3 / SSE-C stubs -- return correct headers so clients that request encryption don't error; no actual encryption | Low |
| ACL / bucket policy stubs -- `get_object_acl` and `get_bucket_acl` done; `put_bucket_policy` still missing | Low |
| CLI S3 integration (M7) -- `rawobjstr import --from s3://` and `export --to s3://` done; `rawobjstr serve` not yet implemented | Low |
| S3 write-through proxy with local cache -- use `BACKEND=s3` as a caching proxy in front of an upstream S3/R2 bucket. PUTs are synchronous write-through: the request succeeds only after both the local RawObjectStore cache and the upstream bucket confirm the write. GETs check the local cache first and fall back to upstream on a miss; cached objects are evicted via a DuckDB LRU index to bound local disk usage. Use case: low-latency local reads with durable upstream storage, edge caching for S3/R2 buckets, or placing a fast local tier in front of a shared cloud bucket. | Low |
| S3 Metadata Tables change tracking -- for S3-type shards backed by shared buckets that other processes may write to, periodically poll the AWS S3 Metadata Tables journal to detect external creates, overwrites, and deletes. Refresh the local index for affected keys without a full bucket re-listing. | Low |
| Flush-after-write option -- `--flush-on-write` / `FLUSH_ON_WRITE=true` flag that calls `flush_index()` after every PUT and DELETE. For single-replica deployments where the 5-second flush interval is not durable enough. | Low |
| Multipart temp extent cleanup -- **Partial.** Lazy reaping in `create_multipart_upload()` purges stale uploads older than `upload_expiry` before enforcing the concurrent upload cap. Missing: a dedicated periodic background GC timer so orphaned extents are cleaned even when no new uploads arrive. | Low |

---

## Production Hardening

Items not yet in a design doc but needed before production at scale.

| Item | Priority | Notes |
|------|----------|-------|
| Write timeout per shard | Medium | `write_to_shards()` uses `join_all()` with no per-shard timeout. One hung shard blocks the entire PUT indefinitely. Add `tokio::time::timeout` around each shard future. |
| Write backpressure / concurrency limit | Medium | **Partial.** `repair_semaphore` (32 permits) bounds concurrent read-repair I/O. Normal write path (`write_to_shards`) still has no semaphore or bounded channel -- under sustained client load, memory grows without bound. Add a `tokio::sync::Semaphore` with a configurable permit count for client writes. |
| Retry with exponential backoff | Medium | Shard writes get a single retry (mark offline, pick new target). Transient failures (HTTP 5xx, connection reset) should retry with configurable backoff before giving up. |
| Connection pool configuration | Low | S3 shard clients use default `reqwest` pool settings. Expose pool size, idle timeout, and per-shard read/write timeouts as config options. |
| ~~Catalog integrity protection~~ | ~~Low~~ | **DONE.** Both JSON and bincode catalog files embed a CRC32c checksum (JSON in an envelope, bincode as a 4-byte LE prefix). Checksum is validated on load. Remaining: keep one `.bak` generation so a corrupt primary can fall back. |
| Access logging / audit trail | Low | No record of read/write/delete operations. Add optional structured access log (JSON lines to file or stdout) with timestamp, operation, key, client IP, request headers, status code, latency, and bytes transferred. Especially valuable in proxy mode: route all S3 traffic through objstrd to get a unified audit log across multiple upstream S3 accounts. Enables compliance, cost attribution, and debugging when multiple teams share buckets. |
| Read cache for hot objects | Low | Every GET reads from disk. Add an optional bounded LRU cache for small hot objects (metadata files, manifests). Especially useful for metadata-heavy workloads (LanceDB). |
| **Metadata suffix cache in S3 adapter** | Medium | Every GetObject (including ranged GETs) calls `get_metadata()` to read the TLV suffix. For workloads like LanceDB that issue hundreds of small range reads per scan, this doubles the I/O per request. Add an in-memory LRU cache keyed by object path with a short TTL (5-10 s). Invalidate on PUT/DELETE. This is the single biggest optimization for scan throughput through the S3 layer -- the Lance bench showed 3.6-9.2x overhead vs direct access, and redundant metadata reads are a large contributor. |
| **Under-replicated objects in web UI** | Medium | **Partial.** `/_admin/recovery` returns `under_replicated_count` and `over_replicated_count` (numbers only). `shardedobjstr report` in full mode prints shard IDs per object. Remaining: add a dedicated `/_admin/under-replicated` JSON endpoint returning the full list with shard IDs (not just count). The `--under-replicated` report filter should also print shard IDs (full report mode does, but the filter omits them). Add a live table to `/_admin/cluster` dashboard fed by the new endpoint. |

---

## objstrd Cluster Roadmap

### Phase C: Node registration and heartbeat

Nodes announce to the coordinator and send periodic health updates.

- Heartbeat sender (node): periodic POST with node_id, address,
  capacity, free space
- Heartbeat receiver (coordinator): `POST /_coord/heartbeat`
- Node registry (coordinator): `HashMap<NodeId, NodeInfo>` with
  last_seen timestamps
- Grace period: configurable timeout before marking node failed
- Extend `/_admin/info` with node_id, address, capacity
- **Version check on node join:** When a node sends its first heartbeat
  (or registers via `POST /_coord/heartbeat`), it includes its
  `build_git_hash` and crate version (from `/_admin/info`). The
  coordinator compares these against its own version. If they differ,
  the coordinator rejects the join with a clear error message
  ("node version X does not match coordinator version Y") and the node
  logs the rejection and exits (or retries, depending on config).
  This prevents protocol/format mismatches from causing silent data
  corruption when nodes run different builds. Configurable via
  `--skip-version-check` / `SKIP_VERSION_CHECK=true` on the
  coordinator to allow mixed-version rolling upgrades.

### Phase D: Global placement index

Coordinator maintains a cluster-wide catalog of which nodes hold each
object.

- Global catalog on coordinator (extends `shardedobjstr::Catalog`)
- Node index pull: coordinator queries each node's S3 listing on startup
- Index update: `POST /_coord/notify` -- node reports writes/deletes
- Node-triggered index invalidation: `POST /_coord/invalidate` -- node
  requests coordinator to drop and rebuild catalog entries for that node
  only (e.g. after crash recovery, detected corruption, or manual command).
  Coordinator marks node Degraded during re-scan, triggers re-replication
  for any objects that were in the catalog but missing from the node's
  fresh listing, then marks node Healthy. CRC mismatch alerts propagate
  as drive-health signals to the coordinator dashboard.
- Endpoints: `/_coord/info`, `/_coord/nodes`, `/_coord/placement`,
  `/_coord/invalidate`

See `DISTRIBUTED-OPERATIONS.md` for the full invalidation protocol
(local multi-shard and coordinator modes) and self-healing pipeline.

### Parked (Phases E-N)

- E: Client-to-node data path
- F: Replication orchestration (sync/async)
- G: Buddy lists (coordinator-down resilience) -- buddies provide the
  replica source for re-replication after a shard/node invalidation.
  When a node invalidates its index, the buddy/replica set is used to
  recover any objects that were lost on the invalidating node.
- H: Coordinator recovery and reconciliation
- I: Coordinator HA (standby)
- J: Repair-replication and defragmentation
- J': Object redistribution (`redistribute_sweep`) -- **DONE.**
  Balances object counts across shards by moving objects from the
  fullest to the emptiest shard until within a configurable tolerance
  (default 10%). Library API in `shardedobjstr`, admin endpoint
  `POST /_admin/redistribute`, Python binding `redistribute()`.
  See `DISTRIBUTED-OPERATIONS.md` section 11c. This is basic
  count-based balancing; hash-ring-aware rebalancing after topology
  changes (item J above) remains parked.
  **Future:** Redistribute should be fragmentation-aware on raw
  devices. If a raw shard is nicely packed (few or zero free
  fragments), do not move objects off it -- removing objects creates
  holes/fragments that hurt sequential-read performance and waste
  allocator metadata. Prefer moving objects off shards that are
  already fragmented, or off non-raw backends (fs, mem, S3) where
  fragmentation does not apply.
- K: Conflict resolution
- L: Hierarchical topology (vdev-style nesting)
- M: Spot instance + S3 backing store -- **DONE.**
  Background repair-replication task in `objstrd` periodically calls
  `rebuild_catalog()` then `repair_replication_sweep()` to populate empty local
  shards (e.g. fresh NVMe) from S3 replicas. Config: `repair_replication_interval`
  / `repair_replication_batch_size` (tree config, CLI, env). Admin status at
  `/_admin/repair-replication-status`, visible on the cluster dashboard.
  Objects become queryable on the local shard incrementally as each one
  is copied (via `catalog.add_replica()` inside `replicate_object()`).
  E2E tests: raw+S3, stripe+S3, S3-to-S3, admin endpoint.
- N: Observability

Also parked: objstrproxyd (edge fetch proxy) -- detailed plan exists in
`objstrproxyd/PLAN.md`, no code.

**TODO:** Add CLI subcommands (`take-offline`, `attach`) to the `shardedobjstr`
binary for the Take Offline / Detach feature. These should call the daemon's
`/_admin/take-offline/{id}` and `/_admin/attach/{id}` HTTP endpoints (requires
adding `reqwest` dependency to `shardedobjstr`). The admin endpoints and Web UI
are already implemented; only the CLI wrapper is missing.

**See `DISTRIBUTED-OPERATIONS.md`** for the consolidated reference covering
crash recovery, replication, index invalidation, buddy lists, lights-out
operation, self-healing pipeline, and manual operations.

---

## Step-by-Step Forward Plan

Steps 1-6 complete. See objstrd and shardedobjstr READMEs.

### Step 7: Heterogeneous auto-recovery cluster (remaining items)

Most of Step 7 is done (health polling, mirror sync, partitioned sync,
proactive re-replication, recovery e2e tests). See `DISTRIBUTED-OPERATIONS.md`
for the full recovery protocol and timelines.

**Remaining:**

| # | Item | Depends on | Status | Notes |
|---|------|------------|--------|-------|
| 7c | Write quiesce during re-attach | 7b/7b' | Not started | Brief write pause so no objects are missed between sync completion and `attach_shard()`. Flow: (1) sync completes, (2) pause writes (reject PUTs with 503 for a short window), (3) run a final delta sync (mirror: re-run `mirror_sync()` which is fast when nearly identical; partitioned: replicate any new objects written during sync), (4) `attach_shard()`, (5) resume writes. The pause should be sub-second for small deltas. Alternative: accept a small window of under-replication instead of pausing (configurable). Currently the alternative is what happens -- sync runs once with no quiesce. |
| 7d | Admin endpoints for recovery | 7a-7c | **Mostly done** | `GET /_admin/recovery` -- DONE. `POST /_admin/drain/:shard_id` -- DONE. `POST /_admin/take-offline/:shard_id` -- DONE. `POST /_admin/attach/:shard_id` -- DONE. `POST /_admin/vacuum` -- DONE. `POST /_admin/redistribute` -- DONE. Remaining: `POST /_admin/sync?shard=N` (trigger sync without re-attach / dry run). |
| 7f | Delete marker auto-vacuum (partitioned mode) | -- | **Partial** | `POST /_admin/vacuum` exists for manual trigger. `shardedobjstr vacuum` CLI exists. Remaining: auto-trigger vacuum after successful re-attach when all shards are healthy, so stale markers are cleaned without operator intervention. Not needed for mirror mode. |

**Recovery timeline -- mirror mode (rf = shard_count):**

```
t=0    S3 shard goes unreachable (network, provider outage)
t=5s   Health poll detects failure (3 consecutive probe failures),
       calls detach_shard(shard_id)
       offline_since set to now automatically
       Cluster continues at rf=2 (degraded but functional)
       PUTs write to 2 healthy shards, catalog records partial placement

t=???  S3 shard becomes reachable again
t+5s   Health poll detects recovery, calls sync_and_reattach():
         shard marked Syncing
         mirror_sync() via ObjectStore API:
           list source shard -> list target shard
           copy missing objects (preserving metadata)
           update stale objects (source newer than target)
           delete objects on target not present on source
t+Ns   Sync complete, attach_shard() called (force=true)
       rebuild_catalog_for_shard(), shard marked Healthy
       Cluster back to rf=3
       (No write quiesce -- small window of under-replication accepted;
        see item 7c for planned improvement)
```

**Recovery timeline -- partitioned mode (rf < shard_count):**

```
t=0    Shard goes unreachable
t=5s   Health poll detects failure (3 consecutive probe failures),
       calls detach_shard(shard_id)
       offline_since set to now automatically
       Cluster continues at rf-1 for affected objects
       Delete markers (__deleted__/{key} objects) written to all
       healthy shards on each DELETE during outage

t=???  Shard becomes reachable again
t+5s   Health poll detects recovery, calls sync_and_reattach():
         shard marked Syncing
         partitioned_sync() runs:
           find_under_replicated() -> list of objects to copy
           replicate_object() for each missing object
           replay delete markers (compare last_modified vs deleted_at)
           update stale objects where catalog shows newer version
           clean stale markers no longer needed
t+Ns   Sync complete, attach_shard() called (force=true)
       rebuild_catalog_for_shard(), shard marked Healthy
       (No write quiesce -- see item 7c)
       Delete marker vacuum available manually via
       `shardedobjstr vacuum` (not auto-triggered; see item 7f)
```

**Implementation order:** 7c (correctness), then 7d remaining
(`/_admin/sync`), then 7f (auto-vacuum after re-attach).

### Step 8: Phase C -- Node registration and heartbeat

- Heartbeat sender (node), receiver (coordinator)
- Node registry with grace period

Files: new `objstrd/src/heartbeat.rs`

### Step 9: Phase D -- Global placement index

- Coordinator-side catalog, node index pull, `/_coord/*` endpoints

Files: `objstrd/src/bin/server.rs`, new `objstrd/src/coordinator.rs`

---

## Event Stream Improvements

The event socket currently broadcasts `PUT <key>`, `DELETE <key>`, and
`FLUSH <shard_id> <txn_id>` as plain text lines. `subscribe_streaming_replica`
exists for keeping a read-only mirror in sync via the event bus (DONE).

Planned enhancements (not started):

- **Add timestamp to every event.** Each event line should include a
  UTC timestamp (epoch micros or ISO 8601) so subscribers know exactly
  when the operation occurred, not just when they received the message.
  Format: `PUT <timestamp> <key>`, `DELETE <timestamp> <key>`,
  `FLUSH <timestamp> <shard_id> <txn_id>`.

- **Add shard ID to PUT and DELETE events.** Currently only FLUSH
  carries a shard_id. In a sharded cluster, subscribers need to know
  which shard the event took place on (e.g. for targeted
  `reload_index()` or per-shard cache invalidation). Format becomes:
  `PUT <timestamp> <shard_id> <key>`,
  `DELETE <timestamp> <shard_id> <key>`.
  Possibly add `GET` requests as well.
  
- **Overall plan:** The event socket should evolve into a lightweight
  change stream that external consumers (read-only replicas, CDC
  pipelines, monitoring) can subscribe to and get a complete,
  ordered feed of mutations with enough context (timestamp, shard,
  key) to act without querying the store. This is the foundation for
  streaming replication, real-time cache invalidation, and audit
  logging.

---

## Not In Scope

- objstrproxyd (edge proxy)
- Server-side encryption
- Object versioning
- ACLs / bucket policy
- Phases E-N of cluster roadmap
