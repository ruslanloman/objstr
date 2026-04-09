# Example: Heterogeneous Auto-Recovery Cluster

A single master objstrd with `rf=3` across three heterogeneous shards: a local
filesystem store, a Cloudflare R2 bucket, and an AWS S3 bucket. The developer
connects to the master's S3 endpoint on port 8000. Every PUT writes to all
3 shards. If any shard goes offline, reads and writes continue against the
remaining replicas. When the offline shard comes back, it is automatically
synced and re-attached with no manual intervention.

This is the showcase for the pipeline architecture: any S3-compatible store
can be a shard, mixed freely, with built-in auto-recovery.

## Config

**hybrid-mirror.conf:**

```
cluster  hybrid-mirror
bucket   project-data

master  rf=3  listen=0.0.0.0:8000  endpoint=http://localhost:8000
  fs   /data/local-store
  s3   endpoint=https://ACCOUNT_ID.r2.cloudflarestorage.com  bucket=project-data  region=auto  access_key=...  secret_key=...
  s3   endpoint=https://s3.us-east-1.amazonaws.com  bucket=project-data  region=us-east-1  access_key=...  secret_key=...
```

```bash
objstrd --config hybrid-mirror.conf --node master
```

Clients talk to `http://localhost:8000`. Behind the scenes the
ShardedObjectStore writes every object to all three shards (local fs, R2,
and AWS S3) and reads from whichever responds first.

## What Works Today

- Tree config with mixed shard types (fs + s3 + s3)
- `rf=3` replication: PUT writes to all 3 shards in parallel
- Degraded mode: if a shard is unreachable, PUT succeeds on remaining
  shards, catalog records partial placement
- Read fallback across replicas
- Manual `detach_shard()` / `attach_shard()` / `invalidate_shard()`
- `find_under_replicated()` and `replicate_object()` for manual recovery
- Shard health polling: background task pings each shard every N seconds;
  auto-detaches on failure, triggers re-attach on recovery
- Mirror-mode sync (rf = shard_count): copies missing objects and deletes
  stale objects from healthy shard to recovering shard via ObjectStore API
- Partitioned-mode sync (rf < shard_count): `find_under_replicated()` +
  `replicate_object()` for gap repair
- Proactive re-replication: when a shard goes offline, copies
  under-replicated objects to healthy shards to restore RF
- Over-replication trim: detects excess copies and removes them

## Remaining Work (see Step 7 in OVERALL-PLAN.md)

| Item | Description |
|------|-------------|
| Write quiesce during re-attach | Brief write pause so no objects are missed between sync completion and re-attach |
| Admin endpoints | `POST /_admin/detach?shard=N`, `POST /_admin/attach?shard=N`, `GET /_admin/recovery` for manual control |
| Dashboard integration | Shard health states, recovery progress, under-replicated counts in `/_admin/cluster` |
| Delete log (partitioned mode) | Append-only ledger for replay during partitioned-mode recovery |

## Recovery Flow (Mirror Mode, rf=3, 3 shards)

Since rf = shard_count, every shard is a full mirror. Recovery uses
`aws s3 sync --delete` directly between S3 endpoints -- each shard is
already an S3 endpoint (objstrd node, AWS, or R2), so the sync runs
natively. This handles both new objects and deletes that happened while
the shard was offline.

```
  R2 goes down         health poll detects     R2 comes back
       |                     |                      |
       v                     v                      v
  cluster at rf=3 --> rf=2 (degraded) -------> aws s3 sync --delete
                      PUTs still work           from healthy shard to R2
                      GETs still work               |
                                                    v
                                              brief write pause
                                              final delta sync
                                                    |
                                                    v
                                              attach_shard()
                                              cluster back to rf=3
```

For partitioned clusters (rf < shard_count), `aws s3 sync` cannot be used
because shards hold different subsets. Those use internal replication +
a delete log. See OVERALL-PLAN.md Step 7 for details.

## Why This Design

- **Any backend can be a shard.** fs, raw block device, mem, AWS S3, R2,
  MinIO, GCS (via S3 compat), another objstrd. Mix and match.
- **Mirror mode uses `aws s3 sync --delete`.** Each shard is an S3 endpoint,
  so sync runs directly between them. Handles deletes without tombstones or
  internal delete logs. Same tool teams already use for S3 workflows.
- **Partitioned mode uses internal replication + delete log.** When shards
  hold different subsets, the catalog knows which objects belong where and a
  delete log tracks removals for replay during recovery.
- **Atomic re-attach.** The write quiesce ensures no objects slip through the
  gap between sync and re-attach. The pause is sub-second for typical deltas.
- **Shows the pipeline architecture.** Each shard is an independent
  ObjectStore. The ShardedObjectStore ties them together with consistent
  hashing, replication, and catalog tracking. Recovery adapts to the
  replication mode automatically.
