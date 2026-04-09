# AGENTS.md - objstrd

> **Common workspace hints** (coding rules, VM setup, Windows notes, binary swap
> detection) are in [`/AGENTS.md`](../AGENTS.md) at the repo root.

## Project Overview

`objstrd` is an S3-compatible distributed object store daemon backed by
`rawobjstr`. It translates AWS S3 API calls (SigV4 auth, presigned URLs,
multipart upload, range GETs) into `ObjectStore` operations via the `s3s` 0.13
library. Supports three roles: standalone (single node), node (data node in a
cluster), and coordinator (cluster orchestrator).

---

## Dependency on shardedobjstr

objstrd depends on `shardedobjstr` for all shard routing, catalog,
and replication. objstrd's old `distribution.rs` and `catalog.rs` were
deleted. What remains in objstrd:

- `adapter.rs` -- Uses `shardedobjstr::metadata` directly for
  `RawRefRegistry` + `ShardKind` enum + metadata-aware routing functions
  (`put_with_meta`, `head_with_meta`, `get_metadata`, `list_with_meta`,
  `set_meta_len`, `put_with_meta_from_file`, `delete_sidecar`). Branches
  on `ShardKind` (Raw/S3Like/Sidecar) to use the right metadata strategy.
  Includes retry logic on write failure.
- `registry.rs` -- `StoreEntry`, `StoreKind`, `StoreRegistry`
- `config.rs` -- tree config parser + JSON/env config, builds
  `ShardedObjectStore` + `RawRefRegistry` with per-shard `ShardKind`
- `recovery.rs` -- background shard health polling, auto-sync, and
  proactive re-replication. Spawned via `spawn_recovery_task()`. The actual
  repair algorithms live in `shardedobjstr::repair` so they can also
  be used by `shardedobjstr` and the Python bindings.

---

## VM Paths

- Source: `~/objstr/objstrd`
- Build dir: `CARGO_TARGET_DIR=~/build-objstrd`
- Release binary: `~/build-objstrd/release/objstrd`

### Verify the Build Version

After deploying and building, confirm the binary picked up your changes:

```bash
curl http://localhost:8080/_admin/info
# Returns JSON with: git_hash, build_date, crate_version
```

---

## Documentation Index

| Document | Contents |
|----------|----------|
| [README.md](README.md) | Quick start, architecture, modules, features, use cases |
| [BUILD.md](BUILD.md) | Building objstrd from source |
| [CLI.md](CLI.md) | CLI flags and environment variables |
| [ADMIN-API.md](ADMIN-API.md) | Admin endpoints, query params, HTML pages |
| [METADATA-STORAGE-FORMAT.md](METADATA-STORAGE-FORMAT.md) | TLV wire format, shard-type storage strategy |
| [CONFIG.md](../CONFIG.md) | Unified config reference |
| [TESTS.md](TESTS.md) | Running the test suite & S3 compatibility tests |
| [benchmarks/README.md](benchmarks/README.md) | Benchmark suites (catalog, lance, listing, rclone) |
