# AGENTS.md - shardedobjstr

> **Common workspace hints** (coding rules, VM setup, Windows notes, binary swap detection) are in [`/AGENTS.md`](../AGENTS.md) at the repo root.

---

## VM Paths

See the **Per-crate Paths** table in [`/AGENTS.md`](../AGENTS.md) for source path,
`CARGO_TARGET_DIR`, and deploy/build commands.

- Source: `~/objstr/shardedobjstr`
- Build dir: `CARGO_TARGET_DIR=~/build-sharded`

### Running Tests

```bash
CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test --release

# Specific test
CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test --release --test cluster_e2e

# With output
CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test --release --test perf_bench -- --nocapture
```

---

## Documentation Map

| Doc | Contents |
|-----|----------|
| [README.md](README.md) | Architecture, features, quick start, example topologies |
| [API.md](API.md) | Full Rust API reference: constructors, store operations, catalog, repair, metadata, types |
| [CLI.md](CLI.md) | CLI command reference for `shardedobjstr` and `shardedobjstr-catview` |
| [CONFIG.md](../CONFIG.md) (unified config reference) |
| [TESTS.md](TESTS.md) | Full test catalog |
| [benchmarks/lance/README.md](benchmarks/lance/README.md) | Lance dataset benchmarks: Python vs Rust, six storage backends |
| [python/README.md](python/README.md) | Python bindings: formatting, CRUD, cluster management, metadata |


---

## Python Bindings (`python/`)

PyO3 bindings for `ShardedObjectStore`. Built via `maturin develop --release`.

When you add, remove, or change public API, update:
- `python/src/lib.rs` (PyO3 bindings)
- `python/python/shardedobjstr/_shardedobjstr.pyi` (type stubs)
- `python/python/shardedobjstr/__init__.py` (re-exports)

---

## Two Write Paths -- Keep in Sync

There are two separate replication loops for writing objects:

1. **Normal puts/deletes** in `src/lib.rs`: `write_to_shards_inner()` and
   `put_delete_marker()`.  These handle generic `ObjectStore::put` payloads.

2. **Metadata-aware puts** in `src/metadata.rs`: `put_with_meta()` and
   `put_with_meta_from_file()`.  These handle metadata per shard kind
   (Raw/S3Like/Sidecar) and cannot use the generic path.

Both paths enforce `min_writes`.  When changing replication logic, quorum
checks, shard health marking, or write error handling, **update both paths**.
`delete_sidecar()` in metadata.rs is best-effort cleanup and does not enforce
min_writes (the actual delete goes through `put_delete_marker`).

**Sync checklist -- these must match in both paths:**

- [ ] `min_writes` enforcement (minimum successful shard writes before Ok)
- [ ] `delete_requires_min_writes` enforcement on delete paths
- [ ] Shard health transitions (marking shards Degraded or Offline on failure)
- [ ] Cleanup-on-failure (rollback partial writes on new keys when quorum not met, including sidecar `.__meta__` files for Sidecar-kind shards)
- [ ] Retry with fresh targets after all initial targets fail
- [ ] Catalog update call (`catalog.put()` with placed shards, crc, meta_len)

---

## copy() Metadata Preservation

`ShardedObjectStore::copy()` reads the source via `get()`, then checks for
metadata via `read_object_metadata()`. If `raw_refs` is configured AND the
source has non-empty metadata, it writes via `put_with_meta()` to preserve
metadata. Otherwise it falls back to a plain `put()` (no metadata).

The `objstrd` adapter also handles copy with metadata preservation in its
own `copy_object()` method. If you change copy behavior in either location,
keep them consistent.

---

## Catalog Changes

- `catalog.put()` takes 5 parameters: `(key, shards, size, crc32c, meta_len)`.
  The `meta_len: u16` field (serde default 0) allows existing catalogs to
  deserialize without migration.
- `catalog.try_insert()` is used for `PutMode::Create` races: it atomically
  inserts a new key only if it does not already exist, preventing two writers
  from both succeeding the "not exists" check.
