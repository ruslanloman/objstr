# AGENTS.md - rawobjstr

> **Common workspace hints** (coding rules, VM setup, Windows notes) are in [`/AGENTS.md`](../AGENTS.md) at the repo root.

## Project Overview

`rawobjstr` is a Rust crate implementing the `ObjectStore` trait that writes
directly to raw block devices or loopback image files. 

---

## Related Docs

| Doc | Contents |
|-----|----------|
| [README.md](README.md) | Features, use cases, quickstart, CLI usage examples |
| [API.md](API.md) | Full Rust API reference: constructors, store operations, ObjectStore trait, OpenMode, metadata extensions |
| [STORAGE-FORMAT.md](STORAGE-FORMAT.md) | On-disk layout, superblock format, index region, data encoding |
| [CLI.md](CLI.md) | CLI command reference with all options and examples |
| [TESTS.md](TESTS.md) | Full test catalog |
| [BUILD.md](BUILD.md) | Building instructions, prerequisites, version baking |
| [PERFORMANCE.md](PERFORMANCE.md) | Benchmark results across storage backends |
| [EVENT-SOCKET.md](EVENT-SOCKET.md) | Unix domain socket event protocol (PUT, DELETE, FLUSH notifications) |
| [LANCEDB.md](LANCEDB.md) | LanceDB integration guide: setup, Rust API, crash safety, portable images |
| [benchmarks/](benchmarks/) | Benchmark results |
| [python/README.md](python/README.md) | Python bindings: installation, sync/async API, fsspec, DuckDB/PyArrow/pandas |

---

## VM Paths

See the **Per-crate Paths** table in [`/AGENTS.md`](../AGENTS.md) for source path,
`CARGO_TARGET_DIR`, and deploy/build commands.

- Source: `~/objstr/rawobjstr`
- Build dir: `CARGO_TARGET_DIR=~/build-rawobjstr`

### Running Tests

```bash
# All tests (release mode recommended - some tests are slow in debug)
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release

# Specific test file
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release --test basic_ops

# Specific test by name
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release --test edge_cases -- test_zero_byte_file

# With output (for benchmarks / debugging)
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release --test perf_bench -- --nocapture

# Unit tests only (in src/ modules)
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release --lib

# DataFusion/Parquet test (requires --features datafusion, slow to compile)
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release --features datafusion --test datafusion_parquet
```

### Feature Flags

| Feature | Adds | Used by |
|---------|------|---------|
| `lance` | lancedb, lance-io, lance-core, arrow-array, arrow-schema, url | `bench_lance` example |
| `datafusion` | datafusion, arrow, parquet | `datafusion_parquet` test only |
| `aws` | object_store S3 backend | S3-backend support |

The `datafusion` feature pulls in ~200 crates (datafusion, arrow, parquet) and
adds significant compile time. It is only needed for the `datafusion_parquet`
integration test. Normal `cargo test --release` skips it automatically.

**Agent rule:** Do NOT run feature-gated tests (`datafusion`, `lance`) unless
the user explicitly asks for them. When the user says "run the tests", run
only `cargo test --release` (no extra features). If you think the feature-gated
tests are relevant, ask the user first and warn them the build will take
significantly longer due to the extra dependencies.

### Running the CLI Tool

See [CLI.md](CLI.md) for the full command reference.

```bash
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo build --release --bin rawobjstr
~/build-rawobjstr/release/rawobjstr --help
```

### Running Benchmarks

```bash
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test --release --test perf_bench -- --nocapture
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo run --release --example bench_storage
```

---

## Architecture

```
RawObjectStore (implements ObjectStore trait)
  |
  +---> RwLock<Inner>
          |-- DeviceIo        (pread/pwrite, O_DIRECT, aligned buffers)
          |-- DeviceIndex     (in-memory file manifest: HashMap<String, ExtentInfo>)
          |-- ExtentAllocator (free list, first-fit, coalescing)
          +-- Superblock      (device metadata, dual-write recovery)
```

**Single RwLock design:** All mutable state is behind one `parking_lot::RwLock<Inner>`.
Readers (get, list, head, export_to) take a read lock and can run concurrently.
Writers (put, delete, copy, flush) take a write lock, serializing mutations.

### Key Internal Algorithms

These are the internal implementation steps (not in API.md, which covers the public contract):

- **put**: alloc extent (first-fit) -> CRC32c -> encode into 4 KB blocks -> write in 1 MB batches -> free old extent if overwriting -> update index -> set dirty
- **get**: index lookup -> read+decode CRC-protected blocks in 1 MB batches -> verify block CRCs + payload CRC -> if `meta_len > 0`, truncate trailing metadata bytes so callers see body-only content
- **head/list**: `meta_for()` returns body-only size (`on_disk_size - meta_len`), so `Content-Length` and list sizes never include the metadata trailer
- **delete**: returns `Ok(())` for missing keys (ObjectStore contract); frees extent + removes from index
- **rename**: O(1) index-only key move, no data I/O
- **multipart**: parts spooled to temp disk extents (`__raw_multipart_tmp/{upload_id}/{part_num}`), concatenated on complete. Lives in the raw store (not objstrd) so direct Rust consumers (LanceDB, DataFusion) get multipart support.
- **flush_index**: serialize index+free list via bincode -> write to inactive index slot -> update superblock -> write both superblock copies -> clear dirty

---

## Python Bindings (`python/`)

PyO3 bindings wrapping the full Rust API. Built via `maturin develop --release` on the VM.
Build target: `~/build-pyrawobjstr`.

When you add, remove, or change any public API in `store.rs` or `lib.rs`, update:
- `python/src/lib.rs` (PyO3 bindings)
- `python/python/rawobjstr/_rawobjstr.pyi` (type stubs)
- `python/python/rawobjstr/__init__.py` (re-exports)

When you add integration tests in `tests/`, add matching Python tests in
`python/tests/test_store.py`.
