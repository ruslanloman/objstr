# rawobjstr - Python bindings for rawobjstr

Bypass the filesystem for I/O on raw block devices and loopback files.

## Quick start

```bash
# Build the native extension (Linux only)
pip install maturin
cd rawobjstr/python
maturin develop --release
```

```python
import rawobjstr

# Format a 1 GB store
store = rawobjstr.format("/tmp/store.raw", size=1_073_741_824)

# Format with custom key length limit (e.g. 512 bytes)
store = rawobjstr.format("/tmp/store.raw", size=1_073_741_824, max_key_length=512)

# Format with compression (applies to objects >= 4096 bytes)
store = rawobjstr.format("/tmp/store.raw", size=1_073_741_824, compression="zstd")
# compression options: "none" (default), "zstd", "snappy", "gzip0".."gzip9"

# Write, read, list
store.put("hello.txt", b"Hello, world!")
data = store.get("hello.txt")
for entry in store.list():
    print(entry.location, entry.size)

# Persist and close
store.flush_index()

# Reopen later
store = rawobjstr.open("/tmp/store.raw")
```

## Examples

### Simple CRUD

```python
import rawobjstr

store = rawobjstr.format("/dev/nvme1n1", size=10 * 1024**3)  # 10 GB raw device

# Create
store.put("test/temp.bin", b"\x01\x02\x03\x04")

# Read
data = store.get("test/temp.bin")

# Read a byte range (offset 1, length 2)
chunk = store.get("test/temp.bin", range=(1, 3))
assert chunk == b"\x02\x03"

# Metadata
meta = store.head("test/temp.bin")
print(f"{meta.location}: {meta.size} bytes, modified {meta.last_modified}")

# List
for entry in store.list("test/"):
    print(entry.location, entry.size)

# Delete
store.delete("test/temp.bin")
```

### Multipart upload (streaming large files)

```python
with store.multipart("data/large.parquet") as upload:
    with open("/tmp/export.parquet", "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):  # 8 MB parts
            upload.put_part(chunk)
# auto-completes on context-manager exit
```

### DuckDB + Arrow integration

```python
import duckdb
import pyarrow as pa
import rawobjstr

store = rawobjstr.open("/dev/nvme1n1")

# ---- Write: DuckDB query -> Arrow -> rawobjstr ----
con = duckdb.connect()
table = con.sql("SELECT i AS id, random() AS val FROM range(1_000_000) t(i)").to_arrow_table()
sink = pa.BufferOutputStream()
writer = pa.ipc.new_file(sink, table.schema)
writer.write_table(table)
writer.close()
store.put("analytics/result.arrow", sink.getvalue().to_pybytes())

# ---- Read: rawobjstr -> Arrow -> DuckDB ----
raw = store.get("analytics/result.arrow")
reader = pa.ipc.open_file(pa.BufferReader(raw))
table2 = reader.read_all()
print(duckdb.sql("SELECT count(*), avg(val) FROM table2"))
```

### Parquet round-trip via PyArrow

```python
import pyarrow.parquet as pq, pyarrow as pa, io, rawobjstr

store = rawobjstr.open("/dev/nvme1n1")

# Write a Parquet file into the store
table = pa.table({"x": range(100_000), "y": [3.14] * 100_000})
buf = io.BytesIO()
pq.write_table(table, buf)
store.put("warehouse/events.parquet", buf.getvalue())

# Read it back
raw = store.get("warehouse/events.parquet")
table2 = pq.read_table(io.BytesIO(raw))
assert table2.num_rows == 100_000
```

### Range reads for columnar scans

```python
store = rawobjstr.open("/dev/nvme1n1")

# Read only the first 4 KB (e.g. Parquet footer / header probing)
header = store.get("warehouse/events.parquet", range=(0, 4096))

# Read last 8 bytes (Parquet magic + footer length)
meta = store.head("warehouse/events.parquet")
tail = store.get("warehouse/events.parquet", range=(meta.size - 8, meta.size))
```

### Device diagnostics

```python
store = rawobjstr.open("/dev/nvme1n1")
info = store.device_info()
print(f"Total: {info.device_size / 1024**3:.1f} GB")
print(f"Used:  {info.device_bytes_used / 1024**3:.1f} GB")
print(f"Free:  {info.free_space / 1024**3:.1f} GB")
print(f"Objects:          {info.file_count}")
print(f"Max key length:   {info.max_key_length} bytes")
print(f"Index slot size:  {info.index_slot_capacity // (1024*1024)} MB")
print(f"Compression:      {info.compression}")

# Read raw on-disk bytes without decompression
result = store.getraw("object.bin")
print(f"Raw bytes:        {len(result['data'])} bytes on disk")
print(f"Uncompressed:     {result['uncompressed_size']} bytes (0 = not compressed)")
print(f"Compression:      {result['compression']}")

# Full integrity check
report = store.verify_all()
print(f"{report.files_ok}/{report.files_checked} objects OK")
```

### Open modes

```python
# Default: fast block-0 scan (CRC check per extent)
store = rawobjstr.open("/tmp/store.raw")

# Full verify: reads every payload block and checks all CRCs on open
store = rawobjstr.open("/tmp/store.raw", mode="full_verify")

# Skip verify: no integrity checks - fastest open for trusted devices
store = rawobjstr.open("/tmp/store.raw", mode="skip_verify")
```

### Read-only mode

```python
# Open in read-only mode (O_RDONLY - safe for concurrent readers)
store = rawobjstr.open("/tmp/store.raw", readonly=True)

data = store.get("key.bin")       # reads work
entries = store.list()             # listing works
assert store.is_read_only()

# Mutating operations raise errors:
# store.put("key.bin", b"data")   -> raises IOError
# store.delete("key.bin")         -> raises IOError
# store.flush_index()             -> raises IOError
```

### Metadata operations

```python
store = rawobjstr.open("/tmp/store.raw")

# Write an object, then attach metadata
store.put("events.parquet", parquet_bytes)
store.update_metadata("events.parquet", b'{"schema_v": 2}')

# Read metadata separately (no body I/O)
meta = store.get_metadata("events.parquet")
print(meta)  # b'{"schema_v": 2}'

# Clear metadata
store.update_metadata("events.parquet", b"")

# Store body + metadata together in one call
store.put_with_meta("tagged.bin", b"body-data", b'{"tag": "v1"}')
meta, meta_len = store.head_with_meta("tagged.bin")
print(f"{meta.location}: meta_len={meta_len}")
raw_meta = store.get_metadata("tagged.bin")
print(raw_meta)  # b'{"tag": "v1"}'

# Store from a file that has metadata appended at the end
# (file = body bytes + 12 bytes of metadata at the tail)
store.put_with_meta_from_file("from-file.bin", "/tmp/body_plus_meta.bin", meta_len=12)

# List objects with their metadata lengths (index-only, no data I/O)
for obj_meta, ml in store.list_with_meta("tagged"):
    print(f"{obj_meta.location}: meta_len={ml}")

# Update meta_len in the index without touching data
store.set_meta_len("tagged.bin", 0)  # effectively clears metadata tracking

# List with full extent details (body_size, meta_len, offset, etc.)
for obj in store.list_full("events"):
    print(f"{obj.key}: body={obj.body_size}, meta={obj.meta_len}, offset={obj.offset}")
```

### Device layout and flag management

```python
# Check if store has unflushed changes
if store.needs_flush():
    store.flush_index()

# Re-read on-disk index (for read-only readers tracking a live writer)
changed = store.reload_index()

# Get the full device layout for visualization
layout = store.layout_map()
print(f"Data region: {layout['data_region_start']}-{layout['data_region_end']}")
print(f"Extents: {len(layout['extents'])}, Free regions: {len(layout['free_regions'])}")

# Toggle write-protect or direct-I/O flags without opening
rawobjstr.modify_flags("/tmp/store.raw", set_flags=rawobjstr.FLAG_WRITE_PROTECT)
rawobjstr.modify_flags("/tmp/store.raw", clear_flags=rawobjstr.FLAG_WRITE_PROTECT)
```

## API reference

### Store methods

| Method | Description |
|--------|-------------|
| `put(key, data, *, mode)` | Write an object (`mode='overwrite'` or `'create'`) |
| `get(key, *, range)` | Read an object or byte range |
| `head(key)` | Get object metadata |
| `delete(key)` | Delete an object |
| `list(prefix)` | List all objects (optional prefix filter) |
| `list_with_delimiter(prefix)` | List with directory grouping |
| `copy(src, dst)` | Copy an object |
| `copy_if_not_exists(src, dst)` | Copy if destination does not exist |
| `rename(src, dst)` | Rename (O(1) index-only) |
| `rename_if_not_exists(src, dst)` | Rename if destination does not exist |
| `multipart(key)` | Start a multipart upload (returns context manager) |
| `flush_index()` | Persist index to disk |
| `needs_flush()` | Whether the store has dirty (unflushed) changes |
| `reload_index()` | Re-read on-disk index (for read-only readers tracking live updates) |
| `is_read_only()` | Whether the store was opened in read-only mode |
| `device_info()` | Get device statistics |
| `verify_all()` | Full integrity check |
| `repair()` | Rebuild free list |
| `list_tombstones()` | Get stale entries from integrity scan |
| `delete_tombstone(path)` | Remove a single tombstone |
| `clear_tombstones()` | Remove all tombstones |
| `scrub_free_space()` | Zero all free regions |
| `import_from(source, *, prefix)` | Import objects from another store (optional prefix filter) |
| `export_to(target)` | Export all objects to another store |
| `getraw(key)` | Read raw on-disk bytes without decompression |
| `get_metadata(key)` | Read the metadata suffix bytes for an object |
| `update_metadata(key, metadata)` | Replace metadata suffix without re-uploading the body |
| `put_with_meta(key, data, metadata)` | Store body + metadata together in one call |
| `put_with_meta_from_file(key, path, meta_len)` | Store body + metadata from a file on disk |
| `head_with_meta(key)` | Get (ObjectMeta, meta_len) for an object (index-only) |
| `list_with_meta(prefix)` | List objects with their meta_len values (index-only) |
| `set_meta_len(key, meta_len)` | Update meta_len in the index without touching data |
| `list_full(prefix)` | List all objects with full extent details (offset, padded size, etc.) |
| `layout_map()` | Get the full device extent layout for visualization |

### Module-level functions and constants

| Function / Constant | Description |
|---------------------|-------------|
| `rawobjstr.format(path, *, ...)` | Format a new store |
| `rawobjstr.open(path, *, mode, readonly)` | Open an existing store |
| `rawobjstr.modify_flags(path, ...)` | Toggle superblock flags without opening |
| `rawobjstr.FLAG_DIRECT_IO` | Flag constant for O_DIRECT mode (value: 1) |
| `rawobjstr.FLAG_WRITE_PROTECT` | Flag constant for write-protect (value: 2) |

### Exceptions

| Python Exception | Raised When |
|-----------------|-------------|
| `FileNotFoundError` | Object key not found (`get`, `head`, `get_metadata`, etc.) |
| `FileExistsError` | Key already exists (`put` with `mode='create'`, `copy_if_not_exists`) |
| `OSError` | No space left on device, shard overflow |
| `ValueError` | Invalid arguments, unformatted device, corrupt superblock/index |
| `IOError` | I/O errors, write on read-only store |

### Context manager

```python
with rawobjstr.format("/tmp/s.raw", size=64*1024*1024) as store:
    store.put("key", b"data")
# auto-flushes on clean exit

with store.multipart("big.bin") as upload:
    upload.put_part(chunk1)
    upload.put_part(chunk2)
# auto-completes on clean exit, auto-aborts on exception
```

## fsspec integration

The `rawobjstr://` protocol is registered with [fsspec](https://filesystem-spec.readthedocs.io/),
so any fsspec-aware library (pandas, PyArrow, Dask, Polars, xarray, etc.) can
read and write directly from a raw object store.

```bash
pip install fsspec
```

### Basic usage

```python
import fsspec

# Open a filesystem backed by a raw store
fs = fsspec.filesystem("rawobjstr", path="/dev/nvme1n1")

# Write and read
fs.pipe("hello.txt", b"Hello from fsspec!")
data = fs.cat("hello.txt")

# List objects
for name in fs.ls("data/", detail=False):
    print(name)

# File-like interface (what pandas/PyArrow use internally)
with fs.open("output.bin", "wb") as f:
    f.write(b"some data")

with fs.open("output.bin", "rb") as f:
    content = f.read()
```

### pandas

```python
import pandas as pd

# Read a Parquet file from the raw store
df = pd.read_parquet("rawobjstr:///data.parquet",
                     storage_options={"path": "/dev/nvme1n1"})

# Write back
df.to_parquet("rawobjstr:///output.parquet",
              storage_options={"path": "/dev/nvme1n1"})
```

### PyArrow

```python
import pyarrow.parquet as pq
import fsspec

fs = fsspec.filesystem("rawobjstr", path="/dev/nvme1n1")
with fs.open("table.parquet", "rb") as f:
    table = pq.read_table(f)
```

### Pre-opened store

```python
import rawobjstr
from rawobjstr.fsspec_impl import RawObjStFileSystem

# Reuse an already-opened store
store = rawobjstr.open("/dev/nvme1n1")
fs = RawObjStFileSystem(store=store)
fs.pipe("key", b"value")
```

### Byte-range reads

```python
# Read only bytes 100-199 (useful for Parquet footers)
chunk = fs.cat_file("large.parquet", start=100, end=200)
```

## Async API

`rawobjstr.aio.AsyncStore` wraps the synchronous `Store` and offloads all
blocking calls to the thread-pool executor via `loop.run_in_executor()`.
No Rust changes needed - pure Python async.

```python
import asyncio
import rawobjstr
from rawobjstr.aio import AsyncStore

async def main():
    store = rawobjstr.open("/tmp/store.raw")
    astore = AsyncStore(store)

    await astore.put("key", b"value")
    data = await astore.get("key")
    meta = await astore.head("key")
    await astore.delete("key")

    for entry in await astore.list():
        print(entry.location, entry.size)

asyncio.run(main())
```

All synchronous `Store` methods have async equivalents on `AsyncStore`.
Tests: `tests/test_async.py`.

## Building

```bash
# Development build (editable)
cd rawobjstr/python
maturin develop --release

# Build a wheel
maturin build --release
# -> target/wheels/rawobjstr-0.1.0-cp3X-...-linux_x86_64.whl

# Install from wheel
pip install target/wheels/rawobjstr-*.whl
```

## Running tests

```bash
# Native extension tests (requires maturin develop first)
cd rawobjstr/python
pytest tests/ -v
```

## Project layout

```
rawobjstr/python/
  Cargo.toml              # PyO3 crate
  pyproject.toml           # maturin build config
  src/
    lib.rs                 # PyO3 bindings (Rust -> Python bridge)
  python/
    rawobjstr/
      __init__.py          # Package entry point
      _rawobjstr.pyi        # Type stubs for IDE support
  tests/
    conftest.py
    test_crud.py           # CRUD operations, put modes, copy, rename, multipart, listing
    test_async.py          # AsyncStore wrapper tests
    test_compression.py    # Compression + metadata interaction tests
    test_duckdb.py         # DuckDB/Arrow/Parquet integration
    test_maintenance.py    # getraw, tombstones, verify, repair, scrub, layout_map
    test_metadata_api.py   # Metadata extension API (put_with_meta, head_with_meta, etc.)
    test_persistence.py    # Flush/reopen cycles, alignment boundaries, allocator
    test_range_reads.py    # Byte-range reads, fuzz random ranges
    test_fsspec/
      test_fsspec.py       # fsspec integration tests (also serve as examples)
    examples/              # Skipped by default (need pandas/polars/pyarrow)
      test_basic.py        # Plain rawobjstr + fsspec (pipe/cat, open, ls, copy/mv/rm)
      test_image_metadata.py # Image with thumbnail metadata example
      test_pandas.py       # CSV and Parquet with pandas
      test_polars.py       # CSV, Parquet, and lazy scanning with Polars
      test_pyarrow.py      # Parquet, CSV, IPC/Feather, batch processing with PyArrow
```

To run the example tests (requires extra packages):

```bash
pip install fsspec pandas polars pyarrow
pytest tests/examples/ -v -m examples --override-ini='addopts='
```

## Moving to its own repo

This folder is self-contained. To extract it:

```bash
cp -r rawobjstr/python/ ~/rawobjstr-python/
cd ~/rawobjstr-python/
# Update Cargo.toml: change `path = ".."` to a git dependency or crates.io version
# e.g. rawobjstr = { git = "https://github.com/sysadminmike/objstr", branch = "main" }
```
