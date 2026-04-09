#!/usr/bin/env python3
"""
Benchmark: Lance large dataset write + scan on 6 backends.

Builds a ~10 GB dataset via chunked writes (1M rows per chunk, 20 chunks)
to exceed the 3.1 GB VM RAM and force real disk I/O on every scan.

Six backends (matching PERFORMANCE.md):
  1. ext4          -- Linux ext4, writes go to page cache
  2. ext4+sync     -- ext4 with sync after each chunk write
  3. img+directio  -- RawObjectStore 16 GB image, O_DIRECT
  4. img+buffered  -- RawObjectStore 16 GB image, buffered
  5. raw+directio  -- RawObjectStore /dev/sdb, O_DIRECT
  6. raw+buffered  -- RawObjectStore /dev/sdb, buffered

Scan workload:
  - Full count_rows (metadata-only scan)
  - Full stream scan (all columns, batched)
  - Filtered scan: name LIKE 'A%' (starts_with)
  - Column projection: id + amount only
  - Point filter: category = 42
"""

import argparse
import gc
import os
import shutil
import string
import subprocess
import sys
import time

import numpy as np
import pyarrow as pa

# ---- configuration -------------------------------------------------------

CHUNK_ROWS = 1_000_000   # rows per write chunk
N_CHUNKS = 20            # 20M rows total
IMG_SIZE = 16 * 1024**3  # 16 GB image file
PAYLOAD_SIZE = 256       # random binary bytes per row (incompressible)
# Per-row size: id(8) + name(~12) + city(~8) + amount(8) + category(4)
#               + payload(256) = ~296 bytes
# 20M rows * 296 = ~5.5 GB Arrow in-memory, ~10 GB on disk with Lance overhead

CITIES = [
    "London", "Paris", "Berlin", "Madrid", "Rome", "Vienna", "Prague",
    "Warsaw", "Dublin", "Lisbon", "Oslo", "Helsinki", "Athens", "Zurich",
    "Brussels", "Amsterdam", "Stockholm", "Copenhagen", "Budapest", "Bucharest",
]

LETTERS = list(string.ascii_uppercase)


def make_chunk(chunk_idx, n_rows):
    """Generate one chunk. Freed after write to stay within RAM."""
    offset = chunk_idx * n_rows
    rng = np.random.default_rng(42 + chunk_idx)

    ids = np.arange(offset, offset + n_rows, dtype=np.int64)
    amounts = (rng.standard_normal(n_rows) * 1000 + 5000).astype(np.float64)
    categories = rng.integers(0, 100, size=n_rows, dtype=np.int32)

    # Names: "Xuser_NNNNNN" -- ~1/26 start with 'A' for LIKE 'A%' filter
    letters = rng.choice(LETTERS, size=n_rows)
    suffixes = rng.integers(100000, 999999, size=n_rows)
    names = pa.array(
        [f"{letters[i]}user_{suffixes[i]}" for i in range(n_rows)],
        type=pa.utf8(),
    )

    cities = pa.array(
        rng.choice(CITIES, size=n_rows).tolist(), type=pa.utf8(),
    )

    # Random binary payload -- incompressible, forces real I/O
    # Generate as a single buffer and slice for performance
    raw = rng.bytes(n_rows * PAYLOAD_SIZE)
    payload = pa.array(
        [raw[i * PAYLOAD_SIZE:(i + 1) * PAYLOAD_SIZE] for i in range(n_rows)],
        type=pa.binary(),
    )

    return pa.table({
        "id": ids,
        "name": names,
        "city": cities,
        "amount": amounts,
        "category": categories,
        "payload": payload,
    })


def fmt_size(nbytes):
    gb = nbytes / (1024**3)
    if gb >= 1.0:
        return f"{gb:.2f} GB"
    return f"{nbytes / (1024**2):.1f} MB"


def fmt_rate(nbytes, elapsed):
    mb = nbytes / (1024**2)
    return f"{mb / elapsed:,.1f} MB/s"


def fmt_time(elapsed):
    if elapsed < 1.0:
        return f"{elapsed * 1000:.1f} ms"
    return f"{elapsed:.2f} s"


def drop_caches():
    try:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
    except PermissionError:
        pass


def do_sync():
    subprocess.run(["sync"], check=False)
    subprocess.run(["sync"], check=False)
    subprocess.run(["sync"], check=False)


def evict_page_cache():
    """Write a 4 GB junk file to evict all page-cache contents, then drop
    caches and remove it.  This ensures no residual cached data from writes
    or previous scans affects the next read benchmark."""
    do_sync()
    drop_caches()
    # Write 4 GB via dd (faster than Python write loop)
    cache_buster = "/tmp/cache_buster.bin"
    subprocess.run(
        ["dd", "if=/dev/zero", f"of={cache_buster}", "bs=1M", "count=4096"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
    )
    do_sync()
    drop_caches()
    try:
        os.unlink(cache_buster)
    except OSError:
        pass


# ---- backend helpers ------------------------------------------------------

class Ext4Backend:
    def __init__(self, base_dir, sync_writes=False):
        tag = "ext4+sync" if sync_writes else "ext4"
        self.uri = os.path.join(base_dir, f"lance_{tag}_bench")
        self.name = tag
        self.object_store = None
        self.sync_writes = sync_writes

    def setup(self):
        if os.path.exists(self.uri):
            shutil.rmtree(self.uri)

    def cleanup(self):
        if os.path.exists(self.uri):
            shutil.rmtree(self.uri)


class RawObjStoreBackend:
    def __init__(self, path, size, direct_io, label):
        self.path = path
        self.size = size
        self.direct_io = direct_io
        self.name = label
        self.uri = "memory:///lance-bench"
        self.object_store = None
        self._store = None
        self.sync_writes = False

    def setup(self):
        from lance_objstore_adapter import create_store
        self._store, self.object_store = create_store(
            self.path, size=self.size, direct_io=self.direct_io
        )

    def cleanup(self):
        if self._store is not None:
            self._store.flush_index()
        self._store = None
        self.object_store = None
        if self.size is not None and os.path.exists(self.path):
            os.unlink(self.path)


# ---- benchmark runner -----------------------------------------------------

def run_benchmark(backend, n_chunks, chunk_rows):
    import lance

    results = {}
    total_rows = n_chunks * chunk_rows
    total_bytes_written = 0

    # -- Chunked write -------------------------------------------------------
    print(f"  writing {n_chunks} x {chunk_rows:,} rows...")
    write_start = time.monotonic()
    for i in range(n_chunks):
        chunk = make_chunk(i, chunk_rows)
        chunk_bytes = sum(c.nbytes for c in chunk.columns)
        total_bytes_written += chunk_bytes

        mode = "create" if i == 0 else "append"
        if backend.object_store:
            lance.write_dataset(
                chunk, backend.uri, mode=mode,
                object_store=backend.object_store,
            )
        else:
            lance.write_dataset(chunk, backend.uri, mode=mode)

        if backend.sync_writes:
            do_sync()

        del chunk
        gc.collect()

        elapsed_so_far = time.monotonic() - write_start
        rate = total_bytes_written / elapsed_so_far / (1024**2)
        print(f"    chunk {i+1:>2}/{n_chunks}: "
              f"{fmt_size(total_bytes_written):>8s} written, "
              f"{rate:>7.1f} MB/s cumulative")

    write_elapsed = time.monotonic() - write_start
    results["write"] = write_elapsed
    results["write_bytes"] = total_bytes_written
    results["write_rate"] = total_bytes_written / write_elapsed
    results["write_rows"] = total_rows
    print(f"  WRITE TOTAL:    {fmt_time(write_elapsed):>10s}  "
          f"({fmt_rate(total_bytes_written, write_elapsed)}, "
          f"{fmt_size(total_bytes_written)})")

    # Evict page cache so scans start cold
    print("  evicting page cache...", end="", flush=True)
    evict_page_cache()
    print(" done")

    # -- Full count_rows (metadata scan) -------------------------------------
    evict_page_cache()
    print("  count_rows...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)
    row_count = ds.count_rows()
    elapsed = time.monotonic() - t0
    results["count_rows"] = elapsed
    results["count_rows_n"] = row_count
    print(f" {row_count:,} rows in {fmt_time(elapsed)}")

    # -- Full stream scan (all columns, batched) -----------------------------
    evict_page_cache()
    print("  full stream scan...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)
    stream_rows = 0
    stream_bytes = 0
    for batch in ds.to_batches():
        stream_rows += len(batch)
        stream_bytes += sum(c.nbytes for c in batch.columns)
    elapsed = time.monotonic() - t0
    results["stream_scan"] = elapsed
    results["stream_scan_rate"] = stream_bytes / elapsed
    results["stream_scan_bytes"] = stream_bytes
    results["stream_scan_rows"] = stream_rows
    print(f" {stream_rows:,} rows ({fmt_size(stream_bytes)}) in "
          f"{fmt_time(elapsed)} ({fmt_rate(stream_bytes, elapsed)})")

    # -- Filtered scan: name LIKE 'A%' (starts_with) -------------------------
    evict_page_cache()
    print("  filter: name LIKE 'A%'...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)
    filt_rows = 0
    filt_bytes = 0
    for batch in ds.to_batches(filter="starts_with(name, 'A')"):
        filt_rows += len(batch)
        filt_bytes += sum(c.nbytes for c in batch.columns)
    elapsed = time.monotonic() - t0
    results["filter_like"] = elapsed
    results["filter_like_rows"] = filt_rows
    results["filter_like_bytes"] = filt_bytes
    print(f" {filt_rows:,} matching rows in {fmt_time(elapsed)}")

    # -- Column projection: id + amount only ---------------------------------
    evict_page_cache()
    print("  column projection (id, amount)...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)
    proj_rows = 0
    proj_bytes = 0
    for batch in ds.to_batches(columns=["id", "amount"]):
        proj_rows += len(batch)
        proj_bytes += sum(c.nbytes for c in batch.columns)
    elapsed = time.monotonic() - t0
    results["col_project"] = elapsed
    results["col_project_rate"] = proj_bytes / elapsed
    results["col_project_rows"] = proj_rows
    results["col_project_bytes"] = proj_bytes
    print(f" {proj_rows:,} rows ({fmt_size(proj_bytes)}) in "
          f"{fmt_time(elapsed)} ({fmt_rate(proj_bytes, elapsed)})")

    # -- Point filter: category = 42 -----------------------------------------
    evict_page_cache()
    print("  filter: category = 42...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)
    point_rows = 0
    for batch in ds.to_batches(filter="category = 42"):
        point_rows += len(batch)
    elapsed = time.monotonic() - t0
    results["point_filter"] = elapsed
    results["point_filter_rows"] = point_rows
    print(f" {point_rows:,} matching rows in {fmt_time(elapsed)}")

    return results


# ---- main -----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Lance large scan benchmark (10+ GB, 6 backends)")
    parser.add_argument("--chunks", type=int, default=N_CHUNKS,
                        help=f"Number of chunks (default: {N_CHUNKS})")
    parser.add_argument("--chunk-rows", type=int, default=CHUNK_ROWS,
                        help=f"Rows per chunk (default: {CHUNK_ROWS:,})")
    parser.add_argument("--img-size", type=int, default=IMG_SIZE,
                        help=f"Image file size bytes (default: {IMG_SIZE})")
    parser.add_argument("--device", type=str, default="/dev/sdb",
                        help="Raw block device (default: /dev/sdb)")
    parser.add_argument("--ext4-dir", type=str, default="/tmp",
                        help="Dir for ext4 benchmarks (default: /tmp)")
    parser.add_argument("--backends", type=str, default="all",
                        help="Comma-separated backends or 'all'")
    args = parser.parse_args()

    total_rows = args.chunks * args.chunk_rows
    est_bytes = total_rows * (8 + 12 + 8 + 8 + 4 + PAYLOAD_SIZE)
    print(f"=== Lance Large Scan Benchmark ===")
    print(f"chunks={args.chunks}  chunk_rows={args.chunk_rows:,}  "
          f"total_rows={total_rows:,}")
    print(f"estimated dataset: ~{fmt_size(est_bytes)}  "
          f"payload={PAYLOAD_SIZE} bytes/row")
    print()

    all_backends = {
        "ext4": Ext4Backend(args.ext4_dir, sync_writes=False),
        "ext4+sync": Ext4Backend(args.ext4_dir, sync_writes=True),
        "img+directio": RawObjStoreBackend(
            "/tmp/lance_scan_dio.img", args.img_size, True, "img+directio"),
        "img+buffered": RawObjStoreBackend(
            "/tmp/lance_scan_buf.img", args.img_size, False, "img+buffered"),
        "raw+directio": RawObjStoreBackend(
            args.device, None, True, "raw+directio"),
        "raw+buffered": RawObjStoreBackend(
            args.device, None, False, "raw+buffered"),
    }
    backend_order = [
        "ext4", "ext4+sync",
        "img+directio", "img+buffered",
        "raw+directio", "raw+buffered",
    ]

    if args.backends == "all":
        selected = backend_order
    else:
        selected = [b.strip() for b in args.backends.split(",")]

    all_results = {}
    for name in selected:
        backend = all_backends[name]
        print(f"{'=' * 60}")
        print(f"  BACKEND: {name}")
        print(f"{'=' * 60}")
        try:
            backend.setup()
            results = run_benchmark(backend, args.chunks, args.chunk_rows)
            all_results[name] = results
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  ERROR: {e}")
            all_results[name] = {"error": str(e)}
        finally:
            backend.cleanup()
        print()

    # ---- Summary tables ---------------------------------------------------
    names = [n for n in selected if n in all_results]

    print("=== Summary: Write ===")
    header = f"{'backend':<16s}{'time':>10s}{'MB/s':>10s}{'data':>10s}"
    print(header)
    print("-" * len(header))
    for n in names:
        r = all_results[n]
        if "error" in r:
            print(f"{n:<16s}  ERROR")
        else:
            print(f"{n:<16s}"
                  f"{fmt_time(r['write']):>10s}"
                  f"{r['write_rate']/(1024**2):>9.1f}"
                  f"{fmt_size(r['write_bytes']):>10s}")
    print()

    print("=== Summary: Scan Times ===")
    ops = [
        ("count_rows", "count"),
        ("stream_scan", "stream"),
        ("filter_like", "LIKE 'A%'"),
        ("col_project", "id+amount"),
        ("point_filter", "cat=42"),
    ]
    header = f"{'backend':<16s}" + "".join(f"{label:>12s}" for _, label in ops)
    print(header)
    print("-" * len(header))
    for n in names:
        r = all_results[n]
        if "error" in r:
            print(f"{n:<16s}  ERROR")
            continue
        cols = []
        for key, _ in ops:
            t = r.get(key)
            if t is not None:
                cols.append(f"{fmt_time(t):>11s}")
            else:
                cols.append(f"{'n/a':>11s}")
        print(f"{n:<16s}" + "".join(cols))

    print()
    print("=== Summary: Stream Scan Throughput ===")
    header = f"{'backend':<16s}{'MB/s':>10s}{'data':>10s}"
    print(header)
    print("-" * len(header))
    for n in names:
        r = all_results[n]
        if "error" in r:
            continue
        rate = r.get("stream_scan_rate", 0)
        sz = r.get("stream_scan_bytes", 0)
        print(f"{n:<16s}{rate/(1024**2):>9.1f}{fmt_size(sz):>10s}")
    print()


if __name__ == "__main__":
    main()
