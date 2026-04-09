#!/usr/bin/env python3
"""
Lance benchmark: objstrd S3 HTTP layer overhead.

Measures the cost of the S3 HTTP protocol layer (s3s, SigV4, hyper, TCP
loopback) by running the same LanceDB workload as the rawobjstr and
shardedobjstr benchmarks, but connecting through objstrd's S3 endpoint
instead of accessing the store directly.

For each backend, the script:
  1. Starts an objstrd process with the chosen backend
  2. Waits for the /_admin/info endpoint to respond
  3. Runs the LanceDB workload via S3 storage_options
  4. Kills objstrd and cleans up

Six backends:
  1. fs           -- objstrd --backend fs (LocalFileSystem via S3)
  2. mem          -- objstrd --backend mem (in-memory, pure HTTP overhead)
  3. img+directio -- objstrd --image <path> --direct-io (O_DIRECT via S3)
  4. img+buffered -- objstrd --image <path> (buffered via S3)
  5. raw+directio -- objstrd --image /dev/sdb --direct-io (raw device via S3)
  6. raw+buffered -- objstrd --image /dev/sdb (raw device via S3)

Scan workload (identical to rawobjstr/shardedobjstr benchmarks):
  - Full count_rows (metadata-only scan)
  - Full stream scan (all columns, batched)
  - Filtered scan: name LIKE 'A%' (starts_with)
  - Column projection: id + amount only
  - Point filter: category = 42

Usage (run on the Linux VM):
  # All 6 backends (requires root for /dev/sdb and drop_caches)
  sudo python3 bench_write_scan.py --device /dev/sdb --backends all

  # Single backend
  python3 bench_write_scan.py --backends fs

  # In-memory only (fast smoke test)
  python3 bench_write_scan.py --backends mem --chunks 5

  # Image-only (no root needed for device)
  python3 bench_write_scan.py --backends img+directio,img+buffered
"""

import argparse
import gc
import os
import shutil
import signal
import string
import subprocess
import sys
import time

import numpy as np
import pyarrow as pa

# ---- configuration -------------------------------------------------------

CHUNK_ROWS = 1_000_000   # rows per write chunk
N_CHUNKS = 20            # 20M rows total
IMG_SIZE_MB = 16384       # 16 GB image file
PAYLOAD_SIZE = 256       # random binary bytes per row (incompressible)
DEFAULT_PORT = 8900
OBJSTRD_BIN = "objstrd"
BUCKET = "testbucket"
ACCESS_KEY = "benchkey"
SECRET_KEY = "benchsecret"

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

    letters = rng.choice(LETTERS, size=n_rows)
    suffixes = rng.integers(100000, 999999, size=n_rows)
    names = pa.array(
        [f"{letters[i]}user_{suffixes[i]}" for i in range(n_rows)],
        type=pa.utf8(),
    )

    cities = pa.array(
        rng.choice(CITIES, size=n_rows).tolist(), type=pa.utf8(),
    )

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
    """Write a 4 GB junk file to evict all page-cache contents."""
    do_sync()
    drop_caches()
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


# ---- objstrd process management -------------------------------------------

def start_objstrd(args_list, objstrd_bin):
    """Start an objstrd process and return the Popen handle."""
    cmd = [objstrd_bin] + args_list
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def wait_for_ready(port, timeout=30):
    """Poll /_admin/info until objstrd is ready."""
    import urllib.request
    url = f"http://localhost:{port}/_admin/info"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def stop_objstrd(proc):
    """Stop the objstrd process."""
    if proc is None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait(timeout=5)


def get_storage_options(port):
    """Return lance storage_options dict for connecting to objstrd."""
    return {
        "aws_endpoint": f"http://localhost:{port}",
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
        "region": "us-east-1",
        "allow_http": "true",
        "aws_s3_allow_unsafe_rename": "true",
    }


# ---- backend definitions -------------------------------------------------

class S3Backend:
    """A backend that starts objstrd with specific flags."""

    def __init__(self, name, objstrd_args, port, objstrd_bin, cleanup_paths=None):
        self.name = name
        self.objstrd_args = objstrd_args
        self.port = port
        self.objstrd_bin = objstrd_bin
        self.cleanup_paths = cleanup_paths or []
        self.proc = None
        self.uri = f"s3://{BUCKET}/lance-bench"
        self.storage_options = get_storage_options(port)

    def setup(self):
        self.proc = start_objstrd(self.objstrd_args, self.objstrd_bin)
        if not wait_for_ready(self.port):
            stop_objstrd(self.proc)
            raise RuntimeError(
                f"objstrd did not become ready on port {self.port} "
                f"(cmd: {self.objstrd_bin} {' '.join(self.objstrd_args)})"
            )

    def cleanup(self):
        stop_objstrd(self.proc)
        self.proc = None
        for path in self.cleanup_paths:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.isfile(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass


# ---- benchmark runner -----------------------------------------------------

def run_benchmark(backend, n_chunks, chunk_rows):
    import lance

    results = {}
    total_rows = n_chunks * chunk_rows
    total_bytes_written = 0

    uri = backend.uri
    opts = backend.storage_options

    # -- Chunked write -------------------------------------------------------
    print(f"  writing {n_chunks} x {chunk_rows:,} rows...")
    write_start = time.monotonic()
    for i in range(n_chunks):
        chunk = make_chunk(i, chunk_rows)
        chunk_bytes = sum(c.nbytes for c in chunk.columns)
        total_bytes_written += chunk_bytes

        mode = "create" if i == 0 else "append"
        lance.write_dataset(
            chunk, uri, mode=mode,
            storage_options=opts,
        )

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
    ds = lance.dataset(uri, storage_options=opts)
    row_count = ds.count_rows()
    elapsed = time.monotonic() - t0
    results["count_rows"] = elapsed
    results["count_rows_n"] = row_count
    print(f" {row_count:,} rows in {fmt_time(elapsed)}")

    # -- Full stream scan (all columns, batched) -----------------------------
    evict_page_cache()
    print("  full stream scan...", end="", flush=True)
    t0 = time.monotonic()
    ds = lance.dataset(uri, storage_options=opts)
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
    ds = lance.dataset(uri, storage_options=opts)
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
    ds = lance.dataset(uri, storage_options=opts)
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
    ds = lance.dataset(uri, storage_options=opts)
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
        description="Lance S3 HTTP layer benchmark (objstrd endpoint)")
    parser.add_argument("--chunks", type=int, default=N_CHUNKS,
                        help=f"Number of chunks (default: {N_CHUNKS})")
    parser.add_argument("--chunk-rows", type=int, default=CHUNK_ROWS,
                        help=f"Rows per chunk (default: {CHUNK_ROWS:,})")
    parser.add_argument("--img-size-mb", type=int, default=IMG_SIZE_MB,
                        help=f"Image file size in MB (default: {IMG_SIZE_MB})")
    parser.add_argument("--device", type=str, default="/dev/sdb",
                        help="Raw block device (default: /dev/sdb)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"objstrd listen port (default: {DEFAULT_PORT})")
    parser.add_argument("--objstrd-bin", type=str, default=OBJSTRD_BIN,
                        help=f"Path to objstrd binary (default: {OBJSTRD_BIN})")
    parser.add_argument("--backends", type=str, default="all",
                        help="Comma-separated backends or 'all'")
    args = parser.parse_args()

    total_rows = args.chunks * args.chunk_rows
    est_bytes = total_rows * (8 + 12 + 8 + 8 + 4 + PAYLOAD_SIZE)
    print(f"=== Lance S3 HTTP Layer Benchmark (Python) ===")
    print(f"objstrd S3 endpoint on port {args.port}")
    print(f"chunks={args.chunks}  chunk_rows={args.chunk_rows:,}  "
          f"total_rows={total_rows:,}")
    print(f"estimated dataset: ~{fmt_size(est_bytes)}  "
          f"payload={PAYLOAD_SIZE} bytes/row")
    print()

    port = args.port
    objstrd = args.objstrd_bin
    fs_root = "/tmp/lance_s3_fs"

    all_backends = {
        "fs": S3Backend(
            "fs",
            ["--backend", "fs", "--image", fs_root,
             "--port", str(port), "--bucket", BUCKET,
             "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY],
            port, objstrd, cleanup_paths=[fs_root],
        ),
        "mem": S3Backend(
            "mem",
            ["--backend", "mem",
             "--port", str(port), "--bucket", BUCKET,
             "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY],
            port, objstrd,
        ),
        "img+directio": S3Backend(
            "img+directio",
            ["--image", "/tmp/lance_s3_dio.img",
             "--size-mb", str(args.img_size_mb), "--direct-io",
             "--port", str(port), "--bucket", BUCKET,
             "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY],
            port, objstrd, cleanup_paths=["/tmp/lance_s3_dio.img"],
        ),
        "img+buffered": S3Backend(
            "img+buffered",
            ["--image", "/tmp/lance_s3_buf.img",
             "--size-mb", str(args.img_size_mb),
             "--port", str(port), "--bucket", BUCKET,
             "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY],
            port, objstrd, cleanup_paths=["/tmp/lance_s3_buf.img"],
        ),
        "raw+directio": S3Backend(
            "raw+directio",
            ["--image", args.device, "--direct-io",
             "--port", str(port), "--bucket", BUCKET,
             "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY],
            port, objstrd,
        ),
        "raw+buffered": S3Backend(
            "raw+buffered",
            ["--image", args.device,
             "--port", str(port), "--bucket", BUCKET,
             "--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY],
            port, objstrd,
        ),
    }

    backend_order = ["fs", "mem", "img+directio", "img+buffered",
                     "raw+directio", "raw+buffered"]

    if args.backends == "all":
        selected = backend_order
    else:
        selected = [b.strip() for b in args.backends.split(",")]

    all_results = {}

    for name in selected:
        if name not in all_backends:
            print(f"unknown backend: {name}")
            continue

        print("=" * 60)
        print(f"  BACKEND: {name}")
        print("=" * 60)

        backend = all_backends[name]
        try:
            backend.setup()
            result = run_benchmark(backend, args.chunks, args.chunk_rows)
            all_results[name] = result
        except Exception as e:
            print(f"  ERROR: {e}")
            all_results[name] = None
        finally:
            backend.cleanup()

        print()

    # -- Summary tables -------------------------------------------------------
    print("=== Summary: Write ===")
    print(f"{'backend':<16}{'time':>10}{'MB/s':>10}{'data':>10}")
    print("-" * 46)
    for name in selected:
        r = all_results.get(name)
        if r:
            rate = r["write_bytes"] / r["write"] / (1024**2)
            print(f"{name:<16}{fmt_time(r['write']):>10}"
                  f"{rate:>9.1f}{fmt_size(r['write_bytes']):>10}")
        else:
            print(f"{name:<16}  ERROR")
    print()

    print("=== Summary: Scan Times ===")
    print(f"{'backend':<16}{'count':>12}{'stream':>12}"
          f"{'LIKE A%':>12}{'id+amount':>12}{'cat=42':>12}")
    print("-" * 76)
    for name in selected:
        r = all_results.get(name)
        if r:
            print(f"{name:<16}"
                  f"{fmt_time(r['count_rows']):>12}"
                  f"{fmt_time(r['stream_scan']):>12}"
                  f"{fmt_time(r['filter_like']):>12}"
                  f"{fmt_time(r['col_project']):>12}"
                  f"{fmt_time(r['point_filter']):>12}")
        else:
            print(f"{name:<16}  ERROR")
    print()

    print("=== Summary: Scan Throughput (MB/s) ===")
    print(f"{'backend':<16}{'stream':>12}{'id+amount':>12}")
    print("-" * 40)
    for name in selected:
        r = all_results.get(name)
        if r:
            stream_rate = r["stream_scan_bytes"] / r["stream_scan"] / (1024**2)
            proj_rate = r["col_project_bytes"] / r["col_project"] / (1024**2)
            print(f"{name:<16}{stream_rate:>11.1f}{proj_rate:>12.1f}")
        else:
            print(f"{name:<16}  ERROR")


if __name__ == "__main__":
    main()
