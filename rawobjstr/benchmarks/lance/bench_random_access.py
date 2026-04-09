#!/usr/bin/env python3
"""
Benchmark: Lance random row access (take).

Measures the latency and throughput of random row lookups using
lance.Dataset.take(), which exercises small random reads on the storage
backend.

Workload:
  - Write a dataset with N rows
  - Take single rows (latency)
  - Take batches of rows (throughput)
  - Take with column projection
"""

import argparse
import os
import shutil
import sys
import time

import numpy as np
import pyarrow as pa

# ---- configuration -------------------------------------------------------

ROWS = 1_000_000        # rows in the dataset
IMG_SIZE = 2 * 1024**3  # 2 GB image file
VECTOR_DIM = 64
SINGLE_TAKES = 200      # number of single-row takes for latency measurement
BATCH_SIZES = [10, 100, 1000, 10000]  # batch take sizes

# --------------------------------------------------------------------------


def make_table(n_rows):
    """Create a PyArrow table with mixed column types."""
    rng = np.random.default_rng(42)
    ids = np.arange(n_rows, dtype=np.int64)
    values = rng.standard_normal(n_rows).astype(np.float64)
    categories = rng.integers(0, 100, size=n_rows, dtype=np.int32)
    labels = pa.array([f"item_{i}" for i in ids], type=pa.utf8())
    vectors = rng.standard_normal((n_rows, VECTOR_DIM)).astype(np.float32)
    vector_list = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.ravel(), type=pa.float32()), VECTOR_DIM
    )
    return pa.table({
        "id": ids,
        "value": values,
        "category": categories,
        "label": labels,
        "vector": vector_list,
    })


def fmt_time(elapsed):
    if elapsed < 0.001:
        return f"{elapsed * 1_000_000:.0f} us"
    if elapsed < 1.0:
        return f"{elapsed * 1000:.1f} ms"
    return f"{elapsed:.2f} s"


def drop_caches():
    try:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
    except PermissionError:
        pass


# ---- backend helpers (same as bench_write_scan.py) ------------------------

class Ext4Backend:
    def __init__(self, base_dir):
        self.uri = os.path.join(base_dir, "lance_random_bench")
        self.name = "ext4"
        self.object_store = None

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


# ---- benchmark ------------------------------------------------------------

def run_benchmark(backend, table, rng):
    """Run random access benchmarks. Returns dict of results."""
    import lance

    results = {}
    n_rows = len(table)

    # -- Write dataset -------------------------------------------------------
    print("  writing dataset...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        lance.write_dataset(table, backend.uri, object_store=backend.object_store)
    else:
        lance.write_dataset(table, backend.uri)
    elapsed = time.monotonic() - t0
    print(f" {fmt_time(elapsed)}")

    # -- Single-row take (latency) -------------------------------------------
    drop_caches()
    indices = rng.integers(0, n_rows, size=SINGLE_TAKES)
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)

    latencies = []
    for idx in indices:
        t0 = time.monotonic()
        ds.take([int(idx)])
        latencies.append(time.monotonic() - t0)

    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    avg = np.mean(latencies)
    results["single_take_p50"] = p50
    results["single_take_p95"] = p95
    results["single_take_p99"] = p99
    results["single_take_avg"] = avg
    print(f"  single take:    avg={fmt_time(avg)}  p50={fmt_time(p50)}  "
          f"p95={fmt_time(p95)}  p99={fmt_time(p99)}")

    # -- Batch takes ---------------------------------------------------------
    for batch_size in BATCH_SIZES:
        drop_caches()
        if backend.object_store:
            ds = lance.dataset(backend.uri, object_store=backend.object_store)
        else:
            ds = lance.dataset(backend.uri)

        batch_indices = rng.integers(0, n_rows, size=batch_size).tolist()
        t0 = time.monotonic()
        taken = ds.take(batch_indices)
        elapsed = time.monotonic() - t0
        rows_per_sec = batch_size / elapsed
        results[f"batch_{batch_size}"] = elapsed
        results[f"batch_{batch_size}_rps"] = rows_per_sec
        print(f"  take({batch_size:>5d}):    {fmt_time(elapsed):>10s}  "
              f"({rows_per_sec:,.0f} rows/s)")

    # -- Batch take with column projection -----------------------------------
    drop_caches()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)
    proj_indices = rng.integers(0, n_rows, size=1000).tolist()
    t0 = time.monotonic()
    taken = ds.take(proj_indices, columns=["id", "value"])
    elapsed = time.monotonic() - t0
    results["proj_take_1000"] = elapsed
    results["proj_take_1000_rps"] = 1000 / elapsed
    print(f"  take(1000,2col): {fmt_time(elapsed):>10s}  "
          f"({1000 / elapsed:,.0f} rows/s)")

    return results


# ---- main -----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Lance random access benchmark")
    parser.add_argument("--rows", type=int, default=ROWS,
                        help=f"Number of rows (default: {ROWS})")
    parser.add_argument("--img-size", type=int, default=IMG_SIZE,
                        help=f"Image file size in bytes (default: {IMG_SIZE})")
    parser.add_argument("--device", type=str, default=None,
                        help="Raw block device path (e.g. /dev/sdb)")
    parser.add_argument("--ext4-dir", type=str, default="/tmp",
                        help="Directory for ext4 benchmark (default: /tmp)")
    args = parser.parse_args()

    print(f"=== Lance Random Access Benchmark ===")
    print(f"rows={args.rows:,}  single_takes={SINGLE_TAKES}  "
          f"batch_sizes={BATCH_SIZES}")
    print()

    rng = np.random.default_rng(123)

    print("Generating table...")
    table = make_table(args.rows)
    print(f"  {len(table)} rows, "
          f"{sum(c.nbytes for c in table.columns) / 1024**2:.1f} MB")
    print()

    backends = [
        Ext4Backend(args.ext4_dir),
        RawObjStoreBackend("/tmp/lance_rand_buf.img", args.img_size, False, "rawobjstr-buffered"),
        RawObjStoreBackend("/tmp/lance_rand_dio.img", args.img_size, True, "rawobjstr-directio"),
    ]
    if args.device:
        backends.append(
            RawObjStoreBackend(args.device, None, True, f"rawobjstr-{args.device}")
        )

    all_results = {}
    for backend in backends:
        print(f"--- {backend.name} ---")
        try:
            backend.setup()
            results = run_benchmark(backend, table, rng)
            all_results[backend.name] = results
        except Exception as e:
            print(f"  ERROR: {e}")
            all_results[backend.name] = {"error": str(e)}
        finally:
            backend.cleanup()
        print()

    # Summary
    print("=== Summary: single-row take latency ===")
    header = f"{'backend':<25s}{'avg':>10s}{'p50':>10s}{'p95':>10s}{'p99':>10s}"
    print(header)
    print("-" * len(header))
    for name, res in all_results.items():
        if "error" in res:
            print(f"{name:<25s}  ERROR: {res['error']}")
            continue
        print(f"{name:<25s}"
              f"{fmt_time(res['single_take_avg']):>10s}"
              f"{fmt_time(res['single_take_p50']):>10s}"
              f"{fmt_time(res['single_take_p95']):>10s}"
              f"{fmt_time(res['single_take_p99']):>10s}")

    print()
    print("=== Summary: batch take (rows/s) ===")
    header = f"{'backend':<25s}" + "".join(f"{'take(' + str(b) + ')':>14s}" for b in BATCH_SIZES)
    print(header)
    print("-" * len(header))
    for name, res in all_results.items():
        if "error" in res:
            print(f"{name:<25s}  ERROR: {res['error']}")
            continue
        cols = []
        for b in BATCH_SIZES:
            rps = res.get(f"batch_{b}_rps", 0)
            cols.append(f"{rps:>13,.0f}")
        print(f"{name:<25s}" + "".join(cols))
    print()


if __name__ == "__main__":
    main()
