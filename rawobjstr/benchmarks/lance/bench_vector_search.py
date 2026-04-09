#!/usr/bin/env python3
"""
Benchmark: Lance vector index build + ANN search.

Workload:
  - Write a dataset with N rows containing float32 vectors
  - Build an IVF-PQ index on the vector column
  - Run K-nearest-neighbor searches
  - Measure index build time and query latency/throughput
"""

import argparse
import os
import shutil
import sys
import time

import numpy as np
import pyarrow as pa

# ---- configuration -------------------------------------------------------

ROWS = 500_000          # rows in the dataset
IMG_SIZE = 4 * 1024**3  # 4 GB image file (index + data)
VECTOR_DIM = 128        # vector dimension
N_QUERIES = 100         # number of ANN queries
TOP_K = 10              # neighbors to retrieve
IVF_PARTITIONS = 64     # IVF partition count
PQ_SUB_VECTORS = 16     # PQ sub-vector count

# --------------------------------------------------------------------------


def make_table(n_rows):
    """Create a table with an ID and a float32 vector column."""
    rng = np.random.default_rng(42)
    ids = np.arange(n_rows, dtype=np.int64)
    vectors = rng.standard_normal((n_rows, VECTOR_DIM)).astype(np.float32)
    vector_list = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.ravel(), type=pa.float32()), VECTOR_DIM
    )
    # Add a few scalar columns for filtered search
    categories = rng.integers(0, 50, size=n_rows, dtype=np.int32)
    return pa.table({
        "id": ids,
        "category": categories,
        "vector": vector_list,
    })


def make_queries(n_queries):
    """Generate random query vectors."""
    rng = np.random.default_rng(99)
    return rng.standard_normal((n_queries, VECTOR_DIM)).astype(np.float32)


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


# ---- backend helpers ------------------------------------------------------

class Ext4Backend:
    def __init__(self, base_dir):
        self.uri = os.path.join(base_dir, "lance_vector_bench")
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

def run_benchmark(backend, table, query_vectors):
    """Run vector index benchmarks. Returns dict of results."""
    import lance

    results = {}
    n_rows = len(table)

    # -- Write dataset -------------------------------------------------------
    print("  writing dataset...", end="", flush=True)
    t0 = time.monotonic()
    if backend.object_store:
        ds = lance.write_dataset(table, backend.uri, object_store=backend.object_store)
    else:
        ds = lance.write_dataset(table, backend.uri)
    elapsed = time.monotonic() - t0
    results["write"] = elapsed
    print(f" {fmt_time(elapsed)}")

    # -- Build IVF-PQ index --------------------------------------------------
    drop_caches()
    print(f"  building IVF-PQ index (partitions={IVF_PARTITIONS}, "
          f"pq_sub={PQ_SUB_VECTORS})...", end="", flush=True)
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)

    t0 = time.monotonic()
    ds.create_index(
        "vector",
        index_type="IVF_PQ",
        num_partitions=IVF_PARTITIONS,
        num_sub_vectors=PQ_SUB_VECTORS,
    )
    elapsed = time.monotonic() - t0
    results["index_build"] = elapsed
    print(f" {fmt_time(elapsed)}")

    # -- ANN search (cold) ---------------------------------------------------
    drop_caches()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)

    latencies = []
    for qv in query_vectors:
        t0 = time.monotonic()
        ds.to_table(
            nearest={"column": "vector", "q": qv, "k": TOP_K},
        )
        latencies.append(time.monotonic() - t0)

    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    avg = np.mean(latencies)
    qps = len(latencies) / sum(latencies)
    results["ann_cold_p50"] = p50
    results["ann_cold_p95"] = p95
    results["ann_cold_avg"] = avg
    results["ann_cold_qps"] = qps
    print(f"  ANN cold:       avg={fmt_time(avg)}  p50={fmt_time(p50)}  "
          f"p95={fmt_time(p95)}  ({qps:.1f} qps)")

    # -- ANN search (warm, no drop_caches) -----------------------------------
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)

    latencies_warm = []
    for qv in query_vectors:
        t0 = time.monotonic()
        ds.to_table(
            nearest={"column": "vector", "q": qv, "k": TOP_K},
        )
        latencies_warm.append(time.monotonic() - t0)

    p50w = np.percentile(latencies_warm, 50)
    p95w = np.percentile(latencies_warm, 95)
    avgw = np.mean(latencies_warm)
    qpsw = len(latencies_warm) / sum(latencies_warm)
    results["ann_warm_p50"] = p50w
    results["ann_warm_p95"] = p95w
    results["ann_warm_avg"] = avgw
    results["ann_warm_qps"] = qpsw
    print(f"  ANN warm:       avg={fmt_time(avgw)}  p50={fmt_time(p50w)}  "
          f"p95={fmt_time(p95w)}  ({qpsw:.1f} qps)")

    # -- Filtered ANN search -------------------------------------------------
    drop_caches()
    if backend.object_store:
        ds = lance.dataset(backend.uri, object_store=backend.object_store)
    else:
        ds = lance.dataset(backend.uri)

    latencies_filt = []
    for qv in query_vectors:
        t0 = time.monotonic()
        ds.to_table(
            nearest={"column": "vector", "q": qv, "k": TOP_K},
            filter="category < 10",
        )
        latencies_filt.append(time.monotonic() - t0)

    p50f = np.percentile(latencies_filt, 50)
    p95f = np.percentile(latencies_filt, 95)
    avgf = np.mean(latencies_filt)
    qpsf = len(latencies_filt) / sum(latencies_filt)
    results["ann_filt_p50"] = p50f
    results["ann_filt_p95"] = p95f
    results["ann_filt_avg"] = avgf
    results["ann_filt_qps"] = qpsf
    print(f"  ANN filtered:   avg={fmt_time(avgf)}  p50={fmt_time(p50f)}  "
          f"p95={fmt_time(p95f)}  ({qpsf:.1f} qps)")

    return results


# ---- main -----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Lance vector index benchmark")
    parser.add_argument("--rows", type=int, default=ROWS,
                        help=f"Number of rows (default: {ROWS})")
    parser.add_argument("--img-size", type=int, default=IMG_SIZE,
                        help=f"Image file size in bytes (default: {IMG_SIZE})")
    parser.add_argument("--device", type=str, default=None,
                        help="Raw block device path (e.g. /dev/sdb)")
    parser.add_argument("--ext4-dir", type=str, default="/tmp",
                        help="Directory for ext4 benchmark (default: /tmp)")
    parser.add_argument("--queries", type=int, default=N_QUERIES,
                        help=f"Number of ANN queries (default: {N_QUERIES})")
    args = parser.parse_args()

    print(f"=== Lance Vector Index Benchmark ===")
    print(f"rows={args.rows:,}  dim={VECTOR_DIM}  queries={args.queries}  "
          f"top_k={TOP_K}")
    print(f"index: IVF({IVF_PARTITIONS}) + PQ({PQ_SUB_VECTORS})")
    print()

    print("Generating data...")
    table = make_table(args.rows)
    query_vectors = make_queries(args.queries)
    print(f"  {len(table)} rows, "
          f"{sum(c.nbytes for c in table.columns) / 1024**2:.1f} MB")
    print()

    backends = [
        Ext4Backend(args.ext4_dir),
        RawObjStoreBackend("/tmp/lance_vec_buf.img", args.img_size, False, "rawobjstr-buffered"),
        RawObjStoreBackend("/tmp/lance_vec_dio.img", args.img_size, True, "rawobjstr-directio"),
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
            results = run_benchmark(backend, table, query_vectors)
            all_results[backend.name] = results
        except Exception as e:
            print(f"  ERROR: {e}")
            all_results[backend.name] = {"error": str(e)}
        finally:
            backend.cleanup()
        print()

    # Summary
    print("=== Summary: Index Build ===")
    for name, res in all_results.items():
        if "error" in res:
            print(f"  {name:<25s} ERROR: {res['error']}")
        else:
            print(f"  {name:<25s} {fmt_time(res['index_build'])}")

    print()
    print("=== Summary: ANN Query Latency (avg) ===")
    header = f"{'backend':<25s}{'cold':>10s}{'warm':>10s}{'filtered':>10s}"
    print(header)
    print("-" * len(header))
    for name, res in all_results.items():
        if "error" in res:
            print(f"{name:<25s}  ERROR: {res['error']}")
            continue
        print(f"{name:<25s}"
              f"{fmt_time(res['ann_cold_avg']):>10s}"
              f"{fmt_time(res['ann_warm_avg']):>10s}"
              f"{fmt_time(res['ann_filt_avg']):>10s}")

    print()
    print("=== Summary: ANN Queries/sec ===")
    header = f"{'backend':<25s}{'cold':>10s}{'warm':>10s}{'filtered':>10s}"
    print(header)
    print("-" * len(header))
    for name, res in all_results.items():
        if "error" in res:
            print(f"{name:<25s}  ERROR: {res['error']}")
            continue
        print(f"{name:<25s}"
              f"{res['ann_cold_qps']:>9.1f}"
              f"{res['ann_warm_qps']:>10.1f}"
              f"{res['ann_filt_qps']:>10.1f}")
    print()


if __name__ == "__main__":
    main()
