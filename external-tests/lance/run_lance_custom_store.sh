#!/bin/bash
#
# Comprehensive lance test with custom Python object store.
# Tests all major lance operations: write, read, append, overwrite,
# merge_insert, delete, update, schema ops, and binary blobs.
#
set -e
source ~/lance/.venv/bin/activate

python3 << 'PYEOF'
import lance
import pyarrow as pa
import os
import sys
import time
import traceback

# ============================================================
# In-memory object store
# ============================================================
class DictObjectStore:
    """In-memory object store backed by a Python dict."""
    def __init__(self):
        self._data = {}
        self.stats = {"put": 0, "get": 0, "head": 0, "delete": 0,
                      "list": 0, "copy": 0, "bytes_written": 0, "bytes_read": 0}

    def put(self, path, data):
        raw = bytes(data)
        self._data[path] = raw
        self.stats["put"] += 1
        self.stats["bytes_written"] += len(raw)

    def get(self, path):
        if path not in self._data:
            raise FileNotFoundError(f"Not found: {path}")
        self.stats["get"] += 1
        d = self._data[path]
        self.stats["bytes_read"] += len(d)
        return d

    def get_range(self, path, offset, length):
        data = self.get(path)
        return data[offset:offset + length]

    def head(self, path):
        if path not in self._data:
            raise FileNotFoundError(f"Not found: {path}")
        self.stats["head"] += 1
        return {"size": len(self._data[path]), "location": path}

    def delete(self, path):
        self._data.pop(path, None)
        self.stats["delete"] += 1

    def list(self, prefix):
        results = []
        pfx = prefix or ""
        for k, v in self._data.items():
            if k.startswith(pfx):
                results.append({"size": len(v), "location": k})
        self.stats["list"] += 1
        return results

    def copy(self, src, dst):
        self._data[dst] = self._data[src]
        self.stats["copy"] += 1

    def list_with_delimiter(self, prefix):
        pfx = prefix or ""
        objects = []
        prefixes = set()
        for k, v in self._data.items():
            if not k.startswith(pfx):
                continue
            rest = k[len(pfx):]
            if "/" in rest:
                prefixes.add(pfx + rest.split("/")[0] + "/")
            else:
                objects.append({"size": len(v), "location": k})
        return {"common_prefixes": sorted(prefixes), "objects": objects}


# ============================================================
# Test runner
# ============================================================
passed = 0
failed = 0
errors = []

def run_test(name, fn):
    global passed, failed
    sys.stdout.write(f"  {name} ... ")
    sys.stdout.flush()
    try:
        fn()
        print("OK")
        passed += 1
    except Exception as e:
        print(f"FAIL: {e}")
        traceback.print_exc()
        failed += 1
        errors.append((name, str(e)))


# ============================================================
# Helper: create a fresh store + dataset
# ============================================================
def fresh_dataset(store=None):
    """Return (store, dataset) with a small table written."""
    if store is None:
        store = DictObjectStore()
    table = pa.table({
        "id":    pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name":  pa.array(["alice", "bob", "charlie", "dave", "eve"]),
        "score": pa.array([10.0, 20.0, 30.0, 40.0, 50.0], type=pa.float64()),
    })
    ds = lance.write_dataset(table, "memory:///test-ds", object_store=store)
    return store, ds


# ============================================================
# 1. Basic write and read
# ============================================================
def test_write_and_read():
    store, ds = fresh_dataset()
    assert ds.count_rows() == 5, f"Expected 5 rows, got {ds.count_rows()}"
    t = ds.to_table()
    assert t.num_rows == 5
    assert set(t.column_names) == {"id", "name", "score"}

def test_head():
    store, ds = fresh_dataset()
    t = ds.head(3)
    assert t.num_rows == 3

def test_take():
    store, ds = fresh_dataset()
    t = ds.take([0, 2, 4])
    assert t.num_rows == 3
    ids = t.column("id").to_pylist()
    assert ids == [1, 3, 5], f"Expected [1,3,5], got {ids}"

def test_count_rows_with_filter():
    store, ds = fresh_dataset()
    n = ds.count_rows(filter="score > 25.0")
    assert n == 3, f"Expected 3 rows with score>25, got {n}"

def test_scanner_filter():
    store, ds = fresh_dataset()
    scanner = ds.scanner(filter="score >= 30.0", columns=["id", "name"])
    t = scanner.to_table()
    assert t.num_rows == 3
    assert set(t.column_names) == {"id", "name"}


# ============================================================
# 2. Append and overwrite
# ============================================================
def test_append():
    store, ds = fresh_dataset()
    extra = pa.table({
        "id":    pa.array([6, 7], type=pa.int64()),
        "name":  pa.array(["frank", "grace"]),
        "score": pa.array([60.0, 70.0], type=pa.float64()),
    })
    ds = lance.write_dataset(extra, "memory:///test-ds",
                             mode="append", object_store=store)
    assert ds.count_rows() == 7, f"Expected 7, got {ds.count_rows()}"

def test_overwrite():
    store, ds = fresh_dataset()
    new_data = pa.table({
        "id":    pa.array([100], type=pa.int64()),
        "name":  pa.array(["zara"]),
        "score": pa.array([99.0], type=pa.float64()),
    })
    ds = lance.write_dataset(new_data, "memory:///test-ds",
                             mode="overwrite", object_store=store)
    assert ds.count_rows() == 1


# ============================================================
# 3. Delete, update, merge_insert
# ============================================================
def test_delete():
    store, ds = fresh_dataset()
    ds.delete("id > 3")
    assert ds.count_rows() == 3

def test_update():
    store, ds = fresh_dataset()
    ds.update({"score": "score * 2"}, where="id = 1")
    t = ds.to_table(filter="id = 1")
    assert t.column("score").to_pylist() == [20.0]

def test_merge_insert():
    store, ds = fresh_dataset()
    # Upsert: update id=1, insert id=99
    new_data = pa.table({
        "id":    pa.array([1, 99], type=pa.int64()),
        "name":  pa.array(["alice_updated", "newbie"]),
        "score": pa.array([11.0, 99.0], type=pa.float64()),
    })
    (ds.merge_insert("id")
       .when_matched_update_all()
       .when_not_matched_insert_all()
       .execute(new_data))
    total = ds.count_rows()
    assert total == 6, f"Expected 6, got {total}"
    t = ds.to_table(filter="id = 1")
    assert t.column("name").to_pylist() == ["alice_updated"]
    t = ds.to_table(filter="id = 99")
    assert t.num_rows == 1


# ============================================================
# 4. Schema operations
# ============================================================
def test_schema():
    store, ds = fresh_dataset()
    schema = ds.schema
    assert "id" in [f.name for f in schema]
    assert "name" in [f.name for f in schema]
    assert "score" in [f.name for f in schema]

def test_add_column():
    store, ds = fresh_dataset()
    ds.add_columns({"doubled": "score * 2"})
    t = ds.to_table()
    assert "doubled" in t.column_names
    assert t.column("doubled").to_pylist() == [20.0, 40.0, 60.0, 80.0, 100.0]

def test_drop_columns():
    store, ds = fresh_dataset()
    ds.drop_columns(["score"])
    t = ds.to_table()
    assert "score" not in t.column_names
    assert "id" in t.column_names


# ============================================================
# 5. Versioning
# ============================================================
def test_versions():
    store, ds = fresh_dataset()
    extra = pa.table({
        "id":    pa.array([6], type=pa.int64()),
        "name":  pa.array(["frank"]),
        "score": pa.array([60.0], type=pa.float64()),
    })
    ds = lance.write_dataset(extra, "memory:///test-ds",
                             mode="append", object_store=store)
    versions = ds.versions()
    assert len(versions) >= 2, f"Expected >=2 versions, got {len(versions)}"

def test_checkout_version():
    store, ds = fresh_dataset()
    v1 = ds.version
    extra = pa.table({
        "id":    pa.array([6], type=pa.int64()),
        "name":  pa.array(["frank"]),
        "score": pa.array([60.0], type=pa.float64()),
    })
    ds = lance.write_dataset(extra, "memory:///test-ds",
                             mode="append", object_store=store)
    assert ds.count_rows() == 6
    # Check out original version
    ds_v1 = lance.dataset("memory:///test-ds", version=v1, object_store=store)
    assert ds_v1.count_rows() == 5


# ============================================================
# 6. Binary blob tests (various sizes)
# ============================================================
def make_blob_table(sizes):
    """Create a table with binary blobs of the given sizes."""
    ids = list(range(len(sizes)))
    blobs = [os.urandom(s) for s in sizes]
    return pa.table({
        "id":   pa.array(ids, type=pa.int64()),
        "data": pa.array(blobs, type=pa.large_binary()),
        "size": pa.array(sizes, type=pa.int64()),
    })

def test_blob_1kb():
    store = DictObjectStore()
    t = make_blob_table([1024] * 10)
    ds = lance.write_dataset(t, "memory:///blob-1kb", object_store=store)
    assert ds.count_rows() == 10
    result = ds.to_table()
    for i in range(10):
        assert len(result.column("data")[i].as_py()) == 1024

def test_blob_10kb():
    store = DictObjectStore()
    t = make_blob_table([10 * 1024] * 5)
    ds = lance.write_dataset(t, "memory:///blob-10kb", object_store=store)
    assert ds.count_rows() == 5
    result = ds.to_table()
    for i in range(5):
        assert len(result.column("data")[i].as_py()) == 10 * 1024

def test_blob_100kb():
    store = DictObjectStore()
    t = make_blob_table([100 * 1024] * 5)
    ds = lance.write_dataset(t, "memory:///blob-100kb", object_store=store)
    assert ds.count_rows() == 5

def test_blob_1mb():
    store = DictObjectStore()
    t = make_blob_table([1024 * 1024] * 3)
    ds = lance.write_dataset(t, "memory:///blob-1mb", object_store=store)
    assert ds.count_rows() == 3
    result = ds.to_table()
    for i in range(3):
        assert len(result.column("data")[i].as_py()) == 1024 * 1024

def test_blob_10mb():
    store = DictObjectStore()
    t = make_blob_table([10 * 1024 * 1024] * 2)
    ds = lance.write_dataset(t, "memory:///blob-10mb", object_store=store)
    assert ds.count_rows() == 2
    result = ds.to_table()
    for i in range(2):
        assert len(result.column("data")[i].as_py()) == 10 * 1024 * 1024

def test_blob_50mb():
    store = DictObjectStore()
    t = make_blob_table([50 * 1024 * 1024])
    ds = lance.write_dataset(t, "memory:///blob-50mb", object_store=store)
    assert ds.count_rows() == 1
    result = ds.to_table()
    assert len(result.column("data")[0].as_py()) == 50 * 1024 * 1024

def test_blob_100mb():
    store = DictObjectStore()
    t = make_blob_table([100 * 1024 * 1024])
    ds = lance.write_dataset(t, "memory:///blob-100mb", object_store=store)
    assert ds.count_rows() == 1
    result = ds.to_table()
    assert len(result.column("data")[0].as_py()) == 100 * 1024 * 1024

def test_blob_mixed_sizes():
    """Mix of 1KB to 1MB blobs in one dataset."""
    store = DictObjectStore()
    sizes = [1024, 5*1024, 10*1024, 50*1024, 100*1024, 500*1024, 1024*1024]
    t = make_blob_table(sizes)
    ds = lance.write_dataset(t, "memory:///blob-mixed", object_store=store)
    assert ds.count_rows() == len(sizes)
    result = ds.to_table()
    for i, sz in enumerate(sizes):
        actual = len(result.column("data")[i].as_py())
        assert actual == sz, f"Row {i}: expected {sz}, got {actual}"

def test_blob_roundtrip_integrity():
    """Verify exact byte-level roundtrip of blob data."""
    store = DictObjectStore()
    import hashlib
    sizes = [1024, 100*1024, 1024*1024]
    blobs = [os.urandom(s) for s in sizes]
    hashes = [hashlib.sha256(b).hexdigest() for b in blobs]
    t = pa.table({
        "id": pa.array(list(range(len(sizes))), type=pa.int64()),
        "data": pa.array(blobs, type=pa.large_binary()),
    })
    ds = lance.write_dataset(t, "memory:///blob-integrity", object_store=store)
    result = ds.to_table()
    for i in range(len(sizes)):
        got = result.column("data")[i].as_py()
        h = hashlib.sha256(got).hexdigest()
        assert h == hashes[i], f"Row {i}: hash mismatch"


# ============================================================
# 7. Data-type tests
# ============================================================
def test_various_types():
    store = DictObjectStore()
    t = pa.table({
        "int8":    pa.array([1, 2, 3], type=pa.int8()),
        "int16":   pa.array([10, 20, 30], type=pa.int16()),
        "int32":   pa.array([100, 200, 300], type=pa.int32()),
        "int64":   pa.array([1000, 2000, 3000], type=pa.int64()),
        "float32": pa.array([1.1, 2.2, 3.3], type=pa.float32()),
        "float64": pa.array([1.11, 2.22, 3.33], type=pa.float64()),
        "str":     pa.array(["a", "bb", "ccc"]),
        "bool":    pa.array([True, False, True]),
        "binary":  pa.array([b"x", b"yy", b"zzz"], type=pa.binary()),
    })
    ds = lance.write_dataset(t, "memory:///types-ds", object_store=store)
    result = ds.to_table()
    assert result.num_rows == 3
    assert result.column("int64").to_pylist() == [1000, 2000, 3000]

def test_nullable_columns():
    store = DictObjectStore()
    t = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "val": pa.array([10.0, None, 30.0], type=pa.float64()),
        "txt": pa.array(["a", None, "c"]),
    })
    ds = lance.write_dataset(t, "memory:///nullable-ds", object_store=store)
    result = ds.to_table()
    assert result.column("val").to_pylist() == [10.0, None, 30.0]
    assert result.column("txt").to_pylist() == ["a", None, "c"]

def test_large_row_count():
    """Write 100k rows and verify count + sample read."""
    store = DictObjectStore()
    n = 100_000
    t = pa.table({
        "id": pa.array(range(n), type=pa.int64()),
        "val": pa.array([float(i) for i in range(n)], type=pa.float64()),
    })
    ds = lance.write_dataset(t, "memory:///large-ds", object_store=store)
    assert ds.count_rows() == n
    h = ds.head(10)
    assert h.num_rows == 10


# ============================================================
# 8. Multiple operations on same dataset
# ============================================================
def test_workflow():
    """Full create/append/update/delete/merge workflow."""
    store = DictObjectStore()

    # Create
    t1 = pa.table({
        "id":    pa.array([1, 2, 3], type=pa.int64()),
        "value": pa.array([10, 20, 30], type=pa.int64()),
    })
    ds = lance.write_dataset(t1, "memory:///workflow-ds", object_store=store)
    assert ds.count_rows() == 3

    # Append
    t2 = pa.table({
        "id":    pa.array([4, 5], type=pa.int64()),
        "value": pa.array([40, 50], type=pa.int64()),
    })
    ds = lance.write_dataset(t2, "memory:///workflow-ds",
                             mode="append", object_store=store)
    assert ds.count_rows() == 5

    # Update
    ds.update({"value": "value + 100"}, where="id <= 2")
    t = ds.to_table(filter="id = 1")
    assert t.column("value").to_pylist() == [110]

    # Delete
    ds.delete("id = 5")
    assert ds.count_rows() == 4

    # Merge insert (upsert)
    t3 = pa.table({
        "id":    pa.array([3, 6], type=pa.int64()),
        "value": pa.array([333, 60], type=pa.int64()),
    })
    (ds.merge_insert("id")
       .when_matched_update_all()
       .when_not_matched_insert_all()
       .execute(t3))
    assert ds.count_rows() == 5
    t = ds.to_table(filter="id = 3")
    assert t.column("value").to_pylist() == [333]
    t = ds.to_table(filter="id = 6")
    assert t.num_rows == 1

    # Final full scan
    final = ds.to_table()
    assert final.num_rows == 5


# ============================================================
# 9. Stats
# ============================================================
def test_store_stats():
    """Verify the store is actually being used (not a local fallback)."""
    store = DictObjectStore()
    t = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "val": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
    })
    ds = lance.write_dataset(t, "memory:///stats-ds", object_store=store)
    assert store.stats["put"] > 0, "No PUT operations recorded"
    assert store.stats["bytes_written"] > 0, "No bytes written"
    assert len(store._data) > 0, "No objects in store"

    # Read should trigger GET
    ds.to_table()
    assert store.stats["get"] > 0, "No GET operations recorded"
    assert store.stats["bytes_read"] > 0, "No bytes read"


# ============================================================
# Run all tests
# ============================================================
print("=" * 60)
print("Lance Custom Object Store - Comprehensive Test Suite")
print("=" * 60)
t0 = time.time()

print("\n--- Basic write/read ---")
run_test("write_and_read", test_write_and_read)
run_test("head", test_head)
run_test("take", test_take)
run_test("count_rows_with_filter", test_count_rows_with_filter)
run_test("scanner_filter", test_scanner_filter)

print("\n--- Append and overwrite ---")
run_test("append", test_append)
run_test("overwrite", test_overwrite)

print("\n--- Delete, update, merge_insert ---")
run_test("delete", test_delete)
run_test("update", test_update)
run_test("merge_insert", test_merge_insert)

print("\n--- Schema operations ---")
run_test("schema", test_schema)
run_test("add_column", test_add_column)
run_test("drop_columns", test_drop_columns)

print("\n--- Versioning ---")
run_test("versions", test_versions)
run_test("checkout_version", test_checkout_version)

print("\n--- Binary blobs ---")
run_test("blob_1kb", test_blob_1kb)
run_test("blob_10kb", test_blob_10kb)
run_test("blob_100kb", test_blob_100kb)
run_test("blob_1mb", test_blob_1mb)
run_test("blob_10mb", test_blob_10mb)
run_test("blob_50mb", test_blob_50mb)
run_test("blob_100mb", test_blob_100mb)
run_test("blob_mixed_sizes", test_blob_mixed_sizes)
run_test("blob_roundtrip_integrity", test_blob_roundtrip_integrity)

print("\n--- Data types ---")
run_test("various_types", test_various_types)
run_test("nullable_columns", test_nullable_columns)
run_test("large_row_count", test_large_row_count)

print("\n--- Workflow ---")
run_test("full_workflow", test_workflow)
run_test("store_stats", test_store_stats)

elapsed = time.time() - t0
print("\n" + "=" * 60)
print(f"Results: {passed} passed, {failed} failed ({elapsed:.1f}s)")
if errors:
    print("\nFailed tests:")
    for name, err in errors:
        print(f"  - {name}: {err}")
print("=" * 60)
sys.exit(1 if failed else 0)

PYEOF
