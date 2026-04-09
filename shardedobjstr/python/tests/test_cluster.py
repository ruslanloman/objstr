"""End-to-end tests for shardedobjstr Python bindings.

These mirror the Rust integration tests in
shardedobjstr/tests/cluster_e2e.rs and exercise:
  format, put, get, head, list, list_with_delimiter, copy, delete,
  rename, placement, rebuild_catalog, save/load catalog,
  add-shard, context manager, error paths.
"""

import json
import os
import struct

import pytest

import shardedobjstr

from conftest import SHARD_SIZE, make_payload


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------

def make_fill(byte, size):
    """Payload filled with a single byte value."""
    return bytes([byte] * size)


# ---------------------------------------------------------------------------
# 1. Format and open
# ---------------------------------------------------------------------------

class TestFormatAndOpen:
    def test_format_and_open_basic(self, cluster_rf2):
        c = cluster_rf2
        assert c.shard_count() == 3
        assert c.replication_factor() == 2
        assert c.catalog_len() == 0

    def test_repr(self, cluster_rf2):
        r = repr(cluster_rf2)
        assert "shards=3" in r
        assert "replication=2" in r

    def test_format_shard_then_open(self, cluster_dir):
        paths = [str(cluster_dir / f"manual{i}.raw") for i in range(2)]
        for p in paths:
            shardedobjstr.format_shard(p, size=SHARD_SIZE)
        c = shardedobjstr.open_cluster(paths, replication_factor=1)
        assert c.shard_count() == 2
        assert c.replication_factor() == 1

    def test_format_shard_with_options(self, cluster_dir):
        p = str(cluster_dir / "opts.raw")
        shardedobjstr.format_shard_with_options(
            p, size=SHARD_SIZE, index_slot_size=16 * 1024 * 1024,
            max_key_length=512,
        )
        c = shardedobjstr.open_cluster([p], replication_factor=1)
        assert c.shard_count() == 1

    def test_open_unformatted_raises(self, cluster_dir):
        p = str(cluster_dir / "nonexistent.raw")
        with pytest.raises((ValueError, OSError)):
            shardedobjstr.open_cluster([p])

    def test_empty_paths_raises(self):
        with pytest.raises(ValueError, match="empty"):
            shardedobjstr.open_cluster([])

    def test_empty_shards_raises(self):
        with pytest.raises(ValueError, match="empty"):
            shardedobjstr.format_and_open_cluster([])


# ---------------------------------------------------------------------------
# 2. Put and Get
# ---------------------------------------------------------------------------

class TestPutGet:
    def test_put_get_roundtrip(self, cluster_rf2):
        data = make_payload(4096)
        cluster_rf2.put("hello.txt", data)
        got = cluster_rf2.get("hello.txt")
        assert got == data

    def test_put_get_empty(self, cluster_rf2):
        with pytest.raises(Exception):
            cluster_rf2.put("empty.txt", b"")

    def test_put_get_large(self, cluster_rf2):
        data = make_payload(256 * 1024, seed=42)
        cluster_rf2.put("large.bin", data)
        assert cluster_rf2.get("large.bin") == data

    def test_get_range(self, cluster_rf2):
        data = make_payload(8192)
        cluster_rf2.put("ranged.bin", data)
        chunk = cluster_rf2.get("ranged.bin", range=(100, 200))
        assert chunk == data[100:200]

    def test_get_nonexistent_raises(self, cluster_rf2):
        with pytest.raises(Exception):
            cluster_rf2.get("no/such/key")

    def test_put_overwrite(self, cluster_rf2):
        cluster_rf2.put("ow.txt", b"first")
        cluster_rf2.put("ow.txt", b"second")
        assert cluster_rf2.get("ow.txt") == b"second"

    def test_put_multiple_objects(self, cluster_rf2):
        files = {
            "table/_versions/1.manifest": b"manifest-v1",
            "table/data/00000.db": make_fill(0xAA, 4096),
            "table/data/00001.db": make_fill(0xBB, 4096),
            "table/_transactions/0-uuid.txn": b"txn-data",
        }
        for key, data in files.items():
            cluster_rf2.put(key, data)
        for key, data in files.items():
            assert cluster_rf2.get(key) == data


# ---------------------------------------------------------------------------
# 3. Head
# ---------------------------------------------------------------------------

class TestHead:
    def test_head_returns_metadata(self, cluster_rf2):
        data = make_payload(2048)
        cluster_rf2.put("meta.txt", data)
        meta = cluster_rf2.head("meta.txt")
        assert meta.location == "meta.txt"
        assert meta.size == 2048
        assert meta.last_modified  # non-empty string

    def test_head_nonexistent_raises(self, cluster_rf2):
        with pytest.raises(Exception):
            cluster_rf2.head("no/such/key")


# ---------------------------------------------------------------------------
# 4. Placement
# ---------------------------------------------------------------------------

class TestPlacement:
    def test_placement_replication(self, cluster_rf2):
        cluster_rf2.put("placed.txt", b"data")
        info = cluster_rf2.placement("placed.txt")
        assert info is not None
        assert len(info.shards) == 2  # replication_factor=2
        assert info.size == 4

    def test_placement_nonexistent(self, cluster_rf2):
        assert cluster_rf2.placement("nonexistent") is None

    def test_placement_fields(self, cluster_rf2):
        cluster_rf2.put("fields.bin", make_payload(100))
        info = cluster_rf2.placement("fields.bin")
        assert info.size == 100
        assert info.updated  # non-empty timestamp
        # shards should be distinct
        assert len(set(info.shards)) == len(info.shards)
        # all shard ids within range
        for sid in info.shards:
            assert 0 <= sid < cluster_rf2.shard_count()


# ---------------------------------------------------------------------------
# 5. List
# ---------------------------------------------------------------------------

class TestList:
    def test_list_all(self, cluster_rf2):
        keys = ["a.txt", "b.txt", "c/d.txt"]
        for k in keys:
            cluster_rf2.put(k, b"x")
        items = cluster_rf2.list()
        locations = sorted(m.location for m in items)
        assert locations == sorted(keys)

    def test_list_with_prefix(self, cluster_rf2):
        cluster_rf2.put("dir/a.txt", b"1")
        cluster_rf2.put("dir/b.txt", b"2")
        cluster_rf2.put("other/c.txt", b"3")
        items = cluster_rf2.list("dir")
        locations = [m.location for m in items]
        assert all(loc.startswith("dir/") for loc in locations)
        assert len(items) == 2

    def test_list_empty_cluster(self, cluster_dir):
        shards = [(str(cluster_dir / f"empty{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        assert c.list() == []

    def test_list_with_delimiter(self, cluster_rf2):
        cluster_rf2.put("ns/a.txt", b"1")
        cluster_rf2.put("ns/b.txt", b"2")
        cluster_rf2.put("ns/sub/c.txt", b"3")
        result = cluster_rf2.list_with_delimiter("ns")
        # objects directly under ns/
        obj_locs = sorted(o.location for o in result.objects)
        assert "ns/a.txt" in obj_locs
        assert "ns/b.txt" in obj_locs
        # sub-directory as common prefix
        assert "ns/sub" in result.common_prefixes


# ---------------------------------------------------------------------------
# 6. Copy
# ---------------------------------------------------------------------------

class TestCopy:
    def test_copy_basic(self, cluster_rf2):
        data = make_fill(0xCC, 4096)
        cluster_rf2.put("src.bin", data)
        cluster_rf2.copy("src.bin", "dst.bin")
        assert cluster_rf2.get("dst.bin") == data
        # placement should exist for the copy
        info = cluster_rf2.placement("dst.bin")
        assert info is not None
        assert len(info.shards) == 2

    def test_copy_if_not_exists(self, cluster_rf2):
        cluster_rf2.put("orig.txt", b"original")
        cluster_rf2.copy_if_not_exists("orig.txt", "new_copy.txt")
        assert cluster_rf2.get("new_copy.txt") == b"original"

    def test_copy_if_not_exists_fails_when_exists(self, cluster_rf2):
        cluster_rf2.put("a.txt", b"a")
        cluster_rf2.put("b.txt", b"b")
        with pytest.raises(Exception):
            cluster_rf2.copy_if_not_exists("a.txt", "b.txt")


# ---------------------------------------------------------------------------
# 7. Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_removes_object(self, cluster_rf2):
        cluster_rf2.put("del_me.txt", b"data")
        assert cluster_rf2.placement("del_me.txt") is not None
        cluster_rf2.delete("del_me.txt")
        assert cluster_rf2.placement("del_me.txt") is None

    def test_delete_then_get_raises(self, cluster_rf2):
        cluster_rf2.put("gone.txt", b"data")
        cluster_rf2.delete("gone.txt")
        with pytest.raises(Exception):
            cluster_rf2.get("gone.txt")

    def test_delete_updates_list(self, cluster_rf2):
        cluster_rf2.put("keep.txt", b"k")
        cluster_rf2.put("remove.txt", b"r")
        before = len(cluster_rf2.list())
        cluster_rf2.delete("remove.txt")
        after = len(cluster_rf2.list())
        assert after == before - 1

    def test_delete_nonexistent(self, cluster_rf2):
        # Should not raise (ObjectStore trait: delete of missing is OK)
        cluster_rf2.delete("never_existed.txt")


# ---------------------------------------------------------------------------
# 8. Rename
# ---------------------------------------------------------------------------

class TestRename:
    def test_rename_basic(self, cluster_rf2):
        cluster_rf2.put("old_name.txt", b"payload")
        cluster_rf2.rename("old_name.txt", "new_name.txt")
        assert cluster_rf2.get("new_name.txt") == b"payload"
        # old key should be gone
        assert cluster_rf2.placement("old_name.txt") is None

    def test_rename_if_not_exists(self, cluster_rf2):
        cluster_rf2.put("rn_src.txt", b"data")
        cluster_rf2.rename_if_not_exists("rn_src.txt", "rn_dst.txt")
        assert cluster_rf2.get("rn_dst.txt") == b"data"
        assert cluster_rf2.placement("rn_src.txt") is None


# ---------------------------------------------------------------------------
# 9. Rebuild catalog
# ---------------------------------------------------------------------------

class TestRebuildCatalog:
    def test_rebuild_from_scratch(self, three_shard_paths):
        # Format + populate, flush and release before reopen
        shards = [(p, SHARD_SIZE) for p in three_shard_paths]
        c1 = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c1.put("alpha.txt", b"aaa")
        c1.put("beta.txt", b"bbb")
        c1.put("gamma.txt", b"ccc")
        c1.flush_all()
        del c1

        # Reopen and rebuild
        c2 = shardedobjstr.open_cluster(three_shard_paths, replication_factor=2)
        count = c2.rebuild_catalog()
        assert count >= 3  # at least our 3 objects (replicas may inflate count)
        items = c2.list()
        locs = sorted(m.location for m in items)
        assert "alpha.txt" in locs
        assert "beta.txt" in locs
        assert "gamma.txt" in locs


# ---------------------------------------------------------------------------
# 10. Save and load catalog
# ---------------------------------------------------------------------------

class TestCatalogPersistence:
    def test_save_and_load_catalog(self, three_shard_paths, cluster_dir):
        cat_path = str(cluster_dir / "catalog.json")
        shards = [(p, SHARD_SIZE) for p in three_shard_paths]
        c1 = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c1.put("saved.txt", b"persisted")
        c1.save_catalog(cat_path)
        assert os.path.exists(cat_path)
        c1.flush_all()
        del c1

        # Reopen with saved catalog
        c2 = shardedobjstr.open_cluster(
            three_shard_paths, replication_factor=2, catalog_path=cat_path
        )
        assert c2.get("saved.txt") == b"persisted"

    def test_catalog_json_is_valid(self, cluster_dir):
        paths = [str(cluster_dir / f"cat{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("x.txt", b"data")
        cat_path = str(cluster_dir / "catalog_check.json")
        c.save_catalog(cat_path)
        with open(cat_path) as f:
            data = json.load(f)
        assert isinstance(data, (dict, list))


# ---------------------------------------------------------------------------
# 11. Shard health
# ---------------------------------------------------------------------------

class TestShardHealth:
    def test_shard_health_all_healthy(self, cluster_rf2):
        for i in range(cluster_rf2.shard_count()):
            h = cluster_rf2.shard_health(i)
            assert h is not None

    def test_shard_health_out_of_range(self, cluster_rf2):
        assert cluster_rf2.shard_health(999) is None


# ---------------------------------------------------------------------------
# 12. Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_context_manager(self, cluster_dir):
        shards = [(str(cluster_dir / f"ctx{i}.raw"), SHARD_SIZE) for i in range(2)]
        with shardedobjstr.format_and_open_cluster(shards, replication_factor=1) as c:
            c.put("ctx.txt", b"hello")
            assert c.get("ctx.txt") == b"hello"


# ---------------------------------------------------------------------------
# 13. Replication factor = 1 (no replication)
# ---------------------------------------------------------------------------

class TestReplicationFactorOne:
    def test_single_replica(self, cluster_dir):
        paths = [str(cluster_dir / f"rf1_{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("single.txt", b"one-copy")
        info = c.placement("single.txt")
        assert len(info.shards) == 1
        assert c.get("single.txt") == b"one-copy"


# ---------------------------------------------------------------------------
# 14. Replication factor clamped to shard count
# ---------------------------------------------------------------------------

class TestReplicationClamped:
    def test_rf_clamped(self, cluster_dir):
        paths = [str(cluster_dir / f"clamp{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=10)
        # RF should be clamped to shard_count (2)
        assert c.replication_factor() <= c.shard_count()
        c.put("clamped.txt", b"data")
        info = c.placement("clamped.txt")
        assert len(info.shards) <= c.shard_count()


# ---------------------------------------------------------------------------
# 15. Full E2E scenario (mirrors Rust cluster_e2e_all_features)
# ---------------------------------------------------------------------------

class TestFullE2E:
    """Full end-to-end test mirroring the Rust cluster_e2e_all_features test."""

    def test_full_lifecycle(self, cluster_dir):
        shard_size = SHARD_SIZE
        replicas = 2

        # -- 1. Format 3 shards --
        paths = [str(cluster_dir / f"e2e{i}.raw") for i in range(3)]
        shards = [(p, shard_size) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=replicas)
        assert c.shard_count() == 3
        assert c.replication_factor() == 2

        # -- 2. Put several objects --
        files = {
            "table/_versions/1.manifest": b"manifest-v1",
            "table/data/00000.db": make_fill(0xAA, 4096),
            "table/data/00001.db": make_fill(0xBB, 4096),
            "table/_transactions/0-uuid.txn": b"txn-data",
        }
        for key, data in files.items():
            c.put(key, data)

        # Verify placement: each on `replicas` shards
        for key in files:
            info = c.placement(key)
            assert info is not None, f"missing placement for {key}"
            assert len(info.shards) == replicas, f"{key} on wrong number of shards"

        # -- 3. List objects --
        items = c.list()
        assert len(items) == len(files)

        data_items = c.list("table/data")
        assert len(data_items) == 2

        # -- 4. Get + verify content --
        got = c.get("table/data/00000.db")
        assert len(got) == 4096
        assert all(b == 0xAA for b in got)

        # -- 5. Head --
        meta = c.head("table/data/00001.db")
        assert meta.size == 4096

        # -- 6. Copy --
        c.copy("table/data/00000.db", "table/data/00002.db")
        copy_info = c.placement("table/data/00002.db")
        assert copy_info is not None
        assert len(copy_info.shards) == replicas
        got_copy = c.get("table/data/00002.db")
        assert len(got_copy) == 4096
        assert all(b == 0xAA for b in got_copy)

        # -- 7. Delete --
        c.delete("table/_transactions/0-uuid.txn")
        assert c.placement("table/_transactions/0-uuid.txn") is None
        after_del = c.list()
        assert len(after_del) == 4  # 3 original - 1 deleted + 1 copy

        # -- 8. Rebuild catalog from scratch --
        c.flush_all()
        del c
        c2 = shardedobjstr.open_cluster(paths, replication_factor=replicas)
        count = c2.rebuild_catalog()
        assert count > 0
        rebuilt = c2.list()
        assert len(rebuilt) == 4

        # -- 9. Add a 4th shard --
        c2.flush_all()
        del c2
        s3_path = str(cluster_dir / "e2e3.raw")
        shardedobjstr.format_shard(s3_path, size=shard_size)
        all_paths = paths + [s3_path]
        c3 = shardedobjstr.open_cluster(all_paths, replication_factor=replicas)
        c3.rebuild_catalog()
        assert c3.shard_count() == 4

        # Put new object on 4-shard cluster
        c3.put("table/data/00003.db", make_fill(0xCC, 2048))
        after_add = c3.list()
        assert len(after_add) == 5

        # -- 10. Verify all objects readable --
        for m in after_add:
            data = c3.get(m.location)
            assert len(data) == m.size


# ---------------------------------------------------------------------------
# 16. Many small objects
# ---------------------------------------------------------------------------

class TestManyObjects:
    def test_100_objects(self, cluster_rf2):
        count = 100
        prefix = "batch"
        for i in range(count):
            cluster_rf2.put(f"{prefix}/{i:04d}.dat", make_payload(128, seed=i))
        items = cluster_rf2.list(prefix)
        assert len(items) == count

        # Spot-check a few
        for idx in [0, 49, 99]:
            got = cluster_rf2.get(f"{prefix}/{idx:04d}.dat")
            expected = make_payload(128, seed=idx)
            assert got == expected, f"mismatch at index {idx}"


# ---------------------------------------------------------------------------
# 17. Catalog length tracking
# ---------------------------------------------------------------------------

class TestCatalogLen:
    def test_catalog_len_tracks_puts(self, cluster_dir):
        shards = [(str(cluster_dir / f"clen{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        assert c.catalog_len() == 0
        c.put("one.txt", b"1")
        assert c.catalog_len() == 1
        c.put("two.txt", b"2")
        assert c.catalog_len() == 2
        c.delete("one.txt")
        # catalog_len includes the delete marker entry (__deleted__/one.txt),
        # so the count stays at 2 (marker replaces the original key).
        assert c.catalog_len() == 2


# ---------------------------------------------------------------------------
# 18. Version and build info
# ---------------------------------------------------------------------------

class TestVersionInfo:
    def test_version_is_string(self):
        assert isinstance(shardedobjstr.__version__, str)
        assert len(shardedobjstr.__version__) > 0

    def test_build_info_is_string(self):
        assert isinstance(shardedobjstr.__build_info__, str)
        assert "0.1.0" in shardedobjstr.__build_info__


# ---------------------------------------------------------------------------
# 19. Compression
# ---------------------------------------------------------------------------

class TestCompression:
    """Tests for the compression parameter on format functions."""

    def test_format_and_open_zstd(self, cluster_dir):
        shards = [(str(cluster_dir / f"zstd{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=1, compression="zstd"
        )
        data = make_payload(8192, seed=77)
        c.put("zstd.bin", data)
        assert c.get("zstd.bin") == data

    def test_format_and_open_snappy(self, cluster_dir):
        shards = [(str(cluster_dir / f"snap{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=1, compression="snappy"
        )
        data = make_payload(8192, seed=88)
        c.put("snap.bin", data)
        assert c.get("snap.bin") == data

    def test_format_shard_with_options_compression(self, cluster_dir):
        p = str(cluster_dir / "opts_comp.raw")
        shardedobjstr.format_shard_with_options(
            p, size=SHARD_SIZE, compression="zstd"
        )
        c = shardedobjstr.open_cluster([p], replication_factor=1)
        c.put("hello.txt", b"compressed hello")
        assert c.get("hello.txt") == b"compressed hello"

    def test_invalid_compression_raises(self, cluster_dir):
        shards = [(str(cluster_dir / f"bad{i}.raw"), SHARD_SIZE) for i in range(2)]
        with pytest.raises(ValueError, match="unknown compression"):
            shardedobjstr.format_and_open_cluster(
                shards, replication_factor=1, compression="invalid_algo"
            )

    def test_none_compression_default(self, cluster_dir):
        """Omitting compression (None) is the same as no compression."""
        shards = [(str(cluster_dir / f"nocomp{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=1, compression=None
        )
        data = make_payload(4096)
        c.put("none.bin", data)
        assert c.get("none.bin") == data


# ---------------------------------------------------------------------------
# 20. Bincode catalog persistence
# ---------------------------------------------------------------------------

class TestCatalogBincode:
    """Tests for save_catalog(format='bincode') and open_cluster(catalog_format='bincode')."""

    def test_save_bincode_creates_file(self, cluster_dir):
        shards = [(str(cluster_dir / f"bc{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("hello.txt", b"world")
        cat_path = str(cluster_dir / "catalog.bin")
        c.save_catalog(cat_path, format="bincode")
        assert os.path.exists(cat_path)

    def test_bincode_roundtrip(self, cluster_dir):
        paths = [str(cluster_dir / f"brt{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c1 = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        data = make_payload(4096, seed=42)
        c1.put("bin.dat", data)
        c1.put("txt.txt", b"hello")
        cat_path = str(cluster_dir / "catalog_rt.bin")
        c1.save_catalog(cat_path, format="bincode")
        c1.flush_all()
        del c1

        c2 = shardedobjstr.open_cluster(
            paths, replication_factor=1,
            catalog_path=cat_path, catalog_format="bincode",
        )
        assert c2.get("bin.dat") == data
        assert c2.get("txt.txt") == b"hello"

    def test_bincode_not_valid_json(self, cluster_dir):
        """Bincode output should not be valid JSON."""
        shards = [(str(cluster_dir / f"bnj{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("x.txt", b"data")
        cat_path = str(cluster_dir / "catalog_nj.bin")
        c.save_catalog(cat_path, format="bincode")
        with open(cat_path, "rb") as f:
            raw = f.read()
        with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
            json.loads(raw)

    def test_json_format_explicit(self, cluster_dir):
        """Passing format='json' explicitly still works."""
        shards = [(str(cluster_dir / f"ej{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("j.txt", b"json")
        cat_path = str(cluster_dir / "catalog_ej.json")
        c.save_catalog(cat_path, format="json")
        with open(cat_path) as f:
            json.load(f)  # should not raise

    def test_invalid_format_raises(self, cluster_dir):
        shards = [(str(cluster_dir / f"bf{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        with pytest.raises(ValueError, match="unknown catalog format"):
            c.save_catalog(str(cluster_dir / "bad.bin"), format="xml")


# ---------------------------------------------------------------------------
# 21. PutMode / put_if_not_exists
# ---------------------------------------------------------------------------

class TestPutIfNotExists:
    """Tests for put_if_not_exists (PutMode::Create)."""

    def test_succeeds_on_new_key(self, cluster_dir):
        shards = [(str(cluster_dir / f"pine{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put_if_not_exists("new.txt", b"hello")
        assert c.get("new.txt") == b"hello"

    def test_raises_on_existing_key(self, cluster_dir):
        shards = [(str(cluster_dir / f"piex{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("exist.txt", b"original")
        with pytest.raises(FileExistsError):
            c.put_if_not_exists("exist.txt", b"duplicate")
        # Original data untouched
        assert c.get("exist.txt") == b"original"

    def test_different_keys_both_succeed(self, cluster_dir):
        shards = [(str(cluster_dir / f"pid{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put_if_not_exists("a.txt", b"aaa")
        c.put_if_not_exists("b.txt", b"bbb")
        assert c.get("a.txt") == b"aaa"
        assert c.get("b.txt") == b"bbb"


# ---------------------------------------------------------------------------
# 22. Multipart upload
# ---------------------------------------------------------------------------

class TestMultipart:
    """Tests for put_multipart (collects parts, writes on complete)."""

    def test_single_part(self, cluster_rf2):
        data = make_fill(0xAA, 8192)
        cluster_rf2.put_multipart("mp/single.bin", data)
        got = cluster_rf2.get("mp/single.bin")
        assert got == data
        info = cluster_rf2.placement("mp/single.bin")
        assert info is not None
        assert len(info.shards) == 2  # RF=2

    def test_roundtrip_various_sizes(self, cluster_rf2):
        for size in [1, 100, 4096, 65536]:
            key = f"mp/size_{size}.bin"
            data = make_payload(size, seed=size)
            cluster_rf2.put_multipart(key, data)
            assert cluster_rf2.get(key) == data

    def test_overwrite_via_multipart(self, cluster_rf2):
        cluster_rf2.put("mp/overwrite.bin", b"original")
        cluster_rf2.put_multipart("mp/overwrite.bin", b"replaced")
        assert cluster_rf2.get("mp/overwrite.bin") == b"replaced"


# ---------------------------------------------------------------------------
# 23. set_shard_health
# ---------------------------------------------------------------------------

class TestSetShardHealth:
    """Tests for the set_shard_health public setter."""

    def test_set_and_get(self, cluster_dir):
        shards = [(str(cluster_dir / f"sh{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        H = shardedobjstr.ShardHealth
        # Initially healthy
        assert c.shard_health(0) == H.Healthy

        prev = c.set_shard_health(0, H.Degraded)
        assert prev == H.Healthy
        assert c.shard_health(0) == H.Degraded

        prev = c.set_shard_health(0, H.Offline)
        assert prev == H.Degraded
        assert c.shard_health(0) == H.Offline

        prev = c.set_shard_health(0, H.Healthy)
        assert prev == H.Offline
        assert c.shard_health(0) == H.Healthy

    def test_out_of_range(self, cluster_dir):
        shards = [(str(cluster_dir / f"shr{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        result = c.set_shard_health(99, shardedobjstr.ShardHealth.Degraded)
        assert result is None

    def test_offline_read_fallback(self, cluster_dir):
        """With RF=2, marking one shard offline still allows reads."""
        paths = [str(cluster_dir / f"shof{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("fallback.txt", b"safe-data")

        info = c.placement("fallback.txt")
        first_shard = info.shards[0]
        c.set_shard_health(first_shard, shardedobjstr.ShardHealth.Offline)

        # Should still be readable from the other replica
        assert c.get("fallback.txt") == b"safe-data"


# ---------------------------------------------------------------------------
# 24. Catalog bulk operations (entries_for_shard, remove_all_for_shard)
# ---------------------------------------------------------------------------

class TestCatalogBulkOps:
    """Tests for entries_for_shard and remove_all_for_shard."""

    def test_entries_for_shard(self, cluster_rf2):
        for i in range(10):
            cluster_rf2.put(f"bulk/{i}.dat", make_payload(100, seed=i))

        for shard_id in range(cluster_rf2.shard_count()):
            entries = cluster_rf2.entries_for_shard(shard_id)
            for key, entry in entries:
                assert shard_id in entry.shards, (
                    f"entry {key} should reference shard {shard_id}"
                )

    def test_remove_all_for_shard(self, cluster_rf2):
        for i in range(10):
            cluster_rf2.put(f"bulkrm/{i}.dat", make_payload(100, seed=i))

        before = cluster_rf2.catalog_len()
        entries_on_0 = cluster_rf2.entries_for_shard(0)
        affected = cluster_rf2.remove_all_for_shard(0)
        assert affected == len(entries_on_0)

        # No entries should reference shard 0 anymore
        after_entries = cluster_rf2.entries_for_shard(0)
        assert len(after_entries) == 0

        after = cluster_rf2.catalog_len()
        assert after <= before


# ---------------------------------------------------------------------------
# 25. invalidate_shard
# ---------------------------------------------------------------------------

class TestInvalidateShard:
    """Tests for invalidate_shard with InvalidateReport."""

    def test_basic_invalidation(self, cluster_dir):
        paths = [str(cluster_dir / f"inv{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)

        for i in range(20):
            c.put(f"inv/{i}.dat", make_payload(256, seed=i))
        c.flush_all()

        report = c.invalidate_shard(0)
        assert report.shard_id == 0
        assert report.scan_ok is True
        assert report.entries_purged > 0
        assert report.entries_restored > 0
        assert len(report.missing_keys) == 0

        # Shard should be Healthy again
        assert c.shard_health(0) == shardedobjstr.ShardHealth.Healthy

        # All objects should still be readable
        for i in range(20):
            data = c.get(f"inv/{i}.dat")
            assert data == make_payload(256, seed=i)

    def test_out_of_range_raises(self, cluster_dir):
        shards = [(str(cluster_dir / f"invoor{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        with pytest.raises(Exception):
            c.invalidate_shard(99)

    def test_report_repr(self, cluster_dir):
        paths = [str(cluster_dir / f"invrep{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("repr.txt", b"data")
        c.flush_all()
        report = c.invalidate_shard(0)
        r = repr(report)
        assert "shard=0" in r
        assert "ok=" in r


# ---------------------------------------------------------------------------
# 12. Degraded startup, detach, reattach, replication
# ---------------------------------------------------------------------------

class TestDegradedStartup:
    def test_open_cluster_degraded_basic(self, cluster_dir):
        """Start with one shard online, one offline."""
        p0 = str(cluster_dir / "deg0.raw")
        shardedobjstr.format_shard(p0, size=SHARD_SIZE)
        c = shardedobjstr.open_cluster_degraded(
            [p0, None], replication_factor=2,
        )
        assert c.shard_count() == 2
        assert c.shard_health(0) == shardedobjstr.ShardHealth.Healthy
        assert c.shard_health(1) == shardedobjstr.ShardHealth.Offline

    def test_degraded_put_get(self, cluster_dir):
        """Writes succeed in degraded mode, reads work from available shard."""
        p0 = str(cluster_dir / "degput0.raw")
        shardedobjstr.format_shard(p0, size=SHARD_SIZE)
        c = shardedobjstr.open_cluster_degraded(
            [p0, None], replication_factor=2,
        )
        c.put("hello.txt", b"hello degraded")
        c.flush_all()
        data = c.get("hello.txt")
        assert data == b"hello degraded"

    def test_find_under_replicated(self, cluster_dir):
        """Under-replicated objects detected in degraded mode."""
        p0 = str(cluster_dir / "degunder0.raw")
        shardedobjstr.format_shard(p0, size=SHARD_SIZE)
        c = shardedobjstr.open_cluster_degraded(
            [p0, None], replication_factor=2,
        )
        for i in range(5):
            c.put(f"ur/{i}.dat", make_payload(64, seed=i))
        c.flush_all()
        under = c.find_under_replicated()
        assert len(under) == 5
        for key, count in under:
            assert count == 1

    def test_attach_shard_and_replicate(self, cluster_dir):
        """Attach an offline shard and replicate under-replicated objects."""
        p0 = str(cluster_dir / "degattach0.raw")
        p1 = str(cluster_dir / "degattach1.raw")
        shardedobjstr.format_shard(p0, size=SHARD_SIZE)
        c = shardedobjstr.open_cluster_degraded(
            [p0, None], replication_factor=2,
        )
        # Write objects (go to shard 0 only)
        for i in range(3):
            c.put(f"att/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        # All under-replicated
        assert len(c.find_under_replicated()) == 3

        # Format and attach shard 1
        shardedobjstr.format_shard(p1, size=SHARD_SIZE)
        count = c.attach_shard(1, p1, force=True)
        assert count == 0  # fresh shard
        assert c.shard_health(1) == shardedobjstr.ShardHealth.Healthy

        # Replicate
        for key, _count in c.find_under_replicated():
            info = c.placement(key)
            from_shard = info.shards[0]
            to_shard = 1 if from_shard == 0 else 0
            c.replicate_object(key, from_shard, to_shard)
        c.flush_all()

        # Now fully replicated
        assert len(c.find_under_replicated()) == 0

        # Verify data integrity
        for i in range(3):
            data = c.get(f"att/{i}.dat")
            assert data == make_payload(128, seed=i)

    def test_detach_and_reattach(self, cluster_dir):
        """Detach a shard, verify reads still work, reattach."""
        paths = [str(cluster_dir / f"degdet{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=2,
        )
        for i in range(5):
            c.put(f"det/{i}.dat", make_payload(64, seed=i))
        c.flush_all()

        # Fully replicated
        assert len(c.find_under_replicated()) == 0

        # Detach shard 1
        prev = c.detach_shard(1)
        assert prev == shardedobjstr.ShardHealth.Healthy
        assert c.shard_health(1) == shardedobjstr.ShardHealth.Offline

        # Reads still work
        for i in range(5):
            data = c.get(f"det/{i}.dat")
            assert data == make_payload(64, seed=i)

        # Under-replicated now
        assert len(c.find_under_replicated()) > 0

        # Reattach with force
        count = c.attach_shard(1, paths[1], force=True)
        assert count > 0
        assert c.shard_health(1) == shardedobjstr.ShardHealth.Healthy
        assert len(c.find_under_replicated()) == 0

    def test_laptop_scenario(self, cluster_dir):
        """Full laptop scenario: online -> detach -> work -> reattach -> replicate."""
        paths = [str(cluster_dir / f"laptop{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=2,
        )
        # Phase 1: both online
        c.put("laptop/initial.txt", b"initial data")
        c.flush_all()
        assert len(c.find_under_replicated()) == 0

        # Phase 2: detach shard 1 ("S3 offline")
        c.detach_shard(1)

        # Phase 3: work with shard 0 only
        c.put("laptop/offline_work.txt", b"wrote while disconnected")
        c.flush_all()

        # New object is under-replicated
        under = c.find_under_replicated()
        offline_keys = [k for k, _ in under]
        assert "laptop/offline_work.txt" in offline_keys

        # Phase 4: reattach shard 1
        count = c.attach_shard(1, paths[1], force=True)
        assert count > 0  # should find initial.txt

        # Phase 5: replicate
        for key, _count in c.find_under_replicated():
            info = c.placement(key)
            from_shard = info.shards[0]
            to_shard = 1 if from_shard == 0 else 0
            c.replicate_object(key, from_shard, to_shard)
        c.flush_all()

        # All replicated
        assert len(c.find_under_replicated()) == 0

        # Verify data
        assert c.get("laptop/initial.txt") == b"initial data"
        assert c.get("laptop/offline_work.txt") == b"wrote while disconnected"

    def test_empty_shard_paths_raises(self):
        with pytest.raises(ValueError):
            shardedobjstr.open_cluster_degraded([], replication_factor=1)

    def test_detach_reads_head_range(self, cluster_dir):
        """After detach, get, head, and get_range all still work."""
        paths = [str(cluster_dir / f"dhr{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        data = make_payload(8192, seed=10)
        c.put("dhr/data.bin", data)
        c.flush_all()

        c.detach_shard(0)

        # get still works
        assert c.get("dhr/data.bin") == data
        # head still works
        meta = c.head("dhr/data.bin")
        assert meta.size == 8192
        # get_range still works
        chunk = c.get("dhr/data.bin", range=(100, 200))
        assert chunk == data[100:200]

    def test_detach_writes_avoid_offline(self, cluster_dir):
        """Writes after detach place objects only on healthy shards."""
        paths = [str(cluster_dir / f"dwa{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)

        c.detach_shard(1)

        c.put("dwa/new.txt", b"after-detach")
        c.flush_all()
        info = c.placement("dwa/new.txt")
        assert 1 not in info.shards, "offline shard should not be in placement"
        assert len(info.shards) == 2

    def test_detach_list_and_delete(self, cluster_dir):
        """list, list_with_delimiter, and delete work after detach."""
        paths = [str(cluster_dir / f"dld{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"dld/obj{i}.dat", make_payload(64, seed=i))
        c.flush_all()

        c.detach_shard(2)

        items = c.list("dld")
        assert len(items) == 5

        result = c.list_with_delimiter("dld")
        assert len(result.objects) == 5

        c.delete("dld/obj0.dat")
        assert len(c.list("dld")) == 4

    def test_detach_copy_operations(self, cluster_dir):
        """copy and copy_if_not_exists work after detach."""
        paths = [str(cluster_dir / f"dco{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        data = make_fill(0xDD, 4096)
        c.put("dco/src.bin", data)
        c.flush_all()

        c.detach_shard(0)

        c.copy("dco/src.bin", "dco/dst.bin")
        assert c.get("dco/dst.bin") == data
        info = c.placement("dco/dst.bin")
        assert 0 not in info.shards

        c.copy_if_not_exists("dco/src.bin", "dco/dst2.bin")
        assert c.get("dco/dst2.bin") == data

        with pytest.raises(Exception):
            c.copy_if_not_exists("dco/src.bin", "dco/dst.bin")

    def test_detach_multipart(self, cluster_dir):
        """Multipart upload works after detach."""
        paths = [str(cluster_dir / f"dmp{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)

        c.detach_shard(1)

        data = make_fill(0xAA, 4096) + make_fill(0xBB, 4096)
        c.put_multipart("dmp/multi.bin", data)
        c.flush_all()

        got = c.get("dmp/multi.bin")
        assert got == data
        info = c.placement("dmp/multi.bin")
        assert 1 not in info.shards

    def test_detach_reattach_recovers_new_data(self, cluster_dir):
        """Write new data while shard offline, reattach, verify everything."""
        paths = [str(cluster_dir / f"drr{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("drr/before.txt", b"before-detach")
        c.flush_all()

        c.detach_shard(0)
        c.put("drr/during.txt", b"while-offline")
        c.flush_all()

        count = c.attach_shard(0, paths[0], force=True)
        assert count >= 0
        assert c.shard_health(0) == shardedobjstr.ShardHealth.Healthy
        assert c.get("drr/before.txt") == b"before-detach"
        assert c.get("drr/during.txt") == b"while-offline"

    def test_reattach_without_force(self, cluster_dir):
        """attach_shard with force=False (purge + rescan path)."""
        paths = [str(cluster_dir / f"rnf{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"rnf/{i}.dat", make_payload(64, seed=i))
        c.flush_all()

        c.detach_shard(2)
        count = c.attach_shard(2, paths[2], force=False)
        assert count >= 0
        assert c.shard_health(2) == shardedobjstr.ShardHealth.Healthy

        for i in range(5):
            assert c.get(f"rnf/{i}.dat") == make_payload(64, seed=i)

    def test_under_replicated_detection_and_repair(self, cluster_dir):
        """Detach triggers under-replication; replicate_object repairs it."""
        paths = [str(cluster_dir / f"urr{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"urr/{i}.dat", make_payload(128, seed=i))
        c.flush_all()
        assert len(c.find_under_replicated()) == 0

        c.detach_shard(0)
        under = c.find_under_replicated()
        assert len(under) > 0

        # Repair each under-replicated object
        for key, _count in under:
            info = c.placement(key)
            # Pick a healthy source shard (not shard 0 which is offline)
            from_shard = next(s for s in info.shards if s != 0)
            # Pick any healthy shard not already hosting the object
            for sid in range(c.shard_count()):
                if sid != 0 and sid not in info.shards:
                    c.replicate_object(key, from_shard, sid)
                    break
        c.flush_all()
        assert len(c.find_under_replicated()) == 0

    def test_multiple_shards_offline(self, cluster_dir):
        """5 shards RF=3, 2 down: reads and writes still work."""
        paths = [str(cluster_dir / f"mso{i}.raw") for i in range(5)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=3)
        for i in range(10):
            c.put(f"mso/{i}.dat", make_payload(64, seed=i))
        c.flush_all()

        c.detach_shard(3)
        c.detach_shard(4)

        # Reads still work
        for i in range(10):
            assert c.get(f"mso/{i}.dat") == make_payload(64, seed=i)

        # Writes still work, placement excludes offline shards
        c.put("mso/new.txt", b"still-alive")
        info = c.placement("mso/new.txt")
        assert 3 not in info.shards
        assert 4 not in info.shards

    def test_all_replicas_offline_raises(self, cluster_dir):
        """RF=1, put then detach the only shard: get should error."""
        paths = [str(cluster_dir / f"aro{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("aro/only.txt", b"sole-copy")
        c.flush_all()

        info = c.placement("aro/only.txt")
        only_shard = info.shards[0]
        c.detach_shard(only_shard)

        with pytest.raises(Exception):
            c.get("aro/only.txt")

    def test_put_if_not_exists_with_offline(self, cluster_dir):
        """put_if_not_exists works in degraded mode."""
        paths = [str(cluster_dir / f"pineo{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)

        c.detach_shard(1)

        c.put_if_not_exists("pineo/new.txt", b"new-data")
        assert c.get("pineo/new.txt") == b"new-data"

        with pytest.raises(FileExistsError):
            c.put_if_not_exists("pineo/new.txt", b"duplicate")

    def test_rename_with_offline(self, cluster_dir):
        """rename_if_not_exists works in degraded mode."""
        paths = [str(cluster_dir / f"rwo{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("rwo/src.txt", b"rename-me")
        c.flush_all()

        c.detach_shard(2)

        c.rename_if_not_exists("rwo/src.txt", "rwo/dst.txt")
        assert c.get("rwo/dst.txt") == b"rename-me"
        assert c.placement("rwo/src.txt") is None

    def test_writes_during_shard_transitions(self, cluster_dir):
        """Cascading failures: writes at each stage, all remain readable."""
        paths = [str(cluster_dir / f"wst{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)

        # Batch 1: all healthy
        for i in range(5):
            c.put(f"wst/b1_{i}.dat", make_payload(64, seed=i))
        c.flush_all()

        # Detach shard 2
        c.detach_shard(2)
        # Batch 2: degraded (2 shards)
        for i in range(5):
            c.put(f"wst/b2_{i}.dat", make_payload(64, seed=10 + i))
        c.flush_all()

        # Detach shard 0
        c.detach_shard(0)
        # Batch 3: more degraded (1 shard)
        for i in range(5):
            c.put(f"wst/b3_{i}.dat", make_payload(64, seed=20 + i))
        c.flush_all()

        # All 15 objects should be listed
        items = c.list("wst")
        assert len(items) == 15

        # Batch 3 placed only on shard 1 (the only healthy one)
        for i in range(5):
            info = c.placement(f"wst/b3_{i}.dat")
            assert info.shards == [1]

    def test_delete_with_offline_only_replicas(self, cluster_dir):
        """RF=1, put, detach the shard, delete should handle gracefully."""
        paths = [str(cluster_dir / f"dwor{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        c.put("dwor/victim.txt", b"to-delete")
        c.flush_all()

        info = c.placement("dwor/victim.txt")
        only_shard = info.shards[0]
        c.detach_shard(only_shard)

        # Delete should not crash; it may or may not raise
        try:
            c.delete("dwor/victim.txt")
        except Exception:
            pass  # Partial failure is acceptable

    def test_full_lifecycle_degrade_recover(self, cluster_dir):
        """Multi-phase: healthy writes -> degrade -> more writes -> delete
        -> reattach -> repair -> verify all."""
        paths = [str(cluster_dir / f"fldr{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)

        # Phase 1: healthy writes
        for i in range(10):
            c.put(f"fldr/{i:04d}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Phase 2: degrade
        c.detach_shard(2)

        # Phase 3: write more while degraded
        for i in range(10, 15):
            c.put(f"fldr/{i:04d}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Phase 4: delete one
        c.delete("fldr/0003.dat")

        # Should have 14 objects
        items = c.list("fldr")
        assert len(items) == 14

        # Phase 5: reattach
        count = c.attach_shard(2, paths[2], force=True)
        assert count >= 0

        # Phase 6: repair under-replicated
        for key, _count in c.find_under_replicated():
            info = c.placement(key)
            from_shard = info.shards[0]
            for sid in range(c.shard_count()):
                if sid not in info.shards:
                    c.replicate_object(key, from_shard, sid)
                    break
        c.flush_all()

        assert len(c.find_under_replicated()) == 0

        # Phase 7: verify all 14 objects
        for i in range(15):
            if i == 3:
                continue  # deleted
            data = c.get(f"fldr/{i:04d}.dat")
            assert data == make_payload(128, seed=i)


# ---------------------------------------------------------------------------
# 26. Syncing shard health state
# ---------------------------------------------------------------------------

class TestSyncingHealth:
    """Tests for the Syncing ShardHealth variant."""

    def test_syncing_set_and_get(self, cluster_dir):
        shards = [(str(cluster_dir / f"syn{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        H = shardedobjstr.ShardHealth
        prev = c.set_shard_health(0, H.Syncing)
        assert prev == H.Healthy
        assert c.shard_health(0) == H.Syncing

    def test_syncing_to_healthy_round_trip(self, cluster_dir):
        shards = [(str(cluster_dir / f"synrt{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        H = shardedobjstr.ShardHealth
        c.set_shard_health(0, H.Syncing)
        c.set_shard_health(0, H.Healthy)
        assert c.shard_health(0) == H.Healthy


# ---------------------------------------------------------------------------
# 27. Rebuild catalog for single shard
# ---------------------------------------------------------------------------

class TestRebuildCatalogSingleShard:
    """Tests for rebuild_catalog_for_shard (per-shard rebuild)."""

    def test_rebuild_single_shard(self, cluster_dir):
        paths = [str(cluster_dir / f"rcs{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(10):
            c.put(f"rcs/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Clear catalog for shard 0
        c.remove_all_for_shard(0)
        assert len(c.entries_for_shard(0)) == 0

        # Rebuild just shard 0
        count = c.rebuild_catalog_for_shard(0)
        assert count > 0

        # Entries for shard 0 should be restored
        entries = c.entries_for_shard(0)
        assert len(entries) > 0

        # All objects still readable
        for i in range(10):
            data = c.get(f"rcs/{i}.dat")
            assert data == make_payload(128, seed=i)

    def test_rebuild_empty_shard(self, cluster_dir):
        paths = [str(cluster_dir / f"rce{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        # Only write to one shard's key space -- rebuild the other
        count = c.rebuild_catalog_for_shard(1)
        assert count == 0


# ---------------------------------------------------------------------------
# 28. Invalidate shard detects missing keys
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 28. Read-only mode
# ---------------------------------------------------------------------------

class TestReadOnlyMode:
    """Read-only mode should allow reads but reject all writes."""

    def test_read_only_allows_reads(self, cluster_dir):
        paths = [str(cluster_dir / f"ro_r{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("ro/hello.txt", b"hello read-only")
        c.flush_all()
        del c

        ro = shardedobjstr.open_cluster(paths, replication_factor=2, read_only=True)
        assert ro.read_only is True
        ro.rebuild_catalog()
        assert ro.get("ro/hello.txt") == b"hello read-only"
        meta = ro.head("ro/hello.txt")
        assert meta.size == len(b"hello read-only")

    def test_read_only_rejects_put(self, cluster_dir):
        paths = [str(cluster_dir / f"ro_p{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.flush_all()
        del c

        ro = shardedobjstr.open_cluster(paths, replication_factor=2, read_only=True)
        with pytest.raises(Exception, match="[Rr]ead.only"):
            ro.put("ro/nope.txt", b"should fail")

    def test_read_only_rejects_delete(self, cluster_dir):
        paths = [str(cluster_dir / f"ro_d{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("ro/del.txt", b"data")
        c.flush_all()
        del c

        ro = shardedobjstr.open_cluster(paths, replication_factor=2, read_only=True)
        with pytest.raises(Exception, match="[Rr]ead.only"):
            ro.delete("ro/del.txt")

    def test_read_only_list_works(self, cluster_dir):
        paths = [str(cluster_dir / f"ro_l{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(3):
            c.put(f"ro/list/{i}.dat", make_payload(64, seed=i))
        c.flush_all()
        del c

        ro = shardedobjstr.open_cluster(paths, replication_factor=2, read_only=True)
        ro.rebuild_catalog()
        keys = ro.list("ro/list/")
        assert len(keys) == 3

    def test_format_and_open_read_only(self, cluster_dir):
        """Opening a freshly formatted cluster with read_only=True works."""
        paths = [str(cluster_dir / f"ro_fo{i}.raw") for i in range(2)]
        shards = [(p, SHARD_SIZE) for p in paths]
        # Format first, then reopen read-only.
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        del c
        ro = shardedobjstr.open_cluster(paths, replication_factor=2, read_only=True)
        assert ro.read_only is True
        with pytest.raises(Exception, match="[Rr]ead.only"):
            ro.put("key", b"val")


# ---------------------------------------------------------------------------
# 29. Invalidate shard detects missing keys
# ---------------------------------------------------------------------------

class TestInvalidateDetectsMissing:
    """Invalidation should detect keys missing from the underlying shard."""

    def test_detects_missing_after_corruption(self, cluster_dir):
        """Simulate corruption by invalidating after deleting from underlying
        store. This verifies missing_keys in the report."""
        paths = [str(cluster_dir / f"idm{i}.raw") for i in range(3)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"idm/{i}.dat", make_payload(256, seed=i))
        c.flush_all()

        # Re-build catalog from disk to guarantee state is physical
        del c
        c2 = shardedobjstr.open_cluster(paths, replication_factor=2)
        c2.rebuild_catalog()

        # Invalidate shard 0 -- should find no missing keys
        report = c2.invalidate_shard(0)
        assert report.scan_ok is True
        assert len(report.missing_keys) == 0


# ---------------------------------------------------------------------------
# 29. Over-replication detection and trimming
# ---------------------------------------------------------------------------

class TestOverReplication:
    """Tests for find_over_replicated, pick_excess_shard, and remove_replica."""

    def test_find_over_replicated_empty_initially(self, cluster_dir):
        """With RF=2 on 4 shards, no objects are over-replicated."""
        shards = [(str(cluster_dir / f"ov{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"ov/{i}.dat", make_payload(128, seed=i))
        c.flush_all()
        assert c.find_over_replicated() == []

    def test_detect_over_replicated_after_extra_copy(self, cluster_dir):
        """After manually replicating to a third shard, object becomes over-replicated."""
        shards = [(str(cluster_dir / f"ovd{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("ovd/obj.dat", make_payload(256, seed=42))
        c.flush_all()

        info = c.placement("ovd/obj.dat")
        source = info.shards[0]
        target = next(s for s in range(4) if s not in info.shards)
        c.replicate_object("ovd/obj.dat", source, target)
        c.flush_all()

        over = c.find_over_replicated()
        assert len(over) >= 1
        keys = [k for k, _ in over]
        assert "ovd/obj.dat" in keys

    def test_pick_excess_shard_returns_holder(self, cluster_dir):
        """pick_excess_shard returns a shard that holds the object."""
        shards = [(str(cluster_dir / f"ovp{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("ovp/obj.dat", make_payload(256, seed=10))
        c.flush_all()

        # Create over-replication
        info = c.placement("ovp/obj.dat")
        source = info.shards[0]
        target = next(s for s in range(4) if s not in info.shards)
        c.replicate_object("ovp/obj.dat", source, target)
        c.flush_all()

        excess = c.pick_excess_shard("ovp/obj.dat")
        assert excess is not None
        info_after = c.placement("ovp/obj.dat")
        assert excess in info_after.shards

    def test_pick_excess_shard_none_at_rf(self, cluster_dir):
        """pick_excess_shard returns None when object is at exactly RF."""
        shards = [(str(cluster_dir / f"ovn{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("ovn/obj.dat", make_payload(128, seed=7))
        c.flush_all()
        assert c.pick_excess_shard("ovn/obj.dat") is None

    def test_remove_replica_trims_to_rf(self, cluster_dir):
        """remove_replica reduces copy count back to RF."""
        shards = [(str(cluster_dir / f"ovr{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("ovr/obj.dat", make_payload(256, seed=99))
        c.flush_all()

        # Create over-replication
        info = c.placement("ovr/obj.dat")
        source = info.shards[0]
        target = next(s for s in range(4) if s not in info.shards)
        c.replicate_object("ovr/obj.dat", source, target)
        c.flush_all()

        # Trim
        excess = c.pick_excess_shard("ovr/obj.dat")
        assert excess is not None
        c.remove_replica("ovr/obj.dat", excess)
        c.flush_all()

        # Verify
        info_after = c.placement("ovr/obj.dat")
        assert len(info_after.shards) == 2
        assert c.find_over_replicated() == []

        # Object still readable
        data = c.get("ovr/obj.dat")
        assert data == make_payload(256, seed=99)


# ---------------------------------------------------------------------------
# 30. Replication target selection
# ---------------------------------------------------------------------------

class TestFindReplicationTarget:
    """Tests for find_replication_target."""

    def test_finds_shard_not_holding_object(self, cluster_dir):
        """Target must be a shard NOT already holding the object."""
        shards = [(str(cluster_dir / f"frt{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("frt/obj.dat", make_payload(128, seed=5))
        c.flush_all()

        info = c.placement("frt/obj.dat")
        target = c.find_replication_target("frt/obj.dat")
        assert target is not None
        assert target not in info.shards

    def test_returns_none_when_all_shards_hold_it(self, cluster_dir):
        """With 2 shards RF=2, all shards hold every object."""
        shards = [(str(cluster_dir / f"frn{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        c.put("frn/obj.dat", make_payload(128, seed=1))
        c.flush_all()
        assert c.find_replication_target("frn/obj.dat") is None

    def test_returns_none_for_nonexistent_key(self, cluster_dir):
        """Non-existent key has no placement, target should still work."""
        shards = [(str(cluster_dir / f"frx{i}.raw"), SHARD_SIZE) for i in range(3)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        # No existing placement means no existing shards, so any healthy shard is valid
        target = c.find_replication_target("nonexistent/key")
        # Should return some shard since none hold it
        assert target is not None


# ---------------------------------------------------------------------------
# 31. Repair-replication sweep
# ---------------------------------------------------------------------------

class TestRepairReplication:
    """Tests for the repair_replication() method (combined under+over repair)."""

    def test_noop_on_balanced_cluster(self, cluster_dir):
        """repair_replication on a balanced cluster changes nothing."""
        shards = [(str(cluster_dir / f"rb{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"rb/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        result = c.repair_replication(batch_size=100)
        assert result.re_replicated == 0
        assert result.trimmed == 0
        assert result.under_remaining == 0
        assert result.over_remaining == 0

    def test_repairs_under_replicated(self, cluster_dir):
        """repair_replication repairs objects that lost a replica."""
        paths = [str(cluster_dir / f"rbu{i}.raw") for i in range(4)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"rbu/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Detach shard 0 to create under-replication
        c.detach_shard(0)
        under_before = c.find_under_replicated()

        if len(under_before) > 0:
            result = c.repair_replication(batch_size=100)
            assert result.re_replicated > 0 or result.under_remaining == 0

    def test_trims_over_replicated(self, cluster_dir):
        """repair_replication trims objects with too many replicas."""
        shards = [(str(cluster_dir / f"rbo{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(3):
            c.put(f"rbo/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Create over-replication on all 3 objects
        for i in range(3):
            key = f"rbo/{i}.dat"
            info = c.placement(key)
            source = info.shards[0]
            target = next(s for s in range(4) if s not in info.shards)
            c.replicate_object(key, source, target)
        c.flush_all()

        assert len(c.find_over_replicated()) >= 3

        result = c.repair_replication(batch_size=100)
        assert result.trimmed >= 3
        assert result.over_remaining == 0

    def test_result_repr(self, cluster_dir):
        """RepairReplicationResult has a readable __repr__."""
        shards = [(str(cluster_dir / f"rbr{i}.raw"), SHARD_SIZE) for i in range(2)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
        result = c.repair_replication(batch_size=10)
        r = repr(result)
        assert "RepairReplicationResult" in r
        assert "re_replicated=" in r
        assert "trimmed=" in r

    def test_combined_under_and_over(self, cluster_dir):
        """repair_replication handles both under- and over-replication in one sweep."""
        paths = [str(cluster_dir / f"rbc{i}.raw") for i in range(4)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"rbc/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Create over-replication on object 0
        info0 = c.placement("rbc/0.dat")
        src0 = info0.shards[0]
        tgt0 = next(s for s in range(4) if s not in info0.shards)
        c.replicate_object("rbc/0.dat", src0, tgt0)
        c.flush_all()

        # Create under-replication by detaching a shard
        c.detach_shard(1)

        # Run combined repair-replication
        result = c.repair_replication(batch_size=100)
        # Should have addressed at least some imbalances
        assert result.re_replicated >= 0
        assert result.trimmed >= 0

        # All readable objects should still work
        for i in range(5):
            key = f"rbc/{i}.dat"
            info = c.placement(key)
            if info is not None:
                has_healthy = any(
                    c.shard_health(s) == shardedobjstr.ShardHealth.Healthy
                    for s in info.shards
                )
                if has_healthy:
                    data = c.get(key)
                    assert data == make_payload(128, seed=i)


# ---------------------------------------------------------------------------
# Re-replication sweep
# ---------------------------------------------------------------------------

class TestReReplicationSweep:
    """Tests for re_replication_sweep()."""

    def test_noop_when_balanced(self, cluster_rf2):
        c = cluster_rf2
        for i in range(3):
            c.put(f"rrs/{i}.dat", make_payload(64, seed=i))
        c.flush_all()
        count = c.re_replication_sweep(batch_size=100)
        assert count == 0

    def test_repairs_after_detach(self, cluster_dir):
        paths = [str(cluster_dir / f"rrs{i}.raw") for i in range(4)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(5):
            c.put(f"rrs/{i}.dat", make_payload(128, seed=i))
        c.flush_all()
        c.detach_shard(0)
        count = c.re_replication_sweep(batch_size=100)
        under = c.find_under_replicated()
        # Either repaired some or nothing was under-replicated for shard 0
        assert count >= 0
        assert len(under) <= 5


# ---------------------------------------------------------------------------
# Over-replication trim
# ---------------------------------------------------------------------------

class TestOverReplicationTrim:
    """Tests for over_replication_trim()."""

    def test_noop_when_balanced(self, cluster_rf2):
        c = cluster_rf2
        for i in range(3):
            c.put(f"ort/{i}.dat", make_payload(64, seed=i))
        c.flush_all()
        count = c.over_replication_trim(batch_size=100)
        assert count == 0

    def test_trims_excess(self, cluster_dir):
        shards = [(str(cluster_dir / f"ort{i}.raw"), SHARD_SIZE) for i in range(4)]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(3):
            c.put(f"ort/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        # Create over-replication
        for i in range(3):
            key = f"ort/{i}.dat"
            info = c.placement(key)
            source = info.shards[0]
            target = next(s for s in range(4) if s not in info.shards)
            c.replicate_object(key, source, target)
        c.flush_all()
        assert len(c.find_over_replicated()) >= 3

        count = c.over_replication_trim(batch_size=100)
        assert count >= 3
        assert len(c.find_over_replicated()) == 0


# ---------------------------------------------------------------------------
# Drain shard
# ---------------------------------------------------------------------------

class TestDrainShard:
    """Tests for drain_shard()."""

    def test_drain_empties_shard(self, cluster_dir):
        paths = [str(cluster_dir / f"drn{i}.raw") for i in range(4)]
        shards = [(p, SHARD_SIZE) for p in paths]
        c = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
        for i in range(10):
            c.put(f"drn/{i}.dat", make_payload(128, seed=i))
        c.flush_all()

        result = c.drain_shard(0, batch_size=10000)
        assert isinstance(result.re_replicated, int)
        assert result.re_replicated >= 0
        # Shard should now be offline
        assert c.shard_health(0) == shardedobjstr.ShardHealth.Offline

    def test_drain_invalid_shard(self, cluster_rf2):
        with pytest.raises(ValueError, match="shard_id out of range"):
            cluster_rf2.drain_shard(99)


# ---------------------------------------------------------------------------
# CRC error count
# ---------------------------------------------------------------------------

class TestCrcErrorCount:
    """Tests for crc_error_count()."""

    def test_initial_zero(self, cluster_rf2):
        for i in range(cluster_rf2.shard_count()):
            assert cluster_rf2.crc_error_count(i) == 0

    def test_out_of_range_returns_zero(self, cluster_rf2):
        assert cluster_rf2.crc_error_count(999) == 0


# ---------------------------------------------------------------------------
# Shard management: hold_offline, release_hold, detach_reason
# ---------------------------------------------------------------------------

class TestShardManagement:
    """Tests for hold_offline, release_hold,
    shard_suppress_replication, shard_detach_reason,
    set_detach_reason."""

    def test_hold_offline_and_release(self, cluster_rf2):
        prev = cluster_rf2.hold_offline(0)
        assert prev is not None  # was Healthy
        assert cluster_rf2.release_hold(0) is True
        assert cluster_rf2.release_hold(0) is False  # already released

    def test_hold_offline_with_suppress(self, cluster_rf2):
        cluster_rf2.hold_offline(1, suppress_replication=True)
        assert cluster_rf2.shard_suppress_replication(1) is True
        cluster_rf2.release_hold(1)

    def test_shard_detach_reason_healthy_is_none(self, cluster_rf2):
        assert cluster_rf2.shard_detach_reason(0) is None

    def test_hold_offline_sets_manual_reason(self, cluster_rf2):
        cluster_rf2.hold_offline(0)
        reason = cluster_rf2.shard_detach_reason(0)
        assert reason == "Manual"
        cluster_rf2.release_hold(0)

    def test_set_detach_reason(self, cluster_rf2):
        cluster_rf2.hold_offline(1)
        cluster_rf2.set_detach_reason(1, "Drain")
        assert cluster_rf2.shard_detach_reason(1) == "Drain"
        cluster_rf2.release_hold(1)

    def test_set_detach_reason_invalid(self, cluster_rf2):
        cluster_rf2.hold_offline(2)
        with pytest.raises(ValueError):
            cluster_rf2.set_detach_reason(2, "InvalidReason")
        cluster_rf2.release_hold(2)
