"""Tests for core CRUD operations, listing, copy, rename, multipart,
format/open, path handling, and context managers.
"""

import pytest

import rawobjstr

from conftest import SMALL_DEVICE


# ===================================================================
# Basic operations (put, get, delete, list, copy, head, flush/reopen)
# ===================================================================

class TestBasicOps:

    def test_put_get_round_trip(self, store):
        store.put("data/00000.db", b"hello world")
        data = store.get("data/00000.db")
        assert data == b"hello world"

    def test_put_overwrite(self, store):
        store.put("test.txt", b"version1")
        store.put("test.txt", b"version2")
        assert store.get("test.txt") == b"version2"

    def test_delete_file(self, store):
        store.put("to_delete.db", b"data")
        store.delete("to_delete.db")
        with pytest.raises(FileNotFoundError):
            store.get("to_delete.db")

    def test_delete_idempotent(self, store):
        # Per ObjectStore trait, delete of missing key returns Ok
        store.delete("nonexistent")

    def test_get_not_found(self, store):
        with pytest.raises(FileNotFoundError):
            store.get("nonexistent")

    def test_list_files(self, store):
        store.put("data/a.db", b"a")
        store.put("data/b.db", b"bb")
        store.put("meta/manifest", b"m")

        all_entries = store.list()
        assert len(all_entries) == 3

        data_entries = store.list("data")
        assert len(data_entries) == 2

    def test_list_with_delimiter(self, store):
        store.put("data/a.db", b"a")
        store.put("data/sub/b.db", b"b")
        store.put("manifest", b"m")

        result = store.list_with_delimiter()
        # "manifest" is a direct child, "data" is a common prefix
        assert len(result.objects) == 1
        assert len(result.common_prefixes) >= 1

    def test_copy_file(self, store):
        store.put("src.db", b"copy me")
        store.copy("src.db", "dst.db")
        assert store.get("dst.db") == b"copy me"
        assert store.get("src.db") == b"copy me"  # source still exists

    def test_copy_if_not_exists(self, store):
        store.put("src.db", b"data")
        store.put("dst.db", b"existing")
        with pytest.raises(FileExistsError):
            store.copy_if_not_exists("src.db", "dst.db")

    def test_head_file(self, store):
        store.put("sized.db", b"12345")
        meta = store.head("sized.db")
        assert meta.size == 5
        assert meta.location == "sized.db"

    def test_flush_and_reopen(self, store_path):
        s = rawobjstr.format(store_path, size=SMALL_DEVICE)
        s.put("persist.db", b"survived")
        s.flush_index()
        del s

        s2 = rawobjstr.open(store_path)
        assert s2.get("persist.db") == b"survived"


# ===================================================================
# PutMode edge cases
# ===================================================================

class TestPutMode:

    def test_put_mode_create_on_new_key(self, store):
        store.put("create_new.bin", b"new file", mode="create")
        assert store.get("create_new.bin") == b"new file"

    def test_put_mode_create_rejects_existing(self, store):
        store.put("create_dup.bin", b"first")
        with pytest.raises(FileExistsError):
            store.put("create_dup.bin", b"second", mode="create")
        # Original data unchanged
        assert store.get("create_dup.bin") == b"first"


# ===================================================================
# Copy edge cases
# ===================================================================

class TestCopyEdgeCases:

    def test_copy_source_not_found(self, store):
        with pytest.raises(FileNotFoundError):
            store.copy("nonexistent", "dest")

    def test_copy_if_not_exists_happy_path(self, store):
        store.put("src.bin", b"copy me")
        store.copy_if_not_exists("src.bin", "dst.bin")
        assert store.get("dst.bin") == b"copy me"

    def test_copy_if_not_exists_source_missing(self, store):
        with pytest.raises(FileNotFoundError):
            store.copy_if_not_exists("nonexistent", "dest")

    def test_copy_if_not_exists_dest_exists(self, store):
        store.put("src.bin", b"source")
        store.put("dst.bin", b"existing")
        with pytest.raises(FileExistsError):
            store.copy_if_not_exists("src.bin", "dst.bin")
        # Dest unchanged
        assert store.get("dst.bin") == b"existing"

    def test_copy_to_same_location(self, store):
        store.put("self_copy.bin", b"self")
        store.copy("self_copy.bin", "self_copy.bin")
        assert store.get("self_copy.bin") == b"self"


# ===================================================================
# Delete edge cases
# ===================================================================

class TestDeleteEdgeCases:

    def test_delete_missing_is_idempotent(self, store):
        store.delete("ghost.bin")

    def test_delete_then_put_same_key(self, store):
        store.put("reuse.bin", b"first")
        store.delete("reuse.bin")
        store.put("reuse.bin", b"second")
        assert store.get("reuse.bin") == b"second"


# ===================================================================
# Head / metadata edge cases
# ===================================================================

class TestHeadEdgeCases:

    def test_head_missing_file(self, store):
        with pytest.raises(FileNotFoundError):
            store.head("nope.bin")

    def test_head_returns_correct_metadata(self, store):
        payload = bytes(12345)
        store.put("meta_test.bin", payload)
        meta = store.head("meta_test.bin")
        assert meta.size == 12345
        assert meta.location == "meta_test.bin"


# ===================================================================
# List edge cases
# ===================================================================

class TestListEdgeCases:

    def test_list_empty_store(self, store):
        assert store.list() == []

    def test_list_with_delimiter_empty_store(self, store):
        result = store.list_with_delimiter()
        assert result.objects == []
        assert result.common_prefixes == []

    def test_list_prefix_no_match(self, store):
        store.put("alpha/file.bin", b"a")
        entries = store.list("beta")
        assert len(entries) == 0

    def test_list_with_delimiter_nested_dirs(self, store):
        store.put("a/b/c/d.bin", b"deep")
        store.put("a/b/e.bin", b"mid")
        store.put("a/f.bin", b"shallow")

        # Top-level: "a" is a common prefix
        top = store.list_with_delimiter()
        assert len(top.objects) == 0
        assert len(top.common_prefixes) == 1

        # Under "a": "b" is a common prefix, "f.bin" is an object
        under_a = store.list_with_delimiter("a")
        assert len(under_a.objects) == 1
        assert len(under_a.common_prefixes) == 1

        # Under "a/b": "c" is a common prefix, "e.bin" is an object
        under_ab = store.list_with_delimiter("a/b")
        assert len(under_ab.objects) == 1
        assert len(under_ab.common_prefixes) == 1

    def test_list_with_delimiter_with_prefix(self, store):
        store.put("table/data/0001.db", b"d1")
        store.put("table/data/0002.db", b"d2")
        store.put("table/meta/manifest", b"m")

        result = store.list_with_delimiter("table")
        assert len(result.objects) == 0
        assert len(result.common_prefixes) == 2  # data and meta

        result = store.list_with_delimiter("table/data")
        assert len(result.objects) == 2
        assert len(result.common_prefixes) == 0


# ===================================================================
# Multipart upload
# ===================================================================

class TestMultipart:

    def test_multipart_single_part(self, store):
        upload = store.multipart("mp_single.bin")
        upload.put_part(b"single")
        upload.complete()
        assert store.get("mp_single.bin") == b"single"

    def test_multipart_zero_parts_empty_file(self, store):
        upload = store.multipart("mp_empty.bin")
        with pytest.raises(Exception):
            upload.complete()

    def test_multipart_abort_then_new_upload(self, store):
        upload = store.multipart("mp_abort.bin")
        upload.put_part(b"aborted data")
        upload.abort()
        with pytest.raises(FileNotFoundError):
            store.get("mp_abort.bin")

        # New upload to same path should succeed
        upload2 = store.multipart("mp_abort.bin")
        upload2.put_part(b"real data")
        upload2.complete()
        assert store.get("mp_abort.bin") == b"real data"

    def test_multipart_many_small_parts(self, store):
        upload = store.multipart("mp_many.bin")
        for i in range(100):
            upload.put_part(bytes([i] * 10))
        upload.complete()

        data = store.get("mp_many.bin")
        assert len(data) == 1000
        for i in range(100):
            chunk = data[i * 10 : (i + 1) * 10]
            assert all(b == i for b in chunk), f"part {i} content mismatch"

    def test_multipart_context_manager(self, store):
        with store.multipart("ctx.txt") as upload:
            upload.put_part(b"part1")
            upload.put_part(b"part2")
        assert store.get("ctx.txt") == b"part1part2"

    def test_multipart_abort_on_exception(self, store):
        try:
            with store.multipart("exc.txt") as upload:
                upload.put_part(b"data")
                raise ValueError("intentional")
        except ValueError:
            pass
        with pytest.raises(FileNotFoundError):
            store.get("exc.txt")


# ===================================================================
# Path handling
# ===================================================================

class TestPathHandling:

    def test_deeply_nested_path(self, store):
        store.put("a/b/c/d/e/f/g/h/i/j/k/l/m.bin", b"deep")
        assert store.get("a/b/c/d/e/f/g/h/i/j/k/l/m.bin") == b"deep"
        assert len(store.list()) == 1

    def test_paths_with_special_chars(self, store):
        test_paths = [
            "data-file.db",
            "data_file.db",
            "file.with.dots.db",
            "UPPERCASE.BIN",
            "MiXeD.CaSe",
        ]
        for p in test_paths:
            store.put(p, b"data")
            assert store.get(p) == b"data", f"failed for path: {p}"


# ===================================================================
# Rename
# ===================================================================

class TestRename:

    def test_rename(self, store):
        store.put("old.txt", b"data")
        store.rename("old.txt", "new.txt")
        assert store.get("new.txt") == b"data"
        with pytest.raises(FileNotFoundError):
            store.get("old.txt")

    def test_rename_if_not_exists(self, store):
        store.put("a.txt", b"data")
        store.put("b.txt", b"other")
        with pytest.raises(FileExistsError):
            store.rename_if_not_exists("a.txt", "b.txt")


# ===================================================================
# Overwrite size changes
# ===================================================================

class TestOverwriteSizeChanges:

    def test_overwrite_smaller_then_larger(self, store):
        large = bytes([0xAA] * 16384)
        store.put("resize.bin", large)

        small = bytes([0xBB] * 100)
        store.put("resize.bin", small)
        assert store.get("resize.bin") == small

        large2 = bytes([0xCC] * 32768)
        store.put("resize.bin", large2)
        assert store.get("resize.bin") == large2

    def test_overwrite_same_key_100_times(self, store):
        import struct
        for i in range(100):
            payload = struct.pack("<I", i)
            store.put("hotkey.bin", payload)

        data = store.get("hotkey.bin")
        val = struct.unpack("<I", data[:4])[0]
        assert val == 99

    def test_put_large_1mb(self, store):
        big = b"x" * (1024 * 1024)
        store.put("big.bin", big)
        assert store.get("big.bin") == big


# ===================================================================
# Format / open
# ===================================================================

class TestFormatOpen:

    def test_format_creates_store(self, store):
        info = store.device_info()
        assert info.file_count == 0
        assert info.device_size == SMALL_DEVICE

    def test_open_existing(self, store_path):
        rawobjstr.format(store_path, size=SMALL_DEVICE)
        s2 = rawobjstr.open(store_path)
        assert s2.device_info().file_count == 0

    def test_open_skip_verify(self, store_path):
        rawobjstr.format(store_path, size=SMALL_DEVICE)
        s2 = rawobjstr.open(store_path, mode="skip_verify")
        assert s2.device_info().file_count == 0

    def test_open_readonly(self, store_path):
        s = rawobjstr.format(store_path, size=SMALL_DEVICE)
        s.put("test.txt", b"hello")
        s.flush_index()
        del s

        s2 = rawobjstr.open(store_path, readonly=True)
        assert s2.is_read_only()
        assert s2.get("test.txt") == b"hello"

    def test_open_bad_mode(self, store_path):
        rawobjstr.format(store_path, size=SMALL_DEVICE)
        with pytest.raises(ValueError, match="mode must be"):
            rawobjstr.open(store_path, mode="bogus")

    def test_format_no_size_no_device(self, tmp_path):
        with pytest.raises(Exception):
            rawobjstr.format(str(tmp_path / "nofile.raw"))


# ===================================================================
# Context managers
# ===================================================================

class TestContextManager:

    def test_store_context_manager(self, store_path):
        with rawobjstr.format(store_path, size=SMALL_DEVICE) as s:
            s.put("ctx.txt", b"data")
        # Should have flushed on exit; delete ref so flock is released
        del s
        s2 = rawobjstr.open(store_path)
        assert s2.get("ctx.txt") == b"data"

    def test_multipart_context_manager_flush(self, store):
        with store.multipart("ctx_mp.txt") as upload:
            upload.put_part(b"hello ")
            upload.put_part(b"world")
        assert store.get("ctx_mp.txt") == b"hello world"
