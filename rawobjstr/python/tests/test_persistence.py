"""Tests for flush/reopen cycles, allocator fragmentation, alignment
boundaries, many-file workloads, and zero-byte edge cases.
"""

import pytest

import rawobjstr

from conftest import (
    SMALL_DEVICE,
    MEDIUM_DEVICE,
    make_payload,
    make_small,
    verify_small,
)


# ===================================================================
# Zero-byte and alignment boundary files
# ===================================================================

class TestZeroByteAndAlignment:

    def test_zero_byte_file_put_get(self, store):
        with pytest.raises(Exception):
            store.put("empty.bin", b"")

    def test_zero_byte_file_persists(self, store_path):
        s = rawobjstr.format(store_path, size=SMALL_DEVICE)
        with pytest.raises(Exception):
            s.put("empty.bin", b"")

    def test_exact_alignment_boundary_4064(self, store):
        # 32-byte header + 4064-byte payload = 4096 exactly aligned
        payload = bytes([0xAA] * 4064)
        store.put("exact_align.bin", payload)
        assert store.get("exact_align.bin") == payload

    def test_alignment_boundary_plus_one_4065(self, store):
        # 32-byte header + 4065-byte payload = 4097 -> padded to 8192
        payload = bytes([0xBB] * 4065)
        store.put("align_plus_one.bin", payload)
        assert store.get("align_plus_one.bin") == payload

    def test_one_byte_file(self, store):
        store.put("one.bin", bytes([0x42]))
        assert store.get("one.bin") == bytes([0x42])

    def test_various_sizes_systematic(self, store):
        sizes = [1, 100, 4064, 4065, 4096, 8160, 8161, 16384, 65536]
        for size in sizes:
            key = f"size_{size}.bin"
            payload = bytes([(size & 0xFF)] * size)
            store.put(key, payload)
            data = store.get(key)
            assert len(data) == size, f"size {size} round-trip failed"
            assert data == payload, f"size {size} content mismatch"


# ===================================================================
# Persistence and flush cycles
# ===================================================================

class TestPersistence:

    def test_reopen_after_format_no_extra_flush(self, store_path):
        s = rawobjstr.format(store_path, size=SMALL_DEVICE)
        del s

        s2 = rawobjstr.open(store_path)
        s2.put("test.bin", b"hello")
        assert s2.get("test.bin") == b"hello"

    def test_many_flush_cycles(self, tmp_path):
        path = str(tmp_path / "flush_cycles.raw")
        s = rawobjstr.format(path, size=MEDIUM_DEVICE)

        for cycle in range(50):
            s.put(f"cycle_{cycle:03}.bin", make_small(cycle, 1024))
            s.flush_index()
        del s

        s2 = rawobjstr.open(path)
        entries = s2.list()
        assert len(entries) == 50

        for cycle in range(50):
            data = s2.get(f"cycle_{cycle:03}.bin")
            verify_small(data, cycle, 1024)

    def test_flush_alternates_index_regions(self, tmp_path):
        path = str(tmp_path / "alt_regions.raw")

        for i in range(10):
            if i == 0:
                s = rawobjstr.format(path, size=SMALL_DEVICE)
            else:
                s = rawobjstr.open(path)
            s.put(f"file_{i:02}.bin", make_small(i, 512))
            s.flush_index()
            del s

            # Reopen and verify
            s2 = rawobjstr.open(path)
            entries = s2.list()
            assert len(entries) == i + 1, f"after flush {i}, expected {i+1} files"
            del s2

    def test_multiple_open_close_cycles(self, tmp_path):
        path = str(tmp_path / "cycles.raw")
        rawobjstr.format(path, size=SMALL_DEVICE)

        for cycle in range(10):
            s = rawobjstr.open(path)
            s.put(f"cycle/{cycle:02}.bin", make_small(cycle, 2048))
            s.flush_index()
            del s

        s2 = rawobjstr.open(path)
        assert len(s2.list()) == 10


# ===================================================================
# Allocator edge cases
# ===================================================================

class TestAllocator:

    def test_fragmented_allocator_survives_reopen(self, tmp_path):
        path = str(tmp_path / "fragmented.raw")
        s = rawobjstr.format(path, size=MEDIUM_DEVICE)

        # Write 100 files
        for i in range(100):
            s.put(f"frag/{i:04}.bin", make_small(i, 4096))

        # Delete even-numbered files (creating 50 gaps)
        for i in range(0, 100, 2):
            s.delete(f"frag/{i:04}.bin")

        s.flush_index()
        del s

        # Reopen
        s2 = rawobjstr.open(path)
        entries = s2.list()
        assert len(entries) == 50

        # Freed space should be usable
        for i in range(100, 150):
            s2.put(f"frag/{i:04}.bin", make_small(i, 4096))

        entries = s2.list()
        assert len(entries) == 100

    def test_allocator_fill_delete_refill(self, store):
        # Fill with 4 KB files until full
        count = 0
        while True:
            try:
                store.put(f"fill/{count:05}.bin", make_small(count, 4096))
                count += 1
            except OSError:
                break
        assert count > 0

        # Delete all
        for i in range(count):
            store.delete(f"fill/{i:05}.bin")

        # Refill -- should fit same number
        count2 = 0
        while True:
            try:
                store.put(f"refill/{count2:05}.bin", make_small(count2, 4096))
                count2 += 1
            except OSError:
                break
        assert count == count2, f"refill should fit same number of files ({count} vs {count2})"


# ===================================================================
# Many small files
# ===================================================================

class TestManySmallFiles:

    def test_thousand_tiny_files(self, tmp_path):
        path = str(tmp_path / "tiny.raw")
        s = rawobjstr.format(path, size=MEDIUM_DEVICE)

        for i in range(1000):
            s.put(f"tiny/{i:05}.bin", make_small(i, 64))

        entries = s.list()
        assert len(entries) == 1000

        # Verify a sampling
        for i in range(0, 1000, 100):
            data = s.get(f"tiny/{i:05}.bin")
            verify_small(data, i, 64)

        # Flush and reopen
        s.flush_index()
        del s

        s2 = rawobjstr.open(path)
        entries = s2.list()
        assert len(entries) == 1000

    def test_many_files_with_various_prefixes(self, medium_store):
        store = medium_store

        # 10 "tables" x 50 files each = 500 files
        for t in range(10):
            for f in range(50):
                store.put(
                    f"table_{t:02}/data/{f:04}.db",
                    make_small(t * 50 + f, 512),
                )

        assert len(store.list()) == 500

        # Per-table count
        for t in range(10):
            entries = store.list(f"table_{t:02}/data")
            assert len(entries) == 50, f"table {t} should have 50 files"

        # list_with_delimiter at root
        root = store.list_with_delimiter()
        assert len(root.common_prefixes) == 10
        assert len(root.objects) == 0
