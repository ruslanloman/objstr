"""Tests for byte-range reads across block boundaries, fuzz, and overwrites."""

import random

import pytest

from conftest import make_payload


# ===================================================================
# GetRange edge cases
# ===================================================================

class TestGetRangeEdgeCases:

    def test_bounded_end_exceeds_size(self, store):
        store.put("range_test.bin", b"hello")
        # end > total_size should be clamped
        data = store.get("range_test.bin", range=(2, 1000))
        assert data == b"llo"

    def test_bounded_zero_length(self, store):
        store.put("range_test.bin", b"hello")
        data = store.get("range_test.bin", range=(3, 3))
        assert data == b""

    def test_get_byte_range(self, store):
        store.put("range.txt", b"0123456789")
        data = store.get("range.txt", range=(2, 5))
        assert data == b"234"


# ===================================================================
# Multi-block CRC boundary reads
# ===================================================================

class TestRangeReads:

    def test_range_read_within_first_block(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        data = store.get("range/1mb.bin", range=(10, 100))
        assert data == payload[10:100]

    def test_range_read_spanning_two_blocks(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        data = store.get("range/1mb.bin", range=(4000, 4200))
        assert data == payload[4000:4200]

    def test_range_read_spanning_many_blocks(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        data = store.get("range/1mb.bin", range=(100_000, 151_200))
        assert data == payload[100_000:151_200]

    def test_range_read_entire_file(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        data = store.get("range/1mb.bin", range=(0, 1024 * 1024))
        assert data == payload

    def test_range_read_last_block_boundary(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        start = 1024 * 1024 - 100
        data = store.get("range/1mb.bin", range=(start, 1024 * 1024))
        assert data == payload[start:]

    def test_range_read_exact_block_boundaries(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        data = store.get("range/1mb.bin", range=(4060, 8152))
        assert data == payload[4060:8152]

    def test_range_read_single_byte_each_block(self, store):
        payload = make_payload(1024 * 1024)
        store.put("range/1mb.bin", payload)
        for offset in [0, 4060, 8152, 100_000, 500_000, 1_048_575]:
            data = store.get("range/1mb.bin", range=(offset, offset + 1))
            assert data[0] == payload[offset], f"mismatch at offset {offset}"

    def test_range_reads_various_sizes(self, store):
        sizes = [1, 100, 4060, 4061, 4092, 8152, 8153, 100_000]
        for size in sizes:
            key = f"sized/{size}.bin"
            payload = make_payload(size)
            store.put(key, payload)

            # Full read
            data = store.get(key)
            assert data == payload, f"full read failed for size {size}"

            # Mid range
            if size > 10:
                mid = size // 2
                end = min(mid + 50, size)
                data = store.get(key, range=(mid, end))
                assert data == payload[mid:end], f"mid-range read failed for size {size}"

    def test_fuzz_random_range_reads_1mb(self, store):
        payload = make_payload(1024 * 1024)
        store.put("fuzz/1mb.bin", payload)

        rng = random.Random(42)
        file_size = len(payload)

        for _ in range(200):
            a = rng.randrange(file_size)
            b = rng.randrange(file_size)
            start, end = min(a, b), max(a, b) + 1
            end = min(end, file_size)
            data = store.get("fuzz/1mb.bin", range=(start, end))
            assert data == payload[start:end], f"fuzz bounded mismatch at {start}..{end}"

    def test_range_reads_after_overwrite(self, store):
        payload_v1 = make_payload(50_000)
        store.put("overwrite/data.bin", payload_v1)

        payload_v2 = bytes((i * 7 + 3) % 251 for i in range(100_000))
        store.put("overwrite/data.bin", payload_v2)

        assert store.get("overwrite/data.bin", range=(0, 100)) == payload_v2[0:100]
        assert store.get("overwrite/data.bin", range=(49_000, 51_000)) == payload_v2[49_000:51_000]
        assert store.get("overwrite/data.bin", range=(99_900, 100_000)) == payload_v2[99_900:100_000]
