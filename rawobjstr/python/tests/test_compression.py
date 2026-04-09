"""Tests for compression combined with metadata operations."""

import pytest

import rawobjstr

from conftest import SMALL_DEVICE, compressible_payload


# ===================================================================
# Compression + metadata interactions
# ===================================================================

class TestCompressionMetadata:

    @pytest.mark.parametrize("comp", ["zstd", "snappy", "gzip6"])
    def test_update_metadata_compressed_roundtrip(self, tmp_path, comp):
        path = str(tmp_path / f"comp_meta_{comp}.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression=comp)
        body = compressible_payload(10_000)
        s.put("f.bin", body)
        s.update_metadata("f.bin", b"my-metadata")

        got = s.get_metadata("f.bin")
        assert got == b"my-metadata"

        # Body should be intact (get returns body+meta)
        info = [r for r in s.list_full() if r.key == "f.bin"][0]
        full = s.get("f.bin")
        assert full[:info.body_size] == body

    @pytest.mark.parametrize("comp", ["zstd", "snappy", "gzip6"])
    def test_update_metadata_compressed_persists(self, tmp_path, comp):
        path = str(tmp_path / f"comp_meta_p_{comp}.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression=comp)
        body = compressible_payload(20_000)
        s.put("f.bin", body)
        s.update_metadata("f.bin", b"meta-v1")
        s.update_metadata("f.bin", b"meta-v2-updated")
        s.flush_index()
        del s

        s2 = rawobjstr.open(path)
        got = s2.get_metadata("f.bin")
        assert got == b"meta-v2-updated"

    @pytest.mark.parametrize("comp", ["zstd", "snappy", "gzip6"])
    def test_getraw_with_metadata_compressed(self, tmp_path, comp):
        path = str(tmp_path / f"getraw_meta_{comp}.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression=comp)
        body = compressible_payload(50_000)
        s.put("f.bin", body)
        s.update_metadata("f.bin", b"some-metadata")

        raw = s.getraw("f.bin")
        assert raw["compression"] == comp
        # uncompressed_size should account for body + metadata
        total_logical = len(body) + len(b"some-metadata")
        assert raw["uncompressed_size"] == total_logical

    def test_metadata_update_then_getraw_uncompressed(self, tmp_path):
        path = str(tmp_path / "meta_raw_unc.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("f.bin", b"body data")
        s.update_metadata("f.bin", b"meta-bytes")

        raw = s.getraw("f.bin")
        assert raw["compression"] == "none"
        assert raw["uncompressed_size"] == 0
        # data should be body + metadata concatenated
        assert raw["data"] == b"body datameta-bytes"

    def test_metadata_large_compressed(self, tmp_path):
        """Large metadata (32 KB) on compressed store."""
        path = str(tmp_path / "large_meta_comp.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression="zstd")
        body = compressible_payload(100_000)
        s.put("big.bin", body)
        meta = bytes(i & 0xFF for i in range(32768))
        s.update_metadata("big.bin", meta)

        got = s.get_metadata("big.bin")
        assert len(got) == 32768
        assert got == meta

        info = [r for r in s.list_full() if r.key == "big.bin"][0]
        assert info.meta_len == 32768
        assert info.body_size == len(body)

    def test_metadata_clear_on_compressed(self, tmp_path):
        """Clearing metadata (update to empty) on compressed store."""
        path = str(tmp_path / "clear_meta_comp.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression="snappy")
        body = compressible_payload(10_000)
        s.put("f.bin", body)
        s.update_metadata("f.bin", b"will-remove")
        s.update_metadata("f.bin", b"")

        got = s.get_metadata("f.bin")
        assert got == b""

        # Body should be intact
        assert s.get("f.bin") == body
