"""Tests for verify, repair, tombstones, scrub, getraw, and repr strings."""

import pytest

import rawobjstr

from conftest import (
    SMALL_DEVICE,
    DATA_START,
    flip_byte,
    make_payload,
    compressible_payload,
)


# ===================================================================
# getraw() -- direct verification
# ===================================================================

class TestGetRaw:

    def test_getraw_uncompressed(self, tmp_path):
        path = str(tmp_path / "raw_unc.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        payload = b"hello raw world"
        s.put("f.bin", payload)

        raw = s.getraw("f.bin")
        assert isinstance(raw, dict)
        assert "data" in raw
        assert "uncompressed_size" in raw
        assert "compression" in raw
        assert raw["compression"] == "none"
        # Uncompressed: data == original, uncompressed_size == 0
        assert raw["data"] == payload
        assert raw["uncompressed_size"] == 0

    def test_getraw_compressed_zstd(self, tmp_path):
        path = str(tmp_path / "raw_zstd.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression="zstd")
        payload = compressible_payload(100_000)
        s.put("big.bin", payload)

        raw = s.getraw("big.bin")
        assert raw["compression"] == "zstd"
        assert raw["uncompressed_size"] == len(payload)
        assert len(raw["data"]) < len(payload), "compressed should be smaller"
        assert isinstance(raw["data"], bytes)

    def test_getraw_compressed_snappy(self, tmp_path):
        path = str(tmp_path / "raw_snappy.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression="snappy")
        payload = compressible_payload(50_000)
        s.put("s.bin", payload)

        raw = s.getraw("s.bin")
        assert raw["compression"] == "snappy"
        assert raw["uncompressed_size"] == len(payload)

    def test_getraw_not_found(self, tmp_path):
        path = str(tmp_path / "raw_nf.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        with pytest.raises(FileNotFoundError):
            s.getraw("ghost.bin")

    def test_getraw_small_object_not_compressed(self, tmp_path):
        """Objects < 4096 bytes bypass compression even on a compressed store."""
        path = str(tmp_path / "raw_small.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE, compression="zstd")
        small = b"tiny"
        s.put("tiny.bin", small)

        raw = s.getraw("tiny.bin")
        # Small objects are stored uncompressed
        assert raw["data"] == small
        assert raw["uncompressed_size"] == 0


# ===================================================================
# Tombstone lifecycle
# ===================================================================

class TestTombstones:

    def _create_store_with_tombstone(self, tmp_path):
        """Write one object, flush, corrupt block 0, reopen -> tombstone."""
        path = str(tmp_path / "tomb.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        # Write victim first so it lands at DATA_START
        s.put("victim.bin", b"will be corrupted")
        s.flush_index()

        # Write survivor AFTER flush so victim's position is at DATA_START
        s.put("survivor.bin", b"will survive")
        s.flush_index()
        del s

        # Corrupt block 0's content area of victim.bin
        flip_byte(path, DATA_START + 4)

        s2 = rawobjstr.open(path)
        return s2, path

    def test_tombstone_created_by_corruption(self, tmp_path):
        s, _ = self._create_store_with_tombstone(tmp_path)
        tombs = s.list_tombstones()
        assert len(tombs) >= 1
        paths = [t.path for t in tombs]
        assert "victim.bin" in paths

    def test_delete_tombstone_removes_entry(self, tmp_path):
        s, _ = self._create_store_with_tombstone(tmp_path)
        tombs_before = s.list_tombstones()
        victim_paths = [t.path for t in tombs_before if t.path == "victim.bin"]
        assert len(victim_paths) == 1

        result = s.delete_tombstone("victim.bin")
        assert result is True

        tombs_after = s.list_tombstones()
        after_paths = [t.path for t in tombs_after]
        assert "victim.bin" not in after_paths

    def test_delete_tombstone_nonexistent_returns_false(self, tmp_path):
        s, _ = self._create_store_with_tombstone(tmp_path)
        result = s.delete_tombstone("never_existed.bin")
        assert result is False

    def test_delete_tombstone_live_file_returns_false(self, tmp_path):
        s, _ = self._create_store_with_tombstone(tmp_path)
        # survivor.bin is a live file, not a tombstone
        result = s.delete_tombstone("survivor.bin")
        assert result is False

    def test_delete_tombstone_persists_after_flush(self, tmp_path):
        s, path = self._create_store_with_tombstone(tmp_path)
        s.delete_tombstone("victim.bin")
        s.flush_index()
        del s

        s2 = rawobjstr.open(path, mode="skip_verify")
        tombs = s2.list_tombstones()
        paths = [t.path for t in tombs]
        assert "victim.bin" not in paths

    def test_tombstone_entry_fields(self, tmp_path):
        s, _ = self._create_store_with_tombstone(tmp_path)
        tombs = s.list_tombstones()
        victim = [t for t in tombs if t.path == "victim.bin"]
        assert len(victim) == 1
        t = victim[0]
        assert t.size > 0
        assert isinstance(t.crc32c, int)
        assert isinstance(t.last_modified, str)
        assert isinstance(t.reason, str)
        assert len(t.reason) > 0
        assert t.tombstone_txn > 0

    def test_tombstone_entry_repr(self, tmp_path):
        s, _ = self._create_store_with_tombstone(tmp_path)
        tombs = s.list_tombstones()
        victim = [t for t in tombs if t.path == "victim.bin"][0]
        r = repr(victim)
        assert "TombstoneEntry" in r
        assert "victim.bin" in r

    def test_tombstone_scrub_live_intact(self, tmp_path):
        """Full workflow: corrupt -> tombstone -> scrub -> live objects intact."""
        path = str(tmp_path / "tomb_scrub.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        # doomed.bin first so it lands at DATA_START
        s.put("doomed.bin", b"will be corrupted and scrubbed")
        s.flush_index()
        s.put("live.bin", b"precious data that must survive")
        s.flush_index()
        del s

        # Corrupt doomed.bin block 0 at DATA_START
        flip_byte(path, DATA_START + 4)

        s2 = rawobjstr.open(path)
        tombs = s2.list_tombstones()
        assert len(tombs) >= 1

        # Scrub free space
        scrub = s2.scrub_free_space()
        assert scrub.bytes_scrubbed >= 0

        # Live data must still be intact
        assert s2.get("live.bin") == b"precious data that must survive"

        # Clear tombstones
        cleared = s2.clear_tombstones()
        assert cleared >= 1
        assert s2.list_tombstones() == []


# ===================================================================
# verify_all() / repair() on corrupted stores
# ===================================================================

class TestVerifyRepair:

    def test_verify_detects_corruption(self, tmp_path):
        path = str(tmp_path / "verify_corrupt.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        # Write one object so it lands at DATA_START
        s.put("bad.bin", b"will be corrupted")
        s.flush_index()
        s.put("good.bin", b"good data here")
        s.flush_index()
        del s

        # Corrupt block 0 of bad.bin (at DATA_START)
        flip_byte(path, DATA_START + 4)

        s2 = rawobjstr.open(path, mode="skip_verify")
        report = s2.verify_all()
        assert report.files_checked >= 2
        assert report.error_count >= 1, "should detect at least 1 corrupt extent"

    def test_verify_all_report_fields(self, tmp_path):
        path = str(tmp_path / "verify_fields.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("a.bin", b"aaa")
        s.put("b.bin", b"bbb")
        s.flush_index()

        report = s.verify_all()
        assert report.files_checked == 2
        assert report.files_ok == 2
        assert report.error_count == 0
        assert report.free_list_consistent is True
        assert report.space_accounted is True
        assert report.total_data_region > 0
        assert report.total_used > 0
        assert report.total_free > 0

    def test_repair_after_corruption(self, tmp_path):
        path = str(tmp_path / "repair_corrupt.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("f1.bin", b"file one data")
        s.put("f2.bin", b"file two data")
        s.flush_index()
        del s

        s2 = rawobjstr.open(path, mode="skip_verify")
        report = s2.repair()
        assert report.free_list_rebuilt is True
        assert report.files_found >= 2
        assert report.flushed is True

    def test_repair_frees_orphaned_space(self, tmp_path):
        """After deleting files and repairing, free space should be consistent."""
        path = str(tmp_path / "repair_free.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("keep.bin", b"keep me")
        for i in range(10):
            s.put(f"temp/{i}.bin", make_payload(10_000))
        s.flush_index()

        # Delete temp files
        for i in range(10):
            s.delete(f"temp/{i}.bin")
        s.flush_index()

        report = s.repair()
        assert report.free_list_rebuilt
        assert report.files_found == 1  # only keep.bin

        # Verify should now be clean
        vr = s.verify_all()
        assert vr.error_count == 0
        assert vr.free_list_consistent
        assert vr.space_accounted


# ===================================================================
# Basic maintenance operations
# ===================================================================

class TestMaintenance:

    def test_flush_index(self, store):
        store.put("f.txt", b"data")
        store.flush_index()
        info = store.device_info()
        assert info.file_count == 1

    def test_verify_all(self, store):
        store.put("v.txt", b"data")
        report = store.verify_all()
        assert report.files_checked >= 1
        assert report.error_count == 0

    def test_device_info(self, store):
        info = store.device_info()
        assert info.device_size > 0
        assert info.free_space > 0

    def test_scrub_free_space(self, store):
        report = store.scrub_free_space()
        assert report.bytes_scrubbed >= 0

    def test_repair(self, store):
        report = store.repair()
        assert report.free_list_rebuilt

    def test_tombstones_empty(self, store):
        assert store.list_tombstones() == []
        assert store.clear_tombstones() == 0


# ===================================================================
# Repr strings for result types
# ===================================================================

class TestReprStrings:

    def test_verify_report_repr(self, tmp_path):
        path = str(tmp_path / "verify_repr.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("x.bin", b"data")
        report = s.verify_all()
        r = repr(report)
        assert "VerifyReport" in r
        assert "checked=" in r

    def test_repair_report_repr(self, tmp_path):
        path = str(tmp_path / "repair_repr.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        report = s.repair()
        r = repr(report)
        assert "RepairReport" in r
        assert "rebuilt=" in r

    def test_scrub_report_repr(self, tmp_path):
        path = str(tmp_path / "scrub_repr.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        report = s.scrub_free_space()
        r = repr(report)
        assert "ScrubReport" in r
        assert "regions=" in r

    def test_object_full_info_repr(self, tmp_path):
        path = str(tmp_path / "fullinfo_repr.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("f.bin", b"data")
        results = s.list_full()
        assert len(results) == 1
        r = repr(results[0])
        assert "f.bin" in r

    def test_device_info_repr(self, tmp_path):
        path = str(tmp_path / "devinfo_repr.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        info = s.device_info()
        r = repr(info)
        assert "DeviceInfo" in r

    def test_object_meta_repr(self, tmp_path):
        path = str(tmp_path / "meta_repr.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("f.bin", b"data")
        meta = s.head("f.bin")
        r = repr(meta)
        assert "f.bin" in r


# ===================================================================
# modify_flags, layout_map, needs_flush
# ===================================================================

class TestUtilityMethods:

    def test_modify_flags_set_and_clear(self, tmp_path):
        path = str(tmp_path / "flags.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("test", b"data")
        s.flush_index()
        del s

        # Set write-protect flag
        flags = rawobjstr.modify_flags(path, set_flags=rawobjstr.FLAG_WRITE_PROTECT)
        assert flags & rawobjstr.FLAG_WRITE_PROTECT != 0

        # Clear it
        flags = rawobjstr.modify_flags(
            path, clear_flags=rawobjstr.FLAG_WRITE_PROTECT
        )
        assert flags & rawobjstr.FLAG_WRITE_PROTECT == 0

    def test_layout_map_structure(self, tmp_path):
        path = str(tmp_path / "layout.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.put("a.txt", b"aaa")
        s.put("b.txt", b"bbbbb")
        s.flush_index()

        layout = s.layout_map()
        assert "device_size" in layout
        assert "extents" in layout
        assert "free_regions" in layout
        assert layout["device_size"] == SMALL_DEVICE
        assert len(layout["extents"]) == 2

    def test_needs_flush(self, tmp_path):
        path = str(tmp_path / "flush.raw")
        s = rawobjstr.format(path, size=SMALL_DEVICE)
        s.flush_index()
        assert not s.needs_flush()
        s.put("test.txt", b"data")
        assert s.needs_flush()
        s.flush_index()
        assert not s.needs_flush()
