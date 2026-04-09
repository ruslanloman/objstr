"""Tests for import_from and export_to Python bindings."""

import rawobjstr

from conftest import SMALL_DEVICE, make_small, verify_small


class TestImportFrom:

    def test_import_from_empty_source(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)
        report = dst.import_from(src)
        assert report.files_imported == 0
        assert report.bytes_imported == 0
        assert report.errors == []

    def test_import_from_basic(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)

        src.put("a.bin", make_small(1, 4096))
        src.put("b.bin", make_small(2, 8192))
        src.flush_index()

        report = dst.import_from(src)
        assert report.files_imported == 2
        assert report.bytes_imported == 4096 + 8192
        assert report.errors == []

        verify_small(dst.get("a.bin"), 1, 4096)
        verify_small(dst.get("b.bin"), 2, 8192)

    def test_import_from_with_prefix(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)

        src.put("tables/a.bin", make_small(1, 1024))
        src.put("tables/b.bin", make_small(2, 1024))
        src.put("other/c.bin", make_small(3, 1024))
        src.flush_index()

        report = dst.import_from(src, prefix="tables/")
        assert report.files_imported == 2
        assert report.errors == []

        items = dst.list()
        keys = sorted(m.location for m in items)
        assert keys == ["tables/a.bin", "tables/b.bin"]

    def test_import_report_repr(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)
        report = dst.import_from(src)
        r = repr(report)
        assert "ImportReport" in r
        assert "imported=0" in r


class TestExportTo:

    def test_export_to_empty_store(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)
        report = src.export_to(dst)
        assert report.files_exported == 0
        assert report.bytes_exported == 0
        assert report.errors == []

    def test_export_to_basic(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)

        src.put("x.bin", make_small(10, 2048))
        src.put("y.bin", make_small(20, 4096))
        src.flush_index()

        report = src.export_to(dst)
        assert report.files_exported == 2
        assert report.bytes_exported == 2048 + 4096
        assert report.errors == []

        verify_small(dst.get("x.bin"), 10, 2048)
        verify_small(dst.get("y.bin"), 20, 4096)

    def test_export_report_repr(self, tmp_path):
        src = rawobjstr.format(str(tmp_path / "src.raw"), size=SMALL_DEVICE)
        dst = rawobjstr.format(str(tmp_path / "dst.raw"), size=SMALL_DEVICE)
        report = src.export_to(dst)
        r = repr(report)
        assert "ExportReport" in r
        assert "exported=0" in r


class TestImportExportRoundtrip:

    def test_roundtrip_preserves_data(self, tmp_path):
        """Export from A to B, import from B to C, verify C matches A."""
        a = rawobjstr.format(str(tmp_path / "a.raw"), size=SMALL_DEVICE)
        b = rawobjstr.format(str(tmp_path / "b.raw"), size=SMALL_DEVICE)
        c = rawobjstr.format(str(tmp_path / "c.raw"), size=SMALL_DEVICE)

        for i in range(10):
            a.put(f"file_{i:03d}.bin", make_small(i, 1024 + i * 100))
        a.flush_index()

        a.export_to(b)
        b.flush_index()

        c.import_from(b)

        a_items = sorted(m.location for m in a.list())
        c_items = sorted(m.location for m in c.list())
        assert a_items == c_items

        for i in range(10):
            key = f"file_{i:03d}.bin"
            assert a.get(key) == c.get(key)
