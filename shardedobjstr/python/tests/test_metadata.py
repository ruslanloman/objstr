"""Tests for metadata-aware Python API:
put_with_meta, put_with_meta_from_file, head_with_meta, get_metadata,
list_with_meta, set_meta_len."""

import os
import pytest
import shardedobjstr

from conftest import SHARD_SIZE


@pytest.fixture
def two_shard_paths(tmp_path):
    return [str(tmp_path / f"shard{i}.raw") for i in range(2)]


@pytest.fixture
def cluster(two_shard_paths):
    shards = [(p, SHARD_SIZE) for p in two_shard_paths]
    store = shardedobjstr.format_and_open_cluster(shards, replication_factor=2)
    yield store
    store.flush_all()


class TestPutWithMeta:
    def test_put_and_get_metadata_roundtrip(self, cluster):
        body = b"hello world"
        meta = b"\x01\x00\x05typeAtext/plain"  # arbitrary metadata bytes
        cluster.put_with_meta("doc.txt", body, meta)
        cluster.flush_all()

        got_meta = cluster.get_metadata("doc.txt")
        assert got_meta == meta

    def test_put_with_empty_metadata(self, cluster):
        body = b"no metadata"
        cluster.put_with_meta("bare.bin", body, b"")
        cluster.flush_all()

        got_meta = cluster.get_metadata("bare.bin")
        assert got_meta == b""

    def test_put_with_meta_body_readable(self, cluster):
        body = b"the actual content"
        meta = b"\xDE\xAD"
        cluster.put_with_meta("readable.bin", body, meta)
        cluster.flush_all()

        data = cluster.get("readable.bin")
        assert data == body


class TestHeadWithMeta:
    def test_head_with_meta_returns_meta_len(self, cluster):
        body = b"some body"
        meta = b"0123456789"  # 10 bytes
        cluster.put_with_meta("sized.bin", body, meta)
        cluster.flush_all()

        obj_meta, meta_len = cluster.head_with_meta("sized.bin")
        assert meta_len == len(meta)
        assert obj_meta.size == len(body)
        assert obj_meta.location == "sized.bin"

    def test_head_with_meta_no_metadata(self, cluster):
        cluster.put("plain.bin", b"just data")
        cluster.flush_all()

        obj_meta, meta_len = cluster.head_with_meta("plain.bin")
        assert meta_len == 0
        assert obj_meta.size == len(b"just data")

    def test_head_with_meta_not_found(self, cluster):
        with pytest.raises(FileNotFoundError):
            cluster.head_with_meta("nonexistent")


class TestGetMetadata:
    def test_get_metadata_not_found(self, cluster):
        with pytest.raises(FileNotFoundError):
            cluster.get_metadata("ghost")

    def test_get_metadata_object_without_meta(self, cluster):
        cluster.put("nometa.bin", b"data only")
        cluster.flush_all()

        got = cluster.get_metadata("nometa.bin")
        assert got == b""


class TestListWithMeta:
    def test_list_with_meta_returns_tuples(self, cluster):
        cluster.put_with_meta("a/one.bin", b"aaa", b"\x01\x02")
        cluster.put_with_meta("a/two.bin", b"bbb", b"\x03\x04\x05")
        cluster.put("a/three.bin", b"ccc")
        cluster.flush_all()

        items = cluster.list_with_meta("a/")
        assert len(items) == 3
        by_name = {m.location: (m, ml) for m, ml in items}

        assert by_name["a/one.bin"][1] == 2
        assert by_name["a/two.bin"][1] == 3
        assert by_name["a/three.bin"][1] == 0

    def test_list_with_meta_empty(self, cluster):
        items = cluster.list_with_meta("empty/")
        assert items == []


class TestPutWithMetaFromFile:
    """Tests for put_with_meta_from_file -- streaming file upload with metadata."""

    def test_roundtrip_body_and_meta(self, cluster, tmp_path):
        body = b"file body content"
        meta = b"\x01\x02\x03\x04"
        file_path = str(tmp_path / "upload.bin")
        with open(file_path, "wb") as f:
            f.write(body)
            f.write(meta)

        cluster.put_with_meta_from_file("from-file.bin", file_path, len(meta))
        cluster.flush_all()

        got_data = cluster.get("from-file.bin")
        assert got_data == body
        got_meta = cluster.get_metadata("from-file.bin")
        assert got_meta == meta

    def test_zero_meta_len(self, cluster, tmp_path):
        body = b"just payload"
        file_path = str(tmp_path / "no_meta.bin")
        with open(file_path, "wb") as f:
            f.write(body)

        cluster.put_with_meta_from_file("no-meta-file.bin", file_path, 0)
        cluster.flush_all()

        assert cluster.get("no-meta-file.bin") == body
        _, meta_len = cluster.head_with_meta("no-meta-file.bin")
        assert meta_len == 0

    def test_nonexistent_file_raises(self, cluster):
        with pytest.raises(OSError):
            cluster.put_with_meta_from_file("k", "/no/such/file.bin", 0)

    def test_list_with_meta_no_prefix(self, cluster):
        cluster.put_with_meta("root.bin", b"x", b"\xFF")
        cluster.flush_all()

        items = cluster.list_with_meta()
        assert any(m.location == "root.bin" for m, _ in items)


@pytest.fixture
def cluster_rf1(two_shard_paths):
    """Single-shard cluster for tests that need deterministic shard reads."""
    shards = [(two_shard_paths[0], SHARD_SIZE)]
    store = shardedobjstr.format_and_open_cluster(shards, replication_factor=1)
    yield store
    store.flush_all()


class TestSetMetaLen:
    def test_set_meta_len_updates(self, cluster_rf1):
        body = b"content"
        meta = b"metadata_bytes_here"
        cluster_rf1.put_with_meta("adjust.bin", body, meta)
        cluster_rf1.flush_all()

        _, original_len = cluster_rf1.head_with_meta("adjust.bin")
        assert original_len == len(meta)

        cluster_rf1.set_meta_len("adjust.bin", 5)
        cluster_rf1.flush_all()

        _, new_len = cluster_rf1.head_with_meta("adjust.bin")
        assert new_len == 5

    def test_set_meta_len_not_found(self, cluster):
        with pytest.raises(FileNotFoundError):
            cluster.set_meta_len("ghost", 10)
