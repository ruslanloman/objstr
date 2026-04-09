"""Tests for metadata-aware Python bindings:
put_with_meta, put_with_meta_from_file, head_with_meta,
list_with_meta, set_meta_len.
"""

import os
import tempfile

import pytest
import rawobjstr


# -- put_with_meta + head_with_meta round-trip --------------------------------

def test_put_with_meta_roundtrip(store):
    body = b"hello world"
    meta = b'{"version": 1}'
    store.put_with_meta("test/obj1", body, meta)

    # head_with_meta returns (ObjectMeta, meta_len)
    obj_meta, meta_len = store.head_with_meta("test/obj1")
    assert obj_meta.location == "test/obj1"
    assert obj_meta.size == len(body)  # size is body-only (excludes metadata)
    assert meta_len == len(meta)

    # get_metadata retrieves just the metadata suffix
    got_meta = store.get_metadata("test/obj1")
    assert got_meta == meta


def test_put_with_meta_empty_metadata(store):
    body = b"no-meta-body"
    store.put_with_meta("test/no_meta", body, b"")

    obj_meta, meta_len = store.head_with_meta("test/no_meta")
    assert meta_len == 0
    assert obj_meta.size == len(body)


def test_put_with_meta_overwrites(store):
    store.put_with_meta("test/overwrite", b"body1", b"meta1")
    store.put_with_meta("test/overwrite", b"body2", b"meta222")

    obj_meta, meta_len = store.head_with_meta("test/overwrite")
    assert meta_len == len(b"meta222")
    got = store.get_metadata("test/overwrite")
    assert got == b"meta222"


# -- put_with_meta_from_file --------------------------------------------------

def test_put_with_meta_from_file_roundtrip(store):
    body = b"file-body-content"
    meta = b"file-meta"
    payload = body + meta

    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(payload)
        tmp_path = f.name

    try:
        store.put_with_meta_from_file("test/from_file", tmp_path, meta_len=len(meta))

        obj_meta, ml = store.head_with_meta("test/from_file")
        assert ml == len(meta)
        assert obj_meta.size == len(body)  # size is body-only (excludes metadata)

        got_meta = store.get_metadata("test/from_file")
        assert got_meta == meta
    finally:
        os.unlink(tmp_path)


def test_put_with_meta_from_file_no_metadata(store):
    body = b"pure-body"
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(body)
        tmp_path = f.name

    try:
        store.put_with_meta_from_file("test/from_file_no_meta", tmp_path, meta_len=0)

        obj_meta, ml = store.head_with_meta("test/from_file_no_meta")
        assert ml == 0
        assert obj_meta.size == len(body)
    finally:
        os.unlink(tmp_path)


def test_put_with_meta_from_file_bad_path(store):
    with pytest.raises(IOError):
        store.put_with_meta_from_file("test/bad", "/nonexistent/path.bin", meta_len=0)


# -- head_with_meta -----------------------------------------------------------

def test_head_with_meta_not_found(store):
    with pytest.raises(FileNotFoundError):
        store.head_with_meta("does/not/exist")


def test_head_with_meta_no_metadata(store):
    store.put("test/plain", b"plain-body")
    obj_meta, ml = store.head_with_meta("test/plain")
    assert ml == 0
    assert obj_meta.location == "test/plain"
    assert obj_meta.size == len(b"plain-body")


# -- list_with_meta -----------------------------------------------------------

def test_list_with_meta_empty(store):
    results = store.list_with_meta()
    assert results == []


def test_list_with_meta_mixed(store):
    store.put("a/plain.txt", b"no-meta")
    store.put_with_meta("a/tagged.txt", b"body", b"meta")
    store.put_with_meta("b/other.txt", b"body2", b"m2")

    # All objects
    all_results = store.list_with_meta()
    assert len(all_results) == 3

    # Filter by prefix
    a_results = store.list_with_meta("a/")
    assert len(a_results) == 2
    keys = {m.location for m, _ in a_results}
    assert keys == {"a/plain.txt", "a/tagged.txt"}

    # Check meta_len values
    meta_map = {m.location: ml for m, ml in a_results}
    assert meta_map["a/plain.txt"] == 0
    assert meta_map["a/tagged.txt"] == len(b"meta")


def test_list_with_meta_prefix_no_match(store):
    store.put("x/y.bin", b"data")
    results = store.list_with_meta("z/")
    assert results == []


# -- set_meta_len -------------------------------------------------------------

def test_set_meta_len_basic(store):
    store.put_with_meta("test/sml", b"body", b"metadata")
    _, ml = store.head_with_meta("test/sml")
    assert ml == len(b"metadata")

    store.set_meta_len("test/sml", 0)
    _, ml = store.head_with_meta("test/sml")
    assert ml == 0


def test_set_meta_len_not_found(store):
    with pytest.raises(FileNotFoundError):
        store.set_meta_len("does/not/exist", 5)


def test_set_meta_len_increases(store):
    store.put("test/sml2", b"some-body-data")
    store.set_meta_len("test/sml2", 4)
    _, ml = store.head_with_meta("test/sml2")
    assert ml == 4

    # get_metadata should return the last 4 bytes of body
    got = store.get_metadata("test/sml2")
    assert got == b"data"
