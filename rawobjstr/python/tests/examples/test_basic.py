"""Basic rawobjstr + fsspec example -- no extra dependencies needed.

This file shows the simplest possible use of rawobjstr through fsspec:
format a 100 MB store, write objects, read them back.

Run (requires fsspec):
    pip install fsspec
    pytest tests/examples/test_basic.py -v

This file is NOT collected by the default test run.  To include it:
    pytest tests/examples/ -v -m examples
"""

import pytest

import rawobjstr

fsspec = pytest.importorskip("fsspec")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STORE_SIZE = 100 * 1024 * 1024  # 100 MB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store_path(tmp_path):
    """Format a fresh 100 MB store image and return its path."""
    path = str(tmp_path / "basic.raw")
    store = rawobjstr.format(path, size=STORE_SIZE)
    store.flush_index()
    del store
    return path


@pytest.fixture
def fs(store_path):
    """Open the store through fsspec."""
    return fsspec.filesystem("rawobjstr", path=store_path)


# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------

@pytest.mark.examples
class TestBasicPipeCat:
    """Write and read raw bytes -- the fastest path for bulk data."""

    def test_write_and_read_text(self, fs):
        # Write a UTF-8 string as bytes
        fs.pipe("greeting.txt", b"Hello from rawobjstr!")

        # Read it back
        data = fs.cat("greeting.txt")
        assert data == b"Hello from rawobjstr!"

    def test_write_and_read_binary(self, fs):
        # Write 10 KB of binary data
        payload = bytes(range(256)) * 40
        fs.pipe("binary.bin", payload)

        assert fs.cat("binary.bin") == payload

    def test_overwrite(self, fs):
        # Writing to the same key replaces the previous value
        fs.pipe("config.json", b'{"version": 1}')
        fs.pipe("config.json", b'{"version": 2}')

        assert fs.cat("config.json") == b'{"version": 2}'


@pytest.mark.examples
class TestBasicFileInterface:
    """Use open() for streaming reads and writes."""

    def test_open_write_then_read(self, fs):
        # Write through a file handle (data committed on close)
        with fs.open("stream.bin", "wb") as f:
            f.write(b"chunk-1 ")
            f.write(b"chunk-2")

        # Read through a file handle
        with fs.open("stream.bin", "rb") as f:
            header = f.read(8)
            rest = f.read()

        assert header == b"chunk-1 "
        assert rest == b"chunk-2"


@pytest.mark.examples
class TestBasicListing:
    """List objects and check existence."""

    def test_ls_and_exists(self, fs):
        fs.pipe("data/train.csv", b"a,b\n1,2\n")
        fs.pipe("data/test.csv", b"a,b\n3,4\n")
        fs.pipe("models/v1.bin", b"\x00" * 100)

        # List everything under data/
        names = fs.ls("data", detail=False)
        assert "data/train.csv" in names
        assert "data/test.csv" in names
        assert "models/v1.bin" not in names

        # Check existence
        assert fs.exists("data/train.csv")
        assert not fs.exists("data/missing.csv")

    def test_info(self, fs):
        fs.pipe("hello.txt", b"world")
        info = fs.info("hello.txt")
        assert info["name"] == "hello.txt"
        assert info["size"] == 5
        assert info["type"] == "file"


@pytest.mark.examples
class TestBasicMutations:
    """Copy, rename, and delete objects."""

    def test_copy_and_delete(self, fs):
        fs.pipe("original.txt", b"keep me")

        # Copy
        fs.copy("original.txt", "backup.txt")
        assert fs.cat("backup.txt") == b"keep me"

        # Delete the original
        fs.rm("original.txt")
        assert not fs.exists("original.txt")
        assert fs.exists("backup.txt")

    def test_rename(self, fs):
        fs.pipe("old_name.txt", b"rename me")
        fs.mv("old_name.txt", "new_name.txt")

        assert not fs.exists("old_name.txt")
        assert fs.cat("new_name.txt") == b"rename me"
