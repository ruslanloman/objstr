"""fsspec integration tests for rawobjstr.

These tests demonstrate how to use the ``rawobjstr://`` fsspec filesystem
with the rawobjstr native object store.  They double as runnable examples
-- each test is self-contained and well-commented so users can copy
patterns directly into their own code.

Requirements:
    pip install fsspec   (fsspec is an optional dependency of rawobjstr)

Run:
    pytest tests/test_fsspec/ -v
"""

import io
import struct

import pytest

import rawobjstr

# Skip the entire module if fsspec is not installed.
fsspec = pytest.importorskip("fsspec")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEVICE_SIZE = 64 * 1024 * 1024  # 64 MB image -- enough for all tests


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store_path(tmp_path):
    """Format a fresh 64 MB store image and return its path as a string."""
    path = str(tmp_path / "test.raw")
    store = rawobjstr.format(path, size=DEVICE_SIZE)
    store.flush_index()
    # Close the store so the fsspec filesystem can open it
    del store
    return path


@pytest.fixture
def fs(store_path):
    """Return a ready-to-use ``RawObjStFileSystem`` backed by a fresh store.

    Example equivalent::

        import fsspec
        fs = fsspec.filesystem("rawobjstr", path="/dev/nvme0n1")
    """
    from rawobjstr.fsspec_impl import RawObjStFileSystem
    return RawObjStFileSystem(path=store_path)


@pytest.fixture
def fs_from_scheme(store_path):
    """Open a filesystem via the fsspec scheme registry.

    This verifies that the ``rawobjstr://`` entry point is correctly
    registered and that ``fsspec.filesystem("rawobjstr", ...)`` works.
    """
    return fsspec.filesystem("rawobjstr", path=store_path)


# =========================================================================
# Basic read/write -- the most common use case
# =========================================================================

class TestBasicReadWrite:
    """Write and read objects using pipe/cat (the fast bulk I/O path)."""

    def test_pipe_and_cat(self, fs):
        """Write bytes with pipe(), read back with cat().

        This is the simplest way to store and retrieve data::

            fs.pipe("key", b"value")
            assert fs.cat("key") == b"value"
        """
        fs.pipe("hello.txt", b"Hello, fsspec!")
        result = fs.cat("hello.txt")
        assert result == b"Hello, fsspec!"

    def test_pipe_and_cat_binary_data(self, fs):
        """Binary data (e.g. compressed blobs, images) round-trips correctly."""
        # 4 KB of structured binary data
        data = struct.pack("<1024I", *range(1024))
        fs.pipe("binary.bin", data)
        assert fs.cat("binary.bin") == data

    def test_pipe_overwrites_existing(self, fs):
        """Writing to an existing key overwrites it (upsert semantics)."""
        fs.pipe("key.txt", b"version 1")
        fs.pipe("key.txt", b"version 2")
        assert fs.cat("key.txt") == b"version 2"

    def test_cat_missing_key_raises(self, fs):
        """Reading a non-existent key raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            fs.cat("does_not_exist.txt")


# =========================================================================
# File-like interface (open / read / write / seek)
# =========================================================================

class TestFileInterface:
    """Use fs.open() to get a file-like object for reading and writing.

    This is how pandas, PyArrow, and other libraries interact with
    fsspec -- they call ``fs.open(path, "rb")`` and read from the
    returned file handle.
    """

    def test_open_read(self, fs):
        """Read an object through a file handle.

        Example::

            fs.pipe("data.bin", some_bytes)
            with fs.open("data.bin", "rb") as f:
                first_chunk = f.read(1024)
                rest = f.read()
        """
        payload = b"A" * 8192
        fs.pipe("readable.bin", payload)

        with fs.open("readable.bin", "rb") as f:
            # Read in chunks -- just like a regular file
            chunk1 = f.read(4096)
            chunk2 = f.read(4096)
            chunk3 = f.read()  # should be empty -- we read everything

        assert chunk1 == b"A" * 4096
        assert chunk2 == b"A" * 4096
        assert chunk3 == b""

    def test_open_write(self, fs):
        """Write an object through a file handle.

        Example::

            with fs.open("output.bin", "wb") as f:
                f.write(b"header")
                f.write(b"body")
            # Object is committed on close
        """
        with fs.open("written.bin", "wb") as f:
            f.write(b"part one ")
            f.write(b"part two")

        # Verify the data was committed to the store
        assert fs.cat("written.bin") == b"part one part two"

    def test_open_read_range(self, fs):
        """Read a byte range through cat_file (start/end offsets).

        This avoids loading the full object into memory -- useful for
        reading Parquet footers, Arrow metadata, etc.::

            # Read bytes 100-199 (100 bytes)
            chunk = fs.cat_file("large.parquet", start=100, end=200)
        """
        data = bytes(range(256)) * 40  # 10 KB
        fs.pipe("ranged.bin", data)

        # Read bytes 100-199
        chunk = fs.cat_file("ranged.bin", start=100, end=200)
        assert chunk == data[100:200]
        assert len(chunk) == 100


# =========================================================================
# Listing and metadata
# =========================================================================

class TestListingAndMetadata:
    """List objects and inspect metadata -- similar to ``ls`` and ``stat``."""

    def test_ls_flat(self, fs):
        """List all objects at the top level.

        Example::

            fs.pipe("a.txt", b"1")
            fs.pipe("b.txt", b"2")
            names = fs.ls("/", detail=False)
            # -> ["a.txt", "b.txt"]
        """
        fs.pipe("alpha.txt", b"a")
        fs.pipe("beta.txt", b"b")

        names = fs.ls("/", detail=False)
        assert "alpha.txt" in names
        assert "beta.txt" in names

    def test_ls_with_prefix(self, fs):
        """List objects under a directory-style prefix.

        The store uses ``/`` as a virtual directory separator::

            fs.pipe("data/train.csv", b"...")
            fs.pipe("data/test.csv", b"...")
            fs.pipe("models/v1.bin", b"...")
            entries = fs.ls("data", detail=False)
            # -> ["data/train.csv", "data/test.csv"]
        """
        fs.pipe("data/train.csv", b"train")
        fs.pipe("data/test.csv", b"test")
        fs.pipe("models/v1.bin", b"model")

        entries = fs.ls("data", detail=False)
        assert "data/train.csv" in entries
        assert "data/test.csv" in entries
        assert "models/v1.bin" not in entries

    def test_ls_detail(self, fs):
        """Detailed listing includes size, type, and modification time.

        Example::

            for entry in fs.ls("data", detail=True):
                print(entry["name"], entry["size"], entry["type"])
        """
        fs.pipe("info/doc.txt", b"hello world")

        entries = fs.ls("info", detail=True)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["name"] == "info/doc.txt"
        assert entry["size"] == 11
        assert entry["type"] == "file"

    def test_info(self, fs):
        """Get metadata for a single object (like ``stat``).

        Example::

            info = fs.info("data.bin")
            print(info["name"], info["size"], info["type"])
        """
        fs.pipe("meta.bin", b"x" * 512)

        info = fs.info("meta.bin")
        assert info["name"] == "meta.bin"
        assert info["size"] == 512
        assert info["type"] == "file"

    def test_exists(self, fs):
        """Check whether a key exists without reading data.

        Example::

            if fs.exists("config.json"):
                config = json.loads(fs.cat("config.json"))
        """
        assert not fs.exists("nope.txt")
        fs.pipe("yep.txt", b"yes")
        assert fs.exists("yep.txt")


# =========================================================================
# Copy, rename, delete
# =========================================================================

class TestMutations:
    """Copy, move (rename), and delete operations."""

    def test_copy(self, fs):
        """Copy an object to a new key.

        Example::

            fs.pipe("original.bin", data)
            fs.copy("original.bin", "backup.bin")
            # Both keys now hold the same data
        """
        fs.pipe("src.txt", b"copy me")
        fs.copy("src.txt", "dst.txt")

        assert fs.cat("src.txt") == b"copy me"
        assert fs.cat("dst.txt") == b"copy me"

    def test_mv(self, fs):
        """Rename (move) an object.

        The old key is removed and the new key holds the data::

            fs.pipe("old_name.txt", b"data")
            fs.mv("old_name.txt", "new_name.txt")
            # old_name.txt no longer exists
        """
        fs.pipe("before.txt", b"move me")
        fs.mv("before.txt", "after.txt")

        assert fs.cat("after.txt") == b"move me"
        assert not fs.exists("before.txt")

    def test_rm(self, fs):
        """Delete an object.

        Example::

            fs.rm("obsolete.log")
        """
        fs.pipe("delete_me.txt", b"goodbye")
        assert fs.exists("delete_me.txt")

        fs.rm("delete_me.txt")
        assert not fs.exists("delete_me.txt")


# =========================================================================
# Pre-opened store passthrough
# =========================================================================

class TestStorePassthrough:
    """Pass a pre-opened rawobjstr.Store to the filesystem.

    This is useful when you already have a store configured and want to
    layer fsspec on top without re-opening the device::

        import rawobjstr
        from rawobjstr.fsspec import RawObjStFileSystem

        store = rawobjstr.open("/dev/nvme0n1")
        fs = RawObjStFileSystem(store=store)
    """

    def test_pre_opened_store(self, store_path):
        """Data written via the store is visible through fsspec and vice versa."""
        from rawobjstr.fsspec_impl import RawObjStFileSystem

        store = rawobjstr.open(store_path)
        fs = RawObjStFileSystem(store=store)

        # Write via native API, read via fsspec
        store.put("native.txt", b"from store")
        assert fs.cat("native.txt") == b"from store"

        # Write via fsspec, read via native API
        fs.pipe("fsspec.txt", b"from fsspec")
        assert store.get("fsspec.txt") == b"from fsspec"

    def test_store_property(self, fs):
        """The underlying store is accessible via the ``store`` property."""
        assert isinstance(fs.store, rawobjstr.Store)


# =========================================================================
# Scheme resolution (entry point registration)
# =========================================================================

class TestSchemeResolution:
    """Verify that ``fsspec.filesystem("rawobjstr", ...)`` works via the
    registered entry point.
    """

    def test_filesystem_factory(self, store_path):
        """Create a filesystem through fsspec's scheme registry.

        This is the canonical way end users will use it::

            import fsspec
            fs = fsspec.filesystem("rawobjstr", path="/dev/nvme0n1")
            df = pd.read_parquet("rawobjstr:///data.parquet",
                                 storage_options={"path": "/dev/nvme0n1"})
        """
        fs = fsspec.filesystem("rawobjstr", path=store_path)
        fs.pipe("via_scheme.txt", b"scheme works")
        assert fs.cat("via_scheme.txt") == b"scheme works"


# =========================================================================
# Protocol stripping
# =========================================================================

class TestProtocolStripping:
    """Verify that rawobjstr:// prefixes are stripped from paths."""

    def test_strip_protocol_in_cat(self, fs):
        """Paths like ``rawobjstr:///key`` are normalized to ``key``."""
        fs.pipe("stripped.txt", b"data")
        # Read with full protocol prefix -- should still work
        assert fs.cat("rawobjstr:///stripped.txt") == b"data"

    def test_strip_protocol_in_pipe(self, fs):
        """Writing with a protocol prefix works transparently."""
        fs.pipe("rawobjstr:///with_prefix.txt", b"prefixed")
        assert fs.cat("with_prefix.txt") == b"prefixed"


# =========================================================================
# Error handling
# =========================================================================

class TestErrors:
    """Verify correct exceptions for error cases."""

    def test_constructor_requires_path_or_store(self):
        """Constructing without path or store raises ValueError."""
        from rawobjstr.fsspec_impl import RawObjStFileSystem
        with pytest.raises(ValueError, match="Either 'path'"):
            RawObjStFileSystem()

    def test_rm_nonexistent_raises(self, fs):
        """Deleting a missing key raises FileNotFoundError.

        Note: ``rm_file()`` is the single-file delete path.  The
        higher-level ``rm()`` may swallow not-found errors during
        path expansion.
        """
        with pytest.raises(FileNotFoundError):
            fs.rm_file("ghost.txt")
