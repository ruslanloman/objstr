"""fsspec integration tests for shardedobjstr.

These tests demonstrate how to use the ``shardedobjst://`` fsspec
filesystem with a multi-shard cluster backed by raw block devices.
Each test is self-contained and well-commented so users can copy
patterns directly into their own code.

The sharded store distributes objects across multiple raw image files
(or block devices) with configurable replication.  The fsspec layer
makes this transparent to pandas, PyArrow, Polars, Dask, and any
other fsspec-aware library.

Requirements:
    pip install fsspec   (fsspec is an optional dependency of shardedobjstr)

Run:
    pytest tests/test_fsspec/ -v
"""

import struct

import pytest

import shardedobjstr

# Skip the entire module if fsspec is not installed.
fsspec = pytest.importorskip("fsspec")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHARD_SIZE = 64 * 1024 * 1024  # 64 MB per shard


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def shard_paths(tmp_path):
    """Format two 64 MB shard images and return their paths.

    In production these would be raw block devices (``/dev/nvme0n1``,
    ``/dev/nvme1n1``) but image files work identically for testing.
    """
    paths = []
    for i in range(2):
        p = str(tmp_path / f"shard{i}.raw")
        shardedobjstr.format_shard(p, size=SHARD_SIZE)
        paths.append(p)
    return paths


@pytest.fixture
def three_shard_paths(tmp_path):
    """Format three 64 MB shard images for replication tests."""
    paths = []
    for i in range(3):
        p = str(tmp_path / f"shard{i}.raw")
        shardedobjstr.format_shard(p, size=SHARD_SIZE)
        paths.append(p)
    return paths


@pytest.fixture
def fs(shard_paths):
    """Return a ready-to-use ``ShardedFileSystem`` backed by 2 shards.

    This is the most common setup -- 2 shards, replication factor 1 (no
    replication), suitable for capacity doubling::

        import fsspec
        fs = fsspec.filesystem("shardedobjst",
                               shard_paths=["/dev/nvme0n1", "/dev/nvme1n1"])
    """
    from shardedobjstr.fsspec_impl import ShardedFileSystem
    return ShardedFileSystem(shard_paths=shard_paths, replication_factor=1)


@pytest.fixture
def fs_replicated(three_shard_paths):
    """Return a filesystem with 3 shards and replication factor 2.

    Every object is stored on 2 of the 3 shards for redundancy::

        fs = fsspec.filesystem("shardedobjst",
                               shard_paths=["/dev/nvme0n1", "/dev/nvme1n1", "/dev/nvme2n1"],
                               replication_factor=2)
    """
    from shardedobjstr.fsspec_impl import ShardedFileSystem
    return ShardedFileSystem(
        shard_paths=three_shard_paths, replication_factor=2,
    )


# =========================================================================
# Basic read/write -- the most common use case
# =========================================================================

class TestBasicReadWrite:
    """Write and read objects using pipe/cat (the fast bulk I/O path)."""

    def test_pipe_and_cat(self, fs):
        """Write bytes with pipe(), read back with cat().

        Simplest possible round-trip::

            fs.pipe("key", b"value")
            assert fs.cat("key") == b"value"
        """
        fs.pipe("hello.txt", b"Hello, sharded fsspec!")
        result = fs.cat("hello.txt")
        assert result == b"Hello, sharded fsspec!"

    def test_pipe_and_cat_binary_data(self, fs):
        """Binary data (e.g. ML model weights) round-trips correctly."""
        data = struct.pack("<1024I", *range(1024))
        fs.pipe("weights.bin", data)
        assert fs.cat("weights.bin") == data

    def test_pipe_overwrites_existing(self, fs):
        """Writing to an existing key overwrites it (upsert semantics)."""
        fs.pipe("config.json", b'{"v": 1}')
        fs.pipe("config.json", b'{"v": 2}')
        assert fs.cat("config.json") == b'{"v": 2}'

    def test_cat_missing_key_raises(self, fs):
        """Reading a non-existent key raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            fs.cat("nonexistent.txt")


# =========================================================================
# Replicated reads -- data survives shard loss
# =========================================================================

class TestReplicatedAccess:
    """Demonstrate that replication makes data available from multiple shards.

    With replication_factor=2 on 3 shards, each object lives on 2 shards.
    Data remains accessible even if a shard goes offline.
    """

    def test_replicated_write_and_read(self, fs_replicated):
        """Write to a replicated cluster and verify reads.

        Objects are automatically placed on 2 of the 3 shards::

            fs = fsspec.filesystem("shardedobjst",
                                   shard_paths=paths,
                                   replication_factor=2)
            fs.pipe("important.dat", data)
            # Data is now on 2 shards -- survives 1 shard failure
        """
        fs_replicated.pipe("replicated.bin", b"safe data")
        assert fs_replicated.cat("replicated.bin") == b"safe data"

        # Verify the underlying store reports 2 replicas
        info = fs_replicated.store.placement("replicated.bin")
        assert info is not None
        assert len(info.shards) == 2


# =========================================================================
# File-like interface (open / read / write / seek)
# =========================================================================

class TestFileInterface:
    """Use fs.open() for file-like access.

    This is how pandas and PyArrow interact with fsspec internally::

        with fs.open("data.parquet", "rb") as f:
            table = pyarrow.parquet.read_table(f)
    """

    def test_open_read(self, fs):
        """Read an object through a file handle in chunks."""
        payload = b"X" * 8192
        fs.pipe("readable.bin", payload)

        with fs.open("readable.bin", "rb") as f:
            chunk1 = f.read(4096)
            chunk2 = f.read(4096)
            rest = f.read()

        assert chunk1 == b"X" * 4096
        assert chunk2 == b"X" * 4096
        assert rest == b""

    def test_open_write(self, fs):
        """Write an object through a file handle.

        Data is buffered in memory and committed to the store on close::

            with fs.open("output.csv", "wb") as f:
                f.write(header_bytes)
                f.write(row_bytes)
            # Object is now stored across shards
        """
        with fs.open("written.txt", "wb") as f:
            f.write(b"line one\n")
            f.write(b"line two\n")

        assert fs.cat("written.txt") == b"line one\nline two\n"

    def test_open_read_range(self, fs):
        """Read a byte range without loading the full object.

        Useful for reading Parquet footers, Arrow metadata, or any
        format that supports random access::

            footer = fs.cat_file("data.parquet", start=-8)  # last 8 bytes
        """
        data = bytes(range(256)) * 40  # 10 KB
        fs.pipe("ranged.bin", data)

        chunk = fs.cat_file("ranged.bin", start=256, end=512)
        assert chunk == data[256:512]
        assert len(chunk) == 256


# =========================================================================
# Listing and metadata
# =========================================================================

class TestListingAndMetadata:
    """List objects and inspect metadata across shards."""

    def test_ls_flat(self, fs):
        """List all objects at the top level.

        Example::

            names = fs.ls("/", detail=False)
        """
        fs.pipe("a.txt", b"1")
        fs.pipe("b.txt", b"2")
        fs.pipe("c.txt", b"3")

        names = fs.ls("/", detail=False)
        assert "a.txt" in names
        assert "b.txt" in names
        assert "c.txt" in names

    def test_ls_with_prefix(self, fs):
        """List objects under a virtual directory.

        The store treats ``/`` in keys as directory separators::

            fs.pipe("dataset/train/001.bin", data)
            fs.pipe("dataset/test/001.bin", data)
            fs.ls("dataset/train")
            # -> ["dataset/train/001.bin"]
        """
        fs.pipe("dataset/train/001.bin", b"t1")
        fs.pipe("dataset/train/002.bin", b"t2")
        fs.pipe("dataset/test/001.bin", b"v1")

        entries = fs.ls("dataset/train", detail=False)
        assert "dataset/train/001.bin" in entries
        assert "dataset/train/002.bin" in entries
        assert "dataset/test/001.bin" not in entries

    def test_ls_detail(self, fs):
        """Detailed listing includes size, type, and modification time."""
        fs.pipe("info/report.csv", b"col1,col2\n1,2\n")

        entries = fs.ls("info", detail=True)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["name"] == "info/report.csv"
        assert entry["size"] == len(b"col1,col2\n1,2\n")
        assert entry["type"] == "file"

    def test_info(self, fs):
        """Get metadata for a single object.

        Example::

            info = fs.info("model.bin")
            print(f"Size: {info['size']} bytes")
        """
        fs.pipe("meta.bin", b"x" * 1024)

        info = fs.info("meta.bin")
        assert info["name"] == "meta.bin"
        assert info["size"] == 1024
        assert info["type"] == "file"

    def test_exists(self, fs):
        """Check whether a key exists without reading data."""
        assert not fs.exists("nope.txt")
        fs.pipe("yep.txt", b"yes")
        assert fs.exists("yep.txt")


# =========================================================================
# Copy, rename, delete
# =========================================================================

class TestMutations:
    """Copy, move (rename), and delete operations across shards."""

    def test_copy(self, fs):
        """Copy an object to a new key (may land on different shards).

        Example::

            fs.copy("model_v1.bin", "model_v1_backup.bin")
        """
        fs.pipe("src.txt", b"copy me")
        fs.copy("src.txt", "dst.txt")

        assert fs.cat("src.txt") == b"copy me"
        assert fs.cat("dst.txt") == b"copy me"

    def test_mv(self, fs):
        """Rename (move) an object.

        Example::

            fs.mv("draft.txt", "final.txt")
        """
        fs.pipe("old.txt", b"rename me")
        fs.mv("old.txt", "new.txt")

        assert fs.cat("new.txt") == b"rename me"
        assert not fs.exists("old.txt")

    def test_rm(self, fs):
        """Delete an object from all replica shards.

        Example::

            fs.rm("obsolete.log")
        """
        fs.pipe("gone.txt", b"bye")
        assert fs.exists("gone.txt")

        fs.rm("gone.txt")
        assert not fs.exists("gone.txt")


# =========================================================================
# Pre-opened store passthrough
# =========================================================================

class TestStorePassthrough:
    """Pass a pre-opened ClusterStore to the filesystem.

    Useful when you have already configured a cluster and want to add
    fsspec on top without re-opening all the shards::

        import shardedobjstr
        store = shardedobjstr.open_cluster(paths, replication_factor=2)

        # ... do some cluster management ...

        from shardedobjstr.fsspec import ShardedFileSystem
        fs = ShardedFileSystem(store=store)
        # Now pandas/PyArrow can use this filesystem
    """

    def test_pre_opened_store(self, shard_paths):
        """Data written via the store is visible through fsspec and vice versa."""
        from shardedobjstr.fsspec_impl import ShardedFileSystem

        store = shardedobjstr.open_cluster(
            shard_paths, replication_factor=1,
        )
        fs = ShardedFileSystem(store=store)

        # Write via native API, read via fsspec
        store.put("native.txt", b"from store")
        assert fs.cat("native.txt") == b"from store"

        # Write via fsspec, read via native API
        fs.pipe("fsspec.txt", b"from fsspec")
        assert store.get("fsspec.txt") == b"from fsspec"

    def test_store_property(self, fs):
        """The underlying ClusterStore is accessible via the ``store`` property."""
        assert isinstance(fs.store, shardedobjstr.ClusterStore)


# =========================================================================
# Scheme resolution (entry point registration)
# =========================================================================

class TestSchemeResolution:
    """Verify that ``fsspec.filesystem("shardedobjst", ...)`` works."""

    def test_filesystem_factory(self, shard_paths):
        """Create a filesystem through fsspec's scheme registry.

        This is how end users will typically use it::

            import fsspec
            fs = fsspec.filesystem("shardedobjst",
                                   shard_paths=["/dev/nvme0n1", "/dev/nvme1n1"],
                                   replication_factor=2)
            df = pd.read_parquet("shardedobjst:///data.parquet",
                                 storage_options={...})
        """
        fs = fsspec.filesystem(
            "shardedobjst",
            shard_paths=shard_paths,
            replication_factor=1,
        )
        fs.pipe("via_scheme.txt", b"scheme works")
        assert fs.cat("via_scheme.txt") == b"scheme works"


# =========================================================================
# Protocol stripping
# =========================================================================

class TestProtocolStripping:
    """Verify that shardedobjst:// prefixes are stripped from paths."""

    def test_strip_protocol_in_cat(self, fs):
        """Paths like ``shardedobjst:///key`` are normalized to ``key``."""
        fs.pipe("stripped.txt", b"data")
        assert fs.cat("shardedobjst:///stripped.txt") == b"data"

    def test_strip_protocol_in_pipe(self, fs):
        """Writing with a protocol prefix works transparently."""
        fs.pipe("shardedobjst:///prefixed.txt", b"ok")
        assert fs.cat("prefixed.txt") == b"ok"


# =========================================================================
# Error handling
# =========================================================================

class TestErrors:
    """Verify correct exceptions for error cases."""

    def test_constructor_requires_paths_or_store(self):
        """Constructing without shard_paths or store raises ValueError."""
        from shardedobjstr.fsspec_impl import ShardedFileSystem
        with pytest.raises(ValueError, match="Either 'shard_paths'"):
            ShardedFileSystem()

    def test_rm_nonexistent_raises(self, fs):
        """Deleting a missing key raises FileNotFoundError.

        Note: ``rm_file()`` is the single-file delete path.  The
        higher-level ``rm()`` may swallow not-found errors during
        path expansion.
        """
        with pytest.raises(FileNotFoundError):
            fs.rm_file("ghost.txt")
