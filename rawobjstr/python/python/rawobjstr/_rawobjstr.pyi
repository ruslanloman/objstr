"""Type stubs for the native _rawobjstr module."""

from __future__ import annotations
from typing import Optional

class ObjectMeta:
    """Metadata for a stored object."""
    location: str
    size: int
    last_modified: str
    e_tag: Optional[str]

class ListResult:
    """Result from list_with_delimiter()."""
    objects: list[ObjectMeta]
    common_prefixes: list[str]

class PyDeviceInfo:
    """Device statistics and metadata."""
    device_path: str
    device_size: int
    format_version: int
    flags: int
    direct_io: bool
    txn_id: int
    file_count: int
    data_bytes_stored: int
    device_bytes_used: int
    free_space: int
    free_fragments: int
    largest_free_extent: int
    last_flush_bytes: int
    max_key_length: int
    index_slot_capacity: int
    compression: str

class PyTombstoneEntry:
    """An object removed during integrity scan."""
    path: str
    size: int
    crc32c: int
    last_modified: str
    reason: str
    tombstone_txn: int

class PyScrubReport:
    """Result from scrub_free_space()."""
    regions_scrubbed: int
    bytes_scrubbed: int

class PyVerifyReport:
    """Result from verify_all()."""
    files_checked: int
    files_ok: int
    error_count: int
    free_list_consistent: bool
    space_accounted: bool
    total_data_region: int
    total_used: int
    total_free: int

class PyRepairReport:
    """Result from repair()."""
    free_list_rebuilt: bool
    old_free_entries: int
    new_free_entries: int
    old_free_space: int
    new_free_space: int
    flushed: bool
    files_found: int

class PyObjectFullInfo:
    """Full extent information for a stored object."""
    key: str
    body_size: int
    meta_len: int
    last_modified: str
    created_txn: int
    offset: int
    padded_size: int

class PyMultipartUpload:
    """Handle for a multipart upload.  Use as a context manager."""
    def put_part(self, data: bytes) -> None: ...
    def complete(self) -> None: ...
    def abort(self) -> None: ...
    def __enter__(self) -> PyMultipartUpload: ...
    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> bool: ...

class Store:
    """A rawobjstr instance backed by a block device or loopback file."""

    # Write operations
    def put(self, key: str, data: bytes, *, mode: str = "overwrite") -> None:
        """Write an object.  mode='overwrite' (default) or 'create' (fail if exists)."""
        ...

    # Read operations
    def get(self, key: str, *, range: tuple[int, int] | None = None) -> bytes:
        """Read an object (or a byte range)."""
        ...

    def getraw(self, key: str) -> dict:
        """Read raw on-disk bytes without decompression.

        Returns a dict with keys:
          - ``data`` (bytes): exact bytes stored on disk
          - ``uncompressed_size`` (int): original size (0 if not compressed)
          - ``compression`` (str): compression algorithm name
        """
        ...

    def head(self, key: str) -> ObjectMeta:
        """Get object metadata without reading the data."""
        ...

    # Delete
    def delete(self, key: str) -> None: ...

    # Listing
    def list(self, prefix: str | None = None) -> list[ObjectMeta]: ...
    def list_with_delimiter(self, prefix: str | None = None) -> ListResult: ...

    # Copy & Rename
    def copy(self, src: str, dst: str) -> None: ...
    def copy_if_not_exists(self, src: str, dst: str) -> None: ...
    def rename(self, src: str, dst: str) -> None: ...
    def rename_if_not_exists(self, src: str, dst: str) -> None: ...

    # Multipart Upload
    def multipart(self, key: str) -> PyMultipartUpload: ...

    # Maintenance
    def flush_index(self) -> None:
        """Persist in-memory index to disk (crash-safe, double-buffered)."""
        ...
    def is_read_only(self) -> bool: ...
    def device_info(self) -> PyDeviceInfo: ...
    def verify_all(self) -> PyVerifyReport: ...
    def repair(self) -> PyRepairReport: ...

    # Tombstones
    def list_tombstones(self) -> list[PyTombstoneEntry]: ...
    def delete_tombstone(self, path: str) -> bool: ...
    def clear_tombstones(self) -> int: ...

    # Scrub
    def scrub_free_space(self) -> PyScrubReport: ...

    # Metadata extensions
    def list_full(self, prefix: str | None = None) -> list[PyObjectFullInfo]:
        """List all objects with full extent details (index-only).  Sorted by key."""
        ...

    def get_metadata(self, key: str) -> bytes:
        """Read the metadata suffix bytes for an object.  Returns b'' if none."""
        ...

    def update_metadata(self, key: str, metadata: bytes) -> None:
        """Replace the metadata suffix of an object without re-uploading the body."""
        ...

    def put_with_meta(self, key: str, data: bytes, metadata: bytes) -> None:
        """Store body + metadata together in one call."""
        ...

    def put_with_meta_from_file(self, key: str, file_path: str, meta_len: int) -> None:
        """Store body + metadata from a file on disk.

        The file must contain body bytes followed by exactly meta_len bytes
        of metadata at the end.
        """
        ...

    def head_with_meta(self, key: str) -> tuple[ObjectMeta, int]:
        """Return (ObjectMeta, meta_len) for an object (index-only, no data I/O)."""
        ...

    def list_with_meta(self, prefix: str | None = None) -> list[tuple[ObjectMeta, int]]:
        """List objects with their meta_len values (index-only, zero data reads)."""
        ...

    def set_meta_len(self, key: str, meta_len: int) -> None:
        """Update meta_len in the index without touching data."""
        ...

    # Index management
    def reload_index(self) -> bool:
        """Re-read the on-disk index.  Returns True if refreshed, False if already up-to-date."""
        ...

    def needs_flush(self) -> bool:
        """Whether the store has dirty (unflushed) changes."""
        ...

    def layout_map(self) -> dict:
        """Return the full device layout for visualization.

        Returns a dict with keys: device_size, data_region_start, data_region_end,
        index_region_a, index_region_b, active_index_region, txn_id,
        extents (list of dicts), free_regions (list of (offset, size) tuples).
        """
        ...

    # Context manager
    def __enter__(self) -> Store: ...
    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> bool: ...

def format(
    path: str,
    *,
    size: int | None = None,
    direct_io: bool = False,
    index_slot_size: int | None = None,
    max_key_length: int | None = None,
    compression: str | None = None,
) -> Store:
    """Format a new store on a file or block device.

    max_key_length: maximum object key length in bytes (default 1024, hard ceiling 65536).
    Values above index_slot_size/256 - 98 are silently clamped to that shard-slot ceiling.

    compression: compression algorithm name ("none", "zstd", "snappy",
    "gzip0".."gzip9"). Default "none".
    """
    ...

def open(
    path: str,
    *,
    mode: str = "default",
    readonly: bool = False,
) -> Store:
    """Open an existing store.  mode: 'default', 'full_verify', or 'skip_verify'."""
    ...

def modify_flags(
    path: str,
    *,
    set_flags: int = 0,
    clear_flags: int = 0,
) -> int:
    """Toggle superblock flags without fully opening the device.

    set_flags bits are OR'd in; clear_flags bits are AND-NOT'd out.
    Returns the resulting flags value.
    """
    ...

FLAG_DIRECT_IO: int
"""Flag constant for O_DIRECT mode (value: 1)."""

FLAG_WRITE_PROTECT: int
"""Flag constant for write-protect (value: 2)."""

__version__: str
"""Package version string from Cargo.toml (e.g. '0.1.0')."""

__build_info__: str
"""Full build info: version, git hash, and build date."""

