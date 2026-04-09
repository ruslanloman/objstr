"""Type stubs for the native _shardedobjstr module."""

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

class PlacementInfo:
    """Placement of an object across cluster shards."""
    shards: list[int]
    size: int
    crc32c: Optional[int]
    updated: str

class ShardHealth:
    """Health state of a shard."""
    Healthy: ShardHealth
    Degraded: ShardHealth
    Offline: ShardHealth
    Syncing: ShardHealth

class InvalidateReport:
    """Report returned by invalidate_shard()."""
    shard_id: int
    entries_purged: int
    entries_restored: int
    missing_keys: list[str]
    scan_ok: bool

class RepairReplicationResult:
    """Result of a repair-replication sweep."""
    re_replicated: int
    trimmed: int
    under_remaining: int
    over_remaining: int

class VerifyReplicaResult:
    """Verification result for a single object replica on one shard."""
    shard_id: int
    crc32c: Optional[int]
    size: Optional[int]
    matches_catalog: Optional[bool]
    error: Optional[str]

class VerifyObjectReport:
    """Verification report for a single object across its replicas."""
    key: str
    catalog_crc: Optional[int]
    replicas: list[VerifyReplicaResult]
    replicas_consistent: bool

class VerifyReport:
    """Aggregate verification report for all objects."""
    objects_checked: int
    objects_ok: int
    objects_mismatched: int
    objects_with_errors: int
    details: list[VerifyObjectReport]

class PlannedAction:
    """A planned repair-replication action (replicate or trim)."""
    key: str
    current_count: int
    target_rf: int
    action_type: str
    source_shard: Optional[int]
    target_shard: Optional[int]
    trim_shard: Optional[int]

class RepairReplicationPlan:
    """Dry-run repair-replication plan showing what repair_replication() would do."""
    replications: list[PlannedAction]
    trims: list[PlannedAction]
    unrepairable: int
    untrimmable: int

class CrossVerifyShardDigest:
    """MD5 digest and metadata for a single object on a single shard."""
    shard_id: int
    md5_hex: str
    size: int
    last_modified: str

class CrossVerifyObjectReport:
    """Report from cross-verifying a single object across its replicas."""
    key: str
    catalog_crc: Optional[int]
    shards: list[CrossVerifyShardDigest]
    consistent: bool
    errors: list[tuple[int, str]]

class CrossVerifyReport:
    """Aggregate report from cross-verifying all objects."""
    objects_checked: int
    objects_ok: int
    objects_mismatched: int
    objects_with_errors: int
    objects_skipped_single_replica: int
    details: list[CrossVerifyObjectReport]

class ClusterStore:
    """A sharded object store backed by multiple RawObjectStore shards."""

    # Write
    def put(self, key: str, data: bytes) -> None:
        """Write an object, replicating across N shards."""
        ...

    def put_if_not_exists(self, key: str, data: bytes) -> None:
        """Write an object only if the key does not already exist.
        Raises FileExistsError if the key is already present."""
        ...

    # Read
    def get(self, key: str, *, range: tuple[int, int] | None = None) -> bytes:
        """Read an object (or a byte range)."""
        ...

    def head(self, key: str) -> ObjectMeta:
        """Get object metadata without reading the data."""
        ...

    # Metadata-aware I/O
    def put_with_meta(self, key: str, data: bytes, metadata: bytes) -> None:
        """Write an object with raw metadata bytes, replicating across shards."""
        ...

    def put_with_meta_from_file(self, key: str, file_path: str, meta_len: int) -> None:
        """Write an object from a local file containing payload + metadata.

        The file must contain the object payload followed by ``meta_len``
        bytes of metadata.  When all target shards are Raw the file is
        streamed directly (~1 MB peak heap)."""
        ...

    def head_with_meta(self, key: str) -> tuple[ObjectMeta, int]:
        """Get object metadata + meta_len without reading the body.
        Returns (ObjectMeta, meta_len)."""
        ...

    def get_metadata(self, key: str) -> bytes:
        """Read the raw metadata bytes for an object.
        Returns empty bytes if the object has no metadata."""
        ...

    def list_with_meta(self, prefix: str | None = None) -> list[tuple[ObjectMeta, int]]:
        """List objects with their metadata lengths.
        Returns a list of (ObjectMeta, meta_len) tuples."""
        ...

    def set_meta_len(self, key: str, meta_len: int) -> None:
        """Set the metadata length for an object on its raw shard."""
        ...

    # Delete
    def delete(self, key: str) -> None: ...

    def list_delete_markers(self) -> list[tuple[str, str]]:
        """List all current delete markers.
        Returns a list of (original_key, deleted_at_rfc3339) tuples."""
        ...

    def vacuum_delete_markers(self) -> tuple[int, int]:
        """Vacuum stale delete markers. All shards must be healthy.
        Returns (markers_purged, stale_objects_cleaned)."""
        ...

    # Listing
    def list(self, prefix: str | None = None) -> list[ObjectMeta]: ...
    def list_with_delimiter(self, prefix: str | None = None) -> ListResult: ...

    # Copy & Rename
    def copy(self, src: str, dst: str) -> None: ...
    def copy_if_not_exists(self, src: str, dst: str) -> None: ...
    def rename(self, src: str, dst: str) -> None: ...
    def rename_if_not_exists(self, src: str, dst: str) -> None: ...

    # Cluster management
    def shard_count(self) -> int:
        """Number of shards in the cluster."""
        ...

    def replication_factor(self) -> int:
        """Configured replication factor."""
        ...

    def min_writes(self) -> int:
        """Minimum successful replica writes required for a put to succeed."""
        ...

    def delete_requires_min_writes(self) -> bool:
        """Whether deletes also enforce min_writes."""
        ...

    def shard_health(self, id: int) -> ShardHealth | None:
        """Health of a specific shard."""
        ...

    def set_shard_health(self, id: int, health: ShardHealth) -> ShardHealth | None:
        """Set shard health. Returns previous health status."""
        ...

    def invalidate_shard(self, shard_id: int) -> InvalidateReport:
        """Purge catalog entries for a shard, re-scan, and return a report."""
        ...

    def entries_for_shard(self, shard_id: int) -> list[tuple[str, PlacementInfo]]:
        """Get all catalog entries placed on a specific shard."""
        ...

    def remove_all_for_shard(self, shard_id: int) -> int:
        """Remove all catalog entries mentioning a shard. Returns count removed."""
        ...

    def put_multipart(self, key: str, data: bytes) -> None:
        """Single-shot multipart upload (delegates to primary shard)."""
        ...

    def placement(self, key: str) -> PlacementInfo | None:
        """Which shards hold this object."""
        ...

    def catalog_len(self) -> int:
        """Total number of objects tracked by the catalog."""
        ...

    def save_catalog(self, path: str, *, format: str = "json") -> None:
        """Save the catalog to a file.

        Args:
            path: File path to save to.
            format: 'json' (default) or 'bincode'.
        """
        ...

    def rebuild_catalog(self) -> int:
        """Rebuild catalog by scanning all shards. Returns object count found."""
        ...

    def rebuild_catalog_for_shard(self, shard_id: int) -> int:
        """Rebuild catalog entries for a single shard. Returns object count found."""
        ...

    def find_under_replicated(self) -> list[tuple[str, int]]:
        """Find objects whose replica count is below the replication factor.
        Returns a list of (key, current_replica_count) tuples."""
        ...

    def hold_offline(self, shard_id: int, suppress_replication: bool = False) -> ShardHealth | None:
        """Detach a shard and hold it offline.

        If ``suppress_replication`` is True, the re-replication sweep will
        skip objects on this shard.  Returns previous health, or None if
        shard_id is out of range."""
        ...

    def release_hold(self, shard_id: int) -> bool:
        """Release the Detached hold on a shard.
        Returns True if the shard was held."""
        ...

    def shard_suppress_replication(self, shard_id: int) -> bool:
        """Check whether re-replication is suppressed for a shard."""
        ...

    def shard_detach_reason(self, shard_id: int) -> str | None:
        """Return why a shard was detached, or None if healthy.
        Possible values: 'Manual', 'ProbeFailure', 'DeviceMissing', 'Drain'."""
        ...

    def set_detach_reason(self, shard_id: int, reason: str) -> None:
        """Annotate a detached shard with a reason.
        Must be one of: 'Manual', 'ProbeFailure', 'DeviceMissing', 'Drain'."""
        ...

    def detach_shard(self, shard_id: int) -> ShardHealth | None:
        """Detach a shard: replace with offline placeholder.
        Catalog entries are preserved. Returns previous health, or None if out of range."""
        ...

    def attach_shard(self, shard_id: int, path: str, *, force: bool = True) -> int:
        """Attach a store to an offline shard slot.
        If force is True, trust existing data (scan only).
        If False, invalidate first (purge + re-scan).
        Returns the number of objects found on the shard."""
        ...

    def replicate_object(self, key: str, from_shard: int, to_shard: int) -> int:
        """Replicate a single object from one shard to another.
        Returns the size of the replicated object in bytes."""
        ...

    def find_over_replicated(self) -> list[tuple[str, int]]:
        """Find objects whose replica count exceeds the replication factor.
        Returns a list of (key, current_replica_count) tuples."""
        ...

    def pick_excess_shard(self, key: str) -> int | None:
        """Pick the best shard from which to remove an excess replica.
        Prefers the fullest healthy shard. Returns shard id or None."""
        ...

    def remove_replica(self, key: str, shard_id: int) -> None:
        """Remove a single replica of an object from a shard."""
        ...

    def find_replication_target(self, key: str) -> int | None:
        """Find the best target shard for replicating an object.
        Picks healthy shard not already holding a copy. Returns shard id or None."""
        ...

    def repair_replication(self, *, batch_size: int = 100) -> RepairReplicationResult:
        """Run a full repair-replication sweep: repair under-replicated objects
        then trim over-replicated ones.
        Returns a RepairReplicationResult with counts."""
        ...

    def re_replication_sweep(self, *, batch_size: int = 100) -> int:
        """Copy under-replicated objects to healthy shards.
        Returns the number of objects re-replicated."""
        ...

    def over_replication_trim(self, *, batch_size: int = 100) -> int:
        """Remove excess replicas beyond the replication factor.
        Returns the number of excess replicas removed."""
        ...

    def drain_shard(self, shard_id: int, *, batch_size: int = 10000) -> RepairReplicationResult:
        """Detach a shard and move all its objects to other shards.
        Returns a RepairReplicationResult with counts."""
        ...

    def crc_error_count(self, shard_id: int) -> int:
        """Cumulative CRC error count for a shard (resets on process restart)."""
        ...

    def shard_offline_since(self, shard_id: int) -> float | None:
        """Return when a shard went offline as a Unix timestamp, or None if healthy."""
        ...

    def shard_free_space(self, shard_id: int) -> int | None:
        """Return the cached free space for a shard in bytes, or None if not set."""
        ...

    def set_shard_free_space(self, shard_id: int, free: int) -> None:
        """Update the cached free space for a shard."""
        ...

    def read_repair_count(self) -> int:
        """Number of background read-repair tasks triggered so far."""
        ...

    def read_repair_success(self) -> int:
        """Number of read-repair tasks that succeeded."""
        ...

    def read_repair_failed(self) -> int:
        """Number of read-repair tasks that failed."""
        ...

    def multipart_expiry(self) -> float:
        """Return the multipart upload expiry duration in seconds."""
        ...

    def read_preference(self) -> str:
        """Get the current read preference ('round-robin' or 'ordered')."""
        ...

    def set_read_preference(self, pref: str) -> None:
        """Set the read preference. Accepts 'round-robin' or 'ordered'."""
        ...

    def __enter__(self) -> ClusterStore: ...
    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> bool: ...

    def cross_verify_object(self, key: str) -> CrossVerifyObjectReport:
        """Cross-verify a single object by comparing MD5 digests across replicas.
        Returns a CrossVerifyObjectReport with per-shard MD5, size, timestamps,
        and a consistent flag."""
        ...

    def cross_verify_all(self, *, prefix: str | None = None) -> CrossVerifyReport:
        """Cross-verify all objects by comparing MD5 digests across replicas.
        Objects with only one replica are skipped.
        Returns a CrossVerifyReport with aggregate counts and details for
        mismatched or errored objects."""
        ...

    def verify_object(self, key: str) -> VerifyObjectReport:
        """Verify a single object by checking CRC32C on each replica.
        Returns a VerifyObjectReport with per-shard results."""
        ...

    def verify_all(self, *, prefix: str | None = None) -> VerifyReport:
        """Verify all objects (optionally filtered by prefix).
        Returns a VerifyReport with aggregate counts and details."""
        ...

    def plan_repair_replication(self, *, batch_size: int = 100) -> RepairReplicationPlan:
        """Dry-run: compute what repair_replication() would do without making changes.
        Returns a RepairReplicationPlan with planned replications and trims."""
        ...

    def validate_shard_access(self) -> list[tuple[int, bool]]:
        """Check accessibility of each shard.
        Returns a list of (shard_id, accessible) tuples."""
        ...

    def multipart_upload_count(self) -> int:
        """Number of in-progress multipart uploads."""
        ...

    def list_multipart_uploads(self) -> list[tuple[int, str, int, int]]:
        """List in-progress multipart uploads.
        Returns list of (upload_id, key, created_epoch, shard_id) tuples."""
        ...

    def purge_stale_multiparts(self) -> int:
        """Purge expired multipart uploads. Returns number purged."""
        ...

    @property
    def read_only(self) -> bool:
        """Whether the cluster is in read-only mode."""
        ...

    def flush_all(self) -> None:
        """Flush the index of every shard so data is persisted to disk."""
        ...

def format_shard(path: str, *, size: int | None = None, direct_io: bool = False) -> None:
    """Format a shard at the given path.

    For block devices, size can be omitted (auto-detected).
    For image files, size must be provided.
    """
    ...

def format_shard_with_options(
    path: str,
    *,
    size: int,
    direct_io: bool = False,
    index_slot_size: int | None = None,
    max_key_length: int | None = None,
    compression: str | None = None,
) -> None:
    """Format a shard with full control over index sizing and compression.

    Supported compression values: "snappy", "zstd", or None (no compression).
    """
    ...

def open_cluster(
    paths: list[str],
    *,
    replication_factor: int = 1,
    min_writes: int | None = None,
    delete_requires_min_writes: bool = False,
    direct_io: bool = False,
    read_only: bool = False,
    catalog_path: str | None = None,
    catalog_format: str = "json",
) -> ClusterStore:
    """Open an existing cluster from a list of shard paths.

    If catalog_path is given and the file exists, it is loaded automatically.
    catalog_format controls the serialization: 'json' (default) or 'bincode'.
    """
    ...

def format_and_open_cluster(
    shards: list[tuple[str, int]],
    *,
    replication_factor: int = 1,
    min_writes: int | None = None,
    delete_requires_min_writes: bool = False,
    direct_io: bool = False,
    compression: str | None = None,
) -> ClusterStore:
    """Format a set of shards and open them as a cluster.

    Each element of shards is a (path, size_bytes) tuple.
    All existing content on the shards is destroyed.
    Supported compression values: "snappy", "zstd", or None (no compression).
    """
    ...

def open_cluster_degraded(
    shard_paths: list[str | None],
    *,
    replication_factor: int = 1,
    min_writes: int | None = None,
    delete_requires_min_writes: bool = False,
    direct_io: bool = False,
    read_only: bool = False,
    catalog_path: str | None = None,
    catalog_format: str = "json",
) -> ClusterStore:
    """Open a cluster where some shards may be offline (degraded startup).

    Each entry in shard_paths is either a path string (available shard)
    or None (offline slot). Use attach_shard() to bring offline shards
    online later.
    """
    ...

def open_fs_cluster(
    roots: list[str],
    *,
    replication_factor: int = 1,
    min_writes: int | None = None,
    delete_requires_min_writes: bool = False,
) -> ClusterStore:
    """Open a cluster backed by filesystem (LocalFileSystem) shards.

    Each entry in roots is a directory path that must already exist.
    """
    ...

def load_config(path: str) -> dict:
    """Load and validate a cluster config file. Returns a dict with parsed config."""
    ...

def check_config(path: str) -> list[dict]:
    """Validate a cluster config file.

    Returns a list of dicts, each with 'level' ('ERROR', 'WARN', 'INFO')
    and 'message' keys.
    """
    ...

def open_cluster_from_config(
    path: str,
    *,
    min_writes: int | None = None,
    direct_io: bool | None = None,
    read_only: bool | None = None,
) -> ClusterStore:
    """Open a cluster from a config file.

    Reads shard paths, replicas, catalog settings from the file.
    Keyword arguments override config values.
    """
    ...

__version__: str
__build_info__: str
