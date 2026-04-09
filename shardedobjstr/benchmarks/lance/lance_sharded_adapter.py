"""
Lance ObjectStore adapter for ShardedObjectStore.

Wraps a shardedobjstr.ClusterStore (backed by one or more raw shards)
to implement the Python object store interface that Lance expects:
  - put(path, data)
  - get(path) -> bytes
  - get_range(path, offset, length) -> bytes
  - head(path) -> dict
  - delete(path)
  - list(prefix) -> list[dict]
  - list_with_delimiter(prefix) -> dict
  - copy(src, dst)
"""

import os
import subprocess
import stat

import shardedobjstr


def _get_block_device_size(path):
    """Return size of a block device in bytes using blockdev --getsize64."""
    result = subprocess.run(
        ["blockdev", "--getsize64", path],
        capture_output=True, text=True, check=True,
    )
    return int(result.stdout.strip())


class LanceShardedObjectStore:
    """Adapter wrapping shardedobjstr.ClusterStore for lance.write_dataset()
    and lance.dataset() via the object_store= parameter."""

    def __init__(self, store: shardedobjstr.ClusterStore):
        self._store = store

    def put(self, path: str, data: bytes) -> None:
        self._store.put(path, data)

    def get(self, path: str) -> bytes:
        return self._store.get(path)

    def get_range(self, path: str, offset: int, length: int) -> bytes:
        return self._store.get(path, range=(offset, offset + length))

    def head(self, path: str) -> dict:
        meta = self._store.head(path)
        return {
            "size": meta.size,
            "location": meta.location,
            "last_modified": meta.last_modified,
            "e_tag": meta.e_tag,
        }

    def delete(self, path: str) -> None:
        self._store.delete(path)

    def list(self, prefix: str = None) -> list:
        items = self._store.list(prefix)
        result = [
            {
                "size": m.size,
                "location": m.location,
                "last_modified": m.last_modified,
                "e_tag": m.e_tag,
            }
            for m in items
        ]
        result.sort(key=lambda x: x["location"])
        return result

    def list_with_delimiter(self, prefix: str = None) -> dict:
        result = self._store.list_with_delimiter(prefix)
        objects = [
            {
                "size": m.size,
                "location": m.location,
                "last_modified": m.last_modified,
                "e_tag": m.e_tag,
            }
            for m in result.objects
        ]
        objects.sort(key=lambda x: x["location"])
        prefixes = sorted(result.common_prefixes)
        return {
            "common_prefixes": prefixes,
            "objects": objects,
        }

    def copy(self, src: str, dst: str) -> None:
        self._store.copy(src, dst)


def create_sharded_store(path, size=None, direct_io=False):
    """Format a single raw shard and wrap it in a ShardedObjectStore with rf=1.

    This gives a single-shard cluster so the benchmark measures the
    overhead of the sharding layer itself.

    Args:
        path: Image file or device path.
        size: Size in bytes (for image files; auto-detected for block devices).
        direct_io: Use O_DIRECT.

    Returns:
        (cluster_store, lance_adapter) tuple.
    """
    if size is None:
        # Block device -- query its size
        st = os.stat(path)
        if stat.S_ISBLK(st.st_mode):
            size = _get_block_device_size(path)
        else:
            raise ValueError(f"size is required for non-block-device path: {path}")
    cluster = shardedobjstr.format_and_open_cluster(
        [(path, size)],
        replication_factor=1,
        direct_io=direct_io,
    )
    return cluster, LanceShardedObjectStore(cluster)


def create_sharded_fs_store(root):
    """Open a single filesystem shard wrapped in a ShardedObjectStore with rf=1.

    Args:
        root: Directory path for the filesystem shard.

    Returns:
        (cluster_store, lance_adapter) tuple.
    """
    os.makedirs(root, exist_ok=True)
    cluster = shardedobjstr.open_fs_cluster(
        [root],
        replication_factor=1,
    )
    return cluster, LanceShardedObjectStore(cluster)
