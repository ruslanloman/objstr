"""
Lance ObjectStore adapter for RawObjectStore.

Wraps a rawobjstr.Store to implement the Python object store interface
that Lance expects:
  - put(path, data)
  - get(path) -> bytes
  - get_range(path, offset, length) -> bytes
  - head(path) -> dict
  - delete(path)
  - list(prefix) -> list[dict]
  - list_with_delimiter(prefix) -> dict
  - copy(src, dst)
"""

import rawobjstr


class LanceRawObjectStore:
    """Adapter that wraps rawobjstr.Store for use with lance.write_dataset()
    and lance.dataset() via the object_store= parameter."""

    def __init__(self, store: rawobjstr.Store):
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


def create_store(path, size=None, direct_io=False):
    """Format and return both a rawobjstr.Store and a LanceRawObjectStore adapter.

    Args:
        path: Device or image file path (e.g. /dev/sdb or /tmp/lance_bench.img)
        size: Size in bytes (required for image files, auto-detected for devices)
        direct_io: Whether to use O_DIRECT

    Returns:
        (raw_store, lance_adapter) tuple
    """
    raw = rawobjstr.format(path, size=size, direct_io=direct_io)
    return raw, LanceRawObjectStore(raw)
