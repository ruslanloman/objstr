"""fsspec filesystem implementation for rawobjstr.

Register the ``rawobjstr://`` protocol so that any fsspec-aware library
(pandas, PyArrow, Dask, Polars, xarray, ...) can read and write objects
stored on a raw block device or loopback file.

Usage::

    import fsspec

    # Open a filesystem backed by a 1 GB image file
    fs = fsspec.filesystem("rawobjstr", path="/tmp/store.raw")

    # Write and read
    fs.pipe("hello.txt", b"Hello from fsspec!")
    data = fs.cat("hello.txt")

    # Or pass a pre-opened store
    import rawobjstr
    store = rawobjstr.open("/tmp/store.raw")
    fs = fsspec.filesystem("rawobjstr", store=store)

Requires the ``fsspec`` package::

    pip install fsspec
"""

from __future__ import annotations

import io
from typing import Any

import fsspec
from fsspec.spec import AbstractBufferedFile

import rawobjstr


def _strip_protocol(path: str) -> str:
    """Remove the ``rawobjstr://`` prefix and any leading slashes."""
    for prefix in ("rawobjstr://", "rawobjstr:"):
        if path.startswith(prefix):
            path = path[len(prefix):]
    return path.lstrip("/")


class RawObjStFileSystem(fsspec.AbstractFileSystem):
    """fsspec filesystem backed by a ``rawobjstr.Store``.

    Parameters
    ----------
    path : str, optional
        Path to the raw device or loopback image file.  Ignored when
        *store* is provided.
    store : rawobjstr.Store, optional
        A pre-opened store instance.  When given, *path*, *mode*, and
        *readonly* are ignored.
    mode : str, optional
        Open mode passed to ``rawobjstr.open()`` (``"default"``,
        ``"full_verify"``, or ``"skip_verify"``).  Default ``"default"``.
    readonly : bool, optional
        Open the store in read-only mode.  Default ``False``.
    """

    protocol = "rawobjstr"

    def __init__(
        self,
        path: str | None = None,
        store: rawobjstr.Store | None = None,
        mode: str = "default",
        readonly: bool = False,
        **storage_options: Any,
    ) -> None:
        super().__init__(**storage_options)
        if store is not None:
            self._store = store
        elif path is not None:
            self._store = rawobjstr.open(path, mode=mode, readonly=readonly)
        else:
            raise ValueError(
                "Either 'path' (device/image path) or 'store' "
                "(pre-opened rawobjstr.Store) is required"
            )

    # -- fast-path bulk I/O ------------------------------------------------

    def cat_file(self, path: str, start: int | None = None,
                 end: int | None = None, **kwargs: Any) -> bytes:
        key = _strip_protocol(path)
        if start is not None or end is not None:
            data = self._store.get(key)
            s = start or 0
            if s < 0:
                s = max(0, len(data) + s)
            if end is not None:
                e = end if end >= 0 else len(data) + end
            else:
                e = len(data)
            return data[s:e]
        return self._store.get(key)

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        key = _strip_protocol(path)
        self._store.put(key, value)

    # -- file-level operations ---------------------------------------------

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict | None = None,
        **kwargs: Any,
    ) -> RawObjStFile:
        return RawObjStFile(
            fs=self,
            path=path,
            mode=mode,
            autocommit=autocommit,
            cache_options=cache_options or {},
            **kwargs,
        )

    # -- directory / listing -----------------------------------------------

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list:
        prefix = _strip_protocol(path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        result = self._store.list_with_delimiter(prefix)

        entries = []
        for obj in result.objects:
            entry = {
                "name": obj.location,
                "size": obj.size,
                "type": "file",
                "last_modified": obj.last_modified,
            }
            entries.append(entry)
        for p in result.common_prefixes:
            entry = {
                "name": p.rstrip("/"),
                "size": 0,
                "type": "directory",
            }
            entries.append(entry)

        if detail:
            return entries
        return [e["name"] for e in entries]

    def info(self, path: str, **kwargs: Any) -> dict:
        key = _strip_protocol(path)
        if not key or key.endswith("/"):
            return {"name": key.rstrip("/") or "/", "size": 0, "type": "directory"}
        try:
            meta = self._store.head(key)
            return {
                "name": meta.location,
                "size": meta.size,
                "type": "file",
                "last_modified": meta.last_modified,
                "etag": meta.e_tag,
            }
        except FileNotFoundError:
            raise FileNotFoundError(key)

    def exists(self, path: str, **kwargs: Any) -> bool:
        key = _strip_protocol(path)
        try:
            self._store.head(key)
            return True
        except FileNotFoundError:
            return False

    # -- mutating operations -----------------------------------------------

    def _rm(self, path: str, **kwargs: Any) -> None:
        key = _strip_protocol(path)
        self._store.delete(key)

    def rm_file(self, path: str) -> None:
        key = _strip_protocol(path)
        # Raise early so callers get the expected FileNotFoundError
        self._store.head(key)
        self._store.delete(key)

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        src = _strip_protocol(path1)
        dst = _strip_protocol(path2)
        self._store.copy(src, dst)

    def mv(self, path1: str, path2: str, **kwargs: Any) -> None:
        src = _strip_protocol(path1)
        dst = _strip_protocol(path2)
        self._store.rename(src, dst)

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _strip_protocol(path: str) -> str:
        return _strip_protocol(path)

    @property
    def store(self) -> rawobjstr.Store:
        """The underlying ``rawobjstr.Store`` instance."""
        return self._store


class RawObjStFile(AbstractBufferedFile):
    """A file-like object backed by a rawobjstr store.

    * **Read mode** (``"rb"``): fetches the full object into memory on first
      read.  Supports ``seek()`` and ``read()`` afterwards.
    * **Write mode** (``"wb"``): buffers all writes in memory, then
      ``put()``s the complete object on ``close()`` / ``flush()``.
    """

    def _fetch_range(self, start: int, end: int) -> bytes:
        key = _strip_protocol(self.path)
        return self.fs._store.get(key, range=(start, end))

    def _upload_chunk(self, final: bool = False) -> bool:
        if final:
            key = _strip_protocol(self.path)
            self.buffer.seek(0)
            data = self.buffer.read()
            self.fs._store.put(key, data)
        return True

    def _initiate_upload(self) -> None:
        # Nothing to do -- we buffer in self.buffer and flush on close.
        pass
