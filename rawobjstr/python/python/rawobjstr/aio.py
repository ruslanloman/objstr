"""Async wrappers for rawobjstr.Store.

Offloads every blocking Rust call to a thread-pool executor via
``asyncio.get_running_loop().run_in_executor()``, so the asyncio
event loop is never blocked.

Usage::

    import asyncio
    from rawobjstr.aio import AsyncStore, async_format, async_open

    async def main():
        store = await async_format("/tmp/store.raw", size=1_073_741_824)
        await store.put("hello.txt", b"Hello, async world!")
        data = await store.get("hello.txt")
        print(data)
        store.flush_index()  # sync -- fast, no I/O wait

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Executor
from functools import partial
from typing import Any, List, Optional, Tuple

import rawobjstr
from rawobjstr import (
    Store,
    ObjectMeta,
    ListResult,
    DeviceInfo,
    MultipartUpload,
)


class AsyncMultipartUpload:
    """Async wrapper around ``rawobjstr.MultipartUpload``.

    Use as an async context manager::

        async with await store.multipart("key") as upload:
            await upload.put_part(chunk1)
            await upload.put_part(chunk2)
        # auto-completes on clean exit, aborts on exception
    """

    def __init__(
        self, inner: MultipartUpload, executor: Optional[Executor] = None
    ) -> None:
        self._inner = inner
        self._executor = executor

    async def put_part(self, data: bytes) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._inner.put_part, data)

    async def complete(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._inner.complete)

    async def abort(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._inner.abort)

    async def __aenter__(self) -> "AsyncMultipartUpload":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is not None:
            await self.abort()
        else:
            await self.complete()
        return False


class AsyncStore:
    """Async wrapper around ``rawobjstr.Store``.

    All I/O methods are async.  Pure in-memory accessors (``device_info``,
    ``is_read_only``, ``needs_flush``, ``list_tombstones``, etc.) remain
    synchronous because they complete instantly.

    Parameters
    ----------
    inner : rawobjstr.Store
        The underlying synchronous store.
    executor : concurrent.futures.Executor, optional
        Thread-pool executor for offloading blocking calls.
        ``None`` (default) uses the loop's default executor.
    """

    def __init__(
        self, inner: Store, executor: Optional[Executor] = None
    ) -> None:
        self._store = inner
        self._executor = executor

    @property
    def sync(self) -> Store:
        """Access the underlying synchronous ``Store`` directly."""
        return self._store

    # -- helpers --------------------------------------------------------------

    def _run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Return a coroutine that runs *fn* in the executor."""
        loop = asyncio.get_running_loop()
        if kwargs:
            return loop.run_in_executor(self._executor, partial(fn, *args, **kwargs))
        return loop.run_in_executor(self._executor, fn, *args)

    # -- write ----------------------------------------------------------------

    async def put(self, key: str, data: bytes, *, mode: str = "overwrite") -> None:
        await self._run(self._store.put, key, data, mode=mode)

    # -- read -----------------------------------------------------------------

    async def get(self, key: str, *, range: Optional[Tuple[int, int]] = None) -> bytes:
        return await self._run(self._store.get, key, range=range)

    async def getraw(self, key: str) -> dict:
        return await self._run(self._store.getraw, key)

    async def head(self, key: str) -> ObjectMeta:
        return await self._run(self._store.head, key)

    async def get_metadata(self, key: str) -> bytes:
        return await self._run(self._store.get_metadata, key)

    # -- delete ---------------------------------------------------------------

    async def delete(self, key: str) -> None:
        await self._run(self._store.delete, key)

    # -- listing --------------------------------------------------------------

    async def list(self, prefix: Optional[str] = None) -> List[ObjectMeta]:
        return await self._run(self._store.list, prefix)

    async def list_with_delimiter(
        self, prefix: Optional[str] = None
    ) -> ListResult:
        return await self._run(self._store.list_with_delimiter, prefix)

    # -- copy & rename --------------------------------------------------------

    async def copy(self, src: str, dst: str) -> None:
        await self._run(self._store.copy, src, dst)

    async def copy_if_not_exists(self, src: str, dst: str) -> None:
        await self._run(self._store.copy_if_not_exists, src, dst)

    async def rename(self, src: str, dst: str) -> None:
        await self._run(self._store.rename, src, dst)

    async def rename_if_not_exists(self, src: str, dst: str) -> None:
        await self._run(self._store.rename_if_not_exists, src, dst)

    # -- multipart ------------------------------------------------------------

    async def multipart(self, key: str) -> AsyncMultipartUpload:
        inner = await self._run(self._store.multipart, key)
        return AsyncMultipartUpload(inner, self._executor)

    # -- maintenance (async because they do disk I/O) -------------------------

    async def flush_index(self) -> None:
        await self._run(self._store.flush_index)

    async def update_metadata(self, key: str, metadata: bytes) -> None:
        await self._run(self._store.update_metadata, key, metadata)

    async def scrub_free_space(self) -> Any:
        return await self._run(self._store.scrub_free_space)

    async def verify_all(self) -> Any:
        return await self._run(self._store.verify_all)

    async def repair(self) -> Any:
        return await self._run(self._store.repair)

    async def reload_index(self) -> bool:
        return await self._run(self._store.reload_index)

    # -- sync accessors (no I/O, instant) -------------------------------------

    def is_read_only(self) -> bool:
        return self._store.is_read_only()

    def device_info(self) -> DeviceInfo:
        return self._store.device_info()

    def needs_flush(self) -> bool:
        return self._store.needs_flush()

    def list_tombstones(self) -> list:
        return self._store.list_tombstones()

    def delete_tombstone(self, path: str) -> bool:
        return self._store.delete_tombstone(path)

    def clear_tombstones(self) -> int:
        return self._store.clear_tombstones()

    def list_full(self, prefix: Optional[str] = None) -> list:
        return self._store.list_full(prefix)

    def layout_map(self) -> dict:
        return self._store.layout_map()

    # -- metadata extensions (I/O) --------------------------------------------

    async def put_with_meta(self, key: str, data: bytes, metadata: bytes) -> None:
        await self._run(self._store.put_with_meta, key, data, metadata)

    async def put_with_meta_from_file(
        self, key: str, file_path: str, meta_len: int
    ) -> None:
        await self._run(self._store.put_with_meta_from_file, key, file_path, meta_len)

    # -- metadata extensions (index-only, instant) ----------------------------

    def head_with_meta(self, key: str) -> Tuple[Any, int]:
        return self._store.head_with_meta(key)

    def list_with_meta(self, prefix: Optional[str] = None) -> list:
        return self._store.list_with_meta(prefix)

    def set_meta_len(self, key: str, meta_len: int) -> None:
        self._store.set_meta_len(key, meta_len)

    # -- context manager ------------------------------------------------------

    async def __aenter__(self) -> "AsyncStore":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            await self.flush_index()
        return False

    def __repr__(self) -> str:
        return f"AsyncStore({self._store!r})"


# -- module-level async constructors -----------------------------------------

async def async_format(
    path: str,
    *,
    size: Optional[int] = None,
    direct_io: bool = False,
    index_slot_size: Optional[int] = None,
    max_key_length: Optional[int] = None,
    compression: Optional[str] = None,
    executor: Optional[Executor] = None,
) -> AsyncStore:
    """Async version of ``rawobjstr.format()``."""
    loop = asyncio.get_running_loop()
    kwargs: dict[str, Any] = {}
    if size is not None:
        kwargs["size"] = size
    if direct_io:
        kwargs["direct_io"] = direct_io
    if index_slot_size is not None:
        kwargs["index_slot_size"] = index_slot_size
    if max_key_length is not None:
        kwargs["max_key_length"] = max_key_length
    if compression is not None:
        kwargs["compression"] = compression
    store = await loop.run_in_executor(
        executor, partial(rawobjstr.format, path, **kwargs)
    )
    return AsyncStore(store, executor)


async def async_open(
    path: str,
    *,
    mode: str = "default",
    readonly: bool = False,
    executor: Optional[Executor] = None,
) -> AsyncStore:
    """Async version of ``rawobjstr.open()``."""
    loop = asyncio.get_running_loop()
    store = await loop.run_in_executor(
        executor, partial(rawobjstr.open, path, mode=mode, readonly=readonly)
    )
    return AsyncStore(store, executor)
