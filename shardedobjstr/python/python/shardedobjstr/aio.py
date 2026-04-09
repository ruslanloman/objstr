"""Async wrappers for shardedobjstr.ClusterStore.

Offloads every blocking Rust call to a thread-pool executor via
``asyncio.get_running_loop().run_in_executor()``, so the asyncio
event loop is never blocked.

Usage::

    import asyncio
    from shardedobjstr.aio import AsyncClusterStore, async_format_and_open_cluster

    async def main():
        store = await async_format_and_open_cluster(
            [("/tmp/s0.raw", 64*1024*1024), ("/tmp/s1.raw", 64*1024*1024)],
            replication_factor=2,
        )
        await store.put("hello.txt", b"Hello, async cluster!")
        data = await store.get("hello.txt")
        print(data)
        store.flush_all()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Executor
from functools import partial
from typing import Any, List, Optional, Tuple

import shardedobjstr
from shardedobjstr import (
    ClusterStore,
    ObjectMeta,
    ListResult,
    PlacementInfo,
    ShardHealth,
    InvalidateReport,
    RepairReplicationResult,
    VerifyObjectReport,
    VerifyReport,
    RepairReplicationPlan,
)


class AsyncClusterStore:
    """Async wrapper around ``shardedobjstr.ClusterStore``.

    I/O-bound methods are async.  Pure in-memory accessors remain
    synchronous.

    Parameters
    ----------
    inner : shardedobjstr.ClusterStore
        The underlying synchronous cluster store.
    executor : concurrent.futures.Executor, optional
        Thread-pool executor for offloading blocking calls.
        ``None`` (default) uses the loop's default executor.
    """

    def __init__(
        self, inner: ClusterStore, executor: Optional[Executor] = None
    ) -> None:
        self._store = inner
        self._executor = executor

    @property
    def sync(self) -> ClusterStore:
        """Access the underlying synchronous ``ClusterStore`` directly."""
        return self._store

    # -- helpers --------------------------------------------------------------

    def _run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        if kwargs:
            return loop.run_in_executor(self._executor, partial(fn, *args, **kwargs))
        return loop.run_in_executor(self._executor, fn, *args)

    # -- write ----------------------------------------------------------------

    async def put(self, key: str, data: bytes) -> None:
        await self._run(self._store.put, key, data)

    async def put_if_not_exists(self, key: str, data: bytes) -> None:
        await self._run(self._store.put_if_not_exists, key, data)

    async def put_multipart(self, key: str, data: bytes) -> None:
        await self._run(self._store.put_multipart, key, data)

    # -- read -----------------------------------------------------------------

    async def get(self, key: str, *, range: Optional[Tuple[int, int]] = None) -> bytes:
        return await self._run(self._store.get, key, range=range)

    async def head(self, key: str) -> ObjectMeta:
        return await self._run(self._store.head, key)

    # -- metadata-aware I/O ---------------------------------------------------

    async def put_with_meta(self, key: str, data: bytes, metadata: bytes) -> None:
        await self._run(self._store.put_with_meta, key, data, metadata)

    async def head_with_meta(self, key: str) -> Tuple[ObjectMeta, int]:
        return await self._run(self._store.head_with_meta, key)

    async def get_metadata(self, key: str) -> bytes:
        return await self._run(self._store.get_metadata, key)

    async def list_with_meta(
        self, prefix: Optional[str] = None
    ) -> List[Tuple[ObjectMeta, int]]:
        return await self._run(self._store.list_with_meta, prefix)

    async def set_meta_len(self, key: str, meta_len: int) -> None:
        await self._run(self._store.set_meta_len, key, meta_len)

    # -- delete ---------------------------------------------------------------

    async def delete(self, key: str) -> None:
        await self._run(self._store.delete, key)

    async def list_delete_markers(self) -> list:
        """List all current delete markers."""
        return await self._run(self._store.list_delete_markers)

    async def vacuum_delete_markers(self) -> tuple:
        """Vacuum stale delete markers. All shards must be healthy."""
        return await self._run(self._store.vacuum_delete_markers)

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

    # -- cluster management (async -- may do I/O) -----------------------------

    async def invalidate_shard(self, shard_id: int) -> InvalidateReport:
        return await self._run(self._store.invalidate_shard, shard_id)

    async def attach_shard(
        self, shard_id: int, path: str, *, force: bool = True
    ) -> int:
        return await self._run(
            self._store.attach_shard, shard_id, path, force=force
        )

    async def replicate_object(
        self, key: str, from_shard: int, to_shard: int
    ) -> int:
        return await self._run(
            self._store.replicate_object, key, from_shard, to_shard
        )

    async def remove_replica(self, key: str, shard_id: int) -> None:
        await self._run(self._store.remove_replica, key, shard_id)

    async def rebuild_catalog(self) -> int:
        return await self._run(self._store.rebuild_catalog)

    async def rebuild_catalog_for_shard(self, shard_id: int) -> int:
        return await self._run(self._store.rebuild_catalog_for_shard, shard_id)

    async def repair_replication(self, *, batch_size: int = 100) -> RepairReplicationResult:
        return await self._run(self._store.repair_replication, batch_size=batch_size)

    async def re_replication_sweep(
        self, *, batch_size: int = 100
    ) -> int:
        return await self._run(
            self._store.re_replication_sweep,
            batch_size=batch_size,
        )

    async def over_replication_trim(self, *, batch_size: int = 100) -> int:
        return await self._run(
            self._store.over_replication_trim, batch_size=batch_size
        )

    async def drain_shard(
        self, shard_id: int, *, batch_size: int = 10000
    ) -> RepairReplicationResult:
        return await self._run(
            self._store.drain_shard, shard_id, batch_size=batch_size
        )

    async def flush_all(self) -> None:
        await self._run(self._store.flush_all)

    # -- verify / cross-verify (async -- does I/O) ----------------------------

    async def verify_object(self, key: str) -> VerifyObjectReport:
        return await self._run(self._store.verify_object, key)

    async def verify_all(self, *, prefix: Optional[str] = None) -> VerifyReport:
        return await self._run(self._store.verify_all, prefix=prefix)

    async def cross_verify_object(self, key: str) -> Any:
        return await self._run(self._store.cross_verify_object, key)

    async def cross_verify_all(self, *, prefix: Optional[str] = None) -> Any:
        return await self._run(self._store.cross_verify_all, prefix=prefix)

    # -- sync accessors (no I/O, instant) -------------------------------------

    def shard_count(self) -> int:
        return self._store.shard_count()

    def replication_factor(self) -> int:
        return self._store.replication_factor()

    @property
    def read_only(self) -> bool:
        return self._store.read_only

    def shard_health(self, shard_id: int) -> Optional[ShardHealth]:
        return self._store.shard_health(shard_id)

    def set_shard_health(
        self, shard_id: int, health: ShardHealth
    ) -> Optional[ShardHealth]:
        return self._store.set_shard_health(shard_id, health)

    def read_preference(self) -> str:
        return self._store.read_preference()

    def set_read_preference(self, pref: str) -> None:
        self._store.set_read_preference(pref)

    def placement(self, key: str) -> Optional[PlacementInfo]:
        return self._store.placement(key)

    def catalog_len(self) -> int:
        return self._store.catalog_len()

    def entries_for_shard(self, shard_id: int) -> list:
        return self._store.entries_for_shard(shard_id)

    def remove_all_for_shard(self, shard_id: int) -> int:
        return self._store.remove_all_for_shard(shard_id)

    def find_under_replicated(self) -> List[Tuple[str, int]]:
        return self._store.find_under_replicated()

    def find_over_replicated(self) -> List[Tuple[str, int]]:
        return self._store.find_over_replicated()

    def find_replication_target(self, key: str) -> Optional[int]:
        return self._store.find_replication_target(key)

    def pick_excess_shard(self, key: str) -> Optional[int]:
        return self._store.pick_excess_shard(key)

    def detach_shard(self, shard_id: int) -> Optional[ShardHealth]:
        return self._store.detach_shard(shard_id)

    def save_catalog(self, path: str, *, format: str = "json") -> None:
        self._store.save_catalog(path, format=format)

    def crc_error_count(self, shard_id: int) -> int:
        return self._store.crc_error_count(shard_id)

    def min_writes(self) -> int:
        return self._store.min_writes()

    def delete_requires_min_writes(self) -> bool:
        return self._store.delete_requires_min_writes()

    def multipart_upload_count(self) -> int:
        return self._store.multipart_upload_count()

    def list_multipart_uploads(self) -> list:
        return self._store.list_multipart_uploads()

    def plan_repair_replication(self, *, batch_size: int = 100) -> RepairReplicationPlan:
        return self._store.plan_repair_replication(batch_size=batch_size)

    async def validate_shard_access(self) -> List[Tuple[int, bool]]:
        return await self._run(self._store.validate_shard_access)

    async def purge_stale_multiparts(self) -> int:
        return await self._run(self._store.purge_stale_multiparts)

    # -- context manager ------------------------------------------------------

    async def __aenter__(self) -> "AsyncClusterStore":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        await self.flush_all()
        return False

    def __repr__(self) -> str:
        return f"AsyncClusterStore({self._store!r})"


# -- module-level async constructors -----------------------------------------

async def async_open_cluster(
    paths: List[str],
    *,
    replication_factor: int = 1,
    direct_io: bool = False,
    read_only: bool = False,
    catalog_path: Optional[str] = None,
    catalog_format: str = "json",
    executor: Optional[Executor] = None,
) -> AsyncClusterStore:
    """Async version of ``shardedobjstr.open_cluster()``."""
    loop = asyncio.get_running_loop()
    store = await loop.run_in_executor(
        executor,
        partial(
            shardedobjstr.open_cluster,
            paths,
            replication_factor=replication_factor,
            direct_io=direct_io,
            read_only=read_only,
            catalog_path=catalog_path,
            catalog_format=catalog_format,
        ),
    )
    return AsyncClusterStore(store, executor)


async def async_format_and_open_cluster(
    shards: List[Tuple[str, int]],
    *,
    replication_factor: int = 1,
    min_writes: Optional[int] = None,
    delete_requires_min_writes: bool = False,
    direct_io: bool = False,
    compression: Optional[str] = None,
    executor: Optional[Executor] = None,
) -> AsyncClusterStore:
    """Async version of ``shardedobjstr.format_and_open_cluster()``."""
    loop = asyncio.get_running_loop()
    kwargs: dict[str, Any] = {
        "replication_factor": replication_factor,
        "direct_io": direct_io,
    }
    if min_writes is not None:
        kwargs["min_writes"] = min_writes
    if delete_requires_min_writes:
        kwargs["delete_requires_min_writes"] = delete_requires_min_writes
    if compression is not None:
        kwargs["compression"] = compression
    store = await loop.run_in_executor(
        executor,
        partial(shardedobjstr.format_and_open_cluster, shards, **kwargs),
    )
    return AsyncClusterStore(store, executor)


async def async_open_cluster_degraded(
    shard_paths: List[Optional[str]],
    *,
    replication_factor: int = 1,
    direct_io: bool = False,
    read_only: bool = False,
    catalog_path: Optional[str] = None,
    catalog_format: str = "json",
    executor: Optional[Executor] = None,
) -> AsyncClusterStore:
    """Async version of ``shardedobjstr.open_cluster_degraded()``."""
    loop = asyncio.get_running_loop()
    store = await loop.run_in_executor(
        executor,
        partial(
            shardedobjstr.open_cluster_degraded,
            shard_paths,
            replication_factor=replication_factor,
            direct_io=direct_io,
            read_only=read_only,
            catalog_path=catalog_path,
            catalog_format=catalog_format,
        ),
    )
    return AsyncClusterStore(store, executor)


async def async_open_cluster_from_config(
    path: str,
    *,
    direct_io: Optional[bool] = None,
    read_only: Optional[bool] = None,
    executor: Optional[Executor] = None,
) -> AsyncClusterStore:
    """Async version of ``shardedobjstr.open_cluster_from_config()``."""
    loop = asyncio.get_running_loop()
    kwargs: dict[str, Any] = {}
    if direct_io is not None:
        kwargs["direct_io"] = direct_io
    if read_only is not None:
        kwargs["read_only"] = read_only
    store = await loop.run_in_executor(
        executor,
        partial(shardedobjstr.open_cluster_from_config, path, **kwargs),
    )
    return AsyncClusterStore(store, executor)
