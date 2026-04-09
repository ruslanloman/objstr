"""Tests for the shardedobjstr.aio async API.

Exercises AsyncClusterStore, async_format_and_open_cluster, and
concurrent operations to validate that the executor-based async
wrappers correctly offload blocking Rust calls.
"""

import asyncio

import pytest
import pytest_asyncio

import shardedobjstr
from shardedobjstr.aio import (
    AsyncClusterStore,
    async_format_and_open_cluster,
)

from conftest import SHARD_SIZE, make_payload


@pytest_asyncio.fixture
async def acluster(tmp_path):
    shards = [
        (str(tmp_path / "shard0.raw"), SHARD_SIZE),
        (str(tmp_path / "shard1.raw"), SHARD_SIZE),
        (str(tmp_path / "shard2.raw"), SHARD_SIZE),
    ]
    store = await async_format_and_open_cluster(shards, replication_factor=2)
    yield store
    await store.flush_all()


# ---------------------------------------------------------------------------
# Format and open
# ---------------------------------------------------------------------------

class TestAsyncFormatAndOpen:
    @pytest.mark.asyncio
    async def test_basic(self, acluster):
        assert acluster.shard_count() == 3
        assert acluster.replication_factor() == 2
        assert acluster.catalog_len() == 0

    @pytest.mark.asyncio
    async def test_repr(self, acluster):
        r = repr(acluster)
        assert "AsyncClusterStore" in r

    @pytest.mark.asyncio
    async def test_sync_property(self, acluster):
        assert isinstance(acluster.sync, shardedobjstr.ClusterStore)


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------

class TestAsyncCRUD:
    @pytest.mark.asyncio
    async def test_put_get(self, acluster):
        await acluster.put("hello.txt", b"Hello, async cluster!")
        data = await acluster.get("hello.txt")
        assert data == b"Hello, async cluster!"

    @pytest.mark.asyncio
    async def test_put_overwrite(self, acluster):
        await acluster.put("key", b"v1")
        await acluster.put("key", b"v2")
        assert await acluster.get("key") == b"v2"

    @pytest.mark.asyncio
    async def test_put_if_not_exists(self, acluster):
        await acluster.put_if_not_exists("unique", b"data")
        with pytest.raises(FileExistsError):
            await acluster.put_if_not_exists("unique", b"other")

    @pytest.mark.asyncio
    async def test_head(self, acluster):
        payload = b"abc"
        await acluster.put("meta.txt", payload)
        meta = await acluster.head("meta.txt")
        assert meta.location == "meta.txt"
        assert meta.size == len(payload)

    @pytest.mark.asyncio
    async def test_delete(self, acluster):
        await acluster.put("del.txt", b"gone")
        await acluster.delete("del.txt")
        with pytest.raises(FileNotFoundError):
            await acluster.get("del.txt")

    @pytest.mark.asyncio
    async def test_get_not_found(self, acluster):
        with pytest.raises(FileNotFoundError):
            await acluster.get("no-such-key")

    @pytest.mark.asyncio
    async def test_get_range(self, acluster):
        await acluster.put("ranged", b"0123456789")
        data = await acluster.get("ranged", range=(2, 5))
        assert data == b"234"


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------

class TestAsyncListing:
    @pytest.mark.asyncio
    async def test_list_empty(self, acluster):
        items = await acluster.list()
        assert items == []

    @pytest.mark.asyncio
    async def test_list_with_prefix(self, acluster):
        await acluster.put("a/1", b"x")
        await acluster.put("a/2", b"y")
        await acluster.put("b/1", b"z")
        items = await acluster.list("a")
        assert len(items) == 2

    @pytest.mark.asyncio
    async def test_list_with_delimiter(self, acluster):
        await acluster.put("dir/a.txt", b"a")
        await acluster.put("dir/b.txt", b"b")
        await acluster.put("other.txt", b"c")
        result = await acluster.list_with_delimiter(None)
        assert any("dir" in p for p in result.common_prefixes)


# ---------------------------------------------------------------------------
# Copy & Rename
# ---------------------------------------------------------------------------

class TestAsyncCopyRename:
    @pytest.mark.asyncio
    async def test_copy(self, acluster):
        await acluster.put("src", b"data")
        await acluster.copy("src", "dst")
        assert await acluster.get("dst") == b"data"

    @pytest.mark.asyncio
    async def test_copy_if_not_exists(self, acluster):
        await acluster.put("a", b"1")
        await acluster.copy_if_not_exists("a", "b")
        assert await acluster.get("b") == b"1"
        with pytest.raises(FileExistsError):
            await acluster.copy_if_not_exists("a", "b")

    @pytest.mark.asyncio
    async def test_rename(self, acluster):
        await acluster.put("old", b"val")
        await acluster.rename("old", "new")
        assert await acluster.get("new") == b"val"
        with pytest.raises(FileNotFoundError):
            await acluster.get("old")


# ---------------------------------------------------------------------------
# Placement & replication
# ---------------------------------------------------------------------------

class TestAsyncPlacement:
    @pytest.mark.asyncio
    async def test_placement(self, acluster):
        await acluster.put("placed", b"data")
        info = acluster.placement("placed")
        assert info is not None
        assert len(info.shards) == 2  # rf=2

    @pytest.mark.asyncio
    async def test_rebuild_catalog(self, acluster):
        await acluster.put("rc1", b"a")
        await acluster.put("rc2", b"b")
        count = await acluster.rebuild_catalog()
        assert count >= 2

    @pytest.mark.asyncio
    async def test_re_replication_sweep(self, acluster):
        await acluster.put("rrs", b"data")
        repaired = await acluster.re_replication_sweep()
        assert isinstance(repaired, int)


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

class TestAsyncConcurrency:
    @pytest.mark.asyncio
    async def test_parallel_puts(self, acluster):
        """Multiple puts in parallel should all succeed."""
        payloads = {f"key-{i}": make_payload(1000, seed=i) for i in range(20)}
        await asyncio.gather(
            *(acluster.put(k, v) for k, v in payloads.items())
        )
        results = await asyncio.gather(
            *(acluster.get(k) for k in payloads)
        )
        for k, data in zip(payloads, results):
            assert data == payloads[k]

    @pytest.mark.asyncio
    async def test_parallel_gets(self, acluster):
        await acluster.put("shared", b"shared-data")
        results = await asyncio.gather(
            *(acluster.get("shared") for _ in range(10))
        )
        assert all(r == b"shared-data" for r in results)


# ---------------------------------------------------------------------------
# Multipart
# ---------------------------------------------------------------------------

class TestAsyncMultipart:
    @pytest.mark.asyncio
    async def test_put_multipart(self, acluster):
        data = make_payload(50000)
        await acluster.put_multipart("mp_key", data)
        result = await acluster.get("mp_key")
        assert result == data


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestAsyncContextManager:
    @pytest.mark.asyncio
    async def test_async_with(self, tmp_path):
        shards = [
            (str(tmp_path / "s0.raw"), SHARD_SIZE),
            (str(tmp_path / "s1.raw"), SHARD_SIZE),
        ]
        async with await async_format_and_open_cluster(
            shards, replication_factor=1
        ) as store:
            await store.put("ctx", b"test-ctx")
            assert await store.get("ctx") == b"test-ctx"


# ---------------------------------------------------------------------------
# Sync accessors
# ---------------------------------------------------------------------------

class TestAsyncSyncAccessors:
    @pytest.mark.asyncio
    async def test_shard_health(self, acluster):
        for i in range(3):
            h = acluster.shard_health(i)
            assert h is not None

    @pytest.mark.asyncio
    async def test_read_preference(self, acluster):
        pref = acluster.read_preference()
        assert isinstance(pref, str)

    @pytest.mark.asyncio
    async def test_find_under_replicated_empty(self, acluster):
        # Fresh cluster with rf=2 and 3 shards: nothing should be under-replicated
        await acluster.put("t", b"data")
        under = acluster.find_under_replicated()
        assert under == []
