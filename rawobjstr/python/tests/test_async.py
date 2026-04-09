"""Tests for the async API (rawobjstr.aio).

Exercises AsyncStore, async_format, async_open, and AsyncMultipartUpload
to validate that the executor-based async wrappers correctly offload
blocking Rust calls.
"""

import asyncio

import pytest
import pytest_asyncio

import rawobjstr
from rawobjstr.aio import AsyncStore, async_format, async_open

from conftest import SMALL_DEVICE


@pytest_asyncio.fixture
async def astore(tmp_path):
    path = str(tmp_path / "async_store.raw")
    store = await async_format(path, size=SMALL_DEVICE)
    yield store
    await store.flush_index()


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------

class TestAsyncBasicOps:
    @pytest.mark.asyncio
    async def test_put_get(self, astore):
        await astore.put("hello.txt", b"Hello, async!")
        data = await astore.get("hello.txt")
        assert data == b"Hello, async!"

    @pytest.mark.asyncio
    async def test_put_overwrite(self, astore):
        await astore.put("key", b"v1")
        await astore.put("key", b"v2")
        assert await astore.get("key") == b"v2"

    @pytest.mark.asyncio
    async def test_put_create_mode(self, astore):
        await astore.put("unique", b"data", mode="create")
        with pytest.raises(FileExistsError):
            await astore.put("unique", b"other", mode="create")

    @pytest.mark.asyncio
    async def test_head(self, astore):
        await astore.put("meta.txt", b"abc")
        meta = await astore.head("meta.txt")
        assert meta.location == "meta.txt"
        assert meta.size == 3

    @pytest.mark.asyncio
    async def test_delete(self, astore):
        await astore.put("del.txt", b"gone")
        await astore.delete("del.txt")
        with pytest.raises(FileNotFoundError):
            await astore.get("del.txt")

    @pytest.mark.asyncio
    async def test_get_not_found(self, astore):
        with pytest.raises(FileNotFoundError):
            await astore.get("no-such-key")

    @pytest.mark.asyncio
    async def test_get_range(self, astore):
        await astore.put("ranged", b"0123456789")
        data = await astore.get("ranged", range=(2, 5))
        assert data == b"234"


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------

class TestAsyncListing:
    @pytest.mark.asyncio
    async def test_list_empty(self, astore):
        items = await astore.list()
        assert items == []

    @pytest.mark.asyncio
    async def test_list_with_prefix(self, astore):
        await astore.put("a/1", b"x")
        await astore.put("a/2", b"y")
        await astore.put("b/1", b"z")
        items = await astore.list("a")
        assert len(items) == 2

    @pytest.mark.asyncio
    async def test_list_with_delimiter(self, astore):
        await astore.put("dir/a.txt", b"a")
        await astore.put("dir/b.txt", b"b")
        await astore.put("other.txt", b"c")
        result = await astore.list_with_delimiter(None)
        # Should have common_prefixes for "dir/"
        assert any("dir" in p for p in result.common_prefixes)


# ---------------------------------------------------------------------------
# Copy & Rename
# ---------------------------------------------------------------------------

class TestAsyncCopyRename:
    @pytest.mark.asyncio
    async def test_copy(self, astore):
        await astore.put("src", b"data")
        await astore.copy("src", "dst")
        assert await astore.get("dst") == b"data"

    @pytest.mark.asyncio
    async def test_copy_if_not_exists(self, astore):
        await astore.put("a", b"1")
        await astore.copy_if_not_exists("a", "b")
        assert await astore.get("b") == b"1"
        with pytest.raises(FileExistsError):
            await astore.copy_if_not_exists("a", "b")

    @pytest.mark.asyncio
    async def test_rename(self, astore):
        await astore.put("old", b"val")
        await astore.rename("old", "new")
        assert await astore.get("new") == b"val"
        with pytest.raises(FileNotFoundError):
            await astore.get("old")


# ---------------------------------------------------------------------------
# Multipart
# ---------------------------------------------------------------------------

class TestAsyncMultipart:
    @pytest.mark.asyncio
    async def test_multipart_upload(self, astore):
        async with await astore.multipart("mp_key") as upload:
            await upload.put_part(b"part1")
            await upload.put_part(b"part2")
        data = await astore.get("mp_key")
        assert data == b"part1part2"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

class TestAsyncConcurrency:
    @pytest.mark.asyncio
    async def test_parallel_puts(self, astore):
        """Multiple puts in parallel should all succeed."""
        payloads = {f"key-{i}": bytes([i % 256]) * 1000 for i in range(20)}
        await asyncio.gather(
            *(astore.put(k, v) for k, v in payloads.items())
        )
        results = await asyncio.gather(
            *(astore.get(k) for k in payloads)
        )
        for k, data in zip(payloads, results):
            assert data == payloads[k]

    @pytest.mark.asyncio
    async def test_parallel_gets(self, astore):
        """Multiple concurrent reads of the same key."""
        await astore.put("shared", b"shared-data")
        results = await asyncio.gather(
            *(astore.get("shared") for _ in range(10))
        )
        assert all(r == b"shared-data" for r in results)


# ---------------------------------------------------------------------------
# Misc: sync accessors, context manager, metadata
# ---------------------------------------------------------------------------

class TestAsyncMisc:
    @pytest.mark.asyncio
    async def test_sync_accessors(self, astore):
        assert not astore.is_read_only()
        info = astore.device_info()
        assert info.device_size == SMALL_DEVICE

    @pytest.mark.asyncio
    async def test_context_manager(self, tmp_path):
        path = str(tmp_path / "ctx.raw")
        async with await async_format(path, size=SMALL_DEVICE) as store:
            await store.put("ctx", b"data")
        # Re-open and verify data was flushed
        store2 = await async_open(path, readonly=True)
        assert await store2.get("ctx") == b"data"

    @pytest.mark.asyncio
    async def test_repr(self, astore):
        r = repr(astore)
        assert "AsyncStore" in r

    @pytest.mark.asyncio
    async def test_sync_property(self, astore):
        """The .sync property gives the underlying Store."""
        assert isinstance(astore.sync, rawobjstr.Store)

    @pytest.mark.asyncio
    async def test_async_open(self, tmp_path):
        path = str(tmp_path / "reopen.raw")
        s = await async_format(path, size=SMALL_DEVICE)
        await s.put("x", b"123")
        await s.flush_index()

        s2 = await async_open(path, readonly=True)
        assert await s2.get("x") == b"123"

    @pytest.mark.asyncio
    async def test_metadata(self, astore):
        await astore.put("m", b"body")
        await astore.update_metadata("m", b"meta-bytes")
        meta = await astore.get_metadata("m")
        assert meta == b"meta-bytes"

    @pytest.mark.asyncio
    async def test_getraw(self, astore):
        await astore.put("raw", b"hello")
        result = await astore.getraw("raw")
        assert "data" in result
        assert "compression" in result
