"""rawobjstr -- Python bindings for rawobjstr.

Bypass the filesystem for 2-5x faster I/O on raw block devices and loopback files.

Quick start::

    import rawobjstr

    # Create a new store (1 GB image file)
    store = rawobjstr.format("/tmp/store.raw", size=1_073_741_824)

    # Write and read
    store.put("hello.txt", b"Hello, world!")
    data = store.get("hello.txt")

    # Persist index to disk
    store.flush_index()

    # Reopen later
    store = rawobjstr.open("/tmp/store.raw")
"""

from rawobjstr._rawobjstr import (
    Store,
    ObjectMeta,
    ListResult,
    PyDeviceInfo as DeviceInfo,
    PyTombstoneEntry as TombstoneEntry,
    PyScrubReport as ScrubReport,
    PyVerifyReport as VerifyReport,
    PyRepairReport as RepairReport,
    PyImportReport as ImportReport,
    PyExportReport as ExportReport,
    PyObjectFullInfo as ObjectFullInfo,
    PyMultipartUpload as MultipartUpload,
    format,
    open,
    modify_flags,
    FLAG_DIRECT_IO,
    FLAG_WRITE_PROTECT,
    __version__,
    __build_info__,
)

__all__ = [
    "Store",
    "ObjectMeta",
    "ListResult",
    "DeviceInfo",
    "TombstoneEntry",
    "ScrubReport",
    "VerifyReport",
    "RepairReport",
    "ImportReport",
    "ExportReport",
    "ObjectFullInfo",
    "MultipartUpload",
    "format",
    "open",
    "modify_flags",
    "FLAG_DIRECT_IO",
    "FLAG_WRITE_PROTECT",
    "__version__",
    "__build_info__",
]

# fsspec integration (optional -- available when fsspec is installed)
try:
    from rawobjstr.fsspec_impl import RawObjStFileSystem
    __all__.append("RawObjStFileSystem")
except ImportError:
    pass

# Async API: ``from rawobjstr.aio import AsyncStore, async_format, async_open``
# Wraps every blocking call in loop.run_in_executor() so the asyncio
# event loop is never blocked.  See ``rawobjstr.aio`` for details.
