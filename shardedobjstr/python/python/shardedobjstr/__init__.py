"""shardedobjstr -- Python bindings for shardedobjstr.

Distribute objects across multiple raw block device shards with replication.

Quick start::

    import shardedobjstr

    # Format two 1 GB shard images and open as a cluster (replication=2)
    store = shardedobjstr.format_and_open_cluster(
        [("/tmp/shard0.raw", 1_073_741_824), ("/tmp/shard1.raw", 1_073_741_824)],
        replication_factor=2,
    )

    # Write and read (same API as rawobjstr.Store)
    store.put("hello.txt", b"Hello, cluster!")
    data = store.get("hello.txt")

    # Inspect placement
    info = store.placement("hello.txt")
    print(info.shards)   # e.g. [0, 1]

    # Open existing shards later
    store = shardedobjstr.open_cluster(
        ["/tmp/shard0.raw", "/tmp/shard1.raw"],
        replication_factor=2,
    )
"""

from shardedobjstr._shardedobjstr import (
    ClusterStore,
    ObjectMeta,
    ListResult,
    PlacementInfo,
    ShardHealth,
    InvalidateReport,
    RepairReplicationResult,
    VerifyReplicaResult,
    VerifyObjectReport,
    VerifyReport,
    PlannedAction,
    RepairReplicationPlan,
    CrossVerifyShardDigest,
    CrossVerifyObjectReport,
    CrossVerifyReport,
    RedistributeResult,
    format_shard,
    format_shard_with_options,
    open_cluster,
    open_cluster_degraded,
    format_and_open_cluster,
    open_fs_cluster,
    load_config,
    check_config,
    open_cluster_from_config,
    __version__,
    __build_info__,
)

__all__ = [
    "ClusterStore",
    "ObjectMeta",
    "ListResult",
    "PlacementInfo",
    "ShardHealth",
    "InvalidateReport",
    "RepairReplicationResult",
    "VerifyReplicaResult",
    "VerifyObjectReport",
    "VerifyReport",
    "PlannedAction",
    "RepairReplicationPlan",
    "CrossVerifyShardDigest",
    "CrossVerifyObjectReport",
    "CrossVerifyReport",
    "RedistributeResult",
    "format_shard",
    "format_shard_with_options",
    "open_cluster",
    "open_cluster_degraded",
    "format_and_open_cluster",
    "open_fs_cluster",
    "load_config",
    "check_config",
    "open_cluster_from_config",
    "__version__",
    "__build_info__",
]

# fsspec integration (optional -- available when fsspec is installed)
try:
    from shardedobjstr.fsspec_impl import ShardedFileSystem
    __all__.append("ShardedFileSystem")
except ImportError:
    pass

# Async API: ``from shardedobjstr.aio import AsyncClusterStore, async_format_and_open_cluster``
# Wraps every blocking call in loop.run_in_executor() so the asyncio
# event loop is never blocked.  See ``shardedobjstr.aio`` for details.
