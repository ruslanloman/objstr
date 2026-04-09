"""Shared fixtures for shardedobjstr tests."""

import pytest

import shardedobjstr

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHARD_SIZE = 64 * 1024 * 1024  # 64 MB per shard


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_payload(size, seed=0):
    """Deterministic payload: byte[i] = (i + seed) % 251."""
    return bytes((i + seed) % 251 for i in range(size))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def session_build_info():
    """Capture build info at session start and verify it is unchanged at the end."""
    build_info = shardedobjstr.__build_info__
    print(f"\n[shardedobjstr] build: {build_info}")
    yield build_info
    current = shardedobjstr.__build_info__
    assert current == build_info, (
        f"shardedobjstr build info changed during test session!\n"
        f"  start: {build_info}\n"
        f"  end:   {current}"
    )


@pytest.fixture
def cluster_dir(tmp_path):
    """Return a temporary directory for shard images."""
    return tmp_path


@pytest.fixture
def three_shard_paths(tmp_path):
    """Return paths for 3 shard images."""
    return [str(tmp_path / f"shard{i}.raw") for i in range(3)]


@pytest.fixture
def cluster_rf2(three_shard_paths):
    """Format 3 shards and open as a cluster with replication_factor=2."""
    shards = [(p, SHARD_SIZE) for p in three_shard_paths]
    return shardedobjstr.format_and_open_cluster(
        shards, replication_factor=2
    )
