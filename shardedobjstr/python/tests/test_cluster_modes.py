"""Tests for cluster access modes: read-only, read preference, min_writes."""

import pytest
import shardedobjstr

from conftest import SHARD_SIZE


# -- Read preference --------------------------------------------------

class TestReadPreference:
    def test_default_is_round_robin(self, cluster_rf2):
        pref = cluster_rf2.read_preference()
        assert pref == "round-robin"

    def test_set_ordered(self, cluster_rf2):
        cluster_rf2.set_read_preference("ordered")
        assert cluster_rf2.read_preference() == "ordered"

    def test_set_round_robin(self, cluster_rf2):
        cluster_rf2.set_read_preference("ordered")
        cluster_rf2.set_read_preference("round-robin")
        assert cluster_rf2.read_preference() == "round-robin"

    def test_reads_work_in_both_modes(self, cluster_rf2):
        cluster_rf2.put("pref/obj.bin", b"test data")
        cluster_rf2.flush_all()

        cluster_rf2.set_read_preference("ordered")
        assert cluster_rf2.get("pref/obj.bin") == b"test data"

        cluster_rf2.set_read_preference("round-robin")
        assert cluster_rf2.get("pref/obj.bin") == b"test data"

    def test_invalid_preference_raises(self, cluster_rf2):
        with pytest.raises(Exception):
            cluster_rf2.set_read_preference("invalid-mode")


# -- min_writes and delete_requires_min_writes -------------------------

class TestMinWrites:
    def test_default_min_writes(self, cluster_rf2):
        # Default min_writes is 1 (best-effort; writes succeed if at least one shard accepts)
        assert cluster_rf2.min_writes() == 1

    def test_custom_min_writes(self, three_shard_paths):
        shards = [(p, SHARD_SIZE) for p in three_shard_paths]
        cluster = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=2, min_writes=1
        )
        assert cluster.min_writes() == 1

    def test_default_delete_requires_min_writes(self, cluster_rf2):
        assert cluster_rf2.delete_requires_min_writes() is False

    def test_delete_requires_min_writes_enabled(self, three_shard_paths):
        shards = [(p, SHARD_SIZE) for p in three_shard_paths]
        cluster = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=2, delete_requires_min_writes=True
        )
        assert cluster.delete_requires_min_writes() is True
