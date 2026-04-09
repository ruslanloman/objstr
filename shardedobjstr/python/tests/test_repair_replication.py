"""Tests for repair-replication planning and shard access validation."""

import pytest
import shardedobjstr

from conftest import SHARD_SIZE, make_payload


@pytest.fixture
def cluster_rf2_seeded(cluster_rf2):
    """Cluster with 10 test objects."""
    for i in range(10):
        cluster_rf2.put(f"data/obj_{i:04d}.bin", make_payload(1024, seed=i))
    cluster_rf2.flush_all()
    return cluster_rf2


# -- Plan repair-replication (dry-run) ----------------------------------------

class TestPlanRepairReplication:
    def test_empty_cluster(self, cluster_rf2):
        plan = cluster_rf2.plan_repair_replication()
        assert len(plan.replications) == 0
        assert len(plan.trims) == 0
        assert plan.unrepairable == 0
        assert plan.untrimmable == 0

    def test_under_replicated_after_detach(self, cluster_rf2_seeded):
        cluster_rf2_seeded.detach_shard(0)
        under = cluster_rf2_seeded.find_under_replicated()
        plan = cluster_rf2_seeded.plan_repair_replication(batch_size=100)
        assert len(plan.replications) == len(under)
        for action in plan.replications:
            assert action.action_type == "replicate"
            assert action.source_shard is not None
            assert action.target_shard is not None

    def test_batch_size_cap(self, cluster_rf2_seeded):
        cluster_rf2_seeded.detach_shard(0)
        plan = cluster_rf2_seeded.plan_repair_replication(batch_size=2)
        assert len(plan.replications) <= 2

    def test_plan_repr(self, cluster_rf2_seeded):
        plan = cluster_rf2_seeded.plan_repair_replication()
        s = repr(plan)
        assert "replications" in s


# -- Validate shard access -------------------------------------------

class TestValidateShardAccess:
    def test_all_healthy(self, cluster_rf2):
        results = cluster_rf2.validate_shard_access()
        assert len(results) == 3
        for shard_id, accessible in results:
            assert accessible is True

    def test_with_offline_shard(self, cluster_rf2):
        cluster_rf2.detach_shard(1)
        results = cluster_rf2.validate_shard_access()
        assert len(results) == 3
        assert results[0][1] is True   # shard 0
        assert results[1][1] is False  # shard 1 offline
        assert results[2][1] is True   # shard 2
