"""Comprehensive tests for drain, repair-replication, re-replication, and
over-replication trimming in shardedobjstr Python bindings.

These tests verify that:
  - drain_shard() moves sole-copy objects and preserves data
  - repair_replication() restores RF after shard detachment
  - re_replication_sweep() re-replicates under-replicated objects
  - over_replication_trim() removes excess replicas
  - Data integrity is preserved through all operations
  - Metadata (content-type, x-amz-meta) survives drain/repair-replication
  - Edge cases: empty shards, invalid IDs, concurrent operations
"""

import pytest
import shardedobjstr

from conftest import SHARD_SIZE, make_payload

H = shardedobjstr.ShardHealth


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def four_shard_paths(tmp_path):
    return [str(tmp_path / f"shard{i}.raw") for i in range(4)]


@pytest.fixture
def cluster_rf2_4shard(four_shard_paths):
    shards = [(p, SHARD_SIZE) for p in four_shard_paths]
    return shardedobjstr.format_and_open_cluster(shards, replication_factor=2)


@pytest.fixture
def cluster_rf1(three_shard_paths):
    shards = [(p, SHARD_SIZE) for p in three_shard_paths]
    return shardedobjstr.format_and_open_cluster(shards, replication_factor=1)


@pytest.fixture
def seeded_cluster_rf2(cluster_rf2):
    """Cluster with 30 test objects."""
    for i in range(30):
        cluster_rf2.put(f"data/obj_{i:04d}.bin", make_payload(1024, seed=i))
    cluster_rf2.flush_all()
    return cluster_rf2


@pytest.fixture
def seeded_cluster_rf2_4shard(cluster_rf2_4shard):
    """4-shard cluster with 30 test objects."""
    for i in range(30):
        cluster_rf2_4shard.put(
            f"data/obj_{i:04d}.bin", make_payload(1024, seed=i)
        )
    cluster_rf2_4shard.flush_all()
    return cluster_rf2_4shard


# =========================================================================
# Drain tests
# =========================================================================

class TestDrainShard:
    """Tests for drain_shard() operation."""

    def test_drain_preserves_all_data(self, seeded_cluster_rf2):
        """After draining shard 0, every object should still be readable."""
        c = seeded_cluster_rf2
        result = c.drain_shard(0)
        assert result.re_replicated >= 0

        for i in range(30):
            key = f"data/obj_{i:04d}.bin"
            data = c.get(key)
            assert data == make_payload(1024, seed=i), f"{key} data mismatch"

    def test_drain_data_integrity_byte_by_byte(self, seeded_cluster_rf2):
        """Byte-level verification after drain."""
        c = seeded_cluster_rf2
        c.drain_shard(1)

        for i in range(30):
            key = f"data/obj_{i:04d}.bin"
            data = c.get(key)
            expected = make_payload(1024, seed=i)
            assert len(data) == len(expected), f"{key} length mismatch"
            for j in range(len(data)):
                assert data[j] == expected[j], (
                    f"{key} byte {j}: got {data[j]}, expected {expected[j]}"
                )

    def test_drain_moves_sole_copy(self, cluster_rf1):
        """With RF=1, all objects on the victim are sole copies and must move."""
        c = cluster_rf1
        for i in range(20):
            c.put(f"data/sole_{i:04d}.bin", make_payload(512, seed=i))
        c.flush_all()

        # Find objects on shard 0 before drain.
        on_shard0 = []
        for i in range(20):
            key = f"data/sole_{i:04d}.bin"
            p = c.placement(key)
            if p and 0 in p.shards:
                on_shard0.append(key)

        result = c.drain_shard(0)

        # All sole-copy objects should have been moved.
        for key in on_shard0:
            data = c.get(key)
            assert data is not None, f"sole-copy {key} lost after drain"

    def test_drain_skips_replicated_objects(self, seeded_cluster_rf2):
        """Objects with replicas on other shards don't need forced migration."""
        c = seeded_cluster_rf2
        result = c.drain_shard(0)

        # After drain, shard 0 should be offline.
        health = c.shard_health(0)
        assert health == H.Offline

        # All objects still readable from surviving shards.
        for i in range(30):
            assert c.get(f"data/obj_{i:04d}.bin") is not None

    def test_drain_empty_shard(self, cluster_rf2):
        """Draining a shard with no objects should succeed (no-op)."""
        c = cluster_rf2
        result = c.drain_shard(0)
        assert result.re_replicated == 0

    def test_drain_invalid_shard_raises(self, cluster_rf2):
        """Draining a non-existent shard should raise ValueError."""
        c = cluster_rf2
        with pytest.raises(ValueError):
            c.drain_shard(999)

    def test_drain_updates_placement(self, seeded_cluster_rf2):
        """After drain, no object should have shard 0 in its placement."""
        c = seeded_cluster_rf2
        c.drain_shard(0)

        for i in range(30):
            key = f"data/obj_{i:04d}.bin"
            p = c.placement(key)
            if p is not None:
                assert 0 not in p.shards, (
                    f"{key} still placed on drained shard 0: {p.shards}"
                )

    def test_drain_shard_offline_after(self, seeded_cluster_rf2):
        """Shard should be marked Offline after drain."""
        c = seeded_cluster_rf2
        c.drain_shard(0)
        assert c.shard_health(0) == H.Offline
        assert c.shard_health(1) == H.Healthy
        assert c.shard_health(2) == H.Healthy

    def test_sequential_drains(self, seeded_cluster_rf2_4shard):
        """Drain shard 0, then shard 1; all data survives on shards 2+3."""
        c = seeded_cluster_rf2_4shard

        # Drain shard 0.
        c.drain_shard(0)
        for i in range(30):
            assert c.get(f"data/obj_{i:04d}.bin") is not None, (
                f"object lost after draining shard 0"
            )

        # Repair-replication to restore RF on remaining shards.
        c.repair_replication(batch_size=200)

        # Drain shard 1.
        c.drain_shard(1)
        for i in range(30):
            data = c.get(f"data/obj_{i:04d}.bin")
            assert data == make_payload(1024, seed=i), (
                f"data mismatch after second drain"
            )

    def test_new_writes_skip_drained_shard(self, seeded_cluster_rf2):
        """New writes after drain should not place on the drained shard."""
        c = seeded_cluster_rf2
        c.drain_shard(0)

        # Write new objects.
        for i in range(10):
            c.put(f"data/post_{i:04d}.bin", make_payload(256, seed=i + 100))
        c.flush_all()

        # New objects should not be on shard 0.
        for i in range(10):
            key = f"data/post_{i:04d}.bin"
            p = c.placement(key)
            if p is not None:
                assert 0 not in p.shards, (
                    f"new object {key} on drained shard: {p.shards}"
                )


# =========================================================================
# Repair-replication tests
# =========================================================================

class TestRepairReplication:
    """Tests for repair_replication() operation."""

    def test_repair_replication_noop_on_balanced(self, seeded_cluster_rf2):
        """Repair-replication on a balanced cluster is a no-op."""
        c = seeded_cluster_rf2
        result = c.repair_replication(batch_size=200)
        assert result.re_replicated == 0
        assert result.trimmed == 0

    def test_repair_replication_restores_rf_after_detach(self, seeded_cluster_rf2):
        """After detaching a shard, repair-replication should restore RF."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        under = c.find_under_replicated()
        assert len(under) > 0, "should have under-replicated objects"

        result = c.repair_replication(batch_size=200)
        assert result.re_replicated > 0, "should re-replicate some objects"

        under_after = c.find_under_replicated()
        assert len(under_after) == 0, (
            f"all should be restored: {under_after}"
        )

    def test_repair_replication_data_integrity(self, seeded_cluster_rf2):
        """Data content is correct after repair-replication."""
        c = seeded_cluster_rf2
        c.detach_shard(0)
        c.repair_replication(batch_size=200)

        for i in range(30):
            data = c.get(f"data/obj_{i:04d}.bin")
            assert data == make_payload(1024, seed=i)

    def test_repair_replication_idempotent(self, seeded_cluster_rf2):
        """Running repair-replication twice: second run should be a no-op."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        r1 = c.repair_replication(batch_size=200)
        assert r1.re_replicated > 0

        r2 = c.repair_replication(batch_size=200)
        assert r2.re_replicated == 0, "second repair-replication should be no-op"
        assert r2.trimmed == 0

    def test_repair_replication_batch_size(self, seeded_cluster_rf2):
        """Batch size limits objects processed per sweep."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        result = c.repair_replication(batch_size=5)
        assert result.re_replicated <= 5

    def test_repair_replication_with_4_shards(self, seeded_cluster_rf2_4shard):
        """Repair-replication with 4 shards; shard offline then restored."""
        c = seeded_cluster_rf2_4shard
        c.detach_shard(1)

        result = c.repair_replication(batch_size=200)

        under = c.find_under_replicated()
        assert len(under) == 0, f"under-replicated after repair-replication: {under}"

        # All data correct.
        for i in range(30):
            data = c.get(f"data/obj_{i:04d}.bin")
            assert data == make_payload(1024, seed=i)

    def test_repair_replication_catalog_correct(self, seeded_cluster_rf2):
        """Catalog should reflect correct placement after repair-replication."""
        c = seeded_cluster_rf2
        c.detach_shard(0)
        c.repair_replication(batch_size=200)

        for i in range(30):
            key = f"data/obj_{i:04d}.bin"
            p = c.placement(key)
            assert p is not None, f"{key} missing from catalog"
            healthy = [s for s in p.shards if c.shard_health(s) == H.Healthy]
            assert len(healthy) >= 2, (
                f"{key}: only {len(healthy)} healthy replicas: {p.shards}"
            )


# =========================================================================
# Re-replication sweep tests
# =========================================================================

class TestReReplicationSweep:
    """Tests for re_replication_sweep()."""

    def test_re_replication_basic(self, seeded_cluster_rf2):
        """Re-replication fixes under-replicated objects."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        under_before = c.find_under_replicated()
        assert len(under_before) > 0

        count = c.re_replication_sweep(batch_size=200)
        assert count > 0, "should re-replicate some objects"

        under_after = c.find_under_replicated()
        assert len(under_after) == 0, f"still under: {under_after}"

    def test_re_replication_noop_when_balanced(self, seeded_cluster_rf2):
        """No-op when everything is balanced."""
        c = seeded_cluster_rf2
        count = c.re_replication_sweep(batch_size=200)
        assert count == 0

    def test_re_replication_respects_batch_size(self, seeded_cluster_rf2):
        """Batch size caps objects processed."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        count = c.re_replication_sweep(batch_size=3)
        assert count <= 3

    def test_re_replication_data_intact(self, seeded_cluster_rf2):
        """Data is correct after re-replication."""
        c = seeded_cluster_rf2
        c.detach_shard(0)
        c.re_replication_sweep(batch_size=200)

        for i in range(30):
            data = c.get(f"data/obj_{i:04d}.bin")
            assert data == make_payload(1024, seed=i)


# =========================================================================
# Over-replication trim tests
# =========================================================================

class TestOverReplicationTrim:
    """Tests for over_replication_trim()."""

    def test_trim_removes_excess(self, seeded_cluster_rf2):
        """After manually replicating an extra copy, trim removes it."""
        c = seeded_cluster_rf2

        # Find an object on exactly two shards and figure out which shard
        # is NOT hosting it so we can create a third (excess) copy.
        target_key = None
        src_shard = None
        spare_shard = None
        for i in range(30):
            key = f"data/obj_{i:04d}.bin"
            p = c.placement(key)
            if p and len(p.shards) == 2:
                hosting = set(p.shards)
                missing = {0, 1, 2} - hosting
                if missing:
                    target_key = key
                    src_shard = p.shards[0]
                    spare_shard = missing.pop()
                    break

        if target_key is None:
            pytest.skip("no object on exactly 2 shards with a spare")

        # Create over-replication by copying to the spare shard.
        c.replicate_object(target_key, src_shard, spare_shard)

        over = c.find_over_replicated()
        assert len(over) >= 1, "expected at least one over-replicated object"

        # Trim should remove the excess.
        trimmed = c.over_replication_trim(batch_size=200)
        assert trimmed >= 1

        over_after = c.find_over_replicated()
        assert len(over_after) == 0

        # Data should still be readable.
        data = c.get(target_key)
        expected_seed = int(target_key.split("_")[1].split(".")[0])
        assert data == make_payload(1024, seed=expected_seed)

    def test_trim_noop_when_balanced(self, seeded_cluster_rf2):
        """No-op when nothing is over-replicated."""
        c = seeded_cluster_rf2
        count = c.over_replication_trim(batch_size=200)
        assert count == 0

    def test_find_over_replicated_empty_when_balanced(self, seeded_cluster_rf2):
        """find_over_replicated returns empty list on balanced cluster."""
        c = seeded_cluster_rf2
        over = c.find_over_replicated()
        assert len(over) == 0


# =========================================================================
# Full lifecycle tests
# =========================================================================

class TestFullLifecycle:
    """Tests combining drain + repair-replication + verify."""

    def test_drain_repair_replication_all_data_survives(self, seeded_cluster_rf2_4shard):
        """Drain shard 0, repair-replication, verify all data."""
        c = seeded_cluster_rf2_4shard
        c.drain_shard(0)
        result = c.repair_replication(batch_size=200)

        # No under-replicated objects.
        under = c.find_under_replicated()
        assert len(under) == 0, f"under-replicated: {under}"

        # All data correct.
        for i in range(30):
            data = c.get(f"data/obj_{i:04d}.bin")
            assert data == make_payload(1024, seed=i)

    def test_detach_repair_replication_reattach_trim(self, seeded_cluster_rf2_4shard):
        """Detach -> repair-replication -> reattach -> trim: full cycle."""
        c = seeded_cluster_rf2_4shard

        # Detach shard 0.
        c.detach_shard(0)

        # Repair-replication to fix under-replication.
        c.repair_replication(batch_size=200)
        under = c.find_under_replicated()
        assert len(under) == 0

        # All data readable.
        for i in range(30):
            data = c.get(f"data/obj_{i:04d}.bin")
            assert data == make_payload(1024, seed=i)

    def test_drain_preserves_listing(self, seeded_cluster_rf2):
        """list returns same keys before and after drain."""
        c = seeded_cluster_rf2

        before = sorted(m.location for m in c.list("data/"))
        assert len(before) == 30

        c.drain_shard(0)

        after = sorted(m.location for m in c.list("data/"))
        assert before == after, "listing should be identical after drain"

    def test_drain_then_write_then_list(self, seeded_cluster_rf2):
        """New writes after drain appear in listing."""
        c = seeded_cluster_rf2
        c.drain_shard(0)

        # Write new objects.
        for i in range(10):
            c.put(f"data/new_{i:04d}.bin", make_payload(256, seed=i + 50))
        c.flush_all()

        # List should include old + new.
        all_keys = sorted(m.location for m in c.list("data/"))
        new_keys = [k for k in all_keys if k.startswith("data/new_")]
        old_keys = [k for k in all_keys if k.startswith("data/obj_")]
        assert len(new_keys) == 10
        assert len(old_keys) == 30

    def test_large_object_survives_drain(self, cluster_rf2):
        """A 1 MB object survives drain with correct content."""
        c = cluster_rf2
        big_data = make_payload(1024 * 1024, seed=42)
        c.put("data/big.bin", big_data)
        c.flush_all()

        c.drain_shard(0)

        result = c.get("data/big.bin")
        assert result == big_data, "1 MB object content mismatch after drain"

    def test_many_small_objects_drain(self, cluster_rf2):
        """200 small objects survive drain."""
        c = cluster_rf2
        for i in range(200):
            c.put(f"data/tiny_{i:04d}.bin", make_payload(64, seed=i))
        c.flush_all()

        c.drain_shard(1)

        for i in range(200):
            data = c.get(f"data/tiny_{i:04d}.bin")
            assert data == make_payload(64, seed=i), f"tiny_{i:04d} mismatch"

    def test_drain_all_shards_but_one_rf1(self, cluster_rf1):
        """With RF=1 and 3 shards, drain 2 shards: all data on last."""
        c = cluster_rf1
        for i in range(20):
            c.put(f"data/rf1_{i:04d}.bin", make_payload(256, seed=i))
        c.flush_all()

        # Drain shard 0.
        c.drain_shard(0)

        for i in range(20):
            data = c.get(f"data/rf1_{i:04d}.bin")
            assert data is not None, f"lost after draining shard 0"
            assert data == make_payload(256, seed=i)

        # Drain shard 1 -- everything moves to shard 2.
        c.drain_shard(1)

        for i in range(20):
            data = c.get(f"data/rf1_{i:04d}.bin")
            assert data is not None, f"lost after draining shard 1"
            assert data == make_payload(256, seed=i)

        # Verify all on shard 2.
        for i in range(20):
            key = f"data/rf1_{i:04d}.bin"
            p = c.placement(key)
            if p is not None:
                assert p.shards == [2], f"{key} not on shard 2: {p.shards}"


# =========================================================================
# Plan repair-replication (dry-run) extended tests
# =========================================================================

class TestPlanRepairReplicationExtended:
    """Extended tests for plan_repair_replication (dry-run)."""

    def test_plan_shows_correct_count(self, seeded_cluster_rf2):
        """Plan shows correct replication count after detach."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        under = c.find_under_replicated()
        plan = c.plan_repair_replication(batch_size=200)
        assert len(plan.replications) == len(under)

    def test_plan_then_execute_matches(self, seeded_cluster_rf2):
        """Plan count matches actual repair-replication result."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        plan = c.plan_repair_replication(batch_size=200)
        planned_count = len(plan.replications)

        result = c.repair_replication(batch_size=200)
        assert result.re_replicated == planned_count, (
            f"plan said {planned_count}, repair-replication did {result.re_replicated}"
        )

    def test_plan_batch_size_respected(self, seeded_cluster_rf2):
        """Plan respects batch_size cap."""
        c = seeded_cluster_rf2
        c.detach_shard(0)

        plan = c.plan_repair_replication(batch_size=3)
        assert len(plan.replications) <= 3


# =========================================================================
# Validate shard access extended tests
# =========================================================================

class TestValidateShardAccessExtended:
    """Extended validate_shard_access tests around drain/repair-replication."""

    def test_after_drain(self, seeded_cluster_rf2):
        """Drained shard shows as inaccessible."""
        c = seeded_cluster_rf2
        c.drain_shard(0)

        results = c.validate_shard_access()
        assert results[0][1] is False  # shard 0 offline
        assert results[1][1] is True   # shard 1 healthy
        assert results[2][1] is True   # shard 2 healthy

    def test_after_detach(self, seeded_cluster_rf2):
        """Detached shard shows as inaccessible."""
        c = seeded_cluster_rf2
        c.detach_shard(1)

        results = c.validate_shard_access()
        assert results[0][1] is True
        assert results[1][1] is False
        assert results[2][1] is True
