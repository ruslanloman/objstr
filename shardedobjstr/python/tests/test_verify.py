"""Tests for replica verification and cross-verification (MD5-based)."""

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


# -- Verify replicas -------------------------------------------------

class TestVerifyReplicas:
    def test_verify_object_consistent(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.verify_object("data/obj_0000.bin")
        assert report.replicas_consistent is True
        assert len(report.replicas) == 2
        for r in report.replicas:
            assert r.crc32c is not None
            assert r.error is None
            assert r.matches_catalog is True

    def test_verify_object_not_found(self, cluster_rf2):
        with pytest.raises(Exception):
            cluster_rf2.verify_object("nonexistent")

    def test_verify_all_consistent(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.verify_all()
        assert report.objects_checked == 10
        assert report.objects_ok == 10
        assert report.objects_mismatched == 0
        assert report.objects_with_errors == 0
        assert len(report.details) == 0

    def test_verify_all_prefix_filter(self, cluster_rf2):
        cluster_rf2.put("alpha/a.bin", b"aaa")
        cluster_rf2.put("beta/b.bin", b"bbb")
        cluster_rf2.flush_all()
        report = cluster_rf2.verify_all(prefix="alpha/")
        assert report.objects_checked == 1
        assert report.objects_ok == 1

    def test_verify_all_with_offline_shard(self, cluster_rf2_seeded):
        cluster_rf2_seeded.detach_shard(0)
        report = cluster_rf2_seeded.verify_all()
        assert report.objects_checked == 10
        # Offline replicas may cause errors but not mismatches.
        assert report.objects_mismatched == 0

    def test_verify_report_repr(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.verify_all()
        s = repr(report)
        assert "checked=" in s


# -- Cross-verify (MD5-based) ----------------------------------------

class TestCrossVerify:
    def test_cross_verify_object_consistent(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.cross_verify_object("data/obj_0000.bin")
        assert report.consistent is True
        assert len(report.shards) == 2
        # All digests should have the same MD5
        md5s = [d.md5_hex for d in report.shards]
        assert md5s[0] == md5s[1]
        for d in report.shards:
            assert d.size == 1024
            assert len(d.md5_hex) == 32
            assert d.last_modified  # non-empty

    def test_cross_verify_object_not_found(self, cluster_rf2):
        with pytest.raises(Exception):
            cluster_rf2.cross_verify_object("nonexistent")

    def test_cross_verify_all_consistent(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.cross_verify_all()
        assert report.objects_checked == 10
        assert report.objects_ok == 10
        assert report.objects_mismatched == 0
        assert report.objects_with_errors == 0
        assert report.objects_skipped_single_replica == 0
        assert len(report.details) == 0

    def test_cross_verify_all_prefix_filter(self, cluster_rf2):
        cluster_rf2.put("alpha/a.bin", b"aaa")
        cluster_rf2.put("beta/b.bin", b"bbb")
        cluster_rf2.flush_all()
        report = cluster_rf2.cross_verify_all(prefix="alpha/")
        assert report.objects_checked == 1
        assert report.objects_ok == 1

    def test_cross_verify_report_repr(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.cross_verify_all()
        s = repr(report)
        assert "checked=" in s
        assert "skipped=" in s

    def test_cross_verify_shard_digest_repr(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.cross_verify_object("data/obj_0000.bin")
        s = repr(report.shards[0])
        assert "md5=" in s

    def test_cross_verify_object_report_repr(self, cluster_rf2_seeded):
        report = cluster_rf2_seeded.cross_verify_object("data/obj_0000.bin")
        s = repr(report)
        assert "consistent=" in s
