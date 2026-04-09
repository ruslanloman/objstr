"""Tests for delete marker operations: list, vacuum, re-PUT cleanup,
and shard offline/reattach behavior."""

import pytest
import shardedobjstr

from conftest import SHARD_SIZE


# -- list_delete_markers / vacuum_delete_markers API -----------------------

class TestListDeleteMarkers:
    def test_empty_cluster_no_markers(self, cluster_rf2):
        markers = cluster_rf2.list_delete_markers()
        assert markers == []

    def test_delete_creates_marker(self, cluster_rf2):
        cluster_rf2.put("obj.bin", b"data")
        cluster_rf2.delete("obj.bin")
        markers = cluster_rf2.list_delete_markers()
        assert len(markers) == 1
        key, ts = markers[0]
        assert key == "obj.bin"
        # Timestamp should be a valid RFC 3339 string
        assert "T" in ts

    def test_multiple_deletes_list_all(self, cluster_rf2):
        for i in range(5):
            cluster_rf2.put(f"file{i}.bin", b"data")
        for i in range(5):
            cluster_rf2.delete(f"file{i}.bin")
        markers = cluster_rf2.list_delete_markers()
        assert len(markers) == 5
        keys = sorted(k for k, _ in markers)
        assert keys == [f"file{i}.bin" for i in range(5)]


class TestVacuumDeleteMarkers:
    def test_vacuum_empty(self, cluster_rf2):
        purged, cleaned = cluster_rf2.vacuum_delete_markers()
        assert purged == 0
        assert cleaned == 0

    def test_vacuum_removes_applied_markers(self, cluster_rf2):
        cluster_rf2.put("del.bin", b"data")
        cluster_rf2.delete("del.bin")
        assert len(cluster_rf2.list_delete_markers()) == 1

        purged, cleaned = cluster_rf2.vacuum_delete_markers()
        assert purged == 1
        assert cleaned == 0

        # Markers should be gone
        assert cluster_rf2.list_delete_markers() == []

    def test_vacuum_fails_with_offline_shard(self, cluster_rf2):
        cluster_rf2.put("obj.bin", b"data")
        cluster_rf2.delete("obj.bin")
        cluster_rf2.detach_shard(0)
        with pytest.raises(Exception, match="offline"):
            cluster_rf2.vacuum_delete_markers()

    def test_vacuum_returns_tuple(self, cluster_rf2):
        result = cluster_rf2.vacuum_delete_markers()
        assert isinstance(result, tuple)
        assert len(result) == 2


# -- PUT cleans stale delete markers ----------------------------------------

class TestPutCleansMarkers:
    def test_reput_after_delete_cleans_marker(self, cluster_rf2):
        """PUT after DELETE should clean up the stale delete marker."""
        cluster_rf2.put("reput.bin", b"v1")
        cluster_rf2.delete("reput.bin")

        markers = cluster_rf2.list_delete_markers()
        assert len(markers) == 1

        # Re-PUT the same key
        cluster_rf2.put("reput.bin", b"v2")

        # Marker should be cleaned by the PUT
        markers = cluster_rf2.list_delete_markers()
        assert len(markers) == 0

        # Data should be correct
        assert cluster_rf2.get("reput.bin") == b"v2"

    def test_put_if_not_exists_after_delete_cleans_marker(self, cluster_rf2):
        """put_if_not_exists after DELETE should also clean the marker."""
        cluster_rf2.put("cond.bin", b"v1")
        cluster_rf2.delete("cond.bin")
        assert len(cluster_rf2.list_delete_markers()) == 1

        cluster_rf2.put_if_not_exists("cond.bin", b"v2")
        assert len(cluster_rf2.list_delete_markers()) == 0
        assert cluster_rf2.get("cond.bin") == b"v2"

    def test_reput_then_vacuum_nothing_to_purge(self, cluster_rf2):
        """After re-PUT cleans the marker, vacuum should find nothing."""
        cluster_rf2.put("obj.bin", b"v1")
        cluster_rf2.delete("obj.bin")
        cluster_rf2.put("obj.bin", b"v2")

        purged, cleaned = cluster_rf2.vacuum_delete_markers()
        assert purged == 0
        assert cleaned == 0


# -- Delete with offline shards --------------------------------------------

class TestDeleteOfflineShard:
    def test_delete_with_offline_shard(self, cluster_rf2):
        """Delete should succeed even if one shard is offline (best-effort)."""
        cluster_rf2.put("offline_del.bin", b"data")
        cluster_rf2.detach_shard(0)

        # Marker is written to healthy shards only
        cluster_rf2.delete("offline_del.bin")
        with pytest.raises(Exception):
            cluster_rf2.get("offline_del.bin")

    def test_delete_marker_retained_on_failure(self, cluster_rf2):
        """Delete markers are not rolled back on partial failure."""
        cluster_rf2.put("retained.bin", b"data")
        cluster_rf2.detach_shard(0)

        cluster_rf2.delete("retained.bin")

        # Even with a shard offline, markers should exist on healthy shards
        markers = cluster_rf2.list_delete_markers()
        assert len(markers) == 1
        assert markers[0][0] == "retained.bin"


# -- Full lifecycle ---------------------------------------------------------

class TestDeleteMarkerLifecycle:
    def test_put_delete_reput_vacuum(self, cluster_rf2):
        """Full lifecycle: put -> delete -> re-put -> vacuum."""
        cluster_rf2.put("life.bin", b"v1")
        cluster_rf2.delete("life.bin")
        assert len(cluster_rf2.list_delete_markers()) == 1

        # Re-PUT cleans marker
        cluster_rf2.put("life.bin", b"v2")
        assert len(cluster_rf2.list_delete_markers()) == 0

        # Delete again
        cluster_rf2.delete("life.bin")
        assert len(cluster_rf2.list_delete_markers()) == 1

        # Vacuum cleans the applied marker
        purged, cleaned = cluster_rf2.vacuum_delete_markers()
        assert purged == 1
        assert cleaned == 0
        assert cluster_rf2.list_delete_markers() == []

    def test_list_hides_deleted_objects(self, cluster_rf2):
        """Deleted objects should not appear in list()."""
        cluster_rf2.put("keep.bin", b"keep")
        cluster_rf2.put("remove.bin", b"remove")
        cluster_rf2.delete("remove.bin")

        keys = [m.location for m in cluster_rf2.list()]
        assert "keep.bin" in keys
        assert "remove.bin" not in keys

        # Marker keys should also be hidden from list
        for k in keys:
            assert not k.startswith("__deleted__/")

    def test_head_returns_not_found_after_delete(self, cluster_rf2):
        """head() should raise for deleted objects."""
        cluster_rf2.put("headtest.bin", b"data")
        cluster_rf2.delete("headtest.bin")
        with pytest.raises(Exception):
            cluster_rf2.head("headtest.bin")


# -- Shard reattach with stale markers -------------------------------------

class TestStaleMarkerOnReattach:
    def test_put_while_shard_offline_then_reattach(self, three_shard_paths):
        """When a shard is offline during DELETE + re-PUT, reattaching
        and syncing should clean up the stale marker on the returning shard."""
        shards = [(p, SHARD_SIZE) for p in three_shard_paths]
        cluster = shardedobjstr.format_and_open_cluster(
            shards, replication_factor=2,
        )

        # Write an object, then take shard 2 offline
        cluster.put("sync_test.bin", b"original")
        cluster.flush_all()
        cluster.detach_shard(2)

        # Delete and re-PUT while shard 2 is offline
        cluster.delete("sync_test.bin")
        cluster.put("sync_test.bin", b"updated")

        # No markers should exist (PUT cleaned them)
        assert len(cluster.list_delete_markers()) == 0

        # Reattach shard 2
        count = cluster.attach_shard(2, three_shard_paths[2], force=True)

        # The live object should be readable
        assert cluster.get("sync_test.bin") == b"updated"

