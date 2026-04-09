"""Tests for multipart upload tracking and stale upload purging."""

import pytest
import shardedobjstr

from conftest import SHARD_SIZE, make_payload


# -- Multipart upload tracking ---------------------------------------

class TestMultipartTracking:
    def test_upload_tracked_and_completed(self, cluster_rf2):
        assert cluster_rf2.multipart_upload_count() == 0
        cluster_rf2.put_multipart("mp/test.bin", make_payload(4096))
        # After complete, count should be 0.
        assert cluster_rf2.multipart_upload_count() == 0

    def test_list_uploads_empty(self, cluster_rf2):
        uploads = cluster_rf2.list_multipart_uploads()
        assert len(uploads) == 0

    def test_purge_with_no_stale(self, cluster_rf2):
        purged = cluster_rf2.purge_stale_multiparts()
        assert purged == 0
