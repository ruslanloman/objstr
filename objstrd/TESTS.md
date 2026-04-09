# objstrd Test Suite

Run all: `CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo test -p objstrd --release`

Run one file: `CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo test -p objstrd --release --test object_crud`

Run one test: `CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo test -p objstrd --release --test object_crud test_put_then_get`

All tests run on the Linux VM. No server should be running on port 8080.

| File | Focus |
|------|-------|
| object_crud | Core S3 CRUD: buckets, PUT/GET/HEAD/DELETE, ETag, content-type, path traversal, spool, zero-byte, no-space |
| listing_range | ListObjectsV2/V1, pagination, delimiter, prefix, encoding-type, Range reads, ListObjectVersions |
| multipart | Multipart lifecycle: create, upload parts, complete, abort, list parts, list uploads, UploadPartCopy |
| multipart_limits | Concurrent upload cap, part number bounds, UploadPartCopy buffering, metadata pressure, abort cleanup, stale purge |
| copy_delete | CopyObject, DeleteObjects (batch), copy-to-same-key, copy-overwrites, nested keys |
| bucket_persistence | Bucket JSON sidecar persistence across restarts, rebuild from index, empty bucket persist |
| large_object | 15 GB single PUT, 10 GB multipart - memory-pressure tests (ignored, require ~30 GB disk) |
| metadata | Custom metadata (x-amz-meta-*), content-type, cache-control, ETag, copy preserve/replace, multipart, range GET |
| object_attributes | GetObjectAttributes: ETag, Size, StorageClass, LastModified, multipart, not-found |
| conditional_requests | If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since on standalone server |
| compression | Zstd PUT/GET round-trip, range reads, small-object passthrough, metadata + compression |
| s3_compat_stubs | ACL stubs (GetObjectAcl, GetBucketAcl), versioning stubs (GET/PUT BucketVersioning) |
| admin_endpoints | Admin ops via subprocess: flush, rebuild-index, clear-bucket-cache, repair-replication, drain, token auth, check-config, compression, logs, sysinfo, heatmap, region, extent, getraw, nodeconfig, shard info |
| admin_html_cors | HTML page 200 checks, per-shard HTML pages, CORS headers and OPTIONS preflight |
| admin_queries | /_admin/objects pagination, prefix, since_txn; /_admin/shards single and multi-shard |
| viz_endpoints | /_admin/info, /_admin/shards, /_admin/flush, sysinfo, heatmap, region, extent, getraw, nodeconfig, shard info, logs, token auth, HTML pages |
| read_only_e2e | Read-only mode: single-store and distributed, mutation rejection |
| event_bus | Event bus emission: PUT, DELETE, DELETE on non-existent |
| event_socket_logs_e2e | Event socket log entries: subscriber connect/disconnect, bad auth |
| streaming_replica_e2e | Event-driven streaming replica: writer + reader, PUT/DELETE propagation |
| distributed_e2e | Multi-shard (3 shards, RF=1) PUT/GET/LIST/DELETE/HEAD/metadata/copy/multipart/pagination |
| distributed_metadata | Body-only size semantics on distributed backend: HEAD/GET/LIST consistency, range, copy, multipart, UploadPartCopy |
| distributed_mixed_metadata | Metadata across mixed shard types (Raw + FS + InMemory), copy preserve/replace, range, size consistency |
| distributed_conditional | Conditional requests (If-None-Match, If-Match, If-Modified-Since) on sharded backend |
| distributed_delete_markers | Delete markers through S3 adapter: create, overwrite-clears, batch, vacuum, nonexistent, lifecycle events |
| distributed_delete_markers_advanced_e2e | Delete markers with offline shards, concurrent deletes, reput lifecycle, stale marker cleanup, vacuum during degraded |
| distributed_degraded_e2e | S3 behavior during shard failures: degraded reads/writes, cascade, event emission, copy, delete, recovery |
| distributed_repair_e2e | Recovery via S3: repair-replication preserves metadata, under-replication repair, over-replication trim, catalog rebuild at scale |
| repair_replication_e2e | Background repair-replication: raw+S3 rf=2, stripe(2 raw)+S3 rf=2, S3-to-S3 rf=2, /_admin/repair-replication-status endpoint |
| replication_mirror | RF=2/RF=3 mirroring: content identity, deduplication, catalog entries, overwrite, metadata |
| recovery_e2e | Health poll detach, mirror sync reattach, partitioned repair, re-replication, brief flap, writes-during-recovery, over-replication trim |
| reload_e2e | SIGHUP reload, HTTP reload, shard rename, add/remove shard, invalid config |
| cross_verify_mixed_e2e | Cross-shard MD5 verification with 3 heterogeneous store types, corruption detection |
| mixed_store_event_e2e | 4-store heterogeneous cluster with event socket, shard failure and recovery |
| drain_rebalance_s3_e2e | Drain and repair-replication verified through S3: data integrity, metadata, listing, placement, multipart, range, copy |
| drain_repair_replication_s3_e2e | Drain and repair-replication verified through S3 (identical scenarios to drain_rebalance_s3_e2e) |
| rebalance_e2e | Background repair-replication subprocess tests: raw+S3 rf=2, stripe+S3 rf=2, S3-to-S3 rf=2, status endpoint |
| logging_levels_e2e | Structured logging: level/category/text filters, limit cap, log file output, chronological order, startup entries |

---

## Shared Test Helpers

### common/mod.rs

| Function | Purpose |
|----------|---------|
| `TestServer::start(bucket)` | In-process S3 server backed by single RawObjectStore |
| `DistributedTestServer::start(shards, rf, bucket)` | In-process S3 server backed by ShardedObjectStore with N raw shards |
| `build_client()` | HTTP client with no redirect following |
| `extract_xml_tags(body, tag)` | Parse all `<tag>` values from XML response body |
| `extract_xml_tag(body, tag)` | Parse single `<tag>` value from XML |
| `complete_xml(parts)` | Build CompleteMultipartUpload XML |
| `put_event_keys(events)` | Extract keys from PUT events |
| `collect_events_timeout(rx)` | Drain events with timeout |
| `count_put_events(events)` | Count PUT events |
| `count_delete_events(events)` | Count DELETE events |
| `drain_events(rx)` | Non-blocking drain of pending events |

### subprocess_helpers/mod.rs

| Function | Purpose |
|----------|---------|
| `ServerProcess` | RAII wrapper for spawned objstrd subprocess |
| `objstrd_bin()` | Path to compiled objstrd binary |
| `format_raw_image(path, size)` | Format a raw device image |
| `write_single_config(dir, img)` | Write standalone TOML config |
| `write_multi_config(dir, imgs)` | Write multi-shard TOML config |
| `start_server(config)` | Spawn objstrd with given config |
| `start_server_with_args(config, args)` | Spawn with extra CLI args |
| `wait_for_server(port)` | Poll until server responds |
| `wait_for_server_with_token(port, token)` | Poll with admin token |
| `build_client()` | Build HTTP client for subprocess tests |

---

## Object CRUD Tests (tests/object_crud.rs)

Core S3 object CRUD operations via in-process TestServer (single raw shard, 64 MB).

| Test | What it covers |
|------|----------------|
| `test_list_buckets` | ListBuckets returns created buckets |
| `test_head_bucket` | HEAD bucket returns 200 for existing bucket |
| `test_head_bucket_not_found` | HEAD non-existent bucket returns 404 |
| `test_create_bucket_already_exists` | CreateBucket duplicate name returns 409 |
| `test_get_bucket_location` | GetBucketLocation returns region |
| `test_put_then_get` | PUT then GET returns same body |
| `test_put_nested_key` | PUT with path separators in key |
| `test_put_overwrite` | PUT to existing key overwrites |
| `test_put_no_such_bucket` | PUT to non-existent bucket returns 404 |
| `test_put_zero_bytes` | PUT empty object (0 bytes) |
| `test_get_not_found` | GET non-existent object returns 404 |
| `test_head_object` | HEAD returns metadata without body |
| `test_head_object_not_found` | HEAD non-existent returns 404 |
| `test_delete_object` | DELETE returns 204 |
| `test_delete_object_not_found_204` | DELETE non-existent returns 204 |
| `test_put_returns_etag` | PUT response includes ETag header |
| `test_put_content_type` | PUT content-type stored and retrieved |
| `test_head_bucket_region_header` | HEAD bucket includes x-amz-bucket-region |
| `test_list_buckets_region` | ListBuckets includes region |
| `test_path_traversal_rejected` | Path traversal (../) in PUT rejected |
| `test_dirmark_reserved_rejected` | Reserved .shard key rejected on PUT |
| `test_path_traversal_rejected_on_get` | Path traversal in GET rejected |
| `test_path_traversal_rejected_on_head` | Path traversal in HEAD rejected |
| `test_path_traversal_rejected_on_delete` | Path traversal in DELETE rejected |
| `test_dirmark_rejected_on_get` | Reserved .shard key rejected on GET |
| `test_dirmark_rejected_on_delete` | Reserved .shard key rejected on DELETE |
| `test_trailing_slash_key_roundtrip` | Keys with trailing slashes round-trip |
| `test_spool_threshold_large_object` | Large objects spooled through temp file |
| `test_spool_threshold_boundary` | Spool threshold boundary handling |
| `test_head_bucket_stats_headers` | HEAD bucket includes object-count and region headers |
| `test_head_bucket_stats_cache_consistency` | Bucket stats cache consistency after PUT/DELETE |
| `test_create_bucket_invalid_dns_name` | Invalid bucket names rejected |
| `test_delete_bucket_not_empty` | DeleteBucket non-empty returns 409 |
| `test_delete_bucket_not_found` | DeleteBucket non-existent returns 404 |
| `test_create_then_delete_empty_bucket` | Create then delete empty bucket succeeds |
| `test_content_encoding_and_language` | content-encoding and content-language round-trip |
| `test_no_space_returns_error` | PUT returns error when store is full |

---

## Listing and Range Tests (tests/listing_range.rs)

ListObjectsV2/V1, pagination, delimiter, prefix, encoding-type, Range reads, and ListObjectVersions.

| Test | What it covers |
|------|----------------|
| `test_v2_basic` | ListObjectsV2 basic listing |
| `test_v2_prefix` | ListObjectsV2 prefix filtering |
| `test_v2_delimiter` | ListObjectsV2 with delimiter (common prefixes) |
| `test_v2_prefix_delimiter` | ListObjectsV2 with both prefix and delimiter |
| `test_v2_max_keys` | ListObjectsV2 max-keys limit |
| `test_v2_continuation_token` | ListObjectsV2 pagination via continuation token |
| `test_v2_empty` | ListObjectsV2 on empty bucket |
| `test_v2_metadata` | ListObjectsV2 includes size and modification time |
| `test_v2_encoding_type_url` | encoding-type=url encodes special chars in keys |
| `test_v2_encoding_type_url_prefixes` | encoding-type=url applied to common-prefixes |
| `test_v2_start_after` | ListObjectsV2 StartAfter parameter |
| `test_range_partial_content` | Range GET returns 206 Partial Content |
| `test_range_skip_leading` | Range GET skips leading bytes |
| `test_range_suffix` | Suffix range (last N bytes) |
| `test_range_entire_object` | Range covering entire object returns 206 |
| `test_range_not_found` | Range GET on non-existent returns 404 |
| `test_range_invalid_416` | Out-of-bounds range returns 416 |
| `test_content_range_header` | Response includes Content-Range header |
| `test_list_object_versions` | ListObjectVersions returns delete markers and versions |
| `test_list_object_versions_prefix` | ListObjectVersions with prefix filter |
| `test_list_versions_trailing_slash_key` | ListObjectVersions with trailing-slash keys |
| `test_list_versions_prefix_trailing_slash` | ListObjectVersions prefix on trailing-slash keys |
| `test_list_objects_v1` | ListObjects (v1) basic listing |
| `test_list_objects_v1_next_marker` | ListObjects v1 pagination with Marker |
| `test_list_objects_v1_encoding_type_url` | ListObjects v1 encoding-type=url |
| `test_list_objects_v1_next_marker_value` | v1 NextMarker calculation |
| `test_list_buckets_multiple` | ListBuckets with multiple buckets |

---

## Multipart Tests (tests/multipart.rs)

Multipart upload lifecycle via in-process TestServer.

| Test | What it covers |
|------|----------------|
| `test_create_multipart_upload` | CreateMultipartUpload succeeds |
| `test_multipart_basic` | Full multipart: create, upload parts, complete |
| `test_multipart_small_parts` | Multipart with very small parts |
| `test_multipart_single_empty_part` | Multipart with single empty part |
| `test_multipart_abort` | AbortMultipartUpload cancels upload |
| `test_multipart_abort_unknown_upload_id` | Abort unknown upload ID returns 404 |
| `test_multipart_list_parts` | ListParts returns parts with ETags |
| `test_multipart_overwrite_existing` | Multipart overwrites existing object |
| `test_multipart_varying_part_sizes` | Heterogeneous part sizes |
| `test_multipart_complete_unknown_upload_id` | Complete unknown ID returns 404 |
| `test_list_multipart_uploads` | ListMultipartUploads returns in-progress uploads |
| `test_list_multipart_uploads_empty_after_abort` | Aborted uploads disappear from listing |
| `test_upload_part_copy` | UploadPartCopy copies source object as part |
| `test_upload_part_copy_with_range` | UploadPartCopy with byte range |
| `test_upload_part_copy_range_out_of_bounds` | UploadPartCopy range beyond object bounds |
| `test_upload_part_copy_range_inverted` | UploadPartCopy with invalid range (start > end) |
| `test_multipart_non_contiguous_parts` | Multipart with gaps in part numbers |
| `test_multipart_missing_part_fails_complete` | Complete fails with missing parts |

---

## Multipart Limits Tests (tests/multipart_limits.rs)

Multipart limits, resource cleanup, and edge cases.

| Test | What it covers |
|------|----------------|
| `test_concurrent_upload_cap` | Concurrent multipart uploads limited by cap |
| `test_part_number_bounds` | Part numbers must be in valid range (1-10000) |
| `test_upload_part_copy_buffers_in_memory` | UploadPartCopy buffers in memory |
| `test_many_parts_metadata_pressure` | 1000+ parts under metadata pressure |
| `test_abort_frees_resources` | Abort releases resources |
| `test_upload_part_bad_upload_id` | Bad upload ID returns 400/404 |
| `test_stale_upload_purged` | Stale multipart auto-purged after expiry |

---

## Copy and Delete Tests (tests/copy_delete.rs)

CopyObject and DeleteObjects (batch) operations.

| Test | What it covers |
|------|----------------|
| `test_copy_object_basic` | Basic copy-object succeeds |
| `test_copy_preserves_content` | Copied object has identical content |
| `test_copy_verify_etag` | Copied ETag matches source |
| `test_copy_source_not_found` | Copy from non-existent source returns 404 |
| `test_copy_object_same_key` | Copy to same key handled per spec |
| `test_copy_object_same_key_replace` | Copy to same key with REPLACE directive |
| `test_copy_to_nested_key` | Copy to key with path separators |
| `test_copy_overwrites_existing` | Copy to existing destination overwrites |
| `test_delete_objects_basic` | DeleteObjects batch succeeds |
| `test_delete_objects_partial_missing` | Mix of existing and missing objects succeeds |
| `test_delete_objects_empty_list` | Empty key list succeeds |
| `test_delete_objects_leaves_others` | Non-targeted objects untouched |
| `test_delete_objects_result_keys` | Response lists deleted keys |
| `test_copy_no_metadata_preserves_etag` | Copy without directive preserves ETag |
| `test_copy_replace_no_source_metadata` | Copy with REPLACE when source has no metadata |

---

## Bucket Persistence Tests (tests/bucket_persistence.rs)

Bucket name persistence via JSON sidecar file across server restarts (subprocess pattern).

| Test | What it covers |
|------|----------------|
| `test_bucket_json_written_on_save` | POST /_admin/save-buckets writes JSON file |
| `test_bucket_json_reflects_delete` | Deleting bucket removes from JSON |
| `test_restart_loads_from_json` | Server restart reloads from JSON sidecar |
| `test_rebuild_from_index_when_json_missing` | Buckets rebuilt from index if JSON missing |
| `test_move_json_rebuild_and_compare` | Rebuild from index matches saved JSON |
| `test_json_format` | JSON file contains valid sorted bucket array |
| `test_empty_bucket_persists` | Empty buckets (no objects) persist to JSON |

---

## Large Object Tests (tests/large_object.rs)

Memory-pressure tests for very large objects. All tests are `#[ignore]` and require ~30 GB disk.

| Test | What it covers |
|------|----------------|
| `large_single_put` | Stream 15 GB as single PUT, verify no OOM, spot-check bytes |
| `large_multipart_upload` | Upload 10 x 1 GB parts, verify assembly and bytes |

---

## Metadata Tests (tests/metadata.rs)

Custom metadata (x-amz-meta-*), content-type, cache-control, ETag, copy, multipart, range GET.

| Test | What it covers |
|------|----------------|
| `test_custom_metadata_roundtrip_head` | x-amz-meta-* round-trips via HEAD |
| `test_custom_metadata_roundtrip_get` | x-amz-meta-* round-trips via GET |
| `test_cache_control_and_content_disposition` | cache-control and content-disposition headers |
| `test_etag_consistency` | ETag consistent across PUT/GET |
| `test_head_content_length_excludes_trailer` | HEAD content-length = body size (no trailer) |
| `test_get_body_no_trailer_leak` | GET body excludes metadata trailer |
| `test_empty_body_metadata` | Empty objects with metadata |
| `test_overwrite_replaces_metadata` | Overwrite replaces metadata |
| `test_list_size_excludes_trailer` | LIST size = body size |
| `test_copy_preserves_metadata` | Copy preserves metadata by default |
| `test_copy_replace_metadata` | Copy with REPLACE updates metadata |
| `test_copy_replace_preserves_etag` | Copy with REPLACE can preserve ETag |
| `test_multipart_metadata_roundtrip` | Metadata assembled with multipart |
| `test_multiple_custom_metadata_keys` | Multiple x-amz-meta-* keys |
| `test_no_meta_sidecar_extents` | Metadata objects don't create separate extents |
| `test_range_get_with_metadata` | Range GET with metadata present |
| `test_delete_removes_metadata` | DELETE removes metadata sidecar |
| `test_head_bucket_bytes_used_logical` | HEAD bucket reports logical (body-only) bytes |
| `test_latin1_metadata_non_latin1_dropped` | Non-Latin1 chars dropped from metadata |
| `test_latin1_metadata_roundtrip` | Latin1 metadata round-trips correctly |

---

## Object Attributes Tests (tests/object_attributes.rs)

GetObjectAttributes S3 operation.

| Test | What it covers |
|------|----------------|
| `test_get_object_attributes_basic` | Returns ETag, Size, StorageClass |
| `test_get_object_attributes_size_only` | Size-only request |
| `test_get_object_attributes_not_found` | Non-existent key returns 404 |
| `test_get_object_attributes_multipart` | Attributes on multipart-assembled object |
| `test_get_object_attributes_last_modified` | Includes LastModified |
| `test_get_object_attributes_etag_only` | ETag-only request |

---

## Conditional Request Tests (tests/conditional_requests.rs)

S3 conditional headers on standalone (single shard) server.

| Test | What it covers |
|------|----------------|
| `test_if_modified_since_304` | If-Modified-Since (not modified) returns 304 |
| `test_if_modified_since_200` | If-Modified-Since (modified) returns 200 |
| `test_if_unmodified_since_412` | If-Unmodified-Since (modified) returns 412 |
| `test_if_unmodified_since_200` | If-Unmodified-Since (not modified) returns 200 |
| `test_if_match_200` | If-Match (matching ETag) returns 200 |
| `test_if_match_412` | If-Match (non-matching ETag) returns 412 |
| `test_if_none_match_304` | If-None-Match (matching ETag) returns 304 |
| `test_if_none_match_200` | If-None-Match (non-matching ETag) returns 200 |

---

## Compression Tests (tests/compression.rs)

Zstd compression via in-process TestServer.

| Test | What it covers |
|------|----------------|
| `test_compression_zstd_put_get_roundtrip` | Zstd-compressed PUT/GET round-trip |
| `test_compression_range_read` | Range GET on compressed objects |
| `test_compression_small_object_passthrough` | Small objects bypass compression |
| `test_compression_with_metadata` | Metadata round-trips with compression |

---

## S3 Compatibility Stubs (tests/s3_compat_stubs.rs)

Stub responses for ACL and versioning endpoints.

| Test | What it covers |
|------|----------------|
| `test_get_object_acl` | GetObjectACL returns stub ACL |
| `test_get_object_acl_not_found` | GetObjectACL on missing object returns 404 |
| `test_get_bucket_acl` | GetBucketACL returns stub ACL |
| `test_get_bucket_versioning` | GetBucketVersioning returns MFADelete=Disabled, Status=Suspended |
| `test_put_bucket_versioning_accepted` | PutBucketVersioning accepted (no-op) |

---

## Admin Endpoints Tests (tests/admin_endpoints.rs)

Admin API via subprocess pattern (spawns real objstrd binary).

| Test | What it covers |
|------|----------------|
| `test_admin_flush` | POST /_admin/flush succeeds on raw-backed server |
| `test_admin_rebuild_index` | POST /_admin/rebuild-index returns bucket count |
| `test_admin_clear_bucket_cache` | POST /_admin/clear-bucket-cache rebuilds registry |
| `test_admin_repair_replication` | POST /_admin/repair-replication performs object migration on cluster |
| `test_admin_repair_replication_standalone_503` | Repair-replication returns 503 on standalone |
| `test_admin_drain_shard` | POST /_admin/drain/{id} evacuates objects |
| `test_admin_drain_invalid_shard_id` | Drain non-existent shard returns 400 |
| `test_admin_drain_standalone_503` | Drain returns 503 on standalone |
| `test_admin_token_required` | Admin endpoints require Bearer token when auth enabled |
| `test_check_config_valid` | --check-config succeeds on valid tree config |
| `test_check_config_broken` | --check-config exits with error on bad syntax |
| `test_check_config_duplicate_nodes` | --check-config rejects duplicate node names |
| `test_check_config_missing_node` | --check-config fails when --node missing from config |
| `test_check_config_no_config_flag` | --check-config fails without --config |
| `test_compression_zstd_round_trip` | --compression=zstd round-trips via subprocess |
| `test_compression_snappy_round_trip` | --compression=snappy round-trips via subprocess |
| `test_logs_endpoint` | GET /_admin/logs returns structured entries |
| `test_logs_text_search` | GET /_admin/logs?search=term filters by keyword |
| `test_log_file_output` | Log file format and field presence |
| `test_admin_sysinfo` | GET /_admin/sysinfo returns system info |
| `test_admin_buckets` | GET /_admin/buckets returns size/count |
| `test_admin_heatmap` | GET /_admin/heatmap returns heat-map data |
| `test_admin_heatmap_custom_chunks` | GET /_admin/heatmap?chunk_size=1000 custom sizing |
| `test_admin_region` | GET /_admin/region returns block device region info |
| `test_admin_extent_meta` | GET /_admin/extent/{id}/meta returns extent metadata |
| `test_admin_getraw` | GET /_admin/getraw returns raw bytes |
| `test_admin_nodeconfig` | GET /_admin/nodeconfig returns parsed tree config |
| `test_admin_shard_info` | GET /_admin/shard/{id}/info returns shard metadata |
| `test_admin_shard_objects` | GET /_admin/shard/{id}/objects lists shard objects |
| `test_admin_shard_invalid_id` | GET /_admin/shard/{invalid}/info returns 404 |
| `test_admin_recovery_status` | GET /_admin/recovery returns recovery status |

---

## Admin HTML and CORS Tests (tests/admin_html_cors.rs)

HTML pages and CORS headers via subprocess pattern.

| Test | What it covers |
|------|----------------|
| `test_html_pages_return_200` | All HTML page endpoints return 200 with text/html |
| `test_per_shard_html_pages` | Per-shard HTML pages return 200 when shard exists |
| `test_unknown_admin_path_404` | Unknown /_admin/* paths return 404 |
| `test_cors_headers_on_json_endpoints` | CORS headers on JSON endpoints with --cors-origin |
| `test_cors_preflight_options` | OPTIONS returns CORS preflight headers |
| `test_no_cors_headers_by_default` | CORS headers absent without --cors-origin |

---

## Admin Query Tests (tests/admin_queries.rs)

Admin query endpoints for object listing and shard information.

| Test | What it covers |
|------|----------------|
| `test_objects_empty_store` | Empty store returns zero objects |
| `test_objects_lists_put_objects` | PUT objects appear in /_admin/objects |
| `test_objects_pagination` | /_admin/objects limit and offset pagination |
| `test_objects_prefix_filter` | /_admin/objects?prefix=X filters by key prefix |
| `test_objects_since_txn` | /_admin/objects?since_txn=N returns after transaction N |
| `test_shards_single_shard` | GET /_admin/shards with single shard |
| `test_shards_multi_shard` | GET /_admin/shards aggregates multiple shards |
| `test_objects_multi_shard_aggregation` | /_admin/objects aggregates across shards |

---

## Viz Endpoints Tests (tests/viz_endpoints.rs)

Admin/visualization endpoints via in-process TestServer (no subprocess).

| Test | What it covers |
|------|----------------|
| `test_admin_info_fields` | /_admin/info returns expected fields |
| `test_admin_shards_single` | /_admin/shards with single shard |
| `test_admin_objects_empty` | /_admin/objects on empty store |
| `test_admin_objects_with_data` | /_admin/objects with stored objects |
| `test_admin_objects_pagination` | /_admin/objects pagination |
| `test_admin_sysinfo_fields` | /_admin/sysinfo returns system info |
| `test_admin_buckets_list` | /_admin/buckets returns bucket list |
| `test_admin_nodeconfig_fields` | /_admin/nodeconfig returns config |
| `test_admin_heatmap_fields` | /_admin/heatmap returns data |
| `test_admin_shard_info` | /_admin/shard/{id}/info returns shard info |
| `test_admin_shard_objects` | /_admin/shard/{id}/objects lists objects |
| `test_admin_shard_invalid_returns_404` | Invalid shard ID returns 404 |
| `test_admin_shard_nonnumeric_returns_404` | Non-numeric shard ID returns 404 |
| `test_admin_flush_post` | POST /_admin/flush |
| `test_admin_rebuild_index_post` | POST /_admin/rebuild-index |
| `test_admin_repair_replication_standalone_503` | Repair-replication returns 503 on standalone |
| `test_admin_drain_standalone_503` | Drain returns 503 on standalone |
| `test_admin_region` | GET /_admin/region |
| `test_admin_extent_meta` | GET /_admin/extent/{id}/meta |
| `test_admin_getraw` | GET /_admin/getraw |
| `test_admin_logs` | GET /_admin/logs |
| `test_admin_recovery_standalone` | GET /_admin/recovery on standalone |
| `test_admin_unknown_path_404` | Unknown /_admin/* returns 404 |
| `test_admin_token_auth_forbidden` | Without token returns 403 |
| `test_admin_token_auth_success` | With Bearer token succeeds |
| `test_admin_token_does_not_affect_s3` | S3 ops work without admin token |
| `test_s3_still_works_through_viz_wrapper` | S3 ops work through viz wrapper |
| `test_html_pages_return_200` | HTML pages return 200 with text/html |
| `test_per_shard_html_pages` | Per-shard HTML pages work |

---

## Read-Only E2E Tests (tests/read_only_e2e.rs)

Read-only mode via --read-only flag.

| Test | What it covers |
|------|----------------|
| `test_read_only_single_store_s3` | Single raw store: rejects PUT, allows GET |
| `test_read_only_distributed_s3` | 3-shard distributed: rejects PUT, allows GET |

---

## Event Bus Tests (tests/event_bus.rs)

Internal event bus emission via in-process TestServer.

| Test | What it covers |
|------|----------------|
| `test_event_bus_put_emitted` | PUT emits StoreEvent::Put |
| `test_event_bus_delete_emitted` | DELETE emits StoreEvent::Delete |
| `test_event_bus_delete_nonexistent_emits` | DELETE non-existent still emits Delete event |

---

## Event Socket Logs E2E (tests/event_socket_logs_e2e.rs)

Event socket logging via subprocess pattern.

| Test | What it covers |
|------|----------------|
| `event_socket_log_entries` | Connections/disconnections appear in /_admin/logs |

---

## Streaming Replica E2E (tests/streaming_replica_e2e.rs)

Event-driven streaming replica via subprocess pattern.

| Test | What it covers |
|------|----------------|
| `streaming_replica_put_delete` | Writer PUT/DELETE events sync to read-only replica via event socket |

---

## Distributed E2E Tests (tests/distributed_e2e.rs)

Multi-shard (3 shards, RF=1) store through S3 protocol layer via in-process DistributedTestServer.

| Test | What it covers |
|------|----------------|
| `distributed_put_get` | PUT then GET on 3 raw shards |
| `distributed_list_many` | PUT 30 objects, verify all list correctly |
| `distributed_objects_spread_across_shards` | Objects distribute across all 3 shards |
| `distributed_delete` | DELETE removes objects |
| `distributed_head` | HEAD returns correct metadata |
| `distributed_head_bucket_region` | HEAD bucket returns x-amz-bucket-region |
| `distributed_list_buckets_region` | ListBuckets returns region |
| `distributed_metadata_roundtrip` | Custom metadata (content-type, x-amz-meta-*) round-trips |
| `distributed_overwrite` | Overwrite updates all shards |
| `distributed_list_with_prefix` | ListObjectsV2 prefix filters correctly |
| `distributed_get_not_found` | GET non-existent returns 404 |
| `distributed_catalog_rebuild` | Catalog rebuilt from raw indexes |
| `distributed_range_get` | Range GET on distributed backend |
| `distributed_range_get_suffix` | Suffix range GET (last N bytes) |
| `distributed_copy_object` | Copy on distributed backend |
| `distributed_copy_preserves_metadata` | Copy preserves metadata across shards |
| `distributed_multi_delete` | DeleteObjects removes multiple objects |
| `distributed_multi_delete_events_for_missing` | DELETE events emitted for missing objects |
| `distributed_list_delimiter` | ListObjectsV2 with delimiter |
| `distributed_list_delimiter_with_prefix` | ListObjectsV2 with delimiter and prefix |
| `distributed_multipart_basic` | Multipart assembles on distributed backend |
| `distributed_multipart_abort` | Abort multipart cleans up on all shards |
| `distributed_list_pagination` | Pagination with continuation token across shards |

---

## Distributed Metadata Tests (tests/distributed_metadata.rs)

Body-only size semantics on distributed (3 shards, RF=1) backend.

| Test | What it covers |
|------|----------------|
| `distributed_head_content_length_excludes_trailer` | HEAD content-length = body size (no trailer) |
| `distributed_get_body_no_trailer_leak` | GET returns exactly body bytes |
| `distributed_list_size_excludes_trailer` | LIST size = body size |
| `distributed_head_get_list_size_consistency` | HEAD, GET, LIST report consistent sizes |
| `distributed_range_get_with_metadata` | Range GET with metadata present |
| `distributed_copy_preserves_metadata_and_size` | Copy preserves metadata and size |
| `distributed_copy_replace_metadata_and_size` | Copy with REPLACE updates size correctly |
| `distributed_multipart_metadata_and_size` | Multipart preserves metadata and size |
| `distributed_upload_part_copy_with_metadata` | UploadPartCopy preserves metadata |
| `distributed_head_bucket_bytes_used_logical` | HEAD bucket reports logical bytes (body only) |
| `distributed_empty_body_metadata` | Empty objects with metadata report size as 0 |

---

## Distributed Mixed Metadata Tests (tests/distributed_mixed_metadata.rs)

Metadata on mixed-backend cluster (RF=3: Raw + LocalFileSystem + InMemory).

| Test | What it covers |
|------|----------------|
| `mixed_metadata_roundtrip` | Custom metadata round-trips across heterogeneous backends |
| `mixed_list_reports_body_only_size` | LIST reports body-only size across mixed backends |
| `mixed_copy_preserves_metadata` | Copy preserves metadata across different shard types |
| `mixed_copy_replace_metadata` | Copy with REPLACE across mixed shards |
| `mixed_range_reads_exclude_metadata` | Range GET excludes metadata across mixed backends |
| `mixed_head_get_size_consistency` | HEAD/GET/LIST consistent on mixed backends |

---

## Distributed Conditional Tests (tests/distributed_conditional.rs)

Conditional requests on sharded (3 shards, RF=2) backend.

| Test | What it covers |
|------|----------------|
| `distributed_if_none_match_304` | If-None-Match (matching ETag) returns 304 |
| `distributed_if_none_match_200` | If-None-Match (non-matching) returns 200 |
| `distributed_if_match_200` | If-Match (matching ETag) returns 200 |
| `distributed_if_match_412` | If-Match (non-matching) returns 412 |
| `distributed_if_modified_since_304` | If-Modified-Since (not modified) returns 304 |
| `distributed_if_modified_since_200` | If-Modified-Since (modified) returns 200 |
| `distributed_overwrite_changes_etag` | Overwrite changes ETag observable through conditionals |

---

## Distributed Delete Markers Tests (tests/distributed_delete_markers.rs)

Delete markers through S3 adapter on ShardedObjectStore (3 shards, RF=2).

| Test | What it covers |
|------|----------------|
| `s3_delete_creates_marker` | DELETE creates marker, GET returns 404, LIST excludes key |
| `s3_reput_after_delete_clears_marker` | Re-PUT after DELETE clears stale marker |
| `s3_batch_delete_creates_markers` | DeleteObjects creates markers for each key |
| `s3_vacuum_cleans_markers` | Vacuum removes stale delete markers |
| `s3_delete_nonexistent_returns_204` | DELETE on non-existent returns 204 |
| `s3_delete_lifecycle_with_events` | DELETE events emitted with marker lifecycle |

---

## Distributed Delete Markers Advanced E2E (tests/distributed_delete_markers_advanced_e2e.rs)

Advanced delete-marker scenarios during shard failures and degraded mode (3 shards, RF=2).

| Test | What it covers |
|------|----------------|
| `s3_delete_with_shard_offline_creates_markers` | DELETE with shard offline creates markers on survivors |
| `s3_batch_delete_with_shard_offline` | DeleteObjects creates markers during outage |
| `s3_concurrent_deletes_all_create_markers` | Concurrent DELETEs each create markers |
| `s3_delete_reput_delete_lifecycle` | DELETE -> PUT -> DELETE lifecycle preserves correct state |
| `s3_stale_marker_cleared_on_get_after_reput` | Stale markers cleared after re-PUT |
| `s3_delete_marker_not_visible_in_list` | Delete markers hidden from LIST |
| `s3_vacuum_after_reput_removes_only_stale` | Vacuum removes only stale markers |
| `s3_vacuum_during_degraded_mode` | Vacuum works with offline shards |

---

## Distributed Degraded E2E Tests (tests/distributed_degraded_e2e.rs)

S3 behavior during shard failures (3 shards, RF=2).

| Test | What it covers |
|------|----------------|
| `s3_read_via_replica_when_shard_offline` | GETs work via replicas with 1-of-3 offline |
| `s3_head_returns_correct_size_during_degraded` | HEAD reports correct content-length during outage |
| `s3_put_succeeds_on_surviving_shards` | PUT succeeds on surviving shards |
| `s3_events_emitted_during_degraded_writes` | PUT events emitted during degraded writes |
| `s3_list_during_shard_failure` | LIST works with offline shards |
| `s3_cascade_failure_rf3_two_offline` | 2-of-3 offline, reads fail (no quorum) |
| `s3_all_shards_offline_then_recover` | All offline -> failure, recovery restores service |
| `s3_copy_during_degraded_mode` | Copy works during degraded mode |
| `s3_delete_during_degraded_then_recover` | DELETE creates markers, recovery respects them |

---

## Distributed Repair E2E Tests (tests/distributed_repair_e2e.rs)

Recovery/repair operations on distributed backend (3 shards, RF=2).

| Test | What it covers |
|------|----------------|
| `s3_repair_replication_preserves_metadata` | Repair-replication after shard offline preserves metadata via S3 |
| `s3_repair_replication_under_replicated_restored` | Repair-replication restores under-replicated objects to full RF |
| `s3_over_replication_trimmed_after_reattach` | Over-replication trimmed when shard reattaches |
| `s3_catalog_rebuild_at_scale` | Rebuild catalog from raw indexes at scale (100 objects) |
| `s3_under_replicated_objects_still_readable` | Under-replicated objects readable via replicas |

---

## Replication Mirror Tests (tests/replication_mirror.rs)

RF=2 and RF=3 mirroring via in-process DistributedTestServer.

| Test | What it covers |
|------|----------------|
| `replication_2_objects_mirrored` | Every object exists on exactly 2 shards |
| `replication_2_content_identical` | RF=2 copies have identical raw bytes |
| `replication_2_listing_deduplicates` | LIST deduplicates objects on 2 shards |
| `replication_2_delete_removes_all_copies` | DELETE removes all RF=2 copies |
| `replication_2_catalog_entries` | Catalog shows single entry per object |
| `replication_2_overwrite` | Overwrite updates all RF=2 copies |
| `replication_3_full_mirror` | RF=3 all-copies scenario |
| `replication_2_metadata_preserved` | Metadata preserved on all RF=2 copies |

---

## Recovery E2E Tests (tests/recovery_e2e.rs)

Auto-recovery: health polling, sync reattach, re-replication via in-process DistributedTestServer.

| Test | What it covers |
|------|----------------|
| `health_poll_detaches_unreachable_shard` | Health polling detects offline shard within ~10s |
| `mirror_sync_reattaches_recovered_shard` | Mirror sync fills gaps and reattaches shard |
| `writes_during_recovery_preserved` | Objects written during outage preserved |
| `partitioned_sync_repairs_under_replicated` | Partitioned sync repairs after cascade failure |
| `brief_flap_does_not_detach` | Brief offline/online doesn't trigger detach |
| `re_replication_restores_rf_on_permanent_failure` | Re-replication restores RF after permanent loss |
| `recovery_disabled_does_not_detach` | Disabling recovery prevents auto-detach |
| `device_path_gone_stays_offline` | Shard path deletion keeps shard offline |
| `over_replication_trimmed_after_shard_returns` | Over-replicated copies trimmed after recovery |
| `two_shards_fail_simultaneously` | Two simultaneous failures handled |
| `recovery_status_tracking` | Recovery status tracked and exposed |

---

## Reload E2E Tests (tests/reload_e2e.rs)

Live config reload (SIGHUP / POST /_admin/reload) via subprocess pattern.

| Test | What it covers |
|------|----------------|
| `reload_sighup_preserves_data` | SIGHUP reloads config, data persists |
| `reload_http_endpoint` | POST /_admin/reload reloads via HTTP |
| `reload_after_shard_rename` | Reload with renamed shard path |
| `reload_adds_new_shard` | Reload adds new shard to cluster |
| `reload_double_sighup` | Multiple SIGHUPs handled correctly |
| `reload_removes_shard` | Reload removes shard from cluster |
| `reload_invalid_config_exits` | Invalid config on reload causes exit |

---

## Cross-Verify Mixed E2E (tests/cross_verify_mixed_e2e.rs)

Cross-shard MD5 verification with 3 heterogeneous store types.

| Test | What it covers |
|------|----------------|
| `cross_verify_three_store_types` | RF=3 (Raw + LocalFS + S3-forwarding): cross_verify_all detects corruption |

---

## Mixed Store Event E2E (tests/mixed_store_event_e2e.rs)

4-store heterogeneous cluster with event socket monitoring.

| Test | What it covers |
|------|----------------|
| `mixed_4store_event_socket_lifecycle` | RF=4 cluster with PUT/DELETE/failure/recovery and event verification |

---

## Drain & Rebalance S3 E2E (tests/drain_rebalance_s3_e2e.rs)

Comprehensive drain and repair-replication operations verified through S3
(3 raw shards, in-process DistributedTestServer).

| Test | What it covers |
|------|----------------|
| `drain_preserves_all_data_via_s3` | Drain shard, verify all objects readable via S3 GET |
| `drain_preserves_metadata_via_s3_head` | Metadata (x-amz-meta-*) survives drain |
| `drain_list_consistency_via_s3` | ListObjectsV2 consistent after drain |
| `drain_then_repair_replication_full_lifecycle_via_s3` | Drain + repair-replication end-to-end |
| `sequential_drains_preserve_data_via_s3` | Two sequential drains, data intact |
| `new_writes_skip_drained_shard_via_s3` | Writes after drain go to healthy shards |
| `repair_replication_restores_rf_verified_via_s3` | Under-replicated objects restored to RF |
| `over_replication_trim_verified_via_s3` | Excess replicas trimmed |
| `concurrent_writes_during_drain_via_s3` | Writes during drain succeed |
| `delete_during_drain_verified_via_s3` | Deletes during drain respected |
| `drain_moves_sole_copy_objects_via_s3` | RF=1 objects moved off drained shard |
| `repair_replication_idempotent_via_s3` | Repeated repair-replication is no-op |
| `drain_repair_replication_at_scale_via_s3` | 200+ objects drain + repair |
| `drain_then_new_bucket_writes_via_s3` | New bucket writes after drain |
| `placement_correct_after_repair_replication_via_s3` | Object placement respects RF post-repair |
| `multipart_object_survives_drain_via_s3` | Multipart objects survive drain |
| `range_reads_after_drain_via_s3` | Range GETs correct after drain |
| `copy_after_drain_via_s3` | CopyObject works after drain |

---

## Drain Repair-Replication S3 E2E (tests/drain_repair_replication_s3_e2e.rs)

Same test scenarios as drain_rebalance_s3_e2e.rs - parallel test file for
independent CI runs.

---

## Rebalance E2E (tests/rebalance_e2e.rs)

Background repair-replication subprocess tests with heterogeneous shard types.

| Test | What it covers |
|------|----------------|
| `test_repair_replication_raw_plus_s3_rf2` | Raw image + FS-backed S3, RF=2 repair |
| `test_repair_replication_stripe_plus_s3_rf2` | 2 raw images + FS-backed S3, RF=2 |
| `test_repair_replication_s3_to_s3_rf2` | Two FS-backed S3 endpoints, RF=2 |
| `test_admin_repair_replication_status_endpoint` | /_admin/repair-replication-status JSON validation |

---

## Logging Levels E2E (tests/logging_levels_e2e.rs)

Structured logging via /_admin/logs and log file output.

| Test | What it covers |
|------|----------------|
| `logs_put_produces_info` | PUT request generates info-level log entry |
| `logs_get_404_produces_warn` | GET 404 generates warn-level entry |
| `logs_level_filter_excludes_lower` | level=warn excludes info entries |
| `logs_category_filter` | category=requests filters correctly |
| `logs_text_search` | q= free-text search in messages |
| `logs_limit_cap` | limit parameter caps results |
| `logs_flush_produces_admin_entry` | /_admin/flush generates admin category entry |
| `logs_combined_level_and_category` | Combined level + category filtering |
| `logs_newest_first` | Entries returned newest first |
| `logfile_entries_written` | --log-file produces tab-separated entries |
| `logfile_matches_admin_logs` | File and API return consistent entries |
| `logfile_contains_startup_entry` | Startup lifecycle entry in log file |
| `logfile_admin_operations` | Admin operations logged to file |
| `logfile_chronological_order` | File entries in chronological order |

---

## External S3-Compatibility Tests

Ceph s3-tests, Mint, and rclone test suites live in `external-tests/`.
See [`external-tests/AGENTS.md`](../external-tests/AGENTS.md) for setup and run instructions.
Detailed per-suite docs:

- [Ceph s3-tests](../external-tests/s3-compat/ceph/ceph-tests.md)
- [MinIO Mint](../external-tests/s3-compat/mint/mint-tests.md)
- [rclone](../external-tests/s3-compat/rclone/rclone-tests.md)

---

## Future Test Ideas

The following test scenarios are not yet implemented.

### Event Socket - Delivery and Limits

- **Multi-reader broadcast**: Connect N authenticated clients, flush, verify all receive the same FLUSH message.
- **Concurrent flushes**: Rapid flush_index() calls; readers receive all messages in order with increasing txn_ids.
- **Large txn_id**: Verify txn_id near u64::MAX is correctly transmitted in the FLUSH protocol line.
- **Max readers limit**: Set max_readers=4, connect 5 clients; 5th should be rejected.
- **Reader disconnect frees slot**: Disconnect a reader, connect a new one within the limit.
- **Dead reader cleanup**: Drop a socket abruptly; next broadcast should succeed without hanging.
- **Reader reconnect**: Disconnect and reconnect with same secret; new connection receives future events.
