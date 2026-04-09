# ShardedObjectStore Test Suite

Run all: `CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test -p shardedobjstr --release`

Run one file: `CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test -p shardedobjstr --release --test cluster_e2e`

Run one test: `CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test -p shardedobjstr --release --test cluster_e2e cluster_e2e_all_features`

| File | Focus |
|------|-------|
| catalog_persistence_e2e | Catalog save/load roundtrips (JSON + bincode), embedded CRC32c validation, dirty flag tracking, corruption/tamper detection, legacy JSON compat, data loss scenarios, CatalogPersistence::None no-op, save_to_file/load_from_file, clear, replace, with_persistence builder API, try_insert, with_entries, load_into |
| catalog_stress_e2e | Large catalog scale testing (1000+ objects), persistence roundtrips at scale, catalog rebuild at scale, verify_all at scale, shard access validation |
| cli_e2e | CLI binary integration: version, usage, check-config, format, info, put, get, delete, verify, health, vacuum, list-deleted; catview JSON and bincode |
| cluster_e2e | Full CRUD lifecycle, put modes, multipart, shard health, catalog ops, invalidation, attach/detach, read-only, read preference, multipart tracking/expiry, RF clamping, rename_if_not_exists, free-space placement, get_opts conditionals, list_with_delimiter, create-mode concurrency |
| concurrent_stress | High-concurrency with shard failures, repair under load, cascade failure, over-replication trim, catalog load under contention, read-repair counters |
| config_validation_e2e | Config file validation: replicas > shard count, unknown compression, raw shard missing path, bad compression, S3 empty endpoint/bucket, FS missing directory, mem shard clean |
| cross_verify_e2e | MD5-based cross-shard verification: consistent raw cluster, consistent mixed cluster, FS corruption detection, single object, single replica error, prefix filter |
| degraded_e2e | Operations with offline shards: reads, writes, list, copy, multipart, reattach, repair, over-replication, CRC, partial-delete orphans, find_replication_target skips Syncing, write retry, read-repair fixes, partial overwrite stale data |
| delete_markers_advanced_e2e | Delete markers with shard failure, recovery, sync, overwrite during offline, vacuum edge cases, concurrent vacuum rejection, vacuum-put race, partial write on degraded |
| delete_markers_e2e | Delete marker basics: creation, hiding, vacuum, re-PUT, idempotent delete, min_writes no-panic, vacuum older object |
| event_socket_e2e | Event socket: PUT/DELETE/FLUSH events over Unix socket, authentication, multiple subscribers, streaming replica catalog sync, non-mirror rejection |
| metadata_e2e | put_with_meta, head_with_meta, list_with_meta, set_meta_len, replication, read-only, min_writes enforcement, sidecar shards, S3-like shards, mixed clusters, TLV decode, put_with_meta_from_file, retry on total failure, cleanup_sidecar_maybe |
| metadata_preservation_e2e | Metadata roundtrip preservation across copy, copy_if_not_exists, rename, replicate, repair sweep, mirror sync, partitioned sync, sync_and_reattach, drain, multipart, mirror without raw refs, orphaned sidecar |
| metadata_regression | Body-only size invariants: head vs get vs list consistency, metadata roundtrip, catalog meta_len after put_with_meta and plain put, rebuild catalog preserves meta_len |
| min_writes_e2e | min_writes quorum enforcement: default values, with_min_writes builder, clamping, partial writes, cleanup-on-fail, strict vs best-effort delete, flat config, progressive failure, replace, delete_requires_min_writes |
| multipart_edge_cases_e2e | Multipart edge cases: abort-reupload, multiple abort cycles, shard offline before complete, multi-part assembly, concurrent tracked uploads, put_multipart_opts |
| multipart_reliability_e2e | Multipart reliability: tracking leak on insufficient writes, orphan data cleanup, metadata preservation on replicas, happy-path tracking, single-shard multipart |
| perf_bench | Multi-phase throughput benchmark (sequential writes, reads, random reads, catalog rebuild) |
| redistribute_sweep_e2e | redistribute_sweep balancing: imbalanced clusters, noop when balanced, batch limits, tolerance thresholds, data integrity, RF preservation |
| repair_e2e | repair_replication_sweep, mirror_sync, partitioned_sync, sync_and_reattach, probe_store, over_replication_trim, re_replication_sweep, replicate_object, verify_object, verify_all, plan_repair_replication, validate_shard_access, find_replication_target, pick_excess_shard, cascade failure, drain_shard, metadata preservation, concurrent write race, batch trim |
| shard_lifecycle_e2e | shard_offline_since tracking, shard_free_space get/set, read_shard_order catalog vs fallback, drain_shard (exclusive/replicated/empty/RF1/sidecar), RawRefRegistry helpers, hold_offline/release_hold, set_detach_reason, Syncing exclusion, jump-hash placement stability, validate_shard_access, rebuild_catalog_for_shard stale entries |
| startup_repair_e2e | Startup under-replication: mem shard loss detection, repair_replication_sweep recovery, detach triggers under-replication |
| write_fanout_partial_e2e | Write fan-out partial failure: quorum met despite failures, all-target retry, InsufficientWrites, min_writes=1 single success, selective Degraded marking, retry on all targets offline, fail when no targets available |

---

## Shared Test Helpers (tests/common/mod.rs)

| Function | Purpose |
|----------|---------|
| `format_shard(path, size)` | Format a new raw shard image at the given path |
| `open_shard(path)` | Open an existing shard image |
| `build_cluster(raws, replicas)` | Create a ShardedObjectStore from raw stores and rebuild catalog |
| `build_cluster_with_events(raws, replicas)` | Create cluster with attached EventBus; returns (cluster, bus) |
| `flush_all(raws)` | Flush all shard indexes to disk |
| `seed_objects(cluster, raws)` | Put 5 test objects (4096 bytes each) and return key list |
| `drain_events(rx)` | Non-blocking drain of pending broadcast events |
| `put_event_keys(events)` | Extract keys from PUT events |
| `delete_event_keys(events)` | Extract keys from DELETE events |
| `count_put_events(events)` | Count PUT events in a slice |
| `count_delete_events(events)` | Count DELETE events in a slice |
| `build_mixed_cluster(raw_path, fs_dir, size, replicas)` | Build cluster from one raw shard + one LocalFileSystem shard; returns (cluster, raw store, fs dir) |

---

## Catalog Persistence E2E Tests (tests/catalog_persistence_e2e.rs)

Catalog save/load with JSON and bincode formats, embedded CRC32c checksums,
dirty flag tracking, corruption detection, legacy JSON compatibility,
data loss scenarios, and catalog API helpers.

| Test | What it covers | Config |
|------|----------------|--------|
| `catalog_save_load_json_roundtrip` | Save catalog as JSON, load into fresh cluster, verify all keys/sizes | 3+3 shards, RF=2, 64 MB |
| `catalog_save_load_bincode_roundtrip` | Save catalog as bincode, load into fresh cluster, verify keys present | 3+3 shards, RF=2, 64 MB |
| `catalog_persistence_none_is_noop` | CatalogPersistence::None: save succeeds (no-op), load returns empty | 3+3 shards, RF=2, 64 MB |
| `catalog_load_missing_file_returns_empty` | load_catalog from non-existent file returns empty catalog (not error) | 3 shards, RF=2, 64 MB |
| `catalog_save_to_file_load_from_file_e2e` | Catalog::save_to_file + Catalog::load_from_file filesystem roundtrip | 3 shards, RF=2, 64 MB |
| `catalog_clear_empties_all` | Catalog::clear removes all entries; len=0 after clear | 3 shards, RF=2, 64 MB |
| `catalog_replace_swaps_entries` | Catalog::replace atomically swaps entire map; old keys gone, new present | 3 shards, RF=2, 64 MB |
| `catalog_persistence_builder_api` | with_persistence builder, persistence() getter, save/load roundtrip | 2 shards, RF=1, 64 MB |
| `catalog_json_and_bincode_produce_same_data` | JSON and bincode formats contain identical catalog entries | 3+3+3 shards, RF=2, 64 MB |
| `catalog_try_insert_returns_false_if_exists` | try_insert returns false for duplicate key | 3 shards, RF=2, 64 MB |
| `catalog_try_insert_different_keys_both_succeed` | try_insert succeeds for two different keys | 3 shards, RF=2, 64 MB |
| `catalog_with_entries_provides_readonly_access` | with_entries provides read-only closure access to all entries | 3 shards, RF=2, 64 MB |
| `catalog_load_into_replaces_all_entries` | load_into replaces all catalog entries with new data | 3 shards, RF=2, 64 MB |
| `catalog_load_into_empty_clears_target` | load_into with empty source clears target catalog | 3 shards, RF=2, 64 MB |
| `catalog_dirty_after_put` | Catalog dirty flag set after put() | 3 shards, RF=2, 64 MB |
| `catalog_dirty_after_remove` | Catalog dirty flag set after remove() | 3 shards, RF=2, 64 MB |
| `catalog_dirty_after_clear` | Catalog dirty flag set after clear() | 3 shards, RF=2, 64 MB |
| `catalog_dirty_after_replace` | Catalog dirty flag set after replace() | 3 shards, RF=2, 64 MB |
| `catalog_dirty_after_add_replica` | Catalog dirty flag set after add_replica() | 3 shards, RF=2, 64 MB |
| `catalog_load_into_clears_dirty` | load_into() clears the dirty flag | 3 shards, RF=2, 64 MB |
| `save_catalog_if_dirty_skips_when_clean` | save_catalog_if_dirty returns Ok(false) when catalog is clean | 3 shards, RF=2, 64 MB |
| `save_catalog_if_dirty_saves_when_dirty` | save_catalog_if_dirty returns Ok(true) and persists when catalog is dirty | 3 shards, RF=2, 64 MB |
| `json_checksum_detects_corruption` | Flip a byte in JSON file body; load returns CRC error | 3 shards, RF=2, 64 MB |
| `bincode_checksum_detects_corruption` | Flip a byte in bincode payload; load returns CRC error | 3 shards, RF=2, 64 MB |
| `json_tampered_checksum_field_rejected` | Modify the checksum value in the JSON envelope; load returns mismatch error | 3 shards, RF=2, 64 MB |
| `bincode_tampered_checksum_header_rejected` | Modify the 4-byte CRC header in bincode file; load returns mismatch error | 3 shards, RF=2, 64 MB |
| `bincode_truncated_file_rejected` | Bincode file shorter than 4 bytes; load returns error | 3 shards, RF=2, 64 MB |
| `no_save_before_drop_loses_catalog_data` | Data is lost if catalog is dropped without save | 3 shards, RF=2, 64 MB |
| `save_then_new_puts_without_flush_loses_new_data` | New puts after a save are lost if not flushed again | 3 shards, RF=2, 64 MB |
| `json_file_contains_checksum_and_data_fields` | Verify JSON file has envelope structure with checksum and data keys | 3 shards, RF=2, 64 MB |
| `json_legacy_format_still_loads` | Plain JSON (no envelope) still loads for backward compatibility | 3 shards, RF=2, 64 MB |
| `json_full_roundtrip_with_checksum` | Save JSON, verify CRC in file, load back, data matches | 3 shards, RF=2, 64 MB |
| `bincode_full_roundtrip_with_checksum` | Save bincode, verify CRC prefix, load back, data matches | 3 shards, RF=2, 64 MB |

---

## Catalog Stress E2E Tests (tests/catalog_stress_e2e.rs)

Large catalog scale testing: persistence roundtrips with 1000+ objects,
catalog rebuild at scale, verify_all at scale, and shard access validation.

| Test | What it covers | Config |
|------|----------------|--------|
| `large_catalog_1000_objects` | Put 1000 objects, verify catalog len and readability | 3 shards, RF=2, 64 MB |
| `large_catalog_persistence_json_roundtrip` | Save/load 1000-object catalog in JSON format | 3 shards, RF=2, 64 MB |
| `large_catalog_persistence_bincode_roundtrip` | Save/load 1000-object catalog in bincode format | 3 shards, RF=2, 64 MB |
| `rebuild_catalog_at_scale` | Clear and rebuild catalog with 1000+ objects | 3 shards, RF=2, 64 MB |
| `verify_all_at_scale` | verify_all on 1000 objects: all consistent | 3 shards, RF=2, 64 MB |
| `validate_shard_access_with_offline_shard` | validate_shard_access with one shard offline at scale | 3 shards, RF=2, 64 MB |

---

## CLI E2E Tests (tests/cli_e2e.rs)

CLI binary integration tests: subcommand parsing, version output, config validation,
full object lifecycle (format/info/put/get/delete), verify, health, vacuum,
list-deleted, and catview with JSON and bincode catalogs.

| Test | What it covers | Config |
|------|----------------|--------|
| `cli_version_prints_version_string` | `shardedobjstr version` exits 0 and prints version | N/A (CLI) |
| `cli_no_args_prints_usage` | Running with no args prints usage/help | N/A (CLI) |
| `cli_check_config_valid` | `--check-config` on a valid config exits 0 | N/A (CLI) |
| `cli_check_config_invalid` | `--check-config` on an invalid config exits non-zero | N/A (CLI) |
| `cli_format_info_put_get_delete_lifecycle` | Format, info, put, get, delete lifecycle via CLI commands | 2 shards, RF=2, 64 MB |
| `cli_verify_and_health` | verify and health subcommands exit 0 on healthy cluster | 2 shards, RF=2, 64 MB |
| `catview_version` | `shardedobjstr-catview --version` exits 0 | N/A (CLI) |
| `catview_reads_json_catalog` | catview reads and displays JSON catalog | 2 shards, RF=2, 64 MB |
| `catview_reads_bincode_catalog` | catview reads and displays bincode catalog | 2 shards, RF=2, 64 MB |
| `cli_vacuum_and_list_deleted` | vacuum and list-deleted subcommands work correctly | 2 shards, RF=2, 64 MB |

---

## Cluster E2E Tests (tests/cluster_e2e.rs)

Full CRUD lifecycle, put modes, multipart uploads, shard health management,
catalog operations, shard invalidation, attach/detach, read-only mode,
read preference, multipart tracking/expiry, rename, free-space placement,
conditional get_opts, and RF clamping.

| Test | What it covers | Config |
|------|----------------|--------|
| `cluster_e2e_all_features` | Full lifecycle: format, put, list, get, copy, delete, verify, add 4th shard, remove shard. Event bus and catalog rebuild. | 3 shards, RF=2, 64 MB |
| `put_mode_create_succeeds_on_new_key` | PutMode::Create allows new key | 1 shard, RF=1 |
| `put_mode_create_rejects_existing_key` | PutMode::Create rejects duplicate (AlreadyExists) | 1 shard, RF=1 |
| `put_mode_overwrite_replaces_data` | PutMode::Overwrite replaces existing data | 1 shard, RF=1 |
| `put_mode_update_without_preconditions_acts_as_overwrite` | PutMode::Update with no e_tag/version acts as overwrite | 1 shard, RF=1 |
| `put_mode_update_with_etag_rejected` | PutMode::Update with e_tag raises Precondition error | 1 shard, RF=1 |
| `put_mode_update_with_version_rejected` | PutMode::Update with version raises Precondition error | 1 shard, RF=1 |
| `multipart_upload_single_part` | Multipart with one part: put, complete, verify placement and event | 2 shards, RF=2, 64 MB |
| `multipart_upload_multiple_parts` | Multipart with 3 parts assembled correctly; catalog tracks size | 3 shards, RF=2, 64 MB |
| `multipart_upload_abort` | Multipart abort: no PUT event, object not in catalog | 1 shard, RF=1, 64 MB |
| `set_shard_health_basic` | set_shard_health transitions shards between Healthy/Degraded/Offline | 2 shards, RF=1 |
| `set_shard_health_offline_excludes_from_reads` | Offline shard excluded from reads; fallback to replica succeeds | 3 shards, RF=2, 64 MB |
| `catalog_entries_for_shard` | entries_for_shard returns objects on a given shard | 3 shards, RF=2, 64 MB |
| `catalog_remove_all_for_shard` | remove_all_for_shard purges all catalog entries referencing a shard | 3 shards, RF=2, 64 MB |
| `invalidate_shard_basic` | Invalidate shard 0, purge and rescan entries, verify objects readable | 3 shards, RF=2, 64 MB |
| `invalidate_shard_detects_missing_keys` | Manually delete key from shard store, invalidate detects missing_keys | 2 shards, RF=1, 64 MB |
| `invalidate_shard_out_of_range` | Error handling for non-existent shard ID 99 | 1 shard, RF=1, 64 MB |
| `rebuild_catalog_for_single_shard` | Clear shard 0 catalog entries, rebuild and verify restoration | 2 shards, RF=2, 64 MB |
| `attach_shard_force_and_replicate` | Start degraded, format new shard, attach with force=true, replicate under-replicated objects | 2 shards, RF=2, 64 MB |
| `detach_reattach_laptop_scenario` | Simulate laptop offline: write while shard unavailable, reattach and replicate | 2 shards, RF=2, 64 MB |
| `put_opts_update_no_precondition_succeeds` | PUT with UpdateVersion(None, None) succeeds as overwrite | 3 shards, RF=2, 64 MB |
| `put_opts_update_with_etag_rejects` | PUT with e_tag precondition rejected | 2 shards, RF=2, 64 MB |
| `read_only_rejects_writes` | Read-only mode: gets work, all writes (put, delete, copy, rename, multipart) fail | 2 shards, RF=2, 64 MB |
| `read_preference_roundrobin_and_ordered` | Default RoundRobin preference, switch to Ordered, verify both modes | 2 shards, RF=2, 64 MB |
| `multipart_upload_tracked_and_completed` | Multipart upload tracked, completed, deregistered | 3 shards, RF=2, 64 MB |
| `multipart_upload_tracked_and_aborted` | Multipart upload tracked, aborted, deregistered | 3 shards, RF=2, 64 MB |
| `purge_stale_multiparts_respects_expiry` | purge_stale_multiparts removes uploads older than expiry | 2 shards, RF=1, 64 MB |
| `rename_if_not_exists_basic` | Rename src to dst: dst exists with correct content, src gone | 3 shards, RF=2, 64 MB |
| `rename_if_not_exists_fails_when_dest_exists` | rename_if_not_exists fails when destination key already exists | 3 shards, RF=2, 64 MB |
| `rename_if_not_exists_updates_catalog` | Catalog updated after rename: src removed, dst present | 3 shards, RF=2, 64 MB |
| `free_space_affects_placement` | Emptiest shard receives most objects when free space varies | 4 shards, RF=1, 64 MB |
| `create_mode_concurrent_exactly_one_wins` | PutMode::Create with concurrent writers: exactly one succeeds | 3 shards, RF=2, 64 MB |
| `list_with_delimiter_returns_prefixes_and_objects` | list_with_delimiter returns correct common_prefixes and objects | 3 shards, RF=2, 64 MB |
| `get_opts_if_none_match_returns_not_modified` | get_opts with if_none_match matching e_tag returns NotModified | 2 shards, RF=2, 64 MB |
| `get_opts_if_match_with_wrong_etag_fails` | get_opts with if_match and wrong e_tag returns Precondition error | 2 shards, RF=2, 64 MB |
| `get_opts_if_match_with_correct_etag_succeeds` | get_opts with if_match and correct e_tag returns data | 2 shards, RF=2, 64 MB |
| `get_opts_if_modified_since_old_date_succeeds` | get_opts with old if_modified_since date returns data | 2 shards, RF=2, 64 MB |
| `rf_exceeds_shard_count` | RF=3 with 2 shards: RF clamped to 2, put succeeds on both shards | 2 shards, RF=3 (clamped to 2), 64 MB |

---

## Concurrent Stress Tests (tests/concurrent_stress.rs)

High-concurrency workloads with shard failures, simultaneous repair,
catalog operations under contention, and read-repair counter tracking.

| Test | What it covers | Config |
|------|----------------|--------|
| `shard_failure_during_concurrent_ops_with_events` | 8 tasks concurrently put/get/delete; detach shard mid-flight; verify consistency and event counts | 4 shards, RF=2, 64 MB |
| `concurrent_repair_under_active_load` | repair_replication_sweep runs concurrently with 4-task workload; verifies repair progress and object readability | 4 shards, RF=2, 64 MB |
| `cascade_failure_two_shards_during_concurrent_ops` | 8 tasks with 2 shard failures injected during execution; RF=3 survives 2 failures | 5 shards, RF=3, 64 MB |
| `over_replication_trim_under_concurrent_writes` | over_replication_trim runs while new writes add over-replication; trim proceeds safely | 4 shards, RF=2, 64 MB |
| `load_catalog_during_concurrent_ops` | Load catalog (save/rebuild) while concurrent put/get operations run | 4 shards, RF=2, 64 MB |
| `read_repair_increments_counter_on_corrupt_read` | CRC error counter increments when reading corrupted data triggers read repair | 3 shards, RF=2, 64 MB |
| `read_repair_counters_start_at_zero` | crc_error_count for all shards starts at 0 | 3 shards, RF=2, 64 MB |

---

## Config Validation E2E Tests (tests/config_validation_e2e.rs)

Cluster config validation diagnostics: replicas, compression, raw shard
paths, S3 endpoints, FS directories, and mem shard configs.

| Test | What it covers | Config |
|------|----------------|--------|
| `validate_cluster_conf_replicas_exceeds_shard_count` | replicas > shard count produces diagnostic | 2 mem shards, replicas=5 |
| `validate_cluster_conf_unknown_compression` | Unknown compression algorithm produces diagnostic | Config text |
| `validate_cluster_conf_raw_shard_missing_path_no_size` | Raw shard with no path/size produces diagnostic | Config text |
| `validate_cluster_conf_raw_shard_with_bad_compression` | Raw shard with invalid compression produces diagnostic | Config text |
| `validate_cluster_conf_s3_empty_endpoint` | S3 shard with empty endpoint produces diagnostic | Config text |
| `validate_cluster_conf_s3_empty_bucket` | S3 shard with empty bucket produces diagnostic | Config text |
| `validate_cluster_conf_fs_missing_directory` | FS shard with non-existent directory produces diagnostic | Config text |
| `validate_cluster_conf_mem_shard_no_warnings` | Mem shard config produces no warnings | Config text |

---

## Cross-Verify E2E Tests (tests/cross_verify_e2e.rs)

MD5-based cross-shard verification: reads object bodies from each shard, computes MD5, and compares across replicas.

| Test | What it covers | Config |
|------|----------------|--------|
| `cross_verify_consistent_raw_cluster` | All-raw cluster with no corruption: all objects report consistent | 3 shards, RF=2, 64 MB |
| `cross_verify_consistent_mixed_cluster` | Mixed raw+FS cluster (via build_mixed_cluster helper): all objects consistent | 2 raw + 1 FS, RF=2, 64 MB |
| `cross_verify_detects_fs_corruption` | Corrupt a file on the FS shard by direct file overwrite: cross_verify detects mismatch | 2 raw + 1 FS, RF=2, 64 MB |
| `cross_verify_single_object` | cross_verify_object on a single key: returns consistent report with all shard digests | 3 shards, RF=2, 64 MB |
| `cross_verify_single_replica_returns_error` | cross_verify_object with RF=1: returns error (needs >= 2 replicas) | 3 shards, RF=1, 64 MB |
| `cross_verify_with_prefix_filter` | cross_verify_all with prefix filter: only checks matching objects | 3 shards, RF=2, 64 MB |

---

## Degraded E2E Tests (tests/degraded_e2e.rs)

Operations in degraded mode (one or more shards offline): reads, writes, list, copy, multipart, reattach, repair, over-replication detection/trim, CRC error handling, write retry, read-repair, and partial failure scenarios.

| Test | What it covers | Config |
|------|----------------|--------|
| `detach_shard_reads_still_work` | Detach shard 0; all objects still readable from replicas via get, head, get_range | 4 shards, RF=2, 64 MB |
| `detach_shard_writes_still_work` | Detach shard 1; new writes place only on healthy shards; read succeeds | 4 shards, RF=2, 64 MB |
| `detach_shard_list_and_delete_work` | Detach shard 2; list returns all objects; delete works; DELETE event fired | 3 shards, RF=2, 64 MB |
| `detach_shard_copy_operations_work` | Detach shard 0; copy and copy_if_not_exists work, avoiding offline shard | 4 shards, RF=2, 64 MB |
| `detach_shard_multipart_still_works` | Detach shard 1; multipart upload completes on healthy shards | 4 shards, RF=2, 64 MB |
| `detach_then_reattach_recovers_data` | Detach shard, write new data, re-attach; original and new data both present | 3 shards, RF=2, 64 MB |
| `reattach_with_invalidation` | Reattach with force=false (invalidation path); shard contents rescanned | 3 shards, RF=2, 64 MB |
| `under_replicated_detection_and_repair` | Detach shard creates under-replication; replicate_object repairs each key | 4 shards, RF=2, 64 MB |
| `multiple_shards_offline` | Detach 2 of 5 shards (RF=3 survives); reads, writes, and list work | 5 shards, RF=3, 64 MB |
| `all_replicas_offline_returns_error` | RF=1, take only replica offline; get returns error | 3 shards, RF=1, 64 MB |
| `start_degraded_with_offline_constructor` | new_with_offline: start with shard 1 missing; writes avoid it; reads work | 3 shards, RF=2, 64 MB |
| `put_mode_create_with_offline_shard` | Create mode works with offline shard; duplicate fails | 3 shards, RF=2, 64 MB |
| `rename_if_not_exists_partial_failure_leaves_both_copies` | Rename with FailDeleteStore: copy succeeds but delete fails; both copies remain | 3 shards, RF=2, 64 MB |
| `rename_if_not_exists_with_offline_shard` | rename_if_not_exists works; files avoid offline shard | 4 shards, RF=2, 64 MB |
| `full_lifecycle_offline_recover_repair` | Phase: write (healthy) -> offline shard -> write (degraded) -> delete -> repair -> verify | 4 shards, RF=2, 64 MB |
| `invalidate_shard_after_corruption_scenario` | Invalidate shard, all objects still readable after rescan | 3 shards, RF=2, 64 MB |
| `writes_during_shard_transitions` | Sequential detach operations during continued writes | 4 shards, RF=2, 64 MB |
| `delete_with_offline_only_replicas` | Delete with RF=1 and only replica offline (partial failure expected) | 4 shards, RF=1, 64 MB |
| `over_replicated_detection_and_trim` | Manually over-replicate, detect, pick excess shard, remove replica | 4 shards, RF=2, 64 MB |
| `find_replication_target_excludes_existing` | Target shard doesn't already hold the object | 4 shards, RF=2, 64 MB |
| `find_replication_target_none_when_all_hold_it` | No target when object on all shards | 2 shards, RF=2, 64 MB |
| `pick_excess_shard_none_when_at_rf` | Excess shard returns None for object at exactly RF replicas | 4 shards, RF=2, 64 MB |
| `repair_replication_sweep_noop_when_balanced` | Balanced cluster produces zero repairs | 4 shards, RF=2, 64 MB |
| `crc_error_count_starts_at_zero` | CRC error counters initialized to 0, tracked per shard | 3 shards, RF=2, 64 MB |
| `read_repair_on_crc_corruption` | Corrupt data on victim shard, healthy shard serves original, corruption detectable | 3 shards, RF=2, 64 MB |
| `write_retries_with_fresh_targets_on_total_failure` | Detach both initial targets; put retries on remaining healthy shards | 4 shards, RF=2, 64 MB |
| `read_repair_counters_increment_on_crc_corruption` | CRC error counter increments on corrupt read, read repair triggers | 3 shards, RF=2, 64 MB |
| `read_repair_fixes_corrupt_shard` | Read repair fixes corrupted shard data from healthy replica | 3 shards, RF=2, 64 MB |
| `write_all_replicas_fail_returns_error` | Put fails when all shards are detached | 3 shards, RF=2, 64 MB |
| `rf_one_single_shard_failure_blocks_writes` | RF=1 with one shard offline blocks writes to keys on that shard | 2 shards, RF=1, 64 MB |
| `partial_overwrite_stale_data_on_failed_shard` | Overwrite with detached shard leaves stale v1 on offline shard; v2 served from healthy | 3 shards, RF=2, 64 MB |
| `partial_delete_best_effort_removes_from_catalog` | Best-effort delete with offline shard succeeds; orphan reappears on rebuild | 3 shards, RF=2, 64 MB |
| `find_replication_target_skips_syncing_shard` | find_replication_target does not return Syncing shard as target | 3 shards, RF=2, 64 MB |

---

## Delete Markers E2E Tests (tests/delete_markers_e2e.rs)

Delete marker basics: creation, hiding from list/get/head, vacuum, re-PUT, idempotent delete, and edge cases.

| Test | What it covers | Config |
|------|----------------|--------|
| `delete_creates_marker_and_removes_object` | Delete creates marker; head/get return NotFound; marker exists internally | 3 shards, RF=2, 64 MB |
| `list_hides_delete_markers` | List does not show deleted keys or __deleted__/* markers | 3 shards, RF=2, 64 MB |
| `head_returns_not_found_for_marker_key` | head on __deleted__/key returns NotFound | 3 shards, RF=2, 64 MB |
| `get_returns_not_found_for_marker_key` | get on __deleted__/key returns NotFound | 3 shards, RF=2, 64 MB |
| `list_delete_markers_returns_entries` | list_delete_markers returns (key, timestamp) tuples for deleted objects | 3 shards, RF=2, 64 MB |
| `vacuum_purges_applied_markers` | vacuum_delete_markers removes markers when object is gone (purged=1, cleaned=0) | 3 shards, RF=2, 64 MB |
| `vacuum_handles_reput_after_delete` | Re-PUT after delete cleans stale marker; vacuum is no-op (purged=0) | 3 shards, RF=2, 64 MB |
| `vacuum_fails_with_offline_shard` | vacuum_delete_markers errors if any shard is offline | 3 shards, RF=2, 64 MB |
| `delete_then_reput_works` | Delete then re-PUT with new content; new version is readable and listed | 3 shards, RF=2, 64 MB |
| `is_delete_marker_helper` | is_delete_marker() correctly identifies __deleted__/ prefixed keys | N/A (unit) |
| `delete_nonexistent_key` | Delete of never-created key succeeds (idempotent) | 3 shards, RF=2, 64 MB |
| `delete_nonexistent_with_min_writes_no_panic` | Delete of nonexistent key with min_writes enabled does not panic | 3 shards, RF=2, 64 MB |
| `vacuum_deletes_older_object` | Vacuum correctly handles object older than marker | 3 shards, RF=2, 64 MB |

---

## Delete Markers Advanced E2E Tests (tests/delete_markers_advanced_e2e.rs)

Delete markers with shard failure, recovery, sync, overwrite during offline, vacuum edge cases, and partitioned/mirror sync interactions.

| Test | What it covers | Config |
|------|----------------|--------|
| `offline_delete_sync_cleans_stale_object_rf2_3shards` | Put on shard 0, shard 0 offline, delete, sync cleans stale | 3 shards, RF=2, 64 MB |
| `offline_delete_sync_cleans_stale_object_rf2_4shards` | Same scenario with 4 shards | 4 shards, RF=2, 64 MB |
| `offline_delete_sync_cleans_stale_object_rf3_5shards` | Same scenario with RF=3 | 5 shards, RF=3, 64 MB |
| `mirror_mode_offline_delete_sync` | Mirror (RF=2, 2 shards): detach 1, delete, reattach; no resurrection | 2 shards, RF=2, 64 MB |
| `mirror_mode_3_shards_offline_delete_sync` | Full mirror (RF=3, 3 shards): detach 1, delete, reattach; no resurrection | 3 shards, RF=3, 64 MB |
| `rf1_delete_creates_marker_and_vacuum_cleans` | RF=1: delete creates marker, vacuum purges it | 3 shards, RF=1, 64 MB |
| `vacuum_fails_offline_shard_4shards` | Vacuum fails with offline shard (4 shards) | 4 shards, RF=2, 64 MB |
| `vacuum_fails_offline_shard_5shards` | Vacuum fails with offline shard (5 shards) | 5 shards, RF=3, 64 MB |
| `put_overwrite_while_shard_offline_rf2` | Put v1, shard offline, put v2, reattach; v2 survives | 3 shards, RF=2, 64 MB |
| `put_overwrite_while_shard_offline_mirror` | Mirror mode: v1, offline, v2, reattach; v2 on returning shard | 2 shards, RF=2, 64 MB |
| `delete_then_reput_while_shard_offline` | Delete while offline, re-PUT while offline, reattach; re-PUT survives | 3 shards, RF=2, 64 MB |
| `mixed_deletes_while_shard_offline` | Put 5 objects, offline shard, delete 3, reattach; deleted stay deleted | 4 shards, RF=2, 64 MB |
| `two_shards_offline_then_recover` | Shard 1 offline, delete, shard 3 offline, delete both, recover both | 5 shards, RF=3, 64 MB |
| `get_opts_and_get_range_filter_deleted` | get_opts and get_range on deleted key return NotFound | 3 shards, RF=2, 64 MB |
| `marker_key_hidden_from_get_opts_and_get_range` | get_opts/get_range on __deleted__/ key return NotFound | 3 shards, RF=2, 64 MB |
| `vacuum_cleans_missed_delete` | Vacuum with normal delete (applied); marker purged, object removed | 3 shards, RF=2, 64 MB |
| `batch_delete_and_vacuum` | Delete 20 objects, list_delete_markers shows 20, vacuum purges all | 3 shards, RF=2, 64 MB |
| `delete_while_holding_shard_offline_rf1` | RF=1, delete while only holding shard offline; marker exists | 3 shards, RF=1, 64 MB |
| `marker_written_to_all_healthy_shards` | Markers written to ALL 4 healthy shards (not just RF shards) | 4 shards, RF=2, 64 MB |
| `full_mirror_4shards_delete_sync` | Full mirror (4 shards, RF=4): offline 1, delete, reattach; no resurrection | 4 shards, RF=4, 64 MB |
| `put_to_deleted_prefix_invisible` | Put to __deleted__/ succeeds at library level but invisible through list/get | 3 shards, RF=2, 64 MB |
| `markers_replicated_during_partitioned_sync` | Markers replicated during partitioned_sync; stale object removed | 3 shards, RF=2, 64 MB |
| `new_object_while_shard_offline_not_duplicated` | New object during offline, reattach, still RF=2 (not more) | 4 shards, RF=2, 64 MB |
| `vacuum_succeeds_after_all_shards_healthy` | Vacuum fails while offline, succeeds after reattach | 3 shards, RF=2, 64 MB |
| `list_with_delimiter_unaffected_by_markers` | list_with_delimiter not affected by markers; no leaks to common_prefixes | 3 shards, RF=2, 64 MB |
| `raw_store_marker_keys_are_regular_objects` | Raw store treats __deleted__/ keys as normal objects | 1 shard, RF=1, 64 MB |
| `raw_store_list_deleted_prefix` | Raw store list with __deleted__ prefix returns marker keys | 1 shard, RF=1, 64 MB |
| `concurrent_deletes_all_create_markers` | 10 concurrent deletes all produce 10 markers; objects hidden from list | 3 shards, RF=2, 64 MB |
| `delete_vacuum_reput_clean_lifecycle` | v1 -> delete -> vacuum -> v2: cleanest lifecycle, no markers after vacuum | 3 shards, RF=2, 64 MB |
| `attach_empty_shard_after_deletes` | Deleted object doesn't reappear with fresh shard attached | 3 shards, RF=2, 64 MB |
| `overwrite_while_offline_shard_level_bytes_partitioned` | Stale v1 on returning shard replaced by v2 during partitioned_sync | 3 shards, RF=2, 64 MB |
| `overwrite_while_offline_shard_level_bytes_mirror` | Mirror sync: v1 on returning shard updated to v2 | 2 shards, RF=2, 64 MB |
| `multiple_overwrites_while_offline` | Multiple overwrites (v1..v5) while offline, reattach; only v5 survives | 3 shards, RF=2, 64 MB |
| `orphan_object_on_returning_shard_cleaned` | Orphan on shard (not in catalog) cleaned during sync_and_reattach | 3 shards, RF=2, 64 MB |
| `catalog_correct_after_stale_overwrite_sync` | Catalog placement correct after stale overwrite and sync | 3 shards, RF=2, 64 MB |
| `reput_after_delete_cleans_marker` | Re-PUT after delete automatically removes stale marker | 3 shards, RF=2, 64 MB |
| `stale_marker_cleaned_on_reattach` | Stale marker on returning shard cleaned during reattach | 3 shards, RF=2, 64 MB |
| `delete_reput_delete_lifecycle` | Delete -> re-PUT (marker gone) -> delete -> vacuum: full lifecycle | 3 shards, RF=2, 64 MB |
| `delete_with_offline_shard_keeps_marker_and_sync_cleans` | Delete with offline shard keeps marker; sync_and_reattach cleans stale object | 3 shards, RF=2, 64 MB |
| `vacuum_idempotent_second_run_is_noop` | Second vacuum run is a no-op (all markers already purged) | 3 shards, RF=2, 64 MB |
| `vacuum_with_re_put_after_delete` | Vacuum handles re-PUT after delete: stale marker cleaned, new data intact | 3 shards, RF=2, 64 MB |
| `concurrent_vacuum_rejected` | Concurrent vacuum calls: second one rejected or serialized | 3 shards, RF=2, 64 MB |
| `vacuum_skips_syncing_shard` | Vacuum skips shards in Syncing state | 3 shards, RF=2, 64 MB |
| `delete_marker_partial_write_on_degraded_shard` | Delete marker write with degraded shard: marker lands on healthy shards | 3 shards, RF=2, 64 MB |
| `vacuum_put_race_allows_resurrection` | Race between vacuum and re-PUT: documents possible resurrection | 3 shards, RF=2, 64 MB |

---

## Event Socket E2E Tests (tests/event_socket_e2e.rs)

Event socket lifecycle: setup_event_socket, subscribe_store_events, PUT/DELETE/FLUSH event delivery over Unix domain socket, authentication rejection, concurrent subscribers, and streaming replica catalog sync.

| Test | What it covers | Config |
|------|----------------|--------|
| `event_socket_put_delete_lifecycle` | PUT 5 objects, DELETE 2; subscriber receives all PUT and DELETE events over Unix socket | 3 shards, RF=2, 64 MB |
| `event_socket_flush_events` | PUT object then flush all raw shards; FLUSH events arrive with valid shard_id | 3 shards, RF=2, 64 MB |
| `event_socket_rejects_bad_secret` | subscribe_store_events with wrong secret returns PermissionDenied error | 3 shards, RF=2, 64 MB |
| `event_socket_multiple_subscribers` | 3 concurrent subscribers all receive the same 3 PUT events | 3 shards, RF=2, 64 MB |
| `streaming_replica_catalog_sync` | subscribe_streaming_replica syncs catalog from writer to read-only replica via event socket | 2 shards, RF=2 (mirror), 64 MB |
| `streaming_replica_rejects_non_mirror_cluster` | subscribe_streaming_replica rejects non-mirror cluster (RF < shard count) with InvalidInput | 3 shards, RF=2, 64 MB |

---

## Metadata E2E Tests (tests/metadata_e2e.rs)

Comprehensive metadata testing: put_with_meta, head_with_meta, list_with_meta,
set_meta_len, get_metadata, sidecar shards, S3-like shards, mixed clusters,
TLV decode, put_with_meta_from_file, min_writes enforcement, retry logic,
and cleanup_sidecar_maybe.

| Test | What it covers | Config |
|------|----------------|--------|
| `put_with_meta_and_get_metadata_roundtrip` | put_with_meta stores payload + metadata; get_metadata retrieves exact bytes | 3 shards, RF=2, 64 MB |
| `head_with_meta_returns_correct_meta_len` | head_with_meta returns correct meta_len and total size | 3 shards, RF=2, 64 MB |
| `list_with_meta_returns_per_object_meta_len` | list_with_meta returns each object with its meta_len | 3 shards, RF=2, 64 MB |
| `set_meta_len_updates_index` | set_meta_len updates meta_len in index; head_with_meta reflects change | 3 shards, RF=2, 64 MB |
| `put_with_meta_replicates_to_all_shards` | put_with_meta replicates to RF shards; survives offline replica | 3 shards, RF=2, 64 MB |
| `put_with_meta_rejects_read_only` | put_with_meta fails when cluster is read-only | 2 shards, RF=2, 64 MB |
| `delete_sidecar_noop_on_raw_shards` | delete_sidecar is no-op on raw shards; object still readable | 3 shards, RF=2, 64 MB |
| `put_with_meta_succeeds_with_enough_replicas` | put_with_meta succeeds when enough healthy shards meet min_writes | 3 shards, RF=2, 64 MB |
| `put_with_meta_succeeds_degraded_above_min_writes` | put_with_meta succeeds with degraded shard when above min_writes threshold | 3 shards, RF=2, 64 MB |
| `put_with_meta_fails_with_insufficient_writes` | put_with_meta returns InsufficientWrites when below min_writes | 3 shards, RF=2, 64 MB |
| `put_with_meta_fails_all_detached` | put_with_meta fails when all shards are detached | 3 shards, RF=2, 64 MB |
| `meta_delete_requires_min_writes_true_enforces` | delete with delete_requires_min_writes=true enforces quorum | 3 shards, RF=2, 64 MB |
| `meta_delete_requires_min_writes_false_allows_degraded` | delete with delete_requires_min_writes=false allows degraded deletes | 3 shards, RF=2, 64 MB |
| `meta_lifecycle_put_delete_with_strict_quorum` | Full put/delete lifecycle with strict min_writes quorum | 3 shards, RF=2, 64 MB |
| `sidecar_put_with_meta_and_get_metadata_roundtrip` | put_with_meta on sidecar (InMemory) shard: metadata stored in __meta__/ companion | 3 InMemory shards, RF=2 |
| `sidecar_list_with_meta_hides_meta_files` | list_with_meta on sidecar cluster hides __meta__/ files from results | 3 InMemory shards, RF=2 |
| `sidecar_delete_sidecar_cleans_meta_file` | delete_sidecar removes __meta__/ companion on sidecar shards | 3 InMemory shards, RF=2 |
| `sidecar_trait_delete_cleans_meta_file` | delete() via ObjectStore trait removes companion __meta__ sidecar on all shards | 3 InMemory shards, RF=2 |
| `mixed_raw_and_sidecar_shards_metadata` | Metadata works across mixed raw + sidecar shards | Raw + InMemory, RF=2, 64 MB |
| `get_metadata_skips_detached_shard` | get_metadata falls back to next shard when first has no raw ref (detached) | 3 shards, RF=2, 64 MB |
| `replicate_loses_metadata_on_sidecar_shard` | replicate_object to sidecar shard without raw ref loses TLV metadata | Raw + InMemory, RF=2, 64 MB |
| `get_metadata_all_shards_offline_returns_error` | get_metadata returns error when all shards holding the object are offline | 3 shards, RF=2, 64 MB |
| `get_metadata_object_without_metadata_returns_empty` | get_metadata on object stored with plain put returns empty bytes | 3 shards, RF=2, 64 MB |
| `tlv_decode_truncated_returns_partial_map` | TLV decode with truncated data returns partial map (no panic) | N/A (unit) |
| `tlv_decode_single_byte_truncation` | TLV decode with single-byte truncation handles gracefully | N/A (unit) |
| `put_with_meta_from_file_roundtrip` | put_with_meta_from_file stores file content + metadata; get_metadata roundtrips | 3 shards, RF=2, 64 MB |
| `s3like_put_with_meta_and_get_metadata_roundtrip` | Metadata via S3-like attributes on non-raw shards | S3-like stores, RF=2 |
| `s3like_get_metadata_empty_when_no_attributes` | get_metadata returns empty when no attributes set on S3-like store | S3-like stores, RF=2 |
| `s3like_list_with_meta_returns_objects` | list_with_meta returns objects on S3-like shards | S3-like stores, RF=2 |
| `mixed_raw_and_s3like_cluster_metadata` | Metadata works across mixed raw + S3-like shards | Raw + S3-like, RF=2 |
| `sidecar_head_with_meta_returns_catalog_meta_len` | head_with_meta on sidecar shard returns catalog meta_len | InMemory shards, RF=2 |
| `sidecar_get_metadata_roundtrip` | get_metadata on sidecar shard reads companion __meta__/ file | InMemory shards, RF=2 |
| `sidecar_get_metadata_with_tlv_encoded` | get_metadata returns TLV-encoded metadata from sidecar companion | InMemory shards, RF=2 |
| `put_with_meta_retries_on_total_initial_failure` | put_with_meta retries on fresh targets when all initial targets fail | 4 shards, RF=2, 64 MB |
| `mixed_shard_kinds_raw_and_sidecar` | Mixed shard kinds (Raw + Sidecar) handle metadata correctly | Raw + InMemory, RF=2, 64 MB |
| `list_with_meta_mixed_cluster` | list_with_meta on mixed raw + sidecar cluster returns correct meta_len | Raw + InMemory, RF=2, 64 MB |
| `delete_sidecar_cleans_companion_file` | delete_sidecar removes companion __meta__/ file on sidecar shard | InMemory shards, RF=2 |
| `put_with_meta_from_file_insufficient_writes_cleans_up` | put_with_meta_from_file cleans up on InsufficientWrites | 3 shards, RF=2, 64 MB |
| `fallback_read_recovers_meta_len_from_raw_shard` | Fallback read recovers meta_len from raw shard when catalog meta_len is 0 | 3 shards, RF=2, 64 MB |
| `cleanup_sidecar_maybe_deletes_on_sidecar_shard` | cleanup_sidecar_maybe removes sidecar file on Sidecar-kind shard | InMemory shards, RF=2 |
| `cleanup_sidecar_maybe_noop_on_raw_shard` | cleanup_sidecar_maybe is no-op on Raw-kind shard | 3 shards, RF=2, 64 MB |
| `cleanup_sidecar_maybe_without_refs_assumes_sidecar` | cleanup_sidecar_maybe without raw refs assumes sidecar and attempts delete | InMemory shards, RF=2 |

---

## Metadata Preservation E2E Tests (tests/metadata_preservation_e2e.rs)

Metadata roundtrip preservation across all operations that move or copy
objects: copy, copy_if_not_exists, rename, replicate, repair sweep,
mirror/partitioned sync, drain, multipart, and edge cases.

| Test | What it covers | Config |
|------|----------------|--------|
| `copy_preserves_metadata` | TLV metadata preserved after copy | 3 shards, RF=2, 64 MB |
| `copy_if_not_exists_preserves_metadata` | TLV metadata preserved after copy_if_not_exists | 3 shards, RF=2, 64 MB |
| `rename_if_not_exists_preserves_metadata` | TLV metadata preserved after rename_if_not_exists | 3 shards, RF=2, 64 MB |
| `replicate_object_preserves_metadata` | TLV metadata preserved after replicate_object | 3 shards, RF=2, 64 MB |
| `repair_replication_sweep_preserves_metadata` | TLV metadata preserved after repair_replication_sweep | 3 shards, RF=2, 64 MB |
| `mirror_sync_preserves_metadata` | TLV metadata preserved after mirror_sync | 2 shards, RF=2, 64 MB |
| `partitioned_sync_preserves_metadata` | TLV metadata preserved after partitioned_sync | 3 shards, RF=2, 64 MB |
| `sync_and_reattach_preserves_metadata` | TLV metadata preserved after sync_and_reattach | 3 shards, RF=2, 64 MB |
| `drain_shard_preserves_metadata` | TLV metadata preserved after drain_shard | 3 shards, RF=1, 64 MB |
| `multipart_complete_should_preserve_metadata` | Multipart complete preserves metadata header | 3 shards, RF=2, 64 MB |
| `multipart_complete_replicates_body_to_all_shards` | Multipart complete replicates assembled body to all RF shards | 3 shards, RF=2, 64 MB |
| `mirror_sync_without_raw_refs_loses_metadata` | Mirror sync without raw refs loses TLV metadata (documents known limitation) | 2 shards, RF=2, 64 MB |
| `remove_replica_leaves_orphaned_sidecar_on_sidecar_shard` | remove_replica on sidecar shard leaves orphaned __meta__/ file (documents known issue) | Raw + InMemory, RF=2, 64 MB |

---

## Metadata Regression Tests (tests/metadata_regression.rs)

Body-only size invariants: ensures head, get, and list APIs consistently
report body-only sizes (excluding metadata), and that catalog meta_len
is correctly preserved through put_with_meta, plain put, and rebuild.

| Test | What it covers | Config |
|------|----------------|--------|
| `catalog_records_meta_len_after_put_with_meta` | Catalog entry records correct meta_len after put_with_meta | 3 shards, RF=2, 64 MB |
| `catalog_meta_len_zero_for_plain_put` | Catalog entry has meta_len=0 for plain put (no metadata) | 3 shards, RF=2, 64 MB |
| `sharded_get_returns_body_only` | get() returns body-only bytes (metadata stripped) | 3 shards, RF=2, 64 MB |
| `sharded_head_reports_body_only_size` | head() reports body-only size in ObjectMeta.size | 3 shards, RF=2, 64 MB |
| `sharded_head_with_meta_body_only_size` | head_with_meta reports body-only size plus separate meta_len | 3 shards, RF=2, 64 MB |
| `sharded_list_reports_body_only_size` | list() reports body-only size for each object | 3 shards, RF=2, 64 MB |
| `sharded_get_metadata_roundtrip` | get_metadata returns exact metadata bytes written by put_with_meta | 3 shards, RF=2, 64 MB |
| `sharded_head_and_get_sizes_consistent` | head().size == get().bytes().len() (both body-only) | 3 shards, RF=2, 64 MB |
| `rebuild_catalog_preserves_meta_len` | Full rebuild_catalog preserves per-object meta_len | 3 shards, RF=2, 64 MB |
| `rebuild_catalog_for_shard_preserves_meta_len` | rebuild_catalog_for_shard preserves meta_len for shard entries | 3 shards, RF=2, 64 MB |

---

## Min Writes E2E Tests (tests/min_writes_e2e.rs)

min_writes quorum enforcement: default values, builder API, clamping,
partial writes with cleanup, strict vs best-effort delete, flat config
parsing, progressive failure scenarios, and replace failures.

| Test | What it covers | Config |
|------|----------------|--------|
| `default_min_writes_rf1` | Default min_writes for RF=1 is 1 | 2 shards, RF=1 |
| `default_min_writes_rf2` | Default min_writes for RF=2 is 1 | 3 shards, RF=2 |
| `default_min_writes_rf3` | Default min_writes for RF=3 is 2 | 4 shards, RF=3 |
| `with_min_writes_sets_value` | with_min_writes builder sets custom value | 3 shards, RF=2 |
| `with_min_writes_clamped_to_rf` | min_writes clamped to RF when set higher | 3 shards, RF=2 |
| `with_min_writes_clamped_to_one` | min_writes clamped to 1 when set to 0 | 3 shards, RF=2 |
| `put_succeeds_with_enough_replicas` | Put succeeds when healthy shards >= min_writes | 3 shards, RF=2, 64 MB |
| `put_fails_with_insufficient_writes` | Put fails (InsufficientWrites) when healthy shards < min_writes | 3 shards, RF=2, 64 MB |
| `min_writes_one_allows_single_replica` | min_writes=1 allows put with only one healthy shard | 3 shards, RF=2, 64 MB |
| `min_writes_equals_rf_rejects_degraded` | min_writes=RF rejects writes when any shard is offline | 3 shards, RF=2, 64 MB |
| `flat_config_min_writes` | Flat config file min_writes field parsed correctly | Config text |
| `flat_config_no_min_writes_defaults_to_none` | Flat config without min_writes defaults to None | Config text |
| `progressive_failure_rf3_min2` | RF=3 min_writes=2: first failure OK, second failure blocks writes | 4 shards, RF=3, 64 MB |
| `replace_fails_with_insufficient_writes` | Replace (overwrite) fails with InsufficientWrites | 3 shards, RF=2, 64 MB |
| `delete_requires_min_writes_false_allows_degraded` | delete_requires_min_writes=false allows delete with degraded shards | 3 shards, RF=2, 64 MB |
| `delete_requires_min_writes_true_enforces_quorum` | delete_requires_min_writes=true enforces quorum on delete | 3 shards, RF=2, 64 MB |
| `flat_config_delete_requires_min_writes` | Flat config delete_requires_min_writes field parsed correctly | Config text |
| `flat_config_delete_requires_min_writes_default_false` | Flat config without delete_requires_min_writes defaults to false | Config text |
| `new_key_cleaned_up_on_insufficient_writes` | New key cleaned up from partial shards on InsufficientWrites | 3 shards, RF=2, 64 MB |
| `overwrite_not_cleaned_up_on_insufficient_writes` | Existing key NOT cleaned up on failed overwrite (data preserved) | 3 shards, RF=2, 64 MB |
| `rf3_min_writes_3_strict_quorum_all_shards_healthy` | RF=3 min_writes=3: succeeds when all 3 shards healthy | 3 shards, RF=3, 64 MB |
| `rf3_min_writes_3_detach_one_shard_fails` | RF=3 min_writes=3: fails when one shard detached | 3 shards, RF=3, 64 MB |
| `rf3_min_writes_2_detach_one_shard_succeeds` | RF=3 min_writes=2: succeeds with one shard detached | 3 shards, RF=3, 64 MB |

---

## Multipart Edge Cases E2E Tests (tests/multipart_edge_cases_e2e.rs)

Multipart upload edge cases: abort-then-reupload, multiple abort cycles,
shard offline during upload, multi-part assembly, concurrent tracking,
and put_multipart_opts.

| Test | What it covers | Config |
|------|----------------|--------|
| `multipart_abort_then_reupload_same_key` | Abort multipart then re-upload same key; new upload succeeds, tracking cleared | 3 shards, RF=2, 64 MB |
| `multipart_multiple_abort_cycles` | 3 abort cycles followed by successful upload; object has final content | 3 shards, RF=2, 64 MB |
| `multipart_shard_offline_before_complete` | Shard goes offline after put_part but before complete; complete still succeeds | 4 shards, RF=2, 64 MB |
| `multipart_multiple_parts_assembled_correctly` | Multiple parts assembled in order; final object is concatenation of parts | 3 shards, RF=2, 64 MB |
| `multipart_concurrent_uploads_tracked` | Multiple concurrent multipart uploads tracked independently | 3 shards, RF=2, 64 MB |
| `put_multipart_opts_completes_successfully` | put_multipart_opts with PutMultipartOptions completes successfully | 3 shards, RF=2, 64 MB |

---

## Multipart Reliability E2E Tests (tests/multipart_reliability_e2e.rs)

Multipart reliability: tracking leak on insufficient writes, orphan data
cleanup, metadata preservation on replicas, and happy-path verification.

| Test | What it covers | Config |
|------|----------------|--------|
| `bug1_tracking_leak_on_insufficient_writes` | Multipart tracking cleared after InsufficientWrites on complete | 3 shards, RF=3, min_writes=3, 64 MB |
| `bug2_orphan_data_after_insufficient_writes` | No orphan data left after multipart InsufficientWrites | 3 shards, RF=3, min_writes=3, 64 MB |
| `bug3_metadata_not_preserved_on_replicas` | Multipart metadata preserved on all replicas after complete | 3 shards, RF=2, 64 MB |
| `happy_path_multipart_tracking_cleared` | Happy-path multipart: tracking cleared after successful complete | 3 shards, RF=2, 64 MB |
| `happy_path_single_shard_multipart` | Single-shard multipart upload completes successfully | 1 shard, RF=1, 64 MB |

---

## Performance Benchmark (tests/perf_bench.rs)

Multi-phase throughput benchmark.

| Test | What it covers | Config |
|------|----------------|--------|
| `perf_benchmark_suite` | Sequential writes (40K small, 2.5K medium, 600 large), sequential reads, random reads (2K ops), catalog rebuild, per-shard stats | 3 shards, RF=2, 4 GB |

Run with output:
```bash
CARGO_TARGET_DIR=~/build-sharded ~/.cargo/bin/cargo test --release --test perf_bench -- --nocapture
```

---

## Repair E2E Tests (tests/repair_e2e.rs)

Repair operations: repair_replication_sweep, mirror_sync, partitioned_sync,
sync_and_reattach, probe_store, over_replication_trim, re_replication_sweep,
replicate_object, verify_object, verify_all, plan_repair_replication,
validate_shard_access, find_replication_target, pick_excess_shard, cascade
failure, drain_shard, metadata preservation, concurrent write race, and
batch trimming.

| Test | What it covers | Config |
|------|----------------|--------|
| `repair_replication_sweep_restores_rf` | Detach shard, repair_replication_sweep re-replicates under-replicated objects. Second sweep is no-op. | 3 shards, RF=2, 64 MB |
| `repair_replication_sweep_trims_over_replicated` | Manually over-replicate one object, repair_replication_sweep trims to exactly RF | 3 shards, RF=2, 64 MB |
| `repair_replication_sweep_batch_size_limits` | repair_replication_sweep respects batch_size limit | 3 shards, RF=2, 64 MB |
| `mirror_sync_copies_missing_and_deletes_stale` | Mirror (2 shards, RF=2): detach 1, write/delete while offline, mirror_sync copies new and deletes stale | 2 shards, RF=2, 64 MB |
| `partitioned_sync_repairs_under_replicated` | Detach shard, partitioned_sync replicates under-replicated objects | 3 shards, RF=2, 64 MB |
| `partitioned_sync_restores_detached_shard` | partitioned_sync re-replicates after shard detach using original_stores handle; documents catalog-preserved-after-detach behavior | 3 shards, RF=2, 64 MB |
| `sync_and_reattach_state_transitions` | sync_and_reattach transitions shard: Offline -> Syncing -> Healthy | 3 shards, RF=2, 64 MB |
| `sync_and_reattach_mirror_vs_partitioned` | sync_and_reattach works in both mirror and partitioned modes | 2 & 3 shards, RF=2, 64 MB |
| `probe_store_healthy_returns_true` | probe_store on healthy store returns true | 1 shard, RF=1, 64 MB |
| `re_replication_sweep_after_detach` | re_replication_sweep re-replicates immediately after shard detach | 3 shards, RF=2, 64 MB |
| `over_replication_trim_removes_excess` | Manually over-replicate, over_replication_trim removes excess replicas | 3 shards, RF=2, 64 MB |
| `verify_object_all_replicas_consistent` | verify_object confirms replicas consistent with catalog CRC32C | 3 shards, RF=2, 64 MB |
| `verify_object_not_found` | verify_object on nonexistent key returns error | 3 shards, RF=2, 64 MB |
| `verify_all_objects_consistent` | verify_all on 10 objects: all OK with 0 errors | 3 shards, RF=2, 64 MB |
| `verify_all_with_prefix_filter` | verify_all with prefix filter checks only matched objects | 3 shards, RF=2, 64 MB |
| `verify_object_with_offline_replica` | verify_all with offline replica: no mismatches (offline OK) | 3 shards, RF=2, 64 MB |
| `plan_repair_replication_empty_cluster` | plan_repair_replication on empty cluster returns empty plan | 3 shards, RF=2, 64 MB |
| `plan_repair_replication_detects_under_replicated` | plan_repair_replication after detach: actions match under-replicated count | 3 shards, RF=2, 64 MB |
| `plan_repair_replication_respects_batch_size` | plan_repair_replication with batch_size=2 capped at 2 actions | 3 shards, RF=2, 64 MB |
| `validate_shard_access_all_healthy` | validate_shard_access on 3 healthy shards: all accessible | 3 shards, RF=2, 64 MB |
| `validate_shard_access_with_offline` | validate_shard_access: offline shard reports inaccessible | 3 shards, RF=2, 64 MB |
| `find_replication_target_excludes_holding_shards` | find_replication_target returns non-holding shard; None if all hold it | 3 shards, RF=2, 64 MB |
| `pick_excess_shard_returns_valid_shard` | pick_excess_shard returns None at RF, valid shard when over-replicated | 3 shards, RF=2, 64 MB |
| `pick_excess_shard_respects_free_space` | pick_excess_shard trims from shard with least free space when all shards hold the object | 3 shards, RF=2, 64 MB |
| `cascade_failure_three_shards_offline_rf3` | 3 of 5 shards offline with RF=3; partial reads, repair-replication detects issues, full recovery via sync_and_reattach | 5 shards, RF=3, 64 MB |
| `cascade_failure_rf1_all_data_on_offline_shard_unreadable` | RF=1 with 2/3 shards offline; only objects on surviving shard readable | 3 shards, RF=1, 64 MB |
| `drain_shard_concurrent_with_writes` | drain_shard runs concurrently with new writes; completes with zero errors | 4 shards, RF=1, 64 MB |
| `repair_replication_sweep_drops_metadata` | repair_replication_sweep with registry preserves TLV metadata on copied shard | 3 shards, RF=2, 64 MB |
| `replicate_object_preserves_metadata` | replicate_object with raw_refs uses put_with_meta; TLV metadata survives on target shard | 3 shards, RF=2, 64 MB |
| `partitioned_sync_concurrent_write_race` | partitioned_sync during concurrent direct writes to detached shard; documents race behavior | 3 shards, RF=2, 64 MB |
| `re_replication_sweep_restores_rf` | re_replication_sweep restores RF after detach (separate from repair_replication_sweep) | 3 shards, RF=2, 64 MB |
| `drain_shard_preserves_metadata` | drain_shard preserves TLV metadata on moved objects | 3 shards, RF=1, 64 MB |
| `over_replication_trim_respects_batch_size` | over_replication_trim with batch_size limits number of trims | 3+1 shards, RF=2, 64 MB |
| `repair_replication_sweep_executes_re_replication_and_trim` | repair_replication_sweep executes both re-replication and over-replication trim | 3 shards, RF=2, 64 MB |
| `repair_replication_sweep_trims_over_replicated_standalone` | repair_replication_sweep Phase 2 trims over-replicated objects (standalone scenario) | 3 shards RF=3 -> RF=2, 64 MB |
| `mirror_sync_fails_when_no_healthy_sources` | mirror_sync with no healthy source shards produces 0 copies or error | 3 shards, RF=2, 64 MB |
| `find_replication_target_returns_none_when_all_hold` | find_replication_target returns None when all shards hold the object | 3 shards, RF=3, 64 MB |
| `pick_excess_shard_returns_none_at_rf` | pick_excess_shard returns None when replicas == RF | 3 shards, RF=1, 64 MB |
| `re_replication_sweep_ignores_degraded_shard` | Degraded replicas don't count as healthy; re_replication_sweep re-replicates from them | 3 shards, RF=2, 64 MB |
| `repair_replication_sweep_trim_leaves_orphaned_sidecar` | Phase 2 trim calls cleanup_sidecar_maybe; no orphaned sidecar after trim | Raw + 2 InMemory, RF=2 |
| `plan_repair_replication_reports_actions` | Dry-run plan reports correct replication actions without modifying catalog | 3 shards, RF=2, 64 MB |

---

## Shard Lifecycle E2E Tests (tests/shard_lifecycle_e2e.rs)

Shard offline tracking, free-space metadata, read ordering, drain operations,
RawRefRegistry helpers, Syncing state exclusion, jump-hash placement stability,
validate_shard_access, hold_offline/release_hold, set_detach_reason, and
rebuild_catalog_for_shard behavior.

| Test | What it covers | Config |
|------|----------------|--------|
| `shard_offline_since_none_when_healthy` | All healthy shards have offline_since = None | 3 shards, RF=2, 64 MB |
| `shard_offline_since_set_when_offline` | set_shard_health(Offline) sets offline_since timestamp | 3 shards, RF=2, 64 MB |
| `shard_offline_since_cleared_on_healthy` | set_shard_health(Healthy) clears offline_since to None | 3 shards, RF=2, 64 MB |
| `shard_free_space_default_is_max` | Default free space is u64::MAX | 2 shards, RF=1, 64 MB |
| `shard_free_space_set_and_get` | set_shard_free_space / shard_free_space round-trip | 3 shards, RF=1, 64 MB |
| `shard_free_space_out_of_range` | shard_free_space for invalid ID returns None | 2 shards, RF=1, 64 MB |
| `read_shard_order_returns_catalog_shards` | read_shard_order for known key returns catalog shards | 3 shards, RF=2, 64 MB |
| `read_shard_order_falls_back_for_unknown_key` | read_shard_order for unknown key returns fallback (all healthy shards) | 3 shards, RF=2, 64 MB |
| `read_shard_order_excludes_offline_shards_in_fallback` | Offline shard excluded from fallback read order | 3 shards, RF=2, 64 MB |
| `drain_shard_moves_exclusive_objects` | drain_shard moves objects only on victim to survivor cluster | 3 shards, RF=1, 64 MB |
| `drain_shard_skips_replicated_objects` | drain_shard skips objects already replicated on non-victim shards | 3 shards, RF=2, 64 MB |
| `drain_shard_empty_victim` | drain_shard on empty store: moved=0, skipped=0, errors=0 | 3 shards, RF=1, 64 MB |
| `registry_single_constructor` | RawRefRegistry::single: shard_count=1, all_raw len=1, first_raw Some | 1 shard, 64 MB |
| `registry_all_raw_filters_none` | RawRefRegistry::all_raw skips None slots; shard_count includes them | 3 slots (1 None), 64 MB |
| `registry_first_raw_returns_first_non_none` | first_raw returns first non-None Arc (skips leading None) | 3 slots (first None), 64 MB |
| `registry_first_raw_none_when_all_offline` | first_raw returns None when all slots are None | 2 None slots |
| `syncing_shard_excluded_from_write_targets` | Syncing shard excluded from write target selection; no objects placed on it | 4 shards, RF=2, 64 MB |
| `syncing_shard_excluded_from_select_targets` | target_shards does not include Syncing shard | 3 shards, RF=2, 64 MB |
| `syncing_shard_still_readable` | Data on Syncing shard is still readable via get | 3 shards, RF=2, 64 MB |
| `jump_hash_placement_stable_on_shard_add` | Adding 1 shard to 4 moves <40% of keys (jump consistent hash property) | 4 -> 5 shards, RF=1, 64 MB |
| `validate_shard_access_all_healthy` | validate_shard_access on 4 healthy shards: all reachable | 4 shards, RF=2, 64 MB |
| `validate_shard_access_offline_shard` | validate_shard_access: offline shard reports inaccessible | 4 shards, RF=2, 64 MB |
| `validate_shard_access_detached_shard` | validate_shard_access with detached shard: does not panic | 3 shards, RF=2, 64 MB |
| `drain_shard_copies_sidecar_files_as_real_objects` | BUG: drain_shard should not copy __meta__/ sidecar files as real objects | Raw + InMemory, RF=2, 64 MB |
| `rebuild_catalog_for_shard_does_not_remove_stale_entries` | BUG: rebuild_catalog_for_shard does not purge stale entries after direct shard delete | 3 shards, RF=2, 64 MB |
| `hold_offline_and_release` | hold_offline sets Detached + suppress_replication + reason; release_hold transitions to Offline | 3 shards, RF=2, 64 MB |
| `hold_offline_drain_reason` | hold_offline with DetachReason::Drain and suppress_replication=false | 3 shards, RF=2, 64 MB |
| `set_detach_reason_after_detach_shard` | set_detach_reason adds reason after detach_shard (ProbeFailure, DeviceMissing) | 3 shards, RF=2, 64 MB |
| `drain_shard_moves_exclusive_objects_rf1` | drain_shard with RF=1: moves exclusive objects to survivor cluster | 3 shards, RF=1, 64 MB |

---

## Redistribute Sweep E2E Tests (tests/redistribute_sweep_e2e.rs)

Library-level redistribute_sweep tests: rebalancing imbalanced clusters,
no-op when already balanced, safety checks (aborts with under-replicated),
batch limits, data integrity after moves, RF preservation.

| Test | What it covers | Config |
|------|----------------|--------|
| `redistribute_balances_uneven_shards` | Imbalanced cluster (shard 0 overloaded) rebalanced within tolerance | 4 shards, RF=1, 64 MB |
| `redistribute_noop_when_balanced` | Already balanced cluster produces moved=0 | 4 shards, RF=1, 64 MB |
| `redistribute_aborts_with_under_replicated` | Aborts early if under-replicated objects exist | 4 shards, RF=2, 64 MB, 1 detached |
| `redistribute_respects_batch_size` | batch_size=5 limits total moves to 5 | 4 shards, RF=1, 64 MB |
| `redistribute_preserves_data_integrity` | Data readable with correct content after redistribution | 4 shards, RF=1, 64 MB |
| `redistribute_noop_with_one_shard` | No-op when fewer than 2 healthy shards | 2 shards (1 detached), RF=1, 64 MB |
| `redistribute_noop_empty_cluster` | No-op on empty cluster, moved=0 | 4 shards, RF=1, 64 MB |
| `redistribute_with_replication_preserves_rf` | RF=2 preserved after redistribute (no under-replicated) | 4 shards, RF=2, 64 MB |

---

## Startup Repair E2E Tests (tests/startup_repair_e2e.rs)

Startup under-replication detection and repair after mem-shard loss or
shard detach. Simulates restart scenarios where volatile shards are gone.

| Test | What it covers | Config |
|------|----------------|--------|
| `mem_shard_loss_detected_after_rebuild` | Mem shard drop causes under-replication after rebuild_catalog | 2 raw + 1 InMemory, RF=2, 64 MB |
| `replicate_restores_rf_after_mem_loss` | repair_replication_sweep restores RF after mem shard loss | 2 raw + 1 InMemory, RF=2, 64 MB |
| `detach_shard_triggers_under_replication` | Detach shard 1 causes find_under_replicated > 0; sweep fixes it | 3 shards, RF=2, 64 MB |

---

## Write Fanout Partial E2E Tests (tests/write_fanout_partial_e2e.rs)

Write fan-out partial failure: quorum behavior when some target shards fail,
retry with fresh targets, InsufficientWrites error, min_writes override,
selective shard Degraded marking, and all-offline scenarios.

| Test | What it covers | Config |
|------|----------------|--------|
| `partial_write_succeeds_when_quorum_met` | RF=2, min_writes=1, shard 0 fails; write succeeds with 1 replica | 3 shards, RF=2, 64 MB, FailingPutStore on shard 0 |
| `partial_write_data_integrity` | All puts succeed (quorum=1), data verified correct after recovery | 3 shards, RF=2, 64 MB |
| `all_targets_fail_triggers_retry` | 4 shards RF=2, shards 0+1 fail; retry on shards 2+3 succeeds | 4 shards, RF=2, 64 MB |
| `insufficient_writes_when_quorum_not_met` | RF=3, min_writes=2, only 1 success returns InsufficientWrites | 3 shards, RF=3, 64 MB |
| `min_writes_1_allows_single_success` | Override min_writes=1 allows single shard success | 3 shards, RF=3, 64 MB |
| `degraded_marking_is_selective` | Only failing shard marked Degraded, others stay Healthy | 3 shards, RF=2, 64 MB |
| `write_path_retries_on_all_targets_offline` | Initial targets offline; put retries on remaining healthy shards | 4 shards, RF=2, 64 MB |
| `write_path_fails_when_no_targets_available` | Put fails when all shards are offline | 2 shards, RF=2, 64 MB |

---
