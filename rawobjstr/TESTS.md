# RawObjectStore Test Suite

Run all: `cargo test --release`
Run one file: `cargo test --release --test cli_tools`

| File | Focus |
|------|-------|
| src/ (unit) | Allocator (incl. bounds validation), extent, index, superblock serialization, lib (compression, align_up, validation) |
| auto_size | Auto-size detection for various image sizes |
| basic_ops | ObjectStore trait basics: put/get/delete/list/copy/head |
| cli_tools | CLI binary E2E: put/get/delete/list/import/export/verify/repair/S3, version/build checks |
| compression_e2e | Zstd/Gzip/Snappy roundtrips, range reads, multipart compression, CLI getraw, mixed compressed/uncompressed, metadata+compression combos |
| concurrency_persistence | Concurrent access, flush/reopen, persistence, multipart+read races, range-read concurrency, flush races |
| concurrent_contention | High-contention stress: 8 threads on 5 hot keys, event socket verification, flush/reopen cycles |
| concurrent_expected_state | 8-task concurrent put/get/delete/copy with expected-state shadow model and event socket verification |
| concurrent_readwrite | Concurrent reads and writes under various conflict patterns |
| corruption_advanced | Targeted bit-rot, partial superblock writes, truncated device, extent-level corruption, repair verification |
| corruption_superblock | Corruption detection and superblock/index recovery |
| crash_checkpoint | Crash recovery with multiple checkpoints and mid-write failures |
| crash_recovery | Interrupted flush, stale superblock, reopen cycles, free-space consistency, crash after concurrent writes |
| datafusion_parquet | DataFusion query on Parquet file in RawObjectStore (requires `--features datafusion`) |
| direct_io | O_DIRECT on/off workload handling and flag persistence |
| e2e_fill | Fill/delete/refill, multipart, fragmentation, copy at scale |
| edge_cases | Range reads, zero-byte files, alignment, put modes, copy integrity, sort order, format options, import/export API, layout_map, multipart edge cases |
| event_protocol | Event socket protocol: auth, PUT/DELETE/FLUSH delivery, max-readers, subscribe helper, store integration, parse roundtrip |
| flock_locking | Writer exclusion via flock, reader coexistence, reload_index, needs_flush, read-only enforcement |
| index_write_amplification | Write-amplification benchmarks: sequential append, random insert, mixed, delete-heavy (all #[ignore]) |
| key_limits | Key name length limits and shard fill distribution with varying key sizes |
| lance_lifecycle | LanceDB-style table lifecycle with remount and compaction (requires `--features lance`) |
| metadata_65k_stress | Max u16 metadata roundtrip, overflow rejection, various large metadata sizes |
| metadata_list_full | put_with_meta/get_metadata roundtrips, head_with_meta, list_with_meta, list_full, update_metadata, put_with_meta_from_file, set_meta_len, metadata range reads |
| metadata_regression | Body-only size invariants: head vs get vs list consistency, range reads, overwrite, metadata roundtrip, encode determinism |
| perf_bench | Sequential/random I/O, overwrite, delete, flush, concurrent-read benchmarks |
| range_reads | Byte-range reads, fuzz random offsets/sizes |
| rename_ops | rename/rename_if_not_exists happy paths, error cases, flush/reopen persistence, metadata preservation, compression, chained renames |
| readonly_writeprotect | Read-only open modes, write-protect flag, modify_flags, CLI --readonly/--full-verify/set-property |
| stress_expected_state | Expected-state shadow model: randomized ops, periodic reopen, crash simulation, fill/refill |
| tombstone_scrub | Tombstone lifecycle, delete/clear tombstones, persist across reopen, scrub_free_space zeroing |

## Unit Tests (src/)

| Module | Test | What it covers |
|--------|------|----------------|
| allocator | `basic_alloc_free` | Alloc two extents, free both, verify coalescing back to one region |
| allocator | `coalesce_three` | Free three adjacent extents, verify they merge into one |
| allocator | `alignment` | Allocations are rounded up to 4 KB alignment |
| allocator | `no_space` | Returns `NoSpace` when request exceeds largest free extent |
| allocator | `from_free_list_filters_out_of_bounds` | Entries before data_start, after data_end, and zero-size are rejected |
| allocator | `from_free_list_rejects_spanning_entry` | Entry starting inside but extending past data_end is rejected |
| extent | `padded_size` | `padded_extent_size()` correctly aligns payload to 4 KB blocks |
| extent | `encode_decode_single_block` | Encode and decode a single-block payload |
| extent | `encode_decode_round_trip` | Multi-block encode/decode round-trip |
| extent | `decode_detects_corruption` | Flipped byte in encoded blocks triggers DataCorruption |
| extent | `decode_block_range_partial` | Partial block-range decode returns correct data |
| index | `shard_round_trip` | Serialize DeviceIndex with files and free_list, verify CRC round-trip |
| index | `shard_corrupt_detection` | Flipping a byte in serialized index triggers `IndexCorrupt` |
| index | `shard_for_key_deterministic` | Same key always maps to the same shard index |
| superblock | `round_trip` | Serialize/deserialize Superblock, verify magic, device_size, txn_id |
| superblock | `corrupt_detection` | Flipping a byte in serialized superblock triggers `SuperblockCorrupt` |
| lib | `align_up_basic` | align_up rounds to 4 KB boundary; already-aligned values unchanged |
| lib | `min_device_size_matches_constant` | min_device_size(DEFAULT_INDEX_SLOT_SIZE) equals MIN_DEVICE_SIZE constant |
| lib | `min_device_size_scales_with_slot` | min_device_size(64 MB) returns larger value than min_device_size(16 MB) |
| lib | `validate_index_slot_size_accepts_valid` | 16 MB, 32 MB, 48 MB, 64 MB accepted |
| lib | `validate_index_slot_size_rejects_invalid` | 0, 15 MB, 17 MB, 1 MB, 4 KB rejected |
| lib | `compression_from_u8_roundtrip` | All valid u8 tags (0-12) round-trip through Compression::from_u8 |
| lib | `compression_from_u8_invalid` | from_u8(99) returns None |
| lib | `compression_from_str_name_roundtrip` | All compression names round-trip through from_str_name/as_str |
| lib | `compression_from_str_name_case_insensitive` | "ZSTD", "Zstd", "zstd" all parse to Compression::Zstd |
| lib | `compression_from_str_name_invalid` | Unknown name returns None |
| lib | `compression_display` | Display trait formats correctly (e.g. "zstd", "gzip0") |
| lib | `compression_default_is_none` | Default::default() returns Compression::None |
| lib | `compression_compress_none_returns_none` | Compression::None.compress() returns None |
| lib | `compression_decompress_none_returns_copy` | Compression::None.decompress() returns input unchanged |
| lib | `compression_zstd_roundtrip` | compress then decompress returns original data |
| lib | `compression_snappy_roundtrip` | compress then decompress returns original data |
| lib | `compression_gzip_roundtrip` | compress then decompress returns original data |
| lib | `error_display_includes_details` | NotFound error Display includes the path |
| lib | `error_display_data_corruption` | DataCorruption error Display includes the message |

## Auto-Size Tests (tests/auto_size.rs)

64 MB device. Tests automatic device-size detection and fill correctness.

| Test | What it covers |
|------|----------------|
| `auto_size_fills_correctly` | Creates various image sizes, auto-detects device boundaries, fills to capacity, verifies all objects |

## Integration Tests (tests/basic_ops.rs)

64 MB test device per test.

| Test | What it covers |
|------|----------------|
| `put_get_round_trip` | Write "hello world", read it back, verify exact match |
| `put_overwrite` | Write "version1", overwrite with "version2", verify latest wins |
| `delete_file` | Write, delete, confirm get returns error |
| `get_not_found` | Get non-existent path returns error |
| `list_files` | Write 3 files across prefixes, list all (3), list with prefix (2) |
| `list_with_delimiter_test` | Directory-style listing: objects vs common_prefixes |
| `copy_file` | Copy src to dst, verify both exist with correct data |
| `head_file` | `head()` returns correct size and location |
| `flush_and_reopen` | Write, flush, drop, reopen  -  data persists |
| `large_file` | Write and read back a 1 MB file |
| `get_range` | Byte-range reads (`0..5`, `6..11`) on "hello world" |

## E2E Tests (tests/e2e_fill.rs)

1 GB test device per test (except where noted).

| Test | What it covers |
|------|----------------|
| `fill_1gb_delete_and_reuse` | Fill 1 GB with 988 x 1 MB chunks, verify all, confirm NoSpace on overflow, delete every other chunk (494), refill freed space (494), flush + reopen, verify persistence |
| `concurrent_read_write` | 10 writer tasks + 10 reader tasks hitting the store simultaneously via `Arc<RawObjectStore>`  -  tests thread safety of the `Mutex<Inner>` design |
| `drop_without_flush_recovers_last_checkpoint` | Phase 1: write 10 files + flush. Phase 2: write 10 more + drop (no flush). Phase 3: reopen  -  only phase 1 data survives. Simulates crash/unmount mid-write |
| `overwrite_reclaims_space` | Fill N-1 slots, overwrite an existing file (alloc-before-free uses the spare slot), verify overwrite at capacity boundary, confirm 100%-full blocks overwrite, delete + replace |
| `multipart_upload_e2e` | Multipart upload with 5 x 1 MB parts, read back and verify all 5 MB concatenated correctly |
| `multipart_abort_no_leak` | Start multipart, add 2 parts, abort  -  no file created, no space leaked, list returns empty |
| `various_file_sizes` | Writes and verifies 22 sizes from 1 byte to 10 MB including block-alignment boundaries (4095, 4096, 4097), Lance v2.2 inline thresholds (32 KB, 64 KB -/+1), and large blob thresholds (4 MB -/+1). Zero-byte objects are not supported. Also checks `head()` returns correct sizes |
| `put_mode_create_rejects_duplicate` | `PutMode::Create` succeeds first time, returns `AlreadyExists` second time, original data untouched |
| `fragmentation_stress` | 5 cycles of: fill 200 x 1 MB + delete all. Then full refill of entire device. Tests allocator coalescing under repeated fragmentation |
| `random_sizes_fill_delete_refill` | Random-sized objects (1 KB - 4 MB) until full, verify all, random 50% delete, refill freed space  -  real-world fragmentation pattern |
| `copy_at_scale` | Write 100 files, copy all to new prefix, verify both sets, `copy_if_not_exists` rejection + success, final count = 201 |
| `multiple_flush_reopen_cycles` | 10 cycles of: write 5 files + flush + reopen-and-verify. Confirms checkpoint chaining across 10 transactions |
| `list_with_delimiter_at_scale` | Deep directory tree (`tables/t1/data/`, `tables/t1/versions/`, `_transactions/`, etc.). Verifies `list_with_delimiter` at root, each nesting level, and flat `list()` |
| `concurrent_overwrite_same_key` | 20 tasks each overwriting the same key 10 times  -  last writer wins, data is valid, no corruption |
| `error_semantics` | `get`, `delete`, `head`, `copy` on non-existent paths all return proper `object_store::Error::NotFound` variants |

## Concurrency & Persistence Tests (tests/concurrency_persistence.rs)

64 MB test device per test.

| Test | What it covers |
|------|----------------|
| `reopen_after_format_no_extra_flush` | Reopen freshly-formatted store without extra flush |
| `many_flush_cycles` | Repeated flush/reopen cycles with data |
| `flush_alternates_index_regions` | Index flush alternates between A/B regions |
| `fragmented_allocator_survives_reopen` | Allocator bitmap persistence after put/delete interleaving |
| `multiple_open_close_cycles` | Many open/close cycles retaining data |
| `allocator_fill_delete_refill` | Fill device, delete, refill  -  freelist correctness |
| `device_full_error` | NoSpace error when device is out of space |
| `thousand_tiny_files` | 1000 small files persisted & verified |
| `many_files_with_various_prefixes` | Files across many nested prefixes |
| `concurrent_50_readers` | 50 concurrent readers on the same key |
| `concurrent_reads_during_writes` | Reads interleaved with concurrent writes |
| `concurrent_get_overwrite_race` | Get vs overwrite race condition |
| `concurrent_delete_read_race` | Delete vs read race condition |
| `concurrent_copy_operations` | Concurrent copy operations |
| `concurrent_multipart_uploads` | Concurrent multipart uploads |
| `get_result_metadata_is_correct` | GetResult metadata fields (size, location, range) |
| `get_range_result_metadata` | Metadata on range-read results |
| `interleaved_write_flush_write` | Write, flush, write more  -  interleaved persistence |
| `flush_after_delete_persists` | Flush after deletes correctly persists state |
| `random_operation_mix_stress` | Random mix of put/get/delete/list stress test |
| `error_messages_are_descriptive` | Verifies error messages contain useful info |
| `concurrent_reads_during_multipart_upload` | 10 readers read pre-existing files while 1 task does 10-part multipart upload; multipart invisible until complete |
| `concurrent_multipart_and_probes` | 2 multipart uploaders + 2 probers GET on in-progress keys; must see NotFound or complete file, never partial |
| `concurrent_range_reads_during_writes` | 5 tasks do bounded range reads on pre-existing files while a writer creates 50 new files; byte-exact verification |
| `concurrent_range_reads_during_overwrites` | Suffix range reads race with overwrites of same files; each read must be all-v0 or all-v1, never mixed |
| `flush_race_with_concurrent_ops` | 1 writer + 1 flusher + 1 reader concurrently; reopen after test recovers all 100 files |
| `flush_race_with_deletes` | Deleter removes even-numbered files while flusher races; reopen confirms only odd files survive |

## Edge-Case Tests (tests/edge_cases.rs)

64 MB device. Exhaustive coverage of boundary conditions.

| Test | What it covers |
|------|----------------|
| `get_range_*` | Bounded, offset, suffix range reads  -  EOF, zero-length, exceeds-size, mid-file |
| `zero_byte_file_*` | Zero-byte puts are rejected (`EmptyPayload` error); zero-byte objects are not supported |
| `*alignment_boundary*` | Files at 4064 and 4065 byte boundaries |
| `one_byte_file` | Single-byte file round-trip |
| `various_sizes_systematic` | Systematic test across many file sizes |
| `put_mode_*` | PutMode::Update, Create (new key), Create (rejects existing) |
| `copy_*` | Source not found, if_not_exists happy/missing/exists, self-copy |
| `delete_*` | Not-found error, delete-then-re-put |
| `list_*` | Empty store, delimiter, prefix no-match, nested dirs, partial name |
| `head_*` | Missing file, correct metadata |
| `multipart_*` | Single part, zero parts, abort+new, many small parts |
| `format_*` | Device too small, minimum size |
| `overwrite_*` | Smaller->larger, same key 100x |
| Path tests | Deeply nested (13 levels), special chars, case sensitivity, 200-char name |
| `display_and_debug` | Display/Debug trait impls |
| `format_with_*_index_slots` | Configurable 32 MB/64 MB slots, reject invalid sizes |
| `put_mode_update_with_{etag,version}_rejected` | PutMode::Update with e_tag/version precondition returns Precondition error |
| `list_with_delimiter_{objects,root}_sorted` | Objects and common_prefixes returned in lexicographic order |
| `copy_various_sizes_integrity` | Raw-block copy for 11 sizes (0 B to 1 MB), data integrity verified |
| `copy_overwrites_destination` | Copy overwrites existing destination, source unchanged |
| `copy_persists_after_flush` | Copied file survives flush + reopen |
| Additional edge cases | `superblock_stores_slot_capacity`, `default_format_uses_16mb_slots` |
| `format_rejects_max_key_over_64k` | format_with_options with max_key_length > 65536 returns MaxKeyLengthTooLarge error |
| `format_clamps_max_key_to_shard_ceiling` | max_key_length larger than shard slot ceiling is silently clamped; put succeeds at clamped limit |
| `max_key_length_default_is_1024` | format() with no max_key_length produces a store with max_key_length == 1024 |
| `max_key_length_persists_across_reopen` | max_key_length set at format time survives flush + reopen via device_info() |
| `key_length_stress` | 900-byte key survives put/get/delete lifecycle |
| `object_size_stress` | Various object sizes from 1 B to 4 MB round-trip correctly after flush+reopen |
| `put_empty_bytes_multiple_keys` | Zero-byte puts rejected for multiple keys (EmptyPayload) |
| `overwrite_data_with_empty` | Overwriting an existing key with empty bytes is rejected |
| `format_rejects_15mb_index_slots` | 15 MB index slots rejected (not a valid multiple of 16 MB) |
| `format_rejects_17mb_index_slots` | 17 MB index slots rejected |
| `format_rejects_1mb_index_slots` | 1 MB index slots rejected |
| `format_accepts_48mb_index_slots` | 48 MB index slots accepted |
| `format_rejects_4k_index_slots` | 4 KB index slots rejected |
| `format_max_key_length_zero` | max_key_length of 0 is rejected or handled correctly |
| `format_full_options_matrix` | Various combinations of index_slot_size and max_key_length all produce valid stores |
| `format_exact_minimum_device_size` | Device exactly at minimum size for given slot size formats successfully |
| `import_from_empty_source` | import_from on empty source store produces 0 imported, 0 errors |
| `export_to_empty_store` | export_to from empty store produces 0 exported, 0 errors |
| `import_from_report_counts` | import_from returns correct imported/errored counts |
| `export_to_report_counts` | export_to returns correct exported/errored counts |
| `import_from_with_prefix_filter` | import_from with prefix only imports matching keys |
| `import_from_no_space_reports_errors` | import_from on full target reports errors without panicking |
| `multipart_many_tiny_parts` | Multipart upload with many tiny (1-byte) parts concatenated correctly |
| `multipart_abort_cleanup` | Multipart abort frees all resources, no space leaked |
| `layout_map_empty_store` | Empty store has no extents, one free region |
| `layout_map_with_objects` | Extents sorted by offset with correct keys/sizes, space accounting |
| `layout_map_after_delete` | Freed space shows in free regions after delete |
| `layout_map_index_regions` | Index regions match device_info |

## Performance Benchmarks (tests/perf_bench.rs)

| Test | What it covers |
|------|----------------|
| `perf_benchmark_suite` | Sequential write, sequential read, overwrite, delete, flush, mixed I/O, concurrent read  -  reports MB/s and ops/sec |

## CLI Tools E2E Tests (tests/cli_tools.rs)

64 MB device. Tests every CLI command via subprocess execution.

| Test | What it covers |
|------|----------------|
| `cli_put_list_get_delete_export_roundtrip` | Put files one-by-one, verify list after each, get all, delete 2, verify list, re-put, export, compare to originals |
| `cli_import_then_get_each` | Import directory at once, get each file individually, verify all match originals |
| `cli_import_export_to_raw_device` | Import to device A, import from A (raw://) into device B, export B, compare to originals |
| `cli_verify_and_repair` | Populate, verify clean, repair (idempotent), verify clean, all files accessible |
| `cli_info_stats` | Info on empty device (0 files), populate, info again (files > 0, data > 0) |
| `cli_format_custom_index_slots` | Format with 32 MB index slots, put a file, verify clean |
| `cli_list_prefix_filter` | List with --prefix for my_table, other_table, nonexistent |
| `cli_get_missing_key_fails` | Get of non-existent key returns non-zero exit |
| `cli_delete_missing_key_is_idempotent` | Delete of non-existent key succeeds (idempotent) |
| `cli_put_overwrite` | Put v1, verify, put v2, verify overwrite, list shows 1 file |
| `cli_export_reimport_fresh_device` | Export device A to dir, import into device B, verify both match originals |
| `cli_large_file_put_get` | 1 MB file put/get round-trip + verify clean |
| `cli_s3_import_graceful_failure` | S3 import produces clear error (no feature/credentials) |
| `cli_many_small_files` | 100 small files: put, list, spot-check get, delete half, export remaining, verify |
| `cli_s3_full_roundtrip` | S3->import->raw A->export->FS->import->raw B (diff index size)->export->S3 (graceful fail without feature/creds) |
| `cli_repair_corrupt_primary_superblock` | Flip primary superblock bytes, open uses backup, repair flushes both fresh, corrupt backup->still works |
| `cli_repair_corrupt_backup_superblock` | Flip backup superblock, repair flushes both, corrupt primary->still works |
| `cli_repair_both_superblocks_destroyed` | Destroy both superblocks, repair fails, re-format recovers |
| `cli_repair_does_not_fix_data_corruption` | Corrupt data extent, verify detects CRC error, repair doesn't fix it, verify still fails |
| `cli_repair_free_space_consistency` | Populate, delete half, repair, free space consistent, remaining files intact |
| `cli_repair_after_churn` | 5 cycles of put-20/delete-20, repair coalesces all free space into 1 fragment |
| `cli_export_to_raw_device` | Export via raw:// URI, flush target, compare to originals |
| `cli_s3_export_graceful_failure` | S3 export produces clear error without feature/credentials |
| `cli_verify_long_shows_ok_files` | Verify without --long hides per-file OK; with --long lists every healthy file |
| `cli_repair_verbose_output` | Repair prints files in index, used extents table, free regions table, flushed status |
| `cli_version_output` | `rawobjstr --version` outputs crate version, git hash, and build date in correct format |
| `cli_version_stable_across_operations` | Version string unchanged after format+info+verify; detects binary replacement during test run |
| `cli_help_shows_descriptions` | `rawobjstr --help` exits 0, lists all 15 commands by name, includes version line |

## Advanced Corruption Tests (tests/corruption_advanced.rs)

64 MB device. Targeted bit-rot, partial writes, truncation, extent-level corruption.

| Test | What it covers |
|------|----------------|
| `partial_primary_superblock_write_recovers_from_backup` | Zero first half of primary SB; store opens from intact backup SB |
| `partial_backup_superblock_write_transparent` | Zero first half of backup SB; store opens normally from primary |
| `truncated_device_cuts_index_region` | Truncate device to remove part of index region; open fails with IndexCorrupt |
| `truncated_device_removes_index_entirely` | Truncate to just past data start; open fails |
| `truncated_device_to_superblocks_only` | Truncate to 8 KB (both SBs only); open fails |
| `truncated_extent_read_fails_gracefully` | Zero second half of a multi-block extent; first two files OK, third corrupted |
| `corrupt_extent_header_magic` | Flip byte in block 0 data region; get returns DataCorruption |
| `corrupt_block_crc_field` | Corrupt the CRC32c bytes in first data block; get returns DataCorruption |
| `corrupt_interior_block_payload` | Flip byte in data payload of second block; CRC mismatch triggers DataCorruption |
| `targeted_bitrot_primary_sb_each_field_individually` | Flip one byte in each SB field (magic through checksum); each opens from backup |
| `targeted_bitrot_both_sbs_same_byte_always_rejects` | Corrupt same byte in both SBs; open fails with SuperblockCorrupt |
| `corrupt_inactive_index_region_is_invisible` | Corrupt the inactive (previous-txn) index region; store opens fine |
| `zero_index_offset_field_in_primary_sb` | Zero the index_region_offset field in primary SB; store falls back to backup |
| `zero_txn_id_field_in_primary_sb` | Zero the txn_id in primary SB; store uses backup (higher txn_id) |
| `verify_report_accurate_after_multi_file_corruption` | Corrupt data blocks of specific files; verify_all reports exact corrupted count |
| `verify_clean_device_all_ok` | Freshly populated device passes verify_all with zero errors |
| `repair_after_data_corruption_preserves_good_files` | Corrupt one file's data; repair succeeds; good files intact, bad one still bad |
| `corrupt_each_block_of_multiblock_extent` | Corrupt one block at a time in a 1 MB extent; each triggers DataCorruption |
| `double_corruption_data_and_index` | Corrupt both index and one data extent; reopen fails or reports errors |
| `overwrite_then_corrupt_old_location` | Overwrite file, corrupt old disk location; new data is still intact |
| `format_over_garbage_data` | Fill device file with random garbage, then format; new store works normally |
| `repair_is_idempotent` | Run repair twice; second run changes nothing, verify_all passes both times |

## Crash Recovery Tests (tests/crash_recovery.rs)

64 MB / 256 MB devices. Simulates crash at various points in the flush sequence.

| Test | What it covers |
|------|----------------|
| `crash_after_primary_sb_update_before_backup` | Write primary SB with new txn_id, leave backup stale; reopen picks the more recent |
| `crash_after_index_write_before_sb_update` | Write new index region but don't update either SB; reopen uses old index (data lost) |
| `cascading_corruption_then_reformat` | Corrupt both SBs, verify open fails, reformat, verify new store works |
| `repair_fixes_primary_superblock` | Corrupt primary SB; repair rebuilds free list and flushes both SBs |
| `repair_fixes_backup_superblock` | Corrupt backup SB; repair flushes both SBs, reopen succeeds |
| `power_loss_before_flush_loses_unflushed_data` | Put files without flush, drop, reopen; unflushed files not present |
| `multi_flush_crash_preserves_all_flushed_batches` | Flush 3 batches, crash (no flush for 4th); first 3 batches intact |
| `many_reopen_cycles_stable` | 20 rounds of put+flush+reopen; all accumulated data persists |
| `rapid_flush_reopen_cycle` | 50 rapid put-one+flush+reopen cycles; all 50 files present at end |
| `crash_after_delete_before_flush_restores_deleted_files` | Delete files, crash without flush; deleted files reappear after reopen |
| `crash_after_overwrite_before_flush_keeps_old_version` | Overwrite file, crash without flush; only works if extent didn't reuse same location |
| `stale_backup_sb_plus_corrupt_primary` | 3 flushes, corrupt primary SB; reopen falls back to backup (txn 2) and loads that data |
| `repair_after_active_index_corruption` | Corrupt active index region; reopen loads old index; repair rebuilds free list |
| `free_space_consistent_after_crash_recovery` | Populate, flush, delete half (no flush), crash, reopen; verify_all reports consistent free space |
| `crash_after_concurrent_writes_recovers_to_last_flush` | Multiple concurrent tasks write, flush, more writes, crash; only flushed data survives |

## Expected-State Stress Tests (tests/stress_expected_state.rs)

Shadow HashMap tracks expected store content.

| Test | What it covers |
|------|----------------|
| `stress_expected_state_random_ops` | 500 random put/get/delete ops on 50-key space; full verification at end |
| `stress_expected_state_with_periodic_reopen` | 300 ops with flush+reopen every 50 ops; verify after each reopen |
| `stress_expected_state_with_copies` | 300 ops including copy; shadow state tracks source->dest content identity |
| `stress_expected_state_crash_simulation` | 6 rounds alternating flush/crash; only flushed data survives reopen |
| `stress_expected_state_list_verification` | 40 files across 4 prefixes; per-prefix list counts; delete subset; verify counts |
| `stress_expected_state_head_verification` | 8 files with sizes 0..64 KB; head() returns correct size for each |
| `stress_expected_state_overwrite_verification` | 20 keys overwritten 100 times with varying sizes; verify content after flush+reopen |
| `stress_expected_state_fill_delete_refill` | Fill device to NoSpace, delete all, refill same count; verify after flush+reopen |
| `stress_expected_state_verify_all_report` | 30 files; verify_all reports correct counts; delete subset; verify_all still correct |
| `stress_long_mixed_workload_with_checkpoints` | 10 rounds x 50 ops (put/get/delete/copy) on 60-key space; flush+reopen+verify each round |

## Tombstone & Scrub Tests (tests/tombstone_scrub.rs)

64 MB device. Tests tombstone lifecycle and free-space scrubbing.

| Test | What it covers |
|------|----------------|
| `tombstone_created_on_corrupt_open` | Corrupt an extent, reopen  -  corrupted file moved to tombstones, not in live index |
| `tombstone_has_correct_metadata` | Tombstone entry preserves original path, size, and last_modified |
| `clean_open_no_tombstones` | Clean device produces empty tombstone list |
| `delete_tombstone_persists_across_reopen` | Delete a tombstone, flush, reopen  -  tombstone stays removed |
| `delete_tombstone_refuses_live_file` | delete_tombstone on a live path returns error |
| `clear_tombstones_bulk` | Multiple tombstones cleared in one call, persist across flush+reopen |
| `tombstones_persist_across_reopen` | Tombstones survive flush+reopen without clear |
| `rewrite_at_tombstoned_path_removes_tombstone` | Put a new file at a tombstoned path  -  tombstone is automatically cleared |
| `scrub_zeros_deleted_object_data` | scrub_free_space zeroes free regions; raw bytes at old extent locations are all zero |
| `scrub_report_is_accurate` | ScrubReport fields (bytes_scrubbed, regions_scrubbed) match expectations |
| `scrub_does_not_corrupt_live_objects` | After scrub, all live objects still readable and verify_all passes |
| `tombstone_and_scrub_combined` | Full lifecycle: corrupt->tombstone->scrub->verify; tombstoned data zeroed, live data intact |

## Read-only & Write-Protect Tests (tests/readonly_writeprotect.rs)

64 MB device. Comprehensive coverage of read-only open modes, write-protect flag, modify_flags, and CLI integration.

| Test | What it covers |
|------|----------------|
| `open_readonly_reports_read_only` | `is_read_only()` returns true for `open_readonly()`, false for `open()` |
| `open_readonly_with_all_modes` | `open_readonly_with_mode()` works with SkipVerify, Default, FullVerify |
| `readonly_reads_succeed` | get, get_range, head, list, list_with_delimiter all work in read-only mode |
| `readonly_rejects_put` | put returns ReadOnly error |
| `readonly_rejects_delete` | delete returns ReadOnly error |
| `readonly_rejects_copy` | copy returns ReadOnly error |
| `readonly_rejects_rename` | rename returns ReadOnly error |
| `readonly_rejects_flush` | flush_index returns ReadOnly error |
| `readonly_rejects_repair` | repair returns ReadOnly error |
| `readonly_rejects_scrub` | scrub_free_space returns ReadOnly error |
| `readonly_rejects_delete_tombstone` | delete_tombstone returns ReadOnly error |
| `readonly_rejects_clear_tombstones` | clear_tombstones returns ReadOnly error |
| `readonly_allows_list_tombstones` | list_tombstones works in read-only mode |
| `readonly_verify_all_succeeds` | verify_all works in read-only mode |
| `readonly_device_info_works` | device_info returns correct data in read-only mode |
| `readonly_does_not_modify_device` | Byte-for-byte comparison: device unchanged after read-only open+operations |
| `modify_flags_write_protect_round_trip` | Set FLAG_WRITE_PROTECT via modify_flags, verify in superblock, clear it |
| `modify_flags_direct_io_round_trip` | Set/clear FLAG_DIRECT_IO via modify_flags |
| `modify_flags_rejects_unknown_bits` | Unknown flag bits return InvalidArgument error |
| `modify_flags_set_and_clear_multiple` | Set and clear multiple flags in one call |
| `write_protect_blocks_rw_open` | Write-protected store rejects put/delete/copy/rename with WriteProtected error |
| `write_protect_allows_readonly_open` | Write-protected store can be opened read-only; reads succeed |
| `write_protect_clear_restores_rw` | Clear write-protect, reopen  -  writes succeed again |
| `modify_flags_persists` | Flags survive across multiple reopen cycles |
| `readonly_drop_is_clean` | Dropping read-only store produces no warnings (no dirty flag) |
| `concurrent_readonly_opens` | Multiple concurrent read-only opens on same device all succeed |
| `readonly_fullverify_corrupt_no_tombstone` | FullVerify in read-only mode detects corruption but doesn't create tombstones |
| `cli_readonly_list` | `rawobjstr list --readonly` shows files |
| `cli_readonly_get` | `rawobjstr get --readonly` retrieves file content |
| `cli_readonly_info` | `rawobjstr info --readonly` shows device info |
| `cli_readonly_verify` | `rawobjstr verify --readonly` runs integrity check |
| `cli_set_property_write_protect` | `rawobjstr set-property --write-protect on/off` toggles flag |
| `cli_set_property_direct_io` | `rawobjstr set-property --direct-io on/off` toggles flag |
| `cli_set_property_invalid_args` | set-property with no flags or bad values fails gracefully |
| `cli_info_shows_write_protect_flag` | `rawobjstr info` output includes write-protect status |
| `cli_full_verify_tombstones` | `--full-verify` with corruption creates tombstones |
| `write_protect_full_lifecycle` | Full lifecycle: format->populate->protect->verify reads->reject writes->unprotect->modify->flush->reopen |
| `readonly_rejects_multipart` | put_multipart returns ReadOnly error |

## Flock Locking & Reload Tests (tests/flock_locking.rs)

64 MB device. Writer exclusion, reader coexistence, reload_index protocol.

| Test | What it covers |
|------|----------------|
| `flock_prevents_double_writer` | Second RW open on same path returns DeviceLocked |
| `flock_writer_plus_readers` | RW + 3 RO handles coexist; all readers see data |
| `flock_multiple_readers_no_writer` | 5 RO handles with no writer all succeed |
| `flock_release_on_drop` | Drop releases flock; new RW open succeeds |
| `flock_explicit_drop_releases` | Explicit drop() mid-scope releases flock |
| `flock_format_while_locked` | format on locked path returns DeviceLocked |
| `reload_index_sees_new_data` | Writer put+flush; reader reload_index sees new file |
| `reload_index_no_change` | reload_index returns false when nothing changed |
| `reload_index_sees_deletes` | Writer delete+flush; reader reload sees file removed |
| `reload_index_sees_overwrites` | Writer overwrite+flush; reader reload sees new content |
| `reload_index_multiple_cycles` | 10 put+flush cycles; reader reloads after each |
| `reload_index_unflushed_invisible` | Unflushed writer data invisible to reader reload |
| `needs_flush_tracks_dirty` | needs_flush: false after format, true after put, false after flush |
| `is_read_only_flag` | is_read_only() true for reader, false for writer |
| `reader_rejects_writes` | put/delete/copy/rename all fail on read-only store |
| `flock_rapid_open_close` | 20 open/close cycles; flock released each time |
| `reload_index_bulk` | 100 files in 2 batches; reader reloads see correct counts |

## Event Socket Protocol Tests (tests/event_protocol.rs)

Unix-only. Tests the EventServer/EventBus protocol over Unix domain sockets.

| Test | What it covers |
|------|----------------|
| `server_start_and_drop_cleanup` | Server binds socket; drop removes socket file |
| `server_rejects_short_secret` | Secret shorter than MIN_SECRET_LENGTH rejected |
| `client_authenticates_ok` | Client sends correct SECRET, receives OK |
| `client_wrong_secret_rejected` | Wrong SECRET receives ERR response |
| `client_receives_flush_event` | emit_flush delivers FLUSH message to client |
| `client_receives_put_and_delete_events` | emit_put delivers PUT, emit_delete delivers DELETE to client |
| `multiple_clients_receive_flush` | 3 clients all receive same FLUSH message |
| `max_readers_limit_enforced` | Connection over max_readers gets ERR or closed |
| `max_readers_capped_at_ceiling` | Requesting > MAX_READERS_CEILING silently caps |
| `events_in_order` | 10 events arrive in order |
| `subscribe_events_end_to_end` | subscribe_events helper invokes callback |
| `subscribe_events_wrong_secret` | subscribe with wrong secret returns error |
| `store_flush_delivers_event` | add_flush_callback + flush_index delivers to subscriber |
| `event_triggers_reload_index` | Writer flush -> event -> reader reload_index sees data |
| `flush_without_callbacks_works` | flush_index works fine with no callbacks attached |
| `client_timeout_no_secret` | Client that sends nothing gets ERR timeout |
| `stale_socket_cleaned_up` | Stale socket file from previous crash is removed on start |
| `max_readers_stress` | 10 concurrent clients all authenticate and receive FLUSH |
| `parse_event_roundtrip` | parse_event correctly parses PUT, DELETE, FLUSH wire format |
| `mixed_events_in_order` | PUT, DELETE, FLUSH events interleaved arrive in order |

## Compression E2E Tests (tests/compression_e2e.rs)

64 MB device. Tests transparent compression with Zstd, Gzip, and Snappy.

| Test | What it covers |
|------|----------------|
| `test_compression_roundtrip_all_algorithms` | Zstd/Snappy/Gzip roundtrip: put compressible data, get back identical |
| `test_small_objects_bypass_compression` | Objects below threshold stored uncompressed |
| `test_compression_threshold_boundary` | Objects at exact threshold boundary: compressed vs not |
| `test_incompressible_data_stored_raw` | Random data that doesn't compress is stored raw |
| `test_all_zeros_compression_ratio` | All-zeros payload achieves high compression ratio |
| `test_range_reads_with_compression` | Byte-range reads on compressed objects return correct data |
| `test_copy_preserves_compression` | Copied object retains compression |
| `test_delete_and_rewrite_compressed` | Delete then rewrite with compression |
| `test_compression_none_no_compression` | Compression::None stores data uncompressed |
| `test_precompressed_data_roundtrip` | Already-compressed data round-trips correctly |
| `test_zstd_various_sizes` | Zstd across many sizes (1 KB to 4 MB) |
| `test_reopen_preserves_compression` | Compressed data survives flush + reopen |
| `test_list_shows_logical_sizes` | list() returns logical (uncompressed) sizes |
| `test_verify_with_compression` | verify_all passes on compressed store |
| `test_cli_getraw` | CLI getraw retrieves raw (compressed) bytes |
| `test_all_gzip_levels` | Gzip levels 0-9 all roundtrip correctly |
| `test_compression_name_roundtrip` | Compression enum name serialization roundtrip |
| `test_mixed_compressed_uncompressed` | Mixed compressed and uncompressed objects coexist |
| `test_getraw_zstd_cli_overwrite_cycles` | CLI getraw with Zstd across overwrite cycles |
| `test_getraw_gzip_cli_overwrite_cycles` | CLI getraw with Gzip across overwrite cycles |
| `test_getraw_compression_magic_bytes` | getraw output starts with correct compression magic bytes |
| `test_getraw_direct_decompress_matches_get` | Manual decompression of getraw output matches get() |
| `test_cli_getraw_chain_external_verify` | CLI getraw piped through decompressor matches original |
| `multipart_zstd_basic` | Multipart upload with Zstd compression |
| `multipart_snappy` | Multipart upload with Snappy compression |
| `multipart_gzip6` | Multipart upload with Gzip level 6 |
| `multipart_zstd_incompressible` | Multipart with incompressible data under Zstd |
| `multipart_zstd_many_tiny_parts` | Multipart with many tiny parts under Zstd |
| `multipart_uncompressed_block_boundary` | Multipart at block boundary without compression |
| `metadata_with_zstd_compression_roundtrip` | Metadata + Zstd compression roundtrip |
| `metadata_with_snappy_compression_roundtrip` | Metadata + Snappy compression roundtrip |
| `metadata_with_gzip_compression_roundtrip` | Metadata + Gzip compression roundtrip |
| `metadata_compressed_persists_after_reopen` | Metadata + compression survives flush + reopen |
| `metadata_compressed_large_body_large_meta` | Large body + large metadata with compression |
| `put_with_meta_from_file_compressed` | File streaming with compression enabled |
| `update_metadata_compressed_store` | update_metadata on compressed store |
| `update_metadata_compressed_persists` | update_metadata on compressed store survives reopen |
| `update_metadata_to_empty_on_compressed` | Clear metadata on compressed object |
| `metadata_range_read_compressed` | Range read on compressed object with metadata |
| `list_with_meta_compressed_store` | list_with_meta on compressed store |
| `get_raw_with_metadata_compressed` | get_raw returns compressed bytes including metadata |
| `compressed_range_read_rejected_above_1gb` | Range reads rejected on compressed objects above 1 GB threshold |

## Metadata List & Full Tests (tests/metadata_list_full.rs)

64 MB device. Tests metadata extensions: put_with_meta, get_metadata, head_with_meta, list_with_meta, list_full, update_metadata, put_with_meta_from_file, set_meta_len, metadata range reads.

| Test | What it covers |
|------|----------------|
| `metadata_roundtrip` | put_with_meta + get_metadata returns identical bytes |
| `metadata_body_unchanged` | Body data unaffected by metadata attachment |
| `metadata_empty_meta` | Zero-length metadata round-trips |
| `metadata_large_body_small_meta` | Large body + small metadata |
| `metadata_overwrite_replaces_meta` | Overwriting object replaces metadata |
| `metadata_not_found` | get_metadata on missing key returns error |
| `head_with_meta_returns_correct_meta_len` | head_with_meta returns correct meta_len |
| `head_with_meta_no_io_for_regular_put` | Regular put shows meta_len=0 |
| `list_with_meta_shows_meta_len` | list_with_meta includes meta_len per entry |
| `list_with_meta_none_prefix_returns_all` | list_with_meta with None prefix returns all files |
| `list_full_returns_sorted_entries` | list_full returns entries sorted by path |
| `list_full_body_size_correct` | list_full body_size matches actual data size |
| `list_full_with_prefix_filter` | list_full respects prefix filter |
| `list_full_empty_store` | list_full on empty store returns empty |
| `list_full_offset_nonzero` | list_full shows non-zero disk offsets |
| `list_full_created_txn_increases` | created_txn increases across flush cycles |
| `update_metadata_replaces_suffix` | update_metadata replaces existing metadata |
| `update_metadata_preserves_body` | update_metadata does not change body data |
| `update_metadata_no_meta_to_some_meta` | Add metadata to object that had none |
| `update_metadata_some_meta_to_no_meta` | Remove metadata from object that had some |
| `update_metadata_not_found` | update_metadata on missing key returns error |
| `update_metadata_persists_across_reopen` | Updated metadata survives flush + reopen |
| `update_metadata_repeated_updates` | Multiple sequential metadata updates |
| `put_with_meta_from_file_basic_roundtrip` | Streaming metadata from file: basic roundtrip |
| `put_with_meta_from_file_zero_metadata` | Streaming put with zero-length metadata |
| `put_with_meta_from_file_large_body_streams` | Large body streamed from file with metadata |
| `put_with_meta_from_file_max_meta` | Maximum metadata size via file streaming |
| `put_with_meta_from_file_persists_after_reopen` | File-streamed metadata survives flush + reopen |
| `set_meta_len_basic` | set_meta_len updates metadata length in index |
| `set_meta_len_not_found` | set_meta_len on missing key returns error |
| `set_meta_len_persists_after_flush` | set_meta_len survives flush + reopen |
| `set_meta_len_zero_clears_metadata` | set_meta_len(0) clears metadata |
| `metadata_range_read_body_portion` | Range read within body portion of metadata object |
| `metadata_range_read_spanning_body_and_meta` | Range read spanning body and metadata boundary |
| `metadata_range_read_only_meta_region` | Range read targeting only metadata region |
| `metadata_suffix_range_read` | Suffix range read on metadata object |

## Metadata 65K Stress Tests (tests/metadata_65k_stress.rs)

64 MB device. Tests per-object metadata at u16 size limits.

| Test | What it covers |
|------|-------------|
| `metadata_max_u16_roundtrip` | 65 535-byte metadata round-trips through put/get/reopen |
| `metadata_overflow_u16_rejected` | 65 536-byte metadata (u16 overflow) is rejected |
| `metadata_various_large_sizes` | Various large metadata sizes (1 KB to 64 KB) survive flush + reopen |

## Rename Tests (tests/rename_ops.rs)

64 MB device. Tests rename() and rename_if_not_exists() happy paths, error cases, persistence, metadata preservation, compression, and chained renames.

| Test | What it covers |
|------|----------------|
| `rename_basic` | Rename moves object to new path |
| `rename_overwrites_existing_destination` | Rename overwrites existing destination |
| `rename_source_not_found` | Rename of non-existent source returns error |
| `rename_preserves_data_across_flush_reopen` | Renamed object survives flush + reopen |
| `rename_to_self_is_noop` | Rename to same path is a no-op |
| `rename_chain` | Chain of renames: A->B->C->D->E->F |
| `rename_if_not_exists_basic` | rename_if_not_exists moves object |
| `rename_if_not_exists_destination_exists_rejected` | rename_if_not_exists rejects existing destination |
| `rename_if_not_exists_source_not_found` | rename_if_not_exists on missing source returns error |
| `rename_if_not_exists_persists_after_flush` | rename_if_not_exists survives flush + reopen |
| `rename_preserves_metadata` | Rename preserves metadata attachment |
| `rename_on_compressed_store` | Rename works on compressed store |

## Index Write Amplification Benchmarks (tests/index_write_amplification.rs)

Measures flush_index() cost under various workload patterns. All tests are `#[ignore]` -- run manually with `--ignored`.

| Test | What it covers |
|------|-------------|
| `bench_w1_sequential_append` | Sequential append: flush per PUT at various counts (100-5000) |
| `bench_w2_batch_append` | Batch append: varying flush intervals (1-500) over 10K objects |
| `bench_w3_mixed_sizes` | Mixed object sizes with flush per PUT |
| `bench_w4_overwrite` | Overwrite-heavy workload: 1000 keys overwritten 2000 times |
| `bench_w5_delete` | Delete-heavy workload: 2000 objects created then deleted |
| `bench_w6_mixed_crud` | Mixed CRUD: 1000 keys, 10K ops, flush every 10 |
| `bench_w7_reopen` | Reopen/recovery time with various index sizes |
| `bench_w8_index_size` | Index size tracking: bytes per entry at various counts |
| `bench_all_workloads` | Runs all workloads sequentially with summary |

## Concurrent Contention Tests (tests/concurrent_contention.rs)

128 MB device. 8 async tasks contending on 5 hot keys with randomized put/get/delete/head/range-read operations. Tests high-contention scenarios including event socket verification and flush/reopen durability cycles.

| Test | What it covers |
|------|-------------|
| `contention_8_threads_5_keys_put_get_delete` | 8 tasks x 400 ops on 5 keys (40% put, 25% get, 15% delete, 10% head, 10% range-read); shadow-state HashMap verifies final consistency |
| `contention_same_keys_with_event_socket` | 8 tasks x 200 ops on 5 keys with event socket subscriber; flusher task fires periodic flushes; verifies PUT/DELETE/FLUSH event counts |
| `contention_many_writers_one_reader_same_key` | 8 writers x 200 overwrites on single key + 1 continuous reader (500+ reads); final value matches last writer |
| `contention_same_keys_flush_reopen_cycles` | 3 rounds of 8 tasks x 100 ops (55% put, 20% get, 25% delete) with flush + reopen between rounds; expected state verified after each cycle |

## Concurrent Expected-State Tests (tests/concurrent_expected_state.rs)

256 MB device. Unix-only (event socket). 8 async tasks share the same key space doing randomized put/get/delete/copy while a `Mutex<HashMap>` shadow model tracks expected content. Event socket subscriber verifies every PUT/DELETE/FLUSH event.

| Test | What it covers |
|------|----------------|
| `concurrent_expected_state_8_tasks` | 8 tasks x 200 ops on 50-key space (put/get/delete/head); event socket records all PUT/DELETE/FLUSH events; final verification of both store content and event stream |
| `concurrent_expected_state_with_flush_reopen` | 4 rounds of 8 tasks x 80 ops; flush + reopen between rounds; expected-state verified pre- and post-reopen each round |
| `concurrent_expected_state_with_copies` | 8 tasks x 150 ops including copy (40% put, 20% copy, 20% get, 10% delete, 10% list); expected-state tracks copy source->dest |
| `concurrent_expected_state_flush_during_ops` | 8 worker tasks + 1 flusher task racing; event socket verifies PUT/DELETE events and multiple FLUSH events with monotonic txn_ids |
| `concurrent_expected_state_high_contention` | 8 tasks x 300 ops on only 5 keys (maximum contention); no yields between ops; expected-state verified after |
| `writer_with_3_readonly_readers_event_reload` | 1 writer + 3 read-only handles; event socket propagates FLUSH; each reader calls reload_index on FLUSH and verifies it sees the same committed data as the writer across 6 rounds of put/delete/flush |

## Crash Checkpoint Tests (tests/crash_checkpoint.rs)

256 MB device. Simulates crash at various checkpoint boundaries with multi-batch workflows.

| Test | What it covers |
|------|----------------|
| `crash_multi_checkpoint_recovery` | Write 3 batches with flush after each, then write a 4th without flush; verify only the 3 flushed batches (60 files) survive reopen |
| `crash_with_deletes_and_overwrites` | Create files, delete some, overwrite others, flush, then do more mutations without flush; verify recovery to last checkpoint state |
| `crash_lance_table_mid_compact` | Simulate Lance table compaction (merge 10 data files into 1, partial delete) then crash before flush; verify pre-compaction state is restored |
| `stress_periodic_flush_then_crash` | Concurrent writes in 5 batches with flush after each, then 50 unflushed writes; verify exactly 100 flushed files survive with correct content |

## Key Limits Tests (tests/key_limits.rs)

Various device sizes (256 MB, MIN_DEVICE_SIZE + 64 MB, 512 MB). Tests key name length limits and shard fill distribution.

| Test | What it covers |
|------|----------------|
| `key_name_length_limit_exact` *(ignored)* | Probes the exact shard slot overflow point by incrementally trying key lengths from 63 KB to 66 KB |
| `max_key_format_different_limits` | Tests max_key_length configs (256, 512, 2048): keys at limit accepted, keys one byte over rejected; also tests copy/rename rejection and persistence across reopen |
| `shard_fill_distribution_tiny_files` | Writes 2000 tiny files with keys 256-1024 bytes; verifies shard fill is uniform (max <= 4x average) and no overflow |

## Range Read Tests (tests/range_reads.rs)

64 MB device. Byte-range reads, fuzz random offsets/sizes.

| Test | What it covers |
|------|----------------|
| `range_read_within_first_block` | Bounded range read fully within block 0 |
| `range_read_spanning_two_blocks` | Bounded range read crossing a single block boundary |
| `range_read_spanning_many_blocks` | 50 KB bounded read spanning ~13 blocks |
| `range_read_entire_file` | Bounded range read of the full 1 MB file |
| `range_read_last_block_boundary` | Bounded read of the last 100 bytes |
| `range_read_suffix_spanning_blocks` | Suffix read of last 10000 bytes (~3 blocks) |
| `range_read_offset_mid_file` | Offset read from mid-file (500000) to end |
| `range_read_exact_block_boundaries` | Bounded read exactly aligned to block 1's payload range |
| `range_read_single_byte_each_block` | Single-byte reads from various block offsets |
| `range_reads_various_sizes` | Full + mid-range + suffix reads on files from 1 byte to 1 MB |
| `fuzz_random_range_reads_1mb` | 200 random bounded range reads on a 1 MB file |
| `fuzz_random_suffix_reads_1mb` | 50 random suffix reads on a 1 MB file |
| `fuzz_random_offset_reads_1mb` | 50 random offset reads on a 1 MB file |
| `fuzz_random_sizes_and_ranges` | 50 random files (1 B-200 KB) with random bounded + suffix reads |
| `range_reads_after_overwrite` | Overwrite a 50 KB file with 100 KB, verify range reads on the new version |
| `range_reads_multiple_files_same_store` | Write 20 random-sized files, do random range reads on each |

## Concurrent Read-Write Tests (tests/concurrent_readwrite.rs)

256 MB device. Concurrent reads and writes under various conflict patterns.

| Test | What it covers |
|------|----------------|
| `concurrent_readers_single_writer` | 10 concurrent readers + 1 writer doing appends, overwrites, and deletes simultaneously |
| `concurrent_multiple_writers` | 5 concurrent writers each writing to separate prefixes; verify isolation and persistence |
| `concurrent_append_and_delete_stress` | 3 appenders + 2 deleters + 5 readers running concurrently on 100 pre-populated files |
| `concurrent_merge_into_pattern` | Simulate merge_into (read source, write to target, delete source) concurrently with readers verifying target never shrinks |
| `stress_all_operations_concurrent` | Full stress: concurrent append + compaction + merge + delete + readers across multiple tables |

## Direct I/O Tests (tests/direct_io.rs)

64 MB device. O_DIRECT on/off workload handling and flag persistence.

| Test | What it covers |
|------|----------------|
| `direct_io_off_workload` | Write/read/list 20 files of varying sizes with buffered I/O (O_DIRECT off) |
| `direct_io_on_workload` *(Linux only)* | Same workload with O_DIRECT enabled |
| `direct_io_flag_persists_in_superblock` *(Linux only)* | Format with O_DIRECT, reopen, verify the flag is auto-detected from the superblock |

## Corruption & Superblock Recovery Tests (tests/corruption_superblock.rs)

64 MB / 256 MB devices. Corruption detection and superblock/index recovery.

| Test | What it covers |
|------|----------------|
| `corruption_detection_data_flip` | Flip a byte in data payload; verify CRC error on read and `verify` CLI detects it |
| `corruption_detection_index_flip` | Corrupt a byte in the index region; verify store falls back to previous index or rejects |
| `corrupt_primary_superblock_recovers_from_backup` | Flip bytes at various positions in the primary superblock; verify backup recovers all data |
| `corrupt_backup_superblock_primary_still_works` | Flip bytes in the backup superblock; verify primary still works |
| `corrupt_both_superblocks_refuses_to_open` | Flip a byte in both superblocks; verify open is refused |
| `zero_both_superblocks_refuses_to_open` | Zero out both entire superblocks; verify open is refused |
| `corrupt_magic_in_both_superblocks` | Zero the magic bytes in both superblocks; verify rejection |
| `sweep_flip_every_byte_primary_superblock` | Flip each of the first 100 bytes of the primary superblock; verify backup always recovers |
| `corrupt_index_region_detects_checksum_mismatch` | Flip bytes at various offsets within an active shard slot; verify checksum mismatch error |
| `zero_index_region_detected` | Zero all active shard slots; verify open fails |
| `corrupt_superblock_index_checksum_field` | Corrupt the index_checksum field in both superblocks; verify rejection |
| `corrupt_active_index_after_multiple_flushes` | After 3 flushes, corrupt the active index region; verify open fails |
| `selective_data_corruption_isolates_damaged_files` | Corrupt specific files' data extents; verify only those files fail CRC, others intact |
| `corrupt_checksum_field_in_both_superblocks` | Flip the same byte in both superblock copies for each of 96 positions; all must be rejected |
| `reformat_after_total_corruption` | Zero both superblocks, verify failure, then re-format and verify the device is usable again |
| `stress_repeated_corrupt_and_recover` | 5 cycles of: format, write, flush, corrupt both superblocks, verify failure |

## Metadata Regression Tests (tests/metadata_regression.rs)

64 MB device. Body-only size invariants: head vs get vs list consistency, range reads, metadata roundtrip.

| Test | What it covers |
|------|----------------|
| `get_returns_body_only_bytes` | `get()` returns body content, not body+metadata |
| `get_returns_body_only_for_large_metadata` | `get()` returns body-only even with 4 KB metadata suffix |
| `head_reports_body_only_size` | `head()` size is body-only, excludes metadata |
| `head_matches_get_length` | `head().size` equals `get().bytes().len()` |
| `list_reports_body_only_size` | `list()` size field is body-only |
| `get_range_bounded_does_not_leak_metadata` | Bounded range extending past body end is clamped, no metadata leaks |
| `get_range_suffix_does_not_leak_metadata` | Suffix read returns tail of body, not tail of body+metadata |
| `get_metadata_returns_empty_for_no_metadata` | `get_metadata()` on a plain object returns empty without panic |
| `get_metadata_roundtrip_exact` | `put_with_meta` then `get_metadata` returns exact metadata bytes |
| `no_metadata_objects_unchanged` | Objects written without metadata have correct body and size in get/head |
