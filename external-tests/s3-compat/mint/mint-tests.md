# MinIO Mint-style Test Results -- s3s Adapter (`objstrd`)

Tracking compatibility using a portable mint-style test suite (Python/boto3)
run against `objstrd` (s3s-based S3 adapter) backed by `RawObjectStore`.

Unlike the upstream [minio/mint](https://github.com/minio/mint) suite which
requires Docker, these tests run natively with only Python 3 + boto3. This
makes them portable to FreeBSD, macOS, and any Linux distro without container
runtimes.

## What is tested

The test suite (`external-tests/s3-compat/mint/s3_mint_tests.py`) covers the same S3 API
operations that mint's multi-SDK suites test, using boto3 as the client:

| Category | Tests | Description |
|----------|-------|-------------|
| Bucket Operations | 6 | CreateBucket, HeadBucket, ListBuckets, GetBucketLocation, DeleteBucket |
| Put/Get/Head/Delete | 13 | Objects from 0B to 10MB, error cases (404 on missing) |
| Metadata & Content-Type | 2 | Custom x-amz-meta-* headers, Content-Type preservation |
| Copy Object | 4 | Same-bucket copy, large copy, metadata replace, self-overwrite |
| Range Reads | 4 | First N bytes, middle range, suffix range, open-end range |
| Listing (V1 & V2) | 9 | Prefix, delimiter, pagination, continuation tokens, markers |
| Multipart Upload | 7 | Small/large parts, content verify, abort, list parts/uploads, metadata |
| Batch Delete | 2 | DeleteObjects with existing and nonexistent keys |
| Special Characters | 4 | Spaces, deep paths, dots, plus signs in key names |
| Overwrite | 2 | Same key different content, different sizes |
| Transfer Manager | 3 | boto3 high-level multipart upload (11MB, 65MB), download verify |
| ETag | 2 | ETag presence, consistency for same content |

**Total: 58 tests**

## How to run

Scripts live in `external-tests/s3-compat/mint/`. Run from the repo root:

```bash
# 1. Deploy scripts to VM
scp -r external-tests/s3-compat/mint test@vmserver:/tmp/mint

# 2. Run with 1 worker (recommended for first run)
ssh test@vmserver "pkill -9 -f objstrd 2>/dev/null; chmod +x /tmp/mint/*.sh; bash /tmp/mint/run_mint_tests.sh 1"

# 3. Run with N parallel workers
ssh test@vmserver "pkill -9 -f objstrd 2>/dev/null; bash /tmp/mint/run_mint_tests.sh 4"

# 4. Parse results into readable summary
ssh test@vmserver "python3 /tmp/mint/parse_mint_results.py /tmp/mint_results/log.json"

# 5. Parse as tab-separated (for scripting)
ssh test@vmserver "python3 /tmp/mint/parse_mint_results.py /tmp/mint_results/log.json --tsv"
```

### Prerequisites on the target machine

- Python 3 with boto3 (`pip3 install boto3`)
- `curl` (for server health checks)
- `dd` (for generating test data files)
- No Docker, no Go, no aws-cli, no mc required

### Data files

Test data files (0B to 129MB of random data) are generated on first run by
`create_data_files.sh` into `/tmp/mint_data/`. They are reused across runs.
The 65MB and 129MB files take a few seconds to generate.

---

## Legend

| Symbol | Meaning |
|--------|---------|
| PASS | Test passed |
| FAIL | Assertion or protocol mismatch |
| SKIP | Not applicable (feature not supported) |

---

## Overall Summary

| Run | Date | Passed | Failed | Skipped | Total | Workers | Notes |
|-----|------|--------|--------|---------|-------|---------|-------|
| Run 1 | 2026-03-27 | 57 | 1 | 0 | 58 | 1 | transfer_manager download ETag mismatch |
| Run 2 | 2026-03-27 | 58 | 0 | 0 | 58 | 1 | Fixed range-GET ETag bug  -  all tests pass |

---

## Detailed Results

### Run 1 (2025-03-27, 1 worker)

| # | Test | Status | Duration (ms) | Notes |
|---|------|--------|---------------|-------|
| 1 | test_make_bucket | PASS | 33 | |
| 2 | test_head_bucket | PASS | 11 | |
| 3 | test_head_bucket_nonexistent | PASS | 3 | |
| 4 | test_list_buckets | PASS | 5 | |
| 5 | test_get_bucket_location | PASS | 6 | |
| 6 | test_delete_bucket | PASS | 6 | |
| 7 | test_put_object_0b | PASS | 16 | |
| 8 | test_put_object_1b | PASS | 22 | |
| 9 | test_put_object_1kb | PASS | 15 | |
| 10 | test_put_object_100kb | PASS | 20 | |
| 11 | test_put_object_1mb | PASS | 40 | |
| 12 | test_put_object_5mb | PASS | 150 | |
| 13 | test_put_object_6mb | PASS | 145 | |
| 14 | test_put_object_10mb | PASS | 271 | |
| 15 | test_head_object | PASS | 12 | |
| 16 | test_head_object_nonexistent | PASS | 8 | |
| 17 | test_delete_object | PASS | 11 | |
| 18 | test_delete_object_nonexistent | PASS | 5 | |
| 19 | test_get_object_nonexistent | PASS | 4 | |
| 20 | test_put_object_with_metadata | PASS | 10 | |
| 21 | test_put_object_with_content_type | PASS | 11 | |
| 22 | test_copy_object | PASS | 35 | |
| 23 | test_copy_object_large | PASS | 299 | |
| 24 | test_copy_object_replace_metadata | PASS | 62 | |
| 25 | test_copy_object_overwrite_self | PASS | 24 | |
| 26 | test_get_object_range_first | PASS | 19 | |
| 27 | test_get_object_range_middle | PASS | 14 | |
| 28 | test_get_object_range_suffix | PASS | 14 | |
| 29 | test_get_object_range_open_end | PASS | 15 | |
| 30 | test_list_objects_v2_basic | PASS | 78 | |
| 31 | test_list_objects_v2_with_delimiter | PASS | 94 | |
| 32 | test_list_objects_v2_max_keys | PASS | 93 | |
| 33 | test_list_objects_v2_continuation | PASS | 106 | |
| 34 | test_list_objects_v2_start_after | PASS | 70 | |
| 35 | test_list_objects_v1 | PASS | 69 | |
| 36 | test_list_objects_v1_with_delimiter | PASS | 76 | |
| 37 | test_list_objects_v1_marker | PASS | 50 | |
| 38 | test_list_objects_empty_prefix | PASS | 4 | |
| 39 | test_multipart_upload_small | PASS | 286 | |
| 40 | test_multipart_upload_10mb | PASS | 365 | |
| 41 | test_multipart_upload_content_verify | PASS | 237 | |
| 42 | test_multipart_abort | PASS | 141 | |
| 43 | test_list_parts | PASS | 296 | |
| 44 | test_list_multipart_uploads | PASS | 17 | |
| 45 | test_multipart_with_metadata | PASS | 211 | |
| 46 | test_delete_objects_batch | PASS | 73 | |
| 47 | test_delete_objects_mixed | PASS | 19 | |
| 48 | test_put_object_special_chars | PASS | 29 | |
| 49 | test_put_object_deep_path | PASS | 24 | |
| 50 | test_put_object_dots_in_key | PASS | 15 | |
| 51 | test_put_object_plus_in_key | PASS | 15 | |
| 52 | test_overwrite_object | PASS | 24 | |
| 53 | test_overwrite_object_different_size | PASS | 28 | |
| 54 | test_transfer_manager_upload_11mb | PASS | 367 | |
| 55 | test_transfer_manager_upload_65mb | PASS | 2302 | |
| 56 | test_transfer_manager_download_verify | PASS | 401 | Fixed: use stored metadata ETag instead of range-slice MD5 |
| 57 | test_etag_present_on_put | PASS | 31 | |
| 58 | test_etag_consistent | PASS | 42 | |

### Failure Analysis

All tests pass as of Run 2. Run 1 had one failure:

**test_transfer_manager_download_verify** (fixed in Run 2): `get_object` was computing the
ETag from `body_bytes`  -  which for range GET requests contains only the requested slice.
This produced a different MD5 than what `head_object` returns (the full-object MD5), causing
boto3 TransferManager's `If-Match` header on ranged GETs to receive a `412 PreconditionFailed`.

Fix: use the ETag stored in per-object metadata sidecar at write time (same source as
`head_object`), falling back to MD5-of-body only when no stored ETag exists. Change is in
`objstrd/src/adapter.rs` in `get_object`.
