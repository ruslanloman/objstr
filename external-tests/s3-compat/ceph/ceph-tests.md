# Ceph s3-tests Results - s3s Adapter (`objstrd`)

Tracking compatibility against the [ceph/s3-tests](https://github.com/ceph/s3-tests) suite
run against `objstrd` (s3s-based S3 adapter) backed by `RawObjectStore`.

The **http-objst** column shows what `http-object-store` achieves in its Run 8,
so we can track parity and see where we need to catch up.

## How to run

Scripts live in `external-tests/s3-compat/ceph/`. Run from the repo root:

```bash
# On the VM 
# Parallel runner (4 workers, each gets its own objstrd instance + data file)
scp -r external-tests/s3-compat/ceph test@vmserver:/tmp/ceph_tests
ssh test@vmserver "chmod +x /tmp/ceph_tests/*.sh && bash /tmp/ceph_tests/run_ceph_tests.sh 4"

# Results: /tmp/s3s_ceph_results/  (per-worker results + JUnit XML)
# Parse into per-test list:
ssh test@vmserver "python3 /tmp/ceph_tests/parse_junit.py /tmp/s3s_ceph_results" > external-tests/s3-compat/ceph/results/ceph_run7_results.txt
```

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | PASSED |
| ❌ | FAILED - assertion or protocol mismatch |
| ⚠️ | ERROR - test setup/teardown cascade (may pass in isolation) |
| ➡️ | NOT APPLICABLE - requires infra we don't provide (KMS, multi-user accounts) |
| ➖ | Not yet run |

---

## Overall Summary

| Run | Date | Passed | Failed | Errors | Skipped | Total | Notes |
|-----|------|--------|--------|--------|---------|-------|-------|
| *http-objst Run 8* | 25 Mar 2026 | **248** | **491** | **2** | **88** | 829 | Reference baseline |
| Run 1 | 26 Mar 2026 | **33** | **258** | **461** | **77** | 829 | Parallel 4-worker run |
| Run 2 | 26 Mar 2026 | **189** | **536** | **14** | **90** | 829 | Path encoding fix |
| Run 3 | 26 Mar 2026 | **197** | **528** | **14** | **90** | 829 | Path encoding fix |
| Run 4 | 27 Mar 2026 | **229** | **510** | **0** | **90** | 829 | Path encoding fix |
| Run 5 | 26 Mar 2026 | **244** | **495** | **0** | **90** | 829 | Path encoding fix |
| Run 6 | 27 Mar 2026 | **255** | **484** | **0** | **90** | 829 | ETag in head/list/copy/multipart, anon ListBuckets |
| Run 7 | 31 Mar 2026 | **242** | **480** | **17** | **90** | 829 | Two-pass run; 17 residual errors are IAM-scope cascade tests |

---

## 1. Bucket Lifecycle

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_list_empty` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_distinct` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_many` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_basic_key_count` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_delete_nonempty` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_get_location` | ✅ | ⚠️ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_exists_nonowner` | ❌ | ⚠️ | No multi-user support | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_recreate_overwrite_acl` | ❌ | ⚠️ | No ACL support | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_recreate_new_acl` | ❌ | ❌ | No ACL support | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_account_usage` | ✅ | ⚠️ | Unfixable: Ceph RGW `?usage` extension on ListBuckets not supported by s3s | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_head_bucket_usage` | ✅ | ⚠️ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_expected_bucket_owner` | ❌ | ⚠️ | No ownership model | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_head_extended` | ✅ | ⚠️ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_special_key_names` | ❌ | ⚠️ | Assertion failure | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_list_buckets_paginated` | ✅ | ❌ | | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |

---

## 2. Bucket Naming Validation

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_create_naming_bad_starts_nonalpha` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_bad_short_one` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_bad_short_two` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_bad_ip` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_dns_underscore` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_dns_dash_at_end` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_dns_dot_dot` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_dns_dot_dash` | ✅ | ⚠️ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_dns_dash_dot` | ✅ | ⚠️ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 3. Object Listing  -  Delimiters & Prefixes

### Passing delimiter edge cases

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_list_delimiter_unreadable` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_unreadable` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_delimiter_empty` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_empty` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_delimiter_none` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_none` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_delimiter_not_exist` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_not_exist` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Delimiter / prefix tests

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_list_delimiter_basic` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_basic` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_encoding_basic` | ✅ | ❌ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_encoding_basic` | ✅ | ❌ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_delimiter_prefix` | ✅ | ❌ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_prefix` | ✅ | ❌ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_delimiter_prefix_ends_with_delimiter` | ❌ | ❌ | Needs investigation | ❌ | ❌ | ❌ | ❌ | ✅ | ⚠️ |
| `test_bucket_listv2_delimiter_prefix_ends_with_delimiter` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ✅ | ⚠️ |
| `test_bucket_list_delimiter_alt` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_listv2_delimiter_alt` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_list_delimiter_prefix_underscore` | ✅ | ❌ | | ❌ | ❌ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_listv2_delimiter_prefix_underscore` | ✅ | ❌ | | ❌ | ❌ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_list_delimiter_percentage` | ❌ | ⚠️ | Needs investigation | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_listv2_delimiter_percentage` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_list_delimiter_whitespace` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_listv2_delimiter_whitespace` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_list_delimiter_dot` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_delimiter_dot` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_delimiter_not_skip_special` | ❌ | ⚠️ | Needs investigation | ⚠️ | ⚠️ | ❌ | ❌ | ✅ | ⚠️ |
| `test_bucket_list_prefix_alt` | ❌ | ⚠️ | Needs investigation | ⚠️ | ⚠️ | ❌ | ❌ | ✅ | ⚠️ |
| `test_bucket_listv2_prefix_alt` | ❌ | ⚠️ | Same | ⚠️ | ⚠️ | ❌ | ❌ | ✅ | ⚠️ |
| `test_bucket_list_prefix_delimiter_alt` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| `test_bucket_listv2_prefix_delimiter_alt` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| `test_bucket_list_prefix_delimiter_prefix_not_exist` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_prefix_delimiter_prefix_not_exist` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_prefix_delimiter_delimiter_not_exist` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_prefix_delimiter_delimiter_not_exist` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_special_prefix` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 4. Object Listing  -  Pagination & Params

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_list_many` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_fetchowner_defaultempty` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_fetchowner_empty` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_fetchowner_notempty` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_return_data` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| `test_bucket_list_return_data_versioning` | ❌ | ⚠️ | Needs ACL owner data | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_unordered` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_unordered` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_maxkeys_zero` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_maxkeys_zero` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_maxkeys_invalid` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_continuationtoken_empty` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_continuationtoken` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_both_continuationtoken_startafter` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_startafter_unreadable` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_startafter_not_in_list` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_startafter_after_list` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_objects_anonymous_fail` | ❌ | ⚠️ | No auth | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_objects_anonymous_fail` | ❌ | ⚠️ | No auth | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 5. Object CRUD

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_object_write_check_etag` | ✅ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| `test_object_write_cache_control` | ✅ | ⚠️ | | ⚠️ | ⚠️ | ❌ | ✅ | ✅ | ✅ |
| `test_object_write_expires` | ✅ | ⚠️ | | ⚠️ | ⚠️ | ❌ | ✅ | ✅ | ✅ |
| `test_object_set_get_metadata_none_to_good` | ✅ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| `test_object_set_get_metadata_none_to_empty` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_set_get_metadata_overwrite_to_empty` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_set_get_unicode_metadata` | ✅ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `test_object_requestid_matches_header_on_error` | ❌ | ⚠️ | RequestId mismatch | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_response_headers` | ❌ | ⚠️ | Missing response headers | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_content_encoding_aws_chunked` | ❌ | ⚠️ | aws-chunked not handled | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_anon_put` | ❌ | ⚠️ | No auth | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multi_object_delete` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multi_objectv2_delete` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multi_object_delete_key_limit` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_multi_objectv2_delete_key_limit` | ✅ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_zero_size` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_same_bucket` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_diff_bucket` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_verify_contenttype` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_to_itself` | ❌ | ⚠️ | Copy to self without REPLACE | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `test_object_copy_to_itself_with_metadata` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_16m` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_bucket_not_found` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_key_not_found` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_retaining_metadata` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_replacing_metadata` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_canned_acl` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_copy_not_owned_bucket` | ❌ | ⚠️ | No multi-user | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_copy_not_owned_object_bucket` | ❌ | ⚠️ | No multi-user | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## 6. Conditional Headers (If-Match / If-Modified-Since)

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_get_object_ifmatch_good` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_get_object_ifmatch_failed` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_get_object_ifnonematch_good` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_get_object_ifnonematch_failed` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_get_object_ifmodifiedsince_good` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_get_object_ifmodifiedsince_failed` | ❌ | ⚠️ | If-Modified-Since not checked | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_get_object_ifunmodifiedsince_good` | ❌ | ⚠️ | If-Unmodified-Since not checked | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_get_object_ifunmodifiedsince_failed` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_put_object_ifmatch_good` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_put_object_ifmatch_failed` | ❌ | ⚠️ | Conditional PUT not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_object_ifmatch_nonexisted_failed` | ❌ | ⚠️ | Conditional PUT not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_object_ifnonmatch_good` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_put_object_ifnonmatch_failed` | ❌ | ⚠️ | Conditional PUT not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_object_ifnonmatch_overwrite_existed_failed` | ❌ | ⚠️ | Conditional PUT not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_copy_object_ifmatch_good` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_copy_object_ifnonematch_failed` | ✅ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 7. Multipart Upload

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_multipart_upload_empty` | ❌ | ⚠️ | Re-complete not supported | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_upload_small` | ✅ | ⚠️ | | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_upload` | ❌ | ⚠️ | Body limit issue | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_upload_multiple_sizes` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_upload_overwrite_existing_object` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_upload_contents` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_upload_resend_part` | ❌ | ⚠️ | Needs investigation | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_upload_size_too_small` | ❌ | ⚠️ | Min part size not enforced | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_upload_missing_part` | ❌ | ⚠️ | Missing part detection | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_upload_incorrect_etag` | ❌ | ⚠️ | ETag validation on complete | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_abort_multipart_upload` | ✅ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_abort_multipart_upload_not_found` | ✅ | ⚠️ | | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `test_list_multipart_upload` | ❌ | ⚠️ | Needs investigation | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_list_multipart_upload_owner` | ❌ | ⚠️ | Owner not in list response | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_get_part` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_copy_small` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_copy_without_range` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_copy_invalid_range` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_copy_improper_range` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_copy_special_names` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_copy_multiple_sizes` | ❌ | ⚠️ | Needs investigation | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `test_multipart_copy_versioned` | ❌ | ⚠️ | Versioning not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## 8. Pre-signed URLs / Presigned Requests

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_object_raw_get_object_acl` | ❌ | ⚠️ | Pre-signed ACL request | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_put_acl_mtime` | ❌ | ⚠️ | No ACL | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_x_amz_expires_not_expired` | ❌ | ⚠️ | Pre-signed URL expiry | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_x_amz_expires_not_expired_tenant` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_x_amz_expires_out_range_zero` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_get_x_amz_expires_out_max_range` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_x_amz_expires_out_positive_range` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_put_authenticated_expired` | ❌ | ⚠️ | Expired pre-signed PUT | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## 9. POST Form Upload

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_post_object_anonymous_request` | ❌ | ⚠️ | POST form not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_authenticated_request` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_authenticated_no_content_type` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_authenticated_request_bad_access_key` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_set_success_code` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_set_invalid_success_code` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_upload_larger_than_chunk` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_set_key_from_filename` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_ignored_header` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_case_insensitive_condition_fields` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_escaped_field_values` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_success_redirect_action` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_invalid_signature` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_invalid_access_key` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_missing_policy_condition` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_user_specified_header` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_request_missing_policy_specified_field` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_expired_policy` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_wrong_bucket` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_invalid_request_field_value` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_upload_size_rgw_chunk_size_bug` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 10. Authentication & Anonymous Access

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_list_buckets_anonymous` | ✅ | ❌ | | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| `test_list_buckets_invalid_auth` | ❌ | ✅ | Bad auth should return 403 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_list_buckets_bad_auth` | ❌ | ✅ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_objects_anonymous_fail` | ❌ | ⚠️ | Should return 403 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_objects_anonymous_fail` | ❌ | ⚠️ | Same | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_anon_put` | ❌ | ⚠️ | Should return 403 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 11. Access Control Lists (ACL)

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_acl_default` | ❌ | ❌ | GetBucketAcl not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_canned_during_create` | ❌ | ❌ | Canned ACL header ignored | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_canned` | ❌ | ❌ | PutBucketAcl not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_canned_publicreadwrite` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_canned_authenticatedread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_acl_grant_group_read` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_userid_fullcontrol` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_userid_read` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_userid_readacp` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_userid_write` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_userid_writeacp` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_nonexist_user` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_header_acl_grants` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_email` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_grant_email_not_exist` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_acl_revoke_all` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_default` | ❌ | ❌ | GetObjectAcl not implemented | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ |
| `test_object_acl_canned_during_create` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_canned` | ❌ | ❌ | PutObjectAcl not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_canned_publicreadwrite` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_canned_authenticatedread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_canned_bucketownerread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_canned_bucketownerfullcontrol` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_full_control_verify_attributes` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_write` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_writeacp` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_read` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_readacp` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_header_acl_grants` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_private_object_private` | ❌ | ❌ | ACL-gated access | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_private_objectv2_private` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_private_object_publicread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_private_objectv2_publicread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_private_object_publicreadwrite` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_private_objectv2_publicreadwrite` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_publicread_object_private` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_publicread_object_publicread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_publicread_object_publicreadwrite` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_publicreadwrite_object_private` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_publicreadwrite_object_publicread` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_access_bucket_publicreadwrite_object_publicreadwrite` | ❌ | ❌ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## 12. Versioning

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_list_return_data_versioning` | ❌ | ⚠️ | Versioning ETag/metadata | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_concurrent_multi_object_delete` | ❌ | ⚠️ | Versioned delete | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_copy_versioned_bucket` | ❌ | ⚠️ | Versioning not implemented | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_copy_versioned_url_encoding` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_copy_versioning_multipart_upload` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_copy_versioned` | ❌ | ⚠️ | Same | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## 13. Encryption (SSE-C / SSE-KMS / SSE-S3)

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_copy_enc[unencrypted-unencrypted-*]` (3 sizes) | ✅ | ➖ | No encryption involved | ➖ |
| `test_copy_enc[sse-c-unencrypted-*]` (3 sizes) | ✅ | ➖ | Source unencrypted copy works | ➖ |
| `test_copy_enc[sse-kms-unencrypted-*]` (3 sizes) | ✅ | ➖ | Source unencrypted copy works | ➖ |
| `test_encrypted_transfer_*` (4 sizes) | ✅ | ➖ | SSE-C PUT/GET (headers ignored) | ➖ |
| `test_sse_kms_transfer_*` (4 sizes) | ✅ | ➖ | SSE-KMS transfers (no real encryption) | ➖ |
| `test_multipart_sse_c_get_part` | ❌ | ⚠️ | Regressed with multipart | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_copy_enc[sse-s3-*]` (many variants) | ❌ | ➖ | SSE-S3 needs server-side encryption | ➖ |
| `test_copy_enc[sse-c-sse-c-*]` (many variants) | ❌ | ➖ | SSE-C to SSE-C copy | ➖ |
| `test_copy_enc[sse-kms-sse-kms-*]` (many variants) | ❌ | ➖ | SSE-KMS requires KMS service | ➖ |
| *(~500 more encryption variants)* | ❌ | ➖ | |

---

## Target: Tests http-object-store passes (248) that we need to match

These are the ✅ tests from the http-objst column above. Grouped by category with counts:

| Category | http-objst passes | Notes |
|----------|------------------|-------|
| 1. Bucket Lifecycle | 10 | |
| 2. Bucket Naming | 9 | |
| 3. Listing  -  Delimiters & Prefixes | 28 | |
| 4. Listing  -  Pagination & Params | 15 | |
| 5. Object CRUD | 23 | |
| 6. Conditional Headers | 10 | |
| 7. Multipart Upload | 6 | |
| 8. Pre-signed URLs | 0 | Skip |
| 9. POST Form Upload | 0 | Skip |
| 10. Authentication | 1 | |
| 11. ACL | 0 | Skip |
| 12. Versioning | 0 | Skip |
| 13. Encryption (unencrypted variants) | ~17 | |
| **Total target** | **~119 non-enc + ~17 enc ~ 136 easy wins** | Tests where both fail are excluded |

The remaining ~112 of http-objst's 248 passes are the expanded encryption wildcard tests
(`test_copy_enc[*-unencrypted-*]`, `test_encrypted_transfer_*`, `test_sse_kms_transfer_*`)
which each expand to multiple individual pytest parametrizations.

---

## 12+ Additional Tests (auto-generated from ceph suite)

### Atomic / Concurrent Operations

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_atomic_conditional_write_1mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_dual_conditional_write_1mb` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_atomic_dual_write_1mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_dual_write_4mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_dual_write_8mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_multipart_upload_write` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_read_1mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_read_4mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_read_8mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_write_1mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_write_4mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_write_8mb` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_atomic_write_bucket_gone` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Bucket Policy

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_acl` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_allow_notprincipal` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_another_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_deny_self_denied_policy` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_policy_deny_self_denied_policy_confirm_header` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_policy_different_tenant` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_get_obj_acl_existing_tag` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_get_obj_existing_tag` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_get_obj_tagging_existing_tag` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_multipart` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_acl` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_copy_source` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_copy_source_meta` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_grant` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_kms_noenc` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_kms_s3` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_request_obj_tag` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_s3_incorrect_algo_sse_s3` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_s3_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_s3_noenc` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_put_obj_tagging_existing_tag` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_set_condition_operator_end_with_IfExists` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_tenanted_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_policy_upload_part_copy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucketv2_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucketv2_policy_acl` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucketv2_policy_another_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_deny_algo_with_bucket_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_enforced_with_bucket_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_authpublic_acl_bucket_policy_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_bucket_policy_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_nonpublicpolicy_acl_bucket_policy_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_nonpublicpolicy_principal_bucket_policy_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_public_acl_bucket_policy_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_publicpolicy_acl_bucket_policy_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_upload_on_a_bucket_with_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_logging_policy_wildcard` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_logging_policy_wildcard_objects` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_set_get_del_bucket_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Block Public Access

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_block_public_object_canned_acls` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_block_public_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_block_public_policy_with_principal` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_block_public_put_bucket_acls` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_block_public_restrict_public_buckets` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_ignore_public_acls` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Bucket Naming (Good)

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_create_naming_good_contains_hyphen` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_contains_period` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_long_60` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_long_61` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_long_62` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_long_63` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_starts_alpha` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_good_starts_digit` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Versioning

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_logging_copy_objects_bucket_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_copy_objects_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_delete_objects_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_get_objects_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_head_objects_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_mpu_copy_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_mpu_versioned_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_mpu_versioned_s` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_multi_delete_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_put_objects_versioned` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_marker_nonversioned` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_marker_versioned` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_version_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_version_if_match_last_modified_time` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_version_if_match_size` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_version_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_version_if_match_last_modified_time` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_version_if_match_size` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_versioned_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_versioned_tags2` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_versioning_enabled` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_plain_null_version_current_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_object_lock_put_obj_retention_versionid` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_suspend_versioning` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioned_concurrent_object_create_and_remove` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioned_concurrent_object_create_concurrent_remove` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioned_object_acl` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioned_object_acl_no_version_specified` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_bucket_atomic_upload_return_version_id` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_bucket_create_suspend` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_bucket_multipart_upload_return_version_id` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_copy_obj_version` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_multi_object_delete` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_multi_object_delete_with_marker` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_multi_object_delete_with_marker_create` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_create_overwrite_multipart` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_create_read_remove` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_create_read_remove_head` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_create_versions_remove_all` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_create_versions_remove_special_names` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_list_marker` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_plain_null_version_overwrite` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_plain_null_version_overwrite_suspended` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_plain_null_version_removal` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_suspend_versions` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_obj_suspended_copy` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_versioning_stack_delete_merkers` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Object Lock & Retention

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_get_public_block_deny_bucket_policy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_undefined_public_block` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_changing_mode_from_compliance` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_changing_mode_from_governance_with_bypass` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_changing_mode_from_governance_without_bypass` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_delete_multipart_object_with_legal_hold_on` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_delete_multipart_object_with_retention` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_delete_object_with_legal_hold_off` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_delete_object_with_legal_hold_on` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_delete_object_with_retention` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_delete_object_with_retention_and_marker` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_legal_hold` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_legal_hold_invalid_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_obj_lock` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_obj_lock_invalid_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_obj_metadata` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_obj_retention` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_obj_retention_invalid_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_get_obj_retention_iso8601` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_multi_delete_object_with_retention` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_legal_hold` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_legal_hold_invalid_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_legal_hold_invalid_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_enable_after_create` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_invalid_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_invalid_days` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_invalid_mode` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_invalid_status` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_invalid_years` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_lock_with_days_and_years` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention_increase_period` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention_invalid_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention_invalid_mode` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention_override_default_retention` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention_shorten_period` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_put_obj_retention_shorten_period_bypass` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_lock_uploading_obj` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_get_delete_public_block` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_public_block` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### SSE-C Encryption

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_encryption_key_no_sse_c` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_invalid_md5` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_method_head` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_multipart_bad_download` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_multipart_invalid_chunks_1` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_multipart_invalid_chunks_2` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_multipart_upload` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_encryption_sse_c_no_key` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_no_md5` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_other_key` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_post_object_authenticated_request` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_encryption_sse_c_present` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encryption_sse_c_unaligned_multipart_upload` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_get_sse_c_encrypted_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_non_multipart_sse_c_get_part` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### SSE-KMS Encryption

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_sse_kms_default_post_object_authenticated_request` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_default_upload_1b` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_default_upload_1kb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_default_upload_1mb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_default_upload_8mb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_method_head` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_multipart_invalid_chunks_1` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_sse_kms_multipart_invalid_chunks_2` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `test_sse_kms_multipart_upload` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_sse_kms_no_key` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_not_declared` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_post_object_authenticated_request` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_sse_kms_present` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_sse_kms_read_declare` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_kms_transfer_13b` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_sse_kms_transfer_1MB` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `test_sse_kms_transfer_1b` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_sse_kms_transfer_1kb` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### SSE-S3 Encryption

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_sse_s3_default_method_head` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_default_multipart_upload` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_default_post_object_authenticated_request` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_default_upload_1b` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_default_upload_1kb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_default_upload_1mb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_default_upload_8mb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_encrypted_upload_1b` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_encrypted_upload_1kb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_encrypted_upload_1mb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_sse_s3_encrypted_upload_8mb` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Encryption Copy

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_copy_enc[sse-c-sse-c-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-c-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-c-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-kms-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-kms-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-kms-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-s3-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-s3-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-s3-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-c-unencrypted-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-c-unencrypted-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-c-unencrypted-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-c-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-kms-sse-c-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-c-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-c-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-kms-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[sse-s3-sse-c-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-c-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-c-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-s3-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-s3-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-s3-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-unencrypted-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-unencrypted-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-unencrypted-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[sse-s3-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-c-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-c-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-c-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-s3-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-s3-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-s3-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_enc[unencrypted-unencrypted-STANDARD-STANDARD-1024]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[unencrypted-unencrypted-STANDARD-STANDARD-1048576]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[unencrypted-unencrypted-STANDARD-STANDARD-1]` | ➖ | ⚠️ | | ✅ |
| `test_copy_enc[unencrypted-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ✅ |

### Tagging

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_delete_tags_obj_public` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_obj_head_tagging` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_obj_tagging` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_tags_acl_public` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_deletemarker_expiration_with_days_tag` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_header_and_tags_head` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_header_tags_head` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_noncur_tags1` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_tags1` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_tags2` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_tags_anonymous_request` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_tags_authenticated_request` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_put_delete_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_excess_key_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_excess_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_excess_val_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_max_kvsize_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_max_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_modify_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_obj_with_tags` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_tags_acl_public` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_set_bucket_tagging` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_set_multipart_tagging` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### CORS

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_cors_header_option` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_origin_response` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_origin_wildcard` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_get_object` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_get_object_tenant` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_get_object_tenant_v2` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_get_object_v2` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_put_object` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_put_object_tenant` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_put_object_tenant_v2` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_put_object_tenant_with_acl` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_put_object_v2` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_cors_presigned_put_object_with_acl` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_set_cors` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Logging

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_logging_bucket_acl_required` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_bucket_auth_type` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_cleanup_bucket_concurrent_deletion_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_concurrent_deletion_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_concurrent_deletion_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_concurrent_deletion_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_deletion_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_deletion_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_deletion_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_bucket_deletion_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_disabling_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_disabling_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_disabling_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_disabling_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_updating_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_updating_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_updating_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_concurrent_updating_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_disabling_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_disabling_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_disabling_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_disabling_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_updating_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_updating_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_updating_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_cleanup_updating_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_concurrent_flush_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_concurrent_flush_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_concurrent_flush_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_concurrent_flush_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_concurrent_updating_pfx_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_concurrent_updating_pfx_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_concurrent_updating_roll_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_concurrent_updating_roll_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_updating_pfx_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_updating_pfx_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_updating_roll_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_conf_updating_roll_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_copy_objects` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_copy_objects_bucket` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_delete_objects` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_event_type_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_event_type_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_flush_empty` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_flush_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_flush_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_flush_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_flush_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_get_objects` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_head_objects` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_key_filter_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_key_filter_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_mpu_copy` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_mpu_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_mpu_s` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_mtime` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_multi_delete` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_multiple_prefixes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_notupdating_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_notupdating_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_notupdating_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_notupdating_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_object_acl_required` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_object_meta` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_owner` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_part_cleanup_concurrent_deletion_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_concurrent_deletion_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_concurrent_disabling_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_concurrent_disabling_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_concurrent_updating_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_concurrent_updating_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_deletion_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_deletion_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_disabling_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_disabling_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_updating_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_part_cleanup_updating_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_partitioned_key` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_permission_change_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_permission_change_s` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_put_and_flush` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_put_concurrency` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_put_objects` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_roll_time` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_simple_key` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_single_prefix` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_logging_target_cleanup_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_target_cleanup_j_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_target_cleanup_s` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_bucket_logging_target_cleanup_s_single` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_put_bucket_logging` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_logging_account_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_put_bucket_logging_account_s` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_logging_errors` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_logging_extensions` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_put_bucket_logging_permissions` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_logging_tenant_j` | ➖ | ➡️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_put_bucket_logging_tenant_s` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_rm_bucket_logging` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Lifecycle

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_lifecycle_cloud_multiple_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_cloud_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_cloud_transition_large_obj` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_delete` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_deletemarker_expiration` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_date` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_days0` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_header_head` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_header_put` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_newer_noncurrent` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_size_gt` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_expiration_size_lt` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_get` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_get_no_id` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_id_too_long` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_invalid_status` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_multipart_expiration` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_noncur_cloud_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_noncur_expiration` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_noncur_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_same_id` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_date` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_deletemarker` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_empty_filter` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_filter` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_invalid_date` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_multipart` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_noncurrent` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_set_noncurrent_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_transition` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecycle_transition_encrypted[NOTSET]` | ➖ | ➡️ | | ➡️ |
| `test_lifecycle_transition_set_invalid_date` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_lifecycle_transition_single_rule_multi_trans` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_lifecyclev2_expiration` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Range Reads

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_ranged_big_request_response_code` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_ranged_request_empty_object` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `test_ranged_request_invalid_range` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `test_ranged_request_response_code` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_ranged_request_return_trailing_bytes_response_code` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_ranged_request_skip_leading_bytes_response_code` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Head / Get Object

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_get_bucket_encryption_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_bucket_encryption_s3` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_checksum_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_multipart_checksum_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_multipart_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_object_torrent` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_paginated_multipart_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_get_single_multipart_object_attributes` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Put Object

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_put_bucket_encryption_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_encryption_s3` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_ownership_bucket_owner_enforced` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_ownership_bucket_owner_preferred` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_bucket_ownership_object_writer` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_current_object_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_current_object_if_none_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_obj_enc_conflict_bad_enc_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_obj_enc_conflict_c_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_obj_enc_conflict_c_s3` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_obj_enc_conflict_s3_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_object_current_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_object_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_put_object_ifmatch_overwrite_existed_good` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_put_object_ifnonmatch_nonexisted_good` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Delete

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_create_delete` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_delete_bucket_ownership` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_delete_notexist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_delete_bucket_encryption_kms` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_bucket_encryption_s3` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_marker_expiration` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_marker_suspended` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_current_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_current_if_match_last_modified_time` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_current_if_match_size` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_if_match_last_modified_time` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_object_if_match_size` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_current_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_current_if_match_last_modified_time` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_current_if_match_size` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_if_match_last_modified_time` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_delete_objects_if_match_size` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_delete_key_bucket_gone` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_write_read_update_read_delete` | ➖ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ |

### 100-Continue

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_100_continue` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_100_continue_error_retry` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Checksum

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_multipart_checksum_sha256` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_checksum_crc64nvme` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_checksum_sha256` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_upload_checksum` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Other / Uncategorized

| Test | http-objst | Run 1 | Notes | Run 2 | Run 3 | Run 4 | Run 5 | Run 6 | Run 7 |
|------|-----------|-------|-------|-------|-------|-------|-------|-------| --- |
| `test_bucket_acl_canned_private_to_private` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_concurrent_set_canned_acl` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_create_exists` | ➖ | ⚠️ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_create_naming_dns_long` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_head` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_head_notexist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_long_name` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_marker_after_list` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_marker_empty` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_marker_none` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_marker_not_in_list` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_marker_unreadable` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_maxkeys_none` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_maxkeys_one` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_objects_anonymous` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_list_prefix_basic` | ➖ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_list_prefix_delimiter_basic` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_prefix_empty` | ➖ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_list_prefix_none` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_prefix_not_exist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_list_prefix_unreadable` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_maxkeys_none` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_maxkeys_one` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_objects_anonymous` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_bucket_listv2_prefix_basic` | ➖ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_listv2_prefix_delimiter_basic` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_prefix_empty` | ➖ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ |
| `test_bucket_listv2_prefix_none` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_prefix_not_exist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_listv2_prefix_unreadable` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_notexist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucket_recreate_not_overriding` | ➖ | ❌ | | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_buckets_create_then_list` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_buckets_list_ctime` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_bucketv2_notexist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_copy_object_ifmatch_failed` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_copy_object_ifnonematch_good` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_copy_part_enc[sse-c-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-c-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-c-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-c-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ✅ |
| `test_copy_part_enc[sse-kms-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-kms-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-kms-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-kms-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ✅ |
| `test_copy_part_enc[sse-s3-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-s3-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-s3-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[sse-s3-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[unencrypted-sse-c-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[unencrypted-sse-kms-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[unencrypted-sse-s3-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ❌ |
| `test_copy_part_enc[unencrypted-unencrypted-STANDARD-STANDARD-8388608]` | ➖ | ⚠️ | | ✅ |
| `test_create_bucket_bucket_owner_enforced` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_create_bucket_bucket_owner_preferred` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_create_bucket_no_ownership_controls` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_create_bucket_object_writer` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_encrypted_transfer_13b` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_encrypted_transfer_1MB` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_encrypted_transfer_1b` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_encrypted_transfer_1kb` | ➖ | ✅ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_multipart_put_current_object_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_put_current_object_if_none_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_put_object_if_match` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_resend_first_finishes_last` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_single_get_part` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_use_cksum_helper_crc32` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_use_cksum_helper_crc32c` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_use_cksum_helper_crc64nvme` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_use_cksum_helper_sha1` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_multipart_use_cksum_helper_sha256` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_non_multipart_get_part` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_acl_full_control_verify_owner` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_anon_put_write_access` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_head_zero_bytes` | ➖ | ⚠️ | | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| `test_object_metadata_replaced_on_put` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| `test_object_presigned_put_object_with_acl` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_presigned_put_object_with_acl_tenant` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_put_authenticated` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_authenticated` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_authenticated_bucket_acl` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_authenticated_bucket_gone` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_authenticated_object_acl` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_authenticated_object_gone` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_raw_get` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_bucket_acl` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_bucket_gone` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_raw_get_object_gone` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_read_not_exist` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_read_unreadable` | ➖ | ❌ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_object_write_file` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_write_to_nonexist_bucket` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_object_write_with_chunked_transfer_encoding` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_condition_is_case_sensitive` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_empty_conditions` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_expires_is_case_sensitive` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_invalid_content_length_argument` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_invalid_date_format` | ➖ | ⚠️ | | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `test_post_object_missing_conditions_list` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_missing_content_length_argument` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_missing_expires_condition` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_missing_signature` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_no_key_specified` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_upload_size_below_minimum` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_post_object_upload_size_limit_exceeded` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `test_read_through` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_restore_noncur_obj` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_restore_object_permanent` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_restore_object_temporary` | ➖ | ⚠️ | | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ | ➡️ |
| `test_upload_part_copy_percent_encoded_key` | ➖ | ⚠️ | | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |


<!-- Missing tests added: 604 -->
