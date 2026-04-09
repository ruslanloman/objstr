# rclone Compatibility Tests

Tests in `external-tests/s3-compat/rclone/` validate `objstrd` against rclone  -  a Go-based S3
client that exercises a completely different code path from `boto3`/`botocore`.

## What Is Tested

| # | Test | Description |
|---|------|-------------|
| 1 | `test_list_buckets` | `rclone lsd`  -  list buckets at root |
| 2 | `test_list_empty_bucket` | `rclone size`  -  verify newly created bucket has 0 objects |
| 3 | `test_copy_small_file` | `rclone copy`  -  upload a small text file |
| 4 | `test_list_after_upload` | `rclone ls`  -  file appears in listing after upload |
| 5 | `test_download_small_file` | `rclone copy`  -  download and diff against original |
| 6 | `test_check_small_file` | `rclone check`  -  verify checksum of small file |
| 7 | `test_copy_1mb_file` | Upload 1 MB binary file |
| 8 | `test_download_1mb_file` | Download 1 MB file and diff |
| 9 | `test_check_1mb_file` | Checksum verify 1 MB file |
| 10 | `test_copy_10mb_file` | Upload 10 MB binary file |
| 11 | `test_download_10mb_file` | Download 10 MB file and diff |
| 12 | `test_check_10mb_file` | Checksum verify 10 MB file |
| 13 | `test_copy_11mb_multipart` | Upload 11 MB file  -  triggers rclone multipart upload |
| 14 | `test_download_11mb_multipart` | Download 11 MB file and diff |
| 15 | `test_check_11mb_multipart` | Checksum verify 11 MB file |
| 16 | `test_sync_upload_dir` | `rclone sync`  -  mirror a local directory to S3 |
| 17 | `test_check_synced_dir` | `rclone check`  -  verify all files in synced directory |
| 18 | `test_sync_update_modified` | Modify a file locally and re-sync |
| 19 | `test_check_after_sync_update` | Verify updated file matches after re-sync |
| 20 | `test_sync_delete_propagates` | Delete a file locally and re-sync with `--delete-before` |
| 21 | `test_deleted_file_gone_remote` | Verify deleted file no longer appears in listing |
| 22 | `test_lsf_listing` | `rclone lsf`  -  machine-readable filename listing |
| 23 | `test_lsjson_listing` | `rclone lsjson`  -  JSON listing |
| 24 | `test_delete_single_object` | `rclone delete`  -  remove a single object |
| 25 | `test_deleted_object_not_listed` | Verify deleted object absent from listing |
| 26 | `test_purge_bucket_prefix` | `rclone purge`  -  remove all objects and bucket |
| 27 | `test_bucket_empty_after_purge` | Recreate bucket, verify 0 objects after purge |

## How to Run

**Prerequisite:** rclone v1.73+ must be installed. The Ubuntu 22.04 apt package
(`v1.53.3-DEV`) is outdated and broken. Install from the official source:

```bash
# With sudo (installs to /usr/local/bin)
curl https://rclone.org/install.sh | sudo bash

# Without sudo (installs to ~/.local/bin  -  runner finds it automatically)
mkdir -p ~/.local/bin
curl -L https://downloads.rclone.org/rclone-current-linux-amd64.zip -o /tmp/rclone.zip
unzip -j /tmp/rclone.zip '*/rclone' -d ~/.local/bin && chmod +x ~/.local/bin/rclone
```

Also requires data files from the mint suite:

```bash
bash external-tests/s3-compat/mint/create_data_files.sh
```

Deploy and run (scripts live in `external-tests/s3-compat/rclone/`):

```bash
# Deploy to VM
scp -r external-tests/s3-compat/rclone test@vmserver:/tmp/rclone_tests

# Run (starts its own objstrd on port 8040)
ssh test@vmserver "pkill -9 -f objstrd 2>/dev/null; chmod +x /tmp/rclone_tests/*.sh; bash /tmp/rclone_tests/run_rclone_tests.sh"
```

## Results

### Run 1  -  2026-03-27

rclone v1.73.3, objstrd built from `objstrd` at HEAD.

**27 / 27 PASS**

| Test | Status | Duration (ms) |
|------|--------|--------------|
| test_list_buckets | PASS | 68 |
| test_list_empty_bucket | PASS | 68 |
| test_copy_small_file | PASS | 63 |
| test_list_after_upload | PASS | 56 |
| test_download_small_file | PASS | 65 |
| test_check_small_file | PASS | 61 |
| test_copy_1mb_file | PASS | 80 |
| test_download_1mb_file | PASS | 71 |
| test_check_1mb_file | PASS | 61 |
| test_copy_10mb_file | PASS | 175 |
| test_download_10mb_file | PASS | 124 |
| test_check_10mb_file | PASS | 87 |
| test_copy_11mb_multipart | PASS | 180 |
| test_download_11mb_multipart | PASS | 115 |
| test_check_11mb_multipart | PASS | 80 |
| test_sync_upload_dir | PASS | 64 |
| test_check_synced_dir | PASS | 59 |
| test_sync_update_modified | PASS | 63 |
| test_check_after_sync_update | PASS | 58 |
| test_sync_delete_propagates | PASS | 58 |
| test_deleted_file_gone_remote | PASS | 63 |
| test_lsf_listing | PASS | 53 |
| test_lsjson_listing | PASS | 50 |
| test_delete_single_object | PASS | 47 |
| test_deleted_object_not_listed | PASS | 51 |
| test_purge_bucket_prefix | PASS | 44 |
| test_bucket_empty_after_purge | PASS | 90 |

Total run time: ~3 seconds.

### Failure History

#### `NO_CHECK_BUCKET=true` causing NoSuchBucket (fixed before Run 1)

Initial scripts set `RCLONE_CONFIG_RAWOBJST_NO_CHECK_BUCKET=true`, which
tells rclone to skip its internal bucket-existence check and creation. This
caused all upload tests to fail with `NoSuchBucket` because rclone never
called `CreateBucket`.

Fix: removed `NO_CHECK_BUCKET=true` from `rclone_env()`. rclone now calls
`CreateBucket` automatically on first use.

#### `rclone ls` exit code on empty bucket (fixed before Run 1)

`rclone ls` returns exit code 1 on an empty S3 bucket/prefix. Tests that
used `rclone ls | wc -l` failed because `set -o pipefail` propagated the
non-zero exit.

Fix: replaced both empty-bucket checks with `rclone size ... | grep 'Total objects: 0'`
which exits 0 cleanly regardless of bucket content.

#### `test_bucket_empty_after_purge` after rclone purge (fixed before Run 1)

`rclone purge` on an S3 bucket deletes all objects AND then calls
`DeleteBucket`. After purge, `rclone size` on the deleted bucket returns an
error. The test was checking for "Total objects: 0" but the bucket no longer
existed.

Fix: test now runs `rclone mkdir` to recreate the bucket before calling
`rclone size`, verifying purge cleared all objects (bucket re-created empty).
