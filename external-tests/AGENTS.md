# AGENTS.md - external-tests

This directory contains black-box, client-perspective tests and operator
examples for objstrd, objst_sharded, and the cluster.

Nothing here is compiled by Cargo. These are pure shell and Python scripts
that talk to objstrd over HTTP/S3.

---

## Directory Structure

```
external-tests/
  common/
    server.sh           - start_server / stop_server / get_build_hash helpers
    s3.sh               - s3_put / s3_get / s3_head / s3_delete / s3_list / assert_eq helpers

  standalone/
    run_basic_s3.sh     - Smoke tests: bucket, CRUD, listing, range GET, admin info
    run_multipart.sh    - Multipart upload via raw S3 XML API (no aws-cli)
    run_full_matrix.sh  - Comprehensive E2E: all 3 components x all backends
                          (raw/fs/mem/s3), tree configs, chained S3, cross-component
                          consistency, concurrency, aws CLI, multipart, copy, etc.

  cluster/
    setup_3node.sh      - Start 3 standalone nodes on ports 8100/8101/8102
    teardown.sh         - Kill all 3 nodes and clean up images
    add_node.sh         - Example: spin up a 4th node to join the cluster
    run_cluster_tests.sh - S3 tests against each cluster node

  large-objects/
    run_large_put.sh    - Single PUT of a multi-GB object, verify HEAD + byte range
    run_large_multipart.sh - aws-cli multipart of 10+ GB

  lance/
    run_lance_cluster.sh - Run LanceDB-on-cluster example (requires Rust toolchain)

  s3-compat/
    ceph/               - Ceph s3-tests parallel runner (requires s3-tests checkout)
      run_ceph_tests.sh
      start_s3s.sh      - Start a single instance configured for all ceph credentials
      parse_junit.py    - Parse JUnit XML from parallel runs into summary
      results/          - Historical per-run result .txt files

    rclone/             - rclone S3 conformance + throughput benchmarks
      run_rclone_tests.sh
      rclone_tests.sh   - The actual rclone test functions (sourced by runner)
      run_bench.sh      - Multi-backend throughput benchmark (8 backends, 7 phases)
      run_quick_mem.sh  - Quick wrapper: mem+fs+raw-file backends only
      dump_results.sh   - Print Upload/Download lines from bench results

    mint/               - MinIO mint-style S3 tests (Python)
      run_mint_tests.sh
      s3_mint_tests.py  - Test functions (mint JSON output per test)
      create_data_files.sh - Generate 15 test data files in MINT_DATA_DIR
      parse_mint_results.py - Summarize mint JSON log
```

---

## Quick Start

### Standalone smoke test (no dependencies)

```bash
# Requires: curl, objstrd binary
BINARY=~/build-objstrd/release/objstrd \
  bash standalone/run_basic_s3.sh
```

### Full matrix test (all components x all backends)

```bash
# Requires: curl, aws-cli, rawobjstr, shardedobjstr, objstrd binaries
# Starts its own objstrd instances on ports 8960-8963
bash standalone/run_full_matrix.sh
```

### 3-node cluster test

```bash
# Start 3 nodes
BINARY=~/build-objstrd/release/objstrd \
  bash cluster/setup_3node.sh

# Run cluster-wide S3 tests
bash cluster/run_cluster_tests.sh

# Tear down when done
bash cluster/teardown.sh
```

### Tree config cluster test (self-contained)

```bash
# Starts its own 3-node tree cluster on ports 8200-8202
# Requires: rawobjstr binary for formatting images
bash cluster/run_tree_config_tests.sh
```

### Add a node to a running cluster

```bash
# Adds a 4th node on port 8103
BINARY=~/build-objstrd/release/objstrd \
  bash cluster/add_node.sh 8103 /tmp/node3.raw 512
```

### Drain, repair-replication, and shard lifecycle tests

```bash
# Drain + repair-replication (starts its own 4-shard cluster)
bash standalone/run_drain_repair_replication.sh

# Shard lifecycle (vacuum, redistribute, double drain)
bash standalone/run_shard_lifecycle.sh

# Take-offline / attach across raw, fs, and S3 backends
bash standalone/run_take_offline.sh

# Startup repair (mem shard data loss + auto recovery)
bash standalone/run_startup_repair.sh
```

### S3 compatibility suites

```bash
# Ceph s3-tests (requires ~/s3-tests checkout + venv)
bash s3-compat/ceph/run_ceph_tests.sh 6

# rclone conformance (requires rclone + data files)
bash s3-compat/rclone/run_rclone_tests.sh

# MinIO mint-style tests (requires boto3 + data files)
bash s3-compat/mint/run_mint_tests.sh
```

### LanceDB integration

```bash
# Requires Rust toolchain for building the LanceDB example
bash lance/run_lance_cluster.sh
```

### Sync-mirror (bidirectional S3 sync)

```bash
# Copy sync-mirror.conf.example, edit endpoints, then:
bash sync-mirror/sync-mirror.sh sync-mirror/my.conf
```

### Ceph s3-tests

```bash
# Requires: ~/s3-tests checkout with venv set up
# See objstrd/ceph-tests.md for setup instructions
bash s3-compat/ceph/run_ceph_tests.sh 6

# Parse JUnit results into summary
python3 s3-compat/ceph/parse_junit.py /tmp/s3s_ceph_results
```

### rclone tests

```bash
# Requires: rclone v1.73+ (NOT the apt version on Ubuntu 22.04)
# Data files are created automatically if missing
bash s3-compat/rclone/run_rclone_tests.sh

# Quick throughput benchmark (mem, fs, raw-file only, ~5 min)
bash s3-compat/rclone/run_quick_mem.sh

# Full multi-backend benchmark
bash s3-compat/rclone/run_bench.sh --quick
```

### Mint tests

```bash
# Requires: Python 3, boto3 (pip install boto3)
bash s3-compat/mint/run_mint_tests.sh

# Parse results
python3 s3-compat/mint/parse_mint_results.py /tmp/mint_results/log.json
```

### Large object tests

```bash
# Requires: dd, /dev/urandom -- about 4+ GB free disk
SIZE_GB=2 bash large-objects/run_large_put.sh

# Requires: aws-cli v2 -- uploads PARTS x PART_MB
PARTS=5 PART_MB=512 bash large-objects/run_large_multipart.sh
```

### LanceDB on cluster

```bash
# Requires: Rust toolchain + lance feature
bash lance/run_lance_cluster.sh
```

---

## Environment Variables

All scripts respect these overrides:

| Variable | Default | Description |
|----------|---------|-------------|
| `BINARY` | `~/build-objstrd/release/objstrd` | Path to objstrd binary |
| `S3_ENDPOINT` | `http://localhost:8900` | S3 endpoint (standalone scripts) |
| `S3_BUCKET` | `testbucket` | Bucket name |
| `MINT_DATA_DIR` | `/tmp/mint_data` | Dir for mint data files |
| `OBJSTRD_BIN` | same as `BINARY` | Alias used by common/server.sh |

---

## Port Assignments

| Suite | Ports |
|-------|-------|
| standalone/run_basic_s3.sh | 8900 |
| standalone/run_multipart.sh | 8901 |
| standalone/run_event_socket.sh | 8905-8906 |
| standalone/run_fs_bypass.sh | 8950 |
| standalone/run_full_matrix.sh | 8960-8963 |
| standalone/run_drain_repair_replication.sh | 8300-8303 |
| standalone/run_shard_lifecycle.sh | 8500 |
| standalone/run_take_offline.sh | 8400 |
| standalone/run_startup_repair.sh | (uses common/server.sh defaults) |
| cluster/setup_3node.sh | 8100-8102 |
| cluster/run_tree_config_tests.sh | 8200-8202 |
| large-objects/run_large_put.sh | 8900 (or `PORT` env) |
| large-objects/run_large_multipart.sh | 8902 |
| s3-compat/ceph | 8010-8015 (6 workers) |
| s3-compat/mint | 8020+ |
| s3-compat/rclone tests | 8040 |
| s3-compat/rclone bench | 8050 |

---

## Deploying from Windows

When copying test scripts from a Windows machine to the Linux VM, beware of
two common issues:

1. **CRLF line endings**: Windows editors save files with `\r\n`. Bash on
   Linux chokes on the `\r` (`invalid option name` errors). After copying,
   run on the VM:
   ```bash
   find ~/objstr/external-tests -name '*.sh' -exec sed -i 's/\r$//' {} +
   find ~/objstr/external-tests -name '*.py' -exec sed -i 's/\r$//' {} +
   ```

2. **`scp -r` nested duplicates**: PowerShell's `scp -r external-tests\`
   sometimes creates a nested copy (`external-tests/external-tests/` or
   `external-tests/standalone/standalone/`). Prefer copying individual
   files with `scp` or clean up after bulk copy:
   ```bash
   rm -rf ~/objstr/external-tests/standalone/standalone
   rm -rf ~/objstr/external-tests/external-tests
   ```

---

## Binary Swap Detection

All scripts that start a server capture the build hash from `/_admin/info` at
startup. On test failure the hash is re-checked.  If the binary was replaced
during the test run (e.g. by a background `cargo build`) a warning is printed
explaining that results may be invalid.

See AGENTS.md at the repo root for more details on this pattern.

---

