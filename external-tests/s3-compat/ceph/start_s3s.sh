#!/bin/bash
#
# s3-compat/ceph/start_s3s.sh
#
# Start a single objstrd instance with all ceph s3-tests credentials configured.
# Useful for manual testing or pointing a single s3tests run at port 8000.
#
# All env vars are overridable:
#   PORT=8000 IMAGE=/tmp/mytest.raw bash start_s3s.sh
#
export IMAGE="${IMAGE:-/tmp/ceph_test_s3s.raw}"
export SIZE_MB="${SIZE_MB:-512}"
export BUCKET="${BUCKET:-testbucket}"
export PORT="${PORT:-8000}"
export ACCESS_KEY="${ACCESS_KEY:-AKIAIOSFODNN7EXAMPLE}"
export SECRET_KEY="${SECRET_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
export ACCESS_KEY_1="${ACCESS_KEY_1:-AKIAI44QH8DHBEXAMPLE}"
export SECRET_KEY_1="${SECRET_KEY_1:-je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY}"
export ACCESS_KEY_2="${ACCESS_KEY_2:-AKIAIOSFODNN7TENANT1}"
export SECRET_KEY_2="${SECRET_KEY_2:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYTENANTKEY}"
export ACCESS_KEY_3="${ACCESS_KEY_3:-AKIAIOSFODNN7IAMEXAM}"
export SECRET_KEY_3="${SECRET_KEY_3:-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}"
export RUST_LOG="${RUST_LOG:-info}"
BINARY="${BINARY:-$HOME/build-objstrd/release/objstrd}"
exec "$BINARY"
