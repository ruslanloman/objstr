#!/bin/bash
#
# common/s3.sh - S3 operation helpers using curl
#
# Source this file in test scripts:
#   source "$(dirname "$0")/../common/s3.sh"
#
# All helpers respect these env vars (set defaults if not exported):
#   S3_ENDPOINT   default: http://localhost:8000
#   S3_BUCKET     default: testbucket
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  (optional SigV4)
#
# If keys are not set, requests are sent without auth (anonymous).
#
# Provides:
#   s3_put <key> <data>         PUT object with string body
#   s3_put_file <key> <file>    PUT object from file
#   s3_get <key>                GET object to stdout
#   s3_head <key>               HEAD object, print status code
#   s3_delete <key>             DELETE object
#   s3_list [prefix]            ListObjectsV2, print keys one per line
#   s3_put_multipart <key> <file> <part_size_mb>
#                               Multipart upload via aws-cli (requires awscli)
#   assert_eq <got> <want> <msg>
#   assert_status <got_status> <want_status> <msg>
#

S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:8000}"
S3_BUCKET="${S3_BUCKET:-testbucket}"

_bucket_url() { echo "${S3_ENDPOINT}/${S3_BUCKET}"; }
_object_url() { echo "${S3_ENDPOINT}/${S3_BUCKET}/${1}"; }

s3_put() {
    local key="$1" data="$2"
    curl -sf -X PUT -d "$data" "$(_object_url "$key")"
}

s3_put_file() {
    local key="$1" file="$2"
    curl -sf -X PUT --data-binary "@${file}" "$(_object_url "$key")"
}

s3_get() {
    local key="$1"
    curl -sf "$(_object_url "$key")"
}

s3_head() {
    local key="$1"
    curl -so /dev/null -w "%{http_code}" -I "$(_object_url "$key")"
}

s3_delete() {
    local key="$1"
    curl -sf -X DELETE "$(_object_url "$key")"
}

s3_list() {
    local prefix="${1:-}"
    local url="$(_bucket_url)?list-type=2"
    [ -n "$prefix" ] && url="${url}&prefix=${prefix}"
    curl -sf "$url" | python3 -c "
import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
# Try with S3 namespace first, fall back to no namespace
ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
keys = root.findall('.//s3:Key', ns)
if not keys:
    keys = root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key')
if not keys:
    keys = root.findall('.//Key')
for c in keys:
    print(c.text)
"
}

s3_put_multipart() {
    local key="$1" file="$2" part_mb="${3:-16}"
    aws --endpoint-url "$S3_ENDPOINT" \
        --no-sign-request \
        s3 cp "$file" "s3://${S3_BUCKET}/${key}" \
        --multipart-chunk-size "$((part_mb))MB"
}

s3_bucket_create() {
    local bucket="${1:-$S3_BUCKET}"
    curl -sf -X PUT "${S3_ENDPOINT}/${bucket}" >/dev/null 2>&1 || true
}

s3_put_stream() {
    local key="$1" size="$2"
    curl -sf -X PUT -H "Content-Length: $size" --data-binary @- "$(_object_url "$key")"
}

s3_get_range() {
    local key="$1" range="$2"
    curl -sf -H "Range: bytes=${range}" "$(_object_url "$key")"
}

s3_copy() {
    local src_key="$1" dst_key="$2"
    curl -sf -X PUT \
        -H "x-amz-copy-source: /${S3_BUCKET}/${src_key}" \
        "$(_object_url "$dst_key")"
}

assert_eq() {
    local got="$1" want="$2" msg="$3"
    if [ "$got" != "$want" ]; then
        echo "FAIL: $msg -- expected '$want' got '$got'" >&2
        exit 1
    fi
}

assert_status() {
    local got="$1" want="$2" msg="$3"
    if [ "$got" != "$want" ]; then
        echo "FAIL: $msg -- expected HTTP $want got $got" >&2
        exit 1
    fi
}
