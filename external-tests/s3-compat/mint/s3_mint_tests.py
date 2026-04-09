#!/usr/bin/env python3
"""
s3_mint_tests.py - Portable S3 conformance tests (mint-style, no Docker needed)

Runs a comprehensive set of S3 API tests using boto3 against any S3-compatible
endpoint. Outputs results in mint-compatible JSON log format (one JSON object
per line). Designed to work on Linux, FreeBSD, macOS -- anywhere Python3 + boto3
are available.

Usage:
    python3 s3_mint_tests.py [options]

Environment variables:
    SERVER_ENDPOINT   host:port of the S3 server (default: localhost:8010)
    ACCESS_KEY        S3 access key (default: AKIAIOSFODNN7EXAMPLE)
    SECRET_KEY        S3 secret key (default: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY)
    SERVER_REGION     AWS region (default: us-east-1)
    MINT_DATA_DIR     Path to data files (default: /tmp/mint_data)
    LOG_FILE          Path to JSON log output (default: /tmp/mint_results/log.json)
    TEST_BUCKET       Bucket name to use (default: mint-test-XXXX random)
    RUN_ON_FAIL       Set to 1 to continue after failures (default: 1)
"""

import boto3
import botocore
import botocore.exceptions
import hashlib
import json
import os
import sys
import time
import traceback
import uuid

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERVER_ENDPOINT = os.environ.get("SERVER_ENDPOINT", "localhost:8010")
ACCESS_KEY = os.environ.get("ACCESS_KEY", "AKIAIOSFODNN7EXAMPLE")
SECRET_KEY = os.environ.get("SECRET_KEY",
                            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
SERVER_REGION = os.environ.get("SERVER_REGION", "us-east-1")
MINT_DATA_DIR = os.environ.get("MINT_DATA_DIR", "/tmp/mint_data")
LOG_FILE = os.environ.get("LOG_FILE", "/tmp/mint_results/log.json")
RUN_ON_FAIL = os.environ.get("RUN_ON_FAIL", "1") == "1"
ENABLE_HTTPS = os.environ.get("ENABLE_HTTPS", "0") == "1"

SCHEME = "https" if ENABLE_HTTPS else "http"
ENDPOINT_URL = "{}://{}".format(SCHEME, SERVER_ENDPOINT)

TEST_BUCKET = os.environ.get(
    "TEST_BUCKET", "mint-test-{}".format(uuid.uuid4().hex[:8]))

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

s3 = None  # boto3 client, initialized in main()
log_fp = None
total_pass = 0
total_fail = 0
total_na = 0
total_error = 0


def init_client():
    """Create the boto3 S3 client."""
    global s3
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=SERVER_REGION,
        config=botocore.config.Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
        ),
    )


def data_file(name):
    """Return full path to a data file."""
    p = os.path.join(MINT_DATA_DIR, name)
    if not os.path.exists(p):
        raise FileNotFoundError("Missing data file: {} (run create_data_files.sh first)".format(p))
    return p


def read_data(name):
    """Read data file contents into bytes."""
    with open(data_file(name), "rb") as f:
        return f.read()


def md5hex(data):
    """Compute MD5 hex digest of bytes."""
    return hashlib.md5(data).hexdigest()


def random_key(prefix="obj"):
    """Generate a random object key."""
    return "{}/{}".format(prefix, uuid.uuid4().hex[:12])


# ---------------------------------------------------------------------------
# Logging (mint-compatible JSON format)
# ---------------------------------------------------------------------------

def log_result(name, function, status, duration_ms,
               args=None, message=None, alert=None, error=None):
    """Write a mint-format JSON log entry."""
    global total_pass, total_fail, total_na, total_error, log_fp
    entry = {
        "name": "boto3",
        "function": function,
        "args": args or {},
        "duration": duration_ms,
        "status": status,
    }
    if alert:
        entry["alert"] = alert
    if message:
        entry["message"] = message
    if error:
        entry["error"] = str(error)[:2000]

    if status == "PASS":
        total_pass += 1
    elif status == "FAIL":
        total_fail += 1
    elif status == "NA":
        total_na += 1
    else:
        total_error += 1

    line = json.dumps(entry, separators=(",", ":"))
    if log_fp:
        log_fp.write(line + "\n")
        log_fp.flush()

    # Also print summary to stdout
    symbol = {"PASS": ".", "FAIL": "F", "NA": "S", "ERROR": "E"}.get(status, "?")
    sys.stdout.write(symbol)
    sys.stdout.flush()


def run_test(func):
    """Run a test function and log the result."""
    name = func.__name__
    start = time.time()
    try:
        func()
        elapsed = int((time.time() - start) * 1000)
        log_result("boto3", name, "PASS", elapsed)
        return True
    except NotImplementedError as e:
        elapsed = int((time.time() - start) * 1000)
        log_result("boto3", name, "NA", elapsed,
                   message=str(e))
        return True
    except AssertionError as e:
        elapsed = int((time.time() - start) * 1000)
        log_result("boto3", name, "FAIL", elapsed,
                   alert=str(e),
                   error=traceback.format_exc())
        return False
    except Exception as e:
        elapsed = int((time.time() - start) * 1000)
        log_result("boto3", name, "FAIL", elapsed,
                   alert=str(e),
                   error=traceback.format_exc())
        return False


# ---------------------------------------------------------------------------
# Setup / teardown helpers
# ---------------------------------------------------------------------------

def ensure_bucket(bucket=None):
    """Create bucket if it does not exist."""
    bucket = bucket or TEST_BUCKET
    try:
        s3.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError:
        s3.create_bucket(Bucket=bucket)


def cleanup_bucket(bucket=None):
    """Delete all objects in a bucket then delete the bucket."""
    bucket = bucket or TEST_BUCKET
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            objects = page.get("Contents", [])
            if objects:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
                )
        s3.delete_bucket(Bucket=bucket)
    except Exception:
        pass


def put_data(key, data_name, bucket=None):
    """Upload a data file by name."""
    bucket = bucket or TEST_BUCKET
    body = read_data(data_name)
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    return body


# ===========================================================================
# TEST FUNCTIONS
# ===========================================================================

# ---- Group 1: Bucket operations ----

def test_make_bucket():
    """CreateBucket - create a new bucket"""
    bkt = "mint-mkbkt-{}".format(uuid.uuid4().hex[:8])
    resp = s3.create_bucket(Bucket=bkt)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 409), \
        "Expected 200 or 409, got {}".format(resp["ResponseMetadata"]["HTTPStatusCode"])
    cleanup_bucket(bkt)


def test_head_bucket():
    """HeadBucket on existing bucket"""
    ensure_bucket()
    resp = s3.head_bucket(Bucket=TEST_BUCKET)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_head_bucket_nonexistent():
    """HeadBucket on nonexistent bucket returns 404"""
    try:
        s3.head_bucket(Bucket="bucket-does-not-exist-{}".format(uuid.uuid4().hex[:8]))
        assert False, "Expected 404 error"
    except botocore.exceptions.ClientError as e:
        code = e.response["ResponseMetadata"]["HTTPStatusCode"]
        assert code in (404, 403), "Expected 404/403, got {}".format(code)


def test_list_buckets():
    """ListBuckets returns at least our test bucket"""
    ensure_bucket()
    resp = s3.list_buckets()
    names = [b["Name"] for b in resp.get("Buckets", [])]
    assert TEST_BUCKET in names, \
        "Bucket {} not in list: {}".format(TEST_BUCKET, names)


def test_get_bucket_location():
    """GetBucketLocation returns a region"""
    ensure_bucket()
    resp = s3.get_bucket_location(Bucket=TEST_BUCKET)
    # Location can be None (us-east-1) or a string
    assert "LocationConstraint" in resp


def test_delete_bucket():
    """Delete an empty bucket"""
    bkt = "mint-delbkt-{}".format(uuid.uuid4().hex[:8])
    s3.create_bucket(Bucket=bkt)
    resp = s3.delete_bucket(Bucket=bkt)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


# ---- Group 2: PutObject / GetObject / HeadObject / DeleteObject ----

def test_put_object_0b():
    """PutObject - 0 byte object"""
    ensure_bucket()
    key = random_key("zero")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"")
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["ContentLength"] == 0
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_1b():
    """PutObject - 1 byte object"""
    ensure_bucket()
    key = random_key("onebyte")
    body = read_data("datafile-1-b")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == body, "Content mismatch: expected {} bytes, got {}".format(len(body), len(got))
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_1kb():
    """PutObject/GetObject - 1 KB"""
    ensure_bucket()
    key = random_key("1kb")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == body, "Content mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_100kb():
    """PutObject/GetObject - 100 KB"""
    ensure_bucket()
    key = random_key("100kb")
    body = read_data("datafile-100-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == body, "Content mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_1mb():
    """PutObject/GetObject - 1 MB"""
    ensure_bucket()
    key = random_key("1mb")
    body = read_data("datafile-1-MB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == body, "Content mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_5mb():
    """PutObject/GetObject - 5 MB (just under multipart threshold)"""
    ensure_bucket()
    key = random_key("5mb")
    body = read_data("datafile-5-MB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == body, "Content mismatch: expected {} bytes, got {}".format(len(body), len(got))
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_6mb():
    """PutObject/GetObject - 6 MB"""
    ensure_bucket()
    key = random_key("6mb")
    body = read_data("datafile-6-MB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["ContentLength"] == len(body)
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_10mb():
    """PutObject/GetObject - 10 MB"""
    ensure_bucket()
    key = random_key("10mb")
    body = read_data("datafile-10-MB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["ContentLength"] == len(body)
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_head_object():
    """HeadObject returns correct Content-Length and ETag"""
    ensure_bucket()
    key = random_key("head")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["ContentLength"] == len(body), \
        "Expected length {}, got {}".format(len(body), resp["ContentLength"])
    assert "ETag" in resp, "Missing ETag"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_head_object_nonexistent():
    """HeadObject on nonexistent key returns 404"""
    ensure_bucket()
    try:
        s3.head_object(Bucket=TEST_BUCKET, Key="does-not-exist-" + uuid.uuid4().hex)
        assert False, "Expected 404"
    except botocore.exceptions.ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_delete_object():
    """DeleteObject removes the object"""
    ensure_bucket()
    key = random_key("del")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"delete me")
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)
    try:
        s3.head_object(Bucket=TEST_BUCKET, Key=key)
        assert False, "Object should have been deleted"
    except botocore.exceptions.ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_delete_object_nonexistent():
    """DeleteObject on nonexistent key returns 204 (S3 semantics)"""
    ensure_bucket()
    resp = s3.delete_object(
        Bucket=TEST_BUCKET,
        Key="nonexistent-" + uuid.uuid4().hex)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_get_object_nonexistent():
    """GetObject on nonexistent key returns NoSuchKey"""
    ensure_bucket()
    try:
        s3.get_object(Bucket=TEST_BUCKET, Key="missing-" + uuid.uuid4().hex)
        assert False, "Expected NoSuchKey"
    except botocore.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] in ("NoSuchKey", "404"), \
            "Unexpected error code: {}".format(e.response["Error"]["Code"])


# ---- Group 3: Metadata and Content-Type ----

def test_put_object_with_metadata():
    """PutObject with custom x-amz-meta-* headers"""
    ensure_bucket()
    key = random_key("meta")
    meta = {"key1": "value1", "key2": "value2"}
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"metadata test",
                  Metadata=meta)
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    got_meta = resp.get("Metadata", {})
    assert got_meta.get("key1") == "value1", \
        "Missing metadata key1, got: {}".format(got_meta)
    assert got_meta.get("key2") == "value2", \
        "Missing metadata key2, got: {}".format(got_meta)
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_with_content_type():
    """PutObject with Content-Type preserved in HeadObject"""
    ensure_bucket()
    key = random_key("ctype")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"<html>test</html>",
                  ContentType="text/html")
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert "text/html" in resp.get("ContentType", ""), \
        "Expected text/html, got: {}".format(resp.get("ContentType"))
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 4: Copy object ----

def test_copy_object():
    """CopyObject - basic same-bucket copy"""
    ensure_bucket()
    src = random_key("copysrc")
    dst = random_key("copydst")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=src, Body=body)
    s3.copy_object(
        Bucket=TEST_BUCKET, Key=dst,
        CopySource="{}/{}".format(TEST_BUCKET, src))
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=dst)
    got = resp["Body"].read()
    assert got == body, "Copied content mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=src)
    s3.delete_object(Bucket=TEST_BUCKET, Key=dst)


def test_copy_object_large():
    """CopyObject - 5 MB object"""
    ensure_bucket()
    src = random_key("cplgsrc")
    dst = random_key("cplgdst")
    body = read_data("datafile-5-MB")
    s3.put_object(Bucket=TEST_BUCKET, Key=src, Body=body)
    s3.copy_object(
        Bucket=TEST_BUCKET, Key=dst,
        CopySource="{}/{}".format(TEST_BUCKET, src))
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=dst)
    assert resp["ContentLength"] == len(body)
    s3.delete_object(Bucket=TEST_BUCKET, Key=src)
    s3.delete_object(Bucket=TEST_BUCKET, Key=dst)


def test_copy_object_replace_metadata():
    """CopyObject with MetadataDirective=REPLACE"""
    ensure_bucket()
    src = random_key("cpmeta-src")
    dst = random_key("cpmeta-dst")
    s3.put_object(Bucket=TEST_BUCKET, Key=src, Body=b"meta replace",
                  Metadata={"original": "true"})
    s3.copy_object(
        Bucket=TEST_BUCKET, Key=dst,
        CopySource="{}/{}".format(TEST_BUCKET, src),
        MetadataDirective="REPLACE",
        Metadata={"replaced": "yes"})
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=dst)
    meta = resp.get("Metadata", {})
    assert meta.get("replaced") == "yes", \
        "Expected replaced metadata, got: {}".format(meta)
    s3.delete_object(Bucket=TEST_BUCKET, Key=src)
    s3.delete_object(Bucket=TEST_BUCKET, Key=dst)


def test_copy_object_overwrite_self():
    """CopyObject to same key (overwrite self with new metadata)"""
    ensure_bucket()
    key = random_key("cpself")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"self copy",
                  Metadata={"version": "1"})
    s3.copy_object(
        Bucket=TEST_BUCKET, Key=key,
        CopySource="{}/{}".format(TEST_BUCKET, key),
        MetadataDirective="REPLACE",
        Metadata={"version": "2"})
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    meta = resp.get("Metadata", {})
    assert meta.get("version") == "2", \
        "Expected version 2, got: {}".format(meta)
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 5: Range reads ----

def test_get_object_range_first():
    """GetObject Range: bytes=0-99 (first 100 bytes)"""
    ensure_bucket()
    key = random_key("range")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key, Range="bytes=0-99")
    got = resp["Body"].read()
    assert got == body[:100], \
        "Range mismatch: expected {} bytes, got {}".format(100, len(got))
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_get_object_range_middle():
    """GetObject Range: bytes=100-199"""
    ensure_bucket()
    key = random_key("range-mid")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key, Range="bytes=100-199")
    got = resp["Body"].read()
    assert got == body[100:200], "Range content mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_get_object_range_suffix():
    """GetObject Range: bytes=-100 (last 100 bytes)"""
    ensure_bucket()
    key = random_key("range-suf")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key, Range="bytes=-100")
    got = resp["Body"].read()
    assert got == body[-100:], "Suffix range mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_get_object_range_open_end():
    """GetObject Range: bytes=500- (from offset to end)"""
    ensure_bucket()
    key = random_key("range-open")
    body = read_data("datafile-1-kB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key, Range="bytes=500-")
    got = resp["Body"].read()
    assert got == body[500:], "Open-end range mismatch"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 6: Listing ----

def _setup_listing_objects():
    """Put a set of objects for listing tests. Returns list of keys."""
    ensure_bucket()
    keys = [
        "list/a.txt",
        "list/b.txt",
        "list/c.txt",
        "list/sub1/d.txt",
        "list/sub1/e.txt",
        "list/sub2/f.txt",
        "list/sub2/sub3/g.txt",
        "other/h.txt",
    ]
    for k in keys:
        s3.put_object(Bucket=TEST_BUCKET, Key=k, Body=b"list-test")
    return keys


def _cleanup_listing_objects(keys):
    """Delete the listing test objects."""
    for k in keys:
        try:
            s3.delete_object(Bucket=TEST_BUCKET, Key=k)
        except Exception:
            pass


def test_list_objects_v2_basic():
    """ListObjectsV2 - returns all objects"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix="list/")
        got_keys = [o["Key"] for o in resp.get("Contents", [])]
        for k in ["list/a.txt", "list/b.txt", "list/c.txt",
                   "list/sub1/d.txt", "list/sub1/e.txt",
                   "list/sub2/f.txt", "list/sub2/sub3/g.txt"]:
            assert k in got_keys, "Missing key {} in listing: {}".format(k, got_keys)
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v2_with_delimiter():
    """ListObjectsV2 with delimiter shows CommonPrefixes"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="list/", Delimiter="/")
        got_keys = [o["Key"] for o in resp.get("Contents", [])]
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        # Direct children
        assert "list/a.txt" in got_keys, "Missing list/a.txt"
        # Sub-prefixes
        assert "list/sub1/" in prefixes, \
            "Missing prefix list/sub1/, got: {}".format(prefixes)
        assert "list/sub2/" in prefixes, \
            "Missing prefix list/sub2/, got: {}".format(prefixes)
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v2_max_keys():
    """ListObjectsV2 with MaxKeys limits results"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="list/", MaxKeys=2)
        contents = resp.get("Contents", [])
        assert len(contents) <= 2, \
            "Expected max 2 keys, got {}".format(len(contents))
        assert resp.get("IsTruncated", False) is True, \
            "Expected IsTruncated=True"
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v2_continuation():
    """ListObjectsV2 pagination with ContinuationToken"""
    keys = _setup_listing_objects()
    try:
        all_keys = []
        token = None
        for _ in range(10):  # safety limit
            kwargs = {"Bucket": TEST_BUCKET, "Prefix": "list/", "MaxKeys": 3}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            all_keys.extend(o["Key"] for o in resp.get("Contents", []))
            if not resp.get("IsTruncated", False):
                break
            token = resp.get("NextContinuationToken")
        assert len(all_keys) == 7, \
            "Expected 7 keys total, got {}".format(len(all_keys))
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v2_start_after():
    """ListObjectsV2 with StartAfter"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="list/", StartAfter="list/b.txt")
        got_keys = [o["Key"] for o in resp.get("Contents", [])]
        assert "list/a.txt" not in got_keys, "a.txt should be before start-after"
        assert "list/b.txt" not in got_keys, "b.txt should be before start-after"
        assert "list/c.txt" in got_keys, "c.txt should be after start-after"
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v1():
    """ListObjects (V1) returns objects"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects(Bucket=TEST_BUCKET, Prefix="list/")
        got_keys = [o["Key"] for o in resp.get("Contents", [])]
        assert len(got_keys) >= 7, \
            "Expected at least 7 keys, got {}".format(len(got_keys))
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v1_with_delimiter():
    """ListObjects (V1) with delimiter"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects(
            Bucket=TEST_BUCKET, Prefix="list/", Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "list/sub1/" in prefixes
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_v1_marker():
    """ListObjects (V1) with Marker for pagination"""
    keys = _setup_listing_objects()
    try:
        resp = s3.list_objects(
            Bucket=TEST_BUCKET, Prefix="list/", Marker="list/b.txt", MaxKeys=2)
        got_keys = [o["Key"] for o in resp.get("Contents", [])]
        assert "list/a.txt" not in got_keys
        assert "list/b.txt" not in got_keys
    finally:
        _cleanup_listing_objects(keys)


def test_list_objects_empty_prefix():
    """ListObjectsV2 with non-matching prefix returns empty"""
    ensure_bucket()
    resp = s3.list_objects_v2(
        Bucket=TEST_BUCKET,
        Prefix="nonexistent-prefix-{}".format(uuid.uuid4().hex))
    assert len(resp.get("Contents", [])) == 0


# ---- Group 7: Multipart upload ----

def test_multipart_upload_small():
    """Multipart upload with 5 MB parts"""
    ensure_bucket()
    key = random_key("mpu-small")
    data_5mb = read_data("datafile-5-MB")
    data_1mb = read_data("datafile-1-MB")

    mpu = s3.create_multipart_upload(Bucket=TEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]
    assert upload_id, "Missing UploadId"

    try:
        # Upload 2 parts: 5MB + 1MB = 6MB total
        part1 = s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=1, Body=data_5mb)
        part2 = s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=2, Body=data_1mb)

        s3.complete_multipart_upload(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": [
                {"PartNumber": 1, "ETag": part1["ETag"]},
                {"PartNumber": 2, "ETag": part2["ETag"]},
            ]})

        resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
        expected = len(data_5mb) + len(data_1mb)
        assert resp["ContentLength"] == expected, \
            "Expected {} bytes, got {}".format(expected, resp["ContentLength"])
    finally:
        s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_multipart_upload_10mb():
    """Multipart upload with 2 x 5 MB parts = 10 MB"""
    ensure_bucket()
    key = random_key("mpu-10mb")
    part_data = read_data("datafile-5-MB")

    mpu = s3.create_multipart_upload(Bucket=TEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]

    try:
        parts = []
        for i in range(1, 3):
            p = s3.upload_part(
                Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
                PartNumber=i, Body=part_data)
            parts.append({"PartNumber": i, "ETag": p["ETag"]})

        s3.complete_multipart_upload(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": parts})

        resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
        assert resp["ContentLength"] == len(part_data) * 2
    finally:
        s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_multipart_upload_content_verify():
    """Multipart upload - verify downloaded content matches parts"""
    ensure_bucket()
    key = random_key("mpu-verify")
    part1_data = read_data("datafile-5-MB")
    part2_data = read_data("datafile-1-MB")
    expected = part1_data + part2_data

    mpu = s3.create_multipart_upload(Bucket=TEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]

    try:
        p1 = s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=1, Body=part1_data)
        p2 = s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=2, Body=part2_data)

        s3.complete_multipart_upload(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": [
                {"PartNumber": 1, "ETag": p1["ETag"]},
                {"PartNumber": 2, "ETag": p2["ETag"]},
            ]})

        resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
        got = resp["Body"].read()
        assert got == expected, \
            "Content mismatch: expected {} bytes, got {}".format(
                len(expected), len(got))
    finally:
        s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_multipart_abort():
    """AbortMultipartUpload cleans up"""
    ensure_bucket()
    key = random_key("mpu-abort")

    mpu = s3.create_multipart_upload(Bucket=TEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]

    # Upload a part
    s3.upload_part(
        Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
        PartNumber=1, Body=read_data("datafile-5-MB"))

    # Abort
    resp = s3.abort_multipart_upload(
        Bucket=TEST_BUCKET, Key=key, UploadId=upload_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    # Object should not exist
    try:
        s3.head_object(Bucket=TEST_BUCKET, Key=key)
        assert False, "Object should not exist after abort"
    except botocore.exceptions.ClientError:
        pass


def test_list_parts():
    """ListParts returns uploaded parts"""
    ensure_bucket()
    key = random_key("mpu-parts")
    data_5mb = read_data("datafile-5-MB")

    mpu = s3.create_multipart_upload(Bucket=TEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]

    try:
        s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=1, Body=data_5mb)
        s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=2, Body=data_5mb)

        resp = s3.list_parts(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id)
        parts = resp.get("Parts", [])
        assert len(parts) == 2, "Expected 2 parts, got {}".format(len(parts))
        assert parts[0]["PartNumber"] == 1
        assert parts[1]["PartNumber"] == 2
    finally:
        s3.abort_multipart_upload(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id)


def test_list_multipart_uploads():
    """ListMultipartUploads shows active uploads"""
    ensure_bucket()
    key = random_key("mpu-list")

    mpu = s3.create_multipart_upload(Bucket=TEST_BUCKET, Key=key)
    upload_id = mpu["UploadId"]

    try:
        resp = s3.list_multipart_uploads(Bucket=TEST_BUCKET, Prefix=key)
        uploads = resp.get("Uploads", [])
        found = any(u["UploadId"] == upload_id for u in uploads)
        assert found, "Upload {} not in list: {}".format(
            upload_id, [u["UploadId"] for u in uploads])
    finally:
        s3.abort_multipart_upload(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id)


def test_multipart_with_metadata():
    """Multipart upload with custom metadata"""
    ensure_bucket()
    key = random_key("mpu-meta")
    data = read_data("datafile-5-MB")

    mpu = s3.create_multipart_upload(
        Bucket=TEST_BUCKET, Key=key,
        Metadata={"mpu-key": "mpu-value"})
    upload_id = mpu["UploadId"]

    try:
        p1 = s3.upload_part(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            PartNumber=1, Body=data)

        s3.complete_multipart_upload(
            Bucket=TEST_BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": [
                {"PartNumber": 1, "ETag": p1["ETag"]},
            ]})

        resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
        meta = resp.get("Metadata", {})
        assert meta.get("mpu-key") == "mpu-value", \
            "Missing multipart metadata, got: {}".format(meta)
    finally:
        s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 8: Batch delete (DeleteObjects) ----

def test_delete_objects_batch():
    """DeleteObjects - batch delete multiple objects"""
    ensure_bucket()
    keys = []
    for i in range(5):
        k = random_key("batch/obj{}".format(i))
        s3.put_object(Bucket=TEST_BUCKET, Key=k, Body=b"batch delete test")
        keys.append(k)

    resp = s3.delete_objects(
        Bucket=TEST_BUCKET,
        Delete={"Objects": [{"Key": k} for k in keys]})

    deleted = [d["Key"] for d in resp.get("Deleted", [])]
    for k in keys:
        assert k in deleted, "Key {} not in Deleted: {}".format(k, deleted)

    # Verify they are gone
    for k in keys:
        try:
            s3.head_object(Bucket=TEST_BUCKET, Key=k)
            assert False, "Object {} should have been deleted".format(k)
        except botocore.exceptions.ClientError:
            pass


def test_delete_objects_mixed():
    """DeleteObjects - mix of existing and nonexistent keys"""
    ensure_bucket()
    key = random_key("batch-mix")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"exists")

    resp = s3.delete_objects(
        Bucket=TEST_BUCKET,
        Delete={"Objects": [
            {"Key": key},
            {"Key": "does-not-exist-" + uuid.uuid4().hex},
        ]})

    deleted = resp.get("Deleted", [])
    errors = resp.get("Errors", [])
    # Both should succeed per S3 semantics
    assert len(deleted) == 2, \
        "Expected 2 deleted, got {} deleted, {} errors".format(
            len(deleted), len(errors))


# ---- Group 9: Special characters in keys ----

def test_put_object_special_chars():
    """PutObject with special characters in key"""
    ensure_bucket()
    key = "special/hello world!.txt"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"special chars test")
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == b"special chars test"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_deep_path():
    """PutObject with deeply nested key"""
    ensure_bucket()
    key = "a/b/c/d/e/f/g/deep.txt"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"deep path")
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["Body"].read() == b"deep path"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_dots_in_key():
    """PutObject with dots in key name"""
    ensure_bucket()
    key = "dot.test/file.name.with.dots.tar.gz"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"dots")
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["ContentLength"] == 4
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_put_object_plus_in_key():
    """PutObject with plus sign in key"""
    ensure_bucket()
    key = "plus/a+b+c.txt"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"plus test")
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["Body"].read() == b"plus test"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 10: Overwrite ----

def test_overwrite_object():
    """PutObject overwrites existing object"""
    ensure_bucket()
    key = random_key("overwrite")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"version1")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"version2")
    resp = s3.get_object(Bucket=TEST_BUCKET, Key=key)
    got = resp["Body"].read()
    assert got == b"version2", "Expected version2, got: {}".format(got)
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_overwrite_object_different_size():
    """PutObject overwrites with different size"""
    ensure_bucket()
    key = random_key("overwrite-sz")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=read_data("datafile-1-kB"))
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=read_data("datafile-100-kB"))
    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert resp["ContentLength"] == len(read_data("datafile-100-kB"))
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 11: Large object multipart via TransferManager ----

def test_transfer_manager_upload_11mb():
    """boto3 TransferManager upload (11 MB, triggers multipart)"""
    ensure_bucket()
    key = random_key("xfer-11mb")
    src_file = data_file("datafile-11-MB")

    from boto3.s3.transfer import TransferConfig
    config = TransferConfig(multipart_threshold=5 * 1024 * 1024,
                            multipart_chunksize=5 * 1024 * 1024)
    s3.upload_file(src_file, TEST_BUCKET, key, Config=config)

    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    expected_size = os.path.getsize(src_file)
    assert resp["ContentLength"] == expected_size, \
        "Expected {} bytes, got {}".format(expected_size, resp["ContentLength"])
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_transfer_manager_upload_65mb():
    """boto3 TransferManager upload (65 MB, multi-part)"""
    ensure_bucket()
    key = random_key("xfer-65mb")
    src_file = data_file("datafile-65-MB")

    from boto3.s3.transfer import TransferConfig
    config = TransferConfig(multipart_threshold=5 * 1024 * 1024,
                            multipart_chunksize=8 * 1024 * 1024)
    s3.upload_file(src_file, TEST_BUCKET, key, Config=config)

    resp = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    expected_size = os.path.getsize(src_file)
    assert resp["ContentLength"] == expected_size, \
        "Expected {} bytes, got {}".format(expected_size, resp["ContentLength"])
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_transfer_manager_download_verify():
    """boto3 TransferManager download and verify content"""
    ensure_bucket()
    key = random_key("xfer-dl")
    body = read_data("datafile-10-MB")
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)

    import tempfile
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    try:
        from boto3.s3.transfer import TransferConfig
        config = TransferConfig(multipart_threshold=5 * 1024 * 1024)
        s3.download_file(TEST_BUCKET, key, tmp_path, Config=config)

        with open(tmp_path, "rb") as f:
            got = f.read()
        assert got == body, "Downloaded content mismatch"
    finally:
        os.unlink(tmp_path)
        s3.delete_object(Bucket=TEST_BUCKET, Key=key)


# ---- Group 12: ETag validation ----

def test_etag_present_on_put():
    """PutObject response includes ETag"""
    ensure_bucket()
    key = random_key("etag-put")
    resp = s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"etag test")
    assert "ETag" in resp, "Missing ETag in PutObject response"
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)


def test_etag_consistent():
    """Same content produces same ETag"""
    ensure_bucket()
    body = b"etag consistency check"
    k1 = random_key("etag1")
    k2 = random_key("etag2")
    r1 = s3.put_object(Bucket=TEST_BUCKET, Key=k1, Body=body)
    r2 = s3.put_object(Bucket=TEST_BUCKET, Key=k2, Body=body)
    assert r1["ETag"] == r2["ETag"], \
        "ETags differ for same content: {} vs {}".format(r1["ETag"], r2["ETag"])
    s3.delete_object(Bucket=TEST_BUCKET, Key=k1)
    s3.delete_object(Bucket=TEST_BUCKET, Key=k2)


# ===========================================================================
# Main
# ===========================================================================

ALL_TESTS = [
    # Bucket operations
    test_make_bucket,
    test_head_bucket,
    test_head_bucket_nonexistent,
    test_list_buckets,
    test_get_bucket_location,
    test_delete_bucket,

    # Put/Get/Head/Delete
    test_put_object_0b,
    test_put_object_1b,
    test_put_object_1kb,
    test_put_object_100kb,
    test_put_object_1mb,
    test_put_object_5mb,
    test_put_object_6mb,
    test_put_object_10mb,
    test_head_object,
    test_head_object_nonexistent,
    test_delete_object,
    test_delete_object_nonexistent,
    test_get_object_nonexistent,

    # Metadata and Content-Type
    test_put_object_with_metadata,
    test_put_object_with_content_type,

    # Copy
    test_copy_object,
    test_copy_object_large,
    test_copy_object_replace_metadata,
    test_copy_object_overwrite_self,

    # Range reads
    test_get_object_range_first,
    test_get_object_range_middle,
    test_get_object_range_suffix,
    test_get_object_range_open_end,

    # Listing
    test_list_objects_v2_basic,
    test_list_objects_v2_with_delimiter,
    test_list_objects_v2_max_keys,
    test_list_objects_v2_continuation,
    test_list_objects_v2_start_after,
    test_list_objects_v1,
    test_list_objects_v1_with_delimiter,
    test_list_objects_v1_marker,
    test_list_objects_empty_prefix,

    # Multipart
    test_multipart_upload_small,
    test_multipart_upload_10mb,
    test_multipart_upload_content_verify,
    test_multipart_abort,
    test_list_parts,
    test_list_multipart_uploads,
    test_multipart_with_metadata,

    # Batch delete
    test_delete_objects_batch,
    test_delete_objects_mixed,

    # Special characters
    test_put_object_special_chars,
    test_put_object_deep_path,
    test_put_object_dots_in_key,
    test_put_object_plus_in_key,

    # Overwrite
    test_overwrite_object,
    test_overwrite_object_different_size,

    # Transfer manager (multipart via boto3 high-level API)
    test_transfer_manager_upload_11mb,
    test_transfer_manager_upload_65mb,
    test_transfer_manager_download_verify,

    # ETag
    test_etag_present_on_put,
    test_etag_consistent,
]


def main():
    global log_fp

    # Ensure log directory exists
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    log_fp = open(LOG_FILE, "w")

    print("=== Mint-style S3 Tests (boto3) ===")
    print("Endpoint:   {}".format(ENDPOINT_URL))
    print("Bucket:     {}".format(TEST_BUCKET))
    print("Data dir:   {}".format(MINT_DATA_DIR))
    print("Log file:   {}".format(LOG_FILE))
    print("Tests:      {}".format(len(ALL_TESTS)))
    print()

    init_client()

    # Verify connectivity
    try:
        s3.list_buckets()
    except Exception as e:
        print("ERROR: Cannot connect to {}: {}".format(ENDPOINT_URL, e))
        sys.exit(1)

    # Run tests
    passed = 0
    failed = 0
    na_count = 0
    errors = 0

    for test_func in ALL_TESTS:
        ok = run_test(test_func)
        if not ok and not RUN_ON_FAIL:
            print("\nStopping on first failure (set RUN_ON_FAIL=1 to continue)")
            break

    # Final cleanup
    try:
        cleanup_bucket()
    except Exception:
        pass

    log_fp.close()

    print()
    print()
    print("=== Results ===")
    print("PASS: {}  FAIL: {}  NA: {}  ERROR: {}  TOTAL: {}".format(
        total_pass, total_fail, total_na, total_error,
        total_pass + total_fail + total_na + total_error))

    if total_fail > 0 or total_error > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
