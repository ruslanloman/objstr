//! End-to-end tests for cluster config validation.
//!
//! Covers: validate_cluster_conf diagnostics for replicas, compression,
//! raw shard paths, S3 empty endpoint/bucket, FS missing directory,
//! and clean mem shard configs.

use shardedobjstr::config::{
    parse_cluster_conf, validate_cluster_conf, ClusterConf, ClusterDiagLevel, ShardConf,
};

// =====================================================================
// replicas > shard count
// =====================================================================

#[test]
fn validate_cluster_conf_replicas_exceeds_shard_count() {
    let text = "replicas 5\nshard mem\nshard mem\n";
    let conf = parse_cluster_conf(text).unwrap();
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("replicas") && d.message.contains("exceeds")),
        "should warn about replicas > shard_count: {:?}",
        diags
    );
}

// =====================================================================
// unknown global compression
// =====================================================================

#[test]
fn validate_cluster_conf_unknown_compression() {
    let text = "compression bogus_codec\nshard mem\n";
    let conf = parse_cluster_conf(text).unwrap();
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("unknown") && d.message.contains("compression")),
        "should report unknown compression: {:?}",
        diags
    );
}

// =====================================================================
// raw shard: missing path without size_mb
// =====================================================================

#[test]
fn validate_cluster_conf_raw_shard_missing_path_no_size() {
    let text = "shard raw /nonexistent/path/to/shard.raw\n";
    let conf = parse_cluster_conf(text).unwrap();
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("does not exist")),
        "should warn about missing raw shard path: {:?}",
        diags
    );
}

// =====================================================================
// raw shard: unknown per-shard compression
// =====================================================================

#[test]
fn validate_cluster_conf_raw_shard_with_bad_compression() {
    let text = "shard raw /dev/null compression=foobar\n";
    let conf = parse_cluster_conf(text).unwrap();
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("unknown compression")),
        "should report unknown per-shard compression: {:?}",
        diags
    );
}

// =====================================================================
// S3 shard: empty endpoint (constructed manually since parser rejects it)
// =====================================================================

#[test]
fn validate_cluster_conf_s3_empty_endpoint() {
    let conf = ClusterConf {
        replicas: 1,
        min_writes: None,
        catalog: None,
        read_prefer: None,
        direct_io: false,
        compression: None,
        size_mb: None,
        read_only: false,
        delete_requires_min_writes: false,
        shards: vec![ShardConf::S3 {
            endpoint: String::new(),
            bucket: "test".to_string(),
            region: None,
            access_key: None,
            secret_key: None,
            path_style: false,
        }],
    };
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("empty endpoint")),
        "should warn about empty S3 endpoint: {:?}",
        diags
    );
}

// =====================================================================
// S3 shard: empty bucket (constructed manually since parser rejects it)
// =====================================================================

#[test]
fn validate_cluster_conf_s3_empty_bucket() {
    let conf = ClusterConf {
        replicas: 1,
        min_writes: None,
        catalog: None,
        read_prefer: None,
        direct_io: false,
        compression: None,
        size_mb: None,
        read_only: false,
        delete_requires_min_writes: false,
        shards: vec![ShardConf::S3 {
            endpoint: "http://localhost".to_string(),
            bucket: String::new(),
            region: None,
            access_key: None,
            secret_key: None,
            path_style: false,
        }],
    };
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("empty bucket")),
        "should warn about empty S3 bucket: {:?}",
        diags
    );
}

// =====================================================================
// FS shard: missing directory
// =====================================================================

#[test]
fn validate_cluster_conf_fs_missing_directory() {
    let text = "shard fs /nonexistent/fs/path\n";
    let conf = parse_cluster_conf(text).unwrap();
    let diags = validate_cluster_conf(&conf);
    assert!(
        diags.iter().any(|d| d.message.contains("does not exist")),
        "should warn about missing FS directory: {:?}",
        diags
    );
}

// =====================================================================
// mem shard: valid config produces no warnings or errors
// =====================================================================

#[test]
fn validate_cluster_conf_mem_shard_no_warnings() {
    let text = "replicas 1\nshard mem\n";
    let conf = parse_cluster_conf(text).unwrap();
    let diags = validate_cluster_conf(&conf);
    let warnings_or_errors: Vec<_> = diags
        .iter()
        .filter(|d| d.level != ClusterDiagLevel::Info)
        .collect();
    assert!(
        warnings_or_errors.is_empty(),
        "mem shard with valid config should produce no warnings/errors: {:?}",
        warnings_or_errors
    );
}
