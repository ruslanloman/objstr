//! End-to-end tests for transparent compression.
//!
//! Exercises multiple compression algorithms (zstd, snappy, gzip0..gzip9),
//! verifies small objects bypass compression, tests getraw for raw byte
//! retrieval, and ensures pre-compressed data round-trips correctly.

mod common;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, PutPayload};
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::{Compression, INDEX_REGION_SIZE, DEFAULT_MAX_KEY_LENGTH};
use std::io::{Seek, Write};
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_compressed_store(compression: Compression) -> (RawObjectStore, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let store = RawObjectStore::format_with_options(
        tmp.path(),
        FormatOptions {
            device_size: common::MEDIUM_DEVICE,
            direct_io: false,
            index_slot_size: INDEX_REGION_SIZE,
            max_key_length: DEFAULT_MAX_KEY_LENGTH,
            compression,
        },
    )
    .unwrap();
    (store, tmp)
}

/// Highly compressible: repeated pattern.
fn compressible_payload(size: usize) -> Bytes {
    let pattern = b"The quick brown fox jumps over the lazy dog. ";
    let mut buf = Vec::with_capacity(size);
    while buf.len() < size {
        let remaining = size - buf.len();
        let chunk = &pattern[..remaining.min(pattern.len())];
        buf.extend_from_slice(chunk);
    }
    Bytes::from(buf)
}

/// Incompressible: random-ish bytes (deterministic via simple PRNG).
fn random_payload(size: usize, seed: u64) -> Bytes {
    let mut buf = Vec::with_capacity(size);
    let mut state = seed;
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        buf.push((state >> 33) as u8);
    }
    Bytes::from(buf)
}

/// All-zeros payload (maximally compressible).
fn zeros_payload(size: usize) -> Bytes {
    Bytes::from(vec![0u8; size])
}

// ---------------------------------------------------------------------------
// Test: each algorithm compresses and decompresses correctly
// ---------------------------------------------------------------------------

#[test]
fn test_compression_roundtrip_all_algorithms() {
    let algorithms = [
        Compression::Zstd,
        Compression::Snappy,
        Compression::Gzip1,
        Compression::Gzip6,
        Compression::Gzip9,
    ];

    let payload = compressible_payload(100_000); // 100 KB of repeated text

    for alg in &algorithms {
        let (store, _tmp) = make_compressed_store(*alg);
        let key = Path::from(format!("test/{}", alg.as_str()));

        // Put
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
            .unwrap();

        // Regular get should return original uncompressed data
        let result = rt.block_on(store.get(&key)).unwrap();
        let got = rt.block_on(result.bytes()).unwrap();
        assert_eq!(
            got.len(),
            payload.len(),
            "{}: get returned wrong size",
            alg.as_str()
        );
        assert_eq!(got, payload, "{}: get data mismatch", alg.as_str());

        // getraw should return compressed data
        let raw_result = store.get_raw(&key).unwrap();
        assert!(
            raw_result.uncompressed_size > 0,
            "{}: should be compressed (uncompressed_size should be > 0)",
            alg.as_str()
        );
        assert_eq!(raw_result.uncompressed_size, payload.len() as u64);
        assert!(
            raw_result.data.len() < payload.len(),
            "{}: compressed should be smaller ({} >= {})",
            alg.as_str(),
            raw_result.data.len(),
            payload.len()
        );
        assert_eq!(raw_result.compression, *alg);

        // Verify device info reports the correct algorithm
        let info = store.device_info();
        assert_eq!(info.compression, *alg);
    }
}

// ---------------------------------------------------------------------------
// Test: small objects are NOT compressed (below 4096 byte threshold)
// ---------------------------------------------------------------------------

#[test]
fn test_small_objects_bypass_compression() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let small_sizes = [1, 10, 100, 1000, 2048, 4000, 4095];

    for &size in &small_sizes {
        let key = Path::from(format!("small/{}", size));
        let payload = compressible_payload(size);

        rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
            .unwrap();

        // get should return original data
        let result = rt.block_on(store.get(&key)).unwrap();
        let got = rt.block_on(result.bytes()).unwrap();
        assert_eq!(got, payload, "small/{size}: get data mismatch");

        // getraw: small objects should NOT be compressed
        let raw_result = store.get_raw(&key).unwrap();
        assert_eq!(
            raw_result.uncompressed_size, 0,
            "small/{size}: should NOT be compressed (uncompressed_size should be 0)"
        );
        // Raw data should equal original
        assert_eq!(
            raw_result.data, payload,
            "small/{size}: raw data should equal original"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: objects exactly at/above 4096 threshold
// ---------------------------------------------------------------------------

#[test]
fn test_compression_threshold_boundary() {
    let (store, _tmp) = make_compressed_store(Compression::Gzip9);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // 4096 bytes: should be compressed (at threshold)
    let key_at = Path::from("boundary/at_4096");
    let payload_at = compressible_payload(4096);
    rt.block_on(store.put(&key_at, PutPayload::from(payload_at.clone())))
        .unwrap();

    let raw_at = store.get_raw(&key_at).unwrap();
    assert!(
        raw_at.uncompressed_size > 0,
        "4096 bytes should be compressed"
    );

    // 4097 bytes: also compressed
    let key_above = Path::from("boundary/at_4097");
    let payload_above = compressible_payload(4097);
    rt.block_on(store.put(&key_above, PutPayload::from(payload_above.clone())))
        .unwrap();

    let raw_above = store.get_raw(&key_above).unwrap();
    assert!(
        raw_above.uncompressed_size > 0,
        "4097 bytes should be compressed"
    );
}

// ---------------------------------------------------------------------------
// Test: incompressible data is stored uncompressed (compression skipped)
// ---------------------------------------------------------------------------

#[test]
fn test_incompressible_data_stored_raw() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Random data does not compress well
    let payload = random_payload(50_000, 42);
    let key = Path::from("random/50k");
    rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
        .unwrap();

    // get should return original data regardless
    let result = rt.block_on(store.get(&key)).unwrap();
    let got = rt.block_on(result.bytes()).unwrap();
    assert_eq!(got, payload);

    // getraw: since random data doesn't compress, uncompressed_size might be 0
    // (store skips compression when compressed >= original)
    let raw_result = store.get_raw(&key).unwrap();
    if raw_result.uncompressed_size == 0 {
        // Not compressed - raw data equals original
        assert_eq!(raw_result.data, payload);
    } else {
        // Was compressed (some algorithms find minor savings even on random)
        assert!(raw_result.data.len() <= payload.len());
    }
}

// ---------------------------------------------------------------------------
// Test: zeros compress extremely well
// ---------------------------------------------------------------------------

#[test]
fn test_all_zeros_compression_ratio() {
    let algorithms = [
        Compression::Zstd,
        Compression::Snappy,
        Compression::Gzip9,
    ];

    let payload = zeros_payload(1_000_000); // 1 MB of zeros

    for alg in &algorithms {
        let (store, _tmp) = make_compressed_store(*alg);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let key = Path::from("zeros/1mb");
        rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
            .unwrap();

        let raw_result = store.get_raw(&key).unwrap();
        assert!(
            raw_result.uncompressed_size > 0,
            "{}: zeros should be compressed",
            alg.as_str()
        );

        let ratio = raw_result.data.len() as f64 / payload.len() as f64;
        assert!(
            ratio < 0.05,
            "{}: 1MB zeros should compress to < 5% (got {:.1}%)",
            alg.as_str(),
            ratio * 100.0
        );

        // Get should return original
        let result = rt.block_on(store.get(&key)).unwrap();
        let got = rt.block_on(result.bytes()).unwrap();
        assert_eq!(got, payload, "{}: roundtrip failed for zeros", alg.as_str());
    }
}

// ---------------------------------------------------------------------------
// Test: range reads work with compressed objects
// ---------------------------------------------------------------------------

#[test]
fn test_range_reads_with_compression() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = compressible_payload(100_000);
    let key = Path::from("range/test");
    rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
        .unwrap();

    // Range read: first 100 bytes
    let opts = object_store::GetOptions {
        range: Some(object_store::GetRange::Bounded(0..100)),
        ..Default::default()
    };
    let result = rt
        .block_on(store.get_opts(&key, opts))
        .unwrap();
    let got = rt.block_on(result.bytes()).unwrap();
    assert_eq!(got.len(), 100);
    assert_eq!(&got[..], &payload[..100]);

    // Range read: middle section
    let opts = object_store::GetOptions {
        range: Some(object_store::GetRange::Bounded(1000..2000)),
        ..Default::default()
    };
    let result = rt
        .block_on(store.get_opts(&key, opts))
        .unwrap();
    let got = rt.block_on(result.bytes()).unwrap();
    assert_eq!(got.len(), 1000);
    assert_eq!(&got[..], &payload[1000..2000]);

    // Range read: last 500 bytes
    let opts = object_store::GetOptions {
        range: Some(object_store::GetRange::Suffix(500)),
        ..Default::default()
    };
    let result = rt
        .block_on(store.get_opts(&key, opts))
        .unwrap();
    let got = rt.block_on(result.bytes()).unwrap();
    assert_eq!(got.len(), 500);
    assert_eq!(&got[..], &payload[payload.len() - 500..]);
}

// ---------------------------------------------------------------------------
// Test: copy preserves compression
// ---------------------------------------------------------------------------

#[test]
fn test_copy_preserves_compression() {
    let (store, _tmp) = make_compressed_store(Compression::Snappy);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = compressible_payload(50_000);
    let src = Path::from("copy/src");
    let dst = Path::from("copy/dst");

    rt.block_on(store.put(&src, PutPayload::from(payload.clone())))
        .unwrap();
    rt.block_on(store.copy(&src, &dst)).unwrap();

    // Both should return same data
    let src_data = rt.block_on(async {
        let r = store.get(&src).await.unwrap();
        r.bytes().await.unwrap()
    });
    let dst_data = rt.block_on(async {
        let r = store.get(&dst).await.unwrap();
        r.bytes().await.unwrap()
    });
    assert_eq!(src_data, payload);
    assert_eq!(dst_data, payload);
}

// ---------------------------------------------------------------------------
// Test: delete + rewrite with compression
// ---------------------------------------------------------------------------

#[test]
fn test_delete_and_rewrite_compressed() {
    let (store, _tmp) = make_compressed_store(Compression::Gzip6);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let key = Path::from("rewrite/test");

    // Write compressible data
    let payload1 = compressible_payload(10_000);
    rt.block_on(store.put(&key, PutPayload::from(payload1.clone())))
        .unwrap();

    // Delete
    rt.block_on(store.delete(&key)).unwrap();

    // Rewrite with different data
    let payload2 = zeros_payload(20_000);
    rt.block_on(store.put(&key, PutPayload::from(payload2.clone())))
        .unwrap();

    let result = rt.block_on(store.get(&key)).unwrap();
    let got = rt.block_on(result.bytes()).unwrap();
    assert_eq!(got, payload2);
}

// ---------------------------------------------------------------------------
// Test: Compression::None stores uncompressed
// ---------------------------------------------------------------------------

#[test]
fn test_compression_none_no_compression() {
    let (store, _tmp) = make_compressed_store(Compression::None);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = compressible_payload(50_000);
    let key = Path::from("none/test");
    rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
        .unwrap();

    let raw_result = store.get_raw(&key).unwrap();
    assert_eq!(raw_result.uncompressed_size, 0, "None should not compress");
    assert_eq!(raw_result.data, payload);
    assert_eq!(raw_result.compression, Compression::None);
}

// ---------------------------------------------------------------------------
// Test: pre-compressed (zip-like) data round-trips without double compression
// ---------------------------------------------------------------------------

#[test]
fn test_precompressed_data_roundtrip() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Simulate a zip file: compress some data manually
    let original = compressible_payload(100_000);
    let pre_compressed = zstd::bulk::compress(&original, 3).unwrap();

    let key = Path::from("precompressed/data.zst");
    rt.block_on(store.put(
        &key,
        PutPayload::from(Bytes::from(pre_compressed.clone())),
    ))
    .unwrap();

    // get should return the pre-compressed bytes (that's what was "put")
    let result = rt.block_on(store.get(&key)).unwrap();
    let got = rt.block_on(result.bytes()).unwrap();
    assert_eq!(got, Bytes::from(pre_compressed.clone()));

    // getraw: the store may or may not compress the already-compressed data
    // (compression of compressed data typically yields no savings, so it
    // should be stored as-is or with minimal overhead)
    let raw_result = store.get_raw(&key).unwrap();
    if raw_result.uncompressed_size == 0 {
        // Store decided not to compress (good -- already compressed)
        assert_eq!(raw_result.data, Bytes::from(pre_compressed));
    }
    // Either way, regular get returns the original put bytes
}

// ---------------------------------------------------------------------------
// Test: various sizes with zstd
// ---------------------------------------------------------------------------

#[test]
fn test_zstd_various_sizes() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let sizes = [
        4096,     // minimum for compression
        8192,     // 8 KB
        65536,    // 64 KB
        262144,   // 256 KB
        1048576,  // 1 MB
        5242880,  // 5 MB
    ];

    for &size in &sizes {
        let key = Path::from(format!("sizes/{}", size));
        let payload = compressible_payload(size);

        rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
            .unwrap();

        let result = rt.block_on(store.get(&key)).unwrap();
        let got = rt.block_on(result.bytes()).unwrap();
        assert_eq!(got.len(), size, "size {size}: wrong length");
        assert_eq!(got, payload, "size {size}: data mismatch");

        let raw = store.get_raw(&key).unwrap();
        if raw.uncompressed_size > 0 {
            assert!(
                raw.data.len() < size,
                "size {size}: compressed should be smaller"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test: reopen preserves compression setting and data
// ---------------------------------------------------------------------------

#[test]
fn test_reopen_preserves_compression() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let payload = compressible_payload(50_000);
    let key = Path::from("persist/test");

    // Format with gzip9, write data, flush, drop
    {
        let store = RawObjectStore::format_with_options(
            &path,
            FormatOptions {
                device_size: common::MEDIUM_DEVICE,
                direct_io: false,
                index_slot_size: INDEX_REGION_SIZE,
                max_key_length: DEFAULT_MAX_KEY_LENGTH,
                compression: Compression::Gzip9,
            },
        )
        .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
            .unwrap();
        store.flush_index().unwrap();
    }

    // Reopen and verify
    {
        let store = RawObjectStore::open(&path).unwrap();
        let info = store.device_info();
        assert_eq!(info.compression, Compression::Gzip9);
        assert_eq!(info.file_count, 1);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(store.get(&key)).unwrap();
        let got = rt.block_on(result.bytes()).unwrap();
        assert_eq!(got, payload);

        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0);
        assert_eq!(raw.compression, Compression::Gzip9);
    }
}

// ---------------------------------------------------------------------------
// Test: list shows correct (logical) sizes for compressed objects
// ---------------------------------------------------------------------------

#[test]
fn test_list_shows_logical_sizes() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let payload = compressible_payload(50_000);
    let key = Path::from("listing/test.txt");
    rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
        .unwrap();

    // head() should report logical (uncompressed) size
    let meta = rt.block_on(store.head(&key)).unwrap();
    assert_eq!(meta.size, 50_000, "head() should report logical size");
}

// ---------------------------------------------------------------------------
// Test: verify works on compressed device
// ---------------------------------------------------------------------------

#[test]
fn test_verify_with_compression() {
    let (store, _tmp) = make_compressed_store(Compression::Snappy);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Write several objects
    for i in 0..5 {
        let key = Path::from(format!("verify/{i}"));
        let payload = compressible_payload(10_000 + i * 5_000);
        rt.block_on(store.put(&key, PutPayload::from(payload)))
            .unwrap();
    }
    store.flush_index().unwrap();

    let report = store.verify_all();
    assert!(
        report.errors.is_empty(),
        "verify should find no errors (found {})",
        report.errors.len()
    );
    assert!(report.free_list_consistent);
    assert!(report.space_accounted);
}

// ---------------------------------------------------------------------------
// Test: CLI getraw command
// ---------------------------------------------------------------------------

#[test]
fn test_cli_getraw() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    // Format with zstd
    common::run_cli(&[
        "format",
        "--file",
        path,
        "--size",
        &common::SMALL_DEVICE.to_string(),
        "--compression",
        "zstd",
    ]);

    // Put a compressible file
    let data_file = NamedTempFile::new().unwrap();
    let payload = compressible_payload(10_000);
    std::fs::write(data_file.path(), &payload).unwrap();

    common::run_cli(&[
        "put",
        "--file",
        path,
        "--key",
        "cli/test.txt",
        "--from",
        data_file.path().to_str().unwrap(),
    ]);

    // getraw: stderr should show compression info
    let _output = common::run_cli(&[
        "getraw",
        "--file",
        path,
        "--key",
        "cli/test.txt",
        "--to",
        NamedTempFile::new().unwrap().path().to_str().unwrap(),
    ]);

    // Info output should mention compression
    let info_output = common::run_cli(&["info", "--file", path]);
    assert!(
        info_output.contains("zstd") || info_output.contains("Zstd"),
        "info should show compression algorithm"
    );
}

// ---------------------------------------------------------------------------
// Test: all gzip levels produce valid output
// ---------------------------------------------------------------------------

#[test]
fn test_all_gzip_levels() {
    let levels = [
        Compression::Gzip0,
        Compression::Gzip1,
        Compression::Gzip2,
        Compression::Gzip3,
        Compression::Gzip4,
        Compression::Gzip5,
        Compression::Gzip6,
        Compression::Gzip7,
        Compression::Gzip8,
        Compression::Gzip9,
    ];

    let payload = compressible_payload(50_000);

    for alg in &levels {
        let (store, _tmp) = make_compressed_store(*alg);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let key = Path::from("gzip_level/test");
        rt.block_on(store.put(&key, PutPayload::from(payload.clone())))
            .unwrap();

        let result = rt.block_on(store.get(&key)).unwrap();
        let got = rt.block_on(result.bytes()).unwrap();
        assert_eq!(got, payload, "{}: roundtrip failed", alg.as_str());
    }
}

// ---------------------------------------------------------------------------
// Test: Compression::from_str_name / as_str round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_compression_name_roundtrip() {
    let all = [
        Compression::None,
        Compression::Zstd,
        Compression::Snappy,
        Compression::Gzip0,
        Compression::Gzip1,
        Compression::Gzip2,
        Compression::Gzip3,
        Compression::Gzip4,
        Compression::Gzip5,
        Compression::Gzip6,
        Compression::Gzip7,
        Compression::Gzip8,
        Compression::Gzip9,
    ];

    for alg in &all {
        let name = alg.as_str();
        let parsed = Compression::from_str_name(name).unwrap();
        assert_eq!(parsed, *alg, "round-trip failed for {name}");
    }

    // Invalid names
    assert!(Compression::from_str_name("lz4").is_err());
    assert!(Compression::from_str_name("").is_err());
    assert!(Compression::from_str_name("gzip10").is_err());
}

// ---------------------------------------------------------------------------
// Test: multiple compressed + uncompressed objects coexist
// ---------------------------------------------------------------------------

#[test]
fn test_mixed_compressed_uncompressed() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Small (not compressed) + large (compressed)
    let small_payload = Bytes::from(vec![42u8; 100]);
    let large_payload = compressible_payload(100_000);
    let random_large = random_payload(100_000, 99);

    let key_small = Path::from("mixed/small");
    let key_large = Path::from("mixed/large");
    let key_random = Path::from("mixed/random");

    rt.block_on(store.put(&key_small, PutPayload::from(small_payload.clone())))
        .unwrap();
    rt.block_on(store.put(&key_large, PutPayload::from(large_payload.clone())))
        .unwrap();
    rt.block_on(store.put(&key_random, PutPayload::from(random_large.clone())))
        .unwrap();

    // Verify all three
    let got_small = rt.block_on(async {
        store.get(&key_small).await.unwrap().bytes().await.unwrap()
    });
    let got_large = rt.block_on(async {
        store.get(&key_large).await.unwrap().bytes().await.unwrap()
    });
    let got_random = rt.block_on(async {
        store.get(&key_random).await.unwrap().bytes().await.unwrap()
    });

    assert_eq!(got_small, small_payload);
    assert_eq!(got_large, large_payload);
    assert_eq!(got_random, random_large);

    // Check compression states
    let raw_small = store.get_raw(&key_small).unwrap();
    assert_eq!(raw_small.uncompressed_size, 0, "small should be uncompressed");

    let raw_large = store.get_raw(&key_large).unwrap();
    assert!(raw_large.uncompressed_size > 0, "large compressible should be compressed");

    // Verify + repair should be clean
    store.flush_index().unwrap();
    let report = store.verify_all();
    assert!(report.errors.is_empty());
}

// ---------------------------------------------------------------------------
// Helpers for external CLI decompression
// ---------------------------------------------------------------------------

/// Write `data` to a tempfile, run `zstd -d --stdout <file>`, return decompressed bytes.
/// Panics if `zstd` is not installed (install with: sudo apt-get install -y zstd).
fn cli_zstd_decompress(data: &[u8]) -> Vec<u8> {
    let raw_file = NamedTempFile::new().unwrap();
    std::fs::write(raw_file.path(), data).unwrap();
    let out = std::process::Command::new("zstd")
        .args(["-d", "--stdout", raw_file.path().to_str().unwrap()])
        .output()
        .expect("zstd not found -- install: sudo apt-get install -y zstd");
    assert!(
        out.status.success(),
        "zstd -d failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// Write `data` to a tempfile, run `gunzip -c <file>`, return decompressed bytes.
fn cli_gunzip_decompress(data: &[u8]) -> Vec<u8> {
    let raw_file = NamedTempFile::new().unwrap();
    std::fs::write(raw_file.path(), data).unwrap();
    let out = std::process::Command::new("gunzip")
        .args(["-c", raw_file.path().to_str().unwrap()])
        .output()
        .expect("gunzip not found");
    assert!(
        out.status.success(),
        "gunzip -c failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

// ---------------------------------------------------------------------------
// Test: put with zstd, getraw, decompress via zstd CLI, compare with original.
//       Overwrite the object twice and verify each version independently.
// ---------------------------------------------------------------------------

#[test]
fn test_getraw_zstd_cli_overwrite_cycles() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let key = Path::from("cli_verify/zstd");

    let versions = [
        compressible_payload(50_000),   // v0: 50 KB text
        zeros_payload(80_000),          // v1: 80 KB zeros (overwrite)
        compressible_payload(120_000),  // v2: 120 KB text (overwrite again)
    ];

    for (i, payload) in versions.iter().enumerate() {
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();

        let raw = store.get_raw(&key).unwrap();
        assert_eq!(raw.compression, Compression::Zstd, "v{i}: wrong algorithm");
        assert!(raw.uncompressed_size > 0, "v{i}: should be compressed");
        assert!(raw.data.len() < payload.len(), "v{i}: compressed should be smaller");

        // Verify raw bytes decompress correctly using the zstd CLI tool.
        let decompressed = cli_zstd_decompress(&raw.data);
        assert_eq!(
            decompressed.as_slice(),
            payload.as_ref(),
            "v{i}: zstd CLI decompressed != original"
        );

        // Sanity: store.get should also return the original.
        let got = rt.block_on(async {
            store.get(&key).await.unwrap().bytes().await.unwrap()
        });
        assert_eq!(got, *payload, "v{i}: store.get mismatch");
    }
}

// ---------------------------------------------------------------------------
// Test: put with gzip6, getraw, decompress via gunzip CLI, compare with original.
//       Overwrite the object twice and verify each version independently.
// ---------------------------------------------------------------------------

#[test]
fn test_getraw_gzip_cli_overwrite_cycles() {
    let (store, _tmp) = make_compressed_store(Compression::Gzip6);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let key = Path::from("cli_verify/gzip");

    let versions = [
        compressible_payload(50_000),
        zeros_payload(80_000),
        compressible_payload(120_000),
    ];

    for (i, payload) in versions.iter().enumerate() {
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();

        let raw = store.get_raw(&key).unwrap();
        assert_eq!(raw.compression, Compression::Gzip6, "v{i}: wrong algorithm");
        assert!(raw.uncompressed_size > 0, "v{i}: should be compressed");

        // Verify raw bytes decompress correctly using gunzip.
        let decompressed = cli_gunzip_decompress(&raw.data);
        assert_eq!(
            decompressed.as_slice(),
            payload.as_ref(),
            "v{i}: gunzip decompressed != original"
        );

        let got = rt.block_on(async {
            store.get(&key).await.unwrap().bytes().await.unwrap()
        });
        assert_eq!(got, *payload, "v{i}: store.get mismatch");
    }
}

// ---------------------------------------------------------------------------
// Test: raw bytes from getraw begin with the correct format magic bytes for
//       each compression algorithm, proving the on-disk format is genuine.
// ---------------------------------------------------------------------------

#[test]
fn test_getraw_compression_magic_bytes() {
    let payload = compressible_payload(100_000);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let key = Path::from("magic/obj");

    // zstd magic: 0xFD2FB528 little-endian = [0x28, 0xB5, 0x2F, 0xFD]
    {
        let (store, _tmp) = make_compressed_store(Compression::Zstd);
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();
        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0, "zstd: should compress");
        let magic = &raw.data[..4];
        assert_eq!(
            magic,
            &[0x28u8, 0xB5, 0x2F, 0xFD],
            "zstd: wrong magic bytes, got {:02X?}",
            magic
        );
    }

    // gzip magic: [0x1F, 0x8B] for all gzip levels that produce smaller output
    for alg in &[Compression::Gzip1, Compression::Gzip6, Compression::Gzip9] {
        let (store, _tmp) = make_compressed_store(*alg);
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();
        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0, "{}: should compress", alg.as_str());
        let magic = &raw.data[..2];
        assert_eq!(
            magic,
            &[0x1Fu8, 0x8B],
            "{}: wrong gzip magic bytes, got {:02X?}",
            alg.as_str(),
            magic
        );
    }

    // snappy has no universal magic header; verify the raw bytes are valid
    // snappy-encoded data by decoding independently with the snap crate.
    {
        let (store, _tmp) = make_compressed_store(Compression::Snappy);
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();
        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0, "snappy: should compress");
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&raw.data)
            .expect("snappy: raw bytes are not valid snappy-encoded data");
        assert_eq!(
            decompressed.as_slice(),
            payload.as_ref(),
            "snappy: independently decompressed != original"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: decompress getraw bytes using the Rust compression crates directly
//       (independent of the store's own decompress code path) -- cross-validates
//       that the raw bytes are a genuine compressed representation.
// ---------------------------------------------------------------------------

#[test]
fn test_getraw_direct_decompress_matches_get() {
    let payload = compressible_payload(100_000);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let key = Path::from("direct/obj");

    // zstd: decompress via zstd::bulk::decompress
    {
        let (store, _tmp) = make_compressed_store(Compression::Zstd);
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();
        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0);
        let direct = zstd::bulk::decompress(&raw.data, raw.uncompressed_size as usize)
            .expect("zstd crate: failed to decompress raw bytes");
        assert_eq!(direct.as_slice(), payload.as_ref(), "zstd direct decompress mismatch");
    }

    // gzip: decompress via flate2::read::GzDecoder
    {
        let (store, _tmp) = make_compressed_store(Compression::Gzip6);
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();
        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0);
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(raw.data.as_ref());
        let mut direct = Vec::new();
        decoder.read_to_end(&mut direct).expect("flate2: failed to decompress raw bytes");
        assert_eq!(direct.as_slice(), payload.as_ref(), "gzip direct decompress mismatch");
    }

    // snappy: decompress via snap::raw::Decoder
    {
        let (store, _tmp) = make_compressed_store(Compression::Snappy);
        rt.block_on(store.put(&key, PutPayload::from(payload.clone()))).unwrap();
        let raw = store.get_raw(&key).unwrap();
        assert!(raw.uncompressed_size > 0);
        let direct = snap::raw::Decoder::new()
            .decompress_vec(&raw.data)
            .expect("snap: failed to decompress raw bytes");
        assert_eq!(direct.as_slice(), payload.as_ref(), "snappy direct decompress mismatch");
    }
}

// ---------------------------------------------------------------------------
// Test: full CLI chain -- `rawobjstr getraw --to file` then external decompressor.
//       Put v0, overwrite v1, overwrite v2; verify each version with gunzip.
//       This is the end-to-end "external tool independently verifies on-disk format".
// ---------------------------------------------------------------------------

#[test]
fn test_cli_getraw_chain_external_verify() {
    let tmp = NamedTempFile::new().unwrap();
    let store_path = tmp.path().to_str().unwrap();

    // Format with gzip9 via CLI
    common::run_cli(&[
        "format", "--file", store_path,
        "--size", &common::MEDIUM_DEVICE.to_string(),
        "--compression", "gzip9",
    ]);

    let versions = [
        compressible_payload(50_000),   // v0
        zeros_payload(80_000),          // v1: overwrite
        compressible_payload(30_000),   // v2: overwrite again
    ];

    for (i, payload) in versions.iter().enumerate() {
        // Write payload via CLI put
        let data_file = NamedTempFile::new().unwrap();
        std::fs::write(data_file.path(), payload.as_ref()).unwrap();
        common::run_cli(&[
            "put", "--file", store_path,
            "--key", "chain/obj.bin",
            "--from", data_file.path().to_str().unwrap(),
        ]);

        // Retrieve raw compressed bytes via CLI getraw
        let raw_out = NamedTempFile::new().unwrap();
        common::run_cli(&[
            "getraw", "--file", store_path,
            "--key", "chain/obj.bin",
            "--to", raw_out.path().to_str().unwrap(),
        ]);

        // Decompress with gunzip (independent of any Rust code) and compare
        let raw_bytes = std::fs::read(raw_out.path()).unwrap();
        let decompressed = cli_gunzip_decompress(&raw_bytes);
        assert_eq!(
            decompressed.as_slice(),
            payload.as_ref(),
            "v{i}: CLI getraw -> gunzip decompress != original payload"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Multipart upload with compression (streaming completion)
// ═══════════════════════════════════════════════════════════════════════

/// Multipart upload with zstd compression: parts are stream-compressed
/// to a temp file during complete(), then written to the device.
#[tokio::test]
async fn multipart_zstd_basic() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let path = Path::from("mp_zstd.bin");

    let part_size = 8192;
    let num_parts = 5;
    let mut expected = Vec::new();

    let mut upload = store.put_multipart(&path).await.unwrap();
    for _ in 0..num_parts {
        let data = compressible_payload(part_size);
        expected.extend_from_slice(&data);
        upload.put_part(PutPayload::from(data)).await.unwrap();
    }
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), expected.len());
    assert_eq!(got.as_ref(), expected.as_slice());

    // Verify logical size matches
    let head = store.head(&path).await.unwrap();
    assert_eq!(head.size, expected.len() as u64, "logical size must match");
    println!("PASS multipart_zstd_basic: {num_parts} parts, logical {}B",
        expected.len());
}

/// Multipart upload with snappy compression.
#[tokio::test]
async fn multipart_snappy() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::Snappy);
    let path = Path::from("mp_snappy.bin");

    let mut expected = Vec::new();
    let mut upload = store.put_multipart(&path).await.unwrap();
    for _ in 0..3u8 {
        let data = compressible_payload(10_000);
        expected.extend_from_slice(&data);
        upload.put_part(PutPayload::from(data)).await.unwrap();
    }
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), expected.as_slice());
    println!("PASS multipart_snappy");
}

/// Multipart upload with gzip compression.
#[tokio::test]
async fn multipart_gzip6() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::Gzip6);
    let path = Path::from("mp_gzip.bin");

    let mut expected = Vec::new();
    let mut upload = store.put_multipart(&path).await.unwrap();
    for _ in 0..4 {
        let data = compressible_payload(6000);
        expected.extend_from_slice(&data);
        upload.put_part(PutPayload::from(data)).await.unwrap();
    }
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), expected.as_slice());
    println!("PASS multipart_gzip6");
}

/// Multipart with compression but incompressible data: verifies the
/// fallback path where compression does not help and the uncompressed
/// streaming path is used instead.
#[tokio::test]
async fn multipart_zstd_incompressible() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let path = Path::from("mp_rand.bin");

    let mut expected = Vec::new();
    let mut upload = store.put_multipart(&path).await.unwrap();
    for i in 0..3u64 {
        let data = random_payload(8192, 42 + i);
        expected.extend_from_slice(&data);
        upload.put_part(PutPayload::from(data)).await.unwrap();
    }
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), expected.as_slice());
    println!("PASS multipart_zstd_incompressible");
}

/// Multipart with many tiny parts that individually fall below the
/// compression threshold (< 4096 bytes each), but concatenated exceed it.
#[tokio::test]
async fn multipart_zstd_many_tiny_parts() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let path = Path::from("mp_tiny.bin");

    let mut expected = Vec::new();
    let mut upload = store.put_multipart(&path).await.unwrap();
    for i in 0..20u8 {
        let data = Bytes::from(vec![i; 500]);
        expected.extend_from_slice(&data);
        upload.put_part(PutPayload::from(data)).await.unwrap();
    }
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), expected.as_slice());
    println!("PASS multipart_zstd_many_tiny_parts");
}

/// Multipart with parts that cross block boundaries (4092 bytes per block).
/// This exercises the StreamingBlockWriter's partial block handling.
#[tokio::test]
async fn multipart_uncompressed_block_boundary() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::None);
    let path = Path::from("mp_boundary.bin");

    // Part sizes chosen to split awkwardly across 4092-byte block boundaries
    let part_sizes: Vec<usize> = vec![4090, 4094, 1, 8184, 3000, 5000, 4092];
    let mut expected = Vec::new();
    let mut upload = store.put_multipart(&path).await.unwrap();
    for (i, &sz) in part_sizes.iter().enumerate() {
        let data = Bytes::from(vec![(i as u8).wrapping_add(0xA0); sz]);
        expected.extend_from_slice(&data);
        upload.put_part(PutPayload::from(data)).await.unwrap();
    }
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), expected.len());
    assert_eq!(got.as_ref(), expected.as_slice());

    // Also test range reads across parts
    let range_start: usize = 4090; // crosses first block boundary
    let range_end: usize = 4094 + 4090;
    let range_data = store
        .get_range(&path, (range_start as u64)..(range_end as u64))
        .await
        .unwrap();
    assert_eq!(range_data.as_ref(), &expected[range_start..range_end]);

    println!("PASS multipart_uncompressed_block_boundary");
}

// =========================================================================
// Metadata + compression combined
// =========================================================================

fn make_meta(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

#[test]
fn metadata_with_zstd_compression_roundtrip() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let body = b"compressible compressible body repeated many times for compression";
    let meta = b"my-zstd-metadata";

    store
        .put_with_meta(
            &Path::from("zstd_meta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    let got_meta = store.get_metadata(&Path::from("zstd_meta.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    let (_, ml) = store.head_with_meta(&Path::from("zstd_meta.bin")).unwrap();
    assert_eq!(ml, meta.len() as u16);

    // Body should still be correct through decompression
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("zstd_meta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS metadata_with_zstd_compression_roundtrip");
}

#[test]
fn metadata_with_snappy_compression_roundtrip() {
    let (store, _tmp) = make_compressed_store(Compression::Snappy);
    let body = b"snappy compressed body with some repeated data data data data";
    let meta = b"snappy-meta-bytes";

    store
        .put_with_meta(
            &Path::from("snappy_meta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    let got_meta = store
        .get_metadata(&Path::from("snappy_meta.bin"))
        .unwrap();
    assert_eq!(got_meta.as_ref(), meta);
    println!("PASS metadata_with_snappy_compression_roundtrip");
}

#[test]
fn metadata_with_gzip_compression_roundtrip() {
    let (store, _tmp) = make_compressed_store(Compression::Gzip6);
    let body = b"gzip compressed body data that repeats repeats repeats for gzip";
    let meta = b"gzip-meta-value";

    store
        .put_with_meta(
            &Path::from("gzip_meta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    let got_meta = store.get_metadata(&Path::from("gzip_meta.bin")).unwrap();
    assert_eq!(got_meta.as_ref(), meta);
    println!("PASS metadata_with_gzip_compression_roundtrip");
}

#[test]
fn metadata_compressed_persists_after_reopen() {
    let (store, tmp) = make_compressed_store(Compression::Zstd);
    let path = tmp.path().to_path_buf();
    let body = b"body for persistence test with compression enabled";
    let meta = b"meta-persist-compressed";

    store
        .put_with_meta(
            &Path::from("persist_cmeta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();
    store.flush_index().unwrap();
    drop(store);

    let store2 = RawObjectStore::open(&path).unwrap();
    let got_meta = store2
        .get_metadata(&Path::from("persist_cmeta.bin"))
        .unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store2
            .get(&Path::from("persist_cmeta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS metadata_compressed_persists_after_reopen");
}

#[test]
fn metadata_compressed_large_body_large_meta() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    // 128 KB body + 32 KB metadata
    let body: Vec<u8> = (0..131072).map(|i| (i & 0xFF) as u8).collect();
    let meta = make_meta(32768);

    store
        .put_with_meta(
            &Path::from("large_cmeta.bin"),
            Bytes::from(body.clone()),
            &meta,
        )
        .unwrap();

    let got_meta = store
        .get_metadata(&Path::from("large_cmeta.bin"))
        .unwrap();
    assert_eq!(got_meta.len(), 32768);
    assert_eq!(got_meta.as_ref(), meta.as_slice());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("large_cmeta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body.as_slice());
    println!("PASS metadata_compressed_large_body_large_meta");
}

#[test]
fn put_with_meta_from_file_compressed() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let body = b"compressible compressible compressible data body content";
    let meta = b"file-meta-compressed";

    let mut src = tempfile::tempfile().unwrap();
    src.write_all(body).unwrap();
    src.write_all(meta).unwrap();
    src.seek(std::io::SeekFrom::Start(0)).unwrap();

    store
        .put_with_meta_from_file(
            &Path::from("compressed_file.bin"),
            &mut src,
            meta.len() as u16,
        )
        .unwrap();

    let got_meta = store
        .get_metadata(&Path::from("compressed_file.bin"))
        .unwrap();
    assert_eq!(got_meta.as_ref(), meta);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("compressed_file.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS put_with_meta_from_file_compressed");
}

// =========================================================================
// update_metadata on compressed store
// =========================================================================

#[test]
fn update_metadata_compressed_store() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let body = b"body for update test that is long enough for compression";
    let meta_v1 = b"original-meta-v1";

    store
        .put_with_meta(
            &Path::from("upd_cmeta.bin"),
            Bytes::copy_from_slice(body),
            meta_v1,
        )
        .unwrap();

    // Verify original
    let got = store.get_metadata(&Path::from("upd_cmeta.bin")).unwrap();
    assert_eq!(got.as_ref(), meta_v1);

    // Update metadata
    store
        .update_metadata(
            &Path::from("upd_cmeta.bin"),
            Bytes::from_static(b"updated-meta-v2-longer"),
        )
        .unwrap();

    let got2 = store
        .get_metadata(&Path::from("upd_cmeta.bin"))
        .unwrap();
    assert_eq!(got2.as_ref(), b"updated-meta-v2-longer");

    // Body should still be intact
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("upd_cmeta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS update_metadata_compressed_store");
}

#[test]
fn update_metadata_compressed_persists() {
    let (store, tmp) = make_compressed_store(Compression::Zstd);
    let path = tmp.path().to_path_buf();
    let body = b"persistent body for compressed update metadata test here";
    let meta_v1 = b"v1";

    store
        .put_with_meta(
            &Path::from("upd_persist.bin"),
            Bytes::copy_from_slice(body),
            meta_v1,
        )
        .unwrap();

    store
        .update_metadata(
            &Path::from("upd_persist.bin"),
            Bytes::from_static(b"v2-updated-metadata-bytes"),
        )
        .unwrap();
    store.flush_index().unwrap();
    drop(store);

    let store2 = RawObjectStore::open(&path).unwrap();
    let got = store2
        .get_metadata(&Path::from("upd_persist.bin"))
        .unwrap();
    assert_eq!(got.as_ref(), b"v2-updated-metadata-bytes");
    println!("PASS update_metadata_compressed_persists");
}

#[test]
fn update_metadata_to_empty_on_compressed() {
    let (store, _tmp) = make_compressed_store(Compression::Snappy);
    let body = b"snappy body content for emptying metadata test here ok";
    let meta = b"will-be-removed";

    store
        .put_with_meta(
            &Path::from("empty_meta.bin"),
            Bytes::copy_from_slice(body),
            meta,
        )
        .unwrap();

    store
        .update_metadata(&Path::from("empty_meta.bin"), Bytes::new())
        .unwrap();

    let (_, ml) = store
        .head_with_meta(&Path::from("empty_meta.bin"))
        .unwrap();
    assert_eq!(ml, 0);

    let got = store
        .get_metadata(&Path::from("empty_meta.bin"))
        .unwrap();
    assert_eq!(got.len(), 0);

    // Body should still be intact
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        store
            .get(&Path::from("empty_meta.bin"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    assert_eq!(data.as_ref(), body);
    println!("PASS update_metadata_to_empty_on_compressed");
}

// =========================================================================
// Metadata + compressed range reads
// =========================================================================

#[tokio::test]
async fn metadata_range_read_compressed() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    // Needs to be large enough to actually get compressed (>= 4096)
    let body: Vec<u8> = std::iter::repeat(b"ABCD").take(2000).flatten().copied().collect();
    let meta = b"compressed-meta-test";

    store
        .put_with_meta(
            &Path::from("crange.bin"),
            Bytes::from(body.clone()),
            meta,
        )
        .unwrap();

    // Range read within the body portion of a compressed object
    let opts = GetOptions {
        range: Some(GetRange::Bounded(100..200)),
        ..Default::default()
    };
    let result = store
        .get_opts(&Path::from("crange.bin"), opts)
        .await
        .unwrap();
    let data = result.bytes().await.unwrap();
    assert_eq!(data.as_ref(), &body[100..200]);
    println!("PASS metadata_range_read_compressed");
}

// =========================================================================
// list_with_meta on compressed store
// =========================================================================

#[test]
fn list_with_meta_compressed_store() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    let body1 = b"body one for list_with_meta compressed test content";
    let meta1 = b"meta-one";
    let body2 = b"body two for list_with_meta compressed test content";
    let meta2 = b"";

    store
        .put_with_meta(
            &Path::from("dir/a.bin"),
            Bytes::copy_from_slice(body1),
            meta1,
        )
        .unwrap();
    store
        .put_with_meta(
            &Path::from("dir/b.bin"),
            Bytes::copy_from_slice(body2),
            meta2,
        )
        .unwrap();

    let mut results = store.list_with_meta(Some(&Path::from("dir")));
    results.sort_by(|(a, _), (b, _)| a.location.as_ref().cmp(b.location.as_ref()));

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1, meta1.len() as u16);
    assert_eq!(results[1].1, 0);
    println!("PASS list_with_meta_compressed_store");
}

// =========================================================================
// get_raw on compressed object with metadata
// =========================================================================

#[test]
fn get_raw_with_metadata_compressed() {
    let (store, _tmp) = make_compressed_store(Compression::Zstd);
    // Body large enough to actually compress
    let body: Vec<u8> = std::iter::repeat(b"COMPRESS").take(2000).flatten().copied().collect();
    let meta = b"raw-get-meta";

    store
        .put_with_meta(
            &Path::from("raw_meta.bin"),
            Bytes::from(body.clone()),
            meta,
        )
        .unwrap();

    let raw = store.get_raw(&Path::from("raw_meta.bin")).unwrap();
    // uncompressed_size should be body + meta
    assert_eq!(
        raw.uncompressed_size,
        (body.len() + meta.len()) as u64,
        "uncompressed_size should reflect body + meta"
    );
    assert!(
        raw.data.len() < body.len() + meta.len(),
        "compressed data should be smaller than original"
    );
    assert_eq!(raw.compression, Compression::Zstd);
    println!("PASS get_raw_with_metadata_compressed");
}

// =========================================================================
// Compressed range read rejected when uncompressed > 1 GB
// =========================================================================

/// Objects compressed on disk with uncompressed_size > COMPRESSED_RANGE_READ_MAX
/// (1 GB) must reject range reads because decompressing the entire payload just
/// to slice a small range is impractical.  This test creates a >1 GB object
/// using multipart upload on a Zstd-compressed store (highly compressible
/// all-zeros data keeps the actual disk usage tiny) and verifies that a bounded
/// range read returns an error while a full GET still succeeds.
#[ignore] // requires >1 GB temp space during multipart complete
#[tokio::test]
async fn compressed_range_read_rejected_above_1gb() {
    use object_store::MultipartUpload;

    let (store, _tmp) = make_compressed_store(Compression::Zstd);

    let num_parts: usize = 1074; // 1074 MB > 1 GB (1073741824)
    let part_size: usize = 1024 * 1024; // 1 MB

    let mut upload = store
        .put_multipart(&Path::from("huge/zeros.bin"))
        .await
        .unwrap();

    for _ in 0..num_parts {
        let zeros = Bytes::from(vec![0u8; part_size]);
        upload
            .put_part(object_store::PutPayload::from(zeros))
            .await
            .unwrap();
    }
    upload.complete().await.unwrap();

    // Verify the object exists and has correct uncompressed size.
    let head = store
        .head(&Path::from("huge/zeros.bin"))
        .await
        .unwrap();
    let expected_size = (num_parts * part_size) as u64;
    assert_eq!(
        head.size, expected_size,
        "head should report full uncompressed size"
    );
    assert!(
        expected_size as u64 > rawobjstr::COMPRESSED_RANGE_READ_MAX,
        "test object must exceed COMPRESSED_RANGE_READ_MAX"
    );

    // Range read must be rejected.
    let err = store
        .get_opts(
            &Path::from("huge/zeros.bin"),
            GetOptions {
                range: Some(GetRange::Bounded(0..100)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("rejected") || msg.contains("exceeds limit"),
        "range read on >1 GB compressed object should be rejected: {msg}"
    );

    println!("PASS compressed_range_read_rejected_above_1gb");
}
