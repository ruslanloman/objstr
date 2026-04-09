//! Key name length limits and shard fill distribution tests:
//!   - Exact shard slot overflow probe (ignored -- slow)
//!   - Multiple max_key_length format configurations
//!   - Shard fill distribution uniformity with tiny files and long keys

mod common;

use std::fs::File;
use std::io::Read;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::store::{FormatOptions, RawObjectStore};
use rawobjstr::Compression;
use tempfile::NamedTempFile;

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: Key name length limit -- find exact shard slot overflow point
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "slow probe test (3K+ iterations); run manually when needed"]
async fn key_name_length_limit_exact() {
    let tmp = NamedTempFile::new().unwrap();
    {
        let f = File::create(tmp.path()).unwrap();
        f.set_len(common::MEDIUM_DEVICE as u64).unwrap();
    }
    // Format with max_key_length set to 65536 (the shard slot ceiling)
    // so the enforcement doesn't kick in before the physical shard limit.
    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size: common::MEDIUM_DEVICE as u64,
        direct_io: false,
        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
        max_key_length: 65536,
        compression: Compression::None,
    }).unwrap();
    let payload = Bytes::from(vec![0xAA; 64]);

    let start = 63 * 1024; // 63 KB
    let ceiling = 66 * 1024; // 66 KB (well past 64 KB)
    let mut exact_max: usize = 0;

    for len in start..=ceiling {
        let key = "K".repeat(len);
        let path = Path::from(key.as_str());

        // put the object
        match store.put(&path, PutPayload::from(payload.clone())).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("put failed at {len} bytes ({:.2} KB): {e}",
                    len as f64 / 1024.0);
                break;
            }
        }

        // force flush to disk -- this is where the shard slot size check happens
        match store.flush_index() {
            Ok(()) => {
                exact_max = len;
                if len % 256 == 0 {
                    eprintln!("  flush ok at {len} bytes ({:.2} KB)",
                        len as f64 / 1024.0);
                }
            }
            Err(e) => {
                eprintln!("flush_index FAILED at key len {len} bytes ({:.2} KB): {e}",
                    len as f64 / 1024.0);
                // delete the key that caused the overflow so we leave clean
                let _ = store.delete(&path).await;
                break;
            }
        }

        // delete the object to keep the shard clean for the next iteration
        if let Err(e) = store.delete(&path).await {
            eprintln!("delete failed at {len} bytes: {e}");
            break;
        }
    }

    eprintln!(
        "=== RESULT: exact max key length = {exact_max} bytes ({:.2} KB) ===",
        exact_max as f64 / 1024.0,
    );
    eprintln!(
        "  shard_slot_size = 64 KB (65536 bytes)",
    );
    eprintln!(
        "  bincode overhead = {} bytes (65536 - {exact_max})",
        65536_usize.saturating_sub(exact_max),
    );

    assert!(
        exact_max >= rawobjstr::MAX_KEY_LENGTH,
        "should support >= {} bytes but exact_max={exact_max}",
        rawobjstr::MAX_KEY_LENGTH,
    );
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: max_key_length -- multiple format configurations
// ═══════════════════════════════════════════════════════════════════════

/// Test several different max_key_length configs.
/// For each limit L, verify: key of length L is accepted; L+1 is rejected.
/// Also verify the limit is reported correctly in device_info().
#[tokio::test]
async fn max_key_format_different_limits() {
    for &limit in &[256usize, 512, 2048] {
        let tmp = NamedTempFile::new().unwrap();
        let device_size = rawobjstr::MIN_DEVICE_SIZE + 64 * 1024 * 1024;

        let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
            device_size,
            direct_io: false,
            index_slot_size: rawobjstr::INDEX_REGION_SIZE,
            max_key_length: limit,
            compression: Compression::None,
        })
        .unwrap();

        // device_info must report the correct limit
        let info = store.device_info();
        assert_eq!(info.max_key_length, limit, "limit={limit}: device_info mismatch");

        // Key exactly at the limit must be accepted
        let ok_key = Path::from("k".repeat(limit).as_str());
        store
            .put(&ok_key, PutPayload::from(Bytes::from("v")))
            .await
            .unwrap_or_else(|e| panic!("limit={limit}: key AT limit should succeed: {e}"));

        let data = store.get(&ok_key).await.unwrap().bytes().await.unwrap();
        assert_eq!(data, Bytes::from("v"), "limit={limit}: round-trip mismatch");

        // Key one byte over the limit must be rejected
        let over_key = Path::from("k".repeat(limit + 1).as_str());
        let err = store
            .put(&over_key, PutPayload::from(Bytes::from("v")))
            .await;
        assert!(
            err.is_err(),
            "limit={limit}: key ONE OVER limit must be rejected"
        );

        // copy() to destination with key one byte over limit must fail
        store
            .put(&Path::from("src.bin"), PutPayload::from(Bytes::from("data")))
            .await
            .unwrap();
        let copy_err = store.copy(&Path::from("src.bin"), &over_key).await;
        assert!(
            copy_err.is_err(),
            "limit={limit}: copy to over-limit destination must fail"
        );

        // rename() to over-limit destination must fail
        let rename_err = store
            .rename_if_not_exists(&Path::from("src.bin"), &over_key)
            .await;
        assert!(
            rename_err.is_err(),
            "limit={limit}: rename_if_not_exists to over-limit destination must fail"
        );

        // Flush and reopen; limit must survive
        store.flush_index().unwrap();
        drop(store);

        let store2 = RawObjectStore::open(tmp.path()).unwrap();
        let info2 = store2.device_info();
        assert_eq!(
            info2.max_key_length, limit,
            "limit={limit}: device_info after reopen mismatch"
        );

        println!("PASS max_key_format_different_limits: limit={limit}");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 3: Shard fill distribution with tiny files and long keys
// ═══════════════════════════════════════════════════════════════════════

/// Write 2 000 tiny files with varying key lengths (256-1024 bytes).
/// After flushing, read the superblock and check that shard fill sizes
/// are roughly uniform (max shard <= 4x average; no overflow).
#[tokio::test]
async fn shard_fill_distribution_tiny_files() {
    use rawobjstr::{NUM_SHARDS, SUPERBLOCK_SIZE};

    // Use a large device so we don't run out of data space
    let tmp = NamedTempFile::new().unwrap();
    let device_size = 512 * 1024 * 1024u64; // 512 MB

    // Format with max_key_length = 1024 (default; key lengths are 256-1024)
    let store = RawObjectStore::format_with_options(tmp.path(), FormatOptions {
        device_size,
        direct_io: false,
        index_slot_size: rawobjstr::INDEX_REGION_SIZE,
        max_key_length: 1024,
        compression: Compression::None,
    })
    .unwrap();

    let total = 2_000usize;
    let payload = Bytes::from(vec![0xABu8; 4]); // 4-byte payload

    // Write files with keys cycling through lengths 256..=1024
    for i in 0..total {
        // Key length cycles 256, 257, ..., 1024, 256, ...
        let key_len = 256 + (i % (1024 - 256 + 1));
        // Embed the index in the key to make every key unique
        let prefix = format!("{i:06}_");
        let fill_len = key_len.saturating_sub(prefix.len());
        let key = format!("{prefix}{}", "k".repeat(fill_len));
        assert_eq!(key.len(), key_len, "key length should be {key_len}");

        let path = Path::from(key.as_str());
        store
            .put(&path, PutPayload::from(payload.clone()))
            .await
            .unwrap_or_else(|e| panic!("put failed at i={i} key_len={key_len}: {e}"));
    }

    // Flush -- this is the most likely point for shard overflow
    store.flush_index().unwrap_or_else(|e| {
        panic!("flush_index failed after {total} tiny files with 256-1024 byte keys: {e}");
    });

    // Read the superblock to inspect per-shard fill sizes
    let mut f = std::fs::File::open(tmp.path()).unwrap();
    let mut sb_bytes = vec![0u8; SUPERBLOCK_SIZE as usize];
    f.read_exact(&mut sb_bytes).unwrap();
    let sb =
        rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();
    drop(f);

    let sizes: Vec<u32> = sb.shard_slots.iter().map(|s| s.size).collect();
    let non_zero: Vec<u32> = sizes.iter().copied().filter(|&s| s > 0).collect();

    assert_eq!(
        sizes.len(),
        NUM_SHARDS,
        "should have {NUM_SHARDS} shard slots"
    );
    assert!(
        !non_zero.is_empty(),
        "at least some shards should have data"
    );

    let max_size = *sizes.iter().max().unwrap() as u64;
    let sum: u64 = sizes.iter().map(|&s| s as u64).sum();
    let avg = sum / NUM_SHARDS as u64;

    // Sanity: no shard size should exceed the slot capacity
    let shard_slot = rawobjstr::INDEX_REGION_SIZE / NUM_SHARDS as u64;
    assert!(
        max_size <= shard_slot,
        "shard overflow: largest shard size {max_size} > slot size {shard_slot}"
    );

    // Distribution check: max shard should not exceed 4x the average
    // (CRC32c hashing is reasonably uniform for random-ish keys)
    if avg > 0 {
        assert!(
            max_size <= avg * 4,
            "shard fill very skewed: max={max_size} avg={avg} (max > 4x avg)"
        );
    }

    // All 2000 files must be readable after reopen
    drop(store);
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let all: Vec<_> = store2.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), total, "all {total} files must survive flush/reopen");

    println!(
        "PASS shard_fill_distribution_tiny_files: {total} files, \
         shard fill: min={} avg={avg} max={max_size} (slot={shard_slot})",
        sizes.iter().min().unwrap()
    );
}
