//! Corruption detection and superblock/index integrity tests:
//!   - Data bit-flip -> CRC error detection
//!   - Index corruption detection
//!   - Primary/backup superblock recovery
//!   - Both-superblock corruption rejection
//!   - Systematic byte-flip sweeps
//!   - Index region corruption
//!   - Reformat after total corruption
//!   - Repeated corrupt-and-recover stress

mod common;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rawobjstr::extent::padded_extent_size;
use rawobjstr::store::{OpenMode, RawObjectStore};
use rawobjstr::{DATA_START, SUPERBLOCK_SIZE};
use tempfile::NamedTempFile;

use common::{make_chunk, make_small, verify_chunk, CHUNK_SIZE, MEDIUM_DEVICE, SMALL_DEVICE};

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Helper: populate a device with known data and flush.
/// Returns (NamedTempFile, file_count). Caller must keep NamedTempFile alive.
async fn setup_populated_device(device_size: u64) -> (NamedTempFile, usize) {
    let tmp = NamedTempFile::new().unwrap();

    let store = RawObjectStore::format_with_size(
        tmp.path(),
        device_size,
        false,
    )
    .unwrap();
    for i in 0..20 {
        store
            .put(
                &Path::from(format!("data/{i:04}.bin")),
                PutPayload::from(make_chunk(i)),
            )
            .await
            .unwrap();
    }
    store.flush_index().unwrap();
    drop(store);

    (tmp, 20)
}

/// Helper: flip a single bit at byte_offset in a file.
fn flip_byte(path: &std::path::Path, byte_offset: u64) {
    let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(byte_offset)).unwrap();
    let mut b = [0u8; 1];
    f.read_exact(&mut b).unwrap();
    b[0] ^= 0xFF;
    f.seek(SeekFrom::Start(byte_offset)).unwrap();
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
}

/// Helper: overwrite a range with zeros.
fn zero_range(path: &std::path::Path, offset: u64, len: usize) {
    let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    let zeros = vec![0u8; len];
    f.write_all(&zeros).unwrap();
    f.sync_all().unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: Data bit-flip -> CRC error
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn corruption_detection_data_flip() {
    let tmp = NamedTempFile::new().unwrap();
    let dev = tmp.path().to_str().unwrap();
    common::cli_format(dev, &SMALL_DEVICE.to_string());

    // Write known data via CLI
    for i in 0..10usize {
        let data = make_chunk(i);
        common::cli_put(dev, &format!("data/{i:03}.db"), &data);
    }

    // Corrupt the payload area of the first file (after the 8 KB superblock region)
    // The first extent starts at DATA_START. Blocks are CRC-protected.
    // Flip some bytes in the payload data area (after the 4-byte block CRC).
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        let corrupt_offset = DATA_START + 4 + 100;  // skip block CRC (4 bytes) + 100 into payload
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF; // flip all bits
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
    }

    // Reading the corrupted file via CLI should fail.
    // OpenMode::Default reads block 0 of each extent on open and tombstones
    // any file whose CRC doesn't match.  So the CLI may report either a CRC
    // error (if the open mode didn't check) or NotFound (because the file was
    // quarantined into tombstones during open).
    let (stdout, stderr, code) = common::cli_get_fail(dev, "data/000.db");
    assert_ne!(code, Some(0), "expected get to fail on corrupted file");
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("orruption") || combined.contains("CRC")
            || combined.contains("checksum") || combined.contains("NotFound"),
        "error should mention corruption/CRC or NotFound (tombstoned): {combined}"
    );

    // Other files should still read fine via CLI
    for i in 1..10usize {
        let data = common::cli_get(dev, &format!("data/{i:03}.db"));
        verify_chunk(&data, i);
    }

    // Verify command should detect the corruption
    let (verify_out, verify_err, _verify_code) = common::run_cli_fail(&["verify", "--file", dev]);
    let verify_combined = format!("{verify_out}{verify_err}");
    assert!(
        verify_combined.contains("Errors") || verify_combined.contains("CrcMismatch")
            || verify_combined.contains("corrupt"),
        "verify should report corruption in its output: {verify_combined}"
    );

    println!("PASS corruption_detection_data_flip");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: Index corruption detection
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corruption_detection_index_flip() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    // Write data and flush
    {
        let store = RawObjectStore::format_with_size(tmp.path(), SMALL_DEVICE, false).unwrap();
        store
            .put(
                &Path::from("test.bin"),
                PutPayload::from(Bytes::from("hello")),
            )
            .await
            .unwrap();
        store.flush_index().unwrap();
    }

    // Read the superblock to find where the index is, then corrupt it
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_buf)
            .unwrap();
        let mut sb_bytes = vec![0u8; SUPERBLOCK_SIZE as usize];
        file.read_exact(&mut sb_bytes).unwrap();
        let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();

        // Flip a byte in the index region
        let corrupt_offset = sb.index_region_offset + 10;
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
    }

    // Also corrupt the backup superblock's index pointer so it can't recover
    // from the backup either. We corrupt both index regions.
    // Since we only flushed once, the txn_id=1 index is in region A.
    // The previous index has txn_id=0 (empty). Corrupting region A should
    // cause a fallback to region B (prev), but that's empty -> no data.
    // Let's verify that the store either returns IndexCorrupt or falls back.
    let result = RawObjectStore::open(&path_buf);
    match result {
        Ok(store) => {
            // It fell back to the previous (empty) index -- that's acceptable
            let files: Vec<_> = store.list(None).try_collect().await.unwrap();
            assert_eq!(
                files.len(),
                0,
                "corrupted index should lose unflushed-to-backup data"
            );
            println!("PASS corruption_detection_index_flip (fallback to prev)");
        }
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.contains("corrupt") || msg.contains("checksum"),
                "expected corruption error: {msg}"
            );
            println!("PASS corruption_detection_index_flip (rejected corrupt index)");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 3: Corrupt primary superblock -- backup should save us
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corrupt_primary_superblock_recovers_from_backup() {
    let (_tmp0, file_count) = setup_populated_device(MEDIUM_DEVICE).await;

    // Flip a byte in the primary superblock (offset 0..4095)
    // Try several positions: magic, version, flags, device_size, checksum area
    for &corrupt_pos in &[0u64, 4, 8, 16, 24, 50, 80] {
        // Re-create a fresh device each time
        let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;
        flip_byte(tmp.path(), corrupt_pos);

        let store = RawObjectStore::open(tmp.path())
            .expect(&format!("backup should recover when primary byte {corrupt_pos} is flipped"));

        // All files should be intact (recovered from backup superblock)
        let files: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(
            files.len(), file_count,
            "byte {corrupt_pos}: expected {file_count} files, got {}",
            files.len()
        );

        // Verify content
        for i in 0..file_count {
            let data = store
                .get(&Path::from(format!("data/{i:04}.bin")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            verify_chunk(&data, i);
        }
    }
    println!("PASS corrupt_primary_superblock_recovers_from_backup");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 4: Corrupt backup superblock -- primary should still work
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corrupt_backup_superblock_primary_still_works() {
    let (_tmp0, file_count) = setup_populated_device(MEDIUM_DEVICE).await;

    // Flip bytes in the backup superblock (offset 4096..8191)
    for &corrupt_pos in &[SUPERBLOCK_SIZE, SUPERBLOCK_SIZE + 4, SUPERBLOCK_SIZE + 50, SUPERBLOCK_SIZE + 80] {
        let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;
        flip_byte(tmp.path(), corrupt_pos);

        let store = RawObjectStore::open(tmp.path())
            .expect(&format!("primary should work when backup byte {corrupt_pos} is flipped"));

        let files: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert_eq!(files.len(), file_count);

        for i in 0..file_count {
            let data = store
                .get(&Path::from(format!("data/{i:04}.bin")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            verify_chunk(&data, i);
        }
    }
    println!("PASS corrupt_backup_superblock_primary_still_works");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 5: Corrupt BOTH superblocks -- must refuse to open
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corrupt_both_superblocks_refuses_to_open() {
    let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

    // Flip a byte in both primary and backup superblocks
    flip_byte(tmp.path(), 10);                   // primary
    flip_byte(tmp.path(), SUPERBLOCK_SIZE + 10); // backup

    let result = RawObjectStore::open(tmp.path());
    assert!(result.is_err(), "should refuse to open with both superblocks corrupt");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("corrupt") || err.contains("magic") || err.contains("checksum"),
        "error should mention corruption: {err}"
    );
    println!("PASS corrupt_both_superblocks_refuses_to_open");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 6: Zero out both entire superblocks -- total destruction
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn zero_both_superblocks_refuses_to_open() {
    let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

    zero_range(tmp.path(), 0, SUPERBLOCK_SIZE as usize);
    zero_range(tmp.path(), SUPERBLOCK_SIZE, SUPERBLOCK_SIZE as usize);

    let result = RawObjectStore::open(tmp.path());
    assert!(result.is_err(), "should refuse to open with both superblocks zeroed");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("corrupt") || err.contains("magic") || err.contains("formatted"),
        "error should mention corruption/format: {err}"
    );
    println!("PASS zero_both_superblocks_refuses_to_open");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 7: Corrupt magic bytes in both superblocks
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corrupt_magic_in_both_superblocks() {
    let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

    // Overwrite the magic bytes (first 8 bytes) in both copies
    zero_range(tmp.path(), 0, 8);
    zero_range(tmp.path(), SUPERBLOCK_SIZE, 8);

    let result = RawObjectStore::open(tmp.path());
    assert!(result.is_err(), "should refuse with bad magic");
    println!("PASS corrupt_magic_in_both_superblocks");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 8: Systematic bit-flip sweep across primary superblock
// ═══════════════════════════════════════════════════════════════════════

/// Systematic bit-flip sweep across every byte of the primary superblock.
/// The backup should always save us.
#[tokio::test]
async fn sweep_flip_every_byte_primary_superblock() {
    // Only sweep the first ~100 bytes (where actual data lives; rest is padding)
    let sweep_len = 100;
    let mut recovered = 0u32;
    let failed = 0u32;

    for byte_pos in 0..sweep_len {
        let (tmp, _) = setup_populated_device(SMALL_DEVICE).await;
        flip_byte(tmp.path(), byte_pos);

        match RawObjectStore::open(tmp.path()) {
            Ok(store) => {
                // Should recover from backup -- verify data is intact
                let files: Vec<_> = store.list(None).try_collect().await.unwrap();
                assert_eq!(files.len(), 20, "byte {byte_pos}: expected 20 files, got {}", files.len());
                recovered += 1;
            }
            Err(_) => {
                // If the flip happened to also corrupt backup (shouldn't with only primary flip)
                // this means our logic has a bug
                panic!("byte {byte_pos}: primary-only flip should recover from backup");
            }
        }
    }
    println!("PASS sweep_flip_every_byte_primary_superblock: {recovered} recovered, {failed} failed (of {sweep_len})");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 9: Flip bits in index shard region -- checksum mismatch
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corrupt_index_region_detects_checksum_mismatch() {
    use rawobjstr::NUM_SHARDS;

    // For each flip offset we want to test, set up a fresh populated device.
    let flip_offsets: Vec<u64> = vec![0, 1, 10, 50];

    for &flip_within_shard in &flip_offsets {
        let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

        // Read superblock to find shard layout
        let mut f = File::open(tmp.path()).unwrap();
        let mut sb_bytes = vec![0u8; SUPERBLOCK_SIZE as usize];
        f.read_exact(&mut sb_bytes).unwrap();
        let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();

        let shard_slot_size = sb.index_slot_capacity / NUM_SHARDS as u64;
        let index_total = sb.index_slot_capacity * 2;
        let index_region_a = sb.device_size - index_total;
        let index_region_b = sb.device_size - sb.index_slot_capacity;

        // Find a shard that has data and is big enough for the flip offset
        let shard_idx = sb.shard_slots.iter().position(|s| s.size as u64 > flip_within_shard)
            .expect("should have at least one populated shard");

        let meta = &sb.shard_slots[shard_idx];
        let shard_offset = if meta.active_slot == 0 {
            index_region_a + shard_idx as u64 * shard_slot_size
        } else {
            index_region_b + shard_idx as u64 * shard_slot_size
        };

        flip_byte(tmp.path(), shard_offset + flip_within_shard);

        let result = RawObjectStore::open(tmp.path());
        assert!(
            result.is_err(),
            "shard {shard_idx} flip at offset {flip_within_shard}: should fail with corrupt shard"
        );
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("corrupt") || err.contains("checksum") || err.contains("deserial"),
            "shard {shard_idx} flip at {flip_within_shard}: error should mention corruption: {err}"
        );
    }
    println!("PASS corrupt_index_region_detects_checksum_mismatch");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 10: Zero out all active shard slots -- reject as corrupt
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn zero_index_region_detected() {
    use rawobjstr::NUM_SHARDS;

    let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

    let mut f = File::open(tmp.path()).unwrap();
    let mut sb_bytes = vec![0u8; SUPERBLOCK_SIZE as usize];
    f.read_exact(&mut sb_bytes).unwrap();
    let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();

    let shard_slot_size = sb.index_slot_capacity / NUM_SHARDS as u64;
    let index_total = sb.index_slot_capacity * 2;
    let index_region_a = sb.device_size - index_total;
    let index_region_b = sb.device_size - sb.index_slot_capacity;

    // Zero every active shard that has data
    for (i, meta) in sb.shard_slots.iter().enumerate() {
        if meta.size == 0 { continue; }
        let offset = if meta.active_slot == 0 {
            index_region_a + i as u64 * shard_slot_size
        } else {
            index_region_b + i as u64 * shard_slot_size
        };
        zero_range(tmp.path(), offset, meta.size as usize);
    }

    let result = RawObjectStore::open(tmp.path());
    assert!(
        result.is_err(),
        "zeroed index region should fail to open"
    );
    println!("PASS zero_index_region_detected");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 11: Corrupt the index checksum field in the superblock
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn corrupt_superblock_index_checksum_field() {
    let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

    // The index_checksum field is at a specific offset in the bincode-serialized
    // superblock. We need to find it. It's easier to just corrupt the bytes
    // right after device_size+block_alignment+index_region_offset+index_region_size+txn_id
    // which is: 8(magic) + 4(version) + 4(flags) + 8(device_size) + 8(block_alignment) +
    //           8(index_region_offset) + 8(index_region_size) + 8(txn_id) = 56
    // So index_checksum is at byte offset 56 in the serialized superblock.
    //
    // We corrupt it in BOTH superblock copies so neither can parse correctly.
    // Actually -- let's flip the index_checksum in both copies.
    let checksum_offset_in_sb = 56u64; // approximate, may vary by bincode

    // Corrupt in both superblock copies
    flip_byte(tmp.path(), checksum_offset_in_sb);
    flip_byte(tmp.path(), SUPERBLOCK_SIZE + checksum_offset_in_sb);

    let result = RawObjectStore::open(tmp.path());
    // Either both superblocks are now corrupt (CRC fails) -> SuperblockCorrupt
    // OR the superblocks parse but the index_checksum is wrong -> IndexCorrupt
    assert!(
        result.is_err(),
        "corrupted index_checksum should prevent opening"
    );
    println!("PASS corrupt_superblock_index_checksum_field");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 12: Corrupt active index after multiple flushes
// ═══════════════════════════════════════════════════════════════════════

/// After multiple flushes, corrupt the active index but leave the previous
/// index region intact. The store should fail (since it only reads the active
/// index pointed to by the superblock), proving we rely on the superblock pointer.
#[tokio::test]
async fn corrupt_active_index_after_multiple_flushes() {
    let tmp = NamedTempFile::new().unwrap();
    let path_buf = tmp.path().to_path_buf();

    {
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        // Flush #1: write 10 files
        for i in 0..10 {
            store.put(
                &Path::from(format!("batch1/{i:03}.bin")),
                PutPayload::from(make_small(i, 4096)),
            ).await.unwrap();
        }
        store.flush_index().unwrap();

        // Flush #2: write 10 more
        for i in 10..20 {
            store.put(
                &Path::from(format!("batch2/{i:03}.bin")),
                PutPayload::from(make_small(i, 4096)),
            ).await.unwrap();
        }
        store.flush_index().unwrap();

        // Flush #3: write 10 more
        for i in 20..30 {
            store.put(
                &Path::from(format!("batch3/{i:03}.bin")),
                PutPayload::from(make_small(i, 4096)),
            ).await.unwrap();
        }
        store.flush_index().unwrap();
    }

    // Read superblock to find active index
    let mut f = File::open(&path_buf).unwrap();
    let mut sb_bytes = vec![0u8; SUPERBLOCK_SIZE as usize];
    f.read_exact(&mut sb_bytes).unwrap();
    let sb = rawobjstr::superblock::Superblock::from_bytes(&sb_bytes).unwrap();

    assert!(sb.txn_id >= 3, "should have done at least 3 flushes: txn_id={}", sb.txn_id);

    // Corrupt the active index region
    flip_byte(&path_buf, sb.index_region_offset + 5);

    let result = RawObjectStore::open(&path_buf);
    assert!(
        result.is_err(),
        "corrupted active index should prevent opening"
    );
    println!("PASS corrupt_active_index_after_multiple_flushes");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 13: Selective data corruption isolates damaged files
// ═══════════════════════════════════════════════════════════════════════

/// Write, flush, then corrupt data extents of specific files.
/// Verify: corrupt files fail to read, non-corrupt files still readable.
#[tokio::test]
async fn selective_data_corruption_isolates_damaged_files() {
    let (tmp, file_count) = setup_populated_device(MEDIUM_DEVICE).await;

    // Corrupt files 0, 5, and 15 by flipping bytes in their data regions
    // File 0 starts at DATA_START, each file is ~1MB + 32-byte header, padded to 4096
    // The exact offsets depend on allocation order. We'll read the index to find them.
    let store = RawObjectStore::open(tmp.path()).unwrap();

    // Read index to get extent offsets for specific files
    let corrupt_indices = [0usize, 5, 15];
    let mut offsets_to_corrupt = Vec::new();
    for &i in &corrupt_indices {
        // Get the file's metadata (which includes its size, so we know the extent location)
        let data = store
            .get(&Path::from(format!("data/{i:04}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.len(), CHUNK_SIZE);
        // The extent for file i is at some aligned offset.
        // We can find it by looking at the index. But since the index is internal,
        // let's just use the known layout: files are allocated sequentially from DATA_START.
        // File i: offset = DATA_START + i * padded_extent_size(CHUNK_SIZE as u64)
        let padded = padded_extent_size(CHUNK_SIZE as u64).unwrap();
        let file_offset = DATA_START + (i as u64) * padded;
        offsets_to_corrupt.push((i, file_offset));
    }
    drop(store);

    // Now corrupt each of those files' payload areas
    for &(_i, offset) in &offsets_to_corrupt {
        // Corrupt 100 bytes into the payload data area (after 4-byte block CRC)
        flip_byte(tmp.path(), offset + 4 + 100);
    }

    // Reopen with SkipVerify so the corrupt entries remain in the index
    // for the CRC-check-on-read to detect.
    let store = RawObjectStore::open_with_mode(tmp.path(), OpenMode::SkipVerify).unwrap();

    // Corrupted files should fail with CRC error
    for &(i, _) in &offsets_to_corrupt {
        let is_error = match store.get(&Path::from(format!("data/{i:04}.bin"))).await {
            Err(_) => true,
            Ok(r) => r.bytes().await.is_err(),
        };
        assert!(is_error, "file {i} should fail CRC check");
    }

    // Non-corrupted files should still be fine
    let non_corrupt: Vec<usize> = (0..file_count)
        .filter(|i| !corrupt_indices.contains(i))
        .collect();
    for i in non_corrupt {
        let data = store
            .get(&Path::from(format!("data/{i:04}.bin")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        verify_chunk(&data, i);
    }

    println!("PASS selective_data_corruption_isolates_damaged_files");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 14: Flip checksum field in both superblocks
// ═══════════════════════════════════════════════════════════════════════

/// Flip bits systematically across the superblock checksum field itself.
/// When both copies have their checksum flipped, the store must refuse.
#[tokio::test]
async fn corrupt_checksum_field_in_both_superblocks() {
    // We'll iterate over every byte of the first 96 bytes and flip the same
    // position in both superblocks. Every single one should result in rejection.
    let mut all_rejected = 0u32;

    for byte_pos in 0..96u64 {
        let (tmp, _) = setup_populated_device(SMALL_DEVICE).await;

        flip_byte(tmp.path(), byte_pos);                   // primary
        flip_byte(tmp.path(), SUPERBLOCK_SIZE + byte_pos);  // backup

        match RawObjectStore::open(tmp.path()) {
            Err(_) => all_rejected += 1,
            Ok(_) => {
                // This means the flip didn't affect the checksum validation
                // (e.g., padding area or the flip didn't change the byte).
                // In our case, flipping 0x00 with ^0xFF at padding would still
                // break the CRC. So if we get here, something is unexpected.
                panic!("byte {byte_pos}: both superblocks flipped but store still opened!");
            }
        }
    }
    println!("PASS corrupt_checksum_field_in_both_superblocks: {all_rejected} rejected out of 96 positions");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 15: Reformat after total corruption
// ═══════════════════════════════════════════════════════════════════════

/// Corrupt both superblocks, then try to use the store (format fresh, write, read).
/// Verify that the device can be re-formatted (wiped) even after total corruption.
#[tokio::test]
async fn reformat_after_total_corruption() {
    let (tmp, _) = setup_populated_device(MEDIUM_DEVICE).await;

    // Total corruption: zero both superblocks and index
    zero_range(tmp.path(), 0, SUPERBLOCK_SIZE as usize);
    zero_range(tmp.path(), SUPERBLOCK_SIZE, SUPERBLOCK_SIZE as usize);

    // Can't open
    assert!(RawObjectStore::open(tmp.path()).is_err());

    // But can re-format
    let store = RawObjectStore::format(tmp.path(), false).unwrap();
    store
        .put(
            &Path::from("after_corruption.bin"),
            PutPayload::from(make_chunk(42)),
        )
        .await
        .unwrap();

    let data = store
        .get(&Path::from("after_corruption.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    verify_chunk(&data, 42);

    store.flush_index().unwrap();
    drop(store); // release flock before reopening

    // Reopen after format -- should work
    let store2 = RawObjectStore::open(tmp.path()).unwrap();
    let data = store2
        .get(&Path::from("after_corruption.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    verify_chunk(&data, 42);

    println!("PASS reformat_after_total_corruption");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 16: Repeated corrupt-and-recover stress
// ═══════════════════════════════════════════════════════════════════════

/// Stress: repeated flush -> corrupt cycle.
/// Write data, flush, corrupt the index, verify failure, re-format, repeat.
#[tokio::test]
async fn stress_repeated_corrupt_and_recover() {
    let tmp = NamedTempFile::new().unwrap();

    for cycle in 0..5 {
        // Format fresh
        let store = RawObjectStore::format_with_size(tmp.path(), MEDIUM_DEVICE, false).unwrap();

        // Write 10 files
        for i in 0..10 {
            let idx = cycle * 100 + i;
            store
                .put(
                    &Path::from(format!("cycle_{cycle}/f{i:02}.bin")),
                    PutPayload::from(make_small(idx, 8192)),
                )
                .await
                .unwrap();
        }
        store.flush_index().unwrap();

        // Verify all files
        for i in 0..10 {
            let idx = cycle * 100 + i;
            let data = store
                .get(&Path::from(format!("cycle_{cycle}/f{i:02}.bin")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(data.len(), 8192);
            let tag = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
            assert_eq!(tag, idx, "cycle {cycle} file {i}: tag mismatch");
        }
        drop(store);

        // Corrupt both superblocks
        flip_byte(tmp.path(), 20);
        flip_byte(tmp.path(), SUPERBLOCK_SIZE + 20);

        // Verify failure
        assert!(
            RawObjectStore::open(tmp.path()).is_err(),
            "cycle {cycle}: should fail with corrupted superblocks"
        );
    }
    println!("PASS stress_repeated_corrupt_and_recover: 5 cycles");
}
