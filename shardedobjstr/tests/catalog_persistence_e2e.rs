//! End-to-end tests for catalog persistence, serialization,
//! and bulk mutation operations (clear, replace).

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::RawObjectStore;

use shardedobjstr::catalog::{Catalog, CatalogPersistence, PlacementEntry};
use shardedobjstr::ShardedObjectStore;

use common::{build_cluster, flush_all, format_shard};

// -- Helpers -----------------------------------------------------------------

fn setup_3shard(
    dir: &tempfile::TempDir,
    prefix: &str,
) -> (ShardedObjectStore, Vec<Arc<RawObjectStore>>) {
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..3)
        .map(|i| format_shard(&dir.path().join(format!("{prefix}{i}.raw")), sz))
        .collect();
    let cluster = build_cluster(&raws, 2);
    (cluster, raws)
}

fn put_objects(
    rt: &tokio::runtime::Runtime,
    cluster: &ShardedObjectStore,
    raws: &[Arc<RawObjectStore>],
    keys: &[&str],
) {
    rt.block_on(async {
        for (i, key) in keys.iter().enumerate() {
            let data = vec![0x10 + i as u8; 512];
            cluster
                .put(&Path::from(*key), PutPayload::from(Bytes::from(data)))
                .await
                .unwrap();
        }
    });
    flush_all(raws);
}

// -- Tests -------------------------------------------------------------------

#[test]
fn catalog_save_load_json_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["doc/a.txt", "doc/b.txt", "img/photo.jpg"];
    put_objects(&rt, &cluster, &raws, keys);

    // Configure JSON persistence.
    let json_path = dir.path().join("catalog.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));

    // Save.
    cluster.save_catalog().expect("save_catalog failed");
    assert!(json_path.exists(), "JSON file should exist after save");

    // Build a fresh cluster and load the saved catalog.
    let (cluster2, _raws2) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::json(&json_path));
    cluster2.load_catalog().expect("load_catalog failed");

    // Verify all keys are present with correct sizes.
    for key in keys {
        let entry = cluster2.placement(key);
        assert!(entry.is_some(), "key '{}' should exist after load", key);
        let entry = entry.unwrap();
        assert_eq!(entry.size, 512, "key '{}' should have size 512", key);
        assert!(!entry.shards.is_empty(), "key '{}' should have shards", key);
    }
}

#[test]
fn catalog_save_load_bincode_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["bin/x.dat", "bin/y.dat"];
    put_objects(&rt, &cluster, &raws, keys);

    let bin_path = dir.path().join("catalog.bin");
    cluster.set_persistence(CatalogPersistence::bincode(&bin_path));

    cluster.save_catalog().expect("save bincode failed");
    assert!(bin_path.exists(), "bincode file should exist after save");

    // Load into fresh cluster.
    let (cluster2, _raws2) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::bincode(&bin_path));
    cluster2.load_catalog().expect("load bincode failed");

    for key in keys {
        let entry = cluster2.placement(key);
        assert!(entry.is_some(), "key '{}' should exist after bincode load", key);
    }
}

#[test]
fn catalog_persistence_none_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["noop/obj.bin"]);

    // Default persistence is None.
    assert!(cluster.persistence().is_none());

    // save_catalog should succeed as a no-op.
    cluster.save_catalog().expect("save with None should succeed");

    // load_catalog should succeed and return an empty catalog.
    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.load_catalog().expect("load with None should succeed");
    assert!(
        cluster2.placement("noop/obj.bin").is_none(),
        "None persistence should not load anything"
    );
}

#[test]
fn catalog_load_missing_file_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_3shard(&dir, "s");

    let missing = dir.path().join("does_not_exist.json");
    cluster.set_persistence(CatalogPersistence::json(&missing));

    // load_catalog from a non-existent file should succeed with empty catalog.
    cluster.load_catalog().expect("load from missing file should succeed");
    assert_eq!(cluster.catalog().len(), 0);
}

#[test]
fn catalog_save_to_file_load_from_file_e2e() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["data/one.bin", "data/two.bin", "data/three.bin"];
    put_objects(&rt, &cluster, &raws, keys);

    // Save directly via Catalog method.
    let file_path = dir.path().join("direct.json");
    cluster.catalog().save_to_file(&file_path).expect("save_to_file");

    // Load into a standalone Catalog.
    let loaded = Catalog::load_from_file(&file_path).expect("load_from_file");
    assert_eq!(loaded.len(), keys.len());
    for key in keys {
        assert!(loaded.get(key).is_some(), "key '{}' missing after load_from_file", key);
    }
}

#[test]
fn catalog_clear_empties_all() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["clear/a.bin", "clear/b.bin"];
    put_objects(&rt, &cluster, &raws, keys);
    assert_eq!(cluster.catalog().len(), 2);

    cluster.catalog().clear();
    assert_eq!(cluster.catalog().len(), 0);
    assert!(cluster.placement("clear/a.bin").is_none());
}

#[test]
fn catalog_replace_swaps_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["old/x.bin"]);
    assert!(cluster.placement("old/x.bin").is_some());

    // Build a replacement map with different keys.
    let mut new_map = HashMap::new();
    new_map.insert(
        "new/y.bin".to_string(),
        PlacementEntry {
            shards: vec![0, 1],
            size: 999,
            crc32c: Some(0xDEAD),
            updated: chrono::Utc::now(),
            meta_len: 0,
        },
    );

    cluster.catalog().replace(new_map);

    // Old key gone, new key present.
    assert!(cluster.placement("old/x.bin").is_none());
    let entry = cluster.placement("new/y.bin").unwrap();
    assert_eq!(entry.size, 999);
    assert_eq!(entry.crc32c, Some(0xDEAD));
}

#[test]
fn catalog_persistence_builder_api() {
    let dir = tempfile::tempdir().unwrap();
    let sz = 64 * 1024 * 1024u64;
    let raws: Vec<Arc<RawObjectStore>> = (0..2)
        .map(|i| format_shard(&dir.path().join(format!("s{i}.raw")), sz))
        .collect();
    let stores: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();

    let json_path = dir.path().join("builder.json");
    let cluster = ShardedObjectStore::new(stores, 1)
        .with_persistence(CatalogPersistence::json(&json_path));

    // Builder should have set persistence.
    assert!(!cluster.persistence().is_none());

    // Round-trip: put, save, reload.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        cluster
            .put(
                &Path::from("builder/obj.bin"),
                PutPayload::from(Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();
    });
    flush_all(&raws);
    rt.block_on(cluster.rebuild_catalog()).unwrap();

    cluster.save_catalog().expect("save via builder");

    // Fresh cluster, load.
    let stores2: Vec<Arc<dyn ObjectStore>> = raws.iter().map(|s| s.clone() as _).collect();
    let cluster2 = ShardedObjectStore::new(stores2, 1)
        .with_persistence(CatalogPersistence::json(&json_path));
    cluster2.load_catalog().expect("load via builder");
    assert!(cluster2.placement("builder/obj.bin").is_some());
}

#[test]
fn catalog_json_and_bincode_produce_same_data() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["compat/a.bin", "compat/b.bin", "compat/c.bin"];
    put_objects(&rt, &cluster, &raws, keys);

    // Save as JSON.
    let json_path = dir.path().join("compat.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.save_catalog().unwrap();

    // Save as bincode.
    let bin_path = dir.path().join("compat.bin");
    cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
    cluster.save_catalog().unwrap();

    // Load both into fresh clusters and compare.
    let (c_json, _) = setup_3shard(&dir, "b");
    c_json.set_persistence(CatalogPersistence::json(&json_path));
    c_json.load_catalog().unwrap();

    let (c_bin, _) = setup_3shard(&dir, "c");
    c_bin.set_persistence(CatalogPersistence::bincode(&bin_path));
    c_bin.load_catalog().unwrap();

    for key in keys {
        let j = c_json.placement(key).expect("JSON missing key");
        let b = c_bin.placement(key).expect("bincode missing key");
        assert_eq!(j.size, b.size, "size mismatch for {}", key);
        assert_eq!(j.shards, b.shards, "shards mismatch for {}", key);
        assert_eq!(j.meta_len, b.meta_len, "meta_len mismatch for {}", key);
    }
}

// =====================================================================
// Catalog API: try_insert, with_entries, load_into
// =====================================================================

#[test]
fn catalog_try_insert_returns_false_if_exists() {
    let cat = Catalog::new();
    let inserted = cat.try_insert("obj/a.bin".into(), vec![0, 1], 1024);
    assert!(inserted, "first insert should succeed");

    let inserted2 = cat.try_insert("obj/a.bin".into(), vec![2], 512);
    assert!(!inserted2, "second insert with same key should return false");

    // Original entry should be unchanged.
    let entry = cat.get("obj/a.bin").unwrap();
    assert_eq!(entry.shards, vec![0, 1]);
    assert_eq!(entry.size, 1024);
}

#[test]
fn catalog_try_insert_different_keys_both_succeed() {
    let cat = Catalog::new();
    assert!(cat.try_insert("key/a".into(), vec![0], 100));
    assert!(cat.try_insert("key/b".into(), vec![1], 200));
    assert_eq!(cat.len(), 2);
}

#[test]
fn catalog_with_entries_provides_readonly_access() {
    let cat = Catalog::new();
    cat.put("ent/a".into(), vec![0], 100, None, 0);
    cat.put("ent/b".into(), vec![1], 200, None, 0);
    cat.put("ent/c".into(), vec![0, 1], 300, None, 0);

    let total_size: u64 = cat.with_entries(|map| {
        map.values().map(|e| e.size).sum()
    });
    assert_eq!(total_size, 600, "with_entries should see all entries");

    let count: usize = cat.with_entries(|map| map.len());
    assert_eq!(count, 3);
}

#[test]
fn catalog_load_into_replaces_all_entries() {
    let cat1 = Catalog::new();
    cat1.put("old/a".into(), vec![0], 100, None, 0);
    cat1.put("old/b".into(), vec![1], 200, None, 0);

    let cat2 = Catalog::new();
    cat2.put("new/x".into(), vec![2], 999, None, 0);

    cat1.load_into(cat2);

    // cat1 should now contain only cat2's entries.
    assert_eq!(cat1.len(), 1);
    assert!(cat1.get("old/a").is_none(), "old entries should be gone");
    assert!(cat1.get("old/b").is_none(), "old entries should be gone");
    let entry = cat1.get("new/x").unwrap();
    assert_eq!(entry.size, 999);
    assert_eq!(entry.shards, vec![2]);
}

#[test]
fn catalog_load_into_empty_clears_target() {
    let cat1 = Catalog::new();
    cat1.put("key/a".into(), vec![0], 100, None, 0);

    let cat2 = Catalog::new(); // empty

    cat1.load_into(cat2);
    assert_eq!(cat1.len(), 0, "loading empty catalog should clear target");
}

// =====================================================================
// Dirty tracking
// =====================================================================

#[test]
fn catalog_dirty_after_put() {
    let cat = Catalog::new();
    assert!(!cat.is_dirty(), "new catalog should not be dirty");

    cat.put("key/a".into(), vec![0], 100, None, 0);
    assert!(cat.is_dirty(), "catalog should be dirty after put");

    cat.clear_dirty();
    assert!(!cat.is_dirty(), "catalog should be clean after clear_dirty");
}

#[test]
fn catalog_dirty_after_remove() {
    let cat = Catalog::new();
    cat.put("key/a".into(), vec![0], 100, None, 0);
    cat.clear_dirty();

    cat.remove("key/a");
    assert!(cat.is_dirty(), "catalog should be dirty after remove");
}

#[test]
fn catalog_dirty_after_clear() {
    let cat = Catalog::new();
    cat.put("key/a".into(), vec![0], 100, None, 0);
    cat.clear_dirty();

    cat.clear();
    assert!(cat.is_dirty(), "catalog should be dirty after clear");
}

#[test]
fn catalog_dirty_after_replace() {
    let cat = Catalog::new();
    cat.put("key/a".into(), vec![0], 100, None, 0);
    cat.clear_dirty();

    cat.replace(HashMap::new());
    assert!(cat.is_dirty(), "catalog should be dirty after replace");
}

#[test]
fn catalog_dirty_after_add_replica() {
    let cat = Catalog::new();
    cat.put("key/a".into(), vec![0], 100, None, 0);
    cat.clear_dirty();

    cat.add_replica("key/a", 1, 100, 0);
    assert!(cat.is_dirty(), "catalog should be dirty after add_replica");
}

#[test]
fn catalog_load_into_clears_dirty() {
    let cat = Catalog::new();
    cat.put("key/a".into(), vec![0], 100, None, 0);
    assert!(cat.is_dirty());

    let other = Catalog::new();
    other.put("key/b".into(), vec![1], 200, None, 0);

    cat.load_into(other);
    assert!(!cat.is_dirty(), "load_into should clear dirty flag");
}

#[test]
fn save_catalog_if_dirty_skips_when_clean() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _raws) = setup_3shard(&dir, "s");
    let json_path = dir.path().join("dirty_test.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));

    // No mutations, so save_if_dirty should return false.
    let saved = cluster.save_catalog_if_dirty().unwrap();
    assert!(!saved, "should not save when catalog is clean");
    assert!(!json_path.exists(), "file should not be created when clean");
}

#[test]
fn save_catalog_if_dirty_saves_when_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let json_path = dir.path().join("dirty_test.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));

    put_objects(&rt, &cluster, &raws, &["dirty/obj.bin"]);

    let saved = cluster.save_catalog_if_dirty().unwrap();
    assert!(saved, "should save when catalog is dirty");
    assert!(json_path.exists(), "file should exist after save");

    // Second call should not save (dirty cleared).
    let saved2 = cluster.save_catalog_if_dirty().unwrap();
    assert!(!saved2, "second save_if_dirty should skip");
}

// =====================================================================
// Checksum (CRC) validation tests
// =====================================================================

#[test]
fn json_checksum_detects_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["crc/a.bin", "crc/b.bin"]);

    let json_path = dir.path().join("checksum.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.save_catalog().unwrap();

    // Corrupt the file by flipping a byte in the middle.
    let mut data = std::fs::read(&json_path).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&json_path, &data).unwrap();

    // Loading should fail with a checksum or parse error.
    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::json(&json_path));
    let result = cluster2.load_catalog();
    assert!(result.is_err(), "corrupted JSON should fail to load");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("checksum") || err_msg.contains("invalid") || err_msg.contains("expected"),
        "error should mention checksum or invalid data, got: {}",
        err_msg
    );
}

#[test]
fn bincode_checksum_detects_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["crc/a.bin", "crc/b.bin"]);

    let bin_path = dir.path().join("checksum.bin");
    cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
    cluster.save_catalog().unwrap();

    // Corrupt the payload (bytes after the 4-byte CRC header).
    let mut data = std::fs::read(&bin_path).unwrap();
    let mid = 4 + (data.len() - 4) / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&bin_path, &data).unwrap();

    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::bincode(&bin_path));
    let result = cluster2.load_catalog();
    assert!(result.is_err(), "corrupted bincode should fail to load");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("checksum"),
        "error should mention checksum mismatch, got: {}",
        err_msg
    );
}

#[test]
fn json_tampered_checksum_field_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["tamper/a.bin"]);

    let json_path = dir.path().join("tamper.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.save_catalog().unwrap();

    // Parse the JSON, change the checksum value, and write it back.
    let raw = std::fs::read_to_string(&json_path).unwrap();
    let mut val: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(ck) = val.get_mut("checksum") {
        let old: u64 = ck.as_u64().unwrap_or(0);
        *ck = serde_json::Value::from(old.wrapping_add(1));
    }
    std::fs::write(&json_path, serde_json::to_vec(&val).unwrap()).unwrap();

    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::json(&json_path));
    let result = cluster2.load_catalog();
    assert!(result.is_err(), "tampered checksum should fail to load");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("checksum"),
        "error should mention checksum, got: {}",
        err_msg
    );
}

#[test]
fn bincode_tampered_checksum_header_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["tamper/a.bin"]);

    let bin_path = dir.path().join("tamper.bin");
    cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
    cluster.save_catalog().unwrap();

    // Change the 4-byte CRC header to a wrong value.
    let mut data = std::fs::read(&bin_path).unwrap();
    let stored_crc = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let bad_crc = stored_crc.wrapping_add(1);
    data[0..4].copy_from_slice(&bad_crc.to_le_bytes());
    std::fs::write(&bin_path, &data).unwrap();

    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::bincode(&bin_path));
    let result = cluster2.load_catalog();
    assert!(result.is_err(), "tampered CRC header should fail to load");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("checksum"),
        "error should mention checksum, got: {}",
        err_msg
    );
}

#[test]
fn bincode_truncated_file_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _) = setup_3shard(&dir, "s");

    // Write a file that is too short to contain even the CRC header.
    let bin_path = dir.path().join("short.bin");
    std::fs::write(&bin_path, &[0u8; 2]).unwrap();

    cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
    let result = cluster.load_catalog();
    assert!(result.is_err(), "truncated file should fail to load");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("too short"),
        "error should mention file too short, got: {}",
        err_msg
    );
}

// =====================================================================
// Data loss without persistence (no flush = data lost)
// =====================================================================

#[test]
fn no_save_before_drop_loses_catalog_data() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("nosave.json");

    // Create a cluster, put data, but do NOT save. Then drop.
    {
        let (cluster, raws) = setup_3shard(&dir, "a");
        let rt = tokio::runtime::Runtime::new().unwrap();
        put_objects(&rt, &cluster, &raws, &["lost/data.bin"]);
        cluster.set_persistence(CatalogPersistence::json(&json_path));
        // Intentionally NOT calling save_catalog() before drop.
    }

    // The file should not exist since we never saved.
    assert!(
        !json_path.exists(),
        "catalog file should not exist without explicit save"
    );

    // A fresh cluster loading from this path gets nothing.
    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::json(&json_path));
    cluster2.load_catalog().unwrap();
    assert_eq!(cluster2.catalog().len(), 0, "catalog should be empty");
}

#[test]
fn save_then_new_puts_without_flush_loses_new_data() {
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("partial.json");

    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Put initial data and save.
    put_objects(&rt, &cluster, &raws, &["saved/obj.bin"]);
    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.save_catalog().unwrap();

    // Put more data but do NOT save again.
    put_objects(&rt, &cluster, &raws, &["unsaved/obj.bin"]);

    // Load from file: only the first object should be there.
    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::json(&json_path));
    cluster2.load_catalog().unwrap();

    assert!(
        cluster2.placement("saved/obj.bin").is_some(),
        "saved object should survive reload"
    );
    assert!(
        cluster2.placement("unsaved/obj.bin").is_none(),
        "unsaved object should be lost on reload"
    );
}

// =====================================================================
// JSON envelope format: checksum is embedded in the file
// =====================================================================

#[test]
fn json_file_contains_checksum_and_data_fields() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "s");
    let rt = tokio::runtime::Runtime::new().unwrap();

    put_objects(&rt, &cluster, &raws, &["field/a.bin"]);

    let json_path = dir.path().join("envelope.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.save_catalog().unwrap();

    // Parse the file and verify envelope structure.
    let raw = std::fs::read_to_string(&json_path).unwrap();
    let val: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(
        val.get("checksum").is_some(),
        "JSON file should have a 'checksum' field"
    );
    assert!(
        val.get("data").is_some(),
        "JSON file should have a 'data' field"
    );
    let checksum = val["checksum"].as_u64().unwrap();
    assert!(checksum > 0, "checksum should be non-zero");
}

// =====================================================================
// Legacy JSON (no envelope) backward compatibility
// =====================================================================

#[test]
fn json_legacy_format_still_loads() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, _) = setup_3shard(&dir, "s");

    // Write a plain JSON file (old format, no envelope).
    let json_path = dir.path().join("legacy.json");
    let legacy = r#"{"legacy/obj.bin":{"shards":[0,1],"size":1024,"crc32c":null,"updated":"2025-01-01T00:00:00Z","meta_len":0}}"#;
    std::fs::write(&json_path, legacy).unwrap();

    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.load_catalog().unwrap();

    let entry = cluster.placement("legacy/obj.bin");
    assert!(entry.is_some(), "legacy format should load successfully");
    assert_eq!(entry.unwrap().size, 1024);
}

// =====================================================================
// Both formats: full roundtrip with checksum validation
// =====================================================================

#[test]
fn json_full_roundtrip_with_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["rt/a.bin", "rt/b.bin", "rt/c.bin", "rt/d.bin", "rt/e.bin"];
    put_objects(&rt, &cluster, &raws, keys);

    let json_path = dir.path().join("roundtrip.json");
    cluster.set_persistence(CatalogPersistence::json(&json_path));
    cluster.save_catalog().unwrap();

    // Load into a fresh cluster.
    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::json(&json_path));
    cluster2.load_catalog().unwrap();

    assert_eq!(cluster2.catalog().len(), keys.len());
    for key in keys {
        let orig = cluster.placement(key).unwrap();
        let loaded = cluster2.placement(key).unwrap();
        assert_eq!(orig.size, loaded.size);
        assert_eq!(orig.shards, loaded.shards);
        assert_eq!(orig.meta_len, loaded.meta_len);
    }
}

#[test]
fn bincode_full_roundtrip_with_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, raws) = setup_3shard(&dir, "a");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let keys = &["rt/a.bin", "rt/b.bin", "rt/c.bin", "rt/d.bin", "rt/e.bin"];
    put_objects(&rt, &cluster, &raws, keys);

    let bin_path = dir.path().join("roundtrip.bin");
    cluster.set_persistence(CatalogPersistence::bincode(&bin_path));
    cluster.save_catalog().unwrap();

    let (cluster2, _) = setup_3shard(&dir, "b");
    cluster2.set_persistence(CatalogPersistence::bincode(&bin_path));
    cluster2.load_catalog().unwrap();

    assert_eq!(cluster2.catalog().len(), keys.len());
    for key in keys {
        let orig = cluster.placement(key).unwrap();
        let loaded = cluster2.placement(key).unwrap();
        assert_eq!(orig.size, loaded.size);
        assert_eq!(orig.shards, loaded.shards);
        assert_eq!(orig.meta_len, loaded.meta_len);
    }
}
