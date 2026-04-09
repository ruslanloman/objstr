//! Catalog Viewer -- dump a persisted catalog file as human-readable text.
//!
//! Reads a catalog file (JSON or bincode) and prints each object's path,
//! size, shard placements, CRC32c, and last-updated timestamp.
//!
//! ```bash
//! # Auto-detect format by extension (.json or .bin):
//! shardedobjstr-catview /tmp/cluster_catalog.json
//! shardedobjstr-catview /tmp/cluster_catalog.bin
//!
//! # Force a specific format:
//! shardedobjstr-catview --format json /tmp/catalog
//! shardedobjstr-catview --format bin  /tmp/catalog
//! ```

use std::path::Path;

use shardedobjstr::catalog::{CatalogPersistence, PlacementEntry};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: shardedobjstr-catview [--format json|bin] <catalog-file>");
        std::process::exit(1);
    }

    let mut format: Option<&str> = None;
    let mut file_path: Option<&str> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--version" | "-V" | "version" => {
                println!(
                    "shardedobjstr-catview {} (git {}, built {})",
                    shardedobjstr::VERSION,
                    shardedobjstr::BUILD_GIT_HASH,
                    shardedobjstr::BUILD_DATE,
                );
                return;
            }
            "--format" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--format requires a value: json or bin");
                    std::process::exit(1);
                }
                format = Some(&args[i]);
            }
            other => {
                file_path = Some(other);
            }
        }
        i += 1;
    }

    let file_path = file_path.unwrap_or_else(|| {
        eprintln!("Usage: shardedobjstr-catview [--format json|bin] <catalog-file>");
        std::process::exit(1);
    });

    let path = Path::new(file_path);
    if !path.exists() {
        eprintln!("file not found: {}", file_path);
        std::process::exit(1);
    }

    // Determine format: explicit flag > extension > try both
    let fmt = match format {
        Some(f) => f.to_string(),
        None => {
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("");
            match ext {
                "json" => "json".to_string(),
                "bin" => "bin".to_string(),
                _ => {
                    // Try JSON first, fall back to bincode
                    "auto".to_string()
                }
            }
        }
    };

    let entries = match fmt.as_str() {
        "json" => load_or_exit(path, CatalogPersistence::json(path)),
        "bin" => load_or_exit(path, CatalogPersistence::bincode(path)),
        "auto" => {
            // Try JSON envelope first, then bincode with CRC.
            if let Ok(cat) = CatalogPersistence::json(path).load() {
                if cat.len() > 0 {
                    cat.all_entries()
                } else if let Ok(cat) = CatalogPersistence::bincode(path).load() {
                    cat.all_entries()
                } else {
                    Vec::new()
                }
            } else if let Ok(cat) = CatalogPersistence::bincode(path).load() {
                cat.all_entries()
            } else {
                eprintln!("failed to parse {} as JSON or bincode", path.display());
                eprintln!("try --format json or --format bin to force a specific format");
                std::process::exit(1);
            }
        }
        other => {
            eprintln!("unknown format: {} (expected json or bin)", other);
            std::process::exit(1);
        }
    };

    // Sort by path for stable output
    let mut entries: Vec<(&str, &PlacementEntry)> = entries
        .iter()
        .map(|(k, v)| (k.as_str(), v))
        .collect();
    entries.sort_by_key(|(k, _)| *k);

    // Print header
    println!(
        "{:<60}  {:>10}  {:>12}  {:>10}  {}",
        "PATH", "SIZE", "CRC32C", "SHARDS", "UPDATED"
    );
    println!("{}", "-".repeat(110));

    let mut total_size: u64 = 0;
    for (path, entry) in &entries {
        let shards = entry
            .shards
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let crc = entry
            .crc32c
            .map(|c| format!("{:#010x}", c))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<60}  {:>10}  {:>12}  {:>10}  {}",
            path,
            format_size(entry.size),
            crc,
            shards,
            entry.updated.format("%Y-%m-%d %H:%M:%S"),
        );
        total_size += entry.size;
    }

    println!("{}", "-".repeat(110));
    println!(
        "{} objects, {} total",
        entries.len(),
        format_size(total_size),
    );
}

fn load_or_exit(
    path: &Path,
    persistence: CatalogPersistence,
) -> Vec<(String, PlacementEntry)> {
    match persistence.load() {
        Ok(cat) => cat.all_entries(),
        Err(e) => {
            eprintln!("failed to load {}: {}", path.display(), e);
            std::process::exit(1);
        }
    }
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    }
}
