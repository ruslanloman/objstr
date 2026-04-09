//! CLI tools for rawobjstr devices.
//!
//! ## Commands
//!
//! ```bash
//! # Format a loopback file (default 16 MB index slots)
//! rawobjstr format --file /tmp/store.raw --size 1073741824
//!
//! # Format with custom index slot size (32 MB per slot)
//! rawobjstr format --file /tmp/store.raw --size 1073741824 --index-slot-size 33554432
//!
//! # Show device info
//! rawobjstr info --file /tmp/store.raw
//!
//! # List files (one per line)
//! rawobjstr list --file /tmp/store.raw
//!
//! # List files with sizes
//! rawobjstr list --file /tmp/store.raw --long
//!
//! # List with prefix filter
//! rawobjstr list --file /tmp/store.raw --prefix my_table
//!
//! # Get a single file (to stdout)
//! rawobjstr get --file /tmp/store.raw --key my_table/data/00000.db > out.db
//!
//! # Get a single file (to a local path)
//! rawobjstr get --file /tmp/store.raw --key my_table/data/00000.db --to out.db
//!
//! # Put a local file into the store
//! rawobjstr put --file /tmp/store.raw --key my_table/data/00003.db --from input.db
//!
//! # Delete a single file
//! rawobjstr delete --file /tmp/store.raw --key my_table/data/00003.db
//!
//! # Verify all extents (fsck)
//! rawobjstr verify --file /tmp/store.raw
//!
//! # Export all files to a local directory
//! rawobjstr export --file /tmp/store.raw --to /tmp/export
//!
//! # Import from a local directory
//! rawobjstr import --file /tmp/store.raw --from /path/to/files
//!
//! # Import from another raw device (upgrade / resize)
//! rawobjstr import --file /tmp/new.raw --from raw:///tmp/old.raw
//!
//! # Import from S3 (requires --features aws)
//! rawobjstr import --file /tmp/store.raw --from s3://bucket/prefix
//!
//! # Repair (rebuild free list, re-flush)
//! rawobjstr repair --file /tmp/store.raw
//! ```

use std::io::Write;
use std::path::PathBuf;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore, PutPayload};
use rawobjstr::store::{FormatOptions, ObjectFullInfo, OpenMode, RawObjectStore, VerifyStatus};
use rawobjstr::{Compression, FLAG_DIRECT_IO, FLAG_WRITE_PROTECT};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "format" => cmd_format(&args[2..]),
        "info" => cmd_info(&args[2..]),
        "list" | "ls" => cmd_list(&args[2..]),
        "get" => cmd_get(&args[2..]),
        "getraw" => cmd_getraw(&args[2..]),
        "getmeta" => cmd_getmeta(&args[2..]),
        "put" => cmd_put(&args[2..]),
        "putmeta" => cmd_putmeta(&args[2..]),
        "list-full" => cmd_list_full(&args[2..]),
        "delete" | "del" => cmd_delete(&args[2..]),
        "verify" => cmd_verify(&args[2..]),
        "export" => cmd_export(&args[2..]),
        "import" => cmd_import(&args[2..]),
        "repair" => cmd_repair(&args[2..]),
        "tombstones" => cmd_tombstones(&args[2..]),
        "del-tombstone" => cmd_del_tombstone(&args[2..]),
        "scrub" => cmd_scrub(&args[2..]),
        "set-property" => cmd_set_property(&args[2..]),
        "vacuum" => cmd_vacuum(&args[2..]),
        "list-deleted" => cmd_list_deleted(&args[2..]),
        "--help" | "-h" | "help" => {
            print_usage();
        }
        "--version" | "-V" | "version" => {
            println!("{}", version_string());
        }
        _ => {
            print_usage();
            std::process::exit(1);
        }
    }
}

fn version_string() -> String {
    format!(
        "rawobjstr {} (git {}, built {})",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_HASH"),
        env!("BUILD_DATE"),
    )
}

fn print_usage() {
    eprintln!("{}", version_string());
    eprintln!();
    eprintln!(
        "Usage: rawobjstr <command> [options]

Commands:
  format          Format a raw device or image file for use as an object store
  info            Display device metadata, index stats, and space usage
  list (ls)       List stored objects, optionally filtered by prefix
  list-full       List objects with full extent details (offset, size, txn, etc.)
  get             Retrieve an object and write to stdout or a local file
  getraw          Retrieve raw on-disk bytes (no decompression) of an object
  getmeta         Retrieve only the metadata suffix of an object
  put             Store a local file as an object on the device
  putmeta         Replace the metadata suffix of an existing object (body unchanged)
  delete (del)    Delete an object from the device
  verify          Verify CRC integrity of all objects and check free list consistency
  export          Export all objects to a directory, another raw device, or S3
  import          Import objects from a directory, another raw device, or S3
  repair          Rebuild the free list from the index (recovers leaked space)
  tombstones      List tombstone records (objects removed during index validation)
  del-tombstone   Remove a specific tombstone record
  scrub           Zero out all free space on the device
  set-property    Change device flags (direct-io, write-protect)
  vacuum          Remove stale delete markers (__deleted__/* keys)
  list-deleted    List delete markers on the device

Options:
  --help, -h      Show this help message
  --version, -V   Show version, git hash, and build date

Common flags:
  --file <path>          Path to the raw device or image file (required)
  --readonly             Open in read-only mode
  --direct-io            Enable O_DIRECT for aligned I/O
  --key <object-path>    Object key for get/put/delete/del-tombstone
  --from <source>        Source file (put) or import source (directory/raw:///s3://)
  --to <dest>            Destination file (get) or export target (directory/raw:///s3://)
  --prefix <prefix>      Filter prefix for list
  --long, -l             Long listing format (sizes) or verbose verify output
  --size <bytes>         Device size in bytes (format)
  --index-slot-size <b>  Index slot capacity in bytes (format, default 16 MB)
  --full-verify          Open with full CRC scan of every extent
  --compression <alg>    Compression algorithm for format (none|zstd|snappy|gzip0..gzip9)"
    );
}

/// Parse common CLI flags.
struct CliArgs {
    path: PathBuf,
    size: Option<u64>,
    direct_io: bool,
    index_slot_size: Option<u64>,
    max_key_length: Option<usize>,
    to: Option<String>,
    from: Option<String>,
    key: Option<String>,
    prefix: Option<String>,
    long: bool,
    aws_region: Option<String>,
    read_only: bool,
    full_verify: bool,
    compression: Option<Compression>,
}

fn next_arg<'a>(args: &'a [String], i: &mut usize, flag: &str) -> &'a str {
    *i += 1;
    if *i >= args.len() {
        eprintln!("error: {flag} requires a value");
        print_usage();
        std::process::exit(1);
    }
    &args[*i]
}

fn parse_args(args: &[String]) -> CliArgs {
    let mut path = None;
    let mut size = None;
    let mut direct = false;
    let mut key = None;
    let mut prefix = None;
    let mut long = false;
    let mut index_slot_size = None;
    let mut max_key_length: Option<usize> = None;
    let mut to = None;
    let mut from = None;
    let mut aws_region = None;
    let mut read_only = false;
    let mut full_verify = false;
    let mut compression = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--file" | "--device" => {
                let val = next_arg(args, &mut i, "--file");
                path = Some(PathBuf::from(val));
            }
            "--size" => {
                let val = next_arg(args, &mut i, "--size");
                size = Some(val.parse::<u64>().unwrap_or_else(|_| {
                    eprintln!("error: invalid --size value: {val}");
                    std::process::exit(1);
                }));
            }
            "--direct-io" => direct = true,
            "--index-slot-size" => {
                let val = next_arg(args, &mut i, "--index-slot-size");
                index_slot_size = Some(val.parse::<u64>().unwrap_or_else(|_| {
                    eprintln!("error: invalid --index-slot-size value: {val}");
                    std::process::exit(1);
                }));
            }
            "--max-key-length" => {
                let val = next_arg(args, &mut i, "--max-key-length");
                max_key_length = Some(val.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("error: invalid --max-key-length value: {val}");
                    std::process::exit(1);
                }));
            }
            "--to" => {
                let val = next_arg(args, &mut i, "--to");
                to = Some(val.to_string());
            }
            "--from" => {
                let val = next_arg(args, &mut i, "--from");
                from = Some(val.to_string());
            }
            "--key" => {
                let val = next_arg(args, &mut i, "--key");
                key = Some(val.to_string());
            }
            "--prefix" => {
                let val = next_arg(args, &mut i, "--prefix");
                prefix = Some(val.to_string());
            }
            "--long" | "-l" => long = true,
            "--readonly" | "--read-only" => read_only = true,
            "--full-verify" => full_verify = true,
            "--aws-region" => {
                let val = next_arg(args, &mut i, "--aws-region");
                aws_region = Some(val.to_string());
            }
            "--compression" => {
                let val = next_arg(args, &mut i, "--compression");
                compression = Some(Compression::from_str_name(val).unwrap_or_else(|_| {
                    eprintln!("error: unknown compression algorithm: {val}");
                    eprintln!("valid: none, zstd, snappy, gzip0, gzip1, ..., gzip9");
                    std::process::exit(1);
                }));
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    if path.is_none() {
        eprintln!("error: --file or --device is required");
        print_usage();
        std::process::exit(1);
    }
    CliArgs {
        path: path.unwrap(),
        size,
        direct_io: direct,
        index_slot_size,
        max_key_length,
        to,
        from,
        key,
        prefix,
        long,
        aws_region,
        read_only,
        full_verify,
        compression,
    }
}

/// Open a store with mode derived from `--readonly` / `--full-verify`.
fn open_store(cli: &CliArgs) -> RawObjectStore {
    let mode = if cli.full_verify {
        OpenMode::FullVerify
    } else {
        OpenMode::Default
    };
    if cli.read_only {
        RawObjectStore::open_readonly_with_mode(&cli.path, mode)
    } else {
        RawObjectStore::open_with_mode(&cli.path, mode)
    }
    .expect("failed to open device")
}

// -- format ---------------------------------------------------------------

fn cmd_format(args: &[String]) {
    let cli = parse_args(args);
    let slot_size = cli.index_slot_size.unwrap_or(16 * 1024 * 1024);
    let max_key_len = cli.max_key_length.unwrap_or(rawobjstr::DEFAULT_MAX_KEY_LENGTH);
    let compression = cli.compression.unwrap_or(Compression::None);

    let store = if let Some(sz) = cli.size {
        println!(
            "Formatting {} ({} MB, index slots {} MB, max key {} B, compression {}) ...",
            cli.path.display(),
            sz / (1024 * 1024),
            slot_size / (1024 * 1024),
            max_key_len,
            compression,
        );
        RawObjectStore::format_with_options(
            &cli.path,
            FormatOptions {
                device_size: sz,
                direct_io: cli.direct_io,
                index_slot_size: slot_size,
                max_key_length: max_key_len,
                compression,
            },
        )
    } else {
        println!("Formatting device {} ...", cli.path.display());
        if cli.index_slot_size.is_some() || cli.max_key_length.is_some() || compression != Compression::None {
            let device_size = {
                let probe_io = rawobjstr::io::DeviceIo::open(&cli.path, false)
                    .expect("cannot open device");
                probe_io.size().expect("cannot determine device size")
            };
            RawObjectStore::format_with_options(
                &cli.path,
                FormatOptions {
                    device_size,
                    direct_io: cli.direct_io,
                    index_slot_size: slot_size,
                    max_key_length: max_key_len,
                    compression,
                },
            )
        } else {
            RawObjectStore::format(&cli.path, cli.direct_io)
        }
    };

    let store = store.expect("format failed");
    store.flush_index().expect("flush failed");
    println!("Done. Device formatted and ready.");
}

// -- info -----------------------------------------------------------------

fn cmd_info(args: &[String]) {
    let cli = parse_args(args);
    // info always uses FullVerify to get accurate counts, but respects --readonly
    let store = if cli.read_only {
        RawObjectStore::open_readonly_with_mode(&cli.path, OpenMode::FullVerify)
    } else {
        RawObjectStore::open_with_mode(&cli.path, OpenMode::FullVerify)
    }
    .expect("failed to open device");
    let info = store.device_info();

    println!("Device:             {}", info.device_path);
    println!(
        "Device size:        {} bytes ({:.1} MB)",
        info.device_size,
        info.device_size as f64 / 1_048_576.0
    );
    println!("Format version:     {}", info.format_version);
    let mut flag_labels = Vec::new();
    if info.direct_io {
        flag_labels.push("O_DIRECT");
    }
    if info.flags & FLAG_WRITE_PROTECT != 0 {
        flag_labels.push("WRITE_PROTECT");
    }
    let flag_suffix = if flag_labels.is_empty() {
        String::new()
    } else {
        format!(" ({})", flag_labels.join(", "))
    };
    println!(
        "Flags:              0x{:08x}{}",
        info.flags, flag_suffix
    );
    println!("Compression:        {}", info.compression);
    println!("Transaction ID:     {}", info.txn_id);
    println!();
    println!(
        "Index slot size:    {} bytes ({} MB)",
        info.index_slot_capacity,
        info.index_slot_capacity / (1024 * 1024)
    );
    println!(
        "Index serialized:   {} bytes ({:.1}% of slot)",
        info.index_serialized_bytes,
        info.index_serialized_bytes as f64 / info.index_slot_capacity as f64 * 100.0
    );
    println!("Index region A:     0x{:012x}", info.index_region_a);
    println!("Index region B:     0x{:012x}", info.index_region_b);
    println!(
        "Active index:       0x{:012x} (region {})",
        info.active_index_region,
        if info.active_index_region == info.index_region_a {
            "A"
        } else {
            "B"
        }
    );
    println!(
        "Last flush I/O:     {} bytes",
        info.last_flush_bytes
    );
    println!(
        "Max key length:     {} bytes",
        info.max_key_length
    );
    println!();
    println!(
        "Data region:        0x{:012x} - 0x{:012x} ({:.1} MB)",
        info.data_region_start,
        info.data_region_end,
        (info.data_region_end - info.data_region_start) as f64 / 1_048_576.0
    );
    println!("Files:              {}", info.file_count);
    println!(
        "Data stored:        {} bytes ({:.1} MB)",
        info.data_bytes_stored,
        info.data_bytes_stored as f64 / 1_048_576.0
    );
    println!(
        "Device bytes used:  {} bytes ({:.1} MB, includes headers + padding)",
        info.device_bytes_used,
        info.device_bytes_used as f64 / 1_048_576.0
    );
    println!(
        "Free space:         {} bytes ({:.1} MB)",
        info.free_space,
        info.free_space as f64 / 1_048_576.0
    );
    println!("Free fragments:     {}", info.free_fragments);
    println!(
        "Largest free:       {} bytes ({:.1} MB)",
        info.largest_free_extent,
        info.largest_free_extent as f64 / 1_048_576.0
    );
}

// -- list -----------------------------------------------------------------

fn cmd_list(args: &[String]) {
    let cli = parse_args(args);
    let store = open_store(&cli);

    let prefix = cli.prefix.as_deref().map(Path::from);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let files: Vec<_> = store
            .list(prefix.as_ref())
            .try_collect()
            .await
            .unwrap();
        let mut sorted: Vec<_> = files.iter().collect();
        sorted.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

        if cli.long {
            for f in &sorted {
                println!("{:>10}  {}", f.size, f.location);
            }
        } else {
            for f in &sorted {
                println!("{}", f.location);
            }
        }
    });
}

// -- get ------------------------------------------------------------------

fn cmd_get(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("error: --key <object-path> required for get");
        std::process::exit(1);
    });
    let store = open_store(&cli);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = rt.block_on(async {
        let result = store
            .get(&Path::from(key))
            .await
            .expect("get failed");
        result.bytes().await.expect("read bytes failed")
    });

    if let Some(to_path) = &cli.to {
        std::fs::write(to_path, &data).expect("failed to write output file");
        eprintln!("{} bytes -> {}", data.len(), to_path);
    } else {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        out.write_all(&data).expect("write to stdout failed");
    }
}

// -- getmeta --------------------------------------------------------------

fn cmd_getraw(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("error: --key <object-path> required for getraw");
        std::process::exit(1);
    });
    let store = open_store(&cli);

    let raw_result = store
        .get_raw(&Path::from(key))
        .expect("getraw failed");

    if raw_result.uncompressed_size > 0 {
        eprintln!(
            "compressed with {} ({} -> {} bytes, {:.1}% savings)",
            raw_result.compression.as_str(),
            raw_result.uncompressed_size,
            raw_result.data.len(),
            (1.0 - raw_result.data.len() as f64 / raw_result.uncompressed_size as f64) * 100.0,
        );
    } else {
        eprintln!("not compressed ({} bytes)", raw_result.data.len());
    }

    if let Some(to_path) = &cli.to {
        std::fs::write(to_path, &raw_result.data).expect("failed to write output file");
        eprintln!("{} raw bytes -> {}", raw_result.data.len(), to_path);
    } else {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        out.write_all(&raw_result.data).expect("write to stdout failed");
    }
}

// -- getmeta --------------------------------------------------------------

fn cmd_getmeta(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("error: --key <object-path> required for getmeta");
        std::process::exit(1);
    });

    let store = open_store(&cli);
    let data = store
        .get_metadata(&Path::from(key))
        .expect("getmeta failed");

    if let Some(to_path) = &cli.to {
        std::fs::write(to_path, &data).expect("failed to write output file");
        eprintln!("{} metadata bytes -> {}", data.len(), to_path);
    } else {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        out.write_all(&data).expect("write to stdout failed");
    }
}

// -- putmeta --------------------------------------------------------------

fn cmd_putmeta(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("error: --key <object-path> required for putmeta");
        std::process::exit(1);
    });
    let from_path = cli.from.as_deref().unwrap_or_else(|| {
        eprintln!("error: --from <local-file> required for putmeta");
        std::process::exit(1);
    });

    let meta_bytes = std::fs::read(from_path).expect("failed to read metadata file");
    let meta_len = meta_bytes.len();

    let store = RawObjectStore::open(&cli.path).expect("failed to open device");
    store
        .update_metadata(&Path::from(key), bytes::Bytes::from(meta_bytes))
        .expect("putmeta failed");
    store.flush_index().expect("flush failed");
    eprintln!("{} metadata bytes <- {} -> {}", meta_len, from_path, key);
}

// -- list-full ------------------------------------------------------------

fn cmd_list_full(args: &[String]) {
    let cli = parse_args(args);
    let store = open_store(&cli);

    let prefix = cli.prefix.as_deref().map(Path::from);
    let results: Vec<ObjectFullInfo> = store.list_full(prefix.as_ref());

    if results.is_empty() {
        return;
    }

    // Column widths for alignment
    let body_w = results.iter().map(|r| digits(r.body_size)).max().unwrap_or(4).max(4);
    let meta_w = results.iter().map(|r| digits(r.meta_len as u64)).max().unwrap_or(4).max(4);

    println!(
        "{:>bw$}  {:>mw$}  {:<19}  {:>6}  {:<14}  {}",
        "body", "meta", "last_modified", "txn", "offset", "key",
        bw = body_w, mw = meta_w,
    );
    println!(
        "{:-<bw$}  {:-<mw$}  {:-<19}  {:->6}  {:-<14}  {:-<4}",
        "", "", "", "", "", "",
        bw = body_w, mw = meta_w,
    );
    for r in &results {
        println!(
            "{:>bw$}  {:>mw$}  {}  {:>6}  0x{:012x}  {}",
            r.body_size,
            r.meta_len,
            r.last_modified.format("%Y-%m-%dT%H:%M:%S"),
            r.created_txn,
            r.offset,
            r.key,
            bw = body_w,
            mw = meta_w,
        );
    }
}

fn digits(n: u64) -> usize {
    if n == 0 { 1 } else { (n as f64).log10() as usize + 1 }
}

// -- put ------------------------------------------------------------------

fn cmd_put(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("error: --key <object-path> required for put");
        std::process::exit(1);
    });
    let from_path = cli.from.as_deref().unwrap_or_else(|| {
        eprintln!("error: --from <local-file> required for put");
        std::process::exit(1);
    });

    let data = std::fs::read(from_path).expect("failed to read input file");
    let len = data.len();

    let store = RawObjectStore::open(&cli.path).expect("failed to open device");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        store
            .put(&Path::from(key), PutPayload::from(Bytes::from(data)))
            .await
            .expect("put failed");
    });
    store.flush_index().expect("flush failed");
    eprintln!("{} bytes <- {} -> {}", len, from_path, key);
}

// -- delete ---------------------------------------------------------------

fn cmd_delete(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("error: --key <object-path> required for delete");
        std::process::exit(1);
    });

    let store = RawObjectStore::open(&cli.path).expect("failed to open device");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        store
            .delete(&Path::from(key))
            .await
            .expect("delete failed");
    });
    store.flush_index().expect("flush failed");
    eprintln!("deleted {}", key);
}

// -- verify ---------------------------------------------------------------

fn cmd_verify(args: &[String]) {
    let cli = parse_args(args);
    // verify always uses FullVerify, but respects --readonly
    let store = if cli.read_only {
        RawObjectStore::open_readonly_with_mode(&cli.path, OpenMode::FullVerify)
    } else {
        RawObjectStore::open_with_mode(&cli.path, OpenMode::FullVerify)
    }
    .expect("failed to open device");
    let report = store.verify_all();
    let verbose = cli.long;

    println!("Verify: {}", cli.path.display());
    println!();
    println!("Files checked:       {}", report.files_checked);
    println!("Files OK:            {}", report.files_ok);
    println!("Errors:              {}", report.errors.len());
    println!("Overlapping extents: {}", report.overlapping_extents.len());
    println!("Free list OK:        {}", report.free_list_consistent);
    println!("Space accounted:     {}", report.space_accounted);
    println!("  Data region:       {} bytes", report.total_data_region);
    println!("  Used:              {} bytes", report.total_used);
    println!("  Free:              {} bytes", report.total_free);

    if verbose && !report.ok_files.is_empty() {
        println!();
        println!("OK files:");
        let mut sorted = report.ok_files;
        sorted.sort_by(|a, b| a.path.cmp(&b.path));
        for e in &sorted {
            println!(
                "  {} (offset 0x{:x}, {} bytes): OK",
                e.path, e.offset, e.expected_size
            );
        }
    }

    if !report.errors.is_empty() {
        println!();
        println!("Errors:");
        for e in &report.errors {
            let status_str = match &e.status {
                VerifyStatus::Ok => "ok".to_string(),
                VerifyStatus::CrcMismatch { expected, actual } => {
                    format!(
                        "CRC mismatch: expected {:#010x}, got {:#010x}",
                        expected, actual
                    )
                }
                VerifyStatus::BlockCorrupt(msg) => format!("block corrupt: {}", msg),
                VerifyStatus::OutOfBounds => "extent out of bounds".to_string(),
                VerifyStatus::ReadError(msg) => format!("read error: {}", msg),
            };
            println!(
                "  {} (offset 0x{:x}, {} bytes): {}",
                e.path, e.offset, e.expected_size, status_str
            );
        }
    }

    if !report.overlapping_extents.is_empty() {
        println!();
        println!("Overlapping extents:");
        for (a, b) in &report.overlapping_extents {
            println!("  {} <-> {}", a, b);
        }
    }

    if report.errors.is_empty()
        && report.overlapping_extents.is_empty()
        && report.free_list_consistent
        && report.space_accounted
    {
        println!();
        println!("Device is clean.");
    } else {
        println!();
        println!("Device has issues. Run 'repair' to fix.");
        std::process::exit(1);
    }
}

// -- export ---------------------------------------------------------------

fn cmd_export(args: &[String]) {
    let cli = parse_args(args);
    let to_uri = cli.to.as_deref().unwrap_or_else(|| {
        eprintln!("error: --to <dir|raw://|s3://> required for export");
        std::process::exit(1);
    });

    let store = open_store(&cli);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt
        .block_on(async {
            export_to_uri(&store, to_uri, cli.aws_region.as_deref()).await
        })
        .expect("export failed");

    println!(
        "Exported {} files ({:.1} MB)",
        report.files_exported,
        report.bytes_exported as f64 / 1_048_576.0
    );
    if !report.errors.is_empty() {
        println!("Errors ({}):", report.errors.len());
        for (path, err) in &report.errors {
            println!("  {}: {}", path, err);
        }
        std::process::exit(1);
    }
}

async fn export_to_uri(
    store: &RawObjectStore,
    uri: &str,
    _aws_region: Option<&str>,
) -> rawobjstr::Result<rawobjstr::store::ExportReport> {
    if let Some(raw_path) = uri.strip_prefix("raw://") {
        let target_path = if raw_path.starts_with('/') {
            raw_path.to_string()
        } else {
            format!("/{}", raw_path)
        };
        println!("Exporting to raw device: {} ...", target_path);
        let target = RawObjectStore::open(std::path::Path::new(&target_path))?;
        let report = store.export_to(&target).await?;
        target.flush_index()?;
        Ok(report)
    } else if uri.starts_with("s3://") {
        export_to_s3(store, uri, _aws_region).await
    } else {
        println!("Exporting to directory: {} ...", uri);
        let target = LocalFileSystem::new_with_prefix(uri).map_err(|e| {
            rawobjstr::RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        store.export_to(&target).await
    }
}

/// Parse an `s3://bucket/prefix` URI into `(bucket, Option<prefix>)`.
#[cfg(feature = "aws")]
fn parse_s3_uri(uri: &str) -> (&str, Option<&str>) {
    let without_scheme = &uri[5..]; // strip "s3://"
    match without_scheme.find('/') {
        Some(pos) => (&without_scheme[..pos], Some(&without_scheme[pos + 1..])),
        None => (without_scheme, None),
    }
}

#[cfg(feature = "aws")]
async fn export_to_s3(
    store: &RawObjectStore,
    uri: &str,
    aws_region: Option<&str>,
) -> rawobjstr::Result<rawobjstr::store::ExportReport> {
    use object_store::aws::AmazonS3Builder;

    let (bucket, _prefix) = parse_s3_uri(uri);

    println!(
        "Exporting to S3: bucket={}, prefix={} ...",
        bucket,
        _prefix.unwrap_or("(root)")
    );

    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(region) = aws_region {
        builder = builder.with_region(region);
    }
    let target = builder.build().map_err(|e| {
        rawobjstr::RawStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("S3 setup failed: {}", e),
        ))
    })?;

    // If there's a prefix, we need a PrefixStore to put files under that prefix
    if let Some(prefix) = _prefix {
        let prefixed = object_store::prefix::PrefixStore::new(target, prefix);
        store.export_to(&prefixed).await
    } else {
        store.export_to(&target).await
    }
}

#[cfg(not(feature = "aws"))]
async fn export_to_s3(
    _store: &RawObjectStore,
    _uri: &str,
    _aws_region: Option<&str>,
) -> rawobjstr::Result<rawobjstr::store::ExportReport> {
    Err(rawobjstr::RawStoreError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "S3 export requires building with --features aws. \
         Re-run: cargo run --bin rawobjstr --features aws -- export ...",
    )))
}

// -- import ---------------------------------------------------------------

fn cmd_import(args: &[String]) {
    let cli = parse_args(args);
    let from_uri = cli.from.as_deref().unwrap_or_else(|| {
        eprintln!("error: --from <source> required for import");
        std::process::exit(1);
    });

    let store = RawObjectStore::open(&cli.path).expect("failed to open target device");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt.block_on(async {
        import_from_uri(&store, from_uri, cli.aws_region.as_deref()).await
    });
    let report = report.expect("import failed");
    store.flush_index().expect("flush after import failed");

    println!(
        "Imported {} files ({:.1} MB)",
        report.files_imported,
        report.bytes_imported as f64 / 1_048_576.0
    );
    if !report.errors.is_empty() {
        println!("Errors ({}):", report.errors.len());
        for (path, err) in &report.errors {
            println!("  {}: {}", path, err);
        }
        std::process::exit(1);
    }
}

async fn import_from_uri(
    target: &RawObjectStore,
    uri: &str,
    _aws_region: Option<&str>,
) -> rawobjstr::Result<rawobjstr::store::ImportReport> {
    if let Some(raw_path) = uri.strip_prefix("raw://") {
        // Import from another rawobjstr device
        let source_path = if raw_path.starts_with('/') {
            raw_path.to_string()
        } else {
            format!("/{}", raw_path)
        };
        println!("Importing from raw device: {} ...", source_path);
        let source = RawObjectStore::open(std::path::Path::new(&source_path))?;
        target.import_from(&source, None).await
    } else if uri.starts_with("s3://") {
        import_from_s3(target, uri, _aws_region).await
    } else {
        // Treat as local filesystem path
        println!("Importing from directory: {} ...", uri);
        let source = LocalFileSystem::new_with_prefix(uri).map_err(|e| {
            rawobjstr::RawStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        target.import_from(&source, None).await
    }
}

#[cfg(feature = "aws")]
async fn import_from_s3(
    target: &RawObjectStore,
    uri: &str,
    aws_region: Option<&str>,
) -> rawobjstr::Result<rawobjstr::store::ImportReport> {
    use object_store::aws::AmazonS3Builder;

    let (bucket, prefix) = parse_s3_uri(uri);

    println!(
        "Importing from S3: bucket={}, prefix={} ...",
        bucket,
        prefix.unwrap_or("(root)")
    );

    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(region) = aws_region {
        builder = builder.with_region(region);
    }
    let source = builder.build().map_err(|e| {
        rawobjstr::RawStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("S3 setup failed: {}", e),
        ))
    })?;

    let prefix_path = prefix.map(Path::from);
    target.import_from(&source, prefix_path.as_ref()).await
}

#[cfg(not(feature = "aws"))]
async fn import_from_s3(
    _target: &RawObjectStore,
    _uri: &str,
    _aws_region: Option<&str>,
) -> rawobjstr::Result<rawobjstr::store::ImportReport> {
    Err(rawobjstr::RawStoreError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "S3 import requires building with --features aws. \
         Re-run: cargo run --bin rawobjstr --features aws -- import ...",
    )))
}

// -- repair ---------------------------------------------------------------

fn cmd_repair(args: &[String]) {
    let cli = parse_args(args);
    let store = RawObjectStore::open_with_mode(&cli.path, OpenMode::FullVerify)
        .expect("failed to open device");

    println!("Repairing {} ...", cli.path.display());
    let report = store.repair().expect("repair failed");

    println!();
    println!("Files in index: {}", report.files_found);

    if !report.used_extents.is_empty() {
        println!();
        println!("Used extents ({}):", report.used_extents.len());
        for (path, offset, psize) in &report.used_extents {
            println!(
                "  0x{:08x} .. 0x{:08x}  ({:>8} bytes)  {}",
                offset,
                offset + psize,
                psize,
                path
            );
        }
    }

    println!();
    println!("Free list rebuilt:");
    println!(
        "  {} entries -> {} entries",
        report.old_free_entries, report.new_free_entries
    );
    println!(
        "  {} bytes -> {} bytes ({:+} bytes)",
        report.old_free_space,
        report.new_free_space,
        (report.new_free_space as i128) - (report.old_free_space as i128),
    );
    if report.new_free_space > report.old_free_space {
        println!(
            "  Recovered {:.1} MB of leaked space",
            (report.new_free_space - report.old_free_space) as f64 / 1_048_576.0
        );
    }

    if !report.new_free_list.is_empty() {
        println!();
        println!("New free regions ({}):", report.new_free_list.len());
        for (offset, size) in &report.new_free_list {
            println!(
                "  0x{:08x} .. 0x{:08x}  ({:>8} bytes)",
                offset,
                offset + size,
                size
            );
        }
    }

    println!();
    println!("Flushed: {}", report.flushed);
    println!("Repair complete.");
}

// -- tombstones -----------------------------------------------------------

fn cmd_tombstones(args: &[String]) {
    let cli = parse_args(args);
    // tombstones needs FullVerify to discover stale entries, but respect --readonly
    let mode = if cli.full_verify {
        OpenMode::FullVerify
    } else {
        OpenMode::FullVerify // default to FullVerify for tombstone discovery
    };
    let store = if cli.read_only {
        RawObjectStore::open_readonly_with_mode(&cli.path, mode)
    } else {
        RawObjectStore::open_with_mode(&cli.path, mode)
    }
    .expect("failed to open device");
    let tombstones = store.list_tombstones();

    if tombstones.is_empty() {
        println!("No tombstones.");
        return;
    }

    println!("Tombstones ({}):", tombstones.len());
    println!();
    for t in &tombstones {
        println!(
            "  {}  ({} bytes, CRC {:#010x}, modified {})",
            t.path,
            t.size,
            t.crc32c,
            t.last_modified.format("%Y-%m-%d %H:%M:%S UTC"),
        );
        println!("    reason: {}", t.reason);
    }
}

// -- del-tombstone --------------------------------------------------------

fn cmd_del_tombstone(args: &[String]) {
    let cli = parse_args(args);
    let key = cli.key.as_deref().unwrap_or_else(|| {
        eprintln!("Error: --key is required for del-tombstone");
        std::process::exit(1);
    });
    let store = RawObjectStore::open_with_mode(&cli.path, OpenMode::FullVerify)
        .expect("failed to open device");

    match store.delete_tombstone(key) {
        Ok(true) => {
            store.flush_index().expect("failed to flush after tombstone removal");
            println!("Tombstone '{}' removed and flushed.", key);
        }
        Ok(false) => {
            eprintln!("No tombstone found for '{}'.", key);
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error removing tombstone: {}", e);
            std::process::exit(1);
        }
    }
}

// -- scrub ----------------------------------------------------------------

fn cmd_scrub(args: &[String]) {
    let cli = parse_args(args);
    let store = RawObjectStore::open_with_mode(&cli.path, OpenMode::FullVerify)
        .expect("failed to open device");

    println!("Scrubbing free space on {} ...", cli.path.display());
    let report = store.scrub_free_space().expect("scrub failed");
    println!(
        "Scrubbed {} free regions, {:.1} MB zeroed.",
        report.regions_scrubbed,
        report.bytes_scrubbed as f64 / 1_048_576.0
    );
}

// -- set-property ---------------------------------------------------------

fn cmd_set_property(args: &[String]) {
    // Dedicated parser — needs --file plus --direct-io on|off / --write-protect on|off
    let mut path: Option<PathBuf> = None;
    let mut set_direct_io: Option<bool> = None;
    let mut set_write_protect: Option<bool> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--file" | "--device" => {
                let val = next_arg(args, &mut i, "--file");
                path = Some(PathBuf::from(val));
            }
            "--direct-io" => {
                let val = next_arg(args, &mut i, "--direct-io");
                set_direct_io = Some(parse_on_off(val, "--direct-io"));
            }
            "--write-protect" => {
                let val = next_arg(args, &mut i, "--write-protect");
                set_write_protect = Some(parse_on_off(val, "--write-protect"));
            }
            other => {
                eprintln!("unknown arg for set-property: {other}");
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let path = path.unwrap_or_else(|| {
        eprintln!("error: --file is required for set-property");
        print_usage();
        std::process::exit(1);
    });

    if set_direct_io.is_none() && set_write_protect.is_none() {
        eprintln!("error: set-property requires at least one of --direct-io or --write-protect");
        print_usage();
        std::process::exit(1);
    }

    let mut set_flags: u32 = 0;
    let mut clear_flags: u32 = 0;

    if let Some(on) = set_direct_io {
        if on {
            set_flags |= FLAG_DIRECT_IO;
        } else {
            clear_flags |= FLAG_DIRECT_IO;
        }
    }
    if let Some(on) = set_write_protect {
        if on {
            set_flags |= FLAG_WRITE_PROTECT;
        } else {
            clear_flags |= FLAG_WRITE_PROTECT;
        }
    }

    let result_flags = RawObjectStore::modify_flags(&path, set_flags, clear_flags)
        .expect("failed to modify flags");

    let mut labels = Vec::new();
    if result_flags & FLAG_DIRECT_IO != 0 {
        labels.push("O_DIRECT");
    }
    if result_flags & FLAG_WRITE_PROTECT != 0 {
        labels.push("WRITE_PROTECT");
    }
    let flag_str = if labels.is_empty() {
        "(none)".to_string()
    } else {
        labels.join(", ")
    };
    println!("Flags updated: 0x{:08x} [{}]", result_flags, flag_str);
}

fn parse_on_off(val: &str, flag: &str) -> bool {
    match val {
        "on" | "true" | "1" | "yes" => true,
        "off" | "false" | "0" | "no" => false,
        _ => {
            eprintln!("error: {flag} value must be on|off, got: {val}");
            std::process::exit(1);
        }
    }
}

// -- vacuum ---------------------------------------------------------------

fn cmd_vacuum(args: &[String]) {
    let cli = parse_args(args);
    let store = open_store(&cli);

    // For a standalone raw store, delete markers are just keys under
    // __deleted__/.  The real object is already gone (it was deleted when
    // the marker was created).  Vacuum simply removes the marker keys.
    let prefix = "__deleted__/";
    let markers: Vec<_> = store
        .list_full(Some(&Path::from(prefix)))
        .into_iter()
        .collect();

    if markers.is_empty() {
        println!("No delete markers found.");
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut purged = 0usize;
    rt.block_on(async {
        for m in &markers {
            let path = Path::from(m.key.as_str());
            match store.delete(&path).await {
                Ok(()) => purged += 1,
                Err(e) => eprintln!("  failed to remove marker {}: {e}", m.key),
            }
        }
    });

    store.flush_index().expect("failed to flush index");
    println!("Vacuum complete: {purged} marker(s) purged.");
}

// -- list-deleted ---------------------------------------------------------

fn cmd_list_deleted(args: &[String]) {
    let cli = parse_args(args);
    let store = open_store(&cli);

    let prefix = "__deleted__/";
    let markers: Vec<_> = store
        .list_full(Some(&Path::from(prefix)))
        .into_iter()
        .collect();

    if markers.is_empty() {
        println!("No delete markers found.");
        return;
    }

    println!("{:<60} {:<30} {:>10}", "KEY", "DELETED AT", "BODY SIZE");
    println!("{:-<60} {:-<30} {:-<10}", "", "", "");
    for m in &markers {
        let original = m.key.strip_prefix(prefix).unwrap_or(&m.key);
        println!("{:<60} {:<30} {:>10}", original, m.last_modified.to_rfc3339(), m.body_size);
    }
    println!("\n{} delete marker(s) total.", markers.len());
}
