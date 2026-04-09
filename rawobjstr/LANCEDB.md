# LanceDB + rawobjstr

This document covers everything you need to use `rawobjstr` as the backing store
for [LanceDB](https://lancedb.com).

`rawobjstr` implements the Apache `ObjectStore` trait (`object_store` 0.12) on top
of a raw block device or a loopback image file.  LanceDB accepts any `ObjectStore`
implementation, so the two wire together with no middleware.

Benefits over a regular filesystem:
- **Self-contained image** - the entire database lives in one `.raw` file; copy, ship, or
  snapshot it with `cp` or `scp`
- **Zero filesystem dependency** - runs on a raw block device (`/dev/nvme0n1p3`) or a
  loopback file with no ext4/XFS in between
- **CRC32c on every extent** - hardware-accelerated integrity check on every read/write
- **Crash-safe** - dual-buffered index + dual superblocks; recovers to the last
  `flush_index()` checkpoint on power loss or kill -9

---

## 1. Create a Store Image with the CLI

### On a loopback file (most common for development)

```bash
# Build the CLI
cargo build --release --bin rawobjstr

# Create a 4 GB image file
rawobjstr format --file /tmp/lance_db.raw --size 4294967296

# Larger index for many tables (32 MB slots instead of the 16 MB default)
rawobjstr format --file /tmp/lance_db.raw --size 4294967296 --index-slot-size 33554432
```

The `--size` argument is the total image size in bytes.  Pick enough headroom for your
dataset plus compaction headroom (LanceDB rewrites files during optimize/compaction).

### On a real block device

```bash
# Auto-detects size from the device.  O_DIRECT bypasses the page cache.
sudo rawobjstr format --device /dev/nvme0n1p3 --direct-io
```

> **Note:** `--direct-io` has only been tested on virtual machines so far.  It
> needs testing on real hardware (physical NVMe / SSD) before relying on it in
> production.

Verify the store was created correctly:

```bash
rawobjstr info --file /tmp/lance_db.raw
```

---

## 2. Attach as a Linux Loopback Device (optional)

If you want to mount the image as a block device so other tools can access it directly,
use `losetup`.  This is optional - LanceDB via Rust accesses the image directly through
the `ObjectStore` API.

```bash
# Attach the image to the next free loop device
sudo losetup --find --show /tmp/lance_db.raw
# Output: /dev/loop0

# The image is now accessible as a block device
sudo blockdev --getsize64 /dev/loop0

# Use it as a raw block device (same format, just different path)
sudo rawobjstr info --device /dev/loop0

# When done, detach
sudo losetup --detach /dev/loop0
```

For persistent loopback (survives reboot), add an entry to `/etc/rc.local` or create a
systemd `.mount` unit.

### Recommended loopback flags

```bash
# Direct I/O on the loop device (avoids double page-caching)
sudo losetup --find --show --direct-io /tmp/lance_db.raw
```

---

## 3. Open the Store from Rust

Add to `Cargo.toml`:

```toml
[dependencies]
rawobjstr = { path = "../rawobjstr" }
object_store = "0.12"
lancedb = "0.27"   # or latest
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### Basic open (default integrity scan at open)

```rust
use std::sync::Arc;
use rawobjstr::store::RawObjectStore;

// Open an existing store image
let store = Arc::new(
    RawObjectStore::open(std::path::Path::new("/tmp/lance_db.raw")).unwrap()
);
```

### Recommended: skip the scan for embedded/long-lived use

LanceDB performs its own MVCC transaction isolation - it always reads from the last
committed manifest version, never from a half-written file.  The rawobjstr
integrity scan (one pread per object at open time) is therefore redundant during normal
operation.

```rust
use rawobjstr::store::{RawObjectStore, OpenMode};

// Zero overhead on open - recommended for long-lived embedded use
let store = Arc::new(
    RawObjectStore::open_with_mode(
        std::path::Path::new("/tmp/lance_db.raw"),
        OpenMode::SkipVerify,
    ).unwrap()
);
```

| Scenario | Recommended `OpenMode` | Why |
|----------|------------------------|-----|
| Long-lived embedded process (LanceDB) | `SkipVerify` | Process owns the store; clean shutdown guaranteed |
| Short-lived process or after unclean shutdown | `Default` | 1 block-per-object pread to catch stale extents |
| Maintenance / audit tooling | `FullVerify` | Full payload CRC scan; use for `verify`/`repair` only |

---

## 4. Connect LanceDB to the Store

LanceDB's `connect_with_store` API accepts any `Arc<dyn object_store::ObjectStore>`:

```rust
use std::sync::Arc;
use rawobjstr::store::{RawObjectStore, OpenMode};
use object_store::ObjectStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Open or format the image
    let raw_store: Arc<dyn ObjectStore> = Arc::new(
        RawObjectStore::open_with_mode(
            std::path::Path::new("/tmp/lance_db.raw"),
            OpenMode::SkipVerify,
        )?
    );

    // Connect LanceDB to the image
    let db = lancedb::connect_with_store("raw:///", raw_store)
        .execute()
        .await?;

    // Create a table
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("vec", arrow_schema::DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new("item", arrow_schema::DataType::Float32, true)),
            128,
        ), false),
    ]));

    let table = db.create_table("embeddings", schema).execute().await?;

    // Query
    let results = table
        .query()
        .nearest_to(&[0.1_f32; 128])?
        .limit(10)
        .execute()
        .await?;

    Ok(())
}
```

### Format a new image and open in one step

```rust
use rawobjstr::store::RawObjectStore;

// Creates the image if it does not exist, opens it if it does
let store = Arc::new(
    RawObjectStore::format_with_size(
        std::path::Path::new("/tmp/lance_db.raw"),
        4_294_967_296,  // 4 GB
        false,          // O_DIRECT: false for loopback files, true for block devices
    ).unwrap()
);
```

---

## 5. Crash Safety and Transaction Model

`rawobjstr` and LanceDB are independently crash-safe:

- **rawobjstr** uses dual superblocks and a dual-buffered index.  On crash, the
  store reverts to the last successful `flush_index()` checkpoint.  Any write that was
  not followed by a flush is lost (the extent is freed and its space reclaimed on next
  open with integrity scan).

- **LanceDB** uses its own MVCC transaction log (`_versions/*.manifest`,
  `_transactions/*.txn`).  On crash, Lance sees the last committed manifest.
  Uncommitted transactions are cleanly absent from the next open.

**Combined behaviour:** rawobjstr reverts to its last checkpoint, Lance reverts to
its last committed version within that checkpoint.  No torn writes, no partial manifests
visible to the application.

### Post-crash recovery sequence (when using `SkipVerify` normally)

```rust
// After detecting an unclean shutdown (e.g. SIGKILL, power loss):
let store = RawObjectStore::open_with_mode(path, OpenMode::FullVerify)?;
store.flush_index()?;   // persist cleaned-up state to disk

// Now safe to resume normal SkipVerify opens
let store = RawObjectStore::open_with_mode(path, OpenMode::SkipVerify)?;
```

---

## 6. Packing and Shipping LanceDB Tables

The `.raw` image is a self-contained archive - a single portable file that holds an
entire LanceDB database or individual tables.

### Pack an existing LanceDB directory into an image

```bash
# Create an image large enough for the data (plus compaction headroom)
rawobjstr format --file /tmp/my_table.raw --size 4294967296

# Import the full LanceDB table directory
rawobjstr import --file /tmp/my_table.raw --from /data/lancedb/my_table
```

### Inspect the contents

```bash
rawobjstr list --file /tmp/my_table.raw --long
```

Example output for a LanceDB table:

```
        512  my_table/_versions/1.manifest
       4096  my_table/data/00000.lance
       4096  my_table/data/00001.lance
         18  my_table/_transactions/0-abc123.txn
```

### Unpack back to a directory

```bash
rawobjstr export --file /tmp/my_table.raw --to /data/restored/my_table
```

### Pack from / unpack to S3

```bash
# Pull from S3 into a local image
rawobjstr import --file /tmp/my_table.raw --from s3://my-bucket/tables/my_table

# Push image back to S3
rawobjstr export --file /tmp/my_table.raw --to s3://my-bucket/archive/my_table
```

### Snapshot and ship workflow

```bash
# 1. Snapshot: pack a live LanceDB table into an image
rawobjstr format --file /tmp/snapshot.raw --size 4294967296
rawobjstr import --file /tmp/snapshot.raw --from /data/lancedb/my_table

# 2. Verify integrity
rawobjstr verify --file /tmp/snapshot.raw

# 3. Ship (scp, S3, NFS, USB stick)
scp /tmp/snapshot.raw user@remote:/data/snapshot.raw

# 4a. Unpack on the remote
rawobjstr export --file /data/snapshot.raw --to /data/lancedb/my_table

# 4b. Or open the image directly (no unpack)
#     Point LanceDB at it via RawObjectStore as shown in section 4
```

---

## 7. Device-to-Device Migration and Resize

```bash
# Resize: copy from a 1 GB image to a 16 GB image
rawobjstr format --file /tmp/big.raw --size 17179869184
rawobjstr import --file /tmp/big.raw --from raw:///tmp/old.raw
rawobjstr verify --file /tmp/big.raw

# Or migrate to a physical device
sudo rawobjstr format --device /dev/nvme1n1 --direct-io
sudo rawobjstr import --device /dev/nvme1n1 --from raw:///tmp/my_table.raw
```

For large devices, a raw `dd` copy of the data region is significantly faster
than `import`. See the
[low-level dd copy](CLI.md#low-level-dd-copy-fastest-resizeupgrade) section
in CLI.md for the full procedure and constraints.

See [CLI.md](CLI.md#import) for all import/export options, source URI
formats, and S3 authentication.

---

## 8. Integrity Checks and Maintenance

```bash
# Full CRC check on every extent
rawobjstr verify --file /tmp/lance_db.raw

# Rebuild free list from index (fixes leaked space after crash)
rawobjstr repair --file /tmp/lance_db.raw

# List files in the store with sizes
rawobjstr list --file /tmp/lance_db.raw --long

# List just one table
rawobjstr list --file /tmp/lance_db.raw --prefix my_table

# Show device stats (free space, fragmentation, txn ID)
rawobjstr info --file /tmp/lance_db.raw

# Zero all free space (secure erase of deleted data)
rawobjstr scrub --file /tmp/lance_db.raw
```

---

## 9. Performance Tips

- Use `OpenMode::SkipVerify` for LanceDB - no per-object scan on open.
- Use `--direct-io` when formatting a physical block device to bypass the kernel page
  cache.  LanceDB and rawobjstr together manage their own I/O.
- For loopback files on NVMe, `O_DIRECT` is optional - the page cache is already fast.
- For large databases with many tables, increase the index slot size at format time:
  ```bash
  # 64 MB index (32 MB per slot) supports ~1 million objects in the index
  rawobjstr format --file /tmp/big_db.raw --size 107374182400 --index-slot-size 33554432
  ```
- Call `flush_index()` explicitly after batch writes rather than after every single put.
  LanceDB does this naturally via its own commit cycle.

---

## 10. Full From-Scratch Example (Linux shell + Rust)

```bash
# Step 1: Create a 2 GB image
rawobjstr format --file /tmp/lancedb.raw --size 2147483648

# Step 2: (Optional) attach as loopback for inspection with block tools
sudo losetup --find --show /tmp/lancedb.raw
# /dev/loop0

# Step 3: verify the empty store looks correct
rawobjstr info --file /tmp/lancedb.raw

# Step 4: run your Rust application (see section 4 for code)
./my_app --store /tmp/lancedb.raw

# Step 5: inspect the tables written by LanceDB
rawobjstr list --file /tmp/lancedb.raw --long

# Step 6: verify integrity
rawobjstr verify --file /tmp/lancedb.raw

# Step 7: detach loopback (if attached)
sudo losetup --detach /dev/loop0
```
