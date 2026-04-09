# CLI Tools

`rawobjstr` is the CLI for managing rawobjstr devices. 

## Commands

| Command | Description |
|---------|-------------|
| `format` | Create a new formatted device (destroys all data) |
| `info` | Show device metadata and space usage |
| `list` / `ls` | List files on the device |
| `list-full` | List files with full extent details (offset, sizes, txn, metadata) |
| `get` | Retrieve a single file from the device |
| `getraw` | Retrieve raw on-disk bytes (no decompression) of an object |
| `getmeta` | Retrieve only the metadata suffix of a file |
| `put` | Store a single local file into the device |
| `putmeta` | Replace the metadata suffix of a file (body unchanged) |
| `delete` / `del` | Delete a single file from the device |
| `verify` | Check every extent's per-block CRC integrity (fsck) |
| `export` | Copy all files from the device to a local dir, raw device, or S3 |
| `import` | Import files from a local directory, another raw device, or S3 |
| `repair` | Rebuild free list from index and re-flush superblocks |
| `tombstones` | List tombstone records (objects removed during index validation) |
| `del-tombstone` | Remove a specific tombstone record |
| `scrub` | Zero out all free space regions on the device |
| `set-property` | Change device properties (write-protect, direct-io) without opening the store |
| `vacuum` | Remove stale delete markers (`__deleted__/*` keys) |
| `list-deleted` | List delete markers on the device |

## Global Options

| Flag | Description |
|------|-------------|
| `--help`, `-h` | Show help with command descriptions (exits 0) |
| `--version`, `-V` | Show version, git hash, and build date (exits 0) |

Example:

```
rawobjstr 0.1.0 (git a1b2c3d, built 2026-03-26 10:00:00 UTC)
```

## Common Options

| Flag | Description |
|------|-------------|
| `--file <path>` / `--device <path>` | Path to loopback file or block device (both flags are equivalent) |
| `--readonly` / `--read-only` | Open device in read-only mode (O_RDONLY). Valid for read operations: `list`, `list-full`, `get`, `getraw`, `getmeta`, `info`, `verify`, `tombstones`, `export`, `vacuum`, `list-deleted` |
| `--full-verify` | Use `OpenMode::FullVerify` (read every block, verify all CRCs on open). Can be combined with `--readonly` |

---

## format

Format a new device or loopback file.

```bash
# Loopback file (1 GB, default 16 MB index slots)
rawobjstr format --file /tmp/store.raw --size 1073741824

# With custom index slot size (32 MB per slot = 64 MB total index)
rawobjstr format --file /tmp/store.raw --size 1073741824 --index-slot-size 33554432

# With compression (zstd, snappy, or gzip0..gzip9)
rawobjstr format --file /tmp/store.raw --size 1073741824 --compression zstd

# Block device with O_DIRECT
sudo rawobjstr format --device /dev/sdb --direct-io
```

| Option | Required | Description |
|--------|----------|-------------|
| `--size <bytes>` | For loopback files | Total device size in bytes |
| `--direct-io` | No | Enable O_DIRECT (recommended for block devices) |
| `--index-slot-size <bytes>` | No | Index slot capacity (default: 16 MB, must be multiple of 16 MB) |
| `--max-key-length <bytes>` | No | Maximum object key length in bytes (default: 1024, hard ceiling: 65536). Values above `index_slot_size / 256 - 98` are silently clamped. |
| `--compression <alg>` | No | Compression algorithm: `none` (default), `zstd`, `snappy`, `gzip0`..`gzip9`. Applied transparently to all objects >= 4096 bytes. |

---

## info

Display device statistics: superblock fields, index usage, data region utilisation, free space, and fragmentation.

```bash
rawobjstr info --file /tmp/store.raw
```

Example output:

```
Device:             /tmp/store.raw
Device size:        268435456 bytes (256.0 MB)
Format version:     4
Flags:              0x00000000
Compression:        None
Transaction ID:     3

Index slot size:    16777216 bytes (16 MB)
Index serialized:   1234 bytes (0.0% of slot)
Index region A:     0x0e0002000
Index region B:     0x0f0002000
Active index:       0x0e0002000 (region A)
Last flush I/O:     4096 bytes
Max key length:     1024 bytes

Data region:        0x000002000 - 0x0e0002000 (224.0 MB)
Files:              6
Data stored:        24832 bytes (0.0 MB)
Device bytes used:  28672 bytes (0.0 MB, includes headers + padding)
Free space:         234876928 bytes (224.0 MB)
Free fragments:     1
Largest free:       234876928 bytes (224.0 MB)
```

Format version 4 uses a 256-shard index (partitioned by `crc32c(key) % 256`) with
optional transparent compression. Each `flush_index()` writes only the dirty shards,
so "Last flush I/O" shows the actual bytes written in the most recent flush -
typically a small fraction of "Index serialized".

---

## list

List files stored on the device. By default prints one filename per line, suitable for piping to other tools.

```bash
# One filename per line
rawobjstr list --file /tmp/store.raw

# With sizes (ls-style)
rawobjstr list --file /tmp/store.raw --long

# Filter by prefix
rawobjstr list --file /tmp/store.raw --prefix photos

# Pipe to scripts
rawobjstr list --file /tmp/store.raw | wc -l
rawobjstr list --file /tmp/store.raw --long | sort -rn | head
```

| Option | Required | Description |
|--------|----------|-------------|
| `--prefix <prefix>` | No | Only list files under this prefix |
| `--long` / `-l` | No | Show file sizes alongside names |

Default output (one per line):

```
photos/cat.jpg
photos/kitten.jpg
photos/puppy.jpg
docs/dog.txt
docs/notes.txt
```

Long output (`--long`):

```
     8401  photos/cat.jpg
     4096  photos/kitten.jpg
     4096  photos/puppy.jpg
       18  docs/dog.txt
     8192  docs/notes.txt
```

Aliases: `ls`

---

## list-full

List files with full extent-level details. Shows body size, metadata size, last-modified timestamp, transaction ID, and on-device offset for each file.

```bash
# All files
rawobjstr list-full --file /tmp/store.raw

# Filter by prefix
rawobjstr list-full --file /tmp/store.raw --prefix photos
```

| Option | Required | Description |
|--------|----------|-------------|
| `--prefix <prefix>` | No | Only list files under this prefix |

Example output:

```
 body  meta  last_modified        txn   offset          key
-----  ----  -------------------  ------  --------------  ---
8401     0  2026-03-29T10:00:01       1  0x000002000  photos/cat.jpg
4096     0  2026-03-29T10:00:02       2  0x000003000  photos/kitten.jpg
4096    42  2026-03-29T10:00:03       3  0x000004000  photos/puppy.jpg
  18     0  2026-03-29T10:00:04       4  0x000006000  docs/dog.txt
8192     0  2026-03-29T10:00:05       5  0x000007000  docs/notes.txt
```

`body` = payload bytes excluding metadata. `meta` = metadata suffix bytes. `offset` = byte offset of the extent on the device.

---

## get

Retrieve a single file from the device. Writes to stdout by default (binary-safe), or to a local file with `--to`.

```bash
# To stdout (pipe or redirect)
rawobjstr get --file /tmp/store.raw --key photos/cat.jpg > cat_out.jpg

# To a local file
rawobjstr get --file /tmp/store.raw --key photos/cat.jpg --to cat_out.jpg
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path of the file to retrieve |
| `--to <local-file>` | No | Write to this file instead of stdout |

When `--to` is used, a summary is printed to stderr. When writing to stdout, no extra output is produced so the stream is clean for piping.

---

## getraw

Retrieve the raw on-disk bytes of an object without decompression. If the object was compressed, this returns the compressed form. Useful for backup/restore, debugging compression ratios, or avoiding redundant decompress-recompress cycles.

```bash
# To a local file
rawobjstr getraw --file /tmp/store.raw --key photos/cat.jpg --to raw_dump.bin

# To stdout
rawobjstr getraw --file /tmp/store.raw --key photos/cat.jpg > raw_dump.bin
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path of the file to retrieve |
| `--to <local-file>` | No | Write to this file instead of stdout |

Compression information is printed to stderr:

```
compressed with zstd (50000 -> 12345 bytes, 75.3% savings)
12345 raw bytes -> raw_dump.bin
```

If the object is not compressed (compression disabled, object too small, or compression did not shrink it):

```
not compressed (50000 bytes)
50000 raw bytes -> raw_dump.bin
```

---

## getmeta

Retrieve only the metadata suffix of a file. Writes raw bytes to stdout by default, or to a local file with `--to`.

```bash
# To stdout
rawobjstr getmeta --file /tmp/store.raw --key photos/puppy.jpg

# To a file
rawobjstr getmeta --file /tmp/store.raw --key photos/puppy.jpg --to meta.bin
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path of the file |
| `--to <local-file>` | No | Write metadata to this file instead of stdout |

Outputs zero bytes (no output) if the file has no metadata suffix.

---

## put

Store a single local file into the device.

```bash
rawobjstr put --file /tmp/store.raw --key photos/bird.jpg --from bird.jpg
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path to store the file as |
| `--from <local-file>` | Yes | Local file to read |

A summary is printed to stderr. The index is flushed after the put.

---

## putmeta

Replace the metadata suffix of an existing file without re-uploading the body. The file body is preserved; only the metadata bytes are changed.

```bash
rawobjstr putmeta --file /tmp/store.raw --key photos/puppy.jpg --from new_meta.bin
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path of the file to update |
| `--from <local-file>` | Yes | File containing the new metadata bytes |

The body is read from the device, combined with the new metadata, and rewritten as a new extent. The old extent is freed. The index is flushed after the update.

---

## delete

Delete a single file from the device.

```bash
rawobjstr delete --file /tmp/store.raw --key photos/bird.jpg
```

| Option | Required | Description |
|--------|----------|-------------|
| `--key <object-path>` | Yes | ObjectStore path of the file to delete |

The index is flushed after the delete. Aliases: `del`

---

## verify

Read and verify every extent on the device. Checks:

- Per-block CRC verification for every file's blocks
- CRC32c of every file's payload against the index
- Overlapping extents (two files claiming the same disk region)
- Free list consistency (sorted, within bounds, no overlaps)
- Space accounting (used + free == total data region)

```bash
rawobjstr verify --file /tmp/store.raw
```

| Option | Required | Description |
|--------|----------|-------------|
| `--long` / `-l` | No | Show per-file OK results after the summary |

Exit code 0 if clean, 1 if any issues found.

Example clean output:

```
Verify: /tmp/store.raw

Files checked:       6
Files OK:            6
Errors:              0
Overlapping extents: 0
Free list OK:        true
Space accounted:     true
  Data region:       234881024 bytes
  Used:              28672 bytes
  Free:              234852352 bytes

Device is clean.
```

With `--long`, each healthy file is listed after the summary:

```
OK files:
  photos/cat.jpg (offset 0x2000, 8401 bytes): OK
  photos/kitten.jpg (offset 0x3000, 4096 bytes): OK
  photos/puppy.jpg (offset 0x4000, 2048 bytes): OK
```

Example with errors:

```
Errors:
  data/corrupt.dat (offset 0x4000, 1024 bytes): CRC mismatch: expected 0xdeadbeef, got 0x12345678

Device has issues. Run 'repair' to fix.
```

---

## export

Copy every file from the device to a target, preserving ObjectStore path structure.
The target can be:

- A **local directory** (any path)
- Another **rawobjstr device** (`raw://` prefix)
- An **S3 bucket** (`s3://` prefix, requires `--features aws`)
- A **shard store** via `objstrd` (use `s3://` with `AWS_ENDPOINT`)

```bash
# To a local directory
rawobjstr export --file /tmp/store.raw --to /tmp/export

# To another raw device
rawobjstr export --file /tmp/store.raw --to raw:///tmp/other.raw

# To S3 (files placed under the given prefix)
rawobjstr export --file /tmp/store.raw --to s3://my-bucket/backup-folder

# To a shard store (objstrd serving a sharded cluster)
AWS_ENDPOINT=http://localhost:8080 AWS_ALLOW_HTTP=true \
  rawobjstr export --file /tmp/store.raw --to s3://data/photos
```

| Option | Required | Description |
|--------|----------|-------------|
| `--to <dir\|raw://\|s3://>` | Yes | Target: local dir, `raw://` device, or `s3://bucket/prefix` |
| `--aws-region <region>` | No | AWS region override (S3 only) |

For local targets, files are written via `LocalFileSystem` and directory structure is created automatically. For `raw://` targets, the target device is opened, files are exported via `ObjectStore::put`, and the target is flushed. After export, the target mirrors the ObjectStore namespace:

```
/tmp/export/
  photos/
    cat.jpg
    kitten.jpg
    puppy.jpg
  docs/
    dog.txt
    notes.txt
```

---

## import

Import files into the device from an external source. The source can be:

- A **local directory** (any path)
- Another **rawobjstr device** (`raw://` prefix)
- An **S3 bucket** (`s3://` prefix, requires `--features aws`)
- A **shard store** via `objstrd` (use `s3://` with `AWS_ENDPOINT`)

```bash
# From a local directory
rawobjstr import --file /tmp/store.raw --from /path/to/files

# From another raw device (for upgrading or resizing)
rawobjstr import --file /tmp/new.raw --from raw:///tmp/old.raw

# From S3
rawobjstr import --file /tmp/store.raw --from s3://my-bucket/my-data
rawobjstr import --file /tmp/store.raw --from s3://my-bucket/my-data --aws-region us-east-1

# From a shard store (objstrd serving a sharded cluster)
AWS_ENDPOINT=http://localhost:8080 AWS_ALLOW_HTTP=true \
  rawobjstr import --file /tmp/store.raw --from s3://data/photos
```

| Option | Required | Description |
|--------|----------|-------------|
| `--from <source>` | Yes | Source URI (directory path, `raw://...`, or `s3://...`) |
| `--aws-region <region>` | No | AWS region for S3 (defaults to env/profile) |

### Source URI formats

| Prefix | Example | Description |
|--------|---------|-------------|
| _(none)_ | `/path/to/dir` | Local filesystem directory |
| `raw://` | `raw:///tmp/old.raw` | Another rawobjstr device |
| `s3://` | `s3://bucket/prefix` | S3 bucket (needs `--features aws`) |

### S3 authentication

S3 credentials are read from the standard AWS environment:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- `AWS_ENDPOINT` - custom S3-compatible endpoint (e.g. `http://localhost:8080` for `objstrd`)
- `AWS_ALLOW_HTTP` - set to `true` when using an HTTP endpoint
- `AWS_PROFILE`
- EC2 instance metadata / IRSA / ECS task role
- `~/.aws/credentials`

### Typical workflows

**Upgrade to a larger device:**

```bash
# 1. Format new device (larger size / bigger index slots)
rawobjstr format --file /tmp/big.raw --size 10737418240 --index-slot-size 33554432

# 2. Import everything from the old device
rawobjstr import --file /tmp/big.raw --from raw:///tmp/old.raw

# 3. Verify
rawobjstr verify --file /tmp/big.raw
```

**Import from S3:**

```bash
rawobjstr format --file /dev/nvme1n1 --direct-io
rawobjstr import --file /dev/nvme1n1 --from s3://my-data-lake/backups
rawobjstr verify --file /dev/nvme1n1
```

**Migrate from local filesystem to raw device:**

```bash
rawobjstr format --device /dev/sdb --direct-io
rawobjstr import --device /dev/sdb --from /mnt/data/files
```

**Export a raw device to a shard store (via objstrd):**

```bash
# objstrd is running on localhost:8080, serving a sharded cluster.
# The bucket name maps to the objstrd virtual bucket.
AWS_ENDPOINT=http://localhost:8080 AWS_ALLOW_HTTP=true \
  rawobjstr export --file /dev/nvme0n1 --to s3://data/backup
```

**Import from a shard store into a raw device:**

```bash
rawobjstr format --file /dev/nvme1n1 --direct-io
AWS_ENDPOINT=http://localhost:8080 AWS_ALLOW_HTTP=true \
  rawobjstr import --file /dev/nvme1n1 --from s3://data/photos
rawobjstr verify --file /dev/nvme1n1
```

### Low-level dd copy (fastest resize/upgrade)

When migrating to a larger device, a raw `dd` of the data region is faster than
`import` because it copies bytes verbatim with no decode/re-encode overhead.

```bash
# 1. Format the bigger destination
rawobjstr format --file /tmp/bigger.raw --size 8589934592   # 8 GB

# 2. Copy the data region, skipping the 8 KB superblock
#    (bs=4096, skip/seek=2 -> starts at offset 8192)
dd if=/tmp/old.raw of=/tmp/bigger.raw bs=4096 skip=2 seek=2 \
   count=$(( (OLD_SIZE - 8192) / 4096 )) conv=notrunc

# 3. Open  -  block-0 scan discovers copied extents, index is rebuilt
rawobjstr verify --file /tmp/bigger.raw
```

> **Minimum size constraint:** the new device must be at least
> `old_device_size + new_index_size` so the fresh index region does not
> overwrite copied data.
>
> **Fragmentation:** `dd` copies fragmentation as-is. Use `import` if you want
> to compact.
>
> **Optional cleanup:** `rawobjstr scrub --file /tmp/bigger.raw` zeros free
> space and erases any stale index bytes from the old layout.

---

## repair

Rebuild the free list by scanning the index, then flush the corrected state to disk.

This fixes:

- Leaked space from unflushed writes before a crash
- Inconsistent free list entries
- Fragmentation from corrupt free list state

```bash
rawobjstr repair --file /tmp/store.raw
```

Repair always prints verbose output showing everything it does.

Example output:

```
Repairing /tmp/store.raw ...
Files in index: 3
Used extents (3):
  0x00002000 .. 0x00003000  (    4096 bytes)  photos/cat.jpg
  0x00003000 .. 0x00004000  (    4096 bytes)  photos/kitten.jpg
  0x00004000 .. 0x00005000  (    4096 bytes)  docs/dog.txt
Free list rebuilt:
  3 entries -> 1 entries
  12288 bytes -> 234868736 bytes
  Recovered 223.9 MB of leaked space
New free regions (1):
  0x00005000 .. 0x0e002000  (234868736 bytes)
Flushed: true
Repair complete.
```

The repair process:

1. Reads the current index (file map)
2. Computes the expected free list as the complement of all used extents in `[DATA_START, data_end)`
3. Replaces the allocator's free list with the rebuilt one
4. Calls `flush_index()` to persist the repaired state and update both superblocks

---

## tombstones

List tombstone records - objects that were removed from the live index during
`open()` because their extent failed the integrity scan (block CRC mismatch,
CRC failure, or out-of-range offset).

```bash
rawobjstr tombstones --file /tmp/store.raw
```

Tombstones persist across flush/reopen and are cleared automatically when a
new object is written at the same path. They can also be removed manually:

```bash
# Remove one tombstone
rawobjstr del-tombstone --file /tmp/store.raw --key data/corrupt.dat
```

See also: `store.list_tombstones()`, `store.delete_tombstone()`, `store.clear_tombstones()` in [API.md](API.md).

---

## scrub

Zero out every free space region on the device. Use this to securely erase
deleted data or to confirm that no residual data exists in free extents.

```bash
rawobjstr scrub --file /tmp/store.raw
```

This runs `store.scrub_free_space()` under the hood. After scrubbing, free
extents contain only zeroes. Has no effect on live object data.

---

## vacuum

Remove stale delete markers from the device. Higher-level layers (e.g. `objstrd`)
record deletions by writing a small marker object under the `__deleted__/` prefix.
The `vacuum` command deletes all such marker keys and flushes the index.

```bash
rawobjstr vacuum --file /tmp/store.raw
```

Example output:

```
Vacuum complete: 3 marker(s) purged.
```

If no markers exist:

```
No delete markers found.
```

---

## list-deleted

List all delete markers on the device. Each marker is a key under `__deleted__/`
created when an object was deleted through the S3 layer.

```bash
rawobjstr list-deleted --file /tmp/store.raw
```

Example output:

```
KEY                                                          DELETED AT                          BODY SIZE
------------------------------------------------------------ ------------------------------ ----------
photos/a.jpg                                                 2026-03-26T10:00:00+00:00             128
docs/readme.txt                                              2026-03-26T10:01:00+00:00              64

2 delete marker(s) total.
```

If no markers exist:

```
No delete markers found.
```

---

## set-property

Change device properties (flags) in the superblock without opening the store for normal I/O. This uses the static `RawObjectStore::modify_flags()` method to directly read/modify/write the superblock.

```bash
rawobjstr set-property --file /tmp/store.raw --write-protect on
rawobjstr set-property --file /tmp/store.raw --direct-io off
rawobjstr set-property --file /tmp/store.raw --write-protect on --direct-io off
```

| Option | Values | Description |
|--------|--------|-------------|
| `--write-protect <on\|off>` | `on`, `off`, `true`, `false`, `1`, `0`, `yes`, `no` | Enable or disable the write-protect flag. When enabled, all write operations return `WriteProtected` errors. |
| `--direct-io <on\|off>` | `on`, `off`, `true`, `false`, `1`, `0`, `yes`, `no` | Enable or disable the direct-I/O flag. When enabled, the device is opened with `O_DIRECT`. |

At least one of `--write-protect` or `--direct-io` must be specified. Both can be used in a single invocation.

Example output:

```
Flags updated: 0x00000002 [WRITE_PROTECT]
```

**Note:** `set-property` does not open the store fully  -  it reads the superblock, modifies the flags field, and writes it back. This means it works even on a write-protected device (it bypasses the write-protect check since it operates below the store layer).
