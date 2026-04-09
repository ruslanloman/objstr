# Metadata Storage Format

## Overview

The S3 adapter stores objects on the underlying **RawObjectStore**  with an
**inline metadata trailer** appended to every object.  What the raw store
sees as one extent is actually:

```
[ S3 object body bytes ] [ variable-length TLV metadata ]
```

The metadata length is recorded in the raw store's index as `meta_len: u16`,
so the on-disk size of every object is **body + meta_len bytes**.

The S3 adapter transparently strips the trailer on reads and appends it on
writes, so S3 clients never see the trailer - `Content-Length`, range
requests, `ListObjects` sizes, and `HEAD` all report the logical body size.

On the read path, custom metadata values are checked for Latin-1
(ISO 8859-1) compatibility before being emitted as HTTP headers.
Non-Latin-1 values are silently dropped from the response (with a
warning log) because HTTP headers cannot carry arbitrary bytes.
The write path does not validate encoding - any byte sequence can
be stored.

## Why Inline?

Previously metadata was stored in a separate "sidecar" extent keyed as
`__meta__/<bucket>/<key>`.  The problem: the raw store's minimum extent
allocation is one 4 096-byte block, so a ~100-byte JSON metadata blob
consumed a full 4 KB block.  Every object therefore used **two** extents
(data + metadata), doubling the index entry count and wasting up to 4 KB
per object.

The inline trailer eliminates the sidecar entirely.  A single extent now
holds both the data and its metadata, cutting index entries in half and
reclaiming one block per object.

## Trailer Layout - Variable-Length TLV

The trailer uses a compact **Tag-Length-Value (TLV)** encoding with no
padding, no fixed size, and no framing marker.  The raw store's index
field `meta_len: u16` records the total byte length of the trailer so
the reader knows where the metadata starts.

```
Layout: [TLV entry] [TLV entry] ... - no padding, no marker
```

### Known-Field Entry (tags 1-7)

Well-known S3 headers are assigned tags 1-7.  Each entry is encoded as
either a **lookup hit** (2 bytes) or a **raw value** (3 + N bytes):

```
Lookup hit:   [tag|0x80:u8] [idx:u8]               -- 2 bytes total
Raw value:    [tag:u8] [len:u16 LE] [value...]      -- 3 + len bytes
```

When the **tag byte** has bit 7 set (`0x80`), the real tag is in the
lower 7 bits and the next byte is an index into a per-field lookup table
of common values.  This lets frequent values like
`application/octet-stream` or `no-cache` be stored in just 2 bytes.
Tags 1-7 with bit 7 set become `0x81-0x87`, which never collide with
`TAG_CUSTOM` (`0x80`).

### Custom x-amz-meta-* Entry (tag 128)

User-defined metadata keys are encoded with tag 128.  The key prefix
`x-amz-meta-` is stripped; only the suffix is stored:

```
[128:u8] [slen:u16 LE] [suffix...] [vlen:u16 LE] [value...]
```

### Maximum Field Length

Both lengths use u16 LE, so the hard maximum for any single key suffix
or value is **65 535 bytes**.  Fields exceeding this limit are rejected
with a `MetadataTooLarge` error.

### Empty Metadata

When no metadata is present, the TLV trailer is empty (0 bytes) and
`meta_len` is 0 in the index.

## Tag Assignments

| Tag | Field | Lookup Table |
|-----|-------|-------------|
| 1 | `content-type` | Yes (32 entries: `application/octet-stream`, `text/plain`, `application/json`, ...) |
| 2 | `cache-control` | Yes (20 entries: `no-cache`, `no-store`, `max-age=3600`, ...) |
| 3 | `content-disposition` | No |
| 4 | `content-encoding` | Yes (6 entries: `gzip`, `br`, `deflate`, `identity`, `zstd`, `compress`) |
| 5 | `content-language` | No |
| 6 | `expires` | No |
| 7 | `etag` | No |
| 128 | `x-amz-meta-*` | No (custom user metadata) |

Lookup tables are append-only.  Indices are stable across versions -
new entries are only added at the end.

## Storage Strategy by Shard Type

S3 metadata is stored differently depending on the shard type. The
`ShardKind` enum in `metadata.rs` drives this:

| ShardKind | Storage mechanism | Read path |
|-----------|-------------------|----------|
| `Raw` | TLV-encoded metadata appended to the object body in a single extent. `meta_len` in the raw index tracks the split point. | Suffix read on the extent; decoded via `decode_metadata()`. |
| `S3Like` (S3, R2, child Node) | Native S3 attributes via `put_opts()` with `PutOptions { attributes }`. | `GetResult.attributes` from `get()`; converted back to TLV via `attributes_to_meta()` + `encode_metadata()`. |
| `Sidecar` (LocalFileSystem, InMemory) | `__meta__` sidecar object at `{path}.__meta__` containing raw TLV bytes. `LocalFileSystem` rejects `Attributes`, so this is the only option. | Separate `get()` on the sidecar path. |

The adapter always works with TLV bytes at its interface boundary. The
conversion to/from native S3 `Attributes` or sidecar files happens inside
`metadata.rs`, invisible to the rest of the adapter.

### Example Encoding

A metadata set `{"content-type": "image/png", "etag": "d41d...7e", "x-amz-meta-author": "alice"}`:

| Entry | Encoding | Bytes |
|-------|----------|-------|
| content-type = image/png | `[0x81] [0x07]` (tag 1 \| 0x80, lookup index 7) | 2 |
| etag = d41d...7e | `[0x07] [0x20 0x00] [d41d...7e]` (tag 7, len=32 LE, raw value) | 35 |
| x-amz-meta-author = alice | `[0x80] [0x06 0x00] [author] [0x05 0x00] [alice]` (tag 128) | 16 |

Total trailer: **53 bytes** (vs 256 bytes in the old fixed-size format).

## How Each Operation Handles the Trailer

| Operation | Behaviour |
|-----------|-----------|
| **PutObject** | Body + TLV trailer written as a single extent. `meta_len` recorded in index. |
| **GetObject** | Only the body portion is streamed (length = stored size - `meta_len`); the trailer is never read on this path. Range requests apply to the logical body only. |
| **HeadObject** | Stored size - `meta_len` = reported `Content-Length`. Trailer read via suffix range to extract metadata headers. |
| **GetObjectAttributes** | Same metadata path as HeadObject. Returns requested fields (ETag, ObjectSize, StorageClass) from trailer. ObjectParts not yet persisted. |
| **DeleteObject** | Extent deleted. For non-raw shards (Sidecar kind), the `{path}.__meta__` sidecar is also removed. |
| **CopyObject (COPY)** | Source object read (body + metadata), then written to destination via `put_with_meta`. Metadata carries over. |
| **CopyObject (REPLACE)** | Source extent read, body extracted, new trailer built from request headers, combined data written to destination. |
| **CompleteMultipartUpload** | Parts streamed to destination, then TLV trailer appended before commit. |
| **ListObjectsV2** | Reported `<Size>` = stored size - `meta_len`. |
| **HeadBucket stats** | `x-rgw-bytes-used` sums logical body sizes per object. |

## Interaction With the Raw Store

The raw store allocates extents in 4 096-byte aligned blocks.  A 1-byte
S3 object with a 10-byte TLV trailer becomes 11 bytes of logical data,
which the raw store pads to one 4 096-byte block on disk.

The variable-length TLV trailer is typically much smaller than the old
256-byte fixed trailer, so small objects waste less space.

---

## Non-Raw Shard Metadata

The format above applies only to **raw block-device shards**.  In a
heterogeneous cluster (tree config), shards can also be S3, R2, child
Nodes, LocalFileSystem, or InMemory.  Each uses a different metadata
strategy, selected by the `ShardKind` enum in `metadata.rs`:

| ShardKind | Mechanism |
|-----------|-----------|
| `Raw` | Inline TLV trailer as described above. |
| `S3Like` | Native S3 attributes (`put_opts` / `GetResult.attributes`). TLV is decoded to `Attributes` on write and re-encoded on read. |
| `Sidecar` | TLV bytes stored in a sidecar object at `{path}.__meta__`. Used for `LocalFileSystem` and `InMemory`, which reject `Attributes`. |

The TLV encoding is the common wire format at the adapter boundary.
Conversion to/from native attributes or sidecar files happens inside
`metadata.rs`.
