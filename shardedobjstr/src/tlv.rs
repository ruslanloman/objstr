//! Variable-length TLV metadata encoding/decoding.
//!
//! Layout: [TLV entries...] -- no padding, no marker, no fixed size.
//! The raw store's `meta_len: u16` in the index records how many bytes
//! of metadata are appended after the body.
//!
//! Known-field entry (tags 1-7):
//!   Lookup hit:  [tag|0x80:u8] [idx:u8]           -- 2 bytes
//!   Raw value:   [tag:u8] [len:u16 LE] [value...] -- 3 + len bytes
//!
//! The LOOKUP_BIT (0x80) is placed on the tag byte, not the descriptor,
//! so that a raw value whose u16 LE length low byte >= 128 is never
//! mistaken for a lookup hit.  Tags 1-7 with bit 7 set become 0x81-0x87,
//! which never collides with TAG_CUSTOM (0x80).
//!
//! Custom x-amz-meta-* entry (tag 128):
//!   [128:u8] [slen:u16 LE] [suffix...] [vlen:u16 LE] [value...]

use std::collections::HashMap;

/// Bit flag in the value descriptor indicating a lookup-table hit.
pub const LOOKUP_BIT: u8 = 0x80;

// Field tags (1-7 = well-known S3 headers, 128 = user x-amz-meta-*)
pub const TAG_CONTENT_TYPE:        u8 = 1;
pub const TAG_CACHE_CONTROL:       u8 = 2;
pub const TAG_CONTENT_DISPOSITION: u8 = 3;
pub const TAG_CONTENT_ENCODING:    u8 = 4;
pub const TAG_CONTENT_LANGUAGE:    u8 = 5;
pub const TAG_EXPIRES:             u8 = 6;
pub const TAG_ETAG:                u8 = 7;
pub const TAG_CUSTOM:              u8 = 128;

pub fn field_name_to_tag(name: &str) -> Option<u8> {
    match name {
        "content-type"        => Some(TAG_CONTENT_TYPE),
        "cache-control"       => Some(TAG_CACHE_CONTROL),
        "content-disposition" => Some(TAG_CONTENT_DISPOSITION),
        "content-encoding"    => Some(TAG_CONTENT_ENCODING),
        "content-language"    => Some(TAG_CONTENT_LANGUAGE),
        "expires"             => Some(TAG_EXPIRES),
        "etag"                => Some(TAG_ETAG),
        _ => None,
    }
}

pub fn tag_to_field_name(tag: u8) -> Option<&'static str> {
    match tag {
        TAG_CONTENT_TYPE        => Some("content-type"),
        TAG_CACHE_CONTROL       => Some("cache-control"),
        TAG_CONTENT_DISPOSITION => Some("content-disposition"),
        TAG_CONTENT_ENCODING    => Some("content-encoding"),
        TAG_CONTENT_LANGUAGE    => Some("content-language"),
        TAG_EXPIRES             => Some("expires"),
        TAG_ETAG                => Some("etag"),
        _ => None,
    }
}

// --- Per-field value lookup tables ---
// Index -> well-known string.  Keeping the index stable is important;
// only *append* to these arrays, never reorder.

pub const CT_VALUES: &[&str] = &[
    "application/octet-stream",            // 0
    "text/plain",                          // 1
    "text/html",                           // 2
    "text/css",                            // 3
    "text/csv",                            // 4
    "application/json",                    // 5
    "application/xml",                     // 6
    "image/png",                           // 7
    "image/jpeg",                          // 8
    "image/gif",                           // 9
    "image/webp",                          // 10
    "image/svg+xml",                       // 11
    "application/pdf",                     // 12
    "application/zip",                     // 13
    "video/mp4",                           // 14
    "audio/mpeg",                          // 15
    "application/javascript",              // 16
    "text/xml",                            // 17
    "application/x-www-form-urlencoded",   // 18
    "multipart/form-data",                 // 19
    "application/gzip",                    // 20
    "application/x-tar",                   // 21
    "image/tiff",                          // 22
    "audio/wav",                           // 23
    "video/webm",                          // 24
    "application/wasm",                    // 25
    "font/woff2",                          // 26
    "font/woff",                           // 27
    "text/plain; charset=utf-8",           // 28
    "application/json; charset=utf-8",     // 29
    "text/html; charset=utf-8",            // 30
    "binary/octet-stream",                 // 31
];

pub const CC_VALUES: &[&str] = &[
    "no-cache",                            // 0
    "no-store",                            // 1
    "no-cache, no-store",                  // 2
    "private",                             // 3
    "public",                              // 4
    "must-revalidate",                     // 5
    "max-age=0",                           // 6
    "max-age=60",                          // 7
    "max-age=300",                         // 8
    "max-age=3600",                        // 9
    "max-age=86400",                       // 10
    "max-age=604800",                      // 11
    "max-age=2592000",                     // 12
    "max-age=31536000",                    // 13
    "public, max-age=3600",                // 14
    "public, max-age=86400",               // 15
    "public, max-age=31536000",            // 16
    "public, max-age=31536000, immutable", // 17
    "private, no-cache",                   // 18
    "no-store, must-revalidate",           // 19
];

pub const CE_VALUES: &[&str] = &[
    "gzip",     // 0
    "br",       // 1
    "deflate",  // 2
    "identity", // 3
    "zstd",     // 4
    "compress", // 5
];

pub fn value_lookup(tag: u8, value: &str) -> Option<u8> {
    let table: &[&str] = match tag {
        TAG_CONTENT_TYPE     => CT_VALUES,
        TAG_CACHE_CONTROL    => CC_VALUES,
        TAG_CONTENT_ENCODING => CE_VALUES,
        _ => return None,
    };
    table.iter().position(|&v| v == value).map(|i| i as u8)
}

pub fn value_from_lookup(tag: u8, idx: u8) -> Option<&'static str> {
    let table: &[&str] = match tag {
        TAG_CONTENT_TYPE     => CT_VALUES,
        TAG_CACHE_CONTROL    => CC_VALUES,
        TAG_CONTENT_ENCODING => CE_VALUES,
        _ => return None,
    };
    table.get(idx as usize).copied()
}

/// Maximum byte length for a single metadata field value or key suffix.
/// The TLV format uses u16 LE for lengths, so 65535 is the hard maximum.
pub const MAX_FIELD_LEN: usize = u16::MAX as usize;

/// Error returned when a metadata field exceeds the TLV encoding limit.
#[derive(Debug)]
pub struct MetadataTooLarge {
    pub field: String,
    pub len: usize,
    pub max: usize,
}

impl std::fmt::Display for MetadataTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "metadata field '{}' is {} bytes, exceeds maximum {} bytes",
            self.field, self.len, self.max
        )
    }
}

impl std::error::Error for MetadataTooLarge {}

/// Encode a metadata map into compact variable-length TLV bytes.
///
/// Returns an exact-sized Vec (no padding).  The caller passes this to
/// `RawObjectStore::put_with_meta()` which records `meta_len` in the index.
///
/// Returns `Err(MetadataTooLarge)` if any field key suffix or value
/// exceeds 65535 bytes.
pub fn encode_metadata(meta: &HashMap<String, String>) -> Result<Vec<u8>, MetadataTooLarge> {
    let mut buf = Vec::with_capacity(128);

    // Sort keys for deterministic encoding order.
    let mut sorted_keys: Vec<&String> = meta.keys().collect();
    sorted_keys.sort();

    for key in sorted_keys {
        let value = &meta[key];
        if let Some(tag) = field_name_to_tag(key) {
            // Try value lookup first -- 2 bytes total
            if let Some(idx) = value_lookup(tag, value) {
                buf.push(tag | LOOKUP_BIT);
                buf.push(idx);
                continue;
            }
            // Raw value with u16 LE length
            let vb = value.as_bytes();
            if vb.len() > MAX_FIELD_LEN {
                return Err(MetadataTooLarge {
                    field: key.clone(),
                    len: vb.len(),
                    max: MAX_FIELD_LEN,
                });
            }
            buf.push(tag);
            buf.extend_from_slice(&(vb.len() as u16).to_le_bytes());
            buf.extend_from_slice(vb);
        } else if let Some(suffix) = key.strip_prefix("x-amz-meta-") {
            // Custom x-amz-meta-* field
            let sb = suffix.as_bytes();
            if sb.len() > MAX_FIELD_LEN {
                return Err(MetadataTooLarge {
                    field: key.clone(),
                    len: sb.len(),
                    max: MAX_FIELD_LEN,
                });
            }
            let vb = value.as_bytes();
            if vb.len() > MAX_FIELD_LEN {
                return Err(MetadataTooLarge {
                    field: key.clone(),
                    len: vb.len(),
                    max: MAX_FIELD_LEN,
                });
            }
            buf.push(TAG_CUSTOM);
            buf.extend_from_slice(&(sb.len() as u16).to_le_bytes());
            buf.extend_from_slice(sb);
            buf.extend_from_slice(&(vb.len() as u16).to_le_bytes());
            buf.extend_from_slice(vb);
        }
    }

    Ok(buf)
}

/// Decode variable-length TLV metadata bytes into a HashMap.
pub fn decode_metadata(data: &[u8]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut pos = 0;
    while pos < data.len() {
        let tag_byte = data[pos];
        pos += 1;

        if tag_byte == TAG_CUSTOM {
            // [slen:u16 LE] [suffix...] [vlen:u16 LE] [value...]
            if pos + 2 > data.len() { break; }
            let slen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + slen > data.len() { break; }
            let suffix = std::str::from_utf8(&data[pos..pos + slen]).unwrap_or_default();
            pos += slen;
            if pos + 2 > data.len() { break; }
            let vlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + vlen > data.len() { break; }
            let val = std::str::from_utf8(&data[pos..pos + vlen]).unwrap_or_default();
            pos += vlen;
            map.insert(format!("x-amz-meta-{suffix}"), val.to_string());
        } else if tag_byte & LOOKUP_BIT != 0 {
            // Lookup hit -- LOOKUP_BIT is on the tag byte, real tag is low 7 bits
            let tag = tag_byte & 0x7F;
            if pos >= data.len() { break; }
            let idx = data[pos];
            pos += 1;
            if let Some(val) = value_from_lookup(tag, idx) {
                if let Some(name) = tag_to_field_name(tag) {
                    map.insert(name.to_string(), val.to_string());
                }
            }
        } else {
            // Raw value -- u16 LE length follows the plain tag
            let tag = tag_byte;
            if pos + 2 > data.len() { break; }
            let vlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + vlen > data.len() { break; }
            let val = std::str::from_utf8(&data[pos..pos + vlen]).unwrap_or_default();
            pos += vlen;
            if let Some(name) = tag_to_field_name(tag) {
                map.insert(name.to_string(), val.to_string());
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip_known_fields() {
        let mut meta = HashMap::new();
        meta.insert("content-type".to_string(), "application/json".to_string());
        meta.insert("cache-control".to_string(), "no-cache".to_string());
        meta.insert("content-encoding".to_string(), "gzip".to_string());

        let encoded = encode_metadata(&meta).unwrap();
        let decoded = decode_metadata(&encoded);

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.get("content-type").unwrap(), "application/json");
        assert_eq!(decoded.get("cache-control").unwrap(), "no-cache");
        assert_eq!(decoded.get("content-encoding").unwrap(), "gzip");
    }

    #[test]
    fn encode_decode_roundtrip_custom_meta() {
        let mut meta = HashMap::new();
        meta.insert("x-amz-meta-foo".to_string(), "bar".to_string());
        meta.insert("x-amz-meta-baz".to_string(), "qux".to_string());

        let encoded = encode_metadata(&meta).unwrap();
        let decoded = decode_metadata(&encoded);

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.get("x-amz-meta-foo").unwrap(), "bar");
        assert_eq!(decoded.get("x-amz-meta-baz").unwrap(), "qux");
    }

    #[test]
    fn encode_decode_mixed_known_and_custom() {
        let mut meta = HashMap::new();
        meta.insert("content-type".to_string(), "text/plain".to_string());
        meta.insert("x-amz-meta-author".to_string(), "alice".to_string());
        meta.insert("cache-control".to_string(), "max-age=3600".to_string());
        meta.insert("x-amz-meta-version".to_string(), "42".to_string());

        let encoded = encode_metadata(&meta).unwrap();
        let decoded = decode_metadata(&encoded);

        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded.get("content-type").unwrap(), "text/plain");
        assert_eq!(decoded.get("x-amz-meta-author").unwrap(), "alice");
        assert_eq!(decoded.get("cache-control").unwrap(), "max-age=3600");
        assert_eq!(decoded.get("x-amz-meta-version").unwrap(), "42");
    }

    #[test]
    fn encode_metadata_too_large() {
        let mut meta = HashMap::new();
        // Create a value that exceeds 65535 bytes
        let huge_value: String = "x".repeat(70_000);
        meta.insert("content-type".to_string(), huge_value);

        let result = encode_metadata(&meta);
        assert!(result.is_err(), "should reject oversized field");
        let err = result.unwrap_err();
        assert_eq!(err.field, "content-type");
        assert_eq!(err.len, 70_000);
        assert_eq!(err.max, MAX_FIELD_LEN);
    }

    #[test]
    fn encode_metadata_too_large_custom_key() {
        let mut meta = HashMap::new();
        let huge_suffix: String = "k".repeat(70_000);
        meta.insert(format!("x-amz-meta-{huge_suffix}"), "val".to_string());

        let result = encode_metadata(&meta);
        assert!(result.is_err(), "should reject oversized custom key suffix");
    }

    #[test]
    fn decode_graceful_on_truncated() {
        // Empty input
        let decoded = decode_metadata(&[]);
        assert!(decoded.is_empty());

        // Single tag byte with no descriptor
        let decoded = decode_metadata(&[TAG_CONTENT_TYPE]);
        assert!(decoded.is_empty());

        // Tag + raw-value length but missing value bytes
        let decoded = decode_metadata(&[TAG_CONTENT_TYPE, 0x05, 0x00]);
        assert!(decoded.is_empty(), "should not panic on truncated value");

        // Custom tag with truncated suffix length
        let decoded = decode_metadata(&[TAG_CUSTOM, 0x03]);
        assert!(decoded.is_empty());

        // Valid entry followed by garbage
        let mut meta = HashMap::new();
        meta.insert("content-type".to_string(), "text/plain".to_string());
        let mut encoded = encode_metadata(&meta).unwrap();
        encoded.push(TAG_CUSTOM); // truncated custom entry at end
        let decoded = decode_metadata(&encoded);
        assert_eq!(decoded.len(), 1, "should decode the valid entry and skip truncated");
        assert_eq!(decoded.get("content-type").unwrap(), "text/plain");
    }

    #[test]
    fn value_lookup_and_reverse() {
        // content-type: "application/json" is in the lookup table at index 5
        let idx = value_lookup(TAG_CONTENT_TYPE, "application/json");
        assert_eq!(idx, Some(5));

        let reversed = value_from_lookup(TAG_CONTENT_TYPE, 5);
        assert_eq!(reversed, Some("application/json"));

        // cache-control: "no-cache" is at index 0
        let idx = value_lookup(TAG_CACHE_CONTROL, "no-cache");
        assert_eq!(idx, Some(0));

        let reversed = value_from_lookup(TAG_CACHE_CONTROL, 0);
        assert_eq!(reversed, Some("no-cache"));

        // content-encoding: "gzip" is at index 0
        let idx = value_lookup(TAG_CONTENT_ENCODING, "gzip");
        assert_eq!(idx, Some(0));

        // Unknown value should return None
        let idx = value_lookup(TAG_CONTENT_TYPE, "application/x-custom-unknown");
        assert_eq!(idx, None);

        // Unknown tag should return None
        let idx = value_lookup(TAG_ETAG, "some-etag-value");
        assert_eq!(idx, None);

        // Out-of-range index should return None
        let val = value_from_lookup(TAG_CONTENT_TYPE, 200);
        assert_eq!(val, None);
    }

    #[test]
    fn known_field_raw_encoding_for_unlisted_value() {
        // Use a content-type NOT in the lookup table
        let mut meta = HashMap::new();
        meta.insert(
            "content-type".to_string(),
            "application/x-custom-unknown".to_string(),
        );

        let encoded = encode_metadata(&meta).unwrap();
        let decoded = decode_metadata(&encoded);

        assert_eq!(decoded.get("content-type").unwrap(), "application/x-custom-unknown");
    }

    #[test]
    fn field_name_tag_mapping() {
        assert_eq!(field_name_to_tag("content-type"), Some(TAG_CONTENT_TYPE));
        assert_eq!(field_name_to_tag("cache-control"), Some(TAG_CACHE_CONTROL));
        assert_eq!(field_name_to_tag("content-disposition"), Some(TAG_CONTENT_DISPOSITION));
        assert_eq!(field_name_to_tag("etag"), Some(TAG_ETAG));
        assert_eq!(field_name_to_tag("unknown-header"), None);

        assert_eq!(tag_to_field_name(TAG_CONTENT_TYPE), Some("content-type"));
        assert_eq!(tag_to_field_name(TAG_ETAG), Some("etag"));
        assert_eq!(tag_to_field_name(99), None);
    }

    #[test]
    fn raw_value_length_ge_128_roundtrips() {
        // Regression: values whose u16 LE length has low byte >= 128
        // were previously misinterpreted as lookup hits because the
        // LOOKUP_BIT was checked on the descriptor byte (which was
        // actually the length low byte).
        let long_val: String = "x".repeat(200); // length 200 > 128
        let mut meta = HashMap::new();
        meta.insert("content-type".to_string(), long_val.clone());

        let encoded = encode_metadata(&meta).unwrap();
        let decoded = decode_metadata(&encoded);
        assert_eq!(decoded.get("content-type").unwrap(), &long_val);

        // Also test a tag without a lookup table (content-disposition)
        let mut meta2 = HashMap::new();
        meta2.insert("content-disposition".to_string(), "x".repeat(130));
        meta2.insert("etag".to_string(), "y".repeat(400)); // length 400, low byte 0x90
        let encoded2 = encode_metadata(&meta2).unwrap();
        let decoded2 = decode_metadata(&encoded2);
        assert_eq!(decoded2.get("content-disposition").unwrap().len(), 130);
        assert_eq!(decoded2.get("etag").unwrap().len(), 400);
    }

    #[test]
    fn encode_metadata_deterministic() {
        // Regression: HashMap iteration order is non-deterministic.
        // encode_metadata must sort keys so identical metadata always
        // produces identical bytes (required for CRC stability).
        let mut meta = HashMap::new();
        meta.insert("content-type".to_string(), "text/plain".to_string());
        meta.insert("cache-control".to_string(), "no-cache".to_string());
        meta.insert("x-amz-meta-foo".to_string(), "bar".to_string());
        meta.insert("x-amz-meta-zzz".to_string(), "last".to_string());
        meta.insert("content-encoding".to_string(), "identity".to_string());

        let first = encode_metadata(&meta).unwrap();
        for _ in 0..20 {
            let again = encode_metadata(&meta).unwrap();
            assert_eq!(first, again, "encode_metadata must be deterministic");
        }
    }
}
