//! M6 large-object / memory-pressure tests.
//!
//! These tests verify that single PUT and multipart upload of very large
//! objects do not cause the server to OOM-allocate the whole body in RAM.
//! The server spools incoming HTTP bodies through an anonymous temp file,
//! so peak server RSS stays roughly constant regardless of object size.
//!
//! # Requirements
//!
//! - ~30 GB of free disk space on the OS temp partition
//! - Several minutes to run
//!
//! # Running
//!
//! ```text
//! cargo test -p objstrd --release -- --ignored large_
//! ```
//!
//! # What is tested
//!
//! - `large_single_put`    : streams a 15 GB body as a single PUT request,
//!   verifies stored size via HEAD + spot-checks first/last bytes via range GET.
//! - `large_multipart_upload` : uploads 10 × 1 GB parts (10 GB total), completes
//!   the multipart, verifies stored size + per-part byte spot-checks.

mod common;
use common::{complete_xml, extract_xml_tag, TestServer};

use bytes::Bytes;
use futures::Stream;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Total store capacity (GB).
const STORE_SIZE_GB: u64 = 30;

/// Size of the single-PUT object (GB).
const SINGLE_PUT_GB: u64 = 15;

/// Number of multipart parts.
const MULTI_PARTS: u32 = 10;

/// Size of each multipart part (GB).
const PART_SIZE_GB: u64 = 1;

/// Client-side chunk size when streaming the body (4 MB).
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a streaming body of `total_bytes` where every byte equals `fill`.
///
/// Each chunk is allocated on demand so the client never holds more than one
/// chunk in RAM at a time — keeping client-side memory flat too.
fn fill_stream(
    total_bytes: u64,
    fill: u8,
) -> impl Stream<Item = Result<Bytes, std::convert::Infallible>> + Send + 'static {
    futures::stream::unfold(0u64, move |pos| async move {
        if pos >= total_bytes {
            return None;
        }
        let take = CHUNK_SIZE.min((total_bytes - pos) as usize);
        Some((Ok(Bytes::from(vec![fill; take])), pos + take as u64))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Stream a 15 GB single PUT to a 30 GB store.
///
/// The server must spool the body through a temp file on disk rather than
/// buffer the whole thing in heap memory.  We verify:
///   1. PUT returns 200.
///   2. HEAD reports the correct Content-Length.
///   3. Range GET of first and last byte returns the expected fill value.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires ~30 GB free disk and several minutes; run with: cargo test --release -- --ignored large_single_put"]
async fn large_single_put() {
    let srv = TestServer::start_with_size_gb(STORE_SIZE_GB, "data").await;
    let total_bytes = SINGLE_PUT_GB * 1024 * 1024 * 1024;
    const FILL: u8 = 0xAB;

    // --- PUT -----------------------------------------------------------------
    let stream = fill_stream(total_bytes, FILL);
    let body = reqwest::Body::wrap_stream(stream);

    let resp = srv
        .client
        .put(&srv.object_url("large/single.bin"))
        .header("content-type", "application/octet-stream")
        .body(body)
        .send()
        .await
        .expect("PUT request failed");
    assert_eq!(resp.status(), 200, "PUT should succeed");

    // --- HEAD: verify stored size -------------------------------------------
    let resp = srv
        .client
        .head(&srv.object_url("large/single.bin"))
        .send()
        .await
        .expect("HEAD request failed");
    assert_eq!(resp.status(), 200, "HEAD should succeed");
    let stored_len: u64 = resp
        .headers()
        .get("content-length")
        .expect("content-length header missing")
        .to_str()
        .unwrap()
        .parse()
        .expect("content-length not a number");
    assert_eq!(stored_len, total_bytes, "stored size should equal uploaded size");

    // --- Range GET: first byte ----------------------------------------------
    let resp = srv
        .client
        .get(&srv.object_url("large/single.bin"))
        .header("range", "bytes=0-0")
        .send()
        .await
        .expect("range GET (first byte) failed");
    assert_eq!(resp.status(), 206);
    let b = resp.bytes().await.unwrap();
    assert_eq!(b[0], FILL, "first byte mismatch");

    // --- Range GET: last byte -----------------------------------------------
    let last = total_bytes - 1;
    let resp = srv
        .client
        .get(&srv.object_url("large/single.bin"))
        .header("range", format!("bytes={last}-{last}"))
        .send()
        .await
        .expect("range GET (last byte) failed");
    assert_eq!(resp.status(), 206);
    let b = resp.bytes().await.unwrap();
    assert_eq!(b[0], FILL, "last byte mismatch");
}

/// Upload 10 × 1 GB parts (10 GB total) via multipart to a 30 GB store.
///
/// Each part body is also streamed (not pre-allocated).  We verify:
///   1. Each UploadPart returns 200 with an ETag.
///   2. CompleteMultipartUpload returns 200.
///   3. HEAD reports the correct total Content-Length.
///   4. Range GET of the first byte in each part returns the expected fill.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires ~30 GB free disk and several minutes; run with: cargo test --release -- --ignored large_multipart_upload"]
async fn large_multipart_upload() {
    let srv = TestServer::start_with_size_gb(STORE_SIZE_GB, "data").await;
    let part_bytes: u64 = PART_SIZE_GB * 1024 * 1024 * 1024;
    let total_bytes = part_bytes * MULTI_PARTS as u64;

    // --- Initiate ------------------------------------------------------------
    let url = format!("{}?uploads", srv.object_url("large/multi.bin"));
    let resp = srv
        .client
        .post(&url)
        .send()
        .await
        .expect("CreateMultipartUpload failed");
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&xml, "UploadId")
        .expect("UploadId not found in CreateMultipartUpload response")
        .to_string();

    // --- Upload parts --------------------------------------------------------
    // Each part uses a different fill byte so the spot-check can distinguish them.
    let mut etags: Vec<(u32, String)> = Vec::new();
    for part_num in 1..=MULTI_PARTS {
        let fill = (part_num & 0xFF) as u8;
        let stream = fill_stream(part_bytes, fill);
        let body = reqwest::Body::wrap_stream(stream);
        let url = format!(
            "{}?partNumber={part_num}&uploadId={upload_id}",
            srv.object_url("large/multi.bin")
        );
        let resp = srv
            .client
            .put(&url)
            .body(body)
            .send()
            .await
            .unwrap_or_else(|e| panic!("UploadPart {part_num} request failed: {e}"));
        assert_eq!(resp.status(), 200, "UploadPart {part_num} should return 200");
        let etag = resp
            .headers()
            .get("etag")
            .unwrap_or_else(|| panic!("no ETag for part {part_num}"))
            .to_str()
            .unwrap()
            .to_string();
        etags.push((part_num, etag));
    }

    // --- Complete ------------------------------------------------------------
    let parts: Vec<(u32, &str)> = etags.iter().map(|(n, e)| (*n, e.as_str())).collect();
    let url = format!(
        "{}?uploadId={upload_id}",
        srv.object_url("large/multi.bin")
    );
    let resp = srv
        .client
        .post(&url)
        .header("content-type", "application/xml")
        .body(complete_xml(&parts))
        .send()
        .await
        .expect("CompleteMultipartUpload request failed");
    assert_eq!(resp.status(), 200, "CompleteMultipartUpload should return 200");

    // --- HEAD: verify total stored size -------------------------------------
    let resp = srv
        .client
        .head(&srv.object_url("large/multi.bin"))
        .send()
        .await
        .expect("HEAD failed");
    assert_eq!(resp.status(), 200);
    let stored_len: u64 = resp
        .headers()
        .get("content-length")
        .expect("content-length header missing")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(stored_len, total_bytes, "stored size should equal total parts size");

    // --- Spot-check: first byte of each part --------------------------------
    for part_num in 1..=MULTI_PARTS {
        let fill = (part_num & 0xFF) as u8;
        let offset = (part_num as u64 - 1) * part_bytes;
        let resp = srv
            .client
            .get(&srv.object_url("large/multi.bin"))
            .header("range", format!("bytes={offset}-{offset}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("range GET for part {part_num} failed: {e}"));
        assert_eq!(
            resp.status(),
            206,
            "range GET for part {part_num} should return 206"
        );
        let b = resp.bytes().await.unwrap();
        assert_eq!(
            b[0], fill,
            "first byte of part {part_num} should be {fill:#04x}"
        );
    }
}
