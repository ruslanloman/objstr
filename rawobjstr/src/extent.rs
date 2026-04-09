use crate::BLOCK_ALIGNMENT;

/// Size of the CRC32c prefix in each 4KB block.
pub(crate) const BLOCK_CRC_SIZE: usize = 4;
/// Usable data bytes per 4KB block (after the CRC prefix).
pub(crate) const BLOCK_DATA_SIZE: usize = BLOCK_ALIGNMENT as usize - BLOCK_CRC_SIZE; // 4092

/// Number of 4KB blocks needed to store `payload_size` bytes
/// (split into BLOCK_DATA_SIZE chunks, each prefixed with a 4-byte CRC).
pub(crate) fn num_blocks(payload_size: u64) -> u64 {
    if payload_size == 0 {
        return 0;
    }
    payload_size.div_ceil(BLOCK_DATA_SIZE as u64)
}

/// Compute the total padded size of an extent (payload encoded into
/// CRC-protected 4KB blocks).
pub fn padded_extent_size(payload_size: u64) -> crate::Result<u64> {
    num_blocks(payload_size)
        .checked_mul(BLOCK_ALIGNMENT)
        .ok_or_else(|| crate::RawStoreError::ExtentInvalid {
            reason: "padded_extent_size: block count overflow".into(),
        })
}

/// Encode payload into CRC-protected 4KB blocks.
///
/// Each 4KB block = [4B CRC32c][4092B data]. The CRC covers the full 4092-byte
/// data region (including zero-padding in the last block).
#[cfg(test)]
pub(crate) fn encode_blocks(payload: &[u8]) -> Vec<u8> {
    let payload_len = payload.len();
    let n = num_blocks(payload_len as u64) as usize;
    let block_size = BLOCK_ALIGNMENT as usize;
    let mut out = vec![0u8; n * block_size];

    for i in 0..n {
        let payload_start = i * BLOCK_DATA_SIZE;
        let payload_end = (payload_start + BLOCK_DATA_SIZE).min(payload_len);
        let block_offset = i * block_size;

        // Copy payload bytes into this block's data region
        let dst = block_offset + BLOCK_CRC_SIZE;
        out[dst..dst + (payload_end - payload_start)]
            .copy_from_slice(&payload[payload_start..payload_end]);

        // CRC covers the entire 4092-byte data region
        let data_region = &out[block_offset + BLOCK_CRC_SIZE..block_offset + block_size];
        let crc = crc32c::crc32c(data_region);
        out[block_offset..block_offset + BLOCK_CRC_SIZE].copy_from_slice(&crc.to_le_bytes());
    }
    out
}

/// Verify a single 4KB block's CRC and return a reference to its data region.
/// `block_label` is used in the error message to identify the block.
fn verify_block_crc(block: &[u8], block_label: impl std::fmt::Display) -> crate::Result<&[u8]> {
    let stored_crc = u32::from_le_bytes(block[..BLOCK_CRC_SIZE].try_into().unwrap());
    let data = &block[BLOCK_CRC_SIZE..];
    let actual_crc = crc32c::crc32c(data);
    if stored_crc != actual_crc {
        return Err(crate::RawStoreError::DataCorruption {
            path: format!("block {block_label}"),
            expected: stored_crc,
            actual: actual_crc,
        });
    }
    Ok(data)
}

/// Decode CRC-protected blocks, verifying each block's CRC.
/// Returns the content bytes (header + payload).
pub(crate) fn decode_blocks(raw: &[u8], expected_content_len: usize) -> crate::Result<Vec<u8>> {
    let block_size = BLOCK_ALIGNMENT as usize;
    debug_assert_eq!(raw.len() % block_size, 0, "decode_blocks: input not block-aligned");
    let n = raw.len() / block_size;
    let mut content = Vec::with_capacity(expected_content_len);

    for i in 0..n {
        let block = &raw[i * block_size..(i + 1) * block_size];
        let data = verify_block_crc(block, i)?;

        let take = (expected_content_len - content.len()).min(BLOCK_DATA_SIZE);
        content.extend_from_slice(&data[..take]);
    }
    Ok(content)
}

/// Decode a range of CRC-protected blocks and extract a byte range from the content.
///
/// `raw` contains blocks starting from `first_block_idx`.
/// Returns content bytes covering `content_range`.
pub(crate) fn decode_block_range(
    raw: &[u8],
    first_block_idx: u64,
    content_range: std::ops::Range<usize>,
) -> crate::Result<Vec<u8>> {
    let block_size = BLOCK_ALIGNMENT as usize;
    debug_assert_eq!(raw.len() % block_size, 0, "decode_block_range: input not block-aligned");
    let n = raw.len() / block_size;
    let mut result = Vec::with_capacity(content_range.len());

    for i in 0..n {
        let block = &raw[i * block_size..(i + 1) * block_size];
        let data = verify_block_crc(block, first_block_idx as usize + i)?;

        // Content byte range for this block
        let block_content_start = (first_block_idx as usize + i) * BLOCK_DATA_SIZE;
        let block_content_end = block_content_start + BLOCK_DATA_SIZE;

        // Overlap with requested range
        let overlap_start = content_range.start.max(block_content_start);
        let overlap_end = content_range.end.min(block_content_end);

        if overlap_start < overlap_end {
            let data_offset = overlap_start - block_content_start;
            let data_end = overlap_end - block_content_start;
            result.extend_from_slice(&data[data_offset..data_end]);
        }
    }
    Ok(result)
}

// -- Batched (streaming) block encoder + writer ----------------------

/// Number of 4KB blocks per I/O batch (1 MB = 256 blocks).
const BATCH_BLOCKS: usize = 256;

/// Encode payload into CRC-protected 4KB blocks and write them to `io`
/// in 1 MB batches.  This avoids allocating a full-extent-sized
/// intermediate `Vec<u8>` -- peak heap usage is one 1 MB batch buffer.
pub(crate) fn encode_and_write_batched(
    io: &crate::io::DeviceIo,
    extent_offset: u64,
    payload: &[u8],
) -> crate::Result<()> {
    let payload_len = payload.len();
    let n = num_blocks(payload_len as u64) as usize;
    let block_size = BLOCK_ALIGNMENT as usize;

    // Process blocks in batches of BATCH_BLOCKS
    let mut block_idx = 0;
    while block_idx < n {
        let batch_end = (block_idx + BATCH_BLOCKS).min(n);
        let batch_count = batch_end - block_idx;
        let buf_len = batch_count * block_size;
        let mut buf = vec![0u8; buf_len];

        for i in block_idx..batch_end {
            let payload_start = i * BLOCK_DATA_SIZE;
            let payload_end = (payload_start + BLOCK_DATA_SIZE).min(payload_len);
            let local = i - block_idx;
            let block_offset = local * block_size;

            // Copy payload bytes into this block's data region
            let dst = block_offset + BLOCK_CRC_SIZE;
            buf[dst..dst + (payload_end - payload_start)]
                .copy_from_slice(&payload[payload_start..payload_end]);

            // CRC covers the entire 4092-byte data region
            let data_region = &buf[block_offset + BLOCK_CRC_SIZE..block_offset + block_size];
            let crc = crc32c::crc32c(data_region);
            buf[block_offset..block_offset + BLOCK_CRC_SIZE]
                .copy_from_slice(&crc.to_le_bytes());
        }

        let disk_offset = extent_offset + (block_idx as u64) * BLOCK_ALIGNMENT;
        io.pwrite(disk_offset, &buf)?;
        block_idx = batch_end;
    }
    Ok(())
}

// -- Streaming block writer (chunked input) --------------------------

/// Streaming block encoder that accepts payload data in arbitrary chunks
/// and writes CRC-protected 4KB blocks to the device in 1 MB batches.
///
/// Unlike `encode_and_write_batched` (which requires a contiguous `&[u8]`
/// for the full payload), this writer can be fed one chunk at a time --
/// for example, multipart parts read back one at a time, or chunks read
/// from a temp file.  It correctly handles block boundaries that fall in
/// the middle of a chunk.
///
/// Peak heap usage: one 1 MB batch buffer + one 4092-byte partial-block
/// buffer = ~1.004 MB regardless of total payload size.
pub(crate) struct StreamingBlockWriter<'a> {
    io: &'a crate::io::DeviceIo,
    extent_offset: u64,
    /// Batch buffer: encoded blocks waiting to be flushed to disk.
    batch: Vec<u8>,
    /// Number of complete blocks currently in `batch`.
    blocks_in_batch: usize,
    /// Partial block: payload bytes for the block currently being filled.
    /// When this reaches BLOCK_DATA_SIZE bytes it is encoded and moved
    /// into the batch buffer.
    partial: Vec<u8>,
    /// Total number of full blocks already written to disk.
    blocks_written: usize,
}

impl<'a> StreamingBlockWriter<'a> {
    pub fn new(io: &'a crate::io::DeviceIo, extent_offset: u64) -> Self {
        Self {
            io,
            extent_offset,
            batch: Vec::with_capacity(BATCH_BLOCKS * BLOCK_ALIGNMENT as usize),
            blocks_in_batch: 0,
            partial: Vec::with_capacity(BLOCK_DATA_SIZE),
            blocks_written: 0,
        }
    }

    /// Feed a chunk of payload data.  Complete blocks are encoded and
    /// batched; when a batch reaches BATCH_BLOCKS it is flushed to disk.
    pub fn write_chunk(&mut self, mut data: &[u8]) -> crate::Result<()> {
        while !data.is_empty() {
            let space = BLOCK_DATA_SIZE - self.partial.len();
            let take = data.len().min(space);
            self.partial.extend_from_slice(&data[..take]);
            data = &data[take..];

            if self.partial.len() == BLOCK_DATA_SIZE {
                self.seal_block()?;
            }
        }
        Ok(())
    }

    /// Flush any remaining partial block (zero-padded) and any
    /// buffered batch data to disk.  Must be called after the last
    /// `write_chunk`.
    pub fn finish(mut self) -> crate::Result<()> {
        // Seal the last partial block (if any payload bytes remain)
        if !self.partial.is_empty() {
            // Zero-pad to BLOCK_DATA_SIZE
            self.partial.resize(BLOCK_DATA_SIZE, 0);
            self.seal_block()?;
        }
        // Flush remaining batch
        if self.blocks_in_batch > 0 {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Encode the completed partial buffer as a CRC-protected block and
    /// append it to the batch.  Flushes the batch if it is full.
    fn seal_block(&mut self) -> crate::Result<()> {
        debug_assert_eq!(self.partial.len(), BLOCK_DATA_SIZE);
        let crc = crc32c::crc32c(&self.partial);
        self.batch.extend_from_slice(&crc.to_le_bytes());
        self.batch.extend_from_slice(&self.partial);
        self.partial.clear();
        self.blocks_in_batch += 1;

        if self.blocks_in_batch == BATCH_BLOCKS {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Write the batch buffer to the device and reset it.
    fn flush_batch(&mut self) -> crate::Result<()> {
        let disk_offset =
            self.extent_offset + (self.blocks_written as u64) * BLOCK_ALIGNMENT;
        self.io.pwrite(disk_offset, &self.batch)?;
        self.blocks_written += self.blocks_in_batch;
        self.batch.clear();
        self.blocks_in_batch = 0;
        Ok(())
    }
}

// -- Batched (streaming) block reader --------------------------------

/// Read and decode CRC-protected blocks in 1 MB batches.
///
/// Functionally identical to `pread` + `decode_blocks` but reads from
/// disk in 1 MB chunks instead of one giant read, reducing peak memory
/// from 2x extent size to extent size + 1 MB.
pub(crate) fn read_and_decode_batched(
    io: &crate::io::DeviceIo,
    extent_offset: u64,
    payload_size: u64,
) -> crate::Result<Vec<u8>> {
    let payload_len = payload_size as usize;
    let n = num_blocks(payload_size) as usize;
    let block_size = BLOCK_ALIGNMENT as usize;
    let mut payload = Vec::with_capacity(payload_len);

    let mut block_idx = 0;
    while block_idx < n {
        let batch_end = (block_idx + BATCH_BLOCKS).min(n);
        let batch_count = batch_end - block_idx;
        let disk_offset = extent_offset + (block_idx as u64) * BLOCK_ALIGNMENT;
        let read_len = batch_count * block_size;
        let raw = io.pread(disk_offset, read_len)?;

        for i in 0..batch_count {
            let block = &raw[i * block_size..(i + 1) * block_size];
            let data = verify_block_crc(block, block_idx + i)?;
            let take = (payload_len - payload.len()).min(BLOCK_DATA_SIZE);
            payload.extend_from_slice(&data[..take]);
        }
        block_idx = batch_end;
    }
    Ok(payload)
}

/// Iterator that reads and decodes CRC-protected blocks in 1 MB batches,
/// yielding decoded payload chunks.
///
/// Each yielded `Vec<u8>` is up to 1 MB of decoded payload data.
///
/// For streaming GET: create this iterator, then wrap each yielded chunk
/// in `Ok(Bytes::from(chunk))` for an `object_store` stream.
pub(crate) struct StreamingBlockReader {
    io: std::sync::Arc<crate::io::DeviceIo>,
    extent_offset: u64,
    total_blocks: usize,
    payload_len: usize,
    /// Next block index to read
    block_idx: usize,
    /// Bytes of payload already consumed
    payload_consumed: usize,
}

impl StreamingBlockReader {
    pub fn new(
        io: std::sync::Arc<crate::io::DeviceIo>,
        extent_offset: u64,
        payload_size: u64,
    ) -> Self {
        let payload_len = payload_size as usize;
        let total_blocks = num_blocks(payload_size) as usize;
        Self {
            io,
            extent_offset,
            total_blocks,
            payload_len,
            block_idx: 0,
            payload_consumed: 0,
        }
    }

    /// Read the next batch of blocks and return decoded payload bytes.
    /// Returns `None` when all blocks have been read.
    pub fn next_chunk(&mut self) -> crate::Result<Option<Vec<u8>>> {
        if self.block_idx >= self.total_blocks {
            return Ok(None);
        }
        let batch_end = (self.block_idx + BATCH_BLOCKS).min(self.total_blocks);
        let batch_count = batch_end - self.block_idx;
        let disk_offset = self.extent_offset + (self.block_idx as u64) * BLOCK_ALIGNMENT;
        let read_len = batch_count * BLOCK_ALIGNMENT as usize;
        let raw = self.io.pread(disk_offset, read_len)?;

        let block_size = BLOCK_ALIGNMENT as usize;
        let mut chunk = Vec::with_capacity(batch_count * BLOCK_DATA_SIZE);
        for i in 0..batch_count {
            let block = &raw[i * block_size..(i + 1) * block_size];
            let data = verify_block_crc(block, self.block_idx + i)?;
            let take = (self.payload_len - self.payload_consumed).min(BLOCK_DATA_SIZE);
            chunk.extend_from_slice(&data[..take]);
            self.payload_consumed += take;
        }
        self.block_idx = batch_end;
        Ok(Some(chunk))
    }
}

/// A buffered `std::io::Read` adapter over [`StreamingBlockReader`].
///
/// Reads CRC-protected blocks in 1 MB batches from the device and exposes
/// them as a continuous byte stream.  This lets [`StreamingBlockReader`] be
/// wrapped by streaming decompressors (`zstd::stream::read::Decoder`,
/// `flate2::read::GzDecoder`, etc.) without buffering the whole payload.
///
/// Snappy does not have a `Read`-based streaming API so its path still
/// buffers the full compressed payload (not the decompressed form).
pub(crate) struct BlockReaderIo {
    reader: StreamingBlockReader,
    /// Buffered chunk from the last `next_chunk()` call.
    buf: Vec<u8>,
    /// Read position within `buf`.
    pos: usize,
    /// Sticky I/O error from a failed `next_chunk()`.
    err: Option<std::io::Error>,
}

impl BlockReaderIo {
    pub fn new(
        io: std::sync::Arc<crate::io::DeviceIo>,
        extent_offset: u64,
        payload_size: u64,
    ) -> Self {
        Self {
            reader: StreamingBlockReader::new(io, extent_offset, payload_size),
            buf: Vec::new(),
            pos: 0,
            err: None,
        }
    }
}

impl std::io::Read for BlockReaderIo {
    fn read(&mut self, dst: &mut [u8]) -> std::io::Result<usize> {
        if let Some(ref e) = self.err {
            return Err(std::io::Error::new(e.kind(), e.to_string()));
        }

        loop {
            // Drain the current buffer first.
            if self.pos < self.buf.len() {
                let available = self.buf.len() - self.pos;
                let n = available.min(dst.len());
                dst[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }

            // Buffer exhausted -- fetch the next chunk from disk.
            match self.reader.next_chunk() {
                Ok(Some(chunk)) if !chunk.is_empty() => {
                    self.buf = chunk;
                    self.pos = 0;
                    // Loop back to drain.
                }
                Ok(_) => {
                    // EOF
                    return Ok(0);
                }
                Err(e) => {
                    let io_err = std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    );
                    self.err = Some(std::io::Error::new(io_err.kind(), io_err.to_string()));
                    return Err(io_err);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn padded_size() {
        // payload 0 -> 0 blocks = 0
        assert_eq!(padded_extent_size(0).unwrap(), 0);
        // payload 1 -> 1 block = 4096
        assert_eq!(padded_extent_size(1).unwrap(), 4096);
        // payload 4092 -> exactly 1 block
        assert_eq!(padded_extent_size(4092).unwrap(), 4096);
        // payload 4093 -> 2 blocks = 8192
        assert_eq!(padded_extent_size(4093).unwrap(), 8192);
        // payload 8184 (= 4092*2) -> exactly 2 blocks
        assert_eq!(padded_extent_size(8184).unwrap(), 8192);
        // payload 8185 -> 3 blocks = 12288
        assert_eq!(padded_extent_size(8185).unwrap(), 12288);
    }

    #[test]
    fn encode_decode_round_trip() {
        let payload = vec![0xBB; 5000];
        let encoded = encode_blocks(&payload);

        // ceil(5000/4092) = 2 blocks
        assert_eq!(encoded.len(), 8192);

        let decoded = decode_blocks(&encoded, payload.len()).unwrap();
        assert_eq!(&decoded, &payload);
    }

    #[test]
    fn encode_decode_single_block() {
        let payload = vec![0xDD; 100];
        let encoded = encode_blocks(&payload);

        assert_eq!(encoded.len(), 4096);

        let decoded = decode_blocks(&encoded, payload.len()).unwrap();
        assert_eq!(&decoded, &payload);
    }

    #[test]
    fn decode_detects_corruption() {
        let payload = vec![0xFF; 100];
        let mut encoded = encode_blocks(&payload);

        // Corrupt a data byte in the first block
        encoded[BLOCK_CRC_SIZE + 10] ^= 0x01;

        let result = decode_blocks(&encoded, payload.len());
        assert!(result.is_err());
    }

    #[test]
    fn decode_block_range_partial() {
        let payload: Vec<u8> = (0u8..=255).cycle().take(10000).collect();
        let encoded = encode_blocks(&payload);

        // Read payload bytes 100..200
        let payload_range = 100..200;

        // Determine which blocks we need
        let first_block = payload_range.start / BLOCK_DATA_SIZE;
        let last_block = (payload_range.end - 1) / BLOCK_DATA_SIZE;
        let block_size = BLOCK_ALIGNMENT as usize;
        let raw_slice = &encoded[first_block * block_size..(last_block + 1) * block_size];

        let data = decode_block_range(
            raw_slice,
            first_block as u64,
            payload_range,
        )
        .unwrap();

        assert_eq!(data.len(), 100);
        assert_eq!(&data, &payload[100..200]);
    }

    #[test]
    fn block_reader_io_sticky_error() {
        use std::io::Read;

        // Create a payload spanning BATCH_BLOCKS+1 blocks so the first batch
        // (256 blocks = 1 MB) reads successfully and the second batch (1 block)
        // hits a corrupted CRC.
        let blocks = BATCH_BLOCKS + 1;
        let payload_size = blocks * BLOCK_DATA_SIZE;
        let payload = vec![0xAA; payload_size];
        let encoded = encode_blocks(&payload);
        assert_eq!(encoded.len(), blocks * BLOCK_ALIGNMENT as usize);

        // Write encoded data to a temp file via the file handle.
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        {
            use std::io::Write;
            tmp.write_all(&encoded).unwrap();
            tmp.flush().unwrap();
            tmp.as_file().sync_all().unwrap();
        }

        // Corrupt the last block's CRC (block 256, first byte of second batch).
        let corrupt_offset = BATCH_BLOCKS as u64 * BLOCK_ALIGNMENT as u64;
        {
            use std::io::{Seek, SeekFrom, Write};
            let f = tmp.as_file_mut();
            f.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            let mut crc_bytes = [0u8; 4];
            std::io::Read::read_exact(f, &mut crc_bytes).unwrap();
            crc_bytes[0] ^= 0xFF;
            f.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            f.write_all(&crc_bytes).unwrap();
            f.sync_all().unwrap();
        }

        let io = std::sync::Arc::new(
            crate::io::DeviceIo::open_readonly(tmp.path(), false).unwrap(),
        );
        let mut reader = BlockReaderIo::new(io, 0, payload_size as u64);

        // First batch (256 blocks) should read successfully.
        let first_batch_bytes = BATCH_BLOCKS * BLOCK_DATA_SIZE;
        let mut total_read = 0usize;
        let mut buf = vec![0u8; 65536];
        while total_read < first_batch_bytes {
            let n = reader.read(&mut buf).unwrap();
            assert!(n > 0, "should read data from first batch");
            assert!(
                buf[..n].iter().all(|&b| b == 0xAA),
                "data should match payload"
            );
            total_read += n;
        }
        assert_eq!(total_read, first_batch_bytes);

        // Next read fetches the second batch (block 256) and hits corruption.
        let err = reader.read(&mut buf).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("CRC") || msg.contains("crc") || msg.contains("corruption"),
            "expected CRC/corruption error, got: {msg}"
        );

        // Sticky: subsequent reads return the same error without re-reading.
        let err2 = reader.read(&mut buf).unwrap_err();
        let msg2 = err2.to_string();
        assert_eq!(msg, msg2, "sticky error message should be identical");
    }
}
