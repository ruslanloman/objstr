use crate::{align_up, RawStoreError, Result, BLOCK_ALIGNMENT};
use tracing::warn;

/// First-fit extent allocator with coalescing.
///
/// Manages a sorted free list of `(offset, size)` pairs.
/// All allocations are 4KB-aligned.
#[derive(Debug)]
pub(crate) struct ExtentAllocator {
    /// Sorted by offset: (offset, size)
    free_list: Vec<(u64, u64)>,
    /// Minimum allocation alignment
    alignment: u64,
}

impl ExtentAllocator {
    /// Create a new allocator with a single free region spanning the data area.
    pub fn new(data_start: u64, data_end: u64, alignment: u64) -> Self {
        let mut free_list = Vec::new();
        if data_end > data_start {
            free_list.push((data_start, data_end - data_start));
        }
        Self {
            free_list,
            alignment,
        }
    }

    /// Restore allocator from a persisted free list.
    ///
    /// Validates that entries are sorted by offset, non-overlapping,
    /// and clamped to `[data_start, data_end)`.
    pub fn from_free_list(
        mut free_list: Vec<(u64, u64)>,
        data_start: u64,
        data_end: u64,
    ) -> Self {
        // Ensure sorted and non-overlapping
        free_list.sort_by_key(|(o, _)| *o);
        free_list.retain(|(_, s)| *s > 0);

        // Clamp entries to [data_start, data_end) — reject anything out of bounds
        let before_len = free_list.len();
        free_list.retain(|(off, sz)| {
            *off >= data_start
                && off.saturating_add(*sz) <= data_end
        });
        let dropped = before_len - free_list.len();
        if dropped > 0 {
            warn!(
                dropped,
                data_start,
                data_end,
                "allocator: dropped out-of-bounds free list entries"
            );
        }

        Self {
            free_list,
            alignment: BLOCK_ALIGNMENT,
        }
    }

    /// Get a reference to the free list (for persistence).
    pub fn free_list(&self) -> &[(u64, u64)] {
        &self.free_list
    }

    /// Total free space available.
    pub fn free_space(&self) -> u64 {
        self.free_list.iter().map(|(_, s)| s).sum()
    }

    /// Find first free extent that fits `needed` bytes.
    /// Returns the offset of the allocated extent.
    /// Splits the remainder back into the free list.
    pub fn alloc(&mut self, needed: u64) -> Result<u64> {
        let needed = align_up(needed, self.alignment);

        // First-fit search
        let pos = self
            .free_list
            .iter()
            .position(|(_, size)| *size >= needed);

        match pos {
            Some(idx) => {
                let (offset, size) = self.free_list[idx];
                let remaining = size - needed;

                if remaining > 0 {
                    // Split: keep the remainder
                    self.free_list[idx] = (offset + needed, remaining);
                } else {
                    // Exact fit: remove the entry
                    self.free_list.remove(idx);
                }

                Ok(offset)
            }
            None => {
                let largest = self.free_list.iter().map(|(_, s)| *s).max().unwrap_or(0);
                Err(RawStoreError::NoSpace {
                    needed,
                    available: largest,
                })
            }
        }
    }

    /// Return an extent to the free list. Merges with adjacent free extents.
    pub fn free(&mut self, offset: u64, padded_size: u64) {
        // Find insertion point (sorted by offset)
        let insert_pos = self
            .free_list
            .partition_point(|(o, _)| *o < offset);

        self.free_list.insert(insert_pos, (offset, padded_size));

        // Try to merge with the next entry (overflow-safe)
        if insert_pos + 1 < self.free_list.len() {
            let (cur_off, cur_sz) = self.free_list[insert_pos];
            let (next_off, next_sz) = self.free_list[insert_pos + 1];
            if cur_off.saturating_add(cur_sz) == next_off {
                self.free_list[insert_pos] = (cur_off, cur_sz + next_sz);
                self.free_list.remove(insert_pos + 1);
            }
        }

        // Try to merge with the previous entry (overflow-safe)
        if insert_pos > 0 {
            let (prev_off, prev_sz) = self.free_list[insert_pos - 1];
            let (cur_off, cur_sz) = self.free_list[insert_pos];
            if prev_off.saturating_add(prev_sz) == cur_off {
                self.free_list[insert_pos - 1] = (prev_off, prev_sz + cur_sz);
                self.free_list.remove(insert_pos);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_alloc_free() {
        let mut alloc = ExtentAllocator::new(8192, 1024 * 1024, 4096);

        let off1 = alloc.alloc(4096).unwrap();
        assert_eq!(off1, 8192);

        let off2 = alloc.alloc(8192).unwrap();
        assert_eq!(off2, 8192 + 4096);

        // Free first, then second — should coalesce
        alloc.free(off1, 4096);
        alloc.free(off2, 8192);

        // Should have merged back
        assert_eq!(alloc.free_list.len(), 1);
        assert_eq!(alloc.free_list[0], (8192, 1024 * 1024 - 8192));
    }

    #[test]
    fn no_space() {
        let mut alloc = ExtentAllocator::new(8192, 8192 + 4096, 4096);

        let _ = alloc.alloc(4096).unwrap();
        let err = alloc.alloc(4096).unwrap_err();
        assert!(matches!(err, RawStoreError::NoSpace { .. }));
    }

    #[test]
    fn alignment() {
        let mut alloc = ExtentAllocator::new(8192, 1024 * 1024, 4096);

        // Allocate 100 bytes — should round up to 4096
        let off = alloc.alloc(100).unwrap();
        assert_eq!(off, 8192);

        let off2 = alloc.alloc(100).unwrap();
        assert_eq!(off2, 8192 + 4096);
    }

    #[test]
    fn coalesce_three() {
        let mut alloc = ExtentAllocator::new(8192, 8192 + 3 * 4096, 4096);

        let a = alloc.alloc(4096).unwrap();
        let b = alloc.alloc(4096).unwrap();
        let c = alloc.alloc(4096).unwrap();

        // Free in order: middle, then first, then last
        alloc.free(b, 4096);
        alloc.free(a, 4096);
        // First two should have coalesced
        assert_eq!(alloc.free_list.len(), 1);
        assert_eq!(alloc.free_list[0], (8192, 8192));

        alloc.free(c, 4096);
        // All three should be one region
        assert_eq!(alloc.free_list.len(), 1);
        assert_eq!(alloc.free_list[0], (8192, 3 * 4096));
    }

    #[test]
    fn from_free_list_filters_out_of_bounds() {
        let data_start = 8192u64;
        let data_end = 8192 + 10 * 4096;

        let free_list = vec![
            (0, 4096),                          // before data_start -- rejected
            (4096, 4096),                        // before data_start -- rejected
            (8192, 4096),                        // valid
            (8192 + 4096, 4096),                 // valid
            (data_end - 4096, 4096),             // valid (last block)
            (data_end, 4096),                    // beyond data_end -- rejected
            (data_end + 4096, 8192),             // beyond data_end -- rejected
            (8192 + 2 * 4096, 0),                // zero-size -- rejected
        ];

        let alloc = ExtentAllocator::from_free_list(free_list, data_start, data_end);
        let fl = alloc.free_list();

        // Only 3 valid entries should survive
        assert_eq!(fl.len(), 3, "expected 3 valid entries, got {:?}", fl);
        assert_eq!(fl[0], (8192, 4096));
        assert_eq!(fl[1], (8192 + 4096, 4096));
        assert_eq!(fl[2], (data_end - 4096, 4096));
    }

    #[test]
    fn from_free_list_rejects_spanning_entry() {
        let data_start = 8192u64;
        let data_end = 8192 + 4 * 4096;

        // Entry that starts inside but extends past data_end
        let free_list = vec![(8192, 100 * 4096)];
        let alloc = ExtentAllocator::from_free_list(free_list, data_start, data_end);
        assert!(
            alloc.free_list().is_empty(),
            "entry spanning past data_end should be rejected: {:?}",
            alloc.free_list()
        );
    }
}
