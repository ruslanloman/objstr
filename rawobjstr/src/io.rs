use std::fs::{File, OpenOptions};
use std::ops::{Deref, DerefMut};
use std::path::Path;

use crate::{Result, BLOCK_ALIGNMENT};

/// A heap-allocated byte buffer whose backing memory is aligned to
/// [`BLOCK_ALIGNMENT`] (4096). Correctly deallocates with the original
/// `Layout` so there is no allocator mismatch.
struct AlignedVec {
    ptr: *mut u8,
    len: usize,
    layout: std::alloc::Layout,
}

// SAFETY: The buffer is an owned heap allocation not shared across threads
// without external synchronisation, same as Vec<u8>.
unsafe impl Send for AlignedVec {}
unsafe impl Sync for AlignedVec {}

impl Deref for AlignedVec {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for self.len bytes (initialised to zero at allocation).
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for AlignedVec {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for self.len bytes (exclusively owned).
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedVec {
    fn drop(&mut self) {
        // SAFETY: deallocate with the same layout used for allocation.
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

impl AlignedVec {
    /// Copy the first `len` bytes into a new `Vec<u8>`, discarding alignment.
    fn to_vec(&self, len: usize) -> Vec<u8> {
        self[..len].to_vec()
    }
}

/// Allocate a zeroed buffer aligned to [`BLOCK_ALIGNMENT`] (4096).
///
/// O_DIRECT requires user-space buffers to be sector-aligned.
///
/// # Panics
/// Panics if `len` is 0.
fn aligned_buf(len: usize) -> AlignedVec {
    assert!(len > 0, "aligned_buf: len must be > 0");
    let layout = std::alloc::Layout::from_size_align(len, BLOCK_ALIGNMENT as usize)
        .expect("invalid layout");
    // SAFETY: layout has non-zero size (asserted above) and valid alignment.
    // The memory is immediately initialised to zero.
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    AlignedVec { ptr, len, layout }
}

/// Round `n` up to the next multiple of [`BLOCK_ALIGNMENT`].
fn align_len(n: usize) -> usize {
    let a = BLOCK_ALIGNMENT as usize;
    n.checked_add(a - 1)
        .expect("align_len: size overflow")
        & !(a - 1)
}

/// Low-level device I/O: wraps a File with pread/pwrite semantics.
///
/// When `direct` is true the file is opened with `O_DIRECT` (Linux only)
/// and all reads/writes use aligned buffers and aligned sizes.
pub struct DeviceIo {
    file: File,
    /// Whether O_DIRECT is active for this file descriptor.
    direct: bool,
    /// Whether the file was opened read-only (O_RDONLY).
    read_only: bool,
}

impl std::fmt::Debug for DeviceIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceIo")
            .field("direct", &self.direct)
            .finish()
    }
}

impl DeviceIo {
    /// Acquire an exclusive advisory lock on an open file descriptor.
    ///
    /// Uses `flock(LOCK_EX | LOCK_NB)` so the call never blocks -- it
    /// returns `DeviceLocked` immediately if another process holds the
    /// lock.  The lock is automatically released when the `File` is
    /// dropped (fd closed).
    #[cfg(unix)]
    fn acquire_exclusive_lock(file: &File, path: &Path) -> Result<()> {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(crate::RawStoreError::DeviceLocked {
                    path: path.to_string_lossy().into_owned(),
                });
            }
            return Err(err.into());
        }
        Ok(())
    }

    /// Open an existing device or file for read/write.
    ///
    /// Acquires an exclusive advisory lock (`flock LOCK_EX`) to prevent
    /// multiple writer processes on the same device.  Returns
    /// `DeviceLocked` if another writer already holds the lock.
    pub fn open(path: &Path, direct: bool) -> Result<Self> {
        let file = Self::open_file(path, direct)?;
        #[cfg(unix)]
        Self::acquire_exclusive_lock(&file, path)?;
        Ok(Self { file, direct, read_only: false })
    }

    /// Open an existing device or file in read-only mode (O_RDONLY).
    ///
    /// No advisory lock is taken -- multiple readers can coexist safely
    /// via positional I/O (`pread`).
    pub fn open_readonly(path: &Path, direct: bool) -> Result<Self> {
        let file = Self::open_file_readonly(path, direct)?;
        Ok(Self { file, direct, read_only: true })
    }

    /// Create a regular file of the given size, or open a block device for formatting.
    ///
    /// Acquires an exclusive advisory lock (same as `open`).
    pub fn create(path: &Path, size: u64, direct: bool) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            if let Ok(meta) = std::fs::metadata(path) {
                if meta.file_type().is_block_device() {
                    let file = Self::open_file(path, direct)?;
                    Self::acquire_exclusive_lock(&file, path)?;
                    return Ok(Self { file, direct, read_only: false });
                }
            }
        }

        // Create the file first without O_DIRECT (set_len may not work with it),
        // then reopen with O_DIRECT if requested.
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            file.set_len(size)?;
        }

        let file = Self::open_file(path, direct)?;
        #[cfg(unix)]
        Self::acquire_exclusive_lock(&file, path)?;
        Ok(Self { file, direct, read_only: false })
    }

    /// Open a file for read/write with optional O_DIRECT.
    fn open_file(path: &Path, direct: bool) -> Result<File> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        #[cfg(target_os = "linux")]
        if direct {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        Ok(file)
    }

    /// Open a file read-only with optional O_DIRECT.
    fn open_file_readonly(path: &Path, direct: bool) -> Result<File> {
        let mut opts = OpenOptions::new();
        opts.read(true);

        #[cfg(target_os = "linux")]
        if direct {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        Ok(file)
    }

    /// Whether O_DIRECT is active.
    pub fn is_direct(&self) -> bool {
        self.direct
    }

    /// Whether this handle was opened read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Get the device/file size.
    ///
    /// For regular files this returns `metadata().len()`.
    /// For Linux block devices this uses the `BLKGETSIZE64` ioctl
    /// (since `metadata().len()` always returns 0 for block devices).
    pub fn size(&self) -> Result<u64> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::FileTypeExt;
            use std::os::unix::io::AsRawFd;
            let meta = self.file.metadata()?;
            if meta.file_type().is_block_device() {
                let fd = self.file.as_raw_fd();
                let mut size: u64 = 0;
                // BLKGETSIZE64 ioctl: returns the block device size in bytes.
                // The constant 0x80081272 is the Linux kernel ioctl request code
                // for BLKGETSIZE64 on x86_64 (defined in <linux/fs.h>).
                let ret =
                    unsafe { libc::ioctl(fd, 0x80081272u64 as libc::c_ulong, &mut size) };
                if ret != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                return Ok(size);
            }
        }
        Ok(self.file.metadata()?.len())
    }

    /// Write `data` at the given byte offset.
    ///
    /// When O_DIRECT is active the write is padded to a 4096-byte boundary
    /// using an aligned intermediate buffer.
    ///
    /// Uses positional I/O (`pwrite64`) so the call is safe with `&self`
    /// and multiple threads can issue writes concurrently.
    pub fn pwrite(&self, offset: u64, data: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            if self.direct {
                assert!(
                    offset % BLOCK_ALIGNMENT == 0,
                    "O_DIRECT pwrite: offset {offset} is not {BLOCK_ALIGNMENT}-byte aligned",
                );
                let aligned_ptr = data.as_ptr() as usize % BLOCK_ALIGNMENT as usize == 0;
                let aligned_size = data.len() % BLOCK_ALIGNMENT as usize == 0;
                if aligned_ptr && aligned_size {
                    self.file.write_all_at(data, offset)?;
                } else {
                    let padded_len = align_len(data.len());
                    let mut buf = aligned_buf(padded_len);
                    buf[..data.len()].copy_from_slice(data);
                    self.file.write_all_at(&buf, offset)?;
                }
            } else {
                self.file.write_all_at(data, offset)?;
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, data);
            unimplemented!("RawObjectStore requires Unix for positional I/O")
        }
    }

    /// Read `len` bytes from the given byte offset.
    ///
    /// When O_DIRECT is active the read is performed with an aligned buffer
    /// rounded up to a 4096-byte boundary, then truncated to the requested length.
    ///
    /// Uses positional I/O (`pread64`) so the call is safe with `&self`
    /// and multiple threads can issue reads concurrently.
    pub fn pread(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            if self.direct {
                assert!(
                    offset % BLOCK_ALIGNMENT == 0,
                    "O_DIRECT pread: offset {offset} is not {BLOCK_ALIGNMENT}-byte aligned",
                );
                let padded_len = align_len(len);
                let mut buf = aligned_buf(padded_len);
                self.file.read_exact_at(&mut buf, offset)?;
                Ok(buf.to_vec(len))
            } else {
                let mut buf = vec![0u8; len];
                self.file.read_exact_at(&mut buf, offset)?;
                Ok(buf)
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, len);
            unimplemented!("RawObjectStore requires Unix for positional I/O")
        }
    }

    /// Flush all pending writes to disk.
    pub fn sync(&self) -> Result<()> {
        if self.read_only {
            return Err(crate::RawStoreError::ReadOnly);
        }
        self.file.sync_all()?;
        Ok(())
    }
}
