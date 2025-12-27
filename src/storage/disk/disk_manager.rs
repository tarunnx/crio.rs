use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::common::{PageId, Result, PAGE_SIZE};

/// DiskManager is responsible for reading and writing pages to/from disk.
/// It manages a single database file and tracks the number of pages allocated.
/// Supports both single-page and sequential multi-page I/O for performance.
pub struct DiskManager {
    /// The database file
    db_file: Mutex<File>,
    /// Path to the database file
    db_path: String,
    /// Number of pages currently allocated
    num_pages: AtomicU32,
    /// Number of disk reads performed (counts each I/O operation, not pages)
    num_reads: AtomicU32,
    /// Number of disk writes performed (counts each I/O operation, not pages)
    num_writes: AtomicU32,
}

impl DiskManager {
    /// Creates a new DiskManager for the given database file path.
    /// Creates the file if it doesn't exist.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let path_str = db_path.as_ref().to_string_lossy().to_string();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&db_path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let num_pages = (file_size / PAGE_SIZE as u64) as u32;

        Ok(Self {
            db_file: Mutex::new(file),
            db_path: path_str,
            num_pages: AtomicU32::new(num_pages),
            num_reads: AtomicU32::new(0),
            num_writes: AtomicU32::new(0),
        })
    }

    /// Reads a page from disk into the provided buffer.
    /// The buffer must be exactly PAGE_SIZE bytes.
    pub fn read_page(&self, page_id: PageId, data: &mut [u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "Buffer must be PAGE_SIZE bytes");

        let offset = (page_id.as_u32() as u64) * (PAGE_SIZE as u64);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset))?;

        // If we're reading beyond the file, fill with zeros
        let bytes_read = file.read(data)?;
        if bytes_read < PAGE_SIZE {
            data[bytes_read..].fill(0);
        }

        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Writes a page to disk from the provided buffer.
    /// The buffer must be exactly PAGE_SIZE bytes.
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        assert_eq!(data.len(), PAGE_SIZE, "Buffer must be PAGE_SIZE bytes");

        let offset = (page_id.as_u32() as u64) * (PAGE_SIZE as u64);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;

        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Reads multiple contiguous pages from disk in a single I/O operation.
    /// This is much faster than calling read_page() multiple times because:
    /// - Only ONE seek operation instead of N seeks
    /// - ONE large read instead of N small reads
    /// - Better utilization of disk bandwidth
    ///
    /// The buffer must be exactly (num_pages * PAGE_SIZE) bytes.
    /// Pages are read starting from start_page_id sequentially.
    pub fn read_pages(&self, start_page_id: PageId, num_pages: u32, data: &mut [u8]) -> Result<()> {
        let expected_size = (num_pages as usize) * PAGE_SIZE;
        assert_eq!(
            data.len(),
            expected_size,
            "Buffer must be {} bytes for {} pages",
            expected_size,
            num_pages
        );

        let offset = (start_page_id.as_u32() as u64) * (PAGE_SIZE as u64);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset))?;

        // Read all pages in one I/O operation
        let bytes_read = file.read(data)?;
        if bytes_read < expected_size {
            // Fill remaining with zeros if we read past end of file
            data[bytes_read..].fill(0);
        }

        // Count as ONE read operation (the whole point of sequential I/O)
        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Writes multiple contiguous pages to disk in a single I/O operation.
    /// This is much faster than calling write_page() multiple times because:
    /// - Only ONE seek operation instead of N seeks
    /// - ONE large write instead of N small writes
    /// - Better utilization of disk bandwidth
    ///
    /// The buffer must be exactly (num_pages * PAGE_SIZE) bytes.
    /// Pages are written starting from start_page_id sequentially.
    pub fn write_pages(&self, start_page_id: PageId, num_pages: u32, data: &[u8]) -> Result<()> {
        let expected_size = (num_pages as usize) * PAGE_SIZE;
        assert_eq!(
            data.len(),
            expected_size,
            "Buffer must be {} bytes for {} pages",
            expected_size,
            num_pages
        );

        let offset = (start_page_id.as_u32() as u64) * (PAGE_SIZE as u64);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;

        // Count as ONE write operation
        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Allocates a new page on disk and returns its page ID.
    /// The page is zero-initialized.
    pub fn allocate_page(&self) -> Result<PageId> {
        let page_id = PageId::new(self.num_pages.fetch_add(1, Ordering::SeqCst));

        // Zero-initialize the new page
        let zeros = [0u8; PAGE_SIZE];
        self.write_page(page_id, &zeros)?;

        Ok(page_id)
    }

    /// Deallocates a page. In this simple implementation, we don't actually
    /// reclaim the space - we just note that the page is no longer in use.
    /// A more sophisticated implementation would maintain a free list.
    pub fn deallocate_page(&self, _page_id: PageId) -> Result<()> {
        // In a real implementation, we would add this page to a free list
        // For now, we just leave it allocated but unused
        Ok(())
    }

    /// Returns the number of pages currently allocated.
    pub fn get_num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::Relaxed)
    }

    /// Returns the number of disk reads performed.
    pub fn get_num_reads(&self) -> u32 {
        self.num_reads.load(Ordering::Relaxed)
    }

    /// Returns the number of disk writes performed.
    pub fn get_num_writes(&self) -> u32 {
        self.num_writes.load(Ordering::Relaxed)
    }

    /// Returns the path to the database file.
    pub fn get_db_path(&self) -> &str {
        &self.db_path
    }

    /// Flushes any buffered writes to disk.
    pub fn sync(&self) -> Result<()> {
        let file = self.db_file.lock();
        file.sync_all()?;
        Ok(())
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        // Ensure all data is flushed to disk
        let file = self.db_file.get_mut();
        let _ = file.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_disk_manager_new() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        assert_eq!(dm.get_num_pages(), 0);
    }

    #[test]
    fn test_disk_manager_allocate_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, PageId::new(0));
        assert_eq!(dm.get_num_pages(), 1);

        let page_id2 = dm.allocate_page().unwrap();
        assert_eq!(page_id2, PageId::new(1));
        assert_eq!(dm.get_num_pages(), 2);
    }

    #[test]
    fn test_disk_manager_read_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        let page_id = dm.allocate_page().unwrap();

        // Write data
        let mut write_data = [0u8; PAGE_SIZE];
        write_data[0] = 42;
        write_data[100] = 255;
        write_data[PAGE_SIZE - 1] = 128;
        dm.write_page(page_id, &write_data).unwrap();

        // Read it back
        let mut read_data = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_data).unwrap();

        assert_eq!(read_data[0], 42);
        assert_eq!(read_data[100], 255);
        assert_eq!(read_data[PAGE_SIZE - 1], 128);
    }

    #[test]
    fn test_disk_manager_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write data
        {
            let dm = DiskManager::new(&path).unwrap();
            let page_id = dm.allocate_page().unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 123;
            dm.write_page(page_id, &data).unwrap();
        }

        // Read it back with a new DiskManager
        {
            let dm = DiskManager::new(&path).unwrap();
            assert_eq!(dm.get_num_pages(), 1);

            let mut data = [0u8; PAGE_SIZE];
            dm.read_page(PageId::new(0), &mut data).unwrap();
            assert_eq!(data[0], 123);
        }
    }

    #[test]
    fn test_disk_manager_sequential_read_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        // Allocate 4 pages
        for _ in 0..4 {
            dm.allocate_page().unwrap();
        }

        // Write 4 pages sequentially in ONE operation
        let mut write_data = vec![0u8; PAGE_SIZE * 4];
        write_data[0] = 1; // Page 0
        write_data[PAGE_SIZE] = 2; // Page 1
        write_data[PAGE_SIZE * 2] = 3; // Page 2
        write_data[PAGE_SIZE * 3] = 4; // Page 3

        dm.write_pages(PageId::new(0), 4, &write_data).unwrap();

        // Read all 4 pages back in ONE operation
        let mut read_data = vec![0u8; PAGE_SIZE * 4];
        dm.read_pages(PageId::new(0), 4, &mut read_data).unwrap();

        // Verify data
        assert_eq!(read_data[0], 1);
        assert_eq!(read_data[PAGE_SIZE], 2);
        assert_eq!(read_data[PAGE_SIZE * 2], 3);
        assert_eq!(read_data[PAGE_SIZE * 3], 4);

        // Should be only 1 read operation
        assert_eq!(dm.get_num_reads(), 1);
    }

    #[test]
    fn test_sequential_vs_random_io_count() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        // Allocate 8 pages
        for _ in 0..8 {
            dm.allocate_page().unwrap();
        }

        // Random access: 8 separate reads = 8 I/O operations
        for i in 0..8 {
            let mut data = [0u8; PAGE_SIZE];
            dm.read_page(PageId::new(i), &mut data).unwrap();
        }
        assert_eq!(dm.get_num_reads(), 8);

        // Create a new DiskManager to reset counters
        let dm2 = DiskManager::new(temp_file.path()).unwrap();

        // Sequential access: 1 bulk read = 1 I/O operation
        let mut data = vec![0u8; PAGE_SIZE * 8];
        dm2.read_pages(PageId::new(0), 8, &mut data).unwrap();
        assert_eq!(dm2.get_num_reads(), 1); // 8x fewer I/O operations!
    }
}
