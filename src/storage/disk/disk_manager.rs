use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::common::{CrioError, PageId, Result, PAGE_SIZE};
use crate::storage::page::{DirectoryPage, DirectoryPageRef};

use super::extent_allocator::ExtentAllocator;

pub const DIRECTORY_PAGE_ID: PageId = PageId::new_const(0);

/// DiskManager is responsible for reading and writing pages to/from disk.
/// It manages a single database file and tracks the number of pages allocated.
/// Supports both single-page and sequential multi-page I/O for performance.
/// Uses extent-based allocation to keep pages for the same table contiguous.
pub struct DiskManager {
    db_file: Mutex<File>,
    db_path: String,
    num_pages: AtomicU32,
    num_reads: AtomicU32,
    num_writes: AtomicU32,
    extent_allocator: ExtentAllocator,
}

impl DiskManager {
    /// Creates a new DiskManager for the given database file path.
    /// If the file doesn't exist, creates it and initializes the directory page.
    /// If the file exists, validates the directory page.
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

        let extent_allocator = if num_pages > 0 {
            ExtentAllocator::from_existing(num_pages)
        } else {
            ExtentAllocator::new()
        };

        let dm = Self {
            db_file: Mutex::new(file),
            db_path: path_str,
            num_pages: AtomicU32::new(num_pages),
            num_reads: AtomicU32::new(0),
            num_writes: AtomicU32::new(0),
            extent_allocator,
        };

        if num_pages == 0 {
            dm.init_directory_page()?;
        } else {
            dm.validate_directory_page()?;
        }

        Ok(dm)
    }

    fn init_directory_page(&self) -> Result<()> {
        let mut data = [0u8; PAGE_SIZE];
        {
            let mut dir_page = DirectoryPage::new(&mut data);
            dir_page.init();
        }

        self.num_pages.store(1, Ordering::SeqCst);

        let mut file = self.db_file.lock();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data)?;
        file.flush()?;

        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn validate_directory_page(&self) -> Result<()> {
        let mut data = [0u8; PAGE_SIZE];

        {
            let mut file = self.db_file.lock();
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut data)?;
        }

        let dir_page = DirectoryPageRef::new(&data);
        if !dir_page.is_valid() {
            return Err(CrioError::InvalidDatabaseFile);
        }

        Ok(())
    }

    pub fn read_directory_page(&self, data: &mut [u8]) -> Result<()> {
        self.read_page(DIRECTORY_PAGE_ID, data)
    }

    pub fn write_directory_page(&self, data: &[u8]) -> Result<()> {
        self.write_page(DIRECTORY_PAGE_ID, data)
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
    /// Note: For better sequential access, use allocate_page_for_table() instead.
    pub fn allocate_page(&self) -> Result<PageId> {
        let page_id = PageId::new(self.num_pages.fetch_add(1, Ordering::SeqCst));

        let zeros = [0u8; PAGE_SIZE];
        self.write_page(page_id, &zeros)?;

        Ok(page_id)
    }

    /// Allocates a new page for a specific table, keeping pages contiguous.
    /// Pages for the same table are allocated within the same extent when possible.
    /// This maximizes sequential access during table scans.
    pub fn allocate_page_for_table(&self, table_id: u32) -> Result<PageId> {
        let page_id = self.extent_allocator.allocate_page_for_table(table_id)?;

        let required_pages = (page_id.as_u32() + 1) as u32;
        let current_pages = self.num_pages.load(Ordering::Relaxed);
        if required_pages > current_pages {
            self.num_pages.store(required_pages, Ordering::SeqCst);
        }

        let zeros = [0u8; PAGE_SIZE];
        self.write_page(page_id, &zeros)?;

        Ok(page_id)
    }

    /// Allocates a full extent (8 contiguous pages) for a table.
    /// Returns all page IDs in the extent. Ideal for bulk table creation.
    pub fn allocate_extent_for_table(&self, table_id: u32) -> Result<Vec<PageId>> {
        let pages = self.extent_allocator.allocate_extent_for_table(table_id)?;

        if let Some(last_page) = pages.last() {
            let required_pages = last_page.as_u32() + 1;
            let current_pages = self.num_pages.load(Ordering::Relaxed);
            if required_pages > current_pages {
                self.num_pages.store(required_pages, Ordering::SeqCst);
            }
        }

        let zeros = vec![0u8; PAGE_SIZE * pages.len()];
        if let Some(first_page) = pages.first() {
            self.write_pages(*first_page, pages.len() as u32, &zeros)?;
        }

        Ok(pages)
    }

    /// Returns contiguous page ranges for a table.
    /// Each tuple is (start_page_id, num_pages) representing a contiguous range.
    /// Use this information to perform sequential reads during table scans.
    pub fn get_table_page_ranges(&self, table_id: u32) -> Vec<(PageId, u32)> {
        self.extent_allocator.get_contiguous_pages(table_id)
    }

    /// Deallocates a page.
    pub fn deallocate_page(&self, page_id: PageId) -> Result<()> {
        self.extent_allocator.deallocate_page(page_id);
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
        assert_eq!(dm.get_num_pages(), 1); // Directory page at page 0
    }

    #[test]
    fn test_disk_manager_allocate_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();

        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, PageId::new(1)); // Page 0 is directory
        assert_eq!(dm.get_num_pages(), 2);

        let page_id2 = dm.allocate_page().unwrap();
        assert_eq!(page_id2, PageId::new(2));
        assert_eq!(dm.get_num_pages(), 3);
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

        {
            let dm = DiskManager::new(&path).unwrap();
            let page_id = dm.allocate_page().unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 123;
            dm.write_page(page_id, &data).unwrap();
        }

        {
            let dm = DiskManager::new(&path).unwrap();
            assert_eq!(dm.get_num_pages(), 2); // Directory + 1 data page

            let mut data = [0u8; PAGE_SIZE];
            dm.read_page(PageId::new(1), &mut data).unwrap();
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
